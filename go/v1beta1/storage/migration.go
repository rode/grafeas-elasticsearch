package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"go.uber.org/zap"
)

type ESMigrator struct {
	client   *elasticsearch.Client
	logger   *zap.Logger
	mappings map[string]*VersionedMapping // resource types -> latest version of mappings
}

func NewESMigrator(logger *zap.Logger, client *elasticsearch.Client) *ESMigrator {
	mappings := map[string]*VersionedMapping{}

	return &ESMigrator{
		client:   client,
		logger:   logger,
		mappings: mappings,
	}
}

type ESBlockIndex struct {
	Name    string `json:"name"`
	Blocked bool   `json:"blocked"`
}

type ESBlockResponse struct {
	Acknowledged       bool           `json:"acknowledged"`
	ShardsAcknowledged bool           `json:"shards_acknowledged"`
	Indices            []ESBlockIndex `json:"indices"`
}

type ESTaskCreationResponse struct {
	Task string `json:"task"`
}

type ESTask struct {
	Completed bool `json:"completed"`
}

type ESActions struct {
	Add    *ESIndexAlias `json:"add,omitempty"`
	Remove *ESIndexAlias `json:"remove,omitempty"`
}

type ESIndexAlias struct {
	Index string `json:"index"`
	Alias string `json:"alias"`
}

type ESIndexAliasRequest struct {
	Actions []ESActions `json:"actions"`
}

type Migration struct {
	DocumentKind string
	Index        string
	Alias        string
}

type ESReindex struct {
	Source      *ReindexFields `json:"source"`
	Destination *ReindexFields `json:"dest"`
}

type ReindexFields struct {
	Index string `json:"index"`
}

type VersionedMapping struct {
	Version  string                 `json:"version"`
	Mappings map[string]interface{} `json:"mappings"`
}

func (e *ESMigrator) LoadMappings(dir string) error {
	if err := filepath.Walk(dir, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		localPath := strings.TrimPrefix(filePath, dir+"/")
		fileName := path.Base(localPath)
		fileNameWithoutExtension := strings.TrimSuffix(fileName, filepath.Ext(fileName))
		versionedMappingJson, err := ioutil.ReadFile(filePath)
		if err != nil {
			return err
		}
		var mapping VersionedMapping

		if err := json.Unmarshal(versionedMappingJson, &mapping); err != nil {
			return err
		}

		e.mappings[fileNameWithoutExtension] = &mapping

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (e *ESMigrator) Migrate(ctx context.Context, migration *Migration) error {
	log := e.logger.Named("Migrate").With(zap.String("indexName", migration.Index))
	log.Info("Starting migration")

	log.Info("Placing write block on index")
	res, err := e.client.Indices.AddBlock([]string{migration.Index}, "write", e.client.Indices.AddBlock.WithContext(ctx))
	if err := getErrorFromESResponse(res, err); err != nil {
		return err
	}

	blockResponse := &ESBlockResponse{}
	if err := decodeResponse(res.Body, blockResponse); err != nil {
		return err
	}

	if len(blockResponse.Indices) == 0 { // TODO: already blocked, should poll for migration completion
		log.Info("Index is already blocked")
		return fmt.Errorf("unblocked")
	}

	index := blockResponse.Indices[0] // TODO: length check
	// TODO: do we need to check acknowledged and shards acknowledged?
	if !(blockResponse.Acknowledged && blockResponse.ShardsAcknowledged && index.Name == migration.Index && index.Blocked) {
		log.Error("Write block unsuccessful", zap.Any("response", blockResponse))
		return fmt.Errorf("unable to block writes for index: %s", migration.Index)
	}

	versionedMapping := e.mappings[migration.DocumentKind]
	newIndexName := fmt.Sprintf("%s-%s", migration.Index, versionedMapping.Version)
	log = log.With(zap.String("newIndex", newIndexName))
	indexBody := map[string]interface{}{
		"mappings": versionedMapping.Mappings,
	}
	indexCreateBody, _ := encodeRequest(&indexBody)

	log.Info("Creating new index")
	res, err = e.client.Indices.Create(
		newIndexName,
		e.client.Indices.Create.WithContext(ctx),
		e.client.Indices.Create.WithBody(indexCreateBody),
	)
	if err := getErrorFromESResponse(res, err); err != nil {
		return err
	}

	reindexReq := &ESReindex{
		Source:      &ReindexFields{Index: migration.Index},
		Destination: &ReindexFields{Index: newIndexName},
	}
	reindexBody, _ := encodeRequest(reindexReq)
	log.Info("Starting reindex")
	res, err = e.client.Reindex(
		reindexBody,
		e.client.Reindex.WithContext(ctx),
		e.client.Reindex.WithWaitForCompletion(false))
	if err := getErrorFromESResponse(res, err); err != nil {
		return err
	}
	taskCreationResponse := &ESTaskCreationResponse{}

	_ = decodeResponse(res.Body, taskCreationResponse)
	log.Info("Reindex started", zap.String("taskId", taskCreationResponse.Task))

	for i := 0; i < 10; i++ {
		log.Info("Polling task API", zap.String("taskId", taskCreationResponse.Task))
		res, err = e.client.Tasks.Get(taskCreationResponse.Task, e.client.Tasks.Get.WithContext(ctx))
		if err := getErrorFromESResponse(res, err); err != nil {
			return err
		}

		task := &ESTask{}
		_ = decodeResponse(res.Body, task)

		if task.Completed {
			log.Info("Reindex completed")
			break
		}

		log.Info("Task incomplete, waiting before polling again", zap.String("taskId", taskCreationResponse.Task))
		time.Sleep(time.Second * 10)
	}
	aliasReq := &ESIndexAliasRequest{
		Actions: []ESActions{
			{
				Remove: &ESIndexAlias{
					Index: migration.Index,
					Alias: migration.Alias,
				},
			},
			{
				Add: &ESIndexAlias{
					Index: newIndexName,
					Alias: migration.Alias,
				},
			},
		},
	}

	aliasReqBody, _ := encodeRequest(aliasReq)

	res, err = e.client.Indices.UpdateAliases(
		aliasReqBody,
		e.client.Indices.UpdateAliases.WithContext(ctx),
	)
	if err := getErrorFromESResponse(res, err); err != nil {
		return err
	}

	res, err = e.client.Indices.Delete(
		[]string{migration.Index},
		e.client.Indices.Delete.WithContext(ctx),
	)

	// TODO: remove old index from metadata doc and add new index at appropriate version

	return getErrorFromESResponse(res, err)
}

func (e *ESMigrator) GetMigrations(ctx context.Context) ([]*Migration, error) {
	createDoc := false
	metadataIndexName := "grafeas-metadata"
	var indexDoc map[string]interface{}
	res, err := e.client.Get(metadataIndexName, "grafeas-indices-metadata", e.client.Get.WithContext(ctx))

	if err != nil {
		return nil, err
	}

	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusNotFound {
		return nil, fmt.Errorf("Unexpected status code from Elasticsearch %d", res.StatusCode)
	}
	if res.StatusCode == http.StatusNotFound {
		createDoc = true
		indexDoc = map[string]interface{}{}
	} else {
		docResponse := map[string]interface{}{}
		_ = decodeResponse(res.Body, &docResponse)
		indexDoc = docResponse["_source"].(map[string]interface{})
	}

	res, err = e.client.Indices.Get([]string{"_all"}, e.client.Indices.Get.WithContext(ctx))
	if err := getErrorFromESResponse(res, err); err != nil {
		return nil, err
	}

	allIndices := map[string]interface{}{}

	_ = decodeResponse(res.Body, &allIndices)
	for indexName, index := range allIndices {
		if indexName == metadataIndexName || !strings.HasPrefix(indexName, "grafeas") {
			continue
		}

		index := index.(map[string]interface{})
		mappings := index["mappings"].(map[string]interface{})
		meta := mappings["_meta"].(map[string]interface{})
		metaType := meta["type"].(string)

		if metaType == "grafeas" {
			if _, ok := indexDoc[indexName]; !ok {
				indexDoc[indexName] = map[string]interface{}{
					"mappingsVersion": "v1",
				}
			}
		}
	}
	var migrations []*Migration
	for indexName, metadata := range indexDoc {
		metadataMap := metadata.(map[string]interface{})
		res := strings.Split(indexName, "-")
		docKind := res[len(res)-1]
		versionedMapping := e.mappings[docKind]
		if metadataMap["mappingsVersion"] != versionedMapping.Version {
			indexData := allIndices[indexName].(map[string]interface{})
			aliases := indexData["aliases"].(map[string]interface{})
			alias := ""
			for key, _ := range aliases {
				alias = key
			}
			migrations = append(migrations, &Migration{Index: indexName, DocumentKind: docKind, Alias: alias})
		}
	}
	if createDoc {
		docBody, _ := encodeRequest(indexDoc)
		res, err = e.client.Create(metadataIndexName, "grafeas-indices-metadata", docBody, e.client.Create.WithContext(ctx))

	} else {
		doc := map[string]interface{}{
			"doc": indexDoc,
		}
		docUpdateReq, _ := encodeRequest(doc)
		res, err = e.client.Update(metadataIndexName, "grafeas-indices-metadata", docUpdateReq, e.client.Update.WithContext(ctx))
	}
	arr, _ := ioutil.ReadAll(res.Body)
	fmt.Printf("Update response %s\n", arr)
	if err := getErrorFromESResponse(res, err); err != nil {
		return nil, err
	}
	return migrations, nil
}

func (e *ESMigrator) CreateIndexFromMigration(ctx context.Context, migration *Migration) error {
	log := e.logger.Named("CreateIndexFromMigration").With(zap.String("index", migration.Index))

	res, err := e.client.Indices.Exists([]string{migration.Index}, e.client.Indices.Exists.WithContext(ctx))
	if err != nil || (res.StatusCode != http.StatusOK && res.StatusCode != http.StatusNotFound) {
		return createError(log, fmt.Sprintf("error checking if %s index already exists", migration.Index), err)
	}

	if !res.IsError() {
		return nil
	}

	mapping, ok := e.mappings[migration.DocumentKind]
	if !ok {
		return fmt.Errorf("no mapping found for index %s with document kind %s", migration.Index, migration.DocumentKind)
	}
	createIndexReq := map[string]interface{}{
		"mappings": mapping.Mappings,
	}
	if migration.Alias != "" {
		createIndexReq["aliases"] = map[string]interface{}{
			migration.Alias: map[string]interface{}{},
		}
	}

	payload, _ := encodeRequest(&createIndexReq)
	res, err = e.client.Indices.Create(migration.Index, e.client.Indices.Create.WithContext(ctx), e.client.Indices.Create.WithBody(payload))
	if err := getErrorFromESResponse(res, err); err != nil {
		return err
	}

	log.Info("index created", zap.String("index", migration.Index))

	// TODO: create or update index doc

	return nil
}

func getErrorFromESResponse(res *esapi.Response, err error) error {
	if err != nil {
		return err
	}

	if res.IsError() {
		return fmt.Errorf("response error from ES: %d", res.StatusCode)
	}
	return nil
}

type Migrator interface {
	LoadMappings(dir string) error                                            // read mappings from files
	GetMigrations(ctx context.Context) ([]*Migration, error)                  // find all migrations that need to run
	Migrate(ctx context.Context, migration *Migration) error                  // run a single migration on a single index
	CreateIndexFromMigration(ctx context.Context, migration *Migration) error // create an index for the first time
}

type MigrationOrchestrator struct {
	logger   *zap.Logger
	migrator Migrator
}

func NewMigrationOrchestrator(logger *zap.Logger, migrator Migrator) *MigrationOrchestrator {
	return &MigrationOrchestrator{
		logger:   logger,
		migrator: migrator,
	}
}

func (m *MigrationOrchestrator) RunMigrations() error {
	// m.migrator.LoadMappings()
	// try to block writes on metadata index
	// if it's already locked, bail
	// m.migrator.GetMigrations()
	// for each migrations: m.migrator.Migrate() (separate go routine)
	// wait group for all migrations to settle
	// unblock metadata index, bump versions
	// fin

	return nil
}
