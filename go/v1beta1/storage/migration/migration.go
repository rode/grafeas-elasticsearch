// Copyright 2021 The Rode Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package migration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"go.uber.org/zap"
)

type ESMigrator struct {
	client       *elasticsearch.Client
	indexManager IndexManager
	logger       *zap.Logger
}

func NewESMigrator(logger *zap.Logger, client *elasticsearch.Client, indexManager IndexManager) *ESMigrator {
	return &ESMigrator{
		client:       client,
		logger:       logger,
		indexManager: indexManager,
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
	Conflicts   string         `json:"conflicts"`
	Source      *ReindexFields `json:"source"`
	Destination *ReindexFields `json:"dest"`
}

type ReindexFields struct {
	Index  string `json:"index"`
	OpType string `json:"op_type,omitempty"`
}

type ESDocumentResponse struct {
	Source json.RawMessage `json:"_source"`
}

// TODO: fail migration if document kind is not supported
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

	newIndexName := e.indexManager.IncrementIndexVersion(migration.Index)
	err = e.indexManager.CreateIndex(ctx, &IndexInfo{
		Index:        newIndexName,
		Alias:        migration.Alias,
		DocumentKind: migration.DocumentKind,
	}, false)
	if err != nil {
		return err
	}

	reindexReq := &ESReindex{
		Conflicts:   "proceed",
		Source:      &ReindexFields{Index: migration.Index},
		Destination: &ReindexFields{Index: newIndexName, OpType: "create"},
	}
	reindexBody := encodeRequest(reindexReq)
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
		if err := decodeResponse(res.Body, task); err != nil {
			return err
		}

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

	aliasReqBody := encodeRequest(aliasReq)

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

	return getErrorFromESResponse(res, err)
}

func (e *ESMigrator) GetMigrations(ctx context.Context) ([]*Migration, error) {
	res, err := e.client.Indices.Get([]string{"_all"}, e.client.Indices.Get.WithContext(ctx))
	if err := getErrorFromESResponse(res, err); err != nil {
		return nil, err
	}

	allIndices := map[string]interface{}{}

	_ = decodeResponse(res.Body, &allIndices)
	var migrations []*Migration
	for indexName, index := range allIndices {
		if !strings.HasPrefix(indexName, "grafeas") {
			continue
		}

		index := index.(map[string]interface{})
		mappings := index["mappings"].(map[string]interface{})
		meta := mappings["_meta"].(map[string]interface{})
		metaType := meta["type"].(string)

		if metaType == "grafeas" {
			indexParts := parseIndexName(indexName)
			latestVersion := e.indexManager.GetLatestVersionForDocumentKind(indexParts.DocumentKind)

			indexData := allIndices[indexName].(map[string]interface{})
			aliases := indexData["aliases"].(map[string]interface{})
			alias := ""
			for key := range aliases {
				alias = key
			}

			if indexParts.Version != latestVersion {
				migrations = append(migrations, &Migration{Index: indexName, DocumentKind: indexParts.DocumentKind, Alias: alias})
			}
		}
	}

	return migrations, nil
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
	GetMigrations(ctx context.Context) ([]*Migration, error) // find all migrations that need to run
	Migrate(ctx context.Context, migration *Migration) error // run a single migration on a single index
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

func (m *MigrationOrchestrator) RunMigrations(ctx context.Context) error {
	log := m.logger.Named("RunMigrations")
	migrationsToRun, err := m.migrator.GetMigrations(ctx)
	if err != nil {
		return err
	}

	if len(migrationsToRun) == 0 {
		log.Info("No migrations to run")
		return nil
	}

	log.Info(fmt.Sprintf("Discovered %d migrations to run", len(migrationsToRun)))

	for _, migration := range migrationsToRun {
		if err := m.migrator.Migrate(ctx, migration); err != nil {
			return err
		}
	}

	return nil
}

func decodeResponse(r io.ReadCloser, i interface{}) error {
	return json.NewDecoder(r).Decode(i)
}

func encodeRequest(body interface{}) io.Reader {
	b, err := json.Marshal(body)
	if err != nil {
		// we should know that `body` is a serializable struct before invoking `encodeRequest`
		panic(err)
	}

	return bytes.NewReader(b)
}
