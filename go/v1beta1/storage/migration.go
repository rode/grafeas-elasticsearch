package storage

import (
	"context"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"go.uber.org/zap"
)

type ESMigrator struct {
	client *elasticsearch.Client
	logger *zap.Logger
}

func NewESMigrator(logger *zap.Logger, client *elasticsearch.Client) *ESMigrator {
	return &ESMigrator{
		client: client,
		logger: logger,
	}
}

// {"acknowledged":true,
// "shards_acknowledged":true,
// "indices":[{"name":"grafeas-v1beta1-rode-occurrences","blocked":true}]}

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

type Migration struct {
	Version string
	Mapping map[string]interface{}
}

// new index requires: mapping, name
func (e *ESMigrator) Migrate(ctx context.Context, indexName string, migration *Migration) error {
	log := e.logger.Named("Migrate").With(zap.String("indexName", indexName))
	log.Info("Starting migration")
	log.Info("Placing write block on index")
	res, err := e.client.Indices.AddBlock([]string{indexName}, "write", e.client.Indices.AddBlock.WithContext(ctx))
	if err != nil {
		log.Error("Error placing write block on index", zap.Error(err))
		return err
	}
	if res.IsError() {
		log.Error("Not ok response from Elasticsearch", zap.Int("status", res.StatusCode))
		return fmt.Errorf("Error from Elasticsearch", res.StatusCode)
	}

	br := &ESBlockResponse{}
	if err := decodeResponse(res.Body, br); err != nil {
		return err
	}

	if len(br.Indices) == 0 { // TODO: already blocked, should poll for migration completion
		log.Info("Index is already blocked")
		return fmt.Errorf("unblocked")
	}

	index := br.Indices[0] // TODO: length check
	// TODO: do we need to check acknowledged and shards acknowledged?
	if !(br.Acknowledged && br.ShardsAcknowledged && index.Name == indexName && index.Blocked) {
		log.Error("Write block unsuccessful", zap.Any("response", br))
		return fmt.Errorf("unable to block writes for index: %s", indexName)
	}

	newIndexName := fmt.Sprintf("%s-%s", indexName, migration.Version)
	log = log.With(zap.String("newIndex", newIndexName))
	indexBody := map[string]interface{}{
		"mappings": migration.Mapping,
	}
	b, _ := encodeRequest(&indexBody)

	log.Info("Creating new index")
	res, err = e.client.Indices.Create(
		newIndexName,
		e.client.Indices.Create.WithContext(ctx),
		e.client.Indices.Create.WithBody(b),
	)

	if err != nil {
		log.Error("Failed to create new index", zap.Error(err))
		return err
	}

	if res.IsError() {
		log.Error("not okay response from Elasticsearch", zap.Int("status", res.StatusCode))
		return fmt.Errorf("response error from ES: %d", res.StatusCode)
	}

	reindexReq := map[string]interface{}{
		"source": map[string]interface{}{"index": indexName},
		"dest":   map[string]interface{}{"index": newIndexName},
	}
	bb, _ := encodeRequest(reindexReq)
	log.Info("Starting reindex")
	res, err = e.client.Reindex(
		bb,
		e.client.Reindex.WithContext(ctx),
		e.client.Reindex.WithWaitForCompletion(false))
	if err != nil {
		log.Error("Reindex error", zap.Error(err))
		return err
	}
	if res.IsError() {
		log.Error("Reindex not-okay response", zap.Int("status", res.StatusCode))
	}
	t := &ESTaskCreationResponse{}

	_ = decodeResponse(res.Body, t)
	log.Info("Reindex started", zap.String("taskId", t.Task))

	for i := 0; i < 10; i++ {
		log.Info("Polling task API", zap.String("taskId", t.Task))
		res, err = e.client.Tasks.Get(t.Task, e.client.Tasks.Get.WithContext(ctx))
		if err != nil {
			log.Error("Failed to query task API", zap.Error(err))
			return err
		}

		if res.IsError() {
			log.Error("not okay response from Elasticsearch", zap.Int("status", res.StatusCode))
			return fmt.Errorf("response error from ES: %d", res.StatusCode)
		}

		tt := &ESTask{}
		_ = decodeResponse(res.Body, tt)

		r, _ := ioutil.ReadAll(res.Body)
		fmt.Printf("raw body: %s\n", r)

		if tt.Completed {
			log.Info("Reindex completed")
			break
		}

		log.Info("Task incomplete, waiting before polling again", zap.String("taskId", t.Task))
		time.Sleep(time.Minute)
	}

	return nil
}
