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

package storage

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/elastic/go-elasticsearch/v7"
	grafeasConfig "github.com/grafeas/grafeas/go/config"
	"github.com/grafeas/grafeas/go/v1beta1/storage"
	"github.com/rode/grafeas-elasticsearch/go/config"
	"go.uber.org/zap"
)

type newElasticsearchStorageFunc func(*config.ElasticsearchConfig) (*ElasticsearchStorage, error)

type registerStorageTypeProviderFunc func(string, *grafeasConfig.StorageConfiguration) (*storage.Storage, error)

// ElasticsearchStorageTypeProviderCreator takes a function that returns a new instance of ElasticsearchStorage,
// when called later with the parsed Grafeas config file into config.ElasticsearchConfig.
// It returns a function that is given to Grafeas to register the Elasticsearch storage type provider.
// This allows for the ability to still inject different ElasticsearchStorage configurations, e.g. testing.
// This is done this way because we do not get access to the parsed config until after Grafeas server is started
// and registers the storage type provider.
func ElasticsearchStorageTypeProviderCreator(newES newElasticsearchStorageFunc, logger *zap.Logger) registerStorageTypeProviderFunc {
	return func(storageType string, sc *grafeasConfig.StorageConfiguration) (*storage.Storage, error) {
		var c *config.ElasticsearchConfig
		log := logger.Named("ElasticsearchStorageTypeProvider")
		log.Info("registering elasticsearch storage provider")

		if storageType != "elasticsearch" {
			return nil, fmt.Errorf("unknown storage type %s, must be 'elasticsearch'", storageType)
		}

		err := grafeasConfig.ConvertGenericConfigToSpecificType(sc, &c)
		if err != nil {
			return nil, fmt.Errorf("unable to convert config for Elasticsearch: %s", err)
		}

		err = c.IsValid()
		if err != nil {
			return nil, err
		}

		es, err := newES(c)
		if err != nil {
			return nil, err
		}

		if err := createMetadataIndexIfNotExists(logger, es.client); err != nil {
			return nil, createError(log, "error creating metadata index", err)
		}

		migrate := os.Getenv("GRAFEAS_MIGRATE")
		fmt.Println("Migrate??", migrate)

		migrator := NewESMigrator(logger, es.client)
		const migrationsDir = "mappings"
		fmt.Println("error?", migrator.LoadMigrations(migrationsDir))
		if migrate == "yes" {
			migration := &Migration{
				DocumentKind: "occurrence",
				Index:        "grafeas-v1beta1-rode-occurrences",
				Alias:        "grafeas-rode-occurrences",
			}
			err = migrator.Migrate(context.Background(), migration)

			if err != nil {
				log.Fatal("migration failed", zap.Error(err))
			}
		}

		res, err := es.client.Indices.Exists([]string{projectsIndex()})
		if err != nil || (res.StatusCode != http.StatusOK && res.StatusCode != http.StatusNotFound) {
			return nil, createError(log, "error checking if project index already exists", err)
		}

		// the response is an error if the index was not found, so we need to create it
		if res.IsError() {
			log := log.With(zap.String("index", projectsIndex()))
			log.Info("initial index for grafeas projects not found, creating...")
			res, err = es.client.Indices.Create(
				projectsIndex(),
				withIndexMetadataAndStringMapping(projectsAlias()),
			)
			if err != nil {
				return nil, createError(log, "error sending index creation request to elasticsearch", err)
			}
			if res.IsError() {
				return nil, createError(log, "error creating index in elasticsearch", fmt.Errorf(res.String()))
			}
			log.Info("project index created", zap.String("index", projectsIndex()))
		}

		return &storage.Storage{
			Ps: es,
			Gs: es,
		}, nil
	}
}

func createMetadataIndexIfNotExists(log *zap.Logger, es *elasticsearch.Client) error {
	metadataIndexName := fmt.Sprintf("grafeas-metadata")
	res, err := es.Indices.Exists([]string{metadataIndexName})
	ctx := context.Background()
	if err != nil || (res.StatusCode != http.StatusOK && res.StatusCode != http.StatusNotFound) {
		return createError(log, fmt.Sprintf("error checking if index %s already exists", metadataIndexName), err)
	}

	// the response is an error if the index was not found, so we need to create it
	if !res.IsError() {
		return nil
	}

	log = log.With(zap.String("index", projectsIndex()))
	log.Info("index not found, creating...", zap.String("index", metadataIndexName))

	metadataSchema := map[string]interface{}{
		"mappings": map[string]interface{}{
			"_meta": map[string]string{
				"type": "grafeas",
			},
		},
	}
	payload, _ := encodeRequest(&metadataSchema)
	res, err = es.Indices.Create(
		metadataIndexName,
		es.Indices.Create.WithBody(payload),
	)
	if err != nil {
		return createError(log, "error sending index creation request to elasticsearch", err)
	}
	if res.IsError() {
		return createError(log, "error creating index in elasticsearch", fmt.Errorf(res.String()))
	}
	log.Info("metadata index created", zap.String("index", metadataIndexName))

	res, err = es.Indices.Get([]string{"_all"}, es.Indices.Get.WithContext(ctx))
	if err != nil {
		return err
	}

	if res.IsError() {
		return fmt.Errorf("not okay response listing indices: %d", res.StatusCode)
	}

	allIndices := map[string]interface{}{}

	_ = decodeResponse(res.Body, &allIndices)
	indexDoc := map[string]interface{}{}
	for indexName, index := range allIndices {
		if indexName == metadataIndexName || !strings.HasPrefix(indexName, "grafeas") {
			continue
		}

		index := index.(map[string]interface{})
		mappings := index["mappings"].(map[string]interface{})
		meta := mappings["_meta"].(map[string]interface{})
		metaType := meta["type"].(string)

		if metaType == "grafeas" {
			indexDoc[indexName] = map[string]interface{}{
				"mappingsVersion": "v1",
			}
		}
	}

	docBody, _ := encodeRequest(indexDoc)
	res, err = es.Create(metadataIndexName, "grafeas-indices-metadata", docBody, es.Create.WithContext(ctx))

	if err != nil {
		return err
	}

	if res.IsError() {
		return fmt.Errorf("not okay response creating metadata doc: %d", res.StatusCode)
	}

	return nil
}
