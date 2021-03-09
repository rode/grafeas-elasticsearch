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

		const migrationsDir = "mappings"
		ctx := context.Background()
		if err := es.migrator.LoadMappings(migrationsDir); err != nil {
			return nil, err
		}

		if err := es.migrator.CreateIndexFromMigration(ctx, &Migration{
			DocumentKind: "metadata",
			Index:        "grafeas-metadata",
		}); err != nil {
			return nil, createError(log, "error creating metadata index", err)
		}

		if err := es.migrator.CreateIndexFromMigration(ctx, &Migration{
			DocumentKind: "projects",
			Index:        projectsIndex(),
			Alias:        projectsAlias(),
		}); err != nil {
			return nil, createError(log, "error creating initial projects index", err)
		}

		migrationOrchestrator := NewMigrationOrchestrator(logger.Named("MigrationOrchestrator"), es.migrator)

		if err := migrationOrchestrator.RunMigrations(ctx); err != nil {
			return nil, fmt.Errorf("error running migrations: %s", err)
		}

		return &storage.Storage{
			Ps: es,
			Gs: es,
		}, nil
	}
}
