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

package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/grafeas/grafeas/go/v1beta1/server"
	grafeasStorage "github.com/grafeas/grafeas/go/v1beta1/storage"
	"github.com/rode/grafeas-elasticsearch/go/config"
	"github.com/rode/grafeas-elasticsearch/go/v1beta1/storage"
	"github.com/rode/grafeas-elasticsearch/go/v1beta1/storage/filtering"
	"github.com/rode/grafeas-elasticsearch/go/v1beta1/storage/migration"
	"go.uber.org/zap"
)

func main() {
	_, debugEnabled := os.LookupEnv("DEBUG")
	logger, err := createLogger(debugEnabled)
	if err != nil {
		log.Fatalf("failed to create logger: %v", err)
	}

	registerStorageTypeProvider := storage.ElasticsearchStorageTypeProviderCreator(func(c *config.ElasticsearchConfig) (*storage.ElasticsearchStorage, error) {
		esClient, err := createESClient(logger, c.URL, c.Username, c.Password, c.InsecureSkipVerify)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to Elasticsearch")
		}

		indexManager := migration.NewIndexManager(logger.Named("IndexManager"), esClient)
		migrator := migration.NewESMigrator(logger.Named("ESMigrator"), esClient, indexManager)
		migrationOrchestrator := migration.NewMigrationOrchestrator(logger.Named("MigrationOrchestrator"), migrator)

		return storage.NewElasticsearchStorage(logger.Named("ElasticsearchStore"), esClient, filtering.NewFilterer(), c, indexManager, migrationOrchestrator), nil
	}, logger)

	err = grafeasStorage.RegisterStorageTypeProvider("elasticsearch", registerStorageTypeProvider)
	if err != nil {
		logger.Fatal("Error when registering elasticsearch storage", zap.NamedError("error", err))
	}

	err = server.StartGrafeas()
	if err != nil {
		logger.Fatal("Failed to start Grafeas server...", zap.NamedError("error", err))
	}
}

func createESClient(logger *zap.Logger, elasticsearchEndpoint, username, password string, insecureSkipVerify bool) (*elasticsearch.Client, error) {
	c, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{
			elasticsearchEndpoint,
		},
		Username: username,
		Password: password,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: insecureSkipVerify},
		},
	})

	if err != nil {
		return nil, err
	}

	res, err := c.Info()
	if err != nil {
		return nil, err
	}

	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return nil, err
	}

	logger.Debug("Successful Elasticsearch connection", zap.String("ES Server version", r["version"].(map[string]interface{})["number"].(string)))

	return c, nil
}

func createLogger(debug bool) (*zap.Logger, error) {
	if debug {
		return zap.NewDevelopment()
	}

	return zap.NewProduction()
}
