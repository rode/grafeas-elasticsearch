package main

import (
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/grafeas/grafeas/go/v1beta1/server"
	grafeasStorage "github.com/grafeas/grafeas/go/v1beta1/storage"
	"github.com/rode/grafeas-elasticsearch/go/config"
	"github.com/rode/grafeas-elasticsearch/go/v1beta1/storage"
	"github.com/rode/grafeas-elasticsearch/go/v1beta1/storage/filtering"
	"go.uber.org/zap"
	"log"
	"os"
)

func main() {
	_, debugEnabled := os.LookupEnv("DEBUG")
	logger, err := createLogger(debugEnabled)
	if err != nil {
		log.Fatalf("failed to create logger: %v", err)
	}

	registerStorageTypeProvider := storage.ElasticsearchStorageTypeProviderCreator(func(c *config.ElasticsearchConfig) (*storage.ElasticsearchStorage, error) {
		esClient, err := createESClient(logger, c.URL, c.Username, c.Password)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to Elasticsearch")
		}

		return storage.NewElasticsearchStorage(logger.Named("ElasticsearchStore"), esClient, filtering.NewFilterer(), c), nil
	}, logger)

	err = grafeasStorage.RegisterStorageTypeProvider("elasticsearch", registerStorageTypeProvider)
	if err != nil {
		logger.Fatal("Error when registering my new storage", zap.NamedError("error", err))
	}

	err = server.StartGrafeas()
	if err != nil {
		logger.Fatal("Failed to start Grafeas server...", zap.NamedError("error", err))
	}
}

func createESClient(logger *zap.Logger, elasticsearchEndpoint, username, password string) (*elasticsearch.Client, error) {
	c, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{
			elasticsearchEndpoint,
		},
		Username: username,
		Password: password,
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
