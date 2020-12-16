package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/grafeas/grafeas/go/v1beta1/server"
	grafeasStorage "github.com/grafeas/grafeas/go/v1beta1/storage"
	"github.com/liatrio/grafeas-elasticsearch/go/config"
	"github.com/liatrio/grafeas-elasticsearch/go/v1beta1/storage"
	"github.com/liatrio/grafeas-elasticsearch/go/v1beta1/storage/filtering"
	"go.uber.org/zap"
	"log"
)

var elasticsearchHost string

func main() {
	flag.StringVar(&elasticsearchHost, "elasticsearch-host", "http://elasticsearch:9200", "the host to use to connect to grafeas")
	flag.Parse()

	logger, err := createLogger(true)
	if err != nil {
		log.Fatalf("failed to create logger: %v", err)
	}

	err = grafeasStorage.RegisterStorageTypeProvider("elasticsearch", storage.GrafeasStorageTypeProviderCreator(func(c *config.ElasticsearchConfig) (*storage.ElasticsearchStorage, error) {
		esClient, err := createESClient(logger, elasticsearchHost)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to Elasticsearch")
		}

		return storage.NewElasticsearchStorage(logger.Named("ElasticsearchStore"), esClient, filtering.NewFilterer(), c), nil
	}, logger))
	if err != nil {
		logger.Fatal("Error when registering my new storage", zap.NamedError("error", err))
	}

	err = server.StartGrafeas()
	if err != nil {
		logger.Fatal("Failed to start Grafeas server...", zap.NamedError("error", err))
	}
}

func createESClient(logger *zap.Logger, elasticsearchEndpoint string) (*elasticsearch.Client, error) {
	c, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{
			elasticsearchEndpoint,
		},
		Username: "grafeas",
		Password: "grafeas",
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
