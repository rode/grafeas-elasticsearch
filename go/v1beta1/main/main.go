package main

import (
	"encoding/json"
	"flag"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/grafeas/grafeas/go/v1beta1/server"
	grafeasStorage "github.com/grafeas/grafeas/go/v1beta1/storage"
	"github.com/liatrio/grafeas-elasticsearch/go/v1beta1/storage"
	"go.uber.org/zap"
	"log"
)

var elasticsearchHost string

func main() {
	flag.StringVar(&elasticsearchHost, "elasticsearch-host", "http://elasticsearch:9200", "the host to use to connect to grafeas")
	flag.Parse()

	err := grafeasStorage.RegisterDefaultStorageTypeProviders()
	if err != nil {
		log.Panicf("Error when registering storage type providers, %s", err)
	}

	logger, err := createLogger(true)
	if err != nil {
		log.Fatalf("failed to create logger: %v", err)
	}

	esClient, err := createESClient(logger, elasticsearchHost)
	if err != nil {
		logger.Fatal("failed to connect to Elasticsearch", zap.NamedError("error", err))
	}

	elasticsearchStorage := storage.NewElasticsearchStore(esClient, logger.Named("ElasticsearchStore"))

	// register a new storage type using the key 'elasticsearch'
	err = grafeasStorage.RegisterStorageTypeProvider("elasticsearch", elasticsearchStorage.ElasticsearchStorageTypeProvider)
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
