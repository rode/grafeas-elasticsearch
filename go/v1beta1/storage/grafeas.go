package storage

import (
	"fmt"
	grafeasConfig "github.com/grafeas/grafeas/go/config"
	"github.com/grafeas/grafeas/go/v1beta1/storage"
	"github.com/liatrio/grafeas-elasticsearch/go/config"
	"go.uber.org/zap"
	"net/http"
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
				withIndexMetadataAndStringMapping(),
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
