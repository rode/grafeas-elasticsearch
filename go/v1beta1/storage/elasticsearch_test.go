package storage

import (
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	. "github.com/onsi/ginkgo"
)

var _ = Describe("elasticsearch storage", func() {
	var (
		elasticsearchStorage *ElasticsearchStorage
	)

	BeforeEach(func() {
		transport := &mockEsTransport{}
		mockEsClient := &elasticsearch.Client{Transport: transport, API: esapi.New(transport)}

		elasticsearchStorage = NewElasticsearchStore(mockEsClient, logger)
	})
})
