package migration

import (
	"context"
	"fmt"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rode/grafeas-elasticsearch/go/v1beta1/storage/esutil"
	"go.uber.org/zap"
)

var _ = Describe("ESMigrator", func() {
	var (
		mockEsTransport *esutil.MockEsTransport
		indexManager	*EsIndexManager
	)
	BeforeEach(func() {
		mockEsTransport = &esutil.MockEsTransport{}
		mockEsClient := &elasticsearch.Client{Transport: mockEsTransport, API: esapi.New(mockEsTransport)}
		migrator := NewESMigrator(logger, mockEsClient, indexManager)
	})

	Context("Migrations Functions", func() {
		Describe("Get Migrations", func() {
			It("should return list of 1 migrations", func() {
				Expect()
			})
		}
	}
}

