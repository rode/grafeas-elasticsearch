package storage

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"net/http"
	"testing"
)

var logger *zap.Logger

func TestStoragePackage(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Storage Suite")
}

var _ = BeforeSuite(func() {
	logger, _ = zap.NewDevelopment()
})

type mockEsTransport struct {
}

func (m *mockEsTransport) Perform(*http.Request) (*http.Response, error) {
	return nil, nil
}
