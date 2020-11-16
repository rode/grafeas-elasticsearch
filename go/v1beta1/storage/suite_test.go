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
	logger = zap.NewNop()
})

type mockEsTransport struct {
	receivedPerformRequest  *http.Request
	preparedPerformResponse *http.Response
	expectedError           error
}

func (m *mockEsTransport) Perform(req *http.Request) (*http.Response, error) {
	m.receivedPerformRequest = req

	// if we have a prepared response, send it. otherwise, return nil
	if m.preparedPerformResponse != nil {
		return m.preparedPerformResponse, m.expectedError
	}

	return nil, m.expectedError
}
