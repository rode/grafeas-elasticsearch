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
	receivedHttpRequest  *http.Request
	preparedHttpResponse *http.Response
	expectedError        error
}

func (m *mockEsTransport) Perform(req *http.Request) (*http.Response, error) {
	m.receivedHttpRequest = req

	// if we have a prepared response, send it. otherwise, return nil
	if m.preparedHttpResponse != nil {
		return m.preparedHttpResponse, m.expectedError
	}

	return nil, m.expectedError
}
