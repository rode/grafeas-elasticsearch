package filtering

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
)

var logger *zap.Logger

func TestFilteringPackage(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Filtering Suite")
}

var _ = BeforeSuite(func() {
	logger = zap.NewNop()
})
