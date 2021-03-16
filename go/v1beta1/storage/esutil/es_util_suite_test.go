package esutil

import (
	"testing"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/brianvoe/gofakeit/v6"
	"go.uber.org/zap"
)

var logger = zap.NewNop()
var fake = gofakeit.New(0)

func TestEsUtilPackage(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "EsUtil Suite")
}
