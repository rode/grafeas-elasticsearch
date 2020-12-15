package config

import (
	"github.com/brianvoe/gofakeit/v5"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestConfig(t *testing.T) {
	gofakeit.Seed(0)

	RegisterFailHandler(Fail)
	RunSpecs(t, "Config Suite")
}
