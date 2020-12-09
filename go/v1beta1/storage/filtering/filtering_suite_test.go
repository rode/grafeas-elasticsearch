package filtering_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestFiltering(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Filtering Suite")
}
