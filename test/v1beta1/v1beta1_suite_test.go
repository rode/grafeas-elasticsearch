package v1beta1_test

import (
	"log"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestV1beta1(t *testing.T) {
	if testing.Short() {
		log.Println("Running with -short flag, skipping tests.")
		return
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "V1beta1 Suite")
}
