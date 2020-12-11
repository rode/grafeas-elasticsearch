package v1beta1_test

import (
	"flag"
	fake "github.com/brianvoe/gofakeit/v5"
	"log"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	flag.Parse()

	if testing.Short() {
		log.Println("Test run with -short flag, skipping integration.")
		os.Exit(0)
	}

	fake.Seed(0)

	os.Exit(m.Run())
}
