package v1beta1_test

import (
	"flag"
	"github.com/brianvoe/gofakeit/v6"
	"log"
	"os"
	"testing"
)

var fake = gofakeit.New(0)

func TestMain(m *testing.M) {
	flag.Parse()

	if testing.Short() {
		log.Println("Test run with -short flag, skipping integration.")
		os.Exit(0)
	}

	os.Exit(m.Run())
}
