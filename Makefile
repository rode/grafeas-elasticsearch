.PHONY: test fmtcheck vet fmt mocks integration coverage
GOFMT_FILES?=$$(find . -name '*.go' | grep -v proto)

GO111MODULE=on

fmtcheck:
	lineCount=$(shell gofmt -l -s $(GOFMT_FILES) | wc -l | tr -d ' ') && exit $$lineCount

fmt:
	gofmt -w -s $(GOFMT_FILES)

vet:
	go vet ./...

mocks:
	mockgen -package mocks -destination go/mocks/filtering.go github.com/rode/grafeas-elasticsearch/go/v1beta1/storage/filtering Filterer
	mockgen -package mocks -destination go/mocks/index.go github.com/rode/grafeas-elasticsearch/go/v1beta1/storage/esutil IndexManager
	counterfeiter go/v1beta1/storage/esutil Client
	mockgen -package mocks -destination go/mocks/orchestrator.go github.com/rode/grafeas-elasticsearch/go/v1beta1/storage/migration Orchestrator

test: fmtcheck vet
	go test -short ./... -coverprofile=coverage.txt -covermode atomic

coverage: test
	go tool cover -html=coverage.txt

integration:
	go test -v -count 1 ./test/...
