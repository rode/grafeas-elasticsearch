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
	go install github.com/maxbrunsfeld/counterfeiter/v6@v6.4.1
	COUNTERFEITER_NO_GENERATE_WARNING="true" go generate ./...

test: fmtcheck vet
	go test -short ./... -coverprofile=coverage.txt -covermode atomic

coverage: test
	go tool cover -html=coverage.txt

integration:
	go test -v -count 1 ./test/...
