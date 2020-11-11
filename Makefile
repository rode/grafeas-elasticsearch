.PHONY: build fmt test vet clean generate

SRC = $(shell find . -type f -name '*.go' -not -path "./grafeas/grafeas/*")
CLEAN := *~

.EXPORT_ALL_VARIABLES:

GO111MODULE=on

build: vet fmt generate
	go build -v ./...

# http://golang.org/cmd/go/#hdr-Run_gofmt_on_package_sources
fmt:
	@gofmt -l -w $(SRC)

test: generate
	@go test -v ./...

vet: generate
	@go vet ./...

generate:
	mkdir -p grafeas
	bash -c "if [ ! -f grafeas/grafeas.tgz ]; then curl https://github.com/grafeas/grafeas/releases/download/v0.1.3/grafeas-0.1.3.tar.gz -o grafeas/grafeas.tgz -L; fi"
	tar xf grafeas/grafeas.tgz -C grafeas

clean: generate
	go clean ./...
	rm -rf $(CLEAN)
	rm -rf test grafeas