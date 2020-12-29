# grafeas-elasticsearch

[![codecov](https://codecov.io/gh/rode/grafeas-elasticsearch/branch/main/graph/badge.svg)](https://codecov.io/gh/rode/grafeas-elasticsearch)
[![test](https://github.com/rode/grafeas-elasticsearch/workflows/test/badge.svg?branch=main)](https://github.com/rode/grafeas-elasticsearch/actions?query=workflow%3Atest+branch%3Amain)

[Elasticsearch](https://www.elastic.co/elasticsearch/) storage backend for [Grafeas](https://grafeas.io/).

## Getting Started

An externally running Elasticsearch cluster must already be available. This repository contains a `docker-compose.yaml` file
that can be used to run a single node Elasticsearch cluster locally:

```bash
docker-compose up -d elasticsearch
```

You can run the Grafeas server by using one of our prebuilt Docker images:

```bash
docker run \
  -p 8080:8080 \
  -v ./config.yaml:/etc/grafeas/config.yaml \
  ghcr.io/rode/grafeas-elasticsearch --config /etc/grafeas/config.yaml
````

A configuration file must be provided, with the path specified with a `--config` flag.

### Configuration

```yaml
grafeas:
  api:
    address: "0.0.0.0:8080"
    cafile:
    keyfile:
    certfile:
    cors_allowed_origins:
  
  # Must be `elasticsearch`
  storage_type: elasticsearch
  
  elasticsearch:
    # URL to external Elasticsearch
    url: "http://elasticsearch:9200"
    
    # Basic auth to external Elasticsearch
    username: "grafeas"
    password: "grafeas"
    
    # How Grafeas should interact with Elasticsearch index refreshes.
    # Recommend using `true`, unless unique circumstances require otherwise.
    # Options are `true`, `wait_for`, `false`.
    refresh: "true"
```

### Features

This backend is still a work in progress, so not all functionality has been finished yet. Below is a checklist of all the
currently implemented features, along with the features that have not been implemented yet:

- [x] Project Methods
  - [x] `CreateProject`
  - [x] `GetProject`
  - [x] `ListProjects`
  - [x] `DeleteProject`
- [ ] Occurrence Methods
  - [x] `CreateOccurrence`
  - [x] `BatchCreateOccurrences`
  - [x] `GetOccurrence`
  - [x] `ListOccurrences`
  - [ ] `UpdateOccurrence`
  - [x] `DeleteOccurrence`
- [ ] Note Methods
  - [x] `CreateNote`
  - [x] `BatchCreateNotes`
  - [x] `GetNote`
  - [x] `ListNotes`
  - [ ] `UpdateNote`
  - [x] `DeleteNote`
- [ ] Misc Methods
  - [ ] `GetOccurrenceNote`
  - [ ] `ListNoteOccurrences`
  - [ ] `GetVulnerabilityOccurrencesSummary`
- [ ] Filtering Support (for `List` methods)
  - [x] `==` operator
  - [x] `!=` operator
  - [x] `&&` operator
  - [x] `||` operator
  - [ ] `<` operator
  - [ ] `>` operator
  - [ ] `<=` operator
  - [ ] `>=` operator
  - [ ] array indexing (ex: `vulnerability.details[0].cpeUri`)
  - [ ] wildcard array indexing (ex: `vulnerability.details[*].cpeUri`)
- [ ] Pagination
- [ ] Elasticsearch config
  - [x] URL
  - [x] Index refresh behavior
  - [ ] Basic Auth
  - [ ] SSL
  
## Local Development

- [Go](https://golang.org/)
- [Docker](https://www.docker.com/get-started)

Shared run configurations for Jetbrains IDEs are kept in the default `.run/` directory.
Theses are automatically read and added to your local run configurations.

### Testing

Unit tests use [Ginkgo](http://onsi.github.io/ginkgo/), and integration tests use the standard [testing](https://golang.org/pkg/testing/) library.
All tests use [Gomega](https://onsi.github.io/gomega/) for assertions and matching, for consistency.

#### unit

Unit tests live alongside production code in `go/` directory.

`make test` will run unit tests, along with vet and fmt.

`go test unit` IDE run configuration is also available. 

`make mocks` will regenerate test mocks in `go/mocks` directory.

#### integration

Integration tests are in the `test/` directory.
These require Elasticsearch and a build of this project to be running.
This is handled through `docker-compose`.

1. `docker-compose up -d --build elasticsearch server`
    - Remove `-d` if you want to watch logs.
    - Remove `--build` if you have already built the local images against the latest code.
   Skipping build will significantly improve startup time.
1. `make integration` or `go test integration` IDE run configuration
   - Can be continuously run between docker-compose resets.
   Tests generate UUIDs for resources, to avoid collisions between runs.
1. `docker-compose down`
