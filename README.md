# grafeas-elasticsearch

[![codecov](https://codecov.io/gh/liatrio/grafeas-elasticsearch/branch/main/graph/badge.svg)](https://codecov.io/gh/liatrio/grafeas-elasticsearch)
[![test](https://github.com/liatrio/grafeas-elasticsearch/workflows/test/badge.svg)](https://github.com/liatrio/grafeas-elasticsearch/actions?query=workflow%3Atest+branch%3Amain)

[Elasticsearch](https://www.elastic.co/elasticsearch/) storage backend for [Grafeas](https://grafeas.io/).

## Local Development

### Testing

Unit tests use [Ginkgo](http://onsi.github.io/ginkgo/), and integration tests use the standard `testing` library.
All tests use [Gomega](https://onsi.github.io/gomega/) for assertions and matching, for consistency.

#### unit

Unit tests live alongside production code in `go/` directory.

`make test` will run unit tests, along with vet and fmt.

`make mocks` will regenerate test mocks in `go/mocks` directory.

#### integration

Integration tests are in the `test/` directory.
These require Elasticsearch and a build of this project to be running.
This is handled through `docker-compose`.

1. `docker-compose up -d --build elasticsearch server`
    - Remove `-d` if you want to watch logs.
    - Remove `--build` if you have already built the local images against the latest code.
   Skipping build will significantly improve startup time.
1. `make integration`
   - Can be continuously run between docker-compose resets.
   Tests generate UUIDs for resources, to avoid collisions between runs.
1. `docker-compose down`
