# grafeas-elasticsearch

## Local Development

### Testing

Unit tests use [Ginkgo](http://onsi.github.io/ginkgo/), and integration tests use the standard `testing` library.
All tests use [Gomega](https://onsi.github.io/gomega/) for assertions and matching, for consistency.

#### unit

`make test` will run unit tests, along with vet and fmt.

#### integration

Integration tests require Elasticsearch and a build of this project running.
This is handled through `docker-compose`.

1. `docker-compose up -d --build elasticsearch server`
    - Remove `-d` if you want to watch logs.
    - Remove `--build` if you have already built the local images against the latest code.
   Skipping build will significantly improve startup time.
1. `make integration`
1. `docker-compose down`