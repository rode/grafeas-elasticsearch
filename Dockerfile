FROM golang:1.15 as builder

COPY . /go/src/github.com/liatrio/grafeas-elasticsearch/
WORKDIR /go/src/github.com/liatrio/grafeas-elasticsearch

RUN make build test

WORKDIR /go/src/github.com/liatrio/grafeas-elasticsearch/go/v1beta1/main
RUN GO111MODULE=on CGO_ENABLED=0 go build -o grafeas-server .

FROM alpine:latest
WORKDIR /
COPY --from=builder /go/src/github.com/liatrio/grafeas-elasticsearch/go/v1beta1/main/grafeas-server /grafeas-server
EXPOSE 8080
ENTRYPOINT ["/grafeas-server"]
