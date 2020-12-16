FROM golang:1.15 as builder

WORKDIR /workspace

COPY go.mod go.sum /workspace/
RUN go mod download

COPY go/ go/

WORKDIR /workspace/go/v1beta1/main
RUN GO111MODULE=on CGO_ENABLED=0 go build -o grafeas-server .

FROM alpine:latest
WORKDIR /
COPY --from=builder /workspace/go/v1beta1/main/grafeas-server /grafeas-server
EXPOSE 8080
ENTRYPOINT ["/grafeas-server"]
