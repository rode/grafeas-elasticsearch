FROM golang:1.16 as builder

WORKDIR /workspace

COPY go.mod go.sum /workspace/
RUN go mod download

COPY go/ go/

WORKDIR /workspace/go/v1beta1/main
RUN CGO_ENABLED=0 go build -o grafeas-server .

FROM alpine:latest
LABEL org.opencontainers.image.source=https://github.com/rode/grafeas-elasticsearch
WORKDIR /
COPY --from=builder /workspace/go/v1beta1/main/grafeas-server /grafeas-server
COPY mappings/ mappings/
EXPOSE 8080
ENTRYPOINT ["/grafeas-server"]
