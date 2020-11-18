module github.com/liatrio/grafeas-elasticsearch

go 1.15

require (
	github.com/brianvoe/gofakeit/v5 v5.10.1
	github.com/elastic/go-elasticsearch/v7 v7.5.1-0.20201104130636-152864b47d96
	github.com/fernet/fernet-go v0.0.0-20180830025343-9eac43b88a5e
	github.com/golang/protobuf v1.4.2
	github.com/google/go-cmp v0.5.2 // indirect
	github.com/grafeas/grafeas v0.1.6
	github.com/onsi/ginkgo v1.14.2
	github.com/onsi/gomega v1.10.3
	github.com/pelletier/go-toml v1.7.0 // indirect
	github.com/stretchr/testify v1.6.1 // indirect
	go.uber.org/zap v1.10.0
	google.golang.org/grpc v1.33.1
	google.golang.org/grpc/examples v0.0.0-20201112215255-90f1b3ee835b // indirect
	google.golang.org/protobuf v1.25.0
)

replace (
	github.com/grpc-ecosystem/grpc-gateway v1.9.0 => github.com/grpc-ecosystem/grpc-gateway v1.11.0
	github.com/grpc-ecosystem/grpc-gateway v1.9.6 => github.com/grpc-ecosystem/grpc-gateway v1.11.0
)
