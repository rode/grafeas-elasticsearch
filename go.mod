module github.com/liatrio/grafeas-elasticsearch

go 1.15

require (
	github.com/Jeffail/gabs/v2 v2.6.0
	github.com/brianvoe/gofakeit/v5 v5.10.1
	github.com/elastic/go-elasticsearch/v7 v7.5.1-0.20201104130636-152864b47d96
	github.com/gogo/protobuf v1.2.1
	github.com/golang/protobuf v1.4.2
	github.com/google/cel-go v0.6.0
	github.com/google/go-cmp v0.5.2 // indirect
	github.com/grafeas/grafeas v0.1.6
	github.com/onsi/ginkgo v1.14.2
	github.com/onsi/gomega v1.10.3
	github.com/pelletier/go-toml v1.7.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/spf13/viper v1.7.0 // indirect
	github.com/stretchr/testify v1.6.1
	go.uber.org/zap v1.10.0
	golang.org/x/sys v0.0.0-20201118182958-a01c418693c7 // indirect
	google.golang.org/genproto v0.0.0-20200806141610-86f49bd18e98
	google.golang.org/grpc v1.33.1
	google.golang.org/grpc/examples v0.0.0-20201112215255-90f1b3ee835b // indirect
	google.golang.org/protobuf v1.25.0
)

replace (
	github.com/grpc-ecosystem/grpc-gateway v1.9.0 => github.com/grpc-ecosystem/grpc-gateway v1.11.0
	github.com/grpc-ecosystem/grpc-gateway v1.9.6 => github.com/grpc-ecosystem/grpc-gateway v1.11.0
)
