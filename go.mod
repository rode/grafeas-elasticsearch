module github.com/rode/grafeas-elasticsearch

go 1.15

require (
	github.com/brianvoe/gofakeit/v6 v6.4.1
	github.com/elastic/go-elasticsearch/v7 v7.12.0
	github.com/golang/mock v1.4.4
	github.com/golang/protobuf v1.5.2
	github.com/google/cel-go v0.6.0
	github.com/google/uuid v1.1.2
	github.com/grafeas/grafeas v0.1.6
	github.com/hashicorp/go-multierror v1.0.0
	github.com/mennanov/fieldmask-utils v0.3.3
	github.com/onsi/ginkgo v1.16.2
	github.com/onsi/gomega v1.12.0
	github.com/pelletier/go-toml v1.7.0 // indirect
	github.com/rode/es-index-manager v0.0.0-20210513143852-8b606ba498a6
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/spf13/viper v1.7.0 // indirect
	github.com/stretchr/testify v1.6.1 // indirect
	go.uber.org/zap v1.16.0
	google.golang.org/genproto v0.0.0-20200806141610-86f49bd18e98
	google.golang.org/grpc v1.33.1
	google.golang.org/grpc/examples v0.0.0-20210111180913-4cf4a98505bc // indirect
	google.golang.org/protobuf v1.26.0
)

replace (
	github.com/rode/es-index-manager => /Users/alex/Developer/liatrio/rode/es-index-manager
	github.com/grpc-ecosystem/grpc-gateway v1.9.0 => github.com/grpc-ecosystem/grpc-gateway v1.11.0
	github.com/grpc-ecosystem/grpc-gateway v1.9.6 => github.com/grpc-ecosystem/grpc-gateway v1.11.0
)
