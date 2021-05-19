module github.com/rode/grafeas-elasticsearch

go 1.15

require (
	github.com/brianvoe/gofakeit/v6 v6.0.0
	github.com/elastic/go-elasticsearch/v7 v7.10.0
	github.com/evanphx/json-patch v0.5.2
	github.com/golang/mock v1.4.4
	github.com/golang/protobuf v1.4.2
	github.com/google/cel-go v0.6.0
	github.com/google/go-cmp v0.5.2 // indirect
	github.com/google/uuid v1.1.2
	github.com/grafeas/grafeas v0.1.6
	github.com/hashicorp/go-multierror v1.0.0
	github.com/mennanov/fieldmask-utils v0.3.3
	github.com/onsi/ginkgo v1.14.2
	github.com/onsi/gomega v1.10.3
	github.com/pelletier/go-toml v1.7.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/spf13/viper v1.7.0 // indirect
	github.com/stretchr/testify v1.6.1 // indirect
	go.uber.org/zap v1.10.0
	golang.org/x/sys v0.0.0-20201118182958-a01c418693c7 // indirect
	google.golang.org/genproto v0.0.0-20200806141610-86f49bd18e98
	google.golang.org/grpc v1.33.1
	google.golang.org/grpc/examples v0.0.0-20210111180913-4cf4a98505bc // indirect
	google.golang.org/protobuf v1.25.0
)

replace (
	github.com/grpc-ecosystem/grpc-gateway v1.9.0 => github.com/grpc-ecosystem/grpc-gateway v1.11.0
	github.com/grpc-ecosystem/grpc-gateway v1.9.6 => github.com/grpc-ecosystem/grpc-gateway v1.11.0
)
