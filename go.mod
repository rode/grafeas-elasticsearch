module github.com/rode/grafeas-elasticsearch

go 1.17

require (
	github.com/brianvoe/gofakeit/v6 v6.4.1
	github.com/elastic/go-elasticsearch/v7 v7.12.0
	github.com/evanphx/json-patch v0.5.2
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
	github.com/rode/es-index-manager v0.2.2
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/spf13/viper v1.7.0 // indirect
	github.com/stretchr/testify v1.6.1 // indirect
	go.uber.org/zap v1.16.0
	google.golang.org/genproto v0.0.0-20200806141610-86f49bd18e98
	google.golang.org/grpc v1.33.1
	google.golang.org/grpc/examples v0.0.0-20210111180913-4cf4a98505bc // indirect
	google.golang.org/protobuf v1.26.0
)

require (
	github.com/antlr/antlr4 v0.0.0-20200503195918-621b933c7a7f // indirect
	github.com/boltdb/bolt v1.3.1 // indirect
	github.com/cockroachdb/cmux v0.0.0-20170110192607-30d10be49292 // indirect
	github.com/fernet/fernet-go v0.0.0-20180830025343-9eac43b88a5e // indirect
	github.com/fsnotify/fsnotify v1.4.9 // indirect
	github.com/google/go-cmp v0.5.5 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.9.6 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/lib/pq v1.2.0 // indirect
	github.com/magiconair/properties v1.8.1 // indirect
	github.com/mitchellh/mapstructure v1.1.2 // indirect
	github.com/nxadm/tail v1.4.8 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/rs/cors v1.7.0 // indirect
	github.com/spf13/afero v1.1.2 // indirect
	github.com/spf13/cast v1.3.0 // indirect
	github.com/spf13/jwalterweatherman v1.0.0 // indirect
	github.com/subosito/gotenv v1.2.0 // indirect
	go.uber.org/atomic v1.6.0 // indirect
	go.uber.org/multierr v1.5.0 // indirect
	golang.org/x/net v0.0.0-20210428140749-89ef3d95e781 // indirect
	golang.org/x/sys v0.0.0-20210423082822-04245dca01da // indirect
	golang.org/x/text v0.3.6 // indirect
	gopkg.in/ini.v1 v1.51.0 // indirect
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

replace (
	github.com/grpc-ecosystem/grpc-gateway v1.9.0 => github.com/grpc-ecosystem/grpc-gateway v1.11.0
	github.com/grpc-ecosystem/grpc-gateway v1.9.6 => github.com/grpc-ecosystem/grpc-gateway v1.11.0
)
