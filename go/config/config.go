package config

// ElasticsearchConfig is...
type ElasticsearchConfig struct {
	URI      string `mapstructure:"uri"`
	Database string `mapstructure:"database"`
}
