package config

type ElasticsearchConfig struct {
	URI     string        `mapstructure:"uri"`
	Refresh RefreshOption `mapstructure:"refresh"`
}

// https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-refresh.html
type RefreshOption string

func (r RefreshOption) String() string {
	return string(r)
}

const (
	RefreshTrue    = "true"
	RefreshWaitFor = "wait_for"
	RefreshFalse   = "false"
)
