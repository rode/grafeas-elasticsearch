package config

import (
	"fmt"
	"github.com/hashicorp/go-multierror"
)

type ElasticsearchConfig struct {
	URI     string        `mapstructure:"uri"`
	Refresh RefreshOption `mapstructure:"refresh"`
}

func (c ElasticsearchConfig) IsValid() (e error) {
	switch c.Refresh {
	case RefreshTrue, RefreshWaitFor, RefreshFalse:
		break
	default:
		e = multierror.Append(e, fmt.Errorf("invalid refresh value: %s", c.Refresh))
	}

	return
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
