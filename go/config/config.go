// Copyright 2021 The Rode Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"fmt"
	"github.com/hashicorp/go-multierror"
)

type ElasticsearchConfig struct {
	Refresh                 RefreshOption
	URL, Username, Password string
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

// RefreshOption is based on https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-refresh.html
type RefreshOption string

func (r RefreshOption) String() string {
	return string(r)
}

const (
	RefreshTrue    = "true"
	RefreshWaitFor = "wait_for"
	RefreshFalse   = "false"
)
