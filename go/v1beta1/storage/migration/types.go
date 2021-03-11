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

package migration

import (
	"context"

	"github.com/elastic/go-elasticsearch/v7"
	"go.uber.org/zap"
)

type ESMigrator struct {
	client       *elasticsearch.Client
	indexManager IndexManager
	logger       *zap.Logger
}

type Migration struct {
	DocumentKind string
	Index        string
	Alias        string
}

type Migrator interface {
	GetMigrations(ctx context.Context) ([]*Migration, error)
	Migrate(ctx context.Context, migration *Migration) error
}

type ESBlockIndex struct {
	Name    string `json:"name"`
	Blocked bool   `json:"blocked"`
}

type ESBlockResponse struct {
	Acknowledged       bool           `json:"acknowledged"`
	ShardsAcknowledged bool           `json:"shards_acknowledged"`
	Indices            []ESBlockIndex `json:"indices"`
}

type ESTaskCreationResponse struct {
	Task string `json:"task"`
}

type ESTask struct {
	Completed bool `json:"completed"`
}

type ESActions struct {
	Add    *ESIndexAlias `json:"add,omitempty"`
	Remove *ESIndexAlias `json:"remove,omitempty"`
}

type ESIndexAlias struct {
	Index string `json:"index"`
	Alias string `json:"alias"`
}

type ESIndexAliasRequest struct {
	Actions []ESActions `json:"actions"`
}

type ESReindex struct {
	Conflicts   string         `json:"conflicts"`
	Source      *ReindexFields `json:"source"`
	Destination *ReindexFields `json:"dest"`
}

type ReindexFields struct {
	Index  string `json:"index"`
	OpType string `json:"op_type,omitempty"`
}

type ESErrorResponse struct {
	Error ESError `json:"error"`
}

type ESError struct {
	Type string `json:"type"`
}
