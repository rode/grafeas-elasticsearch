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

type Migrator interface {
	GetMigrations(ctx context.Context) ([]*IndexInfo, error)
	Migrate(ctx context.Context, migration *IndexInfo) error
}

type IndexManager interface {
	LoadMappings(mappingsDir string) error
	CreateIndex(ctx context.Context, info *IndexInfo, checkExists bool) error

	ProjectsIndex() string
	ProjectsAlias() string

	OccurrencesIndex(projectId string) string
	OccurrencesAlias(projectId string) string

	NotesIndex(projectId string) string
	NotesAlias(projectId string) string

	IncrementIndexVersion(indexName string) string
	GetLatestVersionForDocumentKind(documentKind string) string
	GetAliasForIndex(indexName string) string
}

type VersionedMapping struct {
	Version  string                 `json:"version"`
	Mappings map[string]interface{} `json:"mappings"`
}

type IndexInfo struct {
	Index        string
	Alias        string
	DocumentKind string
}

type IndexNameParts struct {
	DocumentKind string
	Version      string
	ProjectId    string
}

type EsIndexManager struct {
	logger            *zap.Logger
	client            *elasticsearch.Client
	projectMapping    *VersionedMapping
	occurrenceMapping *VersionedMapping
	noteMapping       *VersionedMapping
}
