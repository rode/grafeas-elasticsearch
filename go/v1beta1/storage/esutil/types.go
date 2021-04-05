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

package esutil

import (
	"encoding/json"
	"github.com/rode/grafeas-elasticsearch/go/v1beta1/storage/filtering"
)

// Elasticsearch /_search response

type EsSearchResponse struct {
	Took int                   `json:"took"`
	Hits *EsSearchResponseHits `json:"hits"`
}

type EsSearchResponseHits struct {
	Total *EsSearchResponseTotal `json:"total"`
	Hits  []*EsSearchResponseHit `json:"hits"`
}

type EsSearchResponseTotal struct {
	Value int `json:"value"`
}

type EsSearchResponseHit struct {
	ID         string          `json:"_id"`
	Source     json.RawMessage `json:"_source"`
	Highlights json.RawMessage `json:"highlight"`
	Sort       []interface{}   `json:"sort"`
}

// Elasticsearch /_search query

type EsSearch struct {
	Query    *filtering.Query       `json:"query,omitempty"`
	Sort     map[string]EsSortOrder `json:"sort,omitempty"`
	Collapse *EsCollapse            `json:"collapse,omitempty"`
}

type EsSortOrder string

const (
	EsSortOrderAscending EsSortOrder = "asc"
	EsSortOrderDecending EsSortOrder = "desc"
)

type EsCollapse struct {
	Field string `json:"field,omitempty"`
}

// Elasticsearch /_doc response

type EsIndexDocResponse struct {
	Id      string           `json:"_id"`
	Result  string           `json:"result"`
	Version int              `json:"_version"`
	Status  int              `json:"status"`
	Error   *EsIndexDocError `json:"error,omitempty"`
}

type EsIndexDocError struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

// Elasticsearch /_delete_by_query response

type EsDeleteResponse struct {
	Deleted int `json:"deleted"`
}

// Elasticsearch /_bulk query fragments

type EsBulkQueryFragment struct {
	Index *EsBulkQueryIndexFragment `json:"index"`
}

type EsBulkQueryIndexFragment struct {
	Index string `json:"_index"`
}

// Elasticsearch /_bulk response

type EsBulkResponse struct {
	Items  []*EsBulkResponseItem `json:"items"`
	Errors bool
}

type EsBulkResponseItem struct {
	Index *EsIndexDocResponse `json:"index,omitempty"`
}

// Elasticsearch /_msearch query fragments

type EsMultiSearchQueryFragment struct {
	Index string `json:"index"`
}

// Elasticsearch /_msearch response

type EsMultiSearchResponse struct {
	Responses []*EsMultiSearchResponseHitsSummary `json:"responses"`
}

type EsMultiSearchResponseHitsSummary struct {
	Hits *EsMultiSearchResponseHits `json:"hits"`
}

type EsMultiSearchResponseHits struct {
	Total *EsSearchResponseTotal      `json:"total"`
	Hits  []*EsMultiSearchResponseHit `json:"hits"`
}

type EsMultiSearchResponseHit struct {
	Source json.RawMessage `json:"_source"`
}

// Elasticsearch /$INDEX/block/_write response

type ESBlockIndex struct {
	Name    string `json:"name"`
	Blocked bool   `json:"blocked"`
}

type ESBlockResponse struct {
	Acknowledged       bool           `json:"acknowledged"`
	ShardsAcknowledged bool           `json:"shards_acknowledged"`
	Indices            []ESBlockIndex `json:"indices"`
}

// Elasticsearch /$INDEX/_settings

type ESSettingsResponse struct {
	Settings *ESSettingsIndex `json:"settings"`
}

type ESSettingsIndex struct {
	Index *ESSettingsBlocks `json:"index"`
}

type ESSettingsBlocks struct {
	Blocks *ESSettingsWrite `json:"blocks"`
}

type ESSettingsWrite struct {
	Write string `json:"write"`
}

// response for calls where wait_for_completion=false
type ESTaskCreationResponse struct {
	Task string `json:"task"`
}

// /_tasks/$TASK_ID response
type ESTask struct {
	Completed bool `json:"completed"`
}

// Elasticsearch /_aliases request
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

// Elasticsearch /_reindex request

type ESReindex struct {
	Conflicts   string         `json:"conflicts"`
	Source      *ReindexFields `json:"source"`
	Destination *ReindexFields `json:"dest"`
}

type ReindexFields struct {
	Index  string `json:"index"`
	OpType string `json:"op_type,omitempty"`
}

// Elasticsearch 400 error response

type ESErrorResponse struct {
	Error ESError `json:"error"`
}

type ESError struct {
	Type string `json:"type"`
}

type ESIndex struct {
	Mappings *ESMappings `json:"mappings"`
}

type ESMappings struct {
	Meta *ESMeta `json:"_meta,omitempty"`
}

type ESMeta struct {
	Type string `json:"type,omitempty"`
}
