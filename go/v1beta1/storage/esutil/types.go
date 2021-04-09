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
	Took  int                   `json:"took"`
	Hits  *EsSearchResponseHits `json:"hits"`
	PitId string                `json:"pit_id"`
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
	Collapse *EsSearchCollapse      `json:"collapse,omitempty"`
	Pit      *EsSearchPit           `json:"pit,omitempty"`
}

type EsSortOrder string

const (
	EsSortOrderAscending EsSortOrder = "asc"
	EsSortOrderDecending EsSortOrder = "desc"
)

type EsSearchCollapse struct {
	Field string `json:"field,omitempty"`
}

type EsSearchPit struct {
	Id        string `json:"id"`
	KeepAlive string `json:"keep_alive"`
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
	Took                 int           `json:"took"`
	TimedOut             bool          `json:"timed_out"`
	Total                int           `json:"total"`
	Deleted              int           `json:"deleted"`
	Batches              int           `json:"batches"`
	VersionConflicts     int           `json:"version_conflicts"`
	Noops                int           `json:"noops"`
	ThrottledMillis      int           `json:"throttled_millis"`
	RequestsPerSecond    float64       `json:"requests_per_second"`
	ThrottledUntilMillis int           `json:"throttled_until_millis"`
	Failures             []interface{} `json:"failures"`
}

// Elasticsearch /_bulk query fragments

type EsBulkQueryFragment struct {
	Index  *EsBulkQueryIndexFragment  `json:"index"`
	Create *EsBulkQueryCreateFragment `json:"create"`
}

type EsBulkQueryIndexFragment struct {
	Index string `json:"_index"`
}

type EsBulkQueryCreateFragment struct {
	Id string `json:"_id"`
}

// Elasticsearch /_bulk response

type EsBulkResponse struct {
	Items  []*EsBulkResponseItem `json:"items"`
	Errors bool
}

type EsBulkResponseItem struct {
	Index  *EsIndexDocResponse `json:"index,omitempty"`
	Create *EsIndexDocResponse `json:"create,omitempty"`
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

// Elasticsearch /$INDEX/_pit response

type ESPitResponse struct {
	Id string `json:"id"`
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

type EsMultiGetRequest struct {
	IDs []string `json:"ids"`
}

type EsMultiGetDocument struct {
	ID    string `json:"_id"`
	Found bool   `json:"found"`
}

type EsMultiGetResponse struct {
	Docs []*EsMultiGetDocument `json:"docs"`
}

// response for index creation
type EsIndexResponse struct {
	Acknowledged       bool   `json:"acknowledged"`
	ShardsAcknowledged bool   `json:"shards_acknowledged"`
	Index              string `json:"index"`
}
