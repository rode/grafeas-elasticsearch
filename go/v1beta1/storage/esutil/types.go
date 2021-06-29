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

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/rode/grafeas-elasticsearch/go/v1beta1/storage/filtering"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
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
	Routing  string                 `json:"-"`
}

type EsSortOrder string

const (
	EsSortOrderAscending  EsSortOrder = "asc"
	EsSortOrderDescending EsSortOrder = "desc"
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
	Index  *EsBulkQueryOperationFragment `json:"index,omitempty"`
	Create *EsBulkQueryOperationFragment `json:"create,omitempty"`
}

type EsBulkQueryOperationFragment struct {
	Id      string `json:"_id,omitempty"`
	Index   string `json:"_index,omitempty"`
	Routing string `json:"routing,omitempty"`
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
	Index   string `json:"index"`
	Routing string `json:"routing,omitempty"`
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

// Elasticsearch /$INDEX/_pit response

type ESPitResponse struct {
	Id string `json:"id"`
}

type EsMultiGetItem struct {
	Id      string `json:"_id"`
	Index   string `json:"_index,omitempty"`
	Routing string `json:"routing,omitempty"`
}

type EsMultiGetRequest struct {
	IDs  []string          `json:"ids,omitempty"`
	Docs []*EsMultiGetItem `json:"docs,omitempty"`
}

type EsGetResponse struct {
	Id     string          `json:"_id"`
	Found  bool            `json:"found"`
	Source json.RawMessage `json:"_source"`
}

type EsMultiGetResponse struct {
	Docs []*EsGetResponse `json:"docs"`
}

// response for index creation
type EsIndexResponse struct {
	Acknowledged       bool   `json:"acknowledged"`
	ShardsAcknowledged bool   `json:"shards_acknowledged"`
	Index              string `json:"index"`
}

type EsJoin struct {
	// Field represents the name of the join field
	Field string
	// Name represents the name of the document, as specified within the index mapping
	Name string
	// Parent represents the ID of this resource's parent. Omit this if this document has no parent.
	Parent string
}

// EsDocWithJoin makes it possible to add a "join" field to the source JSON without having to modify the underlying protobuf message.
// The "join" field is used as described here: https://www.elastic.co/guide/en/elasticsearch/reference/7.10/parent-join.html.
// When marshaled to JSON, the "join" field will be merged with the protobuf JSON as a patch.
type EsDocWithJoin struct {
	Join    *EsJoin
	Message proto.Message
}

func (e *EsDocWithJoin) MarshalJSON() ([]byte, error) {
	joinField := map[string]string{
		"name": e.Join.Name,
	}
	if e.Join.Parent != "" {
		joinField["parent"] = e.Join.Parent
	}

	patch := map[string]interface{}{
		e.Join.Field: joinField,
	}

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return nil, err
	}

	messageBytes, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(e.Message)
	if err != nil {
		return nil, err
	}

	return jsonpatch.MergePatch(messageBytes, patchBytes)
}
