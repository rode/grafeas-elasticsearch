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

package storage

import (
	"encoding/json"

	"github.com/rode/grafeas-elasticsearch/go/v1beta1/storage/filtering"
)

// Elasticsearch /_search response

type esSearchResponse struct {
	Took int                   `json:"took"`
	Hits *esSearchResponseHits `json:"hits"`
}

type esSearchResponseHits struct {
	Total *esSearchResponseTotal `json:"total"`
	Hits  []*esSearchResponseHit `json:"hits"`
}

type esSearchResponseTotal struct {
	Value int `json:"value"`
}

type esSearchResponseHit struct {
	ID         string          `json:"_id"`
	Source     json.RawMessage `json:"_source"`
	Highlights json.RawMessage `json:"highlight"`
	Sort       []interface{}   `json:"sort"`
}

// Elasticsearch /_search query

type esSearch struct {
	Query *filtering.Query       `json:"query,omitempty"`
	Sort  map[string]esSortOrder `json:"sort,omitempty"`
}

type esSortOrder string

const (
	esSortOrderAscending esSortOrder = "asc"
	esSortOrderDecending esSortOrder = "desc"
)

// Elasticsearch /_doc response

type esIndexDocResponse struct {
	Id      string           `json:"_id"`
	Result  string           `json:"result"`
	Version int              `json:"_version"`
	Status  int              `json:"status"`
	Error   *esIndexDocError `json:"error,omitempty"`
}

type esIndexDocError struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

// Elasticsearch /_delete_by_query response

type esDeleteResponse struct {
	Deleted int `json:"deleted"`
}

// Elasticsearch /_bulk query fragments

type esBulkQueryFragment struct {
	Index *esBulkQueryIndexFragment `json:"index"`
}

type esBulkQueryIndexFragment struct {
	Index string `json:"_index"`
}

// Elasticsearch /_bulk response

type esBulkResponse struct {
	Items  []*esBulkResponseItem `json:"items"`
	Errors bool
}

type esBulkResponseItem struct {
	Index *esIndexDocResponse `json:"index,omitempty"`
}

// Elasticsearch /_msearch query fragments

type esMultiSearchQueryFragment struct {
	Index string `json:"index"`
}

// Elasticsearch /_msearch response

type esMultiSearchResponse struct {
	Responses []*esMultiSearchResponseHitsSummary `json:"responses"`
}

type esMultiSearchResponseHitsSummary struct {
	Hits *esMultiSearchResponseHits `json:"hits"`
}

type esMultiSearchResponseHits struct {
	Total *esSearchResponseTotal      `json:"total"`
	Hits  []*esMultiSearchResponseHit `json:"hits"`
}

type esMultiSearchResponseHit struct {
	Source json.RawMessage `json:"_source"`
}

type esUpdate struct {
	Doc json.RawMessage `json:"doc"`
}
