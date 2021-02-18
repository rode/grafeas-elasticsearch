package storage

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

type EsCollapse struct {
	Field string `json:"field,omitempty"`
}

type EsSearch struct {
	Query    *filtering.Query       `json:"query,omitempty"`
	Sort     map[string]EsSortOrder `json:"sort,omitempty"`
	Collapse *EsCollapse            `json:"collapse,omitempty"`
}

type EsSortOrder string

const (
	EsSortOrderAscending  EsSortOrder = "asc"
	EsSortOrderDescending EsSortOrder = "desc"
)

// Elasticsearch /_doc response

type EsIndexDocResponse struct {
	Id     string           `json:"_id"`
	Status int              `json:"status"`
	Error  *EsIndexDocError `json:"error,omitempty"`
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
