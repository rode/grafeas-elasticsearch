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
	Query *filtering.Query `json:"query,omitempty"`
}

// Elasticsearch /_doc response

type esIndexDocResponse struct {
	Id     string           `json:"_id"`
	Status int              `json:"status"`
	Error  *esIndexDocError `json:"error,omitempty"`
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
