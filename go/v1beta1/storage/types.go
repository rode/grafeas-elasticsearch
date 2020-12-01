package storage

import (
	"bytes"
	"encoding/json"
	"io"
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
	Query *esSearchQuery `json:"query"`
}

type esSearchQuery struct {
	Term map[string]interface{} `json:"term,omitempty"`
}

func createElasticsearchSearchTermQuery(term map[string]interface{}) (io.Reader, error) {
	b, err := json.Marshal(&esSearch{
		Query: &esSearchQuery{
			Term: term,
		},
	})
	if err != nil {
		return nil, err
	}

	return bytes.NewReader(b), nil
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
	Index *esBulkResponseIndexItem `json:"index,omitempty"`
}

type esBulkResponseIndexItem struct {
	Id     string                   `json:"_id"`
	Status int                      `json:"status"`
	Error  *esBulkResponseItemError `json:"error,omitempty"`
}

type esBulkResponseItemError struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}
