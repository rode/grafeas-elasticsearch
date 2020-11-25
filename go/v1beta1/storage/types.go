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
