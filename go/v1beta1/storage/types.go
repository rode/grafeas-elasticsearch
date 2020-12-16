package storage

import (
	"encoding/json"

	"github.com/liatrio/grafeas-elasticsearch/go/v1beta1/storage/filtering"
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

// Elasticsearch /_msearch query fragments

type esMsearchQueryFragment struct {
	Index string `json:"index"`
}

// Elasticsearch /_msearch response

type esMsearch struct {
	Took      int                 `json:"took"`
	Responses []esMsearchResponse `json:"responses"`
}

type esMsearchResponse struct {
	Took     int  `json:"took"`
	TimedOut bool `json:"timed_out"`
	Shards   struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Skipped    int `json:"skipped"`
		Failed     int `json:"failed"`
	} `json:"_shards"`
	Hits struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		MaxScore float64 `json:"max_score"`
		Hits     []struct {
			Index  string  `json:"_index"`
			Type   string  `json:"_type"`
			ID     string  `json:"_id"`
			Score  float64 `json:"_score"`
			Source struct {
				Name             string `json:"name"`
				ShortDescription string `json:"shortDescription"`
				LongDescription  string `json:"longDescription"`
				Kind             string `json:"kind"`
				Vulnerability    struct {
					Details []struct {
						CpeURI             string `json:"cpeUri"`
						Package            string `json:"package"`
						MinAffectedVersion struct {
							Name     string `json:"name"`
							Revision string `json:"revision"`
							Kind     string `json:"kind"`
						} `json:"minAffectedVersion"`
					} `json:"details"`
				} `json:"vulnerability"`
			} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
	Status int `json:"status"`
}
