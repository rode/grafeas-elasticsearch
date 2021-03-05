package storage

import (
	"context"
	"fmt"

	"github.com/elastic/go-elasticsearch/v7"
)

type ESMigrater struct {
	Client *elasticsearch.Client
}

// {"acknowledged":true,
// "shards_acknowledged":true,
// "indices":[{"name":"grafeas-v1beta1-rode-occurrences","blocked":true}]}
type ESWriteBlockResponse struct {
	Acknowledged bool                     `json:"acknowledged"`
	Indices      []map[string]interface{} `json:"indices"`
}

func (e *ESMigrater) Migrate(ctx context.Context, indexName string) error {
	res, err := e.Client.Indices.AddBlock([]string{indexName}, "write", e.Client.Indices.AddBlock.WithContext(ctx))
	if err != nil {
		return err
	}
	if res.IsError() {
		return fmt.Errorf("Error from Elasticsearch", res.StatusCode)
	}
	fmt.Println(res)
	m := ESWriteBlockResponse{}
	decodeResponse(res.Body, &m)

	return nil

}
