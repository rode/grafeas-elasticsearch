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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type CreateRequest struct {
	Index      string
	Refresh    string // TODO: use RefreshOption type
	Message    proto.Message
	DocumentId string
}

type BulkCreateRequest struct {
	Index   string
	Refresh string // TODO: use RefreshOption type
	Items   []*BulkCreateRequestItem
}

type BulkCreateRequestItem struct {
	Message    proto.Message
	DocumentId string
}

type MultiSearchRequest struct {
	Index    string
	Searches []*EsSearch
}

type MultiGetRequest struct {
	Index       string
	DocumentIds []string
}

type SearchRequest struct {
	Index      string
	Search     *EsSearch
	Pagination *SearchPaginationOptions
}

type SearchPaginationOptions struct {
	Size      int
	Token     string
	Keepalive string
}

type SearchResponse struct {
	Hits          *EsSearchResponseHits
	NextPageToken string
}

type UpdateRequest struct {
	Index      string
	DocumentId string
	Refresh    string // TODO: use RefreshOption type
	Message    proto.Message
}

type DeleteRequest struct {
	Index   string
	Search  *EsSearch
	Refresh string // TODO: use RefreshOption type
}

const defaultPitKeepAlive = "5m"
const grafeasMaxPageSize = 1000

type Client interface {
	Create(ctx context.Context, request *CreateRequest) (string, error)
	BulkCreate(ctx context.Context, request *BulkCreateRequest) (*EsBulkResponse, error)
	Search(ctx context.Context, request *SearchRequest) (*SearchResponse, error)
	MultiSearch(ctx context.Context, request *MultiSearchRequest) (*EsMultiSearchResponse, error)
	MultiGet(ctx context.Context, request *MultiGetRequest) (*EsMultiGetResponse, error)
	Update(ctx context.Context, request *UpdateRequest) error
	Delete(ctx context.Context, request *DeleteRequest) error
}

type client struct {
	logger   *zap.Logger
	esClient *elasticsearch.Client
}

func NewClient(logger *zap.Logger, esClient *elasticsearch.Client) Client {
	return &client{
		logger,
		esClient,
	}
}

func (c *client) Create(ctx context.Context, request *CreateRequest) (string, error) {
	log := c.logger.Named("Create")
	str, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(request.Message)
	if err != nil {
		return "", err
	}

	if request.Refresh == "" {
		request.Refresh = "true"
	}

	indexOpts := []func(*esapi.IndexRequest){
		c.esClient.Index.WithContext(ctx),
		c.esClient.Index.WithRefresh(request.Refresh),
	}
	if request.DocumentId != "" {
		indexOpts = append(indexOpts, c.esClient.Index.WithDocumentID(request.DocumentId))
	}

	res, err := c.esClient.Index(
		request.Index,
		bytes.NewReader(str),
		indexOpts...,
	)
	if err != nil {
		return "", err
	}
	if res.IsError() {
		return "", errors.New(fmt.Sprintf("unexpected response from elasticsearch: %s", res.String()))
	}

	esResponse := EsIndexDocResponse{}
	if err := DecodeResponse(res.Body, &esResponse); err != nil {
		return "", err
	}

	log.Debug("elasticsearch response", zap.Any("response", esResponse))

	return esResponse.Id, nil
}

func (c *client) BulkCreate(ctx context.Context, request *BulkCreateRequest) (*EsBulkResponse, error) {
	log := c.logger.Named("BulkCreate")

	// build the request body using newline delimited JSON (ndjson)
	// each message is represented by two JSON structures:
	// the first is the metadata that represents the ES operation, in this case "index"
	// the second is the source payload to index
	// in total, this body will consist of (len(messages) * 2) JSON structures, separated by newlines, with a trailing newline at the end
	var body bytes.Buffer
	for _, item := range request.Items {
		metadata := &EsBulkQueryFragment{}

		if item.DocumentId != "" {
			metadata.Create = &EsBulkQueryCreateFragment{
				Id: item.DocumentId,
			}
		} else {
			metadata.Index = &EsBulkQueryIndexFragment{
				Index: request.Index,
			}
		}

		metadataBytes, _ := json.Marshal(metadata)
		metadataBytes = append(metadataBytes, '\n')

		data, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(item.Message)
		if err != nil {
			return nil, err
		}

		dataBytes := append(data, '\n')
		body.Grow(len(metadataBytes) + len(dataBytes))
		body.Write(metadataBytes)
		body.Write(dataBytes)
	}

	log.Debug("attempting ES bulk index", zap.String("payload", string(body.Bytes())))

	res, err := c.esClient.Bulk(
		bytes.NewReader(body.Bytes()),
		c.esClient.Bulk.WithContext(ctx),
		c.esClient.Bulk.WithRefresh(request.Refresh),
		c.esClient.Bulk.WithIndex(request.Index),
	)
	if err != nil {
		return nil, err
	}
	if res.IsError() {
		return nil, errors.New(fmt.Sprintf("unexpected response from elasticsearch: %s", res.String()))
	}

	var response EsBulkResponse
	if err = DecodeResponse(res.Body, &response); err != nil {
		return nil, err
	}

	return &response, nil
}

func (c *client) Search(ctx context.Context, request *SearchRequest) (*SearchResponse, error) {
	log := c.logger.Named("Search")
	response := &SearchResponse{}

	body := &EsSearch{}
	if request.Search != nil {
		body = request.Search
	}

	searchOptions := []func(*esapi.SearchRequest){
		c.esClient.Search.WithContext(ctx),
	}

	var (
		searchFrom int
		pitId      string
	)
	if request.Pagination != nil {
		var err error
		log = log.With(zap.String("pageToken", request.Pagination.Token), zap.Int("pageSize", request.Pagination.Size))

		if request.Pagination.Keepalive == "" {
			request.Pagination.Keepalive = defaultPitKeepAlive
		}

		// if no page token is specified, we need to create a new PIT
		if request.Pagination.Token == "" {
			res, err := c.esClient.OpenPointInTime(
				c.esClient.OpenPointInTime.WithContext(ctx),
				c.esClient.OpenPointInTime.WithIndex(request.Index),
				c.esClient.OpenPointInTime.WithKeepAlive(request.Pagination.Keepalive),
			)
			if err != nil {
				return nil, err
			}
			if res.IsError() {
				return nil, errors.New(fmt.Sprintf("unexpected response from elasticsearch: %s", res.String()))
			}

			var pitResponse ESPitResponse
			if err = DecodeResponse(res.Body, &pitResponse); err != nil {
				return nil, err
			}

			pitId = pitResponse.Id
			searchFrom = 0
		} else {
			// get the PIT from the provided page token
			pitId, searchFrom, err = ParsePageToken(request.Pagination.Token)
			if err != nil {
				return nil, err
			}
		}

		body.Pit = &EsSearchPit{
			Id:        pitId,
			KeepAlive: request.Pagination.Keepalive,
		}

		searchOptions = append(searchOptions,
			c.esClient.Search.WithFrom(searchFrom),
			c.esClient.Search.WithSize(request.Pagination.Size),
		)
	} else {
		searchOptions = append(searchOptions,
			c.esClient.Search.WithIndex(request.Index),
			c.esClient.Search.WithSize(grafeasMaxPageSize),
		)
	}

	encodedBody, requestJson := EncodeRequest(body)
	log = log.With(zap.String("request", requestJson))
	log.Debug("performing search")

	res, err := c.esClient.Search(
		append(searchOptions, c.esClient.Search.WithBody(encodedBody))...,
	)
	if err != nil {
		return nil, err
	}
	if res.IsError() {
		return nil, errors.New(fmt.Sprintf("unexpected response from elasticsearch: %s", res.String()))
	}

	var searchResults EsSearchResponse
	if err := DecodeResponse(res.Body, &searchResults); err != nil {
		return nil, err
	}

	response.Hits = searchResults.Hits
	if request.Pagination != nil {
		nextSearchFrom := searchFrom + request.Pagination.Size

		if nextSearchFrom < response.Hits.Total.Value {
			response.NextPageToken = CreatePageToken(pitId, nextSearchFrom)
		}
	}

	return response, nil
}

func (c *client) MultiSearch(ctx context.Context, request *MultiSearchRequest) (*EsMultiSearchResponse, error) {
	log := c.logger.Named("MultiSearch")

	searchMetadata, _ := json.Marshal(&EsMultiSearchQueryFragment{
		Index: request.Index,
	})
	searchMetadata = append(searchMetadata, '\n')

	var searchRequestBody bytes.Buffer
	for _, search := range request.Searches {
		data, _ := json.Marshal(search)
		dataBytes := append(data, '\n')

		searchRequestBody.Grow(len(searchMetadata) + len(dataBytes))
		searchRequestBody.Write(searchMetadata)
		searchRequestBody.Write(dataBytes)
	}

	res, err := c.esClient.Msearch(
		bytes.NewReader(searchRequestBody.Bytes()),
		c.esClient.Msearch.WithContext(ctx),
	)
	if err != nil {
		return nil, err
	}
	if res.IsError() {
		return nil, errors.New(fmt.Sprintf("unexpected response from elasticsearch: %s", res.String()))
	}

	var response EsMultiSearchResponse
	if err = DecodeResponse(res.Body, &response); err != nil {
		return nil, err
	}

	log.Debug("elasticsearch response", zap.Any("response", response))

	return &response, nil
}

func (c *client) MultiGet(ctx context.Context, request *MultiGetRequest) (*EsMultiGetResponse, error) {
	log := c.logger.Named("MultiGet")

	encodedBody, requestJson := EncodeRequest(&EsMultiGetRequest{
		IDs: request.DocumentIds,
	})
	log = log.With(zap.String("request", requestJson))

	res, err := c.esClient.Mget(
		encodedBody,
		c.esClient.Mget.WithContext(ctx),
		c.esClient.Mget.WithIndex(request.Index),
	)
	if err != nil {
		return nil, err
	}
	if res.IsError() {
		return nil, errors.New(fmt.Sprintf("unexpected response from elasticsearch: %s", res.String()))
	}

	var response EsMultiGetResponse
	if err = DecodeResponse(res.Body, &response); err != nil {
		return nil, err
	}

	log.Debug("elasticsearch response", zap.Any("response", response))

	return &response, nil
}

func (c *client) Update(ctx context.Context, request *UpdateRequest) error {
	log := c.logger.Named("Update")
	str, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(request.Message)
	if err != nil {
		return err
	}

	if request.Refresh == "" {
		request.Refresh = "true"
	}

	res, err := c.esClient.Index(
		request.Index,
		bytes.NewReader(str),
		c.esClient.Index.WithDocumentID(request.DocumentId),
		c.esClient.Index.WithContext(ctx),
		c.esClient.Index.WithRefresh(request.Refresh),
	)
	if err != nil {
		return err
	}
	if res.IsError() {
		return errors.New(fmt.Sprintf("unexpected response from elasticsearch: %s", res.String()))
	}

	esResponse := EsIndexDocResponse{}
	if err := DecodeResponse(res.Body, &esResponse); err != nil {
		return err
	}

	log.Debug("elasticsearch response", zap.Any("response", esResponse))

	return nil
}

func (c *client) Delete(ctx context.Context, request *DeleteRequest) error {
	log := c.logger.Named("Delete")
	encodedBody, requestJson := EncodeRequest(request.Search)
	log = log.With(zap.String("request", requestJson))

	if request.Refresh == "" {
		request.Refresh = "true"
	}

	res, err := c.esClient.DeleteByQuery(
		[]string{request.Index},
		encodedBody,
		c.esClient.DeleteByQuery.WithContext(ctx),
		c.esClient.DeleteByQuery.WithRefresh(withRefreshBool(request.Refresh)),
	)
	if err != nil {
		return err
	}
	if res.IsError() {
		return errors.New(fmt.Sprintf("unexpected response from elasticsearch: %s", res.String()))
	}

	deletedResults := EsDeleteResponse{}
	if err = DecodeResponse(res.Body, &deletedResults); err != nil {
		return err
	}

	if deletedResults.Deleted == 0 {
		return errors.New("elasticsearch returned zero deleted documents")
	}

	return nil
}

// DeleteByQuery does not support `wait_for` value, although API docs say it is available.
// Immediately refresh on `wait_for` config, assuming that is likely closer to the desired Grafeas user functionality.
// https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-delete-by-query.html#docs-delete-by-query-api-query-params
func withRefreshBool(o string) bool {
	if o == "false" {
		return false
	}
	return true
}