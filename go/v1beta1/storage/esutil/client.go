package esutil

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/rode/grafeas-elasticsearch/go/v1beta1/storage/filtering"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type CreateRequest struct {
	Index   string
	Refresh string // TODO: use RefreshOption type
}

type GetRequest struct {
	Index  string
	Search *EsSearch
}

type ListRequest struct {
	Index       string
	Filter      string
	Query       *EsSearch
	Pagination  *ListPaginationOptions
	SortOptions *ListSortOptions
}

type ListPaginationOptions struct {
	Size      int
	Token     string
	Keepalive string
}

type ListSortOptions struct {
	Direction EsSortOrder
	Field     string
}

type ListResponse struct {
	Hits          *EsSearchResponseHits
	NextPageToken string
}

const defaultPitKeepAlive = "5m"
const grafeasMaxPageSize = 1000

type Client interface {
	Create(ctx context.Context, request *CreateRequest, message proto.Message) (string, error)
	Get(ctx context.Context, request *GetRequest, message proto.Message) (string, error)
	List(ctx context.Context, request *ListRequest) (*ListResponse, error)
}

type client struct {
	logger   *zap.Logger
	esClient *elasticsearch.Client
	filterer filtering.Filterer
}

func NewClient(logger *zap.Logger, esClient *elasticsearch.Client, filterer filtering.Filterer) Client {
	return &client{
		logger,
		esClient,
		filterer,
	}
}

func (c *client) Create(ctx context.Context, request *CreateRequest, message proto.Message) (string, error) {
	log := c.logger.Named("Create")
	str, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(message)
	if err != nil {
		return "", err
	}

	if request.Refresh == "" {
		request.Refresh = "true"
	}

	res, err := c.esClient.Index(
		request.Index,
		bytes.NewReader(str),
		c.esClient.Index.WithContext(ctx),
		c.esClient.Index.WithRefresh(request.Refresh),
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

func (c *client) Get(ctx context.Context, request *GetRequest, message proto.Message) (string, error) {
	log := c.logger.Named("Get")
	encodedBody, requestJson := EncodeRequest(request.Search)
	log = log.With(zap.String("request", requestJson))

	res, err := c.esClient.Search(
		c.esClient.Search.WithContext(ctx),
		c.esClient.Search.WithIndex(request.Index),
		c.esClient.Search.WithBody(encodedBody),
	)
	if err != nil {
		return "", err
	}
	if res.IsError() {
		return "", errors.New(fmt.Sprintf("unexpected response from elasticsearch: %s", res.String()))
	}

	var searchResults EsSearchResponse
	if err := DecodeResponse(res.Body, &searchResults); err != nil {
		return "", err
	}

	if searchResults.Hits.Total.Value == 0 {
		log.Debug("document not found", zap.Any("search", request.Search))
		return "", nil
	}

	return searchResults.Hits.Hits[0].ID, protojson.Unmarshal(searchResults.Hits.Hits[0].Source, message)
}

func (c *client) List(ctx context.Context, request *ListRequest) (*ListResponse, error) {
	log := c.logger.Named("List")
	response := &ListResponse{}

	body := &EsSearch{}
	if request.Query != nil {
		body = request.Query
	}

	if request.Filter != "" {
		log = log.With(zap.String("filter", request.Filter))
		filterQuery, err := c.filterer.ParseExpression(request.Filter)
		if err != nil {
			return nil, err
		}

		body.Query = filterQuery
	}

	if request.SortOptions != nil {
		body.Sort = map[string]EsSortOrder{
			request.SortOptions.Field: request.SortOptions.Direction,
		}
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
		if searchFrom < response.Hits.Total.Value {
			response.NextPageToken = CreatePageToken(pitId, searchFrom+request.Pagination.Size)
		}
	}

	return response, nil
}
