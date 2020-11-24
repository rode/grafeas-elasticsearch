package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/Jeffail/gabs/v2"
	"github.com/elastic/go-elasticsearch/v7/esapi"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/golang/protobuf/ptypes"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grafeasConfig "github.com/grafeas/grafeas/go/config"
	"github.com/grafeas/grafeas/go/v1beta1/storage"
	pb "github.com/grafeas/grafeas/proto/v1beta1/grafeas_go_proto"
	prpb "github.com/grafeas/grafeas/proto/v1beta1/project_go_proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const apiVersion = "v1beta1"
const indexPrefix = "grafeas-" + apiVersion

// ElasticsearchStorage is...
type ElasticsearchStorage struct {
	client *elasticsearch.Client
	logger *zap.Logger
}

// NewElasticsearchStore is...
func NewElasticsearchStore(client *elasticsearch.Client, logger *zap.Logger) *ElasticsearchStorage {
	return &ElasticsearchStorage{
		client: client,
		logger: logger,
	}
}

// ElasticsearchStorageTypeProvider configures a Grafeas storage backend that utilizes ElasticSearch.
// Configuring this backend will result in an index, representing projects, to be created.
func (es *ElasticsearchStorage) ElasticsearchStorageTypeProvider(storageType string, storageConfig *grafeasConfig.StorageConfiguration) (*storage.Storage, error) {
	log := es.logger.Named("ElasticsearchStorageTypeProvider")
	log.Info("registering elasticsearch storage provider")

	projectIndex := fmt.Sprintf("%s-%s", indexPrefix, "projects")

	if storageType != "elasticsearch" {
		return nil, fmt.Errorf("unknown storage type %s, must be 'elasticsearch'", storageType)
	}

	res, err := es.client.Indices.Exists([]string{projectIndex})
	if err != nil || (res.StatusCode != http.StatusOK && res.StatusCode != http.StatusNotFound) {
		return nil, createError(log, "error checking if project index already exists", err)
	}

	// the response is an error if the index was not found, so we need to create it
	if res.IsError() {
		log := log.With(zap.String("index", projectIndex))
		log.Info("initial index for grafeas projects not found, creating...")
		res, err = es.client.Indices.Create(
			projectIndex,
			withIndexMetadataAndStringMapping(),
		)
		if err != nil {
			return nil, createError(log, "error sending index creation request to elasticsearch", err)
		}
		if res.IsError() {
			return nil, createError(log, "error creating index in elasticsearch", errors.New(res.String()))
		}
		log.Info("index created")
	}

	return &storage.Storage{
		Ps: es,
		Gs: es,
	}, nil
}

// CreateProject creates a project document within the project index, along with two indices that can be used
// to store notes and occurrences.
// Additional metadata is attached to the newly created indices to help identify them as part of a Grafeas project
func (es *ElasticsearchStorage) CreateProject(ctx context.Context, projectID string, p *prpb.Project) (*prpb.Project, error) {
	projectName := fmt.Sprintf("projects/%s", projectID)
	log := es.logger.Named("CreateProject").With(zap.String("project", projectName))

	searchTerm, err := createElasticsearchSearchTermQuery(map[string]interface{}{
		"name": projectName,
	})
	if err != nil {
		return nil, createError(log, "error creating search body JSON", err)
	}

	projectIndex := fmt.Sprintf("%s-%s", indexPrefix, "projects")
	res, err := es.client.Search(
		es.client.Search.WithContext(ctx),
		es.client.Search.WithIndex(projectIndex),
		es.client.Search.WithBody(searchTerm),
	)
	if err != nil || res.IsError() {
		return nil, createError(log, "error searching elasticsearch for projects", err)
	}

	var searchResults esSearchResponse
	if err := json.NewDecoder(res.Body).Decode(&searchResults); err != nil {
		return nil, err
	}
	if searchResults.Hits.Total.Value > 0 {
		log.Info("project already exists")
		return nil, status.Error(codes.AlreadyExists, fmt.Sprintf("project with name %s already exists", projectName))
	}

	p.Name = projectName
	str, err := (&jsonpb.Marshaler{}).MarshalToString(p)
	if err != nil {
		return nil, createError(log, "error marshalling occurrence to json", err)
	}

	// Create new project document
	res, err = es.client.Index(
		projectIndex,
		bytes.NewReader([]byte(str)),
		es.client.Index.WithContext(ctx),
	)
	if err != nil || res.IsError() {
		return nil, createError(log, "error creating occurrence in elasticsearch", err)
	}

	// Create indices for occurrences and notes
	for _, index := range []string{
		fmt.Sprintf("%s-%s-%s", indexPrefix, projectID, "occurrences"),
		fmt.Sprintf("%s-%s-%s", indexPrefix, projectID, "notes"),
	} {
		res, err = es.client.Indices.Create(
			index,
			es.client.Indices.Create.WithContext(ctx),
			withIndexMetadataAndStringMapping(),
		)
		if err != nil || res.IsError() {
			return nil, createError(log, "error creating index", err)
		}
	}

	log.Info("created project")

	return p, nil
}

// GetProject returns the project with the given pID from the store
func (es *ElasticsearchStorage) GetProject(ctx context.Context, projectID string) (*prpb.Project, error) {
	projectName := fmt.Sprintf("projects/%s", projectID)
	log := es.logger.Named("GetProject").With(zap.String("project", projectName))

	searchTerm, err := createElasticsearchSearchTermQuery(map[string]interface{}{
		"name": projectName,
	})
	if err != nil {
		return nil, createError(log, "error creating search body JSON", err)
	}

	projectIndex := fmt.Sprintf("%s-%s", indexPrefix, "projects")
	res, err := es.client.Search(
		es.client.Search.WithContext(ctx),
		es.client.Search.WithIndex(projectIndex),
		es.client.Search.WithBody(searchTerm),
	)
	if err != nil || res.IsError() {
		return nil, createError(log, "error searching elasticsearch for project", err)
	}

	var searchResults esSearchResponse
	if err := decodeResponse(res.Body, &searchResults); err != nil {
		return nil, createError(log, "error unmarshalling elasticsearch response", err)
	}

	if searchResults.Hits.Total.Value == 0 {
		log.Info("project not found")
		return nil, status.Error(codes.NotFound, fmt.Sprintf("project with name %s not found", projectName))
	}

	project := &prpb.Project{}
	err = jsonpb.Unmarshal(strings.NewReader(string(searchResults.Hits.Hits[0].Source)), project)
	if err != nil {
		return nil, createError(log, "error unmarshalling project from elasticsearch", err)
	}

	return project, nil
}

// ListProjects returns up to pageSize number of projects beginning at pageToken (or from
// start if pageToken is the empty string).
func (es *ElasticsearchStorage) ListProjects(ctx context.Context, filter string, pageSize int, pageToken string) ([]*prpb.Project, string, error) {
	//id := decryptInt64(pageToken, es.PaginationKey, 0)
	//TODO
	return nil, "", nil
}

// DeleteProject deletes the project with the given pID from the store
func (es *ElasticsearchStorage) DeleteProject(ctx context.Context, projectID string) error {
	projectName := fmt.Sprintf("projects/%s", projectID)
	log := es.logger.Named("DeleteProject").With(zap.String("project", projectName))

	searchTerm, err := createElasticsearchSearchTermQuery(map[string]interface{}{
		"name": projectName,
	})
	if err != nil {
		return createError(log, "error creating search body JSON", err)
	}

	projectIndex := fmt.Sprintf("%s-%s", indexPrefix, "projects")
	res, err := es.client.DeleteByQuery(
		[]string{projectIndex},
		searchTerm,
		es.client.DeleteByQuery.WithContext(ctx),
	)
	if err != nil || res.IsError() {
		return createError(log, "error deleting elasticsearch project", err)
	}

	var deletedResults esDeleteResponse
	err = decodeResponse(res.Body, &deletedResults)
	if err != nil {
		return createError(log, "error unmarshalling elasticsearch response", err)
	}

	if deletedResults.Deleted == 0 {
		return createError(log, "elasticsearch returned zero deleted documents", nil, zap.Any("response", deletedResults))
	}

	return nil
}

// GetOccurrence returns the occurrence with pID and oID
func (es *ElasticsearchStorage) GetOccurrence(ctx context.Context, projectID, objectID string) (*pb.Occurrence, error) {
	log := es.logger.Named("GetOccurrence")

	queryField := fmt.Sprintf("projects/%s/occurrences/%s", projectID, objectID)

	var getDocumentBuffer bytes.Buffer
	getDocumentBody := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"name": queryField,
			},
		},
	}
	if err := json.NewEncoder(&getDocumentBuffer).Encode(getDocumentBody); err != nil {
		return nil, createError(log, "error encoding elasticsearch get document body", err)
	}

	res, err := es.client.Search(
		es.client.Search.WithContext(ctx),
		es.client.Search.WithIndex(projectID),
		es.client.Search.WithBody(&getDocumentBuffer),
	)
	if err != nil {
		return nil, createError(log, "error retrieving occurrence from elasticsearch", err)
	}
	if res.StatusCode != http.StatusOK {
		return nil, createError(log, "got unexpected status code from elasticsearch", nil, zap.Int("status", res.StatusCode))
	}

	log.Debug("elasticsearch response", zap.Any("response", res))

	getDocumentResponse := &esSearchResponse{}
	if err := json.NewDecoder(res.Body).Decode(&getDocumentResponse); err != nil {
		log.Error("error decoding elasticsearch response body", zap.NamedError("error", err))
		return nil, err
	}

	log.Debug("hit raw source", zap.Any("raw _source", getDocumentResponse.Hits.Hits[0].Source))

	occurrence := &pb.Occurrence{}
	jsonpb.Unmarshal(strings.NewReader(string(getDocumentResponse.Hits.Hits[0].Source)), occurrence)

	log.Debug("converted occurrence", zap.Any("unmarshaled occurrence", occurrence))

	return occurrence, nil
}

// ListOccurrences returns up to pageSize number of occurrences for this project beginning
// at pageToken, or from start if pageToken is the empty string.
func (es *ElasticsearchStorage) ListOccurrences(ctx context.Context, pID, filter, pageToken string, pageSize int32) ([]*pb.Occurrence, string, error) {
	log := es.logger.Named("ListOccurrences")

	var (
		os   []*pb.Occurrence
		body strings.Builder
	)

	log.Debug("Project ID", zap.String("pID", pID))

	body.WriteString("{\n")

	if filter == "" {
		body.WriteString(`"query" : { "match_all" : {} },`)
	} else {
		body.WriteString(filter)
	}

	body.WriteString(fmt.Sprintf(`"size": %d`, pageSize))
	body.WriteString("\n}")

	res, err := es.client.Search(
		es.client.Search.WithIndex(pID),
		es.client.Search.WithBody(strings.NewReader(body.String())),
	)
	if err != nil {
		log.Error("Failed to retrieve documents from Elasticsearch", zap.Error(err))
		return nil, "", err
	}
	defer res.Body.Close()

	if res.IsError() {
		//log.Error("got unexpected status code from elasticsearch", zap.Int("status", res.StatusCode))
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			fmt.Printf("Error parsing the response body: %s", err)
		} else {
			// Print the response status and error information.
			fmt.Printf("[%s] %s: %s",
				res.Status(),
				e["error"].(map[string]interface{})["type"],
				e["error"].(map[string]interface{})["reason"],
			)
		}
	}

	var searchResults esSearchResponse
	if err := json.NewDecoder(res.Body).Decode(&searchResults); err != nil {
		return nil, "", err
	}

	log.Debug("ES Search hits", zap.Any("Total Hits", searchResults.Hits.Total.Value))

	for _, hit := range searchResults.Hits.Hits {
		log.Debug("Occurrence Hit", zap.String("Occ RAW", fmt.Sprintf("%+v", string(hit.Source))))

		occ := &pb.Occurrence{}
		err := json.Unmarshal(hit.Source, &occ)
		if err != nil {
			log.Error("Failed to convert _doc to occurrence", zap.Error(err))
			return nil, "", err
		}

		log.Debug("Occurrence Hit", zap.String("Occ", fmt.Sprintf("%+v", occ)))

		os = append(os, occ)
	}

	return nil, "", nil
}

// CreateOccurrence adds the specified occurrence
func (es *ElasticsearchStorage) CreateOccurrence(ctx context.Context, projectID, userID string, o *pb.Occurrence) (*pb.Occurrence, error) {
	log := es.logger.Named("CreateOccurrence")

	if o.CreateTime == nil {
		o.CreateTime = ptypes.TimestampNow()
	}

	m := jsonpb.Marshaler{}
	str, err := m.MarshalToString(o)
	if err != nil {
		return nil, createError(log, "error marshalling occurrence to json", err)
	}

	res, err := es.client.Index(
		projectID,
		bytes.NewReader([]byte(str)),
		es.client.Index.WithContext(ctx),
	)
	if err != nil {
		return nil, createError(log, "error creating occurrence in elasticsearch", err)
	}

	if res.StatusCode != http.StatusCreated {
		return nil, createError(log, "got unexpected status code from elasticsearch", nil, zap.Int("status", res.StatusCode))
	}

	response, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, createError(log, "error parsing elasticsearch response", err)
	}

	log.Debug("elasticsearch response", zap.String("response", string(response)))

	parsedResponse, err := gabs.ParseJSON(response)
	if err != nil {
		return nil, createError(log, "error parsing elasticsearch response", err)
	}

	o.Name = fmt.Sprintf("projects/%s/occurrences/%s", projectID, parsedResponse.Path("_id").Data().(string))

	return o, nil
}

// BatchCreateOccurrences batch creates the specified occurrences in Elasticsearch.
func (es *ElasticsearchStorage) BatchCreateOccurrences(ctx context.Context, pID string, uID string, occs []*pb.Occurrence) ([]*pb.Occurrence, []error) {
	log := es.logger.Named("BatchCreateOccurrence")

	var buf bytes.Buffer

	// Prepare payload
	metadata := []byte(fmt.Sprintf(`{ "index" : { "_index" : "%s"} }%s`, pID, "\n"))

	// Encode occurrences to JSON
	for _, occ := range occs {
		data, err := json.Marshal(occ)
		if err != nil {
			log.Error("Cannot encode occurrence", zap.Any(occ.Name, err))
		}

		data = append(data, "\n"...)
		buf.Grow(len(metadata) + len(data))
		buf.Write(metadata)
		buf.Write(data)
	}

	log.Debug("Bulk payload", zap.Any("es bulk payload", string(buf.Bytes())))

	res, err := es.client.Bulk(bytes.NewReader(buf.Bytes()))
	if err != nil {
		log.Error("error creating occurrence", zap.NamedError("error", err))
		return nil, []error{status.Error(codes.Internal, "failed to create occurrence in elasticsearch")}
	}

	if res.StatusCode != http.StatusOK {
		log.Error("got unexpected status code from elasticsearch", zap.Int("status", res.StatusCode))
		return nil, []error{status.Error(codes.Internal, "unexpected response from elasticsearch when creating occurrence")}
	}

	return occs, nil
}

// UpdateOccurrence updates the existing occurrence with the given projectID and occurrenceID
func (es *ElasticsearchStorage) UpdateOccurrence(ctx context.Context, pID, oID string, o *pb.Occurrence, mask *fieldmaskpb.FieldMask) (*pb.Occurrence, error) {
	return nil, nil
}

// DeleteOccurrence deletes the occurrence with the given pID and oID
func (es *ElasticsearchStorage) DeleteOccurrence(ctx context.Context, pID, oID string) error {
	log := es.logger.Named("DeleteOccurrence")

	queryField := fmt.Sprintf("projects/%s/occurrences/%s", pID, oID)
	query := []byte(fmt.Sprintf(`{ "query" : { "match" : { "name" :"%s" } } }`, queryField))
	reader := bytes.NewReader(query)
	res, err := es.client.DeleteByQuery([]string{pID}, reader)

	if err != nil {
		log.Error("error deleting occurrence", zap.NamedError("error", err))
		return status.Error(codes.Internal, "failed to delete occurrence in elasticsearch")
	}

	log.Debug("elasticsearch response", zap.Any("res", res))

	if res.StatusCode != http.StatusOK {
		log.Error("got unexpected status code from elasticsearch", zap.Int("status", res.StatusCode))
		return status.Error(codes.Internal, "unexpected response from elasticsearch when deleting occurrence")
	}

	return nil
}

// GetNote returns the note with project (pID) and note ID (nID)
func (es *ElasticsearchStorage) GetNote(ctx context.Context, pID, nID string) (*pb.Note, error) {
	return nil, nil
}

// ListNotes returns up to pageSize number of notes for this project (pID) beginning
// at pageToken (or from start if pageToken is the empty string).
func (es *ElasticsearchStorage) ListNotes(ctx context.Context, pID, filter, pageToken string, pageSize int32) ([]*pb.Note, string, error) {
	log := es.logger.Named("ListNotes")
	log.Debug("Project ID", zap.String("pID", pID))

	var (
		os   []*pb.Occurrence
		body strings.Builder
	)

	body.WriteString("{\n")

	body.WriteString(fmt.Sprintf(`"size": %d`, pageSize))
	body.WriteString("\n}")

	res, err := es.client.Search(
		es.client.Search.WithIndex(pID),
		es.client.Search.WithBody(strings.NewReader(body.String())),
	)
	if err != nil {
		log.Error("Failed to retrieve documents from Elasticsearch", zap.Error(err))
		return nil, "", err
	}
	defer res.Body.Close()

	if res.IsError() {
		//log.Error("got unexpected status code from elasticsearch", zap.Int("status", res.StatusCode))
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			fmt.Printf("Error parsing the response body: %s", err)
		} else {
			// Print the response status and error information.
			fmt.Printf("[%s] %s: %s",
				res.Status(),
				e["error"].(map[string]interface{})["type"],
				e["error"].(map[string]interface{})["reason"],
			)
		}
	}

	var searchResults esSearchResponse
	if err := json.NewDecoder(res.Body).Decode(&searchResults); err != nil {
		return nil, "", err
	}

	log.Debug("ES Search hits", zap.Any("Total Hits", searchResults.Hits.Total.Value))

	for _, hit := range searchResults.Hits.Hits {
		log.Debug("Occurrence Hit", zap.String("Occ RAW", fmt.Sprintf("%+v", string(hit.Source))))

		occ := &pb.Occurrence{}
		err := json.Unmarshal(hit.Source, &occ)
		if err != nil {
			log.Error("Failed to convert _doc to occurrence", zap.Error(err))
			return nil, "", err
		}

		log.Debug("Occurrence Hit", zap.String("Occ", fmt.Sprintf("%+v", occ)))

		os = append(os, occ)
	}

	return nil, "", nil

}

// CreateNote adds the specified note
func (es *ElasticsearchStorage) CreateNote(ctx context.Context, pID, nID, uID string, n *pb.Note) (*pb.Note, error) {
	return nil, nil
}

// BatchCreateNotes batch creates the specified notes in memstore.
func (es *ElasticsearchStorage) BatchCreateNotes(ctx context.Context, pID, uID string, notes map[string]*pb.Note) ([]*pb.Note, []error) {
	return nil, nil
}

// UpdateNote updates the existing note with the given pID and nID
func (es *ElasticsearchStorage) UpdateNote(ctx context.Context, pID, nID string, n *pb.Note, mask *fieldmaskpb.FieldMask) (*pb.Note, error) {
	return nil, nil
}

// DeleteNote deletes the note with the given pID and nID
func (es *ElasticsearchStorage) DeleteNote(ctx context.Context, pID, nID string) error {
	return nil
}

// GetOccurrenceNote gets the note for the specified occurrence from PostgreSQL.
func (es *ElasticsearchStorage) GetOccurrenceNote(ctx context.Context, pID, oID string) (*pb.Note, error) {
	return nil, nil
}

// ListNoteOccurrences is...
func (es *ElasticsearchStorage) ListNoteOccurrences(ctx context.Context, projectID, nID, filter, pageToken string, pageSize int32) ([]*pb.Occurrence, string, error) {
	return []*pb.Occurrence{}, "", nil
}

// GetVulnerabilityOccurrencesSummary gets a summary of vulnerability occurrences from storage.
func (es *ElasticsearchStorage) GetVulnerabilityOccurrencesSummary(ctx context.Context, projectID, filter string) (*pb.VulnerabilityOccurrencesSummary, error) {
	return &pb.VulnerabilityOccurrencesSummary{}, nil
}

// createError is a helper function that allows you to easily log an error and return a gRPC formatted error.
func createError(log *zap.Logger, message string, err error, fields ...zap.Field) error {
	if err == nil {
		log.Error(message, fields...)
		return status.Errorf(codes.Internal, "%s", message)
	}

	log.Error(message, append(fields, zap.Error(err))...)
	return status.Errorf(codes.Internal, "%s: %s", message, err)
}

// withIndexMetadataAndStringMapping adds an index mapping to add metadata that can be used to help identify an index as
// a part of the Grafeas storage backend, and a dynamic template to map all strings to keywords.
func withIndexMetadataAndStringMapping() func(*esapi.IndicesCreateRequest) {
	var indexCreateBuffer bytes.Buffer
	indexCreateBody := map[string]interface{}{
		"mappings": map[string]interface{}{
			"_meta": map[string]string{
				"type": "grafeas",
			},
			"dynamic_templates": []map[string]interface{}{
				{
					"strings": map[string]interface{}{
						"match_mapping_type": "string",
						"mapping": map[string]interface{}{
							"type":  "keyword",
							"norms": false,
						},
					},
				},
			},
		},
	}

	_ = json.NewEncoder(&indexCreateBuffer).Encode(indexCreateBody)

	return esapi.Indices{}.Create.WithBody(&indexCreateBuffer)
}

func decodeResponse(r io.ReadCloser, i interface{}) error {
	return json.NewDecoder(r).Decode(i)
}
