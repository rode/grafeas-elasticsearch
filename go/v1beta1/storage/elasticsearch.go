package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/encoding/protojson"
	"io"
	"net/http"
	"strings"

	"github.com/elastic/go-elasticsearch/v7/esapi"

	"github.com/elastic/go-elasticsearch/v7"
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
	str, err := protojson.Marshal(proto.MessageV2(p))
	if err != nil {
		return nil, createError(log, "error marshalling occurrence to json", err)
	}

	// Create new project document
	res, err = es.client.Index(
		projectIndex,
		bytes.NewReader(str),
		es.client.Index.WithContext(ctx),
	)
	if err != nil || res.IsError() {
		return nil, createError(log, "error creating occurrence in elasticsearch", err)
	}

	// Create indices for occurrences and notes
	for _, index := range []string{
		occurrencesIndex(projectID),
		notesIndex(projectID),
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
	err = protojson.Unmarshal(searchResults.Hits.Hits[0].Source, proto.MessageV2(project))
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
	log.Info("deleting project")

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

	log.Info("project document deleted")

	res, err = es.client.Indices.Delete(
		[]string{
			occurrencesIndex(projectID),
			notesIndex(projectID),
		},
		es.client.Indices.Delete.WithContext(ctx),
	)
	if err != nil || res.IsError() {
		return createError(log, "error deleting elasticsearch indices", err)
	}

	log.Info("project indices for notes / occurrences deleted")

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
	protojson.Unmarshal(getDocumentResponse.Hits.Hits[0].Source, proto.MessageV2(occurrence))

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

	str, err := protojson.Marshal(proto.MessageV2(o))
	if err != nil {
		return nil, createError(log, "error marshalling occurrence to json", err)
	}

	res, err := es.client.Index(
		occurrencesIndex(projectID),
		bytes.NewReader(str),
		es.client.Index.WithContext(ctx),
	)
	if err != nil {
		return nil, createError(log, "error creating occurrence in elasticsearch", err)
	}

	if res.IsError() {
		return nil, createError(log, "got unexpected status code from elasticsearch", nil, zap.Int("status", res.StatusCode))
	}

	esResponse := &esIndexDocResponse{}
	err = decodeResponse(res.Body, esResponse)
	if err != nil {
		return nil, createError(log, "error decoding elasticsearch response", err)
	}

	log.Debug("elasticsearch response", zap.Any("response", esResponse))

	o.Name = fmt.Sprintf("projects/%s/occurrences/%s", projectID, esResponse.Id)

	return o, nil
}

// BatchCreateOccurrences batch creates the specified occurrences in Elasticsearch.
// This method uses the ES "_bulk" API: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
func (es *ElasticsearchStorage) BatchCreateOccurrences(ctx context.Context, projectId string, uID string, occurrences []*pb.Occurrence) ([]*pb.Occurrence, []error) {
	log := es.logger.Named("BatchCreateOccurrences")

	indexMetadata := &esBulkQueryFragment{
		Index: &esBulkQueryIndexFragment{
			Index: occurrencesIndex(projectId),
		},
	}

	metadata, err := json.Marshal(indexMetadata)
	if err != nil {
		return nil, []error{
			createError(log, "error marshaling bulk index request metadata", err),
		}
	}
	metadata = append(metadata, "\n"...)

	// build the request body using newline delimited JSON (ndjson)
	// each occurrence is represented by two JSON structures:
	// the first is the metadata that represents the ES operation, in this case "index"
	// the second is the source payload to index
	// in total, this body will consist of (len(occurrences) * 2) JSON structures, separated by newlines, with a trailing newline at the end
	var body bytes.Buffer
	for _, occurrence := range occurrences {
		data, err := protojson.Marshal(proto.MessageV2(occurrence))
		if err != nil {
			return nil, []error{
				createError(log, "error marshaling occurrence", err),
			}
		}

		dataBytes := append(data, "\n"...)
		body.Grow(len(metadata) + len(dataBytes))
		body.Write(metadata)
		body.Write(dataBytes)
	}

	log.Debug("attempting ES bulk index", zap.String("payload", string(body.Bytes())))

	res, err := es.client.Bulk(
		bytes.NewReader(body.Bytes()),
		es.client.Bulk.WithContext(ctx),
	)
	if err != nil {
		return nil, []error{
			createError(log, "failed while sending request to ES", err),
		}
	}
	if res.IsError() {
		return nil, []error{
			createError(log, "unexpected response from ES", nil),
		}
	}

	response := &esBulkResponse{}
	err = decodeResponse(res.Body, response)
	if err != nil {
		return nil, []error{
			createError(log, "error decoding ES response", nil),
		}
	}

	var (
		createdOccurrences []*pb.Occurrence
		errs               []error
	)
	for i, occurrence := range occurrences {
		indexItem := response.Items[i].Index
		if occErr := indexItem.Error; occErr != nil {
			errs = append(errs, createError(log, "error creating occurrence in ES", fmt.Errorf("[%d] %s: %s", indexItem.Status, occErr.Type, occErr.Reason), zap.Any("occurrence", occurrence)))
			continue
		}

		occurrence.Name = fmt.Sprintf("projects/%s/occurrences/%s", projectId, indexItem.Id)
		createdOccurrences = append(createdOccurrences, occurrence)
	}

	if len(errs) > 0 {
		log.Info("errors while creating occurrences", zap.Any("errors", errs))

		return createdOccurrences, errs
	}

	log.Info("occurrences created successfully")

	return createdOccurrences, nil
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

func occurrencesIndex(projectId string) string {
	return fmt.Sprintf("%s-%s-occurrences", indexPrefix, projectId)
}

func notesIndex(projectId string) string {
	return fmt.Sprintf("%s-%s-notes", indexPrefix, projectId)
}
