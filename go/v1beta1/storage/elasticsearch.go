package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/uuid"
	"github.com/liatrio/grafeas-elasticsearch/go/config"
	"github.com/liatrio/grafeas-elasticsearch/go/v1beta1/storage/filtering"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"io"

	pb "github.com/grafeas/grafeas/proto/v1beta1/grafeas_go_proto"
	prpb "github.com/grafeas/grafeas/proto/v1beta1/project_go_proto"

	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const apiVersion = "v1beta1"
const indexPrefix = "grafeas-" + apiVersion

type ElasticsearchStorage struct {
	client   *elasticsearch.Client
	config   *config.ElasticsearchConfig
	filterer filtering.Filterer
	logger   *zap.Logger
}

func NewElasticsearchStorage(logger *zap.Logger, client *elasticsearch.Client, filterer filtering.Filterer, config *config.ElasticsearchConfig) *ElasticsearchStorage {
	return &ElasticsearchStorage{
		client,
		config,
		filterer,
		logger,
	}
}

// CreateProject creates a project document within the project index, along with two indices that can be used
// to store notes and occurrences.
// Additional metadata is attached to the newly created indices to help identify them as part of a Grafeas project
func (es *ElasticsearchStorage) CreateProject(ctx context.Context, projectId string, p *prpb.Project) (*prpb.Project, error) {
	projectName := fmt.Sprintf("projects/%s", projectId)
	log := es.logger.Named("CreateProject").With(zap.String("project", projectName))

	// check if project already exists
	search := &esSearch{
		Query: &filtering.Query{
			Term: &filtering.Term{
				"name": projectName,
			},
		},
	}
	err := es.genericGet(ctx, log, search, projectsIndex(), &prpb.Project{})
	if err == nil { // project exists
		log.Debug("project already exists")
		return nil, status.Error(codes.AlreadyExists, fmt.Sprintf("project with name %s already exists", projectName))
	} else if status.Code(err) != codes.NotFound { // unexpected error (we expect a not found error here)
		return nil, err
	}

	p.Name = projectName

	// create project document
	err = es.genericCreate(ctx, log, projectsIndex(), p)
	if err != nil {
		return nil, err
	}

	// create indices for occurrences and notes
	for _, index := range []string{
		occurrencesIndex(projectId),
		notesIndex(projectId),
	} {
		res, err := es.client.Indices.Create(
			index,
			es.client.Indices.Create.WithContext(ctx),
			withIndexMetadataAndStringMapping(),
		)
		if err != nil {
			return nil, createError(log, "error sending request to elasticsearch", err)
		}
		if res.IsError() {
			return nil, createError(log, "error creating index in elasticsearch", err)
		}
	}

	log.Debug("created project")

	return p, nil
}

// GetProject returns the project with the given projectId from Elasticsearch
func (es *ElasticsearchStorage) GetProject(ctx context.Context, projectId string) (*prpb.Project, error) {
	projectName := fmt.Sprintf("projects/%s", projectId)
	log := es.logger.Named("GetProject").With(zap.String("project", projectName))

	search := &esSearch{
		Query: &filtering.Query{
			Term: &filtering.Term{
				"name": projectName,
			},
		},
	}
	project := &prpb.Project{}

	err := es.genericGet(ctx, log, search, projectsIndex(), project)
	if err != nil {
		return nil, err
	}

	return project, nil
}

// ListProjects returns up to pageSize number of projects beginning at pageToken (or from
// start if pageToken is the empty string).
func (es *ElasticsearchStorage) ListProjects(ctx context.Context, filter string, pageSize int, pageToken string) ([]*prpb.Project, string, error) {
	var projects []*prpb.Project
	log := es.logger.Named("ListProjects")

	body := &esSearch{}
	if filter != "" {
		log = log.With(zap.String("filter", filter))
		filterQuery, err := es.filterer.ParseExpression(filter)
		if err != nil {
			return nil, "", createError(log, "error while parsing filter expression", err)
		}

		body.Query = filterQuery
	}

	log.Debug("listing projects")

	encodedBody := encodeRequest(body)
	res, err := es.client.Search(
		es.client.Search.WithContext(ctx),
		es.client.Search.WithIndex(projectsIndex()),
		es.client.Search.WithBody(encodedBody),
	)
	if err != nil {
		return nil, "", createError(log, "error sending request to elasticsearch", err)
	}
	if res.IsError() {
		return nil, "", createError(log, "unexpected response from elasticsearch when listing projects", nil, zap.String("response", res.String()))
	}

	var searchResults esSearchResponse
	if err := decodeResponse(res.Body, &searchResults); err != nil {
		return nil, "", createError(log, "error decoding elasticsearch response", err)
	}

	for _, hit := range searchResults.Hits.Hits {
		hitLogger := log.With(zap.String("project raw", string(hit.Source)))

		project := &prpb.Project{}
		err := protojson.Unmarshal(hit.Source, proto.MessageV2(project))
		if err != nil {
			log.Error("failed to convert _doc to project", zap.Error(err))
			return nil, "", createError(hitLogger, "error converting _doc to project", err)
		}

		hitLogger.Debug("project hit", zap.Any("project", project))

		projects = append(projects, project)
	}

	return projects, "", nil
}

// DeleteProject deletes the project with the given projectId from Elasticsearch
// Note that this will always return a 500 due to a bug in Grafeas
func (es *ElasticsearchStorage) DeleteProject(ctx context.Context, projectId string) error {
	projectName := fmt.Sprintf("projects/%s", projectId)
	log := es.logger.Named("DeleteProject").With(zap.String("project", projectName))
	log.Debug("deleting project")

	search := &esSearch{
		Query: &filtering.Query{
			Term: &filtering.Term{
				"name": projectName,
			},
		},
	}

	err := es.genericDelete(ctx, log, search, projectsIndex())
	if err != nil {
		return err
	}

	log.Debug("project document deleted")

	res, err := es.client.Indices.Delete(
		[]string{
			occurrencesIndex(projectId),
			notesIndex(projectId),
		},
		es.client.Indices.Delete.WithContext(ctx),
	)
	if err != nil || res.IsError() {
		return createError(log, "error deleting elasticsearch indices", err)
	}

	log.Debug("project indices for notes / occurrences deleted")

	return nil
}

// GetOccurrence returns the occurrence with name projects/${projectId}/occurrences/${occurrenceId} from Elasticsearch
func (es *ElasticsearchStorage) GetOccurrence(ctx context.Context, projectId, occurrenceId string) (*pb.Occurrence, error) {
	occurrenceName := fmt.Sprintf("projects/%s/occurrences/%s", projectId, occurrenceId)
	log := es.logger.Named("GetOccurrence").With(zap.String("occurrence", occurrenceName))

	search := &esSearch{
		Query: &filtering.Query{
			Term: &filtering.Term{
				"name": occurrenceName,
			},
		},
	}
	occurrence := &pb.Occurrence{}

	err := es.genericGet(ctx, log, search, occurrencesIndex(projectId), occurrence)
	if err != nil {
		return nil, err
	}

	return occurrence, nil
}

// ListOccurrences returns up to pageSize number of occurrences for this project beginning
// at pageToken, or from start if pageToken is the empty string.
func (es *ElasticsearchStorage) ListOccurrences(ctx context.Context, projectId, filter, pageToken string, pageSize int32) ([]*pb.Occurrence, string, error) {
	projectName := fmt.Sprintf("projects/%s", projectId)
	log := es.logger.Named("ListOccurrences").With(zap.String("project", projectName))

	res, err := es.genericList(ctx, log, occurrencesIndex(projectId), filter)
	if err != nil {
		return nil, "", err
	}

	var occurrences []*pb.Occurrence
	for _, hit := range res.Hits {
		hitLogger := log.With(zap.String("occurrence raw", string(hit.Source)))

		occurrence := &pb.Occurrence{}
		err := protojson.Unmarshal(hit.Source, proto.MessageV2(occurrence))
		if err != nil {
			log.Error("failed to convert _doc to occurrence", zap.Error(err))
			return nil, "", createError(hitLogger, "error converting _doc to occurrence", err)
		}

		hitLogger.Debug("occurrence hit", zap.Any("occurrence", occurrence))

		occurrences = append(occurrences, occurrence)
	}

	return occurrences, "", nil
}

// CreateOccurrence adds the specified occurrence to Elasticsearch
func (es *ElasticsearchStorage) CreateOccurrence(ctx context.Context, projectId, userID string, o *pb.Occurrence) (*pb.Occurrence, error) {
	log := es.logger.Named("CreateOccurrence")

	if o.CreateTime == nil {
		o.CreateTime = ptypes.TimestampNow()
	}
	o.Name = fmt.Sprintf("projects/%s/occurrences/%s", projectId, uuid.New().String())

	err := es.genericCreate(ctx, log, occurrencesIndex(projectId), o)
	if err != nil {
		return nil, err
	}

	return o, nil
}

// BatchCreateOccurrences batch creates the specified occurrences in Elasticsearch.
// This method uses the ES "_bulk" API: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
// This method will return all of the occurrences that were successfully created, and all of the errors that were encountered (if any)
func (es *ElasticsearchStorage) BatchCreateOccurrences(ctx context.Context, projectId string, uID string, occurrences []*pb.Occurrence) ([]*pb.Occurrence, []error) {
	log := es.logger.Named("BatchCreateOccurrences")
	log.Debug("creating occurrences")

	indexMetadata := &esBulkQueryFragment{
		Index: &esBulkQueryIndexFragment{
			Index: occurrencesIndex(projectId),
		},
	}

	metadata, _ := json.Marshal(indexMetadata)
	metadata = append(metadata, "\n"...)

	// build the request body using newline delimited JSON (ndjson)
	// each occurrence is represented by two JSON structures:
	// the first is the metadata that represents the ES operation, in this case "index"
	// the second is the source payload to index
	// in total, this body will consist of (len(occurrences) * 2) JSON structures, separated by newlines, with a trailing newline at the end
	var body bytes.Buffer
	for _, occurrence := range occurrences {
		occurrence.Name = fmt.Sprintf("projects/%s/occurrences/%s", projectId, uuid.New().String())
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
		es.client.Bulk.WithRefresh(es.config.Refresh.String()),
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

	// each indexing operation in this bulk request has its own status
	// we need to iterate over each of the items in the response to know whether or not that particular occurrence was created successfully
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

		createdOccurrences = append(createdOccurrences, occurrence)
	}

	if len(errs) > 0 {
		log.Info("errors while creating occurrences", zap.Any("errors", errs))

		return createdOccurrences, errs
	}

	log.Debug("occurrences created successfully")

	return createdOccurrences, nil
}

// UpdateOccurrence updates the existing occurrence with the given projectId and occurrenceId
func (es *ElasticsearchStorage) UpdateOccurrence(ctx context.Context, projectId, occurrenceId string, o *pb.Occurrence, mask *fieldmaskpb.FieldMask) (*pb.Occurrence, error) {
	return nil, nil
}

// DeleteOccurrence deletes the occurrence with the given projectId and occurrenceId
func (es *ElasticsearchStorage) DeleteOccurrence(ctx context.Context, projectId, occurrenceId string) error {
	occurrenceName := fmt.Sprintf("projects/%s/occurrences/%s", projectId, occurrenceId)
	log := es.logger.Named("DeleteOccurrence").With(zap.String("occurrence", occurrenceName))

	log.Debug("deleting occurrence")

	search := &esSearch{
		Query: &filtering.Query{
			Term: &filtering.Term{
				"name": occurrenceName,
			},
		},
	}

	return es.genericDelete(ctx, log, search, occurrencesIndex(projectId))
}

// GetNote returns the note with project (pID) and note ID (nID)
func (es *ElasticsearchStorage) GetNote(ctx context.Context, projectId, noteId string) (*pb.Note, error) {
	noteName := fmt.Sprintf("projects/%s/notes/%s", projectId, noteId)
	log := es.logger.Named("GetNote").With(zap.String("note", noteName))

	search := &esSearch{
		Query: &filtering.Query{
			Term: &filtering.Term{
				"name": noteName,
			},
		},
	}
	note := &pb.Note{}

	err := es.genericGet(ctx, log, search, notesIndex(projectId), note)
	if err != nil {
		return nil, err
	}

	return note, nil
}

// ListNotes returns up to pageSize number of notes for this project (pID) beginning
// at pageToken (or from start if pageToken is the empty string).
func (es *ElasticsearchStorage) ListNotes(ctx context.Context, projectId, filter, pageToken string, pageSize int32) ([]*pb.Note, string, error) {
	projectName := fmt.Sprintf("projects/%s", projectId)
	log := es.logger.Named("ListNotes").With(zap.String("project", projectName))

	res, err := es.genericList(ctx, log, notesIndex(projectId), filter)
	if err != nil {
		return nil, "", err
	}

	var notes []*pb.Note
	for _, hit := range res.Hits {
		hitLogger := log.With(zap.String("note raw", string(hit.Source)))

		note := &pb.Note{}
		err := protojson.Unmarshal(hit.Source, proto.MessageV2(note))
		if err != nil {
			log.Error("failed to convert _doc to note", zap.Error(err))
			return nil, "", createError(hitLogger, "error converting _doc to note", err)
		}

		hitLogger.Debug("note hit", zap.Any("note", note))

		notes = append(notes, note)
	}

	return notes, "", nil
}

// CreateNote adds the specified note
func (es *ElasticsearchStorage) CreateNote(ctx context.Context, projectId, noteId, uID string, n *pb.Note) (*pb.Note, error) {
	noteName := fmt.Sprintf("projects/%s/notes/%s", projectId, noteId)
	log := es.logger.Named("CreateNote").With(zap.String("note", noteName))

	// since note IDs are provided up front by the client, we need to search ES to see if this note already exists before creating it
	search := &esSearch{
		Query: &filtering.Query{
			Term: &filtering.Term{
				"name": noteName,
			},
		},
	}
	err := es.genericGet(ctx, log, search, notesIndex(projectId), &pb.Note{})
	if err == nil { // note exists
		log.Debug("note already exists")
		return nil, status.Error(codes.AlreadyExists, fmt.Sprintf("note with name %s already exists", noteName))
	} else if status.Code(err) != codes.NotFound { // unexpected error (we expect a not found error here)
		return nil, err
	}

	if n.CreateTime == nil {
		n.CreateTime = ptypes.TimestampNow()
	}
	n.Name = noteName

	err = es.genericCreate(ctx, log, notesIndex(projectId), n)
	if err != nil {
		return nil, err
	}

	return n, nil
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
func (es *ElasticsearchStorage) DeleteNote(ctx context.Context, projectId, noteId string) error {
	noteName := fmt.Sprintf("projects/%s/notes/%s", projectId, noteId)
	log := es.logger.Named("DeleteNote").With(zap.String("note", noteName))

	log.Debug("deleting note")

	search := &esSearch{
		Query: &filtering.Query{
			Term: &filtering.Term{
				"name": noteName,
			},
		},
	}

	return es.genericDelete(ctx, log, search, notesIndex(projectId))
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

func (es *ElasticsearchStorage) genericGet(ctx context.Context, log *zap.Logger, search *esSearch, index string, protoMessage interface{}) error {
	res, err := es.client.Search(
		es.client.Search.WithContext(ctx),
		es.client.Search.WithIndex(index),
		es.client.Search.WithBody(encodeRequest(search)),
	)
	if err != nil {
		return createError(log, "error sending request to elasticsearch", err)
	}
	if res.IsError() {
		return createError(log, "error searching elasticsearch for document", nil, zap.String("response", res.String()), zap.Int("status", res.StatusCode))
	}

	var searchResults esSearchResponse
	if err := decodeResponse(res.Body, &searchResults); err != nil {
		return createError(log, "error unmarshalling elasticsearch response", err)
	}

	if searchResults.Hits.Total.Value == 0 {
		log.Debug("document not found", zap.Any("search", search))
		return status.Error(codes.NotFound, fmt.Sprintf("%T not found", protoMessage))
	}

	return protojson.Unmarshal(searchResults.Hits.Hits[0].Source, proto.MessageV2(protoMessage))
}

func (es *ElasticsearchStorage) genericCreate(ctx context.Context, log *zap.Logger, index string, protoMessage interface{}) error {
	str, err := protojson.Marshal(proto.MessageV2(protoMessage))
	if err != nil {
		return createError(log, fmt.Sprintf("error marshalling %T to json", protoMessage), err)
	}

	res, err := es.client.Index(
		index,
		bytes.NewReader(str),
		es.client.Index.WithContext(ctx),
		es.client.Index.WithRefresh(es.config.Refresh.String()),
	)
	if err != nil {
		return createError(log, "error sending request to elasticsearch", err)
	}

	if res.IsError() {
		return createError(log, "error indexing document in elasticsearch", nil, zap.String("response", res.String()), zap.Int("status", res.StatusCode))
	}

	esResponse := &esIndexDocResponse{}
	err = decodeResponse(res.Body, esResponse)
	if err != nil {
		return createError(log, "error decoding elasticsearch response", err)
	}

	log.Debug("elasticsearch response", zap.Any("response", esResponse))

	return nil
}

func (es *ElasticsearchStorage) genericDelete(ctx context.Context, log *zap.Logger, search *esSearch, index string) error {
	res, err := es.client.DeleteByQuery(
		[]string{index},
		encodeRequest(search),
		es.client.DeleteByQuery.WithContext(ctx),
		es.client.DeleteByQuery.WithRefresh(withRefreshBool(es.config.Refresh)),
	)
	if err != nil {
		return createError(log, "error sending request to elasticsearch", err)
	}
	if res.IsError() {
		return createError(log, "received unexpected response from elasticsearch", nil)
	}

	var deletedResults esDeleteResponse
	if err = decodeResponse(res.Body, &deletedResults); err != nil {
		return createError(log, "error unmarshalling elasticsearch response", err)
	}

	if deletedResults.Deleted == 0 {
		return createError(log, "elasticsearch returned zero deleted documents", nil, zap.Any("response", deletedResults))
	}

	return nil
}

func (es *ElasticsearchStorage) genericList(ctx context.Context, log *zap.Logger, index, filter string) (*esSearchResponseHits, error) {
	body := &esSearch{}
	if filter != "" {
		log = log.With(zap.String("filter", filter))
		filterQuery, err := es.filterer.ParseExpression(filter)
		if err != nil {
			return nil, createError(log, "error while parsing filter expression", err)
		}

		body.Query = filterQuery
	}

	encodedBody := encodeRequest(body)
	res, err := es.client.Search(
		es.client.Search.WithContext(ctx),
		es.client.Search.WithIndex(index),
		es.client.Search.WithBody(encodedBody),
	)
	if err != nil {
		return nil, createError(log, "error sending request to elasticsearch", err)
	}
	if res.IsError() {
		return nil, createError(log, "unexpected response from elasticsearch", nil, zap.String("response", res.String()), zap.Int("status", res.StatusCode))
	}

	var searchResults esSearchResponse
	if err := decodeResponse(res.Body, &searchResults); err != nil {
		return nil, createError(log, "error decoding elasticsearch response", err)
	}

	return searchResults.Hits, nil
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
					"strings_as_keywords": map[string]interface{}{
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

func encodeRequest(body interface{}) io.Reader {
	b, err := json.Marshal(body)
	if err != nil {
		// we should know that `body` is a serializable struct before invoking `encodeRequest`
		panic(err)
	}

	return bytes.NewReader(b)
}

func projectsIndex() string {
	return fmt.Sprintf("%s-projects", indexPrefix)
}

func occurrencesIndex(projectId string) string {
	return fmt.Sprintf("%s-%s-occurrences", indexPrefix, projectId)
}

func notesIndex(projectId string) string {
	return fmt.Sprintf("%s-%s-notes", indexPrefix, projectId)
}

// DeleteByQuery does not support `wait_for` value, although API docs say it is available.
// Immediately refresh on `wait_for` config, assuming that is likely closer to the desired Grafeas user functionality.
// https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-delete-by-query.html#docs-delete-by-query-api-query-params
func withRefreshBool(o config.RefreshOption) bool {
	if o == config.RefreshFalse {
		return false
	}
	return true
}
