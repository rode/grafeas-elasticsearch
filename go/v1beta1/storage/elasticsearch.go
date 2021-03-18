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

package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/rode/grafeas-elasticsearch/go/v1beta1/storage/migration"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/uuid"
	"github.com/rode/grafeas-elasticsearch/go/config"
	"github.com/rode/grafeas-elasticsearch/go/v1beta1/storage/esutil"
	"github.com/rode/grafeas-elasticsearch/go/v1beta1/storage/filtering"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	pb "github.com/grafeas/grafeas/proto/v1beta1/grafeas_go_proto"
	prpb "github.com/grafeas/grafeas/proto/v1beta1/project_go_proto"

	"github.com/golang/protobuf/protoc-gen-go/generator"
	fieldmask_utils "github.com/mennanov/fieldmask-utils"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const grafeasMaxPageSize = 1000
const sortField = "createTime"

type ElasticsearchStorage struct {
	client                *elasticsearch.Client
	config                *config.ElasticsearchConfig
	filterer              filtering.Filterer
	indexManager          esutil.IndexManager
	logger                *zap.Logger
	migrationOrchestrator migration.Orchestrator
}

func NewElasticsearchStorage(
	logger *zap.Logger,
	client *elasticsearch.Client,
	filterer filtering.Filterer,
	config *config.ElasticsearchConfig,
	indexManager esutil.IndexManager,
	migrationOrchestrator migration.Orchestrator) *ElasticsearchStorage {
	return &ElasticsearchStorage{
		client,
		config,
		filterer,
		indexManager,
		logger,
		migrationOrchestrator,
	}
}

func (es *ElasticsearchStorage) Initialize(ctx context.Context) error {
	const mappingsDir = "mappings"
	if err := es.indexManager.LoadMappings(mappingsDir); err != nil {
		return err
	}

	if err := es.indexManager.CreateIndex(ctx, &esutil.IndexInfo{
		DocumentKind: esutil.ProjectDocumentKind,
		Index:        es.indexManager.ProjectsIndex(),
		Alias:        es.indexManager.ProjectsAlias(),
	}, true); err != nil {
		return err
	}

	return es.migrationOrchestrator.RunMigrations(ctx)
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
	_, err := es.genericGet(ctx, log, search, es.indexManager.ProjectsAlias(), &prpb.Project{})
	if err == nil { // project exists
		log.Debug("project already exists")
		return nil, status.Error(codes.AlreadyExists, fmt.Sprintf("project with name %s already exists", projectName))
	} else if status.Code(err) != codes.NotFound { // unexpected error (we expect a not found error here)
		return nil, err
	}

	p.Name = projectName

	// create project document
	err = es.genericCreate(ctx, log, es.indexManager.ProjectsAlias(), p)
	if err != nil {
		return nil, err
	}

	indicesToCreate := []*esutil.IndexInfo{
		{
			DocumentKind: esutil.OccurrenceDocumentKind,
			Index:        es.indexManager.OccurrencesIndex(projectId),
			Alias:        es.indexManager.OccurrencesAlias(projectId),
		},
		{
			DocumentKind: esutil.NoteDocumentKind,
			Index:        es.indexManager.NotesIndex(projectId),
			Alias:        es.indexManager.NotesAlias(projectId),
		},
	}

	// create indices for occurrences and notes
	for _, indexToCreate := range indicesToCreate {
		if err := es.indexManager.CreateIndex(ctx, indexToCreate, false); err != nil {
			return nil, createError(log, "error creating index", err)
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

	_, err := es.genericGet(ctx, log, search, es.indexManager.ProjectsAlias(), project)
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

	res, err := es.genericList(ctx, log, es.indexManager.ProjectsAlias(), filter, false)
	if err != nil {
		return nil, "", err
	}

	for _, hit := range res.Hits {
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

	err := es.genericDelete(ctx, log, search, es.indexManager.ProjectsAlias())
	if err != nil {
		return err
	}

	log.Debug("project document deleted")

	res, err := es.client.Indices.Delete(
		[]string{
			es.indexManager.OccurrencesIndex(projectId),
			es.indexManager.NotesIndex(projectId),
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

	_, err := es.genericGet(ctx, log, search, es.indexManager.OccurrencesAlias(projectId), occurrence)
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

	res, err := es.genericList(ctx, log, es.indexManager.OccurrencesAlias(projectId), filter, true)
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

	err := es.genericCreate(ctx, log, es.indexManager.OccurrencesAlias(projectId), o)
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
			Index: es.indexManager.OccurrencesAlias(projectId),
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
		if occurrence.CreateTime == nil {
			occurrence.CreateTime = ptypes.TimestampNow()
		}

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
	err = esutil.DecodeResponse(res.Body, response)
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
	occurrenceName := fmt.Sprintf("projects/%s/occurrences/%s", projectId, occurrenceId)
	log := es.logger.Named("Update Occurrence").With(zap.String("occurrence", occurrenceName))

	search := &esSearch{
		Query: &filtering.Query{
			Term: &filtering.Term{
				"name": occurrenceName,
			},
		},
	}

	occurrence := &pb.Occurrence{}

	targetDocumentID, err := es.genericGet(ctx, log, search, es.indexManager.OccurrencesAlias(projectId), occurrence)

	if err != nil {
		return nil, err
	}

	if o.UpdateTime == nil {
		mask.Paths = append(mask.Paths, "UpdateTime")
		o.UpdateTime = ptypes.TimestampNow()
	}

	m, err := fieldmask_utils.MaskFromPaths(mask.Paths, generator.CamelCase)
	if err != nil {
		log.Info("errors while mapping masks", zap.Any("errors", err))
		return occurrence, err
	}
	fieldmask_utils.StructToStruct(m, o, occurrence)

	err = es.genericUpdate(ctx, log, es.indexManager.OccurrencesAlias(projectId), targetDocumentID, occurrence)

	return occurrence, nil
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

	return es.genericDelete(ctx, log, search, es.indexManager.OccurrencesAlias(projectId))
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

	_, err := es.genericGet(ctx, log, search, es.indexManager.NotesAlias(projectId), note)
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

	res, err := es.genericList(ctx, log, es.indexManager.NotesAlias(projectId), filter, true)
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
	_, err := es.genericGet(ctx, log, search, es.indexManager.NotesAlias(projectId), &pb.Note{})
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

	err = es.genericCreate(ctx, log, es.indexManager.NotesAlias(projectId), n)
	if err != nil {
		return nil, err
	}

	return n, nil
}

// BatchCreateNotes batch creates the specified notes in memstore.
func (es *ElasticsearchStorage) BatchCreateNotes(ctx context.Context, projectId, uID string, notesWithNoteIds map[string]*pb.Note) ([]*pb.Note, []error) {
	log := es.logger.Named("BatchCreateNotes").With(zap.String("projectId", projectId))
	log.Debug("creating notes")

	searchMetadata, _ := json.Marshal(&esMultiSearchQueryFragment{
		Index: es.indexManager.NotesAlias(projectId),
	})
	searchMetadata = append(searchMetadata, "\n"...)

	var (
		notes             []*pb.Note
		searchRequestBody bytes.Buffer
	)
	for noteId, note := range notesWithNoteIds {
		note.Name = fmt.Sprintf("projects/%s/notes/%s", projectId, noteId)
		if note.CreateTime == nil {
			note.CreateTime = ptypes.TimestampNow()
		}

		notes = append(notes, note)

		searchBody := &esSearch{
			Query: &filtering.Query{
				Term: &filtering.Term{
					"name": note.Name,
				},
			},
		}
		data, _ := json.Marshal(searchBody)
		dataBytes := append(data, "\n"...)

		searchRequestBody.Grow(len(searchMetadata) + len(dataBytes))
		searchRequestBody.Write(searchMetadata)
		searchRequestBody.Write(dataBytes)
	}

	log.Debug("attempting ES multisearch", zap.String("payload", string(searchRequestBody.Bytes())))

	res, err := es.client.Msearch(
		bytes.NewReader(searchRequestBody.Bytes()),
		es.client.Msearch.WithContext(ctx),
	)

	if err != nil {
		return nil, []error{
			createError(log, "failed while sending request to ES", err),
		}
	}
	if res.IsError() {
		return nil, []error{
			createError(log, "unexpected response from ES", nil, zap.Any("response", res.String()), zap.Int("status", res.StatusCode)),
		}
	}

	searchResponse := &esMultiSearchResponse{}
	err = esutil.DecodeResponse(res.Body, searchResponse)
	if err != nil {
		return nil, []error{
			createError(log, "error decoding ES response", nil),
		}
	}

	var (
		notesToCreate []*pb.Note
		errs          []error
	)
	for i, res := range searchResponse.Responses {
		if res.Hits.Total.Value != 0 {
			errs = append(errs, status.Errorf(codes.AlreadyExists, "note with the name %s already exists", notes[i].Name))
		} else {
			notesToCreate = append(notesToCreate, notes[i])
		}
	}

	if len(notesToCreate) == 0 {
		log.Error("all notes already exist")
		return nil, errs
	}

	indexMetadata, _ := json.Marshal(&esBulkQueryFragment{
		Index: &esBulkQueryIndexFragment{
			Index: es.indexManager.NotesAlias(projectId),
		},
	})
	indexMetadata = append(indexMetadata, "\n"...)

	// build the request body using newline delimited JSON (ndjson)
	// each note is represented by two JSON structures:
	// the first is the metadata that represents the ES operation, in this case "index"
	// the second is the source payload to index
	// in total, this body will consist of (len(notes) * 2) JSON structures, separated by newlines, with a trailing newline at the end
	var indexBody bytes.Buffer
	for _, note := range notesToCreate {
		data, _ := protojson.Marshal(proto.MessageV2(note))

		dataBytes := append(data, "\n"...)
		indexBody.Grow(len(indexMetadata) + len(dataBytes))
		indexBody.Write(indexMetadata)
		indexBody.Write(dataBytes)
	}

	log.Debug("attempting ES bulk index", zap.String("payload", string(indexBody.Bytes())))

	res, err = es.client.Bulk(
		bytes.NewReader(indexBody.Bytes()),
		es.client.Bulk.WithContext(ctx),
		es.client.Bulk.WithRefresh(es.config.Refresh.String()),
	)
	if err != nil {
		return nil, append(errs, createError(log, "failed while sending request to ES", err))
	}
	if res.IsError() {
		return nil, append(errs, createError(log, "unexpected response from ES", nil, zap.Any("response", res.String()), zap.Int("status", res.StatusCode)))
	}

	bulkResponse := &esBulkResponse{}
	err = esutil.DecodeResponse(res.Body, bulkResponse)
	if err != nil {
		return nil, append(errs, createError(log, "error decoding ES response", nil))
	}

	// each indexing operation in this bulk request has its own status
	// we need to iterate over each of the items in the response to know whether or not that particular note was created successfully
	var createdNotes []*pb.Note
	for i, note := range notesToCreate {
		indexItem := bulkResponse.Items[i].Index
		if indexDocError := indexItem.Error; indexDocError != nil {
			errs = append(errs, createError(log, "error creating note in ES", fmt.Errorf("[%d] %s: %s", indexItem.Status, indexDocError.Type, indexDocError.Reason), zap.Any("note", note)))
			continue
		}

		createdNotes = append(createdNotes, note)
		log.Debug(fmt.Sprintf("note %s created", note.Name))
	}

	if len(errs) > 0 {
		log.Info("errors while creating notes", zap.Any("errors", errs))

		return createdNotes, errs
	}

	log.Debug("notes created successfully")

	return createdNotes, nil
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

	return es.genericDelete(ctx, log, search, es.indexManager.NotesAlias(projectId))
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

func (es *ElasticsearchStorage) genericGet(ctx context.Context, log *zap.Logger, search *esSearch, index string, protoMessage interface{}) (string, error) {
	encodedBody, requestJson := esutil.EncodeRequest(search)
	log = log.With(zap.String("request", requestJson))

	res, err := es.client.Search(
		es.client.Search.WithContext(ctx),
		es.client.Search.WithIndex(index),
		es.client.Search.WithBody(encodedBody),
	)
	if err != nil {
		return "", createError(log, "error sending request to elasticsearch", err)
	}
	if res.IsError() {
		return "", createError(log, "error searching elasticsearch for document", nil, zap.String("response", res.String()), zap.Int("status", res.StatusCode))
	}

	var searchResults esSearchResponse
	if err := esutil.DecodeResponse(res.Body, &searchResults); err != nil {
		return "", createError(log, "error unmarshalling elasticsearch response", err)
	}

	if searchResults.Hits.Total.Value == 0 {
		log.Debug("document not found", zap.Any("search", search))
		return "", status.Error(codes.NotFound, fmt.Sprintf("%T not found", protoMessage))
	}

	return searchResults.Hits.Hits[0].ID, protojson.Unmarshal(searchResults.Hits.Hits[0].Source, proto.MessageV2(protoMessage))
}

func (es *ElasticsearchStorage) genericCreate(ctx context.Context, log *zap.Logger, index string, protoMessage interface{}) error {
	str, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(proto.MessageV2(protoMessage))
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
	err = esutil.DecodeResponse(res.Body, esResponse)
	if err != nil {
		return createError(log, "error decoding elasticsearch response", err)
	}

	log.Debug("elasticsearch response", zap.Any("response", esResponse))

	return nil
}

func (es *ElasticsearchStorage) genericUpdate(ctx context.Context, log *zap.Logger, index string, docID string, protoMessage interface{}) error {
	str, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(proto.MessageV2(protoMessage))
	if err != nil {
		return createError(log, fmt.Sprintf("error marshalling %T to json", protoMessage), err)
	}

	res, err := es.client.Index(
		index,
		bytes.NewReader(str),
		es.client.Index.WithDocumentID(docID),
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
	err = esutil.DecodeResponse(res.Body, esResponse)
	if err != nil {
		return createError(log, "error decoding elasticsearch response", err)
	}

	log.Debug("elasticsearch response", zap.Any("response", esResponse))

	return nil
}

func (es *ElasticsearchStorage) genericDelete(ctx context.Context, log *zap.Logger, search *esSearch, index string) error {
	encodedBody, requestJson := esutil.EncodeRequest(search)
	log = log.With(zap.String("request", requestJson))

	res, err := es.client.DeleteByQuery(
		[]string{index},
		encodedBody,
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
	if err = esutil.DecodeResponse(res.Body, &deletedResults); err != nil {
		return createError(log, "error unmarshalling elasticsearch response", err)
	}

	if deletedResults.Deleted == 0 {
		return createError(log, "elasticsearch returned zero deleted documents", nil, zap.Any("response", deletedResults))
	}

	return nil
}

func (es *ElasticsearchStorage) genericList(ctx context.Context, log *zap.Logger, index, filter string, sort bool) (*esSearchResponseHits, error) {
	body := &esSearch{}
	if filter != "" {
		log = log.With(zap.String("filter", filter))
		filterQuery, err := es.filterer.ParseExpression(filter)
		if err != nil {
			return nil, createError(log, "error while parsing filter expression", err)
		}

		body.Query = filterQuery
	}

	if sort {
		body.Sort = map[string]esSortOrder{
			sortField: esSortOrderDecending,
		}
	}

	encodedBody, requestJson := esutil.EncodeRequest(body)
	log = log.With(zap.String("request", requestJson))
	log.Debug("performing search")

	res, err := es.client.Search(
		es.client.Search.WithContext(ctx),
		es.client.Search.WithIndex(index),
		es.client.Search.WithBody(encodedBody),
		es.client.Search.WithSize(grafeasMaxPageSize),
	)
	if err != nil {
		return nil, createError(log, "error sending request to elasticsearch", err)
	}
	if res.IsError() {
		return nil, createError(log, "unexpected response from elasticsearch", nil, zap.String("response", res.String()), zap.Int("status", res.StatusCode))
	}

	var searchResults esSearchResponse
	if err := esutil.DecodeResponse(res.Body, &searchResults); err != nil {
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

// DeleteByQuery does not support `wait_for` value, although API docs say it is available.
// Immediately refresh on `wait_for` config, assuming that is likely closer to the desired Grafeas user functionality.
// https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-delete-by-query.html#docs-delete-by-query-api-query-params
func withRefreshBool(o config.RefreshOption) bool {
	if o == config.RefreshFalse {
		return false
	}
	return true
}
