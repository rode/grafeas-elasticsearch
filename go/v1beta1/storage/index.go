package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/elastic/go-elasticsearch/v7"
	"go.uber.org/zap"
)

const (
	projectDocumentKind    = "projects"
	occurrenceDocumentKind = "occurrences"
	noteDocumentKind       = "notes"
	metadataDocumentKind   = "metadata"
)

type IndexManager interface {
	LoadMappings(mappingsDir string) error
	CreateIndex(ctx context.Context, info *IndexInfo, checkExists bool) error

	ProjectsIndex() string
	ProjectsAlias() string

	OccurrencesIndex(projectId string) string
	OccurrencesAlias(projectId string) string

	NotesIndex(projectId string) string
	NotesAlias(projectId string) string

	IncrementIndexVersion(indexName string) string
	GetLatestVersionForDocumentKind(documentKind string) string
}

type VersionedMapping struct {
	Version  string                 `json:"version"`
	Mappings map[string]interface{} `json:"mappings"`
}

type IndexInfo struct {
	Index        string
	Alias        string
	DocumentKind string
}

type IndexNameParts struct {
	DocumentKind string
	Version      string
	ProjectId    string
}

type EsIndexManager struct {
	logger            *zap.Logger
	client            *elasticsearch.Client
	projectMapping    *VersionedMapping
	occurrenceMapping *VersionedMapping
	noteMapping       *VersionedMapping
	metadataMapping   *VersionedMapping
}

func NewIndexManager(logger *zap.Logger, client *elasticsearch.Client) IndexManager {
	return &EsIndexManager{
		client: client,
		logger: logger,
	}
}

func (em *EsIndexManager) LoadMappings(mappingsDir string) error {
	if err := filepath.Walk(mappingsDir, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		localPath := strings.TrimPrefix(filePath, mappingsDir+"/")
		fileName := path.Base(localPath)
		documentKind := strings.TrimSuffix(fileName, filepath.Ext(fileName))
		versionedMappingJson, err := ioutil.ReadFile(filePath)
		if err != nil {
			return err
		}
		var mapping VersionedMapping

		if err := json.Unmarshal(versionedMappingJson, &mapping); err != nil {
			return err
		}

		switch documentKind {
		case projectDocumentKind:
			em.projectMapping = &mapping
		case occurrenceDocumentKind:
			em.occurrenceMapping = &mapping
		case noteDocumentKind:
			em.noteMapping = &mapping
		case metadataDocumentKind:
			em.metadataMapping = &mapping
		default:
			em.logger.Info("Unrecognized document kind mapping", zap.String("kind", documentKind))
			return nil
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (em *EsIndexManager) CreateIndex(ctx context.Context, info *IndexInfo, checkExists bool) error {
	log := em.logger.Named("CreateIndex").With(zap.String("index", info.Index))

	if checkExists {
		res, err := em.client.Indices.Exists([]string{info.Index}, em.client.Indices.Exists.WithContext(ctx))
		if err != nil || (res.StatusCode != http.StatusOK && res.StatusCode != http.StatusNotFound) {
			return createError(log, fmt.Sprintf("error checking if %s index already exists", info.Index), err)
		}

		if !res.IsError() {
			return nil
		}
	}

	mapping, err := em.getMappingForDocumentKind(info.DocumentKind)
	if err != nil {
		return err
	}

	createIndexReq := map[string]interface{}{
		"mappings": mapping.Mappings,
	}

	if info.Alias != "" {
		createIndexReq["aliases"] = map[string]interface{}{
			info.Alias: map[string]interface{}{},
		}
	}

	payload, _ := encodeRequest(&createIndexReq)
	res, err := em.client.Indices.Create(info.Index, em.client.Indices.Create.WithContext(ctx), em.client.Indices.Create.WithBody(payload))
	if err := getErrorFromESResponse(res, err); err != nil {
		return err
	}

	log.Info("index created", zap.String("index", info.Index))

	return nil
}

func (em *EsIndexManager) getMappingForDocumentKind(documentKind string) (*VersionedMapping, error) {
	switch documentKind {
	case projectDocumentKind:
		return em.projectMapping, nil
	case occurrenceDocumentKind:
		return em.occurrenceMapping, nil
	case noteDocumentKind:
		return em.noteMapping, nil
	case metadataDocumentKind:
		return em.metadataMapping, nil
	default:
		em.logger.Info("Unrecognized document kind mapping", zap.String("kind", documentKind))
		return nil, fmt.Errorf("no mapping found for document kind %s", documentKind)
	}
}

func (em *EsIndexManager) ProjectsIndex() string {
	indexVersion := em.projectMapping.Version
	return fmt.Sprintf("%s-%s-projects", indexPrefix, indexVersion)
}

func (em *EsIndexManager) ProjectsAlias() string {
	return fmt.Sprintf("%s-projects", aliasPrefix)
}

func (em *EsIndexManager) OccurrencesIndex(projectId string) string {
	indexVersion := em.occurrenceMapping.Version
	return fmt.Sprintf("%s-%s-%s-occurrences", indexPrefix, indexVersion, projectId)
}

func (em *EsIndexManager) OccurrencesAlias(projectId string) string {
	return fmt.Sprintf("%s-%s-occurrences", aliasPrefix, projectId)
}

func (em *EsIndexManager) NotesIndex(projectId string) string {
	indexVersion := em.noteMapping.Version
	return fmt.Sprintf("%s-%s-%s-notes", indexPrefix, indexVersion, projectId)
}

func (em *EsIndexManager) NotesAlias(projectId string) string {
	return fmt.Sprintf("%s-%s-notes", aliasPrefix, projectId)
}

func (em *EsIndexManager) IncrementIndexVersion(indexName string) string {
	indexParts := parseIndexName(indexName)

	switch indexParts.DocumentKind {
	case noteDocumentKind:
		return em.NotesIndex(indexParts.ProjectId)
	case occurrenceDocumentKind:
		return em.OccurrencesIndex(indexParts.ProjectId)
	}

	// unversioned index
	return indexName
}

func (em *EsIndexManager) GetLatestVersionForDocumentKind(documentKind string) string {
	switch documentKind {
	case noteDocumentKind:
		return em.noteMapping.Version
	case occurrenceDocumentKind:
		return em.occurrenceMapping.Version
	case projectDocumentKind:
		return em.projectMapping.Version
	}

	return ""
}

func parseIndexName(indexName string) *IndexNameParts {
	indexParts := strings.Split(indexName, "-")
	documentKind := indexParts[len(indexParts)-1]
	nameParts := &IndexNameParts{
		DocumentKind: documentKind,
	}

	switch documentKind {
	case metadataIndexName:
		// nothing to do
	case projectDocumentKind:
		nameParts.Version = indexParts[1]
	case noteDocumentKind,
		occurrenceDocumentKind:
		nameParts.Version = indexParts[1]
		nameParts.ProjectId = strings.Join(indexParts[2:len(indexParts)-1], "-")
	}

	return nameParts
}
