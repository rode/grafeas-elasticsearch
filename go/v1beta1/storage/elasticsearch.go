package storage

import (
	"context"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net/http"
	"strconv"
	"time"

	"github.com/fernet/fernet-go"
	grafeasConfig "github.com/grafeas/grafeas/go/config"
	"github.com/grafeas/grafeas/go/v1beta1/storage"
	pb "github.com/grafeas/grafeas/proto/v1beta1/grafeas_go_proto"
	prpb "github.com/grafeas/grafeas/proto/v1beta1/project_go_proto"
	"github.com/liatrio/grafeas-elasticsearch/go/config"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

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

// ElasticsearchStorageTypeProvider is...
func (es *ElasticsearchStorage) ElasticsearchStorageTypeProvider(storageType string, storageConfig *grafeasConfig.StorageConfiguration) (*storage.Storage, error) {
	log := es.logger.Named("ElasticsearchStorageTypeProvider")
	log.Info("registering elasticsearch storage")

	if storageType != "elasticsearch" {
		return nil, fmt.Errorf("unknown storage type %s, must be 'elasticsearch'", storageType)
	}

	var storeConfig config.ElasticsearchConfig

	err := grafeasConfig.ConvertGenericConfigToSpecificType(storageConfig, &storeConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create ElasticsearchConfig, %s", err)
	}

	return &storage.Storage{
		Ps: es,
		Gs: es,
	}, nil
}

// CreateProject adds the specified project to the store
func (es *ElasticsearchStorage) CreateProject(ctx context.Context, pID string, p *prpb.Project) (*prpb.Project, error) {
	log := es.logger.Named("CreateProject")
	res, err := es.client.Indices.Create(pID)
	if err != nil {
		log.Error("error creating index", zap.NamedError("error", err))
		return nil, status.Error(codes.Internal, "failed to create index in elasticsearch")
	}

	log.Debug("elasticsearch response", zap.Any("res", res))

	if res.StatusCode != http.StatusOK {
		log.Error("got unexpected status code from elasticsearch", zap.Int("status", res.StatusCode))
		return nil, status.Error(codes.Internal, "unexpected response from elasticsearch when creating index")
	}

	return &prpb.Project{
		Name: pID,
	}, nil
}

// DeleteProject deletes the project with the given pID from the store
func (es *ElasticsearchStorage) DeleteProject(ctx context.Context, pID string) error {
	return nil
}

// GetProject returns the project with the given pID from the store
func (es *ElasticsearchStorage) GetProject(ctx context.Context, pID string) (*prpb.Project, error) {
	return nil, nil
}

// ListProjects returns up to pageSize number of projects beginning at pageToken (or from
// start if pageToken is the empty string).
func (es *ElasticsearchStorage) ListProjects(ctx context.Context, filter string, pageSize int, pageToken string) ([]*prpb.Project, string, error) {
	//id := decryptInt64(pageToken, es.PaginationKey, 0)
	//TODO
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

// DeleteNote deletes the note with the given pID and nID
func (es *ElasticsearchStorage) DeleteNote(ctx context.Context, pID, nID string) error {
	return nil
}

// UpdateNote updates the existing note with the given pID and nID
func (es *ElasticsearchStorage) UpdateNote(ctx context.Context, pID, nID string, n *pb.Note, mask *fieldmaskpb.FieldMask) (*pb.Note, error) {
	return nil, nil
}

// GetNote returns the note with project (pID) and note ID (nID)
func (es *ElasticsearchStorage) GetNote(ctx context.Context, pID, nID string) (*pb.Note, error) {
	return nil, nil
}

// CreateOccurrence adds the specified occurrence
func (es *ElasticsearchStorage) CreateOccurrence(ctx context.Context, pID, uID string, o *pb.Occurrence) (*pb.Occurrence, error) {
	return nil, nil
}

// BatchCreateOccurrences batch creates the specified occurrences in Elasticsearch.
func (es *ElasticsearchStorage) BatchCreateOccurrences(ctx context.Context, pID string, uID string, occs []*pb.Occurrence) ([]*pb.Occurrence, []error) {
	return nil, nil
}

// DeleteOccurrence deletes the occurrence with the given pID and oID
func (es *ElasticsearchStorage) DeleteOccurrence(ctx context.Context, pID, oID string) error {
	return nil
}

// UpdateOccurrence updates the existing occurrence with the given projectID and occurrenceID
func (es *ElasticsearchStorage) UpdateOccurrence(ctx context.Context, pID, oID string, o *pb.Occurrence, mask *fieldmaskpb.FieldMask) (*pb.Occurrence, error) {
	return nil, nil
}

// GetOccurrence returns the occurrence with pID and oID
func (es *ElasticsearchStorage) GetOccurrence(ctx context.Context, pID, oID string) (*pb.Occurrence, error) {
	return nil, nil
}

// ListOccurrences returns up to pageSize number of occurrences for this project beginning
// at pageToken, or from start if pageToken is the empty string.
func (es *ElasticsearchStorage) ListOccurrences(ctx context.Context, pID, filter, pageToken string, pageSize int32) ([]*pb.Occurrence, string, error) {
	return nil, "", nil
}

// GetOccurrenceNote gets the note for the specified occurrence from PostgreSQL.
func (es *ElasticsearchStorage) GetOccurrenceNote(ctx context.Context, pID, oID string) (*pb.Note, error) {
	return nil, nil
}

// ListNotes returns up to pageSize number of notes for this project (pID) beginning
// at pageToken (or from start if pageToken is the empty string).
func (es *ElasticsearchStorage) ListNotes(ctx context.Context, pID, filter, pageToken string, pageSize int32) ([]*pb.Note, string, error) {
	return nil, "", nil
}

// ListNoteOccurrences returns up to pageSize number of occurrences on the particular note (nID)
// for this project (pID) projects beginning at pageToken (or from start if pageToken is the empty string).
func (es *ElasticsearchStorage) ListNoteOccurrences(ctx context.Context, pID, nID, filter, pageToken string, pageSize int32) ([]*pb.Occurrence, string, error) {
	return nil, "", nil
}

// GetVulnerabilityOccurrencesSummary gets a summary of vulnerability occurrences from storage.
func (es *ElasticsearchStorage) GetVulnerabilityOccurrencesSummary(ctx context.Context, projectID, filter string) (*pb.VulnerabilityOccurrencesSummary, error) {
	return &pb.VulnerabilityOccurrencesSummary{}, nil
}

// Encrypt int64 using provided key
func encryptInt64(v int64, key string) (string, error) {
	k, err := fernet.DecodeKey(key)
	if err != nil {
		return "", err
	}
	bytes, err := fernet.EncryptAndSign([]byte(strconv.FormatInt(v, 10)), k)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// Decrypts encrypted int64 using provided key. Returns defaultValue if decryption fails.
func decryptInt64(encrypted string, key string, defaultValue int64) int64 {
	k, err := fernet.DecodeKey(key)
	if err != nil {
		return defaultValue
	}
	bytes := fernet.VerifyAndDecrypt([]byte(encrypted), time.Hour, []*fernet.Key{k})
	if bytes == nil {
		return defaultValue
	}
	decryptedValue, err := strconv.ParseInt(string(bytes), 10, 64)
	if err != nil {
		return defaultValue
	}
	return decryptedValue
}
