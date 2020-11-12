package storage

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"

	elasticsearch "github.com/elastic/go-elasticsearch/v7"
	"github.com/fernet/fernet-go"
	grafeasConfig "github.com/grafeas/grafeas/go/config"
	"github.com/grafeas/grafeas/go/v1beta1/storage"
	pb "github.com/grafeas/grafeas/proto/v1beta1/grafeas_go_proto"
	prpb "github.com/grafeas/grafeas/proto/v1beta1/project_go_proto"
	"github.com/liatrio/grafeas-elasticsearch/go/config"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// Occurrence is...
type Occurrence struct {
	projectName    string
	occurrenceName string
	noteID         string
	data           string
}

// Elasticsearch is...
type Elasticsearch struct {
	*elasticsearch.Client
}

// ElasticsearchStorageTypeProvider is...
func ElasticsearchStorageTypeProvider(storageType string, storageConfig *grafeasConfig.StorageConfiguration) (*storage.Storage, error) {
	if storageType != "elasticsearch" {
		return nil, fmt.Errorf("Unknown storage type %s, must be 'elasticsearch'", storageType)
	}

	var storeConfig config.ElasticsearchConfig

	err := grafeasConfig.ConvertGenericConfigToSpecificType(storageConfig, &storeConfig)
	if err != nil {
		return nil, fmt.Errorf("Unable to create ElasticsearchConfig, %s", err)
	}

	s := NewElasticsearchStore(&storeConfig)

	var r map[string]interface{}
	info, err := s.Client.Info()
	if err != nil {
		log.Fatal(err)
	}
	json.NewDecoder(info.Body).Decode(&r)

	fmt.Printf("ES Server version: %v\n", r["version"].(map[string]interface{})["number"])

	storage := &storage.Storage{
		Ps: s,
		Gs: s,
	}

	return storage, nil
}

// NewElasticsearchStore is...
func NewElasticsearchStore(config *config.ElasticsearchConfig) *Elasticsearch {
	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://localhost:9200",
		},
		Username: "grafeas",
		Password: "grafeas",
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: time.Second,
			DialContext:           (&net.Dialer{Timeout: time.Second}).DialContext,
			TLSClientConfig: &tls.Config{
				MinVersion: tls.VersionTLS11,
			},
		},
	}

	c, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Print("Could not create ES client")
		return nil
	}

	return &Elasticsearch{Client: c}
}

// CreateProject adds the specified project to the store
func (pg *Elasticsearch) CreateProject(ctx context.Context, pID string, p *prpb.Project) (*prpb.Project, error) {
	return nil, nil
}

// DeleteProject deletes the project with the given pID from the store
func (pg *Elasticsearch) DeleteProject(ctx context.Context, pID string) error {
	return nil
}

// GetProject returns the project with the given pID from the store
func (pg *Elasticsearch) GetProject(ctx context.Context, pID string) (*prpb.Project, error) {
	return nil, nil
}

// ListProjects returns up to pageSize number of projects beginning at pageToken (or from
// start if pageToken is the empty string).
func (pg *Elasticsearch) ListProjects(ctx context.Context, filter string, pageSize int, pageToken string) ([]*prpb.Project, string, error) {
	//id := decryptInt64(pageToken, pg.PaginationKey, 0)
	//TODO
	return nil, "", nil
}

// CreateNote adds the specified note
func (pg *Elasticsearch) CreateNote(ctx context.Context, pID, nID, uID string, n *pb.Note) (*pb.Note, error) {
	return nil, nil
}

// BatchCreateNotes batch creates the specified notes in memstore.
func (pg *Elasticsearch) BatchCreateNotes(ctx context.Context, pID, uID string, notes map[string]*pb.Note) ([]*pb.Note, []error) {
	return nil, nil
}

// DeleteNote deletes the note with the given pID and nID
func (pg *Elasticsearch) DeleteNote(ctx context.Context, pID, nID string) error {
	return nil
}

// UpdateNote updates the existing note with the given pID and nID
func (pg *Elasticsearch) UpdateNote(ctx context.Context, pID, nID string, n *pb.Note, mask *fieldmaskpb.FieldMask) (*pb.Note, error) {
	return nil, nil
}

// GetNote returns the note with project (pID) and note ID (nID)
func (pg *Elasticsearch) GetNote(ctx context.Context, pID, nID string) (*pb.Note, error) {
	return nil, nil
}

// CreateOccurrence adds the specified occurrence
func (pg *Elasticsearch) CreateOccurrence(ctx context.Context, pID, uID string, o *pb.Occurrence) (*pb.Occurrence, error) {
	return nil, nil
}

// BatchCreateOccurrences batch creates the specified occurrences in PostreSQL.
func (pg *Elasticsearch) BatchCreateOccurrences(ctx context.Context, pID string, uID string, occs []*pb.Occurrence) ([]*pb.Occurrence, []error) {
	return nil, nil
}

// DeleteOccurrence deletes the occurrence with the given pID and oID
func (pg *Elasticsearch) DeleteOccurrence(ctx context.Context, pID, oID string) error {
	return nil
}

// UpdateOccurrence updates the existing occurrence with the given projectID and occurrenceID
func (pg *Elasticsearch) UpdateOccurrence(ctx context.Context, pID, oID string, o *pb.Occurrence, mask *fieldmaskpb.FieldMask) (*pb.Occurrence, error) {
	return nil, nil
}

// GetOccurrence returns the occurrence with pID and oID
func (pg *Elasticsearch) GetOccurrence(ctx context.Context, pID, oID string) (*pb.Occurrence, error) {
	return nil, nil
}

// ListOccurrences returns up to pageSize number of occurrences for this project beginning
// at pageToken, or from start if pageToken is the empty string.
func (pg *Elasticsearch) ListOccurrences(ctx context.Context, pID, filter, pageToken string, pageSize int32) ([]*pb.Occurrence, string, error) {
	return nil, "", nil
}

// GetOccurrenceNote gets the note for the specified occurrence from PostgreSQL.
func (pg *Elasticsearch) GetOccurrenceNote(ctx context.Context, pID, oID string) (*pb.Note, error) {
	return nil, nil
}

// ListNotes returns up to pageSize number of notes for this project (pID) beginning
// at pageToken (or from start if pageToken is the empty string).
func (pg *Elasticsearch) ListNotes(ctx context.Context, pID, filter, pageToken string, pageSize int32) ([]*pb.Note, string, error) {
	return nil, "", nil
}

// ListNoteOccurrences returns up to pageSize number of occcurrences on the particular note (nID)
// for this project (pID) projects beginning at pageToken (or from start if pageToken is the empty string).
func (pg *Elasticsearch) ListNoteOccurrences(ctx context.Context, pID, nID, filter, pageToken string, pageSize int32) ([]*pb.Occurrence, string, error) {
	return nil, "", nil
}

// GetVulnerabilityOccurrencesSummary gets a summary of vulnerability occurrences from storage.
func (pg *Elasticsearch) GetVulnerabilityOccurrencesSummary(ctx context.Context, projectID, filter string) (*pb.VulnerabilityOccurrencesSummary, error) {
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
