package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/golang/protobuf/jsonpb"
	grafeasConfig "github.com/grafeas/grafeas/go/config"
	prpb "github.com/grafeas/grafeas/proto/v1beta1/project_go_proto"

	"github.com/Jeffail/gabs/v2"
	"github.com/brianvoe/gofakeit/v5"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/grafeas/grafeas/proto/v1beta1/common_go_proto"
	"github.com/grafeas/grafeas/proto/v1beta1/grafeas_go_proto"
	pb "github.com/grafeas/grafeas/proto/v1beta1/grafeas_go_proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("elasticsearch storage", func() {
	var (
		elasticsearchStorage *ElasticsearchStorage
		transport            *mockEsTransport
		projectID            string
		ctx                  context.Context
		err                  error
	)

	BeforeEach(func() {
		transport = &mockEsTransport{}
		mockEsClient := &elasticsearch.Client{Transport: transport, API: esapi.New(transport)}

		projectID = gofakeit.LetterN(8)
		ctx = context.Background()

		elasticsearchStorage = NewElasticsearchStore(mockEsClient, logger)
	})

	Context("creating the elasticsearch storage provider", func() {
		var (
			err                  error
			expectedStorageType  = "elasticsearch"
			expectedProjectIndex = fmt.Sprintf("%s-%s", indexPrefix, "projects")
		)

		// BeforeEach configures the happy path for this context
		// Variables configured here may be overridden in nested BeforeEach blocks
		BeforeEach(func() {
			transport.preparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
				},
				{
					StatusCode: http.StatusOK,
				},
			}
		})

		// JustBeforeEach actually invokes the system under test
		JustBeforeEach(func() {
			randomStorageConfig := grafeasConfig.StorageConfiguration("{}")
			_, err = elasticsearchStorage.ElasticsearchStorageTypeProvider(expectedStorageType, &randomStorageConfig)
		})

		It("should check if an index for projects has already been created", func() {
			Expect(transport.receivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s", expectedProjectIndex)))
			Expect(transport.receivedHttpRequests[0].Method).To(Equal(http.MethodHead))
			Expect(err).ToNot(HaveOccurred())
		})

		When("an index for projects does not exist", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses[0].StatusCode = http.StatusNotFound
			})

			It("should create the index for projects", func() {
				Expect(transport.receivedHttpRequests).To(HaveLen(2))
				Expect(transport.receivedHttpRequests[1].URL.Path).To(Equal(fmt.Sprintf("/%s", expectedProjectIndex)))
				Expect(transport.receivedHttpRequests[1].Method).To(Equal(http.MethodPut))

				assertIndexCreateBodyHasMetadataAndStringMapping(transport.receivedHttpRequests[1].Body)
			})

			When("creating the index for projects fails", func() {
				BeforeEach(func() {
					transport.preparedHttpResponses[1].StatusCode = http.StatusInternalServerError
				})

				It("should return an error", func() {
					Expect(err).To(HaveOccurred())
				})
			})
		})

		When("an index for projects already exists", func() {
			It("should not create an index for projects", func() {
				Expect(transport.receivedHttpRequests).To(HaveLen(1))
			})
		})

		When("checking for the existence of a project index fails", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses[0].StatusCode = http.StatusInternalServerError
			})

			It("should return an error", func() {
				Expect(err).To(HaveOccurred())
			})
		})
	})

	Context("creating a new Grafeas project", func() {
		var (
			createProjectErr     error
			expectedProjectId    string
			expectedProjectIndex string
			expectedProject      *prpb.Project
		)

		// BeforeEach configures the happy path for this context
		// Variables configured here may be overridden in nested BeforeEach blocks
		BeforeEach(func() {
			transport.preparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body:       createEsSearchResponse("project"),
				},
				{
					StatusCode: http.StatusOK,
				},
				{
					StatusCode: http.StatusOK,
				},
				{
					StatusCode: http.StatusOK,
				},
			}
			expectedProjectId = gofakeit.LetterN(10)
			expectedProjectIndex = fmt.Sprintf("%s-%s", indexPrefix, "projects")
		})

		// JustBeforeEach actually invokes the system under test
		JustBeforeEach(func() {
			expectedProject, createProjectErr = elasticsearchStorage.CreateProject(context.Background(), expectedProjectId, &prpb.Project{})
		})

		It("should check if the project document already exists", func() {
			Expect(transport.receivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_search", expectedProjectIndex)))
			Expect(transport.receivedHttpRequests[0].Method).To(Equal(http.MethodGet))

			assertJsonHasValues(transport.receivedHttpRequests[0].Body, map[string]interface{}{
				"query.term.name": fmt.Sprintf("projects/%s", expectedProjectId),
			})
		})

		When("the project already exists", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses[0] = &http.Response{
					StatusCode: http.StatusOK,
					Body:       createEsSearchResponse("project", gofakeit.LetterN(10)),
				}
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(createProjectErr, codes.AlreadyExists)
				Expect(expectedProject).To(BeNil())
			})

			It("should not create any documents or indices for the project", func() {
				Expect(transport.receivedHttpRequests).To(HaveLen(1))
			})
		})

		When("checking if the project exists returns an error", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses[0] = &http.Response{StatusCode: http.StatusBadRequest}
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(createProjectErr, codes.Internal)
				Expect(expectedProject).To(BeNil())
			})

			It("should not create a document or indices", func() {
				Expect(transport.receivedHttpRequests).To(HaveLen(1))
			})
		})

		When("the project does not exist", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses[1] = &http.Response{StatusCode: http.StatusCreated}
			})

			It("should create a new document for the project", func() {
				Expect(transport.receivedHttpRequests[1].URL.Path).To(Equal(fmt.Sprintf("/%s/_doc", expectedProjectIndex)))
				Expect(transport.receivedHttpRequests[1].Method).To(Equal(http.MethodPost))

				projectBody := &prpb.Project{}
				err := jsonpb.Unmarshal(transport.receivedHttpRequests[1].Body, projectBody)
				Expect(err).ToNot(HaveOccurred())

				Expect(projectBody.Name).To(Equal(fmt.Sprintf("projects/%s", expectedProjectId)))
			})

			It("should create indices for storing occurrences/notes for the project", func() {
				Expect(transport.receivedHttpRequests[2].URL.Path).To(Equal(fmt.Sprintf("/%s-%s", indexPrefix, "occurrences")))
				Expect(transport.receivedHttpRequests[2].Method).To(Equal(http.MethodPut))
				assertIndexCreateBodyHasMetadataAndStringMapping(transport.receivedHttpRequests[2].Body)

				Expect(transport.receivedHttpRequests[3].URL.Path).To(Equal(fmt.Sprintf("/%s-%s", indexPrefix, "notes")))
				Expect(transport.receivedHttpRequests[3].Method).To(Equal(http.MethodPut))
				assertIndexCreateBodyHasMetadataAndStringMapping(transport.receivedHttpRequests[3].Body)
			})

			It("should return the project", func() {
				Expect(expectedProject).ToNot(BeNil())
				Expect(expectedProject.Name).To(Equal(fmt.Sprintf("projects/%s", expectedProjectId)))
			})

			When("creating a new document fails", func() {
				BeforeEach(func() {
					transport.preparedHttpResponses[1] = &http.Response{StatusCode: http.StatusBadRequest}
				})

				It("should return an error", func() {
					assertErrorHasGrpcStatusCode(createProjectErr, codes.Internal)
					Expect(expectedProject).To(BeNil())
				})

				It("should not attempt to create indices", func() {
					Expect(transport.receivedHttpRequests).To(HaveLen(2))
				})
			})

			When("creating the indices fails", func() {
				BeforeEach(func() {
					transport.preparedHttpResponses[2] = &http.Response{StatusCode: http.StatusBadRequest}
				})

				It("should return an error", func() {
					assertErrorHasGrpcStatusCode(createProjectErr, codes.Internal)
					Expect(expectedProject).To(BeNil())
				})
			})
		})
	})

	Context("retrieving a Grafeas project", func() {
		var (
			getProjectErr        error
			expectedProjectID    string
			expectedProjectIndex string
			expectedProject      *prpb.Project
		)

		BeforeEach(func() {
			expectedProjectID = gofakeit.LetterN(10)
			expectedProjectIndex = fmt.Sprintf("%s-%s", indexPrefix, "projects")
			transport.preparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body: createGenericEsSearchResponse(&prpb.Project{
						Name: fmt.Sprintf("projects/%s", expectedProjectID),
					}),
				},
			}
		})

		JustBeforeEach(func() {
			expectedProject, getProjectErr = elasticsearchStorage.GetProject(ctx, expectedProjectID)
		})

		It("should query Grafeas for the specified project", func() {
			Expect(transport.receivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_search", expectedProjectIndex)))
			Expect(transport.receivedHttpRequests[0].Method).To(Equal(http.MethodGet))

			assertJsonHasValues(transport.receivedHttpRequests[0].Body, map[string]interface{}{
				"query.term.name": fmt.Sprintf("projects/%s", expectedProjectID),
			})
		})

		When("elasticsearch successfully returns a project document", func() {
			It("should return the Grafeas project", func() {
				Expect(expectedProject).ToNot(BeNil())
				Expect(expectedProject.Name).To(Equal(fmt.Sprintf("projects/%s", expectedProjectID)))
			})

			It("should return without an error", func() {
				Expect(getProjectErr).ToNot(HaveOccurred())
			})
		})

		When("elasticsearch can not find the specified project document", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusOK,
						Body:       createEsSearchResponse("project"),
					},
				}
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(getProjectErr, codes.NotFound)
			})
		})

		When("elasticsearch returns a bad object", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusOK,
						Body:       ioutil.NopCloser(strings.NewReader("bad object")),
					},
				}
			})

			It("should return an error", func() {
				Expect(getProjectErr).To(HaveOccurred())
			})
		})

		When("returns an unexpected response", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusBadRequest,
					},
				}
			})

			It("should return an error", func() {
				Expect(getProjectErr).To(HaveOccurred())
			})
		})
	})

	Context("deleting a Grafeas project", func() {
		var (
			deleteProjectErr     error
			expectedProjectIndex string
		)

		BeforeEach(func() {
			expectedProjectIndex = fmt.Sprintf("%s-%s", indexPrefix, "projects")
		})

		JustBeforeEach(func() {
			err = elasticsearchStorage.DeleteProject(ctx, projectID)
		})

		When("elasticsearch successfully deletes the document", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusOK,
						Body:       formatJson(`{"deleted": 1}`),
					},
				}
			})

			It("should have sent the correct HTTP request", func() {
				Expect(transport.receivedHttpRequests[0].Method).To(Equal("POST"))
				Expect(transport.receivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/%s", expectedProjectIndex, "_delete_by_query")))

				assertJsonHasValues(transport.receivedHttpRequests[0].Body, map[string]interface{}{
					"query.term.name": fmt.Sprintf("projects/%s", projectID),
				})
			})
		})

		When("project does not exist", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusOK,
						Body:       formatJson(`{"deleted": 0}`),
					},
				}
			})

			It("should return an error", func() {
				Expect(deleteProjectErr).ToNot(HaveOccurred())
			})
		})
	})

	Context("retrieving a Grafeas occurrence", func() {
		When("elasticsearch successfully returns a occurrence document", func() {
			var (
				objectID           string
				expectedOccurrence *pb.Occurrence
			)
			BeforeEach(func() {
				objectID = gofakeit.LetterN(8)

				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusOK,
						Body:       createEsSearchResponse("occurrence", objectID),
					},
				}

				expectedOccurrence, err = elasticsearchStorage.GetOccurrence(ctx, projectID, objectID)
				Expect(err).ToNot(HaveOccurred())
			})

			It("should have sent the correct HTTP request", func() {
				Expect(transport.receivedHttpRequests[0].Method).To(Equal("GET"))
				Expect(transport.receivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_search", projectID)))

				requestBody, err := ioutil.ReadAll(transport.receivedHttpRequests[0].Body)
				Expect(err).ToNot(HaveOccurred())

				parsed, err := gabs.ParseJSON(requestBody)
				Expect(err).ToNot(HaveOccurred())

				Expect(parsed.Path("query.match.name").Data().(string)).To(BeEquivalentTo(fmt.Sprintf("projects/%s/occurrences/%s", projectID, objectID)))
			})

			It("should return a Grafeas occurrence", func() {
				Expect(expectedOccurrence.Name).To(Equal(objectID))
			})
		})

		When("elasticsearch can not find elasticsearch document", func() {
			var (
				objectID string
			)
			BeforeEach(func() {
				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusInternalServerError,
					},
				}
				_, err = elasticsearchStorage.GetOccurrence(ctx, projectID, objectID)
			})

			It("should return an error", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		When("elasticsearch returns a bad object", func() {
			var (
				objectID string
			)
			BeforeEach(func() {
				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusOK,
						Body:       ioutil.NopCloser(strings.NewReader("bad object")),
					},
				}

				_, err = elasticsearchStorage.GetOccurrence(ctx, projectID, objectID)
			})

			It("should fail to decode response and return an error", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		When("elasticsearch returns a unexpected response", func() {
			var (
				objectID string
			)
			BeforeEach(func() {
				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusNotFound,
					},
				}

				_, err = elasticsearchStorage.GetOccurrence(ctx, projectID, objectID)
			})

			It("should return an error", func() {
				Expect(err).To(HaveOccurred())
			})
		})
	})

	Context("creating a new Grafeas occurrence", func() {
		var (
			newOccurrence        *pb.Occurrence
			expectedOccurrence   *pb.Occurrence
			expectedOccurrenceId string
		)

		BeforeEach(func() {
			expectedOccurrenceId = gofakeit.LetterN(10)
			newOccurrence = generateTestOccurrence(expectedOccurrenceId)
		})

		When("elasticsearch creates a new document", func() {
			BeforeEach(func() {
				expectedOccurrenceId = gofakeit.LetterN(10)

				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusCreated,
						Body: formatJson(`{
							"_id": "%s"
						}`, expectedOccurrenceId),
					},
				}

				expectedOccurrence, err = elasticsearchStorage.CreateOccurrence(ctx, projectID, "", newOccurrence)
				Expect(err).ToNot(HaveOccurred())
			})

			It("should have sent the correct HTTP request", func() {
				Expect(transport.receivedHttpRequests[0].Method).To(Equal("POST"))
				Expect(transport.receivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_doc", projectID)))
			})

			It("should return a Grafeas occurrence with the correct name", func() {
				Expect(expectedOccurrence).To(Equal(newOccurrence))
				Expect(expectedOccurrence.Name).To(Equal(fmt.Sprintf("projects/%s/occurrences/%s", projectID, expectedOccurrenceId)))
			})
		})

		When("elasticsearch fails to create a new document", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusInternalServerError,
					},
				}
				_, err = elasticsearchStorage.CreateOccurrence(ctx, projectID, "", newOccurrence)
			})

			It("should return an error", func() {
				Expect(err).To(HaveOccurred())
			})
		})
	})

	Context("creating a batch of Grafeas occurrences", func() {
		var (
			err                 []error
			newOccurrences      []*pb.Occurrence
			expectedOccurrences []*pb.Occurrence
		)

		BeforeEach(func() {
			for i := 1; i <= gofakeit.Number(2, 20); i++ {
				newOccurrences = append(newOccurrences, generateTestOccurrence(gofakeit.LetterN(10)))
			}
		})

		When("elasticsearch successfully creates new documents", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusOK,
					},
				}

				expectedOccurrences, err = elasticsearchStorage.BatchCreateOccurrences(ctx, projectID, "", newOccurrences)
			})

			It("should have sent the correct HTTP request", func() {
				Expect(transport.receivedHttpRequests[0].Method).To(Equal("POST"))
				Expect(transport.receivedHttpRequests[0].URL.Path).To(Equal("/_bulk"))
			})

			It("should return a Grafeas occurrence", func() {
				Expect(expectedOccurrences).To(Equal(newOccurrences))
			})

			It("should return without an error", func() {
				Expect(err).To(BeEmpty())
			})
		})

		When("elasticsearch fails to create new documents", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusInternalServerError,
					},
				}

				expectedOccurrences, err = elasticsearchStorage.BatchCreateOccurrences(ctx, projectID, "", newOccurrences)
			})

			It("should return an error", func() {
				Expect(err).ToNot(BeEmpty())
			})
		})
	})

	Context("deleting a Grafeas occurrence", func() {
		var (
			objectID string
			err      error
		)

		BeforeEach(func() {
			objectID = gofakeit.LetterN(8)
		})

		When("elasticsearch successfully deletes the document", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusOK,
					},
				}

				err = elasticsearchStorage.DeleteOccurrence(ctx, projectID, "")
				Expect(err).ToNot(HaveOccurred())
			})

			It("should have sent the correct HTTP request", func() {
				Expect(transport.receivedHttpRequests[0].Method).To(Equal("POST"))
				Expect(transport.receivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/%s", projectID, "_delete_by_query")))
			})

		})

		When("elasticsearch fails to delete documents", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusInternalServerError,
					},
				}

				err = elasticsearchStorage.DeleteOccurrence(ctx, projectID, objectID)
			})

			It("should return an error", func() {
				Expect(err).To(HaveOccurred())
			})
		})
	})

})

func createGenericEsSearchResponse(messages ...proto.Message) io.ReadCloser {
	var hits []*esSearchResponseHit
	marshaller := &jsonpb.Marshaler{}

	for _, m := range messages {
		raw, err := marshaller.MarshalToString(m)
		Expect(err).ToNot(HaveOccurred())

		hits = append(hits, &esSearchResponseHit{
			Source: []byte(raw),
		})
	}

	response := &esSearchResponse{
		Took: gofakeit.Number(1, 10),
		Hits: &esSearchResponseHits{
			Total: &esSearchResponseTotal{
				Value: len(hits),
			},
			Hits: hits,
		},
	}
	responseBody, err := json.Marshal(response)
	Expect(err).ToNot(HaveOccurred())

	return ioutil.NopCloser(bytes.NewReader(responseBody))
}

func createEsSearchResponse(objectType string, hitNames ...string) io.ReadCloser {
	var occurrenceHits []*esSearchResponseHit

	for _, hit := range hitNames {
		switch objectType {
		case "project":
			rawGrafeasObject, err := json.Marshal(generateTestProject(hit))
			Expect(err).ToNot(HaveOccurred())
			occurrenceHits = append(occurrenceHits, &esSearchResponseHit{
				Source: rawGrafeasObject,
			})
		case "occurrence":
			rawGrafeasObject, err := json.Marshal(generateTestOccurrence(hit))
			Expect(err).ToNot(HaveOccurred())
			occurrenceHits = append(occurrenceHits, &esSearchResponseHit{
				Source: rawGrafeasObject,
			})
		case "note":
			rawGrafeasObject, err := json.Marshal(generateTestNote(hit))
			Expect(err).ToNot(HaveOccurred())
			occurrenceHits = append(occurrenceHits, &esSearchResponseHit{
				Source: rawGrafeasObject,
			})
		}
	}

	response := &esSearchResponse{
		Took: gofakeit.Number(1, 10),
		Hits: &esSearchResponseHits{
			Total: &esSearchResponseTotal{
				Value: len(hitNames),
			},
			Hits: occurrenceHits,
		},
	}
	responseBody, err := json.Marshal(response)
	Expect(err).ToNot(HaveOccurred())

	return ioutil.NopCloser(bytes.NewReader(responseBody))
}

func generateTestProject(name string) (occurrence *prpb.Project) {
	return &prpb.Project{
		Name: fmt.Sprintf("projects/%s", name),
	}
}

func generateTestOccurrence(name string) (occurrence *pb.Occurrence) {
	return &pb.Occurrence{
		Name: name,
		Resource: &grafeas_go_proto.Resource{
			Uri: gofakeit.LetterN(10),
		},
		NoteName:    gofakeit.LetterN(10),
		Kind:        common_go_proto.NoteKind_NOTE_KIND_UNSPECIFIED,
		Remediation: gofakeit.LetterN(10),
		Details:     nil,
	}
}

func generateTestNote(name string) (occurrence *pb.Note) {
	return &pb.Note{
		Name: name,
		Kind: common_go_proto.NoteKind_NOTE_KIND_UNSPECIFIED,
	}
}

func formatJson(json string, args ...interface{}) io.ReadCloser {
	return ioutil.NopCloser(strings.NewReader(fmt.Sprintf(json, args...)))
}

func assertJsonHasValues(body io.ReadCloser, values map[string]interface{}) {
	requestBody, err := ioutil.ReadAll(body)
	Expect(err).ToNot(HaveOccurred())

	parsed, err := gabs.ParseJSON(requestBody)
	Expect(err).ToNot(HaveOccurred())

	for k, v := range values {
		Expect(parsed.ExistsP(k)).To(BeTrue(), "expected jsonpath %s to exist", k)

		switch v.(type) {
		case string:
			Expect(parsed.Path(k).Data().(string)).To(Equal(v.(string)))
		case bool:
			Expect(parsed.Path(k).Data().(bool)).To(Equal(v.(bool)))
		case int:
			Expect(parsed.Path(k).Data().(int)).To(Equal(v.(int)))
		default:
			Fail("assertJsonHasValues encountered unexpected type")
		}
	}
}

func assertIndexCreateBodyHasMetadataAndStringMapping(body io.ReadCloser) {
	assertJsonHasValues(body, map[string]interface{}{
		"mappings._meta.type": "grafeas",
		"mappings.dynamic_templates.strings.match_mapping_type": "string",
		"mappings.dynamic_templates.strings.mapping.type":       "keyword",
		"mappings.dynamic_templates.strings.mapping.norms":      false,
	})
}

func assertErrorHasGrpcStatusCode(err error, code codes.Code) {
	Expect(err).To(HaveOccurred())
	s, ok := status.FromError(err)

	Expect(ok).To(BeTrue(), "expected error to have been produced from the grpc/status package")
	Expect(s.Code()).To(Equal(code))
}
