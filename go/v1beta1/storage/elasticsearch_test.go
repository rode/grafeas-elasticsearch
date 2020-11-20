package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	grafeasConfig "github.com/grafeas/grafeas/go/config"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

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

				assertJsonHasStringValues(transport.receivedHttpRequests[1].Body, map[string]string{
					"mappings._meta.type":           "grafeas",
					"mappings.properties.name.type": "keyword",
				})
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
		//err error
		//expectedProjectId string
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
				{
					StatusCode: http.StatusOK,
				},
			}

			//expectedProjectId = gofakeit.LetterN(10)
		})

		// JustBeforeEach actually invokes the system under test
		JustBeforeEach(func() {
			//_, err = elasticsearchStorage.CreateProject(context.Background(), expectedProjectId, &prpb.Project{})
		})

		It("should check if the project already exists", func() {

		})

		When("the project already exists", func() {
			It("should return an error", func() {

			})

			It("should not create a document for the project", func() {

			})

			It("should not create new indicies", func() {

			})
		})

		When("the project does not exist", func() {
			It("should create a new document for the project", func() {

			})

			It("should create indices for storing occurrences/notes for the project", func() {

			})

			When("creating a new document fails", func() {
				It("should return an error", func() {

				})

				It("should not attempt to create indices", func() {

				})
			})

			When("creating the indices fails", func() {
				It("should return an error", func() {

				})
			})
		})

		When("checking if the project exists returns an error", func() {
			It("should return an error", func() {

			})

			It("should not create a document or indices", func() {

			})
		})
	})

	Context("retrieving a Grafeas occurrence", func() {
		When("elasticsearch successfully returns a occurrence document", func() {
			var (
				objectID           string
				testOccurrence     *pb.Occurrence
				expectedOccurrence *pb.Occurrence
				resultResponse     *esSearchResponse
			)
			BeforeEach(func() {
				objectID = gofakeit.LetterN(8)
				testOccurrence = generateTestOccurrence()
				rawTestOccurrence, _ := json.Marshal(testOccurrence)
				resultResponse = &esSearchResponse{
					Took: 10,
					Hits: esSearchResponseHits{
						Total: struct {
							Value int
						}{1},
						Hits: []esSearchResponseHit{
							{
								Source: rawTestOccurrence,
							},
						},
					},
				}

				resultResponseBody, _ := json.Marshal(resultResponse)
				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: 200,
						Body:       ioutil.NopCloser(bytes.NewReader(resultResponseBody)),
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
				Expect(expectedOccurrence).To(Equal(testOccurrence))
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
						StatusCode: 200,
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
						StatusCode: 404,
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
			newOccurrence      *pb.Occurrence
			expectedOccurrence *pb.Occurrence
		)

		BeforeEach(func() {
			newOccurrence = generateTestOccurrence()
		})

		When("elasticsearch creates a new document", func() {
			var expectedOccurrenceId string

			BeforeEach(func() {
				expectedOccurrenceId = gofakeit.LetterN(10)

				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: 201,
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
				newOccurrences = append(newOccurrences, generateTestOccurrence())
			}
		})

		When("elasticsearch successfully creates new documents", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: 200,
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
						StatusCode: 200,
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

func generateTestOccurrence() (occurrence *pb.Occurrence) {
	return &pb.Occurrence{
		Name: gofakeit.LetterN(10),
		Resource: &grafeas_go_proto.Resource{
			Uri: gofakeit.LetterN(10),
		},
		NoteName:    gofakeit.LetterN(10),
		Kind:        common_go_proto.NoteKind_NOTE_KIND_UNSPECIFIED,
		Remediation: gofakeit.LetterN(10),
		Details:     nil,
	}
}

func formatJson(json string, args ...interface{}) io.ReadCloser {
	return ioutil.NopCloser(strings.NewReader(fmt.Sprintf(json, args...)))
}

func assertJsonHasStringValues(body io.ReadCloser, values map[string]string) {
	requestBody, err := ioutil.ReadAll(body)
	Expect(err).ToNot(HaveOccurred())

	parsed, err := gabs.ParseJSON(requestBody)
	Expect(err).ToNot(HaveOccurred())

	for k, v := range values {
		Expect(parsed.Path(k).Data().(string)).To(BeEquivalentTo(v))
	}
}
