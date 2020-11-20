package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	prpb "github.com/grafeas/grafeas/proto/v1beta1/project_go_proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("elasticsearch storage", func() {
	var (
		elasticsearchStorage *ElasticsearchStorage
		transport            *mockEsTransport
		projectID            string
		ctx                  context.Context
		project              *prpb.Project
		expectedProject      *prpb.Project
		err                  error
	)

	BeforeEach(func() {
		transport = &mockEsTransport{}
		transport.expectedError = nil
		mockEsClient := &elasticsearch.Client{Transport: transport, API: esapi.New(transport)}

		projectID = gofakeit.LetterN(8)
		ctx = context.Background()
		project = &prpb.Project{Name: fmt.Sprintf("projects/%s", projectID)}

		elasticsearchStorage = NewElasticsearchStore(mockEsClient, logger)
	})

	Context("creating a new Grafeas project", func() {
		When("elasticsearch successfully creates a new index", func() {
			BeforeEach(func() {
				transport.preparedHttpResponse = &http.Response{
					StatusCode: 200,
				}

				expectedProject, err = elasticsearchStorage.CreateProject(ctx, projectID, project)
				Expect(err).ToNot(HaveOccurred())
			})

			It("should have sent the correct HTTP request", func() {
				Expect(transport.receivedHttpRequest.Method).To(Equal("PUT"))
				Expect(transport.receivedHttpRequest.URL.Path).To(Equal(fmt.Sprintf("/%s", projectID)))

				requestBody, err := ioutil.ReadAll(transport.receivedHttpRequest.Body)
				Expect(err).ToNot(HaveOccurred())

				parsed, err := gabs.ParseJSON(requestBody)
				Expect(err).ToNot(HaveOccurred())

				Expect(parsed.Path("mappings._meta.type").Data().(string)).To(BeEquivalentTo("grafeas-project"))
			})

			It("should return a new Grafeas project", func() {
				Expect(expectedProject.Name).To(Equal(fmt.Sprintf("projects/%s", projectID)))
			})
		})

		When("elasticsearch unsuccessfully creates a new index", func() {
			BeforeEach(func() {
				transport.expectedError = errors.New("failed to create new index")
				_, err = elasticsearchStorage.CreateProject(ctx, projectID, project)
			})

			It("should return an error", func() {
				Expect(err).To(HaveOccurred())
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
				transport.preparedHttpResponse = &http.Response{
					StatusCode: 200,
					Body:       ioutil.NopCloser(bytes.NewReader(resultResponseBody)),
				}

				expectedOccurrence, err = elasticsearchStorage.GetOccurrence(ctx, projectID, objectID)
				Expect(err).ToNot(HaveOccurred())
			})

			It("should have sent the correct HTTP request", func() {
				Expect(transport.receivedHttpRequest.Method).To(Equal("GET"))
				Expect(transport.receivedHttpRequest.URL.Path).To(Equal(fmt.Sprintf("/%s/_search", projectID)))

				requestBody, err := ioutil.ReadAll(transport.receivedHttpRequest.Body)
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
				transport.expectedError = errors.New("failed to find document")
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
				transport.preparedHttpResponse = &http.Response{
					StatusCode: 200,
					Body:       ioutil.NopCloser(strings.NewReader("bad object")),
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
				transport.preparedHttpResponse = &http.Response{
					StatusCode: 404,
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

				transport.preparedHttpResponse = &http.Response{
					StatusCode: 201,
					Body: ioutil.NopCloser(strings.NewReader(fmt.Sprintf(`{
						"_id": "%s"
					}`, expectedOccurrenceId))),
				}

				expectedOccurrence, err = elasticsearchStorage.CreateOccurrence(ctx, projectID, "", newOccurrence)
				Expect(err).ToNot(HaveOccurred())
			})

			It("should have sent the correct HTTP request", func() {
				Expect(transport.receivedHttpRequest.Method).To(Equal("POST"))
				Expect(transport.receivedHttpRequest.URL.Path).To(Equal(fmt.Sprintf("/%s/_doc", projectID)))
			})

			It("should return a Grafeas occurrence with the correct name", func() {
				Expect(expectedOccurrence).To(Equal(newOccurrence))
				Expect(expectedOccurrence.Name).To(Equal(fmt.Sprintf("projects/%s/occurrences/%s", projectID, expectedOccurrenceId)))
			})
		})

		When("elasticsearch fails to create a new document", func() {
			BeforeEach(func() {
				transport.expectedError = errors.New("failed to create new document")
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
				transport.preparedHttpResponse = &http.Response{
					StatusCode: 200,
				}

				expectedOccurrences, err = elasticsearchStorage.BatchCreateOccurrences(ctx, projectID, "", newOccurrences)
			})

			It("should have sent the correct HTTP request", func() {
				Expect(transport.receivedHttpRequest.Method).To(Equal("POST"))
				Expect(transport.receivedHttpRequest.URL.Path).To(Equal("/_bulk"))
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
				transport.expectedError = errors.New("failed to create new documents")

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
				transport.preparedHttpResponse = &http.Response{
					StatusCode: 200,
				}

				err = elasticsearchStorage.DeleteOccurrence(ctx, projectID, "")
				Expect(err).ToNot(HaveOccurred())
			})

			It("should have sent the correct HTTP request", func() {
				Expect(transport.receivedHttpRequest.Method).To(Equal("POST"))
				Expect(transport.receivedHttpRequest.URL.Path).To(Equal(fmt.Sprintf("/%s/%s", projectID, "_delete_by_query")))
			})

		})

		When("elasticsearch fails to delete documents", func() {
			BeforeEach(func() {
				transport.expectedError = errors.New("failed to delete documents")

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
