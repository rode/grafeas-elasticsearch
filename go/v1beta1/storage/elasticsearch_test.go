package storage

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

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
		projectId            string
		ctx                  context.Context
		project              *prpb.Project
		expectedProject      *prpb.Project
		err                  error
	)

	BeforeEach(func() {
		transport = &mockEsTransport{}
		transport.expectedError = nil
		mockEsClient := &elasticsearch.Client{Transport: transport, API: esapi.New(transport)}

		projectId = "rode"
		ctx = context.Background()
		project = &prpb.Project{Name: fmt.Sprintf("projects/%s", projectId)}

		elasticsearchStorage = NewElasticsearchStore(mockEsClient, logger)
	})

	Context("creating a new Grafeas project", func() {
		When("elasticsearch successfully creates a new index", func() {
			BeforeEach(func() {
				transport.preparedHttpResponse = &http.Response{
					StatusCode: 200,
				}

				expectedProject, err = elasticsearchStorage.CreateProject(ctx, projectId, project)
				Expect(err).ToNot(HaveOccurred())
			})

			It("should have sent the correct HTTP request", func() {
				Expect(transport.receivedHttpRequest.Method).To(Equal("PUT"))
				Expect(transport.receivedHttpRequest.URL.Path).To(Equal(fmt.Sprintf("/%s", projectId)))

				requestBody, err := ioutil.ReadAll(transport.receivedHttpRequest.Body)
				Expect(err).ToNot(HaveOccurred())

				parsed, err := gabs.ParseJSON(requestBody)
				Expect(err).ToNot(HaveOccurred())

				Expect(parsed.Path("mappings._meta.type").Data().(string)).To(BeEquivalentTo("grafeas-project"))
			})

			It("should return a new Grafeas project", func() {
				Expect(expectedProject.Name).To(Equal("projects/rode"))
			})
		})

		When("elasticsearch unsuccessfully creates a new index", func() {
			BeforeEach(func() {
				transport.expectedError = errors.New("failed to create new index")
				_, err = elasticsearchStorage.CreateProject(ctx, projectId, project)
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
			newOccurrence = createTestOccurrenceStruct()
		})

		When("elasticsearch creates a new document", func() {
			BeforeEach(func() {
				transport.preparedHttpResponse = &http.Response{
					StatusCode: 201,
				}

				expectedOccurrence, err = elasticsearchStorage.CreateOccurrence(ctx, projectId, "", newOccurrence)
				Expect(err).ToNot(HaveOccurred())
			})

			It("should have sent the correct HTTP request", func() {
				Expect(transport.receivedHttpRequest.Method).To(Equal("POST"))
				Expect(transport.receivedHttpRequest.URL.Path).To(Equal(fmt.Sprintf("/%s/_doc", projectId)))
			})

			It("should return a Grafeas occurrence", func() {
				Expect(expectedOccurrence).To(Equal(newOccurrence))
			})
		})

		When("elasticsearch fails to create a new document", func() {
			BeforeEach(func() {
				transport.expectedError = errors.New("failed to create new document")
				_, err = elasticsearchStorage.CreateOccurrence(ctx, projectId, "", newOccurrence)
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
				newOccurrences = append(newOccurrences, createTestOccurrenceStruct())
			}
		})

		When("elasticsearch successfully creates new documents", func() {
			BeforeEach(func() {
				transport.preparedHttpResponse = &http.Response{
					StatusCode: 200,
				}

				expectedOccurrences, err = elasticsearchStorage.BatchCreateOccurrences(ctx, projectId, "", newOccurrences)
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

				expectedOccurrences, err = elasticsearchStorage.BatchCreateOccurrences(ctx, projectId, "", newOccurrences)
			})

			It("should return an error", func() {
				Expect(err).ToNot(BeEmpty())
			})
		})
	})

	Context("deleting a Grafeas occurrence", func() {
		var (
			objectId string
			err      error
		)

		BeforeEach(func() {
			objectId = gofakeit.LetterN(8)
		})

		When("elasticsearch successfully deletes the document", func() {
			BeforeEach(func() {
				transport.preparedHttpResponse = &http.Response{
					StatusCode: 200,
				}

				err = elasticsearchStorage.DeleteOccurrence(ctx, projectId, "")
				Expect(err).ToNot(HaveOccurred())
			})

			It("should have sent the correct HTTP request", func() {
				Expect(transport.receivedHttpRequest.Method).To(Equal("POST"))
				Expect(transport.receivedHttpRequest.URL.Path).To(Equal(fmt.Sprintf("/%s/%s", projectId, "_delete_by_query")))
			})

		})

		When("elasticsearch fails to delete documents", func() {
			BeforeEach(func() {
				transport.expectedError = errors.New("failed to delete documents")

				err = elasticsearchStorage.DeleteOccurrence(ctx, projectId, objectId)
			})

			It("should return an error", func() {
				Expect(err).To(HaveOccurred())
			})
		})
	})

})

func createTestOccurrenceStruct() (occurrence *pb.Occurrence) {
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
