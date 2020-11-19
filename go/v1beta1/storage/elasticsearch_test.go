package storage

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/brianvoe/gofakeit/v5"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/grafeas/grafeas/proto/v1beta1/common_go_proto"
	"github.com/grafeas/grafeas/proto/v1beta1/grafeas_go_proto"
	pb "github.com/grafeas/grafeas/proto/v1beta1/grafeas_go_proto"
	prpb "github.com/grafeas/grafeas/proto/v1beta1/project_go_proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/types/known/timestamppb"
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
				transport.preparedPerformResponse = &http.Response{
					StatusCode: 200,
				}

				expectedProject, err = elasticsearchStorage.CreateProject(ctx, projectId, project)
				Expect(err).ToNot(HaveOccurred())
			})

			It("should have send the correct HTTP request", func() {
				Expect(transport.receivedPerformRequest.Method).To(Equal("PUT"))
				Expect(transport.receivedPerformRequest.URL.Path).To(Equal(fmt.Sprintf("/%s", projectId)))
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
			newOccurrence = &pb.Occurrence{
				Name: gofakeit.LetterN(10),
				Resource: &grafeas_go_proto.Resource{
					Uri: gofakeit.LetterN(10),
				},
				NoteName:    gofakeit.LetterN(10),
				Kind:        common_go_proto.NoteKind_NOTE_KIND_UNSPECIFIED,
				Remediation: gofakeit.LetterN(10),
				Details:     nil,
			}
		})

		When("elasticsearch creates a new document", func() {
			BeforeEach(func() {
				transport.preparedPerformResponse = &http.Response{
					StatusCode: 201,
				}

				expectedOccurrence, err = elasticsearchStorage.CreateOccurrence(ctx, projectId, "", newOccurrence)
				Expect(err).ToNot(HaveOccurred())
			})

			It("should have send the correct HTTP request", func() {
				Expect(transport.receivedPerformRequest.Method).To(Equal("POST"))
				Expect(transport.receivedPerformRequest.URL.Path).To(Equal(fmt.Sprintf("/%s/_doc", projectId)))
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
				newOccurrences = append(newOccurrences, &pb.Occurrence{
					Name: gofakeit.LetterN(10),
					Resource: &grafeas_go_proto.Resource{
						Uri: gofakeit.LetterN(10),
					},
					NoteName:    gofakeit.LetterN(10),
					Kind:        common_go_proto.NoteKind_NOTE_KIND_UNSPECIFIED,
					Remediation: gofakeit.LetterN(10),
					CreateTime:  timestamppb.New(gofakeit.Date()),
					UpdateTime:  timestamppb.New(gofakeit.Date()),
					Details:     nil,
				})
			}
		})

		When("elasticsearch successfully creates new documents", func() {
			BeforeEach(func() {
				transport.preparedPerformResponse = &http.Response{
					StatusCode: 200,
				}

				expectedOccurrences, err = elasticsearchStorage.BatchCreateOccurrences(ctx, projectId, "", newOccurrences)
			})

			It("should have send the correct HTTP request", func() {
				Expect(transport.receivedPerformRequest.Method).To(Equal("POST"))
				Expect(transport.receivedPerformRequest.URL.Path).To(Equal("/_bulk"))
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
})
