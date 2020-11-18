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
		pID                  string
		ctx                  context.Context
		project              *prpb.Project
		expectedProject      *prpb.Project
		err                  error
	)

	BeforeEach(func() {
		transport = &mockEsTransport{}
		transport.expectedError = nil
		mockEsClient := &elasticsearch.Client{Transport: transport, API: esapi.New(transport)}

		pID = "rode"
		ctx = context.Background()
		project = &prpb.Project{Name: "projects/rode"}

		elasticsearchStorage = NewElasticsearchStore(mockEsClient, logger)
	})

	Context("Creating a new Grafeas project", func() {
		When("elasticsearch successfully creates a new index", func() {
			BeforeEach(func() {
				transport.preparedPerformResponse = &http.Response{
					StatusCode: 200,
				}

				expectedProject, err = elasticsearchStorage.CreateProject(ctx, pID, project)
			})

			It("should have performed a PUT Request", func() {
				Expect(transport.receivedPerformRequest.Method).To(Equal("PUT"))
			})

			It("should have created an index at a path matching the PID", func() {
				Expect(transport.receivedPerformRequest.URL.Path).To(Equal(fmt.Sprintf("/%s", pID)))
			})

			It("should return a new Grafeas project", func() {
				Expect(expectedProject.Name).To(Equal("projects/rode"))
			})

			It("should return without an error", func() {
				Expect(err).ToNot(HaveOccurred())
			})
		})

		When("elasticsearch unsuccessfully creates a new index", func() {
			BeforeEach(func() {
				transport.expectedError = errors.New("failed to create new index")
				_, err = elasticsearchStorage.CreateProject(ctx, pID, project)
			})

			It("should return an error", func() {
				Expect(err).To(HaveOccurred())
			})
		})
	})

	Context("Creating a new Grafeas occurrence", func() {
		var (
			uID                string
			newOccurrence      *pb.Occurrence
			expectedOccurrence *pb.Occurrence
		)

		BeforeEach(func() {
			uID = "sonarqubeMetric"
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

				expectedOccurrence, err = elasticsearchStorage.CreateOccurrence(ctx, pID, uID, newOccurrence)
			})

			It("should perform a PUT request", func() {
				Expect(transport.receivedPerformRequest.Method).To(Equal("POST"))
			})

			It("should have created an index at a path matching the PID", func() {
				Expect(transport.receivedPerformRequest.URL.Path).To(Equal(fmt.Sprintf("/%s/_doc", pID)))
			})

			It("should return a Grafeas occurrence", func() {
				Expect(expectedOccurrence).To(Equal(newOccurrence))
			})

			It("should return without an error", func() {
				Expect(err).ToNot(HaveOccurred())
			})
		})

		When("elasticsearch fails to create a new document", func() {
			BeforeEach(func() {
				transport.expectedError = errors.New("failed to create new document")
				_, err = elasticsearchStorage.CreateProject(ctx, pID, project)
			})

			It("should return an error", func() {
				Expect(err).To(HaveOccurred())
			})
		})
	})

	Context("creating a batch of Grafeas occurrences", func() {
		var (
			uID                 string
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

		When("elastic search successfully creates new documents", func() {
			BeforeEach(func() {
				transport.preparedPerformResponse = &http.Response{
					StatusCode: 200,
				}

				expectedOccurrences, err = elasticsearchStorage.BatchCreateOccurrences(ctx, pID, uID, newOccurrences)
			})

			It("should have performed a POST request", func() {
				Expect(transport.receivedPerformRequest.Method).To(Equal("POST"))
			})

			It("should have created an index at a path matching the PID", func() {
				Expect(transport.receivedPerformRequest.URL.Path).To(Equal("/_bulk"))
			})

			It("should return a Grafeas occurrence", func() {
				Expect(expectedOccurrences).To(Equal(newOccurrences))
			})

			It("should return without an error", func() {
				Expect(err).To(BeEmpty())
			})
		})

		When("elastic search fails to create new documents", func() {
			BeforeEach(func() {
				transport.expectedError = errors.New("failed to create new documents")

				expectedOccurrences, err = elasticsearchStorage.BatchCreateOccurrences(ctx, pID, uID, newOccurrences)
			})

			It("should return an error", func() {
				Expect(err).ToNot(BeEmpty())
			})
		})
	})
})
