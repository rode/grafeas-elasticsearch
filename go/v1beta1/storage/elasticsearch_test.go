package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/rode/grafeas-elasticsearch/go/config"
	"github.com/rode/grafeas-elasticsearch/go/mocks"
	"github.com/rode/grafeas-elasticsearch/go/v1beta1/storage/filtering"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

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
		ctx                  context.Context
		expectedProjectId    string
		mockCtrl             *gomock.Controller
		filterer             *mocks.MockFilterer
		esConfig             *config.ElasticsearchConfig
	)

	BeforeEach(func() {
		expectedProjectId = gofakeit.LetterN(10)

		ctx = context.Background()

		mockCtrl = gomock.NewController(GinkgoT())
		filterer = mocks.NewMockFilterer(mockCtrl)
		transport = &mockEsTransport{}
		esConfig = &config.ElasticsearchConfig{
			URL:     gofakeit.URL(),
			Refresh: config.RefreshTrue,
		}
	})

	JustBeforeEach(func() {
		mockEsClient := &elasticsearch.Client{Transport: transport, API: esapi.New(transport)}

		elasticsearchStorage = NewElasticsearchStorage(logger, mockEsClient, filterer, esConfig)
	})

	AfterEach(func() {
		mockCtrl.Finish()
	})

	Context("creating a new Grafeas project", func() {
		var (
			createProjectErr         error
			expectedProjectIndex     string
			expectedProject          *prpb.Project
			expectedOccurrencesIndex string
			expectedNotesIndex       string
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
					Body: structToJsonBody(&esIndexDocResponse{
						Id: gofakeit.LetterN(10),
					}),
				},
				{
					StatusCode: http.StatusOK,
				},
				{
					StatusCode: http.StatusOK,
				},
			}
			expectedProjectIndex = fmt.Sprintf("%s-%s", indexPrefix, "projects")
			expectedOccurrencesIndex = fmt.Sprintf("%s-%s-%s", indexPrefix, expectedProjectId, "occurrences")
			expectedNotesIndex = fmt.Sprintf("%s-%s-%s", indexPrefix, expectedProjectId, "notes")
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
			It("should create a new document for the project", func() {
				Expect(transport.receivedHttpRequests[1].URL.Path).To(Equal(fmt.Sprintf("/%s/_doc", expectedProjectIndex)))
				Expect(transport.receivedHttpRequests[1].Method).To(Equal(http.MethodPost))

				projectBody := &prpb.Project{}
				err := protojson.Unmarshal(ioReadCloserToByteSlice(transport.receivedHttpRequests[1].Body), proto.MessageV2(projectBody))
				Expect(err).ToNot(HaveOccurred())

				Expect(projectBody.Name).To(Equal(fmt.Sprintf("projects/%s", expectedProjectId)))
			})

			It("should create indices for storing occurrences/notes for the project", func() {
				Expect(transport.receivedHttpRequests[2].URL.Path).To(Equal(fmt.Sprintf("/%s", expectedOccurrencesIndex)))
				Expect(transport.receivedHttpRequests[2].Method).To(Equal(http.MethodPut))
				assertIndexCreateBodyHasMetadataAndStringMapping(transport.receivedHttpRequests[2].Body)

				Expect(transport.receivedHttpRequests[3].URL.Path).To(Equal(fmt.Sprintf("/%s", expectedNotesIndex)))
				Expect(transport.receivedHttpRequests[3].Method).To(Equal(http.MethodPut))
				assertIndexCreateBodyHasMetadataAndStringMapping(transport.receivedHttpRequests[3].Body)
			})

			It("should return the project", func() {
				Expect(expectedProject).ToNot(BeNil())
				Expect(expectedProject.Name).To(Equal(fmt.Sprintf("projects/%s", expectedProjectId)))
			})

			When(fmt.Sprintf("refresh configuration is %s", config.RefreshTrue), func() {
				BeforeEach(func() {
					esConfig.Refresh = config.RefreshTrue
				})

				It("should immediately refresh the index", func() {
					Expect(transport.receivedHttpRequests[1].URL.Query().Get("refresh")).To(Equal("true"))
				})
			})

			When(fmt.Sprintf("refresh configuration is %s", config.RefreshWaitFor), func() {
				BeforeEach(func() {
					esConfig.Refresh = config.RefreshWaitFor
				})

				It("should wait for refresh of index", func() {
					Expect(transport.receivedHttpRequests[1].URL.Query().Get("refresh")).To(Equal("wait_for"))
				})
			})

			When(fmt.Sprintf("refresh configuration is %s", config.RefreshFalse), func() {
				BeforeEach(func() {
					esConfig.Refresh = config.RefreshFalse
				})

				It("should not wait or force refresh of index", func() {
					Expect(transport.receivedHttpRequests[1].URL.Query().Get("refresh")).To(Equal("false"))
				})
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

	Context("listing Grafeas projects", func() {
		var (
			actualErr            error
			actualProjects       []*prpb.Project
			expectedProjects     []*prpb.Project
			expectedProjectIndex string
			expectedFilter       string
			expectedQuery        *filtering.Query
		)

		BeforeEach(func() {
			expectedQuery = &filtering.Query{}
			expectedFilter = ""
			expectedProjectIndex = fmt.Sprintf("%s-%s", indexPrefix, "projects")
			expectedProjects = generateTestProjects(gofakeit.Number(2, 5))
			transport.preparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body: createProjectEsSearchResponse(
						expectedProjects...,
					),
				},
			}
		})

		JustBeforeEach(func() {
			actualProjects, _, actualErr = elasticsearchStorage.ListProjects(ctx, expectedFilter, 0, "")
		})

		It("should query elasticsearch for project documents", func() {
			Expect(transport.receivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_search", expectedProjectIndex)))
			Expect(transport.receivedHttpRequests[0].Method).To(Equal(http.MethodGet))

			requestBody, err := ioutil.ReadAll(transport.receivedHttpRequests[0].Body)
			Expect(err).ToNot(HaveOccurred())

			searchBody := &esSearch{}
			err = json.Unmarshal(requestBody, searchBody)
			Expect(err).ToNot(HaveOccurred())
			Expect(searchBody.Query).To(BeNil())
		})

		When("a valid filter is specified", func() {
			BeforeEach(func() {
				expectedQuery = &filtering.Query{
					Term: &filtering.Term{
						gofakeit.LetterN(10): gofakeit.LetterN(10),
					},
				}
				expectedFilter = gofakeit.LetterN(10)

				filterer.
					EXPECT().
					ParseExpression(expectedFilter).
					Return(expectedQuery, nil)
			})

			It("should send the parsed query to elasticsearch", func() {
				Expect(transport.receivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_search", expectedProjectIndex)))
				Expect(transport.receivedHttpRequests[0].Method).To(Equal(http.MethodGet))

				requestBody, err := ioutil.ReadAll(transport.receivedHttpRequests[0].Body)
				Expect(err).ToNot(HaveOccurred())

				searchBody := &esSearch{}
				err = json.Unmarshal(requestBody, searchBody)
				Expect(err).ToNot(HaveOccurred())
				Expect(searchBody.Query).To(Equal(expectedQuery))
			})
		})

		When("an invalid filter is specified", func() {
			BeforeEach(func() {
				expectedFilter = gofakeit.LetterN(10)

				filterer.
					EXPECT().
					ParseExpression(expectedFilter).
					Return(nil, errors.New(gofakeit.LetterN(10)))
			})

			It("should not send a request to elasticsearch", func() {
				Expect(transport.receivedHttpRequests).To(HaveLen(0))
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})

		When("elasticsearch successfully returns project document(s)", func() {
			It("should return the Grafeas project(s)", func() {
				Expect(actualProjects).ToNot(BeNil())
				Expect(actualProjects).To(Equal(expectedProjects))
			})

			It("should return without an error", func() {
				Expect(actualErr).ToNot(HaveOccurred())
			})
		})

		When("elasticsearch returns zero hits", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusOK,
						Body:       createGenericEsSearchResponse(),
					},
				}
			})

			It("should return an empty array of grafeas projects", func() {
				Expect(actualProjects).To(BeNil())
			})

			It("should not return an error", func() {
				Expect(actualErr).ToNot(HaveOccurred())
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
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})

		When("returns an unexpected response", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusInternalServerError,
					},
				}
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})
	})

	Context("retrieving a Grafeas project", func() {
		var (
			actualErr            error
			expectedProjectIndex string
			actualProject        *prpb.Project
		)

		BeforeEach(func() {
			expectedProjectIndex = fmt.Sprintf("%s-%s", indexPrefix, "projects")
			transport.preparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body: createGenericEsSearchResponse(&prpb.Project{
						Name: fmt.Sprintf("projects/%s", expectedProjectId),
					}),
				},
			}
		})

		JustBeforeEach(func() {
			actualProject, actualErr = elasticsearchStorage.GetProject(ctx, expectedProjectId)
		})

		It("should query elasticsearch for the specified project", func() {
			Expect(transport.receivedHttpRequests).To(HaveLen(1))

			Expect(transport.receivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_search", expectedProjectIndex)))
			Expect(transport.receivedHttpRequests[0].Method).To(Equal(http.MethodGet))

			requestBody, err := ioutil.ReadAll(transport.receivedHttpRequests[0].Body)
			Expect(err).ToNot(HaveOccurred())

			searchBody := &esSearch{}
			err = json.Unmarshal(requestBody, searchBody)
			Expect(err).ToNot(HaveOccurred())

			Expect((*searchBody.Query.Term)["name"]).To(Equal(fmt.Sprintf("projects/%s", expectedProjectId)))
		})

		When("elasticsearch successfully returns a project document", func() {
			It("should return the Grafeas project", func() {
				Expect(actualProject).ToNot(BeNil())
				Expect(actualProject.Name).To(Equal(fmt.Sprintf("projects/%s", expectedProjectId)))
			})

			It("should return without an error", func() {
				Expect(actualErr).ToNot(HaveOccurred())
			})
		})

		When("elasticsearch can not find the specified project document", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusOK,
						Body:       createGenericEsSearchResponse(),
					},
				}
			})

			It("should return a not found error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.NotFound)
			})
		})

		When("elasticsearch returns a bad object", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusOK,
						Body:       ioutil.NopCloser(strings.NewReader(gofakeit.LetterN(10))),
					},
				}
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})

		When("elasticsearch returns an error", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusInternalServerError,
					},
				}
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})
	})

	Context("deleting a Grafeas project", func() {
		var (
			actualErr                error
			expectedProjectIndex     string
			expectedNotesIndex       string
			expectedOccurrencesIndex string
		)

		BeforeEach(func() {
			expectedProjectIndex = fmt.Sprintf("%s-projects", indexPrefix)
			expectedOccurrencesIndex = fmt.Sprintf("%s-%s-%s", indexPrefix, expectedProjectId, "occurrences")
			expectedNotesIndex = fmt.Sprintf("%s-%s-%s", indexPrefix, expectedProjectId, "notes")

			transport.preparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body: structToJsonBody(&esDeleteResponse{
						Deleted: 1,
					}),
				},
				{
					StatusCode: http.StatusOK,
				},
			}
		})

		JustBeforeEach(func() {
			actualErr = elasticsearchStorage.DeleteProject(ctx, expectedProjectId)
		})

		It("should have sent a request to delete the project document", func() {
			Expect(transport.receivedHttpRequests[0].Method).To(Equal(http.MethodPost))
			Expect(transport.receivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_delete_by_query", expectedProjectIndex)))

			requestBody, err := ioutil.ReadAll(transport.receivedHttpRequests[0].Body)
			Expect(err).ToNot(HaveOccurred())

			searchBody := &esSearch{}
			err = json.Unmarshal(requestBody, searchBody)
			Expect(err).ToNot(HaveOccurred())

			Expect((*searchBody.Query.Term)["name"]).To(Equal(fmt.Sprintf("projects/%s", expectedProjectId)))
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshTrue), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshTrue
			})

			It("should immediately refresh the index", func() {
				Expect(transport.receivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("true"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshWaitFor), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshWaitFor
			})

			It("should immediately refresh the index", func() {
				Expect(transport.receivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("true"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshFalse), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshFalse
			})

			It("should not wait for or force refresh of index", func() {
				Expect(transport.receivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("false"))
			})
		})

		When("elasticsearch successfully deletes the project document", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses[0].Body = structToJsonBody(&esDeleteResponse{
					Deleted: 1,
				})
			})

			It("should attempt to delete the indices for notes / occurrences", func() {
				Expect(transport.receivedHttpRequests[1].Method).To(Equal(http.MethodDelete))
				Expect(transport.receivedHttpRequests[1].URL.Path).To(Equal(fmt.Sprintf("/%s,%s", expectedOccurrencesIndex, expectedNotesIndex)))
			})

			When("elasticsearch successfully deletes the indices for notes / occurrences", func() {
				It("should not return an error", func() {
					Expect(actualErr).ToNot(HaveOccurred())
				})
			})

			When("elasticsearch fails to delete the indices for notes / occurrences", func() {
				BeforeEach(func() {
					transport.preparedHttpResponses[1].StatusCode = http.StatusInternalServerError
				})

				It("should return an error", func() {
					assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
				})
			})
		})

		When("project does not exist", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses[0].Body = structToJsonBody(&esDeleteResponse{
					Deleted: 0,
				})
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})

			It("should not attempt to delete the indices for notes / occurrences", func() {
				Expect(transport.receivedHttpRequests).To(HaveLen(1))
			})
		})

		When("deleting the project document fails", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses[0].StatusCode = http.StatusInternalServerError
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})

			It("should not attempt to delete the indices for notes / occurrences", func() {
				Expect(transport.receivedHttpRequests).To(HaveLen(1))
			})
		})
	})

	Context("retrieving a Grafeas occurrence", func() {
		var (
			actualErr               error
			actualOccurrence        *pb.Occurrence
			expectedOccurrenceIndex string
			expectedOccurrenceId    string
			expectedOccurrenceName  string
		)

		BeforeEach(func() {
			expectedOccurrenceId = gofakeit.LetterN(10)
			expectedOccurrenceIndex = fmt.Sprintf("%s-%s-occurrences", indexPrefix, expectedProjectId)
			expectedOccurrenceName = fmt.Sprintf("projects/%s/occurrences/%s", expectedProjectId, expectedOccurrenceId)
			transport.preparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body: createGenericEsSearchResponse(&pb.Occurrence{
						Name: expectedOccurrenceName,
					}),
				},
			}
		})

		JustBeforeEach(func() {
			actualOccurrence, actualErr = elasticsearchStorage.GetOccurrence(ctx, expectedProjectId, expectedOccurrenceId)
		})

		It("should query elasticsearch for the specified occurrence", func() {
			Expect(transport.receivedHttpRequests).To(HaveLen(1))

			Expect(transport.receivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_search", expectedOccurrenceIndex)))
			Expect(transport.receivedHttpRequests[0].Method).To(Equal(http.MethodGet))

			requestBody, err := ioutil.ReadAll(transport.receivedHttpRequests[0].Body)
			Expect(err).ToNot(HaveOccurred())

			searchBody := &esSearch{}
			err = json.Unmarshal(requestBody, searchBody)
			Expect(err).ToNot(HaveOccurred())

			Expect((*searchBody.Query.Term)["name"]).To(Equal(expectedOccurrenceName))
		})

		When("elasticsearch successfully returns an occurrence document", func() {
			It("should return the Grafeas occurrence", func() {
				Expect(actualOccurrence).ToNot(BeNil())
				Expect(actualOccurrence.Name).To(Equal(expectedOccurrenceName))
			})

			It("should return without an error", func() {
				Expect(actualErr).ToNot(HaveOccurred())
			})
		})

		When("elasticsearch can not find the specified occurrence document", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusOK,
						Body:       createGenericEsSearchResponse(),
					},
				}
			})

			It("should return a not found error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.NotFound)
			})
		})

		When("elasticsearch returns a bad object", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusOK,
						Body:       ioutil.NopCloser(strings.NewReader(gofakeit.LetterN(10))),
					},
				}
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})

		When("elasticsearch returns an error", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusInternalServerError,
					},
				}
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})
	})

	Context("creating a new Grafeas occurrence", func() {
		var (
			actualOccurrence         *pb.Occurrence
			expectedOccurrence       *pb.Occurrence
			expectedOccurrenceESId   string
			expectedOccurrencesIndex string
			actualErr                error
		)

		// BeforeEach configures the happy path for this context
		// Variables configured here may be overridden in nested BeforeEach blocks
		BeforeEach(func() {
			expectedOccurrenceESId = gofakeit.LetterN(10)
			expectedOccurrencesIndex = fmt.Sprintf("%s-%s-occurrences", indexPrefix, expectedProjectId)
			expectedOccurrence = generateTestOccurrence("")

			transport.preparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusCreated,
					Body: structToJsonBody(&esIndexDocResponse{
						Id: expectedOccurrenceESId,
					}),
				},
			}
		})

		// JustBeforeEach actually invokes the system under test
		JustBeforeEach(func() {
			occurrence := deepCopyOccurrence(expectedOccurrence)

			transport.preparedHttpResponses[0].Body = structToJsonBody(&esIndexDocResponse{
				Id: expectedOccurrenceESId,
			})
			actualOccurrence, actualErr = elasticsearchStorage.CreateOccurrence(context.Background(), expectedProjectId, "", occurrence)
		})

		It("should attempt to index the occurrence as a document", func() {
			Expect(transport.receivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_doc", expectedOccurrencesIndex)))

			requestBody, err := ioutil.ReadAll(transport.receivedHttpRequests[0].Body)
			Expect(err).ToNot(HaveOccurred())

			indexedOccurrence := &pb.Occurrence{}
			err = protojson.Unmarshal(requestBody, proto.MessageV2(indexedOccurrence))
			Expect(err).ToNot(HaveOccurred())

			expectedOccurrence.Name = actualOccurrence.Name
			Expect(indexedOccurrence).To(Equal(expectedOccurrence))
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshTrue), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshTrue
			})

			It("should immediately refresh the index", func() {
				Expect(transport.receivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("true"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshWaitFor), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshWaitFor
			})

			It("should wait for refresh of index", func() {
				Expect(transport.receivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("wait_for"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshFalse), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshFalse
			})

			It("should not wait or force refresh of index", func() {
				Expect(transport.receivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("false"))
			})
		})

		When("indexing the document fails", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses[0] = &http.Response{
					StatusCode: http.StatusInternalServerError,
					Body: structToJsonBody(&esIndexDocResponse{
						Error: &esIndexDocError{
							Type:   gofakeit.LetterN(10),
							Reason: gofakeit.LetterN(10),
						},
					}),
				}
			})

			It("should return an error", func() {
				Expect(actualOccurrence).To(BeNil())
				Expect(actualErr).To(HaveOccurred())
			})
		})

		When("indexing the document succeeds", func() {
			It("should return the occurrence that was created", func() {
				Expect(actualErr).ToNot(HaveOccurred())

				expectedOccurrence.Name = actualOccurrence.Name
				Expect(actualOccurrence).To(Equal(expectedOccurrence))
			})
		})
	})

	Context("creating a batch of Grafeas occurrences", func() {
		var (
			expectedErrs             []error
			actualErrs               []error
			actualOccurrences        []*pb.Occurrence
			expectedOccurrences      []*pb.Occurrence
			expectedOccurrencesIndex string
		)

		// BeforeEach configures the happy path for this context
		// Variables configured here may be overridden in nested BeforeEach blocks
		BeforeEach(func() {
			expectedOccurrencesIndex = fmt.Sprintf("%s-%s-%s", indexPrefix, expectedProjectId, "occurrences")
			expectedOccurrences = generateTestOccurrences(gofakeit.Number(2, 5))
			for i := 0; i < len(expectedOccurrences); i++ {
				expectedErrs = append(expectedErrs, nil)
			}

			transport.preparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body:       createEsBulkOccurrenceIndexResponse(expectedOccurrences, expectedErrs),
				},
			}
		})

		// JustBeforeEach actually invokes the system under test
		JustBeforeEach(func() {
			occurrences := deepCopyOccurrences(expectedOccurrences)

			transport.preparedHttpResponses[0].Body = createEsBulkOccurrenceIndexResponse(occurrences, expectedErrs)
			actualOccurrences, actualErrs = elasticsearchStorage.BatchCreateOccurrences(context.Background(), expectedProjectId, "", occurrences)
		})

		// this test parses the ndjson request body and ensures that it was formatted correctly
		It("should send a bulk request to ES to index each occurrence", func() {
			var expectedPayloads []interface{}

			for i := 0; i < len(expectedOccurrences); i++ {
				expectedPayloads = append(expectedPayloads, &esBulkQueryFragment{}, &pb.Occurrence{})
			}

			parseEsBulkIndexRequest(transport.receivedHttpRequests[0].Body, expectedPayloads)

			for i, payload := range expectedPayloads {
				if i%2 == 0 { // index metadata
					metadata := payload.(*esBulkQueryFragment)
					Expect(metadata.Index.Index).To(Equal(expectedOccurrencesIndex))
				} else { // occurrence
					occurrence := payload.(*pb.Occurrence)
					expectedOccurrence := expectedOccurrences[(i-1)/2]
					expectedOccurrence.Name = occurrence.Name

					Expect(occurrence).To(Equal(expectedOccurrence))
				}
			}
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshTrue), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshTrue
			})

			It("should immediately refresh the index", func() {
				Expect(transport.receivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("true"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshWaitFor), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshWaitFor
			})

			It("should wait for refresh of index", func() {
				Expect(transport.receivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("wait_for"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshFalse), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshFalse
			})

			It("should not wait or force refresh of index", func() {
				Expect(transport.receivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("false"))
			})
		})

		When("the bulk request returns no errors", func() {
			It("should return all created occurrences", func() {
				for i, occ := range expectedOccurrences {
					expectedOccurrence := deepCopyOccurrence(occ)
					expectedOccurrence.Name = actualOccurrences[i].Name
					Expect(actualOccurrences[i]).To(Equal(expectedOccurrence))
				}

				Expect(actualErrs).To(HaveLen(0))
			})
		})

		When("the bulk request completely fails", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses[0].StatusCode = http.StatusInternalServerError
			})

			It("should return a single error and no occurrences", func() {
				Expect(actualOccurrences).To(BeNil())
				Expect(actualErrs).To(HaveLen(1))
				Expect(actualErrs[0]).To(HaveOccurred())
			})
		})

		When("the bulk request returns some errors", func() {
			var randomErrorIndex int

			BeforeEach(func() {
				randomErrorIndex = gofakeit.Number(0, len(expectedOccurrences)-1)
				expectedErrs = []error{}
				for i := 0; i < len(expectedOccurrences); i++ {
					if i == randomErrorIndex {
						expectedErrs = append(expectedErrs, errors.New(""))
					} else {
						expectedErrs = append(expectedErrs, nil)
					}
				}
			})

			It("should only return the occurrences that were successfully created", func() {
				// remove the bad occurrence from expectedOccurrences
				copy(expectedOccurrences[randomErrorIndex:], expectedOccurrences[randomErrorIndex+1:])
				expectedOccurrences[len(expectedOccurrences)-1] = nil
				expectedOccurrences = expectedOccurrences[:len(expectedOccurrences)-1]

				// assert expectedOccurrences matches actualOccurrences
				for i, occ := range expectedOccurrences {
					expectedOccurrence := deepCopyOccurrence(occ)
					expectedOccurrence.Name = actualOccurrences[i].Name
					Expect(actualOccurrences[i]).To(Equal(expectedOccurrence))
				}

				// assert that we got a single error back
				Expect(actualErrs).To(HaveLen(1))
				Expect(actualErrs[0]).To(HaveOccurred())
			})
		})
	})

	Context("deleting a Grafeas occurrence", func() {
		var (
			actualErr                error
			expectedOccurrencesIndex string
			expectedOccurrenceId     string
			expectedOccurrenceName   string
		)

		BeforeEach(func() {
			expectedOccurrenceId = gofakeit.LetterN(10)
			expectedOccurrencesIndex = fmt.Sprintf("%s-%s-occurrences", indexPrefix, expectedProjectId)
			expectedOccurrenceName = fmt.Sprintf("projects/%s/occurrences/%s", expectedProjectId, expectedOccurrenceId)

			transport.preparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body: structToJsonBody(&esDeleteResponse{
						Deleted: 1,
					}),
				},
				{
					StatusCode: http.StatusOK,
				},
			}
		})

		JustBeforeEach(func() {
			actualErr = elasticsearchStorage.DeleteOccurrence(ctx, expectedProjectId, expectedOccurrenceId)
		})

		It("should have sent a request to elasticsearch to delete the occurrence document", func() {
			Expect(transport.receivedHttpRequests).To(HaveLen(1))
			Expect(transport.receivedHttpRequests[0].Method).To(Equal(http.MethodPost))
			Expect(transport.receivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_delete_by_query", expectedOccurrencesIndex)))

			requestBody, err := ioutil.ReadAll(transport.receivedHttpRequests[0].Body)
			Expect(err).ToNot(HaveOccurred())

			searchBody := &esSearch{}
			err = json.Unmarshal(requestBody, searchBody)
			Expect(err).ToNot(HaveOccurred())

			Expect((*searchBody.Query.Term)["name"]).To(Equal(expectedOccurrenceName))
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshTrue), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshTrue
			})

			It("should immediately refresh the index", func() {
				Expect(transport.receivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("true"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshWaitFor), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshWaitFor
			})

			It("should immediately refresh the index", func() {
				Expect(transport.receivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("true"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshFalse), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshFalse
			})

			It("should not wait or force refresh of index", func() {
				Expect(transport.receivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("false"))
			})
		})

		When("elasticsearch successfully deletes the occurrence document", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses[0].Body = structToJsonBody(&esDeleteResponse{
					Deleted: 1,
				})
			})

			It("should not return an error", func() {
				Expect(actualErr).ToNot(HaveOccurred())
			})
		})

		When("the occurrence does not exist", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses[0].Body = structToJsonBody(&esDeleteResponse{
					Deleted: 0,
				})
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})

		When("deleting the occurrence document fails", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses[0].StatusCode = http.StatusInternalServerError
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})
	})

	Context("listing Grafeas occurrences", func() {
		var (
			actualErr                error
			actualOccurrences        []*pb.Occurrence
			expectedOccurrences      []*pb.Occurrence
			expectedOccurrencesIndex string
			expectedFilter           string
			expectedQuery            *filtering.Query
		)

		BeforeEach(func() {
			expectedQuery = &filtering.Query{}
			expectedFilter = ""
			expectedOccurrencesIndex = fmt.Sprintf("%s-%s-occurrences", indexPrefix, expectedProjectId)
			expectedOccurrences = generateTestOccurrences(gofakeit.Number(2, 5))
			transport.preparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body: createOccurrenceEsSearchResponse(
						expectedOccurrences...,
					),
				},
			}
		})

		JustBeforeEach(func() {
			actualOccurrences, _, actualErr = elasticsearchStorage.ListOccurrences(ctx, expectedProjectId, expectedFilter, "", 0)
		})

		It("should query elasticsearch for occurrences", func() {
			Expect(transport.receivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_search", expectedOccurrencesIndex)))
			Expect(transport.receivedHttpRequests[0].Method).To(Equal(http.MethodGet))

			requestBody, err := ioutil.ReadAll(transport.receivedHttpRequests[0].Body)
			Expect(err).ToNot(HaveOccurred())

			searchBody := &esSearch{}
			err = json.Unmarshal(requestBody, searchBody)
			Expect(err).ToNot(HaveOccurred())
			Expect(searchBody.Query).To(BeNil())
		})

		When("a valid filter is specified", func() {
			BeforeEach(func() {
				expectedQuery = &filtering.Query{
					Term: &filtering.Term{
						gofakeit.LetterN(10): gofakeit.LetterN(10),
					},
				}
				expectedFilter = gofakeit.LetterN(10)

				filterer.
					EXPECT().
					ParseExpression(expectedFilter).
					Return(expectedQuery, nil)
			})

			It("should send the parsed query to elasticsearch", func() {
				Expect(transport.receivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_search", expectedOccurrencesIndex)))
				Expect(transport.receivedHttpRequests[0].Method).To(Equal(http.MethodGet))

				requestBody, err := ioutil.ReadAll(transport.receivedHttpRequests[0].Body)
				Expect(err).ToNot(HaveOccurred())

				searchBody := &esSearch{}
				err = json.Unmarshal(requestBody, searchBody)
				Expect(err).ToNot(HaveOccurred())
				Expect(searchBody.Query).To(Equal(expectedQuery))
			})
		})

		When("an invalid filter is specified", func() {
			BeforeEach(func() {
				expectedFilter = gofakeit.LetterN(10)

				filterer.
					EXPECT().
					ParseExpression(expectedFilter).
					Return(nil, errors.New(gofakeit.LetterN(10)))
			})

			It("should not send a request to elasticsearch", func() {
				Expect(transport.receivedHttpRequests).To(HaveLen(0))
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})

		When("elasticsearch successfully returns occurrence(s)", func() {
			It("should return the Grafeas occurrence(s)", func() {
				Expect(actualOccurrences).ToNot(BeNil())
				Expect(actualOccurrences).To(Equal(expectedOccurrences))
			})

			It("should return without an error", func() {
				Expect(actualErr).ToNot(HaveOccurred())
			})
		})

		When("elasticsearch returns zero hits", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusOK,
						Body:       createGenericEsSearchResponse(),
					},
				}
			})

			It("should return an empty slice of grafeas occurrences", func() {
				Expect(actualOccurrences).To(BeNil())
			})

			It("should not return an error", func() {
				Expect(actualErr).ToNot(HaveOccurred())
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
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})

		When("returns an unexpected response", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusInternalServerError,
					},
				}
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})
	})

	Context("creating a new Grafeas note", func() {
		var (
			actualNote         *pb.Note
			expectedNote       *pb.Note
			expectedNoteId     string
			expectedNoteName   string
			expectedNoteESId   string
			expectedNotesIndex string
			actualErr          error
		)

		// BeforeEach configures the happy path for this context
		// Variables configured here may be overridden in nested BeforeEach blocks
		BeforeEach(func() {
			// this setup is a bit different when compared to the tests for creating occurrences
			// grafeas requires that the user specify a note's ID (and thus its name) beforehand
			expectedNoteId = gofakeit.LetterN(10)
			expectedNoteName = fmt.Sprintf("projects/%s/notes/%s", expectedProjectId, expectedNoteId)
			expectedNotesIndex = fmt.Sprintf("%s-%s-notes", indexPrefix, expectedProjectId)
			expectedNote = generateTestNote(expectedNoteName)
			expectedNoteESId = gofakeit.LetterN(10)

			transport.preparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body:       createGenericEsSearchResponse(), // happy path: a note with this ID does not exist (0 hits), so we create one
				},
				{
					StatusCode: http.StatusCreated,
					Body: structToJsonBody(&esIndexDocResponse{
						Id: expectedNoteESId,
					}),
				},
			}
		})

		// JustBeforeEach actually invokes the system under test
		JustBeforeEach(func() {
			note := deepCopyNote(expectedNote)

			actualNote, actualErr = elasticsearchStorage.CreateNote(context.Background(), expectedProjectId, expectedNoteId, "", note)
		})

		It("should check elasticsearch to see if a note with the specified noteId already exists", func() {
			Expect(transport.receivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_search", expectedNotesIndex)))
			Expect(transport.receivedHttpRequests[0].Method).To(Equal(http.MethodGet))

			requestBody, err := ioutil.ReadAll(transport.receivedHttpRequests[0].Body)
			Expect(err).ToNot(HaveOccurred())

			searchBody := &esSearch{}
			err = json.Unmarshal(requestBody, searchBody)
			Expect(err).ToNot(HaveOccurred())

			Expect((*searchBody.Query.Term)["name"]).To(Equal(expectedNoteName))
		})

		When("a note with the specified noteId does not exist", func() {
			It("should attempt to index the note as a document", func() {
				Expect(transport.receivedHttpRequests).To(HaveLen(2))

				Expect(transport.receivedHttpRequests[1].URL.Path).To(Equal(fmt.Sprintf("/%s/_doc", expectedNotesIndex)))

				requestBody, err := ioutil.ReadAll(transport.receivedHttpRequests[1].Body)
				Expect(err).ToNot(HaveOccurred())

				indexedNote := &pb.Note{}
				err = protojson.Unmarshal(requestBody, proto.MessageV2(indexedNote))
				Expect(err).ToNot(HaveOccurred())

				Expect(indexedNote).To(BeEquivalentTo(expectedNote))
			})

			When("indexing the document fails", func() {
				BeforeEach(func() {
					transport.preparedHttpResponses[0] = &http.Response{
						StatusCode: http.StatusInternalServerError,
						Body: structToJsonBody(&esIndexDocResponse{
							Error: &esIndexDocError{
								Type:   gofakeit.LetterN(10),
								Reason: gofakeit.LetterN(10),
							},
						}),
					}
				})

				It("should return an error", func() {
					Expect(actualNote).To(BeNil())
					Expect(actualErr).To(HaveOccurred())
				})
			})

			When("indexing the document succeeds", func() {
				It("should return the note that was created", func() {
					Expect(actualErr).ToNot(HaveOccurred())

					expectedNote.Name = actualNote.Name
					Expect(actualNote).To(Equal(expectedNote))
				})
			})

			When(fmt.Sprintf("refresh configuration is %s", config.RefreshTrue), func() {
				BeforeEach(func() {
					esConfig.Refresh = config.RefreshTrue
				})

				It("should immediately refresh the index", func() {
					Expect(transport.receivedHttpRequests[1].URL.Query().Get("refresh")).To(Equal("true"))
				})
			})

			When(fmt.Sprintf("refresh configuration is %s", config.RefreshWaitFor), func() {
				BeforeEach(func() {
					esConfig.Refresh = config.RefreshWaitFor
				})

				It("should wait for refresh of index", func() {
					Expect(transport.receivedHttpRequests[1].URL.Query().Get("refresh")).To(Equal("wait_for"))
				})
			})

			When(fmt.Sprintf("refresh configuration is %s", config.RefreshFalse), func() {
				BeforeEach(func() {
					esConfig.Refresh = config.RefreshFalse
				})

				It("should not wait or force refresh of index", func() {
					Expect(transport.receivedHttpRequests[1].URL.Query().Get("refresh")).To(Equal("false"))
				})
			})
		})

		When("a note with the specified noteId exists", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses[0].Body = createGenericEsSearchResponse(&pb.Note{
					Name: expectedNoteName,
				})
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.AlreadyExists)
			})

			It("should not attempt to index a note document", func() {
				Expect(transport.receivedHttpRequests).To(HaveLen(1))
			})
		})

		When("checking for the existence of the note fails", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses[0].StatusCode = http.StatusInternalServerError
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})

			It("should not attempt to index a note document", func() {
				Expect(transport.receivedHttpRequests).To(HaveLen(1))
			})
		})

		When("elasticsearch returns a bad response when checking if a note exists", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses[0].Body = ioutil.NopCloser(strings.NewReader("bad object"))
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})

			It("should not attempt to index a note document", func() {
				Expect(transport.receivedHttpRequests).To(HaveLen(1))
			})
		})
	})

	Context("creating a batch of Grafeas notes", func() {
		var (
			expectedErrs       []error
			actualErrs         []error
			actualNotes        []*pb.Note
			expectedNotes      map[string]*pb.Note
			expectedNotesIndex string
		)

		// BeforeEach configures the happy path for this context
		// Variables configured here may be overridden in nested BeforeEach blocks
		BeforeEach(func() {
			expectedNotesIndex = fmt.Sprintf("%s-%s-%s", indexPrefix, expectedProjectId, "notes")
			expectedNotes = generateTestNotesMap(gofakeit.Number(2, 5))
			for i := 0; i < len(expectedNotes); i++ {
				expectedErrs = append(expectedErrs, nil)
			}

			transport.preparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body:       createEsMsearchNoteResponse(expectedNotes, expectedErrs),
				},
				{
					StatusCode: http.StatusOK,
					Body:       createEsBulkNoteIndexResponse(expectedNotes, expectedErrs),
				},
			}
		})

		// JustBeforeEach actually invokes the system under test
		JustBeforeEach(func() {
			notes := deepCopyNotes(expectedNotes)

			transport.preparedHttpResponses[0].Body = createEsMsearchNoteResponse(notes, expectedErrs)
			transport.preparedHttpResponses[1].Body = createEsBulkNoteIndexResponse(notes, expectedErrs)

			actualNotes, actualErrs = elasticsearchStorage.BatchCreateNotes(context.Background(), expectedProjectId, "", notes)
			expectedNotes = updateExpectedNoteKeys(expectedNotes, expectedProjectId)
		})
		// this test parses the ndjson request body and ensures that it was formatted correctly
		It("should send a multisearch request to ES to check for the existence of each note", func() {
			var expectedPayloads []interface{}

			for i := 0; i < len(expectedNotes); i++ {
				expectedPayloads = append(expectedPayloads, &esMsearchQueryFragment{}, &esSearch{})
			}

			parseEsMsearchIndexRequest(transport.receivedHttpRequests[0].Body, expectedPayloads)

			for i, payload := range expectedPayloads {
				if i%2 == 0 { // index metadata
					metadata := payload.(*esMsearchQueryFragment)
					Expect(metadata.Index).To(Equal(expectedNotesIndex))
				} else { // note
					Expect(payload).To(BeAssignableToTypeOf(&esSearch{}))
				}
			}
		})

		// this test parses the ndjson request body and ensures that it was formatted correctly
		It("should send a bulk request to ES to index each note", func() {
			var expectedPayloads []interface{}

			for i := 0; i < len(expectedNotes); i++ {
				expectedPayloads = append(expectedPayloads, &esBulkQueryFragment{}, &pb.Note{})
			}

			parseEsBulkIndexRequest(transport.receivedHttpRequests[1].Body, expectedPayloads)

			for i, payload := range expectedPayloads {
				if i%2 == 0 { // index metadata
					metadata := payload.(*esBulkQueryFragment)
					Expect(metadata.Index.Index).To(Equal(expectedNotesIndex))
				} else { // note
					note := payload.(*pb.Note)

					expectedNote := expectedNotes[note.Name]
					expectedNote.Name = note.Name

					Expect(note).To(Equal(expectedNote))
				}
			}
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshTrue), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshTrue
			})

			It("should immediately refresh the index", func() {
				Expect(transport.receivedHttpRequests[1].URL.Query().Get("refresh")).To(Equal("true"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshWaitFor), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshWaitFor
			})

			It("should wait for refresh of index", func() {
				Expect(transport.receivedHttpRequests[1].URL.Query().Get("refresh")).To(Equal("wait_for"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshFalse), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshFalse
			})

			It("should not wait or force refresh of index", func() {
				Expect(transport.receivedHttpRequests[1].URL.Query().Get("refresh")).To(Equal("false"))
			})
		})

		When("the multisearch returns no existing notes and the bulk request returns no errors", func() {
			It("should return all created notes", func() {
				for i, note := range actualNotes {
					expectedNote := deepCopyNote(expectedNotes[note.Name])
					expectedNote.Name = actualNotes[i].Name
					Expect(actualNotes[i]).To(Equal(expectedNote))
				}

				Expect(actualErrs).To(HaveLen(0))
			})
		})

		When("the multisearch request completely fails", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses[0].StatusCode = http.StatusInternalServerError
			})

			It("should return a single error and no notes", func() {
				Expect(actualNotes).To(BeNil())
				Expect(actualErrs).To(HaveLen(1))
				Expect(actualErrs[0]).To(HaveOccurred())
			})
		})

		When("the bulk request completely fails", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses[1].StatusCode = http.StatusInternalServerError
			})

			It("should return a single error and no notes", func() {
				Expect(actualNotes).To(BeNil())
				Expect(actualErrs).To(HaveLen(1))
				Expect(actualErrs[0]).To(HaveOccurred())
			})
		})

		When("the bulk request returns some errors", func() {
			var randomErrorIndex int
			var randomErrorKey string
			var index int

			BeforeEach(func() {
				randomErrorIndex = gofakeit.Number(0, len(expectedNotes)-1)
				expectedErrs = []error{}
				index = 0
				for key := range expectedNotes {
					if index == randomErrorIndex {
						expectedErrs = append(expectedErrs, errors.New(""))
						randomErrorKey = key
					} else {
						expectedErrs = append(expectedErrs, nil)
					}
					index++
				}
			})

			It("should only return the notes that were successfully created", func() {
				// remove the bad note from expectedNotes
				delete(expectedNotes, randomErrorKey)

				// assert expectedNotes matches actualNotes
				for i, note := range actualNotes {
					expectedNote := deepCopyNote(expectedNotes[note.Name])
					expectedNote.Name = actualNotes[i].Name
					Expect(actualNotes[i]).To(Equal(expectedNote))
				}

				// assert that we got a single error back
				Expect(actualErrs).To(HaveLen(1))
				Expect(actualErrs[0]).To(HaveOccurred())
			})
		})

		When("the multisearch request returns already existing notes", func() {
			var randomExistingNoteIndex int
			var randomExistingNoteKey string
			var index int
			var msearchResponseBody *esMsearch

			BeforeEach(func() {

				randomExistingNoteIndex = gofakeit.Number(0, len(expectedNotes)-1)
				expectedErrs = []error{}
				index = 0
				for key := range expectedNotes {
					if index == randomExistingNoteIndex {
						expectedErrs = append(expectedErrs, errors.New(""))
						randomExistingNoteKey = key
					} else {
						expectedErrs = append(expectedErrs, nil)
					}
					index++
				}

				msearchResponseBody = &esMsearch{
					[]esMsearchResponse{
						{
							esMsearchResponseHits{
								esSearchResponseTotal{
									Value: 1,
								},
								[]esMsearchResponseNestedHits{
									{
										esMsearchResponseSource{
											Name: randomExistingNoteKey,
										},
									},
								},
							},
						},
					},
				}

				responseBody, err := json.Marshal(msearchResponseBody)
				Expect(err).ToNot(HaveOccurred())

				transport.preparedHttpResponses[0].Body = ioutil.NopCloser(bytes.NewReader(responseBody))

			})

			It("should only return the notes that were successfully created", func() {
				// remove the bad note from expectedNotes
				delete(expectedNotes, randomExistingNoteKey)

				// assert expectedNotes matches actualNotes
				for i, note := range actualNotes {
					expectedNote := deepCopyNote(expectedNotes[note.Name])
					expectedNote.Name = actualNotes[i].Name
					Expect(actualNotes[i]).To(Equal(expectedNote))
				}

				// assert that we got a single error back
				Expect(actualErrs).To(HaveLen(1))
				Expect(actualErrs[0]).To(HaveOccurred())
			})
		})
	})

	Context("retrieving a Grafeas note", func() {
		var (
			actualErr          error
			actualNote         *pb.Note
			expectedNotesIndex string
			expectedNoteId     string
			expectedNoteName   string
		)

		BeforeEach(func() {
			expectedNoteId = gofakeit.LetterN(10)
			expectedNotesIndex = fmt.Sprintf("%s-%s-notes", indexPrefix, expectedProjectId)
			expectedNoteName = fmt.Sprintf("projects/%s/notes/%s", expectedProjectId, expectedNoteId)
			transport.preparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body: createGenericEsSearchResponse(&pb.Note{
						Name: expectedNoteName,
					}),
				},
			}
		})

		JustBeforeEach(func() {
			actualNote, actualErr = elasticsearchStorage.GetNote(ctx, expectedProjectId, expectedNoteId)
		})

		It("should query elasticsearch for the specified note", func() {
			Expect(transport.receivedHttpRequests).To(HaveLen(1))

			Expect(transport.receivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_search", expectedNotesIndex)))
			Expect(transport.receivedHttpRequests[0].Method).To(Equal(http.MethodGet))

			requestBody, err := ioutil.ReadAll(transport.receivedHttpRequests[0].Body)
			Expect(err).ToNot(HaveOccurred())

			searchBody := &esSearch{}
			err = json.Unmarshal(requestBody, searchBody)
			Expect(err).ToNot(HaveOccurred())

			Expect((*searchBody.Query.Term)["name"]).To(Equal(expectedNoteName))
		})

		When("elasticsearch successfully returns a note document", func() {
			It("should return the Grafeas note", func() {
				Expect(actualNote).ToNot(BeNil())
				Expect(actualNote.Name).To(Equal(expectedNoteName))
			})

			It("should return without an error", func() {
				Expect(actualErr).ToNot(HaveOccurred())
			})
		})

		When("elasticsearch can not find the specified note document", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusOK,
						Body:       createGenericEsSearchResponse(),
					},
				}
			})

			It("should return a not found error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.NotFound)
			})
		})

		When("elasticsearch returns a bad object", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusOK,
						Body:       ioutil.NopCloser(strings.NewReader(gofakeit.LetterN(10))),
					},
				}
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})

		When("elasticsearch returns an error", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusInternalServerError,
					},
				}
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})
	})

	Context("listing Grafeas notes", func() {
		var (
			actualErr          error
			actualNotes        []*pb.Note
			expectedNotes      []*pb.Note
			expectedNotesIndex string
			expectedFilter     string
			expectedQuery      *filtering.Query
		)

		BeforeEach(func() {
			expectedQuery = &filtering.Query{}
			expectedFilter = ""
			expectedNotesIndex = fmt.Sprintf("%s-%s-notes", indexPrefix, expectedProjectId)
			expectedNotes = generateTestNotes(gofakeit.Number(2, 5), expectedProjectId)
			transport.preparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body: createNoteEsSearchResponse(
						expectedNotes...,
					),
				},
			}
		})

		JustBeforeEach(func() {
			actualNotes, _, actualErr = elasticsearchStorage.ListNotes(ctx, expectedProjectId, expectedFilter, "", 0)
		})

		It("should query elasticsearch for notes", func() {
			Expect(transport.receivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_search", expectedNotesIndex)))
			Expect(transport.receivedHttpRequests[0].Method).To(Equal(http.MethodGet))

			requestBody, err := ioutil.ReadAll(transport.receivedHttpRequests[0].Body)
			Expect(err).ToNot(HaveOccurred())

			searchBody := &esSearch{}
			err = json.Unmarshal(requestBody, searchBody)
			Expect(err).ToNot(HaveOccurred())
			Expect(searchBody.Query).To(BeNil())
		})

		When("a valid filter is specified", func() {
			BeforeEach(func() {
				expectedQuery = &filtering.Query{
					Term: &filtering.Term{
						gofakeit.LetterN(10): gofakeit.LetterN(10),
					},
				}
				expectedFilter = gofakeit.LetterN(10)

				filterer.
					EXPECT().
					ParseExpression(expectedFilter).
					Return(expectedQuery, nil)
			})

			It("should send the parsed query to elasticsearch", func() {
				Expect(transport.receivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_search", expectedNotesIndex)))
				Expect(transport.receivedHttpRequests[0].Method).To(Equal(http.MethodGet))

				requestBody, err := ioutil.ReadAll(transport.receivedHttpRequests[0].Body)
				Expect(err).ToNot(HaveOccurred())

				searchBody := &esSearch{}
				err = json.Unmarshal(requestBody, searchBody)
				Expect(err).ToNot(HaveOccurred())
				Expect(searchBody.Query).To(Equal(expectedQuery))
			})
		})

		When("an invalid filter is specified", func() {
			BeforeEach(func() {
				expectedFilter = gofakeit.LetterN(10)

				filterer.
					EXPECT().
					ParseExpression(expectedFilter).
					Return(nil, errors.New(gofakeit.LetterN(10)))
			})

			It("should not send a request to elasticsearch", func() {
				Expect(transport.receivedHttpRequests).To(HaveLen(0))
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})

		When("elasticsearch successfully returns note(s)", func() {
			It("should return the Grafeas note(s)", func() {
				Expect(actualNotes).ToNot(BeNil())
				Expect(actualNotes).To(Equal(expectedNotes))
			})

			It("should return without an error", func() {
				Expect(actualErr).ToNot(HaveOccurred())
			})
		})

		When("elasticsearch returns zero hits", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusOK,
						Body:       createGenericEsSearchResponse(),
					},
				}
			})

			It("should return an empty slice of notes", func() {
				Expect(actualNotes).To(BeNil())
			})

			It("should not return an error", func() {
				Expect(actualErr).ToNot(HaveOccurred())
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
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})

		When("returns an unexpected response", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusInternalServerError,
					},
				}
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})
	})

	Context("deleting a Grafeas note", func() {
		var (
			actualErr          error
			expectedNotesIndex string
			expectedNoteId     string
			expectedNoteName   string
		)

		BeforeEach(func() {
			expectedNoteId = gofakeit.LetterN(10)
			expectedNotesIndex = fmt.Sprintf("%s-%s-notes", indexPrefix, expectedProjectId)
			expectedNoteName = fmt.Sprintf("projects/%s/notes/%s", expectedProjectId, expectedNoteId)

			transport.preparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body: structToJsonBody(&esDeleteResponse{
						Deleted: 1,
					}),
				},
				{
					StatusCode: http.StatusOK,
				},
			}
		})

		JustBeforeEach(func() {
			actualErr = elasticsearchStorage.DeleteNote(ctx, expectedProjectId, expectedNoteId)
		})

		It("should have sent a request to elasticsearch to delete the note document", func() {
			Expect(transport.receivedHttpRequests).To(HaveLen(1))
			Expect(transport.receivedHttpRequests[0].Method).To(Equal(http.MethodPost))
			Expect(transport.receivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_delete_by_query", expectedNotesIndex)))

			requestBody, err := ioutil.ReadAll(transport.receivedHttpRequests[0].Body)
			Expect(err).ToNot(HaveOccurred())

			searchBody := &esSearch{}
			err = json.Unmarshal(requestBody, searchBody)
			Expect(err).ToNot(HaveOccurred())

			Expect((*searchBody.Query.Term)["name"]).To(Equal(expectedNoteName))
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshTrue), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshTrue
			})

			It("should immediately refresh the index", func() {
				Expect(transport.receivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("true"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshWaitFor), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshWaitFor
			})

			It("should immediately refresh the index", func() {
				Expect(transport.receivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("true"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshFalse), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshFalse
			})

			It("should not wait or force refresh of index", func() {
				Expect(transport.receivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("false"))
			})
		})

		When("elasticsearch successfully deletes the note document", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses[0].Body = structToJsonBody(&esDeleteResponse{
					Deleted: 1,
				})
			})

			It("should not return an error", func() {
				Expect(actualErr).ToNot(HaveOccurred())
			})
		})

		When("the note does not exist", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses[0].Body = structToJsonBody(&esDeleteResponse{
					Deleted: 0,
				})
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})

		When("deleting the note document fails", func() {
			BeforeEach(func() {
				transport.preparedHttpResponses[0].StatusCode = http.StatusInternalServerError
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})
	})
})

func createProjectEsSearchResponse(projects ...*prpb.Project) io.ReadCloser {
	var messages []proto.Message
	for _, p := range projects {
		messages = append(messages, p)
	}

	return createGenericEsSearchResponse(messages...)
}

func createOccurrenceEsSearchResponse(occurrences ...*pb.Occurrence) io.ReadCloser {
	var messages []proto.Message
	for _, p := range occurrences {
		messages = append(messages, p)
	}

	return createGenericEsSearchResponse(messages...)
}

func createNoteEsSearchResponse(notes ...*pb.Note) io.ReadCloser {
	var messages []proto.Message
	for _, p := range notes {
		messages = append(messages, p)
	}

	return createGenericEsSearchResponse(messages...)
}

func createGenericEsSearchResponse(messages ...proto.Message) io.ReadCloser {
	var hits []*esSearchResponseHit

	for _, m := range messages {
		raw, err := protojson.Marshal(proto.MessageV2(m))
		Expect(err).ToNot(HaveOccurred())

		hits = append(hits, &esSearchResponseHit{
			Source: raw,
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

func createEsBulkOccurrenceIndexResponse(occurrences []*pb.Occurrence, errs []error) io.ReadCloser {
	var (
		responseItems     []*esBulkResponseItem
		responseHasErrors = false
	)
	for i := range occurrences {
		var (
			responseErr  *esIndexDocError
			responseCode = http.StatusCreated
		)
		if errs[i] != nil {
			responseErr = &esIndexDocError{
				Type:   gofakeit.LetterN(10),
				Reason: gofakeit.LetterN(10),
			}
			responseCode = http.StatusInternalServerError
			responseHasErrors = true
		}

		responseItems = append(responseItems, &esBulkResponseItem{
			Index: &esIndexDocResponse{
				Id:     gofakeit.LetterN(10),
				Status: responseCode,
				Error:  responseErr,
			},
		})
	}

	response := &esBulkResponse{
		Items:  responseItems,
		Errors: responseHasErrors,
	}

	responseBody, err := json.Marshal(response)
	Expect(err).ToNot(HaveOccurred())

	return ioutil.NopCloser(bytes.NewReader(responseBody))
}

func createEsBulkNoteIndexResponse(notes map[string]*pb.Note, errs []error) io.ReadCloser {
	var (
		responseItems     []*esBulkResponseItem
		responseHasErrors = false
	)
	for i := 0; i < len(notes); i++ {
		var (
			responseErr  *esIndexDocError
			responseCode = http.StatusCreated
		)
		if errs[i] != nil {
			responseErr = &esIndexDocError{
				Type:   gofakeit.LetterN(10),
				Reason: gofakeit.LetterN(10),
			}
			responseCode = http.StatusInternalServerError
			responseHasErrors = true
		}

		responseItems = append(responseItems, &esBulkResponseItem{
			Index: &esIndexDocResponse{
				Id:     gofakeit.LetterN(10),
				Status: responseCode,
				Error:  responseErr,
			},
		})
	}

	response := &esBulkResponse{
		Items:  responseItems,
		Errors: responseHasErrors,
	}

	responseBody, err := json.Marshal(response)
	Expect(err).ToNot(HaveOccurred())

	return ioutil.NopCloser(bytes.NewReader(responseBody))
}

func createEsMsearchNoteResponse(notes map[string]*pb.Note, errs []error) io.ReadCloser {
	var (
		responseItems     []*esBulkResponseItem
		responseHasErrors = false
	)
	for i := 0; i < len(notes); i++ {
		var (
			responseErr  *esIndexDocError
			responseCode = http.StatusCreated
		)
		if errs[i] != nil {
			responseErr = &esIndexDocError{
				Type:   gofakeit.LetterN(10),
				Reason: gofakeit.LetterN(10),
			}
			responseCode = http.StatusInternalServerError
			responseHasErrors = true
		}

		responseItems = append(responseItems, &esBulkResponseItem{
			Index: &esIndexDocResponse{
				Id:     gofakeit.LetterN(10),
				Status: responseCode,
				Error:  responseErr,
			},
		})
	}

	response := &esBulkResponse{
		Items:  responseItems,
		Errors: responseHasErrors,
	}

	responseBody, err := json.Marshal(response)
	Expect(err).ToNot(HaveOccurred())

	return ioutil.NopCloser(bytes.NewReader(responseBody))
}

func generateTestProject(name string) *prpb.Project {
	return &prpb.Project{
		Name: fmt.Sprintf("projects/%s", name),
	}
}

func generateTestProjects(l int) []*prpb.Project {
	var result []*prpb.Project
	for i := 0; i < l; i++ {
		result = append(result, generateTestProject(gofakeit.LetterN(10)))
	}

	return result
}

func generateTestOccurrence(name string) *pb.Occurrence {
	return &pb.Occurrence{
		Name: name,
		Resource: &grafeas_go_proto.Resource{
			Uri: gofakeit.LetterN(10),
		},
		NoteName:    gofakeit.LetterN(10),
		Kind:        common_go_proto.NoteKind_NOTE_KIND_UNSPECIFIED,
		Remediation: gofakeit.LetterN(10),
		Details:     nil,
		CreateTime:  ptypes.TimestampNow(),
	}
}

func generateTestOccurrences(l int) []*pb.Occurrence {
	var result []*pb.Occurrence
	for i := 0; i < l; i++ {
		result = append(result, generateTestOccurrence(""))
	}

	return result
}

func generateTestNote(name string) *pb.Note {
	return &pb.Note{
		Name:             name,
		ShortDescription: gofakeit.Phrase(),
		LongDescription:  gofakeit.Phrase(),
		Kind:             common_go_proto.NoteKind_NOTE_KIND_UNSPECIFIED,
		CreateTime:       ptypes.TimestampNow(),
	}
}

func generateTestNotes(l int, project string) []*pb.Note {
	var result []*pb.Note
	for i := 0; i < l; i++ {
		result = append(result, generateTestNote(fmt.Sprintf("projects/%s/notes/%s", project, gofakeit.LetterN(10))))
	}

	return result
}

func generateTestNotesMap(l int) map[string]*pb.Note {
	result := make(map[string]*pb.Note)
	for i := 0; i < l; i++ {
		noteName := gofakeit.LetterN(10)
		result[noteName] = generateTestNote(noteName)
	}

	return result
}

func updateExpectedNoteKeys(expectedNotes map[string]*pb.Note, project string) map[string]*pb.Note {
	result := make(map[string]*pb.Note)
	for key := range expectedNotes {
		result[fmt.Sprintf("projects/%s/notes/%s", project, key)] = expectedNotes[key]
	}
	return result
}

func structToJsonBody(i interface{}) io.ReadCloser {
	b, err := json.Marshal(i)
	Expect(err).ToNot(HaveOccurred())

	return ioutil.NopCloser(strings.NewReader(string(b)))
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
		"mappings.dynamic_templates.0.strings_as_keywords.match_mapping_type": "string",
		"mappings.dynamic_templates.0.strings_as_keywords.mapping.type":       "keyword",
		"mappings.dynamic_templates.0.strings_as_keywords.mapping.norms":      false,
	})
}

func assertErrorHasGrpcStatusCode(err error, code codes.Code) {
	Expect(err).To(HaveOccurred())
	s, ok := status.FromError(err)

	Expect(ok).To(BeTrue(), "expected error to have been produced from the grpc/status package")
	Expect(s.Code()).To(Equal(code))
}

// parseEsBulkIndexRequest parses a request body in ndjson format
// each line of the body is assumed to be properly formatted JSON
// every odd line is assumed to be a regular JSON structure that can be unmarshalled via json.Unmarshal
// every even line is assumed to be a JSON structure representing a protobuf message, and will be unmarshalled using protojson.Unmarshal
func parseEsBulkIndexRequest(body io.ReadCloser, structs []interface{}) {
	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(body)
	Expect(err).ToNot(HaveOccurred())

	requestPayload := strings.TrimSuffix(buf.String(), "\n") // _bulk requests need to end in a newline
	jsonPayloads := strings.Split(requestPayload, "\n")
	Expect(jsonPayloads).To(HaveLen(len(structs)))

	for i, s := range structs {
		if i%2 == 0 { // regular JSON
			err = json.Unmarshal([]byte(jsonPayloads[i]), s)
		} else { // protobuf JSON
			err = protojson.Unmarshal([]byte(jsonPayloads[i]), proto.MessageV2(s))
		}

		Expect(err).ToNot(HaveOccurred())
	}
}

func parseEsMsearchIndexRequest(body io.ReadCloser, structs []interface{}) {
	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(body)
	Expect(err).ToNot(HaveOccurred())

	requestPayload := strings.TrimSuffix(buf.String(), "\n") // _bulk requests need to end in a newline
	jsonPayloads := strings.Split(requestPayload, "\n")
	Expect(jsonPayloads).To(HaveLen(len(structs)))

	for i, s := range structs {
		err = json.Unmarshal([]byte(jsonPayloads[i]), s)

		Expect(err).ToNot(HaveOccurred())
	}
}

func deepCopyOccurrences(occs []*pb.Occurrence) []*pb.Occurrence {
	var result []*pb.Occurrence
	for _, occ := range occs {
		result = append(result, deepCopyOccurrence(occ))
	}

	return result
}

func deepCopyOccurrence(occ *pb.Occurrence) *pb.Occurrence {
	result := &pb.Occurrence{}

	str, err := protojson.Marshal(proto.MessageV2(occ))
	Expect(err).ToNot(HaveOccurred())

	err = protojson.Unmarshal(str, proto.MessageV2(result))
	Expect(err).ToNot(HaveOccurred())

	return result
}

func deepCopyNotes(notes map[string]*pb.Note) map[string]*pb.Note {
	result := make(map[string]*pb.Note)
	for key, note := range notes {
		result[key] = deepCopyNote(note)
	}

	return result
}

func deepCopyNote(note *pb.Note) *pb.Note {
	result := &pb.Note{}

	str, err := protojson.Marshal(proto.MessageV2(note))
	Expect(err).ToNot(HaveOccurred())

	err = protojson.Unmarshal(str, proto.MessageV2(result))
	Expect(err).ToNot(HaveOccurred())

	return result
}

// helper function for asserting two messages are equivalent based on their JSON representations
// you can use this to get a cleaner error message when a test fails to help determine the difference
// between two messages
func assertProtoMessagesAreEquivalent(m1, m2 proto.Message) {
	m := protojson.MarshalOptions{
		Indent: "  ",
	}

	str1, err := m.Marshal(proto.MessageV2(m1))
	Expect(err).ToNot(HaveOccurred())

	str2, err := m.Marshal(proto.MessageV2(m2))
	Expect(err).ToNot(HaveOccurred())

	Expect(string(str1)).To(BeEquivalentTo(string(str2)))
}

func ioReadCloserToByteSlice(r io.ReadCloser) []byte {
	builder := new(strings.Builder)
	_, err := io.Copy(builder, r)
	Expect(err).ToNot(HaveOccurred())
	return []byte(builder.String())
}
