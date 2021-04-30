// Copyright 2021 The Rode Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/rode/grafeas-elasticsearch/go/v1beta1/storage/esutil/esutilfakes"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/rode/grafeas-elasticsearch/go/config"
	"github.com/rode/grafeas-elasticsearch/go/mocks"
	"github.com/rode/grafeas-elasticsearch/go/v1beta1/storage/esutil"
	"github.com/rode/grafeas-elasticsearch/go/v1beta1/storage/filtering"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	prpb "github.com/grafeas/grafeas/proto/v1beta1/project_go_proto"

	"github.com/Jeffail/gabs/v2"
	"github.com/grafeas/grafeas/proto/v1beta1/common_go_proto"
	"github.com/grafeas/grafeas/proto/v1beta1/grafeas_go_proto"
	pb "github.com/grafeas/grafeas/proto/v1beta1/grafeas_go_proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("elasticsearch storage", func() {
	var (
		elasticsearchStorage *ElasticsearchStorage
		transport            *esutil.MockEsTransport
		ctx                  context.Context

		expectedProjectId    string
		expectedProjectAlias string

		expectedOccurrencesIndex string
		expectedOccurrencesAlias string

		expectedNotesIndex string
		expectedNotesAlias string

		mockCtrl     *gomock.Controller
		filterer     *mocks.MockFilterer
		indexManager *mocks.MockIndexManager
		client       *esutilfakes.FakeClient
		esConfig     *config.ElasticsearchConfig
		orchestrator *mocks.MockOrchestrator
	)

	BeforeEach(func() {
		expectedProjectId = fake.LetterN(10)
		expectedProjectAlias = fake.LetterN(10)
		expectedOccurrencesIndex = fake.LetterN(10)
		expectedOccurrencesAlias = fake.LetterN(10)
		expectedNotesIndex = fake.LetterN(10)
		expectedNotesAlias = fake.LetterN(10)

		ctx = context.Background()

		mockCtrl = gomock.NewController(GinkgoT())
		filterer = mocks.NewMockFilterer(mockCtrl)
		indexManager = mocks.NewMockIndexManager(mockCtrl)
		client = &esutilfakes.FakeClient{}
		orchestrator = mocks.NewMockOrchestrator(mockCtrl)
		transport = &esutil.MockEsTransport{}
		esConfig = &config.ElasticsearchConfig{
			URL:     fake.URL(),
			Refresh: config.RefreshTrue,
		}

		indexManager.EXPECT().ProjectsAlias().Return(expectedProjectAlias).AnyTimes()
		indexManager.EXPECT().OccurrencesIndex(expectedProjectId).Return(expectedOccurrencesIndex).AnyTimes()
		indexManager.EXPECT().OccurrencesAlias(expectedProjectId).Return(expectedOccurrencesAlias).AnyTimes()
		indexManager.EXPECT().NotesIndex(expectedProjectId).Return(expectedNotesIndex).AnyTimes()
		indexManager.EXPECT().NotesAlias(expectedProjectId).Return(expectedNotesAlias).AnyTimes()
	})

	JustBeforeEach(func() {
		elasticsearchStorage = NewElasticsearchStorage(logger, client, filterer, esConfig, indexManager, orchestrator)
	})

	AfterEach(func() {
		mockCtrl.Finish()
	})

	Context("Initialize", func() {
		var (
			expectedProjectsIndex string
			expectedError         error
			actualError           error
		)

		BeforeEach(func() {
			expectedProjectsIndex = fake.LetterN(10)
			indexManager.EXPECT().ProjectsIndex().Return(expectedProjectsIndex).AnyTimes()
			expectedError = fmt.Errorf(fake.Word())
		})

		JustBeforeEach(func() {
			actualError = elasticsearchStorage.Initialize(ctx)
		})

		Describe("successful initialization", func() {
			BeforeEach(func() {
				indexManager.EXPECT().LoadMappings("mappings").Times(1)
				indexManager.EXPECT().CreateIndex(ctx, &esutil.IndexInfo{
					DocumentKind: "projects",
					Index:        expectedProjectsIndex,
					Alias:        expectedProjectAlias,
				}, true).Times(1)

				orchestrator.EXPECT().RunMigrations(ctx).Times(1)
			})

			It("should not return an error", func() {
				Expect(actualError).To(BeNil())
			})
		})

		Describe("an error occurs while loading mappings", func() {
			BeforeEach(func() {
				indexManager.EXPECT().LoadMappings(gomock.Any()).Return(expectedError)
			})

			It("should return the error", func() {
				Expect(actualError).To(MatchError(expectedError))
			})
		})

		Describe("an error occurs while creating the projects index", func() {
			BeforeEach(func() {
				indexManager.EXPECT().LoadMappings(gomock.Any()).AnyTimes()
				indexManager.EXPECT().CreateIndex(gomock.Any(), gomock.Any(), gomock.Any()).Return(expectedError)
			})

			It("should return the error", func() {
				Expect(actualError).To(MatchError(expectedError))
			})
		})

		Describe("an error occurs while running migrations", func() {
			BeforeEach(func() {
				indexManager.EXPECT().LoadMappings(gomock.Any()).AnyTimes()
				indexManager.EXPECT().CreateIndex(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

				orchestrator.EXPECT().RunMigrations(gomock.Any()).Return(expectedError)
			})

			It("should return the error", func() {
				Expect(actualError).To(MatchError(expectedError))
			})
		})
	})

	Context("creating a new Grafeas project", func() {
		var (
			actualErr       error
			actualProject   *prpb.Project
			expectedProject *prpb.Project

			expectedSearchResponse *esutil.SearchResponse
			expectedSearchError    error

			expectedCreateResponseId string
			expectedCreateError      error
		)

		// BeforeEach configures the happy path for this context
		// Variables configured here may be overridden in nested BeforeEach blocks
		BeforeEach(func() {
			expectedProject = generateTestProject(expectedProjectId)
			projectJson, err := protojson.Marshal(proto.MessageV2(expectedProject))
			Expect(err).ToNot(HaveOccurred())

			expectedSearchResponse = &esutil.SearchResponse{
				Hits: &esutil.EsSearchResponseHits{
					Total: &esutil.EsSearchResponseTotal{
						Value: 0,
					},
					Hits: []*esutil.EsSearchResponseHit{
						{
							ID:     fake.LetterN(10),
							Source: projectJson,
						},
					},
				},
			}
			expectedSearchError = nil

			expectedCreateResponseId = fake.LetterN(10)
			expectedCreateError = nil
		})

		// JustBeforeEach actually invokes the system under test
		JustBeforeEach(func() {
			client.SearchReturns(expectedSearchResponse, expectedSearchError)
			client.CreateReturns(expectedCreateResponseId, expectedCreateError)

			actualProject, actualErr = elasticsearchStorage.CreateProject(context.Background(), expectedProjectId, &prpb.Project{})
		})

		Describe("checking if the project document exists", func() {
			BeforeEach(func() {
				indexManager.EXPECT().CreateIndex(context.Background(), gomock.Any(), false).Times(2)
			})

			It("should check if the project document already exists", func() {
				Expect(client.SearchCallCount()).To(Equal(1))

				_, searchRequest := client.SearchArgsForCall(0)
				Expect(searchRequest.Index).To(Equal(expectedProjectAlias))
				Expect((*searchRequest.Search.Query.Term)["name"]).To(Equal(fmt.Sprintf("projects/%s", expectedProjectId)))
			})
		})

		When("the project already exists", func() {
			BeforeEach(func() {
				expectedSearchResponse.Hits.Total.Value = 1
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.AlreadyExists)
				Expect(actualProject).To(BeNil())
			})

			It("should not create any documents or indices for the project", func() {
				Expect(client.CreateCallCount()).To(Equal(0))
			})
		})

		When("checking if the project exists returns an error", func() {
			BeforeEach(func() {
				expectedSearchResponse = nil
				expectedSearchError = errors.New("error")
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
				Expect(actualProject).To(BeNil())
			})

			It("should not create a document or indices", func() {
				Expect(client.CreateCallCount()).To(Equal(0))
			})
		})

		When("the project does not exist", func() {
			Describe("creating the project", func() {
				BeforeEach(func() {
					indexManager.EXPECT().CreateIndex(context.Background(), &esutil.IndexInfo{
						Index:        expectedOccurrencesIndex,
						Alias:        expectedOccurrencesAlias,
						DocumentKind: esutil.OccurrenceDocumentKind,
					}, false).Times(1)

					indexManager.EXPECT().CreateIndex(context.Background(), &esutil.IndexInfo{
						Index:        expectedNotesIndex,
						Alias:        expectedNotesAlias,
						DocumentKind: esutil.NoteDocumentKind,
					}, false).Times(1)
				})

				It("should create a new document for the project", func() {
					Expect(client.CreateCallCount()).To(Equal(1))

					_, createRequest := client.CreateArgsForCall(0)

					Expect(createRequest.Index).To(Equal(expectedProjectAlias))

					project := proto.MessageV1(createRequest.Message).(*prpb.Project)
					Expect(project.Name).To(Equal(fmt.Sprintf("projects/%s", expectedProjectId)))
				})

				It("should return the project", func() {
					Expect(actualProject).ToNot(BeNil())
					Expect(actualProject.Name).To(Equal(fmt.Sprintf("projects/%s", expectedProjectId)))
				})

				When(fmt.Sprintf("refresh configuration is %s", config.RefreshTrue), func() {
					BeforeEach(func() {
						esConfig.Refresh = config.RefreshTrue
					})

					It("should immediately refresh the index", func() {
						_, createRequest := client.CreateArgsForCall(0)

						Expect(createRequest.Refresh).To(Equal("true"))
					})
				})

				When(fmt.Sprintf("refresh configuration is %s", config.RefreshWaitFor), func() {
					BeforeEach(func() {
						esConfig.Refresh = config.RefreshWaitFor
					})

					It("should wait for refresh of index", func() {
						_, createRequest := client.CreateArgsForCall(0)

						Expect(createRequest.Refresh).To(Equal("wait_for"))
					})
				})

				When(fmt.Sprintf("refresh configuration is %s", config.RefreshFalse), func() {
					BeforeEach(func() {
						esConfig.Refresh = config.RefreshFalse
					})

					It("should not wait or force refresh of index", func() {
						_, createRequest := client.CreateArgsForCall(0)

						Expect(createRequest.Refresh).To(Equal("false"))
					})
				})
			})

			When("creating a new document fails", func() {
				BeforeEach(func() {
					expectedCreateError = errors.New("error creating project")

					indexManager.EXPECT().CreateIndex(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
				})

				It("should return an error", func() {
					assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
					Expect(actualProject).To(BeNil())
				})
			})

			When("creating the indices fails", func() {
				BeforeEach(func() {
					indexManager.EXPECT().CreateIndex(gomock.Any(), gomock.Any(), gomock.Any()).Return(fmt.Errorf("foobar"))
				})

				It("should return an error", func() {
					assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
					Expect(actualProject).To(BeNil())
				})
			})
		})
	})

	Context("listing Grafeas projects", func() {
		var (
			actualErr           error
			actualProjects      []*prpb.Project
			actualNextPageToken string

			expectedProjects      []*prpb.Project
			expectedFilter        string
			expectedQuery         *filtering.Query
			expectedPageSize      int
			expectedPageToken     string
			expectedNextPageToken string

			expectedSearchResponse *esutil.SearchResponse
			expectedSearchError    error
		)

		BeforeEach(func() {
			expectedQuery = &filtering.Query{}
			expectedFilter = ""
			expectedProjects = generateTestProjects(fake.Number(2, 5))
			expectedPageSize = fake.Number(10, 20)
			expectedPageToken = fake.LetterN(10)

			var expectedSearchResponseHits []*esutil.EsSearchResponseHit
			for _, project := range expectedProjects {
				json, err := protojson.Marshal(proto.MessageV2(project))
				Expect(err).ToNot(HaveOccurred())

				expectedSearchResponseHits = append(expectedSearchResponseHits, &esutil.EsSearchResponseHit{
					Source: json,
				})
			}
			expectedNextPageToken = fake.LetterN(10)
			expectedSearchResponse = &esutil.SearchResponse{
				Hits: &esutil.EsSearchResponseHits{
					Total: &esutil.EsSearchResponseTotal{
						Value: len(expectedProjects),
					},
					Hits: expectedSearchResponseHits,
				},
				NextPageToken: expectedNextPageToken,
			}
			expectedSearchError = nil
		})

		JustBeforeEach(func() {
			client.SearchReturns(expectedSearchResponse, expectedSearchError)

			actualProjects, actualNextPageToken, actualErr = elasticsearchStorage.ListProjects(ctx, expectedFilter, expectedPageSize, expectedPageToken)
		})

		It("should query elasticsearch for project documents", func() {
			Expect(client.SearchCallCount()).To(Equal(1))

			_, searchRequest := client.SearchArgsForCall(0)

			Expect(searchRequest.Index).To(Equal(expectedProjectAlias))

			Expect(searchRequest.Pagination).ToNot(BeNil())
			Expect(searchRequest.Pagination.Size).To(Equal(expectedPageSize))
			Expect(searchRequest.Pagination.Token).To(Equal(expectedPageToken))

			Expect(searchRequest.Search.Sort).To(BeNil())

			Expect(searchRequest.Search.Query).To(BeNil())
		})

		It("should return the Grafeas project(s) and the next page token", func() {
			Expect(actualErr).ToNot(HaveOccurred())
			Expect(actualProjects).To(Equal(expectedProjects))
			Expect(actualNextPageToken).To(Equal(expectedNextPageToken))
		})

		When("a valid filter is specified", func() {
			BeforeEach(func() {
				expectedQuery = &filtering.Query{
					Term: &filtering.Term{
						fake.LetterN(10): fake.LetterN(10),
					},
				}
				expectedFilter = fake.LetterN(10)

				filterer.
					EXPECT().
					ParseExpression(expectedFilter).
					Return(expectedQuery, nil)
			})

			It("should send the parsed query to elasticsearch", func() {
				Expect(client.SearchCallCount()).To(Equal(1))

				_, searchRequest := client.SearchArgsForCall(0)

				Expect(searchRequest.Search.Query).To(Equal(expectedQuery))
			})
		})

		When("an invalid filter is specified", func() {
			BeforeEach(func() {
				expectedFilter = fake.LetterN(10)

				filterer.
					EXPECT().
					ParseExpression(expectedFilter).
					Return(nil, errors.New(fake.LetterN(10)))
			})

			It("should not send a request to elasticsearch", func() {
				Expect(client.SearchCallCount()).To(Equal(0))
			})

			It("should return an error", func() {
				Expect(actualErr).To(HaveOccurred())
				Expect(actualProjects).To(BeNil())
				Expect(actualNextPageToken).To(BeEmpty())

				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})

		When("the elasticsearch request fails", func() {
			BeforeEach(func() {
				expectedSearchError = errors.New("search failed")
			})

			It("should return an error", func() {
				Expect(actualErr).To(HaveOccurred())
				Expect(actualProjects).To(BeNil())
				Expect(actualNextPageToken).To(BeEmpty())

				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})

		When("elasticsearch returns zero hits", func() {
			BeforeEach(func() {
				expectedSearchResponse.Hits.Total.Value = 0
				expectedSearchResponse.Hits.Hits = []*esutil.EsSearchResponseHit{}
			})

			It("should return an empty array of grafeas projects and no error", func() {
				Expect(actualProjects).To(BeNil())
				Expect(actualErr).ToNot(HaveOccurred())
			})
		})
	})

	Context("retrieving a Grafeas project", func() {
		var (
			actualErr       error
			actualProject   *prpb.Project
			expectedProject *prpb.Project

			expectedSearchResponse *esutil.SearchResponse
			expectedSearchError    error
		)

		BeforeEach(func() {
			expectedProject = generateTestProject(expectedProjectId)
			projectJson, err := protojson.Marshal(proto.MessageV2(expectedProject))
			Expect(err).ToNot(HaveOccurred())

			expectedSearchResponse = &esutil.SearchResponse{
				Hits: &esutil.EsSearchResponseHits{
					Total: &esutil.EsSearchResponseTotal{
						Value: 1,
					},
					Hits: []*esutil.EsSearchResponseHit{
						{
							Source: projectJson,
						},
					},
				},
			}
			expectedSearchError = nil
		})

		JustBeforeEach(func() {
			client.SearchReturns(expectedSearchResponse, expectedSearchError)

			actualProject, actualErr = elasticsearchStorage.GetProject(ctx, expectedProjectId)
		})

		It("should query elasticsearch for the specified project", func() {
			Expect(client.SearchCallCount()).To(Equal(1))

			_, searchRequest := client.SearchArgsForCall(0)

			Expect(searchRequest.Index).To(Equal(expectedProjectAlias))

			Expect(searchRequest.Pagination).To(BeNil())
			Expect(searchRequest.Search.Sort).To(BeNil())

			Expect((*searchRequest.Search.Query.Term)["name"]).To(Equal(fmt.Sprintf("projects/%s", expectedProjectId)))
		})

		It("should return the Grafeas project and no error", func() {
			Expect(actualProject.Name).To(Equal(fmt.Sprintf("projects/%s", expectedProjectId)))
			Expect(actualErr).ToNot(HaveOccurred())
		})

		When("elasticsearch can not find the specified project document", func() {
			BeforeEach(func() {
				expectedSearchResponse.Hits.Total.Value = 0
				expectedSearchResponse.Hits.Hits = []*esutil.EsSearchResponseHit{}
			})

			It("should return a not found error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.NotFound)
			})
		})

		When("elasticsearch returns an error", func() {
			BeforeEach(func() {
				expectedSearchError = errors.New("failed search")
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})
	})

	Context("deleting a Grafeas project", func() {
		var (
			actualErr error

			expectedDeleteDocumentError error

			expectedDeleteIndexError  error
			deleteOccurrenceIndexCall *gomock.Call
			deleteNoteIndexCall       *gomock.Call
		)

		BeforeEach(func() {
			expectedDeleteDocumentError = nil
			expectedDeleteIndexError = nil
		})

		JustBeforeEach(func() {
			client.DeleteReturns(expectedDeleteDocumentError)

			// expect occurrences and notes indices to be deleted on happy path
			deleteOccurrenceIndexCall = indexManager.
				EXPECT().
				DeleteIndex(ctx, expectedOccurrencesIndex).
				AnyTimes().
				Return(expectedDeleteIndexError)

			deleteNoteIndexCall = indexManager.
				EXPECT().
				DeleteIndex(ctx, expectedNotesIndex).
				AnyTimes().
				Return(expectedDeleteIndexError)

			actualErr = elasticsearchStorage.DeleteProject(ctx, expectedProjectId)
		})

		It("should have sent a request to delete the project document", func() {
			Expect(client.DeleteCallCount()).To(Equal(1))

			_, deleteRequest := client.DeleteArgsForCall(0)

			Expect(deleteRequest.Index).To(Equal(expectedProjectAlias))
			Expect((*deleteRequest.Search.Query.Term)["name"]).To(Equal(fmt.Sprintf("projects/%s", expectedProjectId)))
		})

		It("should attempt to delete the indices for notes / occurrences", func() {
			deleteOccurrenceIndexCall.Times(1)
			deleteNoteIndexCall.Times(1)
		})

		It("should not return an error", func() {
			Expect(actualErr).ToNot(HaveOccurred())
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshTrue), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshTrue
			})

			It("should immediately refresh the index", func() {
				_, deleteRequest := client.DeleteArgsForCall(0)

				Expect(deleteRequest.Refresh).To(Equal("true"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshWaitFor), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshWaitFor
			})

			It("should immediately refresh the index", func() {
				_, deleteRequest := client.DeleteArgsForCall(0)

				Expect(deleteRequest.Refresh).To(Equal("wait_for"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshFalse), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshFalse
			})

			It("should not wait for or force refresh of index", func() {
				_, deleteRequest := client.DeleteArgsForCall(0)

				Expect(deleteRequest.Refresh).To(Equal("false"))
			})
		})

		When("elasticsearch fails to delete the indices for notes / occurrences", func() {
			BeforeEach(func() {
				expectedDeleteIndexError = errors.New("delete index error")
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})

		When("deleting the project document fails", func() {
			BeforeEach(func() {
				expectedDeleteDocumentError = errors.New("failed delete")
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})

			It("should not attempt to delete the indices for notes / occurrences", func() {
				deleteOccurrenceIndexCall.Times(0)
				deleteNoteIndexCall.Times(0)
			})
		})
	})

	Context("GetOccurrence", func() {
		var (
			actualErr              error
			actualOccurrence       *pb.Occurrence
			expectedOccurrenceId   string
			expectedOccurrenceName string
			searchResponse         *esutil.SearchResponse
			searchError            error
		)

		BeforeEach(func() {
			expectedOccurrenceId = fake.LetterN(10)
			expectedOccurrenceName = fmt.Sprintf("projects/%s/occurrences/%s", expectedProjectId, expectedOccurrenceId)

			expectedOccurrence := generateTestOccurrence(expectedOccurrenceName)
			occurrenceJson, err := protojson.Marshal(proto.MessageV2(expectedOccurrence))
			Expect(err).NotTo(HaveOccurred())

			searchResponse = &esutil.SearchResponse{
				Hits: &esutil.EsSearchResponseHits{
					Total: &esutil.EsSearchResponseTotal{
						Value: 1,
					},
					Hits: []*esutil.EsSearchResponseHit{
						{
							Source: occurrenceJson,
						},
					},
				},
			}
			searchError = nil
		})

		JustBeforeEach(func() {
			client.SearchReturns(searchResponse, searchError)

			actualOccurrence, actualErr = elasticsearchStorage.GetOccurrence(ctx, expectedProjectId, expectedOccurrenceId)
		})

		It("should query elasticsearch for the specified occurrence", func() {
			Expect(client.SearchCallCount()).To(Equal(1))

			_, request := client.SearchArgsForCall(0)

			Expect(request.Index).To(Equal(expectedOccurrencesAlias))

			Expect((*request.Search.Query.Term)["name"]).To(Equal(expectedOccurrenceName))
			Expect(request.Pagination).To(BeNil())
			Expect(request.Search.Sort).To(BeNil())
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
				searchResponse.Hits.Total.Value = 0
				searchResponse.Hits.Hits = []*esutil.EsSearchResponseHit{}
			})

			It("should return a not found error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.NotFound)
			})
		})

		When("elasticsearch returns an error", func() {
			BeforeEach(func() {
				searchError = errors.New("failed search")
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})
	})

	Context("CreateOccurrence", func() {
		var (
			actualOccurrence   *pb.Occurrence
			expectedOccurrence *pb.Occurrence
			actualErr          error

			expectedSearchResponse *esutil.SearchResponse
			expectedSearchError    error

			expectedCreateResponseId string
			expectedCreateError      error
		)

		BeforeEach(func() {
			expectedOccurrence = generateTestOccurrence("")

			expectedProject := generateTestProject(expectedProjectId)
			projectJson, err := protojson.Marshal(proto.MessageV2(expectedProject))
			Expect(err).ToNot(HaveOccurred())

			expectedSearchResponse = &esutil.SearchResponse{
				Hits: &esutil.EsSearchResponseHits{
					Total: &esutil.EsSearchResponseTotal{
						Value: 1,
					},
					Hits: []*esutil.EsSearchResponseHit{
						{
							Source: projectJson,
						},
					},
				},
			}
			expectedSearchError = nil

			expectedCreateResponseId = fake.LetterN(10)
			expectedCreateError = nil
		})

		JustBeforeEach(func() {
			occurrence := deepCopyOccurrence(expectedOccurrence)
			client.SearchReturns(expectedSearchResponse, expectedSearchError)
			client.CreateReturns(expectedCreateResponseId, expectedCreateError)

			actualOccurrence, actualErr = elasticsearchStorage.CreateOccurrence(context.Background(), expectedProjectId, "", occurrence)
		})

		It("should check that the occurrence's project exists", func() {
			Expect(client.SearchCallCount()).To(Equal(1))

			_, searchRequest := client.SearchArgsForCall(0)
			Expect(searchRequest.Index).To(Equal(expectedProjectAlias))
			Expect((*searchRequest.Search.Query.Term)["name"]).To(Equal("projects/" + expectedProjectId))
			Expect(searchRequest.Pagination).To(BeNil())
			Expect(searchRequest.Search.Sort).To(BeNil())
		})

		It("should attempt to index the occurrence as a document", func() {
			Expect(client.CreateCallCount()).To(Equal(1))

			_, createRequest := client.CreateArgsForCall(0)
			Expect(createRequest.Index).To(Equal(expectedOccurrencesAlias))

			occurrence := proto.MessageV1(createRequest.Message).(*grafeas_go_proto.Occurrence)
			Expect(occurrence.Name).To(ContainSubstring("projects/" + expectedProjectId + "/occurrences/"))
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshTrue), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshTrue
			})

			It("should immediately refresh the index", func() {
				Expect(client.CreateCallCount()).To(Equal(1))

				_, createRequest := client.CreateArgsForCall(0)
				Expect(createRequest.Refresh).To(Equal("true"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshWaitFor), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshWaitFor
			})

			It("should wait for refresh of index", func() {
				Expect(client.CreateCallCount()).To(Equal(1))

				_, createRequest := client.CreateArgsForCall(0)
				Expect(createRequest.Refresh).To(Equal("wait_for"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshFalse), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshFalse
			})

			It("should not wait or force refresh of index", func() {
				Expect(client.CreateCallCount()).To(Equal(1))

				_, createRequest := client.CreateArgsForCall(0)
				Expect(createRequest.Refresh).To(Equal("false"))
			})
		})

		When("the occurrence's project doesn't exist", func() {
			BeforeEach(func() {
				expectedSearchResponse.Hits.Total.Value = 0
				expectedSearchResponse.Hits.Hits = []*esutil.EsSearchResponseHit{}
			})

			It("should return an error", func() {
				Expect(actualOccurrence).To(BeNil())
				assertErrorHasGrpcStatusCode(actualErr, codes.FailedPrecondition)
			})
		})

		When("indexing the document fails", func() {
			BeforeEach(func() {
				expectedCreateError = errors.New("create failed")
			})

			It("should return an error", func() {
				Expect(actualOccurrence).To(BeNil())
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
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

	Context("BatchCreateOccurrences", func() {
		var (
			actualErrs          []error
			actualOccurrences   []*pb.Occurrence
			expectedOccurrences []*pb.Occurrence

			expectedSearchResponse *esutil.SearchResponse
			expectedSearchError    error

			expectedBulkCreateResponse *esutil.EsBulkResponse
			expectedBulkCreateError    error
		)

		BeforeEach(func() {
			expectedProject := generateTestProject(expectedProjectId)
			projectJson, err := protojson.Marshal(proto.MessageV2(expectedProject))
			Expect(err).ToNot(HaveOccurred())

			expectedSearchResponse = &esutil.SearchResponse{
				Hits: &esutil.EsSearchResponseHits{
					Total: &esutil.EsSearchResponseTotal{
						Value: 1,
					},
					Hits: []*esutil.EsSearchResponseHit{
						{
							Source: projectJson,
						},
					},
				},
			}
			expectedSearchError = nil

			expectedOccurrences = generateTestOccurrences(fake.Number(2, 5))
			var expectedBulkResponseItems []*esutil.EsBulkResponseItem
			for i := 0; i < len(expectedOccurrences); i++ {
				expectedBulkResponseItems = append(expectedBulkResponseItems, &esutil.EsBulkResponseItem{
					Index: &esutil.EsIndexDocResponse{
						Error: nil,
					},
				})
			}

			expectedBulkCreateResponse = &esutil.EsBulkResponse{
				Items:  expectedBulkResponseItems,
				Errors: false,
			}
		})

		JustBeforeEach(func() {
			occurrences := deepCopyOccurrences(expectedOccurrences)

			client.SearchReturns(expectedSearchResponse, expectedSearchError)
			client.BulkCreateReturns(expectedBulkCreateResponse, expectedBulkCreateError)

			actualOccurrences, actualErrs = elasticsearchStorage.BatchCreateOccurrences(context.Background(), expectedProjectId, "", occurrences)
		})

		It("should send a bulk request to ES to index each occurrence", func() {
			Expect(client.BulkCreateCallCount()).To(Equal(1))

			_, bulkCreateRequest := client.BulkCreateArgsForCall(0)
			Expect(bulkCreateRequest.Index).To(Equal(expectedOccurrencesAlias))

			for i, bulkCreateRequestItem := range bulkCreateRequest.Items {
				occurrence := proto.MessageV1(bulkCreateRequestItem.Message).(*grafeas_go_proto.Occurrence)
				expectedOccurrence := expectedOccurrences[i]
				expectedOccurrence.Name = occurrence.Name

				Expect(occurrence).To(Equal(expectedOccurrence))
			}
		})

		It("should check that the occurrence's project exists", func() {
			Expect(client.SearchCallCount()).To(Equal(1))

			_, searchRequest := client.SearchArgsForCall(0)
			Expect(searchRequest.Index).To(Equal(expectedProjectAlias))
			Expect((*searchRequest.Search.Query.Term)["name"]).To(Equal("projects/" + expectedProjectId))
			Expect(searchRequest.Pagination).To(BeNil())
			Expect(searchRequest.Search.Sort).To(BeNil())
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshTrue), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshTrue
			})

			It("should immediately refresh the index", func() {
				Expect(client.BulkCreateCallCount()).To(Equal(1))

				_, bulkCreateRequest := client.BulkCreateArgsForCall(0)
				Expect(bulkCreateRequest.Refresh).To(Equal("true"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshWaitFor), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshWaitFor
			})

			It("should wait for refresh of index", func() {
				Expect(client.BulkCreateCallCount()).To(Equal(1))

				_, bulkCreateRequest := client.BulkCreateArgsForCall(0)
				Expect(bulkCreateRequest.Refresh).To(Equal("wait_for"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshFalse), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshFalse
			})

			It("should not wait or force refresh of index", func() {
				Expect(client.BulkCreateCallCount()).To(Equal(1))

				_, bulkCreateRequest := client.BulkCreateArgsForCall(0)
				Expect(bulkCreateRequest.Refresh).To(Equal("false"))
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

		When("the occurrence's project doesn't exist", func() {
			BeforeEach(func() {
				expectedSearchResponse.Hits.Total.Value = 0
				expectedSearchResponse.Hits.Hits = []*esutil.EsSearchResponseHit{}
			})

			It("should return an error", func() {
				Expect(actualOccurrences).To(BeNil())
				Expect(actualErrs).To(HaveLen(1))
				assertErrorHasGrpcStatusCode(actualErrs[0], codes.FailedPrecondition)
			})
		})

		When("the bulk request completely fails", func() {
			BeforeEach(func() {
				expectedBulkCreateError = errors.New("bulk create failed")
			})

			It("should return a single error and no occurrences", func() {
				Expect(actualOccurrences).To(BeNil())
				Expect(actualErrs).To(HaveLen(1))
				assertErrorHasGrpcStatusCode(actualErrs[0], codes.Internal)
			})
		})

		When("the bulk request returns some errors", func() {
			var randomErrorIndex int

			BeforeEach(func() {
				randomErrorIndex = fake.Number(0, len(expectedOccurrences)-1)
				expectedBulkCreateResponse.Items[randomErrorIndex].Index.Error = &esutil.EsIndexDocError{
					Type:   "error",
					Reason: "error",
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
				assertErrorHasGrpcStatusCode(actualErrs[0], codes.Internal)
			})
		})
	})

	Context("UpdateOccurrence", func() {
		var (
			currentOccurrence *pb.Occurrence

			expectedOccurrence     *pb.Occurrence
			occurrencePatchData    *pb.Occurrence
			expectedOccurrenceId   string
			expectedOccurrenceName string
			expectedDocumentId     string
			fieldMask              *fieldmaskpb.FieldMask
			actualErr              error
			actualOccurrence       *pb.Occurrence

			expectedSearchResponse *esutil.SearchResponse
			expectedSearchError    error

			expectedUpdateError error
		)

		BeforeEach(func() {
			expectedDocumentId = fake.LetterN(10)
			expectedOccurrenceId = fake.LetterN(10)
			expectedOccurrenceName = fmt.Sprintf("projects/%s/occurrences/%s", expectedProjectId, expectedOccurrenceId)
			currentOccurrence = generateTestOccurrence("")
			occurrencePatchData = &grafeas_go_proto.Occurrence{
				Resource: &grafeas_go_proto.Resource{
					Uri: "updatedvalue",
				},
			}
			fieldMask = &fieldmaskpb.FieldMask{
				Paths: []string{"resource.uri"},
			}
			expectedOccurrence = currentOccurrence
			expectedOccurrence.Resource.Uri = "updatedvalue"

			occurrenceJson, err := protojson.Marshal(proto.MessageV2(expectedOccurrence))
			Expect(err).ToNot(HaveOccurred())

			expectedSearchResponse = &esutil.SearchResponse{
				Hits: &esutil.EsSearchResponseHits{
					Total: &esutil.EsSearchResponseTotal{
						Value: 1,
					},
					Hits: []*esutil.EsSearchResponseHit{
						{
							ID:     expectedDocumentId,
							Source: occurrenceJson,
						},
					},
				},
			}
			expectedSearchError = nil
			expectedUpdateError = nil
		})

		JustBeforeEach(func() {
			client.SearchReturns(expectedSearchResponse, expectedSearchError)
			client.UpdateReturns(expectedUpdateError)
			actualOccurrence, actualErr = elasticsearchStorage.UpdateOccurrence(context.Background(), expectedProjectId, expectedOccurrenceId, occurrencePatchData, fieldMask)
		})

		It("should have sent a request to elasticsearch to retrieve the occurrence document", func() {
			Expect(client.SearchCallCount()).To(Equal(1))

			_, searchRequest := client.SearchArgsForCall(0)

			Expect(searchRequest.Index).To(Equal(expectedOccurrencesAlias))

			Expect((*searchRequest.Search.Query.Term)["name"]).To(Equal(expectedOccurrenceName))
			Expect(searchRequest.Pagination).To(BeNil())
			Expect(searchRequest.Search.Sort).To(BeNil())
		})

		It("should have sent a request to elasticsearch to update the occurrence document", func() {
			Expect(client.UpdateCallCount()).To(Equal(1))

			_, updateRequest := client.UpdateArgsForCall(0)

			Expect(updateRequest.Index).To(Equal(expectedOccurrencesAlias))
			Expect(updateRequest.DocumentId).To(Equal(expectedDocumentId))

			occurrence := proto.MessageV1(updateRequest.Message).(*grafeas_go_proto.Occurrence)
			Expect(occurrence.Resource.Uri).To(Equal("updatedvalue"))
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshTrue), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshTrue
			})

			It("should immediately refresh the index", func() {
				Expect(client.UpdateCallCount()).To(Equal(1))

				_, updateRequest := client.UpdateArgsForCall(0)
				Expect(updateRequest.Refresh).To(Equal("true"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshWaitFor), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshWaitFor
			})

			It("should immediately refresh the index", func() {
				Expect(client.UpdateCallCount()).To(Equal(1))

				_, updateRequest := client.UpdateArgsForCall(0)
				Expect(updateRequest.Refresh).To(Equal("wait_for"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshFalse), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshFalse
			})

			It("should not wait or force refresh of index", func() {
				Expect(client.UpdateCallCount()).To(Equal(1))

				_, updateRequest := client.UpdateArgsForCall(0)
				Expect(updateRequest.Refresh).To(Equal("false"))
			})
		})

		When("elasticsearch successfully updates the occurrence document", func() {
			It("should not return an error", func() {
				Expect(actualErr).ToNot(HaveOccurred())
			})
			It("should contain the newly added field", func() {
				Expect(actualOccurrence.Resource.Uri).To(BeEquivalentTo(expectedOccurrence.Resource.Uri))
			})
			It("should have an updated UpdateTime field", func() {
				Expect(actualOccurrence.UpdateTime).ToNot(Equal(expectedOccurrence.UpdateTime))
			})
		})

		When("the occurrence does not exist", func() {
			BeforeEach(func() {
				expectedSearchResponse.Hits.Total.Value = 0
				expectedSearchResponse.Hits.Hits = []*esutil.EsSearchResponseHit{}
			})

			It("should return a not found error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.NotFound)
			})
		})

		When("elasticsearch fails to update the occurrence document", func() {
			BeforeEach(func() {
				expectedUpdateError = errors.New("update failed")
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})

		When("using a badly formatted field mask", func() {
			BeforeEach(func() {
				fieldMask = &fieldmaskpb.FieldMask{
					Paths: []string{"resource..bro"},
				}
			})
			It("should return an error", func() {
				Expect(actualErr).To(HaveOccurred())
			})
		})
	})

	Context("DeleteOccurrence", func() {
		var (
			actualErr              error
			expectedOccurrenceId   string
			expectedOccurrenceName string

			expectedDeleteError error
		)

		BeforeEach(func() {
			expectedOccurrenceId = fake.LetterN(10)
			expectedOccurrenceName = fmt.Sprintf("projects/%s/occurrences/%s", expectedProjectId, expectedOccurrenceId)

			expectedDeleteError = nil
		})

		JustBeforeEach(func() {
			client.DeleteReturns(expectedDeleteError)

			actualErr = elasticsearchStorage.DeleteOccurrence(ctx, expectedProjectId, expectedOccurrenceId)
		})

		It("should have sent a request to elasticsearch to delete the occurrence document", func() {
			Expect(client.DeleteCallCount()).To(Equal(1))

			_, deleteRequest := client.DeleteArgsForCall(0)

			Expect(deleteRequest.Index).To(Equal(expectedOccurrencesAlias))
			Expect((*deleteRequest.Search.Query.Term)["name"]).To(Equal(expectedOccurrenceName))
			Expect(deleteRequest.Search.Sort).To(BeNil())
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshTrue), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshTrue
			})

			It("should immediately refresh the index", func() {
				Expect(client.DeleteCallCount()).To(Equal(1))

				_, deleteRequest := client.DeleteArgsForCall(0)

				Expect(deleteRequest.Refresh).To(Equal("true"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshWaitFor), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshWaitFor
			})

			It("should immediately refresh the index", func() {
				Expect(client.DeleteCallCount()).To(Equal(1))

				_, deleteRequest := client.DeleteArgsForCall(0)

				Expect(deleteRequest.Refresh).To(Equal("wait_for"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshFalse), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshFalse
			})

			It("should not wait or force refresh of index", func() {
				Expect(client.DeleteCallCount()).To(Equal(1))

				_, deleteRequest := client.DeleteArgsForCall(0)

				Expect(deleteRequest.Refresh).To(Equal("false"))
			})
		})

		When("elasticsearch successfully deletes the occurrence document", func() {
			It("should not return an error", func() {
				Expect(actualErr).ToNot(HaveOccurred())
			})
		})

		When("deleting the occurrence document fails", func() {
			BeforeEach(func() {
				expectedDeleteError = errors.New("delete failed")
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})
	})

	Context("ListOccurrences", func() {
		var (
			actualErr           error
			actualNextPageToken string
			actualOccurrences   []*pb.Occurrence

			expectedOccurrences   []*pb.Occurrence
			expectedFilter        string
			expectedQuery         *filtering.Query
			expectedPageSize      int
			expectedPageToken     string
			expectedNextPageToken string

			expectedSearchResponse *esutil.SearchResponse
			expectedSearchError    error
		)

		BeforeEach(func() {
			expectedQuery = &filtering.Query{}
			expectedFilter = ""
			expectedOccurrences = generateTestOccurrences(fake.Number(2, 5))
			expectedPageSize = fake.Number(10, 20)
			expectedPageToken = fake.LetterN(10)

			var expectedSearchResponseHits []*esutil.EsSearchResponseHit
			for _, occurrence := range expectedOccurrences {
				json, err := protojson.Marshal(proto.MessageV2(occurrence))
				Expect(err).NotTo(HaveOccurred())

				expectedSearchResponseHits = append(expectedSearchResponseHits, &esutil.EsSearchResponseHit{
					Source: json,
				})
			}
			expectedNextPageToken = fake.LetterN(10)
			expectedSearchResponse = &esutil.SearchResponse{
				Hits: &esutil.EsSearchResponseHits{
					Total: &esutil.EsSearchResponseTotal{
						Value: len(expectedOccurrences),
					},
					Hits: expectedSearchResponseHits,
				},
				NextPageToken: expectedNextPageToken,
			}

			expectedSearchError = nil
		})

		JustBeforeEach(func() {
			client.SearchReturns(expectedSearchResponse, expectedSearchError)
			actualOccurrences, actualNextPageToken, actualErr = elasticsearchStorage.ListOccurrences(ctx, expectedProjectId, expectedFilter, expectedPageToken, int32(expectedPageSize))
		})

		It("should query elasticsearch for occurrences", func() {
			Expect(client.SearchCallCount()).To(Equal(1))

			_, searchRequest := client.SearchArgsForCall(0)

			Expect(searchRequest.Index).To(Equal(expectedOccurrencesAlias))

			Expect(searchRequest.Pagination).ToNot(BeNil())
			Expect(searchRequest.Pagination.Size).To(Equal(expectedPageSize))
			Expect(searchRequest.Pagination.Token).To(Equal(expectedPageToken))

			Expect(searchRequest.Search.Sort).NotTo(BeNil())
			Expect(searchRequest.Search.Sort[sortField]).To(Equal(esutil.EsSortOrderDescending))
			Expect(searchRequest.Search.Query).To(BeNil())
		})

		When("a valid filter is specified", func() {
			BeforeEach(func() {
				expectedQuery = &filtering.Query{
					Term: &filtering.Term{
						fake.LetterN(10): fake.LetterN(10),
					},
				}
				expectedFilter = fake.LetterN(10)

				filterer.
					EXPECT().
					ParseExpression(expectedFilter).
					Return(expectedQuery, nil)
			})

			It("should send the parsed query to elasticsearch", func() {
				Expect(client.SearchCallCount()).To(Equal(1))

				_, searchRequest := client.SearchArgsForCall(0)

				Expect(searchRequest.Search.Query).To(Equal(expectedQuery))
			})
		})

		When("an invalid filter is specified", func() {
			BeforeEach(func() {
				expectedFilter = fake.LetterN(10)

				filterer.
					EXPECT().
					ParseExpression(expectedFilter).
					Return(nil, errors.New(fake.LetterN(10)))
			})

			It("should not send a request to elasticsearch", func() {
				Expect(client.SearchCallCount()).To(Equal(0))
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
				Expect(actualOccurrences).To(BeNil())
				Expect(actualNextPageToken).To(BeEmpty())
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
				expectedSearchResponse.Hits.Total.Value = 0
				expectedSearchResponse.Hits.Hits = []*esutil.EsSearchResponseHit{}
			})

			It("should return an empty slice of grafeas occurrences", func() {
				Expect(actualOccurrences).To(BeNil())
			})

			It("should not return an error", func() {
				Expect(actualErr).ToNot(HaveOccurred())
			})
		})

		When("returns an unexpected response", func() {
			BeforeEach(func() {
				expectedSearchError = errors.New("search error")
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})
	})

	Context("creating a new Grafeas note", func() {
		var (
			actualNote       *pb.Note
			expectedNote     *pb.Note
			expectedNoteId   string
			expectedNoteName string
			expectedNoteESId string
			actualErr        error
		)

		// BeforeEach configures the happy path for this context
		// Variables configured here may be overridden in nested BeforeEach blocks
		BeforeEach(func() {
			// this setup is a bit different when compared to the tests for creating occurrences
			// grafeas requires that the user specify a note's ID (and thus its name) beforehand
			expectedNoteId = fake.LetterN(10)
			expectedNoteName = fmt.Sprintf("projects/%s/notes/%s", expectedProjectId, expectedNoteId)
			expectedNote = generateTestNote(expectedNoteName)
			expectedNoteESId = fake.LetterN(10)

			transport.PreparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body:       createEsSearchResponse("project", fake.LetterN(10)),
				},
				{
					StatusCode: http.StatusOK,
					Body:       createGenericEsSearchResponse(), // happy path: a note with this ID does not exist (0 hits), so we create one
				},
				{
					StatusCode: http.StatusCreated,
					Body: structToJsonBody(&esutil.EsIndexDocResponse{
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

		It("should check that the occurrence's project exists", func() {
			Expect(transport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodGet))
			Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_search", expectedProjectAlias)))
			requestBody, err := ioutil.ReadAll(transport.ReceivedHttpRequests[0].Body)
			Expect(err).ToNot(HaveOccurred())

			searchBody := &esutil.EsSearch{}
			err = json.Unmarshal(requestBody, searchBody)
			expectedProject := &esutil.EsSearch{
				Query: &filtering.Query{
					Term: &filtering.Term{
						"name": fmt.Sprintf("projects/%s", expectedProjectId),
					},
				},
			}
			Expect(searchBody).To(Equal(expectedProject))
		})

		It("should check elasticsearch to see if a note with the specified noteId already exists", func() {
			Expect(transport.ReceivedHttpRequests[1].URL.Path).To(Equal(fmt.Sprintf("/%s/_search", expectedNotesAlias)))
			Expect(transport.ReceivedHttpRequests[1].Method).To(Equal(http.MethodGet))

			requestBody, err := ioutil.ReadAll(transport.ReceivedHttpRequests[1].Body)
			Expect(err).ToNot(HaveOccurred())

			searchBody := &esutil.EsSearch{}
			err = json.Unmarshal(requestBody, searchBody)
			Expect(err).ToNot(HaveOccurred())

			Expect((*searchBody.Query.Term)["name"]).To(Equal(expectedNoteName))
		})

		When("the notes project doesn't exist", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[0].StatusCode = http.StatusNotFound
			})
			It("should return an error", func() {
				Expect(actualNote).To(BeNil())
				Expect(actualErr).ToNot(BeNil())
			})
		})

		When("a note with the specified noteId does not exist", func() {
			It("should attempt to index the note as a document", func() {
				Expect(transport.ReceivedHttpRequests).To(HaveLen(3))

				Expect(transport.ReceivedHttpRequests[2].URL.Path).To(Equal(fmt.Sprintf("/%s/_doc", expectedNotesAlias)))

				requestBody, err := ioutil.ReadAll(transport.ReceivedHttpRequests[2].Body)
				Expect(err).ToNot(HaveOccurred())

				indexedNote := &pb.Note{}
				err = protojson.Unmarshal(requestBody, proto.MessageV2(indexedNote))
				Expect(err).ToNot(HaveOccurred())

				Expect(indexedNote).To(BeEquivalentTo(expectedNote))
			})

			When("indexing the document fails", func() {
				BeforeEach(func() {
					transport.PreparedHttpResponses[1] = &http.Response{
						StatusCode: http.StatusInternalServerError,
						Body: structToJsonBody(&esutil.EsIndexDocResponse{
							Error: &esutil.EsIndexDocError{
								Type:   fake.LetterN(10),
								Reason: fake.LetterN(10),
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
					Expect(transport.ReceivedHttpRequests[2].URL.Query().Get("refresh")).To(Equal("true"))
				})
			})

			When(fmt.Sprintf("refresh configuration is %s", config.RefreshWaitFor), func() {
				BeforeEach(func() {
					esConfig.Refresh = config.RefreshWaitFor
				})

				It("should wait for refresh of index", func() {
					Expect(transport.ReceivedHttpRequests[2].URL.Query().Get("refresh")).To(Equal("wait_for"))
				})
			})

			When(fmt.Sprintf("refresh configuration is %s", config.RefreshFalse), func() {
				BeforeEach(func() {
					esConfig.Refresh = config.RefreshFalse
				})

				It("should not wait or force refresh of index", func() {
					Expect(transport.ReceivedHttpRequests[2].URL.Query().Get("refresh")).To(Equal("false"))
				})
			})
		})

		When("a note with the specified noteId exists", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[1].Body = createGenericEsSearchResponse(&pb.Note{
					Name: expectedNoteName,
				})
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.AlreadyExists)
			})

			It("should not attempt to index a note document", func() {
				Expect(transport.ReceivedHttpRequests).To(HaveLen(2))
			})
		})

		When("checking for the existence of the note fails", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[1].StatusCode = http.StatusInternalServerError
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})

			It("should not attempt to index a note document", func() {
				Expect(transport.ReceivedHttpRequests).To(HaveLen(2))
			})
		})

		When("elasticsearch returns a bad response when checking if a note exists", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[1].Body = ioutil.NopCloser(strings.NewReader("bad object"))
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})

			It("should not attempt to index a note document", func() {
				Expect(transport.ReceivedHttpRequests).To(HaveLen(2))
			})
		})
	})

	// unit tests for BatchCreateNotes only cover the happy path for now
	Context("creating a batch of Grafeas notes", func() {
		var (
			actualErrs               []error
			actualNotes              []*pb.Note
			expectedNotes            []*pb.Note
			expectedNotesWithNoteIds map[string]*pb.Note
		)

		// BeforeEach configures the happy path for this context
		// Variables configured here may be overridden in nested BeforeEach blocks
		BeforeEach(func() {
			expectedNotes = generateTestNotes(fake.Number(2, 5), expectedProjectId)
			expectedNotesWithNoteIds = convertSliceOfNotesToMap(expectedNotes)

			// happy path: none of the provided notes exist, all of the provided notes were created successfully
			// response 1: every search result will have zero hits
			// response 2: every index operation was successful
			transport.PreparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body:       createEsSearchResponse("project", fake.LetterN(10)),
				},
				{
					StatusCode: http.StatusOK,
					Body:       createEsMultiSearchNoteResponse(expectedNotesWithNoteIds),
				},
				{
					StatusCode: http.StatusOK,
					Body:       createEsBulkNoteIndexResponse(expectedNotesWithNoteIds),
				},
			}
		})

		// JustBeforeEach actually invokes the system under test
		JustBeforeEach(func() {
			actualNotes, actualErrs = elasticsearchStorage.BatchCreateNotes(context.Background(), expectedProjectId, "", deepCopyNotes(expectedNotesWithNoteIds))
		})

		// this test parses the ndjson request body and ensures that it was formatted correctly
		It("should send a multisearch request to ES to check for the existence of each note", func() {
			var expectedPayloads []interface{}

			for i := 0; i < len(expectedNotesWithNoteIds); i++ {
				expectedPayloads = append(expectedPayloads, &esutil.EsMultiSearchQueryFragment{}, &esutil.EsSearch{})
			}

			parseEsMsearchIndexRequest(transport.ReceivedHttpRequests[1].Body, expectedPayloads)

			for i, payload := range expectedPayloads {
				if i%2 == 0 { // index metadata
					metadata := payload.(*esutil.EsMultiSearchQueryFragment)
					Expect(metadata.Index).To(Equal(expectedNotesAlias))
				} else { // note
					Expect(payload).To(BeAssignableToTypeOf(&esutil.EsSearch{}))
					Expect(map[string]string(*payload.(*esutil.EsSearch).Query.Term)["name"]).To(MatchRegexp("projects/%s/notes/\\w+", expectedProjectId))
				}
			}
		})

		It("should check that the notes project exists", func() {
			Expect(transport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodGet))
			Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_search", expectedProjectAlias)))
			requestBody, err := ioutil.ReadAll(transport.ReceivedHttpRequests[0].Body)
			Expect(err).ToNot(HaveOccurred())

			searchBody := &esutil.EsSearch{}
			err = json.Unmarshal(requestBody, searchBody)
			expectedProject := &esutil.EsSearch{
				Query: &filtering.Query{
					Term: &filtering.Term{
						"name": fmt.Sprintf("projects/%s", expectedProjectId),
					},
				},
			}
			Expect(searchBody).To(Equal(expectedProject))
		})

		When("the notes project doesn't exist", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[0].StatusCode = http.StatusNotFound
			})
			It("should return an error", func() {
				Expect(actualNotes).To(BeNil())
				Expect(actualErrs).To(HaveLen(1))
			})
		})

		When("the multisearch request returns an error", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[1].StatusCode = http.StatusInternalServerError
			})

			It("should return a single error and no notes", func() {
				Expect(actualErrs).To(HaveLen(1))
				assertErrorHasGrpcStatusCode(actualErrs[0], codes.Internal)
			})

			It("should not attempt to index any notes", func() {
				Expect(transport.ReceivedHttpRequests).To(HaveLen(2))
			})
		})

		When("the multisearch request returns no notes already exist", func() {
			It("should attempt to bulk index all of the notes", func() {
				var expectedPayloads []interface{}

				for i := 0; i < len(expectedNotes); i++ {
					expectedPayloads = append(expectedPayloads, &esutil.EsBulkQueryFragment{}, &pb.Note{})
				}

				parseEsBulkIndexRequest(transport.ReceivedHttpRequests[2].Body, expectedPayloads)

				for i, payload := range expectedPayloads {
					if i%2 == 0 { // index metadata
						metadata := payload.(*esutil.EsBulkQueryFragment)
						Expect(metadata.Index.Index).To(Equal(expectedNotesAlias))
					} else { // note
						note := payload.(*pb.Note)
						noteId := strings.Split(note.Name, "/")[3] // projects/${projectId}/notes/${noteId}
						expectedNote := expectedNotesWithNoteIds[noteId]
						expectedNote.Name = note.Name

						Expect(note).To(Equal(expectedNote))
					}
				}
			})

			When("the bulk index request returns no errors", func() {
				It("should return all created notes", func() {
					for _, note := range expectedNotesWithNoteIds {
						Expect(actualNotes).To(ContainElement(note))
					}
				})
			})

			When("the bulk index request completely fails", func() {
				BeforeEach(func() {
					transport.PreparedHttpResponses[2].StatusCode = http.StatusInternalServerError
				})

				It("should return a single error and no notes", func() {
					Expect(actualErrs).To(HaveLen(1))
					assertErrorHasGrpcStatusCode(actualErrs[0], codes.Internal)
				})
			})

			When(fmt.Sprintf("refresh configuration is %s", config.RefreshTrue), func() {
				BeforeEach(func() {
					esConfig.Refresh = config.RefreshTrue
				})

				It("should immediately refresh the index", func() {
					Expect(transport.ReceivedHttpRequests[2].URL.Query().Get("refresh")).To(Equal("true"))
				})
			})

			When(fmt.Sprintf("refresh configuration is %s", config.RefreshWaitFor), func() {
				BeforeEach(func() {
					esConfig.Refresh = config.RefreshWaitFor
				})

				It("should wait for refresh of index", func() {
					Expect(transport.ReceivedHttpRequests[2].URL.Query().Get("refresh")).To(Equal("wait_for"))
				})
			})

			When(fmt.Sprintf("refresh configuration is %s", config.RefreshFalse), func() {
				BeforeEach(func() {
					esConfig.Refresh = config.RefreshFalse
				})

				It("should not wait or force refresh of index", func() {
					Expect(transport.ReceivedHttpRequests[2].URL.Query().Get("refresh")).To(Equal("false"))
				})
			})
		})
	})

	Context("retrieving a Grafeas note", func() {
		var (
			actualErr        error
			actualNote       *pb.Note
			expectedNoteId   string
			expectedNoteName string
		)

		BeforeEach(func() {
			expectedNoteId = fake.LetterN(10)
			expectedNoteName = fmt.Sprintf("projects/%s/notes/%s", expectedProjectId, expectedNoteId)
			transport.PreparedHttpResponses = []*http.Response{
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
			Expect(transport.ReceivedHttpRequests).To(HaveLen(1))

			Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_search", expectedNotesAlias)))
			Expect(transport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodGet))

			requestBody, err := ioutil.ReadAll(transport.ReceivedHttpRequests[0].Body)
			Expect(err).ToNot(HaveOccurred())

			searchBody := &esutil.EsSearch{}
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
				transport.PreparedHttpResponses = []*http.Response{
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
				transport.PreparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusOK,
						Body:       ioutil.NopCloser(strings.NewReader(fake.LetterN(10))),
					},
				}
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})

		When("elasticsearch returns an error", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses = []*http.Response{
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
			actualErr      error
			actualNotes    []*pb.Note
			expectedNotes  []*pb.Note
			expectedFilter string
			expectedQuery  *filtering.Query
		)

		BeforeEach(func() {
			expectedQuery = &filtering.Query{}
			expectedFilter = ""
			expectedNotes = generateTestNotes(fake.Number(2, 5), expectedProjectId)
			transport.PreparedHttpResponses = []*http.Response{
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
			Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_search", expectedNotesAlias)))
			Expect(transport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodGet))
			Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("size")).To(Equal(strconv.Itoa(grafeasMaxPageSize)))

			requestBody, err := ioutil.ReadAll(transport.ReceivedHttpRequests[0].Body)
			Expect(err).ToNot(HaveOccurred())

			searchBody := &esutil.EsSearch{}
			err = json.Unmarshal(requestBody, searchBody)
			Expect(err).ToNot(HaveOccurred())
			Expect(searchBody.Query).To(BeNil())
			Expect(searchBody.Sort[sortField]).To(Equal(esutil.EsSortOrderDescending))
		})

		When("a valid filter is specified", func() {
			BeforeEach(func() {
				expectedQuery = &filtering.Query{
					Term: &filtering.Term{
						fake.LetterN(10): fake.LetterN(10),
					},
				}
				expectedFilter = fake.LetterN(10)

				filterer.
					EXPECT().
					ParseExpression(expectedFilter).
					Return(expectedQuery, nil)
			})

			It("should send the parsed query to elasticsearch", func() {
				Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_search", expectedNotesAlias)))
				Expect(transport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodGet))

				requestBody, err := ioutil.ReadAll(transport.ReceivedHttpRequests[0].Body)
				Expect(err).ToNot(HaveOccurred())

				searchBody := &esutil.EsSearch{}
				err = json.Unmarshal(requestBody, searchBody)
				Expect(err).ToNot(HaveOccurred())
				Expect(searchBody.Query).To(Equal(expectedQuery))
			})
		})

		When("an invalid filter is specified", func() {
			BeforeEach(func() {
				expectedFilter = fake.LetterN(10)

				filterer.
					EXPECT().
					ParseExpression(expectedFilter).
					Return(nil, errors.New(fake.LetterN(10)))
			})

			It("should not send a request to elasticsearch", func() {
				Expect(transport.ReceivedHttpRequests).To(HaveLen(0))
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
				transport.PreparedHttpResponses = []*http.Response{
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
				transport.PreparedHttpResponses = []*http.Response{
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
				transport.PreparedHttpResponses = []*http.Response{
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

	Context("listing Grafeas notes with pagination", func() {
		var (
			actualErr           error
			actualNotes         []*pb.Note
			actualNextPageToken string
			expectedNotes       []*pb.Note
			expectedPageToken   string
			expectedPageSize    int32
			expectedPitId       string
			expectedFrom        int
		)

		BeforeEach(func() {
			expectedNotes = generateTestNotes(fake.Number(2, 5), expectedProjectId)
			expectedPageSize = int32(fake.Number(5, 20))
			expectedFrom = fake.Number(int(expectedPageSize), 100)
			expectedPitId = fake.LetterN(20)
			transport.PreparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body: createPaginatedNoteEsSearchResponse(
						fake.Number(1000, 10000),
						expectedNotes...,
					),
				},
			}
		})

		JustBeforeEach(func() {
			actualNotes, actualNextPageToken, actualErr = elasticsearchStorage.ListNotes(ctx, expectedProjectId, "", expectedPageToken, expectedPageSize)
		})

		When("a page token is not specified", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses = append([]*http.Response{
					{
						StatusCode: http.StatusOK,
						Body: structToJsonBody(&esutil.ESPitResponse{
							Id: expectedPitId,
						}),
					},
				}, transport.PreparedHttpResponses...)
			})

			It("should create a PIT in elasticsearch", func() {
				Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_pit", expectedNotesAlias)))
				Expect(transport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodPost))
				Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("keep_alive")).To(Equal(pitKeepAlive))
			})

			It("should query elasticsearch for notes using the PIT id", func() {
				Expect(transport.ReceivedHttpRequests[1].URL.Path).To(Equal("/_search"))
				Expect(transport.ReceivedHttpRequests[1].Method).To(Equal(http.MethodGet))
				Expect(transport.ReceivedHttpRequests[1].URL.Query().Get("size")).To(Equal(strconv.Itoa(int(expectedPageSize))))
				Expect(transport.ReceivedHttpRequests[1].URL.Query().Get("from")).To(Equal(strconv.Itoa(0)))

				requestBody, err := ioutil.ReadAll(transport.ReceivedHttpRequests[1].Body)
				Expect(err).ToNot(HaveOccurred())

				searchBody := &esutil.EsSearch{}
				err = json.Unmarshal(requestBody, searchBody)
				Expect(err).ToNot(HaveOccurred())
				Expect(searchBody.Query).To(BeNil())
				Expect(searchBody.Sort[sortField]).To(Equal(esutil.EsSortOrderDescending))
				Expect(searchBody.Pit.Id).To(Equal(expectedPitId))
				Expect(searchBody.Pit.KeepAlive).To(Equal(pitKeepAlive))
			})

			It("should return the Grafeas note(s) and the new page token", func() {
				Expect(actualNotes).ToNot(BeNil())
				Expect(actualNotes).To(Equal(expectedNotes))
				Expect(actualErr).ToNot(HaveOccurred())

				pitId, from, err := esutil.ParsePageToken(actualNextPageToken)
				Expect(err).ToNot(HaveOccurred())
				Expect(pitId).To(Equal(expectedPitId))
				Expect(from).To(BeEquivalentTo(expectedPageSize))
			})

			When("creating a PIT in elasticsearch fails", func() {
				BeforeEach(func() {
					transport.PreparedHttpResponses[0].StatusCode = http.StatusInternalServerError
				})

				It("should return an error", func() {
					assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
					Expect(actualNextPageToken).To(BeEmpty())
				})
			})
		})

		When("a valid page token is specified", func() {
			BeforeEach(func() {
				expectedPageToken = esutil.CreatePageToken(expectedPitId, expectedFrom)
			})

			It("should query elasticsearch for notes using the PIT id", func() {
				Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal("/_search"))
				Expect(transport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodGet))
				Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("size")).To(Equal(strconv.Itoa(int(expectedPageSize))))
				Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("from")).To(Equal(strconv.Itoa(expectedFrom)))

				requestBody, err := ioutil.ReadAll(transport.ReceivedHttpRequests[0].Body)
				Expect(err).ToNot(HaveOccurred())

				searchBody := &esutil.EsSearch{}
				err = json.Unmarshal(requestBody, searchBody)
				Expect(err).ToNot(HaveOccurred())
				Expect(searchBody.Query).To(BeNil())
				Expect(searchBody.Sort[sortField]).To(Equal(esutil.EsSortOrderDescending))
				Expect(searchBody.Pit.Id).To(Equal(expectedPitId))
				Expect(searchBody.Pit.KeepAlive).To(Equal(pitKeepAlive))
			})

			It("should return the Grafeas note(s) and the new page token", func() {
				Expect(actualNotes).ToNot(BeNil())
				Expect(actualNotes).To(Equal(expectedNotes))
				Expect(actualErr).ToNot(HaveOccurred())

				pitId, from, err := esutil.ParsePageToken(actualNextPageToken)
				Expect(err).ToNot(HaveOccurred())
				Expect(pitId).To(Equal(expectedPitId))
				Expect(from).To(BeEquivalentTo(int(expectedPageSize) + expectedFrom))
			})

			When("getting the last page of results", func() {
				BeforeEach(func() {
					transport.PreparedHttpResponses[0].Body = createPaginatedNoteEsSearchResponse(fake.Number(1, int(expectedPageSize)) + expectedFrom - 1)
				})

				It("should return an empty next page token", func() {
					Expect(actualNextPageToken).To(Equal(""))
					Expect(actualErr).ToNot(HaveOccurred())
				})
			})
		})

		When("an invalid page token is specified (bad format)", func() {
			BeforeEach(func() {
				expectedPageToken = fake.LetterN(50)
			})

			It("should not query elasticsearch", func() {
				Expect(transport.ReceivedHttpRequests).To(HaveLen(0))
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
				Expect(actualNextPageToken).To(BeEmpty())
			})
		})

		When("an invalid page token is specified (bad from)", func() {
			BeforeEach(func() {
				expectedPageToken = fmt.Sprintf("%sfoo", esutil.CreatePageToken(expectedPitId, expectedFrom))
			})

			It("should not query elasticsearch", func() {
				Expect(transport.ReceivedHttpRequests).To(HaveLen(0))
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
				Expect(actualNextPageToken).To(BeEmpty())
			})
		})
	})

	Context("deleting a Grafeas note", func() {
		var (
			actualErr        error
			expectedNoteId   string
			expectedNoteName string
		)

		BeforeEach(func() {
			expectedNoteId = fake.LetterN(10)
			expectedNoteName = fmt.Sprintf("projects/%s/notes/%s", expectedProjectId, expectedNoteId)

			transport.PreparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body: structToJsonBody(&esutil.EsDeleteResponse{
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
			Expect(transport.ReceivedHttpRequests).To(HaveLen(1))
			Expect(transport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodPost))
			Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_delete_by_query", expectedNotesAlias)))

			requestBody, err := ioutil.ReadAll(transport.ReceivedHttpRequests[0].Body)
			Expect(err).ToNot(HaveOccurred())

			searchBody := &esutil.EsSearch{}
			err = json.Unmarshal(requestBody, searchBody)
			Expect(err).ToNot(HaveOccurred())

			Expect((*searchBody.Query.Term)["name"]).To(Equal(expectedNoteName))
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshTrue), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshTrue
			})

			It("should immediately refresh the index", func() {
				Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("true"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshWaitFor), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshWaitFor
			})

			It("should immediately refresh the index", func() {
				Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("true"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshFalse), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshFalse
			})

			It("should not wait or force refresh of index", func() {
				Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("false"))
			})
		})

		When("elasticsearch successfully deletes the note document", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[0].Body = structToJsonBody(&esutil.EsDeleteResponse{
					Deleted: 1,
				})
			})

			It("should not return an error", func() {
				Expect(actualErr).ToNot(HaveOccurred())
			})
		})

		When("the note does not exist", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[0].Body = structToJsonBody(&esutil.EsDeleteResponse{
					Deleted: 0,
				})
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})

		When("deleting the note document fails", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[0].StatusCode = http.StatusInternalServerError
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})
	})
})

func createProjectEsSearchResponse(projects ...*prpb.Project) io.ReadCloser {
	return createPaginatedProjectEsSearchResponse(len(projects), projects...)
}

func createPaginatedProjectEsSearchResponse(totalValue int, projects ...*prpb.Project) io.ReadCloser {
	var messages []proto.Message
	for _, p := range projects {
		messages = append(messages, p)
	}

	return createPaginatedGenericEsSearchResponse(totalValue, messages...)
}

func createOccurrenceEsSearchResponse(occurrences ...*pb.Occurrence) io.ReadCloser {
	return createPaginatedOccurrenceEsSearchResponse(len(occurrences), occurrences...)
}
func createPaginatedOccurrenceEsSearchResponse(totalValue int, occurrences ...*pb.Occurrence) io.ReadCloser {
	var messages []proto.Message
	for _, p := range occurrences {
		messages = append(messages, p)
	}

	return createPaginatedGenericEsSearchResponse(totalValue, messages...)
}
func createNoteEsSearchResponse(notes ...*pb.Note) io.ReadCloser {
	return createPaginatedNoteEsSearchResponse(len(notes), notes...)
}

func createPaginatedNoteEsSearchResponse(totalValue int, notes ...*pb.Note) io.ReadCloser {
	var messages []proto.Message
	for _, p := range notes {
		messages = append(messages, p)
	}

	return createPaginatedGenericEsSearchResponse(totalValue, messages...)
}

func createGenericEsSearchResponse(messages ...proto.Message) io.ReadCloser {
	return createPaginatedGenericEsSearchResponse(len(messages), messages...)
}

func createPaginatedGenericEsSearchResponse(totalValue int, messages ...proto.Message) io.ReadCloser {
	var hits []*esutil.EsSearchResponseHit

	for _, m := range messages {
		raw, err := protojson.Marshal(proto.MessageV2(m))
		Expect(err).ToNot(HaveOccurred())

		hits = append(hits, &esutil.EsSearchResponseHit{
			Source: raw,
		})
	}

	response := &esutil.EsSearchResponse{
		Took: fake.Number(1, 10),
		Hits: &esutil.EsSearchResponseHits{
			Total: &esutil.EsSearchResponseTotal{
				Value: totalValue,
			},
			Hits: hits,
		},
	}
	responseBody, err := json.Marshal(response)
	Expect(err).ToNot(HaveOccurred())

	return ioutil.NopCloser(bytes.NewReader(responseBody))
}

func createEsSearchResponse(objectType string, hitNames ...string) io.ReadCloser {
	var occurrenceHits []*esutil.EsSearchResponseHit

	for _, hit := range hitNames {
		switch objectType {
		case "project":
			rawGrafeasObject, err := json.Marshal(generateTestProject(hit))
			Expect(err).ToNot(HaveOccurred())
			occurrenceHits = append(occurrenceHits, &esutil.EsSearchResponseHit{
				Source: rawGrafeasObject,
			})
		case "occurrence":
			rawGrafeasObject, err := json.Marshal(generateTestOccurrence(hit))
			Expect(err).ToNot(HaveOccurred())
			occurrenceHits = append(occurrenceHits, &esutil.EsSearchResponseHit{
				Source: rawGrafeasObject,
			})
		case "note":
			rawGrafeasObject, err := json.Marshal(generateTestNote(hit))
			Expect(err).ToNot(HaveOccurred())
			occurrenceHits = append(occurrenceHits, &esutil.EsSearchResponseHit{
				Source: rawGrafeasObject,
			})
		}
	}

	response := &esutil.EsSearchResponse{
		Took: fake.Number(1, 10),
		Hits: &esutil.EsSearchResponseHits{
			Total: &esutil.EsSearchResponseTotal{
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
		responseItems     []*esutil.EsBulkResponseItem
		responseHasErrors = false
	)
	for i := range occurrences {
		var (
			responseErr  *esutil.EsIndexDocError
			responseCode = http.StatusCreated
		)
		if errs[i] != nil {
			responseErr = &esutil.EsIndexDocError{
				Type:   fake.LetterN(10),
				Reason: fake.LetterN(10),
			}
			responseCode = http.StatusInternalServerError
			responseHasErrors = true
		}

		responseItems = append(responseItems, &esutil.EsBulkResponseItem{
			Index: &esutil.EsIndexDocResponse{
				Id:     fake.LetterN(10),
				Status: responseCode,
				Error:  responseErr,
			},
		})
	}

	response := &esutil.EsBulkResponse{
		Items:  responseItems,
		Errors: responseHasErrors,
	}

	responseBody, err := json.Marshal(response)
	Expect(err).ToNot(HaveOccurred())

	return ioutil.NopCloser(bytes.NewReader(responseBody))
}

func createEsBulkNoteIndexResponse(notesThatCreatedSuccessfully map[string]*pb.Note) io.ReadCloser {
	var responseItems []*esutil.EsBulkResponseItem
	for range notesThatCreatedSuccessfully {
		responseItems = append(responseItems, &esutil.EsBulkResponseItem{
			Index: &esutil.EsIndexDocResponse{
				Id:     fake.LetterN(10),
				Status: http.StatusCreated,
			},
		})
	}

	response := &esutil.EsBulkResponse{
		Items:  responseItems,
		Errors: false,
	}

	responseBody, err := json.Marshal(response)
	Expect(err).ToNot(HaveOccurred())

	return ioutil.NopCloser(bytes.NewReader(responseBody))
}

func createEsMultiSearchNoteResponse(notes map[string]*pb.Note) io.ReadCloser {
	multiSearchResponse := &esutil.EsMultiSearchResponse{}

	for range notes {
		multiSearchResponse.Responses = append(multiSearchResponse.Responses, &esutil.EsMultiSearchResponseHitsSummary{
			Hits: &esutil.EsMultiSearchResponseHits{
				Total: &esutil.EsSearchResponseTotal{
					Value: 0,
				},
			},
		})
	}

	responseBody, err := json.Marshal(multiSearchResponse)
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
		result = append(result, generateTestProject(fake.LetterN(10)))
	}

	return result
}

func generateTestOccurrence(name string) *pb.Occurrence {
	return &pb.Occurrence{
		Name: name,
		Resource: &grafeas_go_proto.Resource{
			Uri: fake.LetterN(10),
		},
		NoteName:    fake.LetterN(10),
		Kind:        common_go_proto.NoteKind_NOTE_KIND_UNSPECIFIED,
		Remediation: fake.LetterN(10),
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
		ShortDescription: fake.Phrase(),
		LongDescription:  fake.Phrase(),
		Kind:             common_go_proto.NoteKind_NOTE_KIND_UNSPECIFIED,
		CreateTime:       ptypes.TimestampNow(),
	}
}

func generateTestNotes(l int, project string) []*pb.Note {
	var result []*pb.Note
	for i := 0; i < l; i++ {
		result = append(result, generateTestNote(fmt.Sprintf("projects/%s/notes/%s", project, fake.LetterN(10))))
	}

	return result
}

func convertSliceOfNotesToMap(notes []*pb.Note) map[string]*pb.Note {
	result := make(map[string]*pb.Note)
	for _, note := range notes {
		noteId := strings.Split(note.Name, "/")[3]
		result[noteId] = note
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

func ioReadCloserToByteSlice(r io.ReadCloser) []byte {
	builder := new(strings.Builder)
	_, err := io.Copy(builder, r)
	Expect(err).ToNot(HaveOccurred())
	return []byte(builder.String())
}
