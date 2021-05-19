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
	"context"
	"errors"
	"fmt"
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

	"github.com/grafeas/grafeas/proto/v1beta1/common_go_proto"
	"github.com/grafeas/grafeas/proto/v1beta1/grafeas_go_proto"
	pb "github.com/grafeas/grafeas/proto/v1beta1/grafeas_go_proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("elasticsearch storage", func() {
	var (
		elasticsearchStorage *ElasticsearchStorage
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

	Context("CreateProject", func() {
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

	Context("ListProjects", func() {
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

	Context("GetProject", func() {
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
				Expect(actualProject).To(BeNil())
			})
		})
	})

	Context("DeleteProject", func() {
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
			expectedBulkCreateError = nil
		})

		JustBeforeEach(func() {
			occurrences := deepCopyOccurrences(expectedOccurrences)

			client.SearchReturns(expectedSearchResponse, expectedSearchError)
			client.BulkReturns(expectedBulkCreateResponse, expectedBulkCreateError)

			actualOccurrences, actualErrs = elasticsearchStorage.BatchCreateOccurrences(context.Background(), expectedProjectId, "", occurrences)
		})

		It("should send a bulk request to ES to index each occurrence", func() {
			Expect(client.BulkCallCount()).To(Equal(1))

			_, bulkCreateRequest := client.BulkArgsForCall(0)
			Expect(bulkCreateRequest.Index).To(Equal(expectedOccurrencesAlias))

			for i, item := range bulkCreateRequest.Items {
				occurrence := proto.MessageV1(item.Message).(*grafeas_go_proto.Occurrence)
				expectedOccurrence := expectedOccurrences[i]
				expectedOccurrence.Name = occurrence.Name
				Expect(item.Operation).To(Equal(esutil.BULK_CREATE))

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
				Expect(client.BulkCallCount()).To(Equal(1))

				_, bulkCreateRequest := client.BulkArgsForCall(0)
				Expect(bulkCreateRequest.Refresh).To(Equal("true"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshWaitFor), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshWaitFor
			})

			It("should wait for refresh of index", func() {
				Expect(client.BulkCallCount()).To(Equal(1))

				_, bulkCreateRequest := client.BulkArgsForCall(0)
				Expect(bulkCreateRequest.Refresh).To(Equal("wait_for"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshFalse), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshFalse
			})

			It("should not wait or force refresh of index", func() {
				Expect(client.BulkCallCount()).To(Equal(1))

				_, bulkCreateRequest := client.BulkArgsForCall(0)
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

	Context("CreateNote", func() {
		var (
			actualErr  error
			actualNote *pb.Note

			expectedNote     *pb.Note
			expectedNoteId   string
			expectedNoteName string

			expectedProjectSearchResponse *esutil.SearchResponse
			expectedProjectSearchError    error

			expectedNoteSearchResponse *esutil.SearchResponse
			expectedNoteSearchError    error

			expectedNoteESId    string
			expectedCreateError error
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

			// happy path: project exists
			expectedProject := generateTestProject(expectedProjectId)
			expectedProjectJson, err := protojson.Marshal(proto.MessageV2(expectedProject))
			Expect(err).ToNot(HaveOccurred())
			expectedProjectSearchResponse = &esutil.SearchResponse{
				Hits: &esutil.EsSearchResponseHits{
					Total: &esutil.EsSearchResponseTotal{
						Value: 1,
					},
					Hits: []*esutil.EsSearchResponseHit{
						{
							Source: expectedProjectJson,
						},
					},
				},
			}
			expectedProjectSearchError = nil

			// happy path: note does not exist (so it needs to be created)
			expectedNoteSearchResponse = &esutil.SearchResponse{
				Hits: &esutil.EsSearchResponseHits{
					Total: &esutil.EsSearchResponseTotal{
						Value: 0,
					},
				},
			}
			expectedNoteSearchError = nil

			expectedCreateError = nil
		})

		// JustBeforeEach actually invokes the system under test
		JustBeforeEach(func() {
			client.SearchReturnsOnCall(0, expectedProjectSearchResponse, expectedProjectSearchError)
			client.SearchReturnsOnCall(1, expectedNoteSearchResponse, expectedNoteSearchError)
			client.CreateReturns(expectedNoteESId, expectedCreateError)

			actualNote, actualErr = elasticsearchStorage.CreateNote(ctx, expectedProjectId, expectedNoteId, "", deepCopyNote(expectedNote))
		})

		It("should check that the note's project exists", func() {
			// we expect two search calls because we search for the project and the note
			Expect(client.SearchCallCount()).To(Equal(2))

			_, searchRequest := client.SearchArgsForCall(0)
			Expect(searchRequest.Index).To(Equal(expectedProjectAlias))
			Expect((*searchRequest.Search.Query.Term)["name"]).To(Equal(fmt.Sprintf("projects/%s", expectedProjectId)))
		})

		It("should check elasticsearch to see if a note with the specified noteId already exists", func() {
			// we expect two search calls because we search for the project and the note
			Expect(client.SearchCallCount()).To(Equal(2))

			_, searchRequest := client.SearchArgsForCall(1)
			Expect(searchRequest.Index).To(Equal(expectedNotesAlias))
			Expect((*searchRequest.Search.Query.Term)["name"]).To(Equal(expectedNoteName))
		})

		It("should attempt to index the note as a document", func() {
			Expect(client.CreateCallCount()).To(Equal(1))

			_, createRequest := client.CreateArgsForCall(0)

			Expect(createRequest.Index).To(Equal(expectedNotesAlias))

			note := proto.MessageV1(createRequest.Message).(*pb.Note)
			Expect(note).To(Equal(expectedNote))
		})

		It("should return the note that was created", func() {
			Expect(actualErr).ToNot(HaveOccurred())

			expectedNote.Name = actualNote.Name
			Expect(actualNote).To(Equal(expectedNote))
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

		When("the notes project doesn't exist", func() {
			BeforeEach(func() {
				expectedProjectSearchResponse.Hits.Total.Value = 0
				expectedProjectSearchResponse.Hits.Hits = []*esutil.EsSearchResponseHit{}
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.FailedPrecondition)
				Expect(actualNote).To(BeNil())
			})

			It("should not attempt to search for the note or create the note", func() {
				Expect(client.SearchCallCount()).To(Equal(1))
				Expect(client.CreateCallCount()).To(Equal(0))
			})
		})

		When("checking for the project fails", func() {
			BeforeEach(func() {
				expectedProjectSearchError = errors.New("failed searching for project")
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
				Expect(actualNote).To(BeNil())
			})

			It("should not attempt to search for the note or create the note", func() {
				Expect(client.SearchCallCount()).To(Equal(1))
				Expect(client.CreateCallCount()).To(Equal(0))
			})
		})

		When("a note with the specified noteId exists", func() {
			BeforeEach(func() {
				expectedNoteJson, err := protojson.Marshal(proto.MessageV2(expectedNote))
				Expect(err).ToNot(HaveOccurred())

				expectedNoteSearchResponse.Hits.Total.Value = 1
				expectedNoteSearchResponse.Hits.Hits = []*esutil.EsSearchResponseHit{
					{
						Source: expectedNoteJson,
					},
				}
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.AlreadyExists)
				Expect(actualNote).To(BeNil())
			})

			It("should not attempt to index a note document", func() {
				Expect(client.CreateCallCount()).To(Equal(0))
			})
		})

		When("checking for the note fails", func() {
			BeforeEach(func() {
				expectedNoteSearchError = errors.New("failed searching for note")
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
				Expect(actualNote).To(BeNil())
			})

			It("should not attempt to index a note document", func() {
				Expect(client.CreateCallCount()).To(Equal(0))
			})
		})

		When("creating the note fails", func() {
			BeforeEach(func() {
				expectedCreateError = errors.New("failed creating note")
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
				Expect(actualNote).To(BeNil())
			})
		})

		When("the note timestamp is empty", func() {
			BeforeEach(func() {
				expectedNote.CreateTime = nil
			})

			It("should set the timestamp before saving into elasticsearch", func() {
				Expect(actualNote.CreateTime).ToNot(BeNil())
			})
		})
	})

	Context("BatchCreateNotes", func() {
		var (
			actualErrs               []error
			actualNotes              []*pb.Note
			expectedNotes            []*pb.Note
			expectedNotesWithNoteIds map[string]*pb.Note

			expectedProjectSearchResponse *esutil.SearchResponse
			expectedProjectSearchError    error

			expectedNoteMultiSearchResponse *esutil.EsMultiSearchResponse
			expectedNoteMultiSearchError    error

			expectedBulkCreateResponse *esutil.EsBulkResponse
			expectedBulkCreateError    error
		)

		// BeforeEach configures the happy path for this context
		// Variables configured here may be overridden in nested BeforeEach blocks
		BeforeEach(func() {
			expectedNotes = generateTestNotes(fake.Number(2, 5), expectedProjectId)
			expectedNotesWithNoteIds = convertSliceOfNotesToMap(expectedNotes)

			// happy path: project exists
			expectedProject := generateTestProject(expectedProjectId)
			expectedProjectJson, err := protojson.Marshal(proto.MessageV2(expectedProject))
			Expect(err).ToNot(HaveOccurred())
			expectedProjectSearchResponse = &esutil.SearchResponse{
				Hits: &esutil.EsSearchResponseHits{
					Total: &esutil.EsSearchResponseTotal{
						Value: 1,
					},
					Hits: []*esutil.EsSearchResponseHit{
						{
							Source: expectedProjectJson,
						},
					},
				},
			}
			expectedProjectSearchError = nil

			// happy path: none of the provided notes exist, and all of the notes were created successfully
			var (
				expectedMultiSearchResponseHitSummaries []*esutil.EsMultiSearchResponseHitsSummary
				expectedBulkCreateResponseItems         []*esutil.EsBulkResponseItem
			)
			for range expectedNotes {
				expectedMultiSearchResponseHitSummaries = append(expectedMultiSearchResponseHitSummaries, &esutil.EsMultiSearchResponseHitsSummary{
					Hits: &esutil.EsMultiSearchResponseHits{
						Total: &esutil.EsSearchResponseTotal{
							Value: 0,
						},
					},
				})
				expectedBulkCreateResponseItems = append(expectedBulkCreateResponseItems, &esutil.EsBulkResponseItem{
					Index: &esutil.EsIndexDocResponse{
						Id:    fake.LetterN(10),
						Error: nil,
					},
				})
			}

			expectedNoteMultiSearchResponse = &esutil.EsMultiSearchResponse{
				Responses: expectedMultiSearchResponseHitSummaries,
			}
			expectedNoteMultiSearchError = nil
			expectedBulkCreateResponse = &esutil.EsBulkResponse{
				Items: expectedBulkCreateResponseItems,
			}
			expectedBulkCreateError = nil
		})

		// JustBeforeEach actually invokes the system under test
		JustBeforeEach(func() {
			client.SearchReturnsOnCall(0, expectedProjectSearchResponse, expectedProjectSearchError)

			if client.MultiSearchStub == nil {
				client.MultiSearchReturns(expectedNoteMultiSearchResponse, expectedNoteMultiSearchError)
			}

			if client.BulkStub == nil {
				client.BulkReturns(expectedBulkCreateResponse, expectedBulkCreateError)
			}

			actualNotes, actualErrs = elasticsearchStorage.BatchCreateNotes(context.Background(), expectedProjectId, "", deepCopyNotes(expectedNotesWithNoteIds))
		})

		It("should check that the notes project exists", func() {
			Expect(client.SearchCallCount()).To(Equal(1))

			_, searchRequest := client.SearchArgsForCall(0)
			Expect(searchRequest.Index).To(Equal(expectedProjectAlias))
			Expect((*searchRequest.Search.Query.Term)["name"]).To(Equal(fmt.Sprintf("projects/%s", expectedProjectId)))
		})

		It("should send a multisearch request to ES to check for the existence of each note", func() {
			Expect(client.MultiSearchCallCount()).To(Equal(1))

			_, multiSearchRequest := client.MultiSearchArgsForCall(0)

			Expect(multiSearchRequest.Index).To(Equal(expectedNotesAlias))
			Expect(multiSearchRequest.Searches).To(HaveLen(len(expectedNotes)))

			var expectedNoteNames []string
			for _, note := range expectedNotes {
				expectedNoteNames = append(expectedNoteNames, note.Name)
			}

			for _, search := range multiSearchRequest.Searches {
				noteName := (*search.Query.Term)["name"]
				Expect(expectedNoteNames).To(ContainElement(noteName))
			}
		})

		It("should send a bulk request to create each note", func() {
			Expect(client.BulkCallCount()).To(Equal(1))

			_, bulkCreateRequest := client.BulkArgsForCall(0)

			Expect(bulkCreateRequest.Index).To(Equal(expectedNotesAlias))

			for _, item := range bulkCreateRequest.Items {
				note := proto.MessageV1(item.Message).(*pb.Note)
				Expect(expectedNotes).To(ContainElement(note))
				Expect(item.Operation).To(Equal(esutil.BULK_CREATE))
			}
		})

		It("should return all created notes", func() {
			for _, note := range expectedNotesWithNoteIds {
				Expect(actualNotes).To(ContainElement(note))
			}
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshTrue), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshTrue
			})

			It("should immediately refresh the index", func() {
				_, bulkCreateRequest := client.BulkArgsForCall(0)

				Expect(bulkCreateRequest.Refresh).To(Equal("true"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshWaitFor), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshWaitFor
			})

			It("should wait for refresh of index", func() {
				_, bulkCreateRequest := client.BulkArgsForCall(0)

				Expect(bulkCreateRequest.Refresh).To(Equal("wait_for"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshFalse), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshFalse
			})

			It("should not wait or force refresh of index", func() {
				_, bulkCreateRequest := client.BulkArgsForCall(0)

				Expect(bulkCreateRequest.Refresh).To(Equal("false"))
			})
		})

		When("the notes project doesn't exist", func() {
			BeforeEach(func() {
				expectedProjectSearchResponse.Hits.Total.Value = 0
				expectedProjectSearchResponse.Hits.Hits = []*esutil.EsSearchResponseHit{}
			})

			It("should return an error", func() {
				Expect(actualNotes).To(BeNil())
				Expect(actualErrs).To(HaveLen(1))
				assertErrorHasGrpcStatusCode(actualErrs[0], codes.FailedPrecondition)
			})

			It("should not attempt a multisearch or bulkcreate", func() {
				Expect(client.MultiSearchCallCount()).To(Equal(0))
				Expect(client.BulkCallCount()).To(Equal(0))
			})
		})

		When("checking for the project fails", func() {
			BeforeEach(func() {
				expectedProjectSearchError = errors.New("failed searching for project")
			})

			It("should return an error", func() {
				Expect(actualNotes).To(BeNil())
				Expect(actualErrs).To(HaveLen(1))
				assertErrorHasGrpcStatusCode(actualErrs[0], codes.Internal)
			})

			It("should not attempt to search for the note or create the notes", func() {
				Expect(client.MultiSearchCallCount()).To(Equal(0))
				Expect(client.BulkCallCount()).To(Equal(0))
			})
		})

		When("the multisearch request returns an error", func() {
			BeforeEach(func() {
				expectedNoteMultiSearchError = errors.New("failed multisearch")
			})

			It("should return a single error and no notes", func() {
				Expect(actualNotes).To(BeNil())
				Expect(actualErrs).To(HaveLen(1))
				assertErrorHasGrpcStatusCode(actualErrs[0], codes.Internal)
			})

			It("should not attempt to index any notes", func() {
				Expect(client.BulkCallCount()).To(Equal(0))
			})
		})

		When("the bulkcreate request returns an error", func() {
			BeforeEach(func() {
				expectedBulkCreateError = errors.New("failed bulkcreate")
			})

			It("should return a single error and no notes", func() {
				Expect(actualNotes).To(BeNil())
				Expect(actualErrs).To(HaveLen(1))
				assertErrorHasGrpcStatusCode(actualErrs[0], codes.Internal)
			})
		})

		When("a note already exists", func() {
			var (
				nameOfNoteThatAlreadyExists string
			)
			BeforeEach(func() {
				randomIndex := fake.Number(0, len(expectedNotes)-1)
				nameOfNoteThatAlreadyExists = expectedNotes[randomIndex].Name

				// this is required due to the non-deterministic ordering of maps
				client.MultiSearchStub = func(ctx context.Context, request *esutil.MultiSearchRequest) (*esutil.EsMultiSearchResponse, error) {
					var responses []*esutil.EsMultiSearchResponseHitsSummary
					for _, search := range request.Searches {
						response := &esutil.EsMultiSearchResponseHitsSummary{
							Hits: &esutil.EsMultiSearchResponseHits{
								Total: &esutil.EsSearchResponseTotal{
									Value: 0,
								},
							},
						}
						if (*search.Query.Term)["name"] == nameOfNoteThatAlreadyExists {
							response.Hits.Total.Value = 1
						}

						responses = append(responses, response)
					}

					return &esutil.EsMultiSearchResponse{
						Responses: responses,
					}, nil
				}
			})

			It("should not include that note in the bulkcreate request", func() {
				Expect(client.BulkCallCount()).To(Equal(1))

				_, bulkCreateRequest := client.BulkArgsForCall(0)

				Expect(bulkCreateRequest.Items).To(HaveLen(len(expectedNotes) - 1))
				for _, item := range bulkCreateRequest.Items {
					note := proto.MessageV1(item.Message).(*pb.Note)
					Expect(note.Name).ToNot(Equal(nameOfNoteThatAlreadyExists))
					Expect(item.Operation).To(Equal(esutil.BULK_CREATE))
				}
			})

			It("should return an error for the note that wasn't created, and a list of notes that were created", func() {
				Expect(actualErrs).To(HaveLen(1))
				assertErrorHasGrpcStatusCode(actualErrs[0], codes.AlreadyExists)

				for _, note := range actualNotes {
					Expect(note.Name).ToNot(Equal(nameOfNoteThatAlreadyExists))
				}
			})
		})

		When("all notes already exist", func() {
			BeforeEach(func() {
				for _, response := range expectedNoteMultiSearchResponse.Responses {
					response.Hits.Total.Value = 1
				}
			})

			It("should not attempt to create any notes", func() {
				Expect(client.BulkCallCount()).To(Equal(0))
			})

			It("should return an error for every note", func() {
				Expect(actualNotes).To(BeNil())
				Expect(actualErrs).To(HaveLen(len(expectedNotes)))

				for _, err := range actualErrs {
					assertErrorHasGrpcStatusCode(err, codes.AlreadyExists)
				}
			})
		})

		When("a note fails to create", func() {
			var (
				nameOfNoteThatFailedToCreate string
			)

			BeforeEach(func() {
				randomIndex := fake.Number(0, len(expectedNotes)-1)
				nameOfNoteThatFailedToCreate = expectedNotes[randomIndex].Name

				// this is required due to the non-deterministic ordering of maps
				client.BulkStub = func(ctx context.Context, request *esutil.BulkRequest) (*esutil.EsBulkResponse, error) {
					var responses []*esutil.EsBulkResponseItem
					for _, item := range request.Items {
						response := &esutil.EsBulkResponseItem{
							Index: &esutil.EsIndexDocResponse{
								Id:    fake.LetterN(10),
								Error: nil,
							},
						}

						note := proto.MessageV1(item.Message).(*pb.Note)
						if note.Name == nameOfNoteThatFailedToCreate {
							response.Index.Id = ""
							response.Index.Error = &esutil.EsIndexDocError{
								Type:   fake.LetterN(10),
								Reason: fake.LetterN(10),
							}
						}

						responses = append(responses, response)
					}

					return &esutil.EsBulkResponse{
						Items:  responses,
						Errors: true,
					}, nil
				}
			})

			It("should return an error for the note that failed to create, and a list of notes that were created", func() {
				Expect(actualErrs).To(HaveLen(1))
				assertErrorHasGrpcStatusCode(actualErrs[0], codes.Internal)

				for _, note := range actualNotes {
					Expect(note.Name).ToNot(Equal(nameOfNoteThatFailedToCreate))
				}
			})
		})
	})

	Context("GetNote", func() {
		var (
			actualErr  error
			actualNote *pb.Note

			expectedNote     *pb.Note
			expectedNoteId   string
			expectedNoteName string

			expectedSearchResponse *esutil.SearchResponse
			expectedSearchError    error
		)

		BeforeEach(func() {
			expectedNoteId = fake.LetterN(10)
			expectedNoteName = fmt.Sprintf("projects/%s/notes/%s", expectedProjectId, expectedNoteId)
			expectedNote = generateTestNote(expectedNoteName)

			noteJson, err := protojson.Marshal(proto.MessageV2(expectedNote))
			Expect(err).ToNot(HaveOccurred())

			expectedSearchResponse = &esutil.SearchResponse{
				Hits: &esutil.EsSearchResponseHits{
					Total: &esutil.EsSearchResponseTotal{
						Value: 1,
					},
					Hits: []*esutil.EsSearchResponseHit{
						{
							Source: noteJson,
						},
					},
				},
			}
			expectedSearchError = nil
		})

		JustBeforeEach(func() {
			client.SearchReturns(expectedSearchResponse, expectedSearchError)

			actualNote, actualErr = elasticsearchStorage.GetNote(ctx, expectedProjectId, expectedNoteId)
		})

		It("should query elasticsearch for the specified note", func() {
			Expect(client.SearchCallCount()).To(Equal(1))

			_, searchRequest := client.SearchArgsForCall(0)

			Expect(searchRequest.Index).To(Equal(expectedNotesAlias))

			Expect(searchRequest.Pagination).To(BeNil())
			Expect(searchRequest.Search.Sort).To(BeNil())

			Expect((*searchRequest.Search.Query.Term)["name"]).To(Equal(expectedNoteName))
		})

		It("should return the note and no error", func() {
			Expect(actualNote).To(Equal(expectedNote))
			Expect(actualErr).ToNot(HaveOccurred())
		})

		When("elasticsearch can not find the specified note document", func() {
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
				Expect(actualNote).To(BeNil())
			})
		})
	})

	Context("ListNotes", func() {
		var (
			actualErr           error
			actualNotes         []*pb.Note
			actualNextPageToken string

			expectedNotes         []*pb.Note
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
			expectedNotes = generateTestNotes(fake.Number(2, 5), expectedProjectId)
			expectedPageSize = fake.Number(10, 20)
			expectedPageToken = fake.LetterN(10)

			var expectedSearchResponseHits []*esutil.EsSearchResponseHit
			for _, note := range expectedNotes {
				json, err := protojson.Marshal(proto.MessageV2(note))
				Expect(err).ToNot(HaveOccurred())

				expectedSearchResponseHits = append(expectedSearchResponseHits, &esutil.EsSearchResponseHit{
					Source: json,
				})
			}
			expectedNextPageToken = fake.LetterN(10)
			expectedSearchResponse = &esutil.SearchResponse{
				Hits: &esutil.EsSearchResponseHits{
					Total: &esutil.EsSearchResponseTotal{
						Value: len(expectedNotes),
					},
					Hits: expectedSearchResponseHits,
				},
				NextPageToken: expectedNextPageToken,
			}
			expectedSearchError = nil
		})

		JustBeforeEach(func() {
			client.SearchReturns(expectedSearchResponse, expectedSearchError)

			actualNotes, actualNextPageToken, actualErr = elasticsearchStorage.ListNotes(ctx, expectedProjectId, expectedFilter, expectedPageToken, int32(expectedPageSize))
		})

		It("should query elasticsearch for notes", func() {
			Expect(client.SearchCallCount()).To(Equal(1))

			_, searchRequest := client.SearchArgsForCall(0)

			Expect(searchRequest.Index).To(Equal(expectedNotesAlias))

			Expect(searchRequest.Pagination).ToNot(BeNil())
			Expect(searchRequest.Pagination.Size).To(Equal(expectedPageSize))
			Expect(searchRequest.Pagination.Token).To(Equal(expectedPageToken))

			Expect(searchRequest.Search.Sort[sortField]).To(Equal(esutil.EsSortOrderDescending))

			Expect(searchRequest.Search.Query).To(BeNil())
		})

		It("should return the notes and the next page token", func() {
			Expect(actualErr).ToNot(HaveOccurred())
			Expect(actualNotes).To(Equal(expectedNotes))
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
				Expect(actualNotes).To(BeNil())
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
				Expect(actualNotes).To(BeNil())
				Expect(actualNextPageToken).To(BeEmpty())

				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})

		When("elasticsearch returns zero hits", func() {
			BeforeEach(func() {
				expectedSearchResponse.Hits.Total.Value = 0
				expectedSearchResponse.Hits.Hits = []*esutil.EsSearchResponseHit{}
			})

			It("should return an empty array of grafeas notes and no error", func() {
				Expect(actualNotes).To(BeNil())
				Expect(actualErr).ToNot(HaveOccurred())
			})
		})
	})

	Context("DeleteNote", func() {
		var (
			actualErr        error
			expectedNoteId   string
			expectedNoteName string

			expectedDeleteError error
		)

		BeforeEach(func() {
			expectedNoteId = fake.LetterN(10)
			expectedNoteName = fmt.Sprintf("projects/%s/notes/%s", expectedProjectId, expectedNoteId)

			expectedDeleteError = nil
		})

		JustBeforeEach(func() {
			client.DeleteReturns(expectedDeleteError)

			actualErr = elasticsearchStorage.DeleteNote(ctx, expectedProjectId, expectedNoteId)
		})

		It("should have sent a request to elasticsearch to delete the note document", func() {
			Expect(client.DeleteCallCount()).To(Equal(1))

			_, deleteRequest := client.DeleteArgsForCall(0)

			Expect(deleteRequest.Index).To(Equal(expectedNotesAlias))
			Expect((*deleteRequest.Search.Query.Term)["name"]).To(Equal(expectedNoteName))
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

		When("elasticsearch successfully deletes the note document", func() {
			It("should not return an error", func() {
				Expect(actualErr).ToNot(HaveOccurred())
			})
		})

		When("deleting the note document fails", func() {
			BeforeEach(func() {
				expectedDeleteError = errors.New("delete failed")
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})
	})
})

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

func assertErrorHasGrpcStatusCode(err error, code codes.Code) {
	Expect(err).To(HaveOccurred())
	s, ok := status.FromError(err)

	Expect(ok).To(BeTrue(), "expected error to have been produced from the grpc/status package")
	Expect(s.Code()).To(Equal(code))
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
