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
		esConfig     *config.ElasticsearchConfig
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
		mockEsClient := &elasticsearch.Client{Transport: transport, API: esapi.New(transport)}

		elasticsearchStorage = NewElasticsearchStorage(logger, mockEsClient, filterer, esConfig, indexManager)
	})

	AfterEach(func() {
		mockCtrl.Finish()
	})

	Context("creating a new Grafeas project", func() {
		var (
			createProjectErr error
			expectedProject  *prpb.Project
		)

		// BeforeEach configures the happy path for this context
		// Variables configured here may be overridden in nested BeforeEach blocks
		BeforeEach(func() {
			transport.PreparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body:       createEsSearchResponse("project"),
				},
				{
					StatusCode: http.StatusOK,
					Body: structToJsonBody(&esIndexDocResponse{
						Id: fake.LetterN(10),
					}),
				},
			}
		})

		// JustBeforeEach actually invokes the system under test
		JustBeforeEach(func() {
			expectedProject, createProjectErr = elasticsearchStorage.CreateProject(context.Background(), expectedProjectId, &prpb.Project{})
		})

		Describe("checking if the project document exists", func() {
			BeforeEach(func() {
				indexManager.EXPECT().CreateIndex(context.Background(), gomock.Any(), false).Times(2)
			})

			It("should check if the project document already exists", func() {
				Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_search", expectedProjectAlias)))
				Expect(transport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodGet))

				assertJsonHasValues(transport.ReceivedHttpRequests[0].Body, map[string]interface{}{
					"query.term.name": fmt.Sprintf("projects/%s", expectedProjectId),
				})
			})
		})

		When("the project already exists", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[0] = &http.Response{
					StatusCode: http.StatusOK,
					Body:       createEsSearchResponse("project", fake.LetterN(10)),
				}
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(createProjectErr, codes.AlreadyExists)
				Expect(expectedProject).To(BeNil())
			})

			It("should not create any documents or indices for the project", func() {
				Expect(transport.ReceivedHttpRequests).To(HaveLen(1))
			})
		})

		When("checking if the project exists returns an error", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[0] = &http.Response{StatusCode: http.StatusBadRequest}
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(createProjectErr, codes.Internal)
				Expect(expectedProject).To(BeNil())
			})

			It("should not create a document or indices", func() {
				Expect(transport.ReceivedHttpRequests).To(HaveLen(1))
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
					Expect(transport.ReceivedHttpRequests[1].URL.Path).To(Equal(fmt.Sprintf("/%s/_doc", expectedProjectAlias)))
					Expect(transport.ReceivedHttpRequests[1].Method).To(Equal(http.MethodPost))

					projectBody := &prpb.Project{}
					err := protojson.Unmarshal(ioReadCloserToByteSlice(transport.ReceivedHttpRequests[1].Body), proto.MessageV2(projectBody))
					Expect(err).ToNot(HaveOccurred())

					Expect(projectBody.Name).To(Equal(fmt.Sprintf("projects/%s", expectedProjectId)))
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
						Expect(transport.ReceivedHttpRequests[1].URL.Query().Get("refresh")).To(Equal("true"))
					})
				})

				When(fmt.Sprintf("refresh configuration is %s", config.RefreshWaitFor), func() {
					BeforeEach(func() {
						esConfig.Refresh = config.RefreshWaitFor
					})

					It("should wait for refresh of index", func() {
						Expect(transport.ReceivedHttpRequests[1].URL.Query().Get("refresh")).To(Equal("wait_for"))
					})
				})

				When(fmt.Sprintf("refresh configuration is %s", config.RefreshFalse), func() {
					BeforeEach(func() {
						esConfig.Refresh = config.RefreshFalse
					})

					It("should not wait or force refresh of index", func() {
						Expect(transport.ReceivedHttpRequests[1].URL.Query().Get("refresh")).To(Equal("false"))
					})
				})
			})

			When("creating a new document fails", func() {
				BeforeEach(func() {
					transport.PreparedHttpResponses[1] = &http.Response{StatusCode: http.StatusBadRequest}

					indexManager.EXPECT().CreateIndex(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
				})

				It("should return an error", func() {
					assertErrorHasGrpcStatusCode(createProjectErr, codes.Internal)
					Expect(expectedProject).To(BeNil())
				})
			})

			When("creating the indices fails", func() {
				BeforeEach(func() {
					indexManager.EXPECT().CreateIndex(gomock.Any(), gomock.Any(), gomock.Any()).Return(fmt.Errorf("foobar"))
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
			actualErr        error
			actualProjects   []*prpb.Project
			expectedProjects []*prpb.Project
			expectedFilter   string
			expectedQuery    *filtering.Query
		)

		BeforeEach(func() {
			expectedQuery = &filtering.Query{}
			expectedFilter = ""
			expectedProjects = generateTestProjects(fake.Number(2, 5))
			transport.PreparedHttpResponses = []*http.Response{
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
			Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_search", expectedProjectAlias)))
			Expect(transport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodGet))

			requestBody, err := ioutil.ReadAll(transport.ReceivedHttpRequests[0].Body)
			Expect(err).ToNot(HaveOccurred())

			searchBody := &esSearch{}
			err = json.Unmarshal(requestBody, searchBody)
			Expect(err).ToNot(HaveOccurred())
			Expect(searchBody.Query).To(BeNil())
			Expect(searchBody.Sort).To(BeEmpty())
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
				Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_search", expectedProjectAlias)))
				Expect(transport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodGet))

				requestBody, err := ioutil.ReadAll(transport.ReceivedHttpRequests[0].Body)
				Expect(err).ToNot(HaveOccurred())

				searchBody := &esSearch{}
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
				transport.PreparedHttpResponses = []*http.Response{
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

	Context("retrieving a Grafeas project", func() {
		var (
			actualErr     error
			actualProject *prpb.Project
		)

		BeforeEach(func() {
			transport.PreparedHttpResponses = []*http.Response{
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
			Expect(transport.ReceivedHttpRequests).To(HaveLen(1))

			Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_search", expectedProjectAlias)))
			Expect(transport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodGet))

			requestBody, err := ioutil.ReadAll(transport.ReceivedHttpRequests[0].Body)
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

	Context("deleting a Grafeas project", func() {
		var (
			actualErr error
		)

		BeforeEach(func() {
			transport.PreparedHttpResponses = []*http.Response{
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
			Expect(transport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodPost))
			Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_delete_by_query", expectedProjectAlias)))

			requestBody, err := ioutil.ReadAll(transport.ReceivedHttpRequests[0].Body)
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

			It("should not wait for or force refresh of index", func() {
				Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("false"))
			})
		})

		When("elasticsearch successfully deletes the project document", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[0].Body = structToJsonBody(&esDeleteResponse{
					Deleted: 1,
				})
			})

			It("should attempt to delete the indices for notes / occurrences", func() {
				Expect(transport.ReceivedHttpRequests[1].Method).To(Equal(http.MethodDelete))
				Expect(transport.ReceivedHttpRequests[1].URL.Path).To(Equal(fmt.Sprintf("/%s,%s", expectedOccurrencesIndex, expectedNotesIndex)))
			})

			When("elasticsearch successfully deletes the indices for notes / occurrences", func() {
				It("should not return an error", func() {
					Expect(actualErr).ToNot(HaveOccurred())
				})
			})

			When("elasticsearch fails to delete the indices for notes / occurrences", func() {
				BeforeEach(func() {
					transport.PreparedHttpResponses[1].StatusCode = http.StatusInternalServerError
				})

				It("should return an error", func() {
					assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
				})
			})
		})

		When("project does not exist", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[0].Body = structToJsonBody(&esDeleteResponse{
					Deleted: 0,
				})
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})

			It("should not attempt to delete the indices for notes / occurrences", func() {
				Expect(transport.ReceivedHttpRequests).To(HaveLen(1))
			})
		})

		When("deleting the project document fails", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[0].StatusCode = http.StatusInternalServerError
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})

			It("should not attempt to delete the indices for notes / occurrences", func() {
				Expect(transport.ReceivedHttpRequests).To(HaveLen(1))
			})
		})
	})

	Context("retrieving a Grafeas occurrence", func() {
		var (
			actualErr              error
			actualOccurrence       *pb.Occurrence
			expectedOccurrenceId   string
			expectedOccurrenceName string
		)

		BeforeEach(func() {
			expectedOccurrenceId = fake.LetterN(10)
			expectedOccurrenceName = fmt.Sprintf("projects/%s/occurrences/%s", expectedProjectId, expectedOccurrenceId)
			transport.PreparedHttpResponses = []*http.Response{
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
			Expect(transport.ReceivedHttpRequests).To(HaveLen(1))

			Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_search", expectedOccurrencesAlias)))
			Expect(transport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodGet))

			requestBody, err := ioutil.ReadAll(transport.ReceivedHttpRequests[0].Body)
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

	Context("creating a new Grafeas occurrence", func() {
		var (
			actualOccurrence       *pb.Occurrence
			expectedOccurrence     *pb.Occurrence
			expectedOccurrenceESId string
			actualErr              error
		)

		// BeforeEach configures the happy path for this context
		// Variables configured here may be overridden in nested BeforeEach blocks
		BeforeEach(func() {
			expectedOccurrenceESId = fake.LetterN(10)
			expectedOccurrence = generateTestOccurrence("")

			transport.PreparedHttpResponses = []*http.Response{
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

			transport.PreparedHttpResponses[0].Body = structToJsonBody(&esIndexDocResponse{
				Id: expectedOccurrenceESId,
			})
			actualOccurrence, actualErr = elasticsearchStorage.CreateOccurrence(context.Background(), expectedProjectId, "", occurrence)
		})

		It("should attempt to index the occurrence as a document", func() {
			Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_doc", expectedOccurrencesAlias)))

			requestBody, err := ioutil.ReadAll(transport.ReceivedHttpRequests[0].Body)
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
				Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("true"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshWaitFor), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshWaitFor
			})

			It("should wait for refresh of index", func() {
				Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("wait_for"))
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

		When("indexing the document fails", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[0] = &http.Response{
					StatusCode: http.StatusInternalServerError,
					Body: structToJsonBody(&esIndexDocResponse{
						Error: &esIndexDocError{
							Type:   fake.LetterN(10),
							Reason: fake.LetterN(10),
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
			expectedErrs        []error
			actualErrs          []error
			actualOccurrences   []*pb.Occurrence
			expectedOccurrences []*pb.Occurrence
		)

		// BeforeEach configures the happy path for this context
		// Variables configured here may be overridden in nested BeforeEach blocks
		BeforeEach(func() {
			expectedOccurrences = generateTestOccurrences(fake.Number(2, 5))
			for i := 0; i < len(expectedOccurrences); i++ {
				expectedErrs = append(expectedErrs, nil)
			}

			transport.PreparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body:       createEsBulkOccurrenceIndexResponse(expectedOccurrences, expectedErrs),
				},
			}
		})

		// JustBeforeEach actually invokes the system under test
		JustBeforeEach(func() {
			occurrences := deepCopyOccurrences(expectedOccurrences)

			transport.PreparedHttpResponses[0].Body = createEsBulkOccurrenceIndexResponse(occurrences, expectedErrs)
			actualOccurrences, actualErrs = elasticsearchStorage.BatchCreateOccurrences(context.Background(), expectedProjectId, "", occurrences)
		})

		// this test parses the ndjson request body and ensures that it was formatted correctly
		It("should send a bulk request to ES to index each occurrence", func() {
			var expectedPayloads []interface{}

			for i := 0; i < len(expectedOccurrences); i++ {
				expectedPayloads = append(expectedPayloads, &esBulkQueryFragment{}, &pb.Occurrence{})
			}

			parseEsBulkIndexRequest(transport.ReceivedHttpRequests[0].Body, expectedPayloads)

			for i, payload := range expectedPayloads {
				if i%2 == 0 { // index metadata
					metadata := payload.(*esBulkQueryFragment)
					Expect(metadata.Index.Index).To(Equal(expectedOccurrencesAlias))
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
				Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("true"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshWaitFor), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshWaitFor
			})

			It("should wait for refresh of index", func() {
				Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("wait_for"))
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
				transport.PreparedHttpResponses[0].StatusCode = http.StatusInternalServerError
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
				randomErrorIndex = fake.Number(0, len(expectedOccurrences)-1)
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

	Context("updating a Grafeas occurrence", func() {
		var (
			currentOccurrence *pb.Occurrence

			expectedOccurrence     *pb.Occurrence
			occurrencePatchData    *pb.Occurrence
			expectedOccurrenceId   string
			expectedOccurrenceName string
			fieldMask              *fieldmaskpb.FieldMask
			actualErr              error
			actualOccurrence       *pb.Occurrence
		)

		// BeforeEach configures the happy path for this context
		// Variables configured here may be overridden in nested BeforeEach blocks
		BeforeEach(func() {
			expectedOccurrenceId = fake.LetterN(10)
			expectedOccurrenceName = fmt.Sprintf("projects/%s/occurrences/%s", expectedProjectId, expectedOccurrenceId)
			currentOccurrence = generateTestOccurrence("")
			occurrencePatchData = &grafeas_go_proto.Occurrence{
				Resource: &grafeas_go_proto.Resource{
					Uri: "updatedvalue",
				},
			}
			fieldMask = &fieldmaskpb.FieldMask{
				Paths: []string{"Resource.Uri"},
			}
			expectedOccurrence = currentOccurrence
			expectedOccurrence.Resource.Uri = "updatedvalue"

			transport.PreparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body:       createGenericEsSearchResponse(currentOccurrence),
				},
				{
					StatusCode: http.StatusCreated,
					Body: structToJsonBody(&esIndexDocResponse{
						Result: "updated",
					}),
				},
			}
		})

		// JustBeforeEach actually invokes the system under test
		JustBeforeEach(func() {
			// actualOccurrence, actualErr = elasticsearchStorage.UpdateOccurrence(context.Background(), expectedProjectId, "", occurrence, nil)
			actualOccurrence, actualErr = elasticsearchStorage.UpdateOccurrence(context.Background(), expectedProjectId, expectedOccurrenceId, occurrencePatchData, fieldMask)
		})

		It("should have sent a request to elasticsearch to retreive the occurrence document", func() {
			Expect(transport.ReceivedHttpRequests).To(HaveLen(2))
			Expect(transport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodGet))
			Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_search", expectedOccurrencesAlias)))

			requestBody, err := ioutil.ReadAll(transport.ReceivedHttpRequests[0].Body)
			Expect(err).ToNot(HaveOccurred())

			searchBody := &esSearch{}
			err = json.Unmarshal(requestBody, searchBody)
			Expect(err).ToNot(HaveOccurred())

			Expect((*searchBody.Query.Term)["name"]).To(Equal(expectedOccurrenceName))
		})

		It("should have sent a request to elasticsearch to update the occurrence document", func() {
			Expect(transport.ReceivedHttpRequests).To(HaveLen(2))
			Expect(transport.ReceivedHttpRequests[1].Method).To(Equal(http.MethodPost))
			Expect(transport.ReceivedHttpRequests[1].URL.Path).To(Equal(fmt.Sprintf("/%s/_doc", expectedOccurrencesAlias)))

			_, err := ioutil.ReadAll(transport.ReceivedHttpRequests[1].Body)
			Expect(err).ToNot(HaveOccurred())
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshTrue), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshTrue
			})

			It("should immediately refresh the index", func() {
				Expect(transport.ReceivedHttpRequests[1].URL.Query().Get("refresh")).To(Equal("true"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshWaitFor), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshWaitFor
			})

			It("should immediately refresh the index", func() {
				Expect(transport.ReceivedHttpRequests[1].URL.Query().Get("refresh")).To(Equal("wait_for"))
			})
		})

		When(fmt.Sprintf("refresh configuration is %s", config.RefreshFalse), func() {
			BeforeEach(func() {
				esConfig.Refresh = config.RefreshFalse
			})

			It("should not wait or force refresh of index", func() {
				Expect(transport.ReceivedHttpRequests[1].URL.Query().Get("refresh")).To(Equal("false"))
			})
		})

		When("elasticsearch successfully updates the occurrence document", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[1].Body = structToJsonBody(&esIndexDocResponse{
					Result: "updated",
				})
			})

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

		When("elasticsearch fails to update the occurrence document", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[0].StatusCode = http.StatusInternalServerError
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})

		When("using a badly formatted field mask", func() {
			BeforeEach(func() {
				fieldMask = &fieldmaskpb.FieldMask{
					Paths: []string{"Resource..bro"},
				}
			})
			It("should return an error", func() {
				Expect(actualErr).To(HaveOccurred())
			})
		})
	})

	Context("deleting a Grafeas occurrence", func() {
		var (
			actualErr              error
			expectedOccurrenceId   string
			expectedOccurrenceName string
		)

		BeforeEach(func() {
			expectedOccurrenceId = fake.LetterN(10)
			expectedOccurrenceName = fmt.Sprintf("projects/%s/occurrences/%s", expectedProjectId, expectedOccurrenceId)

			transport.PreparedHttpResponses = []*http.Response{
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
			Expect(transport.ReceivedHttpRequests).To(HaveLen(1))
			Expect(transport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodPost))
			Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_delete_by_query", expectedOccurrencesAlias)))

			requestBody, err := ioutil.ReadAll(transport.ReceivedHttpRequests[0].Body)
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

		When("elasticsearch successfully deletes the occurrence document", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[0].Body = structToJsonBody(&esDeleteResponse{
					Deleted: 1,
				})
			})

			It("should not return an error", func() {
				Expect(actualErr).ToNot(HaveOccurred())
			})
		})

		When("the occurrence does not exist", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[0].Body = structToJsonBody(&esDeleteResponse{
					Deleted: 0,
				})
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})

		When("deleting the occurrence document fails", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[0].StatusCode = http.StatusInternalServerError
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})
		})
	})

	Context("listing Grafeas occurrences", func() {
		var (
			actualErr           error
			actualOccurrences   []*pb.Occurrence
			expectedOccurrences []*pb.Occurrence
			expectedFilter      string
			expectedQuery       *filtering.Query
		)

		BeforeEach(func() {
			expectedQuery = &filtering.Query{}
			expectedFilter = ""
			expectedOccurrences = generateTestOccurrences(fake.Number(2, 5))
			transport.PreparedHttpResponses = []*http.Response{
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
			Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_search", expectedOccurrencesAlias)))
			Expect(transport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodGet))
			Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("size")).To(Equal(strconv.Itoa(grafeasMaxPageSize)))

			requestBody, err := ioutil.ReadAll(transport.ReceivedHttpRequests[0].Body)
			Expect(err).ToNot(HaveOccurred())

			searchBody := &esSearch{}
			err = json.Unmarshal(requestBody, searchBody)
			Expect(err).ToNot(HaveOccurred())
			Expect(searchBody.Query).To(BeNil())
			Expect(searchBody.Sort[sortField]).To(Equal(esSortOrderDecending))
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
				Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_search", expectedOccurrencesAlias)))
				Expect(transport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodGet))

				requestBody, err := ioutil.ReadAll(transport.ReceivedHttpRequests[0].Body)
				Expect(err).ToNot(HaveOccurred())

				searchBody := &esSearch{}
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
				transport.PreparedHttpResponses = []*http.Response{
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
			Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_search", expectedNotesAlias)))
			Expect(transport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodGet))

			requestBody, err := ioutil.ReadAll(transport.ReceivedHttpRequests[0].Body)
			Expect(err).ToNot(HaveOccurred())

			searchBody := &esSearch{}
			err = json.Unmarshal(requestBody, searchBody)
			Expect(err).ToNot(HaveOccurred())

			Expect((*searchBody.Query.Term)["name"]).To(Equal(expectedNoteName))
		})

		When("a note with the specified noteId does not exist", func() {
			It("should attempt to index the note as a document", func() {
				Expect(transport.ReceivedHttpRequests).To(HaveLen(2))

				Expect(transport.ReceivedHttpRequests[1].URL.Path).To(Equal(fmt.Sprintf("/%s/_doc", expectedNotesAlias)))

				requestBody, err := ioutil.ReadAll(transport.ReceivedHttpRequests[1].Body)
				Expect(err).ToNot(HaveOccurred())

				indexedNote := &pb.Note{}
				err = protojson.Unmarshal(requestBody, proto.MessageV2(indexedNote))
				Expect(err).ToNot(HaveOccurred())

				Expect(indexedNote).To(BeEquivalentTo(expectedNote))
			})

			When("indexing the document fails", func() {
				BeforeEach(func() {
					transport.PreparedHttpResponses[0] = &http.Response{
						StatusCode: http.StatusInternalServerError,
						Body: structToJsonBody(&esIndexDocResponse{
							Error: &esIndexDocError{
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
					Expect(transport.ReceivedHttpRequests[1].URL.Query().Get("refresh")).To(Equal("true"))
				})
			})

			When(fmt.Sprintf("refresh configuration is %s", config.RefreshWaitFor), func() {
				BeforeEach(func() {
					esConfig.Refresh = config.RefreshWaitFor
				})

				It("should wait for refresh of index", func() {
					Expect(transport.ReceivedHttpRequests[1].URL.Query().Get("refresh")).To(Equal("wait_for"))
				})
			})

			When(fmt.Sprintf("refresh configuration is %s", config.RefreshFalse), func() {
				BeforeEach(func() {
					esConfig.Refresh = config.RefreshFalse
				})

				It("should not wait or force refresh of index", func() {
					Expect(transport.ReceivedHttpRequests[1].URL.Query().Get("refresh")).To(Equal("false"))
				})
			})
		})

		When("a note with the specified noteId exists", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[0].Body = createGenericEsSearchResponse(&pb.Note{
					Name: expectedNoteName,
				})
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.AlreadyExists)
			})

			It("should not attempt to index a note document", func() {
				Expect(transport.ReceivedHttpRequests).To(HaveLen(1))
			})
		})

		When("checking for the existence of the note fails", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[0].StatusCode = http.StatusInternalServerError
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})

			It("should not attempt to index a note document", func() {
				Expect(transport.ReceivedHttpRequests).To(HaveLen(1))
			})
		})

		When("elasticsearch returns a bad response when checking if a note exists", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[0].Body = ioutil.NopCloser(strings.NewReader("bad object"))
			})

			It("should return an error", func() {
				assertErrorHasGrpcStatusCode(actualErr, codes.Internal)
			})

			It("should not attempt to index a note document", func() {
				Expect(transport.ReceivedHttpRequests).To(HaveLen(1))
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
				expectedPayloads = append(expectedPayloads, &esMultiSearchQueryFragment{}, &esSearch{})
			}

			parseEsMsearchIndexRequest(transport.ReceivedHttpRequests[0].Body, expectedPayloads)

			for i, payload := range expectedPayloads {
				if i%2 == 0 { // index metadata
					metadata := payload.(*esMultiSearchQueryFragment)
					Expect(metadata.Index).To(Equal(expectedNotesAlias))
				} else { // note
					Expect(payload).To(BeAssignableToTypeOf(&esSearch{}))
					Expect(map[string]string(*payload.(*esSearch).Query.Term)["name"]).To(MatchRegexp("projects/%s/notes/\\w+", expectedProjectId))
				}
			}
		})

		When("the multisearch request returns an error", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[0].StatusCode = http.StatusInternalServerError
			})

			It("should return a single error and no notes", func() {
				Expect(actualErrs).To(HaveLen(1))
				assertErrorHasGrpcStatusCode(actualErrs[0], codes.Internal)
			})

			It("should not attempt to index any notes", func() {
				Expect(transport.ReceivedHttpRequests).To(HaveLen(1))
			})
		})

		When("the multisearch request returns no notes already exist", func() {
			It("should attempt to bulk index all of the notes", func() {
				var expectedPayloads []interface{}

				for i := 0; i < len(expectedNotes); i++ {
					expectedPayloads = append(expectedPayloads, &esBulkQueryFragment{}, &pb.Note{})
				}

				parseEsBulkIndexRequest(transport.ReceivedHttpRequests[1].Body, expectedPayloads)

				for i, payload := range expectedPayloads {
					if i%2 == 0 { // index metadata
						metadata := payload.(*esBulkQueryFragment)
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
					transport.PreparedHttpResponses[1].StatusCode = http.StatusInternalServerError
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
					Expect(transport.ReceivedHttpRequests[1].URL.Query().Get("refresh")).To(Equal("true"))
				})
			})

			When(fmt.Sprintf("refresh configuration is %s", config.RefreshWaitFor), func() {
				BeforeEach(func() {
					esConfig.Refresh = config.RefreshWaitFor
				})

				It("should wait for refresh of index", func() {
					Expect(transport.ReceivedHttpRequests[1].URL.Query().Get("refresh")).To(Equal("wait_for"))
				})
			})

			When(fmt.Sprintf("refresh configuration is %s", config.RefreshFalse), func() {
				BeforeEach(func() {
					esConfig.Refresh = config.RefreshFalse
				})

				It("should not wait or force refresh of index", func() {
					Expect(transport.ReceivedHttpRequests[1].URL.Query().Get("refresh")).To(Equal("false"))
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

			searchBody := &esSearch{}
			err = json.Unmarshal(requestBody, searchBody)
			Expect(err).ToNot(HaveOccurred())
			Expect(searchBody.Query).To(BeNil())
			Expect(searchBody.Sort[sortField]).To(Equal(esSortOrderDecending))
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

				searchBody := &esSearch{}
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
			Expect(transport.ReceivedHttpRequests).To(HaveLen(1))
			Expect(transport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodPost))
			Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_delete_by_query", expectedNotesAlias)))

			requestBody, err := ioutil.ReadAll(transport.ReceivedHttpRequests[0].Body)
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
				transport.PreparedHttpResponses[0].Body = structToJsonBody(&esDeleteResponse{
					Deleted: 1,
				})
			})

			It("should not return an error", func() {
				Expect(actualErr).ToNot(HaveOccurred())
			})
		})

		When("the note does not exist", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[0].Body = structToJsonBody(&esDeleteResponse{
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
		Took: fake.Number(1, 10),
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
		Took: fake.Number(1, 10),
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
				Type:   fake.LetterN(10),
				Reason: fake.LetterN(10),
			}
			responseCode = http.StatusInternalServerError
			responseHasErrors = true
		}

		responseItems = append(responseItems, &esBulkResponseItem{
			Index: &esIndexDocResponse{
				Id:     fake.LetterN(10),
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

func createEsBulkNoteIndexResponse(notesThatCreatedSuccessfully map[string]*pb.Note) io.ReadCloser {
	var responseItems []*esBulkResponseItem
	for range notesThatCreatedSuccessfully {
		responseItems = append(responseItems, &esBulkResponseItem{
			Index: &esIndexDocResponse{
				Id:     fake.LetterN(10),
				Status: http.StatusCreated,
			},
		})
	}

	response := &esBulkResponse{
		Items:  responseItems,
		Errors: false,
	}

	responseBody, err := json.Marshal(response)
	Expect(err).ToNot(HaveOccurred())

	return ioutil.NopCloser(bytes.NewReader(responseBody))
}

func createEsMultiSearchNoteResponse(notes map[string]*pb.Note) io.ReadCloser {
	multiSearchResponse := &esMultiSearchResponse{}

	for range notes {
		multiSearchResponse.Responses = append(multiSearchResponse.Responses, &esMultiSearchResponseHitsSummary{
			Hits: &esMultiSearchResponseHits{
				Total: &esSearchResponseTotal{
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
