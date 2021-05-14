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

package migration

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rode/grafeas-elasticsearch/go/mocks"
	"github.com/rode/grafeas-elasticsearch/go/v1beta1/storage/esutil"
)

var _ = Describe("EsMigrator", func() {
	var (
		ctx             context.Context
		mockEsTransport *esutil.MockEsTransport
		migrator        *EsMigrator
		mockCtrl        *gomock.Controller
		newIndexInfo    *esutil.IndexInfo
		indexManager    *mocks.MockIndexManager
	)
	BeforeEach(func() {
		ctx = context.Background()
		timeSleep = func(duration time.Duration) {}
		mockEsTransport = &esutil.MockEsTransport{}
		mockEsClient := &elasticsearch.Client{Transport: mockEsTransport, API: esapi.New(mockEsTransport)}
		mockCtrl = gomock.NewController(GinkgoT())

		indexManager = mocks.NewMockIndexManager(mockCtrl)

		migrator = NewEsMigrator(logger, mockEsClient, indexManager)
	})

	Describe("GetMigrations", func() {
		var (
			actualMigrations  []*esutil.IndexInfo
			actualError       error
			expectedAliasName string
		)

		BeforeEach(func() {
			mockEsTransport.PreparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body: esutil.CreateESBody(map[string]interface{}{
						"grafeas-v1beta1-projects": map[string]interface{}{
							"mappings": map[string]interface{}{
								"_meta": map[string]interface{}{
									"type": "grafeas",
								},
							},
						},
					}),
				},
			}
			expectedAliasName = fake.LetterN(5)
			indexManager.EXPECT().GetLatestVersionForDocumentKind(esutil.ProjectDocumentKind).Return(fake.LetterN(5)).AnyTimes()
			indexManager.EXPECT().GetAliasForIndex("grafeas-v1beta1-projects").Return(expectedAliasName).AnyTimes()
		})

		JustBeforeEach(func() {
			actualMigrations, actualError = migrator.GetMigrations(ctx)
		})

		It("should return all indices", func() {
			Expect(actualError).To(BeNil())
			Expect(mockEsTransport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodGet))
			Expect(mockEsTransport.ReceivedHttpRequests[0].URL.Path).To(Equal("/_all"))
		})

		It("should return a list of 1 migration", func() {
			Expect(actualError).To(BeNil())
			Expect(actualMigrations).To(HaveLen(1))
			Expect(actualMigrations[0].Index).To(Equal("grafeas-v1beta1-projects"))
			Expect(actualMigrations[0].DocumentKind).To(Equal(esutil.ProjectDocumentKind))
			Expect(actualMigrations[0].Alias).To(Equal(expectedAliasName))
		})

		Describe("no index names begin with grafeas", func() {
			BeforeEach(func() {
				mockEsTransport.PreparedHttpResponses[0].Body = esutil.CreateESBody(map[string]interface{}{
					fake.Word(): map[string]interface{}{
						"mappings": map[string]interface{}{},
					},
				})
			})

			It("should find no migrations", func() {
				Expect(actualError).To(BeNil())
				Expect(actualMigrations).To(BeEmpty())
			})

		})

		Describe("index name matches grafeas but meta type does not", func() {
			BeforeEach(func() {
				mockEsTransport.PreparedHttpResponses[0].Body = esutil.CreateESBody(map[string]interface{}{
					"grafeas-v1beta1-projects": map[string]interface{}{
						"mappings": map[string]interface{}{
							"_meta": map[string]interface{}{
								"type": fake.Word(),
							},
						},
					},
				})
			})

			It("should find no migrations", func() {
				Expect(actualError).To(BeNil())
				Expect(actualMigrations).To(BeEmpty())
			})
		})
	})

	Describe("Migrate", func() {
		var (
			taskId       string
			projectId    string
			migration    *esutil.IndexInfo
			newIndexName string
		)

		BeforeEach(func() {
			taskId = fake.LetterN(10)
			projectId = fake.LetterN(5)
			migration = &esutil.IndexInfo{
				DocumentKind: esutil.OccurrenceDocumentKind,
				Index:        esutil.CreateIndexOrAliasName(fake.LetterN(5), projectId, string(esutil.OccurrenceDocumentKind)),
				Alias:        esutil.CreateIndexOrAliasName(projectId, string(esutil.OccurrenceDocumentKind)),
			}
			newIndexName = fake.LetterN(5)
			newIndexInfo = &esutil.IndexInfo{
				DocumentKind: migration.DocumentKind,
				Index:        newIndexName,
				Alias:        migration.Alias,
			}
			indexManager.EXPECT().IncrementIndexVersion(migration.Index).Return(newIndexName).AnyTimes()
			indexManager.EXPECT().GetLatestVersionForDocumentKind(esutil.OccurrenceDocumentKind).Return(fake.LetterN(5)).AnyTimes()

			mockEsTransport.PreparedHttpResponses = []*http.Response{
				// get index settings
				{
					StatusCode: http.StatusOK,
					Body: esutil.CreateESBody(map[string]interface{}{
						migration.Index: esutil.ESSettingsResponse{
							Settings: &esutil.ESSettingsIndex{
								Index: &esutil.ESSettingsBlocks{
									Blocks: &esutil.ESSettingsWrite{
										Write: "false",
									},
								},
							},
						},
					}),
				},
				// add write block
				{
					StatusCode: http.StatusOK,
					Body: esutil.CreateESBody(&esutil.ESBlockResponse{
						Acknowledged:       true,
						ShardsAcknowledged: true,
					}),
				},
				// reindex
				{
					StatusCode: http.StatusOK,
					Body: esutil.CreateESBody(&esutil.ESTaskCreationResponse{
						Task: taskId,
					}),
				},
				// poll task
				{
					StatusCode: http.StatusOK,
					Body: esutil.CreateESBody(&esutil.ESTask{
						Completed: true,
					}),
				},
				// delete task document
				{
					StatusCode: http.StatusOK,
				},
				// update aliases
				{
					StatusCode: http.StatusOK,
				},
				// delete old index
				{
					StatusCode: http.StatusOK,
				},
			}
		})

		Describe("successful migration", func() {
			var actualError error

			BeforeEach(func() {
				indexManager.EXPECT().CreateIndex(ctx, newIndexInfo, true).Return(nil).Times(1)
				actualError = migrator.Migrate(ctx, migration)
			})

			It("should not return an error", func() {
				Expect(actualError).To(BeNil())
			})

			It("should check if the source index already has a write block", func() {
				Expect(mockEsTransport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodGet))
				Expect(mockEsTransport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_settings", migration.Index)))
			})

			It("should place a write block on the source index", func() {
				Expect(mockEsTransport.ReceivedHttpRequests[1].Method).To(Equal(http.MethodPut))
				Expect(mockEsTransport.ReceivedHttpRequests[1].URL.Path).To(Equal(fmt.Sprintf("/%s/_block/write", migration.Index)))
			})

			It("should start a reindex on the existing index to the new index", func() {
				expectedBody := &esutil.ESReindex{
					Conflicts: "proceed",
					Source: &esutil.ReindexFields{
						Index: migration.Index,
					},
					Destination: &esutil.ReindexFields{
						Index:  newIndexName,
						OpType: "create",
					},
				}
				actualBody := &esutil.ESReindex{}

				esutil.ReadRequestBody(mockEsTransport.ReceivedHttpRequests[2], actualBody)

				Expect(mockEsTransport.ReceivedHttpRequests[2].Method).To(Equal(http.MethodPost))
				Expect(mockEsTransport.ReceivedHttpRequests[2].URL.Path).To(Equal("/_reindex"))
				Expect(mockEsTransport.ReceivedHttpRequests[2].URL.Query().Get("wait_for_completion")).To(Equal("false"))
				Expect(actualBody).To(Equal(expectedBody))
			})

			It("should poll for the reindex task to complete", func() {
				Expect(mockEsTransport.ReceivedHttpRequests[3].Method).To(Equal(http.MethodGet))
				Expect(mockEsTransport.ReceivedHttpRequests[3].URL.Path).To(Equal("/_tasks/" + taskId))
			})

			It("should delete the task document once the reindex has finished", func() {
				Expect(mockEsTransport.ReceivedHttpRequests[4].Method).To(Equal(http.MethodDelete))
				Expect(mockEsTransport.ReceivedHttpRequests[4].URL.Path).To(Equal(fmt.Sprintf("/%s/_doc/%s", ".tasks", taskId)))
			})

			It("should point the alias to the new index", func() {
				expectedBody := &esutil.ESIndexAliasRequest{
					Actions: []esutil.ESActions{
						{
							Remove: &esutil.ESIndexAlias{
								Index: migration.Index,
								Alias: migration.Alias,
							},
						},
						{
							Add: &esutil.ESIndexAlias{
								Index: newIndexName,
								Alias: migration.Alias,
							},
						},
					},
				}
				actualBody := &esutil.ESIndexAliasRequest{}
				esutil.ReadRequestBody(mockEsTransport.ReceivedHttpRequests[5], actualBody)

				Expect(mockEsTransport.ReceivedHttpRequests[5].Method).To(Equal(http.MethodPost))
				Expect(mockEsTransport.ReceivedHttpRequests[5].URL.Path).To(Equal("/_aliases"))
				Expect(actualBody).To(Equal(expectedBody))
			})

			It("should delete the source index", func() {
				Expect(mockEsTransport.ReceivedHttpRequests[6].Method).To(Equal(http.MethodDelete))
				Expect(mockEsTransport.ReceivedHttpRequests[6].URL.Path).To(Equal("/" + migration.Index))
			})
		})

		Describe("migration errors", func() {
			var (
				createIndexError error
				actualError      error
			)

			JustBeforeEach(func() {
				indexManager.EXPECT().CreateIndex(ctx, newIndexInfo, true).Return(createIndexError).Times(1)
				actualError = migrator.Migrate(ctx, migration)
			})

			Describe("error checking if the source index has a write block", func() {
				BeforeEach(func() {
					mockEsTransport.PreparedHttpResponses[0].StatusCode = http.StatusInternalServerError
				})

				It("should return an error", func() {
					Expect(actualError).NotTo(BeNil())
					Expect(actualError.Error()).To(ContainSubstring("error checking if write block is enabled on index"))
				})
			})

			Describe("error decoding the index settings response", func() {
				BeforeEach(func() {
					mockEsTransport.PreparedHttpResponses[0].Body = createInvalidBody()
				})

				It("should return an error", func() {
					Expect(actualError).NotTo(BeNil())
					Expect(actualError.Error()).To(ContainSubstring("error decoding settings response"))
				})
			})

			Describe("error placing write block on the source index", func() {
				BeforeEach(func() {
					mockEsTransport.PreparedHttpResponses[1].StatusCode = http.StatusInternalServerError
				})

				It("should return an error", func() {
					Expect(actualError).NotTo(BeNil())
					Expect(actualError.Error()).To(ContainSubstring("error placing write block on index"))
				})
			})

			Describe("error decoding write block response", func() {
				BeforeEach(func() {
					mockEsTransport.PreparedHttpResponses[1].Body = createInvalidBody()
				})

				It("should return an error", func() {
					Expect(actualError).NotTo(BeNil())
					Expect(actualError.Error()).To(ContainSubstring("error decoding write block response"))
				})
			})

			Describe("write block isn't acknowledged", func() {
				BeforeEach(func() {
					mockEsTransport.PreparedHttpResponses[1].Body = esutil.CreateESBody(&esutil.ESBlockResponse{
						Acknowledged: false,
					})
				})

				It("should return an error", func() {
					Expect(actualError).NotTo(BeNil())
					Expect(actualError.Error()).To(ContainSubstring("unable to block writes for index: " + migration.Index))
				})
			})

			Describe("write block isn't acknowledged by shards", func() {
				BeforeEach(func() {
					mockEsTransport.PreparedHttpResponses[1].Body = esutil.CreateESBody(&esutil.ESBlockResponse{
						Acknowledged:       true,
						ShardsAcknowledged: false,
					})
				})

				It("should return an error", func() {
					Expect(actualError).NotTo(BeNil())
					Expect(actualError.Error()).To(ContainSubstring("unable to block writes for index: " + migration.Index))
				})
			})

			Describe("creating the index fails", func() {
				BeforeEach(func() {
					createIndexError = fmt.Errorf(fake.Word())
				})

				AfterEach(func() {
					createIndexError = nil
				})

				It("should return an error", func() {
					Expect(actualError).NotTo(BeNil())
					Expect(actualError.Error()).To(ContainSubstring("error creating target index"))
				})
			})

			Describe("reindex request fails", func() {
				BeforeEach(func() {
					mockEsTransport.PreparedHttpResponses[2].StatusCode = http.StatusInternalServerError
				})

				It("should return an error", func() {
					Expect(actualError).NotTo(BeNil())
					Expect(actualError.Error()).To(ContainSubstring("error initiating reindex"))
				})
			})

			Describe("error decoding reindex response", func() {
				BeforeEach(func() {
					mockEsTransport.PreparedHttpResponses[2].Body = createInvalidBody()
				})

				It("should return an error", func() {
					Expect(actualError).NotTo(BeNil())
					Expect(actualError.Error()).To(ContainSubstring("error decoding reindex response"))
				})
			})

			Describe("reindexing doesn't complete in time", func() {
				BeforeEach(func() {
					responses := make([]*http.Response, 13)

					for i := 0; i < len(responses); i++ {
						if i >= 3 {
							responses[i] = &http.Response{
								StatusCode: http.StatusOK,
								Body: esutil.CreateESBody(&esutil.ESTask{
									Completed: false,
								}),
							}
						} else {
							responses[i] = mockEsTransport.PreparedHttpResponses[i]
						}
					}

					mockEsTransport.PreparedHttpResponses = responses
				})

				It("should return an error", func() {
					Expect(actualError).NotTo(BeNil())
					Expect(actualError.Error()).To(ContainSubstring("reindex did not complete after 10 polls"))
				})
			})

			Describe("error in the task response", func() {
				BeforeEach(func() {
					mockEsTransport.PreparedHttpResponses[3].StatusCode = http.StatusInternalServerError
					taskCompletedResponse := &http.Response{
						StatusCode: http.StatusOK,
						Body: esutil.CreateESBody(&esutil.ESTask{
							Completed: true,
						}),
					}
					mockEsTransport.PreparedHttpResponses = insertResponseAt(mockEsTransport.PreparedHttpResponses, taskCompletedResponse, 4)
				})

				It("should continue and not return an error", func() {
					Expect(actualError).To(BeNil())
				})
			})

			Describe("error decoding task response", func() {
				BeforeEach(func() {
					mockEsTransport.PreparedHttpResponses[3].Body = createInvalidBody()

					taskCompletedResponse := &http.Response{
						StatusCode: http.StatusOK,
						Body: esutil.CreateESBody(&esutil.ESTask{
							Completed: true,
						}),
					}
					mockEsTransport.PreparedHttpResponses = insertResponseAt(mockEsTransport.PreparedHttpResponses, taskCompletedResponse, 4)
				})

				It("should continue and not return an error", func() {
					Expect(actualError).To(BeNil())
				})
			})

			Describe("error occurs while deleting the reindex task document", func() {
				BeforeEach(func() {
					mockEsTransport.PreparedHttpResponses[4].StatusCode = http.StatusInternalServerError
				})

				It("should not return an error", func() {
					Expect(actualError).To(BeNil())
				})
			})

			Describe("an error occurs while updating the alias", func() {
				BeforeEach(func() {
					mockEsTransport.PreparedHttpResponses[5].StatusCode = http.StatusInternalServerError
				})

				It("should return an error", func() {
					Expect(actualError).NotTo(BeNil())
					Expect(actualError.Error()).To(ContainSubstring("error occurred while swapping the alias"))
				})
			})

			Describe("an error occurs while deleting the source index", func() {
				BeforeEach(func() {
					mockEsTransport.PreparedHttpResponses[6].StatusCode = http.StatusInternalServerError
				})

				It("should return an error", func() {
					Expect(actualError).NotTo(BeNil())
					Expect(actualError.Error()).To(ContainSubstring("failed to remove the previous index"))
				})
			})
		})
	})
})

func createInvalidBody() io.ReadCloser {
	return esutil.CreateESBody('{')
}

func insertResponseAt(allResponses []*http.Response, response *http.Response, index int) []*http.Response {
	return append(append(allResponses[:index], response), allResponses[index:]...)
}