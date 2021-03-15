package migration

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rode/grafeas-elasticsearch/go/v1beta1/storage/esutil"
)

var _ = Describe("ESMigrator", func() {
	var (
		ctx             context.Context
		mockEsTransport *esutil.MockEsTransport
		indexManager    *EsIndexManager
		migrator        *ESMigrator
	)
	BeforeEach(func() {
		ctx = context.Background()
		mockEsTransport = &esutil.MockEsTransport{}
		mockEsClient := &elasticsearch.Client{Transport: mockEsTransport, API: esapi.New(mockEsTransport)}
		indexManager = NewEsIndexManager(logger, mockEsClient)
		populateIndexMappings(indexManager)

		migrator = NewESMigrator(logger, mockEsClient, indexManager, func(duration time.Duration) {})
	})

	Describe("GetMigrations", func() {
		BeforeEach(func() {
			mockEsTransport.PreparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body: createEsBody(map[string]interface{}{
						"grafeas-v1beta1-projects": map[string]interface{}{},
					}),
				},
			}
		})

		It("should return all indices", func() {
			migrator.GetMigrations(ctx)

			Expect(mockEsTransport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodGet))
			Expect(mockEsTransport.ReceivedHttpRequests[0].URL.Path).To(Equal("/_all"))
		})

		It("should return list of 1 migrations", func() {
			migrationArr, err := migrator.GetMigrations(ctx)

			Expect(err).To(BeNil())
			Expect(migrationArr).To(HaveLen(1))
			Expect(migrationArr[0].Index).To(Equal("grafeas-v1beta1-projects"))
			Expect(migrationArr[0].DocumentKind).To(Equal(ProjectDocumentKind))
			Expect(migrationArr[0].Alias).To(Equal("grafeas-projects"))
		})

		It("should find no migrations", func() {
			mockEsTransport.PreparedHttpResponses[0].Body = createEsBody(map[string]interface{}{
				fake.Word(): map[string]interface{}{},
			})
			migrationArr, err := migrator.GetMigrations(ctx)
			Expect(err).To(BeNil())
			Expect(migrationArr).To(BeEmpty())
		})
	})

	Describe("Migrate", func() {
		var (
			taskId       string
			projectId    string
			migration    *Migration
			newIndexName string
		)

		BeforeEach(func() {
			taskId = fake.LetterN(10)
			projectId = fake.LetterN(5)
			migration = &Migration{
				DocumentKind: OccurrenceDocumentKind,
				Index:        createIndexOrAliasName(fake.LetterN(5), projectId, OccurrenceDocumentKind),
				Alias:        createIndexOrAliasName(projectId, OccurrenceDocumentKind),
			}
			newIndexName = createIndexOrAliasName(indexManager.occurrenceMapping.Version, projectId, OccurrenceDocumentKind)

			mockEsTransport.PreparedHttpResponses = []*http.Response{
				// get index settings
				{
					StatusCode: http.StatusOK,
					Body: createEsBody(map[string]interface{}{
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
					Body: createEsBody(&esutil.ESBlockResponse{
						Acknowledged:       true,
						ShardsAcknowledged: true,
					}),
				},
				// check if new index exists
				{
					StatusCode: http.StatusNotFound,
				},
				// create index
				{
					StatusCode: http.StatusOK,
				},
				// reindex
				{
					StatusCode: http.StatusOK,
					Body: createEsBody(&esutil.ESTaskCreationResponse{
						Task: taskId,
					}),
				},
				// poll task
				{
					StatusCode: http.StatusOK,
					Body: createEsBody(&esutil.ESTask{
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

			It("should create a new index, checking first if it already exists", func() {
				Expect(mockEsTransport.ReceivedHttpRequests[2].Method).To(Equal(http.MethodHead))
				Expect(mockEsTransport.ReceivedHttpRequests[2].URL.Path).To(Equal("/" + newIndexName))

				Expect(mockEsTransport.ReceivedHttpRequests[3].Method).To(Equal(http.MethodPut))
				Expect(mockEsTransport.ReceivedHttpRequests[3].URL.Path).To(Equal("/" + newIndexName))
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

				readRequestBody(mockEsTransport.ReceivedHttpRequests[4], actualBody)

				Expect(mockEsTransport.ReceivedHttpRequests[4].Method).To(Equal(http.MethodPost))
				Expect(mockEsTransport.ReceivedHttpRequests[4].URL.Path).To(Equal("/_reindex"))
				Expect(mockEsTransport.ReceivedHttpRequests[4].URL.Query().Get("wait_for_completion")).To(Equal("false"))
				Expect(actualBody).To(Equal(expectedBody))
			})

			It("should poll for the reindex task to complete", func() {
				Expect(mockEsTransport.ReceivedHttpRequests[5].Method).To(Equal(http.MethodGet))
				Expect(mockEsTransport.ReceivedHttpRequests[5].URL.Path).To(Equal("/_tasks/" + taskId))
			})

			It("should delete the task document once the reindex has finished", func() {
				Expect(mockEsTransport.ReceivedHttpRequests[6].Method).To(Equal(http.MethodDelete))
				Expect(mockEsTransport.ReceivedHttpRequests[6].URL.Path).To(Equal(fmt.Sprintf("/%s/_doc/%s", ".tasks", taskId)))
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
				readRequestBody(mockEsTransport.ReceivedHttpRequests[7], actualBody)

				Expect(mockEsTransport.ReceivedHttpRequests[7].Method).To(Equal(http.MethodPost))
				Expect(mockEsTransport.ReceivedHttpRequests[7].URL.Path).To(Equal("/_aliases"))
				Expect(actualBody).To(Equal(expectedBody))
			})

			It("should delete the source index", func() {
				Expect(mockEsTransport.ReceivedHttpRequests[8].Method).To(Equal(http.MethodDelete))
				Expect(mockEsTransport.ReceivedHttpRequests[8].URL.Path).To(Equal("/" + migration.Index))
			})
		})

		Describe("migration errors", func() {
			var actualError error

			JustBeforeEach(func() {
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
					mockEsTransport.PreparedHttpResponses[1].Body = createEsBody(&esutil.ESBlockResponse{
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
					mockEsTransport.PreparedHttpResponses[1].Body = createEsBody(&esutil.ESBlockResponse{
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
					mockEsTransport.PreparedHttpResponses[3].StatusCode = http.StatusInternalServerError
				})

				It("should return an error", func() {
					Expect(actualError).NotTo(BeNil())
					Expect(actualError.Error()).To(ContainSubstring("error creating target index"))
				})
			})

			Describe("reindex request fails", func() {
				BeforeEach(func() {
					mockEsTransport.PreparedHttpResponses[4].StatusCode = http.StatusInternalServerError
				})

				It("should return an error", func() {
					Expect(actualError).NotTo(BeNil())
					Expect(actualError.Error()).To(ContainSubstring("error initiating reindex"))
				})
			})

			Describe("error decoding reindex response", func() {
				BeforeEach(func() {
					mockEsTransport.PreparedHttpResponses[4].Body = createInvalidBody()
				})

				It("should return an error", func() {
					Expect(actualError).NotTo(BeNil())
					Expect(actualError.Error()).To(ContainSubstring("error decoding reindex response"))
				})
			})

			Describe("reindexing doesn't complete in time", func() {
				BeforeEach(func() {
					responses := make([]*http.Response, 15)

					for i := 0; i < len(responses); i++ {
						if i >= 5 {
							responses[i] = &http.Response{
								StatusCode: http.StatusOK,
								Body: createEsBody(&esutil.ESTask{
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
					mockEsTransport.PreparedHttpResponses[5].StatusCode = http.StatusInternalServerError

					responses := append(mockEsTransport.PreparedHttpResponses[:6], &http.Response{
						StatusCode: http.StatusOK,
						Body: createEsBody(&esutil.ESTask{
							Completed: true,
						}),
					})
					mockEsTransport.PreparedHttpResponses = append(responses, mockEsTransport.PreparedHttpResponses[6:]...)
				})

				It("should continue and not return an error", func() {
					Expect(actualError).To(BeNil())
				})
			})

			Describe("error decoding task response", func() {
				BeforeEach(func() {
					mockEsTransport.PreparedHttpResponses[5].Body = createInvalidBody()

					responses := append(mockEsTransport.PreparedHttpResponses[:6], &http.Response{
						StatusCode: http.StatusOK,
						Body: createEsBody(&esutil.ESTask{
							Completed: true,
						}),
					})
					mockEsTransport.PreparedHttpResponses = append(responses, mockEsTransport.PreparedHttpResponses[6:]...)
				})

				It("should continue and not return an error", func() {
					Expect(actualError).To(BeNil())
				})
			})

			Describe("error occurs while deleting the reindex task document", func() {
				BeforeEach(func() {
					mockEsTransport.PreparedHttpResponses[6].StatusCode = http.StatusInternalServerError
				})

				It("should not return an error", func() {
					Expect(actualError).To(BeNil())
				})
			})

			Describe("an error occurs while updating the alias", func() {
				BeforeEach(func() {
					mockEsTransport.PreparedHttpResponses[7].StatusCode = http.StatusInternalServerError
				})

				It("should return an error", func() {
					Expect(actualError).NotTo(BeNil())
					Expect(actualError.Error()).To(ContainSubstring("error occurred while swapping the alias"))
				})
			})

			Describe("an error occurs while deleting the source index", func() {
				BeforeEach(func() {
					mockEsTransport.PreparedHttpResponses[8].StatusCode = http.StatusInternalServerError
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
	return createEsBody('{')
}
