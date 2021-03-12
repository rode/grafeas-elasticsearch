package migration

import (
	"context"
	"fmt"
	"net/http"

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

		migrator = NewESMigrator(logger, mockEsClient, indexManager)
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
			taskId    string
			projectId string
			migration *Migration
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
						migration.Index: ESSettingsResponse{
							Settings: &ESSettingsIndex{
								Index: &ESSettingsBlocks{
									Blocks: &ESSettingsWrite{
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
					Body: createEsBody(&ESBlockResponse{
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
					Body: createEsBody(&ESTaskCreationResponse{
						Task: taskId,
					}),
				},
				// poll task
				{
					StatusCode: http.StatusOK,
					Body: createEsBody(&ESTask{
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
				expectedBody := &ESReindex{
					Conflicts: "proceed",
					Source: &ReindexFields{
						Index:  migration.Index,
					},
					Destination: &ReindexFields{
						Index:  newIndexName,
						OpType: "create",
					},
				}
				actualBody := &ESReindex{}

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
				expectedBody := &ESIndexAliasRequest{
					Actions: []ESActions{
						{
							Remove: &ESIndexAlias{
								Index: migration.Index,
								Alias: migration.Alias,
							},
						},
						{
							Add: &ESIndexAlias{
								Index: newIndexName,
								Alias: migration.Alias,
							},
						},
					},
				}
				actualBody := &ESIndexAliasRequest{}
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
	})
})
