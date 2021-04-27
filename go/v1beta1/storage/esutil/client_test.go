package esutil

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	protov1 "github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/grafeas/grafeas/proto/v1beta1/common_go_proto"
	pb "github.com/grafeas/grafeas/proto/v1beta1/grafeas_go_proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rode/grafeas-elasticsearch/go/v1beta1/storage/filtering"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
)

var _ = Describe("elasticsearch client", func() {
	var (
		client    Client
		transport *MockEsTransport
		ctx       context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()

		transport = &MockEsTransport{}
	})

	JustBeforeEach(func() {
		mockEsClient := &elasticsearch.Client{Transport: transport, API: esapi.New(transport)}
		client = NewClient(logger, mockEsClient)
	})

	Context("Create", func() {
		var (
			actualDocumentId string
			actualErr        error

			expectedCreateRequest *CreateRequest
			expectedDocumentId    string
			expectedIndex         string
			expectedMessage       proto.Message
			expectedOccurrence    *pb.Occurrence
		)

		BeforeEach(func() {
			expectedDocumentId = fake.LetterN(10)
			expectedIndex = fake.LetterN(10)
			expectedOccurrence = createRandomOccurrence()
			expectedMessage = protov1.MessageV2(expectedOccurrence)
			expectedCreateRequest = &CreateRequest{
				Index:   expectedIndex,
				Message: expectedMessage,
			}

			transport.PreparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body: structToJsonBody(&EsIndexDocResponse{
						Id: expectedDocumentId,
					}),
				},
			}
		})

		JustBeforeEach(func() {
			actualDocumentId, actualErr = client.Create(ctx, expectedCreateRequest)
		})

		It("should index the document in ES", func() {
			Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_doc", expectedCreateRequest.Index)))

			requestBody, err := ioutil.ReadAll(transport.ReceivedHttpRequests[0].Body)
			Expect(err).ToNot(HaveOccurred())

			indexedMessage := &pb.Occurrence{}
			err = protojson.Unmarshal(requestBody, protov1.MessageV2(indexedMessage))
			Expect(err).ToNot(HaveOccurred())
			Expect(indexedMessage).To(BeEquivalentTo(expectedOccurrence))
		})

		It("should refresh the index by default", func() {
			Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("true"))
		})

		It("should return the indexed document ID and no error", func() {
			Expect(actualDocumentId).To(Equal(expectedDocumentId))
			Expect(actualErr).ToNot(HaveOccurred())
		})

		When("a document ID is provided", func() {
			BeforeEach(func() {
				expectedCreateRequest.DocumentId = fake.LetterN(10)
			})

			It("should index the document using the provided ID", func() {
				Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_doc/%s", expectedCreateRequest.Index, expectedCreateRequest.DocumentId)))
			})
		})

		When("indexing the document fails", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[0] = &http.Response{
					StatusCode: http.StatusInternalServerError,
					Body: structToJsonBody(&EsIndexDocResponse{
						Error: &EsIndexDocError{
							Type:   fake.LetterN(10),
							Reason: fake.LetterN(10),
						},
					}),
				}
			})

			It("should return an error", func() {
				Expect(actualDocumentId).To(BeEmpty())
				Expect(actualErr).To(HaveOccurred())
			})
		})

		When("the refresh option is set to false", func() {
			BeforeEach(func() {
				expectedCreateRequest.Refresh = "false"
			})

			It("should not refresh the index after creating the document", func() {
				Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("false"))
			})
		})
	})

	Context("BulkCreate", func() {
		var (
			expectedBulkCreateRequest  *BulkCreateRequest
			expectedBulkCreateResponse *EsBulkResponse
			expectedBulkItems          []*BulkCreateRequestItem
			expectedOccurrences        []*pb.Occurrence
			expectedIndex              string
			expectedErrs               []error

			actualBulkCreateResponse *EsBulkResponse
			actualErr                error
		)

		BeforeEach(func() {
			expectedIndex = fake.LetterN(10)
			expectedOccurrences = createRandomOccurrences(fake.Number(2, 5))
			expectedBulkItems = []*BulkCreateRequestItem{}
			for _, o := range expectedOccurrences {
				expectedBulkItems = append(expectedBulkItems, &BulkCreateRequestItem{
					Message: protov1.MessageV2(o),
				})
				expectedErrs = append(expectedErrs, nil)
			}

			expectedBulkCreateResponse = createEsBulkOccurrenceIndexResponse(expectedOccurrences, expectedErrs)
			expectedBulkCreateRequest = &BulkCreateRequest{
				Index: expectedIndex,
				Items: expectedBulkItems,
			}

			transport.PreparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body:       structToJsonBody(expectedBulkCreateResponse),
				},
			}
		})

		JustBeforeEach(func() {
			actualBulkCreateResponse, actualErr = client.BulkCreate(ctx, expectedBulkCreateRequest)
		})

		It("should send a bulk request to ES to create a document for each message", func() {
			Expect(transport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodPost))
			Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal("/_bulk"))

			var expectedPayloads []interface{}

			for i := 0; i < len(expectedOccurrences); i++ {
				expectedPayloads = append(expectedPayloads, &EsBulkQueryFragment{}, &pb.Occurrence{})
			}

			parseEsNDJSONRequestBodyWithProtobufs(transport.ReceivedHttpRequests[0].Body, expectedPayloads)

			for i, payload := range expectedPayloads {
				if i%2 == 0 { // index metadata
					metadata := payload.(*EsBulkQueryFragment)
					Expect(metadata.Index.Index).To(Equal(expectedIndex))
					Expect(metadata.Create).To(BeNil()) // document ID is not set for this test
				} else { // occurrence
					occurrence := payload.(*pb.Occurrence)
					expectedOccurrence := expectedOccurrences[(i-1)/2]

					Expect(occurrence).To(Equal(expectedOccurrence))
				}
			}
		})

		It("should return the response from the bulk index operation", func() {
			Expect(actualErr).ToNot(HaveOccurred())
			Expect(actualBulkCreateResponse).To(BeEquivalentTo(expectedBulkCreateResponse))
		})

		When("a document ID is specified for an item", func() {
			var (
				randomItemIndex    int
				expectedDocumentId string
			)

			BeforeEach(func() {
				expectedDocumentId = fake.LetterN(10)
				randomItemIndex = fake.Number(0, len(expectedBulkItems)-1)
				expectedBulkItems[randomItemIndex].DocumentId = expectedDocumentId
			})

			It("should use the provided document ID when indexing that item", func() {
				var expectedPayloads []interface{}

				for i := 0; i < len(expectedOccurrences); i++ {
					expectedPayloads = append(expectedPayloads, &EsBulkQueryFragment{}, &pb.Occurrence{})
				}

				parseEsNDJSONRequestBodyWithProtobufs(transport.ReceivedHttpRequests[0].Body, expectedPayloads)

				metadataIndex := randomItemIndex * 2
				metadataWithDocumentId := expectedPayloads[metadataIndex].(*EsBulkQueryFragment)

				Expect(metadataWithDocumentId.Index.Index).To(Equal(expectedIndex))
				Expect(metadataWithDocumentId.Create).ToNot(BeNil())
				Expect(metadataWithDocumentId.Create.Id).To(Equal(expectedDocumentId))
			})
		})

		When("the refresh option is set to false", func() {
			BeforeEach(func() {
				expectedBulkCreateRequest.Refresh = "false"
			})

			It("should not refresh the index after creating the document", func() {
				Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("false"))
			})
		})

		When("the bulk operation fails", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[0] = &http.Response{
					StatusCode: http.StatusInternalServerError,
					Body: structToJsonBody(&EsIndexDocResponse{
						Error: &EsIndexDocError{
							Type:   fake.LetterN(10),
							Reason: fake.LetterN(10),
						},
					}),
				}
			})

			It("should return an error", func() {
				Expect(actualBulkCreateResponse).To(BeNil())
				Expect(actualErr).To(HaveOccurred())
			})
		})
	})

	Context("Search", func() {
		var (
			expectedSearchRequest  *SearchRequest
			expectedSearchResponse *EsSearchResponse
			expectedIndex          string

			actualSearchResponse *SearchResponse
			actualErr            error
		)

		BeforeEach(func() {
			expectedIndex = fake.LetterN(10)
			expectedSearchResponse = &EsSearchResponse{
				Hits: &EsSearchResponseHits{
					Hits: []*EsSearchResponseHit{
						{
							ID:         fake.LetterN(10),
							Source:     []byte("{}"),
							Highlights: []byte("{}"),
						},
					},
				},
			}
			expectedSearchRequest = &SearchRequest{
				Index: expectedIndex,
			}

			transport.PreparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body:       structToJsonBody(expectedSearchResponse),
				},
			}
		})

		JustBeforeEach(func() {
			actualSearchResponse, actualErr = client.Search(ctx, expectedSearchRequest)
		})

		It("should send a search request to ES", func() {
			Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_search", expectedIndex)))

			searchRequest := &EsSearch{}
			ReadRequestBody(transport.ReceivedHttpRequests[0], &searchRequest)

			// by default, everything is empty
			Expect(searchRequest.Query).To(BeNil())
			Expect(searchRequest.Sort).To(BeEmpty())
			Expect(searchRequest.Collapse).To(BeNil())
			Expect(searchRequest.Pit).To(BeNil())
		})

		It("should return the results of the search operation", func() {
			Expect(actualErr).ToNot(HaveOccurred())
			Expect(actualSearchResponse.Hits).To(BeEquivalentTo(expectedSearchResponse.Hits))
		})

		When("a refined search is provided", func() {
			var (
				expectedSearch *EsSearch
			)

			BeforeEach(func() {
				expectedSearch = &EsSearch{
					Query: &filtering.Query{
						Term: &filtering.Term{
							fake.LetterN(10): fake.LetterN(10),
						},
					},
				}
				expectedSearchRequest.Search = expectedSearch
			})

			It("should use the user-provided search", func() {
				searchRequest := &EsSearch{}
				ReadRequestBody(transport.ReceivedHttpRequests[0], &searchRequest)

				Expect(searchRequest).To(BeEquivalentTo(expectedSearch))
			})
		})

		When("the search operation fails", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[0] = &http.Response{
					StatusCode: http.StatusInternalServerError,
				}
			})

			It("should return an error", func() {
				Expect(actualSearchResponse).To(BeNil())
				Expect(actualErr).To(HaveOccurred())
			})
		})
	})

	Context("MultiSearch", func() {
		var (
			expectedSearches            []*EsSearch
			expectedIndex               string
			expectedMultiSearchRequest  *MultiSearchRequest
			expectedMultiSearchResponse *EsMultiSearchResponse

			actualMultiSearchResponse *EsMultiSearchResponse
			actualErr                 error
		)

		BeforeEach(func() {
			expectedIndex = fake.LetterN(10)
			expectedSearches = createRandomSearches(fake.Number(2, 5))

			expectedMultiSearchRequest = &MultiSearchRequest{
				Index:    expectedIndex,
				Searches: expectedSearches,
			}
			expectedMultiSearchResponse = &EsMultiSearchResponse{
				Responses: []*EsMultiSearchResponseHitsSummary{
					{
						Hits: &EsMultiSearchResponseHits{
							Total: &EsSearchResponseTotal{
								Value: fake.Number(0, 1000),
							},
						},
					},
				},
			}

			transport.PreparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body:       structToJsonBody(expectedMultiSearchResponse),
				},
			}
		})

		JustBeforeEach(func() {
			actualMultiSearchResponse, actualErr = client.MultiSearch(ctx, expectedMultiSearchRequest)
		})

		It("should send a multisearch request to ES", func() {
			Expect(transport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodGet))
			Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal("/_msearch"))

			var expectedPayloads []interface{}

			for i := 0; i < len(expectedSearches); i++ {
				expectedPayloads = append(expectedPayloads, &EsMultiSearchQueryFragment{}, &EsSearch{})
			}

			parseEsNDJSONRequestBody(transport.ReceivedHttpRequests[0].Body, expectedPayloads)

			for i, payload := range expectedPayloads {
				if i%2 == 0 { // search metadata
					metadata := payload.(*EsMultiSearchQueryFragment)
					Expect(metadata.Index).To(Equal(expectedIndex))
				} else { // search
					search := payload.(*EsSearch)
					expectedSearch := expectedSearches[(i-1)/2]

					Expect(search).To(Equal(expectedSearch))
				}
			}
		})

		It("should return the response from the multisearch operation", func() {
			Expect(actualErr).ToNot(HaveOccurred())
			Expect(actualMultiSearchResponse).To(BeEquivalentTo(expectedMultiSearchResponse))
		})

		When("the multisearch operation fails", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[0] = &http.Response{
					StatusCode: http.StatusInternalServerError,
				}
			})

			It("should return an error", func() {
				Expect(actualMultiSearchResponse).To(BeNil())
				Expect(actualErr).To(HaveOccurred())
			})
		})
	})

	Context("MultiGet", func() {
		var (
			expectedDocumentIds      []string
			expectedIndex            string
			expectedMultiGetRequest  *MultiGetRequest
			expectedMultiGetResponse *EsMultiGetResponse

			actualMultiGetResponse *EsMultiGetResponse
			actualErr              error
		)

		BeforeEach(func() {
			expectedIndex = fake.LetterN(10)
			expectedDocumentIds = createRandomDocumentIds(fake.Number(2, 5))
			expectedMultiGetRequest = &MultiGetRequest{
				Index:       expectedIndex,
				DocumentIds: expectedDocumentIds,
			}

			expectedMultiGetResponse = &EsMultiGetResponse{
				Docs: []*EsMultiGetDocument{},
			}
			for _, id := range expectedDocumentIds {
				expectedMultiGetResponse.Docs = append(expectedMultiGetResponse.Docs, &EsMultiGetDocument{
					ID:    id,
					Found: fake.Bool(),
				})
			}

			transport.PreparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body:       structToJsonBody(expectedMultiGetResponse),
				},
			}
		})

		JustBeforeEach(func() {
			actualMultiGetResponse, actualErr = client.MultiGet(ctx, expectedMultiGetRequest)
		})

		It("should send the multiget request to ES", func() {
			Expect(transport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodGet))
			Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_mget", expectedIndex)))

			requestBody := &EsMultiGetRequest{}
			ReadRequestBody(transport.ReceivedHttpRequests[0], &requestBody)
			Expect(requestBody.IDs).To(BeEquivalentTo(expectedMultiGetRequest.DocumentIds))
		})

		It("should return the response from the multiget operation", func() {
			Expect(actualErr).ToNot(HaveOccurred())
			Expect(actualMultiGetResponse).To(BeEquivalentTo(expectedMultiGetResponse))
		})

		When("the multiget operation fails", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[0] = &http.Response{
					StatusCode: http.StatusInternalServerError,
				}
			})

			It("should return an error", func() {
				Expect(actualMultiGetResponse).To(BeNil())
				Expect(actualErr).To(HaveOccurred())
			})
		})
	})

	Context("Update", func() {
		var (
			actualErr error

			expectedUpdateRequest *UpdateRequest
			expectedDocumentId    string
			expectedIndex         string
			expectedMessage       proto.Message
			expectedOccurrence    *pb.Occurrence
		)

		BeforeEach(func() {
			expectedDocumentId = fake.LetterN(10)
			expectedIndex = fake.LetterN(10)
			expectedOccurrence = createRandomOccurrence()
			expectedMessage = protov1.MessageV2(expectedOccurrence)
			expectedUpdateRequest = &UpdateRequest{
				Index:      expectedIndex,
				Message:    expectedMessage,
				DocumentId: expectedDocumentId,
			}

			transport.PreparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body: structToJsonBody(&EsIndexDocResponse{
						Id: expectedDocumentId,
					}),
				},
			}
		})

		JustBeforeEach(func() {
			actualErr = client.Update(ctx, expectedUpdateRequest)
		})

		It("should index (update) the document in ES", func() {
			Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_doc/%s", expectedUpdateRequest.Index, expectedUpdateRequest.DocumentId)))

			requestBody, err := ioutil.ReadAll(transport.ReceivedHttpRequests[0].Body)
			Expect(err).ToNot(HaveOccurred())

			indexedMessage := &pb.Occurrence{}
			err = protojson.Unmarshal(requestBody, protov1.MessageV2(indexedMessage))
			Expect(err).ToNot(HaveOccurred())
			Expect(indexedMessage).To(BeEquivalentTo(expectedOccurrence))
		})

		It("should refresh the index by default", func() {
			Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("true"))
		})

		It("should return no error", func() {
			Expect(actualErr).ToNot(HaveOccurred())
		})

		When("indexing the document fails", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[0] = &http.Response{
					StatusCode: http.StatusInternalServerError,
					Body: structToJsonBody(&EsIndexDocResponse{
						Error: &EsIndexDocError{
							Type:   fake.LetterN(10),
							Reason: fake.LetterN(10),
						},
					}),
				}
			})

			It("should return an error", func() {
				Expect(actualErr).To(HaveOccurred())
			})
		})

		When("the refresh option is set to false", func() {
			BeforeEach(func() {
				expectedUpdateRequest.Refresh = "false"
			})

			It("should not refresh the index after updating the document", func() {
				Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("false"))
			})
		})
	})

	Context("Delete", func() {
		var (
			actualErr error

			expectedUpdateRequest *UpdateRequest
			expectedDocumentId    string
			expectedIndex         string
			expectedMessage       proto.Message
			expectedOccurrence    *pb.Occurrence
		)

		BeforeEach(func() {
			expectedDocumentId = fake.LetterN(10)
			expectedIndex = fake.LetterN(10)
			expectedOccurrence = createRandomOccurrence()
			expectedMessage = protov1.MessageV2(expectedOccurrence)
			expectedUpdateRequest = &UpdateRequest{
				Index:      expectedIndex,
				Message:    expectedMessage,
				DocumentId: expectedDocumentId,
			}

			transport.PreparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body: structToJsonBody(&EsIndexDocResponse{
						Id: expectedDocumentId,
					}),
				},
			}
		})

		JustBeforeEach(func() {
			actualErr = client.Update(ctx, expectedUpdateRequest)
		})

		It("should index (update) the document in ES", func() {
			Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_doc/%s", expectedUpdateRequest.Index, expectedUpdateRequest.DocumentId)))

			requestBody, err := ioutil.ReadAll(transport.ReceivedHttpRequests[0].Body)
			Expect(err).ToNot(HaveOccurred())

			indexedMessage := &pb.Occurrence{}
			err = protojson.Unmarshal(requestBody, protov1.MessageV2(indexedMessage))
			Expect(err).ToNot(HaveOccurred())
			Expect(indexedMessage).To(BeEquivalentTo(expectedOccurrence))
		})

		It("should refresh the index by default", func() {
			Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("true"))
		})

		It("should return no error", func() {
			Expect(actualErr).ToNot(HaveOccurred())
		})

		When("deleting the document fails", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[0] = &http.Response{
					StatusCode: http.StatusInternalServerError,
				}
			})

			It("should return an error", func() {
				Expect(actualErr).To(HaveOccurred())
			})
		})

		When("the refresh option is set to false", func() {
			BeforeEach(func() {
				expectedUpdateRequest.Refresh = "false"
			})

			It("should not refresh the index after updating the document", func() {
				Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("false"))
			})
		})
	})
})

func createRandomOccurrence() *pb.Occurrence {
	return &pb.Occurrence{
		Name: fake.LetterN(10),
		Resource: &pb.Resource{
			Uri: fake.LetterN(10),
		},
		NoteName:    fake.LetterN(10),
		Kind:        common_go_proto.NoteKind_NOTE_KIND_UNSPECIFIED,
		Remediation: fake.LetterN(10),
		Details:     nil,
		CreateTime:  ptypes.TimestampNow(),
	}
}

func createRandomOccurrences(l int) []*pb.Occurrence {
	var result []*pb.Occurrence
	for i := 0; i < l; i++ {
		result = append(result, createRandomOccurrence())
	}

	return result
}

func createRandomSearch() *EsSearch {
	return &EsSearch{
		Query: &filtering.Query{
			Term: &filtering.Term{
				fake.LetterN(10): fake.LetterN(10),
			},
		},
		Sort: map[string]EsSortOrder{
			fake.LetterN(10): EsSortOrderDescending,
		},
		Collapse: &EsSearchCollapse{
			Field: fake.LetterN(10),
		},
	}
}

func createRandomSearches(l int) []*EsSearch {
	var result []*EsSearch
	for i := 0; i < l; i++ {
		result = append(result, createRandomSearch())
	}

	return result
}

func createRandomDocumentIds(l int) []string {
	var result []string
	for i := 0; i < l; i++ {
		result = append(result, fake.LetterN(10))
	}

	return result
}

func structToJsonBody(i interface{}) io.ReadCloser {
	b, err := json.Marshal(i)
	Expect(err).ToNot(HaveOccurred())

	return ioutil.NopCloser(strings.NewReader(string(b)))
}

// helper functions for _bulk requests

func createEsBulkOccurrenceIndexResponse(occurrences []*pb.Occurrence, errs []error) *EsBulkResponse {
	var (
		responseItems     []*EsBulkResponseItem
		responseHasErrors = false
	)
	for i := range occurrences {
		var (
			responseErr  *EsIndexDocError
			responseCode = http.StatusCreated
		)
		if errs[i] != nil {
			responseErr = &EsIndexDocError{
				Type:   fake.LetterN(10),
				Reason: fake.LetterN(10),
			}
			responseCode = http.StatusInternalServerError
			responseHasErrors = true
		}

		responseItems = append(responseItems, &EsBulkResponseItem{
			Index: &EsIndexDocResponse{
				Id:     fake.LetterN(10),
				Status: responseCode,
				Error:  responseErr,
			},
		})
	}

	return &EsBulkResponse{
		Items:  responseItems,
		Errors: responseHasErrors,
	}
}

// parseEsNDJSONRequestBodyWithProtobufs parses a request body in ndjson format
// each line of the body is assumed to be properly formatted JSON
// every odd line is assumed to be a regular JSON structure that can be unmarshalled via json.Unmarshal
// every even line is assumed to be a JSON structure representing a protobuf message, and will be unmarshalled using protojson.Unmarshal
func parseEsNDJSONRequestBodyWithProtobufs(body io.ReadCloser, structs []interface{}) {
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
			err = protojson.Unmarshal([]byte(jsonPayloads[i]), protov1.MessageV2(s))
		}

		Expect(err).ToNot(HaveOccurred())
	}
}

// parseEsNDJSONRequestBodyWithProtobufs parses a request body in ndjson format
// each line of the body is assumed to be properly formatted JSON that can be unmarshalled via json.Unmarshal
func parseEsNDJSONRequestBody(body io.ReadCloser, structs []interface{}) {
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
