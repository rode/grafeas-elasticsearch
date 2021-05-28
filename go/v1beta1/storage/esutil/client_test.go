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

package esutil

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	protov1 "github.com/golang/protobuf/proto"
	"github.com/grafeas/grafeas/proto/v1beta1/common_go_proto"
	pb "github.com/grafeas/grafeas/proto/v1beta1/grafeas_go_proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rode/grafeas-elasticsearch/go/v1beta1/storage/filtering"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
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
			Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("routing")).To(BeEmpty())

			requestBody, err := io.ReadAll(transport.ReceivedHttpRequests[0].Body)
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

		When("a join field is used", func() {
			var (
				expectedJoinField string
				expectedJoinName  string
			)

			BeforeEach(func() {
				expectedJoinField = fake.LetterN(10)
				expectedJoinName = fake.LetterN(10)

				expectedCreateRequest.Join = &EsJoin{
					Field: expectedJoinField,
					Name:  expectedJoinName,
				}
			})

			It("should marshal the join fields into the request body json", func() {
				requestBody, err := io.ReadAll(transport.ReceivedHttpRequests[0].Body)
				Expect(err).ToNot(HaveOccurred())

				// the proto message should still be marshalled
				indexedMessage := &pb.Occurrence{}
				err = protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(requestBody, protov1.MessageV2(indexedMessage))
				Expect(err).ToNot(HaveOccurred())
				Expect(indexedMessage).To(BeEquivalentTo(expectedOccurrence))

				// and we should also see the join field
				jsonMap := map[string]interface{}{}
				err = json.Unmarshal(requestBody, &jsonMap)
				Expect(err).ToNot(HaveOccurred())

				joinField := jsonMap[expectedJoinField].(map[string]interface{})
				Expect(joinField["name"]).To(BeEquivalentTo(expectedJoinName))
			})

			When("the parent is set", func() {
				var expectedParent string

				BeforeEach(func() {
					expectedParent = fake.LetterN(10)

					expectedCreateRequest.Join.Parent = expectedParent
				})

				It("should set the routing to the parent ID", func() {
					Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_doc", expectedCreateRequest.Index)))
					Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("routing")).To(Equal(expectedParent))
				})

				It("should include the parent info in the request body", func() {
					requestBody, err := io.ReadAll(transport.ReceivedHttpRequests[0].Body)
					Expect(err).ToNot(HaveOccurred())

					jsonMap := map[string]interface{}{}
					err = json.Unmarshal(requestBody, &jsonMap)
					Expect(err).ToNot(HaveOccurred())

					joinField := jsonMap[expectedJoinField].(map[string]interface{})
					Expect(joinField["parent"]).To(BeEquivalentTo(expectedParent))
				})
			})
		})
	})

	Context("Bulk", func() {
		var (
			expectedBulkCreateRequest  *BulkRequest
			expectedBulkCreateResponse *EsBulkResponse
			expectedBulkItems          []*BulkRequestItem
			expectedOccurrences        []*pb.Occurrence
			expectedIndex              string
			expectedErrs               []error

			actualBulkCreateResponse *EsBulkResponse
			actualErr                error
		)

		BeforeEach(func() {
			expectedIndex = fake.LetterN(10)
			expectedOccurrences = createRandomOccurrences(fake.Number(2, 5))
			expectedBulkItems = []*BulkRequestItem{}
			for _, o := range expectedOccurrences {
				expectedBulkItems = append(expectedBulkItems, &BulkRequestItem{
					Message:   protov1.MessageV2(o),
					Operation: BULK_INDEX,
				})
				expectedErrs = append(expectedErrs, nil)
			}

			expectedBulkCreateResponse = createEsBulkOccurrenceIndexResponse(expectedOccurrences, expectedErrs)
			expectedBulkCreateRequest = &BulkRequest{
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
			actualBulkCreateResponse, actualErr = client.Bulk(ctx, expectedBulkCreateRequest)
		})

		It("should send a bulk request to ES to create a document for each message", func() {
			Expect(transport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodPost))
			Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_bulk", expectedIndex)))

			var expectedPayloads []interface{}

			for i := 0; i < len(expectedOccurrences); i++ {
				expectedPayloads = append(expectedPayloads, &EsBulkQueryFragment{}, &pb.Occurrence{})
			}

			parseNDJSONRequestBodyWithProtobufs(transport.ReceivedHttpRequests[0].Body, expectedPayloads)

			for i, payload := range expectedPayloads {
				if i%2 == 0 { // index metadata
					metadata := payload.(*EsBulkQueryFragment)
					Expect(metadata.Index.Index).To(Equal(expectedIndex))
					Expect(metadata.Create).To(BeNil()) // create is not used for this test
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

		When("the create operation is specified for an item", func() {
			var (
				randomItemIndex    int
				expectedDocumentId string
			)

			BeforeEach(func() {
				expectedDocumentId = fake.LetterN(10)
				randomItemIndex = fake.Number(0, len(expectedBulkItems)-1)
				expectedBulkItems[randomItemIndex].DocumentId = expectedDocumentId
				expectedBulkItems[randomItemIndex].Operation = BULK_CREATE
			})

			It("should use the provided document ID when indexing that item", func() {
				var expectedPayloads []interface{}

				for i := 0; i < len(expectedOccurrences); i++ {
					expectedPayloads = append(expectedPayloads, &EsBulkQueryFragment{}, &pb.Occurrence{})
				}

				parseNDJSONRequestBodyWithProtobufs(transport.ReceivedHttpRequests[0].Body, expectedPayloads)

				metadataIndex := randomItemIndex * 2
				metadataWithDocumentId := expectedPayloads[metadataIndex].(*EsBulkQueryFragment)

				Expect(metadataWithDocumentId.Index).To(BeNil())
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

		When("a join field is used", func() {
			var (
				expectedJoinField string
				expectedJoinName  string
				randomIndex       int
				expectedPayloads  []string
			)

			BeforeEach(func() {
				randomIndex = fake.Number(0, len(expectedOccurrences)-1)
				expectedJoinField = fake.LetterN(10)
				expectedJoinName = fake.LetterN(10)

				expectedBulkItems[randomIndex].Join = &EsJoin{
					Field: expectedJoinField,
					Name:  expectedJoinName,
				}
			})

			JustBeforeEach(func() {
				buf := new(bytes.Buffer)
				_, err := buf.ReadFrom(transport.ReceivedHttpRequests[0].Body)
				Expect(err).ToNot(HaveOccurred())

				requestPayload := strings.TrimSuffix(buf.String(), "\n") // _bulk requests need to end in a newline
				expectedPayloads = strings.Split(requestPayload, "\n")
			})

			It("should marshal the join field into the payload that used it", func() {
				payloadWithJoin := expectedPayloads[randomIndex*2+1]

				// the proto message should still be marshalled
				indexedMessage := &pb.Occurrence{}
				err := protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal([]byte(payloadWithJoin), protov1.MessageV2(indexedMessage))
				Expect(err).ToNot(HaveOccurred())
				Expect(indexedMessage).To(BeEquivalentTo(expectedOccurrences[randomIndex]))

				// and we should also see the join field
				jsonMap := map[string]interface{}{}
				err = json.Unmarshal([]byte(payloadWithJoin), &jsonMap)
				Expect(err).ToNot(HaveOccurred())

				joinField := jsonMap[expectedJoinField].(map[string]interface{})
				Expect(joinField["name"]).To(BeEquivalentTo(expectedJoinName))
			})

			When("the parent is set", func() {
				var expectedParent string

				BeforeEach(func() {
					expectedParent = fake.LetterN(10)

					expectedBulkItems[randomIndex].Join.Parent = expectedParent
				})

				It("should set the routing to the parent ID", func() {
					payloadWithJoinOperation := expectedPayloads[randomIndex*2]
					operation := &EsBulkQueryFragment{}

					err := json.Unmarshal([]byte(payloadWithJoinOperation), operation)
					Expect(err).ToNot(HaveOccurred())
					Expect(operation.Index.Routing).To(Equal(expectedParent))
				})

				It("should include the parent info in the request body", func() {
					payloadWithJoin := expectedPayloads[randomIndex*2+1]

					jsonMap := map[string]interface{}{}
					err := json.Unmarshal([]byte(payloadWithJoin), &jsonMap)
					Expect(err).ToNot(HaveOccurred())

					joinField := jsonMap[expectedJoinField].(map[string]interface{})
					Expect(joinField["parent"]).To(BeEquivalentTo(expectedParent))
				})
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
					Total: &EsSearchResponseTotal{
						Value: fake.Number(1000, 10000),
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
			// page size should be 1000 by default
			Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("size")).To(Equal(strconv.Itoa(grafeasMaxPageSize)))

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

		When("pagination is used", func() {
			var (
				expectedPageSize int
				expectedPitId    string
			)

			BeforeEach(func() {
				expectedPageSize = fake.Number(20, 100)
				expectedPitId = fake.LetterN(10)
				expectedSearchRequest.Pagination = &SearchPaginationOptions{
					Size: expectedPageSize,
				}

				transport.PreparedHttpResponses = append([]*http.Response{
					{
						StatusCode: http.StatusOK,
						Body: structToJsonBody(&ESPitResponse{
							Id: expectedPitId,
						}),
					},
				}, transport.PreparedHttpResponses...)
			})

			When("a page token is not specified", func() {
				It("should create a PIT in ES before performing a search", func() {
					Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_pit", expectedIndex)))
					Expect(transport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodPost))
					Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("keep_alive")).To(Equal(defaultPitKeepAlive))
				})

				It("should perform a search using the PIT id", func() {
					Expect(transport.ReceivedHttpRequests[1].URL.Path).To(Equal("/_search"))
					Expect(transport.ReceivedHttpRequests[1].Method).To(Equal(http.MethodGet))
					Expect(transport.ReceivedHttpRequests[1].URL.Query().Get("size")).To(Equal(strconv.Itoa(expectedPageSize)))
					Expect(transport.ReceivedHttpRequests[1].URL.Query().Get("from")).To(Equal(strconv.Itoa(0)))

					searchRequest := &EsSearch{}
					ReadRequestBody(transport.ReceivedHttpRequests[1], &searchRequest)

					Expect(searchRequest.Pit.Id).To(Equal(expectedPitId))
				})

				It("should return the next page token", func() {
					pitId, from, err := ParsePageToken(actualSearchResponse.NextPageToken)
					Expect(err).ToNot(HaveOccurred())
					Expect(pitId).To(Equal(expectedPitId))
					Expect(from).To(BeEquivalentTo(expectedPageSize))
				})

				When("creating the PIT fails", func() {
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

				When("the search operation fails", func() {
					BeforeEach(func() {
						transport.PreparedHttpResponses[1] = &http.Response{
							StatusCode: http.StatusInternalServerError,
						}
					})

					It("should return an error", func() {
						Expect(actualSearchResponse).To(BeNil())
						Expect(actualErr).To(HaveOccurred())
					})
				})

				When("the end of the search results has been reached", func() {
					BeforeEach(func() {
						expectedSearchResponse.Hits.Total.Value = fake.Number(0, expectedPageSize-1)
						transport.PreparedHttpResponses[1].Body = structToJsonBody(expectedSearchResponse)
					})

					It("should return an empty next page token", func() {
						Expect(actualSearchResponse.NextPageToken).To(BeEmpty())
					})
				})
			})

			When("a page token is specified", func() {
				var (
					expectedFrom int
				)

				BeforeEach(func() {
					expectedFrom = fake.Number(10, 20)
					expectedSearchRequest.Pagination.Token = CreatePageToken(expectedPitId, expectedFrom)

					// we only expect one ES response now for the search operation
					transport.PreparedHttpResponses = []*http.Response{
						transport.PreparedHttpResponses[1],
					}
				})

				It("should perform a search using the provided PIT", func() {
					Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal("/_search"))
					Expect(transport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodGet))
					Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("size")).To(Equal(strconv.Itoa(expectedPageSize)))
					Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("from")).To(Equal(strconv.Itoa(expectedFrom)))
				})

				When("the end of the search results has been reached", func() {
					BeforeEach(func() {
						expectedSearchResponse.Hits.Total.Value = fake.Number(0, expectedFrom+expectedPageSize-1)
						transport.PreparedHttpResponses[0].Body = structToJsonBody(expectedSearchResponse)
					})

					It("should return an empty next page token", func() {
						Expect(actualSearchResponse.NextPageToken).To(BeEmpty())
					})
				})

				When("the provided page token is invalid", func() {
					BeforeEach(func() {
						expectedSearchRequest.Pagination.Token = fake.LetterN(10)
					})

					It("should return an error", func() {
						Expect(actualSearchResponse).To(BeNil())
						Expect(actualErr).To(HaveOccurred())
					})
				})
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

			parseNDJSONRequestBody(transport.ReceivedHttpRequests[0].Body, expectedPayloads)

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

	Context("Get", func() {
		var (
			expectedDocumentId string
			expectedIndex      string

			expectedGetRequest  *GetRequest
			expectedGetResponse *EsGetResponse

			actualGetResponse *EsGetResponse
			actualErr         error
		)

		BeforeEach(func() {
			expectedDocumentId = fake.LetterN(10)
			expectedIndex = fake.LetterN(10)

			expectedGetRequest = &GetRequest{
				Index:      expectedIndex,
				DocumentId: expectedDocumentId,
			}

			expectedOccurrence := createRandomOccurrence()
			expectedOccurrenceJson, _ := protojson.Marshal(protov1.MessageV2(expectedOccurrence))

			expectedGetResponse = &EsGetResponse{
				Id:     expectedDocumentId,
				Found:  true,
				Source: expectedOccurrenceJson,
			}

			transport.PreparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body:       structToJsonBody(expectedGetResponse),
				},
			}
		})

		JustBeforeEach(func() {
			actualGetResponse, actualErr = client.Get(ctx, expectedGetRequest)
		})

		It("should send the get request to ES", func() {
			Expect(transport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodGet))
			Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_doc/%s", expectedIndex, expectedDocumentId)))
			Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("routing")).To(BeEmpty())
		})

		It("should return the response and no error", func() {
			Expect(actualErr).ToNot(HaveOccurred())
			Expect(actualGetResponse.Id).To(Equal(expectedDocumentId))
			Expect(actualGetResponse.Found).To(BeTrue())
			Expect(actualGetResponse.Source).To(MatchJSON(expectedGetResponse.Source))
		})

		When("the get operation fails", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusInternalServerError,
					},
				}
			})

			It("should return an error", func() {
				Expect(actualErr).To(HaveOccurred())
				Expect(actualGetResponse).To(BeNil())
			})
		})

		When("the get operation can't find the document", func() {
			BeforeEach(func() {
				expectedGetResponse.Found = false
				expectedGetResponse.Source = nil
				transport.PreparedHttpResponses[0] = &http.Response{
					Body:       structToJsonBody(expectedGetResponse),
					StatusCode: http.StatusNotFound,
				}
			})

			It("should return the response and no error", func() {
				Expect(actualErr).ToNot(HaveOccurred())
				Expect(actualGetResponse.Found).To(BeFalse())
				Expect(actualGetResponse.Id).To(Equal(expectedDocumentId))

				actualJson, err := actualGetResponse.Source.MarshalJSON()
				Expect(err).NotTo(HaveOccurred())
				Expect(actualJson).To(Equal([]byte("null")))
			})
		})

		When("the document id is a url", func() {
			BeforeEach(func() {
				expectedDocumentId = fake.URL()
				expectedGetRequest.DocumentId = expectedDocumentId
			})

			It("should query escape the document id", func() {
				Expect(transport.ReceivedHttpRequests[0].Method).To(Equal(http.MethodGet))
				Expect(transport.ReceivedHttpRequests[0].URL.RawPath).To(ContainSubstring(url.QueryEscape(expectedDocumentId)))
			})
		})

		When("the document routing is specified", func() {
			var expectedRouting string

			BeforeEach(func() {
				expectedRouting = fake.UUID()
				expectedGetRequest.Routing = expectedRouting
			})

			It("should route the query using the parent id", func() {
				Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("routing")).To(Equal(expectedRouting))
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
				Docs: []*EsGetResponse{},
			}
			for _, id := range expectedDocumentIds {
				expectedMultiGetResponse.Docs = append(expectedMultiGetResponse.Docs, &EsGetResponse{
					Id:     id,
					Found:  fake.Bool(),
					Source: []byte("null"),
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

		When("multiget items are specified", func() {
			var expectedItems []*EsMultiGetItem

			BeforeEach(func() {
				expectedItems = []*EsMultiGetItem{}
				for i := 0; i < len(expectedDocumentIds); i++ {
					expectedItems = append(expectedItems, &EsMultiGetItem{
						Id:      expectedDocumentIds[i],
						Routing: fake.LetterN(10),
					})
				}
				expectedMultiGetRequest.DocumentIds = nil
				expectedMultiGetRequest.Items = expectedItems
			})

			It("should send the items instead of document ids", func() {
				requestBody := &EsMultiGetRequest{}
				ReadRequestBody(transport.ReceivedHttpRequests[0], &requestBody)

				Expect(requestBody.IDs).To(BeNil())
				Expect(requestBody.Docs).To(ConsistOf(expectedItems))
			})
		})

		When("the multiget operation fails", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses = []*http.Response{
					{
						StatusCode: http.StatusInternalServerError,
					},
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

			requestBody, err := io.ReadAll(transport.ReceivedHttpRequests[0].Body)
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

			expectedDeleteRequest *DeleteRequest
			expectedIndex         string
			expectedSearch        *EsSearch
		)

		BeforeEach(func() {
			expectedIndex = fake.LetterN(10)
			expectedSearch = &EsSearch{
				Query: &filtering.Query{
					Term: &filtering.Term{
						fake.LetterN(10): fake.LetterN(10),
					},
				},
			}
			expectedDeleteRequest = &DeleteRequest{
				Index:  expectedIndex,
				Search: expectedSearch,
			}

			transport.PreparedHttpResponses = []*http.Response{
				{
					StatusCode: http.StatusOK,
					Body: structToJsonBody(&EsDeleteResponse{
						Deleted: 1,
					}),
				},
			}
		})

		JustBeforeEach(func() {
			actualErr = client.Delete(ctx, expectedDeleteRequest)
		})

		It("should delete the document in ES", func() {
			Expect(transport.ReceivedHttpRequests[0].URL.Path).To(Equal(fmt.Sprintf("/%s/_delete_by_query", expectedDeleteRequest.Index)))
			Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("routing")).To(BeEmpty())

			searchRequest := &EsSearch{}
			ReadRequestBody(transport.ReceivedHttpRequests[0], &searchRequest)

			Expect(searchRequest).To(BeEquivalentTo(expectedSearch))
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
				expectedDeleteRequest.Refresh = "false"
			})

			It("should not refresh the index after updating the document", func() {
				Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("refresh")).To(Equal("false"))
			})
		})

		When("zero documents are deleted", func() {
			BeforeEach(func() {
				transport.PreparedHttpResponses[0].Body = structToJsonBody(&EsDeleteResponse{
					Deleted: 0,
				})
			})

			It("should return an error", func() {
				Expect(actualErr).To(HaveOccurred())
			})
		})

		When("the document routing is specified", func() {
			var expectedRouting string

			BeforeEach(func() {
				expectedRouting = fake.UUID()
				expectedDeleteRequest.Routing = expectedRouting
			})

			It("should route the query using the parent id", func() {
				Expect(transport.ReceivedHttpRequests[0].URL.Query().Get("routing")).To(Equal(expectedRouting))
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
		CreateTime:  timestamppb.Now(),
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

	return io.NopCloser(strings.NewReader(string(b)))
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

// parseNDJSONRequestBodyWithProtobufs parses a request body in ndjson format
// each line of the body is assumed to be properly formatted JSON
// every odd line is assumed to be a regular JSON structure that can be unmarshalled via json.Unmarshal
// every even line is assumed to be a JSON structure representing a protobuf message, and will be unmarshalled using protojson.Unmarshal
func parseNDJSONRequestBodyWithProtobufs(body io.ReadCloser, structs []interface{}) {
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

// parseNDJSONRequestBody parses a request body in ndjson format
// each line of the body is assumed to be properly formatted JSON that can be unmarshalled via json.Unmarshal
func parseNDJSONRequestBody(body io.ReadCloser, structs []interface{}) {
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
