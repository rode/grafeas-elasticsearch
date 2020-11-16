package storage

import (
	"context"
	"errors"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	prpb "github.com/grafeas/grafeas/proto/v1beta1/project_go_proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http"
)

var _ = Describe("elasticsearch storage", func() {
	var (
		elasticsearchStorage *ElasticsearchStorage
		transport            *mockEsTransport
	)

	BeforeEach(func() {
		transport = &mockEsTransport{}
		transport.expectedError = nil
		mockEsClient := &elasticsearch.Client{Transport: transport, API: esapi.New(transport)}

		elasticsearchStorage = NewElasticsearchStore(mockEsClient, logger)
	})

	Context("Creating a new Grafeas project", func() {
		var (
			pID             string
			ctx             context.Context
			project         *prpb.Project
			expectedProject *prpb.Project
			err             error
		)

		BeforeEach(func() {
			pID = "rode"
			ctx = context.Background()
			project = &prpb.Project{Name: "projects/rode"}
		})

		When("elasticsearch successfully creates a new index", func() {
			BeforeEach(func() {
				transport.preparedPerformResponse = &http.Response{
					StatusCode: 200,
				}

				expectedProject, err = elasticsearchStorage.CreateProject(ctx, pID, project)
			})
			It("should return a new Grafeas project", func() {

				Expect(err).ToNot(HaveOccurred())
				Expect(expectedProject.Name).To(Equal("projects/rode"))
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
})
