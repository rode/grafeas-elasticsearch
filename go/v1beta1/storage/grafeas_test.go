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
	"fmt"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/golang/mock/gomock"
	grafeasConfig "github.com/grafeas/grafeas/go/config"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rode/grafeas-elasticsearch/go/config"
	"github.com/rode/grafeas-elasticsearch/go/mocks"
	"github.com/rode/grafeas-elasticsearch/go/v1beta1/storage/esutil"
)

var _ = Describe("Grafeas integration", func() {
	var (
		elasticsearchStorage    *ElasticsearchStorage
		mockCtrl                *gomock.Controller
		filterer                *mocks.MockFilterer
		indexManager            *mocks.MockIndexManager
		esConfig                *config.ElasticsearchConfig
		newElasticsearchStorage newElasticsearchStorageFunc
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		filterer = mocks.NewMockFilterer(mockCtrl)
		indexManager = mocks.NewMockIndexManager(mockCtrl)
		esConfig = &config.ElasticsearchConfig{
			URL:     fake.URL(),
			Refresh: config.RefreshTrue,
		}
	})

	JustBeforeEach(func() {
		transport := &esutil.MockEsTransport{}
		mockEsClient := &elasticsearch.Client{Transport: transport, API: esapi.New(transport)}

		elasticsearchStorage = NewElasticsearchStorage(logger, mockEsClient, filterer, esConfig, indexManager)
	})

	AfterEach(func() {
		mockCtrl.Finish()
	})

	Context("creating the elasticsearch storage provider", func() {
		var (
			err                 error
			expectedStorageType string
			storageConfig       grafeasConfig.StorageConfiguration
		)

		// BeforeEach configures the happy path for this context
		// Variables configured here may be overridden in nested BeforeEach blocks
		BeforeEach(func() {
			storageConfig = grafeasConfig.StorageConfiguration(esConfig)
			expectedStorageType = "elasticsearch"

			newElasticsearchStorage = func(ec *config.ElasticsearchConfig) (*ElasticsearchStorage, error) {
				return elasticsearchStorage, nil
			}
		})

		// JustBeforeEach actually invokes the system under test
		JustBeforeEach(func() {
			registerStorageTypeProvider := ElasticsearchStorageTypeProviderCreator(newElasticsearchStorage, logger)
			_, err = registerStorageTypeProvider(expectedStorageType, &storageConfig)
		})

		When("storage configuration is not parsable", func() {
			BeforeEach(func() {
				storageConfig = grafeasConfig.StorageConfiguration("")
			})

			It("should return error", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		When("storage configuration is not valid", func() {
			BeforeEach(func() {
				esConfig.Refresh = "invalid"
				storageConfig = grafeasConfig.StorageConfiguration(esConfig)
			})

			It("should return error", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		When("provided storage type is not elasticsearch", func() {
			BeforeEach(func() {
				expectedStorageType = "fdas"
			})

			It("should return error", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		When("creating the elasticsearchStorage fails", func() {
			BeforeEach(func() {
				newElasticsearchStorage = func(elasticsearchConfig *config.ElasticsearchConfig) (*ElasticsearchStorage, error) {
					return nil, fmt.Errorf("fail")
				}
			})

			It("should return an error", func() {
				Expect(err).To(HaveOccurred())
			})
		})
	})
})
