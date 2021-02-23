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

package config

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("ElasticsearchConfig", func() {
	DescribeTable("validation", func(c ElasticsearchConfig, shouldErr bool) {
		err := c.IsValid()

		if shouldErr {
			Expect(err).To(HaveOccurred())
		} else {
			Expect(err).ToNot(HaveOccurred())
		}
	},
		Entry("valid url, refresh true", ElasticsearchConfig{
			URL:     fake.URL(),
			Refresh: RefreshTrue,
		}, false),
		Entry("valid url, refresh wait_for", ElasticsearchConfig{
			URL:     fake.URL(),
			Refresh: RefreshWaitFor,
		}, false),
		Entry("valid url, refresh false", ElasticsearchConfig{
			URL:     fake.URL(),
			Refresh: RefreshFalse,
		}, false),
		Entry("valid url, invalid refresh option", ElasticsearchConfig{
			URL:     fake.URL(),
			Refresh: "somethingInvalid",
		}, true),
	)
})
