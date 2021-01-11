package config

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("ElasticsearchConfig", func() {
	DescribeTable("validation", func(c ElasticsearchConfig, shouldErr bool) {
		err := c.IsValid()

		println(c.URL)

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
