package config

import (
	"github.com/brianvoe/gofakeit/v5"
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
			URL:     gofakeit.URL(),
			Refresh: RefreshTrue,
		}, false),
		Entry("valid url, refresh wait_for", ElasticsearchConfig{
			URL:     gofakeit.URL(),
			Refresh: RefreshWaitFor,
		}, false),
		Entry("valid url, refresh false", ElasticsearchConfig{
			URL:     gofakeit.URL(),
			Refresh: RefreshFalse,
		}, false),
		Entry("valid url, invalid refresh option", ElasticsearchConfig{
			URL:     gofakeit.URL(),
			Refresh: "somethingInvalid",
		}, true),
	)
})
