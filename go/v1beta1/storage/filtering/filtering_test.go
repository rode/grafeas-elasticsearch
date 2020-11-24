package filtering

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("filtering queries", func() {

	Context("Performing AND queries", func() {

		When("when use a single TERM", func() {
			var (
				filter         string
				expectedResult string
				actualResult   string
			)
			BeforeEach(func() {
				filter = "a==\"b\""
				expectedResult = `{"query":{"bool":{"must":[{"term":{"a":"b"}}]}}}`
				actualResult = ParseExpressionEntrypoint(filter)

			})

			It("should return a singular elastic query json", func() {
				Expect(expectedResult).To(BeIdenticalTo(actualResult))
			})
		})

		When("when use a single TERM with a left const and right const", func() {
			var (
				filter         string
				expectedResult string
				actualResult   string
			)
			BeforeEach(func() {
				filter = "\"a\"==\"b\""
				expectedResult = `{"query":{"bool":{"must":[{"term":{"a":"b"}}]}}}`
				actualResult = ParseExpressionEntrypoint(filter)

			})

			It("should return a singular elastic query json", func() {
				Expect(expectedResult).To(BeIdenticalTo(actualResult))
			})
		})

		When("when use a single TERM with a left ident and right ident", func() {
			var (
				filter         string
				expectedResult string
				actualResult   string
			)
			BeforeEach(func() {
				filter = "a==b"
				expectedResult = `{"query":{"bool":{"must":[{"term":{"a":"b"}}]}}}`
				actualResult = ParseExpressionEntrypoint(filter)

			})

			It("should return a singular elastic query json", func() {
				Expect(expectedResult).To(BeIdenticalTo(actualResult))
			})
		})

		When("when use a single TERM with a left const and right ident", func() {
			var (
				filter         string
				expectedResult string
				actualResult   string
			)
			BeforeEach(func() {
				filter = "\"a\"==b"
				expectedResult = `{"query":{"bool":{"must":[{"term":{"a":"b"}}]}}}`
				actualResult = ParseExpressionEntrypoint(filter)

			})

			It("should return a singular elastic query json", func() {
				Expect(expectedResult).To(BeIdenticalTo(actualResult))
			})
		})

		When("when ANDing two TERMs", func() {
			var (
				filter         string
				expectedResult string
				actualResult   string
			)
			BeforeEach(func() {
				filter = "(a==\"b\")&&(c==\"d\")"
				expectedResult = `{"query":{"bool":{"must":[{"term":{"a":"b"}},{"term":{"c":"d"}}]}}}`
				actualResult = ParseExpressionEntrypoint(filter)

			})

			It("should return a singular elastic query json", func() {
				Expect(expectedResult).To(BeIdenticalTo(actualResult))
			})
		})

		When("when ANDing three TERMs", func() {
			var (
				filter         string
				expectedResult string
				actualResult   string
			)
			BeforeEach(func() {
				filter = "(a==\"b\")&&(c==\"d\")&&(e==\"f\")"
				expectedResult = `{"query":{"bool":{"must":[{"bool":{"must":[{"term":{"a":"b"}},{"term":{"c":"d"}}]}},{"term":{"e":"f"}}]}}}`
				actualResult = ParseExpressionEntrypoint(filter)

			})

			It("should return a singular elastic query json", func() {
				Expect(expectedResult).To(BeIdenticalTo(actualResult))
			})
		})

		When("when ANDing a term and an OR set", func() {
			var (
				filter         string
				expectedResult string
				actualResult   string
			)
			BeforeEach(func() {
				filter = "(a==\"b\")&&((c==\"d\")||(e==\"f\"))"
				expectedResult = `{"query":{"bool":{"must":[{"term":{"a":"b"}},{"bool":{"should":[{"term":{"c":"d"}},{"term":{"e":"f"}}]}}]}}}`
				actualResult = ParseExpressionEntrypoint(filter)

			})

			It("should return a singular elastic query json", func() {
				Expect(expectedResult).To(BeIdenticalTo(actualResult))
			})
		})

		When("when ANDing an AND set and an OR set", func() {
			var (
				filter         string
				expectedResult string
				actualResult   string
			)
			BeforeEach(func() {
				filter = "((a==\"b\")&&(g==\"h\")) && ((c==\"d\")||(e==\"f\"))"
				expectedResult = `{"query":{"bool":{"must":[{"bool":{"must":[{"term":{"a":"b"}},{"term":{"g":"h"}}]}},{"bool":{"should":[{"term":{"c":"d"}},{"term":{"e":"f"}}]}}]}}}`
				actualResult = ParseExpressionEntrypoint(filter)

			})

			It("should return a singular elastic query json", func() {
				Expect(expectedResult).To(BeIdenticalTo(actualResult))
			})
		})

	})
})
