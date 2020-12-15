package filtering

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("Filter", func() {
	Describe("ParseExpression", func() {
		DescribeTable("filter cases", func(filter string, expected interface{}) {
			result, err := NewFilterer().ParseExpression(filter)

			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal(expected))
		},
			Entry("single term left ident right const", `a=="b"`, &Query{
				Bool: &Bool{
					Must: &Must{
						&Bool{
							Term: &Term{
								"a": "b",
							},
						},
					},
				},
			}),
			Entry("single term left const right const", `"a"=="b"`, &Query{
				Bool: &Bool{
					Must: &Must{
						&Bool{
							Term: &Term{
								"a": "b",
							},
						},
					},
				},
			}),
			Entry("single term left ident right ident", `a==b`, &Query{
				Bool: &Bool{
					Must: &Must{
						&Bool{
							Term: &Term{
								"a": "b",
							},
						},
					},
				},
			}),
			Entry("single term left const right ident", `"a"==b`, &Query{
				Bool: &Bool{
					Must: &Must{
						&Bool{
							Term: &Term{
								"a": "b",
							},
						},
					},
				},
			}),
			Entry("and two terms", `(a=="b")&&(c=="d")`, &Query{
				Bool: &Bool{
					Must: &Must{
						&Bool{
							Term: &Term{
								"a": "b",
							},
						},
						&Bool{
							Term: &Term{
								"c": "d",
							},
						},
					},
				},
			}),
			Entry("and three terms", `(a=="b")&&(c=="d")&&(e=="f")`, &Query{
				Bool: &Bool{
					Must: &Must{
						&Query{
							Bool: &Bool{
								Must: &Must{
									&Bool{
										Term: &Term{
											"a": "b",
										},
									},
									&Bool{
										Term: &Term{
											"c": "d",
										},
									},
								},
							},
						},
						&Bool{
							Term: &Term{
								"e": "f",
							},
						},
					},
				},
			}),
			Entry("and term with or set", `(a=="b")&&((c=="d")||(e=="f"))`, &Query{
				Bool: &Bool{
					Must: &Must{
						&Bool{
							Term: &Term{
								"a": "b",
							},
						},
						&Query{
							Bool: &Bool{
								Should: &Should{
									&Bool{
										Term: &Term{
											"c": "d",
										},
									},
									&Bool{
										Term: &Term{
											"e": "f",
										},
									},
								},
							},
						},
					},
				},
			}),
			Entry("and (and set) with or set", `((a=="b")&&(g=="h")) && ((c=="d")||(e=="f"))`, &Query{
				Bool: &Bool{
					Must: &Must{
						&Query{
							Bool: &Bool{
								Must: &Must{
									&Bool{
										Term: &Term{
											"a": "b",
										},
									},
									&Bool{
										Term: &Term{
											"g": "h",
										},
									},
								},
							},
						},
						&Query{
							Bool: &Bool{
								Should: &Should{
									&Bool{
										Term: &Term{
											"c": "d",
										},
									},
									&Bool{
										Term: &Term{
											"e": "f",
										},
									},
								},
							},
						},
					},
				},
			}),
		)

		DescribeTable("error handling", func(filter string) {
			result, err := NewFilterer().ParseExpression(filter)

			Expect(err).To(HaveOccurred())
			Expect(result).To(BeNil())
		},
			Entry("single term missing lhs value", `==b`),
			Entry("single term missing rhs value", `a==`),
			Entry("doesn't resemble anything close to a filter", "lol"),
			Entry("equal comparison with lhs value containing unknown operator without quotes", `a/b==c`),
			Entry("equal comparison with rhs value containing unknown operator without quotes", `a==b/c`),
			Entry("or comparison with lhs value containing unknown operator without quotes", `a/b||c==d`),
			Entry("or comparison with rhs value containing unknown operator without quotes", `a==b||c/d`),
			Entry("and comparison with lhs value containing unknown operator without quotes", `a/b&&c==d`),
			Entry("and comparison with rhs value containing unknown operator without quotes", `a==b&&c/d`),
		)
	})
})
