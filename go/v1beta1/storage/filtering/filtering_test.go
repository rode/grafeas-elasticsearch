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
			Entry("single term missing right value", `a==`),
			Entry("single term missing left value", `==b`),
			Entry("const containing unknown operator without quotes", `a==b/c`),
		)
	})
})
