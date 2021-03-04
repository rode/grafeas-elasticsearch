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

package filtering

import (
	"encoding/json"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("Filter", func() {
	Describe("ParseExpression", func() {
		DescribeTable("filter cases", func(filter string, expected interface{}) {
			result, err := NewFilterer().ParseExpression(filter)
			resultJson, _ := json.MarshalIndent(result, "", "  ")

			Expect(err).ToNot(HaveOccurred())
			Expect(result).To(Equal(expected), string(resultJson))
		},
			Entry("single term left ident right const", `a=="b"`, &Query{
				Term: &Term{
					"a": "b",
				},
			}),
			Entry("single term left const right const", `"a"=="b"`, &Query{
				Term: &Term{
					"a": "b",
				},
			}),
			Entry("single term left ident right ident", `a==b`, &Query{
				Term: &Term{
					"a": "b",
				},
			}),
			Entry("single term left const right ident", `"a"==b`, &Query{
				Term: &Term{
					"a": "b",
				},
			}),
			Entry("and two terms", `(a=="b")&&(c=="d")`, &Query{
				Bool: &Bool{
					Must: &Must{
						&Query{
							Term: &Term{
								"a": "b",
							},
						},
						&Query{
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
									&Query{
										Term: &Term{
											"a": "b",
										},
									},
									&Query{
										Term: &Term{
											"c": "d",
										},
									},
								},
							},
						},
						&Query{
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
						&Query{
							Term: &Term{
								"a": "b",
							},
						},
						&Query{
							Bool: &Bool{
								Should: &Should{
									&Query{
										Term: &Term{
											"c": "d",
										},
									},
									&Query{
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
									&Query{
										Term: &Term{
											"a": "b",
										},
									},
									&Query{
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
									&Query{
										Term: &Term{
											"c": "d",
										},
									},
									&Query{
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
			Entry("simple not equals", `"a" != "b"`, &Query{
				Bool: &Bool{
					MustNot: &MustNot{
						&Bool{
							Term: &Term{
								"a": "b",
							},
						},
					},
				},
			}),
			Entry("and two terms, equals and not equals", `a == b && c != d`, &Query{
				Bool: &Bool{
					Must: &Must{
						&Query{
							Term: &Term{
								"a": "b",
							},
						},
						&Query{
							Bool: &Bool{
								MustNot: &MustNot{
									&Bool{
										Term: &Term{
											"c": "d",
										},
									},
								},
							},
						},
					},
				},
			}),
			Entry("or two terms, equals and not equals", `a == b || c != d`, &Query{
				Bool: &Bool{
					Should: &Should{
						&Query{
							Term: &Term{
								"a": "b",
							},
						},
						&Query{
							Bool: &Bool{
								MustNot: &MustNot{
									&Bool{
										Term: &Term{
											"c": "d",
										},
									},
								},
							},
						},
					},
				},
			}),
			Entry("startsWith ident lhs const rhs", `a.startsWith("b")`, &Query{
				Prefix: &Term{
					"a": "b",
				},
			}),
			Entry("or, equals and startsWith", `a == b || c.startsWith(d)`, &Query{
				Bool: &Bool{
					Should: &Should{
						&Query{
							Term: &Term{
								"a": "b",
							},
						},
						&Query{
							Prefix: &Term{
								"c": "d",
							},
						},
					},
				},
			}),
			Entry("basic contains", `a.contains("b")`, &Query{
				QueryString: &QueryString{
					DefaultField: "a",
					Query:        "*b*",
				},
			}),
			Entry("contains with escaped special characters", `"resource.uri".contains("https://")`, &Query{
				QueryString: &QueryString{
					DefaultField: "resource.uri",
					Query:        `*https\:\/\/*`,
				},
			}),
			Entry("select expression function call", `a.b.c.d.startsWith("e")`, &Query{
				Prefix: &Term{
					"a.b.c.d": "e",
				},
			}),
			Entry("contains on select expression", `a.b.c.contains("d")`, &Query{
				QueryString: &QueryString{
					DefaultField: "a.b.c",
					Query:        "*d*",
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
			Entry("not equal comparison with lhs value containing unknown operator without quotes", `a/b!=c`),
			Entry("not equal comparison with rhs value containing unknown operator without quotes", `a!=b/c`),
			Entry("or comparison with lhs value containing unknown operator without quotes", `a/b||c==d`),
			Entry("or comparison with rhs value containing unknown operator without quotes", `a==b||c/d`),
			Entry("and comparison with lhs value containing unknown operator without quotes", `a/b&&c==d`),
			Entry("and comparison with rhs value containing unknown operator without quotes", `a==b&&c/d`),
		)
	})
})
