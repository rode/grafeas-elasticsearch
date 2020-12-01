package filtering

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSingleTerm(t *testing.T) {
	filter := `a=="b"`

	// {"query":{"bool":{"must":[{"term":{"a":"b"}}]}}}`
	expectedResult := &Query{
		Query: &Bool{
			Bool: &Must{
				Must: []interface{}{
					&Term{
						Term: map[string]string{
							"a": "b",
						},
					},
				},
			},
		},
	}

	actualResult, _ := ParseExpressionEntrypoint(filter)
	assert.Equal(t, expectedResult, actualResult)
}

func TestSingleTermLeftConstRightConst(t *testing.T) {
	filter := `"a"=="b"`

	// {"query":{"bool":{"must":[{"term":{"a":"b"}}]}}}
	expectedResult := &Query{
		Query: &Bool{
			Bool: &Must{
				Must: []interface{}{
					&Term{
						Term: map[string]string{
							"a": "b",
						},
					},
				},
			},
		},
	}

	actualResult, _ := ParseExpressionEntrypoint(filter)
	assert.Equal(t, expectedResult, actualResult)
}

func TestSingleTermLeftIdentRightIdent(t *testing.T) {
	filter := "a==b"

	// {"query":{"bool":{"must":[{"term":{"a":"b"}}]}}}
	expectedResult := &Query{
		Query: &Bool{
			Bool: &Must{
				Must: []interface{}{
					&Term{
						Term: map[string]string{
							"a": "b",
						},
					},
				},
			},
		},
	}

	actualResult, _ := ParseExpressionEntrypoint(filter)
	assert.Equal(t, expectedResult, actualResult)
}

func TestSingleTermLeftConstRightIdent(t *testing.T) {
	filter := `"a"==b`

	// {"query":{"bool":{"must":[{"term":{"a":"b"}}]}}}
	expectedResult := &Query{
		Query: &Bool{
			Bool: &Must{
				Must: []interface{}{
					&Term{
						Term: map[string]string{
							"a": "b",
						},
					},
				},
			},
		},
	}

	actualResult, _ := ParseExpressionEntrypoint(filter)
	assert.Equal(t, expectedResult, actualResult)
}

func TestAndingTwoTerm(t *testing.T) {
	filter := `(a=="b")&&(c=="d")`

	// {"query":{"bool":{"must":[{"term":{"a":"b"}},{"term":{"c":"d"}}]}}}
	expectedResult := &Query{
		Query: &Bool{
			Bool: &Must{
				Must: []interface{}{
					&Term{
						Term: map[string]string{
							"a": "b",
						},
					},
					&Term{
						Term: map[string]string{
							"c": "d",
						},
					},
				},
			},
		},
	}

	actualResult, _ := ParseExpressionEntrypoint(filter)
	assert.Equal(t, expectedResult, actualResult)
}

func TestAndingThreeTerms(t *testing.T) {
	filter := `(a=="b")&&(c=="d")&&(e=="f")`

	// {"query":{"bool":{"must":[{"bool":{"must":[{"term":{"a":"b"}},{"term":{"c":"d"}}]}},{"term":{"e":"f"}}]}}}
	expectedResult := &Query{
		Query: &Bool{
			Bool: &Must{
				Must: []interface{}{
					&Bool{
						Bool: &Must{
							Must: []interface{}{
								&Term{
									Term: map[string]string{
										"a": "b",
									},
								},
								&Term{
									Term: map[string]string{
										"c": "d",
									},
								},
							},
						},
					},
					&Term{
						Term: map[string]string{
							"e": "f",
						},
					},
				},
			},
		},
	}

	actualResult, _ := ParseExpressionEntrypoint(filter)
	assert.Equal(t, expectedResult, actualResult)
}

func TestAndingTermWithOrSet(t *testing.T) {
	filter := `(a=="b")&&((c=="d")||(e=="f"))`

	// {"query":{"bool":{"must":[{"term":{"a":"b"}},{"bool":{"should":[{"term":{"c":"d"}},{"term":{"e":"f"}}]}}]}}}
	expectedResult := &Query{
		Query: &Bool{
			Bool: &Must{
				Must: []interface{}{
					&Term{
						Term: map[string]string{
							"a": "b",
						},
					},
					&Bool{
						Bool: &Should{
							Should: []interface{}{
								&Term{
									Term: map[string]string{
										"c": "d",
									},
								},
								&Term{
									Term: map[string]string{
										"e": "f",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	actualResult, _ := ParseExpressionEntrypoint(filter)
	assert.Equal(t, expectedResult, actualResult)
}

func TestAndingAndSetWithOrSet(t *testing.T) {
	filter := `((a=="b")&&(g=="h")) && ((c=="d")||(e=="f"))`
	// `{"query":{"bool":{"must":[{"bool":{"must":[{"term":{"a":"b"}},{"term":{"g":"h"}}]}},{"bool":{"should":[{"term":{"c":"d"}},{"term":{"e":"f"}}]}}]}}}`
	actualResult, _ := ParseExpressionEntrypoint(filter)
	expectedResult := &Query{
		Query: &Bool{
			Bool: &Must{
				Must: []interface{}{

					&Bool{
						Bool: &Must{
							Must: []interface{}{
								&Term{
									Term: map[string]string{
										"a": "b",
									},
								},
								&Term{
									Term: map[string]string{
										"g": "h",
									},
								},
							},
						},
					},

					&Bool{
						Bool: &Should{
							Should: []interface{}{
								&Term{
									Term: map[string]string{
										"c": "d",
									},
								},
								&Term{
									Term: map[string]string{
										"e": "f",
									},
								},
							},
						},
					},
				},
			},
		},
	}
	assert.Equal(t, expectedResult, actualResult)
}

func TestBadFilter(t *testing.T) {
	filter := `a==`
	expectedErrorMessage := "Syntax error: mismatched input"
	actualResult, err := ParseExpressionEntrypoint(filter)
	assert.Nil(t, actualResult)
	assert.Contains(t, err[0].Message, expectedErrorMessage)
	assert.Equal(t, 1, err[0].Location.Line())
	assert.Equal(t, 3, err[0].Location.Column())
}
