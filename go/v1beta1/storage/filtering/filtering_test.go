package filtering

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSingleTerm(t *testing.T) {
	filter := `a=="b"`

	// {"query":{"bool":{"must":[{"term":{"a":"b"}}]}}}`
	expectedResult := &Query{
		Bool: &Bool{
			Must: &Must{
				&Bool{
					Term: &Term{
						"a": "b",
					},
				},
			},
		},
	}

	actualResult, _ := NewFilterer().ParseExpression(filter)
	assert.Equal(t, expectedResult, actualResult)
}

func TestSingleTermLeftConstRightConst(t *testing.T) {
	filter := `"a"=="b"`

	// {"query":{"bool":{"must":[{"term":{"a":"b"}}]}}}
	expectedResult := &Query{
		Bool: &Bool{
			Must: &Must{
				&Bool{
					Term: &Term{
						"a": "b",
					},
				},
			},
		},
	}

	actualResult, _ := NewFilterer().ParseExpression(filter)
	assert.Equal(t, expectedResult, actualResult)
}

func TestSingleTermLeftIdentRightIdent(t *testing.T) {
	filter := "a==b"

	// {"query":{"bool":{"must":[{"term":{"a":"b"}}]}}}
	expectedResult := &Query{
		Bool: &Bool{
			Must: &Must{
				&Bool{
					Term: &Term{
						"a": "b",
					},
				},
			},
		},
	}

	actualResult, _ := NewFilterer().ParseExpression(filter)
	assert.Equal(t, expectedResult, actualResult)
}

func TestSingleTermLeftConstRightIdent(t *testing.T) {
	filter := `"a"==b`

	// {"query":{"bool":{"must":[{"term":{"a":"b"}}]}}}
	expectedResult := &Query{
		Bool: &Bool{
			Must: &Must{
				&Bool{
					Term: &Term{
						"a": "b",
					},
				},
			},
		},
	}

	actualResult, _ := NewFilterer().ParseExpression(filter)
	assert.Equal(t, expectedResult, actualResult)
}

func TestAndingTwoTerm(t *testing.T) {
	filter := `(a=="b")&&(c=="d")`

	// {"query":{"bool":{"must":[{"term":{"a":"b"}},{"term":{"c":"d"}}]}}}
	expectedResult := &Query{
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
	}

	actualResult, _ := NewFilterer().ParseExpression(filter)
	assert.Equal(t, expectedResult, actualResult)
}

func TestAndingThreeTerms(t *testing.T) {
	filter := `(a=="b")&&(c=="d")&&(e=="f")`

	// {"query":{"bool":{"must":[{"bool":{"must":[{"term":{"a":"b"}},{"term":{"c":"d"}}]}},{"term":{"e":"f"}}]}}}
	expectedResult := &Query{
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
	}

	actualResult, _ := NewFilterer().ParseExpression(filter)
	assert.Equal(t, expectedResult, actualResult)
}

func TestAndingTermWithOrSet(t *testing.T) {
	filter := `(a=="b")&&((c=="d")||(e=="f"))`

	// {"query":{"bool":{"must":[{"term":{"a":"b"}},{"bool":{"should":[{"term":{"c":"d"}},{"term":{"e":"f"}}]}}]}}}
	expectedResult := &Query{
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
	}

	actualResult, _ := NewFilterer().ParseExpression(filter)
	assert.Equal(t, expectedResult, actualResult)
}

func TestAndingAndSetWithOrSet(t *testing.T) {
	filter := `((a=="b")&&(g=="h")) && ((c=="d")||(e=="f"))`
	actualResult, _ := NewFilterer().ParseExpression(filter)

	// `{"query":{"bool":{"must":[{"bool":{"must":[{"term":{"a":"b"}},{"term":{"g":"h"}}]}},{"bool":{"should":[{"term":{"c":"d"}},{"term":{"e":"f"}}]}}]}}}`
	expectedResult := &Query{
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
	}

	assert.Equal(t, expectedResult, actualResult)
}

func TestBadFilter(t *testing.T) {
	filter := `a==`
	actualResult, err := NewFilterer().ParseExpression(filter)
	assert.Nil(t, actualResult)
	assert.Error(t, err)
}
