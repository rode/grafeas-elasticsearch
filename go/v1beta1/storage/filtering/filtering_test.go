package filtering

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSingleTerm(t *testing.T) {
	filter := `a=="b"`
	expectedResult := `{"query":{"bool":{"must":[{"term":{"a":"b"}}]}}}`
	actualResult, _ := ParseExpressionEntrypoint(filter)
	assert.Equal(t, actualResult, expectedResult)
}

func TestSingleTermLeftConstRightConst(t *testing.T) {
	filter := `"a"=="b"`
	expectedResult := `{"query":{"bool":{"must":[{"term":{"a":"b"}}]}}}`
	actualResult, _ := ParseExpressionEntrypoint(filter)
	assert.Equal(t, actualResult, expectedResult)
}

func TestSingleTermLeftIdentRightIdent(t *testing.T) {
	filter := "a==b"
	expectedResult := `{"query":{"bool":{"must":[{"term":{"a":"b"}}]}}}`
	actualResult, _ := ParseExpressionEntrypoint(filter)
	assert.Equal(t, actualResult, expectedResult)
}

func TestSingleTermLeftConstRightIdent(t *testing.T) {
	filter := `"a"==b`
	expectedResult := `{"query":{"bool":{"must":[{"term":{"a":"b"}}]}}}`
	actualResult, _ := ParseExpressionEntrypoint(filter)
	assert.Equal(t, actualResult, expectedResult)
}

func TestAndingTwoTerm(t *testing.T) {
	filter := `(a=="b")&&(c=="d")`
	expectedResult := `{"query":{"bool":{"must":[{"term":{"a":"b"}},{"term":{"c":"d"}}]}}}`
	actualResult, _ := ParseExpressionEntrypoint(filter)
	assert.Equal(t, actualResult, expectedResult)
}

func TestAndingThreeTerms(t *testing.T) {
	filter := `(a=="b")&&(c=="d")&&(e=="f")`
	expectedResult := `{"query":{"bool":{"must":[{"bool":{"must":[{"term":{"a":"b"}},{"term":{"c":"d"}}]}},{"term":{"e":"f"}}]}}}`
	actualResult, _ := ParseExpressionEntrypoint(filter)
	assert.Equal(t, actualResult, expectedResult)
}

func TestAndingTermWithOrSet(t *testing.T) {
	filter := `(a=="b")&&((c=="d")||(e=="f"))`
	expectedResult := `{"query":{"bool":{"must":[{"term":{"a":"b"}},{"bool":{"should":[{"term":{"c":"d"}},{"term":{"e":"f"}}]}}]}}}`
	actualResult, _ := ParseExpressionEntrypoint(filter)
	assert.Equal(t, actualResult, expectedResult)
}

func TestAndingAndSetWithOrSet(t *testing.T) {
	filter := `((a=="b")&&(g=="h")) && ((c=="d")||(e=="f"))`
	expectedResult := `{"query":{"bool":{"must":[{"bool":{"must":[{"term":{"a":"b"}},{"term":{"g":"h"}}]}},{"bool":{"should":[{"term":{"c":"d"}},{"term":{"e":"f"}}]}}]}}}`
	actualResult, _ := ParseExpressionEntrypoint(filter)
	assert.Equal(t, actualResult, expectedResult)
}

func TestBadFilter(t *testing.T) {
	filter := `a==`
	expectedResult := "Bad Filter"
	expectedErrorMessage := "Syntax error: mismatched input"
	actualResult, err := ParseExpressionEntrypoint(filter)
	assert.Equal(t, actualResult, expectedResult)
	assert.Contains(t, err[0].Message, expectedErrorMessage)
	assert.Equal(t, err[0].Location.Line(), 1)
	assert.Equal(t, err[0].Location.Column(), 3)
}
