package filtering

import "testing"

func TestSingleTerm(t *testing.T) {
	filter := `a=="b"`
	expectedResult := `{"query":{"bool":{"must":[{"term":{"a":"b"}}]}}}`
	actualResult := ParseExpressionEntrypoint(filter)
	if actualResult != expectedResult {
		t.Errorf("Expected: %v \n Received: %v", expectedResult, actualResult)
	}
}

func TestSingleTermLeftConstRightConst(t *testing.T) {
	filter := `"a"=="b"`
	expectedResult := `{"query":{"bool":{"must":[{"term":{"a":"b"}}]}}}`
	actualResult := ParseExpressionEntrypoint(filter)
	if actualResult != expectedResult {
		t.Errorf("Expected: %v \n Received: %v", expectedResult, actualResult)
	}
}

func TestSingleTermLeftIdentRightIdent(t *testing.T) {
	filter := "a==b"
	expectedResult := `{"query":{"bool":{"must":[{"term":{"a":"b"}}]}}}`
	actualResult := ParseExpressionEntrypoint(filter)
	if actualResult != expectedResult {
		t.Errorf("Expected: %v \n Received: %v", expectedResult, actualResult)
	}
}

func TestSingleTermLeftConstRightIdent(t *testing.T) {
	filter := `"a"==b`
	expectedResult := `{"query":{"bool":{"must":[{"term":{"a":"b"}}]}}}`
	actualResult := ParseExpressionEntrypoint(filter)
	if actualResult != expectedResult {
		t.Errorf("Expected: %v \n Received: %v", expectedResult, actualResult)
	}
}

func TestAndingTwoTerm(t *testing.T) {
	filter := `(a=="b")&&(c=="d")`
	expectedResult := `{"query":{"bool":{"must":[{"term":{"a":"b"}},{"term":{"c":"d"}}]}}}`
	actualResult := ParseExpressionEntrypoint(filter)
	if actualResult != expectedResult {
		t.Errorf("Expected: %v \n Received: %v", expectedResult, actualResult)
	}
}

func TestAndingThreeTerms(t *testing.T) {
	filter := `(a=="b")&&(c=="d")&&(e=="f")`
	expectedResult := `{"query":{"bool":{"must":[{"bool":{"must":[{"term":{"a":"b"}},{"term":{"c":"d"}}]}},{"term":{"e":"f"}}]}}}`
	actualResult := ParseExpressionEntrypoint(filter)
	if actualResult != expectedResult {
		t.Errorf("Expected: %v \n Received: %v", expectedResult, actualResult)
	}
}

func TestAndingTermWithOrSet(t *testing.T) {
	filter := `(a=="b")&&((c=="d")||(e=="f"))`
	expectedResult := `{"query":{"bool":{"must":[{"term":{"a":"b"}},{"bool":{"should":[{"term":{"c":"d"}},{"term":{"e":"f"}}]}}]}}}`
	actualResult := ParseExpressionEntrypoint(filter)
	if actualResult != expectedResult {
		t.Errorf("Expected: %v \n Received: %v", expectedResult, actualResult)
	}
}

func TestAndingAndSetWithOrSet(t *testing.T) {
	filter := `((a=="b")&&(g=="h")) && ((c=="d")||(e=="f"))`
	expectedResult := `{"query":{"bool":{"must":[{"bool":{"must":[{"term":{"a":"b"}},{"term":{"g":"h"}}]}},{"bool":{"should":[{"term":{"c":"d"}},{"term":{"e":"f"}}]}}]}}}`
	actualResult := ParseExpressionEntrypoint(filter)
	if actualResult != expectedResult {
		t.Errorf("Expected: %v \n Received: %v", expectedResult, actualResult)
	}
}

func TestBadFilter(t *testing.T) {
	filter := `a==`
	expectedResult := "Bad Filter"
	actualResult := ParseExpressionEntrypoint(filter)
	if actualResult != expectedResult {
		t.Errorf("Expected: %v \n Received: %v", expectedResult, actualResult)
	}
}
