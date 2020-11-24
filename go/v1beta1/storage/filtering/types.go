package filtering

// Query holds a parent query that carries the entire search query
type Query struct {
	Query interface{} `json:"query"`
}

// Bool holds a general query that carries any number of
// Must and Should operations
type Bool struct {
	Bool interface{} `json:"bool"`
}

// Must holds a must operator which each equates to an AND operation
type Must struct {
	Must []interface{} `json:"must"`
}

// Should holds a should operator which equates to an OR operation
type Should struct {
	Should []interface{} `json:"should"`
}

// Term holds a comparison for equating two strings
type Term struct {
	Term map[string]string `json:"term"`
}

// The representation of all operations currently supported. Eventually need to assess
// if an enum exists within the cel package.
const (
	AndOperation   = "_&&_"
	OrOperation    = "_||_"
	EqualOperation = "_==_"
)

var operationText = map[string]string{
	AndOperation:   "must",
	OrOperation:    "should",
	EqualOperation: "equal",
}

// OperationText returns the operation provided's corresponding string field name
func OperationText(operation string) string {
	return operationText[operation]
}
