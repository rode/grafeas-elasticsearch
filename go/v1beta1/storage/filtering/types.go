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

// Operation is the representation of all operations currently supported. Eventually needs to assess
// if an enum exists within the cel package.
type Operation string

const (
	AndOperation   Operation = "_&&_"
	OrOperation    Operation = "_||_"
	EqualOperation Operation = "_==_"
)
