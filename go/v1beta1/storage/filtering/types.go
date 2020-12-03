package filtering

// Query holds a parent query that carries the entire search query
type Query struct {
	Bool *Bool `json:"bool,omitempty"`
	Term *Term `json:"term,omitempty"`
}

// Bool holds a general query that carries any number of
// Must and Should operations
type Bool struct {
	Must   *Must   `json:"must,omitempty"`
	Should *Should `json:"should,omitempty"`
	Term   *Term   `json:"term,omitempty"`
}

// Must holds a must operator which each equates to an AND operation
type Must []interface{}

// Should holds a should operator which equates to an OR operation
type Should []interface{}

// Term holds a comparison for equating two strings
type Term map[string]string

// Operation is the representation of all operations currently supported. Eventually needs to assess
// if an enum exists within the cel package.
type Operation string

const (
	AndOperation   Operation = "_&&_"
	OrOperation    Operation = "_||_"
	EqualOperation Operation = "_==_"
)
