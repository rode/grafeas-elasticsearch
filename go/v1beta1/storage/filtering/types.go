package filtering

// Query holds a parent query that carries the entire search query
type Query struct {
	Bool *Bool `json:"bool,omitempty"`
	Term *Term `json:"term,omitempty"`
}

// Bool holds a general query that carries any number of
// Must, MustNot, and Should operations
type Bool struct {
	Must    *Must    `json:"must,omitempty"`
	MustNot *MustNot `json:"must_not,omitempty"`
	Should  *Should  `json:"should,omitempty"`
	Term    *Term    `json:"term,omitempty"`
}

// Must holds a must operator which each equates to an AND operation
type Must []interface{}

// MustNot holds a must_not operator which each equates to a != operation
type MustNot struct {
	Bool *Bool `json:"bool,omitempty"`
	Term *Term `json:"term,omitempty"`
}

// Should holds a should operator which equates to an OR operation
type Should []interface{}

// Term holds a comparison for equating two strings
type Term map[string]string
