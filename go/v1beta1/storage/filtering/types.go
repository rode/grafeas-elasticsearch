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

// Query holds a parent query that carries the entire search query
type Query struct {
	Bool        *Bool             `json:"bool,omitempty"`
	Term        *Term             `json:"term,omitempty"`
	Prefix      *Term             `json:"prefix,omitempty"`
	QueryString *QueryString      `json:"query_string,omitempty"`
	Range       map[string]*Range `json:"range,omitempty"`
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
type MustNot []interface{}

// Should holds a should operator which equates to an OR operation
type Should []interface{}

// Term holds a comparison for equating two strings
type Term map[string]string

type QueryString struct {
	DefaultField string `json:"default_field"`
	Query        string `json:"query"`
}

type Range struct {
	Greater       string `json:"gt,omitempty"`
	GreaterEquals string `json:"gte,omitempty"`
	Less          string `json:"lt,omitempty"`
	LessEquals    string `json:"lte,omitempty"`
}
