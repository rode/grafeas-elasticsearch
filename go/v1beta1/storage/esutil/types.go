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

package esutil

// Elasticsearch /$INDEX/block/_write response

type ESBlockIndex struct {
	Name    string `json:"name"`
	Blocked bool   `json:"blocked"`
}

type ESBlockResponse struct {
	Acknowledged       bool           `json:"acknowledged"`
	ShardsAcknowledged bool           `json:"shards_acknowledged"`
	Indices            []ESBlockIndex `json:"indices"`
}

// Elasticsearch /$INDEX/_settings

type ESSettingsResponse struct {
	Settings *ESSettingsIndex `json:"settings"`
}

type ESSettingsIndex struct {
	Index *ESSettingsBlocks `json:"index"`
}

type ESSettingsBlocks struct {
	Blocks *ESSettingsWrite `json:"blocks"`
}

type ESSettingsWrite struct {
	Write string `json:"write"`
}

// response for calls where wait_for_completion=false
type ESTaskCreationResponse struct {
	Task string `json:"task"`
}

// /_tasks/$TASK_ID response
type ESTask struct {
	Completed bool `json:"completed"`
}

// Elasticsearch /_aliases request
type ESActions struct {
	Add    *ESIndexAlias `json:"add,omitempty"`
	Remove *ESIndexAlias `json:"remove,omitempty"`
}

type ESIndexAlias struct {
	Index string `json:"index"`
	Alias string `json:"alias"`
}

type ESIndexAliasRequest struct {
	Actions []ESActions `json:"actions"`
}

// Elasticsearch /_reindex request

type ESReindex struct {
	Conflicts   string         `json:"conflicts"`
	Source      *ReindexFields `json:"source"`
	Destination *ReindexFields `json:"dest"`
}

type ReindexFields struct {
	Index  string `json:"index"`
	OpType string `json:"op_type,omitempty"`
}

// Elasticsearch 400 error response

type ESErrorResponse struct {
	Error ESError `json:"error"`
}

type ESError struct {
	Type string `json:"type"`
}
