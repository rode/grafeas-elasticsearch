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

package migration

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/brianvoe/gofakeit/v6"
	"go.uber.org/zap"
)

var logger = zap.NewNop()
var fake = gofakeit.New(0)

func TestMigrationPackage(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Migration Suite")
}

func createEsBody(value interface{}) io.ReadCloser {
	responseBody, err := json.Marshal(value)
	Expect(err).To(BeNil())

	return ioutil.NopCloser(bytes.NewReader(responseBody))
}

func readRequestBody(request *http.Request, target interface{}) {
	rawBody, err := ioutil.ReadAll(request.Body)
	Expect(err).To(BeNil())


	Expect(json.Unmarshal(rawBody, target)).To(BeNil())
}
