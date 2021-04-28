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

import (
	"bytes"
	"encoding/json"
	. "github.com/onsi/gomega"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
)

type TransportAction = func(req *http.Request) (*http.Response, error)

type MockEsTransport struct {
	ReceivedHttpRequests  []*http.Request
	PreparedHttpResponses []*http.Response
	Actions               []TransportAction
}

func (m *MockEsTransport) Perform(req *http.Request) (*http.Response, error) {
	m.ReceivedHttpRequests = append(m.ReceivedHttpRequests, req)

	// if we have an action, return its result
	if len(m.Actions) != 0 {
		action := m.Actions[0]
		if action != nil {
			m.Actions = append(m.Actions[:0], m.Actions[1:]...)
			return action(req)
		}
	}

	// if we have a prepared response, send it
	if len(m.PreparedHttpResponses) != 0 {
		res := m.PreparedHttpResponses[0]
		m.PreparedHttpResponses = append(m.PreparedHttpResponses[:0], m.PreparedHttpResponses[1:]...)

		return res, nil
	}

	// return nil if we don't know what to do
	return nil, nil
}

func CreateIndexOrAliasName(parts ...string) string {
	withPrefix := append([]string{"grafeas"}, parts...)

	return strings.Join(withPrefix, "-")
}

func CreateESBody(value interface{}) io.ReadCloser {
	responseBody, err := json.Marshal(value)
	Expect(err).To(BeNil())

	return ioutil.NopCloser(bytes.NewReader(responseBody))
}

func ReadRequestBody(request *http.Request, target interface{}) {
	rawBody, err := ioutil.ReadAll(request.Body)
	Expect(err).ToNot(HaveOccurred())

	Expect(json.Unmarshal(rawBody, target)).ToNot(HaveOccurred())
}
