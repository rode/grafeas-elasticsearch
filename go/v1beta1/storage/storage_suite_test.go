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

package storage

import (
	"github.com/brianvoe/gofakeit/v6"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"net/http"
	"testing"
)

var logger = zap.NewNop()
var fake = gofakeit.New(0)

func TestStoragePackage(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Storage Suite")
}

type mockEsTransport struct {
	receivedHttpRequests  []*http.Request
	preparedHttpResponses []*http.Response
	actions               []func(req *http.Request) (*http.Response, error)
}

func (m *mockEsTransport) Perform(req *http.Request) (*http.Response, error) {
	m.receivedHttpRequests = append(m.receivedHttpRequests, req)

	// if we have an action, return its result
	if len(m.actions) != 0 {
		action := m.actions[0]
		if action != nil {
			m.actions = append(m.actions[:0], m.actions[1:]...)
			return action(req)
		}
	}

	// if we have a prepared response, send it
	if len(m.preparedHttpResponses) != 0 {
		res := m.preparedHttpResponses[0]
		m.preparedHttpResponses = append(m.preparedHttpResponses[:0], m.preparedHttpResponses[1:]...)

		return res, nil
	}

	// return nil if we don't know what to do
	return nil, nil
}
