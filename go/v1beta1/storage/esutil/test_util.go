package esutil

import "net/http"

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
