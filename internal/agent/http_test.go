package agent_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/szymonrychu/tatara-operator/internal/agent"
)

func staticToken(_ context.Context) (string, error) { return "test-bearer", nil }

// recordedCall holds one call to the metrics recorder.
type recordedCall struct {
	method, outcome string
	seconds         float64
}

type fakeMetrics struct {
	calls []recordedCall
}

func (f *fakeMetrics) AgentHTTP(method, outcome string, seconds float64) {
	f.calls = append(f.calls, recordedCall{method, outcome, seconds})
}

func newSession(t *testing.T, h http.HandlerFunc) (agent.Session, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return agent.NewHTTPSession(staticToken), srv
}

func newSessionWithMetrics(t *testing.T, h http.HandlerFunc, m agent.AgentHTTPRecorder) (agent.Session, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return agent.NewHTTPSessionWithMetrics(staticToken, m), srv
}

// TestHTTPMetrics_SuccessRecorded checks that a successful do() call records
// method="submit_turn" and outcome="ok".
func TestHTTPMetrics_SuccessRecorded(t *testing.T) {
	m := &fakeMetrics{}
	s, srv := newSessionWithMetrics(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"turnId": "t1"})
	}, m)

	_, err := s.SubmitTurn(context.Background(), srv.URL, "hi", "http://cb")
	require.NoError(t, err)

	require.Len(t, m.calls, 1)
	require.Equal(t, "submit_turn", m.calls[0].method)
	require.Equal(t, "ok", m.calls[0].outcome)
	require.Positive(t, m.calls[0].seconds)
}

// TestHTTPMetrics_HTTPErrorRecorded checks that an HTTP error records outcome="http_error".
func TestHTTPMetrics_HTTPErrorRecorded(t *testing.T) {
	m := &fakeMetrics{}
	s, srv := newSessionWithMetrics(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte("conflict"))
	}, m)

	_, err := s.SubmitTurn(context.Background(), srv.URL, "hi", "http://cb")
	require.Error(t, err)

	require.Len(t, m.calls, 1)
	require.Equal(t, "submit_turn", m.calls[0].method)
	require.Equal(t, "http_error", m.calls[0].outcome)
}

// TestHTTPMetrics_UnreachableRecorded checks that a transport error records outcome="unreachable".
func TestHTTPMetrics_UnreachableRecorded(t *testing.T) {
	m := &fakeMetrics{}
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close()

	s := agent.NewHTTPSessionWithMetrics(staticToken, m)
	_, err := s.SubmitTurn(context.Background(), url, "hi", "http://cb")
	require.Error(t, err)

	require.Len(t, m.calls, 1)
	require.Equal(t, "submit_turn", m.calls[0].method)
	require.Equal(t, "unreachable", m.calls[0].outcome)
}

// TestHTTPMetrics_NilRecorder ensures NewHTTPSession (no metrics) does not panic.
func TestHTTPMetrics_NilRecorder(t *testing.T) {
	s, srv := newSession(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"turnId": "t2"})
	})
	_, err := s.SubmitTurn(context.Background(), srv.URL, "hi", "http://cb")
	require.NoError(t, err) // must not panic
}

func TestSubmitTurn_202(t *testing.T) {
	s, srv := newSession(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/v1/messages", r.URL.Path)
		require.Equal(t, "Bearer test-bearer", r.Header.Get("Authorization"))

		var in struct {
			Text        string `json:"text"`
			CallbackURL string `json:"callbackUrl"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&in))
		require.Equal(t, "do the thing", in.Text)
		require.Equal(t, "http://op/internal/turn-complete", in.CallbackURL)

		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"turnId": "turn-1"})
	})

	id, err := s.SubmitTurn(context.Background(), srv.URL, "do the thing", "http://op/internal/turn-complete")
	require.NoError(t, err)
	require.Equal(t, "turn-1", id)
}

func TestInterject_202(t *testing.T) {
	s, srv := newSession(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/v1/interject", r.URL.Path)
		require.Equal(t, "Bearer test-bearer", r.Header.Get("Authorization"))

		var in struct {
			Text string `json:"text"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&in))
		require.Equal(t, "new context", in.Text)

		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{})
	})

	require.NoError(t, s.Interject(context.Background(), srv.URL, "new context"))
}

func TestInterject_409WhenNoInflightTurn(t *testing.T) {
	s, srv := newSession(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte("no in-flight turn"))
	})
	err := s.Interject(context.Background(), srv.URL, "x")
	var he *agent.HTTPError
	require.ErrorAs(t, err, &he)
	require.Equal(t, 409, he.Status)
}

func TestSubmitTurn_409(t *testing.T) {
	s, srv := newSession(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte("turn in flight"))
	})
	_, err := s.SubmitTurn(context.Background(), srv.URL, "x", "y")
	require.Error(t, err)
	var he *agent.HTTPError
	require.ErrorAs(t, err, &he)
	require.Equal(t, 409, he.Status)
}

func TestSubmitTurn_TransportError_IsUnreachable(t *testing.T) {
	// A closed port yields a dial-refused transport error: the wrapper pod is
	// booting its turn server even though the Service has endpoints.
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close()

	s := agent.NewHTTPSession(staticToken)
	_, err := s.SubmitTurn(context.Background(), url, "x", "y")
	require.Error(t, err)
	var ue *agent.UnreachableError
	require.ErrorAs(t, err, &ue, "transport failure must classify as UnreachableError")
}

func TestSubmitTurn_HTTPError_NotUnreachable(t *testing.T) {
	s, srv := newSession(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte("turn in flight"))
	})
	_, err := s.SubmitTurn(context.Background(), srv.URL, "x", "y")
	var ue *agent.UnreachableError
	require.False(t, errors.As(err, &ue), "an HTTP non-2xx response must NOT classify as UnreachableError")
}

func TestGetTurn(t *testing.T) {
	s, srv := newSession(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "/v1/messages/turn-9", r.URL.Path)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"state":          "completed",
			"finalText":      "all green",
			"stopReason":     "end_turn",
			"lastActivityAt": "2026-06-19T12:34:56Z",
		})
	})
	tr, err := s.GetTurn(context.Background(), srv.URL, "turn-9")
	require.NoError(t, err)
	require.Equal(t, "completed", tr.State)
	require.Equal(t, "all green", tr.FinalText)
	require.Equal(t, "end_turn", tr.StopReason)
	require.Empty(t, tr.Err)
	require.Equal(t, time.Date(2026, 6, 19, 12, 34, 56, 0, time.UTC), tr.LastActivityAt.UTC())
}

func TestGetTurn_MissingLastActivityIsZero(t *testing.T) {
	s, srv := newSession(t, func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"state": "running"})
	})
	tr, err := s.GetTurn(context.Background(), srv.URL, "turn-1")
	require.NoError(t, err)
	require.True(t, tr.LastActivityAt.IsZero(), "absent lastActivityAt must deserialize to zero time")
}

func TestGetTurn_CarriesErrorField(t *testing.T) {
	s, srv := newSession(t, func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"state": "failed",
			"error": "boom",
		})
	})
	tr, err := s.GetTurn(context.Background(), srv.URL, "turn-x")
	require.NoError(t, err)
	require.Equal(t, "failed", tr.State)
	require.Equal(t, "boom", tr.Err)
}

func TestGetTurn_NotFound(t *testing.T) {
	s, srv := newSession(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	_, err := s.GetTurn(context.Background(), srv.URL, "missing")
	var he *agent.HTTPError
	require.ErrorAs(t, err, &he)
	require.Equal(t, 404, he.Status)
}

func TestDeleteSession(t *testing.T) {
	called := false
	s, srv := newSession(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodDelete, r.Method)
		require.Equal(t, "/v1/session", r.URL.Path)
		require.Equal(t, "Bearer test-bearer", r.Header.Get("Authorization"))
		called = true
		w.WriteHeader(http.StatusNoContent)
	})
	require.NoError(t, s.DeleteSession(context.Background(), srv.URL))
	require.True(t, called)
}

func TestDeleteSession_PropagatesError(t *testing.T) {
	s, srv := newSession(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("kaboom"))
	})
	err := s.DeleteSession(context.Background(), srv.URL)
	var he *agent.HTTPError
	require.ErrorAs(t, err, &he)
	require.Equal(t, 500, he.Status)
}
