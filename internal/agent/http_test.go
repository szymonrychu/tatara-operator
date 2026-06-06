package agent_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/szymonrychu/tatara-operator/internal/agent"
)

func staticToken(_ context.Context) (string, error) { return "test-bearer", nil }

func newSession(t *testing.T, h http.HandlerFunc) (agent.Session, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return agent.NewHTTPSession(staticToken), srv
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

func TestGetTurn(t *testing.T) {
	s, srv := newSession(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "/v1/messages/turn-9", r.URL.Path)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"state":      "completed",
			"finalText":  "all green",
			"stopReason": "end_turn",
		})
	})
	tr, err := s.GetTurn(context.Background(), srv.URL, "turn-9")
	require.NoError(t, err)
	require.Equal(t, "completed", tr.State)
	require.Equal(t, "all green", tr.FinalText)
	require.Equal(t, "end_turn", tr.StopReason)
	require.Empty(t, tr.Err)
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
