package agent_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/szymonrychu/tatara-operator/internal/agent"
)

func decodeJSONBody(r *http.Request, out any) error {
	return json.NewDecoder(r.Body).Decode(out)
}

// sessionJSON is the wrapper's GET /v1/session body: its six pre-existing fields
// (session.Snapshot) plus contractVersion.
const sessionJSON = `{
  "state": "ready",
  "turnsCompleted": 3,
  "turnsFinished": 4,
  "model": "claude-opus-4-8",
  "repo": "tatara-operator",
  "lastActivityAt": "2026-07-12T10:00:00Z",
  "contractVersion": 2
}`

// TestGetSession_KeepsAllSixExistingFields: the contract's illustrative
// {alive, turnInFlight, contractVersion} shape would have DROPPED state and
// turnsCompleted, which the operator reads. The response is the existing object
// PLUS one field, and every one of the six must survive the round trip.
func TestGetSession_KeepsAllSixExistingFields(t *testing.T) {
	s, srv := newSession(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "/v1/session", r.URL.Path)
		_, _ = w.Write([]byte(sessionJSON))
	})

	info, err := s.GetSession(context.Background(), srv.URL)
	require.NoError(t, err)

	require.Equal(t, "ready", info.State)
	require.Equal(t, 3, info.TurnsCompleted)
	require.Equal(t, 4, info.TurnsFinished)
	require.Equal(t, "claude-opus-4-8", info.Model)
	require.Equal(t, "tatara-operator", info.Repo)
	require.Equal(t, time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC), info.LastActivityAt.UTC())
	require.NotNil(t, info.ContractVersion)
	require.Equal(t, 2, *info.ContractVersion)
	require.False(t, info.TurnInFlight())
}

func TestSessionInfo_TurnInFlightIsStateBusy(t *testing.T) {
	require.True(t, agent.SessionInfo{State: agent.SessionStateBusy}.TurnInFlight())
	require.False(t, agent.SessionInfo{State: agent.SessionStateReady}.TurnInFlight())
	require.False(t, agent.SessionInfo{State: agent.SessionStateBooting}.TurnInFlight())
	require.False(t, agent.SessionInfo{State: agent.SessionStateDead}.TurnInFlight())
}

// turnRefusingSession fails the test if a turn is EVER submitted. The whole point
// of the G.10 handshake is that a skewed wrapper burns ZERO tokens.
type turnRefusingSession struct {
	t    *testing.T
	body string
	err  error
}

func (s *turnRefusingSession) SubmitTurn(context.Context, string, string, string) (string, error) {
	s.t.Fatal("SubmitTurn was called: a contract-mismatched pod must burn ZERO turns")
	return "", nil
}

func (s *turnRefusingSession) SubmitHandoffTurn(context.Context, string, string, string) (string, error) {
	s.t.Fatal("SubmitHandoffTurn was called: a contract-mismatched pod must burn ZERO turns")
	return "", nil
}

func (s *turnRefusingSession) Interject(context.Context, string, string) error { return nil }

func (s *turnRefusingSession) GetTurn(context.Context, string, string) (agent.TurnResult, error) {
	s.t.Fatal("GetTurn was called: a contract-mismatched pod must burn ZERO turns")
	return agent.TurnResult{}, nil
}

func (s *turnRefusingSession) GetSession(context.Context, string) (agent.SessionInfo, error) {
	if s.err != nil {
		return agent.SessionInfo{}, s.err
	}
	return decodeSessionInfo(s.t, s.body), nil
}

func (s *turnRefusingSession) DeleteSession(context.Context, string) error { return nil }

func decodeSessionInfo(t *testing.T, body string) agent.SessionInfo {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()
	info, err := agent.NewHTTPSession(staticToken).GetSession(context.Background(), srv.URL)
	require.NoError(t, err)
	return info
}

// TestAssertContractVersion covers the G.10 handshake. On a mismatch - OR on a
// response with no contractVersion field at all (an old wrapper) - the assertion
// fails and NOT ONE turn is submitted.
func TestAssertContractVersion(t *testing.T) {
	for _, tc := range []struct {
		name    string
		body    string
		wantErr bool
		wantGot int
	}{
		{name: "matching version", body: `{"state":"ready","contractVersion":2}`},
		{name: "old wrapper reports v1", body: `{"state":"ready","contractVersion":1}`, wantErr: true, wantGot: 1},
		{
			name:    "old wrapper has no contractVersion field at all",
			body:    `{"state":"ready","turnsCompleted":0,"turnsFinished":0,"model":"m","repo":"r","lastActivityAt":"2026-07-12T10:00:00Z"}`,
			wantErr: true, wantGot: 0,
		},
		{name: "future version", body: `{"state":"ready","contractVersion":3}`, wantErr: true, wantGot: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &turnRefusingSession{t: t, body: tc.body}
			err := agent.AssertContractVersion(context.Background(), s, "http://wrapper")
			if !tc.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.True(t, agent.IsContractMismatch(err), "want a ContractMismatchError, got %v", err)
			var mm *agent.ContractMismatchError
			require.ErrorAs(t, err, &mm)
			require.Equal(t, agent.ContractVersion, mm.Expected)
			require.Equal(t, tc.wantGot, mm.Got)
		})
	}
}

// TestSubmitHandoffTurn_SetsHandoffTrue: "handoff": true is a WIRE-TYPE CHANGE
// and without it the whole TTL mechanism is dead - the wrapper 410s the handoff
// turn and Task.status.notes is empty after EVERY TTL stop.
func TestSubmitHandoffTurn_SetsHandoffTrue(t *testing.T) {
	var got map[string]any
	s, srv := newSession(t, func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, decodeJSONBody(r, &got))
		_, _ = w.Write([]byte(`{"turnId":"t-1"}`))
	})
	id, err := s.SubmitHandoffTurn(context.Background(), srv.URL, agent.HandoffTurnText, "http://cb")
	require.NoError(t, err)
	require.Equal(t, "t-1", id)
	require.Equal(t, true, got["handoff"])
	require.Equal(t, agent.HandoffTurnText, got["text"])
	require.Equal(t, "http://cb", got["callbackUrl"])
}

// TestSubmitTurn_DoesNotSetHandoff: the allowance is SCOPED TO PAST t0, and it is
// exactly one turn. A normal turn that carried handoff:true would burn the one
// handoff slot early and leave the pod unable to write its handoff when the TTL
// actually arrives - which is the failure the carve-out exists to prevent.
func TestSubmitTurn_DoesNotSetHandoff(t *testing.T) {
	var got map[string]any
	s, srv := newSession(t, func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, decodeJSONBody(r, &got))
		_, _ = w.Write([]byte(`{"turnId":"t-1"}`))
	})
	_, err := s.SubmitTurn(context.Background(), srv.URL, "do work", "http://cb")
	require.NoError(t, err)
	_, present := got["handoff"]
	require.False(t, present, "a NORMAL turn must never carry handoff:true")
}

// TestIsTTLGone: the TTL refusal is 410 Gone. Not 409 (already taken by "a turn
// is in flight", which the stop sequence branches on) and not 503 ("retry later",
// the opposite of the truth - this pod is never taking another turn).
func TestIsTTLGone(t *testing.T) {
	require.True(t, agent.IsTTLGone(&agent.HTTPError{Status: http.StatusGone}))
	require.False(t, agent.IsTTLGone(&agent.HTTPError{Status: http.StatusConflict}))
	require.False(t, agent.IsTTLGone(&agent.HTTPError{Status: http.StatusServiceUnavailable}))
	require.False(t, agent.IsTTLGone(nil))
	// 409 remains "session busy" and 503 remains transient - unchanged.
	require.True(t, agent.IsSessionBusy(&agent.HTTPError{Status: http.StatusConflict}))
	require.True(t, agent.IsTransientWrapper(&agent.HTTPError{Status: http.StatusServiceUnavailable}))
	require.False(t, agent.IsTransientWrapper(&agent.HTTPError{Status: http.StatusGone}))
}
