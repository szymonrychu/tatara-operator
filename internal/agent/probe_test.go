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

// TestProbe_SendsAndReturnsProbeID covers the happy path: POST /v1/probe with the
// probe text, 202, probeId back.
func TestProbe_SendsAndReturnsProbeID(t *testing.T) {
	var gotPath, gotMethod string
	var gotBody struct {
		Text string `json:"text"`
	}
	s, srv := newSession(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod = r.URL.Path, r.Method
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"probeId": "pr-1"})
	})

	id, err := s.Probe(context.Background(), srv.URL, "are you alive?")
	require.NoError(t, err)
	require.Equal(t, "pr-1", id)
	require.Equal(t, "/v1/probe", gotPath)
	require.Equal(t, http.MethodPost, gotMethod)
	require.Equal(t, "are you alive?", gotBody.Text)
}

// TestProbeStatus_DecodesEveryState walks the three probe states and asserts the
// timestamps decode. The state is what the caller branches on, so each one is
// pinned rather than only the answered case.
func TestProbeStatus_DecodesEveryState(t *testing.T) {
	sent := time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)
	delivered := sent.Add(2 * time.Second)
	answered := delivered.Add(3 * time.Second)

	cases := []struct {
		name         string
		body         map[string]string
		wantState    string
		wantAnswer   string
		wantAnswered bool
		wantAnswerAt time.Time
	}{
		{
			name:      "pending: enqueued, never consumed",
			body:      map[string]string{"probeId": "pr-1", "state": agent.ProbeStatePending, "sentAt": sent.Format(time.RFC3339)},
			wantState: agent.ProbeStatePending,
		},
		{
			name: "delivered but unanswered",
			body: map[string]string{"probeId": "pr-1", "state": agent.ProbeStateDelivered,
				"sentAt": sent.Format(time.RFC3339), "deliveredAt": delivered.Format(time.RFC3339)},
			wantState: agent.ProbeStateDelivered,
		},
		{
			name: "answered",
			body: map[string]string{"probeId": "pr-1", "state": agent.ProbeStateAnswered,
				"sentAt": sent.Format(time.RFC3339), "deliveredAt": delivered.Format(time.RFC3339),
				"answeredAt": answered.Format(time.RFC3339), "answer": "TATARA-ALIVE running the test suite"},
			wantState:    agent.ProbeStateAnswered,
			wantAnswer:   "TATARA-ALIVE running the test suite",
			wantAnswered: true,
			wantAnswerAt: answered,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotPath string
			s, srv := newSession(t, func(w http.ResponseWriter, r *http.Request) {
				gotPath = r.URL.Path
				_ = json.NewEncoder(w).Encode(tc.body)
			})

			got, err := s.ProbeStatus(context.Background(), srv.URL, "pr-1")
			require.NoError(t, err)
			require.Equal(t, "/v1/probe/pr-1", gotPath)
			require.Equal(t, tc.wantState, got.State)
			require.Equal(t, tc.wantAnswer, got.Answer)
			require.Equal(t, tc.wantAnswered, got.Answered())
			require.True(t, got.SentAt.Equal(sent))
			if !tc.wantAnswerAt.IsZero() {
				require.True(t, got.AnsweredAt.Equal(tc.wantAnswerAt))
			}
		})
	}
}

// TestProbeStatus_UnparseableTimestampDegradesToZero: the STATE is what decides,
// so one malformed optional stamp must not fail the whole read and turn a
// cosmetic wrapper bug into a stall verdict.
func TestProbeStatus_UnparseableTimestampDegradesToZero(t *testing.T) {
	s, srv := newSession(t, func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"probeId": "pr-1", "state": agent.ProbeStateAnswered, "answeredAt": "not-a-time",
		})
	})

	got, err := s.ProbeStatus(context.Background(), srv.URL, "pr-1")
	require.NoError(t, err)
	require.True(t, got.Answered())
	require.True(t, got.AnsweredAt.IsZero())
}

// TestInterrupt_PostsToInterruptEndpoint covers the ESC primitive.
func TestInterrupt_PostsToInterruptEndpoint(t *testing.T) {
	var gotPath, gotMethod string
	s, srv := newSession(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod = r.URL.Path, r.Method
		w.WriteHeader(http.StatusAccepted)
	})

	require.NoError(t, s.Interrupt(context.Background(), srv.URL))
	require.Equal(t, "/v1/interrupt", gotPath)
	require.Equal(t, http.MethodPost, gotMethod)
}

// TestProbeEndpoints_404And405AreProbeUnsupported IS THE OLD-WRAPPER CONTRACT.
//
// An operator that has rolled ahead of the wrapper image - a guaranteed window on
// every release train - must map "no such endpoint" onto ErrProbeUnsupported so
// the caller falls back to the pre-probe behaviour verbatim. Any other mapping
// turns a routine mid-train state into a failure, which is the #544 shape.
func TestProbeEndpoints_404And405AreProbeUnsupported(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusMethodNotAllowed} {
		for _, call := range []struct {
			name string
			run  func(agent.Session, string) error
		}{
			{"probe", func(s agent.Session, u string) error { _, err := s.Probe(context.Background(), u, "x"); return err }},
			{"probe_status", func(s agent.Session, u string) error {
				_, err := s.ProbeStatus(context.Background(), u, "pr-1")
				return err
			}},
			{"interrupt", func(s agent.Session, u string) error { return s.Interrupt(context.Background(), u) }},
		} {
			t.Run(http.StatusText(status)+"/"+call.name, func(t *testing.T) {
				s, srv := newSession(t, func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(status)
					_, _ = w.Write([]byte("no such route"))
				})
				err := call.run(s, srv.URL)
				require.Error(t, err)
				require.True(t, agent.IsProbeUnsupported(err),
					"%s on %s must map to ErrProbeUnsupported, got %v", http.StatusText(status), call.name, err)
			})
		}
	}
}

// TestProbeEndpoints_OtherErrorsStayErrors: a BROKEN new wrapper must never be
// silently downgraded to "old wrapper", or a real outage disappears behind the
// fallback path.
func TestProbeEndpoints_OtherErrorsStayErrors(t *testing.T) {
	for _, status := range []int{http.StatusInternalServerError, http.StatusUnauthorized,
		http.StatusServiceUnavailable, http.StatusConflict} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			s, srv := newSession(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
			})
			_, err := s.Probe(context.Background(), srv.URL, "x")
			require.Error(t, err)
			require.False(t, agent.IsProbeUnsupported(err),
				"%d must NOT be read as an old wrapper", status)
		})
	}
}

// TestProbeEndpoints_TransportErrorIsNotProbeUnsupported: an unreachable pod is a
// pod that is still booting or already gone, not a pod on an old image.
func TestProbeEndpoints_TransportErrorIsNotProbeUnsupported(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close() // nothing is listening any more

	s := agent.NewHTTPSession(staticToken)
	_, err := s.Probe(context.Background(), url, "x")
	require.Error(t, err)
	require.False(t, agent.IsProbeUnsupported(err))
	require.True(t, agent.IsTransientWrapper(err))
}

// TestProbeEndpoints_MetricsLabels pins the logical method names, which are the
// operator_agent_http_total{method} label values.
func TestProbeEndpoints_MetricsLabels(t *testing.T) {
	m := &fakeMetrics{}
	s, srv := newSessionWithMetrics(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"probeId": "pr-1", "state": agent.ProbeStatePending})
	}, m)

	_, err := s.Probe(context.Background(), srv.URL, "x")
	require.NoError(t, err)
	_, err = s.ProbeStatus(context.Background(), srv.URL, "pr-1")
	require.NoError(t, err)
	require.NoError(t, s.Interrupt(context.Background(), srv.URL))

	require.Len(t, m.calls, 3)
	require.Equal(t, "probe", m.calls[0].method)
	require.Equal(t, "probe_status", m.calls[1].method)
	require.Equal(t, "interrupt", m.calls[2].method)
}

// TestSessionInfo_SubagentFieldsArePointers IS THE REASON THEY ARE POINTERS.
//
// An old wrapper omits both fields. Decoded into plain values they would read as
// "no subagent activity, ever" and "no subagents running" - two claims the old
// wrapper never made, and precisely the false-idle reading this whole phase
// exists to remove.
func TestSessionInfo_SubagentFieldsArePointers(t *testing.T) {
	t.Run("old wrapper omits both", func(t *testing.T) {
		s, srv := newSession(t, func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"state": "busy", "lastActivityAt": "2026-08-09T10:00:00Z", "contractVersion": agent.ContractVersion,
			})
		})
		info, err := s.GetSession(context.Background(), srv.URL)
		require.NoError(t, err)
		require.Nil(t, info.LastSubagentActivityAt)
		require.Nil(t, info.OutstandingSubagentCalls)
	})

	t.Run("new wrapper reports both, including a genuine zero", func(t *testing.T) {
		sub := time.Date(2026, 8, 9, 10, 35, 0, 0, time.UTC)
		s, srv := newSession(t, func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"state": "busy", "lastActivityAt": "2026-08-09T10:00:00Z",
				"contractVersion":          agent.ContractVersion,
				"lastSubagentActivityAt":   sub.Format(time.RFC3339),
				"outstandingSubagentCalls": 0,
			})
		})
		info, err := s.GetSession(context.Background(), srv.URL)
		require.NoError(t, err)
		require.NotNil(t, info.LastSubagentActivityAt)
		require.True(t, info.LastSubagentActivityAt.Equal(sub))
		// A REPORTED zero is not the same fact as an absent field, and the pointer
		// is the only thing that can tell them apart.
		require.NotNil(t, info.OutstandingSubagentCalls)
		require.Equal(t, 0, *info.OutstandingSubagentCalls)
	})
}
