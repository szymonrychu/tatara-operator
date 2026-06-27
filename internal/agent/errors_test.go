package agent_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// TestIsTransientWrapper checks that UnreachableError and the transient wrapper
// statuses (503/425) classify as "wrapper not ready yet" while other non-2xx
// statuses and unrelated errors do not (issue #164).
func TestIsTransientWrapper(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"unreachable", &agent.UnreachableError{Err: errors.New("connection refused")}, true},
		{"http_503", &agent.HTTPError{Status: 503}, true},
		{"http_425", &agent.HTTPError{Status: 425}, true},
		{"http_409", &agent.HTTPError{Status: 409}, false},
		{"http_500", &agent.HTTPError{Status: 500}, false},
		{"wrapped_503", fmt.Errorf("submit plan turn: %w", &agent.HTTPError{Status: 503}), true},
		{"other", errors.New("boom"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, agent.IsTransientWrapper(tc.err))
		})
	}
}

// TestIsSessionBusy checks that only a wrapper HTTP 409 ("session busy")
// classifies as transient backpressure, while other statuses, transport-level
// errors and unrelated errors do not (issue #168).
func TestIsSessionBusy(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"http_409", &agent.HTTPError{Status: 409}, true},
		{"wrapped_409", fmt.Errorf("submit plan turn: %w", &agent.HTTPError{Status: 409}), true},
		{"http_503", &agent.HTTPError{Status: 503}, false},
		{"http_425", &agent.HTTPError{Status: 425}, false},
		{"http_500", &agent.HTTPError{Status: 500}, false},
		{"unreachable", &agent.UnreachableError{Err: errors.New("connection refused")}, false},
		{"other", errors.New("boom"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, agent.IsSessionBusy(tc.err))
		})
	}
}

// TestSubmitOutcome checks the low-cardinality outcome label mapping used by the
// operator_turn_submit_total metric and the failure log.
func TestSubmitOutcome(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, "ok"},
		{"unreachable", &agent.UnreachableError{Err: errors.New("x")}, "unreachable"},
		{"http_503", &agent.HTTPError{Status: 503}, "http_503"},
		{"http_409", &agent.HTTPError{Status: 409}, "http_409"},
		{"http_425", &agent.HTTPError{Status: 425}, "http_425"},
		{"http_other", &agent.HTTPError{Status: 500}, "http_error"},
		{"timeout", fmt.Errorf("do request: %w", context.DeadlineExceeded), "timeout"},
		{"canceled", fmt.Errorf("do request: %w", context.Canceled), "timeout"},
		{"other", errors.New("decode boom"), "error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, agent.SubmitOutcome(tc.err))
		})
	}
}

func TestHTTPError_CarriesStatus(t *testing.T) {
	err := &agent.HTTPError{Status: 409, Body: "turn in flight"}
	require.Contains(t, err.Error(), "409")

	var he *agent.HTTPError
	require.True(t, errors.As(error(err), &he))
	require.Equal(t, 409, he.Status)
}

// Compile-time assertion that *httpSession satisfies Session is in session.go;
// here we only assert the interface and TurnResult shape are present.
func TestTurnResult_Fields(t *testing.T) {
	tr := agent.TurnResult{State: "completed", FinalText: "done", StopReason: "end_turn", Err: ""}
	require.Equal(t, "completed", tr.State)
	require.Equal(t, "done", tr.FinalText)
	require.Equal(t, "end_turn", tr.StopReason)
}
