package agent_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/szymonrychu/tatara-operator/internal/agent"
)

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
