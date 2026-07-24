package scm

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClientsSatisfyInterface(t *testing.T) {
	var _ Client = (*GitHub)(nil)
	var _ Client = (*GitLab)(nil)
}

func TestProviders(t *testing.T) {
	require.Equal(t, "github", (&GitHub{}).Provider())
	require.Equal(t, "gitlab", (&GitLab{}).Provider())
}

// TestM5MethodsNotImplemented was removed in M5: OpenChange and Comment are
// now fully implemented for both providers; the stub assertion is no longer valid.

func TestWebhookEventZeroValue(t *testing.T) {
	var e WebhookEvent
	require.Equal(t, "", e.Kind)
	require.Nil(t, e.Labels)
	_ = http.Header{}
}

func TestHTTPErrorBodyTruncated(t *testing.T) {
	// build a 600-char body to exceed the 200-char truncation limit
	buf := make([]byte, 600)
	for i := range buf {
		buf[i] = 'x'
	}
	longBody := string(buf)

	e := &HTTPError{Status: 422, Path: "/repos/owner/repo/pulls", Body: longBody}
	msg := e.Error()

	// full body must not appear verbatim in the error string
	require.NotContains(t, msg, longBody, "Error() must not emit the full body")
	// but the path and status must still be present
	require.Contains(t, msg, "/repos/owner/repo/pulls")
	require.Contains(t, msg, "422")
}

func TestHTTPErrorShortBodyUnchanged(t *testing.T) {
	e := &HTTPError{Status: 404, Path: "/repos/owner/repo", Body: "not found"}
	msg := e.Error()
	require.Contains(t, msg, "not found")
	require.Contains(t, msg, "404")
}

func TestIssueState_ClosedField(t *testing.T) {
	st := IssueState{Author: "alice", Closed: true}
	if !st.Closed {
		t.Fatal("IssueState.Closed must round-trip true")
	}
}

func TestErrorStatus(t *testing.T) {
	require.Equal(t, "", ErrorStatus(nil))
	require.Equal(t, "401", ErrorStatus(&HTTPError{Status: 401, Path: "/x"}))
	require.Equal(t, "429", ErrorStatus(fmt.Errorf("wrapped: %w", &HTTPError{Status: 429, Path: "/x"})))
	require.Equal(t, "network", ErrorStatus(errors.New("dial tcp: connection refused")))
}

func TestIsNotFound(t *testing.T) {
	require.False(t, IsNotFound(nil))
	require.False(t, IsNotFound(errors.New("dial tcp: connection refused")))
	require.False(t, IsNotFound(&HTTPError{Status: 500, Path: "/x"}))
	require.True(t, IsNotFound(&HTTPError{Status: 404, Path: "/x"}))
	require.True(t, IsNotFound(fmt.Errorf("wrapped: %w", &HTTPError{Status: 404, Path: "/x"})))
}
