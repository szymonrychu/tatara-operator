package scm

import (
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
