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
