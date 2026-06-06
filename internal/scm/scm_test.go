package scm

import (
	"context"
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

func TestM5MethodsNotImplemented(t *testing.T) {
	ctx := context.Background()
	for _, c := range []Client{&GitHub{}, &GitLab{}} {
		_, err := c.OpenChange(ctx, "https://x/r", "tok", "src", "dst", "t", "b")
		require.ErrorContains(t, err, "not implemented")
		require.ErrorContains(t, c.Comment(ctx, "tok", "o/r#1", "b"), "not implemented")
	}
}

func TestWebhookEventZeroValue(t *testing.T) {
	var e WebhookEvent
	require.Equal(t, "", e.Kind)
	require.Nil(t, e.Labels)
	_ = http.Header{}
}
