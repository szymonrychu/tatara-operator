package scm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func boolPtr(b bool) *bool { return &b }

// stubMergeStateDelay replaces mergeStateRecomputeDelay with 0 so tests that
// exercise the lazy-recompute path do not sleep for the production 2s delay.
func stubMergeStateDelay(t *testing.T) {
	t.Helper()
	orig := mergeStateRecomputeDelay
	mergeStateRecomputeDelay = 0
	t.Cleanup(func() { mergeStateRecomputeDelay = orig })
}

func TestGitHubGetMergeState(t *testing.T) {
	cases := []struct {
		name           string
		mergeable      *bool
		mergeableState string
		want           MergeState
	}{
		{"clean", boolPtr(true), "clean", MergeStateClean},
		{"dirty", boolPtr(false), "dirty", MergeStateDirty},
		{"behind", boolPtr(true), "behind", MergeStateBehind},
		{"blocked", boolPtr(false), "blocked", MergeStateBlocked},
		{"draft", boolPtr(false), "draft", MergeStateBlocked},
		{"unstable_is_clean", boolPtr(true), "unstable", MergeStateClean},
		{"has_hooks_is_clean", boolPtr(true), "has_hooks", MergeStateClean},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"mergeable":       tc.mergeable,
					"mergeable_state": tc.mergeableState,
				})
			}))
			defer srv.Close()
			c := &GitHub{apiBase: srv.URL}
			got, err := c.GetMergeState(context.Background(), "https://github.com/o/r", "tok", 7)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestGitHubGetMergeState_LazyRecompute(t *testing.T) {
	stubMergeStateDelay(t)
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{"mergeable": nil, "mergeable_state": "unknown"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"mergeable": true, "mergeable_state": "clean"})
	}))
	defer srv.Close()
	c := &GitHub{apiBase: srv.URL}
	got, err := c.GetMergeState(context.Background(), "https://github.com/o/r", "tok", 7)
	require.NoError(t, err)
	assert.Equal(t, MergeStateClean, got)
	assert.Equal(t, 2, calls, "must poll once more when mergeable is null")
}
