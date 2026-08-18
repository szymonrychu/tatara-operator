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
		// unstable is NOT clean: it is GitHub saying "mergeable, and the red
		// checks are not the ones that block it". Folding it into clean loses
		// exactly the bit the readiness gate needs to let a repo whose only red
		// check is a non-required scanner ship at all.
		{"unstable_is_mergeable_with_red_non_required_checks", boolPtr(true), "unstable", MergeStateUnstable},
		// has_hooks is a clean merge behind a pre-receive hook, not a red check.
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

// THE REQUIRED-CONTEXT READ IS GRAPHQL, NOT THE PROTECTION REST ENDPOINT.
// GET /repos/{o}/{r}/branches/{b}/protection/required_status_checks needs ADMIN
// on the repo; the operator's bot token has push, not admin, so that endpoint
// would 403 on every repo and - failing closed - would make the suppression
// permanently dead code. statusCheckRollup's per-context isRequired flag is
// readable with plain read access and is what `gh pr checks --required` uses.
func TestGitHubHasRequiredChecks(t *testing.T) {
	// rollup renders the GraphQL data envelope for a set of isRequired flags.
	rollup := func(required ...bool) string {
		nodes := make([]map[string]any, 0, len(required))
		for _, r := range required {
			nodes = append(nodes, map[string]any{"isRequired": r})
		}
		b, err := json.Marshal(map[string]any{"data": map[string]any{
			"repository": map[string]any{"pullRequest": map[string]any{
				"statusCheckRollup": map[string]any{"contexts": map[string]any{"nodes": nodes}},
			}},
		}})
		require.NoError(t, err)
		return string(b)
	}
	cases := []struct {
		name string
		body string
		want bool
	}{
		{"one required context among several is enough", rollup(false, true, false), true},
		{"every context non-required is an unprotected repo", rollup(false, false), false},
		{"no contexts at all", rollup(), false},
		{"a null rollup is no contexts", `{"data":{"repository":{"pullRequest":{"statusCheckRollup":null}}}}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotQuery map[string]any
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewDecoder(r.Body).Decode(&gotQuery)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			c := &GitHub{apiBase: "unused", graphQLBase: srv.URL}
			got, err := c.HasRequiredChecks(context.Background(), "https://github.com/o/r", "tok", 424)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
			vars, _ := gotQuery["variables"].(map[string]any)
			assert.Equal(t, float64(424), vars["number"],
				"isRequired is per-pull-request: the PR number is part of the question, not just the lookup")
		})
	}
}

// A GraphQL failure is returned, never swallowed into a false "no required
// contexts" - the caller fails CLOSED on the error, and a silent false would be
// indistinguishable from an unprotected repo.
func TestGitHubHasRequiredChecks_ErrorsSurface(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"message":"Resource not accessible by integration"}`))
	}))
	defer srv.Close()
	c := &GitHub{apiBase: "unused", graphQLBase: srv.URL}
	_, err := c.HasRequiredChecks(context.Background(), "https://github.com/o/r", "tok", 7)
	require.Error(t, err)
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
