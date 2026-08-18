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

func TestGitLabGetMergeState(t *testing.T) {
	cases := []struct {
		name         string
		mergeStatus  string
		hasConflicts bool
		want         MergeState
	}{
		{"clean", "can_be_merged", false, MergeStateClean},
		{"dirty", "cannot_be_merged", true, MergeStateDirty},
		{"blocked_no_conflict", "cannot_be_merged", false, MergeStateBlocked},
		{"recheck_conflict", "cannot_be_merged_recheck", true, MergeStateDirty},
		{"unchecked", "unchecked", false, MergeStateUnknown},
		{"checking", "checking", false, MergeStateUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"merge_status":  tc.mergeStatus,
					"has_conflicts": tc.hasConflicts,
				})
			}))
			defer srv.Close()
			c := &GitLab{apiBase: srv.URL}
			got, err := c.GetMergeState(context.Background(), "https://gitlab.com/o/r", "tok", 7)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
			// THE C-2 PIN. GitLab's can_be_merged is true even when the pipeline
			// is red, so it can never be allowed to mean "the red checks do not
			// block this merge". No merge_status may ever map to Unstable, or
			// every GitLab submit silently stops honouring a red pipeline.
			assert.NotEqual(t, MergeStateUnstable, got,
				"gitlab has no mergeable-with-red-non-required-checks signal")
		})
	}
}

// THE SECOND HALF OF THE C-2 PIN. GitLab must also never grow the required-
// context capability by accident: RequiredCheckLister is an OPTIONAL interface,
// so a method added to *GitLab with the right signature would silently arm the
// suppression on a provider whose can_be_merged already means "mergeable with a
// red pipeline". Both halves have to stay false for GitLab to stay fail-closed.
func TestGitLabNeverSuppressesRedCI(t *testing.T) {
	var w SCMWriter = &GitLab{apiBase: "unused"}
	_, ok := w.(RequiredCheckLister)
	assert.False(t, ok, "gitlab must not answer the required-context question")

	for _, ms := range []MergeState{MergeStateClean, MergeStateBlocked, MergeStateDirty,
		MergeStateBehind, MergeStateUnknown, MergeStateUnstable} {
		got, err := CIRedSuppressed(context.Background(), w, "https://gitlab.com/o/r", "tok", 7, ms)
		require.NoError(t, err)
		assert.False(t, got, "state %s must never suppress red CI on gitlab", ms)
	}
}
