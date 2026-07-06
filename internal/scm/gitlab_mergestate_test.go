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
		})
	}
}
