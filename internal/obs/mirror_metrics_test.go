package obs

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// MirrorWriteDroppedTotal is incremented at every best-effort mirror/webhook
// drop site (see mirror_metrics.go's doc comment for why these drops happen
// and why the caller still returns 200). Table-driven over a representative
// slice of the real (project, kind, site) tuples wired in internal/webhook
// and internal/restapi, so a site's label values drifting (typo, wrong kind)
// fails here instead of only showing up as a missing series in production.
func TestMirrorWriteDroppedTotal_IncrementsPerSite(t *testing.T) {
	tests := []struct {
		name    string
		project string
		kind    string
		site    string
	}{
		{"issue_body_title", "proj-a", "Issue", "issue_body_title"},
		{"issue_state", "proj-a", "Issue", "issue_state"},
		{"mr_refresh", "proj-a", "MergeRequest", "mr_refresh"},
		{"comment_append_issue", "proj-a", "issue_comment", "comment_append"},
		{"comment_append_mr", "proj-a", "mr_comment", "comment_append"},
		{"incident_refire", "proj-b", "Issue", "incident_refire"},
		{"incident_escalate", "proj-b", "Issue", "incident_escalate"},
		{"record_bot_head", "proj-b", "MergeRequest", "record_bot_head"},
		{"incident_cooldown_reset", "proj-b", "Issue", "incident_cooldown_reset"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := testutil.ToFloat64(MirrorWriteDroppedTotal.WithLabelValues(tt.project, tt.kind, tt.site))
			MirrorWriteDroppedTotal.WithLabelValues(tt.project, tt.kind, tt.site).Inc()
			after := testutil.ToFloat64(MirrorWriteDroppedTotal.WithLabelValues(tt.project, tt.kind, tt.site))
			if after-before != 1 {
				t.Fatalf("project=%s kind=%s site=%s: want +1, got %v -> %v", tt.project, tt.kind, tt.site, before, after)
			}
		})
	}
}

// The metric must be gathered under its documented name and label order - a
// rename here without updating a dashboard/alert query is exactly the
// silent-drift failure mode this suite exists to catch.
func TestMirrorWriteDroppedTotal_GatheredUnderName(t *testing.T) {
	MirrorWriteDroppedTotal.WithLabelValues("proj-gather", "Issue", "issue_state").Inc()
	got := gatheredLabelNames(t, MirrorWriteDroppedTotal, "operator_mirror_write_dropped_total")
	// Prometheus gathers label pairs sorted alphabetically by name, not
	// declaration order (kind, project, site - not project, kind, site).
	want := []string{"kind", "project", "site"}
	if len(got) != len(want) {
		t.Fatalf("label names = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("label names = %v, want %v", got, want)
		}
	}
}
