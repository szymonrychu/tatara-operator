package obs

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// SweepErrorsTotal must be pre-seeded across the full (activity x reason)
// closed set at startup, matching the house seedLabels pattern used for
// webhookEvents/authTotal/restapiRequestsTotal/etc - otherwise a healthy
// sweep with zero errors shows literally no series, and the
// TataraSweepErrors alert has no zero-baseline to evaluate against on the
// first real error (metric-wiring audit, issue #370).
func TestSweepErrorsTotal_PreSeeded(t *testing.T) {
	const wantActivities = 2 // SweepActivity, SweepNightlyActivity
	const wantReasons = 13   // the closed fail(reason, ...) set in sweep.go, plus list_tasks
	if got := testutil.CollectAndCount(SweepErrorsTotal); got != wantActivities*wantReasons {
		t.Errorf("operator_sweep_errors_total has %d series, want %d (pre-seeded activity x reason)",
			got, wantActivities*wantReasons)
	}
}
