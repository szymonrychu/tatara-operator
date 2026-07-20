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
//
// Two independent seedLabels calls contribute to the total: the original
// sweep/nightlySweep x 13-reason set, and the brainstorm/documentation/
// issueScan x 6-reason set added for the refine-barrier stall fix (issue
// #401, projectscan.go's runScans cron activities).
func TestSweepErrorsTotal_PreSeeded(t *testing.T) {
	const sweepActivities = 2 // SweepActivity, SweepNightlyActivity
	const sweepReasons = 13   // the closed fail(reason, ...) set in sweep.go, plus list_tasks
	const scanActivities = 3  // brainstorm, documentation, issueScan
	const scanReasons = 6     // refine_barrier_held/_timeout, refine_(inflight_)check_failed, invalid_cron, stamp_failed
	want := sweepActivities*sweepReasons + scanActivities*scanReasons
	if got := testutil.CollectAndCount(SweepErrorsTotal); got != want {
		t.Errorf("operator_sweep_errors_total has %d series, want %d (pre-seeded activity x reason, both seedLabels calls)",
			got, want)
	}
}
