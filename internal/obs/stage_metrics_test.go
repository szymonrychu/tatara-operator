package obs

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// StageDriftTotal must be pre-seeded across every state so a healthy operator
// (0 drift, the expected steady state) still exposes a zero baseline per
// state from startup, matching the house seedLabels pattern - otherwise a
// sustained-rate alert added later has nothing to evaluate against until the
// first drift on that particular state (metric-wiring audit, issue #370).
//
// #521 collapsed the 16-stage machine into the 8-member state enum (park is
// now an orthogonal flag, not a stage), so the pre-seeded set is exactly
// stage.AllStates(), one series per state - not one per legacy stage.
func TestStageDriftTotal_PreSeeded(t *testing.T) {
	wantStages := []string{
		tatarav1alpha1.StateNew, tatarav1alpha1.StateRefined, tatarav1alpha1.StateUnderImplementation,
		tatarav1alpha1.StateAwaitingReview, tatarav1alpha1.StateMerged, tatarav1alpha1.StateDeployed,
		tatarav1alpha1.StateDone, tatarav1alpha1.StateRejected,
	}
	if got := testutil.CollectAndCount(StageDriftTotal); got != len(wantStages) {
		t.Errorf("operator_stage_drift_total has %d series, want %d (pre-seeded per state)", got, len(wantStages))
	}
}
