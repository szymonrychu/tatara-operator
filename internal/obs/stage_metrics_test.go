package obs

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// StageDriftTotal must be pre-seeded across every stage so a healthy operator
// (0 drift, the expected steady state) still exposes a zero baseline per
// stage from startup, matching the house seedLabels pattern - otherwise a
// sustained-rate alert added later has nothing to evaluate against until the
// first drift on that particular stage (metric-wiring audit, issue #370).
func TestStageDriftTotal_PreSeeded(t *testing.T) {
	wantStages := []string{
		tatarav1alpha1.StageTriaging, tatarav1alpha1.StageBrainstorming, tatarav1alpha1.StageClarifying,
		tatarav1alpha1.StageInvestigating, tatarav1alpha1.StageRefining, tatarav1alpha1.StageApproved,
		tatarav1alpha1.StageImplementing, tatarav1alpha1.StageReviewing, tatarav1alpha1.StageMerging,
		tatarav1alpha1.StageDeploying, tatarav1alpha1.StageDelivered, tatarav1alpha1.StageDocumenting,
		tatarav1alpha1.StageRejected, tatarav1alpha1.StageFailed, tatarav1alpha1.StageParked,
	}
	if got := testutil.CollectAndCount(StageDriftTotal); got != len(wantStages) {
		t.Errorf("operator_stage_drift_total has %d series, want %d (pre-seeded per stage)", got, len(wantStages))
	}
}
