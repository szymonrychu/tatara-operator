package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// ---------------------------------------------------------------------------
// THE CI-DECLINE EVIDENCE IS SCOPED TO ONE REFUSAL AT ONE HEAD.
//
// AnnDeclineCI / AnnDeclineHeads describe ONE 409 pr-not-ready the readiness
// gate answered on the ci-red axis, and driveCIRecoveryUnparks treats their
// presence as a licence to un-park. Every OTHER park in the operator goes
// through ParkTask - the live-pod eviction (livepods.go), admission starvation,
// the podwatch paths, the merge and deploy timeouts - and until the clear
// existed each of those INHERITED whatever evidence an earlier refusal left, so
// a flake re-running green at the same head released a park that had nothing to
// do with CI.
// ---------------------------------------------------------------------------

// declineEvidenceTask is a Task carrying the evidence an earlier ci-red
// readiness refusal stamped, in a LIVE state with no park of its own yet.
func declineEvidenceTask(name, heads string) *tatarav1alpha1.Task {
	task := tsTask(name, "implement", tatarav1alpha1.StateUnderImplementation, time.Now().Add(-time.Hour))
	task.Annotations = map[string]string{
		tatarav1alpha1.AnnDeclineCI:    tatarav1alpha1.CIEvidenceRed,
		tatarav1alpha1.AnnDeclineHeads: heads,
	}
	return task
}

// A park written by a NON-CI path must not inherit the refusal's evidence.
// ParkTask is THE park choke point, so one clear there covers awaiting-human
// from the live-pod eviction, pod-recreation exhausted, admission-starved and
// every podwatch failure path at once.
func TestParkTask_ClearsTheCIDeclineEvidence(t *testing.T) {
	for _, reason := range []string{
		stage.ReasonAwaitingHuman, // the livepods.go:143 eviction, the sharpest case
		stage.ReasonAdmissionStarved,
		stage.ReasonPodRecreationExhausted,
	} {
		t.Run(reason, func(t *testing.T) {
			task := declineEvidenceTask("park-evidence-"+reason, "mr-containers-1281@4c11cad2")
			c := newMirrorClient(t, task)

			require.NoError(t, ParkTask(context.Background(), c, nil,
				obs.NewOperatorMetrics(prometheus.NewRegistry()),
				task, reason, time.Now(), nil))

			got := mdGetTask(t, c, task.Name)
			require.True(t, tatarav1alpha1.Parked(got), "the park itself must still land")
			require.NotContains(t, got.Annotations, tatarav1alpha1.AnnDeclineCI,
				"this park is not the refusal the evidence describes; keeping it licenses a spurious ci-recovery un-park")
			require.NotContains(t, got.Annotations, tatarav1alpha1.AnnDeclineHeads)
		})
	}
}

// The clear costs nothing on the overwhelmingly common park, which carries no
// evidence at all: the patch is skipped entirely rather than written as a no-op.
// PollOnce parks in a loop over every Task in the namespace.
func TestParkTask_WritesNothingWhenThereIsNoCIDeclineEvidence(t *testing.T) {
	task := tsTask("park-evidence-none", "implement", tatarav1alpha1.StateUnderImplementation,
		time.Now().Add(-time.Hour))
	c := newMirrorClient(t, task)
	before := mdGetTask(t, c, task.Name).ResourceVersion

	require.NoError(t, ParkTask(context.Background(), c, nil,
		obs.NewOperatorMetrics(prometheus.NewRegistry()),
		task, stage.ReasonAwaitingHuman, time.Now(), nil))

	got := mdGetTask(t, c, task.Name)
	require.True(t, tatarav1alpha1.Parked(got))
	require.NotEqual(t, before, got.ResourceVersion, "the status write still happened")
	require.Empty(t, got.Annotations, "no annotations existed, so no metadata write was needed")
}

// THE ORDERING THE CLEAR DEPENDS ON, asserted rather than assumed.
//
// The give-up verdicts - discuss and declined - park through
// restapi/outcome.go's o.commit, which is objbudget.FitTask + stage.Park and
// NEVER controller.ParkTask (outcome.go's own comments at the TaskParked and
// DeleteWrapper sites say so, and ParkTask has eight non-test callers, all of
// them controller-side). So the clear cannot run between a refusal and the
// give-up that inherits its evidence: this test reproduces the outcome-shaped
// park exactly and proves the evidence survives it into the recovery driver.
func TestOutcomeShapedPark_KeepsTheCIDeclineEvidenceForTheRecoveryDriver(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	proj := reapProject("cirec")
	tk := ciDeclinedTask("upgrade", tatarav1alpha1.CIEvidenceRed, ciRecoveryMR+"@4c11cad2")
	// Un-park it: the give-up has not happened yet, only the refusal that
	// stamped the evidence.
	tk.Status.ParkReason = ""
	tk.Status.ParkedAt = nil
	mr := ciRecoveryMirror("open", tatarav1alpha1.OwnershipTatara, "green", "4c11cad2")
	r := newUnparkTestReconciler(t, proj, reapSecret(), tk, mr)

	// EXACTLY what o.commit does for action=discuss and action=declined.
	var parkErr error
	require.NoError(t, objbudget.FitTask(ctx, r.Client, nil, objectKeyOf(tk),
		func(t *tatarav1alpha1.Task) { parkErr = stage.Park(t, stage.ReasonImplementDeclined, now) }))
	require.NoError(t, parkErr)

	parked := &tatarav1alpha1.Task{}
	require.NoError(t, r.Get(ctx, objectKeyOf(tk), parked))
	require.Equal(t, tatarav1alpha1.CIEvidenceRed, parked.Annotations[tatarav1alpha1.AnnDeclineCI],
		"the give-up path does not route through ParkTask, so its evidence is untouched")

	require.NoError(t, r.driveCIRecoveryUnparks(ctx, proj, now))
	got := &tatarav1alpha1.Task{}
	require.NoError(t, r.Get(ctx, objectKeyOf(tk), got))
	require.False(t, tatarav1alpha1.Parked(got),
		"the decline the infrastructure caused is still re-driven when the same head goes green")
}
