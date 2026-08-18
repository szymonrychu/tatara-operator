package controller

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// ---------------------------------------------------------------------------
// The CI-recovery un-park: a decline the INFRASTRUCTURE caused, re-driven once
// the infrastructure recovered. containers!1281 is the measured shape.
// ---------------------------------------------------------------------------

const ciRecoveryMR = "mr-containers-1281"

// ciDeclinedTask is a Task parked implement-declined that OWNS one merge
// request, carrying the decline-time evidence the outcome endpoint stamps.
func ciDeclinedTask(kind, declineCI, declineHeads string) *tatarav1alpha1.Task {
	now := time.Now()
	t := reapTask("cirec", "t-declined", kind, tatarav1alpha1.StateUnderImplementation,
		stage.ReasonImplementDeclined, now.Add(-2*time.Hour))
	parkedAt := metav1.NewTime(now.Add(-time.Hour))
	t.Status.ParkedAt = &parkedAt
	t.Status.ParkedFromState = tatarav1alpha1.StateUnderImplementation
	t.Status.MRRefs = []string{ciRecoveryMR}
	t.Annotations = map[string]string{}
	if declineCI != "" {
		t.Annotations[tatarav1alpha1.AnnDeclineCI] = declineCI
	}
	if declineHeads != "" {
		t.Annotations[tatarav1alpha1.AnnDeclineHeads] = declineHeads
	}
	return t
}

// ciRecoveryMirror is the owned MergeRequest mirror as it reads NOW.
func ciRecoveryMirror(state, ownership, ci, head string) *tatarav1alpha1.MergeRequest {
	mr := &tatarav1alpha1.MergeRequest{}
	mr.Namespace = testNS
	mr.Name = ciRecoveryMR
	mr.OwnerReferences = []metav1.OwnerReference{reapOwnerRef("t-declined", true)}
	mr.Spec = tatarav1alpha1.MergeRequestSpec{RepositoryRef: "containers", Number: 1281, ProjectRef: "cirec"}
	mr.Status.State = state
	mr.Status.Ownership = ownership
	mr.Status.CIStatus = ci
	mr.Status.HeadSHA = head
	return mr
}

// THE WHOLE POINT, IN ONE TEST. The agent declined because it could not submit
// - the operator's own readiness gate answered 409 ci-red - and the merge
// request tatara owns end to end has since gone green AT THE SAME HEAD. The
// operator holds the proof its own decline is void; this is what reads it.
func TestDriveCIRecoveryUnparks_RedeliversADeclineTheInfrastructureCaused(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	proj := reapProject("cirec")
	tk := ciDeclinedTask("upgrade", tatarav1alpha1.CIEvidenceRed, ciRecoveryMR+"@4c11cad2")
	mr := ciRecoveryMirror("open", tatarav1alpha1.OwnershipTatara, "green", "4c11cad2")
	r := newUnparkTestReconciler(t, proj, reapSecret(), tk, mr)
	counter := r.Metrics.TaskUnparkedCounter(stage.ReasonImplementDeclined, obs.UnparkClassCIRecovered)

	require.NoError(t, r.driveCIRecoveryUnparks(ctx, proj, now))

	got := &tatarav1alpha1.Task{}
	require.NoError(t, r.Get(ctx, objectKeyOf(tk), got))
	require.False(t, tatarav1alpha1.Parked(got), "the decline was void and must be released")
	require.Equal(t, tatarav1alpha1.StateUnderImplementation, got.Status.State,
		"the un-park is IN PLACE: the Task returns to the implementation flow it parked in")
	require.Nil(t, got.Status.PodStartedAt, "the re-arm must clear the pod clocks")
	require.Nil(t, got.Status.StateWorkStartedAt)
	require.Equal(t, "1", got.Annotations[tatarav1alpha1.AnnCIRecoveryUnparks],
		"the absolute ceiling is charged one lap")
	require.Equal(t, ciRecoveryMR+"@4c11cad2", got.Annotations[tatarav1alpha1.AnnCIRecoveryHeads],
		"the head latch records WHICH head this fired for")
	require.Equal(t, float64(1), testutil.ToFloat64(counter))
}

// A DECLINE ON THE MERITS IS PERMANENT AND STAYS PERMANENT. helmfile!1407 was
// declined because main had already moved past it and the merge request carried
// zero net change; its CI was never the problem. Re-driving it would re-open a
// settled decision, which is the one thing this driver must never do.
func TestDriveCIRecoveryUnparks_NeverRedrivesADeclineOnTheMerits(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	proj := reapProject("cirec")

	for _, declineCI := range []string{
		tatarav1alpha1.CIEvidenceGreen,
		tatarav1alpha1.CIEvidenceUnknown,
		"", // no evidence at all: a Task parked by a build predating the stamp
	} {
		t.Run("declined at ci="+declineCI, func(t *testing.T) {
			tk := ciDeclinedTask("implement", declineCI, ciRecoveryMR+"@4c11cad2")
			mr := ciRecoveryMirror("open", tatarav1alpha1.OwnershipTatara, "green", "4c11cad2")
			r := newUnparkTestReconciler(t, proj, reapSecret(), tk, mr)

			require.NoError(t, r.driveCIRecoveryUnparks(ctx, proj, now))

			got := &tatarav1alpha1.Task{}
			require.NoError(t, r.Get(ctx, objectKeyOf(tk), got))
			require.True(t, tatarav1alpha1.Parked(got),
				"only a decline made against RED ci is evidence of an infrastructure block")
			require.Equal(t, stage.ReasonImplementDeclined, got.Status.ParkReason)
			require.NotContains(t, got.Annotations, tatarav1alpha1.AnnCIRecoveryUnparks)
		})
	}
}

// Everything that must hold at the mirror, one refusal per row. Each one is a
// different way of asking "is this still tatara's own, still open, and green".
func TestDriveCIRecoveryUnparks_RefusesEveryMirrorThatIsNotOursGreenAndOpen(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	proj := reapProject("cirec")
	declined := ciRecoveryMR + "@4c11cad2"

	for _, tc := range []struct {
		name string
		mr   *tatarav1alpha1.MergeRequest
	}{
		{"ownership external is a human's merge request and is never touched",
			ciRecoveryMirror("open", tatarav1alpha1.OwnershipExternal, "green", "4c11cad2")},
		{"ownership unclassified",
			ciRecoveryMirror("open", "", "green", "4c11cad2")},
		{"the merge request closed",
			ciRecoveryMirror("closed", tatarav1alpha1.OwnershipTatara, "green", "4c11cad2")},
		{"the merge request merged",
			ciRecoveryMirror("merged", tatarav1alpha1.OwnershipTatara, "green", "4c11cad2")},
		{"ci is still red",
			ciRecoveryMirror("open", tatarav1alpha1.OwnershipTatara, "red", "4c11cad2")},
		{"ci has no verdict yet",
			ciRecoveryMirror("open", tatarav1alpha1.OwnershipTatara, "pending", "4c11cad2")},
		{"the head moved: the green is about different code",
			ciRecoveryMirror("open", tatarav1alpha1.OwnershipTatara, "green", "99999999")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tk := ciDeclinedTask("upgrade", tatarav1alpha1.CIEvidenceRed, declined)
			r := newUnparkTestReconciler(t, proj, reapSecret(), tk, tc.mr)

			require.NoError(t, r.driveCIRecoveryUnparks(ctx, proj, now))

			got := &tatarav1alpha1.Task{}
			require.NoError(t, r.Get(ctx, objectKeyOf(tk), got))
			require.True(t, tatarav1alpha1.Parked(got))
			require.Equal(t, stage.ReasonImplementDeclined, got.Status.ParkReason)
		})
	}
}

// A TAKEOVER'S implement-declined IS NOT A CI VERDICT. On kind=takeover it is
// the agent's LOCAL-GIT name for a human push (an ls-remote mismatch, a
// non-fast-forward rejection); stage.UpgradeDeclineToOwnershipLost owns that
// reason there. Re-driving it would put an agent back on a branch its author
// has taken back, in the window before the ownership flip lands.
func TestDriveCIRecoveryUnparks_NeverTouchesATakeover(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	proj := reapProject("cirec")
	tk := ciDeclinedTask(stage.KindTakeover, tatarav1alpha1.CIEvidenceRed, ciRecoveryMR+"@4c11cad2")
	mr := ciRecoveryMirror("open", tatarav1alpha1.OwnershipTatara, "green", "4c11cad2")
	r := newUnparkTestReconciler(t, proj, reapSecret(), tk, mr)

	require.NoError(t, r.driveCIRecoveryUnparks(ctx, proj, now))

	got := &tatarav1alpha1.Task{}
	require.NoError(t, r.Get(ctx, objectKeyOf(tk), got))
	require.True(t, tatarav1alpha1.Parked(got))
	require.Equal(t, stage.ReasonImplementDeclined, got.Status.ParkReason)
}

// The driver is NARROW on the park reason for the same reason the O3 migration
// is: releasing anything else here would be an arbitrary un-park wearing a
// recovery's name.
func TestDriveCIRecoveryUnparks_IgnoresEveryOtherParkReason(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	proj := reapProject("cirec")

	// awaiting-human is NOT in this list any more: since the discuss verdict
	// stamps the same evidence, an awaiting-human park that carries it is the
	// second case this driver owns. The evidence, not the reason, is what keeps
	// it narrow - see TestDriveCIRecoveryUnparks_LeavesAnEvidencelessAwaitingHumanParkAlone.
	for _, reason := range []string{
		stage.ReasonCIRed, stage.ReasonCIBlocked, stage.ReasonMergeBlocked,
		stage.ReasonOwnershipLost,
	} {
		t.Run(reason, func(t *testing.T) {
			tk := ciDeclinedTask("implement", tatarav1alpha1.CIEvidenceRed, ciRecoveryMR+"@4c11cad2")
			tk.Status.ParkReason = reason
			mr := ciRecoveryMirror("open", tatarav1alpha1.OwnershipTatara, "green", "4c11cad2")
			r := newUnparkTestReconciler(t, proj, reapSecret(), tk, mr)

			require.NoError(t, r.driveCIRecoveryUnparks(ctx, proj, now))

			got := &tatarav1alpha1.Task{}
			require.NoError(t, r.Get(ctx, objectKeyOf(tk), got))
			require.True(t, tatarav1alpha1.Parked(got))
			require.Equal(t, reason, got.Status.ParkReason)
		})
	}
}

// THE PER-HEAD LATCH. A second decline at the SAME head, with CI red again (a
// re-run that flipped green -> red -> green), must NOT fire a second time: that
// is the ping-pong this bound exists to make impossible.
func TestDriveCIRecoveryUnparks_AtMostOncePerHead(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	proj := reapProject("cirec")
	declined := ciRecoveryMR + "@4c11cad2"
	tk := ciDeclinedTask("upgrade", tatarav1alpha1.CIEvidenceRed, declined)
	mr := ciRecoveryMirror("open", tatarav1alpha1.OwnershipTatara, "green", "4c11cad2")
	r := newUnparkTestReconciler(t, proj, reapSecret(), tk, mr)

	require.NoError(t, r.driveCIRecoveryUnparks(ctx, proj, now))
	got := &tatarav1alpha1.Task{}
	require.NoError(t, r.Get(ctx, objectKeyOf(tk), got))
	require.False(t, tatarav1alpha1.Parked(got))

	// RE-PARK at the same head, with the same red evidence, exactly as a second
	// declining turn would.
	got.Status.ParkReason = stage.ReasonImplementDeclined
	parkedAt := metav1.NewTime(now)
	got.Status.ParkedAt = &parkedAt
	require.NoError(t, r.Status().Update(ctx, got))

	require.NoError(t, r.driveCIRecoveryUnparks(ctx, proj, now))

	again := &tatarav1alpha1.Task{}
	require.NoError(t, r.Get(ctx, objectKeyOf(tk), again))
	require.True(t, tatarav1alpha1.Parked(again),
		"a head already re-driven once is spent; it ages out at ParkRetention")
	require.Equal(t, "1", again.Annotations[tatarav1alpha1.AnnCIRecoveryUnparks],
		"a refused pass must not charge the ceiling")
}

// A FRESH PUSH IS A GENUINELY NEW SITUATION and gets its own lap - up to the
// absolute ceiling, which is what stops a Task that keeps declining from
// ping-ponging forever. The ceiling is charged per FIRING, never per pass.
func TestDriveCIRecoveryUnparks_TheCeilingIsAbsolute(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	proj := reapProject("cirec")

	for _, tc := range []struct {
		name      string
		spent     int
		wantFired bool
	}{
		{"one lap left", tatarav1alpha1.MaxCIRecoveryUnparks - 1, true},
		{"exhausted", tatarav1alpha1.MaxCIRecoveryUnparks, false},
		{"over-spent by an older build", tatarav1alpha1.MaxCIRecoveryUnparks + 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tk := ciDeclinedTask("upgrade", tatarav1alpha1.CIEvidenceRed, ciRecoveryMR+"@newhead1")
			tk.Annotations[tatarav1alpha1.AnnCIRecoveryUnparks] = strconv.Itoa(tc.spent)
			tk.Annotations[tatarav1alpha1.AnnCIRecoveryHeads] = ciRecoveryMR + "@oldhead0"
			mr := ciRecoveryMirror("open", tatarav1alpha1.OwnershipTatara, "green", "newhead1")
			r := newUnparkTestReconciler(t, proj, reapSecret(), tk, mr)

			require.NoError(t, r.driveCIRecoveryUnparks(ctx, proj, now))

			got := &tatarav1alpha1.Task{}
			require.NoError(t, r.Get(ctx, objectKeyOf(tk), got))
			require.Equal(t, !tc.wantFired, tatarav1alpha1.Parked(got))
			if !tc.wantFired {
				require.Equal(t, strconv.Itoa(tc.spent), got.Annotations[tatarav1alpha1.AnnCIRecoveryUnparks],
					"an exhausted bound stops the driver and charges nothing further")
				require.Equal(t, ciRecoveryMR+"@oldhead0", got.Annotations[tatarav1alpha1.AnnCIRecoveryHeads])
			}
		})
	}
}

// THE SECOND SHAPE THE DRIVER OWNS. An agent that cannot submit because the
// readiness gate answered 409 ci-red may decline OR ask a human, and asking is
// the better answer - but it parks awaiting-human, and until the discuss verdict
// stamped the same evidence the better answer was the one that could never be
// re-driven. Same five conditions, same annotations, one more park reason.
func TestDriveCIRecoveryUnparks_ReleasesAnAwaitingHumanParkWithRedCIEvidence(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	proj := reapProject("cirec")
	tk := ciDeclinedTask("implement", tatarav1alpha1.CIEvidenceRed, ciRecoveryMR+"@4c11cad2")
	tk.Status.ParkReason = stage.ReasonAwaitingHuman
	mr := ciRecoveryMirror("open", tatarav1alpha1.OwnershipTatara, "green", "4c11cad2")
	r := newUnparkTestReconciler(t, proj, reapSecret(), tk, mr)
	counter := r.Metrics.TaskUnparkedCounter(stage.ReasonAwaitingHuman, obs.UnparkClassCIRecovered)

	require.NoError(t, r.driveCIRecoveryUnparks(ctx, proj, now))

	got := &tatarav1alpha1.Task{}
	require.NoError(t, r.Get(ctx, objectKeyOf(tk), got))
	require.False(t, tatarav1alpha1.Parked(got), "the blocker the agent asked about has cleared itself")
	require.Equal(t, tatarav1alpha1.StateUnderImplementation, got.Status.State)
	require.Equal(t, "1", got.Annotations[tatarav1alpha1.AnnCIRecoveryUnparks])
	require.Equal(t, float64(1), testutil.ToFloat64(counter),
		"the metric must name the reason that was actually released, not a literal")
}

// THE SAFETY PIN, and the reason widening the reason filter is not a widening of
// the driver. awaiting-human is by far the most common park on the platform and
// almost all of it is a genuine question for a human. Only the discuss and
// decline commits stamp AnnDeclineCI, so a park with no evidence - which is every
// ordinary "I asked a maintainer something" - is untouched no matter how green
// its merge requests are.
func TestDriveCIRecoveryUnparks_LeavesAnEvidencelessAwaitingHumanParkAlone(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	proj := reapProject("cirec")

	for _, tc := range []struct {
		name      string
		ci, heads string
	}{
		{"no evidence at all: an ordinary question for a human", "", ""},
		{"evidence says ci was green: the question was never about ci",
			tatarav1alpha1.CIEvidenceGreen, ciRecoveryMR + "@4c11cad2"},
		{"evidence says ci had no verdict yet",
			tatarav1alpha1.CIEvidenceUnknown, ciRecoveryMR + "@4c11cad2"},
		{"a red with no heads is not a fingerprint and cannot be matched",
			tatarav1alpha1.CIEvidenceRed, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tk := ciDeclinedTask("implement", tc.ci, tc.heads)
			tk.Status.ParkReason = stage.ReasonAwaitingHuman
			mr := ciRecoveryMirror("open", tatarav1alpha1.OwnershipTatara, "green", "4c11cad2")
			r := newUnparkTestReconciler(t, proj, reapSecret(), tk, mr)

			require.NoError(t, r.driveCIRecoveryUnparks(ctx, proj, now))

			got := &tatarav1alpha1.Task{}
			require.NoError(t, r.Get(ctx, objectKeyOf(tk), got))
			require.True(t, tatarav1alpha1.Parked(got),
				"a human is still owed an answer and nothing here proves otherwise")
			require.Equal(t, stage.ReasonAwaitingHuman, got.Status.ParkReason)
			require.NotContains(t, got.Annotations, tatarav1alpha1.AnnCIRecoveryUnparks)
		})
	}
}

// The bound is the SAME bound. A widened reason filter that shipped its own
// unlatched lane would be a second ping-pong nobody counted.
func TestDriveCIRecoveryUnparks_StillRespectsTheHeadLatchAndTheCapOnAwaitingHuman(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	proj := reapProject("cirec")

	t.Run("the head latch refuses a second lap at the same head", func(t *testing.T) {
		declined := ciRecoveryMR + "@4c11cad2"
		tk := ciDeclinedTask("implement", tatarav1alpha1.CIEvidenceRed, declined)
		tk.Status.ParkReason = stage.ReasonAwaitingHuman
		tk.Annotations[tatarav1alpha1.AnnCIRecoveryHeads] = declined
		tk.Annotations[tatarav1alpha1.AnnCIRecoveryUnparks] = "1"
		mr := ciRecoveryMirror("open", tatarav1alpha1.OwnershipTatara, "green", "4c11cad2")
		r := newUnparkTestReconciler(t, proj, reapSecret(), tk, mr)

		require.NoError(t, r.driveCIRecoveryUnparks(ctx, proj, now))

		got := &tatarav1alpha1.Task{}
		require.NoError(t, r.Get(ctx, objectKeyOf(tk), got))
		require.True(t, tatarav1alpha1.Parked(got))
		require.Equal(t, "1", got.Annotations[tatarav1alpha1.AnnCIRecoveryUnparks])
	})

	t.Run("the absolute ceiling refuses a fresh head once it is spent", func(t *testing.T) {
		tk := ciDeclinedTask("implement", tatarav1alpha1.CIEvidenceRed, ciRecoveryMR+"@newhead1")
		tk.Status.ParkReason = stage.ReasonAwaitingHuman
		tk.Annotations[tatarav1alpha1.AnnCIRecoveryHeads] = ciRecoveryMR + "@oldhead0"
		tk.Annotations[tatarav1alpha1.AnnCIRecoveryUnparks] = strconv.Itoa(tatarav1alpha1.MaxCIRecoveryUnparks)
		mr := ciRecoveryMirror("open", tatarav1alpha1.OwnershipTatara, "green", "newhead1")
		r := newUnparkTestReconciler(t, proj, reapSecret(), tk, mr)

		require.NoError(t, r.driveCIRecoveryUnparks(ctx, proj, now))

		got := &tatarav1alpha1.Task{}
		require.NoError(t, r.Get(ctx, objectKeyOf(tk), got))
		require.True(t, tatarav1alpha1.Parked(got))
		require.Equal(t, stage.ReasonAwaitingHuman, got.Status.ParkReason)
	})
}

// A TAKEOVER stays excluded on the new reason too: the exclusion is on the Task
// kind, not on the reason it happens to be parked with.
func TestDriveCIRecoveryUnparks_NeverTouchesATakeoverParkedAwaitingHuman(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	proj := reapProject("cirec")
	tk := ciDeclinedTask(stage.KindTakeover, tatarav1alpha1.CIEvidenceRed, ciRecoveryMR+"@4c11cad2")
	tk.Status.ParkReason = stage.ReasonAwaitingHuman
	mr := ciRecoveryMirror("open", tatarav1alpha1.OwnershipTatara, "green", "4c11cad2")
	r := newUnparkTestReconciler(t, proj, reapSecret(), tk, mr)

	require.NoError(t, r.driveCIRecoveryUnparks(ctx, proj, now))

	got := &tatarav1alpha1.Task{}
	require.NoError(t, r.Get(ctx, objectKeyOf(tk), got))
	require.True(t, tatarav1alpha1.Parked(got))
	require.Equal(t, stage.ReasonAwaitingHuman, got.Status.ParkReason)
}

// CONSTRAINT 1's FAILURE MODE, pinned. This driver lives OUTSIDE stage.Unpark
// on purpose: implement-declined stays UnparkNever, so the reaper's unparkFires
// probe still answers "no re-entry" and a Task this driver never fires for ages
// out at ParkRetention exactly as it did before. An arm inside stage.Unpark
// would have held every such Task alive forever.
func TestImplementDeclinedStillDeclinesThroughOrdinaryUnpark(t *testing.T) {
	now := time.Now()
	class, ok := stage.UnparkClassFor(stage.ReasonImplementDeclined)
	require.True(t, ok)
	require.Equal(t, stage.UnparkNever, class,
		"implement-declined must NOT be reclassified: UnparkTimer requires a bounding CRD counter it does not have")

	tk := ciDeclinedTask("upgrade", tatarav1alpha1.CIEvidenceRed, ciRecoveryMR+"@4c11cad2")
	require.Equal(t, stage.DeclineNoReentry, stage.Unpark(stage.UnparkInput{
		Task: tk, LiveHasRoom: true, Now: now}),
		"the CI recovery is a driver, not a re-entry rule")
	require.True(t, tatarav1alpha1.Parked(tk), "a declined un-park must mutate nothing")
}

// stage.UnparkCIRecovered's OWN guards, asserted apart from the driver: the
// driver filters, but a future second caller must not be able to point the
// primitive at an arbitrary park or at a takeover.
func TestUnparkCIRecovered_RefusesAnythingOutsideItsCase(t *testing.T) {
	now := time.Now()

	other := ciDeclinedTask("implement", "", "")
	other.Status.ParkReason = stage.ReasonMergeBlocked
	require.Error(t, stage.UnparkCIRecovered(other, now))
	require.Equal(t, stage.ReasonMergeBlocked, other.Status.ParkReason)

	notParked := ciDeclinedTask("implement", "", "")
	notParked.Status.ParkReason = ""
	require.Error(t, stage.UnparkCIRecovered(notParked, now))

	takeover := ciDeclinedTask(stage.KindTakeover, "", "")
	require.Error(t, stage.UnparkCIRecovered(takeover, now))
	require.Equal(t, stage.ReasonImplementDeclined, takeover.Status.ParkReason)
}

// THE CEILING THE RECOVERY USED TO WALK STRAIGHT PAST. UnparkCIRecovered
// re-arms the LIVE state the Task parked in, so the very next reconcile mints an
// agent pod - and this driver is the one un-park path that never asked whether
// the project had room for one. Every other one (stage.Unpark's liveRoomDecline,
// the reaper's unparkFires probe, ownership_redeliver) does.
func TestDriveCIRecoveryUnparks_RefusesWhenTheProjectIsAtItsLivePodCeiling(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	proj := reapProject("cirec")
	proj.Spec.MaxLivePods = 1

	tk := ciDeclinedTask("upgrade", tatarav1alpha1.CIEvidenceRed, ciRecoveryMR+"@4c11cad2")
	mr := ciRecoveryMirror("open", tatarav1alpha1.OwnershipTatara, "green", "4c11cad2")
	// One live, un-parked Task already occupies the only slot.
	busy := reapTask("cirec", "t-live", "implement", tatarav1alpha1.StateUnderImplementation, "", now)
	r := newUnparkTestReconciler(t, proj, reapSecret(), tk, mr, busy)

	require.NoError(t, r.driveCIRecoveryUnparks(ctx, proj, now))

	got := &tatarav1alpha1.Task{}
	require.NoError(t, r.Get(ctx, objectKeyOf(tk), got))
	require.True(t, tatarav1alpha1.Parked(got),
		"the project is at its live-pod ceiling; the recovery must wait, not push it over")
	require.NotContains(t, got.Annotations, tatarav1alpha1.AnnCIRecoveryUnparks,
		"and it must not spend a lap of the absolute bound on a refusal it will retry")
	require.NotContains(t, got.Annotations, tatarav1alpha1.AnnCIRecoveryHeads)
}
