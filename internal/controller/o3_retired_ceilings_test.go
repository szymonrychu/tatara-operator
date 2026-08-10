package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// ---------------------------------------------------------------------------
// O3: the retired-park migration.
// ---------------------------------------------------------------------------

// retiredParkTask builds a Task parked under a ceiling O3 deleted, parked `age`
// ago. It writes the park fields directly rather than through stage.Park: the
// point of the fixture is an object written by an EARLIER BUILD, which is
// exactly what the migration exists to find.
func retiredParkTask(name, reason string, age time.Duration, now time.Time) *tatarav1alpha1.Task {
	t := reapTask("retired", name, "implement", tatarav1alpha1.StateUnderImplementation,
		reason, now.Add(-age-time.Hour))
	parkedAt := metav1.NewTime(now.Add(-age))
	t.Status.ParkedAt = &parkedAt
	t.Status.ParkedFromState = tatarav1alpha1.StateUnderImplementation
	t.Status.Stats.Turns = 400
	t.Status.Stats.PodRecreations = 9
	return t
}

// TestDriveRetiredUnparks_ReleasesEachRetiredCeilingExactlyOnce is the migration's
// whole contract in one table: every park written by a deleted ceiling is
// released, in place, with its clocks re-armed - and a SECOND pass over the same
// Tasks is a pure no-op, because the annotation latch has been stamped.
//
// Idempotence is not a nicety here. Without the latch this runs on every paced
// project reconcile forever, minting a pod per Task per pass, which is a
// token-burning loop dressed as a migration.
func TestDriveRetiredUnparks_ReleasesEachRetiredCeilingExactlyOnce(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	proj := reapProject("retired")

	for _, reason := range []string{
		stage.ReasonTurnBudgetExhausted,
		stage.ReasonReviewLoopExhausted,
		stage.ReasonPodRecreationExhausted,
	} {
		t.Run(reason, func(t *testing.T) {
			tk := retiredParkTask("retired-"+reason, reason, time.Hour, now)
			r := newUnparkTestReconciler(t, proj, reapSecret(), tk)
			counter := r.Metrics.TaskUnparkedCounter(reason, obs.UnparkClassRetired)

			require.NoError(t, r.driveRetiredUnparks(ctx, proj, now))

			got := &tatarav1alpha1.Task{}
			require.NoError(t, r.Get(ctx, objectKeyOf(tk), got))
			require.False(t, tatarav1alpha1.Parked(got),
				"a park written by a deleted ceiling must be released")
			require.Equal(t, tatarav1alpha1.StateUnderImplementation, got.Status.State,
				"the migration is an un-park in place; it must not move state")
			require.Nil(t, got.Status.PodStartedAt, "the re-arm must clear the pod clocks")
			require.Nil(t, got.Status.StateWorkStartedAt)
			require.Equal(t, 0, got.Status.Stats.PodRecreations, "the re-arm resets the recreation count")
			require.Equal(t, 400, got.Status.Stats.Turns,
				"turns are NOT reset: they are observability, and nothing enforces them any more")
			require.Contains(t, got.Annotations, tatarav1alpha1.AnnRetiredParkMigrated,
				"the once-only latch must be stamped")
			require.Equal(t, float64(1), testutil.ToFloat64(counter))

			// SECOND PASS. Re-park it under the same retired reason to prove the
			// latch - not the un-parked state - is what stops it.
			got.Status.ParkReason = reason
			parkedAt := metav1.NewTime(now)
			got.Status.ParkedAt = &parkedAt
			require.NoError(t, r.Status().Update(ctx, got))

			require.NoError(t, r.driveRetiredUnparks(ctx, proj, now))

			again := &tatarav1alpha1.Task{}
			require.NoError(t, r.Get(ctx, objectKeyOf(tk), again))
			require.True(t, tatarav1alpha1.Parked(again),
				"the second pass must be a NO-OP: exactly once per Task, idempotent forever")
			require.Equal(t, reason, again.Status.ParkReason)
			require.Equal(t, float64(1), testutil.ToFloat64(counter),
				"the counter must not advance on a Task already migrated")
		})
	}
}

// TestDriveRetiredUnparks_LeavesOldWreckageToAgeOut is the 48h cutoff. Old
// wreckage is genuinely dead work - stale branch, moved-on issue - and waking it
// up means minting a pod that burns tokens rediscovering that. It finishes aging
// out at ParkRetention instead, exactly as it would have without this release.
func TestDriveRetiredUnparks_LeavesOldWreckageToAgeOut(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	proj := reapProject("retired")
	old := retiredParkTask("retired-ancient", stage.ReasonTurnBudgetExhausted, 50*time.Hour, now)
	r := newUnparkTestReconciler(t, proj, reapSecret(), old)

	require.NoError(t, r.driveRetiredUnparks(ctx, proj, now))

	got := &tatarav1alpha1.Task{}
	require.NoError(t, r.Get(ctx, objectKeyOf(old), got))
	require.True(t, tatarav1alpha1.Parked(got),
		"a Task parked 50h ago is past the 48h window and must be left to age out at ParkRetention")
	require.Equal(t, stage.ReasonTurnBudgetExhausted, got.Status.ParkReason)
	require.NotContains(t, got.Annotations, tatarav1alpha1.AnnRetiredParkMigrated,
		"a SKIPPED Task must not be latched: the skip is an age decision, not a migration")
}

// The boundary, both sides, so the cutoff cannot silently drift into "no Task is
// ever migrated" or "every Task is".
func TestDriveRetiredUnparks_TheCutoffIsFortyEightHours(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	proj := reapProject("retired")

	for _, tc := range []struct {
		name        string
		age         time.Duration
		wantParked  bool
		wantLatched bool
	}{
		{"just inside", RetiredParkMigrationWindow - time.Minute, false, true},
		{"just outside", RetiredParkMigrationWindow + time.Minute, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tk := retiredParkTask("retired-boundary", stage.ReasonReviewLoopExhausted, tc.age, now)
			r := newUnparkTestReconciler(t, proj, reapSecret(), tk)

			require.NoError(t, r.driveRetiredUnparks(ctx, proj, now))

			got := &tatarav1alpha1.Task{}
			require.NoError(t, r.Get(ctx, objectKeyOf(tk), got))
			require.Equal(t, tc.wantParked, tatarav1alpha1.Parked(got))
			_, latched := got.Annotations[tatarav1alpha1.AnnRetiredParkMigrated]
			require.Equal(t, tc.wantLatched, latched)
		})
	}
}

// The migration is NARROW. It must not launder an unrelated park - a Task parked
// awaiting-human or merge-blocked is parked for a reason that still exists, and
// clearing it here would be an arbitrary un-park wearing a migration's name.
func TestDriveRetiredUnparks_IgnoresEveryOtherParkReason(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	proj := reapProject("retired")

	for _, reason := range []string{
		stage.ReasonAwaitingHuman, stage.ReasonMergeBlocked, stage.ReasonStageDeadline,
		stage.ReasonNoOutcome, stage.ReasonAdmissionStarved, stage.ReasonBacklogSweep,
	} {
		t.Run(reason, func(t *testing.T) {
			tk := retiredParkTask("retired-other", reason, time.Hour, now)
			r := newUnparkTestReconciler(t, proj, reapSecret(), tk)

			require.NoError(t, r.driveRetiredUnparks(ctx, proj, now))

			got := &tatarav1alpha1.Task{}
			require.NoError(t, r.Get(ctx, objectKeyOf(tk), got))
			require.True(t, tatarav1alpha1.Parked(got),
				"%s is not a retired ceiling and the migration must not touch it", reason)
			require.NotContains(t, got.Annotations, tatarav1alpha1.AnnRetiredParkMigrated)
		})
	}
}

// stage.UnparkRetiredPark REFUSES anything outside its class. This is the
// primitive's own guard, asserted separately from the driver: the driver filters,
// but a future second caller must not be able to point it at an arbitrary park.
func TestUnparkRetiredPark_RefusesANonRetiredReason(t *testing.T) {
	now := time.Now()
	tk := retiredParkTask("guard", stage.ReasonAwaitingHuman, time.Hour, now)
	require.Error(t, stage.UnparkRetiredPark(tk, now))
	require.Equal(t, stage.ReasonAwaitingHuman, tk.Status.ParkReason, "a refused un-park must mutate nothing")

	notParked := retiredParkTask("guard2", "", time.Hour, now)
	notParked.Status.ParkReason = ""
	require.Error(t, stage.UnparkRetiredPark(notParked, now))
}

// The three retired reasons still DECLINE through stage.Unpark. That is what
// keeps an un-migrated leftover (past the 48h window, or from a project the
// migration has not swept yet) ageing out at ParkRetention exactly as it did
// before O3, instead of being held alive forever by the reaper's unparkFires
// probe answering true.
func TestRetiredReasonsStillDeclineThroughOrdinaryUnpark(t *testing.T) {
	now := time.Now()
	for _, reason := range []string{
		stage.ReasonTurnBudgetExhausted,
		stage.ReasonReviewLoopExhausted,
		stage.ReasonPodRecreationExhausted,
	} {
		class, ok := stage.UnparkClassFor(reason)
		require.True(t, ok)
		require.Equal(t, stage.UnparkRetired, class, "%s must be classified as a retired ceiling", reason)

		tk := retiredParkTask("decline", reason, time.Hour, now)
		require.Equal(t, stage.DeclineNoReentry, stage.Unpark(stage.UnparkInput{
			Task: tk, LiveHasRoom: true, Now: now}),
			"the migration is a one-shot sweep, not a re-entry rule")
	}
}

// ---------------------------------------------------------------------------
// O3: the reaper stand-down.
// ---------------------------------------------------------------------------

// standDownFixture builds a LIVE, un-parked Task whose pod holds NO turn and
// whose last activity was `idleFor` ago, plus the pod itself. That is the
// #237 idle-backstop shape AND the shape of a conversation waiting on a human;
// which of the two it is, is the whole question this stand-down answers.
func standDownFixture(idleFor time.Duration) (*tatarav1alpha1.Task, *corev1.Pod) {
	now := time.Now()
	task := &tatarav1alpha1.Task{}
	task.Namespace = testNS
	task.Name = "standdown"
	task.UID = "uid-standdown"
	task.Spec.ProjectRef = "standdown-proj"
	task.Spec.Kind = "implement"
	task.Status.State = tatarav1alpha1.StateUnderImplementation

	entered := metav1.NewTime(now.Add(-2 * time.Hour))
	podAt := metav1.NewTime(now.Add(-90 * time.Minute))
	lastEvent := metav1.NewTime(now.Add(-idleFor))
	task.Status.StateEnteredAt = &entered
	task.Status.PodStartedAt = &podAt
	task.Status.StateWorkStartedAt = &podAt
	task.Status.ConversationLastEventAt = &lastEvent
	task.Annotations = map[string]string{
		tatarav1alpha1.AnnTurnComplete: lastEvent.UTC().Format(time.RFC3339),
	}

	pod := &corev1.Pod{}
	pod.Namespace = testNS
	pod.Name = "wrapper-standdown"
	pod.CreationTimestamp = podAt
	pod.Labels = map[string]string{
		agent.LabelTask:    task.Name,
		agent.LabelTaskUID: string(task.UID),
	}
	return task, pod
}

// THE REASON THIS HAD TO SHIP IN THE SAME PR AS THE CEILING REMOVAL.
//
// "No turn in flight" is also what a conversation waiting on a human looks like.
// idlePodReapMinutes is 30 and ConversationIdleDefault is 60, so every such
// conversation was reaped at the half-hour mark - survivable only because
// maxPodRecreations terminated the resulting churn after a few laps. With that
// ceiling deleted the same churn runs for the full 24h residency cap (~48 pods)
// and trips the very operator_pod_recreations_total alert that replaced it.
func TestIdleReapStandsDownForALiveConversationInsideItsBudget(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = testNS
	proj.Name = "standdown-proj"

	for _, tc := range []struct {
		name     string
		idleFor  time.Duration
		wantReap bool
	}{
		{"inside the conversation budget", 40 * time.Minute, false},
		{"just inside the boundary", 59 * time.Minute, false},
		{"past the conversation budget", 70 * time.Minute, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task, pod := standDownFixture(tc.idleFor)
			s := &CallbackServer{
				Namespace:        testNS,
				ReaperGrace:      time.Millisecond,
				IdlePodReapAfter: 30 * time.Minute,
			}
			tasks := map[string]*tatarav1alpha1.Task{task.Name: task}
			projects := map[string]*tatarav1alpha1.Project{proj.Name: proj}

			reason, reap := s.orphanReason(pod, tasks, projects)
			require.Equal(t, tc.wantReap, reap,
				"idle %s, budget 60m: reason %q", tc.idleFor, reason)
			if tc.wantReap {
				require.Equal(t, "idle no live turn", reason,
					"past the budget the #237 backstop must regain its full reach")
			}
		})
	}
}

// The stand-down reads the PROJECT's budget, not a hard-coded 60m. A project
// that shortens conversationIdleMinutes shortens the stand-down with it, or the
// reaper and reconcileClocks disagree about when a conversation is over.
func TestIdleReapStandDownHonoursTheProjectConversationBudget(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = testNS
	proj.Name = "standdown-proj"
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{ConversationIdleMinutes: 35}

	task, pod := standDownFixture(40 * time.Minute)
	s := &CallbackServer{
		Namespace:        testNS,
		ReaperGrace:      time.Millisecond,
		IdlePodReapAfter: 30 * time.Minute,
	}
	tasks := map[string]*tatarav1alpha1.Task{task.Name: task}
	projects := map[string]*tatarav1alpha1.Project{proj.Name: proj}

	_, reap := s.orphanReason(pod, tasks, projects)
	require.True(t, reap, "40m idle is past a 35m conversation budget; the stand-down must have lifted")
}

// A PARKED Task owns no pod, so the stand-down must not cover it - park is what
// takes the pod down, and a parked Task still holding a live wrapper is the leak
// #237 exists to collect.
func TestIdleReapStandDownDoesNotCoverAParkedTask(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = testNS
	proj.Name = "standdown-proj"

	task, pod := standDownFixture(40 * time.Minute)
	parkedAt := metav1.NewTime(time.Now().Add(-10 * time.Minute))
	task.Status.ParkReason = stage.ReasonAwaitingHuman
	task.Status.ParkedAt = &parkedAt

	s := &CallbackServer{
		Namespace:        testNS,
		ReaperGrace:      time.Millisecond,
		IdlePodReapAfter: 30 * time.Minute,
	}
	tasks := map[string]*tatarav1alpha1.Task{task.Name: task}
	projects := map[string]*tatarav1alpha1.Project{proj.Name: proj}

	_, reap := s.orphanReason(pod, tasks, projects)
	require.True(t, reap, "a parked Task's pod is a leak, not a live conversation")
}

// A turn IN FLIGHT was never the idle backstop's business and still is not: the
// turn-stall path owns it. The stand-down must not be what is doing the work
// here, or a regression in it would silently hand working agents to the reaper.
func TestIdleReapNeverTouchesATaskWithATurnInFlight(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = testNS
	proj.Name = "standdown-proj"

	task, pod := standDownFixture(3 * time.Hour)
	// A turn is IN FLIGHT: started, not complete (stage.TurnInFlight tests both).
	task.Annotations[tatarav1alpha1.AnnCurrentTurn] = "turn-1"
	delete(task.Annotations, tatarav1alpha1.AnnTurnComplete)
	task.Annotations[tatarav1alpha1.AnnTurnStartedAt] = time.Now().Add(-3 * time.Hour).UTC().Format(time.RFC3339)

	s := &CallbackServer{
		Namespace:        testNS,
		ReaperGrace:      time.Millisecond,
		IdlePodReapAfter: 30 * time.Minute,
	}
	tasks := map[string]*tatarav1alpha1.Task{task.Name: task}
	projects := map[string]*tatarav1alpha1.Project{proj.Name: proj}

	_, reap := s.orphanReason(pod, tasks, projects)
	require.False(t, reap, "an in-flight turn is owned by the stall path, never by #237")
}
