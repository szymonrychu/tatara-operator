package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// --- the conflict sweep (G2) ----------------------------------------------
//
// GetMergeState had exactly two call sites before this sweep: the merge
// corridor (reachable ONLY from `merged`) and the submit gate. An owned merge
// request that went DIRTY while its Task sat at awaiting-review - or parked -
// was therefore invisible for as long as it stayed there. tatara-operator#625
// is the measured shape: CONFLICTING for days behind a parked Task, cleared by
// a human merging main in by hand.

// csForge is a GetMergeState-only fake. scm.SCMWriter is embedded nil so any
// OTHER call panics: the sweep must read mergeability and nothing else.
type csForge struct {
	scm.SCMWriter

	mergeState map[int]scm.MergeState
	err        error
	calls      int
}

func (f *csForge) GetMergeState(_ context.Context, _, _ string, number int) (scm.MergeState, error) {
	f.calls++
	if f.err != nil {
		return "", f.err
	}
	if ms, ok := f.mergeState[number]; ok {
		return ms, nil
	}
	return scm.MergeStateClean, nil
}

func csNewForge(ms scm.MergeState) *csForge {
	return &csForge{mergeState: map[int]scm.MergeState{7: ms}}
}

func csReconciler(c client.Client, f *csForge) *ProjectReconciler {
	return &ProjectReconciler{
		Client:  c,
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		SCMFor:  func(string) (scm.SCMWriter, error) { return f, nil },
	}
}

var csNow = time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)

// csResults snapshots every operator_conflict_sweep_total series so a test can
// assert the DELTA: the counter is package-global (ctrlmetrics.Registry), so an
// absolute reading is whatever the tests before it left behind.
func csResults() map[string]float64 {
	out := map[string]float64{}
	for _, r := range []string{"clean", "dirty", "routed", "capped", "error"} {
		out[r] = testutil.ToFloat64(obs.ConflictSweepTotal.WithLabelValues(r))
	}
	return out
}

func csDelta(t *testing.T, before map[string]float64, want map[string]float64) {
	t.Helper()
	after := csResults()
	for r, b := range before {
		got := after[r] - b
		if got != want[r] {
			t.Fatalf("operator_conflict_sweep_total{result=%q} delta = %v, want %v", r, got, want[r])
		}
	}
}

// csFixture is the shape every case starts from: one Task in `state`, one
// tatara-owned OPEN merge request whose MIRROR says not-mergeable.
func csFixture(t *testing.T, state string, parkReason string) (*tatarav1alpha1.Task, client.Client) {
	t.Helper()
	task := mdTask("t1", "implement", state)
	task.Spec.MergeOrder = []string{"tatara-operator"}
	if parkReason != "" {
		task.Status.ParkReason = parkReason
	}
	mr := mdMR(task, "tatara-operator", 7)
	mr.Status.Mergeable = false
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)
	return task, c
}

// THE HEADLINE. A DIRTY owned merge request routes to the rebase edge that
// already exists, from every state the rebase edge is reachable from - not just
// the merge corridor.
//
// deployed is in the table and is DELIBERATELY not routed: `deployed` has
// exactly one exit (done, stage.go's Transitions), so under-implementation is
// not reachable from it and EnterStage would refuse the edge. The conflict is
// still confirmed and counted there - observed, not acted on.
//
// PARKED is in the table and is DELIBERATELY not routed either. There is
// exactly one way out of a park and it is Unpark (stage.Enter refuses a parked
// Task by design); resurrecting a park behind the human it is waiting on is the
// thing internal/stage's mergeStageParks exists to prevent.
func TestConflictSweepRoutesDirtyToTheRebaseEdgeFromEveryLiveState(t *testing.T) {
	cases := []struct {
		name       string
		state      string
		parkReason string
		wantState  string
		wantLaps   int
		wantNote   bool
		wantResult string
	}{
		{
			name: "under-implementation is observed, never re-routed onto itself", state: tatarav1alpha1.StateUnderImplementation,
			wantState: tatarav1alpha1.StateUnderImplementation, wantLaps: 0, wantResult: "dirty",
		},
		{
			name: "awaiting-review routes back to the agent", state: tatarav1alpha1.StateAwaitingReview,
			wantState: tatarav1alpha1.StateUnderImplementation, wantLaps: 1, wantNote: true, wantResult: "routed",
		},
		{
			name: "merged routes back to the agent", state: tatarav1alpha1.StateMerged,
			wantState: tatarav1alpha1.StateUnderImplementation, wantLaps: 1, wantNote: true, wantResult: "routed",
		},
		{
			name: "deployed is observed, never routed", state: tatarav1alpha1.StateDeployed,
			wantState: tatarav1alpha1.StateDeployed, wantLaps: 0, wantResult: "dirty",
		},
		{
			name: "parked under-implementation is observed, never resurrected", state: tatarav1alpha1.StateUnderImplementation,
			parkReason: stage.ReasonAwaitingHuman, wantState: tatarav1alpha1.StateUnderImplementation,
			wantLaps: 0, wantResult: "dirty",
		},
		{
			name: "parked awaiting-review is observed, never resurrected", state: tatarav1alpha1.StateAwaitingReview,
			parkReason: stage.ReasonAwaitingHuman, wantState: tatarav1alpha1.StateAwaitingReview,
			wantLaps: 0, wantResult: "dirty",
		},
		{
			name: "parked merged is observed, never resurrected", state: tatarav1alpha1.StateMerged,
			parkReason: stage.ReasonMergeTimeout, wantState: tatarav1alpha1.StateMerged,
			wantLaps: 0, wantResult: "dirty",
		},
		{
			name: "parked deployed is observed, never resurrected", state: tatarav1alpha1.StateDeployed,
			parkReason: stage.ReasonDeployTimeout, wantState: tatarav1alpha1.StateDeployed,
			wantLaps: 0, wantResult: "dirty",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, c := csFixture(t, tc.state, tc.parkReason)
			f := csNewForge(scm.MergeStateDirty)
			r := csReconciler(c, f)
			before := csResults()

			r.driveConflictSweeps(context.Background(), mdProject(), csNow)
			if f.calls != 1 {
				t.Fatalf("GetMergeState calls = %d, want 1", f.calls)
			}
			got := mdGetTask(t, c, "t1")
			if got.Status.State != tc.wantState {
				t.Fatalf("state = %q, want %q", got.Status.State, tc.wantState)
			}
			if got.Status.MergeConflictReentries != tc.wantLaps {
				t.Fatalf("mergeConflictReentries = %d, want %d", got.Status.MergeConflictReentries, tc.wantLaps)
			}
			if tc.parkReason != "" && got.Status.ParkReason != tc.parkReason {
				t.Fatalf("parkReason = %q, want %q: the sweep must not clear a park",
					got.Status.ParkReason, tc.parkReason)
			}
			if tc.parkReason == "" && tatarav1alpha1.Parked(got) {
				t.Fatalf("parked(%q); the first conflict must route, not park", got.Status.ParkReason)
			}
			// The bundle an implement pod renders carries no mergeability field at
			// all, so the note is the ONLY thing that tells the agent this turn is a
			// conflict resolution rather than a fresh implementation.
			var note string
			for _, n := range got.Status.Notes {
				if n.Agent == "operator" && strings.Contains(n.Body, "CONFLICTS") {
					note = n.Body
				}
			}
			if tc.wantNote {
				if note == "" {
					t.Fatalf("no operator note naming the conflict; notes = %+v", got.Status.Notes)
				}
				for _, want := range []string{"tatara-operator", "!7", "main"} {
					if !strings.Contains(note, want) {
						t.Fatalf("note %q does not name %q", note, want)
					}
				}
			} else if note != "" {
				t.Fatalf("note appended on a conflict that was not routed: %q", note)
			}
			csDelta(t, before, map[string]float64{tc.wantResult: 1})
		})
	}
}

// THE LOAD PIN. The mirror's status.mergeable is the CHEAP trigger; a project
// with no conflicted merge requests must cost ZERO forge calls per tick.
func TestConflictSweepMakesNoForgeCallWhenTheMirrorSaysMergeable(t *testing.T) {
	task, _ := csFixture(t, tatarav1alpha1.StateAwaitingReview, "")
	mr := mdMR(task, "tatara-operator", 7)
	mr.Status.Mergeable = true
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := csNewForge(scm.MergeStateDirty)
	r := csReconciler(c, f)
	before := csResults()

	r.driveConflictSweeps(context.Background(), mdProject(), csNow)
	if f.calls != 0 {
		t.Fatalf("GetMergeState calls = %d, want 0: a mergeable mirror must never pay for a live read", f.calls)
	}
	if got := mdGetTask(t, c, "t1"); got.Status.State != tatarav1alpha1.StateAwaitingReview {
		t.Fatalf("state = %q, want awaiting-review", got.Status.State)
	}
	csDelta(t, before, map[string]float64{})
}

// Ownership decides whether an agent may be put on the branch at all. An
// external merge request is a HUMAN's, and a stood-down one is merge-ELIGIBLE
// and still not ours to push to - so raw ownership is the test, exactly as the
// merge corridor's own conflict arm has it.
func TestConflictSweepIgnoresAnExternallyOwnedMR(t *testing.T) {
	task := mdTask("t1", takeoverKind, tatarav1alpha1.StateAwaitingReview)
	task.Spec.MergeOrder = []string{"tatara-operator"}
	mr := mdMR(task, "tatara-operator", 7)
	mr.Status.Mergeable = false
	mr.Status.Ownership = tatarav1alpha1.OwnershipExternal
	mr.Status.OwnershipReason = "external-push:human-head"
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := csNewForge(scm.MergeStateDirty)
	r := csReconciler(c, f)
	before := csResults()

	r.driveConflictSweeps(context.Background(), mdProject(), csNow)
	if f.calls != 0 {
		t.Fatalf("GetMergeState calls = %d, want 0: an external merge request is never swept", f.calls)
	}
	if got := mdGetTask(t, c, "t1"); got.Status.MergeConflictReentries != 0 {
		t.Fatalf("mergeConflictReentries = %d, want 0", got.Status.MergeConflictReentries)
	}
	csDelta(t, before, map[string]float64{})
}

// THE STALE-MIRROR NO-OP. status.mergeable is written by the mirror sync on
// MirrorCadence, so it is routinely minutes behind. The trigger is cheap
// BECAUSE it is allowed to be wrong; only the LIVE read may act.
func TestConflictSweepIsANoOpWhenTheLiveReadSaysClean(t *testing.T) {
	for _, ms := range []scm.MergeState{scm.MergeStateClean, scm.MergeStateBlocked, scm.MergeStateBehind} {
		t.Run(string(ms), func(t *testing.T) {
			_, c := csFixture(t, tatarav1alpha1.StateAwaitingReview, "")
			f := csNewForge(ms)
			r := csReconciler(c, f)
			before := csResults()

			r.driveConflictSweeps(context.Background(), mdProject(), csNow)
			if f.calls != 1 {
				t.Fatalf("GetMergeState calls = %d, want 1", f.calls)
			}
			got := mdGetTask(t, c, "t1")
			if got.Status.State != tatarav1alpha1.StateAwaitingReview {
				t.Fatalf("state = %q, want awaiting-review: only DIRTY routes", got.Status.State)
			}
			if got.Status.MergeConflictReentries != 0 {
				t.Fatalf("mergeConflictReentries = %d, want 0", got.Status.MergeConflictReentries)
			}
			csDelta(t, before, map[string]float64{"clean": 1})
		})
	}
}

// THE BOUND, reused unchanged. main keeps moving, so conflict -> implement ->
// push -> conflict is a real loop; at MaxMergeConflictReentries the Task parks
// merge-blocked, which is where the stall-and-time-out path landed it before
// any of this existed.
func TestConflictSweepParksMergeBlockedOnTheFourthLap(t *testing.T) {
	task, c := csFixture(t, tatarav1alpha1.StateAwaitingReview, "")
	task.Status.MergeConflictReentries = tatarav1alpha1.MaxMergeConflictReentries
	if err := c.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed the spent budget: %v", err)
	}
	f := csNewForge(scm.MergeStateDirty)
	r := csReconciler(c, f)
	before := csResults()

	r.driveConflictSweeps(context.Background(), mdProject(), csNow)
	got := mdGetTask(t, c, "t1")
	if got.Status.State != tatarav1alpha1.StateAwaitingReview {
		t.Fatalf("state = %q, want awaiting-review: a spent budget parks in place", got.Status.State)
	}
	if !tatarav1alpha1.Parked(got) || got.Status.ParkReason != stage.ReasonMergeBlocked {
		t.Fatalf("parkReason = %q, want %q", got.Status.ParkReason, stage.ReasonMergeBlocked)
	}
	if got.Status.MergeConflictReentries != tatarav1alpha1.MaxMergeConflictReentries {
		t.Fatalf("mergeConflictReentries = %d, want %d (the cap is not raised by the park)",
			got.Status.MergeConflictReentries, tatarav1alpha1.MaxMergeConflictReentries)
	}
	csDelta(t, before, map[string]float64{"capped": 1})
}

// THE MUTATION TRAP. stage.MergeConflict INCREMENTS
// status.mergeConflictReentries as a side effect, so calling it twice in one
// pass silently spends two laps of a three-lap budget.
func TestConflictSweepBumpsTheCounterExactlyOncePerPass(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StateAwaitingReview)
	task.Spec.MergeOrder = []string{"tatara-operator", "tatara-cli"}
	mr1 := mdMR(task, "tatara-operator", 7)
	mr1.Status.Mergeable = false
	mr2 := mdMR(task, "tatara-cli", 8)
	mr2.Status.Mergeable = false
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), mdRepo("tatara-cli"), task, mr1, mr2)

	f := &csForge{mergeState: map[int]scm.MergeState{7: scm.MergeStateDirty, 8: scm.MergeStateDirty}}
	r := csReconciler(c, f)
	before := csResults()

	r.driveConflictSweeps(context.Background(), mdProject(), csNow)
	got := mdGetTask(t, c, "t1")
	if got.Status.MergeConflictReentries != 1 {
		t.Fatalf("mergeConflictReentries = %d, want 1: two conflicted MRs are still ONE lap",
			got.Status.MergeConflictReentries)
	}
	if got.Status.State != tatarav1alpha1.StateUnderImplementation {
		t.Fatalf("state = %q, want under-implementation", got.Status.State)
	}
	csDelta(t, before, map[string]float64{"routed": 1})
}

// FAIL OPEN. A forge read that errors is logged, counted and SKIPPED - never
// returned. The sweep is a refinement on top of the merge corridor, not a
// precondition for anything, and failing the whole pass would take every OTHER
// Task's sweep down with it.
func TestConflictSweepFailsOpenOnAForgeError(t *testing.T) {
	_, c := csFixture(t, tatarav1alpha1.StateAwaitingReview, "")
	f := csNewForge(scm.MergeStateDirty)
	f.err = errors.New("forge 503")
	r := csReconciler(c, f)
	before := csResults()

	r.driveConflictSweeps(context.Background(), mdProject(), csNow)
	if got := mdGetTask(t, c, "t1"); got.Status.MergeConflictReentries != 0 {
		t.Fatalf("mergeConflictReentries = %d, want 0", got.Status.MergeConflictReentries)
	}
	csDelta(t, before, map[string]float64{"error": 1})
}

// A Task in a state the sweep does not cover (new, refined with no MR, done) is
// never listed against the forge. `done` is the one that matters: a reaped
// Task's stale mirror must not keep costing forge reads forever.
func TestConflictSweepSkipsTasksOutsideTheLiveAndOperatorDrivenStates(t *testing.T) {
	for _, state := range []string{tatarav1alpha1.StateNew, tatarav1alpha1.StateDone, tatarav1alpha1.StateRejected} {
		t.Run(state, func(t *testing.T) {
			_, c := csFixture(t, state, "")
			f := csNewForge(scm.MergeStateDirty)
			r := csReconciler(c, f)

			r.driveConflictSweeps(context.Background(), mdProject(), csNow)
			if f.calls != 0 {
				t.Fatalf("GetMergeState calls = %d, want 0 for state %q", f.calls, state)
			}
		})
	}
}

// THE PACING. A conflict does not need 30-second latency and the sweep costs a
// List plus N forge reads, so it runs at most once per ConflictSweepInterval
// however fast Reconcile() is being driven by something else.
func TestConflictSweepIsPacedPerProject(t *testing.T) {
	_, c := csFixture(t, tatarav1alpha1.StateAwaitingReview, "")
	f := csNewForge(scm.MergeStateClean)
	r := csReconciler(c, f)

	requeue := r.driveConflictSweepsPaced(context.Background(), mdProject(), csNow)
	if requeue != defaultConflictSweepInterval {
		t.Fatalf("requeue = %v, want %v", requeue, defaultConflictSweepInterval)
	}
	if f.calls != 1 {
		t.Fatalf("GetMergeState calls = %d, want 1 on the first pass", f.calls)
	}

	// One minute later: inside the floor, so the whole sweep is skipped and the
	// requeue reports the REMAINING time.
	requeue = r.driveConflictSweepsPaced(context.Background(), mdProject(), csNow.Add(time.Minute))
	if want := defaultConflictSweepInterval - time.Minute; requeue != want {
		t.Fatalf("requeue = %v, want %v", requeue, want)
	}
	if f.calls != 1 {
		t.Fatalf("GetMergeState calls = %d, want 1: the second pass was inside the floor", f.calls)
	}

	// Past the floor: it runs again.
	r.driveConflictSweepsPaced(context.Background(), mdProject(),
		csNow.Add(defaultConflictSweepInterval+time.Second))
	if f.calls != 2 {
		t.Fatalf("GetMergeState calls = %d, want 2 once the floor elapsed", f.calls)
	}
}

// THE SELF-EDGE KILL (finding 1). under-implementation IS the rebase edge:
// there is nothing to route to, and stage.MergeConflict INCREMENTS
// status.mergeConflictReentries as a side effect. Nothing suppressed the next
// pass - the merge request stays DIRTY on the forge until the agent pushes - so
// a Task the sweep had just handed to an agent burnt one lap of a three-lap
// budget every five minutes and, on the fourth, parked merge-blocked, which is
// UnparkNever AND deletes the pod that was mid-conflict-resolution.
//
// Two consecutive passes, the same conflict, nothing moved: ZERO laps.
func TestConflictSweepNeverSpendsALapAtUnderImplementation(t *testing.T) {
	_, c := csFixture(t, tatarav1alpha1.StateUnderImplementation, "")
	f := csNewForge(scm.MergeStateDirty)
	r := csReconciler(c, f)
	before := csResults()

	for pass := 1; pass <= 2; pass++ {
		at := csNow.Add(time.Duration(pass) * defaultConflictSweepInterval)
		r.driveConflictSweeps(context.Background(), mdProject(), at)
		got := mdGetTask(t, c, "t1")
		if got.Status.MergeConflictReentries != 0 {
			t.Fatalf("pass %d: mergeConflictReentries = %d, want 0: the self-edge is not a lap",
				pass, got.Status.MergeConflictReentries)
		}
		if tatarav1alpha1.Parked(got) {
			t.Fatalf("pass %d: parked(%q): the sweep killed a Task whose agent is still working the branch",
				pass, got.Status.ParkReason)
		}
		if got.Status.State != tatarav1alpha1.StateUnderImplementation {
			t.Fatalf("pass %d: state = %q, want under-implementation", pass, got.Status.State)
		}
		for _, n := range got.Status.Notes {
			if n.Agent == "operator" && strings.Contains(n.Body, "CONFLICTS") {
				t.Fatalf("pass %d: conflict note appended at under-implementation: the pod already has the branch", pass)
			}
		}
	}
	csDelta(t, before, map[string]float64{"dirty": 2})
}

// A Task the sweep ROUTED must not then be killed by the passes that follow it.
// awaiting-review -> under-implementation spends lap 1; every subsequent pass
// observes the same still-DIRTY merge request and spends nothing.
func TestConflictSweepDoesNotBurnTheBudgetOfTheTaskItJustRouted(t *testing.T) {
	_, c := csFixture(t, tatarav1alpha1.StateAwaitingReview, "")
	f := csNewForge(scm.MergeStateDirty)
	r := csReconciler(c, f)

	for pass := 0; pass < 6; pass++ {
		at := csNow.Add(time.Duration(pass) * defaultConflictSweepInterval)
		r.driveConflictSweeps(context.Background(), mdProject(), at)
	}
	got := mdGetTask(t, c, "t1")
	if got.Status.MergeConflictReentries != 1 {
		t.Fatalf("mergeConflictReentries = %d, want 1: only the routing pass is a lap",
			got.Status.MergeConflictReentries)
	}
	if tatarav1alpha1.Parked(got) {
		t.Fatalf("parked(%q) after six passes on ONE unresolved conflict", got.Status.ParkReason)
	}
}

// csListCounter counts List calls per list type, and optionally fails one.
func csListCounter(counts map[string]int, failList client.ObjectList, failErr error) interceptor.Funcs {
	return interceptor.Funcs{
		List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			switch list.(type) {
			case *tatarav1alpha1.MergeRequestList:
				counts["MergeRequestList"]++
			case *tatarav1alpha1.TaskList:
				counts["TaskList"]++
			}
			if failList != nil && fmt.Sprintf("%T", list) == fmt.Sprintf("%T", failList) {
				return failErr
			}
			return cl.List(ctx, list, opts...)
		},
	}
}

// THE LOAD PIN, second half (finding 3). ownedMergeRequests is a full
// namespace-wide MergeRequestList, and calling it per Task is the
// tatara-operator#368 shape the pacing rationale itself cites: 30 live Tasks x
// 200 MergeRequest CRs is 6000 deep-copied objects per pass. ONE List per pass,
// indexed by controller-owner, and a Task that owns nothing costs nothing.
func TestConflictSweepListsMergeRequestsOncePerPass(t *testing.T) {
	objs := []client.Object{mdProject(), mdSecret(), mdRepo("tatara-operator")}
	for _, name := range []string{"t1", "t2", "t3"} {
		task := mdTask(name, "implement", tatarav1alpha1.StateAwaitingReview)
		objs = append(objs, task)
	}
	// Only t1 owns a merge request; t2 and t3 own none at all.
	owner := mdTask("t1", "implement", tatarav1alpha1.StateAwaitingReview)
	mr := mdMR(owner, "tatara-operator", 7)
	mr.Status.Mergeable = false
	objs = append(objs, mr)

	counts := map[string]int{}
	c := newMirrorClientIntercepted(t, csListCounter(counts, nil, nil), objs...)
	f := csNewForge(scm.MergeStateClean)
	r := csReconciler(c, f)

	r.driveConflictSweeps(context.Background(), mdProject(), csNow)
	if counts["MergeRequestList"] != 1 {
		t.Fatalf("MergeRequestList calls = %d, want 1: one List per PASS, not one per Task", counts["MergeRequestList"])
	}
	if counts["TaskList"] != 1 {
		t.Fatalf("TaskList calls = %d, want 1", counts["TaskList"])
	}
}

// FAIL OPEN, ALL THE WAY (finding 2). The forge read was the only error class
// this sweep swallowed. A rotated scmSecretRef, a deleted Repository CR or a
// failing List each aborted the WHOLE Project reconcile - and the sweep runs
// BEFORE enforceLivePodCeiling, resumeNoReentryParks, driveStrandedParks and
// ReapTerminal, so all four went permanently dead for that Project on a
// credentials typo.
func TestConflictSweepFailsOpenOnEveryErrorClass(t *testing.T) {
	base := func(t *testing.T) []client.Object {
		t.Helper()
		task := mdTask("t1", "implement", tatarav1alpha1.StateAwaitingReview)
		task.Spec.MergeOrder = []string{"tatara-operator"}
		mr := mdMR(task, "tatara-operator", 7)
		mr.Status.Mergeable = false
		return []client.Object{mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr}
	}
	cases := []struct {
		name  string
		build func(t *testing.T) (client.Client, *ProjectReconciler)
	}{
		{
			name: "no scm writer for the provider",
			build: func(t *testing.T) (client.Client, *ProjectReconciler) {
				c := newMirrorClient(t, base(t)...)
				r := csReconciler(c, csNewForge(scm.MergeStateDirty))
				r.SCMFor = func(string) (scm.SCMWriter, error) { return nil, errors.New("no writer for provider") }
				return c, r
			},
		},
		{
			name: "the scm secret was rotated out from under us",
			build: func(t *testing.T) (client.Client, *ProjectReconciler) {
				objs := base(t)
				kept := objs[:0]
				for _, o := range objs {
					if _, ok := o.(*corev1.Secret); !ok {
						kept = append(kept, o)
					}
				}
				c := newMirrorClient(t, kept...)
				return c, csReconciler(c, csNewForge(scm.MergeStateDirty))
			},
		},
		{
			name: "the Repository CR is gone",
			build: func(t *testing.T) (client.Client, *ProjectReconciler) {
				objs := base(t)
				kept := objs[:0]
				for _, o := range objs {
					if _, ok := o.(*tatarav1alpha1.Repository); !ok {
						kept = append(kept, o)
					}
				}
				c := newMirrorClient(t, kept...)
				return c, csReconciler(c, csNewForge(scm.MergeStateDirty))
			},
		},
		{
			name: "the MergeRequest List fails",
			build: func(t *testing.T) (client.Client, *ProjectReconciler) {
				c := newMirrorClientIntercepted(t, csListCounter(map[string]int{},
					&tatarav1alpha1.MergeRequestList{}, errors.New("cache not started")), base(t)...)
				return c, csReconciler(c, csNewForge(scm.MergeStateDirty))
			},
		},
		{
			name: "the Task List fails",
			build: func(t *testing.T) (client.Client, *ProjectReconciler) {
				c := newMirrorClientIntercepted(t, csListCounter(map[string]int{},
					&tatarav1alpha1.TaskList{}, errors.New("cache not started")), base(t)...)
				return c, csReconciler(c, csNewForge(scm.MergeStateDirty))
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, r := tc.build(t)
			before := csResults()

			requeue := r.driveConflictSweepsPaced(context.Background(), mdProject(), csNow)
			if requeue != defaultConflictSweepInterval {
				t.Fatalf("requeue = %v, want %v", requeue, defaultConflictSweepInterval)
			}
			if got := mdGetTask(t, c, "t1"); got.Status.MergeConflictReentries != 0 {
				t.Fatalf("mergeConflictReentries = %d, want 0", got.Status.MergeConflictReentries)
			}
			csDelta(t, before, map[string]float64{"error": 1})
		})
	}
}

// THE PACING IS STAMPED BEFORE THE RUN, not after it. Stamped after, a pass
// that fails never records itself, so the namespace-wide TaskList plus the
// MergeRequestList re-ran on EVERY reconcile - unpaced - for exactly as long as
// the credentials stayed broken.
func TestConflictSweepPacesEvenWhenThePassFails(t *testing.T) {
	task := mdTask("t1", "implement", tatarav1alpha1.StateAwaitingReview)
	task.Spec.MergeOrder = []string{"tatara-operator"}
	mr := mdMR(task, "tatara-operator", 7)
	mr.Status.Mergeable = false
	counts := map[string]int{}
	// No mdSecret: mirrorSCMToken fails on every pass.
	c := newMirrorClientIntercepted(t, csListCounter(counts, nil, nil),
		mdProject(), mdRepo("tatara-operator"), task, mr)
	r := csReconciler(c, csNewForge(scm.MergeStateDirty))

	r.driveConflictSweepsPaced(context.Background(), mdProject(), csNow)
	after := counts["TaskList"]
	if after != 1 {
		t.Fatalf("TaskList calls = %d, want 1 on the first pass", after)
	}
	r.driveConflictSweepsPaced(context.Background(), mdProject(), csNow.Add(time.Minute))
	if counts["TaskList"] != after {
		t.Fatalf("TaskList calls = %d, want %d: a FAILED pass must still be paced",
			counts["TaskList"], after)
	}
}

// THE PARKED READ (finding 4). A parked Task can never be acted on - there is
// exactly one way out of a park and this sweep is not it - so a forge read
// every five minutes for the seven days until the reaper collects it (~2000
// calls) buys one log line. It is read on the PARKED MIRROR CADENCE instead,
// which is also the cadence status.mergeable itself is refreshed at.
//
// The mirror trigger is DROPPED there for the same reason: gating a 24-hourly
// read on a field written 24-hourly makes tatara-operator#625's own latency two
// full days.
func TestConflictSweepReadsAParkedTaskOnTheParkedMirrorCadence(t *testing.T) {
	for _, mergeable := range []bool{false, true} {
		name := "mirror says not-mergeable"
		if mergeable {
			name = "mirror says mergeable and is a day stale"
		}
		t.Run(name, func(t *testing.T) {
			task := mdTask("t1", "implement", tatarav1alpha1.StateAwaitingReview)
			task.Spec.MergeOrder = []string{"tatara-operator"}
			task.Status.ParkReason = stage.ReasonAwaitingHuman
			mr := mdMR(task, "tatara-operator", 7)
			mr.Status.Mergeable = mergeable
			c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)
			f := csNewForge(scm.MergeStateDirty)
			r := csReconciler(c, f)

			r.driveConflictSweeps(context.Background(), mdProject(), csNow)
			if f.calls != 1 {
				t.Fatalf("GetMergeState calls = %d, want 1: a parked conflict is confirmed live, not from the mirror", f.calls)
			}
			// Every pass for the next day is free.
			for _, d := range []time.Duration{5 * time.Minute, time.Hour, 23 * time.Hour} {
				r.driveConflictSweeps(context.Background(), mdProject(), csNow.Add(d))
				if f.calls != 1 {
					t.Fatalf("GetMergeState calls = %d after +%v, want 1: a park cannot be acted on, so it "+
						"must not be re-read on the five-minute floor", f.calls, d)
				}
			}
			r.driveConflictSweeps(context.Background(), mdProject(),
				csNow.Add(MirrorCadenceParked+time.Minute))
			if f.calls != 2 {
				t.Fatalf("GetMergeState calls = %d, want 2 once the parked mirror cadence elapsed", f.calls)
			}
			if got := mdGetTask(t, c, "t1"); got.Status.ParkReason != stage.ReasonAwaitingHuman {
				t.Fatalf("parkReason = %q: the sweep must never clear a park", got.Status.ParkReason)
			}
		})
	}
}

// The parked cadence is the PARKED half's clock only. An active Task keeps the
// five-minute floor.
func TestConflictSweepKeepsTheFiveMinuteFloorForActiveTasks(t *testing.T) {
	_, c := csFixture(t, tatarav1alpha1.StateAwaitingReview, "")
	f := csNewForge(scm.MergeStateClean)
	r := csReconciler(c, f)

	for pass := 0; pass < 3; pass++ {
		r.driveConflictSweeps(context.Background(), mdProject(),
			csNow.Add(time.Duration(pass)*defaultConflictSweepInterval))
	}
	if f.calls != 3 {
		t.Fatalf("GetMergeState calls = %d, want 3: an ACTIVE Task is read on the sweep's own floor", f.calls)
	}
}

// An unwired SCMFor (a unit test with no forge) is a no-op, not a panic: the
// sweep is a refinement of the merge corridor, never a precondition for it.
func TestConflictSweepIsANoOpWithNoForgeWired(t *testing.T) {
	_, c := csFixture(t, tatarav1alpha1.StateAwaitingReview, "")
	r := &ProjectReconciler{Client: c, Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry())}
	r.driveConflictSweeps(context.Background(), mdProject(), csNow)
}
