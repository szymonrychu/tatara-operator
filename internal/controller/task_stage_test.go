package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/promptguidance"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// THE RECONCILER HALF OF THE STAGE CONTRACT (Section I).
//
// internal/stage/stage_test.go holds the PURE half: the F.3 table, the F.4
// budgets, Unpark. This file holds the half that actually decides whether the
// platform works - that the RECONCILER applies those clocks, refuses those
// transitions, and does not kill a Task for the crime of waiting in a queue.

// tsProject carries Phase=Ready but NO ReadySince, so it reads NOT stably
// ready. That used to be load-bearing by accident: the memory gate stopped
// every PodStartedAt==nil fixture short of ensureStagePod's
// ValidatePodSecretRefs. The gate is gone, so a fixture that reaches a pod
// stage really does build a pod - which is why tsPodConfig exists - and the
// only thing tsProject's missing ReadySince now decides is whether the turn
// carries the degraded appendix. tsStablyReadyProject is the healthy variant.
func tsProject(maxAgents int) *tatarav1alpha1.Project {
	return &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "proj", Namespace: mdNS},
		Spec: tatarav1alpha1.ProjectSpec{
			MaxConcurrentAgents: maxAgents,
			ScmSecretRef:        "scm-secret",
			Scm:                 &tatarav1alpha1.ScmSpec{Provider: "github", BotLogin: "tatara-bot"},
		},
		Status: tatarav1alpha1.ProjectStatus{
			Memory: &tatarav1alpha1.MemoryStatus{Phase: "Ready", Endpoint: "http://mem"},
		},
	}
}

// tsStablyReadyProject is tsProject with ReadySince backdated past
// memoryReadyStabilizationWindow, for tests that exercise an actual turn
// submission (reconcilePodStage's pre-SubmitTurn gate, issue #355) rather than
// admission queueing.
func tsStablyReadyProject(maxAgents int) *tatarav1alpha1.Project {
	p := tsProject(maxAgents)
	readySince := metav1.NewTime(time.Now().Add(-10 * time.Minute))
	p.Status.Memory.ReadySince = &readySince
	return p
}

// tsTask is a Task already at a state, with stateEnteredAt set.
func tsTask(name, kind, stg string, enteredAt time.Time) *tatarav1alpha1.Task {
	at := metav1.NewTime(enteredAt)
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: mdNS, UID: types.UID("uid-" + name)},
		Spec:       tatarav1alpha1.TaskSpec{Kind: kind, ProjectRef: "proj", Goal: "do the thing"},
		Status: tatarav1alpha1.TaskStatus{
			State:          stg,
			StateEnteredAt: &at,
			AgentKind:      stage.AgentKindFor(stg, kind),
		},
	}
}

// tsReconciler builds a TaskReconciler over the fake client. Session is a
// PANICKING one: no test in this file may reach turn submission by accident, and
// the review-Task test depends on that.
func tsReconciler(c client.Client) *TaskReconciler {
	return &TaskReconciler{
		Client:    c,
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Session:   panicSession{newFakeSession()},
		PodConfig: tsPodConfig(),
	}
}

// tsWorkingReconciler is tsReconciler with a session that ACCEPTS a turn. It is
// for the O3 tests, whose whole point is that the Task keeps working: a panicking
// session proves a Task never runs an agent, and proving the opposite needs a
// session that lets it.
func tsWorkingReconciler(c client.Client) *TaskReconciler {
	return &TaskReconciler{
		Client:    c,
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Session:   newFakeSession(),
		PodConfig: tsPodConfig(),
	}
}

// tsPodConfig carries the two secret names ValidatePodSecretRefs demands. Every
// fixture that reaches a pod stage needs it now that memory readiness no longer
// holds an un-spawned Task short of the pod build.
func tsPodConfig() agent.PodConfig {
	return agent.PodConfig{
		Namespace:           mdNS,
		AnthropicSecretName: "anthropic",
		CLIOIDCSecretName:   "tatara-cli-oidc",
	}
}

// panicSession is THE PANICKING POD FACTORY: a Task that must never run an agent
// must never reach a turn, and a test that only asserts on a counter cannot prove
// that.
type panicSession struct{ *fakeSession }

func (panicSession) SubmitTurn(_ context.Context, _, _, _ string) (string, error) {
	panic("a turn was submitted on a Task that must never run an agent")
}

func tsReconcile(t *testing.T, r *TaskReconciler, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, now time.Time) *tatarav1alpha1.Task {
	t.Helper()
	if _, err := r.reconcileStage(context.Background(), proj, task, now); err != nil {
		t.Fatalf("reconcileStage: %v", err)
	}
	return mdGetTask(t, r.Client, task.Name)
}

// F6-1 (3-replica HA): the wrapper pod is absent and the LIVE stage has moved off
// the stage this reconcile is acting on (a non-leader webhook transitioned +
// tore the pod down). ensureStagePod must NOT create a pod for the stale stage.
// Cached view: reviewing/no-pod; live reader: merging.
func TestEnsureStagePod_SkipsCreateWhenLiveStageMoved(t *testing.T) {
	cachedTask := tsTask("drift", "clarify", tatarav1alpha1.StateAwaitingReview, time.Now())
	proj := tsProject(3)
	cached := newMirrorClient(t, proj, mdSecret(), cachedTask)
	live := newMirrorClient(t, proj, mdSecret(),
		tsTask("drift", "clarify", tatarav1alpha1.StateMerged, time.Now()))
	r := tsReconciler(cached)
	r.APIReader = live

	skipped, err := r.ensureStagePod(context.Background(), proj, cachedTask)
	if err != nil {
		t.Fatalf("ensureStagePod: %v", err)
	}
	if !skipped {
		t.Fatal("ensureStagePod must report skipped=true so the caller early-returns instead of submitting a turn")
	}
	var pod corev1.Pod
	err = cached.Get(context.Background(),
		types.NamespacedName{Namespace: mdNS, Name: agent.PodName(cachedTask)}, &pod)
	if err == nil {
		t.Fatal("a pod was created for a stage the Task has live-left; the F6-1 guard did not fire")
	}
	if !apierrors.IsNotFound(err) {
		t.Fatalf("unexpected error checking for pod: %v", err)
	}
}

// liveStageDiffers: nil APIReader trusts the cache; equal live stage is not
// stale; a live stage moved off the acting stage is stale.
func TestLiveStageDiffers(t *testing.T) {
	acting := tsTask("d", "clarify", tatarav1alpha1.StateAwaitingReview, time.Now())
	proj := tsProject(3)
	r := tsReconciler(newMirrorClient(t, proj, acting))

	if r.liveStageDiffers(context.Background(), acting) {
		t.Fatal("nil APIReader must return false (trust the cache)")
	}
	r.APIReader = newMirrorClient(t, proj, tsTask("d", "clarify", tatarav1alpha1.StateAwaitingReview, time.Now()))
	if r.liveStageDiffers(context.Background(), acting) {
		t.Fatal("live stage equal to the acting stage must return false")
	}
	r.APIReader = newMirrorClient(t, proj, tsTask("d", "clarify", tatarav1alpha1.StateMerged, time.Now()))
	if !r.liveStageDiffers(context.Background(), acting) {
		t.Fatal("live stage moved off the acting stage must return true")
	}
}

// ---------------------------------------------------------------------------
// STEADY STATE. THE MOST IMPORTANT TEST IN THIS FILE (fixes V6-1, V7-7).
// ---------------------------------------------------------------------------

// A fourth Task queues 40 minutes behind three live agents at
// maxConcurrentAgents=3. It reaches its stage NORMALLY and IT DOES NOT
// TERMINATE. A previous round's "fix" killed every Task that ever queued, in
// normal steady state, because it measured the pod-readiness deadline (5m) from
// stageEnteredAt - which INCLUDES the admission queue.
func TestSteadyStateQueuedTaskDoesNotTerminate(t *testing.T) {
	entered := time.Now().Add(-40 * time.Minute)
	task := tsTask("queued", "clarify", tatarav1alpha1.StateRefined, entered)
	// It has no pod: it is waiting for an admission slot. podStartedAt == nil.
	proj := tsProject(3)
	c := newMirrorClient(t, proj, mdSecret(), task)
	r := tsReconciler(c)

	got := tsReconcile(t, r, proj, task, time.Now())

	if got.Status.State != tatarav1alpha1.StateRefined {
		t.Fatalf("stage = %q, want clarifying: a Task that queued 40m in normal steady state MUST NOT move",
			got.Status.State)
	}
	if got.Status.ParkReason != "" {
		t.Fatalf("stageReason = %q, want empty: queueing is not a fault", got.Status.ParkReason)
	}
	if tatarav1alpha1.TaskDone(got) || tatarav1alpha1.Parked(got) {
		t.Fatal("a queued Task was TERMINATED. This is the V6-1 regression; the fix is wrong, not the test")
	}
	// And the armed clock is CLOCK 1 (24h), not CLOCK 2 (5m).
	clock, _, budget, _ := stage.ArmedClock(got, false)
	if clock != stage.ClockAdmission || budget != tatarav1alpha1.AdmissionStarvedBudget {
		t.Fatalf("armed clock = %s/%s, want admission/24h", clock, budget)
	}
}

// H12: a Task that sat in the admission queue for 3 HOURS and then ran a pod is
// measured from stageWorkStartedAt, not stageEnteredAt. Its 2h investigating
// budget has NOT elapsed one minute after the pod became ready.
func TestWorkBudgetMeasuredFromPodReadyNotStageEntry(t *testing.T) {
	now := time.Now()
	task := tsTask("slow-queue", "incident", tatarav1alpha1.StateRefined, now.Add(-3*time.Hour))
	podAt := metav1.NewTime(now.Add(-2 * time.Minute))
	workAt := metav1.NewTime(now.Add(-1 * time.Minute))
	task.Status.PodStartedAt = &podAt
	task.Status.StateWorkStartedAt = &workAt
	proj := tsProject(3)
	c := newMirrorClient(t, proj, mdSecret(), task, tsReadyPod(task))
	r := tsReconciler(c)
	r.Session = newFakeSession() // this Task is ALLOWED to run: it is not a review Task

	got := tsReconcile(t, r, proj, task, now)

	if got.Status.State != tatarav1alpha1.StateRefined {
		t.Fatalf("stage = %q, want investigating: the 2h budget runs from stageWorkStartedAt (fix H12), not from the 3h queue wait",
			got.Status.State)
	}
}

// ---------------------------------------------------------------------------
// THE THREE CLOCKS (F.4). Gap 5: nothing drove them before.
// ---------------------------------------------------------------------------

// THE PAUSE CARVE-OUT SURVIVES THE RESIDENCY BACKSTOP. An earlier round of #521
// deleted the two admission cases below on the reasoning that residency runs
// BEFORE ArmedClock and dominates them. It does dominate them - and that was the
// bug, not the justification: residency parks at stage-deadline, which is
// UnparkNever, so a project paused for four hours shredded every queued Task
// into a 7-day death row. `paused` is the ONE deadline exception in the
// contract and it now gates residency for exactly the reason it gates clock 1.
func TestReconcilerAppliesTheThreeClocks(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name       string
		maxAgents  int
		mutate     func(*tatarav1alpha1.Task)
		stg        string
		entered    time.Duration // before now
		wantParked bool
		wantReason string
	}{
		{
			// A live state's IDLE clock (conversationLastEventAt), not a plain work
			// budget: #521 replaced the single `conversing` stage's mechanism with a
			// property every live state now shares. It elapses to park(awaiting-
			// human), never stage-deadline (that reason is reserved for the
			// residency backstop). entered is kept well under refined's 24h
			// residency cap so only the idle clock is in play here.
			name: "a live state's idle clock elapses", maxAgents: 3,
			stg: tatarav1alpha1.StateRefined, entered: 10 * time.Hour,
			mutate: func(tk *tatarav1alpha1.Task) {
				pod := metav1.NewTime(now.Add(-3 * time.Hour))
				work := metav1.NewTime(now.Add(-2 * time.Hour))
				lastEvent := metav1.NewTime(now.Add(-61 * time.Minute))
				tk.Status.PodStartedAt = &pod
				tk.Status.StateWorkStartedAt = &work
				tk.Status.ConversationLastEventAt = &lastEvent
				// The agent handed off, so the wait IS on a human. A live state whose
				// pod ended with no agent-authored handoff note re-arms instead
				// (#527 follow-up); that case is covered in livepods_no_handoff_test.go.
				tk.Status.Notes = []tatarav1alpha1.Note{{
					At: metav1.NewTime(now), Agent: "implement", Kind: agent.NoteKindHandoff,
					Body: "need a decision on the migration order before I continue",
				}}
			},
			wantParked: true, wantReason: stage.ReasonAwaitingHuman,
		},
		{
			// A POD-LESS, OPERATOR-DRIVEN stage runs CLOCK 3 from stateEnteredAt
			// (contradiction #5, the budget table wins). Without this merging NEVER
			// reaches merge-timeout and the bounded merge cycle never engages at
			// all. merged is not one of the three LIVE states, so the residency
			// backstop does not apply to it (see
			// TestReconcileClocks_ResidencyDoesNotApplyToOperatorDrivenStates).
			name: "podless merging reaches merge-timeout", maxAgents: 3,
			stg: tatarav1alpha1.StateMerged, entered: 5 * time.Hour,
			wantParked: true, wantReason: stage.ReasonMergeTimeout,
		},
		{
			name: "podless deploying reaches deploy-timeout", maxAgents: 3,
			stg: tatarav1alpha1.StateDeployed, entered: 3 * time.Hour,
			wantParked: true, wantReason: stage.ReasonDeployTimeout,
		},
		{
			// The 5m triage budget. `new` is operator triage, not live, so no
			// residency cap applies and this elapses to a plain park exactly as
			// before - triage-stalled is a PARK reason now, not a `failed` stage.
			name: "triaging stalls", maxAgents: 3,
			stg: tatarav1alpha1.StateNew, entered: 6 * time.Minute,
			wantParked: true, wantReason: stage.ReasonTriageStalled,
		},
		{
			// A LIVE state with NO POD is queueing for an admission slot, and it has
			// no StateWorkStartedAt: work never started (fix B3). Residency no
			// longer applies at all here - it is gated on StateWorkStartedAt != nil,
			// specifically so it cannot charge admission-queue time against the
			// live-state caps (6h/4h), which are far tighter than the 24h admission
			// budget clock 1 owns. On a RUNNING project that is genuine starvation
			// and it must still park, but through clock 1: admission-starved at its
			// 24h budget, not stage-deadline.
			name: "a queued live task on a running project parks", maxAgents: 3,
			stg: tatarav1alpha1.StateRefined, entered: 25 * time.Hour,
			wantParked: true, wantReason: stage.ReasonAdmissionStarved,
		},
		{
			// THE PAUSE CARVE-OUT. Restored from the pre-#521 table, where it read
			// "clock1 skipped while paused". `paused` exempts clock 1 directly
			// (stage.go), and this Task's residency does not even reach that
			// question: it has no StateWorkStartedAt, so residency never applies to
			// it regardless of pause state.
			name: "a queued live task is exempt while paused", maxAgents: 0,
			stg: tatarav1alpha1.StateUnderImplementation, entered: 30 * time.Hour,
			wantParked: false, wantReason: "",
		},
		{
			// The same Task on a RUNNING project is exempt from nothing: it has no
			// pod and no StateWorkStartedAt, so clock 1 (admission, 24h) owns it and
			// 30h is past that budget.
			name: "the same task parks once the project is running", maxAgents: 3,
			stg: tatarav1alpha1.StateUnderImplementation, entered: 30 * time.Hour,
			wantParked: true, wantReason: stage.ReasonAdmissionStarved,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := tsTask("t", "implement", tc.stg, now.Add(-tc.entered))
			if tc.mutate != nil {
				tc.mutate(task)
			}
			proj := tsProject(tc.maxAgents)
			c := newMirrorClient(t, proj, mdSecret(), task)
			got := tsReconcile(t, tsReconciler(c), proj, task, now)

			if got.Status.State != tc.stg {
				t.Fatalf("state = %q, want unchanged %q: park never moves state", got.Status.State, tc.stg)
			}
			if tatarav1alpha1.Parked(got) != tc.wantParked {
				t.Fatalf("parked = %v, want %v", tatarav1alpha1.Parked(got), tc.wantParked)
			}
			if got.Status.ParkReason != tc.wantReason {
				t.Fatalf("parkReason = %q, want %q", got.Status.ParkReason, tc.wantReason)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// THE ABSOLUTE RESIDENCY BACKSTOP, at the RECONCILER. internal/stage covers the
// predicate; these cover the thing that actually drives it. Without them the
// non-negotiable mitigation for the liveness generalisation had zero coverage at
// the only layer that can park a Task.
// ---------------------------------------------------------------------------

// residencyTask is a live Task with a POD UP and a FRESH conversation stamp, so
// the idle clock can never be what fires. Only residency can.
func residencyTask(state string, now time.Time, entered time.Duration) *tatarav1alpha1.Task {
	tk := tsTask("resident", "implement", state, now.Add(-entered))
	pod := metav1.NewTime(now.Add(-entered))
	work := metav1.NewTime(now.Add(-entered))
	last := metav1.NewTime(now.Add(-time.Minute))
	tk.Status.PodStartedAt = &pod
	tk.Status.StateWorkStartedAt = &work
	tk.Status.ConversationLastEventAt = &last
	return tk
}

// residencyClient seeds the fixture WITH ITS POD. O3 dropped the recreation gate
// from BudgetExit's podStoppedNoOutcome arm, so a live-state Task whose pod
// object is absent now parks no-outcome on the very first pass - which would
// mask every residency assertion below with the wrong park reason. A residency
// test is about the ABSOLUTE clock on a Task whose pod is up and fine.
func residencyClient(t *testing.T, proj *tatarav1alpha1.Project, tk *tatarav1alpha1.Task) client.Client {
	t.Helper()
	return newMirrorClient(t, proj, mdSecret(), tk, tsReadyPod(tk))
}

// THE ONE GENUINE REGRESSION in promoting liveness to a property, and its
// mandatory mitigation. A live state arms the IDLE clock, so a chatty
// under-implementation Task resets its deadline on every human comment and the
// 6h absolute bound the old `implementing` stage had is gone. This is that
// bound, restored as a separate check.
func TestReconcileClocks_ResidencyExceededParksALiveStateWhoseIdleClockKeepsResetting(t *testing.T) {
	now := time.Now()
	tk := residencyTask(tatarav1alpha1.StateUnderImplementation, now, 25*time.Hour)
	proj := tsProject(3)
	c := residencyClient(t, proj, tk)

	got := tsReconcile(t, tsReconciler(c), proj, tk, now)

	if !tatarav1alpha1.Parked(got) {
		t.Fatal("25h in under-implementation with a fresh idle clock did NOT park: the residency dead-man switch is not wired")
	}
	if got.Status.ParkReason != stage.ReasonStageDeadline {
		t.Fatalf("parkReason = %q, want %q", got.Status.ParkReason, stage.ReasonStageDeadline)
	}
	if got.Status.State != tatarav1alpha1.StateUnderImplementation {
		t.Fatalf("state = %q, want unchanged: a park does not move the Task", got.Status.State)
	}
}

// AND IT FIRES THROUGH AN IN-FLIGHT TURN. This is the pair to C1: the idle clock
// is disarmed while the agent works, so residency is the ONLY thing left holding
// a silently-working agent to a deadline.
func TestReconcileClocks_ResidencyFiresOnAnAgentThatNeverStopsWorking(t *testing.T) {
	now := time.Now()
	tk := residencyTask(tatarav1alpha1.StateUnderImplementation, now, 25*time.Hour)
	tk.Annotations = map[string]string{tatarav1alpha1.AnnCurrentTurn: "turn-1"}
	proj := tsProject(3)
	c := residencyClient(t, proj, tk)

	got := tsReconcile(t, tsReconciler(c), proj, tk, now)

	if !tatarav1alpha1.Parked(got) || got.Status.ParkReason != stage.ReasonStageDeadline {
		t.Fatalf("parked=%v reason=%q, want parked/%s: an agent mid-turn at 25h is past the 24h cap and residency is its only bound",
			tatarav1alpha1.Parked(got), got.Status.ParkReason, stage.ReasonStageDeadline)
	}
}

// AND A SEVEN-HOUR RUN SURVIVES. The old under-implementation cap was 6h, which
// is where a long healthy coding run died. This is the O3 half of the pair: the
// dead-man switch still exists, it is just no longer a work budget in disguise.
func TestReconcileClocks_ResidencyDoesNotFireOnALongHealthyRun(t *testing.T) {
	now := time.Now()
	tk := residencyTask(tatarav1alpha1.StateUnderImplementation, now, 7*time.Hour)
	tk.Annotations = map[string]string{tatarav1alpha1.AnnCurrentTurn: "turn-1"}
	proj := tsProject(3)
	c := residencyClient(t, proj, tk)

	// A WORKING reconciler: the point of the test is that the Task keeps going,
	// and a panicking session cannot express that.
	got := tsReconcile(t, tsWorkingReconciler(c), proj, tk, now)

	if tatarav1alpha1.Parked(got) {
		t.Fatalf("parked at %q: 7h of implement work is a long job, and the 6h cap that killed it is deleted",
			got.Status.ParkReason)
	}
}

func TestReconcileClocks_ResidencyIsCumulativeAcrossAParkRoundTrip(t *testing.T) {
	now := time.Now()
	tk := residencyTask(tatarav1alpha1.StateUnderImplementation, now, time.Hour)
	tk.Status.StageElapsedCarrySeconds = int((23*time.Hour + 30*time.Minute).Seconds())
	proj := tsProject(3)
	c := residencyClient(t, proj, tk)

	got := tsReconcile(t, tsReconciler(c), proj, tk, now)

	if !tatarav1alpha1.Parked(got) {
		t.Fatal("24h30m of CUMULATIVE residency exceeds the 24h cap; buying a fresh cap per re-entry is the unbounded-loop shape #480 killed")
	}
	if got.Status.ParkReason != stage.ReasonStageDeadline {
		t.Fatalf("parkReason = %q, want %q", got.Status.ParkReason, stage.ReasonStageDeadline)
	}
}

func TestReconcileClocks_ResidencyDoesNotFireUnderTheCap(t *testing.T) {
	now := time.Now()
	tk := residencyTask(tatarav1alpha1.StateUnderImplementation, now, 23*time.Hour)
	proj := tsProject(3)
	c := residencyClient(t, proj, tk)

	got := tsReconcile(t, tsWorkingReconciler(c), proj, tk, now)

	if tatarav1alpha1.Parked(got) {
		t.Fatalf("23h is UNDER the 24h cap and parked anyway, reason %q: the backstop must not be a hair trigger",
			got.Status.ParkReason)
	}
}

// merged has its own 4h WORK budget; residency must not double-park it with a
// second, different reason.
func TestReconcileClocks_ResidencyDoesNotApplyToOperatorDrivenStates(t *testing.T) {
	now := time.Now()
	tk := tsTask("resident", "implement", tatarav1alpha1.StateMerged, now.Add(-100*time.Hour))
	proj := tsProject(3)
	c := newMirrorClient(t, proj, mdSecret(), tk)

	got := tsReconcile(t, tsReconciler(c), proj, tk, now)

	if got.Status.ParkReason != stage.ReasonMergeTimeout {
		t.Fatalf("parkReason = %q, want %q: the work clock owns merged and residency must not shadow it",
			got.Status.ParkReason, stage.ReasonMergeTimeout)
	}
}

// THE PAUSE EXEMPTION, at the backstop. `paused` disarms clock 1 because the
// kill switch must not shred the backlog; residency needs the identical guard or
// the switch shreds it anyway, at stage-deadline, which never un-parks.
func TestReconcileClocks_ResidencyIsExemptWhileTheProjectIsPaused(t *testing.T) {
	now := time.Now()
	tk := residencyTask(tatarav1alpha1.StateAwaitingReview, now, 25*time.Hour)
	proj := tsProject(0) // maxConcurrentAgents == 0 is the pause
	c := residencyClient(t, proj, tk)

	got := tsReconcile(t, tsWorkingReconciler(c), proj, tk, now)

	if tatarav1alpha1.Parked(got) {
		t.Fatalf("a PAUSED project parked an awaiting-review Task at %q. stage-deadline is UnparkNever, so this is the pause kill switch acting as a backlog shredder",
			got.Status.ParkReason)
	}
}

// A PARKED Task is not resident: park folds its residency into the carry and
// takes the pod down, and the park clock (ParkRetention) owns it from there.
func TestReconcileClocks_ResidencyDoesNotFireOnAnAlreadyParkedTask(t *testing.T) {
	now := time.Now()
	tk := residencyTask(tatarav1alpha1.StateUnderImplementation, now, 25*time.Hour)
	parkedAt := metav1.NewTime(now.Add(-time.Hour))
	tk.Status.ParkReason = stage.ReasonAwaitingHuman
	tk.Status.ParkedAt = &parkedAt
	proj := tsProject(3)
	c := residencyClient(t, proj, tk)

	got := tsReconcile(t, tsReconciler(c), proj, tk, now)

	if got.Status.ParkReason != stage.ReasonAwaitingHuman {
		t.Fatalf("parkReason = %q, want unchanged %q: residency must not overwrite an existing park",
			got.Status.ParkReason, stage.ReasonAwaitingHuman)
	}
}

// somePodRecreations is "this Task has respawned a few times already". It used
// to be the maxPodRecreations constant, which O3 deleted: no gate anywhere reads
// a recreation count now, so the value is no longer load-bearing and the tests
// below assert that explicitly.
const somePodRecreations = 3

// O3: A RECREATION COUNT NO LONGER TERMINATES ANYTHING. This test used to assert
// that an exhausted budget parked pod-recreation-exhausted through the park choke
// point. There is no budget, so the Task respawns at ten recreations exactly as
// it does at one - and the counter still advances, because it is the churn
// alert's only input.
func TestRespawnLostPod_TenRecreationsStillRespawns(t *testing.T) {
	now := time.Now()
	task := tsTask("lost-pod", "implement", tatarav1alpha1.StateUnderImplementation, now.Add(-time.Hour))
	pod := metav1.NewTime(now.Add(-30 * time.Minute))
	work := metav1.NewTime(now.Add(-29 * time.Minute))
	task.Status.PodStartedAt = &pod
	task.Status.StateWorkStartedAt = &work
	task.Status.Stats.PodRecreations = 10
	proj := tsProject(3)
	r := tsReconciler(newMirrorClient(t, proj, mdSecret(), task))

	if _, err := r.respawnLostPod(context.Background(), proj, task, obs.RecreationReasonPodGone, now); err != nil {
		t.Fatalf("respawnLostPod: %v", err)
	}

	got := mdGetTask(t, r.Client, task.Name)
	if tatarav1alpha1.Parked(got) {
		t.Fatalf("parked at %q: O3 deleted maxPodRecreations, so 10 recreations must still respawn",
			got.Status.ParkReason)
	}
	if got.Status.Stats.PodRecreations != 11 {
		t.Fatalf("podRecreations = %d, want 11: the count feeds the churn alert and must not stop with the cap",
			got.Status.Stats.PodRecreations)
	}
	if got.Status.PodStartedAt != nil || got.Status.StateWorkStartedAt != nil {
		t.Fatal("the pod clocks must be nil so a replacement pod re-stamps them")
	}
}

// pod-not-ready IS NOT A STAGE REASON. It was never a terminal state - it was a
// respawn trigger wearing a terminal's name - and it must appear NOWHERE.
func TestPodNotReadyIsNotAStageReason(t *testing.T) {
	for _, r := range stage.Reasons {
		if r == "pod-not-ready" {
			t.Fatal("pod-not-ready is in the F.5 closed set. A never-Ready pod RESPAWNS; the terminal is pod-recreation-exhausted")
		}
	}
	task := tsTask("t", "implement", tatarav1alpha1.StateUnderImplementation, time.Now())
	err := stage.Enter(task, nil, tatarav1alpha1.StateRejected, "pod-not-ready", time.Now())
	if err == nil {
		t.Fatal("stage.Enter accepted rejected(pod-not-ready): it is not in the F.5 closed set")
	}
	if _, ok := err.(*stage.UnknownReasonError); !ok {
		t.Fatalf("err = %T, want *stage.UnknownReasonError", err)
	}
}

// ---------------------------------------------------------------------------
// THE CAPS (F.4).
// ---------------------------------------------------------------------------

// O3: A 400-TURN TASK IS A LONG JOB, NOT A STALLED ONE. This used to park
// turn-budget-exhausted at 300; the ceiling is deleted and the Task keeps working.
func TestFourHundredTurnsDoesNotPark(t *testing.T) {
	now := time.Now()
	task := tsTask("burner", "implement", tatarav1alpha1.StateUnderImplementation, now.Add(-time.Minute))
	podAt := metav1.NewTime(now.Add(-time.Minute))
	workAt := metav1.NewTime(now.Add(-30 * time.Second))
	task.Status.PodStartedAt = &podAt
	task.Status.StateWorkStartedAt = &workAt
	task.Status.Stats.Turns = 400
	proj := tsProject(3)
	c := newMirrorClient(t, proj, mdSecret(), task, tsReadyPod(task))

	got := tsReconcile(t, tsWorkingReconciler(c), proj, task, now)

	if tatarav1alpha1.Parked(got) {
		t.Fatalf("parked at %q: 400 turns must not terminate a Task after O3", got.Status.ParkReason)
	}
	if got.Status.State != tatarav1alpha1.StateUnderImplementation {
		t.Fatalf("state = %q, want under-implementation", got.Status.State)
	}
}

// A pod that RAN and vanished with no outcome parks at no-outcome, and since O3
// it does so with NO recreation gate: podStoppedNoOutcome is a fact about this
// Task right now, not a count of how much it has done. A pod that never ran
// (podStartedAt == nil - the admission queue) is NOT this, and that distinction
// is the whole of the V6-1 fix.
func TestPodStoppedWithNoOutcomeParksWithNoRecreationGate(t *testing.T) {
	now := time.Now()
	task := tsTask("lost", "implement", tatarav1alpha1.StateUnderImplementation, now.Add(-time.Hour))
	podAt := metav1.NewTime(now.Add(-30 * time.Minute))
	workAt := metav1.NewTime(now.Add(-29 * time.Minute))
	task.Status.PodStartedAt = &podAt
	task.Status.StateWorkStartedAt = &workAt
	task.Status.Stats.PodRecreations = 0
	proj := tsProject(3)
	// No Pod object: the pod is GONE.
	c := newMirrorClient(t, proj, mdSecret(), task)

	got := tsReconcile(t, tsReconciler(c), proj, task, now)

	if got.Status.State != tatarav1alpha1.StateUnderImplementation || !tatarav1alpha1.Parked(got) ||
		got.Status.ParkReason != stage.ReasonNoOutcome {
		t.Fatalf("state=%q parked=%v reason=%q, want under-implementation/parked/no-outcome",
			got.Status.State, tatarav1alpha1.Parked(got), got.Status.ParkReason)
	}
}

// ---------------------------------------------------------------------------
// A kind=review Task NEVER reaches implementing. By ANY path.
// ---------------------------------------------------------------------------

// fix V7-1 / V6-3. request_changes on a kind=review Task is the review agent's
// NORMAL verdict on a bad HUMAN pull request, and it was the PRIMARY path into
// an implement pod spawning against someone else's PR with no Issue, no approval
// evidence, and no C.6 gate anywhere in its history. It lands in
// parked(awaiting-human): the human fixes their own PR.
func TestRequestChangesOnAReviewTaskParksAwaitingHuman(t *testing.T) {
	task := tsTask("rev", "review", tatarav1alpha1.StateAwaitingReview, time.Now())
	edge, ok := stage.RequestChanges(task)
	if !ok {
		t.Fatal("RequestChanges returned no edge")
	}
	if edge.To != stage.ParkTarget || edge.Reason != stage.ReasonAwaitingHuman {
		t.Fatalf("edge = %s(%s), want (park)(awaiting-human)", edge.To, edge.Reason)
	}
}

// THE EMPTY SET IS NOT A LICENCE. A review Task owns ZERO Issues, and no
// universal quantifier over an empty set may ever gate code execution. The
// choke point REFUSES the transition, no pod is created, and the illegal-edge
// counter fires. The Session panics if a turn is ever submitted.
//
// Every fixture is ALSO parked(awaiting-human) (#521: park is orthogonal to
// state, so "can this kind=review Task ever reach implementing/merging" must
// hold whether it is parked or not) - stage.Enter refuses ANY transition off a
// parked Task before it ever reaches the kind guard, so this proves the
// refusal holds doubly over for every one of these origin states.
func TestReviewTaskCanNeverEnterImplementingOrMerging(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	for _, from := range []string{
		tatarav1alpha1.StateAwaitingReview,
		tatarav1alpha1.StateDeployed,
		tatarav1alpha1.StateRefined,
		tatarav1alpha1.StateNew,
	} {
		for _, to := range []string{tatarav1alpha1.StateUnderImplementation, tatarav1alpha1.StateMerged} {
			task := tsTask("rev", "review", from, now)
			task.Status.ParkReason = stage.ReasonAwaitingHuman
			proj := tsProject(3)
			c := newMirrorClient(t, proj, mdSecret(), task)
			r := tsReconciler(c)

			before := illegalCount(t, obs.IllegalStageTransitionCounter(from, to))
			err := r.enter(ctx, proj, task, nil, to, "", now)
			if err == nil {
				t.Fatalf("%s -> %s was ACCEPTED on a kind=review Task. There is no path, no condition, no exception", from, to)
			}
			got := mdGetTask(t, c, task.Name)
			if got.Status.State != from {
				t.Fatalf("the refused transition was WRITTEN anyway: stage = %q", got.Status.State)
			}
			if after := illegalCount(t, obs.IllegalStageTransitionCounter(from, to)); after <= before {
				t.Fatalf("operator_illegal_stage_transition_total{%s,%s} did not fire", from, to)
			}
			pods := &corev1.PodList{}
			if err := c.List(ctx, pods, client.InNamespace(mdNS)); err != nil {
				t.Fatalf("list pods: %v", err)
			}
			if len(pods.Items) != 0 {
				t.Fatalf("a pod was created for a review Task heading to %s", to)
			}
		}
	}
}

// TestTriageNeverRoutesPastTheApprovalGate is the #521 update of the OLD
// "triageTarget has no implement row" guarantee. Under the merged model EVERY
// code-bearing kind - implement INCLUDED - starts at refined (triageTarget's
// own doc comment: "That is not a hole in the approval gate: refined is where
// the gate RUNS"). The old machine reached code through
// clarifying -> approved -> implementing and had to refuse kind=implement at
// triage, or a Task minted there would have skipped the gate; under the
// merged model the SAME agent brainstorms, is approved and implements, so the
// gate survives as the ONE edge out of refined into under-implementation
// (restapi's submit_outcome(action=approved) grant), not as a triage refusal.
// What this test still pins: no kind's triage target is under-implementation
// or merged directly - every code-bearing kind lands in front of the gate,
// never past it.
func TestTriageNeverRoutesPastTheApprovalGate(t *testing.T) {
	want := map[string]string{
		"brainstorm": tatarav1alpha1.StateRefined,
		"implement":  tatarav1alpha1.StateRefined,
		"incident":   tatarav1alpha1.StateRefined,
		"refine":     tatarav1alpha1.StateRefined,
		"takeover":   tatarav1alpha1.StateRefined,
		"review":     tatarav1alpha1.StateAwaitingReview,
	}
	for kind, wantStage := range want {
		got, ok := triageTarget(kind)
		if !ok || got != wantStage {
			t.Fatalf("triageTarget(%q) = %q,%v; want %q", kind, got, ok, wantStage)
		}
		if wantStage == tatarav1alpha1.StateUnderImplementation {
			t.Fatalf("triageTarget(%q) lands past the approval gate", kind)
		}
		if !stage.LegalFor(tsTask("t", kind, tatarav1alpha1.StateNew, time.Now()), nil,
			tatarav1alpha1.StateNew, wantStage) {
			t.Fatalf("triaging -> %s is not a legal edge for kind %q", wantStage, kind)
		}
	}
}

// EVERY triage target must be a LEGAL edge, and the documentation row proved
// that was not machine-checked: it returned under-implementation, which is not
// in the table's `new` row at all, so a documentation Task reaching triage would
// have errored on every reconcile forever. The nightly batch never reaches
// triage - docbatch.go mints it at under-implementation through the CREATE edge -
// so the row was dead code that lied. Its absence is now the assertion.
func TestTriageTargetIsAlwaysALegalEdgeOutOfNew(t *testing.T) {
	if _, ok := triageTarget(stage.AgentDocumentation); ok {
		t.Fatal("triageTarget has a documentation row again: new -> under-implementation is not in the table and never will be")
	}
	for _, kind := range []string{"brainstorm", "implement", "incident", "refine", "takeover", "review", "documentation", "upgrade", "nonsense"} {
		target, ok := triageTarget(kind)
		if !ok {
			continue // no row: reconcileTriaging parks at triage-stalled, which is legal from anywhere
		}
		if !stage.LegalFor(tsTask("t", kind, tatarav1alpha1.StateNew, time.Now()), nil,
			tatarav1alpha1.StateNew, target) {
			t.Fatalf("triageTarget(%q) = %q, which stage.LegalFor refuses out of new", kind, target)
		}
	}
}

// ---------------------------------------------------------------------------
// THE CHOKE POINT.
// ---------------------------------------------------------------------------

// EVERY illegal (from, to) pair in the F.3 table is refused, counted, and NOT
// written. This is the table test contract Section I demands, on the reconciler
// side: the pure package can prove the table says no; only this can prove the
// operator OBEYS it.
func TestEveryIllegalTransitionIsRefusedAndCounted(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	stages := stage.AllStates()

	refused := 0
	for _, from := range stages {
		for _, to := range stages {
			if from == to {
				// Self-transitions are a silent no-op (issue #403), covered by
				// TestSelfTransitionIsANoOpNotCounted, not an illegal-and-counted
				// edge: they never appear in the F.3 table (stage.Legal(x,x) is
				// always false), so without this skip every one of the 16 stages
				// would be double-counted as "illegal" here too.
				continue
			}
			if stage.Legal(from, to) {
				continue
			}
			task := tsTask("t", "implement", from, now)
			proj := tsProject(3)
			c := newMirrorClient(t, proj, mdSecret(), task)
			r := tsReconciler(c)

			before := illegalCount(t, obs.IllegalStageTransitionCounter(from, to))
			if err := r.enter(ctx, proj, task, nil, to, stage.ReasonOperatorError, now); err == nil {
				t.Fatalf("illegal transition %s -> %s was ACCEPTED", from, to)
			}
			if got := mdGetTask(t, c, task.Name); got.Status.State != from {
				t.Fatalf("illegal transition %s -> %s was WRITTEN: stage = %q", from, to, got.Status.State)
			}
			if after := illegalCount(t, obs.IllegalStageTransitionCounter(from, to)); after != before+1 {
				t.Fatalf("operator_illegal_stage_transition_total{%s,%s} = %v, want +1", from, to, after-before)
			}
			refused++
		}
	}
	if refused == 0 {
		t.Fatal("the F.3 table has no illegal pairs at all; the table test is vacuous")
	}
}

// TestSelfTransitionIsANoOpNotCounted proves issue #403's fix: a to==from
// EnterStage call is a silent no-op, not a refused-and-counted illegal edge.
// stage.Enter's side effects (re-stamping StageEnteredAt, clearing
// PodStartedAt/PodRecreations) are non-idempotent (ref #348, no self-edges
// added to the F.3 table), so re-applying them on a same-stage re-entry would
// corrupt clocks already in flight; the fix must short-circuit before any of
// that runs.
func TestSelfTransitionIsANoOpNotCounted(t *testing.T) {
	ctx := context.Background()
	// time.Unix (not time.Now) deliberately: metav1.Time round-trips through
	// the fake client's JSON-based tracker at second precision with no
	// monotonic reading, same as a real apiserver; a sub-second/monotonic
	// "now" would make the unchanged-clock assertions below flaky.
	now := time.Unix(50000, 0)
	enteredAt := now.Add(-time.Hour)
	podAt := now.Add(-30 * time.Minute)

	for _, stg := range stage.AllStates() {
		t.Run(stg, func(t *testing.T) {
			task := tsTask("t", "implement", stg, enteredAt)
			if stage.AgentKindFor(stg, "implement") != "" {
				pa := metav1.NewTime(podAt)
				task.Status.PodStartedAt = &pa
			}
			proj := tsProject(3)
			c := newMirrorClient(t, proj, mdSecret(), task)
			r := tsReconciler(c)

			before := illegalCount(t, obs.IllegalStageTransitionCounter(stg, stg))
			if err := r.enter(ctx, proj, task, nil, stg, stage.ReasonOperatorError, now); err != nil {
				t.Fatalf("self-transition %s -> %s returned error: %v", stg, stg, err)
			}
			if after := illegalCount(t, obs.IllegalStageTransitionCounter(stg, stg)); after != before {
				t.Fatalf("operator_illegal_stage_transition_total{%s,%s} = %v, want unchanged", stg, stg, after-before)
			}

			got := mdGetTask(t, c, task.Name)
			if got.Status.StateEnteredAt == nil || !got.Status.StateEnteredAt.Time.Equal(enteredAt) {
				t.Fatalf("StageEnteredAt was re-stamped on self-transition %s -> %s: got %v, want unchanged %v",
					stg, stg, got.Status.StateEnteredAt, enteredAt)
			}
			if stage.AgentKindFor(stg, "implement") != "" {
				if got.Status.PodStartedAt == nil || !got.Status.PodStartedAt.Time.Equal(podAt) {
					t.Fatalf("PodStartedAt was cleared/re-stamped on self-transition %s -> %s: got %v, want unchanged %v",
						stg, stg, got.Status.PodStartedAt, podAt)
				}
			}
		})
	}
}

// TestEnterStage_StaleCacheSelfIllegalIsNoOp is the actual production bug Fix 3
// targets: the caller's in-memory Task copy is stale (still refined) because
// another writer already committed the SAME target state (under-
// implementation). The fresh read inside objbudget.FitTask sees that
// committed state, and stage.Enter's OWN from-derivation (fresh.Status.State)
// reports a from==to edge as illegal - this must still be a silent no-op, not
// a counted illegal transition. Modeled on
// TestReconcileStage_PodStageCapsAreIdempotentAgainstAStaleCache
// (task_controller_test.go).
//
// #521: the OLD fixture raced two writers onto `parked`, which was itself a
// STAGE in the 16-stage machine. Park is a flag now, never an EnterStage
// target, so the race this test proves EnterStage tolerates is instead two
// writers converging on the SAME real state (under-implementation).
func TestEnterStage_StaleCacheSelfIllegalIsNoOp(t *testing.T) {
	before := illegalCount(t, obs.IllegalStageTransitionCounter(tatarav1alpha1.StateUnderImplementation, tatarav1alpha1.StateUnderImplementation))

	proj := tsProject(3)
	now := time.Unix(20000, 0)
	entered := metav1.NewTime(now.Add(-10 * time.Minute))

	// The caller's IN-MEMORY copy: still refined.
	staleCopy := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "t-stale", Namespace: mdNS, UID: types.UID("uid-t-stale")},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "proj", Kind: "implement"},
		Status: tatarav1alpha1.TaskStatus{
			State:          tatarav1alpha1.StateRefined,
			AgentKind:      stage.AgentImplement,
			StateEnteredAt: &entered,
		},
	}
	// THE API SERVER: another writer already committed this Task to
	// under-implementation.
	live := staleCopy.DeepCopy()
	live.Status.State = tatarav1alpha1.StateUnderImplementation

	c := newMirrorClient(t, proj, mdSecret(), live)

	err := EnterStage(context.Background(), c, nil, obs.NewOperatorMetrics(prometheus.NewRegistry()),
		staleCopy, nil, tatarav1alpha1.StateUnderImplementation, "", now, nil)
	require.NoError(t, err, "a stale-cache self-illegal race must be a no-op, not an error")

	after := illegalCount(t, obs.IllegalStageTransitionCounter(tatarav1alpha1.StateUnderImplementation, tatarav1alpha1.StateUnderImplementation))
	require.Equal(t, before, after,
		"the stale-cache self-illegal race must not bump operator_illegal_stage_transition_total")

	got := mdGetTask(t, c, "t-stale")
	if got.Status.State != tatarav1alpha1.StateUnderImplementation {
		t.Fatalf("persisted state got re-stamped: %+v", got.Status)
	}
}

// EVERY transition clears BOTH pod timestamps (fix V7-4). v6 forgot podStartedAt
// and it is load-bearing: a stale one leaves the Task under NO CLOCK while it
// queues on a re-entry edge, and TTL-stops its next pod before that pod's first
// turn.
func TestEveryTransitionClearsBothPodClocksAndResetsRecreations(t *testing.T) {
	now := time.Now()
	task := tsTask("t", "implement", tatarav1alpha1.StateUnderImplementation, now.Add(-time.Hour))
	podAt := metav1.NewTime(now.Add(-30 * time.Minute))
	workAt := metav1.NewTime(now.Add(-29 * time.Minute))
	task.Status.PodStartedAt = &podAt
	task.Status.StateWorkStartedAt = &workAt
	task.Status.Stats.PodRecreations = 2
	proj := tsProject(3)
	c := newMirrorClient(t, proj, mdSecret(), task)
	r := tsReconciler(c)

	// #521: this is stage.Enter's own clearing (fix V7-4), which a PARK no longer
	// goes through at all - stage.Park does not touch pod stamps (they are torn
	// down by ParkTask's DeleteWrapper, not zeroed on Status, and only Unpark's
	// reArm clears them on resume). So the genuine STATE transition this fix
	// covers is exercised here directly, off a real edge.
	if err := r.enter(context.Background(), proj, task, nil, tatarav1alpha1.StateAwaitingReview,
		"", now); err != nil {
		t.Fatalf("enter: %v", err)
	}
	got := mdGetTask(t, c, task.Name)
	if got.Status.PodStartedAt != nil {
		t.Fatal("podStartedAt survived a transition (fix V7-4). The next pod is TTL-stopped before its first turn")
	}
	if got.Status.StateWorkStartedAt != nil {
		t.Fatal("stageWorkStartedAt survived a transition")
	}
	if got.Status.Stats.PodRecreations != 0 {
		t.Fatalf("podRecreations = %d after a transition, want 0", got.Status.Stats.PodRecreations)
	}
	if got.Status.StateEnteredAt == nil || !got.Status.StateEnteredAt.Time.Equal(now.UTC().Truncate(time.Second)) {
		// metav1.Time truncates to the second; compare at that resolution.
		if got.Status.StateEnteredAt == nil {
			t.Fatal("stageEnteredAt was not stamped")
		}
	}
	// The caller's in-memory copy follows the write.
	if task.Status.State != tatarav1alpha1.StateAwaitingReview || task.Status.PodStartedAt != nil {
		t.Fatal("the choke point did not update the caller's copy")
	}
}

// D1: EVERY terminal entry fires operator_task_terminal_total{kind,state,
// stateReason}. Twenty-nine tatara-observability rules ride on it, and it is the
// ONLY counter of terminal outcomes the platform has. A MINT is not an outcome.
//
// #521 narrowed the terminal set to {done, rejected}: `failed` and `parked` are
// GONE as states, so a park never fires this metric at all any more - see the
// "a park never fires the D1 metric" case below, and
// operator_task_parked_total (TestTaskParkedFiresOnlyOnARealParkTransition) for
// where a stall is counted instead.
func TestTerminalEntryFiresTheD1Metric(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	tests := []struct {
		name             string
		from, to, reason string
		kind             string
		wantFire         bool
	}{
		{"rejected", tatarav1alpha1.StateRefined, tatarav1alpha1.StateRejected, stage.ReasonDeclined, "refine", true},
		{"delivered", tatarav1alpha1.StateRefined, tatarav1alpha1.StateDone, "", "brainstorm", true},
		{"non-terminal", tatarav1alpha1.StateNew, tatarav1alpha1.StateRefined, "", "brainstorm", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := tsTask("t", tc.kind, tc.from, now)
			proj := tsProject(3)
			c := newMirrorClient(t, proj, mdSecret(), task)
			reg := prometheus.NewRegistry()
			r := tsReconciler(c)
			r.Metrics = obs.NewOperatorMetrics(reg)

			if err := r.enter(ctx, proj, task, nil, tc.to, tc.reason, now); err != nil {
				t.Fatalf("enter %s -> %s: %v", tc.from, tc.to, err)
			}
			got := terminalCount(t, reg, tc.kind, tc.to, tc.reason)
			if tc.wantFire && got != 1 {
				t.Fatalf("operator_task_terminal_total{%s,%s,%s} = %v, want 1", tc.kind, tc.to, tc.reason, got)
			}
			if !tc.wantFire && got != 0 {
				t.Fatalf("operator_task_terminal_total fired on a NON-terminal transition")
			}
		})
	}

	t.Run("a park never fires the D1 metric", func(t *testing.T) {
		task := tsTask("t-park-d1", "implement", tatarav1alpha1.StateUnderImplementation, now)
		proj := tsProject(3)
		c := newMirrorClient(t, proj, mdSecret(), task)
		reg := prometheus.NewRegistry()
		r := tsReconciler(c)
		r.Metrics = obs.NewOperatorMetrics(reg)

		if err := r.park(ctx, proj, task, stage.ReasonImplementDeclined, now); err != nil {
			t.Fatalf("park: %v", err)
		}
		if got := terminalCount(t, reg, "implement", tatarav1alpha1.StateUnderImplementation, stage.ReasonImplementDeclined); got != 0 {
			t.Fatalf("a PARK fired operator_task_terminal_total (%v)", got)
		}
	})
}

// A MINT is not an outcome. The sweep mints a Task straight into
// parked(backlog-sweep): it never ran and never failed - it is the durable owner
// of an Issue CR at zero agent cost. Counting it as a terminal drowns the D1
// terminal-rate alerts in Tasks that never did anything. Direct unit test of
// TaskTerminalEntry's own from=="" mint exclusion, off a `new` create-edge
// target (the real one MintStage/the create-edge choose - #521 has no `parked`
// state to target at all).
func TestMintingParkedBacklogSweepDoesNotFireTheTerminalMetric(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := obs.NewOperatorMetrics(reg)
	m.TaskTerminalEntry("clarify", "", tatarav1alpha1.StateNew, stage.ReasonBacklogSweep)
	if got := terminalCount(t, reg, "clarify", tatarav1alpha1.StateNew, stage.ReasonBacklogSweep); got != 0 {
		t.Fatalf("a MINT fired operator_task_terminal_total (%v)", got)
	}
}

// Contract K.1: operator_task_parked_total increments once per park, labelled
// by the STATE the Task parked FROM (the stalling state) and the park reason.
func TestTaskParkedFiresOnlyOnARealParkTransition(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	t.Run("a real park increments the counter", func(t *testing.T) {
		task := tsTask("t-park", "implement", tatarav1alpha1.StateUnderImplementation, now)
		proj := tsProject(3)
		c := newMirrorClient(t, proj, mdSecret(), task)
		reg := prometheus.NewRegistry()
		r := tsReconciler(c)
		r.Metrics = obs.NewOperatorMetrics(reg)

		if err := r.park(ctx, proj, task, stage.ReasonImplementDeclined, now); err != nil {
			t.Fatalf("park: %v", err)
		}
		got := testutil.ToFloat64(r.Metrics.TaskParkedCounter(tatarav1alpha1.StateUnderImplementation, stage.ReasonImplementDeclined))
		if got != 1 {
			t.Fatalf("operator_task_parked_total{under-implementation,%s} = %v, want 1", stage.ReasonImplementDeclined, got)
		}
	})

	// A MINT IS NOT AN OUTCOME, and #521 nearly lost that. The old machine got
	// the exclusion free from EnterStage's `prev != ""` gate, because its
	// `-> parked` create edge was a single from=="" write. #521 made the park a
	// flag applied ALONGSIDE the state, so by the time the park runs
	// status.state is already "new" and a `prev != ""` gate no longer excludes
	// anything - which would have drowned the park-rate alert in the 52 backlog
	// owners that never ran and never gave up. reconcileStage therefore mints
	// through MintParked, which applies both in ONE status write and emits no
	// park counter at all. This test is that exclusion; the atomicity it rides
	// on is TestCreateEdge_MintsParkedInOneStatusWrite.
	t.Run("a genuine sweep mint into parked(backlog-sweep) does NOT count as a park", func(t *testing.T) {
		task := &tatarav1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{Name: "t-mint", Namespace: mdNS, UID: types.UID("uid-t-mint")},
			Spec: tatarav1alpha1.TaskSpec{
				Kind: "refine", ProjectRef: "proj", Goal: "do the thing",
				InitialState:      tatarav1alpha1.StateNew,
				InitialParkReason: stage.ReasonBacklogSweep,
			},
		}
		proj := tsProject(3)
		c := newMirrorClient(t, proj, mdSecret(), task)
		reg := prometheus.NewRegistry()
		r := tsReconciler(c)
		r.Metrics = obs.NewOperatorMetrics(reg)

		if _, err := r.reconcileStage(ctx, proj, task, now); err != nil {
			t.Fatalf("reconcileStage (mint): %v", err)
		}
		gotTask := mdGetTask(t, c, "t-mint")
		if !tatarav1alpha1.Parked(gotTask) || gotTask.Status.ParkReason != stage.ReasonBacklogSweep {
			t.Fatalf("mint did not land parked(backlog-sweep): %+v", gotTask.Status)
		}
		if v := testutil.ToFloat64(r.Metrics.TaskParkedCounter(tatarav1alpha1.StateNew, stage.ReasonBacklogSweep)); v != 0 {
			t.Fatalf("operator_task_parked_total{new,backlog-sweep} = %v after a genuine sweep mint, want 0: a mint is not an outcome", v)
		}
	})

	t.Run("a non-parked entry does not increment the counter", func(t *testing.T) {
		task := tsTask("t-nopark", "implement", tatarav1alpha1.StateNew, now)
		proj := tsProject(3)
		c := newMirrorClient(t, proj, mdSecret(), task)
		reg := prometheus.NewRegistry()
		r := tsReconciler(c)
		r.Metrics = obs.NewOperatorMetrics(reg)

		if err := r.enter(ctx, proj, task, nil, tatarav1alpha1.StateRefined, "", now); err != nil {
			t.Fatalf("enter: %v", err)
		}
		mfs, err := reg.Gather()
		if err != nil {
			t.Fatalf("gather: %v", err)
		}
		for _, mf := range mfs {
			if mf.GetName() == "operator_task_parked_total" && len(mf.GetMetric()) != 0 {
				t.Fatalf("operator_task_parked_total has series after a non-parked entry: %v", mf)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// F.4's real invariant: NO STAGE WITHOUT AN EXIT.
// ---------------------------------------------------------------------------

// Every member of the F.1 enum has a budget row AND an onElapse row, and the
// RECONCILER can reach that exit: ArmedClock never returns ClockNone for a
// non-parked stage that has a clock to arm. A stage with no exit is a Task that
// sits there forever, worked by nobody.
func TestEveryStageHasAReachableExit(t *testing.T) {
	now := time.Now()
	for _, stg := range stage.AllStates() {
		budget, ok := stage.Budget(stg)
		if !ok {
			t.Fatalf("stage %q has NO ROW in the F.4 deadline table", stg)
		}
		edge, ok := stage.OnElapse(stg)
		if !ok || edge.To == "" {
			t.Fatalf("stage %q has no onElapse edge", stg)
		}
		task := tsTask("t", "implement", stg, now.Add(-budget-time.Hour))
		// #521: `parked` is GONE as a member of stage.AllStates() (park is a flag
		// orthogonal to state now), so there is no member of this loop to stamp a
		// ParkReason on any more - the ONE exemption (parked(backlog-sweep) never
		// ages out) is proven separately below, off a REAL park built with
		// stage.Park rather than a pseudo-state.
		if stage.Live(stg) {
			// Arm the pod stamps AND the idle clock: a live state's clock is the
			// IDLE clock (conversationLastEventAt), not stageWorkStartedAt (#521
			// dissolved the old single `conversing` stage into this property over
			// all three live states). A pod stage with no stamps at all is CLOCK 1
			// (admission), whose exit is admission-starved, not the state's own
			// idle budget.
			pod := metav1.NewTime(now.Add(-budget - time.Hour))
			work := metav1.NewTime(now.Add(-budget - time.Hour))
			lastEvent := metav1.NewTime(now.Add(-budget - time.Hour))
			task.Status.PodStartedAt = &pod
			task.Status.StateWorkStartedAt = &work
			task.Status.ConversationLastEventAt = &lastEvent
		}
		clock, _, _, got := stage.ArmedClock(task, false)
		if clock == stage.ClockNone {
			t.Fatalf("stage %q arms NO CLOCK: it has no exit deadline at all", stg)
		}
		if got.To != edge.To {
			t.Fatalf("stage %q: ArmedClock edge %q != OnElapse edge %q", stg, got.To, edge.To)
		}
		// And it has actually ELAPSED: an exit you cannot reach is not an exit.
		if _, fired := stage.Elapsed(task, false, now); !fired {
			t.Fatalf("stage %q: its own budget (%s) does not fire even an hour past it", stg, budget)
		}
	}
	// The ONE exemption, and it is a REASON, not a stage: parked(backlog-sweep)
	// on a REAL park (#521 has no `parked` state to build a fixture at directly).
	sweep := tsTask("t", "refine", tatarav1alpha1.StateNew, now.Add(-5*365*24*time.Hour))
	if err := stage.Park(sweep, stage.ReasonBacklogSweep, now); err != nil {
		t.Fatalf("stage.Park: %v", err)
	}
	if clock, _, _, _ := stage.ArmedClock(sweep, false); clock != stage.ClockNone {
		t.Fatalf("parked(backlog-sweep) armed clock %s; it consumes nothing and NEVER ages out", clock)
	}
}

// The named F.4 case: podStartedAt == nil AND stageWorkStartedAt == nil is
// CLOCK 1. It is a case, not an inference.
func TestNoStampsIsClock1(t *testing.T) {
	task := tsTask("t", "implement", tatarav1alpha1.StateUnderImplementation, time.Now())
	if task.Status.PodStartedAt != nil || task.Status.StateWorkStartedAt != nil {
		t.Fatal("fixture is wrong")
	}
	clock, since, budget, edge := stage.ArmedClock(task, false)
	if clock != stage.ClockAdmission {
		t.Fatalf("clock = %s, want admission", clock)
	}
	if !since.Equal(task.Status.StateEnteredAt.Time) {
		t.Fatal("CLOCK 1 must measure from stageEnteredAt")
	}
	if budget != tatarav1alpha1.AdmissionStarvedBudget {
		t.Fatalf("budget = %s, want 24h", budget)
	}
	if edge.Reason != stage.ReasonAdmissionStarved {
		t.Fatalf("edge reason = %q, want admission-starved", edge.Reason)
	}
}

// ---------------------------------------------------------------------------
// B2: THE POD-LIVENESS CAPS ARE BLIND TO A COMMITTED OUTCOME.
//
// kind=review is the ONLY outcome kind whose commit does not call stage.Enter:
// the advance is deferred to MergeRequestReconciler's DrainPendingReview. While
// the Task sits at reviewing awaiting that drain, the caps and the respawn read
// only pod liveness + stats.podRecreations, so they keep driving a FINISHED Task
// as an ordinary live pod stage.
// ---------------------------------------------------------------------------

// tsReviewTaskWithOutcome is a kind=review Task at reviewing whose review pod
// has RUN (stageWorkStartedAt set) and whose outcome carries the given condition
// reason. reason=="Review" is a COMMITTED outcome; reason=="Outcome" is a BARE
// CLAIM.
func tsReviewTaskWithOutcome(reason string, recreations int, at time.Time) *tatarav1alpha1.Task {
	stamp := metav1.NewTime(at)
	// The Task reached awaiting-review BEFORE its review agent committed, because
	// a review pod has to be scheduled, booted and run in between. Modelling the
	// entry and the commit at the same instant made the fixture describe the one
	// shape that means the opposite thing - an outcome written in the same etcd
	// write as the entry it CAUSED, which is not a handoff at all (#547).
	task := tsTask("rev", "review", tatarav1alpha1.StateAwaitingReview, at.Add(-time.Minute))
	task.Status.PodStartedAt = &stamp
	task.Status.StateWorkStartedAt = &stamp
	task.Status.Stats.PodRecreations = recreations
	task.Status.Conditions = []metav1.Condition{{
		Type:               tatarav1alpha1.ConditionOutcomeAccepted,
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            "fp",
		LastTransitionTime: stamp,
	}}
	return task
}

// A COMMITTED outcome + a pod that is GONE must not respawn, must not burn a
// recreation, and must not trip a cap. The agent's work is DONE; only the C.5.3
// phase-2 drain is outstanding. This is exactly what re-reviewed the
// already-merged PR four times on cfsw4/llkfb and burned the recreations that
// killed 7k7pd/cgthv/rfzwv.
func TestReconcile_CommittedOutcomeSuppressesRespawnAndCaps(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0)
	// The pod is seen gone with no outcome, so BudgetExit would park it
	// no-outcome were the committed-outcome suppression not in the way.
	task := tsReviewTaskWithOutcome(tatarav1alpha1.OutcomeReasonFor(stage.AgentReview),
		somePodRecreations, now.Add(-time.Minute))
	proj := tsProject(3)
	c := newMirrorClient(t, proj, mdSecret(), task) // no Pod object -> podGone == true
	r := tsReconciler(c)

	res, err := r.reconcileStage(ctx, proj, task, now)
	if err != nil {
		t.Fatalf("reconcileStage: %v", err)
	}
	if res.RequeueAfter != stageRequeue {
		t.Fatalf("requeueAfter = %s, want %s: it polls for the drain instead of acting",
			res.RequeueAfter, stageRequeue)
	}
	got := mdGetTask(t, c, task.Name)
	if got.Status.State != tatarav1alpha1.StateAwaitingReview {
		t.Fatalf("stage = %q(%s), want reviewing: a committed outcome must not be terminated by a pod-liveness cap",
			got.Status.State, got.Status.ParkReason)
	}
	if got.Status.Stats.PodRecreations != somePodRecreations {
		t.Fatalf("podRecreations = %d, want %d: no recreation may be burned",
			got.Status.Stats.PodRecreations, somePodRecreations)
	}
	var pods corev1.PodList
	if err := c.List(ctx, &pods, client.InNamespace(mdNS)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("%d pods respawned for a task whose outcome landed", len(pods.Items))
	}
}

// THE ARGOCD-WEDGE REGRESSION GUARD. A BARE CLAIM (Reason "Outcome") is a
// failed-validation or crashed-mid-flight stub. It must remain FULLY subject to
// the caps: guarding it would freeze the Task forever, reproducing ArgoCD's
// status.operationState stuck-in-Running - the anti-pattern twin of the very bug
// this change fixes.
func TestReconcile_BareClaimIsStillFullySubjectToTheCaps(t *testing.T) {
	now := time.Unix(1000, 0)
	task := tsReviewTaskWithOutcome(tatarav1alpha1.OutcomeReasonClaimed,
		somePodRecreations, now.Add(-time.Minute))
	proj := tsProject(3)
	c := newMirrorClient(t, proj, mdSecret(), task)
	r := tsReconciler(c)

	got := tsReconcile(t, r, proj, task, now)

	if got.Status.State != tatarav1alpha1.StateAwaitingReview || !tatarav1alpha1.Parked(got) || got.Status.ParkReason != stage.ReasonNoOutcome {
		t.Fatalf("state=%q parked=%v reason=%q, want awaiting-review/parked/no-outcome: a bare claim must NOT be protected, the caps apply exactly as they do today",
			got.Status.State, tatarav1alpha1.Parked(got), got.Status.ParkReason)
	}
}

// The condition is per-TASK and survives across stages. An implement Task
// arrives at reviewing with Reason=Implement ALREADY committed: its review pod
// has not spawned yet and the guard must NOT suppress it, or every implement
// Task wedges - a strictly worse failure than the one being fixed.
func TestReconcile_CommittedImplementOutcomeDoesNotGagTheReviewingStagePod(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0)
	task := tsReviewTaskWithOutcome(tatarav1alpha1.OutcomeReasonFor(stage.AgentImplement), 0, now)
	task.Spec.Kind = stage.AgentImplement
	task.Status.PodStartedAt = nil
	task.Status.StateWorkStartedAt = nil
	proj := tsProject(3)
	readySince := metav1.NewTime(now.Add(-time.Hour))
	proj.Status.Memory.ReadySince = &readySince
	c := newMirrorClient(t, proj, mdSecret(), task)
	r := tsReconciler(c)
	r.PodConfig = agent.PodConfig{
		Namespace:           mdNS,
		AnthropicSecretName: "anthropic",
		CLIOIDCSecretName:   "cli-oidc",
	}

	if _, err := r.reconcileStage(ctx, proj, task, now); err != nil {
		t.Fatalf("reconcileStage: %v", err)
	}

	var pods corev1.PodList
	if err := c.List(ctx, &pods, client.InNamespace(mdNS)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("pods = %d, want 1: the reviewing stage's OWN review pod must still spawn", len(pods.Items))
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func tsReadyPod(task *tatarav1alpha1.Task) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "wrapper-" + task.Name,
			Namespace: mdNS,
			Annotations: map[string]string{
				annPodStage: task.Status.State,
			},
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
}

func illegalCount(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	return testutil.ToFloat64(c)
}

func terminalCount(t *testing.T, reg *prometheus.Registry, kind, stg, reason string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "operator_task_terminal_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			match := map[string]string{"kind": kind, "state": stg, "stateReason": reason}
			ok := true
			for _, lp := range m.GetLabel() {
				if want, has := match[lp.GetName()]; has && want != lp.GetValue() {
					ok = false
				}
			}
			if ok {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// ---------------------------------------------------------------------------
// SPEC TEST 9. THE cgthv REPRODUCTION, end to end.
//
// 2026-07-16T18:12Z: six kind=review Tasks were minted. cgthv's review agent
// genuinely completed - mr-tatara-agent-skills-20 carries status:approved and a
// stamped reviewedSHA - and the Task still ended failed(pod-recreation-exhausted),
// because while status.stage was still reviewing the caps kept driving it as an
// ordinary live pod stage through the v1.2.0 rollout's pod-loss burst.
//
// The expected happy path is reviewing -> parked(awaiting-human). Zero of the six
// reached it.
// ---------------------------------------------------------------------------

func TestReviewTask_CommittedOutcomePlusLostPodReachesAwaitingHuman(t *testing.T) {
	// Step 2 drives advanceAfterReview (reviewpost.go) via DrainPendingReview
	// with the RAW edge from reviewAdvanceEdge, which is
	// Edge{To: stage.ParkTarget, Reason: ReasonAwaitingHuman} for EVERY
	// kind=review verdict. EnterStage refuses ParkTarget by design, so this is
	// the test that proves the ParkTarget branch at reviewpost.go's applier
	// exists. Do not re-skip it: an unbranched applier 500s on the platform's
	// commonest review outcome.

	ctx := context.Background()
	now := time.Unix(1000, 0)
	stamp := metav1.NewTime(now.Add(-time.Minute))
	// Entered awaiting-review BEFORE the review agent committed: a commit sharing
	// the entry's instant is the transition's OWN outcome, not a handoff (#547).
	entered := metav1.NewTime(now.Add(-2 * time.Minute))

	proj := tsProject(3)
	repo := mdRepo("tatara-agent-skills")
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "cgthv", Namespace: mdNS, UID: types.UID("uid-cgthv")},
		Spec:       tatarav1alpha1.TaskSpec{Kind: "review", ProjectRef: "proj", Goal: "review the PR"},
		Status: tatarav1alpha1.TaskStatus{
			State:              tatarav1alpha1.StateAwaitingReview,
			AgentKind:          "review",
			StateEnteredAt:     &entered,
			PodStartedAt:       &stamp,
			StateWorkStartedAt: &stamp,
			// podRuns=5 => 4 recreations => 4 > 3 => failed(pod-recreation-exhausted)
			// under the old code, the moment the lost pod is noticed.
			Stats: tatarav1alpha1.TaskStats{PodRuns: 5, PodRecreations: 4},
			Conditions: []metav1.Condition{{
				Type:               tatarav1alpha1.ConditionOutcomeAccepted,
				Status:             metav1.ConditionTrue,
				Reason:             tatarav1alpha1.OutcomeReasonFor(stage.AgentReview), // COMMITTED: the review outcome landed
				Message:            "fp-cgthv",
				LastTransitionTime: stamp,
			}},
		},
	}
	mr := mdMR(task, "tatara-agent-skills", 20)
	mr.Status.Status = "approved"
	mr.Status.ReviewedSHA = "reviewedsha"
	mr.Status.PendingReview = &tatarav1alpha1.PendingReview{
		Body: "## Review: approved", SHA: "reviewedsha", Round: 1,
	}

	c := newMirrorClient(t, proj, mdSecret(), repo, task, mr)

	// 1. The pod is GONE and the recreation budget is spent. Under v1.3.0 this
	//    reconcile fails the Task. It must now do nothing but wait for the drain.
	tr := tsReconciler(c)
	res, err := tr.reconcileStage(ctx, proj, task, now)
	if err != nil {
		t.Fatalf("reconcileStage: %v", err)
	}
	if task.Status.State != tatarav1alpha1.StateAwaitingReview {
		t.Fatalf("stage = %q(%s), want reviewing: the review LANDED, a pod that is no longer needed must not fail the Task",
			task.Status.State, task.Status.ParkReason)
	}
	if task.Status.Stats.PodRecreations != 4 {
		t.Fatalf("podRecreations = %d, want 4: no recreation may be burned for a committed outcome",
			task.Status.Stats.PodRecreations)
	}
	if res.RequeueAfter != stageRequeue {
		t.Fatalf("requeueAfter = %s, want %s", res.RequeueAfter, stageRequeue)
	}
	var pods corev1.PodList
	if err := c.List(ctx, &pods, client.InNamespace(mdNS)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("%d pods respawned for a task whose review outcome already landed", len(pods.Items))
	}

	// 2. The drain runs (the MergeRequest reconciler's half). It posts the review,
	//    clears pendingReview, and advanceAfterReview takes the F.3 edge.
	f := newFakeForge(t)
	d := mdNewDriver(t, f, c)
	if err := d.DrainPendingReview(ctx, mdGetMR(t, c, mr.Name)); err != nil {
		t.Fatalf("DrainPendingReview: %v", err)
	}

	got := mdGetTask(t, c, "cgthv")
	if got.Status.State != tatarav1alpha1.StateAwaitingReview || !tatarav1alpha1.Parked(got) {
		t.Fatalf("state = %q, parked = %v, want awaiting-review/parked", got.Status.State, tatarav1alpha1.Parked(got))
	}
	if got.Status.ParkReason != stage.ReasonAwaitingHuman {
		t.Fatalf("stageReason = %q, want awaiting-human: the expected happy path for a kind=review Task is a human's PR fixed and merged by the human",
			got.Status.ParkReason)
	}
	if got.Status.ParkReason == stage.ReasonPodRecreationExhausted {
		t.Fatalf("stageReason = pod-recreation-exhausted: this is the exact production failure being reproduced")
	}
}

// ---------------------------------------------------------------------------
// REVERSAL OF ISSUE #355's pre-SubmitTurn memory gate. reconcilePodStage used
// to HOLD turn0 whenever the project memory stack was not stably ready, with no
// timeout and no escape hatch, and to surface the hold as a per-Task issue
// comment written THROUGH objbudget.FitIssue (which then returned the error,
// turning a clean 15s poll into an error-backoff loop). The turn now goes out:
// the agent is told memory is degraded and works with reduced recall.
// ---------------------------------------------------------------------------

func TestTurnSubmit_ProceedsWhenMemoryNotStablyReady(t *testing.T) {
	now := time.Now()
	task := tsTask("ts-degraded", "implement", tatarav1alpha1.StateUnderImplementation, now.Add(-time.Minute))
	podAt := metav1.NewTime(now.Add(-30 * time.Second))
	workAt := metav1.NewTime(now.Add(-10 * time.Second))
	task.Status.PodStartedAt = &podAt
	task.Status.StateWorkStartedAt = &workAt
	issName := tatarav1alpha1.IssueName("tatara-operator", 1)
	task.Status.IssueRefs = []string{issName}

	proj := tsProject(3)
	proj.Status.Memory = &tatarav1alpha1.MemoryStatus{Phase: "Provisioning", Endpoint: "http://mem"}
	iss := ownedIssue(issName, 1, task, tatarav1alpha1.IssueStatus{State: "open"})

	c := newMirrorClient(t, proj, mdSecret(), task, tsReadyPod(task), iss)
	r := tsReconciler(c)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
	fs := newFakeSession()
	r.Session = fs

	if _, err := r.reconcileStage(context.Background(), proj, task, now); err != nil {
		t.Fatalf("reconcileStage: %v", err)
	}
	if _, ok := fs.lastSubmit(); !ok {
		t.Fatal("turn-0 must be submitted with memory degraded: the memory gate is gone")
	}
	got := mdGetTask(t, c, task.Name)
	if got.Annotations[annStageTurn0] == "" {
		t.Fatalf("annStageTurn0 unset: the turn was not recorded")
	}
	// No per-Task issue comment: the memory-stack alert is the human-facing
	// signal and the Task condition is the drill-down. A comment per held Task
	// was noise-per-task, and it was written through the byte-budget path.
	gotIss := getIssueCR(t, c, issName)
	if len(gotIss.Status.PendingComments) != 0 {
		t.Fatalf("PendingComments = %d, want 0: degraded memory must not comment per Task",
			len(gotIss.Status.PendingComments))
	}
}

// The degraded state must reach the AGENT: the assignment it receives carries
// the degraded appendix telling it recall is down and not to stop over it.
func TestTurnSubmit_DegradedBundleCarriesTheGuidance(t *testing.T) {
	now := time.Now()
	task := tsTask("ts-degraded-prompt", "implement", tatarav1alpha1.StateUnderImplementation, now.Add(-time.Minute))
	podAt := metav1.NewTime(now.Add(-30 * time.Second))
	workAt := metav1.NewTime(now.Add(-10 * time.Second))
	task.Status.PodStartedAt = &podAt
	task.Status.StateWorkStartedAt = &workAt

	proj := tsProject(3)
	proj.Status.Memory = &tatarav1alpha1.MemoryStatus{Phase: "Degraded", Endpoint: "http://mem"}

	c := newMirrorClient(t, proj, mdSecret(), task, tsReadyPod(task))
	r := tsReconciler(c)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
	fs := newFakeSession()
	r.Session = fs

	if _, err := r.reconcileStage(context.Background(), proj, task, now); err != nil {
		t.Fatalf("reconcileStage: %v", err)
	}
	sub, ok := fs.lastSubmit()
	if !ok {
		t.Fatal("turn-0 must be submitted")
	}
	if !strings.Contains(sub.Text, promptguidance.MemoryDegradedGuidance) {
		t.Fatalf("turn-0 bundle is missing the degraded guidance:\n%s", sub.Text)
	}
}

// ---------------------------------------------------------------------------
// TURN-SUBMIT METRIC. Re-pointed from the retired machine's
// TestTurnSubmitted_{Metric,ErrorMetric}Emitted (task_controller_audit_test.go),
// which drove the deleted driveTurns path. operator_turn_submit_total is LIVE -
// task_stage.go fires it on every turn-0 - and nothing else asserts it.
// ---------------------------------------------------------------------------

func TestTurnSubmit_MetricEmittedOnTurnZero(t *testing.T) {
	now := time.Now()
	task := tsTask("ts-ok", "implement", tatarav1alpha1.StateUnderImplementation, now.Add(-time.Minute))
	podAt := metav1.NewTime(now.Add(-30 * time.Second))
	workAt := metav1.NewTime(now.Add(-10 * time.Second))
	task.Status.PodStartedAt = &podAt
	task.Status.StateWorkStartedAt = &workAt
	proj := tsStablyReadyProject(3)

	c := newMirrorClient(t, proj, mdSecret(), task, tsReadyPod(task))
	reg := prometheus.NewRegistry()
	r := tsReconciler(c)
	r.Metrics = obs.NewOperatorMetrics(reg)
	fs := newFakeSession()
	r.Session = fs

	tsReconcile(t, r, proj, task, now)

	if _, ok := fs.lastSubmit(); !ok {
		t.Fatal("turn-0 must be submitted")
	}
	if v := turnSubmitCount(t, reg, "implement", "ok"); v < 1 {
		t.Errorf("operator_turn_submit_total{kind=implement,result=ok} = %v, want >= 1", v)
	}
}

func TestTurnSubmit_ErrorMetricEmittedOnSubmitFailure(t *testing.T) {
	now := time.Now()
	task := tsTask("ts-err", "review", tatarav1alpha1.StateAwaitingReview, now.Add(-time.Minute))
	podAt := metav1.NewTime(now.Add(-30 * time.Second))
	workAt := metav1.NewTime(now.Add(-10 * time.Second))
	task.Status.PodStartedAt = &podAt
	task.Status.StateWorkStartedAt = &workAt
	proj := tsStablyReadyProject(3)

	c := newMirrorClient(t, proj, mdSecret(), task, tsReadyPod(task))
	reg := prometheus.NewRegistry()
	r := tsReconciler(c)
	r.Metrics = obs.NewOperatorMetrics(reg)
	fs := newFakeSession()
	fs.submitErr = &agent.HTTPError{Status: 500, Body: "internal error"}
	r.Session = fs

	if _, err := r.reconcileStage(context.Background(), proj, task, now); err == nil {
		t.Fatal("want an error from a 500 SubmitTurn")
	}
	if v := turnSubmitCount(t, reg, "review", "error"); v < 1 {
		t.Errorf("operator_turn_submit_total{kind=review,result=error} = %v, want >= 1", v)
	}
}

// turnSubmitCount reads operator_turn_submit_total{kind,result} out of reg.
func turnSubmitCount(t *testing.T, reg *prometheus.Registry, kind, result string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "operator_turn_submit_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			got := map[string]string{}
			for _, lp := range m.GetLabel() {
				got[lp.GetName()] = lp.GetValue()
			}
			if got["kind"] == kind && got["result"] == result {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// ---------------------------------------------------------------------------
// TICKET CLASS BY STAGE, NOT BY TASK KIND (production bug).
// ---------------------------------------------------------------------------

// ticketMirrorClient is newMirrorClient's twin, but with QueuedEvent's status
// subresource enabled too: EnqueueEvent does a Create then a Status().Update
// to stamp state=Queued, which a client that does not know QueuedEvent has a
// status subresource 404s on. newMirrorClient omits it because none of its
// (many) other callers ever enqueue a ticket through it.
func ticketMirrorClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(mirrorScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(&tatarav1alpha1.Issue{}, &tatarav1alpha1.MergeRequest{},
			&tatarav1alpha1.Task{}, &tatarav1alpha1.QueuedEvent{}).
		WithIndex(&tatarav1alpha1.QueuedEvent{}, queue.TaskRefIndex, queue.TaskRefIndexer).
		Build()
}

// TestEnsureTicketClassByStageAgentKind covers the production symptom: an
// incident Task's DOWNSTREAM stages (clarify, implement, ...) were classed
// QueueClassAlert just because task.Spec.Kind == "incident", starving them
// behind AlertCapacity=1 alongside the investigating stage they queue behind.
// ensureTicket classes by the AGENT KIND it is handed, never by
// task.Spec.Kind, and that is exactly what this drives directly.
//
// #521 note: AgentKindFor(state, specKind) now keys ONLY on the ORIGIN kind
// for refined/under-implementation - an incident-origin Task therefore runs
// the SAME "incident" agent kind through both those states (there is no
// longer a distinct downstream "clarify"/"implement" agent for it the way the
// old per-stage table had one), so an incident Task's own refined/under-
// implementation tickets are alert-class BOTH times in practice now. What
// this test still proves, and the reason ensureTicket takes agentKind as a
// parameter rather than deriving it itself, is that CLASSIFICATION goes by
// that parameter alone: a downstream ticket carrying some OTHER agent kind is
// normal-class even on an incident-origin Task.
func TestEnsureTicketClassByStageAgentKind(t *testing.T) {
	cases := []struct {
		name      string
		stg       string
		specKind  string
		agentKind string
		wantClass string
	}{
		{"an incident Task's own refined ticket is alert-class", tatarav1alpha1.StateRefined, "incident", stage.AgentIncident, tatarav1alpha1.QueueClassAlert},
		{"a downstream ticket classed by a non-incident agent kind is normal-class, even on an incident-origin task", tatarav1alpha1.StateRefined, "incident", stage.AgentImplement, tatarav1alpha1.QueueClassNormal},
		{"an implement-origin task's under-implementation ticket is normal-class", tatarav1alpha1.StateUnderImplementation, "implement", stage.AgentImplement, tatarav1alpha1.QueueClassNormal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			task := tsTask("t-"+tc.stg, tc.specKind, tc.stg, time.Now())
			proj := tsProject(3)
			c := ticketMirrorClient(t, proj, mdSecret(), task)
			r := tsReconciler(c)
			r.Seq = &queue.SeqSource{Client: c, Namespace: mdNS}

			if _, err := r.ensureTicket(ctx, proj, task, tc.agentKind); err != nil {
				t.Fatalf("ensureTicket: %v", err)
			}

			var qel tatarav1alpha1.QueuedEventList
			if err := c.List(ctx, &qel); err != nil {
				t.Fatalf("list queuedevents: %v", err)
			}
			var found *tatarav1alpha1.QueuedEvent
			for i := range qel.Items {
				if qel.Items[i].Spec.Payload.TaskRef == task.Name {
					found = &qel.Items[i]
					break
				}
			}
			if found == nil {
				t.Fatalf("no admission ticket enqueued for task %s", task.Name)
			}
			if found.Spec.Class != tc.wantClass {
				t.Errorf("ticket class = %q, want %q (task.Spec.Kind=%s, state=%s, agentKind=%s)",
					found.Spec.Class, tc.wantClass, tc.specKind, tc.stg, tc.agentKind)
			}
		})
	}
}

// TestEnsureTicket_IncidentKindGetsPriorityZero covers issue #402's real
// starvation fix: an incident Task's downstream ticket (clarify/implement/
// review, class=Normal) must still be stamped Priority=0 so it outranks
// sweep-originated Normal-class tickets at admission (admitPool sorts by
// (priority, seq)); a non-incident Task's ticket is left at the class default
// (nil override -> Priority=2 via EffectiveQueuePriority).
func TestEnsureTicket_IncidentKindGetsPriorityZero(t *testing.T) {
	ctx := context.Background()

	incidentTask := tsTask("t-incident", "incident", tatarav1alpha1.StateRefined, time.Now())
	proj := tsProject(3)
	c := ticketMirrorClient(t, proj, mdSecret(), incidentTask)
	r := tsReconciler(c)
	r.Seq = &queue.SeqSource{Client: c, Namespace: mdNS}

	// The DOWNSTREAM ticket: agentKind is NOT incident (an incident-origin
	// Task's own refined phase now runs the incident agent throughout, #521 -
	// see TestEnsureTicketClassByStageAgentKind's note - but the priority
	// override this test targets is about Spec.Kind, not the class the ticket
	// happens to draw from), so BOTH tickets below are Normal-class; only
	// Spec.Kind=="incident" decides Priority.
	agentKind := stage.AgentImplement
	if _, err := r.ensureTicket(ctx, proj, incidentTask, agentKind); err != nil {
		t.Fatalf("ensureTicket: %v", err)
	}
	found := findTicketForTask(t, c, incidentTask.Name)
	if found.Spec.Priority == nil || *found.Spec.Priority != 0 {
		t.Fatalf("incident-kind Task's ticket priority = %v, want 0", found.Spec.Priority)
	}

	normalTask := tsTask("t-normal", "implement", tatarav1alpha1.StateRefined, time.Now())
	c2 := ticketMirrorClient(t, proj, mdSecret(), normalTask)
	r2 := tsReconciler(c2)
	r2.Seq = &queue.SeqSource{Client: c2, Namespace: mdNS}

	if _, err := r2.ensureTicket(ctx, proj, normalTask, agentKind); err != nil {
		t.Fatalf("ensureTicket: %v", err)
	}
	foundNormal := findTicketForTask(t, c2, normalTask.Name)
	if foundNormal.Spec.Priority == nil || *foundNormal.Spec.Priority != 2 {
		t.Fatalf("non-incident Task's ticket priority = %v, want 2 (Normal-class default, no override)", foundNormal.Spec.Priority)
	}
}

// findTicketForTask is TestEnsureTicketClassByStageAgentKind's list-and-find
// helper, factored out for reuse.
func findTicketForTask(t *testing.T, c client.Client, taskName string) *tatarav1alpha1.QueuedEvent {
	t.Helper()
	var qel tatarav1alpha1.QueuedEventList
	if err := c.List(context.Background(), &qel); err != nil {
		t.Fatalf("list queuedevents: %v", err)
	}
	for i := range qel.Items {
		if qel.Items[i].Spec.Payload.TaskRef == taskName {
			return &qel.Items[i]
		}
	}
	t.Fatalf("no admission ticket enqueued for task %s", taskName)
	return nil
}

// SPEC TEST 7. The B2 guard must never become an unbounded hold. A committed
// outcome whose drain NEVER runs (the sibling MergeRequest CR was deleted, the
// drain is broken, a leader-election changeover dropped the workqueue item) parks
// at handoff-stalled after HandoffDeadline. The drain normally lands in ~1s; the
// 4h reviewing work budget is far too loose to surface a broken one, and a
// suppressed Task holds its admitted concurrency ticket for the whole window.
func TestReconcile_CommittedOutcomeWithNoDrainParksHandoffStalled(t *testing.T) {
	base := time.Unix(1000, 0)
	committed := tatarav1alpha1.OutcomeReasonFor(stage.AgentReview)

	t.Run("inside the deadline it waits", func(t *testing.T) {
		task := tsReviewTaskWithOutcome(committed, 0, base)
		proj := tsProject(3)
		r := tsReconciler(newMirrorClient(t, proj, mdSecret(), task))

		res, err := r.reconcileStage(context.Background(), proj, task,
			base.Add(tatarav1alpha1.HandoffDeadline-time.Second))
		if err != nil {
			t.Fatalf("reconcileStage: %v", err)
		}
		got := mdGetTask(t, r.Client, task.Name)
		if got.Status.State != tatarav1alpha1.StateAwaitingReview {
			t.Fatalf("stage = %q(%s), want reviewing: the deadline has not elapsed yet",
				got.Status.State, got.Status.ParkReason)
		}
		if res.RequeueAfter != stageRequeue {
			t.Fatalf("requeueAfter = %s, want %s: it must keep polling for the drain",
				res.RequeueAfter, stageRequeue)
		}
	})

	t.Run("past the deadline it parks handoff-stalled", func(t *testing.T) {
		task := tsReviewTaskWithOutcome(committed, 0, base)
		proj := tsProject(3)
		r := tsReconciler(newMirrorClient(t, proj, mdSecret(), task))

		got := tsReconcile(t, r, proj, task, base.Add(tatarav1alpha1.HandoffDeadline+time.Second))

		if got.Status.State != tatarav1alpha1.StateAwaitingReview || !tatarav1alpha1.Parked(got) ||
			got.Status.ParkReason != stage.ReasonHandoffStalled {
			t.Fatalf("state=%q parked=%v reason=%q, want awaiting-review/parked/handoff-stalled: the B2 suppression must be bounded",
				got.Status.State, tatarav1alpha1.Parked(got), got.Status.ParkReason)
		}
		if got.Status.ParkedFromState != tatarav1alpha1.StateAwaitingReview {
			t.Fatalf("parkedFromStage = %q, want reviewing", got.Status.ParkedFromState)
		}
	})

	t.Run("the deadline runs from the COMMIT, not from stageEnteredAt", func(t *testing.T) {
		// The Task has been at reviewing for an hour, but its outcome committed one
		// minute ago: the handoff clock starts when the commit stamped the condition.
		task := tsReviewTaskWithOutcome(committed, 0, base.Add(-time.Hour))
		stamp := metav1.NewTime(base.Add(-time.Minute))
		task.Status.Conditions[0].LastTransitionTime = stamp
		proj := tsProject(3)
		r := tsReconciler(newMirrorClient(t, proj, mdSecret(), task))

		got := tsReconcile(t, r, proj, task, base)

		if got.Status.State != tatarav1alpha1.StateAwaitingReview {
			t.Fatalf("stage = %q(%s), want reviewing: 1m since the commit is inside the 5m deadline",
				got.Status.State, got.Status.ParkReason)
		}
	})

	t.Run("a BARE CLAIM never arms the handoff deadline", func(t *testing.T) {
		// It has no handoff to wait for. It is a failed-validation stub and the
		// ordinary caps own it.
		task := tsReviewTaskWithOutcome(tatarav1alpha1.OutcomeReasonClaimed, somePodRecreations, base)
		proj := tsProject(3)
		r := tsReconciler(newMirrorClient(t, proj, mdSecret(), task))

		got := tsReconcile(t, r, proj, task, base.Add(tatarav1alpha1.HandoffDeadline+time.Second))

		if got.Status.ParkReason == stage.ReasonHandoffStalled {
			t.Fatalf("reason = handoff-stalled: a bare claim has no handoff outstanding")
		}
		if got.Status.ParkReason != stage.ReasonNoOutcome {
			t.Fatalf("reason = %q, want no-outcome: the ordinary caps own a bare claim", got.Status.ParkReason)
		}
	})

	t.Run("an IMPLEMENT outcome at reviewing never arms the handoff deadline", func(t *testing.T) {
		// The condition is per-TASK and survives across stages: an implement Task
		// arrives at reviewing with Reason=Implement ALREADY committed. A bare
		// OutcomeCommitted check here would park EVERY implement Task at
		// handoff-stalled 5m after it reached reviewing.
		task := tsReviewTaskWithOutcome(tatarav1alpha1.OutcomeReasonFor(stage.AgentImplement), 0, base)
		task.Spec.Kind = stage.AgentImplement
		task.Status.PodStartedAt = nil
		task.Status.StateWorkStartedAt = nil
		proj := tsProject(3)
		readySince := metav1.NewTime(base.Add(-time.Hour))
		proj.Status.Memory.ReadySince = &readySince
		r := tsReconciler(newMirrorClient(t, proj, mdSecret(), task))
		r.PodConfig = agent.PodConfig{
			Namespace:           mdNS,
			AnthropicSecretName: "anthropic",
			CLIOIDCSecretName:   "cli-oidc",
		}

		got := tsReconcile(t, r, proj, task, base.Add(tatarav1alpha1.HandoffDeadline+time.Second))

		if got.Status.State != tatarav1alpha1.StateAwaitingReview ||
			got.Status.ParkReason == stage.ReasonHandoffStalled {
			t.Fatalf("stage=%q reason=%q, want reviewing: the implement commit is not THIS stage's handoff",
				got.Status.State, got.Status.ParkReason)
		}
	})

	t.Run("a PREVIOUS round's review commit never arms the handoff deadline", func(t *testing.T) {
		// stage.Enter never clears the condition, so a Task that RE-ENTERS reviewing
		// carries the last round's Reason=Review commit with it: merging -> reviewing
		// on a head move (cycle 4) and the kind=review awaiting-human unpark both do
		// exactly this. THIS occupancy's review agent has not run yet, so the handoff
		// is not outstanding and the pod must still spawn. Without the occupancy
		// check the first reconcile parks it at handoff-stalled - recoverable only
		// by a human comment - and both cycles stall SILENTLY, because
		// reviewing -> parked is a legal transition.
		task := tsReviewTaskWithOutcome(committed, 0, base)
		reEntered := metav1.NewTime(base.Add(time.Hour))
		task.Status.StateEnteredAt = &reEntered
		task.Status.PodStartedAt = nil
		task.Status.StateWorkStartedAt = nil
		proj := tsProject(3)
		readySince := metav1.NewTime(base.Add(-time.Hour))
		proj.Status.Memory.ReadySince = &readySince
		c := newMirrorClient(t, proj, mdSecret(), task)
		r := tsReconciler(c)
		r.PodConfig = agent.PodConfig{
			Namespace:           mdNS,
			AnthropicSecretName: "anthropic",
			CLIOIDCSecretName:   "cli-oidc",
		}

		got := tsReconcile(t, r, proj, task, base.Add(time.Hour+tatarav1alpha1.HandoffDeadline+time.Second))

		if got.Status.State != tatarav1alpha1.StateAwaitingReview ||
			got.Status.ParkReason == stage.ReasonHandoffStalled {
			t.Fatalf("stage=%q reason=%q, want reviewing: a commit from a PREVIOUS occupancy of this stage is not this occupancy's handoff",
				got.Status.State, got.Status.ParkReason)
		}
		var pods corev1.PodList
		if err := c.List(context.Background(), &pods, client.InNamespace(mdNS)); err != nil {
			t.Fatalf("list pods: %v", err)
		}
		if len(pods.Items) != 1 {
			t.Fatalf("pods = %d, want 1: the re-entered reviewing stage's own review pod must still spawn", len(pods.Items))
		}
	})

	t.Run("the handoff deadline fires BEFORE clock 3's 4h reviewing budget", func(t *testing.T) {
		task := tsReviewTaskWithOutcome(committed, 0, base)
		proj := tsProject(3)
		r := tsReconciler(newMirrorClient(t, proj, mdSecret(), task))

		got := tsReconcile(t, r, proj, task, base.Add(10*time.Minute))

		if got.Status.ParkReason != stage.ReasonHandoffStalled {
			t.Fatalf("reason = %q, want handoff-stalled: the 5m handoff deadline must fire first",
				got.Status.ParkReason)
		}
	})
}

// #547: the outcome that CAUSED the transition is not the new state's handoff.
//
// AgentKindFor returns the SAME agent for refined and under-implementation once
// clarify folded into implement (#521), so the OutcomeAccepted{Reason=Implement}
// that /outcome writes on the approval edge names the agent of the state it
// lands in. Both stamps come from that one write and metav1.Time is
// second-granular, so they are EQUAL and the strict Before() occupancy guard
// let it through: every approved implement Task had its implementation turn
// suppressed by B2 and parked handoff-stalled 5m later, recoverable only by a
// human comment. Two of two approvals in the 24h after v2.0.1 hit it
// (mtg-decks#18, tatara-operator#529).
func TestReconcile_TheOutcomeThatCausedTheTransitionIsNotTheNewStatesHandoff(t *testing.T) {
	base := time.Unix(1000, 0)

	// tsApprovedImplementTask is the exact post-approval shape: under-implementation,
	// entered by the same /outcome write that stamped the commit, no pod yet.
	tsApprovedImplementTask := func(condAt time.Time) *tatarav1alpha1.Task {
		task := tsTask("appr", stage.AgentImplement,
			tatarav1alpha1.StateUnderImplementation, base)
		task.Status.Conditions = []metav1.Condition{{
			Type:               tatarav1alpha1.ConditionOutcomeAccepted,
			Status:             metav1.ConditionTrue,
			Reason:             tatarav1alpha1.OutcomeReasonFor(stage.AgentImplement),
			Message:            "fp",
			LastTransitionTime: metav1.NewTime(condAt),
		}}
		return task
	}

	tsImplementReconciler := func(t *testing.T, proj *tatarav1alpha1.Project,
		task *tatarav1alpha1.Task) *TaskReconciler {
		t.Helper()
		readySince := metav1.NewTime(base.Add(-time.Hour))
		proj.Status.Memory.ReadySince = &readySince
		r := tsReconciler(newMirrorClient(t, proj, mdSecret(), task))
		r.PodConfig = agent.PodConfig{
			Namespace:           mdNS,
			AnthropicSecretName: "anthropic",
			CLIOIDCSecretName:   "cli-oidc",
		}
		return r
	}

	// The regression itself: commit and entry stamped in the SAME write.
	t.Run("a commit stamped AT stateEnteredAt never arms the handoff deadline", func(t *testing.T) {
		proj := tsProject(3)
		task := tsApprovedImplementTask(base)
		r := tsImplementReconciler(t, proj, task)

		got := tsReconcile(t, r, proj, task, base.Add(tatarav1alpha1.HandoffDeadline+time.Second))

		if got.Status.ParkReason == stage.ReasonHandoffStalled {
			t.Fatalf("reason = handoff-stalled: the approval that CAUSED under-implementation is not that state's handoff")
		}
		if got.Status.State != tatarav1alpha1.StateUnderImplementation {
			t.Fatalf("state = %q(%s), want under-implementation: the approved Task must stay and work",
				got.Status.State, got.Status.ParkReason)
		}
	})

	// The B2 pod suppression rides the same predicate, and it is what actually
	// cost the delivery: for the whole 5m before the park, ensureStagePod
	// refused to spawn the implement pod that was supposed to write the code.
	t.Run("the implement pod still spawns", func(t *testing.T) {
		proj := tsProject(3)
		task := tsApprovedImplementTask(base)
		r := tsImplementReconciler(t, proj, task)

		if _, err := r.reconcileStage(context.Background(), proj, task, base.Add(time.Second)); err != nil {
			t.Fatalf("reconcileStage: %v", err)
		}

		var pods corev1.PodList
		if err := r.List(context.Background(), &pods, client.InNamespace(mdNS)); err != nil {
			t.Fatalf("list pods: %v", err)
		}
		if len(pods.Items) != 1 {
			t.Fatalf("pods = %d, want 1: the approved Task's own implement pod must spawn", len(pods.Items))
		}
	})

	// The structural half of the fix, independent of any stamp ordering:
	// under-implementation has no deferred second reconciler to wait on, so it
	// can never hold an outstanding handoff even when the commit is genuinely
	// later than the entry.
	t.Run("no state but awaiting-review can hold an outstanding handoff", func(t *testing.T) {
		proj := tsProject(3)
		task := tsApprovedImplementTask(base.Add(time.Minute))
		r := tsImplementReconciler(t, proj, task)

		got := tsReconcile(t, r, proj, task,
			base.Add(time.Minute+tatarav1alpha1.HandoffDeadline+time.Second))

		if got.Status.ParkReason == stage.ReasonHandoffStalled {
			t.Fatalf("reason = handoff-stalled: only awaiting-review defers its advance to a second reconciler")
		}
	})
}

// #379: the reviewing->advance in DrainPendingReview is EDGE-triggered - a
// one-shot at the tail of the drain - and once pendingReview is cleared every
// later MergeRequest reconcile short-circuits and never re-attempts it. If that
// one shot missed (a stale cached owned-MR read, a transient controller-owner
// sever, a multi-MR ordering race) the review already landed but the Task sat out
// the whole HandoffDeadline and parked handoff-stalled instead of advancing. The
// Task reconciler must LEVEL-trigger the advance: on every reviewing reconcile,
// if every owned MR has drained its pendingReview, take the F.3 edge NOW.
func TestReconcile_ReviewHandoffReDrivesTheDroppedAdvance(t *testing.T) {
	base := time.Unix(1000, 0)
	committed := tatarav1alpha1.OutcomeReasonFor(stage.AgentReview)

	// A DRAINED owned MR (pendingReview == nil) whose advance was dropped must be
	// re-driven INSIDE the deadline - not left to wait it out and park. A
	// kind=review Task advances to parked/awaiting-human (fixing/merging a human's
	// PR is a human action).
	t.Run("a drained MR advances inside the deadline instead of waiting", func(t *testing.T) {
		// reviewAdvanceEdge (reviewpost.go) returns
		// Edge{To: stage.ParkTarget, Reason: ReasonAwaitingHuman} for EVERY
		// kind=review verdict (#521 made park a flag, not an edge), and
		// stage.Enter refuses any target outside the table. This is the test
		// that proves reconcileClocks' review re-drive branches on ParkTarget
		// before calling r.enter. Do not re-skip it: without the branch every
		// kind=review Task whose outcome commits and needs re-driving fails the
		// reconcile with an IllegalTransitionError.

		task := tsReviewTaskWithOutcome(committed, 0, base)
		mr := mdMR(task, "tatara-operator", 364)
		mr.Status.Status = "approved" // the drain settled it
		proj := tsProject(3)
		r := tsReconciler(newMirrorClient(t, proj, mdSecret(), task, mr))

		got := tsReconcile(t, r, proj, task, base.Add(tatarav1alpha1.HandoffDeadline-time.Second))

		if got.Status.State != tatarav1alpha1.StateAwaitingReview || !tatarav1alpha1.Parked(got) ||
			got.Status.ParkReason != stage.ReasonAwaitingHuman {
			t.Fatalf("state=%q parked=%v reason=%q, want awaiting-review/parked/awaiting-human: the dropped advance must be re-driven, not waited out",
				got.Status.State, tatarav1alpha1.Parked(got), got.Status.ParkReason)
		}
	})

	// The incident case: past the deadline, a drained MR must reach its CORRECT
	// terminal (awaiting-human), NOT the spurious handoff-stalled park.
	t.Run("a drained MR reaches awaiting-human past the deadline, not handoff-stalled", func(t *testing.T) {
		// The past-the-deadline twin of the subtest above, over the same
		// ParkTarget branch in reconcileClocks' review re-drive.

		task := tsReviewTaskWithOutcome(committed, 0, base)
		mr := mdMR(task, "tatara-operator", 366)
		mr.Status.Status = "approved"
		proj := tsProject(3)
		r := tsReconciler(newMirrorClient(t, proj, mdSecret(), task, mr))

		got := tsReconcile(t, r, proj, task, base.Add(tatarav1alpha1.HandoffDeadline+time.Second))

		if got.Status.ParkReason == stage.ReasonHandoffStalled {
			t.Fatalf("reason = handoff-stalled: a review that already landed must advance, not park stalled")
		}
		if got.Status.State != tatarav1alpha1.StateAwaitingReview || !tatarav1alpha1.Parked(got) ||
			got.Status.ParkReason != stage.ReasonAwaitingHuman {
			t.Fatalf("state=%q parked=%v reason=%q, want awaiting-review/parked/awaiting-human",
				got.Status.State, tatarav1alpha1.Parked(got), got.Status.ParkReason)
		}
	})

	// The re-drive must NOT advance while the drain is genuinely outstanding: an
	// owned MR still carrying a pendingReview means the review has NOT been posted
	// yet, so the Task must stay reviewing (the one signal that distinguishes a
	// requested-not-posted review from a drained one is pendingReview itself).
	t.Run("an UNdrained MR does not advance", func(t *testing.T) {
		task := tsReviewTaskWithOutcome(committed, 0, base)
		mr := mdMR(task, "tatara-operator", 364)
		mr.Status.Status = "approved"
		mr.Status.PendingReview = &tatarav1alpha1.PendingReview{Round: 1, SHA: "deadbeef"}
		proj := tsProject(3)
		r := tsReconciler(newMirrorClient(t, proj, mdSecret(), task, mr))

		got := tsReconcile(t, r, proj, task, base.Add(tatarav1alpha1.HandoffDeadline-time.Second))

		if got.Status.State != tatarav1alpha1.StateAwaitingReview {
			t.Fatalf("stage=%q reason=%q, want reviewing: an undrained pendingReview must not advance the Task",
				got.Status.State, got.Status.ParkReason)
		}
	})

	// The implement-review flow: an approved, drained MR on a non-review Task
	// advances to merging (the operator then merges on green CI).
	t.Run("an approved drained MR on an implement Task advances to merging", func(t *testing.T) {
		task := tsReviewTaskWithOutcome(committed, 0, base)
		task.Spec.Kind = stage.AgentImplement
		mr := mdMR(task, "tatara-operator", 364)
		mr.Status.Status = "approved"
		proj := tsProject(3)
		r := tsReconciler(newMirrorClient(t, proj, mdSecret(), task, mr))

		got := tsReconcile(t, r, proj, task, base.Add(tatarav1alpha1.HandoffDeadline-time.Second))

		if got.Status.State != tatarav1alpha1.StateMerged {
			t.Fatalf("stage=%q reason=%q, want merging: an approved drained MR must advance an implement Task to merging",
				got.Status.State, got.Status.ParkReason)
		}
	})

	// Issue #403: the MR reconciler's OWN advance already fired (the level-
	// triggered comment above reviewAdvanceEdge's call site says this costs "at
	// most one illegal-edge counter" pre-fix) - the PERSISTED Task is already at
	// the re-drive's target stage, but this reconcile runs against the
	// pre-advance in-memory copy (still reviewing). The redundant X->X must be a
	// silent no-op: no error, no illegal-transition counter bump, stage
	// unchanged at its already-correct value.
	t.Run("already-at-target: a redundant re-drive is a no-op, not a counted illegal edge", func(t *testing.T) {
		task := tsReviewTaskWithOutcome(committed, 0, base)
		task.Spec.Kind = stage.AgentImplement
		mr := mdMR(task, "tatara-operator", 364)
		mr.Status.Status = "approved"
		proj := tsProject(3)
		live := task.DeepCopy()
		live.Status.State = tatarav1alpha1.StateMerged
		r := tsReconciler(newMirrorClient(t, proj, mdSecret(), live, mr))

		before := illegalCount(t, obs.IllegalStageTransitionCounter(tatarav1alpha1.StateMerged, tatarav1alpha1.StateMerged))
		got := tsReconcile(t, r, proj, task, base.Add(tatarav1alpha1.HandoffDeadline-time.Second))
		after := illegalCount(t, obs.IllegalStageTransitionCounter(tatarav1alpha1.StateMerged, tatarav1alpha1.StateMerged))

		if got.Status.State != tatarav1alpha1.StateMerged {
			t.Fatalf("stage=%q, want merging (already at target, unchanged)", got.Status.State)
		}
		if after != before {
			t.Fatalf("operator_illegal_stage_transition_total{merging,merging} = %v, want unchanged", after-before)
		}
	})
}

// The reconcileCaps B2 guard must be scoped to THIS stage's agent kind exactly as
// reconcilePodStage's is. An implement-Reason Task at reviewing whose review pod
// RAN and vanished with its recreations exhausted must still park(no-outcome): a
// bare OutcomeCommitted check at that site would suppress the cap on a Task whose
// committed outcome belongs to a stage it already left.
func TestReconcile_CapsSuppressionIsScopedToTheStagesOwnAgentKind(t *testing.T) {
	now := time.Unix(1000, 0)
	task := tsReviewTaskWithOutcome(tatarav1alpha1.OutcomeReasonFor(stage.AgentImplement),
		somePodRecreations, now.Add(-time.Minute))
	task.Spec.Kind = stage.AgentImplement
	proj := tsProject(3)
	r := tsReconciler(newMirrorClient(t, proj, mdSecret(), task)) // no Pod object -> podGone

	got := tsReconcile(t, r, proj, task, now)

	if got.Status.State != tatarav1alpha1.StateAwaitingReview || !tatarav1alpha1.Parked(got) || got.Status.ParkReason != stage.ReasonNoOutcome {
		t.Fatalf("state=%q parked=%v reason=%q, want awaiting-review/parked/no-outcome: only an outcome committed BY THIS STAGE'S agent may suppress the caps",
			got.Status.State, tatarav1alpha1.Parked(got), got.Status.ParkReason)
	}
}

// The same scoping in the other axis: the agent kind matches, but the commit is
// from a PREVIOUS occupancy of reviewing (a head-move re-entry, or the
// awaiting-human unpark). THIS occupancy's pod ran and vanished with its
// recreations spent, so the caps own it exactly as they own any other stage whose
// agent submitted nothing.
func TestReconcile_CapsSuppressionIsScopedToTheCurrentStageOccupancy(t *testing.T) {
	base := time.Unix(1000, 0)
	task := tsReviewTaskWithOutcome(tatarav1alpha1.OutcomeReasonFor(stage.AgentReview),
		somePodRecreations, base)
	reEntered := metav1.NewTime(base.Add(time.Hour))
	ran := metav1.NewTime(base.Add(time.Hour + time.Minute))
	task.Status.StateEnteredAt = &reEntered
	task.Status.PodStartedAt = &ran
	task.Status.StateWorkStartedAt = &ran
	proj := tsProject(3)
	r := tsReconciler(newMirrorClient(t, proj, mdSecret(), task)) // no Pod object -> podGone

	got := tsReconcile(t, r, proj, task, base.Add(time.Hour+2*time.Minute))

	if got.Status.State != tatarav1alpha1.StateAwaitingReview || !tatarav1alpha1.Parked(got) || got.Status.ParkReason != stage.ReasonNoOutcome {
		t.Fatalf("state=%q parked=%v reason=%q, want awaiting-review/parked/no-outcome: a commit predating stageEnteredAt must not suppress this occupancy's caps",
			got.Status.State, tatarav1alpha1.Parked(got), got.Status.ParkReason)
	}
}

// The B2 suppression is scoped to the two POD-LIVENESS caps by REASON, and that
// reason clause is the only thing holding the line. The TURN BUDGET is not a
// O3 INVERTED THIS TEST, and the inversion is the whole point of the phase. The
// turn budget it used to protect is DELETED, so a Task at 400 turns with a live
// pod has no BudgetExit left to trip at all - suppressed or not. What survives is
// the assertion that the B2 guard does not manufacture a park of its own.
func TestReconcile_ATaskAtFourHundredTurnsWithALivePodIsNotParked(t *testing.T) {
	now := time.Unix(1000, 0)
	// THIS occupancy's own review outcome, committed at stageEnteredAt: the
	// handoff condition is armed, so the B2 guard IS entered.
	task := tsReviewTaskWithOutcome(tatarav1alpha1.OutcomeReasonFor(stage.AgentReview), 0, now)
	task.Status.Stats.Turns = 400
	proj := tsProject(3)
	// A LIVE, Ready pod: podGone is false, so no-outcome cannot fire either.
	r := tsReconciler(newMirrorClient(t, proj, mdSecret(), task, tsReadyPod(task)))

	got := tsReconcile(t, r, proj, task, now.Add(time.Minute))

	if tatarav1alpha1.Parked(got) {
		t.Fatalf("parked at %q: neither the deleted turn budget nor the B2 guard may park a working Task",
			got.Status.ParkReason)
	}
	if got.Status.State != tatarav1alpha1.StateAwaitingReview {
		t.Fatalf("state = %q, want awaiting-review", got.Status.State)
	}
}

// handoffCondition FAILS CLOSED on a Task with no stage stamp. The occupancy
// check is "did the commit land at or after stageEnteredAt", and with no
// stageEnteredAt there is no occupancy to compare against - so there is no
// handoff to bound either, and a suppression that cannot be bounded must never
// be granted. Every path into a stage runs stage.Enter, which always stamps it,
// so this is unreachable today; fail-closed is what keeps it that way.
//
// Invert the nil check to fail OPEN and a Task that somehow lost its stamp
// suppresses BOTH pod-liveness caps forever: the handoff deadline reads the same
// nil stamp and disarms, so nothing bounds it in the other direction either.
// That is the one shape where the guard has no backstop at all, and no other test
// constructs it.
func TestHandoffCondition_FailsClosedWithNoStageStamp(t *testing.T) {
	base := time.Unix(1000, 0)

	t.Run("handoffCondition returns nil", func(t *testing.T) {
		task := tsReviewTaskWithOutcome(tatarav1alpha1.OutcomeReasonFor(stage.AgentReview), 0, base)
		task.Status.StateEnteredAt = nil

		if got := handoffCondition(task); got != nil {
			t.Fatalf("handoffCondition = %+v, want nil: with no stage stamp there is no occupancy to attribute the commit to, "+
				"and no handoff deadline to bound the suppression", got)
		}
	})

	t.Run("so the caps apply normally", func(t *testing.T) {
		task := tsReviewTaskWithOutcome(tatarav1alpha1.OutcomeReasonFor(stage.AgentReview),
			somePodRecreations, base)
		task.Status.StateEnteredAt = nil
		proj := tsProject(3)
		r := tsReconciler(newMirrorClient(t, proj, mdSecret(), task)) // no Pod object -> podGone

		got := tsReconcile(t, r, proj, task, base.Add(time.Minute))

		if got.Status.State != tatarav1alpha1.StateAwaitingReview || !tatarav1alpha1.Parked(got) || got.Status.ParkReason != stage.ReasonNoOutcome {
			t.Fatalf("state=%q parked=%v reason=%q, want awaiting-review/parked/no-outcome: an unbounded suppression must never be granted",
				got.Status.State, tatarav1alpha1.Parked(got), got.Status.ParkReason)
		}
	})
}

// #33 SHAPE: a kind=review Task sitting in reviewing whose PR a human merged,
// with NO outcome ever committed (no handoffCondition armed), is finalized to
// delivered(mr-merged-externally) by reconcileClocks - not respawn-looped.
func TestReconcileClocks_ReviewMergedExternally_FinalizesDelivered(t *testing.T) {
	proj := mdProject()
	task := mdTask("t1", "review", tatarav1alpha1.StateAwaitingReview)
	task.Status.StateEnteredAt = &metav1.Time{Time: time.Now()}
	mr := mdMR(task, "tatara-operator", 33)
	mr.Status.State = "merged"
	c := newMirrorClient(t, proj, mdSecret(), mdRepo("tatara-operator"), task, mr)
	r := tsReconciler(c)

	_, handled, err := r.reconcileClocks(context.Background(), proj, task, time.Now())
	require.NoError(t, err)
	require.True(t, handled)
	got := mdGetTask(t, c, "t1")
	require.Equal(t, tatarav1alpha1.StateDone, got.Status.State)
	require.Equal(t, stage.ReasonMRMergedExternally, got.Status.StateReason)
}

func TestReconcileClocks_ReviewClosedExternally_FinalizesRejected(t *testing.T) {
	proj := mdProject()
	task := mdTask("t1", "review", tatarav1alpha1.StateAwaitingReview)
	task.Status.StateEnteredAt = &metav1.Time{Time: time.Now()}
	mr := mdMR(task, "tatara-operator", 33)
	mr.Status.State = "closed"
	c := newMirrorClient(t, proj, mdSecret(), mdRepo("tatara-operator"), task, mr)
	r := tsReconciler(c)

	_, handled, err := r.reconcileClocks(context.Background(), proj, task, time.Now())
	require.NoError(t, err)
	require.True(t, handled)
	got := mdGetTask(t, c, "t1")
	require.Equal(t, tatarav1alpha1.StateRejected, got.Status.State)
	require.Equal(t, stage.ReasonMRClosedExternally, got.Status.StateReason)
}

// An OPEN MR must NOT finalize: the normal reviewing path continues.
func TestReconcileClocks_ReviewOpenMR_DoesNotFinalize(t *testing.T) {
	proj := mdProject()
	task := mdTask("t1", "review", tatarav1alpha1.StateAwaitingReview)
	task.Status.StateEnteredAt = &metav1.Time{Time: time.Now()}
	mr := mdMR(task, "tatara-operator", 33) // mdMR defaults State "open"
	c := newMirrorClient(t, proj, mdSecret(), mdRepo("tatara-operator"), task, mr)
	r := tsReconciler(c)

	_, _, err := r.reconcileClocks(context.Background(), proj, task, time.Now())
	require.NoError(t, err)
	got := mdGetTask(t, c, "t1")
	require.Equal(t, tatarav1alpha1.StateAwaitingReview, got.Status.State, "an open MR must not finalize")
}

// #33 SHAPE AT THE SOURCE: ensureStagePod must never build a review pod for a
// Task whose owned MR already reached a terminal forge state - reconcileClocks
// finalizing it on the NEXT reconcile is too late if a pod was already spawned
// this reconcile. tsReconciler's Session is a panicking pod factory, so "no pod
// spawned" is provable by the absence of a panic plus the finalize below.
func TestEnsureStagePod_ReviewMRTerminal_SpawnsNoPodAndFinalizes(t *testing.T) {
	proj := mdProject()
	task := mdTask("t1", "review", tatarav1alpha1.StateAwaitingReview)
	mr := mdMR(task, "tatara-operator", 33)
	mr.Status.State = "merged"
	c := newMirrorClient(t, proj, mdSecret(), mdRepo("tatara-operator"), task, mr)
	r := tsReconciler(c)

	skipped, err := r.ensureStagePod(context.Background(), proj, task)
	require.NoError(t, err)
	require.True(t, skipped, "a terminal-MR review Task must spawn no pod")
	got := mdGetTask(t, c, "t1")
	require.Equal(t, tatarav1alpha1.StateDone, got.Status.State)
	require.Equal(t, stage.ReasonMRMergedExternally, got.Status.StateReason)

	// No wrapper pod was created.
	var pod corev1.Pod
	err = c.Get(context.Background(), types.NamespacedName{Namespace: mdNS, Name: agent.PodName(task)}, &pod)
	require.True(t, apierrors.IsNotFound(err), "no wrapper pod may exist")
}

func TestEnsureStagePod_ReviewMROpen_ProceedsNormally(t *testing.T) {
	proj := mdProject()
	task := mdTask("t1", "review", tatarav1alpha1.StateAwaitingReview)
	mr := mdMR(task, "tatara-operator", 33) // open
	c := newMirrorClient(t, proj, mdSecret(), mdRepo("tatara-operator"), task, mr)
	r := tsReconciler(c)
	// Reaching normal pod creation past the guard needs a satisfiable
	// PodConfig (tsReconciler's default has no AnthropicSecretName/
	// CLIOIDCSecretName, matching the neighboring pod-creation tests' pattern).
	r.PodConfig = agent.PodConfig{
		Namespace:           mdNS,
		AnthropicSecretName: "anthropic",
		CLIOIDCSecretName:   "cli-oidc",
	}

	skipped, err := r.ensureStagePod(context.Background(), proj, task)
	require.NoError(t, err)
	require.False(t, skipped, "an open-MR review Task must proceed to normal pod creation")
	got := mdGetTask(t, c, "t1")
	require.Equal(t, tatarav1alpha1.StateAwaitingReview, got.Status.State, "no finalize on an open MR")
}

// TestRespawnLostPod_CountsThePodRecreation.
//
// operator_pod_recreations_total IS THE ALERT THAT REPLACED THE CAP. O3 deleted
// the enforcement, so this counter is now the ONLY thing that makes a crash loop
// visible before the 24h residency dead-man switch - it has to fire on every
// attempt, at every recreation count, and it must never park.
func TestRespawnLostPod_CountsThePodRecreation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		recreations int
		wantParked  bool
	}{
		{"first respawn", 0, false},
		{"deep into a crash loop", 12, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			task := tsTask("lost-pod-metric", "implement", tatarav1alpha1.StateUnderImplementation, now.Add(-time.Hour))
			pod := metav1.NewTime(now.Add(-30 * time.Minute))
			work := metav1.NewTime(now.Add(-29 * time.Minute))
			task.Status.PodStartedAt = &pod
			task.Status.StateWorkStartedAt = &work
			task.Status.Stats.PodRecreations = tc.recreations
			proj := tsProject(3)
			r := tsReconciler(newMirrorClient(t, proj, mdSecret(), task))

			c := obs.PodRecreationCounter(task.Spec.ProjectRef, task.Spec.Kind, obs.RecreationReasonPodGone)
			before := testutil.ToFloat64(c)
			if _, err := r.respawnLostPod(context.Background(), proj, task, obs.RecreationReasonPodGone, now); err != nil {
				t.Fatalf("respawnLostPod: %v", err)
			}
			if got := testutil.ToFloat64(c) - before; got != 1 {
				t.Errorf("operator_pod_recreations_total delta = %v, want 1", got)
			}
			if tatarav1alpha1.Parked(mdGetTask(t, r.Client, task.Name)) != tc.wantParked {
				t.Errorf("parked = %v, want %v", !tc.wantParked, tc.wantParked)
			}
		})
	}
}

// TestTTLCauseForLiveExit maps this package's live-exit vocabulary onto the stop
// metric's. The two are deliberately NOT one set of constants: causeIdle/
// causeEvicted also label operator_live_closed_total, an already-emitted series,
// and re-spelling "evicted" there would break every existing selector on it.
func TestTTLCauseForLiveExit(t *testing.T) {
	if got := ttlCauseForLiveExit(causeEvicted); got != agent.TTLCauseEviction {
		t.Errorf("evicted -> %q, want %q", got, agent.TTLCauseEviction)
	}
	if got := ttlCauseForLiveExit(causeIdle); got != agent.TTLCauseIdle {
		t.Errorf("idle -> %q, want %q", got, agent.TTLCauseIdle)
	}
}
