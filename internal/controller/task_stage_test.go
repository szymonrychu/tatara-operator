package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// THE RECONCILER HALF OF THE STAGE CONTRACT (Section I).
//
// internal/stage/stage_test.go holds the PURE half: the F.3 table, the F.4
// budgets, Unpark. This file holds the half that actually decides whether the
// platform works - that the RECONCILER applies those clocks, refuses those
// transitions, and does not kill a Task for the crime of waiting in a queue.

// tsProject deliberately carries Phase=Ready but NO ReadySince, so
// memoryStablyReady reads false (issue #355's pre-SubmitTurn gate holds any
// PodStartedAt==nil task here). Most tsProject-based tests never intend to
// reach a real wrapper Pod build (PodConfig carries no AnthropicSecretName),
// so this incidental "not stably ready" is what keeps them from ever reaching
// ensureStagePod's ValidatePodSecretRefs. A test that DOES need to submit a
// turn must opt in explicitly via tsStablyReadyProject.
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

// tsTask is a Task already at a stage, with stageEnteredAt set.
func tsTask(name, kind, stg string, enteredAt time.Time) *tatarav1alpha1.Task {
	at := metav1.NewTime(enteredAt)
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: mdNS, UID: types.UID("uid-" + name)},
		Spec:       tatarav1alpha1.TaskSpec{Kind: kind, ProjectRef: "proj", Goal: "do the thing"},
		Status: tatarav1alpha1.TaskStatus{
			Stage:          stg,
			StageEnteredAt: &at,
			AgentKind:      stage.AgentKindFor(stg),
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
		PodConfig: agent.PodConfig{Namespace: mdNS},
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
	cachedTask := tsTask("drift", "clarify", tatarav1alpha1.StageReviewing, time.Now())
	proj := tsProject(3)
	cached := newMirrorClient(t, proj, mdSecret(), cachedTask)
	live := newMirrorClient(t, proj, mdSecret(),
		tsTask("drift", "clarify", tatarav1alpha1.StageMerging, time.Now()))
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
	acting := tsTask("d", "clarify", tatarav1alpha1.StageReviewing, time.Now())
	proj := tsProject(3)
	r := tsReconciler(newMirrorClient(t, proj, acting))

	if r.liveStageDiffers(context.Background(), acting) {
		t.Fatal("nil APIReader must return false (trust the cache)")
	}
	r.APIReader = newMirrorClient(t, proj, tsTask("d", "clarify", tatarav1alpha1.StageReviewing, time.Now()))
	if r.liveStageDiffers(context.Background(), acting) {
		t.Fatal("live stage equal to the acting stage must return false")
	}
	r.APIReader = newMirrorClient(t, proj, tsTask("d", "clarify", tatarav1alpha1.StageMerging, time.Now()))
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
	task := tsTask("queued", "clarify", tatarav1alpha1.StageClarifying, entered)
	// It has no pod: it is waiting for an admission slot. podStartedAt == nil.
	proj := tsProject(3)
	c := newMirrorClient(t, proj, mdSecret(), task)
	r := tsReconciler(c)

	got := tsReconcile(t, r, proj, task, time.Now())

	if got.Status.Stage != tatarav1alpha1.StageClarifying {
		t.Fatalf("stage = %q, want clarifying: a Task that queued 40m in normal steady state MUST NOT move",
			got.Status.Stage)
	}
	if got.Status.StageReason != "" {
		t.Fatalf("stageReason = %q, want empty: queueing is not a fault", got.Status.StageReason)
	}
	if tatarav1alpha1.StageTerminal(got) {
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
	task := tsTask("slow-queue", "incident", tatarav1alpha1.StageInvestigating, now.Add(-3*time.Hour))
	podAt := metav1.NewTime(now.Add(-2 * time.Minute))
	workAt := metav1.NewTime(now.Add(-1 * time.Minute))
	task.Status.PodStartedAt = &podAt
	task.Status.StageWorkStartedAt = &workAt
	proj := tsProject(3)
	c := newMirrorClient(t, proj, mdSecret(), task, tsReadyPod(task))
	r := tsReconciler(c)
	r.Session = newFakeSession() // this Task is ALLOWED to run: it is not a review Task

	got := tsReconcile(t, r, proj, task, now)

	if got.Status.Stage != tatarav1alpha1.StageInvestigating {
		t.Fatalf("stage = %q, want investigating: the 2h budget runs from stageWorkStartedAt (fix H12), not from the 3h queue wait",
			got.Status.Stage)
	}
}

// ---------------------------------------------------------------------------
// THE THREE CLOCKS (F.4). Gap 5: nothing drove them before.
// ---------------------------------------------------------------------------

func TestReconcilerAppliesTheThreeClocks(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name       string
		maxAgents  int
		mutate     func(*tatarav1alpha1.Task)
		stg        string
		entered    time.Duration // before now
		wantStage  string
		wantReason string
	}{
		{
			// CLOCK 1: pod stage, podStartedAt == nil, 24h from stageEnteredAt.
			name: "clock1 admission starved", maxAgents: 3,
			stg: tatarav1alpha1.StageImplementing, entered: 25 * time.Hour,
			wantStage: tatarav1alpha1.StageParked, wantReason: stage.ReasonAdmissionStarved,
		},
		{
			// CLOCK 1 is SKIPPED ENTIRELY on a PAUSED project. The pause is a kill
			// switch, not a backlog shredder.
			name: "clock1 skipped while paused", maxAgents: 0,
			stg: tatarav1alpha1.StageImplementing, entered: 30 * time.Hour,
			wantStage: tatarav1alpha1.StageImplementing, wantReason: "",
		},
		{
			// CLOCK 3 on a POD stage: from stageWorkStartedAt, the F.4 budget.
			name: "clock3 work budget", maxAgents: 3,
			stg: tatarav1alpha1.StageBrainstorming, entered: 10 * time.Hour,
			mutate: func(tk *tatarav1alpha1.Task) {
				pod := metav1.NewTime(now.Add(-3 * time.Hour))
				work := metav1.NewTime(now.Add(-2*time.Hour - time.Minute))
				tk.Status.PodStartedAt = &pod
				tk.Status.StageWorkStartedAt = &work
			},
			wantStage: tatarav1alpha1.StageParked, wantReason: stage.ReasonStageDeadline,
		},
		{
			// A POD-LESS stage runs CLOCK 3 from stageEnteredAt (contradiction #5,
			// the budget table wins). Without this merging NEVER reaches
			// merge-timeout and the bounded merge cycle never engages at all.
			name: "podless merging reaches merge-timeout", maxAgents: 3,
			stg: tatarav1alpha1.StageMerging, entered: 5 * time.Hour,
			wantStage: tatarav1alpha1.StageParked, wantReason: stage.ReasonMergeTimeout,
		},
		{
			name: "podless deploying reaches deploy-timeout", maxAgents: 3,
			stg: tatarav1alpha1.StageDeploying, entered: 3 * time.Hour,
			wantStage: tatarav1alpha1.StageParked, wantReason: stage.ReasonDeployTimeout,
		},
		{
			// approved is pod-less AND its budget elapses to admission-starved, so
			// the paused carve-out covers it too.
			name: "approved admission budget skipped while paused", maxAgents: 0,
			stg: tatarav1alpha1.StageApproved, entered: 30 * time.Hour,
			wantStage: tatarav1alpha1.StageApproved, wantReason: "",
		},
		{
			name: "approved admission budget fires when running", maxAgents: 3,
			stg: tatarav1alpha1.StageApproved, entered: 25 * time.Hour,
			wantStage: tatarav1alpha1.StageParked, wantReason: stage.ReasonAdmissionStarved,
		},
		{
			// The 5m triage budget.
			name: "triaging stalls", maxAgents: 3,
			stg: tatarav1alpha1.StageTriaging, entered: 6 * time.Minute,
			wantStage: tatarav1alpha1.StageFailed, wantReason: stage.ReasonTriageStalled,
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

			if got.Status.Stage != tc.wantStage || got.Status.StageReason != tc.wantReason {
				t.Fatalf("stage=%q reason=%q, want %q/%q",
					got.Status.Stage, got.Status.StageReason, tc.wantStage, tc.wantReason)
			}
		})
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
	task := tsTask("t", "implement", tatarav1alpha1.StageImplementing, time.Now())
	if err := stage.Enter(task, nil, tatarav1alpha1.StageFailed, "pod-not-ready", time.Now()); err == nil {
		t.Fatal("stage.Enter accepted failed(pod-not-ready)")
	}
}

// ---------------------------------------------------------------------------
// THE CAPS (F.4).
// ---------------------------------------------------------------------------

func TestTurnBudgetExhausted(t *testing.T) {
	now := time.Now()
	task := tsTask("burner", "implement", tatarav1alpha1.StageImplementing, now.Add(-time.Minute))
	podAt := metav1.NewTime(now.Add(-time.Minute))
	workAt := metav1.NewTime(now.Add(-30 * time.Second))
	task.Status.PodStartedAt = &podAt
	task.Status.StageWorkStartedAt = &workAt
	task.Status.Stats.Turns = defaultMaxTurnsPerTask
	proj := tsProject(3)
	c := newMirrorClient(t, proj, mdSecret(), task, tsReadyPod(task))

	got := tsReconcile(t, tsReconciler(c), proj, task, now)

	if got.Status.Stage != tatarav1alpha1.StageFailed ||
		got.Status.StageReason != stage.ReasonTurnBudgetExhausted {
		t.Fatalf("stage=%q reason=%q, want failed/turn-budget-exhausted",
			got.Status.Stage, got.Status.StageReason)
	}
}

// A pod that RAN and vanished with no outcome, with the recreation budget spent,
// parks at no-outcome. A pod that never ran (podStartedAt == nil - the admission
// queue) is NOT this, and that distinction is the whole of the V6-1 fix.
func TestPodStoppedWithNoOutcomeParksOnlyWhenTheBudgetIsSpent(t *testing.T) {
	now := time.Now()
	task := tsTask("lost", "implement", tatarav1alpha1.StageImplementing, now.Add(-time.Hour))
	podAt := metav1.NewTime(now.Add(-30 * time.Minute))
	workAt := metav1.NewTime(now.Add(-29 * time.Minute))
	task.Status.PodStartedAt = &podAt
	task.Status.StageWorkStartedAt = &workAt
	task.Status.Stats.PodRecreations = maxPodRecreations
	proj := tsProject(3)
	// No Pod object: the pod is GONE.
	c := newMirrorClient(t, proj, mdSecret(), task)

	got := tsReconcile(t, tsReconciler(c), proj, task, now)

	if got.Status.Stage != tatarav1alpha1.StageParked ||
		got.Status.StageReason != stage.ReasonNoOutcome {
		t.Fatalf("stage=%q reason=%q, want parked/no-outcome", got.Status.Stage, got.Status.StageReason)
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
	task := tsTask("rev", "review", tatarav1alpha1.StageReviewing, time.Now())
	mr := mdMR(task, "tatara-operator", 9)
	edge, ok := stage.RequestChanges(task, []tatarav1alpha1.MergeRequest{*mr}, 3)
	if !ok {
		t.Fatal("RequestChanges returned no edge")
	}
	if edge.To != tatarav1alpha1.StageParked || edge.Reason != stage.ReasonAwaitingHuman {
		t.Fatalf("edge = %s(%s), want parked(awaiting-human)", edge.To, edge.Reason)
	}
}

// THE EMPTY SET IS NOT A LICENCE. A review Task owns ZERO Issues, and no
// universal quantifier over an empty set may ever gate code execution. The
// choke point REFUSES the transition, no pod is created, and the illegal-edge
// counter fires. The Session panics if a turn is ever submitted.
func TestReviewTaskCanNeverEnterImplementingOrMerging(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	for _, from := range []string{
		tatarav1alpha1.StageReviewing,
		tatarav1alpha1.StageParked,
		tatarav1alpha1.StageApproved,
		tatarav1alpha1.StageTriaging,
	} {
		for _, to := range []string{tatarav1alpha1.StageImplementing, tatarav1alpha1.StageMerging} {
			task := tsTask("rev", "review", from, now)
			task.Status.StageReason = stage.ReasonAwaitingHuman
			proj := tsProject(3)
			c := newMirrorClient(t, proj, mdSecret(), task)
			r := tsReconciler(c)

			before := illegalCount(t, obs.IllegalStageTransitionCounter(from, to))
			err := r.enter(ctx, proj, task, nil, to, "", now)
			if err == nil {
				t.Fatalf("%s -> %s was ACCEPTED on a kind=review Task. There is no path, no condition, no exception", from, to)
			}
			got := mdGetTask(t, c, task.Name)
			if got.Status.Stage != from {
				t.Fatalf("the refused transition was WRITTEN anyway: stage = %q", got.Status.Stage)
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

// triageTarget has NO implement row. Code execution is reached ONLY through
// clarifying -> approved -> implementing, i.e. only through the C.6 approval
// gate: a triaging -> implementing edge does not exist in F.3 and must not be
// invented by the reconciler.
func TestTriageNeverRoutesStraightToImplementing(t *testing.T) {
	if to, ok := triageTarget("implement"); ok {
		t.Fatalf("triage routes kind=implement straight to %q, skipping the approval gate", to)
	}
	want := map[string]string{
		"brainstorm":    tatarav1alpha1.StageBrainstorming,
		"clarify":       tatarav1alpha1.StageClarifying,
		"incident":      tatarav1alpha1.StageInvestigating,
		"refine":        tatarav1alpha1.StageRefining,
		"review":        tatarav1alpha1.StageReviewing,
		"documentation": tatarav1alpha1.StageDocumenting,
	}
	for kind, wantStage := range want {
		got, ok := triageTarget(kind)
		if !ok || got != wantStage {
			t.Fatalf("triageTarget(%q) = %q,%v; want %q", kind, got, ok, wantStage)
		}
		if !stage.Legal(tatarav1alpha1.StageTriaging, wantStage) {
			t.Fatalf("triaging -> %s is not in the F.3 table", wantStage)
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
	stages := stage.AllStages()

	refused := 0
	for _, from := range stages {
		for _, to := range stages {
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
			if got := mdGetTask(t, c, task.Name); got.Status.Stage != from {
				t.Fatalf("illegal transition %s -> %s was WRITTEN: stage = %q", from, to, got.Status.Stage)
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

// EVERY transition clears BOTH pod timestamps (fix V7-4). v6 forgot podStartedAt
// and it is load-bearing: a stale one leaves the Task under NO CLOCK while it
// queues on a re-entry edge, and TTL-stops its next pod before that pod's first
// turn.
func TestEveryTransitionClearsBothPodClocksAndResetsRecreations(t *testing.T) {
	now := time.Now()
	task := tsTask("t", "implement", tatarav1alpha1.StageImplementing, now.Add(-time.Hour))
	podAt := metav1.NewTime(now.Add(-30 * time.Minute))
	workAt := metav1.NewTime(now.Add(-29 * time.Minute))
	task.Status.PodStartedAt = &podAt
	task.Status.StageWorkStartedAt = &workAt
	task.Status.Stats.PodRecreations = 2
	proj := tsProject(3)
	c := newMirrorClient(t, proj, mdSecret(), task)
	r := tsReconciler(c)

	if err := r.enter(context.Background(), proj, task, nil, tatarav1alpha1.StageParked,
		stage.ReasonImplementDeclined, now); err != nil {
		t.Fatalf("enter: %v", err)
	}
	got := mdGetTask(t, c, task.Name)
	if got.Status.PodStartedAt != nil {
		t.Fatal("podStartedAt survived a transition (fix V7-4). The next pod is TTL-stopped before its first turn")
	}
	if got.Status.StageWorkStartedAt != nil {
		t.Fatal("stageWorkStartedAt survived a transition")
	}
	if got.Status.Stats.PodRecreations != 0 {
		t.Fatalf("podRecreations = %d after a transition, want 0", got.Status.Stats.PodRecreations)
	}
	if got.Status.StageEnteredAt == nil || !got.Status.StageEnteredAt.Time.Equal(now.UTC().Truncate(time.Second)) {
		// metav1.Time truncates to the second; compare at that resolution.
		if got.Status.StageEnteredAt == nil {
			t.Fatal("stageEnteredAt was not stamped")
		}
	}
	// The caller's in-memory copy follows the write.
	if task.Status.Stage != tatarav1alpha1.StageParked || task.Status.PodStartedAt != nil {
		t.Fatal("the choke point did not update the caller's copy")
	}
}

// D1: EVERY terminal entry fires operator_task_terminal_total{kind,stage,
// stageReason}. Twenty-nine tatara-observability rules ride on it, and it is the
// ONLY counter of terminal outcomes the platform has. A MINT is not an outcome.
func TestTerminalEntryFiresTheD1Metric(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	tests := []struct {
		name             string
		from, to, reason string
		kind             string
		wantFire         bool
	}{
		{"failed", tatarav1alpha1.StageTriaging, tatarav1alpha1.StageFailed, stage.ReasonTriageStalled, "implement", true},
		{"parked", tatarav1alpha1.StageImplementing, tatarav1alpha1.StageParked, stage.ReasonImplementDeclined, "implement", true},
		{"rejected", tatarav1alpha1.StageClarifying, tatarav1alpha1.StageRejected, stage.ReasonDeclined, "clarify", true},
		{"delivered", tatarav1alpha1.StageBrainstorming, tatarav1alpha1.StageDelivered, "", "brainstorm", true},
		{"non-terminal", tatarav1alpha1.StageTriaging, tatarav1alpha1.StageClarifying, "", "clarify", false},
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
}

// A MINT is not an outcome. The sweep mints a Task straight into
// parked(backlog-sweep): it never ran and never failed - it is the durable owner
// of an Issue CR at zero agent cost. Counting it as a park drowns the park-rate
// alerts in Tasks that never did anything.
func TestMintingParkedBacklogSweepDoesNotFireTheTerminalMetric(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := obs.NewOperatorMetrics(reg)
	m.TaskTerminalEntry("clarify", "", tatarav1alpha1.StageParked, stage.ReasonBacklogSweep)
	if got := terminalCount(t, reg, "clarify", tatarav1alpha1.StageParked, stage.ReasonBacklogSweep); got != 0 {
		t.Fatalf("a MINT fired operator_task_terminal_total (%v)", got)
	}
}

// Contract K.1: operator_task_parked_total increments once per park
// TRANSITION, labelled by the stage the Task parked FROM (the stalling
// stage), not "parked" itself. A MINT straight into parked (from == "") is not
// a transition - the sweep's zero-agent-cost mints must not inflate the
// park-rate signal, mirroring D1's mint exclusion for the terminal counter.
func TestTaskParkedFiresOnlyOnARealParkTransition(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	t.Run("real transition into parked increments the counter", func(t *testing.T) {
		task := tsTask("t-park", "implement", tatarav1alpha1.StageImplementing, now)
		proj := tsProject(3)
		c := newMirrorClient(t, proj, mdSecret(), task)
		reg := prometheus.NewRegistry()
		r := tsReconciler(c)
		r.Metrics = obs.NewOperatorMetrics(reg)

		if err := r.enter(ctx, proj, task, nil, tatarav1alpha1.StageParked, stage.ReasonImplementDeclined, now); err != nil {
			t.Fatalf("enter: %v", err)
		}
		got := testutil.ToFloat64(r.Metrics.TaskParkedCounter(tatarav1alpha1.StageImplementing, stage.ReasonImplementDeclined))
		if got != 1 {
			t.Fatalf("operator_task_parked_total{implementing,%s} = %v, want 1", stage.ReasonImplementDeclined, got)
		}
	})

	t.Run("a mint straight into parked does not increment the counter", func(t *testing.T) {
		// Create -> Parked(backlog-sweep) is a legal F.3 edge (the sweep's
		// zero-agent-cost mint path), and a fresh Task's Status.Stage is "" -
		// exactly the prev == "" mint case EnterStage's guard excludes.
		task := &tatarav1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{Name: "t-mint", Namespace: mdNS, UID: types.UID("uid-t-mint")},
			Spec:       tatarav1alpha1.TaskSpec{Kind: "clarify", ProjectRef: "proj", Goal: "do the thing"},
		}
		proj := tsProject(3)
		c := newMirrorClient(t, proj, mdSecret(), task)
		reg := prometheus.NewRegistry()
		r := tsReconciler(c)
		r.Metrics = obs.NewOperatorMetrics(reg)

		if err := r.enter(ctx, proj, task, nil, tatarav1alpha1.StageParked, stage.ReasonBacklogSweep, now); err != nil {
			t.Fatalf("enter (mint): %v", err)
		}
		mfs, err := reg.Gather()
		if err != nil {
			t.Fatalf("gather: %v", err)
		}
		for _, mf := range mfs {
			if mf.GetName() == "operator_task_parked_total" && len(mf.GetMetric()) != 0 {
				t.Fatalf("a MINT into parked fired operator_task_parked_total: %v", mf)
			}
		}
	})

	t.Run("a non-parked entry does not increment the counter", func(t *testing.T) {
		task := tsTask("t-nopark", "implement", tatarav1alpha1.StageTriaging, now)
		proj := tsProject(3)
		c := newMirrorClient(t, proj, mdSecret(), task)
		reg := prometheus.NewRegistry()
		r := tsReconciler(c)
		r.Metrics = obs.NewOperatorMetrics(reg)

		if err := r.enter(ctx, proj, task, nil, tatarav1alpha1.StageClarifying, "", now); err != nil {
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
	for _, stg := range stage.AllStages() {
		budget, ok := stage.Budget(stg)
		if !ok {
			t.Fatalf("stage %q has NO ROW in the F.4 deadline table", stg)
		}
		edge, ok := stage.OnElapse(stg)
		if !ok || edge.To == "" {
			t.Fatalf("stage %q has no onElapse edge", stg)
		}
		task := tsTask("t", "implement", stg, now.Add(-budget-time.Hour))
		if stg == tatarav1alpha1.StageParked {
			task.Status.StageReason = stage.ReasonStageDeadline // NOT backlog-sweep
		}
		if !tatarav1alpha1.StagePodless(stg) {
			// Arm CLOCK 3: a pod stage with no stamps is CLOCK 1 (admission), whose
			// exit is admission-starved, not the stage's own work budget.
			pod := metav1.NewTime(now.Add(-budget - time.Hour))
			work := metav1.NewTime(now.Add(-budget - time.Hour))
			task.Status.PodStartedAt = &pod
			task.Status.StageWorkStartedAt = &work
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
	// The ONE exemption, and it is a REASON, not a stage.
	sweep := tsTask("t", "clarify", tatarav1alpha1.StageParked, now.Add(-5*365*24*time.Hour))
	sweep.Status.StageReason = stage.ReasonBacklogSweep
	if clock, _, _, _ := stage.ArmedClock(sweep, false); clock != stage.ClockNone {
		t.Fatalf("parked(backlog-sweep) armed clock %s; it consumes nothing and NEVER ages out", clock)
	}
}

// The named F.4 case: podStartedAt == nil AND stageWorkStartedAt == nil is
// CLOCK 1. It is a case, not an inference.
func TestNoStampsIsClock1(t *testing.T) {
	task := tsTask("t", "implement", tatarav1alpha1.StageImplementing, time.Now())
	if task.Status.PodStartedAt != nil || task.Status.StageWorkStartedAt != nil {
		t.Fatal("fixture is wrong")
	}
	clock, since, budget, edge := stage.ArmedClock(task, false)
	if clock != stage.ClockAdmission {
		t.Fatalf("clock = %s, want admission", clock)
	}
	if !since.Equal(task.Status.StageEnteredAt.Time) {
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
	task := tsTask("rev", "review", tatarav1alpha1.StageReviewing, at)
	task.Status.PodStartedAt = &stamp
	task.Status.StageWorkStartedAt = &stamp
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
	// podRecreations == maxPodRecreations, so today's BudgetExit parks it
	// no-outcome the moment the pod is seen gone.
	task := tsReviewTaskWithOutcome(tatarav1alpha1.OutcomeReasonFor(stage.AgentReview),
		maxPodRecreations, now.Add(-time.Minute))
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
	if got.Status.Stage != tatarav1alpha1.StageReviewing {
		t.Fatalf("stage = %q(%s), want reviewing: a committed outcome must not be terminated by a pod-liveness cap",
			got.Status.Stage, got.Status.StageReason)
	}
	if got.Status.Stats.PodRecreations != maxPodRecreations {
		t.Fatalf("podRecreations = %d, want %d: no recreation may be burned",
			got.Status.Stats.PodRecreations, maxPodRecreations)
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
		maxPodRecreations, now.Add(-time.Minute))
	proj := tsProject(3)
	c := newMirrorClient(t, proj, mdSecret(), task)
	r := tsReconciler(c)

	got := tsReconcile(t, r, proj, task, now)

	if got.Status.Stage != tatarav1alpha1.StageParked || got.Status.StageReason != stage.ReasonNoOutcome {
		t.Fatalf("stage=%q reason=%q, want parked/no-outcome: a bare claim must NOT be protected, the caps apply exactly as they do today",
			got.Status.Stage, got.Status.StageReason)
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
	task.Status.StageWorkStartedAt = nil
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
				annPodStage: task.Status.Stage,
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
			match := map[string]string{"kind": kind, "stage": stg, "stageReason": reason}
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
	ctx := context.Background()
	now := time.Unix(1000, 0)
	stamp := metav1.NewTime(now.Add(-time.Minute))

	proj := tsProject(3)
	repo := mdRepo("tatara-agent-skills")
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "cgthv", Namespace: mdNS, UID: types.UID("uid-cgthv")},
		Spec:       tatarav1alpha1.TaskSpec{Kind: "review", ProjectRef: "proj", Goal: "review the PR"},
		Status: tatarav1alpha1.TaskStatus{
			Stage:              tatarav1alpha1.StageReviewing,
			AgentKind:          "review",
			StageEnteredAt:     &stamp,
			PodStartedAt:       &stamp,
			StageWorkStartedAt: &stamp,
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
	if task.Status.Stage != tatarav1alpha1.StageReviewing {
		t.Fatalf("stage = %q(%s), want reviewing: the review LANDED, a pod that is no longer needed must not fail the Task",
			task.Status.Stage, task.Status.StageReason)
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
	if got.Status.Stage != tatarav1alpha1.StageParked {
		t.Fatalf("stage = %q, want parked", got.Status.Stage)
	}
	if got.Status.StageReason != stage.ReasonAwaitingHuman {
		t.Fatalf("stageReason = %q, want awaiting-human: the expected happy path for a kind=review Task is a human's PR fixed and merged by the human",
			got.Status.StageReason)
	}
	if got.Status.StageReason == stage.ReasonPodRecreationExhausted {
		t.Fatalf("stageReason = pod-recreation-exhausted: this is the exact production failure being reproduced")
	}
}

// ---------------------------------------------------------------------------
// ISSUE #355: pre-SubmitTurn memory-readiness gate. reconcilePodStage must
// hold turn0 (never call SubmitTurn) when the project memory stack is not
// stably ready, even though StageWorkStartedAt/PodStartedAt are already set -
// closing the admission-time-gate-vs-actual-submit gap (a respawned or
// TTL-rotated pod, or a first pod that boots slowly, could otherwise submit a
// turn against a backend that went unhealthy after admission).
// ---------------------------------------------------------------------------

func TestTurnSubmit_HeldWhenMemoryNotStablyReady(t *testing.T) {
	now := time.Now()
	task := tsTask("ts-gated", "implement", tatarav1alpha1.StageImplementing, now.Add(-time.Minute))
	podAt := metav1.NewTime(now.Add(-30 * time.Second))
	workAt := metav1.NewTime(now.Add(-10 * time.Second))
	task.Status.PodStartedAt = &podAt
	task.Status.StageWorkStartedAt = &workAt
	issName := tatarav1alpha1.IssueName("tatara-operator", 1)
	task.Status.IssueRefs = []string{issName}

	proj := tsProject(3)
	proj.Status.Memory = &tatarav1alpha1.MemoryStatus{Phase: "Provisioning", Endpoint: "http://mem"}
	iss := ownedIssue(issName, 1, task, tatarav1alpha1.IssueStatus{State: "open"})

	c := newMirrorClient(t, proj, mdSecret(), task, tsReadyPod(task), iss)
	reg := prometheus.NewRegistry()
	r := tsReconciler(c) // panicSession: fails the test if SubmitTurn is ever called
	r.Metrics = obs.NewOperatorMetrics(reg)

	res, err := r.reconcileStage(context.Background(), proj, task, now)
	if err != nil {
		t.Fatalf("reconcileStage: %v", err)
	}
	if res.RequeueAfter != memGateRequeue {
		t.Fatalf("RequeueAfter = %v, want memGateRequeue %v", res.RequeueAfter, memGateRequeue)
	}
	got := mdGetTask(t, c, task.Name)
	if got.Annotations[annStageTurn0] != "" {
		t.Fatalf("annStageTurn0 = %q, want unset: no turn was submitted", got.Annotations[annStageTurn0])
	}
	if v := testutil.ToFloat64(r.Metrics.MemoryGateHoldCounter("proj")); v != 1 {
		t.Fatalf("operator_memory_gate_hold_total{project=proj} = %v, want 1", v)
	}
	gotIss := getIssueCR(t, c, issName)
	if len(gotIss.Status.PendingComments) != 1 {
		t.Fatalf("PendingComments = %d, want 1 (the held-turn surfacing comment)", len(gotIss.Status.PendingComments))
	}
	if gotIss.Status.LastMemoryGateCommentAt == nil {
		t.Fatalf("LastMemoryGateCommentAt not stamped")
	}

	// A second hold on the same episode must not enqueue a duplicate comment
	// (its own cooldown marker), same one-shot shape as deploy-timeout.
	if _, err := r.reconcileStage(context.Background(), proj, mdGetTask(t, c, task.Name), now.Add(time.Minute)); err != nil {
		t.Fatalf("second reconcileStage: %v", err)
	}
	gotIss2 := getIssueCR(t, c, issName)
	if len(gotIss2.Status.PendingComments) != 1 {
		t.Fatalf("PendingComments after second hold = %d, want 1 (own cooldown, no duplicate)", len(gotIss2.Status.PendingComments))
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
	task := tsTask("ts-ok", "implement", tatarav1alpha1.StageImplementing, now.Add(-time.Minute))
	podAt := metav1.NewTime(now.Add(-30 * time.Second))
	workAt := metav1.NewTime(now.Add(-10 * time.Second))
	task.Status.PodStartedAt = &podAt
	task.Status.StageWorkStartedAt = &workAt
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
	task := tsTask("ts-err", "review", tatarav1alpha1.StageReviewing, now.Add(-time.Minute))
	podAt := metav1.NewTime(now.Add(-30 * time.Second))
	workAt := metav1.NewTime(now.Add(-10 * time.Second))
	task.Status.PodStartedAt = &podAt
	task.Status.StageWorkStartedAt = &workAt
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
		Build()
}

// TestEnsureTicketClassByStageAgentKind covers the production symptom: an
// incident Task's DOWNSTREAM stages (clarify, implement, ...) were classed
// QueueClassAlert just because task.Spec.Kind == "incident", starving them
// behind AlertCapacity=1 alongside the investigating stage they queue behind.
// Only the investigating stage - whose agentKind IS incident - may draw from
// the alert pool; every other stage of the same incident Task is a normal
// downstream ticket and must use the normal pool.
func TestEnsureTicketClassByStageAgentKind(t *testing.T) {
	cases := []struct {
		name      string
		stg       string
		wantClass string
	}{
		{"investigating is alert-class", tatarav1alpha1.StageInvestigating, tatarav1alpha1.QueueClassAlert},
		{"clarifying is normal-class", tatarav1alpha1.StageClarifying, tatarav1alpha1.QueueClassNormal},
		{"implementing is normal-class", tatarav1alpha1.StageImplementing, tatarav1alpha1.QueueClassNormal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			task := tsTask("t-"+tc.stg, "incident", tc.stg, time.Now())
			proj := tsProject(3)
			c := ticketMirrorClient(t, proj, mdSecret(), task)
			r := tsReconciler(c)
			r.Seq = &queue.SeqSource{Client: c, Namespace: mdNS}

			agentKind := stage.AgentKindFor(tc.stg)
			if _, err := r.ensureTicket(ctx, proj, task, agentKind); err != nil {
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
				t.Errorf("ticket class = %q, want %q (task.Spec.Kind=incident, stage=%s, agentKind=%s)",
					found.Spec.Class, tc.wantClass, tc.stg, agentKind)
			}
		})
	}
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
		if got.Status.Stage != tatarav1alpha1.StageReviewing {
			t.Fatalf("stage = %q(%s), want reviewing: the deadline has not elapsed yet",
				got.Status.Stage, got.Status.StageReason)
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

		if got.Status.Stage != tatarav1alpha1.StageParked ||
			got.Status.StageReason != stage.ReasonHandoffStalled {
			t.Fatalf("stage=%q reason=%q, want parked/handoff-stalled: the B2 suppression must be bounded",
				got.Status.Stage, got.Status.StageReason)
		}
		if got.Status.ParkedFromStage != tatarav1alpha1.StageReviewing {
			t.Fatalf("parkedFromStage = %q, want reviewing", got.Status.ParkedFromStage)
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

		if got.Status.Stage != tatarav1alpha1.StageReviewing {
			t.Fatalf("stage = %q(%s), want reviewing: 1m since the commit is inside the 5m deadline",
				got.Status.Stage, got.Status.StageReason)
		}
	})

	t.Run("a BARE CLAIM never arms the handoff deadline", func(t *testing.T) {
		// It has no handoff to wait for. It is a failed-validation stub and the
		// ordinary caps own it.
		task := tsReviewTaskWithOutcome(tatarav1alpha1.OutcomeReasonClaimed, maxPodRecreations, base)
		proj := tsProject(3)
		r := tsReconciler(newMirrorClient(t, proj, mdSecret(), task))

		got := tsReconcile(t, r, proj, task, base.Add(tatarav1alpha1.HandoffDeadline+time.Second))

		if got.Status.StageReason == stage.ReasonHandoffStalled {
			t.Fatalf("reason = handoff-stalled: a bare claim has no handoff outstanding")
		}
		if got.Status.StageReason != stage.ReasonNoOutcome {
			t.Fatalf("reason = %q, want no-outcome: the ordinary caps own a bare claim", got.Status.StageReason)
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
		task.Status.StageWorkStartedAt = nil
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

		if got.Status.Stage != tatarav1alpha1.StageReviewing ||
			got.Status.StageReason == stage.ReasonHandoffStalled {
			t.Fatalf("stage=%q reason=%q, want reviewing: the implement commit is not THIS stage's handoff",
				got.Status.Stage, got.Status.StageReason)
		}
	})

	t.Run("a PREVIOUS round's review commit never arms the handoff deadline", func(t *testing.T) {
		// stage.Enter never clears the condition, so a Task that RE-ENTERS reviewing
		// carries the last round's Reason=Review commit with it: merging -> reviewing
		// on a head move (cycle 4) and the kind=review awaiting-human unpark both do
		// exactly this. THIS occupancy's review agent has not run yet, so the handoff
		// is not outstanding and the pod must still spawn. Without the occupancy
		// check the first reconcile parks it at handoff-stalled - which has no F.6
		// re-entry - and both cycles die permanently and SILENTLY, because
		// reviewing -> parked is a legal transition.
		task := tsReviewTaskWithOutcome(committed, 0, base)
		reEntered := metav1.NewTime(base.Add(time.Hour))
		task.Status.StageEnteredAt = &reEntered
		task.Status.PodStartedAt = nil
		task.Status.StageWorkStartedAt = nil
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

		if got.Status.Stage != tatarav1alpha1.StageReviewing ||
			got.Status.StageReason == stage.ReasonHandoffStalled {
			t.Fatalf("stage=%q reason=%q, want reviewing: a commit from a PREVIOUS occupancy of this stage is not this occupancy's handoff",
				got.Status.Stage, got.Status.StageReason)
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

		if got.Status.StageReason != stage.ReasonHandoffStalled {
			t.Fatalf("reason = %q, want handoff-stalled: the 5m handoff deadline must fire first",
				got.Status.StageReason)
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
		task := tsReviewTaskWithOutcome(committed, 0, base)
		mr := mdMR(task, "tatara-operator", 364)
		mr.Status.Status = "approved" // the drain settled it
		proj := tsProject(3)
		r := tsReconciler(newMirrorClient(t, proj, mdSecret(), task, mr))

		got := tsReconcile(t, r, proj, task, base.Add(tatarav1alpha1.HandoffDeadline-time.Second))

		if got.Status.Stage != tatarav1alpha1.StageParked ||
			got.Status.StageReason != stage.ReasonAwaitingHuman {
			t.Fatalf("stage=%q reason=%q, want parked/awaiting-human: the dropped advance must be re-driven, not waited out",
				got.Status.Stage, got.Status.StageReason)
		}
	})

	// The incident case: past the deadline, a drained MR must reach its CORRECT
	// terminal (awaiting-human), NOT the spurious handoff-stalled park.
	t.Run("a drained MR reaches awaiting-human past the deadline, not handoff-stalled", func(t *testing.T) {
		task := tsReviewTaskWithOutcome(committed, 0, base)
		mr := mdMR(task, "tatara-operator", 366)
		mr.Status.Status = "approved"
		proj := tsProject(3)
		r := tsReconciler(newMirrorClient(t, proj, mdSecret(), task, mr))

		got := tsReconcile(t, r, proj, task, base.Add(tatarav1alpha1.HandoffDeadline+time.Second))

		if got.Status.StageReason == stage.ReasonHandoffStalled {
			t.Fatalf("reason = handoff-stalled: a review that already landed must advance, not park stalled")
		}
		if got.Status.Stage != tatarav1alpha1.StageParked ||
			got.Status.StageReason != stage.ReasonAwaitingHuman {
			t.Fatalf("stage=%q reason=%q, want parked/awaiting-human",
				got.Status.Stage, got.Status.StageReason)
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

		if got.Status.Stage != tatarav1alpha1.StageReviewing {
			t.Fatalf("stage=%q reason=%q, want reviewing: an undrained pendingReview must not advance the Task",
				got.Status.Stage, got.Status.StageReason)
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

		if got.Status.Stage != tatarav1alpha1.StageMerging {
			t.Fatalf("stage=%q reason=%q, want merging: an approved drained MR must advance an implement Task to merging",
				got.Status.Stage, got.Status.StageReason)
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
		maxPodRecreations, now.Add(-time.Minute))
	task.Spec.Kind = stage.AgentImplement
	proj := tsProject(3)
	r := tsReconciler(newMirrorClient(t, proj, mdSecret(), task)) // no Pod object -> podGone

	got := tsReconcile(t, r, proj, task, now)

	if got.Status.Stage != tatarav1alpha1.StageParked || got.Status.StageReason != stage.ReasonNoOutcome {
		t.Fatalf("stage=%q reason=%q, want parked/no-outcome: only an outcome committed BY THIS STAGE'S agent may suppress the caps",
			got.Status.Stage, got.Status.StageReason)
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
		maxPodRecreations, base)
	reEntered := metav1.NewTime(base.Add(time.Hour))
	ran := metav1.NewTime(base.Add(time.Hour + time.Minute))
	task.Status.StageEnteredAt = &reEntered
	task.Status.PodStartedAt = &ran
	task.Status.StageWorkStartedAt = &ran
	proj := tsProject(3)
	r := tsReconciler(newMirrorClient(t, proj, mdSecret(), task)) // no Pod object -> podGone

	got := tsReconcile(t, r, proj, task, base.Add(time.Hour+2*time.Minute))

	if got.Status.Stage != tatarav1alpha1.StageParked || got.Status.StageReason != stage.ReasonNoOutcome {
		t.Fatalf("stage=%q reason=%q, want parked/no-outcome: a commit predating stageEnteredAt must not suppress this occupancy's caps",
			got.Status.Stage, got.Status.StageReason)
	}
}

// The B2 suppression is scoped to the two POD-LIVENESS caps by REASON, and that
// reason clause is the only thing holding the line. The TURN BUDGET is not a
// pod-liveness cap - it reads stats.turns, which a committed outcome says nothing
// about - and BudgetExit checks it FIRST, so a Task over maxTurnsPerTask must
// still fail here even with this occupancy's own review outcome committed and the
// handoff genuinely in flight.
//
// Drop the reason clause from reconcileCaps' guard (leaving a bare
// `handoffCondition(task) != nil`) and a runaway agent that committed an outcome
// buys itself an unbounded turn budget for the whole handoff window. Every other
// caps test uses a Task with no committed outcome, so none of them enters the
// guard at all and none of them would notice.
func TestReconcile_CapsSuppressionDoesNotCoverTheTurnBudget(t *testing.T) {
	now := time.Unix(1000, 0)
	// THIS occupancy's own review outcome, committed at stageEnteredAt: the
	// handoff condition is armed, so the guard IS entered.
	task := tsReviewTaskWithOutcome(tatarav1alpha1.OutcomeReasonFor(stage.AgentReview), 0, now)
	task.Status.Stats.Turns = defaultMaxTurnsPerTask
	proj := tsProject(3)
	// A LIVE, Ready pod: podGone is false, so no-outcome cannot fire and the turn
	// budget is the only exit BudgetExit can be reporting.
	r := tsReconciler(newMirrorClient(t, proj, mdSecret(), task, tsReadyPod(task)))

	got := tsReconcile(t, r, proj, task, now.Add(time.Minute))

	if got.Status.Stage != tatarav1alpha1.StageFailed ||
		got.Status.StageReason != stage.ReasonTurnBudgetExhausted {
		t.Fatalf("stage=%q reason=%q, want failed/turn-budget-exhausted: the B2 guard suppresses the POD-LIVENESS caps only, "+
			"never the lifetime turn budget", got.Status.Stage, got.Status.StageReason)
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
		task.Status.StageEnteredAt = nil

		if got := handoffCondition(task); got != nil {
			t.Fatalf("handoffCondition = %+v, want nil: with no stage stamp there is no occupancy to attribute the commit to, "+
				"and no handoff deadline to bound the suppression", got)
		}
	})

	t.Run("so the caps apply normally", func(t *testing.T) {
		task := tsReviewTaskWithOutcome(tatarav1alpha1.OutcomeReasonFor(stage.AgentReview),
			maxPodRecreations, base)
		task.Status.StageEnteredAt = nil
		proj := tsProject(3)
		r := tsReconciler(newMirrorClient(t, proj, mdSecret(), task)) // no Pod object -> podGone

		got := tsReconcile(t, r, proj, task, base.Add(time.Minute))

		if got.Status.Stage != tatarav1alpha1.StageParked || got.Status.StageReason != stage.ReasonNoOutcome {
			t.Fatalf("stage=%q reason=%q, want parked/no-outcome: an unbounded suppression must never be granted",
				got.Status.Stage, got.Status.StageReason)
		}
	})
}
