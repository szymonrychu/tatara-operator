package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// ISSUE #566: THE FAILURE MODE IS "LOGS FOREVER", NOT "ERRORS ONCE".
//
// #551 moved the stalled-turn teardown out of PollOnce and into
// TaskReconciler.stalledTurnStop. That was right - the old code hard-deleted the
// pod with no handoff - but the deferral assumed a live pod on the receiving
// side. A PARKED Task has no pod (ParkTask is what takes it down) and
// reconcileStage returns early for one before any pod stage runs, so the
// deferral target never fires, nothing clears the annotations, and PollOnce
// re-detects the same permanently-stale deadline every 30 seconds.
//
// Measured on v2.1.5: 1109 turn_timeout lines in 30 minutes, 19 Tasks, each
// exactly 60 times in the window, forever - 100% of the operator's INFO volume,
// with zero agent pods running.
//
// A test that only checked "the annotation is cleared" or "some function returns
// false" would not have caught it and would not catch a regression. These assert
// the ABSENCE of the repeated line across multiple poll cycles, which is the
// property that was actually violated.

// countAction returns how many recorded log entries carry action=want FOR ONE
// task. The task filter is not optional decoration: PollOnce sweeps the whole
// namespace and the envtest control plane is shared by every test in the
// package, so an unfiltered count would answer for other tests' fixtures too.
func countAction(entries []kvLogEntry, want, task string) int {
	n := 0
	for _, e := range entries {
		if e.field("action") == want && e.field("task") == task {
			n++
		}
	}
	return n
}

// setTaskPodStartedAt stamps status.podStartedAt, the "this Task has had a pod"
// clock. The parked Tasks in #566 all carried one (podStartedAt=2026-08-08T12:59:01Z
// against a turn that ended at 13:10), which is exactly why podStartedAt alone
// cannot be the guard.
func setTaskPodStartedAt(t *testing.T, name string, at time.Time) {
	t.Helper()
	tk := getTask(t, name)
	stamp := metav1.NewTime(at)
	tk.Status.PodStartedAt = &stamp
	if err := k8sClient.Status().Update(context.Background(), tk); err != nil {
		t.Fatalf("set podStartedAt on %s: %v", name, err)
	}
}

// assertNoTurnAnnotations fails when any of the four turn annotations survives.
func assertNoTurnAnnotations(t *testing.T, task *tatarav1alpha1.Task, why string) {
	t.Helper()
	for _, ann := range []string{annCurrentTurn, annTurnStartedAt, annTurnLastActivity, annTurnComplete} {
		if got := task.Annotations[ann]; got != "" {
			t.Errorf("annotation %s = %q, want cleared: %s", ann, got, why)
		}
	}
}

// THE HEADLINE TEST. A parked Task carrying the turn annotations of a turn that
// ended long ago must produce NO turn_timeout line - not one per cycle, not one
// at all - because there is no pod to stop and no reconciler that will act.
//
// Three cycles, because one cycle cannot distinguish "logged once" from "logs
// forever"; the defect is the repetition.
func TestPollOnce_ParkedTaskWithStaleTurn_NeverLogsTurnTimeout(t *testing.T) {
	mkTaskProject(t, "p-orph", 3)
	mkTaskRepository(t, "r-orph", "p-orph")
	mkTask(t, "t-orph", "p-orph", "r-orph")
	setTaskStage(t, "t-orph", tatarav1alpha1.StateAwaitingReview)
	// The production shape exactly: a pod DID run (podStartedAt is set and stays
	// set through a park), the turn annotations are ~20h stale, and the Task is
	// parked awaiting-human with the pod long gone.
	setTaskPodStartedAt(t, "t-orph", time.Now().Add(-20*time.Hour))
	setTaskParkReason(t, "t-orph", stage.ReasonAwaitingHuman)
	stale := time.Now().Add(-20 * time.Hour).UTC().Format(time.RFC3339)
	annotate(t, "t-orph", map[string]string{
		annCurrentTurn:      "turn-dkjkyirye1mw",
		annTurnStartedAt:    stale,
		annTurnLastActivity: stale,
	})

	reg := prometheus.NewRegistry()
	cb := &CallbackServer{
		Client:    k8sClient,
		Metrics:   obs.NewOperatorMetrics(reg),
		Namespace: testNS,
		Session:   newFakeSession(),
	}

	ctx, entries := kvLoggingCtx()
	for i := 0; i < 3; i++ {
		cb.PollOnce(ctx)
	}

	if n := countAction(*entries, "turn_timeout", "t-orph"); n != 0 {
		t.Errorf("turn_timeout logged %d times over 3 poll cycles, want 0: "+
			"a parked Task has no pod to stop and no reconciler that will act on it (#566)", n)
	}
	if got := testutil.ToFloat64(cb.Metrics.TurnTimeoutCounter("poll_backstop")); got != 0 {
		t.Errorf("operator_turn_timeout_total{source=poll_backstop} = %v, want 0", got)
	}

	// The repair is announced exactly ONCE, not once per cycle: the second and
	// third passes must find nothing left to do and stay silent.
	if n := countAction(*entries, "orphaned_turn_cleared", "t-orph"); n != 1 {
		t.Errorf("orphaned_turn_cleared logged %d times over 3 poll cycles, want exactly 1: "+
			"the repair must converge, not become the new every-30s line", n)
	}
	if got := testutil.ToFloat64(cb.Metrics.OrphanedTurnClearedCounter("p-orph")); got != 1 {
		t.Errorf("operator_orphaned_turn_annotations_cleared_total{project=p-orph} = %v, want 1", got)
	}

	// The underlying wrongness is gone, not just the log. Stale turn annotations
	// keep stage.TurnInFlight true forever, which disarms the idle clock, makes
	// the Task permanently un-evictable, exempts its pod from the idle reaper and
	// gags every conversation follow-up turn.
	got := getTask(t, "t-orph")
	assertNoTurnAnnotations(t, got, "the pod that ran this turn is gone")
	if stage.TurnInFlight(got) {
		t.Error("stage.TurnInFlight is still true for a parked Task with no pod")
	}
	if !tatarav1alpha1.Parked(got) {
		t.Error("the repair must not un-park the Task; it only retires the turn")
	}
}

// The same repair for the OTHER shape the reconciler cannot reach: an un-parked
// Task whose pod clocks were nil'd (a TTL rotation, a lost pod, a handoff-less
// re-arm) and which is now waiting on admission. reconcilePodStage's
// turnTimedOut branch sits behind `task.Status.PodStartedAt != nil`, so nothing
// evaluates the stall until a replacement pod exists - and CLOCK 1's budget is
// 24 hours, which is 2880 turn_timeout lines per Task.
func TestPollOnce_NoPodClocksWithStaleTurn_NeverLogsTurnTimeout(t *testing.T) {
	mkTaskProject(t, "p-orph2", 3)
	mkTaskRepository(t, "r-orph2", "p-orph2")
	mkTask(t, "t-orph2", "p-orph2", "r-orph2")
	setTaskStage(t, "t-orph2", tatarav1alpha1.StateUnderImplementation)
	stale := time.Now().Add(-3 * time.Hour).UTC().Format(time.RFC3339)
	annotate(t, "t-orph2", map[string]string{
		annCurrentTurn:      "turn-rearmed",
		annTurnStartedAt:    stale,
		annTurnLastActivity: stale,
	})

	cb := newCallbackServer()
	cb.Session = newFakeSession()
	ctx, entries := kvLoggingCtx()
	for i := 0; i < 3; i++ {
		cb.PollOnce(ctx)
	}

	if n := countAction(*entries, "turn_timeout", "t-orph2"); n != 0 {
		t.Errorf("turn_timeout logged %d times over 3 poll cycles, want 0: "+
			"with podStartedAt nil the reconciler never reaches stalledTurnStop", n)
	}
	assertNoTurnAnnotations(t, getTask(t, "t-orph2"), "there is no pod running this turn")
}

// THE OTHER HALF OF THE GUARD, and it is what keeps #551 intact. A LIVE pod
// whose turn stalled must STILL be detected here and STILL be left for
// TaskReconciler.stalledTurnStop to stop gracefully. If this ever goes quiet the
// fix has over-reached and the graceful handoff has been bypassed again.
func TestPollOnce_LivePodStalledTurn_StillDefersToTheReconciler(t *testing.T) {
	mkTaskProject(t, "p-orph3", 3)
	mkTaskRepository(t, "r-orph3", "p-orph3")
	mkTask(t, "t-orph3", "p-orph3", "r-orph3")
	setTaskStage(t, "t-orph3", tatarav1alpha1.StateUnderImplementation)
	setTaskPodStartedAt(t, "t-orph3", time.Now().Add(-3*time.Hour))
	stale := time.Now().Add(-3 * time.Hour).UTC().Format(time.RFC3339)
	annotate(t, "t-orph3", map[string]string{
		annCurrentTurn:      "turn-live",
		annTurnStartedAt:    stale,
		annTurnLastActivity: stale,
	})

	cb := newCallbackServer()
	cb.Session = newFakeSession()
	ctx, entries := kvLoggingCtx()
	cb.PollOnce(ctx)

	if n := countAction(*entries, "turn_timeout", "t-orph3"); n != 1 {
		t.Errorf("turn_timeout logged %d times, want 1: a stalled turn on a LIVE pod is still the reconciler's to stop", n)
	}
	if n := countAction(*entries, "orphaned_turn_cleared", "t-orph3"); n != 0 {
		t.Errorf("orphaned_turn_cleared logged %d times, want 0: this turn's pod still exists", n)
	}
	got := getTask(t, "t-orph3")
	if got.Annotations[annCurrentTurn] != "turn-live" {
		t.Errorf("annCurrentTurn = %q, want turn-live: the backstop must not tear down a turn the reconciler will stop gracefully",
			got.Annotations[annCurrentTurn])
	}
}

// --- the eager half: every path that ends the pod retires the turn ---

// ParkTask is THE park choke point, so this one assertion covers every park
// reason in the operator: awaiting-human, the live-pod eviction, pod-recreation
// exhausted, admission-starved, the podwatch failure paths. Park is where the
// pod is torn down, and the turn annotations are pod-scoped.
func TestParkTask_ClearsTheTurnAnnotations(t *testing.T) {
	task := tsTask("park-turn-1", "implement", tatarav1alpha1.StateAwaitingReview, time.Now().Add(-time.Hour))
	stale := time.Now().Add(-20 * time.Hour).UTC().Format(time.RFC3339)
	task.Annotations = map[string]string{
		annCurrentTurn:      "turn-parked",
		annTurnStartedAt:    stale,
		annTurnLastActivity: stale,
	}
	c := newMirrorClient(t, task)

	if err := ParkTask(context.Background(), c, nil, obs.NewOperatorMetrics(prometheus.NewRegistry()),
		task, stage.ReasonAwaitingHuman, time.Now(), nil); err != nil {
		t.Fatalf("ParkTask: %v", err)
	}

	got := mdGetTask(t, c, task.Name)
	if !tatarav1alpha1.Parked(got) {
		t.Fatalf("task is not parked: parkReason=%q", got.Status.ParkReason)
	}
	assertNoTurnAnnotations(t, got, "park takes the pod down, so the turn is over by definition")
}

// A TTL rotation stops the pod mid-turn by design. Before #566 it left the turn
// annotations behind, so the replacement pod inherited a Task that still claimed
// a turn was in flight - which gags its conversation follow-up turn and disarms
// its idle clock.
func TestTTLStop_ClearsTheTurnAnnotations(t *testing.T) {
	proj, task, r, _ := newConversingExitFixture(t)
	// A LIVE state floors the pod TTL at the conversation idle window
	// (agent.PodTTLSeconds), so t0 is pushed past by ageing the pod rather than by
	// shrinking the TTL, and conversationLastEventAt is dropped so podStartedAt is
	// the anchor. turnTimeoutSeconds is cut to bound the stop sequence's real
	// timers - this test is about the annotations, not about waiting them out.
	proj.Spec.Agent.TurnTimeoutSeconds = 1
	proj.Spec.AgentPodTTLSeconds = 60
	task.Annotations = map[string]string{
		annCurrentTurn:      "turn-ttl",
		annTurnStartedAt:    time.Now().Add(-5 * time.Minute).UTC().Format(time.RFC3339),
		annTurnLastActivity: time.Now().Add(-time.Minute).UTC().Format(time.RFC3339),
	}
	if err := r.Update(context.Background(), task); err != nil {
		t.Fatalf("seed annotations: %v", err)
	}
	// AFTER the Update, never before: an Update against a client with a status
	// subresource writes the STORED status back into the passed object, so status
	// set beforehand is silently reverted to the fixture's.
	podAt := metav1.NewTime(time.Now().Add(-3 * time.Hour))
	task.Status.PodStartedAt = &podAt
	task.Status.ConversationLastEventAt = nil
	task.Status.State = tatarav1alpha1.StateUnderImplementation
	task.Status.AgentKind = stage.AgentKindFor(tatarav1alpha1.StateUnderImplementation, "implement")
	if !agent.TTLExpired(proj, task, time.Now()) {
		t.Fatal("fixture is wrong: the pod TTL must be past for this test to exercise ttlStop")
	}

	if _, err := r.reconcilePodStage(context.Background(), proj, task,
		stage.AgentImplement, time.Now()); err != nil {
		t.Fatalf("reconcilePodStage: %v", err)
	}

	assertNoTurnAnnotations(t, mdGetTask(t, r.Client, task.Name),
		"the TTL stop deleted the pod that was running this turn")
}

// A pod that RAN and VANISHED took its turn with it: nothing will ever complete
// it, and the pod clocks are nil'd here, which is precisely what puts the Task
// out of stalledTurnStop's reach until a replacement exists.
func TestRespawnLostPod_ClearsTheTurnAnnotations(t *testing.T) {
	proj, task, r, _ := newConversingExitFixture(t)
	task.Status.State = tatarav1alpha1.StateUnderImplementation
	task.Status.AgentKind = stage.AgentKindFor(tatarav1alpha1.StateUnderImplementation, "implement")
	task.Annotations = map[string]string{
		annCurrentTurn:   "turn-lost",
		annTurnStartedAt: time.Now().Add(-time.Minute).UTC().Format(time.RFC3339),
	}
	if err := r.Update(context.Background(), task); err != nil {
		t.Fatalf("seed annotations: %v", err)
	}
	// The pod disappears out from under a live turn.
	if err := agent.DeleteWrapper(context.Background(), r.Client, mdNS, task); err != nil {
		t.Fatalf("delete wrapper: %v", err)
	}

	if _, err := r.reconcilePodStage(context.Background(), proj, task,
		stage.AgentImplement, time.Now()); err != nil {
		t.Fatalf("reconcilePodStage: %v", err)
	}

	got := mdGetTask(t, r.Client, task.Name)
	if got.Status.PodStartedAt != nil {
		t.Fatalf("fixture is wrong: podStartedAt=%v, want nil - this test must exercise the respawn re-arm", got.Status.PodStartedAt)
	}
	assertNoTurnAnnotations(t, got, "the pod running this turn vanished")
}

// reArmWithoutHandoff is the NON-PARK exit added by #561/#563: the pod ended
// with the agent having asked nothing, so the Task gets a replacement pod rather
// than a fabricated human gate. ParkTask's clear does not cover it, and
// stage.ReArmAfterPodLoss is a pure status mutation that cannot write metadata,
// so this path needs its own.
func TestReArmWithoutHandoff_ClearsTheTurnAnnotations(t *testing.T) {
	proj, task, r, _ := newConversingExitFixture(t)
	// A plain fakeSession writes NO handoff note, so agentAskedSomething is false
	// and liveHandoffAndPark takes the re-arm exit rather than the park exit. That
	// also means the stopper waits out its handoff-note deadline for real, and
	// liveHandoffAndPark sets no MaxWait - so turnTimeoutSeconds is what bounds it.
	r.Session = newFakeSession()
	proj.Spec.Agent.TurnTimeoutSeconds = 1
	task.Annotations = map[string]string{
		annCurrentTurn:      "turn-rearm",
		annTurnStartedAt:    time.Now().Add(-30 * time.Minute).UTC().Format(time.RFC3339),
		annTurnLastActivity: time.Now().Add(-20 * time.Minute).UTC().Format(time.RFC3339),
	}
	if err := r.Update(context.Background(), task); err != nil {
		t.Fatalf("seed annotations: %v", err)
	}

	if err := r.liveHandoffAndPark(context.Background(), proj, task, nil, causeEvicted, time.Now()); err != nil {
		t.Fatalf("liveHandoffAndPark: %v", err)
	}

	got := mdGetTask(t, r.Client, task.Name)
	if tatarav1alpha1.Parked(got) {
		t.Fatalf("fixture is wrong: the Task parked (%s), so this test did not exercise reArmWithoutHandoff",
			got.Status.ParkReason)
	}
	assertNoTurnAnnotations(t, got, "the pod that ran this turn was stopped for the eviction")
}
