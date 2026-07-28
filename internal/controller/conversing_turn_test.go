package controller

import (
	"context"
	"strconv"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/prometheus/client_golang/prometheus"

	v1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// FINDING 1: SubmitTurn had exactly ONE production caller, gated by the
// annStageTurn0 marker, so a LIVE pod never received a second turn. Without this
// branch a conversing pod is warm and deaf: comments queue as pendingEvents and
// nothing delivers them until the pod is replaced.
func TestConversingPodTakesAFollowUpTurnOnANewEvent(t *testing.T) {
	proj, task, r, sess := newConversingTurnFixture(t)

	// turn-0 has already been submitted for THIS pod and has COMPLETED.
	task.Annotations = map[string]string{
		annStageTurn0:   turn0Marker(task),
		annCurrentTurn:  "turn-0",
		annTurnComplete: time.Now().UTC().Format(time.RFC3339),
	}
	task.Status.PendingEvents = []v1alpha1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Repo: "helmfile", Number: 26,
		Author: "szymonrychu", Body: "one more thing",
	}}

	if _, err := r.reconcilePodStage(context.Background(), proj, task, "clarify", time.Now()); err != nil {
		t.Fatalf("reconcilePodStage: %v", err)
	}

	if sess.submitted != 1 {
		t.Fatalf("SubmitTurn called %d times, want 1: the live conversing pod never got the new comment", sess.submitted)
	}

	fresh := &v1alpha1.Task{}
	if err := r.Get(context.Background(), objectKeyOf(task), fresh); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if len(fresh.Status.PendingEvents) != 0 {
		t.Errorf("PendingEvents = %d, want 0: the delta was rendered into the turn and must be spent", len(fresh.Status.PendingEvents))
	}
}

// A turn already in flight must never be raced. POST /v1/messages 409s while a
// turn is in flight, and a second submit is exactly the shape of the 2026-06
// webhook-redelivery defect that double-injected text into a live session.
func TestConversingPodDoesNotSubmitWhileATurnIsInFlight(t *testing.T) {
	proj, task, r, sess := newConversingTurnFixture(t)
	task.Annotations = map[string]string{
		annStageTurn0:  turn0Marker(task),
		annCurrentTurn: "turn-0",
		// annTurnComplete deliberately ABSENT: the turn is in flight.
	}
	task.Status.PendingEvents = []v1alpha1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Author: "szymonrychu", Body: "hurry up",
	}}

	if _, err := r.reconcilePodStage(context.Background(), proj, task, "clarify", time.Now()); err != nil {
		t.Fatalf("reconcilePodStage: %v", err)
	}
	if sess.submitted != 0 {
		t.Fatalf("SubmitTurn called %d times, want 0: a follow-up turn raced a live turn", sess.submitted)
	}

	fresh := &v1alpha1.Task{}
	if err := r.Get(context.Background(), objectKeyOf(task), fresh); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if len(fresh.Status.PendingEvents) != 1 {
		t.Errorf("PendingEvents = %d, want 1: an undelivered event was dropped", len(fresh.Status.PendingEvents))
	}
}

// The follow-up turn is scoped to conversing ONLY. Every other pod stage keeps
// exactly one turn per pod: widening it would change the turn model for
// implementing and reviewing, which is not what this change is for.
func TestNonConversingPodTakesNoFollowUpTurn(t *testing.T) {
	proj, task, r, sess := newConversingTurnFixture(t)
	task.Status.Stage = v1alpha1.StageClarifying
	task.Annotations = map[string]string{
		annStageTurn0:   turn0Marker(task),
		annCurrentTurn:  "turn-0",
		annTurnComplete: time.Now().UTC().Format(time.RFC3339),
	}
	task.Status.PendingEvents = []v1alpha1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Author: "szymonrychu", Body: "ping",
	}}

	if _, err := r.reconcilePodStage(context.Background(), proj, task, "clarify", time.Now()); err != nil {
		t.Fatalf("reconcilePodStage: %v", err)
	}
	if sess.submitted != 0 {
		t.Fatalf("SubmitTurn called %d times on a clarifying pod, want 0", sess.submitted)
	}
}

// FINDING 5: the elision counter already exists and is already labelled by agent
// kind. A long conversation degrades by dropping history, and this asserts the
// degradation is observable rather than silent.
func TestConversingRenderCountsBundleElision(t *testing.T) {
	proj, task, r, _ := newConversingTurnFixture(t)
	proj.Spec.MaxBundleBytes = 50000
	task.Annotations = map[string]string{
		annStageTurn0:   turn0Marker(task),
		annCurrentTurn:  "turn-0",
		annTurnComplete: time.Now().UTC().Format(time.RFC3339),
	}
	// Enough notes to blow the budget so the byte guard must elide.
	for i := 0; i < 50; i++ {
		task.Status.Notes = append(task.Status.Notes, v1alpha1.Note{
			At: metav1.Now(), Agent: "clarify", Kind: "note", Body: string(make([]byte, 4000)),
		})
	}
	task.Status.PendingEvents = []v1alpha1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Author: "szymonrychu", Body: "and another thing",
	}}

	if _, err := r.reconcilePodStage(context.Background(), proj, task, "clarify", time.Now()); err != nil {
		t.Fatalf("reconcilePodStage: %v", err)
	}
	if got := bundleElidedFor(t, r, "clarify"); got == 0 {
		t.Fatal("operator_bundle_elided_total{agent_kind=clarify} = 0: a long conversation is dropping history silently")
	}
}

// REVIEW FINDING: the webhook handler (internal/webhook/server.go,
// HandlerRunnable.NeedLeaderElection() == false) runs on every replica,
// independent of leader election and of this reconcile loop, and calls
// AppendTaskEvent directly. reconcile-vs-reconcile cannot race - the
// controller-runtime workqueue serialises reconciles per object key, and
// leader election means only one replica reconciles at all - but SubmitTurn
// is a real network round trip to the wrapper pod, and a comment can land
// in status.pendingEvents via the webhook while that round trip is still
// outstanding. A drain that unconditionally nils pendingEvents at that point
// erases the late comment: it was never rendered into any turn, and nothing
// ever resends it - the exact failure this feature exists to fix, on its own
// hot path.
//
// This reproduces the window directly: the fake Session's SubmitTurn calls
// AppendTaskEvent mid-call (via sess.onSubmit), simulating the webhook
// write landing between render and drain. The late event must survive THIS
// reconcile's drain, and must be delivered as a genuine follow-up turn on
// the next one.
func TestConversingFollowUpDrainSurvivesAConcurrentAppend(t *testing.T) {
	proj, task, r, sess := newConversingTurnFixture(t)
	task.Annotations = map[string]string{
		annStageTurn0:   turn0Marker(task),
		annCurrentTurn:  "turn-0",
		annTurnComplete: time.Now().UTC().Format(time.RFC3339),
	}
	task.Status.PendingEvents = []v1alpha1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Author: "szymonrychu", Body: "one more thing",
	}}

	late := v1alpha1.TaskEvent{
		At: metav1.Now(), Kind: "issue_comment", Author: "szymonrychu",
		Body: "LATE ARRIVAL: landed while the turn was in flight",
	}
	sess.onSubmit = func() {
		// The webhook handler's own path: Get, append, Status().Update - a
		// SEPARATE object from the reconciler's "task", exactly like a real
		// concurrent replica would use.
		live := &v1alpha1.Task{}
		if err := r.Get(context.Background(), objectKeyOf(task), live); err != nil {
			t.Fatalf("get task mid-submit: %v", err)
		}
		if err := AppendTaskEvent(context.Background(), r.Client, live, late); err != nil {
			t.Fatalf("append task event mid-submit: %v", err)
		}
	}

	if _, err := r.reconcilePodStage(context.Background(), proj, task, "clarify", time.Now()); err != nil {
		t.Fatalf("reconcilePodStage: %v", err)
	}
	if sess.submitted != 1 {
		t.Fatalf("SubmitTurn called %d times, want 1", sess.submitted)
	}

	fresh := &v1alpha1.Task{}
	if err := r.Get(context.Background(), objectKeyOf(task), fresh); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if len(fresh.Status.PendingEvents) != 1 || fresh.Status.PendingEvents[0].Body != late.Body {
		t.Fatalf("PendingEvents = %+v, want exactly the late arrival to have survived the drain", fresh.Status.PendingEvents)
	}

	// The next reconcile, once the wrapper's callback marks the turn complete,
	// must deliver the surviving event as a genuine follow-up turn - "survives
	// the drain" is worthless if it is never actually delivered. onSubmit is a
	// ONE-SHOT simulation of the concurrent webhook write; left armed it would
	// fire again on this second SubmitTurn and inject a second late arrival,
	// muddying what this assertion is actually proving.
	sess.onSubmit = nil
	task.Annotations[annTurnComplete] = time.Now().UTC().Format(time.RFC3339)
	if _, err := r.reconcilePodStage(context.Background(), proj, task, "clarify", time.Now()); err != nil {
		t.Fatalf("reconcilePodStage (follow-up): %v", err)
	}
	if sess.submitted != 2 {
		t.Fatalf("SubmitTurn called %d times after the follow-up reconcile, want 2: the late arrival was never delivered", sess.submitted)
	}
	final := &v1alpha1.Task{}
	if err := r.Get(context.Background(), objectKeyOf(task), final); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if len(final.Status.PendingEvents) != 0 {
		t.Errorf("PendingEvents = %d, want 0 after the late arrival's own turn", len(final.Status.PendingEvents))
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// Compile-time check: countingSession satisfies agent.Session.
var _ agent.Session = (*countingSession)(nil)

// countingSession is an agent.Session stub, mirroring fakeSession
// (fakesession_test.go) but reduced to what this file needs: a bare submit
// counter. SubmitTurn returns a fresh "turn-N" id and never errors; nothing
// here exercises ErrBusy (task_controller_test.go covers that path).
//
// onSubmit, when set, fires INSIDE SubmitTurn, before it returns: this is
// how TestConversingFollowUpDrainSurvivesAConcurrentAppend simulates the
// webhook-vs-drain race - SubmitTurn is the real network round trip to the
// wrapper pod, and the webhook handler that calls AppendTaskEvent runs on
// every replica independent of leader election, so a comment can land in
// status.pendingEvents at exactly this point: after render, before the
// drain that follows SubmitTurn's return.
type countingSession struct {
	submitted int
	onSubmit  func()
}

func (s *countingSession) SubmitTurn(_ context.Context, _, _, _ string) (string, error) {
	if s.onSubmit != nil {
		s.onSubmit()
	}
	s.submitted++
	return "turn-" + strconv.Itoa(s.submitted), nil
}

func (s *countingSession) SubmitHandoffTurn(ctx context.Context, baseURL, text, callbackURL string) (string, error) {
	return s.SubmitTurn(ctx, baseURL, text, callbackURL)
}

func (s *countingSession) GetTurn(_ context.Context, _ string, _ string) (agent.TurnResult, error) {
	return agent.TurnResult{}, nil
}

func (s *countingSession) GetSession(_ context.Context, _ string) (agent.SessionInfo, error) {
	v := agent.ContractVersion
	return agent.SessionInfo{State: agent.SessionStateReady, ContractVersion: &v}, nil
}

func (s *countingSession) DeleteSession(_ context.Context, _ string) error {
	return nil
}

// conversingTurnRegistries tracks the *prometheus.Registry each fixture's
// reconciler was built with, keyed by reconciler identity, so bundleElidedFor
// can gather it back out without obs.BundleMetrics needing to expose its
// unexported CounterVec to tests. Package-scoped test state, not production
// wiring: nothing outside this file reads it.
var conversingTurnRegistries = map[*TaskReconciler]*prometheus.Registry{}

// liveTaskClient makes Get/Update of THIS FIXTURE's Task always read from and
// write through the SAME *v1alpha1.Task pointer the test mutates directly
// (task.Annotations = ..., task.Status.PendingEvents = ...) after the fixture
// returns, instead of the vanilla fake client's Build-time snapshot.
//
// The vanilla fake.NewClientBuilder deep-copies every WithObjects() argument
// once, at Build. A test that mutates its `task` pointer afterward - the
// pattern every test in this file uses, and the pattern that matches a real
// reconcile (handed an already-fetched, live object) - is invisible to that
// snapshot: Get would keep returning the pre-mutation copy forever.
//
// Update/Status().Update write the incoming object's content INTO task via
// DeepCopyInto, rather than relying on a caller's own copy-back (an earlier
// version of this wrapper relied on patchTaskAnnotations/patchTaskStatus's own
// `*task = *fresh` for that, which only holds when the caller's "task"
// parameter happens to alias THIS exact pointer - true for reconcilePodStage's
// own call chain, but NOT for a second, independent caller reading/writing
// through a freshly Get'd copy of its own, which is exactly what
// AppendTaskEvent does in TestConversingFollowUpDrainSurvivesAConcurrentAppend
// to simulate the webhook handler's concurrent write. Writing unconditionally
// here makes both callers correct without depending on which one happens to
// hold the aliased pointer.
type liveTaskClient struct {
	client.Client
	task *v1alpha1.Task
}

func (c *liveTaskClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if t, ok := obj.(*v1alpha1.Task); ok {
		c.task.DeepCopyInto(t)
		return nil
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c *liveTaskClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if t, ok := obj.(*v1alpha1.Task); ok {
		t.DeepCopyInto(c.task)
		return nil
	}
	return c.Client.Update(ctx, obj, opts...)
}

func (c *liveTaskClient) Status() client.SubResourceWriter {
	return &liveTaskStatusWriter{SubResourceWriter: c.Client.Status(), task: c.task}
}

type liveTaskStatusWriter struct {
	client.SubResourceWriter
	task *v1alpha1.Task
}

func (w *liveTaskStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if t, ok := obj.(*v1alpha1.Task); ok {
		t.DeepCopyInto(w.task)
		return nil
	}
	return w.SubResourceWriter.Update(ctx, obj, opts...)
}

// newConversingTurnFixture builds a conversing Task with a Ready wrapper pod,
// mirroring the pod-stage fixtures in task_stage_test.go (tsProject/tsTask/
// tsReadyPod) rather than inventing a second fixture shape.
func newConversingTurnFixture(t *testing.T) (*v1alpha1.Project, *v1alpha1.Task, *TaskReconciler, *countingSession) {
	t.Helper()

	now := time.Now()
	proj := tsStablyReadyProject(3)
	task := tsTask("conv-1", "clarify", v1alpha1.StageConversing, now.Add(-time.Hour))
	podAt := metav1.NewTime(now.Add(-10 * time.Minute))
	workAt := metav1.NewTime(now.Add(-9 * time.Minute))
	lastEvent := metav1.NewTime(now.Add(-time.Minute))
	task.Status.PodStartedAt = &podAt
	task.Status.StageWorkStartedAt = &workAt
	task.Status.ConversationLastEventAt = &lastEvent
	task.Status.AgentKind = stage.AgentClarify

	c := newMirrorClient(t, proj, mdSecret(), task, tsReadyPod(task))
	r := tsReconciler(&liveTaskClient{Client: c, task: task})
	reg := prometheus.NewRegistry()
	r.BundleMetrics = obs.NewBundleMetrics(reg)
	r.Metrics = obs.NewOperatorMetrics(reg)
	sess := &countingSession{}
	r.Session = sess
	conversingTurnRegistries[r] = reg

	return proj, task, r, sess
}

// bundleElidedFor reads operator_bundle_elided_total{agent_kind=kind} out of
// the registry newConversingTurnFixture built r's BundleMetrics on, the same
// Gather-and-match idiom as turnSubmitCount/terminalCount above.
func bundleElidedFor(t *testing.T, r *TaskReconciler, kind string) float64 {
	t.Helper()
	reg, ok := conversingTurnRegistries[r]
	if !ok {
		t.Fatalf("no registry recorded for this reconciler: build it via newConversingTurnFixture")
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "operator_bundle_elided_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "agent_kind" && lp.GetValue() == kind {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}
