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

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// Compile-time check: countingSession satisfies agent.Session.
var _ agent.Session = (*countingSession)(nil)

// countingSession is an agent.Session stub, mirroring fakeSession
// (fakesession_test.go) but reduced to what this file needs: a bare submit
// counter. SubmitTurn returns a fresh "turn-N" id and never errors; nothing
// here exercises ErrBusy (task_controller_test.go covers that path).
type countingSession struct {
	submitted int
}

func (s *countingSession) SubmitTurn(_ context.Context, _, _, _ string) (string, error) {
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
// snapshot: Get would keep returning the pre-mutation copy forever. Update is a
// deliberate no-op against the underlying store for the same reason: nothing in
// reconcilePodStage's flow re-Lists Task, so there is nothing for it to keep in
// sync, and patchTaskAnnotations/patchTaskStatus's own `*task = *fresh` already
// writes back through this exact pointer (task, live and the parameter named
// "task" three call frames down are all literally the same struct), which is
// what keeps a REAL reconcile's caller-held object current too.
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
	if _, ok := obj.(*v1alpha1.Task); ok {
		return nil
	}
	return c.Client.Update(ctx, obj, opts...)
}

func (c *liveTaskClient) Status() client.SubResourceWriter {
	return &liveTaskStatusWriter{SubResourceWriter: c.Client.Status()}
}

type liveTaskStatusWriter struct {
	client.SubResourceWriter
}

func (w *liveTaskStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if _, ok := obj.(*v1alpha1.Task); ok {
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
