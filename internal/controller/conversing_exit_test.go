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

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// A conversation that goes quiet must not simply have its pod deleted: the notes
// journal IS the continuation state, and a park that leaves it empty makes the
// next pod start from nothing, redo the work and burn maxTurns. So the exit is a
// HANDOFF TURN first, park second.
func TestConversingIdleExitTakesAHandoffTurnBeforeParking(t *testing.T) {
	proj, task, r, sess := newConversingExitFixture(t)
	proj.Spec.Scm.ConversationIdleMinutes = 5
	task.Status.ConversationLastEventAt = &metav1.Time{Time: time.Now().Add(-6 * time.Minute)}

	res, handled, err := r.reconcileClocks(context.Background(), proj, task, time.Now())
	if err != nil {
		t.Fatalf("reconcileClocks: %v", err)
	}
	if !handled {
		t.Fatalf("handled = false (res %v), want true: the idle conversation did not age out", res)
	}
	if sess.handoffTurns != 1 {
		t.Errorf("SubmitHandoffTurn called %d times, want 1", sess.handoffTurns)
	}

	fresh := &tatarav1alpha1.Task{}
	if err := r.Get(context.Background(), objectKeyOf(task), fresh); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if fresh.Status.Stage != tatarav1alpha1.StageParked || fresh.Status.StageReason != stage.ReasonAwaitingHuman {
		t.Fatalf("stage = %s(%s), want parked(awaiting-human)", fresh.Status.Stage, fresh.Status.StageReason)
	}
	if len(fresh.Status.Notes) == 0 {
		t.Error("notes are empty after the handoff: the continuation state was lost")
	}
	if wrapperPodExists(t, r, agent.PodName(task)) {
		t.Error("the wrapper pod survived the park: parked means no pod")
	}
}

// A conversation still inside its idle window is not touched.
func TestConversingWithinTheIdleWindowIsUntouched(t *testing.T) {
	proj, task, r, sess := newConversingExitFixture(t)
	proj.Spec.Scm.ConversationIdleMinutes = 60
	task.Status.ConversationLastEventAt = &metav1.Time{Time: time.Now().Add(-5 * time.Minute)}

	_, handled, err := r.reconcileClocks(context.Background(), proj, task, time.Now())
	if err != nil {
		t.Fatalf("reconcileClocks: %v", err)
	}
	if handled {
		t.Fatal("handled = true: a live conversation was parked five minutes into a sixty-minute window")
	}
	if sess.handoffTurns != 0 {
		t.Errorf("SubmitHandoffTurn called %d times, want 0", sess.handoffTurns)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// exitSession is an agent.Session stub for the idle-exit path: it embeds
// fakeSession for the ordinary bookkeeping (GetSession reports Ready/idle by
// default, so the TTL stopper's waitIdle never blocks) and overrides
// SubmitHandoffTurn to do what a real agent's task_note(kind=handoff) call
// would do - write a handoff Note straight onto the Task through the SAME
// client the reconciler uses - so TTLStopper.waitHandoffNote's very first poll
// (before any real sleep) observes it and returns immediately. Without this the
// stopper would sleep out its real wall-clock TTLPollInterval waiting for a note
// that never arrives, and a unit test would either hang or spuriously degrade to
// the synthetic-note path.
type exitSession struct {
	*fakeSession
	client       client.Client
	namespace    string
	taskName     string
	handoffTurns int
}

func (s *exitSession) SubmitHandoffTurn(ctx context.Context, baseURL, text, callbackURL string) (string, error) {
	id, err := s.fakeSession.SubmitHandoffTurn(ctx, baseURL, text, callbackURL)
	if err != nil {
		return "", err
	}
	s.handoffTurns++
	fresh := &tatarav1alpha1.Task{}
	if err := s.client.Get(ctx, types.NamespacedName{Namespace: s.namespace, Name: s.taskName}, fresh); err != nil {
		return "", err
	}
	fresh.Status.Notes = append(fresh.Status.Notes, tatarav1alpha1.Note{
		At:    metav1.Now(),
		Agent: "clarify",
		Kind:  agent.NoteKindHandoff,
		Body:  "everything the next pod needs",
	})
	if err := s.client.Status().Update(ctx, fresh); err != nil {
		return "", err
	}
	return id, nil
}

// newConversingExitFixture builds a conversing Task with a ready wrapper pod and
// an idle-but-alive session, mirroring newConversingTurnFixture
// (conversing_turn_test.go) but wired for the G.7 handoff-and-park exit rather
// than the follow-up turn.
func newConversingExitFixture(t *testing.T) (*tatarav1alpha1.Project, *tatarav1alpha1.Task, *TaskReconciler, *exitSession) {
	t.Helper()

	now := time.Now()
	proj := tsStablyReadyProject(3)
	proj.Spec.Agent.TurnTimeoutSeconds = 600

	task := tsTask("exit-1", "clarify", tatarav1alpha1.StageConversing, now.Add(-time.Hour))
	podAt := metav1.NewTime(now.Add(-10 * time.Minute))
	workAt := metav1.NewTime(now.Add(-9 * time.Minute))
	lastEvent := metav1.NewTime(now.Add(-time.Minute))
	task.Status.PodStartedAt = &podAt
	task.Status.StageWorkStartedAt = &workAt
	task.Status.ConversationLastEventAt = &lastEvent
	task.Status.AgentKind = stage.AgentClarify

	c := newMirrorClient(t, proj, mdSecret(), task, tsReadyPod(task))
	r := tsReconciler(c)
	sess := &exitSession{fakeSession: newFakeSession(), client: c, namespace: task.Namespace, taskName: task.Name}
	r.Session = sess

	return proj, task, r, sess
}

// wrapperPodExists reports whether the wrapper Pod named `name` still exists in
// r's client. Named distinctly from reaper_test.go's podExists(t, name), which
// reads the package-wide envtest k8sClient rather than a fixture's own fake
// client - the two are not interchangeable and cannot share a name.
func wrapperPodExists(t *testing.T, r *TaskReconciler, name string) bool {
	t.Helper()
	err := r.Get(context.Background(), types.NamespacedName{Namespace: mdNS, Name: name}, &corev1.Pod{})
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("get pod %s: %v", name, err)
	}
	return err == nil
}
