package controller

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/budget"
	"github.com/szymonrychu/tatara-operator/internal/stage"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// conflictOnceQueuedEventClient wraps k8sClient, injecting one Conflict on the
// first QueuedEvent Status().Update call. Reproduces the #348 storm: every
// queue_admit -> queue_done cycle logged one "the object has been modified"
// error from admitPool's raw Status().Update racing the informer cache against
// the pool-wide scan a sibling QueuedEvent's own reconcile had just run.
type conflictOnceQueuedEventClient struct {
	client.Client
	calls *atomic.Int32
}

func (c *conflictOnceQueuedEventClient) Status() client.SubResourceWriter {
	return &conflictOnceWriter{
		SubResourceWriter: c.Client.Status(),
		calls:             c.calls,
		gr:                schema.GroupResource{Group: "tatara.dev", Resource: "queuedevents"},
		name:              "conflict-target",
	}
}

// conflictOnceTaskClient wraps k8sClient, injecting one Conflict on the first
// Task Update (metadata/labels) AND the first Task Status().Update call.
// Reproduces admitTicket's raw r.Update/r.Status().Update, the two other
// unguarded writes fixed alongside the QueuedEvent one.
type conflictOnceTaskClient struct {
	client.Client
	updateCalls *atomic.Int32
	statusCalls *atomic.Int32
}

func (c *conflictOnceTaskClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if _, ok := obj.(*tatarav1alpha1.Task); ok && c.updateCalls.Add(1) == 1 {
		return apierrors.NewConflict(schema.GroupResource{Group: "tatara.dev", Resource: "tasks"}, obj.GetName(), nil)
	}
	return c.Client.Update(ctx, obj, opts...)
}

func (c *conflictOnceTaskClient) Status() client.SubResourceWriter {
	return &conflictOnceWriter{
		SubResourceWriter: c.Client.Status(),
		calls:             c.statusCalls,
		gr:                schema.GroupResource{Group: "tatara.dev", Resource: "tasks"},
		name:              "conflict-target",
	}
}

// TestPatchQueuedEventStatus_RetriesOnConflict is the narrowest repro: the raw
// r.Status().Update admitPool used to call would return this Conflict straight
// to Reconcile's caller (the "Reconciler error" Loki saw 691 times in 48h).
// patchQueuedEventStatus must retry against a freshly Get'd copy and land the
// write.
func TestPatchQueuedEventStatus_RetriesOnConflict(t *testing.T) {
	ctx := context.Background()
	q := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "qe-", Namespace: testNS},
		Spec:       tatarav1alpha1.QueuedEventSpec{Class: tatarav1alpha1.QueueClassNormal, ProjectRef: "p"},
	}
	mustCreate(t, ctx, q)

	var calls atomic.Int32
	cc := &conflictOnceQueuedEventClient{Client: k8sClient, calls: &calls}

	admittedAt := metav1.Now()
	err := patchQueuedEventStatus(ctx, cc, q, func(fresh *tatarav1alpha1.QueuedEvent) bool {
		fresh.Status.State = tatarav1alpha1.QueueStateAdmitted
		fresh.Status.TaskRef = "some-task"
		fresh.Status.AdmittedAt = &admittedAt
		return true
	})
	if err != nil {
		t.Fatalf("patchQueuedEventStatus returned an error instead of retrying: %v", err)
	}
	if calls.Load() < 2 {
		t.Fatalf("Status().Update called %d times, want >= 2 (must have retried after the injected conflict)", calls.Load())
	}

	got := refreshQE(t, ctx, q)
	if got.Status.State != tatarav1alpha1.QueueStateAdmitted || got.Status.TaskRef != "some-task" {
		t.Fatalf("write did not land after retry: %+v", got.Status)
	}
}

// TestAdmit_RetriesOnConflict_AdmitsDespiteStaleCache exercises the real
// admission path (admitPool, via r.admit) with an injected conflict on the
// QueuedEvent status write, proving the end-to-end fix, not just the helper in
// isolation.
func TestAdmit_RetriesOnConflict_AdmitsDespiteStaleCache(t *testing.T) {
	ctx := context.Background()
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "p-conflict-admit", Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{Queue: &tatarav1alpha1.QueueSpec{Capacity: 2, AlertCapacity: 1}},
	}
	mustCreate(t, ctx, proj)

	q := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "qe-", Namespace: testNS},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Seq: 1, Class: tatarav1alpha1.QueueClassNormal, Kind: "review", ProjectRef: proj.Name,
			Payload: tatarav1alpha1.QueuedEventPayload{Kind: "review", GenerateName: "scan-"},
		},
	}
	mustCreate(t, ctx, q)
	q.Status.State = tatarav1alpha1.QueueStateQueued
	mustStatusUpdate(t, ctx, q)

	var calls atomic.Int32
	cc := &conflictOnceQueuedEventClient{Client: k8sClient, calls: &calls}
	r := &DispatcherReconciler{Client: cc, Scheme: k8sClient.Scheme()}

	qes, tasks := listQEsTasks(t, ctx, proj.Name)
	if _, _, _, err := r.admit(ctx, proj, qes, tasks, budget.Decision{}, budget.Config{}, budget.Subscription{}, time.Now()); err != nil {
		t.Fatalf("admit returned an error instead of retrying the conflicting write: %v", err)
	}
	if calls.Load() < 2 {
		t.Fatalf("Status().Update called %d times, want >= 2", calls.Load())
	}
	if got := refreshQE(t, ctx, q); got.Status.State != tatarav1alpha1.QueueStateAdmitted {
		t.Fatalf("QE not admitted despite the retry, state=%q", got.Status.State)
	}
}

// TestAdmitTicket_RetriesOnConflict_StageEntersDespiteStaleCache covers the two
// other raw writes admitTicket makes (Task label Update, Task status Update for
// the agentKind self-heal): both must survive an injected conflict the same way
// the QueuedEvent write does.
//
// #521 deleted the OLD approved -> implementing admission edge admitTicket used
// to drive (refined is itself LIVE and runs the gate agent, so admission spawns
// a pod where the Task already is; the ONLY thing that now moves a Task to
// under-implementation is restapi's approval gate granting - see admitTicket's
// own doc comment). What is left for admission to write is the agentKind
// self-heal, and that write is SKIPPED ENTIRELY when status.agentKind already
// agrees with the ticket - so the fixture must seed a genuine MISMATCH (a stale
// agentKind, as a Task minted before an agentKind rename would carry) or the
// conflict this test injects is never even attempted.
func TestAdmitTicket_RetriesOnConflict_StageEntersDespiteStaleCache(t *testing.T) {
	ctx := context.Background()
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "p-conflict-ticket", Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{Queue: &tatarav1alpha1.QueueSpec{Capacity: 2, AlertCapacity: 1}},
	}
	mustCreate(t, ctx, proj)

	task := stageTask(t, ctx, proj.Name, "p-conflict-ticket-t1", "implement", tatarav1alpha1.StateRefined, 10*time.Minute, false)
	// Force the mismatch stageTask's own AgentKindFor stamp would not produce.
	// status.agentKind is itself a closed CRD enum, so the stand-in must be a
	// real (if wrong-for-this-Task) member of it, not an arbitrary string.
	task.Status.AgentKind = "review"
	mustStatusUpdate(t, ctx, task)
	q := ticket(t, ctx, proj.Name, task.Name, stage.AgentImplement, 1, tatarav1alpha1.QueueStateQueued)

	var updateCalls, statusCalls atomic.Int32
	cc := &conflictOnceTaskClient{Client: k8sClient, updateCalls: &updateCalls, statusCalls: &statusCalls}
	r := &DispatcherReconciler{Client: cc, Scheme: k8sClient.Scheme()}

	if _, _, err := r.admitTicket(ctx, q, time.Now()); err != nil {
		t.Fatalf("admitTicket returned an error instead of retrying the conflicting writes: %v", err)
	}
	if updateCalls.Load() < 2 {
		t.Fatalf("Task Update (label) called %d times, want >= 2", updateCalls.Load())
	}
	if statusCalls.Load() < 2 {
		t.Fatalf("Task Status().Update (agentKind self-heal) called %d times, want >= 2", statusCalls.Load())
	}
	gotTask := getTask(t, task.Name)
	if gotTask.Status.State != tatarav1alpha1.StateRefined {
		t.Fatalf("admission must not move state (the approved -> implementing edge is gone, #521), got %q", gotTask.Status.State)
	}
	if gotTask.Status.AgentKind != stage.AgentImplement {
		t.Fatalf("agentKind self-heal did not land: got %q, want %q", gotTask.Status.AgentKind, stage.AgentImplement)
	}
}
