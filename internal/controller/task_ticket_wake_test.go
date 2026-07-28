package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// TestAdmittedTicketWakesTaskReconcile runs a REAL manager against the envtest
// control plane with the REAL controllerBuilder (the same function
// SetupWithManager registers) and a recording reconciler, and drives the exact
// production sequence of an admission:
//
//	create the Task                -> wakes (For(&Task{}); drained)
//	create the ticket, Queued      -> must NOT wake (nothing is admittable yet)
//	flip the ticket to Admitted    -> MUST wake, immediately
//
// The last edge is a status-subresource write, which is why
// GenerationChangedPredicate is forbidden on this edge: it would drop it.
//
// This is the ONLY test that can distinguish the bug from the fix. Before the
// Watches edge, the Admitted flip woke nobody and the Task waited a full
// admissionRequeue (5m) for its pod. A fake client cannot cover it: it runs no
// informer, no predicate, no map func and no workqueue.
func TestAdmittedTicketWakesTaskReconcile(t *testing.T) {
	mgr := newTestManager(t)
	wakes := make(chan reconcile.Request, 32)
	metrics := obs.NewOperatorMetrics(prometheus.NewRegistry())

	// .Named() is REQUIRED: controller-runtime's controller-name registry is
	// process-wide, and task_controller_test.go already registers a controller
	// named "task" in this same test binary. This registers the SAME builder
	// production registers (controllerBuilder), not a hand-copied duplicate, so
	// deleting the real Watches edge turns this test RED instead of leaving it
	// passing against a stale copy.
	r := &TaskReconciler{Client: mgr.GetClient(), Metrics: metrics}
	err := r.controllerBuilder(mgr).
		Named("task-ticket-wake-envtest").
		Complete(reconcile.Func(func(_ context.Context, req reconcile.Request) (reconcile.Result, error) {
			select {
			case wakes <- req:
			default:
			}
			return reconcile.Result{}, nil
		}))
	if err != nil {
		t.Fatalf("register the test controller: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan error, 1)
	go func() { started <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-started
	})
	if !mgr.GetCache().WaitForCacheSync(ctx) {
		t.Fatal("manager cache did not sync")
	}

	// Kind must be a member of Task's own enum (api/v1alpha1/task_types.go:119:
	// brainstorm;incident;clarify;refine;review;documentation;takeover). It does
	// NOT include "implement" - that is an AGENT kind, which is a different enum
	// (queuedevent_types.go:57) and lives on the ticket's payload, not here.
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "ticket-wake-task", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "ticket-wake-proj", Kind: "review", Goal: "spawn the review pod",
		},
	}
	mustCreate(t, ctx, task)
	key := types.NamespacedName{Namespace: testNS, Name: task.Name}
	if !awaitWake(wakes, key, timeout) {
		t.Fatal("the Task create never reached the reconciler; the manager is not actually running")
	}

	q := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "ticket-wake-qe-", Namespace: testNS},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Seq: 1, Class: tatarav1alpha1.QueueClassNormal, Kind: "review",
			ProjectRef: "ticket-wake-proj",
			Payload: tatarav1alpha1.QueuedEventPayload{
				Kind: "review", AgentKind: "review", TaskRef: task.Name,
			},
		},
	}
	mustCreate(t, ctx, q)
	q.Status.State = tatarav1alpha1.QueueStateQueued
	mustStatusUpdate(t, ctx, q)
	if awaitWake(wakes, key, 2*time.Second) {
		t.Fatal("a Queued ticket woke the Task reconcile; only an ADMITTED ticket may")
	}

	// THE ADMISSION. The dispatcher writes the Task first and flips the ticket
	// LAST (queue_controller.go admitTicket, then the status patch), so this write
	// is the ONLY signal that the pod may now spawn.
	q.Status.State = tatarav1alpha1.QueueStateAdmitted
	mustStatusUpdate(t, ctx, q)
	if !awaitWake(wakes, key, timeout) {
		t.Fatalf("an admitted ticket did not wake the Task reconcile within %s; "+
			"without this edge the pod waits a full admissionRequeue (%s)", timeout, admissionRequeue)
	}

	// Exactly 1, not merely "at least one": EnqueueRequestsFromMapFunc.Update
	// runs the map func on both ObjectOld and ObjectNew, so a guard that
	// forgets to check Status.State would double-count this single admission.
	if got := testutil.ToFloat64(metrics.AdmissionWakeCounter(tatarav1alpha1.QueueClassNormal, "review")); got != 1 {
		t.Fatalf("operator_admission_wake_total = %v, want exactly 1 for one admission", got)
	}
}
