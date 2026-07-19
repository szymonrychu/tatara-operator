package controller

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/budget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"github.com/szymonrychu/tatara-operator/internal/stage"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func qe(name, class, state, taskRef string) tatarav1alpha1.QueuedEvent {
	return tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"},
		Spec:       tatarav1alpha1.QueuedEventSpec{Class: class, ProjectRef: "p"},
		Status:     tatarav1alpha1.QueuedEventStatus{State: state, TaskRef: taskRef},
	}
}

func tk(name, stg, queuedEvent string) tatarav1alpha1.Task {
	return tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara", Labels: map[string]string{queue.LabelQueuedEvent: queuedEvent}},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "p"},
		Status:     tatarav1alpha1.TaskStatus{Stage: stg},
	}
}

func TestAdmit_AlertBeforeNormal_AndCapacity(t *testing.T) {
	ctx := context.Background()
	ns := "tatara"
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "p-admit", Namespace: ns},
		Spec:       tatarav1alpha1.ProjectSpec{Queue: &tatarav1alpha1.QueueSpec{Capacity: 1, AlertCapacity: 1}},
	}
	mustCreate(t, ctx, proj)

	mkQE := func(seq int64, class string) *tatarav1alpha1.QueuedEvent {
		q := &tatarav1alpha1.QueuedEvent{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "qe-", Namespace: ns},
			Spec: tatarav1alpha1.QueuedEventSpec{
				Seq: seq, Class: class, Kind: "incident", ProjectRef: proj.Name,
				Payload: tatarav1alpha1.QueuedEventPayload{Kind: "incident", GenerateName: "x-"},
			},
		}
		mustCreate(t, ctx, q)
		q.Status.State = tatarav1alpha1.QueueStateQueued
		mustStatusUpdate(t, ctx, q)
		return q
	}
	normalQE := mkQE(1, tatarav1alpha1.QueueClassNormal) // older
	alertQE := mkQE(2, tatarav1alpha1.QueueClassAlert)   // newer but priority pool

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	qes, tasks := listQEsTasks(t, ctx, proj.Name)
	if _, _, err := r.admit(ctx, proj, qes, tasks, budget.Decision{}, budget.Config{}, budget.Subscription{}, time.Now()); err != nil {
		t.Fatal(err)
	}

	// Both pools have capacity 1: one alert + one normal admitted.
	got := refreshQE(t, ctx, alertQE)
	if got.Status.State != tatarav1alpha1.QueueStateAdmitted || got.Status.TaskRef == "" {
		t.Fatalf("alert not admitted: %+v", got.Status)
	}
	gotN := refreshQE(t, ctx, normalQE)
	if gotN.Status.State != tatarav1alpha1.QueueStateAdmitted {
		t.Fatalf("normal not admitted: %+v", gotN.Status)
	}
}

func TestAdmit_IdempotentOnReadmit(t *testing.T) {
	ctx := context.Background()
	ns := "tatara"
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "p-idem", Namespace: ns},
		Spec:       tatarav1alpha1.ProjectSpec{Queue: &tatarav1alpha1.QueueSpec{Capacity: 2, AlertCapacity: 2}},
	}
	mustCreate(t, ctx, proj)

	qe := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "qe-", Namespace: ns},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Seq: 1, Class: tatarav1alpha1.QueueClassNormal, Kind: "review", ProjectRef: proj.Name,
			Payload: tatarav1alpha1.QueuedEventPayload{Kind: "review", GenerateName: "scan-"},
		},
	}
	mustCreate(t, ctx, qe)
	qe.Status.State = tatarav1alpha1.QueueStateQueued
	mustStatusUpdate(t, ctx, qe)

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}

	// First admit: creates Task, marks Admitted.
	qes, tasks := listQEsTasks(t, ctx, proj.Name)
	if _, _, err := r.admit(ctx, proj, qes, tasks, budget.Decision{}, budget.Config{}, budget.Subscription{}, time.Now()); err != nil {
		t.Fatalf("first admit: %v", err)
	}
	got := refreshQE(t, ctx, qe)
	if got.Status.State != tatarav1alpha1.QueueStateAdmitted || got.Status.TaskRef == "" {
		t.Fatalf("after first admit: state=%q taskRef=%q", got.Status.State, got.Status.TaskRef)
	}
	taskRef := got.Status.TaskRef

	// Simulate failed Status().Update: reset state back to Queued without deleting the Task.
	got.Status.State = tatarav1alpha1.QueueStateQueued
	got.Status.TaskRef = ""
	got.Status.AdmittedAt = nil
	if err := k8sClient.Status().Update(ctx, got); err != nil {
		t.Fatalf("reset to Queued: %v", err)
	}

	// Second admit: Task already exists (AlreadyExists), must not create a second one.
	qes, tasks = listQEsTasks(t, ctx, proj.Name)
	if _, _, err := r.admit(ctx, proj, qes, tasks, budget.Decision{}, budget.Config{}, budget.Subscription{}, time.Now()); err != nil {
		t.Fatalf("second admit: %v", err)
	}

	// Exactly one Task with the QueuedEvent label.
	var tl tatarav1alpha1.TaskList
	if err := k8sClient.List(ctx, &tl, client.InNamespace(ns), client.MatchingLabels{queue.LabelQueuedEvent: qe.Name}); err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tl.Items) != 1 {
		t.Fatalf("expected 1 Task, got %d", len(tl.Items))
	}
	if tl.Items[0].Name != taskRef {
		t.Fatalf("task name changed: want %q got %q", taskRef, tl.Items[0].Name)
	}

	// QueuedEvent must end Admitted with correct TaskRef.
	got2 := refreshQE(t, ctx, qe)
	if got2.Status.State != tatarav1alpha1.QueueStateAdmitted {
		t.Fatalf("after re-admit: state=%q", got2.Status.State)
	}
	if got2.Status.TaskRef != taskRef {
		t.Fatalf("taskRef mismatch: want %q got %q", taskRef, got2.Status.TaskRef)
	}
}

func TestDispatcherReconcile_AdmitsThenFreesOnTerminal(t *testing.T) {
	ctx := context.Background()
	ns := "tatara"
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "p-disp", Namespace: ns},
		Spec:       tatarav1alpha1.ProjectSpec{Queue: &tatarav1alpha1.QueueSpec{Capacity: 1, AlertCapacity: 1}},
	}
	mustCreate(t, ctx, proj)

	mk := func(seq int64) *tatarav1alpha1.QueuedEvent {
		q := &tatarav1alpha1.QueuedEvent{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "qe-", Namespace: ns},
			Spec: tatarav1alpha1.QueuedEventSpec{Seq: seq, Class: tatarav1alpha1.QueueClassNormal, Kind: "incident", ProjectRef: proj.Name,
				Payload: tatarav1alpha1.QueuedEventPayload{Kind: "incident", GenerateName: "x-"}},
		}
		mustCreate(t, ctx, q)
		q.Status.State = tatarav1alpha1.QueueStateQueued
		mustStatusUpdate(t, ctx, q)
		return q
	}
	q1 := mk(1)
	q2 := mk(2)

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	if _, err := r.Reconcile(ctx, reqFor(q1)); err != nil {
		t.Fatal(err)
	}
	// capacity 1: q1 admitted, q2 still queued (head-of-line).
	if refreshQE(t, ctx, q1).Status.State != tatarav1alpha1.QueueStateAdmitted {
		t.Fatal("q1 should be admitted")
	}
	if refreshQE(t, ctx, q2).Status.State != tatarav1alpha1.QueueStateQueued {
		t.Fatal("q2 should still be queued (capacity 1)")
	}
	// Drive q1's task terminal, reconcile -> q1 Done, q2 admitted.
	task := taskForQE(t, ctx, refreshQE(t, ctx, q1))
	task.Status.Stage = tatarav1alpha1.StageDelivered
	mustStatusUpdate(t, ctx, task)
	if _, err := r.Reconcile(ctx, reqFor(q1)); err != nil {
		t.Fatal(err)
	}
	q1gone := &tatarav1alpha1.QueuedEvent{}
	err := k8sClient.Get(ctx, types.NamespacedName{Name: q1.Name, Namespace: q1.Namespace}, q1gone)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("q1 should be deleted after terminal, got state=%q err=%v", q1gone.Status.State, err)
	}
	if refreshQE(t, ctx, q2).Status.State != tatarav1alpha1.QueueStateAdmitted {
		t.Fatal("q2 should now be admitted")
	}
	// Reconciling from q2's own request must be a stable no-op (idempotent).
	if _, err := r.Reconcile(ctx, reqFor(q2)); err != nil {
		t.Fatal(err)
	}
	if refreshQE(t, ctx, q2).Status.State != tatarav1alpha1.QueueStateAdmitted {
		t.Fatal("q2 should remain admitted after re-reconcile")
	}
}

func TestDispatcherReconcile_HealsEmptyStateQE(t *testing.T) {
	ctx := context.Background()
	ns := "tatara"
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "p-heal", Namespace: ns},
		Spec:       tatarav1alpha1.ProjectSpec{Queue: &tatarav1alpha1.QueueSpec{Capacity: 1, AlertCapacity: 1}},
	}
	mustCreate(t, ctx, proj)

	// Simulate a stranded QE: Create without Status().Update (State=="").
	q := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "qe-heal-", Namespace: ns},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Seq: 1, Class: tatarav1alpha1.QueueClassNormal, Kind: "incident", ProjectRef: proj.Name,
			Payload: tatarav1alpha1.QueuedEventPayload{Kind: "incident", GenerateName: "x-"},
		},
	}
	mustCreate(t, ctx, q)
	// Do NOT call mustStatusUpdate — State remains "".

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	if _, err := r.Reconcile(ctx, reqFor(q)); err != nil {
		t.Fatal(err)
	}
	got := refreshQE(t, ctx, q)
	if got.Status.State != tatarav1alpha1.QueueStateAdmitted {
		t.Fatalf("stranded QE (State=='') should be admitted, got %q", got.Status.State)
	}
	if got.Status.TaskRef == "" {
		t.Fatal("admitted QE must have TaskRef set")
	}
}

// TestAdmit_StaleTerminalTaskNameCollision covers Finding #4: when a deterministic
// Task name collides with a TERMINAL Task, admit must delete the stale Task and
// NOT mark the QE Admitted on the first pass. On the second pass it creates a
// fresh Task and marks Admitted.
func TestAdmit_StaleTerminalTaskNameCollision(t *testing.T) {
	ctx := context.Background()
	ns := "tatara"
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "p-stale-terminal", Namespace: ns},
		Spec:       tatarav1alpha1.ProjectSpec{Queue: &tatarav1alpha1.QueueSpec{Capacity: 2, AlertCapacity: 2}},
	}
	mustCreate(t, ctx, proj)

	const fixedName = "my-deterministic-task"
	staleTask := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: fixedName, Namespace: ns},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Kind: "incident"},
	}
	mustCreate(t, ctx, staleTask)
	staleTask.Status.Stage = tatarav1alpha1.StageDelivered
	mustStatusUpdate(t, ctx, staleTask)

	q := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "qe-stale-", Namespace: ns},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Seq:        10,
			Class:      tatarav1alpha1.QueueClassNormal,
			Kind:       "incident",
			ProjectRef: proj.Name,
			Payload:    tatarav1alpha1.QueuedEventPayload{Kind: "incident", Name: fixedName},
		},
	}
	mustCreate(t, ctx, q)
	q.Status.State = tatarav1alpha1.QueueStateQueued
	mustStatusUpdate(t, ctx, q)

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}

	// First Reconcile: stale terminal Task collision -> delete stale, QE stays Queued.
	if _, err := r.Reconcile(ctx, reqFor(q)); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	staleGot := &tatarav1alpha1.Task{}
	err := k8sClient.Get(ctx, types.NamespacedName{Name: fixedName, Namespace: ns}, staleGot)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("stale terminal Task should be deleted after first Reconcile, got err=%v phase=%q", err, staleGot.Status.Stage)
	}
	got := refreshQE(t, ctx, q)
	if got.Status.State == tatarav1alpha1.QueueStateAdmitted {
		t.Fatalf("QE must not be Admitted on first pass, got state=%q taskRef=%q", got.Status.State, got.Status.TaskRef)
	}

	// Second Reconcile: stale Task gone -> fresh Task created, QE Admitted.
	if _, err := r.Reconcile(ctx, reqFor(q)); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	got2 := refreshQE(t, ctx, q)
	if got2.Status.State != tatarav1alpha1.QueueStateAdmitted {
		t.Fatalf("QE should be Admitted after second Reconcile, got state=%q", got2.Status.State)
	}
	if got2.Status.TaskRef == "" {
		t.Fatal("admitted QE must have TaskRef set")
	}
	freshTask := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: fixedName, Namespace: ns}, freshTask); err != nil {
		t.Fatalf("fresh Task %q not found: %v", fixedName, err)
	}
	if tatarav1alpha1.TaskDone(freshTask) {
		t.Fatalf("fresh Task must be non-terminal, got phase=%q ls=%q", freshTask.Status.Stage, freshTask.Status.StageReason)
	}
}

// TestAdmit_StaleTerminalDelete_RequeuesAndContinuesPool verifies that after a
// stale terminal Task is deleted on name collision, the Reconcile result has
// RequeueAfter > 0 AND a second QueuedEvent (no collision) in the same pool is
// admitted in the SAME first pass (pool loop does not early-return).
func TestAdmit_StaleTerminalDelete_RequeuesAndContinuesPool(t *testing.T) {
	ctx := context.Background()
	ns := "tatara"
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "p-stale-continue", Namespace: ns},
		Spec:       tatarav1alpha1.ProjectSpec{Queue: &tatarav1alpha1.QueueSpec{Capacity: 2, AlertCapacity: 2}},
	}
	mustCreate(t, ctx, proj)

	const fixedName = "my-det-task-continue"
	staleTask := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: fixedName, Namespace: ns},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Kind: "incident"},
	}
	mustCreate(t, ctx, staleTask)
	staleTask.Status.Stage = tatarav1alpha1.StageDelivered
	mustStatusUpdate(t, ctx, staleTask)

	q := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "qe-stale-cont-", Namespace: ns},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Seq: 10, Class: tatarav1alpha1.QueueClassNormal, Kind: "incident", ProjectRef: proj.Name,
			Payload: tatarav1alpha1.QueuedEventPayload{Kind: "incident", Name: fixedName},
		},
	}
	mustCreate(t, ctx, q)
	q.Status.State = tatarav1alpha1.QueueStateQueued
	mustStatusUpdate(t, ctx, q)

	q2 := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "qe-normal-cont-", Namespace: ns},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Seq: 11, Class: tatarav1alpha1.QueueClassNormal, Kind: "incident", ProjectRef: proj.Name,
			Payload: tatarav1alpha1.QueuedEventPayload{Kind: "incident", GenerateName: "normal-task-"},
		},
	}
	mustCreate(t, ctx, q2)
	q2.Status.State = tatarav1alpha1.QueueStateQueued
	mustStatusUpdate(t, ctx, q2)

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	result, err := r.Reconcile(ctx, reqFor(q))
	if err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if result.RequeueAfter <= 0 {
		t.Fatalf("expected RequeueAfter > 0 after stale delete, got %v", result.RequeueAfter)
	}
	got2 := refreshQE(t, ctx, q2)
	if got2.Status.State != tatarav1alpha1.QueueStateAdmitted {
		t.Fatalf("q2 (seq=11, no collision) must be Admitted in first pass, got state=%q", got2.Status.State)
	}
}

// TestAdmit_StaleTerminalDelete_SecondReconcileAdmits verifies the first
// Reconcile signals RequeueAfter > 0 (no admission for the colliding QE) and the
// second Reconcile admits the colliding QE against the fresh Task.
func TestAdmit_StaleTerminalDelete_SecondReconcileAdmits(t *testing.T) {
	ctx := context.Background()
	ns := "tatara"
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "p-stale-second", Namespace: ns},
		Spec:       tatarav1alpha1.ProjectSpec{Queue: &tatarav1alpha1.QueueSpec{Capacity: 2, AlertCapacity: 2}},
	}
	mustCreate(t, ctx, proj)

	const fixedName = "my-det-task-second"
	staleTask := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: fixedName, Namespace: ns},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Kind: "incident"},
	}
	mustCreate(t, ctx, staleTask)
	staleTask.Status.Stage = tatarav1alpha1.StageDelivered
	mustStatusUpdate(t, ctx, staleTask)

	q := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "qe-stale-second-", Namespace: ns},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Seq: 10, Class: tatarav1alpha1.QueueClassNormal, Kind: "incident", ProjectRef: proj.Name,
			Payload: tatarav1alpha1.QueuedEventPayload{Kind: "incident", Name: fixedName},
		},
	}
	mustCreate(t, ctx, q)
	q.Status.State = tatarav1alpha1.QueueStateQueued
	mustStatusUpdate(t, ctx, q)

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	result, err := r.Reconcile(ctx, reqFor(q))
	if err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if result.RequeueAfter <= 0 {
		t.Fatalf("expected RequeueAfter > 0 after stale delete, got %v", result.RequeueAfter)
	}
	got := refreshQE(t, ctx, q)
	if got.Status.State == tatarav1alpha1.QueueStateAdmitted {
		t.Fatalf("QE must not be Admitted on first pass, got state=%q", got.Status.State)
	}

	if _, err := r.Reconcile(ctx, reqFor(q)); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	got2 := refreshQE(t, ctx, q)
	if got2.Status.State != tatarav1alpha1.QueueStateAdmitted {
		t.Fatalf("QE should be Admitted after second Reconcile, got state=%q", got2.Status.State)
	}
	if got2.Status.TaskRef == "" {
		t.Fatal("admitted QE must have TaskRef set")
	}
}

// mkQueuedPriority is mkQueued (queue_controller_budget_test.go) plus an
// explicit priority and a creation-time override (raw-patched after Create,
// since CreationTimestamp is server-managed) so starvation-reservation tests
// can construct an event that has been Queued for a controlled duration.
func mkQueuedPriority(t *testing.T, ctx context.Context, projRef string, seq int64, class, kind string, priority int) *tatarav1alpha1.QueuedEvent {
	t.Helper()
	q := mkQueued(t, ctx, projRef, seq, class, kind)
	q.Spec.Priority = &priority
	if err := k8sClient.Update(ctx, q); err != nil {
		t.Fatalf("mkQueuedPriority: set priority: %v", err)
	}
	return q
}

// agedQEs returns a copy of qes with target's CreationTimestamp set to age in
// the past. metadata.creationTimestamp is server-managed and immutable - the
// real API server's generic registry Update/Patch strategy silently resets
// any client-supplied value back to the original, so it cannot be aged via a
// round-trip. admit() only reads the qes/tasks slices its caller passes in
// (the manager's cached List in production; whatever Reconcile/the test
// supplies otherwise), so aging the in-memory copy fed to admit() exercises
// exactly the code path under test.
func agedQEs(qes []tatarav1alpha1.QueuedEvent, target string, age time.Duration) []tatarav1alpha1.QueuedEvent {
	out := make([]tatarav1alpha1.QueuedEvent, len(qes))
	copy(out, qes)
	for i := range out {
		if out[i].Name == target {
			out[i].CreationTimestamp = metav1.NewTime(time.Now().Add(-age))
		}
	}
	return out
}

// TestAdmit_PriorityOrdersAheadOfSeq verifies (priority, seq) ascending
// admission (contract B.7 fix B3): a webhook-originated event (priority 1)
// is admitted ahead of an older-seq'd, lower-priority-number sweep event
// (priority 2), even though the sweep event was queued first.
func TestAdmit_PriorityOrdersAheadOfSeq(t *testing.T) {
	ctx := context.Background()
	name := "p-priority-order"
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{Queue: &tatarav1alpha1.QueueSpec{Capacity: 1, AlertCapacity: 1}},
	}
	mustCreate(t, ctx, proj)

	sweep1 := mkQueuedPriority(t, ctx, name, 1, tatarav1alpha1.QueueClassNormal, "documentation", 2)
	sweep2 := mkQueuedPriority(t, ctx, name, 2, tatarav1alpha1.QueueClassNormal, "documentation", 2)
	webhook := mkQueuedPriority(t, ctx, name, 3, tatarav1alpha1.QueueClassNormal, "clarify", 1)

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	qes, tasks := listQEsTasks(t, ctx, proj.Name)
	if _, _, err := r.admit(ctx, proj, qes, tasks, budget.Decision{}, budget.Config{}, budget.Subscription{}, time.Now()); err != nil {
		t.Fatal(err)
	}
	assertQEAdmitted(t, ctx, webhook, true)
	assertQEAdmitted(t, ctx, sweep1, false)
	assertQEAdmitted(t, ctx, sweep2, false)
}

// TestAdmit_PriorityFIFOWithinSamePriority verifies seq FIFO is preserved
// WITHIN a priority tier.
func TestAdmit_PriorityFIFOWithinSamePriority(t *testing.T) {
	ctx := context.Background()
	name := "p-priority-fifo"
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{Queue: &tatarav1alpha1.QueueSpec{Capacity: 1, AlertCapacity: 1}},
	}
	mustCreate(t, ctx, proj)

	older := mkQueuedPriority(t, ctx, name, 1, tatarav1alpha1.QueueClassNormal, "documentation", 2)
	newer := mkQueuedPriority(t, ctx, name, 2, tatarav1alpha1.QueueClassNormal, "documentation", 2)

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	qes, tasks := listQEsTasks(t, ctx, proj.Name)
	if _, _, err := r.admit(ctx, proj, qes, tasks, budget.Decision{}, budget.Config{}, budget.Subscription{}, time.Now()); err != nil {
		t.Fatal(err)
	}
	assertQEAdmitted(t, ctx, older, true)
	assertQEAdmitted(t, ctx, newer, false)
}

// TestAdmit_IncidentAheadOf150SweepEvents is the contract's literal scenario:
// an incident QueuedEvent (alert class, admitted from a pool the sweep
// backlog can never touch) is admitted despite a saturated normal pool full
// of lower-priority sweep work.
func TestAdmit_IncidentAheadOf150SweepEvents(t *testing.T) {
	ctx := context.Background()
	name := "p-incident-vs-sweep"
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{Queue: &tatarav1alpha1.QueueSpec{Capacity: 1, AlertCapacity: 1}},
	}
	mustCreate(t, ctx, proj)

	const sweepCount = 150
	var seq int64 = 1
	for i := 0; i < sweepCount; i++ {
		mkQueuedPriority(t, ctx, name, seq, tatarav1alpha1.QueueClassNormal, "documentation", 2)
		seq++
	}
	incident := mkQueuedPriority(t, ctx, name, seq, tatarav1alpha1.QueueClassAlert, "incident", 0)

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	qes, tasks := listQEsTasks(t, ctx, proj.Name)
	if _, _, err := r.admit(ctx, proj, qes, tasks, budget.Decision{}, budget.Config{}, budget.Subscription{}, time.Now()); err != nil {
		t.Fatal(err)
	}
	assertQEAdmitted(t, ctx, incident, true)
}

// TestAdmit_AlertCapacity_ReservesSlotUnderSaturatedNormalPool proves
// AlertCapacity reserves a slot independent of normal-pool saturation:
// admit() drains the alert pool FIRST (queue_controller.go), so a saturated
// normal pool never crowds out an incident.
func TestAdmit_AlertCapacity_ReservesSlotUnderSaturatedNormalPool(t *testing.T) {
	ctx := context.Background()
	name := "p-alert-reserved"
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{Queue: &tatarav1alpha1.QueueSpec{Capacity: 1, AlertCapacity: 1}},
	}
	mustCreate(t, ctx, proj)

	normal1 := mkQueued(t, ctx, name, 1, tatarav1alpha1.QueueClassNormal, "review")
	normal2 := mkQueued(t, ctx, name, 2, tatarav1alpha1.QueueClassNormal, "review")
	alert := mkQueued(t, ctx, name, 3, tatarav1alpha1.QueueClassAlert, "incident")

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	qes, tasks := listQEsTasks(t, ctx, proj.Name)
	if _, _, err := r.admit(ctx, proj, qes, tasks, budget.Decision{}, budget.Config{}, budget.Subscription{}, time.Now()); err != nil {
		t.Fatal(err)
	}
	assertQEAdmitted(t, ctx, alert, true)
	assertQEAdmitted(t, ctx, normal1, true) // normal pool has its own capacity=1, independent of alert
	assertQEAdmitted(t, ctx, normal2, false)
}

// TestAdmit_Priority2StarvationReservation is the contract's literal
// scenario (fix M21): a busy normal pool fully consumed by priority 0/1 work
// must still admit a priority-2 event once it has been Queued for > 1h -
// without the reservation, the nightly doc batch never gets a slot at all.
func TestAdmit_Priority2StarvationReservation(t *testing.T) {
	ctx := context.Background()
	name := "p-starve-p2"
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{Queue: &tatarav1alpha1.QueueSpec{Capacity: 2, AlertCapacity: 1}},
	}
	mustCreate(t, ctx, proj)

	// Two priority-1 (webhook) events crowd the whole 2-slot normal pool.
	p1a := mkQueuedPriority(t, ctx, name, 1, tatarav1alpha1.QueueClassNormal, "clarify", 1)
	p1b := mkQueuedPriority(t, ctx, name, 2, tatarav1alpha1.QueueClassNormal, "clarify", 1)
	// The nightly doc batch: priority 2, queued long enough to starve.
	docBatch := mkQueuedPriority(t, ctx, name, 3, tatarav1alpha1.QueueClassNormal, "documentation", 2)

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	qes, tasks := listQEsTasks(t, ctx, proj.Name)
	qes = agedQEs(qes, docBatch.Name, 2*time.Hour)
	if _, _, err := r.admit(ctx, proj, qes, tasks, budget.Decision{}, budget.Config{}, budget.Subscription{}, time.Now()); err != nil {
		t.Fatal(err)
	}
	assertQEAdmitted(t, ctx, docBatch, true)
	// Exactly one of the two priority-1 events yields its slot to the reserve.
	admittedP1 := 0
	if refreshQE(t, ctx, p1a).Status.State == tatarav1alpha1.QueueStateAdmitted {
		admittedP1++
	}
	if refreshQE(t, ctx, p1b).Status.State == tatarav1alpha1.QueueStateAdmitted {
		admittedP1++
	}
	if admittedP1 != 1 {
		t.Fatalf("want exactly 1 of 2 priority-1 events admitted (1 slot reserved for the starving priority-2 event), got %d", admittedP1)
	}
}

// TestAdmit_Priority2NotStarving_NoReservation is the negative case: a fresh
// (not-yet-1h-old) priority-2 event does NOT trigger the reservation, so
// priority 0/1 work fills the pool exactly as before this fix.
func TestAdmit_Priority2NotStarving_NoReservation(t *testing.T) {
	ctx := context.Background()
	name := "p-nostarve-p2"
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{Queue: &tatarav1alpha1.QueueSpec{Capacity: 2, AlertCapacity: 1}},
	}
	mustCreate(t, ctx, proj)

	p1a := mkQueuedPriority(t, ctx, name, 1, tatarav1alpha1.QueueClassNormal, "clarify", 1)
	p1b := mkQueuedPriority(t, ctx, name, 2, tatarav1alpha1.QueueClassNormal, "clarify", 1)
	docBatch := mkQueuedPriority(t, ctx, name, 3, tatarav1alpha1.QueueClassNormal, "documentation", 2)
	// docBatch is left at its real (fresh) CreationTimestamp: must not starve-reserve.

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	qes, tasks := listQEsTasks(t, ctx, proj.Name)
	if _, _, err := r.admit(ctx, proj, qes, tasks, budget.Decision{}, budget.Config{}, budget.Subscription{}, time.Now()); err != nil {
		t.Fatal(err)
	}
	assertQEAdmitted(t, ctx, p1a, true)
	assertQEAdmitted(t, ctx, p1b, true)
	assertQEAdmitted(t, ctx, docBatch, false)
}

// TestQueuedEvent_PriorityDefaultsToTwo_ViaAPIServer is THE regression test
// the contract calls out by name (Step 4): it must construct the QueuedEvent
// through the real envtest API server, not a Go literal - the whole bug
// (fix M17) is about what client-go actually serialises onto the wire when
// Priority is left unset. A Go literal has Priority==nil either way and would
// pass even a broken (non-pointer) implementation.
func TestQueuedEvent_PriorityDefaultsToTwo_ViaAPIServer(t *testing.T) {
	ctx := context.Background()
	q := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "qe-prio-default-", Namespace: testNS},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Seq: 1, Class: tatarav1alpha1.QueueClassNormal, Kind: "incident", ProjectRef: "p-prio-default",
			Payload: tatarav1alpha1.QueuedEventPayload{Kind: "incident", GenerateName: "x-"},
			// Priority deliberately omitted.
		},
	}
	mustCreate(t, ctx, q)
	got := refreshQE(t, ctx, q)
	if got.Spec.Priority == nil {
		t.Fatal("want Priority defaulted to non-nil by the API server (CRD +kubebuilder:default=2), got nil")
	}
	if *got.Spec.Priority != 2 {
		t.Fatalf("want Priority defaulted to 2, got %d", *got.Spec.Priority)
	}
}

// TestQueuedEvent_ExplicitPriorityZero_SurvivesAPIServer is the companion
// case: an explicit incident priority of 0 must round-trip as 0, not be
// mistaken for "unset" anywhere in the admission path.
func TestQueuedEvent_ExplicitPriorityZero_SurvivesAPIServer(t *testing.T) {
	ctx := context.Background()
	zero := 0
	q := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "qe-prio-zero-", Namespace: testNS},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Seq: 1, Class: tatarav1alpha1.QueueClassAlert, Kind: "incident", ProjectRef: "p-prio-zero",
			Payload:  tatarav1alpha1.QueuedEventPayload{Kind: "incident", GenerateName: "x-"},
			Priority: &zero,
		},
	}
	mustCreate(t, ctx, q)
	got := refreshQE(t, ctx, q)
	if got.Spec.Priority == nil || *got.Spec.Priority != 0 {
		t.Fatalf("want explicit Priority=0 to survive, got %v", got.Spec.Priority)
	}
}

// TestAdmit_UncachedReader_CountsInflightDespiteStaleCache is the contract's
// fix M28 regression test: a QE Admitted in reconcile N must be counted in
// reconcile N+1 even when the informer cache (simulated here by a STALE
// qes/tasks slice, exactly what Reconcile's cached List would have produced)
// has not caught up. Without r.APIReader wired, admit() would trust the
// stale slices and over-admit q2 past capacity=1.
func TestAdmit_UncachedReader_CountsInflightDespiteStaleCache(t *testing.T) {
	ctx := context.Background()
	name := "p-uncached-reader"
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{Queue: &tatarav1alpha1.QueueSpec{Capacity: 1, AlertCapacity: 1}},
	}
	mustCreate(t, ctx, proj)

	q1 := mkQueued(t, ctx, name, 1, tatarav1alpha1.QueueClassNormal, "incident")

	// "reconcile N": admit q1 for real via the fresh (envtest) client.
	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), APIReader: k8sClient}
	qes, tasks := listQEsTasks(t, ctx, proj.Name)
	if _, _, err := r.admit(ctx, proj, qes, tasks, budget.Decision{}, budget.Config{}, budget.Subscription{}, time.Now()); err != nil {
		t.Fatal(err)
	}
	assertQEAdmitted(t, ctx, q1, true)

	// "reconcile N+1": q2 arrives. Build a STALE qes/tasks pair - as if the
	// informer cache had not yet observed q1's admission - showing q1 still
	// Queued and no Task for it. If admit() trusted these for the inflight
	// count it would (wrongly) see 0 inflight and over-admit q2 past
	// capacity 1.
	q2 := mkQueued(t, ctx, name, 2, tatarav1alpha1.QueueClassNormal, "incident")
	staleQEs := []tatarav1alpha1.QueuedEvent{
		{ObjectMeta: q1.ObjectMeta, Spec: q1.Spec, Status: tatarav1alpha1.QueuedEventStatus{State: tatarav1alpha1.QueueStateQueued}},
		*refreshQE(t, ctx, q2),
	}
	staleTasks := []tatarav1alpha1.Task{}

	if _, _, err := r.admit(ctx, proj, staleQEs, staleTasks, budget.Decision{}, budget.Config{}, budget.Subscription{}, time.Now()); err != nil {
		t.Fatal(err)
	}
	assertQEAdmitted(t, ctx, q2, false)
}

// TestAdmit_NoAPIReader_FallsBackToPassedSlices verifies the nil-safe
// degrade path: with no APIReader wired (unit tests, zero-value reconciler),
// admit() uses the caller-supplied qes/tasks exactly as it did before fix
// M28 - it must not panic or error.
func TestAdmit_NoAPIReader_FallsBackToPassedSlices(t *testing.T) {
	ctx := context.Background()
	name := "p-no-apireader"
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{Queue: &tatarav1alpha1.QueueSpec{Capacity: 1, AlertCapacity: 1}},
	}
	mustCreate(t, ctx, proj)
	q1 := mkQueued(t, ctx, name, 1, tatarav1alpha1.QueueClassNormal, "incident")

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()} // APIReader nil
	qes, tasks := listQEsTasks(t, ctx, proj.Name)
	if _, _, err := r.admit(ctx, proj, qes, tasks, budget.Decision{}, budget.Config{}, budget.Subscription{}, time.Now()); err != nil {
		t.Fatal(err)
	}
	assertQEAdmitted(t, ctx, q1, true)
}

func TestPoolInflight_CountsAdmittedNonTerminal(t *testing.T) {
	r := &DispatcherReconciler{}
	// ticketFor models a live per-stage pod ticket (payload.agentKind set to
	// the stage's agent kind), not a mint: a mint's slot is spent once its
	// Task leaves the create/triaging bootstrap (ticketSpent), so a
	// no-agentKind event whose Task is already at reviewing/delivered would
	// no longer exercise "still holds a slot" - use the realistic shape.
	ticketFor := func(name, class, taskRef, stg string) tatarav1alpha1.QueuedEvent {
		q := qe(name, class, tatarav1alpha1.QueueStateAdmitted, taskRef)
		q.Spec.Payload.AgentKind = stage.AgentKindFor(stg)
		return q
	}
	qes := []tatarav1alpha1.QueuedEvent{
		ticketFor("a", tatarav1alpha1.QueueClassNormal, "t-a", tatarav1alpha1.StageReviewing), // running -> counts
		ticketFor("b", tatarav1alpha1.QueueClassNormal, "t-b", tatarav1alpha1.StageDelivered), // terminal -> not
		ticketFor("c", tatarav1alpha1.QueueClassAlert, "t-c", tatarav1alpha1.StageReviewing),  // alert running
		qe("d", tatarav1alpha1.QueueClassNormal, tatarav1alpha1.QueueStateQueued, ""),         // queued -> not
	}
	tasks := []tatarav1alpha1.Task{
		tk("t-a", tatarav1alpha1.StageReviewing, "a"),
		tk("t-b", tatarav1alpha1.StageDelivered, "b"),
		tk("t-c", tatarav1alpha1.StageReviewing, "c"),
	}
	if got := r.poolInflight(qes, tasks, tatarav1alpha1.QueueClassNormal); got != 1 {
		t.Fatalf("normal inflight = %d, want 1", got)
	}
	if got := r.poolInflight(qes, tasks, tatarav1alpha1.QueueClassAlert); got != 1 {
		t.Fatalf("alert inflight = %d, want 1", got)
	}
}

// --- G1: admission of an EXISTING Task (contract B.7 / F.3) -----------------
//
// In the task-centric model a Task already EXISTS by the time it reaches a pod
// stage. Its QueuedEvent is an ADMISSION TICKET for that Task's pod, not a
// Task-creation request: payload.taskRef names the Task, payload.newTask (or a
// legacy flat payload) is the only shape that still mints.

// stageTask creates a Task parked at the given stage, entered enteredAgo ago.
// podStarted stamps status.podStartedAt (a live agent), leaving it nil means the
// Task is QUEUEING for admission (clock 1).
func stageTask(t *testing.T, ctx context.Context, project, name, kind, stg string, enteredAgo time.Duration, podStarted bool) *tatarav1alpha1.Task {
	t.Helper()
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: project, Kind: kind, Goal: "g"},
	}
	mustCreate(t, ctx, task)
	entered := metav1.NewTime(time.Now().Add(-enteredAgo))
	task.Status.Stage = stg
	task.Status.StageEnteredAt = &entered
	task.Status.AgentKind = stage.AgentKindFor(stg)
	if podStarted {
		started := metav1.NewTime(time.Now().Add(-enteredAgo / 2))
		task.Status.PodStartedAt = &started
	}
	mustStatusUpdate(t, ctx, task)
	return task
}

// ticket creates a QueuedEvent that ADMITS an existing Task (payload.taskRef).
func ticket(t *testing.T, ctx context.Context, project, taskRef, agentKind string, seq int64, state string) *tatarav1alpha1.QueuedEvent {
	t.Helper()
	q := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "qe-ticket-", Namespace: testNS},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Seq: seq, Class: tatarav1alpha1.QueueClassNormal, Kind: agentKind, ProjectRef: project,
			Payload: tatarav1alpha1.QueuedEventPayload{Kind: agentKind, AgentKind: agentKind, TaskRef: taskRef},
		},
	}
	mustCreate(t, ctx, q)
	q.Status.State = state
	if state == tatarav1alpha1.QueueStateAdmitted {
		q.Status.TaskRef = taskRef
		now := metav1.Now()
		q.Status.AdmittedAt = &now
	}
	mustStatusUpdate(t, ctx, q)
	return q
}

func countTasks(t *testing.T, ctx context.Context, project string) int {
	t.Helper()
	_, tasks := listQEsTasks(t, ctx, project)
	return len(tasks)
}

func refreshTask(t *testing.T, ctx context.Context, name string) *tatarav1alpha1.Task {
	t.Helper()
	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNS}, got); err != nil {
		t.Fatalf("refreshTask %s: %v", name, err)
	}
	return got
}

// TestAdmit_Ticket_AdmitsExistingTaskAndMintsNothing: the core G1 property.
func TestAdmit_Ticket_AdmitsExistingTaskAndMintsNothing(t *testing.T) {
	ctx := context.Background()
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "p-ticket", Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{Queue: &tatarav1alpha1.QueueSpec{Capacity: 2, AlertCapacity: 1}},
	}
	mustCreate(t, ctx, proj)

	task := stageTask(t, ctx, proj.Name, "p-ticket-impl", "clarify", tatarav1alpha1.StageImplementing, 5*time.Minute, false)
	q := ticket(t, ctx, proj.Name, task.Name, stage.AgentImplement, 1, tatarav1alpha1.QueueStateQueued)

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	if _, err := r.Reconcile(ctx, reqFor(q)); err != nil {
		t.Fatal(err)
	}

	got := refreshQE(t, ctx, q)
	if got.Status.State != tatarav1alpha1.QueueStateAdmitted || got.Status.TaskRef != task.Name {
		t.Fatalf("ticket not admitted onto the existing Task: state=%q taskRef=%q", got.Status.State, got.Status.TaskRef)
	}
	if n := countTasks(t, ctx, proj.Name); n != 1 {
		t.Fatalf("admission minted a second Task: %d Tasks in project", n)
	}
	gotTask := refreshTask(t, ctx, task.Name)
	if gotTask.Labels[queue.LabelQueuedEvent] != q.Name {
		t.Fatalf("admitted Task must carry the queued-event label (pod-ensure gate + slot accounting): %v", gotTask.Labels)
	}
	if gotTask.Status.Stage != tatarav1alpha1.StageImplementing {
		t.Fatalf("stage must be untouched, got %q", gotTask.Status.Stage)
	}
	if got := r.poolInflight([]tatarav1alpha1.QueuedEvent{*got}, []tatarav1alpha1.Task{*gotTask}, tatarav1alpha1.QueueClassNormal); got != 1 {
		t.Fatalf("an admitted ticket must hold exactly one slot, got %d", got)
	}
}

// TestAdmit_Ticket_ApprovedEntersImplementing: contract F.3's
// `approved -> implementing` edge - "a QueuedEvent for the implement pod is
// ADMITTED". approved is POD-LESS: the Task waits there, and ADMISSION is the
// trigger.
func TestAdmit_Ticket_ApprovedEntersImplementing(t *testing.T) {
	ctx := context.Background()
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "p-approved", Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{Queue: &tatarav1alpha1.QueueSpec{Capacity: 2, AlertCapacity: 1}},
	}
	mustCreate(t, ctx, proj)

	task := stageTask(t, ctx, proj.Name, "p-approved-t1", "clarify", tatarav1alpha1.StageApproved, 10*time.Minute, false)
	q := ticket(t, ctx, proj.Name, task.Name, stage.AgentImplement, 1, tatarav1alpha1.QueueStateQueued)

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	if _, err := r.Reconcile(ctx, reqFor(q)); err != nil {
		t.Fatal(err)
	}

	if got := refreshQE(t, ctx, q); got.Status.State != tatarav1alpha1.QueueStateAdmitted {
		t.Fatalf("ticket not admitted: %q", got.Status.State)
	}
	gotTask := refreshTask(t, ctx, task.Name)
	if gotTask.Status.Stage != tatarav1alpha1.StageImplementing {
		t.Fatalf("approved -> implementing not applied on admission, stage=%q", gotTask.Status.Stage)
	}
	if gotTask.Status.AgentKind != stage.AgentImplement {
		t.Fatalf("status.agentKind = %q, want implement", gotTask.Status.AgentKind)
	}
	if gotTask.Status.PodStartedAt != nil || gotTask.Status.StageWorkStartedAt != nil {
		t.Fatal("stage.Enter must clear podStartedAt / stageWorkStartedAt")
	}
	if gotTask.Status.StageEnteredAt == nil || time.Since(gotTask.Status.StageEnteredAt.Time) > time.Minute {
		t.Fatalf("stageEnteredAt not re-stamped: %v", gotTask.Status.StageEnteredAt)
	}
	if n := countTasks(t, ctx, proj.Name); n != 1 {
		t.Fatalf("minted a Task on the approved edge: %d", n)
	}
}

// TestAdmit_Ticket_TwiceSpawnsOnePod: admitting the same ticket twice must not
// double-admit (one pod, one slot).
func TestAdmit_Ticket_TwiceSpawnsOnePod(t *testing.T) {
	ctx := context.Background()
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "p-ticket-idem", Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{Queue: &tatarav1alpha1.QueueSpec{Capacity: 3, AlertCapacity: 1}},
	}
	mustCreate(t, ctx, proj)

	task := stageTask(t, ctx, proj.Name, "p-ticket-idem-t", "clarify", tatarav1alpha1.StageClarifying, time.Minute, false)
	q := ticket(t, ctx, proj.Name, task.Name, stage.AgentClarify, 1, tatarav1alpha1.QueueStateQueued)

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	if _, err := r.Reconcile(ctx, reqFor(q)); err != nil {
		t.Fatal(err)
	}
	first := refreshQE(t, ctx, q)
	if first.Status.State != tatarav1alpha1.QueueStateAdmitted || first.Status.AdmittedAt == nil {
		t.Fatalf("not admitted: %+v", first.Status)
	}
	if _, err := r.Reconcile(ctx, reqFor(q)); err != nil {
		t.Fatal(err)
	}
	second := refreshQE(t, ctx, q)
	if !second.Status.AdmittedAt.Equal(first.Status.AdmittedAt) {
		t.Fatal("re-reconcile re-admitted the ticket (would spawn a second pod)")
	}
	if n := countTasks(t, ctx, proj.Name); n != 1 {
		t.Fatalf("re-admission minted a Task: %d", n)
	}
	qes, tasks := listQEsTasks(t, ctx, proj.Name)
	if got := r.poolInflight(qes, tasks, tatarav1alpha1.QueueClassNormal); got != 1 {
		t.Fatalf("inflight = %d, want 1 (one Task, one ticket)", got)
	}
}

// TestAdmit_Ticket_DroppedWhenTaskGoneOrTerminal: a ticket whose Task has since
// been deleted or has gone terminal is dropped CLEANLY - deleted, no slot burned,
// no requeue loop, no panic - and the pool keeps draining behind it.
func TestAdmit_Ticket_DroppedWhenTaskGoneOrTerminal(t *testing.T) {
	ctx := context.Background()
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "p-ticket-drop", Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{Queue: &tatarav1alpha1.QueueSpec{Capacity: 1, AlertCapacity: 1}},
	}
	mustCreate(t, ctx, proj)

	// Ticket 1: the Task never existed (deleted mid-flight).
	ghost := ticket(t, ctx, proj.Name, "p-ticket-drop-gone", stage.AgentImplement, 1, tatarav1alpha1.QueueStateQueued)

	// Ticket 2: the Task went terminal (failed) while queueing.
	dead := stageTask(t, ctx, proj.Name, "p-ticket-drop-dead", "clarify", tatarav1alpha1.StageImplementing, time.Minute, false)
	dead.Status.Stage = tatarav1alpha1.StageFailed
	dead.Status.StageReason = stage.ReasonTurnBudgetExhausted
	mustStatusUpdate(t, ctx, dead)
	deadQE := ticket(t, ctx, proj.Name, dead.Name, stage.AgentImplement, 2, tatarav1alpha1.QueueStateQueued)

	// Ticket 3: a live Task behind them. Capacity is 1: it must still be admitted,
	// i.e. the two dropped tickets burned no slot.
	live := stageTask(t, ctx, proj.Name, "p-ticket-drop-live", "clarify", tatarav1alpha1.StageClarifying, time.Minute, false)
	liveQE := ticket(t, ctx, proj.Name, live.Name, stage.AgentClarify, 3, tatarav1alpha1.QueueStateQueued)

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	if _, err := r.Reconcile(ctx, reqFor(ghost)); err != nil {
		t.Fatalf("reconcile must not error on a vanished Task: %v", err)
	}

	for _, q := range []*tatarav1alpha1.QueuedEvent{ghost, deadQE} {
		err := k8sClient.Get(ctx, types.NamespacedName{Name: q.Name, Namespace: testNS}, &tatarav1alpha1.QueuedEvent{})
		if !apierrors.IsNotFound(err) {
			t.Fatalf("stale ticket %s must be deleted, err=%v", q.Name, err)
		}
	}
	if got := refreshQE(t, ctx, liveQE); got.Status.State != tatarav1alpha1.QueueStateAdmitted {
		t.Fatalf("live ticket behind two dropped ones must still be admitted, state=%q", got.Status.State)
	}
}

// TestAdmit_Ticket_StageBeatsPayloadAgentKind: the stage machine is the source of
// truth; payload.agentKind is advisory. On disagreement the STAGE wins.
func TestAdmit_Ticket_StageBeatsPayloadAgentKind(t *testing.T) {
	ctx := context.Background()
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "p-ticket-kind", Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{Queue: &tatarav1alpha1.QueueSpec{Capacity: 2, AlertCapacity: 1}},
	}
	mustCreate(t, ctx, proj)

	task := stageTask(t, ctx, proj.Name, "p-ticket-kind-t", "clarify", tatarav1alpha1.StageReviewing, time.Minute, false)
	// Stale ticket: it says implement, but the Task is REVIEWING.
	q := ticket(t, ctx, proj.Name, task.Name, stage.AgentImplement, 1, tatarav1alpha1.QueueStateQueued)

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	if _, err := r.Reconcile(ctx, reqFor(q)); err != nil {
		t.Fatal(err)
	}
	if got := refreshQE(t, ctx, q); got.Status.State != tatarav1alpha1.QueueStateAdmitted {
		t.Fatalf("ticket should still admit (the stage decides the pod), state=%q", got.Status.State)
	}
	gotTask := refreshTask(t, ctx, task.Name)
	if gotTask.Status.AgentKind != stage.AgentReview {
		t.Fatalf("status.agentKind = %q, want review (the STAGE wins over payload.agentKind)", gotTask.Status.AgentKind)
	}
	if gotTask.Status.Stage != tatarav1alpha1.StageReviewing {
		t.Fatalf("stage must not follow the payload, got %q", gotTask.Status.Stage)
	}
}

// TestAdmit_SteadyState_QueuedFortyMinutesReachesImplementing is fix V6-1, and it
// is the single most important test in this file. maxConcurrentAgents=3, three
// agents live, a FOURTH Task enters a pod stage and QUEUES for 40 minutes. It must
// reach implementing NORMALLY. It must NOT terminate. A Task queueing behind three
// live agents IS the normal steady state at maxOpenTasks=6.
func TestAdmit_SteadyState_QueuedFortyMinutesReachesImplementing(t *testing.T) {
	ctx := context.Background()
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "p-steady", Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			MaxConcurrentAgents: 3,
			Queue:               &tatarav1alpha1.QueueSpec{Capacity: 3, AlertCapacity: 1},
		},
	}
	mustCreate(t, ctx, proj)

	// Three live agents: pods started, tickets Admitted.
	for i := 1; i <= 3; i++ {
		name := "p-steady-live-" + strconv.Itoa(i)
		live := stageTask(t, ctx, proj.Name, name, "clarify", tatarav1alpha1.StageImplementing, 30*time.Minute, true)
		q := ticket(t, ctx, proj.Name, live.Name, stage.AgentImplement, int64(i), tatarav1alpha1.QueueStateAdmitted)
		live.Labels = map[string]string{queue.LabelQueuedEvent: q.Name}
		if err := k8sClient.Update(ctx, live); err != nil {
			t.Fatal(err)
		}
	}

	// The fourth Task: in a pod stage, no pod, queueing for 40 minutes.
	waiter := stageTask(t, ctx, proj.Name, "p-steady-waiter", "clarify", tatarav1alpha1.StageImplementing, 40*time.Minute, false)
	waiterQE := ticket(t, ctx, proj.Name, waiter.Name, stage.AgentImplement, 4, tatarav1alpha1.QueueStateQueued)

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	if _, err := r.Reconcile(ctx, reqFor(waiterQE)); err != nil {
		t.Fatal(err)
	}

	// Saturated: the waiter is still Queued, and it is NOT terminated. The 40m wait
	// is covered by CLOCK 1 (admission, 24h), not by the 5m readiness clock.
	if got := refreshQE(t, ctx, waiterQE); got.Status.State != tatarav1alpha1.QueueStateQueued {
		t.Fatalf("waiter must stay Queued at capacity, state=%q", got.Status.State)
	}
	got := refreshTask(t, ctx, waiter.Name)
	if tatarav1alpha1.StageTerminal(got) {
		t.Fatalf("QUEUEING KILLED THE TASK: stage=%q reason=%q", got.Status.Stage, got.Status.StageReason)
	}
	clock, _, budget, _ := stage.ArmedClock(got, false)
	if clock != stage.ClockAdmission || budget != tatarav1alpha1.AdmissionStarvedBudget {
		t.Fatalf("armed clock = %q/%v, want admission/24h", clock, budget)
	}
	if _, elapsed := stage.Elapsed(got, false, time.Now()); elapsed {
		t.Fatal("40 minutes in the queue must not elapse any clock")
	}

	// One agent finishes: the waiter is admitted, NORMALLY, still implementing.
	done := refreshTask(t, ctx, "p-steady-live-1")
	done.Status.Stage = tatarav1alpha1.StageDelivered
	mustStatusUpdate(t, ctx, done)

	if _, err := r.Reconcile(ctx, reqFor(waiterQE)); err != nil {
		t.Fatal(err)
	}
	admitted := refreshQE(t, ctx, waiterQE)
	if admitted.Status.State != tatarav1alpha1.QueueStateAdmitted || admitted.Status.TaskRef != waiter.Name {
		t.Fatalf("waiter not admitted once a slot freed: state=%q taskRef=%q", admitted.Status.State, admitted.Status.TaskRef)
	}
	final := refreshTask(t, ctx, waiter.Name)
	if final.Status.Stage != tatarav1alpha1.StageImplementing || tatarav1alpha1.StageTerminal(final) {
		t.Fatalf("waiter must reach implementing normally, stage=%q reason=%q", final.Status.Stage, final.Status.StageReason)
	}
	if final.Labels[queue.LabelQueuedEvent] != waiterQE.Name {
		t.Fatalf("admitted Task not labelled with its ticket: %v", final.Labels)
	}
}

// --- regression: incident mint-vs-own-pod-ticket admission deadlock --------
//
// Production incident (2026-07-13, first live incident after cutover): a
// mint payload (agentKind=="") never became ticketSpent, so an incident's
// CREATE QueuedEvent (class=alert) stayed inflight forever, holding the
// single AlertCapacity=1 slot. The Task's OWN investigating pod-ticket
// (also class=alert, same Task) then needed that same slot to admit - the
// Task held the slot its own pod needed, and every subsequent incident
// queued behind it. See MEMORY.md 2026-07-13.

// TestTicketSpent_MintPayload is table-driven over every stage bucket a mint
// payload's ticketSpent switch distinguishes: still-bootstrapping ("",
// triaging) must NOT spend the mint's slot (two incidents must not both
// jump the alert lane while their Tasks are still being minted/triaged), and
// every stage past bootstrap - including the Task's own investigating pod
// stage, the deadlock's exact trigger - MUST spend it.
func TestTicketSpent_MintPayload(t *testing.T) {
	mint := &tatarav1alpha1.QueuedEvent{
		Spec: tatarav1alpha1.QueuedEventSpec{Payload: tatarav1alpha1.QueuedEventPayload{AgentKind: ""}},
	}
	cases := []struct {
		name  string
		stage string
		want  bool
	}{
		{"unstamped, not yet touched by the stage machine", "", false},
		{"triaging bootstrap", tatarav1alpha1.StageTriaging, false},
		{"investigating: the Task's OWN pod stage, the deadlock trigger", tatarav1alpha1.StageInvestigating, true},
		{"clarifying: any other pod stage", tatarav1alpha1.StageClarifying, true},
		{"approved: pod-less admission gate, past bootstrap", tatarav1alpha1.StageApproved, true},
		{"delivered: quasi-terminal", tatarav1alpha1.StageDelivered, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := &tatarav1alpha1.Task{Status: tatarav1alpha1.TaskStatus{Stage: tc.stage}}
			if got := ticketSpent(mint, task); got != tc.want {
				t.Fatalf("ticketSpent(mint, stage=%q) = %v, want %v", tc.stage, got, tc.want)
			}
		})
	}
}

// TestAdmit_IncidentMintReleasesAlertSlot_RegressionForDeadlock reproduces the
// production deadlock end to end against a real envtest apiserver (not the
// Seq==nil unit-test bypass that let this ship): a mint CREATE event admits
// and mints an incident Task at AlertCapacity=1, the Task is driven to its
// own investigating pod stage, and its OWN pod-ticket (same alert class, same
// Task) is queued behind the still-Admitted mint event. Before the fix,
// poolInflight(alert) reads 1 (AT capacity) and the pod-ticket never admits -
// a permanent deadlock. After the fix, the mint's slot is spent the moment
// the Task leaves triaging, poolInflight(alert) reads 0, and the pod-ticket
// admits normally.
func TestAdmit_IncidentMintReleasesAlertSlot_RegressionForDeadlock(t *testing.T) {
	ctx := context.Background()
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "p-incident-deadlock", Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{Queue: &tatarav1alpha1.QueueSpec{Capacity: 2, AlertCapacity: 1}},
	}
	mustCreate(t, ctx, proj)

	// The incident's CREATE (mint) event: class=alert, agentKind="" (payload
	// carries no requested stage - it only brings the Task into existence).
	mint := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "qe-incident-create-", Namespace: testNS},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Seq: 1, Class: tatarav1alpha1.QueueClassAlert, Kind: "incident", ProjectRef: proj.Name,
			Payload: tatarav1alpha1.QueuedEventPayload{Kind: "incident", GenerateName: "incident-"},
		},
	}
	mustCreate(t, ctx, mint)
	mint.Status.State = tatarav1alpha1.QueueStateQueued
	mustStatusUpdate(t, ctx, mint)

	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	qes, tasks := listQEsTasks(t, ctx, proj.Name)
	if _, _, err := r.admit(ctx, proj, qes, tasks, budget.Decision{}, budget.Config{}, budget.Subscription{}, time.Now()); err != nil {
		t.Fatal(err)
	}
	gotMint := refreshQE(t, ctx, mint)
	if gotMint.Status.State != tatarav1alpha1.QueueStateAdmitted || gotMint.Status.TaskRef == "" {
		t.Fatalf("mint not admitted: %+v", gotMint.Status)
	}
	taskName := gotMint.Status.TaskRef

	// Drive the Task to its OWN investigating pod stage (as the triaging
	// controller would for spec.kind=incident), same alert class.
	task := refreshTask(t, ctx, taskName)
	task.Status.Stage = tatarav1alpha1.StageInvestigating
	task.Status.AgentKind = stage.AgentIncident
	entered := metav1.Now()
	task.Status.StageEnteredAt = &entered
	mustStatusUpdate(t, ctx, task)

	// The investigating pod's OWN ticket: same alert class, same Task, the
	// exact shape that deadlocks against the still-Admitted mint event.
	podTicket := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "qe-incident-pod-", Namespace: testNS},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Seq: 2, Class: tatarav1alpha1.QueueClassAlert, Kind: stage.AgentIncident, ProjectRef: proj.Name,
			Payload: tatarav1alpha1.QueuedEventPayload{Kind: stage.AgentIncident, AgentKind: stage.AgentIncident, TaskRef: taskName},
		},
	}
	mustCreate(t, ctx, podTicket)
	podTicket.Status.State = tatarav1alpha1.QueueStateQueued
	mustStatusUpdate(t, ctx, podTicket)

	// The core accounting assertion (do this BEFORE the second admit pass, so
	// it isolates poolInflight/ticketSpent rather than the full admit flow):
	// the mint's slot must already be spent once the Task is past triaging.
	qes, tasks = listQEsTasks(t, ctx, proj.Name)
	if got := r.poolInflight(qes, tasks, tatarav1alpha1.QueueClassAlert); got != 0 {
		t.Fatalf("alert poolInflight = %d, want 0 once the Task is at investigating - "+
			"this IS the production deadlock (the mint holds the slot its own pod-ticket needs)", got)
	}

	// End to end: the pod-ticket now admits (proves the whole admit() path,
	// not just the accounting helpers, clears the deadlock).
	if _, _, err := r.admit(ctx, proj, qes, tasks, budget.Decision{}, budget.Config{}, budget.Subscription{}, time.Now()); err != nil {
		t.Fatal(err)
	}
	gotTicket := refreshQE(t, ctx, podTicket)
	if gotTicket.Status.State != tatarav1alpha1.QueueStateAdmitted {
		t.Fatalf("investigating pod-ticket deadlocked: state=%q (want Admitted)", gotTicket.Status.State)
	}
}

// TestDispatcherEnqueuePending_NoWatchTrigger guards issue #395: the
// dispatcher is otherwise purely watch-driven, so a QueuedEvent left Queued
// with no fresh watch trigger (a rollout/leader-handoff race) can stall
// admission indefinitely. EnqueuePending is the leader-only backstop's core
// list-and-push step: called directly here with NO watch and NO ticker
// running, it must still find the pending QueuedEvent and push a GenericEvent
// for it onto BackstopEvents.
func TestDispatcherEnqueuePending_NoWatchTrigger(t *testing.T) {
	ctx := context.Background()
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "p-backstop-", Namespace: testNS},
	}
	mustCreate(t, ctx, proj)

	q := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "qe-backstop-", Namespace: testNS},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Seq: 1, Class: tatarav1alpha1.QueueClassNormal, Kind: "documentation", ProjectRef: proj.Name,
			Payload: tatarav1alpha1.QueuedEventPayload{Kind: "documentation", GenerateName: "x-"},
		},
	}
	mustCreate(t, ctx, q)
	q.Status.State = tatarav1alpha1.QueueStateQueued
	mustStatusUpdate(t, ctx, q)

	events := make(chan event.GenericEvent, 4)
	metrics := obs.NewOperatorMetrics(prometheus.NewRegistry())
	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Metrics: metrics, BackstopEvents: events}

	before := testutil.ToFloat64(metrics.DispatcherBackstopEnqueuedCounter(proj.Name))
	n, err := r.EnqueuePending(ctx)
	if err != nil {
		t.Fatalf("EnqueuePending: %v", err)
	}
	if n < 1 {
		t.Fatalf("EnqueuePending enqueued %d events, want >= 1 for the pending QueuedEvent", n)
	}
	select {
	case ev := <-events:
		if ev.Object.GetName() != q.Name || ev.Object.GetNamespace() != q.Namespace {
			t.Fatalf("enqueued object = %s/%s, want %s/%s", ev.Object.GetNamespace(), ev.Object.GetName(), q.Namespace, q.Name)
		}
	default:
		t.Fatal("EnqueuePending reported events but pushed none onto BackstopEvents")
	}
	if after := testutil.ToFloat64(metrics.DispatcherBackstopEnqueuedCounter(proj.Name)); after <= before {
		t.Fatalf("operator_dispatcher_backstop_enqueued_total{project=%s} did not increment: before=%v after=%v", proj.Name, before, after)
	}
}

// TestDispatcherEnqueuePending_NilChannelIsNoop asserts a DispatcherReconciler
// built without BackstopEvents wired (every existing unit test in this file)
// is unaffected: EnqueuePending must not panic or block.
func TestDispatcherEnqueuePending_NilChannelIsNoop(t *testing.T) {
	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	if _, err := r.EnqueuePending(context.Background()); err != nil {
		t.Fatalf("EnqueuePending with nil BackstopEvents: %v", err)
	}
}

// TestDispatcherRunBackstop_TicksIndependentlyOfWatch guards issue #395's
// deterministic-admission requirement: RunBackstop must push a fresh
// GenericEvent on Start AND again on every tick, with no controller, no
// manager, and no watch event involved at all - proving the periodic backstop
// fires on its own clock, not on replay-timing luck.
func TestDispatcherRunBackstop_TicksIndependentlyOfWatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proj := &tatarav1alpha1.Project{ObjectMeta: metav1.ObjectMeta{GenerateName: "p-tick-", Namespace: testNS}}
	mustCreate(t, ctx, proj)
	q := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "qe-tick-", Namespace: testNS},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Seq: 1, Class: tatarav1alpha1.QueueClassNormal, Kind: "documentation", ProjectRef: proj.Name,
			Payload: tatarav1alpha1.QueuedEventPayload{Kind: "documentation", GenerateName: "x-"},
		},
	}
	mustCreate(t, ctx, q)
	q.Status.State = tatarav1alpha1.QueueStateQueued
	mustStatusUpdate(t, ctx, q)

	events := make(chan event.GenericEvent, 32)
	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), BackstopEvents: events}

	done := make(chan error, 1)
	go func() { done <- r.RunBackstop(ctx, 20*time.Millisecond) }()

	deadline := time.After(2 * time.Second)
	seen := 0
	for seen < 2 {
		select {
		case <-events:
			seen++
		case <-deadline:
			t.Fatalf("RunBackstop pushed only %d events in 2s, want >= 2 (Start pass + at least one tick)", seen)
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunBackstop returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RunBackstop did not return after ctx cancel")
	}
}

// TestDispatcherRunBackstop_ReturnsOnCtxCancel mirrors
// TestRunMaintenance_ReturnsOnCtxCancel: a pre-cancelled context must return
// immediately with no goroutine/ticker leak, even before the Start-time pass
// completes.
func TestDispatcherRunBackstop_ReturnsOnCtxCancel(t *testing.T) {
	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { done <- r.RunBackstop(ctx, time.Minute) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunBackstop returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RunBackstop did not return on cancelled context")
	}
}

// TestDispatcherReconciler_SetupWithManager_WithBackstopChannel asserts the
// production wiring path - a non-nil BackstopEvents channel passed into
// SetupWithManager - registers cleanly via WatchesRawSource(source.Channel).
func TestDispatcherReconciler_SetupWithManager_WithBackstopChannel(t *testing.T) {
	mgr := newTestManager(t)
	ch := make(chan event.GenericEvent, 1)
	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), BackstopEvents: ch}
	if err := r.SetupWithManager(mgr); err != nil && !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("SetupWithManager: %v", err)
	}
}
