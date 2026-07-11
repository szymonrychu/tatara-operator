package controller

import (
	"context"
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/budget"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func qe(name, class, state, taskRef string) tatarav1alpha1.QueuedEvent {
	return tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"},
		Spec:       tatarav1alpha1.QueuedEventSpec{Class: class, ProjectRef: "p"},
		Status:     tatarav1alpha1.QueuedEventStatus{State: state, TaskRef: taskRef},
	}
}

func tk(name, phase, lifecycle, queuedEvent string) tatarav1alpha1.Task {
	return tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara", Labels: map[string]string{queue.LabelQueuedEvent: queuedEvent}},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "p"},
		Status:     tatarav1alpha1.TaskStatus{Phase: phase, DeployState: lifecycle},
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

func TestPoolInflight_CountsUnlabelledPreQueueTasks(t *testing.T) {
	r := &DispatcherReconciler{}
	var qes []tatarav1alpha1.QueuedEvent
	tasks := []tatarav1alpha1.Task{
		preQueueTask("old-normal", "Running", "review", ""),     // no queued-event label -> normal pool
		preQueueTask("old-incident", "Running", "incident", ""), // -> alert pool
		preQueueTask("old-done", "Succeeded", "review", ""),     // terminal -> not counted
	}
	if got := r.poolInflight(qes, tasks, tatarav1alpha1.QueueClassNormal); got != 1 {
		t.Fatalf("normal pre-queue inflight = %d, want 1", got)
	}
	if got := r.poolInflight(qes, tasks, tatarav1alpha1.QueueClassAlert); got != 1 {
		t.Fatalf("alert pre-queue inflight = %d, want 1", got)
	}
}

// preQueueTask: a Task with NO LabelQueuedEvent (created before the queue existed).
func preQueueTask(name, phase, kind, _ string) tatarav1alpha1.Task {
	return tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara"}, // no labels
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "p", Kind: kind},
		Status:     tatarav1alpha1.TaskStatus{Phase: phase},
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
	task.Status.Phase = "Succeeded"
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
	staleTask.Status.Phase = "Succeeded"
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
		t.Fatalf("stale terminal Task should be deleted after first Reconcile, got err=%v phase=%q", err, staleGot.Status.Phase)
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
	if tatarav1alpha1.TaskTerminal(freshTask) {
		t.Fatalf("fresh Task must be non-terminal, got phase=%q ls=%q", freshTask.Status.Phase, freshTask.Status.DeployState)
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
	staleTask.Status.Phase = "Succeeded"
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
	staleTask.Status.Phase = "Succeeded"
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

func TestPoolInflight_CountsAdmittedNonTerminal(t *testing.T) {
	r := &DispatcherReconciler{}
	qes := []tatarav1alpha1.QueuedEvent{
		qe("a", tatarav1alpha1.QueueClassNormal, tatarav1alpha1.QueueStateAdmitted, "t-a"), // running -> counts
		qe("b", tatarav1alpha1.QueueClassNormal, tatarav1alpha1.QueueStateAdmitted, "t-b"), // terminal -> not
		qe("c", tatarav1alpha1.QueueClassAlert, tatarav1alpha1.QueueStateAdmitted, "t-c"),  // alert running
		qe("d", tatarav1alpha1.QueueClassNormal, tatarav1alpha1.QueueStateQueued, ""),      // queued -> not
	}
	tasks := []tatarav1alpha1.Task{
		tk("t-a", "Running", "", "a"),
		tk("t-b", "Succeeded", "", "b"),
		tk("t-c", "Running", "", "c"),
	}
	if got := r.poolInflight(qes, tasks, tatarav1alpha1.QueueClassNormal); got != 1 {
		t.Fatalf("normal inflight = %d, want 1", got)
	}
	if got := r.poolInflight(qes, tasks, tatarav1alpha1.QueueClassAlert); got != 1 {
		t.Fatalf("alert inflight = %d, want 1", got)
	}
}
