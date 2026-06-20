package controller

import (
	"context"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tatara", Labels: map[string]string{LabelQueuedEvent: queuedEvent}},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "p"},
		Status:     tatarav1alpha1.TaskStatus{Phase: phase, LifecycleState: lifecycle},
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
	if err := r.admit(ctx, proj, qes, tasks); err != nil {
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
	if err := r.admit(ctx, proj, qes, tasks); err != nil {
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
	if err := r.admit(ctx, proj, qes, tasks); err != nil {
		t.Fatalf("second admit: %v", err)
	}

	// Exactly one Task with the QueuedEvent label.
	var tl tatarav1alpha1.TaskList
	if err := k8sClient.List(ctx, &tl, client.InNamespace(ns), client.MatchingLabels{LabelQueuedEvent: qe.Name}); err != nil {
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
