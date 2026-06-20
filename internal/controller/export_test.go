package controller

import (
	"context"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// mustCreate creates obj in k8sClient and registers a cleanup to delete it.
func mustCreate(t *testing.T, ctx context.Context, obj client.Object) {
	t.Helper()
	if err := k8sClient.Create(ctx, obj); err != nil {
		t.Fatalf("mustCreate %T: %v", obj, err)
	}
	t.Cleanup(func() {
		_ = k8sClient.Delete(context.Background(), obj)
	})
}

// mustStatusUpdate updates the status subresource of obj in k8sClient.
func mustStatusUpdate(t *testing.T, ctx context.Context, obj client.Object) {
	t.Helper()
	if err := k8sClient.Status().Update(ctx, obj); err != nil {
		t.Fatalf("mustStatusUpdate %T/%s: %v", obj, obj.GetName(), err)
	}
}

// listQEsTasks lists all QueuedEvents and Tasks for a given projectRef in the tatara namespace.
func listQEsTasks(t *testing.T, ctx context.Context, projectRef string) ([]tatarav1alpha1.QueuedEvent, []tatarav1alpha1.Task) {
	t.Helper()
	var qel tatarav1alpha1.QueuedEventList
	if err := k8sClient.List(ctx, &qel, client.InNamespace(testNS)); err != nil {
		t.Fatalf("listQEsTasks: list QEs: %v", err)
	}
	var tl tatarav1alpha1.TaskList
	if err := k8sClient.List(ctx, &tl, client.InNamespace(testNS)); err != nil {
		t.Fatalf("listQEsTasks: list Tasks: %v", err)
	}
	// filter by projectRef
	var qes []tatarav1alpha1.QueuedEvent
	for _, q := range qel.Items {
		if q.Spec.ProjectRef == projectRef {
			qes = append(qes, q)
		}
	}
	var tasks []tatarav1alpha1.Task
	for _, task := range tl.Items {
		if task.Spec.ProjectRef == projectRef {
			tasks = append(tasks, task)
		}
	}
	return qes, tasks
}

// refreshQE fetches the latest version of a QueuedEvent by name/namespace.
func refreshQE(t *testing.T, ctx context.Context, q *tatarav1alpha1.QueuedEvent) *tatarav1alpha1.QueuedEvent {
	t.Helper()
	got := &tatarav1alpha1.QueuedEvent{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: q.Name, Namespace: q.Namespace}, got); err != nil {
		t.Fatalf("refreshQE %s: %v", q.Name, err)
	}
	return got
}

// reqFor returns a ctrl.Request for the given object.
func reqFor(obj client.Object) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Name: obj.GetName(), Namespace: obj.GetNamespace()}}
}

// taskForQE fetches the Task referenced by a QueuedEvent via its Status.TaskRef.
func taskForQE(t *testing.T, ctx context.Context, q *tatarav1alpha1.QueuedEvent) *tatarav1alpha1.Task {
	t.Helper()
	if q.Status.TaskRef == "" {
		t.Fatalf("taskForQE %s: TaskRef is empty (not admitted)", q.Name)
	}
	task := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: q.Status.TaskRef, Namespace: q.Namespace}, task); err != nil {
		t.Fatalf("taskForQE %s (taskRef=%s): %v", q.Name, q.Status.TaskRef, err)
	}
	return task
}
