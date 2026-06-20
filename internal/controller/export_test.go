package controller

import (
	"context"
	"testing"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// newTestScheme builds a *runtime.Scheme with core + tatara + cnpg types
// registered, suitable for use with the controller-runtime fake client.
func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("newTestScheme: clientgo: %v", err)
	}
	if err := tatarav1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("newTestScheme: tatarav1alpha1: %v", err)
	}
	if err := cnpgv1.AddToScheme(s); err != nil {
		t.Fatalf("newTestScheme: cnpgv1: %v", err)
	}
	return s
}

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
