package controller

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// TestScanTaskLabels_NoSourceDedupeLabels verifies that scanTaskLabels no
// longer writes the three dedup labels (source-repo, source-number, head-sha).
// Kind/activity/is-pr labels are still written.
func TestScanTaskLabels_NoSourceDedupeLabels(t *testing.T) {
	got := scanTaskLabels(candidate{repo: "o/r", number: 5, headSHA: "abc"}, "mrScan", "review")
	// Use string literals: the consts are deleted in Phase 2 Task 9.
	for _, badKey := range []string{
		"tatara.io/source-repo",
		"tatara.io/source-number",
		"tatara.io/head-sha",
	} {
		if _, ok := got[badKey]; ok {
			t.Errorf("scanTaskLabels must not write %q any more; got %+v", badKey, got)
		}
	}
	// Must still carry kind + activity.
	if got[tatarav1alpha1.LabelSourceKind] != "review" {
		t.Errorf("LabelSourceKind missing or wrong: %+v", got)
	}
	if got[tatarav1alpha1.LabelActivity] != "mrScan" {
		t.Errorf("LabelActivity missing or wrong: %+v", got)
	}
}

// TestReconcile_SeedsLedgerFromSpec verifies that after one reconcile a Task
// with a populated Spec.Source has Status.WorkItems seeded from it.
func TestReconcile_SeedsLedgerFromSpec(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	reg := prometheus.NewRegistry()
	r := &TaskReconciler{
		Client:           k8sClient,
		Scheme:           k8sClient.Scheme(),
		Metrics:          obs.NewOperatorMetrics(reg),
		LifecycleMetrics: obs.NewLifecycleMetrics(reg),
		Session:          newFakeSession(),
	}

	mkSecret(t, "seed-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "seed-proj", Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{ScmSecretRef: "seed-scm"},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}
	proj.Status.Memory = &tatarav1alpha1.MemoryStatus{Phase: "Ready", Endpoint: "http://mem.svc:8080"}
	if err := k8sClient.Status().Update(ctx, proj); err != nil {
		t.Fatalf("set memory ready: %v", err)
	}

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "seed-task", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "seed-proj", Goal: "g", Kind: "issueLifecycle",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#5", Number: 5, IsPR: false,
			},
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task: %v", err)
	}

	// Trigger one reconcile.
	_, _ = r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: "seed-task"}})

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "seed-task"}, got); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if len(got.Status.WorkItems) == 0 {
		t.Fatal("expected Status.WorkItems to be seeded after first reconcile; got empty")
	}
	if got.Status.WorkItems[0].Repo != "o/r" || got.Status.WorkItems[0].Number != 5 {
		t.Errorf("unexpected WorkItem: %+v", got.Status.WorkItems[0])
	}
}
