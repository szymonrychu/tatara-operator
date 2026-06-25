package controller

// D1: ledger propagation test - verifies that the refiner's close_issue action
// updates matching Task work-item ledger entries to state:closed.
// Written per plan: run first; implement only if FAIL.

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// TestLedger_RefinerClosedIssueUpdatesWorkItem seeds a Task with a WorkItem
// for issue o/r#42 in state:open, calls markWorkItemsClosed (the helper
// closeProjectIssue invokes after CloseIssue succeeds), and asserts the
// work-item entry is marked closed.
func TestLedger_RefinerClosedIssueUpdatesWorkItem(t *testing.T) {
	ctx := context.Background()

	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "lr-scm", Namespace: testNS},
		Data:       map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")},
	}
	if err := k8sClient.Create(ctx, sec); err != nil {
		t.Fatalf("create secret: %v", err)
	}
	t.Cleanup(func() { k8sClient.Delete(ctx, sec) }) //nolint:errcheck

	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "lr-proj", Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "lr-scm",
		},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}
	t.Cleanup(func() { k8sClient.Delete(ctx, proj) }) //nolint:errcheck

	_ = mkScanRepo(t, "lr-proj", "lr-repo", "https://github.com/o/r.git")

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "lr-task", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			Kind:          "issueLifecycle",
			ProjectRef:    "lr-proj",
			RepositoryRef: "lr-repo",
			Source: &tatarav1alpha1.TaskSource{
				IssueRef: "o/r#42",
				Number:   42,
				Provider: "github",
			},
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task: %v", err)
	}
	t.Cleanup(func() { k8sClient.Delete(ctx, task) }) //nolint:errcheck

	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{
			Provider: "github",
			Kind:     tatarav1alpha1.WorkItemIssue,
			Repo:     "o/r",
			Number:   42,
			Role:     "source",
			State:    "open",
		},
	}
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed work items: %v", err)
	}

	// Call markWorkItemsClosed - the function closeProjectIssue will invoke.
	r := newProjectReconciler()
	if err := r.markWorkItemsClosed(ctx, proj.Namespace, "o/r", 42); err != nil {
		t.Fatalf("markWorkItemsClosed: %v", err)
	}

	// Verify the work-item is now closed.
	var fresh tatarav1alpha1.Task
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "lr-task"}, &fresh); err != nil {
		t.Fatalf("get fresh task: %v", err)
	}
	found := false
	for _, wi := range fresh.Status.WorkItems {
		if wi.Repo == "o/r" && wi.Number == 42 {
			found = true
			if wi.State != "closed" {
				t.Fatalf("work item o/r#42 state = %q, want closed", wi.State)
			}
		}
	}
	if !found {
		t.Fatalf("work item o/r#42 not found in ledger after markWorkItemsClosed")
	}
}
