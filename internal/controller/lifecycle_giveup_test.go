package controller

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

func TestSetDeployState_ImplementGiveUps(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	reg := prometheus.NewRegistry()
	r := &TaskReconciler{
		Client:           k8sClient,
		Scheme:           k8sClient.Scheme(),
		Metrics:          obs.NewOperatorMetrics(reg),
		LifecycleMetrics: obs.NewLifecycleMetrics(reg),
	}

	mkSecret(t, "gu-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "gu-proj", Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{ScmSecretRef: "gu-scm"},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "gu-task", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "gu-proj", Goal: "g", Kind: "issueLifecycle",
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task: %v", err)
	}

	// First give-up: Implement -> Parked with recoverable reason.
	if err := r.setDeployState(ctx, task, "Implement", "initial"); err != nil {
		t.Fatalf("setDeployState Implement: %v", err)
	}
	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "gu-task"}, got); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if err := r.setDeployState(ctx, got, "Parked", "implement-failed"); err != nil {
		t.Fatalf("setDeployState Parked: %v", err)
	}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "gu-task"}, got); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.Status.ImplementGiveUps != 1 {
		t.Errorf("ImplementGiveUps = %d after first give-up, want 1", got.Status.ImplementGiveUps)
	}

	// Second give-up: re-enter Implement then Parked again with recoverable reason.
	if err := r.setDeployState(ctx, got, "Implement", "triage-implement"); err != nil {
		t.Fatalf("setDeployState Implement 2: %v", err)
	}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "gu-task"}, got); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if err := r.setDeployState(ctx, got, "Parked", "maxIterations"); err != nil {
		t.Fatalf("setDeployState Parked 2: %v", err)
	}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "gu-task"}, got); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.Status.ImplementGiveUps != 2 {
		t.Errorf("ImplementGiveUps = %d after second give-up, want 2", got.Status.ImplementGiveUps)
	}

	// Non-recoverable reason: counter must NOT increment.
	if err := r.setDeployState(ctx, got, "Implement", "triage-implement"); err != nil {
		t.Fatalf("setDeployState Implement 3: %v", err)
	}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "gu-task"}, got); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if err := r.setDeployState(ctx, got, "Parked", "refused-declined"); err != nil {
		t.Fatalf("setDeployState Parked non-recoverable: %v", err)
	}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "gu-task"}, got); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.Status.ImplementGiveUps != 2 {
		t.Errorf("ImplementGiveUps = %d after non-recoverable park, want 2 (unchanged)", got.Status.ImplementGiveUps)
	}

	// Transition NOT from Implement: counter must NOT increment.
	if err := r.setDeployState(ctx, got, "Triage", ""); err != nil {
		t.Fatalf("setDeployState Triage: %v", err)
	}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "gu-task"}, got); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if err := r.setDeployState(ctx, got, "Parked", "implement-failed"); err != nil {
		t.Fatalf("setDeployState Parked from Triage: %v", err)
	}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "gu-task"}, got); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.Status.ImplementGiveUps != 2 {
		t.Errorf("ImplementGiveUps = %d after park from Triage, want 2 (unchanged)", got.Status.ImplementGiveUps)
	}
}
