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

func TestSetLifecycleState_ParkReasonPersisted(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	reg := prometheus.NewRegistry()
	r := &TaskReconciler{
		Client:           k8sClient,
		Scheme:           k8sClient.Scheme(),
		Metrics:          obs.NewOperatorMetrics(reg),
		LifecycleMetrics: obs.NewLifecycleMetrics(reg),
	}

	mkSecret(t, "pr-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "pr-proj", Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{ScmSecretRef: "pr-scm"},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "pr-task", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "pr-proj", RepositoryRef: "", Goal: "g",
			Kind: "issueLifecycle",
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task: %v", err)
	}

	// Transition to Parked with a reason.
	if err := r.setLifecycleState(ctx, task, "Parked", "triage-failed"); err != nil {
		t.Fatalf("setLifecycleState Parked: %v", err)
	}
	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "pr-task"}, got); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.Status.ParkReason != "triage-failed" {
		t.Errorf("expected ParkReason=triage-failed, got %q", got.Status.ParkReason)
	}
	if got.Status.LifecycleState != "Parked" {
		t.Errorf("expected LifecycleState=Parked, got %q", got.Status.LifecycleState)
	}

	// Transition out of Parked: ParkReason must be cleared.
	if err := r.setLifecycleState(ctx, got, "Implement", "triage-implement"); err != nil {
		t.Fatalf("setLifecycleState Implement: %v", err)
	}
	got2 := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "pr-task"}, got2); err != nil {
		t.Fatalf("get task after Implement: %v", err)
	}
	if got2.Status.ParkReason != "" {
		t.Errorf("expected ParkReason cleared, got %q", got2.Status.ParkReason)
	}
}
