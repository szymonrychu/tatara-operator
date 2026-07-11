// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// seedTerminalTokensTask creates a project+repo+task with the given
// resolved model + cumulative token classes, entering the given lifecycle
// state. Distinct names per case to avoid the shared-envtest-namespace lesson.
func seedTerminalTokensTask(t *testing.T, name, project, repo, model string, in, out, cr, cc int64) *tatarav1alpha1.Task {
	t.Helper()
	ctx := context.Background()

	mkSecret(t, project+"-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})

	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: project, Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{ScmSecretRef: project + "-scm"},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}

	repoObj := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: repo, Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef:       project,
			URL:              "https://github.com/o/r.git",
			DefaultBranch:    "main",
			ReingestSchedule: "0 6 * * *",
		},
	}
	if err := k8sClient.Create(ctx, repoObj); err != nil {
		t.Fatalf("create repo: %v", err)
	}

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    project,
			RepositoryRef: repo,
			Goal:          "test terminal tokens",
			Kind:          "issueLifecycle",
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task: %v", err)
	}
	task.Status.DeployState = "Implement"
	task.Status.ResolvedModel = model
	task.Status.CumulativeInput = in
	task.Status.CumulativeOutput = out
	task.Status.CumulativeCacheRead = cr
	task.Status.CumulativeCacheCreation = cc
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed task status: %v", err)
	}
	return task
}

func TestSetDeployState_EmitsTerminalTokens(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	t.Run("Parked churned", func(t *testing.T) {
		task := seedTerminalTokensTask(t, "tt-task-churn", "tt-proj-churn", "tt-repo-churn", "claude-sonnet-5", 1000, 300, 500, 50)

		reg := prometheus.NewRegistry()
		r := &TaskReconciler{
			Client:  k8sClient,
			Scheme:  k8sClient.Scheme(),
			Metrics: obs.NewOperatorMetrics(reg),
		}
		if err := r.setDeployState(ctx, task, "Parked", "implement-failed"); err != nil {
			t.Fatalf("setDeployState: %v", err)
		}

		const proj, repo, model = "tt-proj-churn", "tt-repo-churn", "claude-sonnet-5"
		if got := testutil.ToFloat64(r.Metrics.TaskTerminalTokensCounter(proj, repo, "churned", model, "input")); got != 1000 {
			t.Errorf("churned input = %v, want 1000", got)
		}
		if got := testutil.ToFloat64(r.Metrics.TaskTerminalTokensCounter(proj, repo, "churned", model, "output")); got != 300 {
			t.Errorf("churned output = %v, want 300", got)
		}
		if got := testutil.ToFloat64(r.Metrics.TaskTerminalTokensCounter(proj, repo, "churned", model, "cache_read")); got != 500 {
			t.Errorf("churned cache_read = %v, want 500", got)
		}
		if got := testutil.ToFloat64(r.Metrics.TaskTerminalTokensCounter(proj, repo, "churned", model, "cache_creation")); got != 50 {
			t.Errorf("churned cache_creation = %v, want 50", got)
		}
	})

	t.Run("Done delivered", func(t *testing.T) {
		task := seedTerminalTokensTask(t, "tt-task-done", "tt-proj-done", "tt-repo-done", "claude-opus-4-8", 700, 200, 100, 10)

		reg := prometheus.NewRegistry()
		r := &TaskReconciler{
			Client:  k8sClient,
			Scheme:  k8sClient.Scheme(),
			Metrics: obs.NewOperatorMetrics(reg),
		}
		if err := r.setDeployState(ctx, task, "Done", ""); err != nil {
			t.Fatalf("setDeployState: %v", err)
		}

		const proj, repo, model = "tt-proj-done", "tt-repo-done", "claude-opus-4-8"
		if got := testutil.ToFloat64(r.Metrics.TaskTerminalTokensCounter(proj, repo, "delivered", model, "input")); got != 700 {
			t.Errorf("delivered input = %v, want 700", got)
		}
	})
}
