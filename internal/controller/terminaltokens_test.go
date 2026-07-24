// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// seedTerminalTokensTask creates a project+repo+task with the given resolved
// model + cumulative token classes, parked in a non-terminal stage. Distinct
// names per case to avoid the shared-envtest-namespace lesson.
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
			Kind:          "clarify",
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task: %v", err)
	}
	task.Status.Stage = tatarav1alpha1.StageImplementing
	task.Status.ResolvedModel = model
	task.Status.Stats.TokensInput = in
	task.Status.Stats.TokensOutput = out
	task.Status.Stats.TokensCacheRead = cr
	task.Status.Stats.TokensCacheCreation = cc
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed task status: %v", err)
	}
	return task
}

// TestEnterStage_EmitsTerminalTokens guards operator_task_terminal_tokens_total.
// Its only emitter used to be the retired machine's setDeployState; it now fires
// from EnterStage, the single transition choke point, so EVERY terminal entry
// (TaskReconciler's, StageDriver's, the doc batch's) is accounted. The `outcome`
// label is the terminal STAGE, matching D1's stage vocabulary.
func TestEnterStage_EmitsTerminalTokens(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	t.Run("failed", func(t *testing.T) {
		task := seedTerminalTokensTask(t, "tt-task-churn", "tt-proj-churn", "tt-repo-churn", "claude-sonnet-5", 1000, 300, 500, 50)

		reg := prometheus.NewRegistry()
		m := obs.NewOperatorMetrics(reg)
		if err := EnterStage(ctx, k8sClient, nil, m, task, nil,
			tatarav1alpha1.StageFailed, stage.ReasonTurnBudgetExhausted, time.Now(), nil); err != nil {
			t.Fatalf("EnterStage: %v", err)
		}

		const proj, repo, model = "tt-proj-churn", "tt-repo-churn", "claude-sonnet-5"
		for _, tc := range []struct {
			class string
			want  float64
		}{{"input", 1000}, {"output", 300}, {"cache_read", 500}, {"cache_creation", 50}} {
			if got := testutil.ToFloat64(m.TaskTerminalTokensCounter(proj, repo, tatarav1alpha1.StageFailed, model, tc.class)); got != tc.want {
				t.Errorf("failed %s = %v, want %v", tc.class, got, tc.want)
			}
		}
	})

	t.Run("no emit on a non-terminal entry", func(t *testing.T) {
		task := seedTerminalTokensTask(t, "tt-task-live", "tt-proj-live", "tt-repo-live", "claude-opus-5", 700, 200, 100, 10)

		reg := prometheus.NewRegistry()
		m := obs.NewOperatorMetrics(reg)
		if err := EnterStage(ctx, k8sClient, nil, m, task, nil,
			tatarav1alpha1.StageReviewing, "", time.Now(), nil); err != nil {
			t.Fatalf("EnterStage: %v", err)
		}
		got := testutil.ToFloat64(m.TaskTerminalTokensCounter("tt-proj-live", "tt-repo-live", tatarav1alpha1.StageReviewing, "claude-opus-5", "input"))
		if got != 0 {
			t.Errorf("a non-terminal stage entry must not emit terminal tokens, got %v", got)
		}
	})
}
