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
			Kind:          "implement",
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task: %v", err)
	}
	task.Status.State = tatarav1alpha1.StateUnderImplementation
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
// (TaskReconciler's, StageDriver's, the doc batch's) is accounted. The
// `outcome` label is the terminal STATE, matching D1's vocabulary: done or
// rejected, the only two tatarav1alpha1.TaskIsTerminalOutcome members.
//
// #521 retired `failed` as an Enter target: turn-budget-exhausted (and every
// other former `failed` reason) is a PARK now, applied by ParkTask, which
// never calls emitTerminalTokens - a park is not a terminal OUTCOME, the Task
// may still resume and accumulate more tokens, and double/premature-counting
// them at a stall that might not be the last one is exactly what the terminal
// counter must not do. The positive case below moves to `rejected`, a genuine
// TaskIsTerminalOutcome member, and a new negative case pins that a park -
// the modern shape of the old `failed` scenario - still does not emit.
func TestEnterStage_EmitsTerminalTokens(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	t.Run("rejected", func(t *testing.T) {
		task := seedTerminalTokensTask(t, "tt-task-churn", "tt-proj-churn", "tt-repo-churn", "claude-sonnet-5", 1000, 300, 500, 50)

		reg := prometheus.NewRegistry()
		m := obs.NewOperatorMetrics(reg)
		if err := EnterStage(ctx, k8sClient, nil, m, task, nil,
			tatarav1alpha1.StateRejected, stage.ReasonDeclined, time.Now(), nil); err != nil {
			t.Fatalf("EnterStage: %v", err)
		}

		const proj, repo, model = "tt-proj-churn", "tt-repo-churn", "claude-sonnet-5"
		for _, tc := range []struct {
			class string
			want  float64
		}{{"input", 1000}, {"output", 300}, {"cache_read", 500}, {"cache_creation", 50}} {
			if got := testutil.ToFloat64(m.TaskTerminalTokensCounter(proj, repo, tatarav1alpha1.StateRejected, model, tc.class)); got != tc.want {
				t.Errorf("rejected %s = %v, want %v", tc.class, got, tc.want)
			}
		}
	})

	t.Run("no emit on a non-terminal entry", func(t *testing.T) {
		task := seedTerminalTokensTask(t, "tt-task-live", "tt-proj-live", "tt-repo-live", "claude-opus-5", 700, 200, 100, 10)

		reg := prometheus.NewRegistry()
		m := obs.NewOperatorMetrics(reg)
		if err := EnterStage(ctx, k8sClient, nil, m, task, nil,
			tatarav1alpha1.StateAwaitingReview, "", time.Now(), nil); err != nil {
			t.Fatalf("EnterStage: %v", err)
		}
		got := testutil.ToFloat64(m.TaskTerminalTokensCounter("tt-proj-live", "tt-repo-live", tatarav1alpha1.StateAwaitingReview, "claude-opus-5", "input"))
		if got != 0 {
			t.Errorf("a non-terminal stage entry must not emit terminal tokens, got %v", got)
		}
	})

	t.Run("no emit on a park (the modern shape of the old failed scenario)", func(t *testing.T) {
		task := seedTerminalTokensTask(t, "tt-task-parked", "tt-proj-parked", "tt-repo-parked", "claude-opus-5", 900, 400, 200, 20)

		reg := prometheus.NewRegistry()
		m := obs.NewOperatorMetrics(reg)
		if err := ParkTask(ctx, k8sClient, nil, m, task, stage.ReasonTurnBudgetExhausted, time.Now(), nil); err != nil {
			t.Fatalf("ParkTask: %v", err)
		}
		got := testutil.ToFloat64(m.TaskTerminalTokensCounter("tt-proj-parked", "tt-repo-parked", tatarav1alpha1.StateUnderImplementation, "claude-opus-5", "input"))
		if got != 0 {
			t.Errorf("a park must not emit terminal tokens: the Task is not done, it may still resume, got %v", got)
		}
	})
}
