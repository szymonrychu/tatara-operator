// Copyright 2026 tatara authors.

package controller

// M3 Task 4 tests: context-guard trips at re-implement transitions.

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// seedMRCITaskWithTokens seeds an MRCI task with project-level context window
// config and a pre-set LastTurnInputTokens.
func seedMRCITaskWithTokens(t *testing.T, suffix string, ctxWindowTokens, thresholdPct int, lastInputTokens int64, prState scm.PRState) (*TaskReconciler, *obs.LifecycleMetrics, string) {
	t.Helper()
	ctx := context.Background()
	name := "lc-m3-mrci-" + suffix
	proj := "lc-m3-mrci-p-" + suffix
	repo := "lc-m3-mrci-r-" + suffix
	sec := "lc-m3-mrci-s-" + suffix
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#200",
		URL: "https://github.com/o/r/issues/200", Number: 200,
	}
	mkSecret(t, sec, map[string][]byte{"token": []byte("tok"), "webhookSecret": []byte("wh")})

	scmSpec := &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "bot"}
	p := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: proj, Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: sec,
			Scm:          scmSpec,
			Agent: tatarav1alpha1.AgentSpec{
				Model: "claude-x", Image: "wrapper:1", PermissionMode: "bypassPermissions",
				MaxTurnsPerTask: 50, TurnTimeoutSeconds: 1800,
				ContextWindowTokens:      ctxWindowTokens,
				HandoverThresholdPercent: thresholdPct,
			},
		},
	}
	if err := k8sClient.Create(ctx, p); err != nil {
		t.Fatalf("create project %s: %v", proj, err)
	}
	p.Status.Memory = stableMemStatus("http://mem.svc:8080")
	if err := k8sClient.Status().Update(ctx, p); err != nil {
		t.Fatalf("set memory ready: %v", err)
	}

	r := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: repo, Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef: proj, URL: "https://github.com/o/r.git",
			DefaultBranch: "main", ReingestSchedule: "0 6 * * *",
		},
	}
	if err := k8sClient.Create(ctx, r); err != nil {
		t.Fatalf("create repo %s: %v", repo, err)
	}

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    proj,
			RepositoryRef: repo,
			Goal:          "Fix login bug",
			Kind:          "issueLifecycle",
			Source:        src,
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task %s: %v", name, err)
	}
	task.Status.LifecycleState = "MRCI"
	task.Status.PRNumber = 42
	task.Status.PrURL = "https://github.com/o/r/pull/42"
	task.Status.HeadBranch = "tatara/task-" + name
	task.Status.LastTurnInputTokens = lastInputTokens
	task.Status.ResultSummary = "Implemented login fix"
	dl := metav1.NewTime(time.Now().Add(time.Hour))
	task.Status.DeadlineAt = &dl
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed task: %v", err)
	}

	fw := &lifecycleFakeSCMWriterMRCI{prState: prState}
	reg := prometheus.NewRegistry()
	lm := obs.NewLifecycleMetrics(reg)
	reconciler := &TaskReconciler{
		Client:           k8sClient,
		Scheme:           k8sClient.Scheme(),
		Metrics:          obs.NewOperatorMetrics(reg),
		LifecycleMetrics: lm,
		Session:          newFakeSession(),
		PodConfig: agent.PodConfig{
			Namespace:           testNS,
			CallbackURL:         "http://op-internal.tatara.svc:8082",
			OIDCIssuer:          "https://keycloak.tatara.svc/realms/master",
			AnthropicSecretName: "anthropic",
			CLIOIDCSecretName:   "tatara-cli-oidc",
		},
	}
	reconciler.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }
	return reconciler, lm, name
}

// TestContextGuard_HeavyContext_MRCIFailure_SetsResumeMarker verifies that a
// MRCI failure with high LastTurnInputTokens (above threshold) sets the
// pending-handover-resume annotation, populates Status.Handover, and increments
// the handover metric.
func TestContextGuard_HeavyContext_MRCIFailure_SetsResumeMarker(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	// 200000 token window, 50% threshold -> trip at >= 100000.
	// LastTurnInputTokens = 150000 (75%) -> should trip.
	r, lm, name := seedMRCITaskWithTokens(t, "heavy", 200000, 50, 150000,
		scm.PRState{Author: "bot", CIStatus: "failure"})

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	got := fetchTask(t, name)
	if got.Status.LifecycleState != "Implement" {
		t.Errorf("LifecycleState = %q, want Implement", got.Status.LifecycleState)
	}
	if got.Annotations[annPendingHandoverResume] != "true" {
		t.Errorf("annotation %q = %q, want 'true'", annPendingHandoverResume, got.Annotations[annPendingHandoverResume])
	}
	if got.Status.Handover == "" {
		t.Error("Status.Handover must be set when context guard trips")
	}
	if v := testutil.ToFloat64(lm.HandoverTotal()); v != 1 {
		t.Errorf("handover metric = %v, want 1", v)
	}
}

// TestContextGuard_LightContext_MRCIFailure_NoMarker verifies that a light-
// context MRCI failure (below threshold) does NOT set the resume annotation.
func TestContextGuard_LightContext_MRCIFailure_NoMarker(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	// LastTurnInputTokens = 10000 (5% of 200000) -> below 50% threshold, no trip.
	r, _, name := seedMRCITaskWithTokens(t, "light", 200000, 50, 10000,
		scm.PRState{Author: "bot", CIStatus: "failure"})

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	got := fetchTask(t, name)
	if got.Annotations[annPendingHandoverResume] != "" {
		t.Errorf("annotation %q = %q, want empty (no context guard trip)",
			annPendingHandoverResume, got.Annotations[annPendingHandoverResume])
	}
}

// TestContextGuard_DefaultThresholdIs25 verifies the issue #114 decision-2
// default: with HandoverThresholdPercent unset (0 -> defaulted to 25), a 30%
// context trips compaction (it would NOT have under the old 50% default).
func TestContextGuard_DefaultThresholdIs25(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	// thresholdPct=0 -> CRD/in-code default 25. LastTurnInputTokens = 60000
	// (30% of 200000) -> over 25%, under the old 50%, so it must trip now.
	r, _, name := seedMRCITaskWithTokens(t, "default25", 200000, 0, 60000,
		scm.PRState{Author: "bot", CIStatus: "failure"})

	if _, err := r.reconcileLifecycle(ctx, fetchTask(t, name)); err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	got := fetchTask(t, name)
	if got.Annotations[annPendingHandoverResume] != "true" {
		t.Errorf("annotation %q = %q, want 'true' (30%% over the 25%% default)",
			annPendingHandoverResume, got.Annotations[annPendingHandoverResume])
	}
}
