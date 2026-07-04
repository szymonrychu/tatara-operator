package controller

// Tests for Task 3 of the quality-feedback-loop plan: handleMRCI must record
// operator_implement_ci_total (G4) on the terminal CI conclusions
// (success -> "pass", failure -> "fail"), and record nothing on "pending".

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

func TestHandleMRCI_FailureRecordsImplementCIFail(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, _, name := seedMRCITask(t, "ci-fail", scm.PRState{Author: "bot", CIStatus: "failure"}, 0)

	task := fetchTask(t, name)
	task.Status.ResolvedModel = "claude-opus-4-8"
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("set ResolvedModel: %v", err)
	}

	if _, err := r.reconcileLifecycle(ctx, fetchTask(t, name)); err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	got := testutil.ToFloat64(r.Metrics.ImplementCICounter("lc-mrcip-ci-fail", "lc-mrcir-ci-fail", "claude-opus-4-8", "fail"))
	if got != 1 {
		t.Errorf("operator_implement_ci_total{result=fail} = %v, want 1", got)
	}
}

func TestHandleMRCI_SuccessRecordsImplementCIPass(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, _, name := seedMRCITask(t, "ci-pass", scm.PRState{Author: "bot", CIStatus: "success"}, 0)

	task := fetchTask(t, name)
	task.Status.ResolvedModel = "claude-opus-4-8"
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("set ResolvedModel: %v", err)
	}

	if _, err := r.reconcileLifecycle(ctx, fetchTask(t, name)); err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	got := testutil.ToFloat64(r.Metrics.ImplementCICounter("lc-mrcip-ci-pass", "lc-mrcir-ci-pass", "claude-opus-4-8", "pass"))
	if got != 1 {
		t.Errorf("operator_implement_ci_total{result=pass} = %v, want 1", got)
	}
}

func TestHandleMRCI_PendingRecordsNoImplementCI(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, _, name := seedMRCITask(t, "ci-pending", scm.PRState{Author: "bot", CIStatus: "pending"}, 0)

	task := fetchTask(t, name)
	task.Status.ResolvedModel = "claude-opus-4-8"
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("set ResolvedModel: %v", err)
	}

	if _, err := r.reconcileLifecycle(ctx, fetchTask(t, name)); err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	gotPass := testutil.ToFloat64(r.Metrics.ImplementCICounter("lc-mrcip-ci-pending", "lc-mrcir-ci-pending", "claude-opus-4-8", "pass"))
	gotFail := testutil.ToFloat64(r.Metrics.ImplementCICounter("lc-mrcip-ci-pending", "lc-mrcir-ci-pending", "claude-opus-4-8", "fail"))
	if gotPass != 0 || gotFail != 0 {
		t.Errorf("pending CI must not record implement CI metric, got pass=%v fail=%v", gotPass, gotFail)
	}
}
