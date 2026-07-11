// Copyright 2026 tatara authors.

package controller

// Tests for issue #268: triageCloseIssue must treat a permanently-gone source
// issue (410 deleted / 404 not found) as terminal - skip the close without
// requeue and record a distinct result="gone" metric instead of "error", so a
// single deleted issue no longer retry-loops and inflates the SCM
// write-failure-ratio alert. Mirrors the AddLabel guard from #263.

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// closeGoneWriter returns a configurable error from CloseIssue; AddLabel and the
// rest come from the embedded fake (all succeed).
type closeGoneWriter struct {
	lifecycleFakeSCMWriter
	closeErr error
}

func (w *closeGoneWriter) CloseIssue(_ context.Context, _, _ string, _ int, _ string) error {
	return w.closeErr
}

func TestTriageCloseIssue_TargetGone_TerminalNoRequeue(t *testing.T) {
	tests := []struct {
		name       string
		closeErr   error
		wantErr    bool   // reconcile returns error -> controller-runtime requeues
		wantResult string // result label the close write must be counted under
	}{
		{"410 gone -> terminal", &scm.HTTPError{Status: 410, Path: "/repos/o/r/issues/7/comments", Body: `{"message":"This issue was deleted"}`}, false, "gone"},
		{"404 not found -> terminal", &scm.HTTPError{Status: 404, Path: "/repos/o/r/issues/7/comments"}, false, "gone"},
		{"500 server error -> retryable", &scm.HTTPError{Status: 500, Path: "/repos/o/r/issues/7/comments"}, true, "error"},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := logf.IntoContext(context.Background(), logf.Log)

			name := fmtName("cgone", i)
			src := &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#700",
				URL: "https://github.com/o/r/issues/700", Number: 700,
			}
			task := seedLifecycleTask(t, name, name+"-p", name+"-r", name+"-s", src)
			task.Status.DeployState = "Triage"
			task.Status.Phase = "Succeeded"
			task.Status.IssueOutcome = &tatarav1alpha1.IssueOutcome{Action: "close", Comment: "closing this"}
			if err := k8sClient.Status().Update(context.Background(), task); err != nil {
				t.Fatalf("seed: %v", err)
			}

			reg := prometheus.NewRegistry()
			om := obs.NewOperatorMetrics(reg)
			lm := obs.NewLifecycleMetrics(reg)
			fw := &closeGoneWriter{closeErr: tc.closeErr}
			r := &TaskReconciler{
				Client:           k8sClient,
				Scheme:           k8sClient.Scheme(),
				Metrics:          om,
				LifecycleMetrics: lm,
			}
			r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

			_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error (requeue) for a transient close failure, got nil")
				}
			} else {
				if err != nil {
					t.Fatalf("expected nil error (no requeue) for a permanently-gone target, got %v", err)
				}
				if got := fetchTask(t, name); got.Status.DeployState != "Done" {
					t.Errorf("DeployState = %q, want Done after terminal close", got.Status.DeployState)
				}
			}

			// The close write is counted under the expected result and NOT double-counted.
			if v := testutil.ToFloat64(om.SCMWriteCounter("github", "close_issue", tc.wantResult)); v != 1 {
				t.Errorf("close_issue{result=%q} = %v, want 1", tc.wantResult, v)
			}
			// A gone close must not also count as an error (the whole point of #268).
			if tc.wantResult == "gone" {
				if v := testutil.ToFloat64(om.SCMWriteCounter("github", "close_issue", "error")); v != 0 {
					t.Errorf("close_issue{result=error} = %v, want 0 for a gone target", v)
				}
			}
		})
	}
}

func fmtName(prefix string, i int) string {
	return prefix + "-" + string(rune('a'+i))
}
