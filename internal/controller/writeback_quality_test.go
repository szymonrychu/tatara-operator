package controller

// Tests for Task 2 of the quality-feedback-loop plan: writeBackReview must
// record operator_review_outcome_total + operator_review_findings_total
// (G4) after a successful Approve/RequestChanges write-back.

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func TestWriteBackReview_RecordsApprovedOutcome(t *testing.T) {
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "wbq-rev-approve", "wbq-proj-a", "wbq-repo-a", "wbq-scm-a",
		tatarav1alpha1.TaskSpec{
			Goal: "review a PR",
			Kind: "review",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#20", IsPR: true, Number: 20,
			},
		}, nil)
	task.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{Decision: "approve", Body: "lgtm"}
	task.Status.ResolvedModel = "claude-sonnet-5"
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	require.True(t, fw.approveCalled, "Approve must be called")
	got := testutil.ToFloat64(r.Metrics.ReviewOutcomeCounter("wbq-proj-a", "wbq-repo-a", "claude-sonnet-5", "approved"))
	require.Equal(t, float64(1), got, "operator_review_outcome_total{verdict=approved}")
}

func TestWriteBackReview_RecordsChangesRequestedOutcomeAndFindings(t *testing.T) {
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "wbq-rev-rc", "wbq-proj-rc", "wbq-repo-rc", "wbq-scm-rc",
		tatarav1alpha1.TaskSpec{
			Goal: "review a PR",
			Kind: "review",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#21", IsPR: true, Number: 21,
			},
		}, nil)
	task.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{
		Decision: "request_changes",
		Body:     "nope",
		Suggestions: []tatarav1alpha1.Suggestion{
			{Path: "a.go", Line: 5, Body: "x := 1"},
			{Path: "b.go", Line: 9, Body: "y := 2"},
			{Path: "c.go", Line: 12, Body: "z := 3"},
		},
	}
	task.Status.ResolvedModel = "claude-sonnet-5"
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	require.True(t, fw.requestChangesCalled, "RequestChanges must be called")
	gotOutcome := testutil.ToFloat64(r.Metrics.ReviewOutcomeCounter("wbq-proj-rc", "wbq-repo-rc", "claude-sonnet-5", "changes_requested"))
	require.Equal(t, float64(1), gotOutcome, "operator_review_outcome_total{verdict=changes_requested}")
	gotFindings := testutil.ToFloat64(r.Metrics.ReviewFindingsCounter("wbq-proj-rc", "wbq-repo-rc", "claude-sonnet-5"))
	require.Equal(t, float64(3), gotFindings, "operator_review_findings_total = len(Suggestions)")
}
