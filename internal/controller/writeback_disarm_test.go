package controller

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// seedDisarmTask creates a kind=implement Task with an already-open PR and a
// declared RemainingScope, so reconcile enters checkRemainingScopeHardFail's
// disarm path (F2).
func seedDisarmTask(t *testing.T, name, project, repo, scmSecret string) *tatarav1alpha1.Task {
	t.Helper()
	task := seedWritebackKindTask(t, name, project, repo, scmSecret,
		tatarav1alpha1.TaskSpec{
			Goal: "Implement issue 200",
			Kind: "implement",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#200", URL: "https://github.com/o/r/issues/200", Number: 200,
			},
		}, nil)
	task.Status.PrURL = "https://github.com/o/r/pull/201"
	task.Status.ChangeSummary = &tatarav1alpha1.ChangeSummary{
		PRTitle:        "feat: partial",
		PRBody:         "Partial.",
		DeliveredScope: "half",
		RemainingScope: "the other half",
		Significance:   "minor",
	}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))
	return task
}

// TestDisarm_TransientError_RequeuesWithoutTerminating is the F2 regression:
// a transient SCM error during disarmOpenChanges (ClosePR 500) must NOT let
// the Task terminate - the previous fail-open behavior swallowed every
// disarm error and terminated Failed unconditionally, leaving an armed PR
// open with nothing tracking that the disarm never actually verified.
func TestDisarm_TransientError_RequeuesWithoutTerminating(t *testing.T) {
	fw := &fullFakeSCMWriter{closePRErrs: []error{&scm.HTTPError{Status: 500, Body: "boom"}}}
	r := newFullFakeReconciler(t, fw)
	task := seedDisarmTask(t, "wbk-disarm-transient", "wbk-disarm-tp", "wbk-disarm-tr", "wbk-disarm-ts")

	res, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)
	require.Equal(t, disarmRetryRequeue, res.RequeueAfter, "a dirty disarm sweep under the cap must requeue, not terminate")

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	require.NotEqual(t, "Failed", got.Status.Phase, "the Task must not terminate while the disarm is unverified")
	require.Equal(t, 1, got.Status.DisarmFailures)
	cond := apimeta.FindStatusCondition(got.Status.Conditions, "WritebackPending")
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionTrue, cond.Status, "WritebackPending must stay armed so the next reconcile retries the disarm")
	require.True(t, fw.closePRCalled)
	require.Nil(t, apimeta.FindStatusCondition(got.Status.Conditions, "DisarmFailed"))
}

// TestDisarm_CapExhausted_TerminatesLoudly verifies that after disarmFailureCap
// consecutive dirty sweeps the Task terminates anyway (it cannot retry
// forever) but records a distinct DisarmFailed condition and increments the
// operator_writeback_outcome_total{result="disarm_failed"} counter, so an
// armed-PR-that-could-not-be-disarmed is alertable instead of silently
// dropped.
func TestDisarm_CapExhausted_TerminatesLoudly(t *testing.T) {
	fw := &fullFakeSCMWriter{closePRErrs: []error{&scm.HTTPError{Status: 500, Body: "boom"}}}
	r := newFullFakeReconciler(t, fw)
	task := seedDisarmTask(t, "wbk-disarm-capped", "wbk-disarm-cp", "wbk-disarm-cr", "wbk-disarm-cs")

	require.Zero(t, testutil.ToFloat64(r.Metrics.WritebackOutcomeCounter("disarm_failed")))

	for i := 0; i < disarmFailureCap; i++ {
		_, err := reconcileWriteback(t, r, task.Name)
		require.NoError(t, err)
	}

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	require.Equal(t, "Failed", got.Status.Phase, "the cap must eventually let the Task terminate rather than retry forever")
	readyCond := apimeta.FindStatusCondition(got.Status.Conditions, "Ready")
	require.NotNil(t, readyCond)
	require.Equal(t, "IncompleteImplementation", readyCond.Reason)
	disarmCond := apimeta.FindStatusCondition(got.Status.Conditions, "DisarmFailed")
	require.NotNil(t, disarmCond, "an exhausted disarm budget must record a distinct DisarmFailed condition")
	require.Equal(t, metav1.ConditionTrue, disarmCond.Status)
	require.Equal(t, "DisarmCapReached", disarmCond.Reason)
	require.Equal(t, float64(1), testutil.ToFloat64(r.Metrics.WritebackOutcomeCounter("disarm_failed")),
		"the give-up must be alertable via a counter, not just a log line")
	require.Equal(t, disarmFailureCap, fw.closePRCallCount)
}

// TestDisarm_PermanentAlreadyClosed_TreatedAsSuccess verifies that a permanent
// SCM error (404, the PR is already gone/closed) counts as a clean disarm, not
// a failure: the Task terminates immediately on the first attempt, with no
// DisarmFailed condition and no retry.
func TestDisarm_PermanentAlreadyClosed_TreatedAsSuccess(t *testing.T) {
	fw := &fullFakeSCMWriter{closePRErrs: []error{&scm.HTTPError{Status: 404, Body: "Not Found"}}}
	r := newFullFakeReconciler(t, fw)
	task := seedDisarmTask(t, "wbk-disarm-gone", "wbk-disarm-gp", "wbk-disarm-gr", "wbk-disarm-gs")

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	require.Equal(t, "Failed", got.Status.Phase, "a permanently-gone target must count as disarmed and let the Task terminate on the first attempt")
	readyCond := apimeta.FindStatusCondition(got.Status.Conditions, "Ready")
	require.NotNil(t, readyCond)
	require.Equal(t, "IncompleteImplementation", readyCond.Reason)
	require.Nil(t, apimeta.FindStatusCondition(got.Status.Conditions, "DisarmFailed"))
	require.Equal(t, 0, got.Status.DisarmFailures)
	require.Equal(t, 1, fw.closePRCallCount, "a permanent 404 must not be retried")
}
