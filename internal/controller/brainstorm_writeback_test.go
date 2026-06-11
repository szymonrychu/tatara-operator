package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// TestDoWriteBackBrainstormDoesNotOpenPR verifies that a brainstorm Task does
// not invoke OpenChange (which would 422 because there is no task branch to PR
// from) and instead clears WritebackPending cleanly.
func TestDoWriteBackBrainstormDoesNotOpenPR(t *testing.T) {
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "bswb-task", "bswb-proj", "bswb-repo", "bswb-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "brainstorm new issues",
			Kind: "brainstorm",
		}, nil)

	// No IssueOutcome set - brainstorm tasks only call propose_issue to open
	// child tasks; the brainstorm task itself must not open any PR.
	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	// OpenChange must NOT be invoked.
	require.Zero(t, fw.openCalls, "brainstorm Task must not call OpenChange")

	// WritebackPending must be cleared.
	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	cond := findCond(got.Status.Conditions, "WritebackPending")
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status)
	require.Equal(t, "BrainstormProposed", cond.Reason)
}

// seedBrainstormWithPendingWriteback seeds a brainstorm Task in WritebackPending
// but with no prURL, verifying idempotency guard doesn't short-circuit the fix.
func TestDoWriteBackBrainstorm_AlreadyDone(t *testing.T) {
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "bswb-task2", "bswb-proj2", "bswb-repo2", "bswb-scm2",
		tatarav1alpha1.TaskSpec{
			Goal: "brainstorm ideas",
			Kind: "brainstorm",
		}, nil)
	// Set prURL so the idempotency guard fires; the brainstorm case must also
	// clear pending in that branch (the idempotency guard clears it).
	task.Status.PrURL = "https://example/pr/1"
	apimeta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
		Type: "WritebackPending", Status: metav1.ConditionTrue, Reason: "AwaitingM5",
	})
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)
	require.Zero(t, fw.openCalls, "OpenChange must not be called even with prURL set")
}
