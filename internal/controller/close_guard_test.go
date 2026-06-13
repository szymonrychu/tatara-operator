package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func TestHasUnmergedChange(t *testing.T) {
	require.False(t, hasUnmergedChange(&tatarav1alpha1.Task{}))
	require.True(t, hasUnmergedChange(&tatarav1alpha1.Task{Status: tatarav1alpha1.TaskStatus{PrURL: "https://github.com/o/r/pull/9"}}))
	require.True(t, hasUnmergedChange(&tatarav1alpha1.Task{Status: tatarav1alpha1.TaskStatus{HeadBranch: "tatara/feat-x"}}))
}

// A "close" outcome on an issue that already has an unmerged change (open PR)
// must NOT close the issue; it parks in Conversation as an idea instead.
func TestFinishTriage_Close_WithUnmergedChange_NotClosed(t *testing.T) {
	ctx := context.Background()
	_, task, w := seedLabelTask(t, "close-unmerged", nil)
	got := getTaskByName(t, task.Name)
	got.Status.PrURL = "https://github.com/o/r/pull/9"
	require.NoError(t, k8sClient.Status().Update(ctx, got))
	r := reconcilerFor(w, &commentReader{})
	proj := projOf(t, task)
	markSucceededWithOutcome(t, task.Name, "close")
	_, err := r.finishTriage(ctx, proj, getTaskByName(t, task.Name))
	require.NoError(t, err)
	require.Empty(t, w.closed, "issue must NOT be closed while an unmerged change exists")
	require.Equal(t, "Conversation", getTaskByName(t, task.Name).Status.LifecycleState)
	require.Equal(t, []string{"tatara-idea"}, w.added, "label idea, not rejected")
}

// A "close" outcome on an issue with NO code artifact is a legitimate triage
// reject: it closes the issue and labels it rejected.
func TestFinishTriage_Close_NoChange_Closed(t *testing.T) {
	ctx := context.Background()
	_, task, w := seedLabelTask(t, "close-reject", nil)
	r := reconcilerFor(w, &commentReader{})
	proj := projOf(t, task)
	markSucceededWithOutcome(t, task.Name, "close")
	_, err := r.finishTriage(ctx, proj, getTaskByName(t, task.Name))
	require.NoError(t, err)
	require.Equal(t, []int{7}, w.closed, "pure triage reject closes the issue")
	require.Equal(t, "Done", getTaskByName(t, task.Name).Status.LifecycleState)
	require.Contains(t, w.added, "tatara-rejected")
}
