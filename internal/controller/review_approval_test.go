package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// Phase 6 sub-step 2: review approval applies tatara-approved + native Approve
// and NEVER merges; an unmergeable PR (or request_changes) re-adds
// tatara-implementation instead of approving.

func TestWriteBackReview_ApproveAppliesApprovedLabel(t *testing.T) {
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "rev-approve-label", "rap-proj", "rap-repo", "rap-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "review a PR",
			Kind: "review",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#9", IsPR: true, Number: 9,
			},
		}, nil)
	task.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{Decision: "approve", Body: "lgtm"}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	require.True(t, fw.approveCalled, "Approve must be called on approve verdict")
	require.True(t, fw.addLabelCalled, "tatara-approved must be applied on approve")
	require.Equal(t, "tatara-approved", fw.addLabelLabel, "approve must add exactly tatara-approved")
	require.False(t, fw.mergeCalled, "review must NEVER call Merge")
}

func TestWriteBackReview_UnmergeableRoutesToImplement(t *testing.T) {
	fw := &fullFakeSCMWriter{mergeState: scm.MergeStateDirty}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "rev-unmergeable", "run-proj", "run-repo", "run-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "review a PR",
			Kind: "review",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#11", IsPR: true, Number: 11,
			},
		}, nil)
	task.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{Decision: "approve", Body: "lgtm"}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	require.False(t, fw.approveCalled, "an unmergeable PR must NOT be approved")
	require.False(t, fw.mergeCalled, "review must NEVER call Merge")
	require.True(t, fw.addLabelCalled, "unmergeable must re-add tatara-implementation")
	require.Equal(t, "tatara-implementation", fw.addLabelLabel, "unmergeable routes back to implement")
}

func TestWriteBackReview_RequestChangesReAddsImplementation(t *testing.T) {
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "rev-rc-impl", "rrc-proj", "rrc-repo", "rrc-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "review a PR",
			Kind: "review",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#12", IsPR: true, Number: 12,
			},
		}, nil)
	task.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{Decision: "request_changes", Body: "fix it"}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	require.True(t, fw.requestChangesCalled, "request_changes must post RequestChanges")
	require.False(t, fw.mergeCalled, "review must NEVER call Merge")
	require.True(t, fw.addLabelCalled, "request_changes must re-add tatara-implementation")
	require.Equal(t, "tatara-implementation", fw.addLabelLabel, "request_changes routes back to implement")
}
