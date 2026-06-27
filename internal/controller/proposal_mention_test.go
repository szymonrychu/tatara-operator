package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
)

// TestCreateProposal_MentionsApprovers verifies a newly-opened proposal issue
// @mentions the project's approvers so they are notified.
func TestCreateProposal_MentionsApprovers(t *testing.T) {
	ctx := context.Background()
	fw := &labelCapturingWriter{}
	r := newProposalReconciler(t, fw, nil)

	task := seedProposalTask(t, "mention-1", "mention-proj-1", "mention-repo-1", "mention-scm-1", "Fix a thing needing review")

	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))
	proj.Spec.Scm.MaintainerLogins = []string{"szymonrychu"}
	require.NoError(t, k8sClient.Update(ctx, &proj))
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))

	_, err := r.createProposal(ctx, &proj, task)
	require.NoError(t, err)

	require.Contains(t, fw.lastBody, "cc: @szymonrychu")
}

// TestCreateProposal_NoMentionWhenNoApprovers verifies no cc line is added when
// the project has no approvers.
func TestCreateProposal_NoMentionWhenNoApprovers(t *testing.T) {
	ctx := context.Background()
	fw := &labelCapturingWriter{}
	r := newProposalReconciler(t, fw, nil)

	task := seedProposalTask(t, "mention-2", "mention-proj-2", "mention-repo-2", "mention-scm-2", "Fix another thing")

	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))

	_, err := r.createProposal(ctx, &proj, task)
	require.NoError(t, err)

	require.NotContains(t, fw.lastBody, "cc: @")
}
