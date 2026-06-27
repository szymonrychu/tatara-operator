package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"k8s.io/apimachinery/pkg/types"
)

// labelCapturingWriter captures CreateIssue labels for assertion.
type labelCapturingWriter struct {
	scm.SCMWriter
	lastLabels []string
	lastBody   string
}

func (w *labelCapturingWriter) CreateIssue(_ context.Context, _, _ string, req scm.IssueReq) (scm.CreatedIssue, error) {
	w.lastLabels = req.Labels
	w.lastBody = req.Body
	return scm.CreatedIssue{Ref: "o/r#99", URL: "https://github.com/o/r/issues/99"}, nil
}

func (w *labelCapturingWriter) AddBoardItem(_ context.Context, _ string, _ scm.BoardRef, _ string) error {
	return nil
}

func TestCreateProposal_AddsIncidentLabelWhenIncident(t *testing.T) {
	fw := &labelCapturingWriter{}
	r := newProposalReconciler(t, fw, nil)

	task := seedProposalTask(t, "inc-prop-1", "inc-prop-proj-1", "inc-prop-repo-1", "inc-prop-scm-1", "Fix incident-originated issue A")
	task.Spec.ProposedIssue.Incident = true
	require.NoError(t, k8sClient.Update(context.Background(), task))
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, task))

	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))

	_, err := r.createProposal(context.Background(), &proj, task)
	require.NoError(t, err)

	require.Contains(t, fw.lastLabels, "tatara-brainstorming")
	require.Contains(t, fw.lastLabels, "tatara-incident")
}

func TestCreateProposal_NoIncidentLabelWhenNotIncident(t *testing.T) {
	fw := &labelCapturingWriter{}
	r := newProposalReconciler(t, fw, nil)

	task := seedProposalTask(t, "inc-prop-2", "inc-prop-proj-2", "inc-prop-repo-2", "inc-prop-scm-2", "Fix regular brainstorm issue B")
	task.Spec.ProposedIssue.Incident = false
	require.NoError(t, k8sClient.Update(context.Background(), task))
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, task))

	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))

	_, err := r.createProposal(context.Background(), &proj, task)
	require.NoError(t, err)

	require.Contains(t, fw.lastLabels, "tatara-brainstorming")
	require.NotContains(t, fw.lastLabels, "tatara-incident")
}
