package controller

import (
	"context"
	"strings"
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
	// No child proposal Task seeded -> BrainstormComplete (no yield).
	require.Equal(t, "BrainstormComplete", cond.Reason)
}

// TestDoWriteBackBrainstorm_WithProposal: a brainstorm Task that has at least
// one child proposal Task (spec.proposedIssue set, same project+repo) must
// clear WritebackPending with reason BrainstormProposed.
func TestDoWriteBackBrainstorm_WithProposal(t *testing.T) {
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "bswb-proposal-task", "bswb-proposal-proj", "bswb-proposal-repo", "bswb-proposal-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "brainstorm new issues",
			Kind: "brainstorm",
		}, nil)

	// Seed a child proposal Task with spec.proposedIssue set, same project+repo.
	proposalTask := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bswb-proposal-child",
			Namespace: testNS,
		},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    task.Spec.ProjectRef,
			RepositoryRef: task.Spec.RepositoryRef,
			Goal:          "implement: add caching layer",
			Kind:          "implement",
			ProposedIssue: &tatarav1alpha1.ProposedIssueSpec{
				RepositoryRef: task.Spec.RepositoryRef,
				Title:         "Add caching layer",
				Body:          "Proposal body",
				Kind:          "improvement",
			},
		},
	}
	require.NoError(t, k8sClient.Create(context.Background(), proposalTask))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	// OpenChange must NOT be invoked.
	require.Zero(t, fw.openCalls, "brainstorm Task must not call OpenChange")

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	cond := findCond(got.Status.Conditions, "WritebackPending")
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status)
	// With a proposal child: must use BrainstormProposed.
	require.Equal(t, "BrainstormProposed", cond.Reason, "brainstorm with a proposal child must use BrainstormProposed")
}

// TestDoWriteBackBrainstorm_NoProposal: a brainstorm Task with NO child proposal
// Tasks must use BrainstormComplete (no yield), not BrainstormProposed.
func TestDoWriteBackBrainstorm_NoProposal(t *testing.T) {
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "bswb-noprop-task", "bswb-noprop-proj", "bswb-noprop-repo", "bswb-noprop-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "brainstorm new issues",
			Kind: "brainstorm",
		}, nil)

	// No child proposal Tasks created.

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	require.Zero(t, fw.openCalls, "brainstorm Task must not call OpenChange")

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	cond := findCond(got.Status.Conditions, "WritebackPending")
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status)
	// Without a proposal child: must use BrainstormComplete, NOT BrainstormProposed.
	require.Equal(t, "BrainstormComplete", cond.Reason, "brainstorm with no proposal must use BrainstormComplete")
}

// TestBrainstormGoal_ContainsProposeIssueRequirement verifies the goal string
// explicitly mandates propose_issue and single-proposal framing.
func TestBrainstormGoal_ContainsProposeIssueRequirement(t *testing.T) {
	goal := brainstormGoal("owner/repo")
	require.Contains(t, goal, "propose_issue", "brainstorm goal must name propose_issue as a hard requirement")
	require.Contains(t, goal, "exactly one", "brainstorm goal must state exactly one proposal")
	// Must frame the decision explicitly so the agent does not invite open-ended back-and-forth.
	lower := strings.ToLower(goal)
	hasDecisionFraming := strings.Contains(lower, "approve") || strings.Contains(lower, "decision")
	require.True(t, hasDecisionFraming, "brainstorm goal must include single-decision framing (approve/refine)")
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
