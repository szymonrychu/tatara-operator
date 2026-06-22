package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
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

// TestDoWriteBackBrainstorm_PriorCycleProposalNotCounted: a proposal Task from a
// PRIOR brainstorm cycle (created before this brainstorm run) for the same
// project+repo must NOT be counted as this run's yield -> BrainstormComplete.
func TestDoWriteBackBrainstorm_PriorCycleProposalNotCounted(t *testing.T) {
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	proj, repo := "bswb-prior-proj", "bswb-prior-repo"

	priorProposal := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "bswb-prior-child", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: proj, RepositoryRef: repo, Goal: "implement: prior", Kind: "implement",
			ProposedIssue: &tatarav1alpha1.ProposedIssueSpec{
				RepositoryRef: repo, Title: "Prior", Body: "x", Kind: "improvement",
			},
		},
	}
	require.NoError(t, k8sClient.Create(context.Background(), priorProposal))
	// CreationTimestamp is second-granular; ensure the brainstorm run lands in a
	// strictly later second than the prior-cycle proposal.
	time.Sleep(1100 * time.Millisecond)
	task := seedWritebackKindTask(t, "bswb-prior-task", proj, repo, "bswb-prior-scm",
		tatarav1alpha1.TaskSpec{Goal: "brainstorm new issues", Kind: "brainstorm"}, nil)

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	cond := findCond(got.Status.Conditions, "WritebackPending")
	require.NotNil(t, cond)
	require.Equal(t, "BrainstormComplete", cond.Reason, "a prior-cycle proposal must not count as this run's yield")
}

// TestBrainstormGoal_ContainsProposeIssueRequirement verifies the goal string
// explicitly mandates propose_issue, the systemic multi-issue contract, and
// single-decision framing. The old "Exactly one action per run" clause is gone:
// systemic proposals allow one propose_issue per affected repo (bounded <=6).
func TestBrainstormGoal_ContainsProposeIssueRequirement(t *testing.T) {
	goal := brainstormGoalProject([]string{"owner/repo"}, "", "")
	require.Contains(t, goal, "propose_issue", "brainstorm goal must name propose_issue as a hard requirement")
	require.Contains(t, goal, "systemicId", "brainstorm goal must mention systemicId for multi-repo proposals")
	// Must frame the decision explicitly so the agent does not invite open-ended back-and-forth.
	lower := strings.ToLower(goal)
	hasDecisionFraming := strings.Contains(lower, "approve") || strings.Contains(lower, "decision")
	require.True(t, hasDecisionFraming, "brainstorm goal must include single-decision framing (approve/refine)")
	// Old single-action clause must be gone (multi-issue systemic is now allowed).
	require.NotContains(t, goal, "Exactly one action per run", "stale single-action clause must be removed")
}

// TestDoWriteBackBrainstorm_Metrics verifies the per-run yield counter
// operator_brainstorm_outcome_total is incremented on the right branch: a run
// with a proposal child bumps result="proposed", a no-yield run bumps
// result="no_yield".
func TestDoWriteBackBrainstorm_Metrics(t *testing.T) {
	t.Run("proposed", func(t *testing.T) {
		fw := &fullFakeSCMWriter{}
		r := newFullFakeReconciler(t, fw)
		task := seedWritebackKindTask(t, "bswb-metric-prop-task", "bswb-metric-prop-proj", "bswb-metric-prop-repo", "bswb-metric-prop-scm",
			tatarav1alpha1.TaskSpec{Goal: "brainstorm new issues", Kind: "brainstorm"}, nil)

		proposalTask := &tatarav1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{Name: "bswb-metric-prop-child", Namespace: testNS},
			Spec: tatarav1alpha1.TaskSpec{
				ProjectRef: task.Spec.ProjectRef, RepositoryRef: task.Spec.RepositoryRef,
				Goal: "implement: idea", Kind: "implement",
				ProposedIssue: &tatarav1alpha1.ProposedIssueSpec{
					RepositoryRef: task.Spec.RepositoryRef, Title: "idea", Body: "body", Kind: "improvement",
				},
			},
		}
		require.NoError(t, k8sClient.Create(context.Background(), proposalTask))

		_, err := reconcileWriteback(t, r, task.Name)
		require.NoError(t, err)

		require.Equal(t, float64(1), testutil.ToFloat64(r.Metrics.BrainstormOutcomeCounter("proposed")),
			"a brainstorm run with a proposal child must bump result=proposed")
		require.Equal(t, float64(0), testutil.ToFloat64(r.Metrics.BrainstormOutcomeCounter("no_yield")),
			"a proposed run must not bump result=no_yield")
	})

	t.Run("no_yield", func(t *testing.T) {
		fw := &fullFakeSCMWriter{}
		r := newFullFakeReconciler(t, fw)
		task := seedWritebackKindTask(t, "bswb-metric-noyield-task", "bswb-metric-noyield-proj", "bswb-metric-noyield-repo", "bswb-metric-noyield-scm",
			tatarav1alpha1.TaskSpec{Goal: "brainstorm new issues", Kind: "brainstorm"}, nil)

		_, err := reconcileWriteback(t, r, task.Name)
		require.NoError(t, err)

		require.Equal(t, float64(1), testutil.ToFloat64(r.Metrics.BrainstormOutcomeCounter("no_yield")),
			"a brainstorm run with no proposal must bump result=no_yield")
		require.Equal(t, float64(0), testutil.ToFloat64(r.Metrics.BrainstormOutcomeCounter("proposed")),
			"a no-yield run must not bump result=proposed")
	})
}

// TestWriteBackBrainstormNoneIsComplete: a brainstorm Task with
// Status.BrainstormOutcome={Action:"none", Reason:"x"} and no proposal child
// must clear WritebackPending with reason BrainstormComplete and a message
// containing the early-exit prefix.
func TestWriteBackBrainstormNoneIsComplete(t *testing.T) {
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "bswb-none-task", "bswb-none-proj", "bswb-none-repo", "bswb-none-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "brainstorm ideas",
			Kind: "brainstorm",
		}, nil)
	task.Status.BrainstormOutcome = &tatarav1alpha1.BrainstormOutcome{Action: "none", Reason: "x"}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	require.Zero(t, fw.openCalls, "brainstorm Task must not call OpenChange")

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	cond := findCond(got.Status.Conditions, "WritebackPending")
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status)
	require.Equal(t, "BrainstormComplete", cond.Reason)
	require.Contains(t, cond.Message, "early-exit: x", "message must contain the early-exit reason")
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
