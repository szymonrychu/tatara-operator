package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// reapProposalIssue builds a proposal mirror controller-owned by taskName, in
// the state named by state/status.
func reapProposalIssue(proj, repo, taskName, state, status string, number int) *tatarav1alpha1.Issue {
	body := tatarav1alpha1.StampProposalMarker("an idea worth having", tatarav1alpha1.ProposalKindBrainstorm)
	iss := &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{
			Name: tatarav1alpha1.IssueName(repo, number), Namespace: testNS,
			OwnerReferences: []metav1.OwnerReference{reapOwnerRef(taskName, true)},
		},
		Spec: tatarav1alpha1.IssueSpec{
			RepositoryRef: repo, Number: number, ProjectRef: proj,
			ProposalKind:     tatarav1alpha1.ProposalKindBrainstorm,
			ProposalBodyHash: tatarav1alpha1.ComputeProposalContentHash(body),
		},
	}
	iss.Status.Author = "tatara-bot"
	iss.Status.Body = body
	iss.Status.State = state
	iss.Status.Status = status
	return iss
}

// TestReapKeepsAnOpenUndecidedProposalCountable pins the invariant the whole
// target-backlog control law rests on: a proposal that is still OPEN and still
// AWAITING a maintainer verdict keeps its Issue CR when the clarify Task that
// owns it is reaped.
//
// This is fix H13's rule ("an artifact that is still OPEN must be re-mintable
// RIGHT NOW": drop the ownerRef, let the CR survive OWNERLESS, let the sweep
// re-adopt it) read for its NEW consequence. Since O2/O5 the CR is not only the
// mirror, it is the COUNTER: it carries spec.proposalKind, and
// pendingProposalCount is a count of these CRs. Cascade it and the proposal
// silently stops counting, the deficit opens up, and the operator refills
// against a proposal that is still sitting in the maintainer's queue - the
// backlog drifts ABOVE target, penalising exactly the proposals a maintainer
// sits on longest. It is also what would leave the CR re-mintable ONLY through
// ensureIssueCR, which writes no Spec provenance at all (O5 review round 3).
func TestReapKeepsAnOpenUndecidedProposalCountable(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status string
	}{
		{"never triaged", "new"},
		{"no status at all", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			proj := reapProject("openprop")
			repo := reapRepo("openprop", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")

			// The clarify Task that filed the proposal, parked 8 days ago: PAST
			// parkRetention, so this reap pass deletes it.
			task := reapTask("openprop", "clarify-task", "clarify",
				tatarav1alpha1.StageParked, stage.ReasonAwaitingHuman, time.Now().Add(-8*24*time.Hour))
			iss := reapProposalIssue("openprop", repo.Name, task.Name, "open", tc.status, 11)
			task.Status.IssueRefs = []string{iss.Name}

			c := newMirrorClient(t, proj, repo, reapSecret(), task, iss)
			w := &reapWriter{
				comment:  func(string, string) error { return nil },
				addLabel: func(string, string) error { return nil },
			}
			r := reapReconciler(c, w)
			require.NoError(t, r.ReapTerminal(ctx, proj))

			_, alive := mustGetTask(t, c, task.Name)
			require.False(t, alive, "a parked(awaiting-human) task past parkRetention must be reaped")

			got := mustGetIssue(t, c, iss.Name)
			require.Equal(t, tatarav1alpha1.ProposalKindBrainstorm, got.Spec.ProposalKind,
				"the durable provenance the backlog count reads must survive the reap")
			require.True(t, proposalPending(got, "tatara-bot"),
				"an open, undecided proposal must still count against the target after its Task is reaped")
			require.Empty(t, got.OwnerReferences,
				"the mirror must be left OWNERLESS so it cannot cascade with the dead Task and the sweep re-adopts it")
		})
	}
}

// TestReapCascadesARetainedDeclinedProposal is the OTHER side of that rule, and
// it is what BOUNDS the O9 retention. A declined proposal's mirror is kept on
// purpose - it is the only record of the decline - but it keeps its ownerRef on
// the rejected Task, so it cascades with that Task's reap. Declined history is
// therefore bounded by the terminal-Task retention window, not unbounded CR
// growth, and NOT by historyWindow.
func TestReapCascadesARetainedDeclinedProposal(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("declprop")
	repo := reapRepo("declprop", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")

	// The shape recordProposalDecline leaves behind: the Task is
	// rejected(issue-closed) and no longer lists the issue, the mirror is closed,
	// stamped rejected, and still owned.
	task := reapTask("declprop", "clarify-task", "clarify",
		tatarav1alpha1.StageRejected, stage.ReasonIssueClosed, time.Now().Add(-8*24*time.Hour))
	iss := reapProposalIssue("declprop", repo.Name, task.Name, "closed", "rejected", 12)

	c := newMirrorClient(t, proj, repo, reapSecret(), task, iss)
	// ZERO forge writes: releaseTerminal comments and labels OPEN issues only, and
	// this one is closed. A panicking fake is the assertion.
	r := reapReconciler(c, &reapWriter{})
	require.NoError(t, r.ReapTerminal(ctx, proj))

	_, alive := mustGetTask(t, c, task.Name)
	require.False(t, alive, "a rejected task past rejectedRetention must be reaped")

	got := mustGetIssue(t, c, iss.Name)
	owner, owned := own.ControllerOwner(got)
	require.True(t, owned, "the retained mirror must keep its ownerRef")
	require.Equal(t, task.Name, owner,
		"it cascades with the reaped Task: that is what bounds retained declined history")
	require.Equal(t, "declined", proposalDisplayStatus(got))
}
