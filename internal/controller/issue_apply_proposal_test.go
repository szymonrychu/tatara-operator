package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// proposalMirror builds a CLOSED mirror of a tatara-filed proposal issue,
// controller-owned by task and carrying the one maintainer comment that IS the
// decline reason the <proposal_history> block renders.
//
// stamped=true writes the O5 durable provenance pair into Spec (kind + anchor),
// which is what mintIssueCR does for every proposal it files. stamped=false
// leaves Spec empty so the legacy pre-O5 read path (effectiveProposalKind's
// bot-authorship + anchor gated body-marker fallback) is the thing under test.
func proposalMirror(task *tatarav1alpha1.Task, kind string, stamped bool) *tatarav1alpha1.Issue {
	body := "an idea worth having"
	if kind != "" {
		body = tatarav1alpha1.StampProposalMarker(body, kind)
	}
	iss := ownedIssue(tatarav1alpha1.IssueName("tatara-operator", 1), 1, task, tatarav1alpha1.IssueStatus{
		State: "closed", Status: "new", Author: "tatara-bot", Title: "an idea", Body: body,
		Comments: []tatarav1alpha1.Comment{{
			ExternalID: "c1", Author: "maintainer", Body: "not worth it",
			CreatedAt: metav1.NewTime(time.Now()),
		}},
	})
	if kind != "" {
		iss.Spec.ProposalBodyHash = tatarav1alpha1.ComputeProposalContentHash(body)
		if stamped {
			iss.Spec.ProposalKind = kind
		}
	}
	return iss
}

// seedClosedProposal wires a Project/Repository/clarify Task/closed proposal
// mirror onto one fake client. The Task is at clarifying (a live source stage,
// so the WS3-I3 stop edge fires) and lists the issue in Status.IssueRefs.
func seedClosedProposal(t *testing.T, kind string, stamped bool) (client.Client, *tatarav1alpha1.Task, *tatarav1alpha1.Issue) {
	t.Helper()
	proj, repo := mirrorProject("tatara-bot"), mirrorRepo()
	task := taskAtStage(tatarav1alpha1.StageClarifying, "")
	task.Spec.Kind = "clarify"
	iss := proposalMirror(task, kind, stamped)
	task.Status.IssueRefs = []string{iss.Name}
	return newMirrorClient(t, proj, repo, task, iss, scmSecret()), task, iss
}

func getProposalIssue(t *testing.T, c client.Client, name string) (*tatarav1alpha1.Issue, bool) {
	t.Helper()
	if issueGone(t, c, name) {
		return nil, false
	}
	return getIssueCR(t, c, name), true
}

// TestIssueClosedStopRetainsABrainstormProposalMirror is conflict C3: without
// retention a declined proposal has NO CR to read, so the <proposal_history>
// block's declined rows are unreachable and a killed idea is invisible to the
// next brainstorm session (the immortal-PR failure mode). It is also conflict
// C2: this is the first production path that ever writes status="rejected".
func TestIssueClosedStopRetainsABrainstormProposalMirror(t *testing.T) {
	for _, tc := range []struct {
		name    string
		stamped bool
	}{
		{"spec stamped by mintIssueCR", true},
		{"legacy unstamped, recovered from the body marker", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			c, task, iss := seedClosedProposal(t, tatarav1alpha1.ProposalKindBrainstorm, tc.stamped)

			stopped, err := ApplyIssueClosedStop(ctx, c, task, iss.Name, time.Now())
			require.NoError(t, err)
			require.True(t, stopped)

			gotTask := getTaskCR(t, c, task.Name)
			require.Equal(t, tatarav1alpha1.StageRejected, gotTask.Status.Stage)
			require.Equal(t, stage.ReasonIssueClosed, gotTask.Status.StageReason)
			require.NotContains(t, gotTask.Status.IssueRefs, iss.Name,
				"the Task side is always severed so the reaper never walks a stale ref")

			gotIss, ok := getProposalIssue(t, c, iss.Name)
			require.True(t, ok, "a discarded proposal's mirror must be RETAINED: it is the only record of the decline")
			require.Equal(t, "rejected", gotIss.Status.Status)
			require.Len(t, gotIss.Status.Comments, 1)
			require.Equal(t, "not worth it", gotIss.Status.Comments[0].Body,
				"the maintainer's reason is load-bearing for the next session's proposal history")
			require.Equal(t, "declined", proposalDisplayStatus(gotIss),
				"the retained verdict must read as declined, the status the history block renders")
		})
	}
}

// TestIssueClosedStopRetentionKeepsTheOwnerReference pins what BOUNDS the
// retention. The mirror keeps its ownerRef on the now rejected Task, so it
// cascades with that Task's reap at rejectedRetention instead of leaking a
// zero-owner CR nothing ever collects. Orphaning it here would retain declined
// proposals FOREVER.
func TestIssueClosedStopRetentionKeepsTheOwnerReference(t *testing.T) {
	ctx := context.Background()
	c, task, iss := seedClosedProposal(t, tatarav1alpha1.ProposalKindBrainstorm, true)

	_, err := ApplyIssueClosedStop(ctx, c, task, iss.Name, time.Now())
	require.NoError(t, err)

	gotIss, ok := getProposalIssue(t, c, iss.Name)
	require.True(t, ok)
	owner, owned := own.ControllerOwner(gotIss)
	require.True(t, owned, "a retained mirror must never be left with zero controller owners (B.2 rule 5)")
	require.Equal(t, task.Name, owner,
		"the rejected Task stays the owner: its 7d reap is what bounds the retained history")
}

// TestIssueClosedStopStillDeletesAnOrdinaryMirror is the blast-radius guard.
// Retention is scoped to BRAINSTORM proposals: every other issue's mirror stays
// a rebuildable projection and is deleted exactly as before, which is what keeps
// the reopen mint prompt.
func TestIssueClosedStopStillDeletesAnOrdinaryMirror(t *testing.T) {
	for _, tc := range []struct {
		name    string
		kind    string
		stamped bool
	}{
		{"a human-filed issue", "", false},
		{"an incident tracker issue", tatarav1alpha1.ProposalKindIncident, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			c, task, iss := seedClosedProposal(t, tc.kind, tc.stamped)

			stopped, err := ApplyIssueClosedStop(ctx, c, task, iss.Name, time.Now())
			require.NoError(t, err)
			require.True(t, stopped)
			require.True(t, issueGone(t, c, iss.Name),
				"only a brainstorm proposal's mirror is retained; everything else is deleted, exactly as before")
		})
	}
}

// TestIssueClosedStopRetentionIsIdempotent: the stop edge is driven from a
// reconcile that can re-run at any time.
func TestIssueClosedStopRetentionIsIdempotent(t *testing.T) {
	ctx := context.Background()
	c, task, iss := seedClosedProposal(t, tatarav1alpha1.ProposalKindBrainstorm, true)

	for i := 0; i < 2; i++ {
		if _, err := ApplyIssueClosedStop(ctx, c, task, iss.Name, time.Now()); err != nil {
			t.Fatalf("pass %d: %v", i, err)
		}
	}
	gotIss, ok := getProposalIssue(t, c, iss.Name)
	require.True(t, ok)
	require.Equal(t, "rejected", gotIss.Status.Status)
}

// TestIssueClosedStopRetentionNeverOverwritesApproval: an APPROVED proposal a
// maintainer later closes must not be rewritten to rejected. The approval is a
// verified, single-use evidence record and the close is just housekeeping.
func TestIssueClosedStopRetentionNeverOverwritesApproval(t *testing.T) {
	ctx := context.Background()
	c, task, iss := seedClosedProposal(t, tatarav1alpha1.ProposalKindBrainstorm, true)
	live := getIssueCR(t, c, iss.Name)
	live.Status.Status = "approved"
	require.NoError(t, c.Status().Update(ctx, live))

	_, err := ApplyIssueClosedStop(ctx, c, task, iss.Name, time.Now())
	require.NoError(t, err)

	gotIss, ok := getProposalIssue(t, c, iss.Name)
	require.True(t, ok)
	require.Equal(t, "approved", gotIss.Status.Status)
	require.Equal(t, "approved", proposalDisplayStatus(gotIss))
}

// TestIssueReconcileReSeverKeepsARetainedProposal is the OTHER half of the C3
// fix, and without it the whole task is a no-op: handleIssueClosed's re-sever
// hardening treats "rejected(issue-closed) owner Task + the CR still present" as
// a crash-interrupted delete and finishes it. After retention that state is the
// STEADY state of every declined proposal, so an unguarded re-sever deletes the
// mirror on the very next reconcile.
func TestIssueReconcileReSeverKeepsARetainedProposal(t *testing.T) {
	ctx := context.Background()
	c, task, iss := seedClosedProposal(t, tatarav1alpha1.ProposalKindBrainstorm, true)

	_, err := ApplyIssueClosedStop(ctx, c, task, iss.Name, time.Now())
	require.NoError(t, err)

	w := &mirrorWriter{}
	r := newIssueReconciler(c, w, nil)
	reconcileIssue(t, r, iss.Name)
	reconcileIssue(t, r, iss.Name)

	gotIss, ok := getProposalIssue(t, c, iss.Name)
	require.True(t, ok, "the re-sever hardening must not delete a RETAINED proposal mirror")
	require.Equal(t, "rejected", gotIss.Status.Status)
	require.Contains(t, w.added, "tatara-declined",
		"a retained mirror keeps reconciling, so the rejected verdict still projects onto the forge label")
}

// TestIssueReconcileReSeverStillFinishesAnOrdinaryCrashedDelete: the crash
// window the hardening exists for is unchanged for every non-proposal issue.
func TestIssueReconcileReSeverStillFinishesAnOrdinaryCrashedDelete(t *testing.T) {
	proj, repo := mirrorProject("tatara-bot"), mirrorRepo()
	task := taskAtStage(tatarav1alpha1.StageRejected, stage.ReasonIssueClosed)
	iss := proposalMirror(task, "", false)
	c := newMirrorClient(t, proj, repo, task, iss, scmSecret())

	r := newIssueReconciler(c, &mirrorWriter{}, nil)
	reconcileIssue(t, r, iss.Name)
	require.True(t, issueGone(t, c, iss.Name), "re-sever must still finish the interrupted DeleteCR")
}

// TestReopenedRetainedProposalIsOrphanedAndCountsAgain closes the hole retention
// itself opens. A retained mirror keeps a dead Task as its controller owner and
// carries status=rejected, so a maintainer who REOPENS the proposal would
// otherwise get an issue that: is not an orphan, so the sweep never re-mints a
// Task for it; can never be approved (approvalInScope refuses a rejected
// thread); and does not count as pending, so the controller refills over it. The
// pre-O9 delete-and-re-create had none of those problems, so retention must undo
// itself on reopen.
func TestReopenedRetainedProposalIsOrphanedAndCountsAgain(t *testing.T) {
	ctx := context.Background()
	c, task, iss := seedClosedProposal(t, tatarav1alpha1.ProposalKindBrainstorm, true)
	_, err := ApplyIssueClosedStop(ctx, c, task, iss.Name, time.Now())
	require.NoError(t, err)

	// The maintainer reopens it; the mirror sync writes state back to open.
	live := getIssueCR(t, c, iss.Name)
	live.Status.State = "open"
	require.NoError(t, c.Status().Update(ctx, live))

	r := newIssueReconciler(c, &mirrorWriter{}, nil)
	reconcileIssue(t, r, iss.Name)

	gotIss, ok := getProposalIssue(t, c, iss.Name)
	require.True(t, ok, "a reopened proposal keeps its mirror; only the verdict is undone")
	require.Equal(t, "new", gotIss.Status.Status, "the decline is undone, so the thread is approvable again")
	require.True(t, proposalPending(gotIss, "tatara-bot"), "a reopened proposal counts against the target again")
	_, owned := own.ControllerOwner(gotIss)
	require.False(t, owned,
		"the dead rejected Task must not keep owning a live issue: the mirror is orphaned so the sweep re-mints it")
}

// TestReopenedOrdinaryIssueIsUntouched: the reopen undo is brainstorm-scoped
// too. Nothing writes rejected on an ordinary issue, but the guard must not fire
// on one that carries the status for any other reason.
func TestReopenedOrdinaryIssueIsUntouched(t *testing.T) {
	ctx := context.Background()
	proj, repo := mirrorProject("tatara-bot"), mirrorRepo()
	task := taskAtStage(tatarav1alpha1.StageParked, stage.ReasonAwaitingHuman)
	iss := proposalMirror(task, "", false)
	c := newMirrorClient(t, proj, repo, task, iss, scmSecret())
	live := getIssueCR(t, c, iss.Name)
	live.Status.State, live.Status.Status = "open", "rejected"
	require.NoError(t, c.Status().Update(ctx, live))

	r := newIssueReconciler(c, &mirrorWriter{}, nil)
	reconcileIssue(t, r, iss.Name)

	gotIss, ok := getProposalIssue(t, c, iss.Name)
	require.True(t, ok)
	require.Equal(t, "rejected", gotIss.Status.Status)
	owner, owned := own.ControllerOwner(gotIss)
	require.True(t, owned)
	require.Equal(t, task.Name, owner)
}
