package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
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

// seedRetainedDecline drives a real decline to completion and returns the client
// plus the retained mirror's name. It never hand-writes the retained state.
func seedRetainedDecline(t *testing.T) (client.Client, *tatarav1alpha1.Project, *tatarav1alpha1.Repository, string) {
	t.Helper()
	proj, repo := sweepProject("reopen-proj"), sweepRepo("reopen-proj")
	task := taskAtStage(tatarav1alpha1.StageClarifying, "")
	task.Spec.Kind = "clarify"
	iss := proposalMirror(task, tatarav1alpha1.ProposalKindBrainstorm, true)
	iss.Spec.ProjectRef = proj.Name
	task.Status.IssueRefs = []string{iss.Name}
	c := newMirrorClient(t, proj, repo, task, iss, scmSecret())

	if _, err := ApplyIssueClosedStop(context.Background(), c, task, iss.Name, time.Now()); err != nil {
		t.Fatalf("ApplyIssueClosedStop: %v", err)
	}
	return c, proj, repo, iss.Name
}

// TestSweepUndoesRetentionOnAReopenedProposal drives the undo through the REAL
// caller, not by hand-writing Status.State.
//
// That distinction is the whole point of this test. The first version of O9
// gated the undo on state=open and proved only that the FUNCTION worked - while
// no production path could ever set a retained mirror's state back to open, so
// the undo was dead code and reopen was newly broken rather than fixed. The
// sweep is the backstop half of the fix: it lists OPEN forge issues, and a
// retained mirror is controller-owned, so IsOrphanIssue skips it forever unless
// the undo runs FIRST.
func TestSweepUndoesRetentionOnAReopenedProposal(t *testing.T) {
	c, proj, repo, issName := seedRetainedDecline(t)

	before := getIssueCR(t, c, issName)
	require.Equal(t, "rejected", before.Status.Status, "precondition: a real decline was retained")
	require.Equal(t, "closed", before.Status.State)
	_, ownedBefore := own.ControllerOwner(before)
	require.True(t, ownedBefore, "precondition: the retained mirror is owned, so it is NOT an orphan")

	// The maintainer reopens it on the forge. The sweep sees an OPEN issue.
	rd := &sweepReader{
		issues: []scm.IssueRef{{
			Repo: "szymonrychu/tatara-operator", Number: 1,
			Author: "tatara-bot", Title: "an idea", State: "open",
		}},
		content: map[int]scm.IssueContent{1: {Title: "an idea", Body: before.Status.Body}},
	}
	runSweep(t, c, proj, repo, rd)

	got, ok := getProposalIssue(t, c, issName)
	require.True(t, ok, "a reopened proposal keeps its mirror; only the verdict is undone")
	require.Equal(t, "new", got.Status.Status, "the decline is undone, so the thread is approvable again")
	require.Equal(t, "open", got.Status.State)
	require.True(t, proposalPending(got, "tatara-bot"), "a reopened proposal counts against the target again")

	// And it is workable again: the same pass adopts it, because the undo dropped
	// the ownerRef before IsOrphanIssue read it.
	owner, owned := own.ControllerOwner(got)
	require.True(t, owned, "the sweep must re-mint a Task for the revived proposal")
	require.NotEqual(t, "t-1", owner, "the dead rejected Task must not still own a live issue")
}

// TestSweepLeavesAnOrdinaryRejectedIssueAlone: the undo is brainstorm-scoped.
func TestSweepLeavesAnOrdinaryRejectedIssueAlone(t *testing.T) {
	proj, repo := sweepProject("ord-proj"), sweepRepo("ord-proj")
	task := taskAtStage(tatarav1alpha1.StageParked, stage.ReasonAwaitingHuman)
	iss := proposalMirror(task, "", false)
	iss.Spec.ProjectRef = proj.Name
	iss.Status.State, iss.Status.Status = "open", "rejected"
	c := newMirrorClient(t, proj, repo, task, iss, scmSecret())

	rd := &sweepReader{
		issues: []scm.IssueRef{{
			Repo: "szymonrychu/tatara-operator", Number: 1,
			Author: "alice", Title: "an idea", State: "open",
		}},
		content: map[int]scm.IssueContent{1: {Title: "an idea", Body: iss.Status.Body}},
	}
	runSweep(t, c, proj, repo, rd)

	got := getIssueCR(t, c, iss.Name)
	require.Equal(t, "rejected", got.Status.Status)
	owner, owned := own.ControllerOwner(got)
	require.True(t, owned)
	require.Equal(t, task.Name, owner)
}

// TestReopenUndoOrphansBeforeClearingTheVerdict is an ORDERING PROBE, the same
// idiom TestReapClosesOwnBotMRAfterHandover uses.
//
// The order is crash safety. Orphan first, clear second: the interruptible state
// is rejected+ownerless, which the sweep backstop keys on (status=rejected
// alone) and recovers. Clear first and the interruptible state is new+owned by a
// dead Task - no gate re-enters it, no sweep adopts it, and the reaper cascades
// the mirror away.
func TestReopenUndoOrphansBeforeClearingTheVerdict(t *testing.T) {
	c, proj, _, issName := seedRetainedDecline(t)

	var ownersAtStatusWrite int
	probe := interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string,
			obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if iss, isIssue := obj.(*tatarav1alpha1.Issue); isIssue && iss.Status.Status == "new" {
				var live tatarav1alpha1.Issue
				if err := cl.Get(ctx, client.ObjectKeyFromObject(obj), &live); err != nil {
					return err
				}
				ownersAtStatusWrite = len(live.GetOwnerReferences())
			}
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
	}
	probed := fake.NewClientBuilder().
		WithScheme(c.Scheme()).
		WithObjects(getIssueCR(t, c, issName), getTaskCR(t, c, "t-1"), proj).
		WithStatusSubresource(&tatarav1alpha1.Issue{}, &tatarav1alpha1.Task{}).
		WithInterceptorFuncs(probe).
		Build()

	iss := &tatarav1alpha1.Issue{}
	require.NoError(t, probed.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: issName}, iss))
	require.NoError(t, reopenRetainedProposal(context.Background(), probed, iss, "tatara-bot"))

	require.Equal(t, 0, ownersAtStatusWrite,
		"the verdict was cleared while the dead Task still owned the mirror: a crash there wedges it forever")
}

// seedParkedProposalClose wires a PARKED clarify Task owning a CLOSED proposal
// mirror - the shape a maintainer produces by just closing the issue, with no
// comment to unpark anything first. reason picks which park.
func seedParkedProposalClose(t *testing.T, reason, kind string, stamped bool) (client.Client, *tatarav1alpha1.Task, *tatarav1alpha1.Issue) {
	t.Helper()
	proj, repo := mirrorProject("tatara-bot"), mirrorRepo()
	task := taskAtStage(tatarav1alpha1.StageParked, reason)
	task.Spec.Kind = "clarify"
	iss := proposalMirror(task, kind, stamped)
	task.Status.IssueRefs = []string{iss.Name}
	return newMirrorClient(t, proj, repo, task, iss, scmSecret()), task, iss
}

// TestParkedProposalCloseIsRecordedWithoutStoppingTheTask is the 2026-07-27
// ruling: the stop edge and the decline record are SEPARATE concerns.
//
// This is the shape that dominates real declines - closing an issue is one click
// and needs no comment, so nothing ever unparks the owner into clarifying,
// AllowsIssueClosedStop is false, and before this every such decline went
// unrecorded and its mirror was cascaded by the very next reap pass. Recording
// it must NOT stop the Task: stage, stage reason and pod are untouched.
func TestParkedProposalCloseIsRecordedWithoutStoppingTheTask(t *testing.T) {
	for _, reason := range []string{stage.ReasonBacklogSweep, stage.ReasonAwaitingHuman} {
		t.Run(reason, func(t *testing.T) {
			c, task, iss := seedParkedProposalClose(t, reason, tatarav1alpha1.ProposalKindBrainstorm, true)

			r := newIssueReconciler(c, &mirrorWriter{}, nil)
			reconcileIssue(t, r, iss.Name)

			gotTask := getTaskCR(t, c, task.Name)
			require.Equal(t, tatarav1alpha1.StageParked, gotTask.Status.Stage,
				"recording a decline must never move the owner's stage")
			require.Equal(t, reason, gotTask.Status.StageReason)
			require.NotContains(t, gotTask.Status.IssueRefs, iss.Name,
				"the sever still runs: that is what puts the mirror in the retained shape")
			require.NotEmpty(t, gotTask.Annotations[AnnProposalDeclinedAt],
				"the retention window is anchored on the decline, not on the park")

			gotIss, ok := getProposalIssue(t, c, iss.Name)
			require.True(t, ok, "the mirror is retained, not cascaded")
			require.Equal(t, "rejected", gotIss.Status.Status)
			require.Equal(t, "declined", proposalDisplayStatus(gotIss))
			owner, owned := own.ControllerOwner(gotIss)
			require.True(t, owned)
			require.Equal(t, task.Name, owner, "the retained mirror keeps its ownerRef")
		})
	}
}

// TestParkedNonProposalCloseIsLeftCompletelyAlone is the blast-radius guard on
// the widened gate. This branch is now reached for EVERY closed issue whose
// owner is not stoppable, so it must write nothing at all for an ordinary issue
// - not a sever, and above all not the delete recordProposalDecline would do.
func TestParkedNonProposalCloseIsLeftCompletelyAlone(t *testing.T) {
	for _, tc := range []struct {
		name    string
		kind    string
		stamped bool
	}{
		{"a human-filed issue", "", false},
		{"an incident tracker issue", tatarav1alpha1.ProposalKindIncident, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, task, iss := seedParkedProposalClose(t, stage.ReasonAwaitingHuman, tc.kind, tc.stamped)

			r := newIssueReconciler(c, &mirrorWriter{}, nil)
			reconcileIssue(t, r, iss.Name)

			gotIss, ok := getProposalIssue(t, c, iss.Name)
			require.True(t, ok, "an ordinary parked issue's mirror must not be deleted by the decline path")
			require.Equal(t, "new", gotIss.Status.Status, "no verdict is invented for a non-proposal")

			gotTask := getTaskCR(t, c, task.Name)
			require.Contains(t, gotTask.Status.IssueRefs, iss.Name,
				"no sever either: the Task's relationship to an ordinary issue is untouched")
			require.Empty(t, gotTask.Annotations[AnnProposalDeclinedAt])
		})
	}
}

// TestReopenAfterAParkedDeclineRecoversTheSameWay: the reopen undo keys on the
// RETAINED SHAPE, not on rejected(issue-closed), so a decline recorded against a
// parked owner reopens exactly like one recorded against a stopped Task.
func TestReopenAfterAParkedDeclineRecoversTheSameWay(t *testing.T) {
	ctx := context.Background()
	c, task, iss := seedParkedProposalClose(t, stage.ReasonBacklogSweep, tatarav1alpha1.ProposalKindBrainstorm, true)
	r := newIssueReconciler(c, &mirrorWriter{}, nil)
	reconcileIssue(t, r, iss.Name)
	require.Equal(t, "rejected", getIssueCR(t, c, iss.Name).Status.Status, "precondition: the decline was recorded")

	live := getIssueCR(t, c, iss.Name)
	require.NoError(t, reopenRetainedProposal(ctx, c, live, "tatara-bot"))

	got := getIssueCR(t, c, iss.Name)
	require.Equal(t, "new", got.Status.Status)
	require.Equal(t, "open", got.Status.State)
	require.True(t, proposalPending(got, "tatara-bot"))
	_, owned := own.ControllerOwner(got)
	require.False(t, owned,
		"a parked owner that already severed the mirror must release it too, or the reopen wedges one shape over")

	require.Equal(t, tatarav1alpha1.StageParked, getTaskCR(t, c, task.Name).Status.Stage,
		"the reopen undo does not move the owner's stage either")
}

// TestReopenNeverOrphansAMirrorTheOwnerIsStillWorking: the retained-shape key
// must not fire on a live owner that still LISTS the issue.
func TestReopenNeverOrphansAMirrorTheOwnerIsStillWorking(t *testing.T) {
	ctx := context.Background()
	proj, repo := mirrorProject("tatara-bot"), mirrorRepo()
	task := taskAtStage(tatarav1alpha1.StageClarifying, "")
	iss := proposalMirror(task, tatarav1alpha1.ProposalKindBrainstorm, true)
	iss.Status.State, iss.Status.Status = "open", "rejected"
	task.Status.IssueRefs = []string{iss.Name}
	c := newMirrorClient(t, proj, repo, task, iss, scmSecret())

	live := getIssueCR(t, c, iss.Name)
	require.NoError(t, reopenRetainedProposal(ctx, c, live, "tatara-bot"))

	got := getIssueCR(t, c, iss.Name)
	require.Equal(t, "new", got.Status.Status, "the verdict is still cleared")
	owner, owned := own.ControllerOwner(got)
	require.True(t, owned, "but a Task that still lists the issue keeps owning it")
	require.Equal(t, task.Name, owner)
}
