package controller

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

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

// TestDeclinedProposalRetentionBoundary pins the ACTUAL boundary, at the actual
// constant, on both sides of it.
//
// The retained mirror keeps its ownerRef on purpose, so the owner Task's reap is
// what finally collects it - which makes the Task's retention window the real
// bound on how long a decline stays queryable. The generic RejectedRetention is
// 24h, which would give a decline a ONE-DAY half-life: the next brainstorm
// session no longer sees the killed idea and re-proposes it. So a
// rejected(issue-closed) Task holding a retained proposal gets
// DeclinedProposalRetention instead.
//
// The previous version of this test used a single -8d Task and asserted only
// that it was reaped, which passes at 24h AND at 14d - it pinned nothing. These
// cases straddle the constant, so moving it breaks them.
func TestDeclinedProposalRetentionBoundary(t *testing.T) {
	for _, tc := range []struct {
		name     string
		age      time.Duration
		wantReap bool
	}{
		{"past RejectedRetention but inside the decline window", tatarav1alpha1.RejectedRetention + time.Hour, false},
		{"one hour short of the decline window", tatarav1alpha1.DeclinedProposalRetention - time.Hour, false},
		{"one hour past the decline window", tatarav1alpha1.DeclinedProposalRetention + time.Hour, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			proj := reapProject("declprop")
			repo := reapRepo("declprop", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")

			// The shape recordProposalDecline leaves behind: the Task is
			// rejected(issue-closed) and no longer lists the issue (the sever cleared
			// IssueRefs), the mirror is closed, stamped rejected, and still owned.
			task := reapTask("declprop", "clarify-task", "clarify",
				tatarav1alpha1.StageRejected, stage.ReasonIssueClosed, time.Now().Add(-tc.age))
			iss := reapProposalIssue("declprop", repo.Name, task.Name, "closed", "rejected", 12)

			c := newMirrorClient(t, proj, repo, reapSecret(), task, iss)
			// ZERO forge writes: releaseTerminal comments and labels OPEN issues only,
			// and this one is closed. A panicking fake is the assertion.
			r := reapReconciler(c, &reapWriter{})
			require.NoError(t, r.ReapTerminal(ctx, proj))

			_, alive := mustGetTask(t, c, task.Name)
			require.Equal(t, tc.wantReap, !alive,
				"reaped=%v at age %v, want %v", !alive, tc.age, tc.wantReap)

			got := mustGetIssue(t, c, iss.Name)
			owner, owned := own.ControllerOwner(got)
			require.True(t, owned, "the retained mirror must keep its ownerRef in every case")
			require.Equal(t, task.Name, owner,
				"it cascades with the reaped Task: that is what bounds retained declined history")
			require.Equal(t, "declined", proposalDisplayStatus(got))
		})
	}
}

// TestDeclinedProposalHoldIsScopedToTheRetainedShape: the exception must not
// become a general 14-day rejected retention.
//
// The key is the RETAINED SHAPE - a closed, brainstorm-provenance mirror this
// Task still owns but no longer LISTS - and not the stage reason, which a
// parked-owner decline never carries. These four neighbours each miss the shape
// by one clause and keep the generic 24h.
func TestDeclinedProposalHoldIsScopedToTheRetainedShape(t *testing.T) {
	for _, tc := range []struct {
		name        string
		withIssue   bool
		kind        string
		state       string
		stillListed bool
	}{
		{name: "owns no mirror at all"},
		{name: "owns a NON-proposal mirror", withIssue: true, state: "closed"},
		{name: "owns a brainstorm mirror it is STILL WORKING", withIssue: true,
			kind: tatarav1alpha1.ProposalKindBrainstorm, state: "closed", stillListed: true},
		{name: "owns an OPEN brainstorm mirror", withIssue: true,
			kind: tatarav1alpha1.ProposalKindBrainstorm, state: "open"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			proj := reapProject("scopeprop")
			repo := reapRepo("scopeprop", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")

			task := reapTask("scopeprop", "clarify-task", "clarify",
				tatarav1alpha1.StageRejected, stage.ReasonIssueClosed,
				time.Now().Add(-tatarav1alpha1.RejectedRetention-time.Hour))
			objs := []client.Object{proj, repo, reapSecret(), task}
			if tc.withIssue {
				iss := reapProposalIssue("scopeprop", repo.Name, task.Name, tc.state, "rejected", 13)
				if tc.kind == "" {
					iss.Spec.ProposalKind, iss.Spec.ProposalBodyHash = "", ""
					iss.Status.Body = "an ordinary issue"
				}
				if tc.stillListed {
					task.Status.IssueRefs = []string{iss.Name}
				}
				objs = append(objs, iss)
			}
			c := newMirrorClient(t, objs...)
			w := &reapWriter{
				comment:  func(string, string) error { return nil },
				addLabel: func(string, string) error { return nil },
			}
			r := reapReconciler(c, w)
			require.NoError(t, r.ReapTerminal(ctx, proj))

			_, alive := mustGetTask(t, c, task.Name)
			require.False(t, alive,
				"only a Task holding a mirror in the RETAINED shape gets the longer window")
		})
	}
}

// TestParkedDeclineMirrorSurvivesToTheSameBoundary is the measurement the round-1
// report asked for, turned into a regression test.
//
// Measured before the fix: a proposal closed while its owner was
// parked(backlog-sweep) had its mirror cascaded by the NEXT ReapTerminal pass -
// one minute after the close - because that branch has no age gate at all (it
// collects as soon as every owned Issue is closed) and the decline was never
// recorded, so nothing held it. The declined row survived minutes.
//
// The boundary cases are the point: a value like -8d would pass at 24h, at 14d
// and at no retention whatsoever for the first case, so it would pin nothing.
func TestParkedDeclineMirrorSurvivesToTheSameBoundary(t *testing.T) {
	for _, tc := range []struct {
		name         string
		sinceClose   time.Duration
		wantMirror   bool
		wantTaskGone bool
	}{
		{"one minute after the close", time.Minute, true, false},
		{"one hour short of the decline window", tatarav1alpha1.DeclinedProposalRetention - time.Hour, true, false},
		{"one hour past the decline window", tatarav1alpha1.DeclinedProposalRetention + time.Hour, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			proj := reapProject("parkdecl")
			repo := reapRepo("parkdecl", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")

			// The Task has been parked(backlog-sweep) for MONTHS - that park never ages
			// out - and the decline happened sinceClose ago. The window has to be
			// anchored on the decline, or it expired long before the maintainer acted.
			task := reapTask("parkdecl", "clarify-task", "clarify",
				tatarav1alpha1.StageParked, stage.ReasonBacklogSweep, time.Now().Add(-90*24*time.Hour))
			task.Annotations = map[string]string{
				AnnProposalDeclinedAt: time.Now().Add(-tc.sinceClose).UTC().Format(time.RFC3339),
			}
			// The retained shape: closed, rejected, still owned, NOT in IssueRefs.
			iss := reapProposalIssue("parkdecl", repo.Name, task.Name, "closed", "rejected", 31)

			c := newMirrorClient(t, proj, repo, reapSecret(), task, iss)
			r := reapReconciler(c, &reapWriter{})
			require.NoError(t, r.ReapTerminal(ctx, proj))

			_, alive := mustGetTask(t, c, task.Name)
			require.Equal(t, tc.wantTaskGone, !alive,
				"task reaped=%v at %v after the close, want %v", !alive, tc.sinceClose, tc.wantTaskGone)

			var got tatarav1alpha1.Issue
			err := c.Get(ctx, types.NamespacedName{Namespace: testNS, Name: iss.Name}, &got)
			if tc.wantMirror {
				require.NoError(t, err, "the declined mirror must outlive the reap pass")
				owner, owned := own.ControllerOwner(&got)
				require.True(t, owned)
				require.Equal(t, task.Name, owner, "it is still bounded by its owner's reap")
			} else {
				// The fake client runs no GC, so the cascade is asserted at its cause:
				// the Task is gone and the mirror's only owner went with it.
				require.NoError(t, err)
				gotOwner, gotOwned := own.ControllerOwner(&got)
				require.True(t, gotOwned && gotOwner == task.Name,
					"the ownerRef is kept, so the deleted Task cascades the mirror")
			}
		})
	}
}

// TestParkedTaskStillWorkingItsIssueIsNotHeld: the hold keys on the RETAINED
// shape, so a parked Task that still LISTS its closed issue - an ordinary park,
// no decline recorded - reaps on its normal schedule.
func TestParkedTaskStillWorkingItsIssueIsNotHeld(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("notheld")
	repo := reapRepo("notheld", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")

	task := reapTask("notheld", "clarify-task", "clarify",
		tatarav1alpha1.StageParked, stage.ReasonBacklogSweep, time.Now().Add(-time.Hour))
	task.Annotations = map[string]string{AnnProposalDeclinedAt: time.Now().Format(time.RFC3339)}
	iss := reapProposalIssue("notheld", repo.Name, task.Name, "closed", "rejected", 32)
	// STILL LISTED: this is not a severed retention.
	task.Status.IssueRefs = []string{iss.Name}

	c := newMirrorClient(t, proj, repo, reapSecret(), task, iss)
	r := reapReconciler(c, &reapWriter{})
	require.NoError(t, r.ReapTerminal(ctx, proj))

	_, alive := mustGetTask(t, c, task.Name)
	require.False(t, alive,
		"a backlog-sweep park whose owned issues are all closed reaps as before when no retention is in play")
}

// TestHeldTaskStillReleasesItsOtherArtifacts: the hold defers the DELETE, not the
// terminal sequence.
//
// Asked once at the top of reapParked it also skipped releaseTerminal, so a held
// Task's OTHER open issues went up to DeclinedProposalRetention with no terminal
// comment, no tatara-parked label and no ownerRef release - starving unrelated
// artifacts to keep one mirror queryable. The Task here is past ParkRetention and
// holds a retained decline, so it must be kept alive AND still release the
// unrelated open issue it owns.
func TestHeldTaskStillReleasesItsOtherArtifacts(t *testing.T) {
	ctx := context.Background()
	proj := reapProject("heldrel")
	repo := reapRepo("heldrel", "tatara-operator", "https://github.com/szymonrychu/tatara-operator.git")

	task := reapTask("heldrel", "clarify-task", "clarify",
		tatarav1alpha1.StageParked, stage.ReasonAwaitingHuman, time.Now().Add(-8*24*time.Hour))
	task.Annotations = map[string]string{AnnProposalDeclinedAt: time.Now().Format(time.RFC3339)}

	// The retained decline (severed: not in IssueRefs).
	declined := reapProposalIssue("heldrel", repo.Name, task.Name, "closed", "rejected", 41)
	// An UNRELATED open issue the Task is still working.
	other := reapProposalIssue("heldrel", repo.Name, task.Name, "open", "new", 42)
	other.Spec.ProposalKind, other.Spec.ProposalBodyHash = "", ""
	other.Status.Body, other.Status.Author = "an ordinary issue", "alice"
	task.Status.IssueRefs = []string{other.Name}

	c := newMirrorClient(t, proj, repo, reapSecret(), task, declined, other)
	w := &reapWriter{
		comment:  func(string, string) error { return nil },
		addLabel: func(string, string) error { return nil },
	}
	r := reapReconciler(c, w)
	require.NoError(t, r.ReapTerminal(ctx, proj))

	_, alive := mustGetTask(t, c, task.Name)
	require.True(t, alive, "the hold must keep the Task alive for its retained decline")

	require.Len(t, w.comments, 1, "the unrelated OPEN issue still gets its terminal comment: %v", w.comments)
	require.Contains(t, w.comments[0], "#42")
	require.Len(t, w.labels, 1, "and its tatara-parked label: %v", w.labels)
	require.Contains(t, w.labels[0], "#42")

	// The unrelated issue is released (fix H13 orphan), while the declined mirror
	// keeps the ownerRef that bounds it.
	require.Empty(t, mustGetIssue(t, c, other.Name).OwnerReferences,
		"the unrelated open issue must be re-mintable now, not in 14 days")
	heldOwner, heldOwned := own.ControllerOwner(mustGetIssue(t, c, declined.Name))
	require.True(t, heldOwned && heldOwner == task.Name,
		"the declined mirror keeps the ownerRef that bounds it")
}
