package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// ISSUE #578. ownMRsShippedEdge is the NON-review sibling of terminalMREdge, for
// the shape that burned mt-i-mtg-decks-22: a kind=issue Task at awaiting-review
// whose own MR was merged out of band. terminalMREdge is kind-locked to review,
// so before this there was NO finalize for that Task at all - and no outcome
// could commit either, so reviewAdvanceEdge (the only other thing that would
// have moved it) was never reached. Permanent deadlock, re-armed by every human
// comment on the driving issue.
func TestOwnMRsShippedEdge(t *testing.T) {
	merged := tatarav1alpha1.MergeRequest{Status: tatarav1alpha1.MergeRequestStatus{State: "merged"}}
	closed := tatarav1alpha1.MergeRequest{Status: tatarav1alpha1.MergeRequestStatus{State: "closed"}}
	open := tatarav1alpha1.MergeRequest{Status: tatarav1alpha1.MergeRequestStatus{State: "open"}}
	owed := tatarav1alpha1.MergeRequest{Status: tatarav1alpha1.MergeRequestStatus{
		State:         "merged",
		PendingReview: &tatarav1alpha1.PendingReview{Body: "b", SHA: "sha1", Round: 1},
	}}

	tests := []struct {
		name       string
		kind       string
		mrs        []tatarav1alpha1.MergeRequest
		wantOK     bool
		wantTo     string
		wantReason string
	}{
		{
			// THE LIVE SHAPE. `merged`, not `done`: the work SHIPPED but is not
			// DELIVERED - the merge-order cursor, the deploy ledger, the issue
			// closes and status.deliveredAt all live past `merged`, and LegalFor
			// GUARD 3 refuses awaiting-review -> done for anything but kind=review
			// for exactly that reason. ReconcileMerging's idempotent already-merged
			// branch is what this lands on.
			name: "kind=issue, every owned MR merged out of band", kind: "issue",
			mrs: []tatarav1alpha1.MergeRequest{merged}, wantOK: true,
			wantTo: tatarav1alpha1.StateMerged,
		},
		{
			name: "kind=implement, multi-repo, all merged", kind: "implement",
			mrs: []tatarav1alpha1.MergeRequest{merged, merged}, wantOK: true,
			wantTo: tatarav1alpha1.StateMerged,
		},
		{
			name: "terminal but one abandoned -> rejected", kind: "issue",
			mrs: []tatarav1alpha1.MergeRequest{merged, closed}, wantOK: true,
			wantTo: tatarav1alpha1.StateRejected, wantReason: stage.ReasonMRClosedExternally,
		},
		{
			name: "all closed -> rejected", kind: "implement",
			mrs: []tatarav1alpha1.MergeRequest{closed}, wantOK: true,
			wantTo: tatarav1alpha1.StateRejected, wantReason: stage.ReasonMRClosedExternally,
		},
		{
			name: "one still open -> no finalize", kind: "issue",
			mrs: []tatarav1alpha1.MergeRequest{merged, open},
		},
		{
			// An empty set is a DIFFERENT, pre-existing condition (the mint/binding
			// fault intake repairs) and is deliberately out of scope, exactly as it
			// is for terminalMREdge and AllMRsTerminal.
			name: "no MRs at all -> no finalize", kind: "issue",
		},
		{
			// terminalMREdge owns the review kind and must keep owning it: it
			// finalizes at done(mr-merged-externally), which is a different verdict.
			name: "kind=review is terminalMREdge's, never this one", kind: "review",
			mrs: []tatarav1alpha1.MergeRequest{merged},
		},
		{
			// LegalFor GUARD 2 (reviewGateOpen) refuses awaiting-review -> merged
			// while any owned MR still owes a review to the forge, so firing here
			// could only produce an IllegalTransitionError. The ordinary
			// DrainPendingReview -> advanceAfterReview path owns that decision.
			name: "a review is still owed -> no finalize", kind: "issue",
			mrs: []tatarav1alpha1.MergeRequest{owed},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := mdTask("t1", tc.kind, tatarav1alpha1.StateAwaitingReview)
			edge, ok := ownMRsShippedEdge(task, tc.mrs)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if !tc.wantOK {
				return
			}
			if edge.To != tc.wantTo || edge.Reason != tc.wantReason {
				t.Fatalf("edge = %q/%q, want %q/%q", edge.To, edge.Reason, tc.wantTo, tc.wantReason)
			}
			// Whatever it emits must be a LEGAL edge for this Task, or the
			// finalize is just an IllegalTransitionError on a requeue loop.
			if !stage.LegalFor(task, tc.mrs, tatarav1alpha1.StateAwaitingReview, edge.To) {
				t.Fatalf("awaiting-review -> %s is not legal for kind=%s", edge.To, tc.kind)
			}
		})
	}
}

// externalTerminalEdge is the ONE resolution both convergent finalize sites
// call. The two halves are mutually exclusive by kind, and this pins that: a
// review Task keeps done(mr-merged-externally), everything else gets `merged`.
func TestExternalTerminalEdge_RoutesByKind(t *testing.T) {
	merged := []tatarav1alpha1.MergeRequest{
		{Status: tatarav1alpha1.MergeRequestStatus{State: "merged"}}}

	edge, ok := externalTerminalEdge(mdTask("r", "review", tatarav1alpha1.StateAwaitingReview), merged)
	if !ok || edge.To != tatarav1alpha1.StateDone || edge.Reason != stage.ReasonMRMergedExternally {
		t.Fatalf("review: edge = %q/%q ok=%v, want done/mr-merged-externally", edge.To, edge.Reason, ok)
	}
	edge, ok = externalTerminalEdge(mdTask("i", "issue", tatarav1alpha1.StateAwaitingReview), merged)
	if !ok || edge.To != tatarav1alpha1.StateMerged {
		t.Fatalf("issue: edge = %q/%q ok=%v, want merged", edge.To, edge.Reason, ok)
	}
}

// The reconcileClocks half of the convergent finalize, on the LIVE shape: a
// kind=issue Task parked nowhere, sitting at awaiting-review, whose only MR a
// human merged. Before #578 this returned handled=false and the Task sat there
// forever, respawning a review pod that could only be told "no open MR".
func TestReconcileClocks_FinalizesIssueTaskWhoseMRMergedExternally(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	proj := tsProject(3)
	task := tsTask("mt-i-22", "issue", tatarav1alpha1.StateAwaitingReview, now.Add(-time.Minute))
	mr := mdMR(task, "mtg-decks", 30)
	mr.Status.State = "merged"

	c := newMirrorClient(t, proj, mdSecret(), task, mr)
	r := tsReconciler(c)

	_, handled, err := r.reconcileClocks(ctx, proj, task, now)
	if err != nil {
		t.Fatalf("reconcileClocks: %v", err)
	}
	if !handled {
		t.Fatal("reconcileClocks must finalize a non-review Task whose every owned MR merged externally")
	}
	got := mdGetTask(t, c, "mt-i-22")
	if got.Status.State != tatarav1alpha1.StateMerged {
		t.Fatalf("state = %q, want merged: the shipped work still owes the deploy ledger and the issue closes",
			got.Status.State)
	}
}

// The pre-dispatch half: the Task must be finalized POD-LESSLY. Spawning the
// review pod at all is what cost seven pod runs and three pod recreations, since
// every one of them re-ran the same doomed turn.
func TestEnsureStagePod_SpawnsNoPodForAnIssueTaskWhoseMRMerged(t *testing.T) {
	ctx := context.Background()
	proj := tsProject(3)
	task := tsTask("mt-i-23", "issue", tatarav1alpha1.StateAwaitingReview, time.Now())
	mr := mdMR(task, "mtg-decks", 31)
	mr.Status.State = "merged"

	c := newMirrorClient(t, proj, mdSecret(), task, mr)
	r := tsReconciler(c)

	skipped, err := r.ensureStagePod(ctx, proj, task)
	if err != nil {
		t.Fatalf("ensureStagePod: %v", err)
	}
	if !skipped {
		t.Fatal("ensureStagePod must report skipped=true after a pod-less finalize")
	}
	var pod corev1.Pod
	err = c.Get(ctx, types.NamespacedName{Namespace: mdNS, Name: agent.PodName(task)}, &pod)
	if err == nil {
		t.Fatal("a review pod was spawned against an already-merged MR; the pre-dispatch guard did not fire")
	}
	if !apierrors.IsNotFound(err) {
		t.Fatalf("unexpected error checking for pod: %v", err)
	}
	if got := mdGetTask(t, c, "mt-i-23"); got.Status.State != tatarav1alpha1.StateMerged {
		t.Fatalf("state = %q, want merged", got.Status.State)
	}
}

// The guard must not fire on a Task still mid-flight: one MR merged, one open is
// the ordinary multi-repo merge order, and finalizing there would strand the
// unmerged repo.
func TestEnsureStagePod_DoesNotFinalizeWhileAnMRIsStillOpen(t *testing.T) {
	ctx := context.Background()
	proj := tsProject(3)
	task := tsTask("mt-i-24", "issue", tatarav1alpha1.StateAwaitingReview, time.Now())
	a := mdMR(task, "mtg-decks", 32)
	a.Status.State = "merged"
	b := mdMR(task, "mtg-cards", 33)

	c := newMirrorClient(t, proj, mdSecret(), task, a, b)
	r := tsReconciler(c)

	if _, err := r.ensureStagePod(ctx, proj, task); err != nil {
		t.Fatalf("ensureStagePod: %v", err)
	}
	if got := mdGetTask(t, c, "mt-i-24"); got.Status.State != tatarav1alpha1.StateAwaitingReview {
		t.Fatalf("state = %q, want awaiting-review: a half-merged multi-repo task must not finalize",
			got.Status.State)
	}
}
