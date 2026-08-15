package controller

import (
	"context"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// THE COMMON PATH USED TO PUBLISH NOTHING AT ALL.
//
// The two writers of MergeRequestStatus.Significance are the implement/upgrade
// `submitted` outcome and the review outcome's OPTIONAL escalation. On an
// adopted merge request APPROVED AT FIRST REVIEW - which is the common case and
// the whole point of the design - neither runs: no upgrade turn ever happens
// and the reviewer declares nothing. reconcileMerge then calls that state "an
// operator bug", merges anyway, and CI cuts NO tag: nothing publishes, no pin
// propagates, deployedAt is never stamped, and the Task sits in `deploying`
// until its budget parks it.
//
// The mint seeds the FLOOR so the common path can never be that state.
func TestMintAdoptedUpgradeTask_SeedsTheSemverFloorOnItsMirror(t *testing.T) {
	ctx := context.Background()
	proj, repo := adoptProject(t, ctx)
	m := newTestMinter(t)

	if _, _, err := m.MintAdoptedUpgradeTask(ctx, proj, repo, adoptedPR(61), testSpiller(t)); err != nil {
		t.Fatalf("mint: %v", err)
	}
	var mr tatarav1alpha1.MergeRequest
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: proj.Namespace,
		Name: tatarav1alpha1.MergeRequestName(repo.Name, 61)}, &mr); err != nil {
		t.Fatalf("get mirror: %v", err)
	}
	if mr.Status.Significance != adoptedSignificanceFloor {
		t.Fatalf("mirror significance = %q, want %q: an adopted merge request approved on "+
			"its first review has no other writer, and an empty significance means CI cuts no tag",
			mr.Status.Significance, adoptedSignificanceFloor)
	}
}

// THE END-TO-END SHAPE THE FLOOR EXISTS FOR: mint, approve on the FIRST review
// with no change_significance declared, take the awaiting-review -> merged edge,
// and arrive at the merge corridor with a significance in hand.
func TestAdoptedUpgradeApprovedOnFirstReviewReachesMergedWithASignificance(t *testing.T) {
	ctx := context.Background()
	proj, repo := adoptProject(t, ctx)
	m := newTestMinter(t)
	pr := adoptedPR(62)

	task, _, err := m.MintAdoptedUpgradeTask(ctx, proj, repo, pr, testSpiller(t))
	if err != nil {
		t.Fatalf("mint: %v", err)
	}

	// The review agent's approve, replayed EXACTLY as restapi's review arm
	// writes it (internal/restapi/outcome.go): the verdict, the reviewed SHA,
	// and the escalation clause - which is `if sig != "" && rank[sig] >
	// rank[current]` and therefore writes NOTHING when the reviewer declares no
	// change_significance. That is the whole defect.
	key := client.ObjectKey{Namespace: proj.Namespace, Name: tatarav1alpha1.MergeRequestName(repo.Name, pr.Number)}
	var mr tatarav1alpha1.MergeRequest
	if err := k8sClient.Get(ctx, key, &mr); err != nil {
		t.Fatalf("get mirror: %v", err)
	}
	mr.Status.Status = "approved"
	mr.Status.ReviewedSHA = pr.HeadSHA
	mr.Status.HeadSHA = pr.HeadSHA
	mr.Status.PendingReview = nil
	if err := k8sClient.Status().Update(ctx, &mr); err != nil {
		t.Fatalf("record the approve: %v", err)
	}

	// The Task takes the review lane's exit into the merge corridor.
	stampTaskStatus(t, ctx, task, tatarav1alpha1.StateAwaitingReview, "")
	if !stage.LegalFor(task, []tatarav1alpha1.MergeRequest{mr},
		tatarav1alpha1.StateAwaitingReview, tatarav1alpha1.StateMerged) {
		t.Fatal("awaiting-review -> merged is refused for an approved adopted upgrade Task")
	}

	// reconcileMerge's own wedge check (internal/controller/merge.go): an empty
	// significance here is the "no label -> no tag -> nothing publishes" state.
	if err := k8sClient.Get(ctx, key, &mr); err != nil {
		t.Fatalf("re-get mirror: %v", err)
	}
	if mr.Status.Significance == "" {
		t.Fatal("the adopted merge request reached the merge corridor with NO change significance: " +
			"the operator merges it, CI cuts no release tag, nothing publishes and the Task " +
			"sits in deploying until its budget parks it")
	}
}

// THE FLOOR IS A FLOOR, NOT A CEILING. A reviewer who reads a breaking change in
// the changelog raises it with change_significance, and the escalation clause
// that already exists must still win over the seeded patch.
func TestAdoptedSemverFloorIsRaisedByAReviewEscalation(t *testing.T) {
	ctx := context.Background()
	proj, repo := adoptProject(t, ctx)
	m := newTestMinter(t)
	pr := adoptedPR(63)

	if _, _, err := m.MintAdoptedUpgradeTask(ctx, proj, repo, pr, testSpiller(t)); err != nil {
		t.Fatalf("mint: %v", err)
	}
	key := client.ObjectKey{Namespace: proj.Namespace, Name: tatarav1alpha1.MergeRequestName(repo.Name, pr.Number)}
	var mr tatarav1alpha1.MergeRequest
	if err := k8sClient.Get(ctx, key, &mr); err != nil {
		t.Fatalf("get mirror: %v", err)
	}
	// restapi's rank table is {patch:1, minor:2, major:3}; the seeded floor is
	// the LOWEST rank precisely so every escalation the reviewer can declare
	// outranks it.
	if rank := map[string]int{"patch": 1, "minor": 2, "major": 3}; rank["major"] <= rank[mr.Status.Significance] {
		t.Fatalf("the seeded floor %q is not outranked by major; a review could never escalate it",
			mr.Status.Significance)
	}
}
