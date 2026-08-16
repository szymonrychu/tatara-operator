package controller

import (
	"context"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/stage"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	if _, _, err := m.MintAdoptedUpgradeTask(ctx, proj, repo, adoptedPR(61), testSpiller(t), nil); err != nil {
		t.Fatalf("mint: %v", err)
	}
	var mr tatarav1alpha1.MergeRequest
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: proj.Namespace,
		Name: tatarav1alpha1.MergeRequestName(repo.Name, 61)}, &mr); err != nil {
		t.Fatalf("get mirror: %v", err)
	}
	if mr.Status.Significance != AdoptedSignificanceFloor {
		t.Fatalf("mirror significance = %q, want %q: an adopted merge request approved on "+
			"its first review has no other writer, and an empty significance means CI cuts no tag",
			mr.Status.Significance, AdoptedSignificanceFloor)
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

	task, _, err := m.MintAdoptedUpgradeTask(ctx, proj, repo, pr, testSpiller(t), nil)
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

// THE FLOOR IS A FLOOR, NOT A CEILING - see
// TestOutcome_Review_EscalatesTheSeededAdoptedFloor (internal/restapi), which
// drives the REAL review outcome handler against a mirror seeded at this floor.
// It lives there and not here because the escalation clause and its rank table
// are restapi's, and a copy of that table asserted against itself proves
// nothing about either.

// THE SEED MUST CONVERGE, because everything else about the mint already does.
//
// The seed is a SEPARATE write after the Task and the mirror both exist, so an
// interrupted mint - FitMergeRequest exhausting its conflict retries, or
// ErrObjectTooLarge on a fat mirror - leaves both objects persisted with no
// floor on them. The next sweep then finds the Task by its deterministic name
// and, on the early MintExistingLive return, never retries anything: the merge
// request merges with an empty significance, CI cuts no tag and the Task sits in
// `deploying`. That is the exact CRITICAL wedge the floor was added to close,
// reintroduced through the back door.
func TestMintAdoptedUpgradeTask_ConvergesAnInterruptedMintOnTheNextPass(t *testing.T) {
	ctx := context.Background()
	proj, repo := adoptProject(t, ctx)
	m := newTestMinter(t)
	pr := adoptedPR(64)

	// The state an interrupted mint leaves behind: the Task is live under its
	// deterministic natural key, and NOTHING else happened.
	task, _, err := m.MintAdoptedUpgradeTask(ctx, proj, repo, pr, testSpiller(t), nil)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	key := client.ObjectKey{Namespace: proj.Namespace, Name: tatarav1alpha1.MergeRequestName(repo.Name, pr.Number)}
	if err := k8sClient.Delete(ctx, &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name}}); err != nil {
		t.Fatalf("drop the mirror to simulate the interrupted mint: %v", err)
	}
	if err := m.stampMintStatus(ctx, task, func(fresh *tatarav1alpha1.Task) {
		fresh.Status.MRRefs = nil
	}); err != nil {
		t.Fatalf("clear mrRefs: %v", err)
	}

	// The next sweep pass, byte-identical to the first.
	if _, outcome, err := m.MintAdoptedUpgradeTask(ctx, proj, repo, pr, testSpiller(t), nil); err != nil {
		t.Fatalf("second mint: %v", err)
	} else if outcome != MintExistingLive {
		t.Fatalf("second mint outcome = %q, want existing_live", outcome)
	}

	var mr tatarav1alpha1.MergeRequest
	if err := k8sClient.Get(ctx, key, &mr); err != nil {
		t.Fatalf("the interrupted mint never re-created its mirror: %v", err)
	}
	if mr.Status.Significance != AdoptedSignificanceFloor {
		t.Fatalf("mirror significance = %q, want %q: the seed is never retried, so this merge "+
			"request merges with no semver label, CI cuts no tag and the Task wedges in deploying",
			mr.Status.Significance, AdoptedSignificanceFloor)
	}
	if owner, ok := own.ControllerOwner(&mr); !ok || owner != task.Name {
		t.Fatalf("mirror controller owner = %q (owned=%v), want %q", owner, ok, task.Name)
	}
}
