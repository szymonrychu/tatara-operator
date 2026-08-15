package controller

import (
	"context"
	"strconv"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// seedAdoptedUpgradeMR persists the steady state of an ADOPTED dependency merge
// request: a tatara-owned, open mirror on the engine's own branch whose
// controller owner is an upgrade Task minted ONTO it (spec.source.isPR with a
// number, which is stage.AdoptedMR), sitting in awaiting-review where the review
// pod decides.
func seedAdoptedUpgradeMR(t *testing.T, ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, number int, lastBotHeadSHA string) *tatarav1alpha1.MergeRequest {
	t.Helper()

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      AdoptedUpgradeTaskName(proj.Name, repo.Name, number),
			Namespace: proj.Namespace,
			Annotations: map[string]string{
				tatarav1alpha1.AnnTakeoverHeadBranch: "renovate/cilium",
			},
		},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    proj.Name,
			RepositoryRef: repo.Name,
			Kind:          "upgrade",
			Goal:          "review the adopted bump",
			MergeOrder:    []string{repo.Name},
			Source: &tatarav1alpha1.TaskSource{
				Provider: providerOf(proj), Number: number, IsPR: true,
				Title: "chore(deps): update cilium",
			},
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create adopted task: %v", err)
	}
	if !stage.AdoptedMR(task) {
		t.Fatal("the fixture Task is not stage.AdoptedMR; the test proves nothing")
	}
	stampTaskStatus(t, ctx, task, tatarav1alpha1.StateAwaitingReview, "")

	mr := &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tatarav1alpha1.MergeRequestName(repo.Name, number),
			Namespace: proj.Namespace,
		},
		Spec: tatarav1alpha1.MergeRequestSpec{
			RepositoryRef: repo.Name, ProjectRef: proj.Name, Number: number,
			URL: "https://github.com/o/r/pull/" + strconv.Itoa(number),
		},
	}
	if err := controllerutil.SetControllerReference(task, mr, k8sClient.Scheme()); err != nil {
		t.Fatalf("set controller ref: %v", err)
	}
	if err := k8sClient.Create(ctx, mr); err != nil {
		t.Fatalf("create adopted mirror: %v", err)
	}
	mr.Status = tatarav1alpha1.MergeRequestStatus{
		Author:          proj.Spec.Scm.BotLogin,
		State:           "open",
		HeadBranch:      "renovate/cilium",
		HeadSHA:         lastBotHeadSHA,
		LastBotHeadSHA:  lastBotHeadSHA,
		Ownership:       tatarav1alpha1.OwnershipTatara,
		OwnershipReason: "initial",
		Significance:    adoptedSignificanceFloor,
	}
	if err := k8sClient.Status().Update(ctx, mr); err != nil {
		t.Fatalf("stamp adopted mirror status: %v", err)
	}
	return mr
}

// A HUMAN PUSH TO AN ADOPTED MERGE REQUEST MEANS THE OPERATOR WILL NOT MERGE IT.
//
// flipToExternal used to stamp `external-push:<sha>` for every owner, and that
// reason is one mergeAllowedForOwnership ACCEPTS and DrainStandDownMerge
// re-drives. On a takeover that is correct and deliberate: a maintainer ASKED
// the platform to take the merge request over, so merge-on-approve continues
// across their push. Nobody asked for anything here - adoption is automatic - so
// the same semantics would hand a human's commits to the merge corridor on the
// next approve, on the strength of a decision no human made.
func TestReconcileOwnership_AnAdoptedMRStandsDownIntoHumanMergedOnly(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	mr := seedAdoptedUpgradeMR(t, ctx, proj, repo, 81, "engine-head")

	flipped, err := d.ReconcileOwnership(ctx, proj, repo, mr, "a-humans-commit", nil)
	if err != nil || !flipped {
		t.Fatalf("expected a flip on an unattributable push, got flipped=%v err=%v", flipped, err)
	}
	got := getMR(t, ctx, proj, repo, 81)
	if got.Status.Ownership != tatarav1alpha1.OwnershipExternal {
		t.Fatalf("ownership = %q, want external", got.Status.Ownership)
	}
	if got.Status.OwnershipReason != adoptedPushReasonPrefix+"a-humans-commit" {
		t.Fatalf("ownershipReason = %q, want %q: an adopted merge request a human pushed to must "+
			"carry a reason distinct from the takeover stand-down",
			got.Status.OwnershipReason, adoptedPushReasonPrefix+"a-humans-commit")
	}
	if mergeAllowedForOwnership(got) {
		t.Fatal("the operator is still allowed to merge an adopted merge request a human pushed to")
	}
}

// The other half of the same fact: the drain must not re-drive the parked
// adopted Task into `merging` when the next review approve lands on the human's
// head.
func TestDrainStandDownMerge_RefusesToReDriveAnAdoptedMR(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	mr := seedAdoptedUpgradeMR(t, ctx, proj, repo, 82, "engine-head")

	if _, err := d.ReconcileOwnership(ctx, proj, repo, mr, "a-humans-commit", nil); err != nil {
		t.Fatalf("flip: %v", err)
	}
	// The review agent approves the human's head, which is what re-drives a
	// stood-down TAKEOVER merge request into the merge corridor.
	got := getMR(t, ctx, proj, repo, 82)
	got.Status.Status = "approved"
	got.Status.HeadSHA = "a-humans-commit"
	got.Status.ReviewedSHA = "a-humans-commit"
	if err := k8sClient.Status().Update(ctx, got); err != nil {
		t.Fatalf("record the approve: %v", err)
	}

	if err := d.DrainStandDownMerge(ctx, proj, repo, got); err != nil {
		t.Fatalf("DrainStandDownMerge: %v", err)
	}

	var task tatarav1alpha1.Task
	name := AdoptedUpgradeTaskName(proj.Name, repo.Name, 82)
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: proj.Namespace, Name: name}, &task); err != nil {
		t.Fatalf("get adopted task: %v", err)
	}
	if !tatarav1alpha1.Parked(&task) || task.Status.ParkReason != stage.ReasonOwnershipLost {
		t.Fatalf("the adopted Task was re-driven off its ownership-lost park: state=%q park=%q. "+
			"The operator is about to merge a human's commits on a merge request nobody asked it to take over.",
			task.Status.State, task.Status.ParkReason)
	}
	if task.Status.State == tatarav1alpha1.StateMerged {
		t.Fatal("the adopted Task reached `merged` after a human push")
	}
}

// THE TAKEOVER SEMANTICS ARE UNTOUCHED. A maintainer's explicit take-over
// request is the case merge-after-stand-down was written for, and it must keep
// stamping the reason both mergeAllowedForOwnership and the drain accept.
func TestReconcileOwnership_TakeoverStandDownKeepsItsMergeableReason(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	mr := seedTataraOwnedMRWithTakeoverTask(t, ctx, proj, repo, 83, "tatara/feat-83", "bot-head")

	if _, err := d.ReconcileOwnership(ctx, proj, repo, mr, "human-head", nil); err != nil {
		t.Fatalf("flip: %v", err)
	}
	got := getMR(t, ctx, proj, repo, 83)
	if got.Status.OwnershipReason != externalPushReasonPrefix+"human-head" {
		t.Fatalf("takeover reason = %q, want %q", got.Status.OwnershipReason, externalPushReasonPrefix+"human-head")
	}
	if !mergeAllowedForOwnership(got) {
		t.Fatal("a stood-down TAKEOVER merge request is no longer mergeable; the adoption fix weakened it")
	}
}

// A REAL TAKEOVER TASK CARRIES spec.source.isPR WITH A NUMBER TOO. stage.AdoptedMR
// is therefore TRUE for it (and for a kind=review Task on a human's pull
// request), so a discriminator built on AdoptedMR alone would silently strip
// merge-after-stand-down from every takeover on the platform. The fixture in
// ownership_test.go has no source at all and cannot catch that.
func TestReconcileOwnership_ARealTakeoverTaskKeepsItsMergeableReason(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	mr := seedTataraOwnedMRWithTakeoverTask(t, ctx, proj, repo, 84, "contrib/fix-84", "bot-head")

	// Stamp the source the takeover mint actually writes.
	var tk tatarav1alpha1.Task
	name := takeoverTaskName(proj, repo, 84)
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: proj.Namespace, Name: name}, &tk); err != nil {
		t.Fatalf("get takeover task: %v", err)
	}
	tk.Spec.Source = &tatarav1alpha1.TaskSource{
		Provider: providerOf(proj), Number: 84, IsPR: true, Title: "external change #84",
	}
	if err := k8sClient.Update(ctx, &tk); err != nil {
		t.Fatalf("stamp takeover source: %v", err)
	}
	if !stage.AdoptedMR(&tk) {
		t.Fatal("the fixture must be AdoptedMR-true; that is the whole point of this test")
	}

	if _, err := d.ReconcileOwnership(ctx, proj, repo, mr, "human-head", nil); err != nil {
		t.Fatalf("flip: %v", err)
	}
	got := getMR(t, ctx, proj, repo, 84)
	if got.Status.OwnershipReason != externalPushReasonPrefix+"human-head" {
		t.Fatalf("takeover reason = %q, want %q: a takeover a maintainer ASKED for must keep "+
			"merge-after-stand-down", got.Status.OwnershipReason, externalPushReasonPrefix+"human-head")
	}
	if !mergeAllowedForOwnership(got) {
		t.Fatal("a stood-down TAKEOVER merge request is no longer mergeable")
	}
}
