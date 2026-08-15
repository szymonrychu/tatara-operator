package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// adoptedPR is the listing row the sweep hands the minter: authored by the
// project's bot (seedProjectRepo sets botLogin "tatara-bot"), on the engine's
// own branch, with no mirror anywhere yet.
func adoptedPR(number int) scm.PRRef {
	return scm.PRRef{
		Repo:       "o/r",
		HeadRepo:   "o/r",
		Number:     number,
		Author:     "tatara-bot",
		Title:      fmt.Sprintf("chore(deps): update cilium to v1.17.%d", number),
		HeadBranch: "renovate/cilium",
		HeadSHA:    fmt.Sprintf("sha-%d", number),
		Body:       "### Release Notes\n\ncilium 1.17: the `hubble.relay` key was renamed.",
	}
}

// adoptProject arms seedProjectRepo's project for adoption.
func adoptProject(t *testing.T, ctx context.Context) (*tatarav1alpha1.Project, *tatarav1alpha1.Repository) {
	t.Helper()
	proj, repo := seedProjectRepo(t, ctx)
	proj.Spec.UpgradePolicy = &tatarav1alpha1.UpgradePolicySpec{
		Engine: "renovate", MajorStrategy: "nextHopOnly", AdoptBranchPrefix: "renovate/",
	}
	if err := k8sClient.Update(ctx, proj); err != nil {
		t.Fatalf("arm adoption: %v", err)
	}
	return proj, repo
}

// The Task name is a pure function of (project, kind, repo, number), so the
// mint is idempotent per merge request. This is the ONLY dedup mechanism
// adoption has, and it is total: a second call returns the same Task.
func TestMintAdoptedUpgradeTask_IsIdempotentPerMergeRequest(t *testing.T) {
	ctx := context.Background()
	proj, repo := adoptProject(t, ctx)
	m := newTestMinter(t)

	first, outcome, err := m.MintAdoptedUpgradeTask(ctx, proj, repo, adoptedPR(41), testSpiller(t))
	if err != nil {
		t.Fatalf("first mint: %v", err)
	}
	if outcome != MintCreated {
		t.Fatalf("first mint outcome = %q, want created", outcome)
	}
	second, outcome, err := m.MintAdoptedUpgradeTask(ctx, proj, repo, adoptedPR(41), testSpiller(t))
	if err != nil {
		t.Fatalf("second mint: %v", err)
	}
	if outcome != MintExistingLive {
		t.Fatalf("second mint outcome = %q, want existing_live", outcome)
	}
	if second.Name != first.Name {
		t.Fatalf("second mint made a different Task: %q vs %q", second.Name, first.Name)
	}
	if first.Name != AdoptedUpgradeTaskName(proj.Name, repo.Name, 41) {
		t.Fatalf("Task name %q is not the deterministic natural key", first.Name)
	}

	var tasks tatarav1alpha1.TaskList
	if err := k8sClient.List(ctx, &tasks, client.InNamespace(proj.Namespace)); err != nil {
		t.Fatal(err)
	}
	n := 0
	for i := range tasks.Items {
		if tasks.Items[i].Spec.ProjectRef == proj.Name {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("%d Tasks exist for this project, want exactly 1 per merge request", n)
	}
}

// The entry state. `new`, NOT under-implementation and NOT refined:
//   - under-implementation would pay an upgrade turn on every adopted merge
//     request, including the trivial pin bumps that are most of them. Review
//     first, implement only if review asks;
//   - refined can never be left by a Task owning zero Issue CRs;
//   - awaiting-review is not mintable at all - InitialState's CRD enum is
//     new;refined;under-implementation.
//
// `new` runs no pod (AgentKindFor(new, *) == ""), so the entry is free, and
// reconcileTriaging walks it to awaiting-review via the widened edge.
func TestMintAdoptedUpgradeTask_MintsAtNewSoReviewGoesFirst(t *testing.T) {
	ctx := context.Background()
	proj, repo := adoptProject(t, ctx)
	task, _, err := newTestMinter(t).MintAdoptedUpgradeTask(ctx, proj, repo, adoptedPR(42), testSpiller(t))
	if err != nil {
		t.Fatal(err)
	}
	if task.Spec.InitialState != tatarav1alpha1.StateNew {
		t.Fatalf("InitialState = %q, want new", task.Spec.InitialState)
	}
	if !stage.AdoptedMR(task) {
		t.Fatal("the minted Task must satisfy stage.AdoptedMR: it is the discriminator the review lane keys on")
	}
	if k := stage.AgentKindFor(tatarav1alpha1.StateNew, task.Spec.Kind); k != "" {
		t.Fatalf("AgentKindFor(new, upgrade) = %q, want \"\": the entry must cost no pod", k)
	}
	target, ok := triageTarget(task)
	if !ok || target != tatarav1alpha1.StateAwaitingReview {
		t.Fatalf("triageTarget = (%q, %v), want (awaiting-review, true)", target, ok)
	}
	if !stage.LegalFor(task, nil, tatarav1alpha1.StateNew, tatarav1alpha1.StateAwaitingReview) {
		t.Fatal("GUARD 5 refuses the edge this mint depends on")
	}
}

// THE MIRROR DOES NOT EXIST YET, AND THE MINT IS WHAT CREATES IT.
// ensureMergeRequestCR has exactly ONE caller, SyncMergeRequest, which has
// exactly one caller, bindMRToTask. A merge request no Task has ever bound
// therefore has NO MergeRequest CR, and Minter.mergeRequestCR returns
// (nil, nil) for it forever. A minter that REQUIRED a pre-existing CR could
// never adopt a NEW merge request at all - it would only ever work on the
// backlog, which already has mirrors from the review Tasks it minted.
func TestMintAdoptedUpgradeTask_CreatesTheMirrorItBindsTo(t *testing.T) {
	ctx := context.Background()
	proj, repo := adoptProject(t, ctx)
	m := newTestMinter(t)

	// Precondition: no mirror at all.
	if cr, err := m.mergeRequestCR(ctx, proj, repo, 43); err != nil || cr != nil {
		t.Fatalf("precondition: mergeRequestCR = (%v, %v), want (nil, nil)", cr, err)
	}
	pr := adoptedPR(43)
	task, _, err := m.MintAdoptedUpgradeTask(ctx, proj, repo, pr, testSpiller(t))
	if err != nil {
		t.Fatal(err)
	}
	mr := getMR(t, ctx, proj, repo, 43)
	if ctrl, ok := ownerControllerName(mr); !ok || ctrl != task.Name {
		t.Fatalf("the adopted Task must controller-own the mirror it created; owner=%q ok=%v", ctrl, ok)
	}
	if mr.Status.Author != pr.Author {
		t.Fatalf("mirror author = %q, want %q: ownershipForAuthor reads exactly this field",
			mr.Status.Author, pr.Author)
	}
	if mr.Status.HeadBranch != pr.HeadBranch {
		t.Fatalf("mirror head branch = %q, want %q", mr.Status.HeadBranch, pr.HeadBranch)
	}
	if mr.Status.Body != pr.Body {
		t.Fatalf("mirror body = %q, want the merge request body: it is the ONLY copy of the changelog", mr.Status.Body)
	}
	if mr.Status.Title != pr.Title {
		t.Fatalf("mirror title = %q, want %q", mr.Status.Title, pr.Title)
	}
}

// The two things the takeover path gets right and a plain mint does not.
func TestMintAdoptedUpgradeTask_BindsAndStampsTheBranch(t *testing.T) {
	ctx := context.Background()
	proj, repo := adoptProject(t, ctx)
	task, _, err := newTestMinter(t).MintAdoptedUpgradeTask(ctx, proj, repo, adoptedPR(44), testSpiller(t))
	if err != nil {
		t.Fatal(err)
	}
	if task.Spec.Kind != "upgrade" {
		t.Fatalf("kind = %q, want upgrade: the Task needs the upgrade tool and skill profiles, and it "+
			"must count against maxOpenUpgrades", task.Spec.Kind)
	}
	if task.Spec.RepositoryRef != repo.Name {
		t.Fatalf("repositoryRef = %q, want %q", task.Spec.RepositoryRef, repo.Name)
	}
	if len(task.Spec.MergeOrder) != 1 || task.Spec.MergeOrder[0] != repo.Name {
		t.Fatalf("mergeOrder = %v, want [%s]: an adopted Task is single-repo by construction",
			task.Spec.MergeOrder, repo.Name)
	}
	if task.Spec.Source == nil || !task.Spec.Source.IsPR || task.Spec.Source.Number != 44 {
		t.Fatalf("source not bound to the merge request: %+v", task.Spec.Source)
	}
	if hb := task.Annotations[tatarav1alpha1.AnnTakeoverHeadBranch]; hb != "renovate/cilium" {
		t.Fatalf("AnnTakeoverHeadBranch = %q at mint, want renovate/cilium", hb)
	}
	fresh := getTask(t, task.Name)
	want := tatarav1alpha1.MergeRequestName(repo.Name, 44)
	found := false
	for _, ref := range fresh.Status.MRRefs {
		if ref == want {
			found = true
		}
	}
	if !found {
		t.Fatalf("status.mrRefs = %v, want it to contain %q", fresh.Status.MRRefs, want)
	}
}

// THE ASSERTION THAT REPLACED THE FLIP, AND IT IS A NEGATIVE ONE. The minter
// must leave ownership ENTIRELY alone. ReconcileOwnership is "the single
// convergence function for MR ownership" and a second writer of
// Status.Ownership is exactly the kind of thing that is correct on the day it
// is written and wrong six months later.
func TestMintAdoptedUpgradeTask_StampsNoOwnershipAtAll(t *testing.T) {
	ctx := context.Background()
	proj, repo := adoptProject(t, ctx)
	before := testutil.ToFloat64(obs.OwnershipFlipCounter("to-external", "external-push"))

	if _, _, err := newTestMinter(t).MintAdoptedUpgradeTask(ctx, proj, repo, adoptedPR(45), testSpiller(t)); err != nil {
		t.Fatal(err)
	}
	mr := getMR(t, ctx, proj, repo, 45)
	if mr.Status.Ownership != "" {
		t.Errorf("Status.Ownership = %q at mint, want empty: classification is ReconcileOwnership's", mr.Status.Ownership)
	}
	if mr.Status.OwnershipReason != "" {
		t.Errorf("Status.OwnershipReason = %q at mint, want empty", mr.Status.OwnershipReason)
	}
	if mr.Status.LastBotHeadSHA != "" {
		t.Errorf("Status.LastBotHeadSHA = %q at mint, want empty: the backfill seeds it", mr.Status.LastBotHeadSHA)
	}
	if mr.Status.OwnershipChangedAt != nil {
		t.Errorf("Status.OwnershipChangedAt set at mint, want nil")
	}
	if after := testutil.ToFloat64(obs.OwnershipFlipCounter("to-external", "external-push")); after != before {
		t.Errorf("the mint emitted an ownership-flip metric")
	}
}

// The mint is sound only because ReconcileOwnership then classifies it TATARA.
// This is the test that ties the two halves together, and it is the one to read
// first if an adopted Task ever dies at the merge corridor.
func TestMintAdoptedUpgradeTask_ReconcileOwnershipMakesItMergeable(t *testing.T) {
	ctx := context.Background()
	proj, repo := adoptProject(t, ctx)
	d := &StageDriver{Client: k8sClient, APIReader: k8sClient,
		SpillerFor: func(*tatarav1alpha1.Project) objbudget.Spiller { return &mirrorSpiller{} }}

	if _, _, err := newTestMinter(t).MintAdoptedUpgradeTask(ctx, proj, repo, adoptedPR(46), testSpiller(t)); err != nil {
		t.Fatal(err)
	}
	mr := getMR(t, ctx, proj, repo, 46)
	if _, err := d.ReconcileOwnership(ctx, proj, repo, mr, "live-head-46", nil); err != nil {
		t.Fatalf("ReconcileOwnership: %v", err)
	}
	got := getMR(t, ctx, proj, repo, 46)
	if got.Status.Ownership != tatarav1alpha1.OwnershipTatara {
		t.Fatalf("ownership = %q, want tatara: ownershipForAuthor classifies the bot's own merge request",
			got.Status.Ownership)
	}
	if got.Status.OwnershipReason != "initial" {
		t.Fatalf("ownership reason = %q, want initial", got.Status.OwnershipReason)
	}
	if got.Status.LastBotHeadSHA != "live-head-46" {
		t.Fatalf("LastBotHeadSHA = %q, want the live head: the backfill seeds the baseline in the same write",
			got.Status.LastBotHeadSHA)
	}
	if !mergeAllowedForOwnership(got) {
		t.Fatal("the merge corridor must accept the adopted merge request with NO agent turn having run")
	}

	// The negative that makes it meaningful: the SAME merge request authored by
	// a HUMAN classifies external/initial and the corridor refuses it. That
	// shape is unreachable through adoption - AdoptUpgradeMR refuses a
	// human-authored merge request - and this is here to fail loudly if the
	// author test is ever relaxed back to branch-only recognition.
	human := &tatarav1alpha1.MergeRequest{}
	human.Status.Ownership = ownershipForAuthor(proj, "some-human")
	human.Status.OwnershipReason = "initial"
	if human.Status.Ownership != tatarav1alpha1.OwnershipExternal || mergeAllowedForOwnership(human) {
		t.Fatal("a human-authored merge request must stay external and unmergeable")
	}
}

// The drift check must still work. Widening what counts as an attributable head
// move must never become "an adopted merge request cannot be stood down",
// because standing down on a HUMAN push is the entire purpose of the ownership
// state machine.
func TestMintAdoptedUpgradeTask_AHumanPushStillStandsItDown(t *testing.T) {
	ctx := context.Background()
	proj, repo := adoptProject(t, ctx)
	d := &StageDriver{Client: k8sClient, APIReader: k8sClient,
		SpillerFor: func(*tatarav1alpha1.Project) objbudget.Spiller { return &mirrorSpiller{} }}

	task, _, err := newTestMinter(t).MintAdoptedUpgradeTask(ctx, proj, repo, adoptedPR(47), testSpiller(t))
	if err != nil {
		t.Fatal(err)
	}
	mr := getMR(t, ctx, proj, repo, 47)
	if _, err := d.ReconcileOwnership(ctx, proj, repo, mr, "H0", nil); err != nil {
		t.Fatal(err)
	}
	// A human pushed: the head moved and nothing advanced LastBotHeadSHA.
	mr = getMR(t, ctx, proj, repo, 47)
	flipped, err := d.ReconcileOwnership(ctx, proj, repo, mr, "H1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !flipped {
		t.Fatal("an unattributable head move must stand the merge request down")
	}
	got := getMR(t, ctx, proj, repo, 47)
	if got.Status.Ownership != tatarav1alpha1.OwnershipExternal {
		t.Fatalf("ownership = %q, want external", got.Status.Ownership)
	}
	// The ADOPTED reason, not the takeover one: this stand-down ends in
	// human-merged-only. See adoptedPushReasonPrefix and
	// TestReconcileOwnership_AnAdoptedMRStandsDownIntoHumanMergedOnly.
	if got.Status.OwnershipReason != adoptedPushReasonPrefix+"H1" {
		t.Fatalf("ownership reason = %q, want %q", got.Status.OwnershipReason, adoptedPushReasonPrefix+"H1")
	}
	if mergeAllowedForOwnership(got) {
		t.Fatal("the operator may still merge an adopted merge request a human pushed to")
	}
	parked := getTask(t, task.Name)
	if parked.Status.ParkReason != stage.ReasonOwnershipLost {
		t.Fatalf("the adopted Task park reason = %q, want ownership-lost", parked.Status.ParkReason)
	}
}

// The agent must work on the engine's branch, not on a synthetic one - and this
// is true from the FIRST turn, which is now the REVIEW turn. branchEnvValues
// gates its read-only review arm on Spec.Kind == "review", which an adopted
// Task never is, so AnnTakeoverHeadBranch is the ONLY thing that gets the
// review pod onto the merge request at all. Without it the review agent gets
// TaskBranch(task) - a tatara/chore-47-... branch that does not exist on the
// forge - and reviews the wrong tree.
func TestMintAdoptedUpgradeTask_PodWorksOnTheRenovateBranchFromTheFirstTurn(t *testing.T) {
	ctx := context.Background()
	proj, repo := adoptProject(t, ctx)
	task, _, err := newTestMinter(t).MintAdoptedUpgradeTask(ctx, proj, repo, adoptedPR(48), testSpiller(t))
	if err != nil {
		t.Fatal(err)
	}
	if got := agent.PushBranch(task); got != "renovate/cilium" {
		t.Fatalf("PushBranch = %q, want renovate/cilium", got)
	}
	if got := agent.TaskBranch(task); got == "renovate/cilium" {
		t.Fatal("TaskBranch must stay the derived name: taskForBranch keys on it and " +
			"a renovate branch must never resolve to this Task by branch lookup")
	}
	if hb := task.Annotations[tatarav1alpha1.AnnTakeoverHeadBranch]; hb != "renovate/cilium" {
		t.Fatalf("AnnTakeoverHeadBranch = %q at mint, want renovate/cilium", hb)
	}
}

// Binding buys DISCOVERY, and discovery is now the only thing the mint has to
// buy. ownedMergeRequests and mrForRepo filter on the controller owner and
// never read Status.Ownership, so without the bind the corridor finds nothing
// for the repo and parks operator-error.
func TestMintAdoptedUpgradeTask_WithoutTheBindTheCorridorParks(t *testing.T) {
	ctx := context.Background()
	proj, repo := adoptProject(t, ctx)

	// The SAME Task shape, created WITHOUT the bind.
	unbound := &tatarav1alpha1.Task{}
	unbound.Name = AdoptedUpgradeTaskName(proj.Name, repo.Name, 49)
	unbound.Namespace = proj.Namespace
	unbound.Spec = tatarav1alpha1.TaskSpec{
		ProjectRef: proj.Name, RepositoryRef: repo.Name, Kind: "upgrade", Goal: "g",
		Source: &tatarav1alpha1.TaskSource{Provider: "github", IsPR: true, Number: 49},
	}
	if err := k8sClient.Create(ctx, unbound); err != nil {
		t.Fatal(err)
	}
	mrs, err := ownedMergeRequests(ctx, k8sClient, unbound)
	if err != nil {
		t.Fatal(err)
	}
	if len(mrs) != 0 {
		t.Fatalf("an unbound Task owns %d merge requests, want 0", len(mrs))
	}
	if mrForRepo(mrs, repo.Name) != nil {
		t.Fatal("mrForRepo must return nil for an unbound Task: this is the operator-error park")
	}
	// There is no mirror at all either, which is the deadlock a mint that
	// WAITED for one would sit in forever.
	var mr tatarav1alpha1.MergeRequest
	err = k8sClient.Get(ctx, client.ObjectKey{
		Namespace: proj.Namespace, Name: tatarav1alpha1.MergeRequestName(repo.Name, 49)}, &mr)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("get unbound mirror: %v, want NotFound (nothing else creates one)", err)
	}
}

// THE REAP IS WHERE THE REFUSAL IS RECORDED. releaseTerminal (B.6) orphans the
// mirror and frees the deterministic Task name; without a marker stamped in that
// same sequence the next sweep re-adopts the identical merge request and the
// decline is gone. Step 2a runs BEFORE step 3, because step 3 drops the very
// ownerRef that says which merge requests were this Task's.
func TestReleaseTerminal_MarksARefusedAdoptedMergeRequest(t *testing.T) {
	ctx := context.Background()
	proj, repo := adoptProject(t, ctx)
	task, _, err := newTestMinter(t).MintAdoptedUpgradeTask(ctx, proj, repo, adoptedPR(51), testSpiller(t))
	if err != nil {
		t.Fatal(err)
	}
	// The upgrade agent declined the bump as unsafe, and the Task aged out.
	stampTaskStatus(t, ctx, task, tatarav1alpha1.StateUnderImplementation, stage.ReasonImplementDeclined)

	r := &ProjectReconciler{Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry())}
	if err := r.releaseTerminal(ctx, proj, getTask(t, task.Name), map[string]bool{}); err != nil {
		t.Fatalf("releaseTerminal: %v", err)
	}

	mr := getMR(t, ctx, proj, repo, 51)
	want := stage.ReasonImplementDeclined + adoptionRefusedSHASep + adoptedPR(51).HeadSHA
	if got := mr.Annotations[AnnAdoptionRefused]; got != want {
		t.Fatalf("mirror annotation %s = %q, want %q: the sweep re-adopts this merge request "+
			"on its next pass and a fresh review turn can approve the bump the upgrade agent refused",
			AnnAdoptionRefused, got, want)
	}
	if AdoptUpgradeMR(proj, adoptedPR(51), nil, "", mr) {
		t.Fatal("the refused merge request is still adoptable")
	}
}
