package controller

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// reapAdopted mints an adopted upgrade Task on `number`, ends it with the given
// state and reason, and runs the B.6 terminal release over it. It returns the
// surviving mirror.
func reapAdopted(t *testing.T, ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, number int, state, reason string) *tatarav1alpha1.MergeRequest {

	t.Helper()
	task, _, err := newTestMinter(t).MintAdoptedUpgradeTask(ctx, proj, repo, adoptedPR(number), testSpiller(t), nil)
	if err != nil {
		t.Fatalf("mint %d: %v", number, err)
	}
	stampTaskStatus(t, ctx, task, state, reason)
	r := &ProjectReconciler{Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry())}
	if err := r.releaseTerminal(ctx, proj, getTask(t, task.Name), map[string]bool{}); err != nil {
		t.Fatalf("releaseTerminal %d: %v", number, err)
	}
	return getMR(t, ctx, proj, repo, number)
}

// A REAP IS NOT A DECISION. The refusal marker is PERMANENT and it exists for
// exactly one fact: the platform DECIDED against this bump, so a second review
// must not be able to approve what the first declined.
//
// Every other way an adopted Task dies says nothing whatsoever about the bump.
// stage-deadline is a review pod that never answered; merge-timeout and
// merge-blocked are the forge; ci-red is the dependency's own pipeline;
// operator-error, object-too-large and admission-starved are the platform's own
// faults. Stamping a permanent refusal for any of them retires a merge request -
// and, because a dependency engine REUSES its branch, the whole dependency line
// behind it - on a transient failure nobody ever sees.
func TestReleaseTerminal_DoesNotRefuseAdoptionForATransientReapReason(t *testing.T) {
	ctx := context.Background()
	proj, repo := adoptProject(t, ctx)

	for i, reason := range []string{
		stage.ReasonStageDeadline,
		stage.ReasonMergeTimeout,
		stage.ReasonCIRed,
		stage.ReasonOperatorError,
	} {
		number := 120 + i
		mr := reapAdopted(t, ctx, proj, repo, number, tatarav1alpha1.StateAwaitingReview, reason)
		if got := mr.Annotations[AnnAdoptionRefused]; got != "" {
			t.Fatalf("park reason %q stamped a PERMANENT adoption refusal %q: a flaky review pod "+
				"or a forge outage now retires this merge request, and the engine's reused branch "+
				"takes the whole dependency line with it", reason, got)
		}
		if !AdoptUpgradeMR(proj, adoptedPR(number), nil, "", mr) {
			t.Fatalf("park reason %q left the merge request unadoptable", reason)
		}
	}
}

// THE ESCAPE HATCH HAS TO BE REAL. A refusal is keyed to the TREE that was
// refused, not to the merge request forever: a dependency engine reuses one
// branch per dependency line (Renovate's default is renovate/<dep>-<major>.x)
// and FORCE-PUSHES the next bump onto it, keeping the same number and the same
// mirror. A marker that ignored the head SHA would refuse 1.4 because 1.3 was
// declined, with no signal anywhere and no way back except a human deleting an
// annotation they do not know exists.
func TestAdoptionRefusalIsScopedToTheREFUSEDTree(t *testing.T) {
	ctx := context.Background()
	proj, repo := adoptProject(t, ctx)

	mr := reapAdopted(t, ctx, proj, repo, 130,
		tatarav1alpha1.StateUnderImplementation, stage.ReasonImplementDeclined)
	if mr.Annotations[AnnAdoptionRefused] == "" {
		t.Fatal("a DECLINED adopted merge request carries no refusal marker at all")
	}
	// The declined tree stays declined.
	if AdoptUpgradeMR(proj, adoptedPR(130), nil, "", mr) {
		t.Fatal("the refused merge request is adoptable again at the very SHA that was refused")
	}
	// The engine force-pushes the next bump onto the same branch and number.
	next := adoptedPR(130)
	next.HeadSHA = "sha-130-rebumped"
	if !AdoptUpgradeMR(proj, next, nil, "", mr) {
		t.Fatal("a FRESH engine push on the reused branch is still refused: the whole dependency " +
			"line is retired by one decline, which is not what the marker is for")
	}
}

// AN ADOPTED MERGE REQUEST A HUMAN PUSHED TO MUST NEVER BE RE-ADOPTED.
//
// flipToExternal hands the mirror to a kind=review Task, so the adopted Task's
// own reap sees wasController=false and marks nothing. When that review Task is
// later reaped the mirror is ORPHANED, and nothing else stops the next sweep
// re-adopting it - telling a fresh review pod "approving MERGES it" for a merge
// request mergeAllowedForOwnership refuses, which hard-errors reconcileMerge on
// every reconcile until the merging deadline parks the Task.
func TestAdoptUpgradeMR_RefusesAMirrorThatStoodDownToAnExternalPush(t *testing.T) {
	ctx := context.Background()
	d, proj, repo := newOwnershipDriver(t, ctx)
	proj.Spec.UpgradePolicy = &tatarav1alpha1.UpgradePolicySpec{
		Engine: "renovate", AdoptBranchPrefix: "renovate/",
	}
	if err := k8sClient.Update(ctx, proj); err != nil {
		t.Fatalf("arm adoption: %v", err)
	}
	mr := seedAdoptedUpgradeMR(t, ctx, proj, repo, 140, "engine-head")
	if _, err := d.ReconcileOwnership(ctx, proj, repo, mr, "a-humans-commit", nil); err != nil {
		t.Fatalf("flip: %v", err)
	}
	got := getMR(t, ctx, proj, repo, 140)
	if mergeAllowedForOwnership(got) {
		t.Fatal("fixture is wrong: the stood-down adopted mirror is still mergeable")
	}

	// The review Task that took the mirror over is itself reaped, so the mirror
	// is an orphan with no live owner and the deterministic Task name is free.
	pr := adoptedPR(140)
	pr.HeadBranch = got.Status.HeadBranch
	pr.HeadSHA = got.Status.HeadSHA
	if AdoptUpgradeMR(proj, pr, nil, "", got) {
		t.Fatal("a merge request the operator may never merge is adoptable into a Task whose " +
			"whole contract is `approving MERGES it`: reconcileMerge hard-errors on it forever")
	}
}
