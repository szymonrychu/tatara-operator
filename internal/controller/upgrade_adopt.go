package controller

import (
	"context"
	"fmt"
	"slices"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/upgrade"
)

// adoptedUpgradeKind is the Task kind an adopted third-party dependency merge
// request mints. It is `upgrade`, NOT `takeover`: the Task must get the upgrade
// tool profile (so it has submit_outcome at all), the upgrade skill profile, the
// upgrade outcome schema, and it must count against maxOpenUpgrades.
const adoptedUpgradeKind = "upgrade"

// adoptedSignificanceFloor is the change significance an adopted merge request's
// mirror is seeded with at mint.
//
// WITHOUT IT THE COMMON PATH PUBLISHES NOTHING. Significance has exactly two
// writers - the implement/upgrade `submitted` outcome and the review outcome's
// OPTIONAL escalation - and on an adopted merge request APPROVED AT FIRST
// REVIEW neither runs: there is no upgrade turn at all, and a reviewer that
// declares no change_significance writes nothing (the escalation clause is
// `if sig != ""`). reconcileMerge then finds an empty significance, calls it an
// operator bug, and merges anyway: no semver label lands at the merge commit,
// CI cuts NO tag, nothing publishes, no pin propagates, deployedAt is never
// stamped, and the Task sits in `deploying` until its budget parks it. That is
// the COMMON case, not an edge - most adopted bumps are trivial and approved on
// the first review.
//
// WHY A DEFAULT AND NOT A REQUIREMENT. The alternative was to make
// change_significance MANDATORY on a review outcome for an adopted Task. It was
// rejected on three counts:
//
//  1. it puts a NEW hard refusal on the highest-consequence common path, driven
//     by an agent this repo documents as flaky. A refused approve costs a whole
//     review turn and can loop; a conservative tag cannot.
//  2. under-tagging is RECOVERABLE and not-tagging is the wedge. A patch tag on
//     a bump that deserved minor is corrected by the next release; no tag at all
//     leaves a merged commit that never publishes and a Task that never
//     resolves.
//  3. `patch` is the honest floor for what the REPO ITSELF changed: one pin
//     moved, which is a patch-level change to this repo's own artifact
//     regardless of the dependency's own version jump.
//
// The reviewer keeps its say: `patch` is the LOWEST rank in restapi's
// significanceRank table, so the review escalation clause outranks it for every
// value a reviewer can declare, and both GoalAdopted and the review assignment
// paragraph now tell the review agent so. An upgrade turn's `submitted` outcome
// overwrites it outright, exactly as it does for any other Task.
const adoptedSignificanceFloor = "patch"

// THERE IS NO adoptedOwnershipReasonPrefix, AND THAT IS THE POINT. An earlier
// draft stamped Status.Ownership=tatara with an "upgrade-adopted:" reason here,
// because a dependency merge request authored by a human classifies `external`
// and mergeAllowedForOwnership refuses that with a hard error. The engine now
// authors as the bot (AdoptUpgradeMR requires it, or an allowlisted engine login
// that ownershipForAuthor accepts through the SAME predicate), so
// ownershipForAuthor classifies it `tatara` on its own and there is nothing to
// flip. Ownership has exactly ONE writer, ReconcileOwnership, and it stays that
// way.

// AdoptedUpgradeTaskName is the deterministic name of the upgrade Task adopted
// onto (repo, number). Being a pure function of the merge request's identity is
// what makes the mint idempotent, and idempotence is the WHOLE dedup mechanism
// for adoption: one Task per merge request, no matter how many sweeps run.
func AdoptedUpgradeTaskName(proj, repo string, number int) string {
	return tatarav1alpha1.IntakeTaskName(proj, adoptedUpgradeKind, repo, number)
}

// MintAdoptedUpgradeTask mints an upgrade Task bound to an EXISTING
// dependency-upgrade merge request, and binds that merge request to it.
//
// Modelled on mintOrUnparkTakeoverTask, which is the only other place in the
// tree that binds a Task to a merge request it did not open. Not a call into it:
// that function hardcodes kind, initial state, goal, name derivation and an
// ownership-lost unpark, and all five differ here.
//
// IT DOES EXACTLY TWO THINGS A PLAIN TASK MINT DOES NOT, and the list is short
// on purpose - an earlier draft had a third:
//
//  1. Binds the MergeRequest CR as the Task's, via bindMRToTask - which also
//     CREATES it. ensureMergeRequestCR is reachable only through
//     SyncMergeRequest, which is reachable only through bindMRToTask, so a
//     merge request no Task has ever bound has no mirror at all and
//     Minter.mergeRequestCR returns (nil, nil) for it forever. Without the
//     bind, ownedMergeRequests returns empty, mrForRepo returns nil, and the
//     merge corridor parks operator-error.
//  2. Stamps AnnTakeoverHeadBranch (below).
//
// THE THIRD THING IS GONE: no ownership flip, no ownership reason, no
// LastBotHeadSHA seed. AdoptUpgradeMR requires the merge request to be authored
// by an identity adoptableAuthor accepts, and ownershipForAuthor asks the same
// predicate, so the mirror classifies `tatara` on the first ReconcileOwnership
// pass - which happens LATER IN THIS SAME SWEEP PASS, with a live head from
// GetPRHead. That backfill also seeds LastBotHeadSHA in the same write, and an
// absent seed cannot cause a flip anyway (ownership.go seeds and returns rather
// than flipping when the baseline is empty). Stamping any of it here would make
// this a SECOND writer of Status.Ownership, which ReconcileOwnership's own doc
// block asks callers not to be.
//
// It does NOT stamp the head branch as a work branch on its own: that is
// AnnTakeoverHeadBranch, read kind-agnostically by branchEnvValues, which is
// why NEITHER pod needs a change. The REVIEW pod at awaiting-review gets
// renovate/<slug> checked out because branchEnvValues' read-only review arm
// gates on Spec.Kind == "review" and this Task's is "upgrade"; the UPGRADE pod
// at under-implementation gets the same branch to push onto. Without the
// annotation the review pod would fall through to TaskBranch(task) - a
// tatara/chore-<n>-<slug> branch that does not exist on the forge - and review
// the wrong tree.
func (m *Minter) MintAdoptedUpgradeTask(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, pr scm.PRRef,
	sp objbudget.Spiller) (*tatarav1alpha1.Task, MintOutcome, error) {

	name := AdoptedUpgradeTaskName(proj.Name, repo.Name, pr.Number)

	var existing tatarav1alpha1.Task
	err := m.Client.Get(ctx, client.ObjectKey{Namespace: proj.Namespace, Name: name}, &existing)
	if err == nil {
		// Already adopted. No unpark arm, unlike takeover: a parked adopted Task
		// is the reaper's, and re-adopting it would restart work a human may have
		// deliberately stopped.
		return &existing, MintExistingLive, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, MintNotOwed, fmt.Errorf("adopt: get task %s: %w", name, err)
	}

	// The mirror snapshot the bind upserts, built from the listing row the sweep
	// already has. It also derives the merge request URL the same way the review
	// mint does, since a PRRef carries no URL of its own.
	ext := mrSnapshot(proj, repo, pr)
	slug := repo.Name
	if o, n, oerr := scm.OwnerRepo(repo.Spec.URL); oerr == nil {
		slug = o + "/" + n
	}
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: proj.Namespace,
			Annotations: map[string]string{
				// Kind-agnostic in branchEnvValues: the pod's TASK_BRANCH becomes
				// this branch, so the REVIEW pod reviews the right tree and the
				// UPGRADE pod's commits land on the merge request that already
				// exists.
				tatarav1alpha1.AnnTakeoverHeadBranch: pr.HeadBranch,
			},
		},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    proj.Name,
			RepositoryRef: repo.Name,
			Kind:          adoptedUpgradeKind,
			Goal: upgrade.GoalAdopted(slug, pr.HeadBranch, pr.Title,
				pr.Number, proj.Spec.UpgradePolicy),
			// `new`, so REVIEW GOES FIRST. Not under-implementation (which is
			// where the CRON mint goes): an adopted merge request is already
			// complete and already CI-tested, so paying an upgrade turn on every
			// one of them is exactly the cost this design removes. The review
			// turn decides, and a request_changes is what hands it to the upgrade
			// agent.
			//
			// Not `refined` either: refined's only exit into under-implementation
			// is submit_outcome(action=approved), which routes through
			// verifyApprovalScope and is refused with no-live-issue for a Task
			// owning zero Issue CRs - which a merge-request-born Task always
			// does. And awaiting-review is not mintable at all: InitialState's
			// CRD enum is new;refined;under-implementation.
			//
			// `new` costs nothing: AgentKindFor(new, *) is "", so no pod runs
			// there, and reconcileTriaging walks the Task to awaiting-review on
			// the edge stage.AdoptedMR guards (internal/stage GUARD 5,
			// task_stage.go triageTarget). Both of those MUST already be widened
			// or this Task parks triage-stalled.
			InitialState: tatarav1alpha1.StateNew,
			// ONE repo, deliberately. If this Task opened a merge request in a
			// second repo, TASK_BRANCH would be this same renovate/<slug> name
			// THERE too, where the third-party bot may already own a branch of
			// that name for a different unit. The skill declines the cross-repo
			// case and leaves it to the scheduled discovery path, which is
			// unconstrained and pushes to tatara/task-<name>.
			MergeOrder: []string{repo.Name},
			Source: &tatarav1alpha1.TaskSource{
				Provider:    providerOf(proj),
				IssueRef:    ext.URL,
				URL:         ext.URL,
				Number:      pr.Number,
				IsPR:        true,
				HeadSHA:     pr.HeadSHA,
				Title:       pr.Title,
				AuthorLogin: pr.Author,
			},
		},
	}
	// repo.Name here, not "": with Source.Number set, podNameIDSegment returns
	// p<N> and never reaches upgradeIDSegment, so the pod is
	// upg-<project>-<repo>-p<number> - unique per merge request by the same key
	// that makes the Task name unique, and distinct from the cron-minted
	// upg-<project>-<8hex>.
	agent.StampPodName(task, proj.Name, repo.Name)
	if err := controllerutil.SetControllerReference(proj, task, m.Scheme); err != nil {
		return nil, MintNotOwed, fmt.Errorf("adopt: set task ownerref: %w", err)
	}
	outcome, twin, err := m.createTaskRaceSafe(ctx, task)
	if err != nil {
		return nil, MintNotOwed, err
	}
	if outcome == MintExistingLive {
		return twin, MintExistingLive, nil
	}
	if outcome == MintTombstoneDeleted {
		// A dead twin holds the name. Unlike takeover this does NOT retry: the
		// name is deterministic and the sweep runs again in minutes, so a retry
		// buys nothing and a bounded-retry arm is a state machine nobody needs.
		return nil, MintTombstoneDeleted, nil
	}

	// The bind CREATES the mirror. Built from the PRRef the sweep listed, the
	// same way MintReviewTask does for a human's PR, rather than from a
	// MergeRequest CR that does not exist yet.
	if err := m.bindMRToTask(ctx, proj, repo, ext, task, sp); err != nil {
		return nil, MintNotOwed, err
	}
	mrName := tatarav1alpha1.MergeRequestName(repo.Name, pr.Number)
	if err := m.stampMintStatus(ctx, task, func(fresh *tatarav1alpha1.Task) {
		if !slices.Contains(fresh.Status.MRRefs, mrName) {
			fresh.Status.MRRefs = append(fresh.Status.MRRefs, mrName)
		}
	}); err != nil {
		return nil, MintNotOwed, err
	}

	// SEED THE SEMVER FLOOR. See adoptedSignificanceFloor for why this exists at
	// all and why it is a default rather than a requirement on the review
	// outcome. IF-EMPTY, never an overwrite: a re-mint after a reap must not
	// undo an escalation a review already recorded, and SyncMergeRequest never
	// touches this field so the seed survives every mirror sync.
	if err := objbudget.FitMergeRequest(ctx, m.Client, sp,
		client.ObjectKey{Namespace: proj.Namespace, Name: mrName},
		func(fresh *tatarav1alpha1.MergeRequest) {
			if fresh.Status.Significance == "" {
				fresh.Status.Significance = adoptedSignificanceFloor
			}
		}); err != nil {
		return nil, MintNotOwed, fmt.Errorf("adopt: seed the semver floor on %s: %w", mrName, err)
	}

	// AND THAT IS THE WHOLE MINT. No ownership write of any kind: the merge
	// request is authored by an identity adoptableAuthor accepts, by
	// AdoptUpgradeMR's own precondition, so ReconcileOwnership classifies it
	// `tatara` and seeds LastBotHeadSHA later in this same sweep pass. Adding an
	// ownership stamp here would make this a second writer of a field with one
	// owner.
	return task, MintCreated, nil
}
