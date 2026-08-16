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

// AdoptedUpgradeKind is the Task kind an adopted third-party dependency merge
// request mints. It is `upgrade`, NOT `takeover`: the Task must get the upgrade
// tool profile (so it has submit_outcome at all), the upgrade skill profile, the
// upgrade outcome schema, and it must count against maxOpenUpgrades.
//
// EXPORTED because the webhook is the OTHER producer of an adoption payload and
// was carrying its own "upgrade" literal for the same field. Two spellings of
// one contract, in two packages, with nothing holding them together.
const AdoptedUpgradeKind = "upgrade"

// AdoptedSignificanceFloor is the change significance an adopted merge request's
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
const AdoptedSignificanceFloor = "patch"

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
	return tatarav1alpha1.IntakeTaskName(proj, AdoptedUpgradeKind, repo, number)
}

// MintAdoptedUpgradeTask mints an upgrade Task bound to an EXISTING
// dependency-upgrade merge request, and binds that merge request to it.
//
// Modelled on mintOrUnparkTakeoverTask, which is the only other place in the
// tree that binds a Task to a merge request it did not open. Not a call into it:
// that function hardcodes kind, initial state, goal, name derivation and an
// ownership-lost unpark, and all five differ here.
//
// IT DOES EXACTLY THREE THINGS A PLAIN TASK MINT DOES NOT:
//
//  1. Binds the MergeRequest CR as the Task's, via bindMRToTask - which also
//     CREATES it. ensureMergeRequestCR is reachable only through
//     SyncMergeRequest, which is reachable only through bindMRToTask, so a
//     merge request no Task has ever bound has no mirror at all and
//     Minter.mergeRequestCR returns (nil, nil) for it forever. Without the
//     bind, ownedMergeRequests returns empty, mrForRepo returns nil, and the
//     merge corridor parks operator-error.
//  2. Stamps AnnTakeoverHeadBranch (below).
//  3. Stamps LabelUpgradeOrigin=adopted, so openUpgradeLaneCount can exclude
//     this Task from the CRON's maxOpenUpgrades budget (design D2). Adopted
//     work is bounded by the general pool; the cron's knob counts only its own.
//
// NO OWNERSHIP FLIP, NO OWNERSHIP REASON, NO LASTBOTHEADSHA SEED. An earlier
// draft stamped ownership here; this one does not. AdoptUpgradeMR requires the
// merge request to be authored by an identity adoptableAuthor accepts, and
// ownershipForAuthor asks the same predicate, so the mirror classifies
// `tatara` on the first ReconcileOwnership pass - which happens LATER IN THIS
// SAME SWEEP PASS, with a live head from GetPRHead. That backfill also seeds
// LastBotHeadSHA in the same write, and an absent seed cannot cause a flip
// anyway (ownership.go seeds and returns rather than flipping when the
// baseline is empty). Stamping any of it here would make this a SECOND writer
// of Status.Ownership, which ReconcileOwnership's own doc block asks callers
// not to be.
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
//
// `stamp` is the minting QueuedEvent's label set (queue.MintStamp) and may be
// nil. It is TAKEN, not derived, because this mint builds its Task by hand
// rather than through BuildTaskFromQueuedEvent - and without it the event that
// admitted this Task is never reaped: reconcileDone finds the Task by
// LabelQueuedEvent and mintedTask resolves idempotency by LabelMintedBy.
func (m *Minter) MintAdoptedUpgradeTask(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, pr scm.PRRef,
	sp objbudget.Spiller, stamp map[string]string) (*tatarav1alpha1.Task, MintOutcome, error) {

	name := AdoptedUpgradeTaskName(proj.Name, repo.Name, pr.Number)

	// The mirror snapshot the bind upserts, built from the listing row the sweep
	// already has. It also derives the merge request URL the same way the review
	// mint does, since a PRRef carries no URL of its own.
	ext := mrSnapshot(proj, repo, pr)

	var existing tatarav1alpha1.Task
	err := m.Client.Get(ctx, client.ObjectKey{Namespace: proj.Namespace, Name: name}, &existing)
	if err == nil {
		// Already adopted. No unpark arm, unlike takeover: a parked adopted Task
		// is the reaper's, and re-adopting it would restart work a human may have
		// deliberately stopped. It DOES converge the bind and the floor, which is
		// not work and not a restart - see convergeAdoptedMint.
		if cerr := m.convergeAdoptedMint(ctx, proj, repo, ext, &existing, sp); cerr != nil {
			return nil, MintNotOwed, cerr
		}
		return &existing, MintExistingLive, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, MintNotOwed, fmt.Errorf("adopt: get task %s: %w", name, err)
	}

	slug := repo.Name
	if o, n, oerr := scm.OwnerRepo(repo.Spec.URL); oerr == nil {
		slug = o + "/" + n
	}
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: proj.Namespace,
			Labels:    adoptedTaskLabels(stamp),
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
			Kind:          AdoptedUpgradeKind,
			Goal: upgrade.GoalAdopted(slug, pr.HeadBranch, pr.Title,
				pr.Number, proj.Spec.UpgradePolicy),
			// `new`, so REVIEW GOES FIRST. Not under-implementation (which is
			// where the CRON mint goes): an adopted merge request is already
			// complete and already CI-tested, so paying an upgrade turn on every
			// one of them is exactly the cost this design removes. The review
			// turn decides, and a request_changes is what hands it to the upgrade
			// agent.
			//
			// Not `refined` either, and since #604 it is not even admissible:
			// refined's only exit into under-implementation is
			// submit_outcome(action=approved), which routes through
			// verifyApprovalScope and is refused with no-live-issue for a Task
			// owning zero Issue CRs - which a merge-request-born Task always is.
			// #604 deleted the (create) -> refined edge and narrowed the enum in
			// lockstep, so this paragraph now explains why the value is GONE
			// rather than why it is not chosen. awaiting-review was never
			// mintable. Read the live set off TaskSpec.InitialState's kubebuilder
			// Enum marker rather than a copy of it here: copies of it drifted
			// twice during #604's own review, which is what
			// TestNoProseRestatesTheRetiredInitialStateEnum now fails on.
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
		if cerr := m.convergeAdoptedMint(ctx, proj, repo, ext, twin, sp); cerr != nil {
			return nil, MintNotOwed, cerr
		}
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
	if err := m.seedAdoptedSemverFloor(ctx, proj, mrName, sp); err != nil {
		return nil, MintNotOwed, err
	}

	// AND THAT IS THE WHOLE MINT. No ownership write of any kind: the merge
	// request is authored by an identity adoptableAuthor accepts, by
	// AdoptUpgradeMR's own precondition, so ReconcileOwnership classifies it
	// `tatara` and seeds LastBotHeadSHA later in this same sweep pass. Adding an
	// ownership stamp here would make this a second writer of a field with one
	// owner.
	return task, MintCreated, nil
}

// convergeAdoptedMint re-runs the two writes that happen AFTER the Task exists -
// the mirror bind and the semver floor - for an adopted Task the mint found
// already live.
//
// IT EXISTS BECAUSE A MINT IS THREE WRITES AND ONLY THE FIRST IS ATOMIC. The
// Task create, the bind that creates the mirror, and the floor seed are separate
// API calls; any of the last two can fail (a conflict-exhausted
// FitMergeRequest, an ErrObjectTooLarge on a fat mirror, a lost lease) and leave
// the Task persisted with the rest undone. The Task name is deterministic, so
// the very next sweep pass Gets it and takes the MintExistingLive arm - which
// used to return immediately and therefore NEVER retried either write. The
// resulting merge request merges with an empty significance, CI cuts no tag,
// nothing publishes, and the Task sits in `deploying` until its budget parks it:
// the exact wedge AdoptedSignificanceFloor was added to close.
//
// IT IS NOT A RE-ADOPTION AND IT RESTARTS NOTHING. repairMRBinding never steals
// a controller-owned mirror and the seed is if-empty, so on the steady state
// this is two Gets and no writes. It cannot even be reached for a mirror a live
// Task owns - AdoptUpgradeMR clause (e) refuses that merge request before the
// mint is called - so reaching here means the bind is genuinely incomplete.
func (m *Minter) convergeAdoptedMint(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, ext scm.MergeRequest, task *tatarav1alpha1.Task,
	sp objbudget.Spiller) error {

	if err := m.repairMRBinding(ctx, proj, repo, ext, task, sp); err != nil {
		return err
	}
	return m.seedAdoptedSemverFloor(ctx, proj, tatarav1alpha1.MergeRequestName(repo.Name, ext.Number), sp)
}

// seedAdoptedSemverFloor writes AdoptedSignificanceFloor onto an adopted merge
// request's mirror. See AdoptedSignificanceFloor for why this exists at all and
// why it is a default rather than a requirement on the review outcome.
//
// IF-EMPTY, never an overwrite: a re-mint after a reap must not undo an
// escalation a review already recorded, and SyncMergeRequest never touches this
// field so the seed survives every mirror sync. That is also what makes it safe
// to run on EVERY pass, which convergeAdoptedMint does.
func (m *Minter) seedAdoptedSemverFloor(ctx context.Context, proj *tatarav1alpha1.Project,
	mrName string, sp objbudget.Spiller) error {

	if err := objbudget.FitMergeRequest(ctx, m.Client, sp,
		client.ObjectKey{Namespace: proj.Namespace, Name: mrName},
		func(fresh *tatarav1alpha1.MergeRequest) {
			if fresh.Status.Significance == "" {
				fresh.Status.Significance = AdoptedSignificanceFloor
			}
		}); err != nil {
		return fmt.Errorf("adopt: seed the semver floor on %s: %w", mrName, err)
	}
	return nil
}

// adoptedTaskLabels merges the caller's mint stamp with the adoption origin
// marker. The origin is stamped LAST and unconditionally: it is this repo's own
// invariant, not the caller's to override.
func adoptedTaskLabels(stamp map[string]string) map[string]string {
	labels := make(map[string]string, len(stamp)+1)
	for k, v := range stamp {
		labels[k] = v
	}
	labels[tatarav1alpha1.LabelUpgradeOrigin] = tatarav1alpha1.UpgradeOriginAdopted
	return labels
}

// ClampAdoptedUpgradeBody cuts a merge-request description to the byte budget
// AdoptedUpgradeRef.Body's kubebuilder marker enforces (issue #495's class: a
// bounded CRD field fed from an unbounded upstream one).
//
// UNCLAMPED, AN OVERSIZED BODY TAKES THE WEBHOOK DARK FOR THAT PROJECT. The
// Create 422s, enqueueAdoption's failure policy is a 500 so the forge
// redelivers, and it 500s again on every redelivery - and GitLab counts
// consecutive delivery failures toward auto-disabling the project hook. The
// sweep backstop fails enqueue_adopt_upgrade on every pass for as long as the
// merge request is open, and refreshQueuedAdoption's Update is rejected the
// same way, which is design D4's freshness guarantee silently ending for
// exactly the merge requests whose changelogs are largest.
//
// MergeRequestBodyMaxBytes, not GoalMaxBytes: this string never becomes a Task
// goal. It is copied onto MergeRequest.status.body (mrSnapshot ->
// bindMRToTask -> SyncMergeRequest), whose own cap is this one, so anything
// smaller would truncate the adopted mirror below what every other mirror path
// stores for the same merge request.
func ClampAdoptedUpgradeBody(body string) string {
	return tatarav1alpha1.TruncateUTF8(body, tatarav1alpha1.MergeRequestBodyMaxBytes)
}

// AdoptedUpgradeRefFromPR snapshots a listing/delivery PRRef into the CRD shape
// a QueuedEvent carries. Everything AdoptUpgradeMR and MintAdoptedUpgradeTask
// read is copied, HeadRepo included - the fork guard fails CLOSED on an empty
// one, so a lossy copy silently disarms adoption rather than breaking it loudly.
//
// IT IS ALSO THE ONE TRUNCATION FUNNEL for the snapshot, deliberately: both
// producers (the webhook's enqueueAdoption and the sweep's PRAdoptUpgrade arm)
// build their snapshot here and nowhere else, so a clamp on this line is a
// clamp on both without either having to remember. Only Body is clamped -
// neither AdoptedUpgradeRef.Title nor MergeRequest.status.title carries a
// MaxLength marker, so clamping the title would be inventing a limit no
// consumer imposes.
func AdoptedUpgradeRefFromPR(pr scm.PRRef) *tatarav1alpha1.AdoptedUpgradeRef {
	return &tatarav1alpha1.AdoptedUpgradeRef{
		Number: pr.Number, Title: pr.Title, Author: pr.Author,
		HeadSHA: pr.HeadSHA, HeadBranch: pr.HeadBranch,
		Body:   ClampAdoptedUpgradeBody(pr.Body),
		Labels: pr.Labels, Repo: pr.Repo, HeadRepo: pr.HeadRepo,
	}
}

// PRRefFromAdopted is AdoptedUpgradeRefFromPR's inverse, used at admit time.
// Exported for the same reason its inverse is: the webhook's own tests assert
// that the snapshot they enqueue survives AdoptUpgradeMR, and they must ask that
// question through the EXACT conversion admission uses. A hand-copied twin in a
// test would answer a question about itself the moment this drifts.
// UpdatedAt is deliberately left zero: nothing on the adoption path reads it.
func PRRefFromAdopted(a *tatarav1alpha1.AdoptedUpgradeRef) scm.PRRef {
	if a == nil {
		return scm.PRRef{}
	}
	return scm.PRRef{
		Number: a.Number, Title: a.Title, Author: a.Author,
		HeadSHA: a.HeadSHA, HeadBranch: a.HeadBranch, Body: a.Body,
		Labels: a.Labels, Repo: a.Repo, HeadRepo: a.HeadRepo,
	}
}
