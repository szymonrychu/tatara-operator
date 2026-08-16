package controller

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// takeoverKind is Task.Spec.Kind for a maintainer-gated takeover of an existing
// MR (MR ownership design). It is its own kind - not review, not clarify - so
// its deterministic natural-key name never collides with the review Task or a
// clarify/issue Task minted for the same forge number.
const takeoverKind = "takeover"

// takeoverTaskName is the deterministic natural-key name for the ONE
// full-lifecycle takeover Task an MR ever gets, across every flip round: a
// maintainer can take-over/stand-down/take-over many times over an MR's life,
// and every round must land on the SAME Task so its history (turn count,
// review rounds, MR ownership) is never reset by a fresh mint.
func takeoverTaskName(proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, number int) string {
	return tatarav1alpha1.IntakeTaskName(proj.Name, takeoverKind, repo.Name, number)
}

// TakeoverTaskName is the exported form of takeoverTaskName. OP9's takeover
// REST endpoint (internal/restapi) needs the identical deterministic name to
// reason about the takeover Task without re-deriving the "takeover" kind
// string itself, which would risk the two packages drifting on the naming
// scheme.
func TakeoverTaskName(proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, number int) string {
	return takeoverTaskName(proj, repo, number)
}

// MintOrUnparkTakeoverTask is the ONE entry point a maintainer's "take over"
// comment drives (OP9's takeover endpoint calls this). It returns the single
// full-lifecycle takeover Task bound to mr:
//
//   - No Task exists yet for (proj, takeover, repo, mr.Spec.Number): mint one
//     straight into `under-implementation` (the Create->under-implementation
//     edge stage.go carries for every kind with no gate to face), carrying the
//     MR's existing head branch as AnnTakeoverHeadBranch so the implement pod
//     PUSHES to it instead of a derived tatara/* branch, and controller-own the
//     MR mirror.
//   - The Task already exists and is parked(ownership-lost) - a re-take after
//     a stand-down: re-enter `under-implementation` so the SAME Task resumes
//     pushing, rather than minting a duplicate.
//   - The Task already exists in any OTHER live state (including a fresh
//     mint that raced us here): return it unchanged. Idempotent - except for the
//     mrRefs backfill described under INTERRUPTED MINT below.
//
// The OTHER exit from parked(ownership-lost) - parked->`merged` - is driven by
// DrainStandDownMerge (OP11) on an approved review; that is a different
// trigger (a maintainer's REVIEW, not a maintainer's COMMENT) and does not
// belong on this path, which always targets under-implementation (RESUME
// PUSHING).
//
// WHY NOT `refined` (#604). It was minted there until #604, on the reading that
// a takeover "still faces the gate". It cannot: `refined`'s only forward edge is
// submit_outcome(action=approved) -> verifyApprovalScope, which refuses
// no-live-issue for a Task owning zero Issue CRs, and a takeover owns zero BY
// CONSTRUCTION - Source.IsPR is always true below, and mintIssueCRs bails on
// exactly that. Every takeover therefore burned a pod at `refined` and then
// parked awaiting-human, permanently, with the human's merge request already
// flipped to tatara ownership and no Task able to advance it. The authorisation
// the gate would have looked for has already been performed, and more strictly:
// restapi.mrTakeover refuses unless the triggering comment exists in the mirror
// AND its recorded author is a verified MAINTAINER, before it flips anything.
//
// A DELIBERATE DECLINE IS TERMINAL FOR A TAKEOVER, and that is deliberate rather
// than unnoticed. At `under-implementation` the implement outcome schema's only
// non-code action is `declined`, which parks(implement-declined) - UnparkNever -
// and stage.go's GUARD 4 closes under-implementation -> done to every kind but
// documentation, so a declined takeover parks rather than reaching a terminal
// state. Nothing in this file hands the merge request back either: the ONLY
// writer of Ownership=external is ownership.go's flipToExternal, which fires on
// a HUMAN PUSH, and the un-park branch below only fires on ReasonOwnershipLost -
// so a re-take comment on a DELIBERATELY declined takeover cannot resume it.
// That case is REFUSED at the door rather than silently accepted:
// restapi.mrTakeover answers 409 for any UnparkNever park that is not
// ownership-lost (it used to answer 200 while doing nothing, forever). The two
// ways the MR does come back are both real and both outside this path: the human
// pushes to it, or the reaper collects the parked Task at ParkRetention and
// releases ownership with it. The takeover job text (assignment.go) therefore
// requires the agent to COMMENT on the merge request explaining itself before it
// declines - a decline the maintainer never sees is the failure mode this
// terminal would otherwise produce.
//
// THE ORDINARY STAND-DOWN IS NOT THAT TERMINAL, and separating the two is #604's
// review round 2. tatara-implement-takeover - the skill this pod is required to
// invoke - instructs `declined` on a branch divergence (an ls-remote mismatch,
// or a non-fast-forward push rejection), which is not a refusal at all: it is a
// human taking their branch back, the single most routine event in a takeover's
// life, and the job text calls it "normal and not a failure" in the same breath.
// Both of the agent's signals are LOCAL git calls, so that verdict regularly
// lands BEFORE the operator's own flip (webhook -> mirror headSHA ->
// ReconcileOwnership), and the resulting implement-declined park is what the
// paragraph above makes permanent.
//
// So the flip corrects it: parkOwnerTask upgrades a takeover's
// implement-declined park to ownership-lost (stage.UpgradeDeclineToOwnershipLost),
// and the un-park branch below then resumes it exactly as it would any other
// stand-down. The distinction is made by the WORLD, not by parsing what the
// agent wrote: a deliberate decline is never followed by a flip, so it keeps its
// terminal; a divergence decline is followed by one by definition, because the
// push the agent saw IS the push the flip is reacting to. The other order needs
// nothing - restapi/outcome.go refuses an outcome on an already-parked Task, so
// an operator-first stand-down never produces the decline in the first place.
//
// expectFrom (optional, see MintReviewTask/bindMRToTask) is forwarded to the
// fresh-mint path's bindMRToTask call unchanged - fix #408: the takeover REST
// endpoint's caller Task is the MR's current controller owner by the time
// this runs (its own gate already proved it), so it passes its own name here
// to let bindMRToTask -> Minter.ownMergeRequest hand the controller flag over
// atomically, in one Update, instead of the endpoint demoting it standalone
// first.
func (m *Minter) MintOrUnparkTakeoverTask(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, mr *tatarav1alpha1.MergeRequest, requestingUser, commentBody string,
	sp objbudget.Spiller, expectFrom ...string) (*tatarav1alpha1.Task, error) {

	return m.mintOrUnparkTakeoverTask(ctx, proj, repo, mr, requestingUser, commentBody, sp, false, expectFrom...)
}

// mintOrUnparkTakeoverTask carries the retried flag MintOrUnparkTakeoverTask's
// exported signature does not: the tombstone path below retries against the
// freed name, and that retry must be BOUNDED (issue #521 boy-scout - it used to
// recurse with no bound, so two mints racing the same dead name could recurse
// indefinitely).
func (m *Minter) mintOrUnparkTakeoverTask(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, mr *tatarav1alpha1.MergeRequest, requestingUser, commentBody string,
	sp objbudget.Spiller, retried bool, expectFrom ...string) (*tatarav1alpha1.Task, error) {

	name := takeoverTaskName(proj, repo, mr.Spec.Number)

	var existing tatarav1alpha1.Task
	err := m.Client.Get(ctx, client.ObjectKey{Namespace: proj.Namespace, Name: name}, &existing)
	if err == nil {
		if existing.Status.ParkReason == stage.ReasonOwnershipLost {
			// THE ONE STATE-MOVING UN-PARK. A maintainer re-took ownership, so the
			// parked takeover Task resumes at under-implementation to push again -
			// from wherever the ownership flip caught it. stage.UnparkTakeover is
			// the only function allowed to clear the flag and move state together,
			// and it refuses every other park reason.
			if eerr := UnparkTakeoverTask(ctx, m.Client, sp, m.Metrics, &existing,
				tatarav1alpha1.StateUnderImplementation, time.Now()); eerr != nil {
				return nil, fmt.Errorf("takeover: un-park %s: %w", existing.Name, eerr)
			}
		}
		// INTERRUPTED MINT (#604). The fall-through is otherwise a pure no-op,
		// which is what the endpoint's idempotency relies on - but a Task whose
		// mrRefs are EMPTY is not a finished mint, it is one that died between
		// createTaskRaceSafe and stampMintStatus with MR ownership already
		// flipped. Returning it unchanged makes the maintainer's retry
		// permanently unable to repair it, and at under-implementation an empty
		// mrRefs is not survivable: submit_outcome(action=submitted) 400s
		// no-open-mr, and mr_write(action=open) reads mrRefs for its
		// idempotency check and would open a DUPLICATE PR on the human's branch.
		// So re-run the bind and the stamp. Both are idempotent
		// (ownMergeRequest no-ops when it already owns the CR; the stamp checks
		// for the ref first), so this is safe on every repeat call.
		if len(existing.Status.MRRefs) == 0 {
			if rerr := m.bindAndStampTakeoverMR(ctx, proj, repo, mr, &existing, sp, expectFrom...); rerr != nil {
				return nil, rerr
			}
		}
		return &existing, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("takeover: get task %s: %w", name, err)
	}

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: proj.Namespace,
			Annotations: map[string]string{
				tatarav1alpha1.AnnTakeoverHeadBranch: mr.Status.HeadBranch,
			},
		},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    proj.Name,
			RepositoryRef: repo.Name,
			Kind:          takeoverKind,
			Goal:          takeoverGoal(mr, requestingUser, commentBody),
			// #604: the WORK, not the gate. A takeover owns zero Issue CRs
			// (Source.IsPR below is what makes mintIssueCRs bail), so the gate at
			// `refined` could never grant for it and the Task stranded there. This
			// is also exactly where the re-take un-park above already lands.
			InitialState: tatarav1alpha1.StateUnderImplementation,
			// InitialParkReason is left empty: a takeover mint is NOT parked - a
			// maintainer just asked for it - and "requested by <user>" is not a
			// park reason anyway, so stamping it would make the create edge's
			// stage.Park refuse with UnknownReasonError. The requester and the
			// trigger comment live in Goal instead, which is free text.
			MergeOrder: []string{repo.Name},
			Source: &tatarav1alpha1.TaskSource{
				Provider:    providerOf(proj),
				IssueRef:    mr.Spec.URL,
				URL:         mr.Spec.URL,
				Number:      mr.Spec.Number,
				IsPR:        true,
				HeadSHA:     mr.Status.HeadSHA,
				Title:       mr.Status.Title,
				AuthorLogin: mr.Status.Author,
			},
		},
	}
	// Issue #517's descriptive pod name (see MintIssueTask). The literal already
	// carries an annotation map, which StampPodName adds to rather than replaces.
	agent.StampPodName(task, proj.Name, repo.Name)
	if err := controllerutil.SetControllerReference(proj, task, m.Scheme); err != nil {
		return nil, fmt.Errorf("takeover: set task ownerref: %w", err)
	}
	outcome, twin, err := m.createTaskRaceSafe(ctx, task)
	if err != nil {
		return nil, err
	}
	switch outcome {
	case MintExistingLive:
		// THE SECOND EXISTING-TASK PATH, and it needs the same mrRefs backfill as
		// the cached-Get one above. It is reached precisely when two endpoint
		// calls race: m.Client.Get is the CACHED reader, so a Task another
		// request created seconds ago 404s there, the create then collides here,
		// and the twin arrives with whatever state that other request had got to
		// - including, if it died mid-mint, empty mrRefs. Returning it unrepaired
		// left the repair to depend on the maintainer commenting a THIRD time,
		// after the cache had warmed.
		if len(twin.Status.MRRefs) == 0 {
			if rerr := m.bindAndStampTakeoverMR(ctx, proj, repo, mr, twin, sp, expectFrom...); rerr != nil {
				return nil, rerr
			}
		}
		return twin, nil
	case MintTombstoneDeleted:
		// createTaskRaceSafe collided with a DEAD twin and just deleted the
		// tombstone. There is no "next tick" on this endpoint-driven path:
		// retry ONCE against the now-freed name rather than surfacing a false
		// negative to the caller. BOUNDED (issue #521 boy-scout): the recursion
		// used to be unbounded, so two mints racing the same dead name could
		// recurse indefinitely.
		if retried {
			return nil, fmt.Errorf("takeover: task %s name still held by a dead twin after one retry", name)
		}
		return m.mintOrUnparkTakeoverTask(ctx, proj, repo, mr, requestingUser, commentBody, sp, true, expectFrom...)
	}

	if err := m.bindAndStampTakeoverMR(ctx, proj, repo, mr, task, sp, expectFrom...); err != nil {
		return nil, err
	}
	return task, nil
}

// bindAndStampTakeoverMR is the LAST TWO of the takeover mint's three writes:
// mirror-and-own the MergeRequest, then record it in status.mrRefs. Both the
// fresh-mint path and the interrupted-mint repair above call it, so the repair
// cannot drift from the thing it repairs.
//
// Both halves are idempotent - ownMergeRequest returns early when the Task
// already controller-owns the CR, and the stamp checks for the ref first - which
// is what makes it safe to re-run on an endpoint retry.
//
// PRECONDITION: mr IS OPEN, and since #604 that is load-bearing rather than
// incidental. mrExtFromMR hardcodes State:"open" and SyncMergeRequest assigns
// mr.Status.State from it unconditionally, so calling this for a CLOSED merge
// request would force its mirror back to `open`. On the fresh-mint path that
// could never happen; the repair fall-throughs above re-run it on a Task that
// already exists, which is new. It holds because restapi.mrTakeover refuses a
// non-open merge request before it ever reaches the minter - the ONLY caller
// chain into here. Any future caller must preserve that check or re-read the
// live state instead of trusting this snapshot.
func (m *Minter) bindAndStampTakeoverMR(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, mr *tatarav1alpha1.MergeRequest, task *tatarav1alpha1.Task,
	sp objbudget.Spiller, expectFrom ...string) error {

	if err := m.bindMRToTask(ctx, proj, repo, mrExtFromMR(mr), task, sp, expectFrom...); err != nil {
		return err
	}
	return m.stampMRRef(ctx, task, tatarav1alpha1.MergeRequestName(repo.Name, mr.Spec.Number))
}

// mrExtFromMR builds the scm.MergeRequest snapshot bindMRToTask's SyncMergeRequest
// upserts from the MergeRequest CR mint already has in hand, instead of routing
// through mrSnapshot's scm.PRRef (that path reconstructs the URL from a
// provider+owner/repo slug MintReviewTask gets from a forge PR listing; a
// takeover always has an authoritative mr.Spec.URL already, so reconstructing it
// would risk diverging from the one on record). It is still the SAME mirror
// writer (SyncMergeRequest, via bindMRToTask) - just fed from the CR instead of
// a fresh forge listing row.
func mrExtFromMR(mr *tatarav1alpha1.MergeRequest) scm.MergeRequest {
	return scm.MergeRequest{
		Number:     mr.Spec.Number,
		URL:        mr.Spec.URL,
		Title:      mr.Status.Title,
		Author:     mr.Status.Author,
		Body:       mr.Status.Body,
		State:      "open",
		HeadBranch: mr.Status.HeadBranch,
		HeadSHA:    mr.Status.HeadSHA,
		CIStatus:   mr.Status.CIStatus,
		Mergeable:  mr.Status.Mergeable,
	}
}

func takeoverGoal(mr *tatarav1alpha1.MergeRequest, user, comment string) string {
	return fmt.Sprintf("Take over %s at @%s's request. Triggering comment:\n\n%s",
		mr.Spec.URL, user, comment)
}
