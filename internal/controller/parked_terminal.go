// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// reconcileParkedExternalTerminal is the PARKED sibling of the two
// externalTerminalEdge finalize sites, and it is the ONE self-heal besides
// repairParkedWithLivePod that may run before Reconcile's parked early return.
//
// THE DEFECT IT CLOSES, measured 2026-08-10 at 7 out of 7: every review Task
// whose PR was merged sat parked awaiting-human and never reached terminal. The
// review agent posts its verdict and parks; a maintainer merges the PR one to
// twenty minutes later; the Task never notices. It is then reaped at
// ParkRetention, seven days on.
//
// THE MECHANISM IS ONE STEP EARLIER THAN "stage.Enter REFUSES A PARKED TASK".
// TaskReconciler.Reconcile returns for ANY parked Task before reconcileClocks
// runs at all, so externalTerminalEdge - which has computed exactly the right
// answer since #33, and since #578 for non-review kinds - is never even
// EVALUATED. Enter's StillParkedError is only the second line of defence behind
// that, which is why the fix belongs here and not at the choke point.
//
// NOTHING ALREADY COVERED IT, and one place looked straight at the condition
// and chose otherwise: stage.Unpark's ReasonAwaitingHuman arm answers
// DeclineMergedMR on a merged MR, with the comment "the Task ages out at
// ParkRetention and is reaped". That decline is still correct - it decides never
// to spawn a REVIEW POD against a merged PR, and this spawns nothing - but its
// stated consequence predates #33's pod-less finalize. This completes it rather
// than overriding it, and the decline is left exactly as it is.
//
// TERMINAL OUTCOMES ONLY, and that is the whole scope argument. A park stops a
// Task making stage PROGRESS while it waits; an MR reaching a terminal forge
// state is the world moving on, which makes the wait pointless but does not
// answer the question the park was asked. Ending the Task is therefore
// permitted; RESTARTING one is not. ownMRsShippedEdge targets `merged` for a
// non-review Task - still owing the merge cursor, the deploy ledger and the
// issue closes - so that edge is deliberately NOT taken here: resuming a parked
// Task into a live pipeline behind the back of the human it was waiting for is
// exactly what a park exists to prevent. A non-review Task whose MR was CLOSED
// unmerged does finalize, because that edge is genuinely terminal.
//
// Nothing is stranded by the exclusion. A non-review Task parked under an
// UnparkNever reason belongs to driveStrandedParks (C.3), and one parked
// awaiting-human is waiting on a human who has genuinely not answered.
//
// C.2 STILL RUNS. It goes through r.enter, so the choke point posts the terminal
// notice and stamps tatara-parked on every still-open owned Issue - which is the
// entire point: a review Task going terminal is exactly when its issue must not
// be silently dropped.
//
// It returns handled=true when it applied an edge, in which case the caller
// returns immediately rather than falling through to the parked early return.
func (r *TaskReconciler) reconcileParkedExternalTerminal(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, now time.Time) (handled bool, err error) {

	if !tatarav1alpha1.Parked(task) || tatarav1alpha1.TaskDone(task) {
		return false, nil
	}
	// THE CHEAP GATE, and it is deliberately the SAME ONE ensureStagePod's
	// pre-dispatch guard uses, so the three external-terminal sites agree on
	// their population as well as on their edge. It keeps this off the hot path:
	// parked Tasks are roughly two thirds of the live population and most of them
	// are backlog owners (kind=issue at `new`), which cost one comparison and no
	// API call. Only a review-kind Task, or one sitting at awaiting-review, pays
	// for the List.
	//
	// It CANNOT be status.mrRefs, however tempting the zero-cost check looks:
	// ownedMergeRequests resolves ownership by CONTROLLER OWNERREF over a List,
	// not from that field, so a Task owning MRs with an unset mrRefs - which is a
	// real shape, mt-i-mtg-decks-44 has `mergeRequestRefs: null` - would be
	// skipped by a guard that trusted it while the finalize sites saw its MRs.
	if task.Spec.Kind != "review" && task.Status.State != tatarav1alpha1.StateAwaitingReview {
		return false, nil
	}

	mrs, err := ownedMergeRequests(ctx, r.mrReader(), task)
	if err != nil {
		return false, err
	}

	edge, ok := externalTerminalEdge(task, mrs)
	if !ok && len(mrs) == 0 {
		// The takeover sibling, same as both live finalize sites carry: a
		// taken-over parent controller-owns zero MRs, so externalTerminalEdge
		// cannot fire on the empty set, yet its refs are owned by a different live
		// Task and it has no lifecycle left. Reached only when mrRefs is non-empty
		// but none of them is ours any more, so it costs nothing on the common path.
		over, oerr := TaskTakenOver(ctx, r.mrReader(), task)
		if oerr != nil {
			return false, oerr
		}
		if over {
			edge, ok = stage.Edge{To: tatarav1alpha1.StateRejected, Reason: stage.ReasonMRTakenOver}, true
		}
	}
	if !ok || !tatarav1alpha1.TaskIsTerminalOutcome(edge.To) {
		return false, nil
	}

	// THE TWO BRANCHES ARE NOT EQUALLY GUARDED, and it is worth knowing which is
	// load-bearing. done(mr-merged-externally) is protected TWICE - by
	// AllMRsTerminal here and again by LegalFor's own AllMRsMerged conjunct on
	// awaiting-review -> done - so a too-broad predicate aimed at it is caught
	// downstream anyway. rejected(mr-closed-externally) has NO such second
	// guard: LegalFor does not constrain that edge on MR state at all, so
	// AllMRsTerminal inside externalTerminalEdge is the ONLY thing standing
	// between a still-open PR and a rejected Task. That is the branch the
	// boundary tests mutate against.
	//
	// LEGALITY IS PRE-CHECKED, not left to fail inside the choke point. An
	// illegal edge there costs operator_illegal_stage_transition_total, whose
	// alert declares ANY non-zero value a bug in the transition table - and a
	// parked Task can be sitting in a state this edge is not legal from (LegalFor
	// GUARD 3 allows awaiting-review -> done for kind=review only). LegalFor is
	// park-blind, so asking it here is exact.
	if !stage.LegalFor(task, mrs, task.Status.State, edge.To) {
		return false, nil
	}

	log.FromContext(ctx).Info("parked task finalized: every owned MR reached a terminal forge state while it waited",
		"action", "parked_finalize_terminal_mr", "resource_id", task.Name,
		"kind", task.Spec.Kind, "from", task.Status.State, "park_reason", task.Status.ParkReason,
		"to", edge.To, "reason", edge.Reason)

	return true, EnterStage(ctx, r.Client, r.spiller(proj), r.Metrics, task, mrs,
		edge.To, edge.Reason, now, nil,
		WithParkClearedForMRTerminal(),
		WithTerminalIssueRelease(&TerminalReleaser{Client: r.Client, SCMFor: r.SCMFor, Metrics: r.Metrics}))
}
