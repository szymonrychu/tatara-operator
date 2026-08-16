package stage

import (
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// UnparkClass is the ONE axis park reasons divide on: who un-parks. It replaces
// stage.HasReentry, whose boolean could not distinguish "a human's comment
// resumes this" from "a timer retries this" from "nothing ever does".
type UnparkClass int

const (
	// UnparkNever ages out at ParkRetention and is reaped, and ONLY THEN does
	// the still-open issue become an orphan the ordinary sweep re-mints as a
	// fresh parked(backlog-sweep) owner - a parked Task RETAINS its controller
	// ownership, so IsOrphanIssue answers issue_owned for the whole seven days.
	// A human reply is not made to wait that long:
	// ProjectReconciler.resumeNoReentryParks severs and re-mints early on a
	// non-bot comment, which is the one-reply guarantee. Aging out is what
	// happens when nobody replies, and it is what the deleted `failed` terminal
	// did at FailedRetention.
	UnparkNever UnparkClass = iota
	// UnparkHuman: a non-bot comment resumes it.
	UnparkHuman
	// UnparkTimer: a backoff retry drives it, BOUNDED BY A COUNTER.
	UnparkTimer
	// UnparkRetired is a MIGRATION CLASS, and it is deliberately not one of the
	// three behavioural ones. Its members are parks written by a ceiling that no
	// longer exists: turn-budget-exhausted, review-loop-exhausted and
	// pod-recreation-exhausted. Every Task carrying one was stalled by a rule
	// O3 deleted, so leaving them to age out at ParkRetention would spend the
	// full seven days punishing them for a policy that is gone.
	//
	// EXACTLY ONCE, EVER. The driver (controller.driveRetiredUnparks) stamps
	// tatara.dev/retired-park-migrated before it un-parks and skips anything
	// already stamped, so this class is a one-shot sweep and not a fourth
	// re-entry rule. A Task that re-parks for some other reason afterwards is
	// handled by that reason's own class, not by this one.
	//
	// stage.Unpark does NOT have an arm for these reasons and must not grow one:
	// it would turn a migration into a permanent retry loop. Unpark treats them
	// exactly as it treats UnparkNever (DeclineNoReentry), which is also what
	// keeps the reaper's unparkFires probe honest - an un-migrated leftover still
	// ages out at ParkRetention rather than being held alive forever.
	UnparkRetired
)

// unparkClasses is total over ParkReasons and that totality is asserted.
//
// DELIBERATE DEVIATION FROM THE #521 PLAN, and the reason is the counter. The
// plan put eleven reasons in UnparkTimer, including the eight that used to
// target `failed` (operator-error, object-too-large, triage-stalled,
// agent-contract-mismatch, merge-order-missing, admission-starved,
// merge-auth-refused, fold-adoption-unverified). NONE of those has a bounding
// counter, and there is no CRD field to hold one - so classifying them as
// timer-retriable is either a lie or an unbounded retry loop, which is the
// exact shape #480 and #513 killed. They stay UnparkNever, which reproduces
// today's behaviour EXACTLY: they age out at ParkRetention, the reaper collects
// them, and only THEN is the still-open issue an orphan the sweep re-mints as a
// NEW Task rather than a resurrected zombie one. A human who replies before that
// does not wait it out - resumeNoReentryParks severs and re-mints in the same
// pass - but nothing here un-parks, which is the point.
//
// UnparkTimer is therefore the three reasons that carry a real bound today:
// merge-timeout (MergeReentries), deploy-timeout (DeployReentries) and
// no-outcome (parkedFromState gate).
var unparkClasses = map[string]UnparkClass{
	// A human's comment. These are the four that were comment-driven before.
	ReasonBacklogSweep:       UnparkHuman,
	ReasonAwaitingHuman:      UnparkHuman,
	ReasonIdentityUnverified: UnparkHuman,
	ReasonHandoffStalled:     UnparkHuman,

	// A bounded retry.
	ReasonMergeTimeout:  UnparkTimer,
	ReasonDeployTimeout: UnparkTimer,
	ReasonNoOutcome:     UnparkTimer,

	// A ONE-SHOT MIGRATION. The ceiling that wrote each of these is deleted, so
	// the park is a verdict from a policy that no longer exists. See
	// UnparkRetired. The three constants and the task_types.go ParkReason enum
	// members STAY: removing an enum member breaks validation on the next status
	// write of EVERY object already carrying it, including the ones this
	// migration is about to touch.
	ReasonTurnBudgetExhausted:    UnparkRetired,
	ReasonReviewLoopExhausted:    UnparkRetired,
	ReasonPodRecreationExhausted: UnparkRetired,

	// Nobody. These are exhaustion terminals and ex-`failed` reasons:
	// re-entering one would escape its own cap one lap at a time.
	ReasonStageDeadline:          UnparkNever,
	ReasonNameTooLong:            UnparkNever,
	ReasonImplementDeclined:      UnparkNever,
	ReasonReviewPostRefused:      UnparkNever,
	ReasonMergeBlocked:           UnparkNever,
	ReasonDeployBlocked:          UnparkNever,
	ReasonHeadMoving:             UnparkNever,
	ReasonCIRed:                  UnparkNever,
	ReasonCIBlocked:              UnparkNever,
	ReasonOwnershipLost:          UnparkNever,
	ReasonTriageStalled:          UnparkNever,
	ReasonOperatorError:          UnparkNever,
	ReasonObjectTooLarge:         UnparkNever,
	ReasonAgentContractMismatch:  UnparkNever,
	ReasonMergeOrderMissing:      UnparkNever,
	ReasonAdmissionStarved:       UnparkNever,
	ReasonMergeAuthRefused:       UnparkNever,
	ReasonFoldAdoptionUnverified: UnparkNever,
}

// UnparkClassFor reports who un-parks a park reason. ok is false for a reason
// that is not a park reason at all.
func UnparkClassFor(reason string) (UnparkClass, bool) {
	c, ok := unparkClasses[reason]
	return c, ok
}

// IsParkReason reports whether r may be written to status.parkReason.
func IsParkReason(r string) bool { return parkReasonSet[r] }

// IsRejectReason reports whether r is a status.stateReason for `rejected`.
func IsRejectReason(r string) bool { return rejectReasonSet[r] }

// IsDoneReason reports whether r is a status.stateReason for `done`.
func IsDoneReason(r string) bool { return doneReasonSet[r] }

// StillParkedError is returned by Enter when a caller tries to move a PARKED
// Task's state without un-parking it first. It exists because `parkReason` is a
// stringly flag, and a stringly flag a writer can forget to clear is a new
// silent wedge in the same genre as #521. There is exactly one way out of a
// park and it is Unpark (or UnparkTakeover, the one documented exception).
type StillParkedError struct {
	State      string
	ParkReason string
}

func (e *StillParkedError) Error() string {
	return fmt.Sprintf("task is parked at %s(%s); un-park before entering another state", e.State, e.ParkReason)
}

// NotParkedError is returned by the un-park functions for a Task that is not
// parked at all. It is a caller bug, not a decline.
type NotParkedError struct{ State string }

func (e *NotParkedError) Error() string {
	return fmt.Sprintf("task at %s is not parked", e.State)
}

// Park is the ONE way a Task is parked. It stamps the whole tuple in one
// mutation - reason, parkedAt, parkedFromState - because a park that is not
// atomic with its reason is the #521 bug shape: annotations were rejected as a
// representation for exactly this, since they are not on the status subresource
// and cannot be one write.
//
// It does NOT change State. A Task parks WHERE IT IS.
func Park(t *v1alpha1.Task, reason string, now time.Time) error {
	if !IsParkReason(reason) {
		return &UnknownReasonError{Reason: reason}
	}
	if t.Status.ParkReason != "" {
		return nil // idempotent: already parked, first reason wins
	}
	stamp := metav1.NewTime(now)
	t.Status.ParkReason = reason
	t.Status.ParkedAt = &stamp
	t.Status.ParkedFromState = t.Status.State
	// FOLD THIS RESIDENCY INTO THE CARRY, on every park reason, not just the two
	// timeouts. Un-park re-arms the state clock from now (or a Task un-parked
	// with a stale stateEnteredAt re-parks on the same pass, live in #513), so
	// the carry is the ONLY thing that keeps residency continuous across a
	// round trip - and ResidencyExceeded reads it, which is the whole point:
	// six hours in under-implementation across three re-entries IS six hours,
	// and a fresh cap per re-entry is the unbounded-loop shape #480 killed.
	//
	// stampEnter zeroes it on every genuine state TRANSITION, so an unrelated
	// later entry never inherits it.
	if t.Status.StateEnteredAt != nil {
		t.Status.StageElapsedCarrySeconds += int(now.Sub(t.Status.StateEnteredAt.Time).Seconds())
	}
	t.Status.StateEnteredAt = &stamp
	return nil
}

// clearPark is the ONLY assignment that empties ParkReason. It is unexported on
// purpose: Unpark, UnparkTakeover and repark are its only callers.
func clearPark(t *v1alpha1.Task) {
	t.Status.ParkReason = ""
	t.Status.ParkedAt = nil
}

// repark swaps one park reason for another WITHOUT ever leaving the Task
// un-parked. Its only use is a timer un-park that has spent its counter:
// parked(merge-timeout) at MaxMergeReentries becomes parked(merge-blocked),
// which is UnparkNever, so it ages out instead of retrying forever. It replaces
// the deleted failed(merge-blocked) terminal.
func repark(t *v1alpha1.Task, reason string, now time.Time) {
	clearPark(t)
	// Park validates the reason; both call sites pass a compile-time constant
	// that is a member of ParkReasons, so the error is unreachable.
	_ = Park(t, reason, now)
}

// UpgradeDeclineToOwnershipLost swaps a TAKEOVER Task's implement-declined park
// for ownership-lost, and refuses every other (kind, reason) pair. It is the
// #604 review's fix for the ordinary stand-down, and it exists because the two
// halves of that change disagreed about what a human push means:
//
//   - the takeover job text calls a human push "normal and not a failure";
//   - tatara-implement-takeover, the skill that turn is now REQUIRED to invoke,
//     tells the pod to submit_outcome(action="declined") on exactly that signal
//     (an ls-remote mismatch, or a non-fast-forward push rejection).
//
// Both of the agent's signals are LOCAL git calls, so it reaches that verdict
// ahead of the operator's own flip, which waits on webhook -> mirror headSHA ->
// ReconcileOwnership. The agent-first order parks implement-declined, which is
// UnparkNever, which nothing resumes: the re-take is refused, DrainStandDownMerge
// cannot re-drive an approved external head, and a merge request a human simply
// took back is stuck until ParkRetention.
//
// THE RULE IS "A HUMAN PUSH SUPERSEDES A DECLINED TAKEOVER", NOT "A DIVERGENCE
// DECLINE IS TOLD APART FROM A DELIBERATE ONE". The first draft of this claimed
// the second, and it is FALSE: the job text requires the agent to comment on the
// merge request explaining itself before it declines, so the expected human
// response to a DELIBERATE decline is to read that comment and push a fix - and
// that push flips ownership exactly like a divergence does. There is no signal
// that separates the two. The agent's decline_reason is free text and cannot be
// trusted with the question, and a trustworthy one would be a new outcome field,
// i.e. an API change and its own design call.
//
// So the discriminator is not intent, it is the merge request: a takeover Task
// is refused resumption for as long as the branch is still tatara's, and stops
// being refused the moment it is the human's again. That is the right rule
// anyway, and it is the one the rest of the machine already follows - it is what
// UnparkTakeover's targets, DrainStandDownMerge and the whole stand-down path
// are keyed on. The alternative, refusing a takeover forever on a merge request
// its author has since moved on, is #604's own failure shape with a different
// label on it.
//
// So a DELIBERATE decline is terminal until the human pushes, and resumable
// after. That is a behaviour change on the decline, stated plainly rather than
// smuggled in under an invariant that does not hold, and it includes
// DrainStandDownMerge being able to merge - on an approved review of the HUMAN's
// head - a merge request an earlier turn declined.
//
// "The branch is the human's again" is an OBSERVATION the caller makes, and it
// is imperfect: the operator's only signal is liveHead != LastBotHeadSHA, so a
// stale baseline reads tatara's own push as a hand-back. That is this package's
// caller's problem, not this function's - it is argued, with the remedies that do
// not work, at controller/ownership.go's parkOwnerTask.
//
// NARROW ON BOTH AXES ANYWAY, because both narrowings still are load-bearing:
//
//   - kind=takeover only. Only a takeover's Task is CONTINGENT on tatara owning
//     somebody else's merge request; on any other kind the branch is tatara's
//     own, so a human pushing to it is not "taking it back" and must not reopen
//     a refusal the agent stands behind.
//   - implement-declined only. Every other UnparkNever park on a takeover is a
//     genuine terminal (an exhaustion cap, an operator error, a contract
//     mismatch) reached without any judgement about the merge request at all,
//     and a human push must not launder one into a resumable Task. That is also
//     what keeps restapi.mrTakeover's 409 gate alive rather than dead.
//
// It is NOT repark, and the reason is narrow: this is ONE park's reason being
// corrected, not a second park. repark clears and re-Parks, so Park's park-event
// bookkeeping - fold this state's elapsed residency into StageElapsedCarrySeconds,
// re-arm StateEnteredAt, restamp ParkedFromState - would run a second time for a
// Task that only ever parked once. Only the reason and the park stamp move here.
// The stamp DOES move, so the maintainer gets a full ParkRetention window from
// the moment the branch actually came back, exactly as a fresh park would have.
//
// THE JUSTIFICATION THAT USED TO BE HERE WAS FALSE; do not reinstate it. It
// said a repark would charge the resumed Task for the days it sat parked and so
// blow ResidencyExceeded on its first pass back. It cannot: both exits from
// parked(ownership-lost) - MintOrUnparkTakeoverTask and DrainStandDownMerge - go
// through UnparkTakeover -> stampEnter, which zeroes StageElapsedCarrySeconds
// and nils StateWorkStartedAt, and ResidencyExceeded returns false outright on a
// nil StateWorkStartedAt. Unpark/reArm, the one path that DOES preserve the
// carry, refuses ownership-lost by class. Nothing reads the carry while the Task
// is parked either (task_stage.go checks residency only on an UN-parked Task).
//
// changed is false, with no error, when the Task is ALREADY parked
// ownership-lost. That is a converged state, not a misuse: every caller is a
// converge-by-retry path (resumeFlipToExternal exists to re-drive an interrupted
// flip, and its Get reads a CACHE that can still say implement-declined after
// the write landed), so erroring there would block the very retry the caller is
// making and would double-count the upgrade.
func UpgradeDeclineToOwnershipLost(t *v1alpha1.Task, now time.Time) (bool, error) {
	if t.Spec.Kind != kindTakeover {
		return false, fmt.Errorf("decline upgrade is takeover-only, got kind %q", t.Spec.Kind)
	}
	if t.Status.ParkReason == ReasonOwnershipLost {
		return false, nil // already converged
	}
	if t.Status.ParkReason != ReasonImplementDeclined {
		return false, fmt.Errorf("decline upgrade requires parkReason=%s, got %q",
			ReasonImplementDeclined, t.Status.ParkReason)
	}
	stamp := metav1.NewTime(now)
	t.Status.ParkReason = ReasonOwnershipLost
	t.Status.ParkedAt = &stamp
	return true, nil
}

// takeoverTargets is UnparkTakeover's OWN allow-list, and it is deliberately
// not the transition table. under-implementation -> merged is not an edge any
// other caller may take (it would skip review), but a re-taken MR whose human
// head already carries an approved review resumes at the merge phase, from
// wherever the ownership flip happened to catch it.
var takeoverTargets = map[string]bool{
	v1alpha1.StateUnderImplementation: true,
	v1alpha1.StateMerged:              true,
}

// UnparkTakeover is the ONE function permitted to clear parkReason AND change
// State in the same write, and it is narrow on every axis:
//
//   - the park reason must be ownership-lost;
//   - the target must be in takeoverTargets;
//   - GUARD 1 still applies - a kind=review Task may never reach merged or
//     under-implementation, by this path or any other.
//
// Everything else goes through Unpark, which never changes State.
func UnparkTakeover(t *v1alpha1.Task, to string, now time.Time) error {
	if t.Status.ParkReason != ReasonOwnershipLost {
		return fmt.Errorf("takeover un-park requires parkReason=%s, got %q", ReasonOwnershipLost, t.Status.ParkReason)
	}
	if !takeoverTargets[to] {
		return &IllegalTransitionError{From: t.Status.State, To: to}
	}
	if t.Spec.Kind == kindReview {
		return &IllegalTransitionError{From: t.Status.State, To: to}
	}
	clearPark(t)
	stampEnter(t, to, "", now)
	return nil
}

// mrTerminalReasons is UnparkForMRTerminal's allow-list: the three stateReasons
// that mean THE WORLD MOVED ON, not that the Task made progress. Every one of
// them is written by externalTerminalEdge or the takeover finalize, off a forge
// fact the operator merely observed.
var mrTerminalReasons = map[string]bool{
	ReasonMRMergedExternally: true,
	ReasonMRClosedExternally: true,
	ReasonMRTakenOver:        true,
}

// UnparkForMRTerminal is the THIRD and last exception to "there is exactly one
// way out of a park and it is Unpark", and it is the narrowest of them.
//
// WHY A PARK MUST NOT SURVIVE THIS. A park stops a Task making stage PROGRESS
// while it waits on something - a human's answer, a retry clock, a ceiling. An
// owned MR reaching a TERMINAL forge state is not progress: it is the world
// moving on, and it makes the wait pointless. The measured shape (2026-08-10,
// 7 of 7) is a review Task that posts its verdict, parks awaiting-human, and
// then has its PR merged by a maintainer minutes later. Nobody comments on a
// merged PR, so the UnparkHuman rule never fires; stage.Unpark's own
// ReasonAwaitingHuman arm sees the merge and answers DeclineMergedMR, whose
// comment says the Task "ages out at ParkRetention and is reaped". That was the
// right call for what it decides (never spawn a pod against a merged MR) and it
// is still the right call - but its stated CONSEQUENCE was written before #33's
// pod-less finalize existed. This closes that gap without touching the decline.
//
// IT IS NARROW ON EVERY AXIS, and each guard is load-bearing:
//
//   - `to` must be a TERMINAL OUTCOME. This is the difference between ending a
//     Task and RESTARTING one. ownMRsShippedEdge targets `merged`, which still
//     owes the merge cursor, the deploy ledger and the issue closes - resuming a
//     parked Task into that pipeline is real work restarted behind the back of
//     the human the park was waiting for, which is precisely what a park exists
//     to prevent.
//   - `reason` must be in mrTerminalReasons, so this can only ever be reached
//     from an external forge fact and never laundered onto an ordinary edge.
//   - It does NOT re-arm the clocks (no reArm). The Task is going terminal;
//     stampEnter inside Enter sets what a terminal entry needs, and re-arming a
//     residency clock for a Task that will never run again is meaningless.
//
// It clears the flag ONLY: the caller applies Enter in the SAME mutation, so
// the un-park and the terminal state reach the API server as ONE status write.
// Two writes would leave a window in which the Task is live, un-parked and
// non-terminal, which is the #521 bug shape exactly.
//
// A Task that is not parked is a no-op success, so the caller never has to ask.
func UnparkForMRTerminal(t *v1alpha1.Task, to, reason string) error {
	if t.Status.ParkReason == "" {
		return nil
	}
	if !v1alpha1.TaskIsTerminalOutcome(to) {
		return fmt.Errorf("mr-terminal un-park requires a terminal outcome, got %q", to)
	}
	if !mrTerminalReasons[reason] {
		return fmt.Errorf("mr-terminal un-park requires an external-terminal reason, got %q", reason)
	}
	clearPark(t)
	return nil
}

// UnparkRetiredPark is the O3 MIGRATION un-park, and it is a takeover in
// everything but name: the operator - not a human, not a timer, not a re-entry
// rule - decides that this park is void because the ceiling that wrote it has
// been deleted. Like UnparkTakeover it bypasses Unpark's re-entry rules
// entirely; unlike UnparkTakeover it does NOT move State, because there is
// nothing wrong with where the Task is. Park is orthogonal to state (#521), so
// clearing the flag drops the Task straight back into the live state it was
// working in.
//
// It refuses any reason outside UnparkRetired. That is the whole safety
// argument: the three reasons in that class are the ONLY parks O3 invalidated,
// and a caller that could point this at awaiting-human or merge-blocked would be
// laundering an arbitrary park through a migration.
//
// It does NOT stamp the tatara.dev/retired-park-migrated annotation. internal/
// stage is a pure status-mutating package and cannot write metadata; the
// once-only guarantee is the CALLER's (controller.driveRetiredUnparks), which
// stamps before it writes and skips anything already stamped.
//
// reArm, not stampEnter: same stamps as every other un-park, no state change, no
// reason change, and StageElapsedCarrySeconds PRESERVED - so the 24h residency
// dead-man switch stays cumulative across the migration and a Task that had
// already burned 20 hours does not buy a fresh day by being un-parked.
func UnparkRetiredPark(t *v1alpha1.Task, now time.Time) error {
	class, ok := UnparkClassFor(t.Status.ParkReason)
	if !ok || class != UnparkRetired {
		return fmt.Errorf("retired un-park requires a retired-ceiling parkReason, got %q", t.Status.ParkReason)
	}
	if now.IsZero() {
		now = time.Now()
	}
	reArm(t, now)
	return nil
}
