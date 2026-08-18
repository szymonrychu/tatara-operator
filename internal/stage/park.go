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
	// UnparkRetry is the MACHINE lane: the park names a technical blocker that a
	// machine is expected to clear on its own, so the release is a TIMER and not
	// an event. It is the only class whose release is bounded by an attempt
	// count (status.retryAttempts, capped at MaxUnparkRetries) rather than by a
	// counter tied to one specific edge, and it is the only one whose exhaustion
	// is LOUD: the lane re-parks to ReasonRetryExhausted, which is UnparkHuman,
	// and the controller posts a comment naming the blocker on the way.
	//
	// It differs from UnparkTimer in what the wait is FOR. A timer park waits on
	// this operator's own next lap (a merge cursor, a deploy ledger) and its
	// counter is per-edge; a retry park waits on something OUTSIDE the operator
	// - a forge pipeline, a base branch that keeps moving - which is why the
	// wait grows (UnparkRetryBackoffBase doubling to UnparkRetryBackoffCap)
	// instead of firing on every 30s pass.
	UnparkRetry
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

	// A bounded retry. no-outcome has a SECOND owner and needs it: its arm
	// declines merged-mr, so on a Task the UnparkRetry lane released - which has
	// a merged merge request by construction - the timer never fires at all. See
	// LaneStranded.
	ReasonMergeTimeout:  UnparkTimer,
	ReasonDeployTimeout: UnparkTimer,
	ReasonNoOutcome:     UnparkTimer,

	// A BACKED-OFF retry against a blocker outside this operator, bounded by
	// MaxUnparkRetries and escalated LOUDLY when the budget is spent. See
	// UnparkRetry. merge-auth-refused is deliberately NOT here: a refused
	// credential does not fix itself, and retrying a merge that may have
	// partially succeeded is how a double-merge happens.
	ReasonCIPending:          UnparkRetry,
	ReasonCIFailed:           UnparkRetry,
	ReasonMergeConflictRetry: UnparkRetry,
	ReasonMRSurfaceSpent:     UnparkRetry,
	// The lane's own terminal, and it is a HUMAN park on purpose: the machine
	// has had MaxUnparkRetries laps and a human is the next actor.
	ReasonRetryExhausted: UnparkHuman,

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
	ReasonMergeConflict:          UnparkNever,
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

// mergeStageParks is the SECOND axis park reasons divide on, and it is
// ORTHOGONAL to UnparkClass: not who un-parks, but WHERE IN THE LIFECYCLE the
// park was written.
//
// A member can only be written once the implementation has been ACCEPTED and
// the work is at or past the merge. What blocks it is therefore OUTSIDE the
// code - a credential, a protected branch, a sibling repo that already landed,
// somebody else pushing to the branch - so a fresh implementation Task cannot
// clear the obstacle, and minting one CLOSES the reviewed merge request that
// holds the only copy of the finished work. Measured twice on 2026-08-10:
// ansible!16 and terraform!215, both approved and conflict-free, closed unmerged
// by the automatic re-entry lap with their blocker untouched.
//
// PER MEMBER, the writer that makes it merge-stage:
//
//	merge-timeout        the merged-state stage deadline, after review
//	merge-blocked        repark of merge-timeout at MaxMergeReentries
//	merge-auth-refused   the forge refused the merge CREDENTIAL (merge.go)
//	merge-order-missing  ReconcileMerging entered with an empty mergeOrder
//	head-moving          MaxHeadMoveReentries: somebody else owns the branch
//	ci-red               CIRed's anyMerged arm ONLY: part of mergeOrder LANDED
//	merge-conflict       MergeConflict's anyMerged arm ONLY: same, for a DIRTY
//	                     merge request instead of a red check
//	deploy-timeout       the deployed-state stage deadline, post-merge
//	deploy-blocked       repark of deploy-timeout at MaxDeployReentries
//
// ci-blocked IS THE BOUNDARY CASE AND IS DELIBERATELY OUT. stage.CIRed reaches
// it only when NOTHING in mergeOrder has merged, and red CI on an unmerged
// branch is fixed by a new commit and nothing else - so re-implementing is the
// remedy there, not a way of destroying one. merge-conflict does NOT follow
// ci-blocked out, and the asymmetry is in the reasons, not an oversight: the
// exhaustion terminal of the conflict cycle is merge-blocked (already a member),
// and merge-conflict is written ONLY on the anyMerged refusal, where something
// HAS landed - the ci-red half of the pair, not the ci-blocked half. Every other park reason is written
// at or before review, about the attempt itself, and keeps its existing
// treatment; ownership-lost is doubly out, because the merge request is a
// HUMAN's and ourMR already refuses to touch it.
var mergeStageParks = map[string]bool{
	ReasonMergeTimeout:      true,
	ReasonMergeBlocked:      true,
	ReasonMergeAuthRefused:  true,
	ReasonMergeOrderMissing: true,
	ReasonHeadMoving:        true,
	ReasonCIRed:             true,
	ReasonMergeConflict:     true,
	// The retry-lane names for the SAME two writers (CIRed's and
	// MergeConflict's anyMerged arms). Moving a park into the lane changes WHO
	// releases it, not WHERE IN THE LIFECYCLE it was written, so they are
	// classified here for the axis to stay honest about what the reason MEANS.
	//
	// THEY DO NO WORK TODAY, AND SAYING SO IS THE POINT. IsMergeStagePark's two
	// production consumers (controller.driveStrandedParks and
	// controller.closeTaskBotMRs) are both reached only PAST a
	// `class != UnparkNever && class != UnparkRetired` filter, and both of these
	// are UnparkRetry - so what actually keeps the ansible!16 / terraform!215
	// shape from re-opening for them is the CLASS, not this map. These two
	// entries are the belt to that braces: the day a consumer is added outside
	// the class filter, or the day one of these reasons is reclassified, the
	// refusal is already correct instead of having to be remembered.
	// ReasonRetryExhausted is deliberately absent for the same honesty: it is
	// UnparkHuman, no consumer reaches it either, and adding it would only widen
	// a map whose members are already inert.
	ReasonCIFailed:           true,
	ReasonMergeConflictRetry: true,
	ReasonDeployTimeout:      true,
	ReasonDeployBlocked:      true,
}

// IsMergeStagePark reports whether reason was written at or after the merge, on
// work that was already implemented and reviewed. Callers use it to refuse the
// two things that destroy such work: an automatic re-implementation, and the
// close of the merge request it would replace.
func IsMergeStagePark(reason string) bool { return mergeStageParks[reason] }

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
//
// RetryNextAt goes with the park because it is a schedule FOR that park, and a
// stale one would let the next retry park be released on a timer somebody else
// set. RetryAttempts and RetryBlocker deliberately do NOT: see RetryAttempts'
// field doc - refunding the lap on the release it paid for is what would make
// MaxUnparkRetries unreachable, and the blocker name is exactly what tells the
// next ArmRetry whether the re-park is the SAME blocker (keep the spend) or a
// new one (start over).
func clearPark(t *v1alpha1.Task) {
	t.Status.ParkReason = ""
	t.Status.ParkedAt = nil
	t.Status.RetryNextAt = nil
}

// RetryWait is the UnparkRetry backoff: the wait an attempts-th lap serves.
// attempts is the count ALREADY SPENT, so a fresh park (0) waits
// UnparkRetryBackoffBase and each further lap doubles it up to
// UnparkRetryBackoffCap. The doubling is a loop rather than a shift so a large
// (or corrupt) counter cannot overflow into a negative duration, which would
// make every lap due immediately - the failure direction that turns a backoff
// into a hot loop.
func RetryWait(attempts int) time.Duration {
	wait := v1alpha1.UnparkRetryBackoffBase
	for i := 0; i < attempts; i++ {
		if wait >= v1alpha1.UnparkRetryBackoffCap {
			break
		}
		wait *= 2
	}
	if wait > v1alpha1.UnparkRetryBackoffCap {
		return v1alpha1.UnparkRetryBackoffCap
	}
	return wait
}

// ArmRetry schedules the next release of an UnparkRetry park and SPENDS the lap
// it is scheduling. The two happen in one mutation on purpose: a schedule
// written without the charge is a lane that retries forever, and a charge
// written without the schedule is a lane that burns its budget on one pass.
//
// It is the ONLY thing that increments RetryAttempts, so "how many laps has
// this blocker had" has exactly one writer.
//
// IT IS ALSO WHERE THE COUNTER IS SCOPED TO A BLOCKER. A lap is charged against
// status.retryBlocker, and a park reason that differs from the recorded one
// starts from zero: the previous blocker is behind us (the merge corridor moved
// on, or a different technical wall was hit), and inheriting its spend
// escalates early with a comment naming laps that were never spent on the
// blocker it names.
func ArmRetry(t *v1alpha1.Task, now time.Time) error {
	if t.Status.ParkReason == "" {
		return &NotParkedError{State: t.Status.State}
	}
	if class, ok := UnparkClassFor(t.Status.ParkReason); !ok || class != UnparkRetry {
		return fmt.Errorf("stage: park reason %q is not in the retry lane", t.Status.ParkReason)
	}
	if t.Status.RetryBlocker != t.Status.ParkReason {
		t.Status.RetryBlocker = t.Status.ParkReason
		t.Status.RetryAttempts = 0
	}
	next := metav1.NewTime(now.Add(RetryWait(t.Status.RetryAttempts)))
	t.Status.RetryNextAt = &next
	t.Status.RetryAttempts++
	return nil
}

// RescheduleRetry re-serves the CURRENT lap's wait WITHOUT spending another
// one. It is ArmRetry's twin for the one verdict that is neither "the blocker is
// still there" nor "release it": the blocker has CLEARED but the project has no
// live room for the pod the release would mint.
//
// Without it that verdict is not recorded anywhere, so the driver re-reads the
// forge for the same Task on every 30s pass and gets the same answer forever -
// two such Tasks consume maxRetryBlockerReadsPerPass outright and defer every
// other due park behind them. Re-arming paces the re-read by the backoff
// instead, and the Task still releases on the first due pass that finds room.
//
// NO LAP IS CHARGED and retryBlocker is untouched, because a ceiling is the
// operator's constraint and not the blocker's: charging it would escalate a
// queue to a human as though a pipeline had failed. The wait is therefore
// RetryWait(attempts-1) - the one the lap already paid for - and not the longer
// one the next lap would buy.
func RescheduleRetry(t *v1alpha1.Task, now time.Time) error {
	if t.Status.ParkReason == "" {
		return &NotParkedError{State: t.Status.State}
	}
	if class, ok := UnparkClassFor(t.Status.ParkReason); !ok || class != UnparkRetry {
		return fmt.Errorf("stage: park reason %q is not in the retry lane", t.Status.ParkReason)
	}
	next := metav1.NewTime(now.Add(RetryWait(t.Status.RetryAttempts - 1)))
	t.Status.RetryNextAt = &next
	return nil
}

// ResetRetryBudget launders the retry lane's three status fields. It is the ONE
// way anything outside this package refunds the budget, so "who may give a Task
// a fresh set of laps" stays enumerable: stampEnter (a genuine transition), the
// UnparkHuman release (a human answered), and the merge corridor advancing
// status.mergeCursor - the one blocker-ending event that makes NO transition at
// all, because a repo that merged and moved the cursor leaves the Task in
// `merging` throughout.
func ResetRetryBudget(t *v1alpha1.Task) {
	t.Status.RetryAttempts = 0
	t.Status.RetryBlocker = ""
	t.Status.RetryNextAt = nil
}

// RetryDue reports whether an ARMED retry park has served its wait. An unarmed
// park (nil RetryNextAt) is never due: the driver arms it instead, so a park
// written by a path that forgot the schedule costs one lap of latency and not a
// budget-free release on every pass.
func RetryDue(t *v1alpha1.Task, now time.Time) bool {
	return t.Status.RetryNextAt != nil && !now.Before(t.Status.RetryNextAt.Time)
}

// ExhaustRetry ends the lane: it re-parks an UnparkRetry park as
// retry-exhausted (UnparkHuman) WITHOUT ever leaving the Task un-parked, and
// returns the blocker reason it replaced so the caller can name it on the
// forge. That return value is the point of doing this here rather than at the
// call site - repark clears the reason, and an escalation that cannot say what
// it escalated is the silent park this whole lane exists to remove.
//
// RetryAttempts is left AT THE CAP. It is the record the comment is written
// from, and the human release path is what refunds it.
func ExhaustRetry(t *v1alpha1.Task, now time.Time) (from string, err error) {
	if t.Status.ParkReason == "" {
		return "", &NotParkedError{State: t.Status.State}
	}
	if class, ok := UnparkClassFor(t.Status.ParkReason); !ok || class != UnparkRetry {
		return "", fmt.Errorf("stage: park reason %q is not in the retry lane", t.Status.ParkReason)
	}
	from = t.Status.ParkReason
	repark(t, ReasonRetryExhausted, now)
	return from, nil
}

// LaneStranded reports whether t is a Task the retry lane RELEASED and that has
// since been re-parked under a reason the lane no longer recognises, where
// NOBODY owns it.
//
// THE SHAPE. The lane releases a park (reArm, which clears the flag and leaves
// the state where it is), the reconciler mints the pod that release was for, the
// agent writes a handoff note and stops, and at AgentStopReArmCap
// reconcilePodStage parks no-outcome and spawns nothing. That park is then
// owned by nobody: no-outcome is UnparkTimer whose own arm declines merged-mr -
// and every Task in the lane has a merged merge request BY CONSTRUCTION, since
// ci-failed and merge-conflict-retry are written only on the anyMerged arms of
// CIRed and MergeConflict - driveStrandedParks refuses any class that is not
// UnparkNever/UnparkRetired, and the retry driver no longer matches the reason.
// The Task ages out silently at ParkRetention, which is the helmfile#27/#32,
// ansible!17/!18, terraform!221 failure this lane exists to remove.
//
// THE FINGERPRINT IS retryBlocker + retryAttempts, and it is exact rather than
// approximate. Both are written only by ArmRetry, cleared only by
// ResetRetryBudget - stampEnter on a genuine transition, the UnparkHuman
// release, the merge cursor advancing - and deliberately PRESERVED by clearPark
// across the release the lane earns. So a non-zero pair on a park written
// afterwards means: this Task spent those laps on that blocker, in the state it
// is still in, and the lane put it back here. no-outcome is written by several
// unrelated paths (reconcileCaps, a pre-implement state that never terminated)
// and none of them carries the pair, which is what keeps this from escalating
// parks that were never the lane's.
//
// IT DOES NOT ASK WHETHER THE PARK IS ACTUALLY UNRELEASABLE, because that needs
// the owned merge requests and this package must not grow a second copy of
// Unpark's own anyMerged rule: two copies of a filter that disagree do not
// error, they silently stop matching. The caller (controller.driveUnparks) keys
// the escalation on the decline stage.Unpark ITSELF just returned.
func LaneStranded(t *v1alpha1.Task) bool {
	if t.Status.ParkReason != ReasonNoOutcome || t.Status.RetryAttempts <= 0 {
		return false
	}
	class, ok := UnparkClassFor(t.Status.RetryBlocker)
	return ok && class == UnparkRetry
}

// StrandRetryLane is ExhaustRetry's twin for the lane's OTHER end: not a budget
// spent against a standing blocker, but a lane the agent-stop re-arm cap took
// over. It re-parks as retry-exhausted (UnparkHuman, so a comment resumes it)
// WITHOUT ever leaving the Task un-parked, and returns the BLOCKER - not the
// no-outcome park it replaces - because that is what the escalation has to name:
// "no-outcome" tells a human nothing, "the pipeline that was still red" does.
//
// RetryAttempts is left where it is, exactly as ExhaustRetry leaves it at the
// cap: it is the record the comment is written from, and the UnparkHuman release
// is what refunds it.
func StrandRetryLane(t *v1alpha1.Task, now time.Time) (blocker string, err error) {
	if !LaneStranded(t) {
		return "", fmt.Errorf("stage: park %q with %d laps on blocker %q is not a stranded retry lane",
			t.Status.ParkReason, t.Status.RetryAttempts, t.Status.RetryBlocker)
	}
	blocker = t.Status.RetryBlocker
	repark(t, ReasonRetryExhausted, now)
	return blocker, nil
}

// Repark is repark for the ONE caller outside this package that needs it:
// controller.driveRetryLaneMigration, which moves a ci-red / merge-conflict park
// written before the retry lane existed onto its lane name. It is exported
// rather than folded into a bespoke Unpark arm because a MIGRATION is not a
// re-entry rule - there is no condition to evaluate, the Task never becomes
// un-parked, and putting it in Unpark would mean every ApplyUnpark caller
// silently re-classifying parks.
//
// It refuses a Task that is not parked and a reason that is not a park reason;
// the unexported form takes compile-time constants and needs neither.
func Repark(t *v1alpha1.Task, reason string, now time.Time) error {
	if t.Status.ParkReason == "" {
		return &NotParkedError{State: t.Status.State}
	}
	if !IsParkReason(reason) {
		return &UnknownReasonError{Reason: reason}
	}
	repark(t, reason, now)
	return nil
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
//
//   - implement-declined only, because THE UPGRADE RECONCILES TWO NAMES FOR ONE
//     EVENT. On a takeover, implement-declined is the AGENT's local-git name for
//     the very push the flip is here to record: the skill declines on an
//     ls-remote mismatch or a non-fast-forward rejection, both local calls, so
//     the agent's verdict routinely lands before the operator's webhook ->
//     mirror -> here chain. Two observers, one event, two names; renaming is all
//     this does. No other park reason has that property: each is written about
//     something other than the push this flip is reacting to.
//
//     DO NOT justify it with "a human push must not launder a verdict", which
//     stood here as "every other UnparkNever park is a genuine terminal (an
//     exhaustion cap, an operator error, a contract mismatch) reached without
//     any judgement about the merge request at all". That is FALSE, and ci-red
//     alone retires it: enterCIRed writes it off the merge request's own
//     pipeline verdict. Do not answer that with a corrected LIST of which
//     reasons are merge-request-derived - #604's review tried twice and got two
//     different wrong lists; one counterexample is what a universal claim costs,
//     and a list is a second thing to keep true.
//
//     ci-blocked AND head-moving ARE OPEN QUESTIONS, not settled by the
//     principle above. A human push can turn red CI green or land the head the
//     corridor was waiting for, and afterwards the merge request is still
//     mergeable on an approved review - a real case for upgrading them too. What
//     defends excluding them is only that the operator classified no OWNERSHIP
//     change when it wrote either park, so upgrading would assert a hand-back it
//     never measured. A conservative default, not a derived result; the price is
//     ParkRetention. An earlier draft claimed head-moving was structurally
//     distinguishable because the flip always preempts it - FALSE, and not to be
//     reinstated: DrainStandDownMerge runs a LIVE takeover Task against an
//     external-owned merge request, and the flip branch gates on
//     Ownership == tatara, so it cannot fire there at all.
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
// blow ResidencyExceeded on its first pass back. It cannot: every way out of
// parked(ownership-lost) THAT LEAVES THE TASK ALIVE writes a state entry in the
// same pass, and stampEnter zeroes StageElapsedCarrySeconds and nils
// StateWorkStartedAt, on which ResidencyExceeded returns false outright. The two
// that resume the work - MintOrUnparkTakeoverTask and DrainStandDownMerge - go
// through UnparkTakeover; UnparkForMRTerminal is a THIRD exit and does NOT (it
// guards on the target state and the state reason, never on the park reason),
// but it clears the park into a terminal via Enter, so stampEnter runs there
// too. The qualifier is load-bearing, and NOT because the exits can be counted:
// there are also ways out that write no state entry at all, because they DELETE
// the Task rather than resume it (the reaper on ParkRetention, driveStrandedParks
// and resumeNoReentryParks on a Task that owns Issues). A carry those leave
// behind is moot, not carried. reArm is the mutation that PRESERVES the carry;
// what matters here is only that no reArm caller admits ownership-lost - Unpark
// and UnparkRetiredPark refuse it by class, reenterParkedOnReview by an explicit
// switch over the reasons it accepts.
//
// NO DECISION reads the carry while the Task is parked (task_stage.go checks
// residency only on an UN-parked Task), but a READER does, so do not restate
// that as an absolute either: project_controller.go's updateTaskStageGauges
// calls StateElapsedSeconds for every Task with StateEnteredAt set, with no
// Parked filter, and Park leaves StateEnteredAt set. A re-fold is nonetheless
// INVISIBLE there, and the arithmetic is the point: Park folds
// now.Sub(StateEnteredAt) into the carry and re-stamps StateEnteredAt to the
// same instant, so StateElapsedSeconds - now.Sub(StateEnteredAt) + carry -
// moves the identical quantity from the first term to the second and reads the
// same before and after (that continuity is what operator_task_state_age_seconds
// is carry-adjusted FOR). The only thing a second fold would move is the raw
// Status.StageElapsedCarrySeconds field - and "nothing exposes or decides on
// that" is the third overstatement this paragraph has carried, so do not write
// it either. It is a CRD status field (task_types.go), and ArmedClock DECIDES on
// it: `reentered && carry > 0` at merged/deployed is the timeout-re-entry
// discriminator, and ArmedClock's own comment calls that read exact - the
// merge-timeout round trip, where Unpark's reArm deliberately PRESERVES the
// carry, is that decision working as designed. The scope that survives is only
// WHILE THE TASK IS PARKED: ArmedClock takes the park branch whenever ParkReason
// is set and so never reaches the discriminator. That is enough here, because
// this function neither un-parks nor transitions. So the gauge is neither a
// reason to repark nor a reason not to; the reason is the one given above, that
// a Task which parked once must not be booked two park events.
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
//     Task and RESTARTING one. OwnMRsShippedEdge targets `merged`, which still
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
// UnparkCIRecovered releases a Task whose give-up was a verdict on the
// INFRASTRUCTURE rather than on the change: the agent could not hand the work on
// because the operator's own submission gate answered 409 ci-red, the blocker
// has since cleared, and the merge request tatara owns end to end is green at the
// very head the agent gave up at.
//
// TWO PARK REASONS, because there are two ways to give up on a red head:
// implement-declined from the decline verdict, and awaiting-human from the
// discuss verdict, which is the BETTER answer and was the one this primitive
// used to refuse. Neither reason authorises anything on its own - both have far
// more common causes that have nothing to do with CI - and the caller,
// controller.driveCIRecoveryUnparks, is what supplies the discriminator: the
// decline-time evidence annotations, which only those two commits ever write.
// This function stays a guard, not the decision.
//
// IT IS A DRIVER'S PRIMITIVE, NOT A RE-ENTRY RULE, and the distinction is the
// whole safety argument. implement-declined stays UnparkNever: stage.Unpark has
// no arm for it and must not grow one, because the reaper's unparkFires probe
// calls Unpark to decide whether a park is somebody's to re-enter, and an arm
// here would hold EVERY declined Task alive past ParkRetention - including the
// overwhelming majority whose decline was a genuine verdict on the change.
// Reclassifying it as UnparkTimer is refused for the reason unparkClasses gives:
// a timer class requires a bounding counter and there is no CRD field to hold
// one. The bound this recovery does have lives on ANNOTATIONS, read by
// controller.driveCIRecoveryUnparks, exactly as driveRetiredUnparks' latch does.
//
// TAKEOVER IS EXCLUDED, and not defensively. On kind=takeover, implement-declined
// is the agent's LOCAL-GIT name for a human push - the takeover skill declines on
// an ls-remote mismatch or a non-fast-forward rejection, both local calls that
// land before the operator's ownership flip - and UpgradeDeclineToOwnershipLost
// already owns that reason there. Re-driving it would put an agent back on a
// branch whose author has taken it back, in exactly the window before the flip
// is recorded.
//
// reArm, not stampEnter: no state change, no reason change, and
// StageElapsedCarrySeconds PRESERVED, so the residency dead-man switch stays
// cumulative across the recovery. The Task lands back in the state it parked in -
// under-implementation - where the reconciler mints a fresh implement pod, the
// agent finds its own pushed work and a green pipeline, and the submission that
// was refused goes through.
func UnparkCIRecovered(t *v1alpha1.Task, now time.Time) error {
	if t.Status.ParkReason != ReasonImplementDeclined && t.Status.ParkReason != ReasonAwaitingHuman {
		return fmt.Errorf("ci-recovery un-park requires parkReason=%s or %s, got %q",
			ReasonImplementDeclined, ReasonAwaitingHuman, t.Status.ParkReason)
	}
	if t.Spec.Kind == kindTakeover {
		return fmt.Errorf("ci-recovery un-park is never for a takeover: implement-declined there names a human push")
	}
	if now.IsZero() {
		now = time.Now()
	}
	reArm(t, now)
	return nil
}

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
