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
// no-outcome (parkedFromState gate plus maxTurnsPerTask).
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

	// Nobody. These are exhaustion terminals and ex-`failed` reasons:
	// re-entering one would escape its own cap one lap at a time.
	ReasonStageDeadline:          UnparkNever,
	ReasonNameTooLong:            UnparkNever,
	ReasonImplementDeclined:      UnparkNever,
	ReasonReviewLoopExhausted:    UnparkNever,
	ReasonReviewPostRefused:      UnparkNever,
	ReasonMergeBlocked:           UnparkNever,
	ReasonDeployBlocked:          UnparkNever,
	ReasonTurnBudgetExhausted:    UnparkNever,
	ReasonPodRecreationExhausted: UnparkNever,
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
