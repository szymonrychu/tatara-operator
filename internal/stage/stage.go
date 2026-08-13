// Package stage is the Task state machine (contract F). It is a PURE package:
// tables, predicates, Park and Unpark. It never talks to the API server, the
// forge, or a Kubelet, and it must never import internal/controller.
//
// The tables are DATA, not switch statements with a default. The point of a
// table is that a new state cannot be added without appearing in it: a switch
// with a default silently accepts anything.
//
// #521 replaced the 16-stage machine with THREE ORTHOGONAL properties:
//
//	status.state       WHERE THE WORK IS   - 8 closed values, this file's table
//	status.parkReason  WHETHER IT IS STALLED - a flag, park.go
//	Live(state)        WHETHER A POD IS UP  - a property, liveness.go
//
// The old machine folded all three into one value, which is how `parked` came
// to be simultaneously a stage, a terminal and a pod-less marker, and how
// TaskDone(parked) ended up true - issue #521 itself.
package stage

import (
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// Create is the pseudo-state a Task is minted FROM. Reap, Respawn and
// ParkTarget are pseudo-TARGETS: none is a state, and none is ever written to
// status.state. Reap means "the reaper deletes this Task"; Respawn means "the
// pod is recreated" (clock 2 is a respawn trigger, not a terminal); ParkTarget
// means "stamp the park flag WHERE THE TASK IS" and is what replaced every
// `-> parked` and `-> failed` edge in the old table.
const (
	Create     = "(create)"
	Reap       = "(reap)"
	Respawn    = "(respawn)"
	ParkTarget = "(park)"
)

// The seven agent kinds. `clarify` was one of the original seven and is
// DELETED: its three decisions became action values on the implement outcome,
// behind the extended approval gate (#521). `upgrade` was added in its place as
// a distinct kind (2026-08-13): a scheduled dependency-upgrade agent that opens
// merge requests the way implement does, with no approval gate.
const (
	AgentBrainstorm    = "brainstorm"
	AgentIncident      = "incident"
	AgentRefine        = "refine"
	AgentImplement     = "implement"
	AgentReview        = "review"
	AgentDocumentation = "documentation"
	AgentUpgrade       = "upgrade"
)

// kindReview is the Task.Spec.Kind that may NEVER reach under-implementation or
// merged. There is no path, no condition, no exception. It does not exist.
const kindReview = "review"

// The three clocks (contract F.4). Exactly ONE is armed at a time, and WHICH
// one is armed is decided by which timestamps are set - never by the state
// alone.
const (
	ClockNone      = "none"
	ClockAdmission = "admission"
	ClockReadiness = "readiness"
	ClockWork      = "work"
)

// Reasons (contract F.5, the CLOSED set).
const (
	ReasonBacklogSweep           = "backlog-sweep"
	ReasonTriageStalled          = "triage-stalled"
	ReasonNameTooLong            = "name-too-long"
	ReasonStageDeadline          = "stage-deadline"
	ReasonAwaitingHuman          = "awaiting-human"
	ReasonIdentityUnverified     = "identity-unverified"
	ReasonImplementDeclined      = "implement-declined"
	ReasonDeclined               = "declined"
	ReasonFalsePositive          = "false-positive"
	ReasonTrackedElsewhere       = "tracked-elsewhere"
	ReasonReviewLoopExhausted    = "review-loop-exhausted"
	ReasonReviewPostRefused      = "review-post-refused"
	ReasonMergeTimeout           = "merge-timeout"
	ReasonMergeBlocked           = "merge-blocked"
	ReasonMergeOrderMissing      = "merge-order-missing"
	ReasonDeployTimeout          = "deploy-timeout"
	ReasonDeployBlocked          = "deploy-blocked"
	ReasonNoOutcome              = "no-outcome"
	ReasonTurnBudgetExhausted    = "turn-budget-exhausted"
	ReasonPodRecreationExhausted = "pod-recreation-exhausted"
	ReasonObjectTooLarge         = "object-too-large"
	ReasonFoldAdoptionUnverified = "fold-adoption-unverified"
	ReasonAdmissionStarved       = "admission-starved"
	ReasonAgentContractMismatch  = "agent-contract-mismatch"
	ReasonDocTimeout             = "doc-timeout"
	ReasonOperatorError          = "operator-error"
	ReasonHeadMoving             = "head-moving"
	ReasonHandoffStalled         = "handoff-stalled"
	ReasonIssueClosed            = "issue-closed"
	ReasonOwnershipLost          = "ownership-lost"
	ReasonMRMergedExternally     = "mr-merged-externally"
	ReasonMRClosedExternally     = "mr-closed-externally"
	ReasonMergeAuthRefused       = "merge-auth-refused"
	ReasonMRTakenOver            = "mr-taken-over"
	ReasonCIRed                  = "ci-red"
	ReasonCIBlocked              = "ci-blocked"
)

// ParkReasons is the 28-member vocabulary of status.parkReason. It is the CRD
// enum on that field, verbatim.
var ParkReasons = []string{
	ReasonBacklogSweep,
	ReasonTriageStalled,
	ReasonNameTooLong,
	ReasonStageDeadline,
	ReasonAwaitingHuman,
	ReasonIdentityUnverified,
	ReasonImplementDeclined,
	ReasonReviewLoopExhausted,
	ReasonReviewPostRefused,
	ReasonMergeTimeout,
	ReasonMergeBlocked,
	ReasonMergeOrderMissing,
	ReasonDeployTimeout,
	ReasonDeployBlocked,
	ReasonNoOutcome,
	ReasonTurnBudgetExhausted,
	ReasonPodRecreationExhausted,
	ReasonObjectTooLarge,
	ReasonFoldAdoptionUnverified,
	ReasonAdmissionStarved,
	ReasonAgentContractMismatch,
	ReasonOperatorError,
	ReasonHeadMoving,
	ReasonHandoffStalled,
	ReasonOwnershipLost,
	ReasonMergeAuthRefused,
	ReasonCIRed,
	ReasonCIBlocked,
}

// RejectReasons is the 6-member vocabulary of status.stateReason on `rejected`.
var RejectReasons = []string{
	ReasonDeclined,
	ReasonFalsePositive,
	ReasonTrackedElsewhere,
	ReasonIssueClosed,
	ReasonMRClosedExternally,
	ReasonMRTakenOver,
}

// DoneReasons is the 2-member vocabulary of status.stateReason on `done`. Most
// deliveries carry NO reason at all - these two name the ways a Task finishes
// without the ordinary merge/deploy path.
var DoneReasons = []string{
	ReasonDocTimeout,
	ReasonMRMergedExternally,
}

// Reasons is the F.5 closed set: ParkReasons + RejectReasons + DoneReasons,
// partitioned with no overlap and no remainder. A reason not in it is REJECTED
// by Enter.
//
// pod-not-ready IS NOT A MEMBER (fix V7-7): it was never a terminal state, it
// was a respawn trigger wearing a terminal's name.
var Reasons = func() []string {
	out := make([]string, 0, len(ParkReasons)+len(RejectReasons)+len(DoneReasons))
	out = append(out, ParkReasons...)
	out = append(out, RejectReasons...)
	out = append(out, DoneReasons...)
	return out
}()

func reasonIndex(rs []string) map[string]bool {
	m := make(map[string]bool, len(rs))
	for _, r := range rs {
		m[r] = true
	}
	return m
}

var (
	parkReasonSet   = reasonIndex(ParkReasons)
	rejectReasonSet = reasonIndex(RejectReasons)
	doneReasonSet   = reasonIndex(DoneReasons)
	reasonSet       = reasonIndex(Reasons)
)

// ValidReason reports whether r is a member of the F.5 closed set.
func ValidReason(r string) bool { return reasonSet[r] }

// issueClosedStopStates is the closed set of states from which a human-closed
// driving issue stops the Task at rejected(issue-closed) (WS3-I3).
//
// `deployed` is EXCLUDED on purpose: a merged, deploying change is finished
// work and a late issue close must not rewind it (the same boundary the review
// path enforces). The two terminals are excluded because they are terminals.
var issueClosedStopStates = map[string]bool{
	v1alpha1.StateNew:                 true,
	v1alpha1.StateRefined:             true,
	v1alpha1.StateUnderImplementation: true,
	v1alpha1.StateAwaitingReview:      true,
	v1alpha1.StateMerged:              true,
}

// AllowsIssueClosedStop reports whether a Task in `state` may be stopped at
// rejected(issue-closed) by a human closing its driving issue (WS3-I3).
func AllowsIssueClosedStop(state string) bool { return issueClosedStopStates[state] }

// Edge is one row of the transition table. To is a state, or one of the
// pseudo-targets Reap / Respawn / ParkTarget. Reason is the reason stamped on
// To (empty when To carries none). Trigger is the contract's own prose.
type Edge struct {
	To      string
	Reason  string
	Trigger string
}

// Transitions is the transition table as data, keyed by the FROM state (plus
// the Create pseudo-state). No agent writes status.state; only the operator
// does, and a transition not in this table is REJECTED (Enter returns
// *IllegalTransitionError, off which the reconciler labels
// operator_illegal_state_transition_total{from,to}).
//
// TWENTY-SIX EDGES. Four of the six amendments to the #521 design's 20 are work
// that never opens an MR, or never faces the gate; the last two are the #521
// terminal-reset guard, and they are the only edges in the table that exist
// because of a MIGRATION rather than because of the lifecycle:
//
//   - `refined -> done`: a brainstorm Task's outcome is propose/skip, a refine
//     Task's is folds/closes/links, and an incident Task's file_issue mints a
//     tracker and stops. None of the three traverses awaiting-review or
//     deployed, so without this edge they have NO path to done at all.
//   - `under-implementation -> done`: the nightly documentation BATCH finishes
//     at done(doc-timeout) when it declines or its 2h budget elapses, having
//     opened no MR. It runs at under-implementation (it writes code), so
//     `refined -> done` does not reach it.
//   - `(create) -> under-implementation`: the SAME batch is minted straight into
//     implementation work. It has no driving issue to triage and no approval to
//     gate - a nightly batch is the operator's own decision, already made - so
//     routing it through `new` or `refined` would put it in front of a gate with
//     nothing to approve.
//   - `new -> awaiting-review`: a kind=review Task reviews a HUMAN's PR. It owns
//     ZERO Issues by construction, so the gate at `refined` can never grant for
//     it and routing it there would strand it. Without this edge a review Task
//     minted ACTIVE (rather than pre-parked) can never complete triage at all.
//   - `(create) -> done` and `(create) -> rejected`: THE #521 TERMINAL-RESET
//     GUARD. Removing status.stage from the CRD prunes it on the READ path, so
//     every pre-#521 Task is served STATELESS and takes the create edge again.
//     A Task that had already finished must land on its terminal rather than be
//     re-triaged, and stage.Enter is the only writer of status.state - so the
//     create edge has to reach both terminals or the guard has no legal stamp.
//     The DECISION is controller.terminalResetTarget's, which fires only on
//     evidence that survived pruning and defaults to leaving the Task stateless.
//

// EVERY OTHER old edge that targeted `parked` or `failed` is GONE from this
// table: parking is orthogonal to state now, so it is stage.Park, not an edge.
var Transitions = map[string][]Edge{
	Create: {
		{To: v1alpha1.StateNew, Trigger: "Task minted for triage: webhook-originated, a sweep-discovered backlog issue (minted parked(backlog-sweep) alongside), or a human has the last word on the thread (B.4)"},
		{To: v1alpha1.StateRefined, Trigger: "a maintainer-gated takeover mints a Task already bound to an existing MR: the MR exists, so there is nothing to triage, but the work still faces the gate"},
		{To: v1alpha1.StateUnderImplementation, Trigger: "the NIGHTLY DOCUMENTATION BATCH is minted straight into implementation work. No triage (it has no driving issue) and no gate (a nightly batch is the operator's own decision, already made)"},
		{To: v1alpha1.StateDone, Trigger: "THE #521 TERMINAL-RESET GUARD: a Task served stateless by the narrowed CRD carries proof it already DELIVERED (status.deliveredAt, status.documentedBy, or every owned Issue mirror stamped done). It is stamped where it finished instead of being re-triaged"},
		{To: v1alpha1.StateRejected, Trigger: "THE #521 TERMINAL-RESET GUARD: a Task served stateless by the narrowed CRD carries proof it already STOPPED (an owned Issue mirror declined, or every owned mirror closed on the forge with no platform verdict). It is stamped where it finished instead of being re-triaged"},
	},

	v1alpha1.StateNew: {
		{To: v1alpha1.StateRefined, Trigger: "triage passed: spec validates and the Task is routed to its origin kind's agent"},
		{To: v1alpha1.StateAwaitingReview, Trigger: "triage passed on a kind=review Task. It reviews a HUMAN's PR, so there is no plan to write and no approval to grant - the gate at `refined` has nothing to do for it, and GUARD 1 already refuses it every state past this one"},
		{To: v1alpha1.StateRejected, Trigger: "false_positive, tracked_elsewhere, or a human closed the driving issue mid-triage"},
	},

	v1alpha1.StateRefined: {
		{To: v1alpha1.StateUnderImplementation, Trigger: "submit_outcome(action=approved) AND the extended approval gate GRANTS: the citation verifies for every LIVE owned Issue, the declared approvingMaintainer agrees with it, and the plan note is pinned"},
		{To: v1alpha1.StateDone, Trigger: "a non-code kind finished: brainstorm propose/skip, refine folds/closes/links applied and VERIFIED, incident file_issue minted its tracker. None of the three ever opens an MR"},
		{To: v1alpha1.StateRejected, Trigger: "submit_outcome(action=rejected) closes the issue, false_positive, or a human closed the driving issue"},
	},

	v1alpha1.StateUnderImplementation: {
		{To: v1alpha1.StateAwaitingReview, Trigger: "submit_outcome(action=submitted) and >= 1 owned MR is open"},
		{To: v1alpha1.StateRefined, Trigger: "the plan pinned at grant no longer matches the plan note (plan-hash-mismatch): the CHEAP path out, back to the gate, never a park"},
		{To: v1alpha1.StateDone, Trigger: "the nightly documentation batch declined or its budget elapsed: done(doc-timeout), no MR opened"},
		{To: v1alpha1.StateRejected, Trigger: "a human closed the driving issue mid-flight"},
	},

	v1alpha1.StateAwaitingReview: {
		{To: v1alpha1.StateUnderImplementation, Trigger: "submit_outcome(request_changes) AND spec.kind != review (O3 deleted the reviewRounds < maxReviewRounds condition), or an approve whose LIVE CI at the reviewed head has FAILED (issue #476). Gated on pendingReview == nil"},
		{To: v1alpha1.StateMerged, Trigger: "submit_outcome(approve) AND spec.kind != review. Gated on pendingReview == nil, and on the LIVE CI at the reviewed head not being red (issue #476)"},
		{To: v1alpha1.StateDone, Reason: ReasonMRMergedExternally, Trigger: "kind=review Task, every owned MR merged externally before/while reviewing - no open MR to post an outcome against, so the operator finalizes the honest finished work"},
		{To: v1alpha1.StateRejected, Trigger: "mr-closed-externally (the review target was abandoned), mr-taken-over (a maintainer took the MR over and this parent owns zero MRs), or a human closed the driving issue"},
	},

	v1alpha1.StateMerged: {
		{To: v1alpha1.StateDeployed, Trigger: "every repo in mergeOrder merged, in order, each on green CI"},
		{To: v1alpha1.StateAwaitingReview, Trigger: "a live head != reviewedSHA, or Merge 409s head-moved. INCREMENTS status.headMoveReentries (the FOURTH cycle, fix M3-9)"},
		{To: v1alpha1.StateUnderImplementation, Trigger: "a maintainer requested changes on the still-open MR before it merged, or the LIVE CI at the reviewed head has FAILED (issue #476). kind=review refused by LegalFor"},
		{To: v1alpha1.StateRejected, Trigger: "a human closed the driving issue before the merge completed"},
	},

	v1alpha1.StateDeployed: {
		{To: v1alpha1.StateDone, Trigger: "every owned MR merged AND deployedAt != nil. The OPERATOR closes every owned Issue and stamps deliveredAt (C.4). deployed carries NO issue-closed edge: merged work is never rewound"},
	},

	// done and rejected are TERMINAL. They have no state exits: they age out and
	// the reaper collects them.
	v1alpha1.StateDone:     {{To: Reap, Trigger: "DeliveredRetention elapses and the Task is documented or provably needs no coverage"}},
	v1alpha1.StateRejected: {{To: Reap, Trigger: "RejectedRetention elapses"}},
}

// legalPairs is Transitions collapsed to a from/to set, so Legal is O(1).
var legalPairs = func() map[[2]string]bool {
	m := map[[2]string]bool{}
	for from, edges := range Transitions {
		for _, e := range edges {
			m[[2]string{from, e.To}] = true
		}
	}
	return m
}()

// Legal reports whether the from -> to edge exists in the table. It has no Task
// in scope, so it CANNOT enforce the kind guard: use LegalFor (or Enter, which
// uses it) wherever a Task is available.
func Legal(from, to string) bool { return legalPairs[[2]string{from, to}] }

// LegalFor is Legal plus the guards that need the Task and its owned MRs.
//
// GUARD 1 (fixes V7-1, V6-3, C3-2; widened 2026-07-28 security review IMPORTANT
// 3). A kind=review Task may NEVER enter under-implementation or merged. Not
// from awaiting-review on request_changes (the review agent's NORMAL verdict on
// a bad human PR - the PRIMARY path v6 missed), not on approve, not from a
// takeover un-park, not from anywhere. There is no author check to get wrong
// because the sweep ignores bot-authored non-adoptable PRs, so EVERY review Task
// is non-bot-authored by construction. Merging or fixing a human's PR is a HUMAN
// action.
//
// The old machine also listed `approved` here - the pod-less admission gate,
// included because it was the door to implementing. That state is gone: the
// merged model admits straight into under-implementation, so the two names in
// this guard are the whole door.
//
// GUARD 2 (contract C.5.3). awaiting-review -> under-implementation and
// awaiting-review -> merged BOTH require that every owned MergeRequest has
// status.pendingReview == nil. A non-nil pendingReview means "a review is owed
// to the forge and the mirror has not recorded it yet"; a pod spawned then
// renders a bundle with no findings in it, re-submits, and spins another empty
// review round. An EMPTY owned-MR set does not open the gate either.
//
// GUARD 3. awaiting-review -> done is the kind=review external-merge finalize
// and nothing else may take it.
//
// GUARD 4. under-implementation -> done is the nightly DOCUMENTATION batch's
// terminal and nothing else may take it. Any other kind arriving at done from
// under-implementation has opened no MR and been reviewed by nobody.
//
// GUARD 5. new -> awaiting-review is the kind=review triage target and nothing
// else may take it. Every other kind is triaged to `refined`, which is where the
// approval gate runs; letting another kind take the review lane out of triage is
// a door around the gate.
//
// GUARD 6. refined -> done is the three non-code kinds' terminal - brainstorm
// propose/skip, refine folds/closes/links applied and VERIFIED, incident
// file_issue minted its tracker - and nothing else may take it. An implement,
// takeover, review or documentation Task reaching done from refined has opened
// no MR and faced no review either, the same hole GUARDS 3-5 close for their
// own edges.
//
// GUARDS 4, 5 AND 6 WERE CALLER-GATED UNTIL #521's REVIEW (GUARD 6 slipped past
// that same review and was only caught in the round after). The table's own
// Trigger prose named the kind in every case and only triageTarget and the
// outcome handler happened to honour it. A guard that lives in the caller is
// not a guard: the point of LegalFor is that it travels with the edge, so a new
// call site cannot reintroduce the hole by not knowing about it.
func LegalFor(t *v1alpha1.Task, mrs []v1alpha1.MergeRequest, from, to string) bool {
	if !Legal(from, to) {
		return false
	}
	if t != nil && t.Spec.Kind == kindReview &&
		(to == v1alpha1.StateUnderImplementation || to == v1alpha1.StateMerged) {
		return false
	}
	if from == v1alpha1.StateAwaitingReview &&
		(to == v1alpha1.StateUnderImplementation || to == v1alpha1.StateMerged) &&
		!reviewGateOpen(mrs) {
		return false
	}
	if from == v1alpha1.StateAwaitingReview && to == v1alpha1.StateDone {
		if t == nil || t.Spec.Kind != kindReview || !AllMRsMerged(mrs) {
			return false
		}
	}
	if from == v1alpha1.StateUnderImplementation && to == v1alpha1.StateDone {
		if t == nil || t.Spec.Kind != AgentDocumentation {
			return false
		}
	}
	if from == v1alpha1.StateNew && to == v1alpha1.StateAwaitingReview {
		if t == nil || t.Spec.Kind != kindReview {
			return false
		}
	}
	if from == v1alpha1.StateRefined && to == v1alpha1.StateDone {
		if t == nil ||
			(t.Spec.Kind != AgentBrainstorm && t.Spec.Kind != AgentRefine && t.Spec.Kind != AgentIncident) {
			return false
		}
	}
	return true
}

func reviewGateOpen(mrs []v1alpha1.MergeRequest) bool {
	if len(mrs) == 0 {
		return false
	}
	for i := range mrs {
		if mrs[i].Status.PendingReview != nil {
			return false
		}
	}
	return true
}

// MRTerminal reports whether an MR has reached a terminal forge state:
// Status.State in {"merged","closed"}. This is the complement of openMRs' open
// set {"", "open"}; a blank or open state is NOT terminal.
func MRTerminal(mr v1alpha1.MergeRequest) bool {
	return mr.Status.State == "merged" || mr.Status.State == "closed"
}

// AllMRsTerminal reports whether EVERY owned MR is terminal. An empty slice is
// NOT terminal: a Task with no MR refs is a different, pre-existing condition
// and out of scope for the external-terminal finalize.
func AllMRsTerminal(mrs []v1alpha1.MergeRequest) bool {
	if len(mrs) == 0 {
		return false
	}
	for i := range mrs {
		if !MRTerminal(mrs[i]) {
			return false
		}
	}
	return true
}

// AllMRsMerged reports whether EVERY owned MR merged. An empty slice is false.
func AllMRsMerged(mrs []v1alpha1.MergeRequest) bool {
	if len(mrs) == 0 {
		return false
	}
	for i := range mrs {
		if mrs[i].Status.State != "merged" {
			return false
		}
	}
	return true
}

// IllegalTransitionError is returned by Enter when the edge is not in the table
// (or a guard refuses it). From/To are the labels the reconciler puts on
// operator_illegal_state_transition_total{from,to}.
type IllegalTransitionError struct {
	From string
	To   string
}

func (e *IllegalTransitionError) Error() string {
	return fmt.Sprintf("illegal state transition %s -> %s", e.From, e.To)
}

// UnknownReasonError is returned for a reason outside the F.5 closed set, and
// by Park for a reason that is in it but is not a PARK reason.
type UnknownReasonError struct{ Reason string }

func (e *UnknownReasonError) Error() string {
	return fmt.Sprintf("reason %q is not in the closed set for this field", e.Reason)
}

// MissingReasonError is returned by Enter when `rejected` is entered with no
// reason. The reason is MANDATORY there.
type MissingReasonError struct{ To string }

func (e *MissingReasonError) Error() string {
	return fmt.Sprintf("state %s requires a state reason", e.To)
}

// Enter is the ONE way a state is entered, so no caller can forget the things
// EVERY transition does:
//
//	status.stateEnteredAt      = now
//	status.stateReason         = reason
//	status.agentKind           = AgentKindFor(to, spec.kind)
//	status.podStartedAt        = nil     <- load-bearing
//	status.stateWorkStartedAt  = nil
//	stats.podRecreations       = 0
//	status.conversationLastEventAt = now, on entry into a LIVE state
//
// Forgetting podStartedAt = nil leaves a Task covered by NO CLOCK while it
// queues on a re-entry edge (clock 1 is armed only when podStartedAt == nil,
// and clock 2 needs a pod that does not exist yet), and puts the G.7 TTL base
// t0 = podStartedAt + agentPodTTLSeconds ALREADY IN THE PAST for the next pod,
// so the operator TTL-stops it before its first turn.
//
// IT REFUSES A PARKED TASK. `parkReason` is a stringly flag and a flag a writer
// can forget to clear is a new silent wedge in the #521 genre, so there is
// exactly one way out of a park and it is Unpark (or UnparkTakeover, the one
// documented exception). A caller that gets *StillParkedError has a bug, not a
// decline.
//
// ENTRY INTO A LIVE STATE ALSO ARMS THE IDLE CLOCK, here rather than at each of
// the several entry call sites. Enter is the ONE choke point every one of them
// already goes through, so this is the one place a CALLER THAT USES Enter's OWN
// OUTPUT cannot forget it. It is NOT a guarantee against a caller that
// hand-copies Enter's RESULT field-by-field instead of using the mutated Task
// (queue_controller.go's admission write does exactly that, for a different
// edge, and must copy this field too).
//
// mrs are the MergeRequests this Task OWNS; they feed the C.5.3 pendingReview
// gate. Pass nil when the Task owns none.
func Enter(t *v1alpha1.Task, mrs []v1alpha1.MergeRequest, to, reason string, now time.Time) error {
	if t.Status.ParkReason != "" {
		return &StillParkedError{State: t.Status.State, ParkReason: t.Status.ParkReason}
	}
	from := t.Status.State
	if from == "" {
		from = Create
	}
	// A PSEUDO-TARGET IS NOT A STATE, and LegalFor cannot say so: Reap is a real
	// To in the table (done and rejected both carry it), so Legal(done, Reap) is
	// TRUE and this would stamp status.state = "(reap)" - a value outside the
	// enum, with no budget row, no agent kind and no exit. Reap belongs to the
	// reaper, Respawn to RecordRespawn, ParkTarget to Park, Create to nobody.
	if pseudoTarget(to) {
		return &IllegalTransitionError{From: from, To: to}
	}
	if !LegalFor(t, mrs, from, to) {
		return &IllegalTransitionError{From: from, To: to}
	}
	if !reasonAllowedFor(to, reason) {
		return &UnknownReasonError{Reason: reason}
	}
	if reason == "" && reasonRequired(to) {
		return &MissingReasonError{To: to}
	}
	stampEnter(t, to, reason, now)
	return nil
}

// pseudoTarget reports whether to is one of the four table sentinels rather than
// a member of the state enum. Each has its own applier and none of them may be
// stamped onto status.state.
func pseudoTarget(to string) bool {
	switch to {
	case Create, Reap, Respawn, ParkTarget:
		return true
	default:
		return false
	}
}

// stampEnter applies the per-transition stamps. It is the shared body of Enter,
// UnparkTakeover and reArm, and it does NO validation: every caller has already
// done its own.
func stampEnter(t *v1alpha1.Task, to, reason string, now time.Time) {
	stamp := metav1.NewTime(now)
	t.Status.State = to
	t.Status.StateReason = reason
	t.Status.AgentKind = AgentKindFor(to, t.Spec.Kind)
	t.Status.StateEnteredAt = &stamp
	t.Status.PodStartedAt = nil
	t.Status.StateWorkStartedAt = nil
	// The last turn's continuation state belongs to the state occupancy the pod
	// clocks above were measuring (#527). Carried across an edge, the new state's
	// G.7 TTL stop would build its synthetic handoff note out of the OLD state's
	// final text - a clarify agent's closing message handed to an implement pod as
	// though it were the last thing that happened. A stale note is worse than an
	// empty one: the next agent cannot tell that it is stale.
	t.Status.LastTurnFinalText = ""
	t.Status.LastTurnPushedRepos = nil
	t.Status.Stats.PodRecreations = 0
	t.Status.StageElapsedCarrySeconds = 0
	if Live(to) {
		t.Status.ConversationLastEventAt = &stamp
	} else {
		t.Status.ConversationLastEventAt = nil
	}
}

// reasonAllowedFor is the PER-TARGET reason vocabulary, and it is what Enter
// validates against wherever the target HAS one.
//
// `done` and `rejected` have one - DoneReasons and RejectReasons, the closed
// sets this file already declares - and they are the two states the CRD makes a
// stateReason MANDATORY on. Validating them against the GLOBAL set instead was
// validating against the union of all three vocabularies, 28 of whose 36 members
// are PARK reasons that belong to `status.parkReason`, a different field with a
// disjoint enum. It accepted awaiting-human on `rejected` and declined on
// `done`: values outside the closed set the field itself documents, stamped by
// the one function that is supposed to be the guard. No call site does it today
// - they all pass compile-time constants - but "no caller does it today" is not
// an invariant, and the per-state sets and their predicates already existed.
// This is Enter using them.
//
// THE OTHER SIX STATES KEEP THE GLOBAL CHECK, because they have no closed set of
// their own to check against. They are not reason-free either: stage.CIRed's
// re-implement edge enters under-implementation with ci-red, so a rule of
// "a non-terminal takes no reason" would be a rule this machine does not follow.
func reasonAllowedFor(to, reason string) bool {
	if reason == "" {
		// The empty reason is the ordinary case everywhere; where it is not
		// allowed, reasonRequired says so.
		return true
	}
	switch to {
	case v1alpha1.StateRejected:
		return IsRejectReason(reason)
	case v1alpha1.StateDone:
		return IsDoneReason(reason)
	default:
		return ValidReason(reason)
	}
}

// reasonRequired: `rejected` MUST name why. `done` deliberately does NOT - it
// absorbed the old `delivered`, whose ordinary success path carries no reason
// at all, and inventing one per delivery would make the vocabulary noise.
func reasonRequired(to string) bool { return to == v1alpha1.StateRejected }

// budgets is the WORK-clock table. EVERY member of the state enum has a row: a
// new state cannot be added without one, and a table-driven test asserts it.
//
// A LIVE state's budget is the IDLE budget, not a work budget - reconcileClocks
// substitutes the project's scm.conversationIdleMinutes over it. It is armed
// only while NO turn is in flight, so it bounds a conversation rather than the
// agent's work. The bound on the WORK is residencyCaps (liveness.go), checked
// separately by reconcileClocks.
//
// A PARKED Task's budget is ParkRetention, keyed on the park flag rather than
// on any row here; see ArmedClock.
var budgets = map[string]time.Duration{
	v1alpha1.StateNew:                 5 * time.Minute,
	v1alpha1.StateRefined:             v1alpha1.ConversationIdleDefault,
	v1alpha1.StateUnderImplementation: v1alpha1.ConversationIdleDefault,
	v1alpha1.StateAwaitingReview:      v1alpha1.ConversationIdleDefault,
	v1alpha1.StateMerged:              4 * time.Hour,
	v1alpha1.StateDeployed:            2 * time.Hour,
	v1alpha1.StateDone:                v1alpha1.DeliveredRetention,
	v1alpha1.StateRejected:            v1alpha1.RejectedRetention,
}

// onElapse is the other column of the same row: where the WORK clock goes when
// the budget is spent. Every live state elapses to park(awaiting-human) - a
// conversation that goes quiet becomes an ordinary park, and an ordinary park is
// re-entered by the ordinary rules.
var onElapse = map[string]Edge{
	v1alpha1.StateNew:                 {To: ParkTarget, Reason: ReasonTriageStalled},
	v1alpha1.StateRefined:             {To: ParkTarget, Reason: ReasonAwaitingHuman},
	v1alpha1.StateUnderImplementation: {To: ParkTarget, Reason: ReasonAwaitingHuman},
	v1alpha1.StateAwaitingReview:      {To: ParkTarget, Reason: ReasonAwaitingHuman},
	v1alpha1.StateMerged:              {To: ParkTarget, Reason: ReasonMergeTimeout},
	v1alpha1.StateDeployed:            {To: ParkTarget, Reason: ReasonDeployTimeout},
	v1alpha1.StateDone:                {To: Reap},
	v1alpha1.StateRejected:            {To: Reap},
}

// Budget is the WORK-clock table. ok is false only for a state that is not in
// the enum.
func Budget(state string) (time.Duration, bool) {
	d, ok := budgets[state]
	return d, ok
}

// OnElapse is where the WORK clock goes when Budget is spent. Edge.To may be
// the Reap or ParkTarget pseudo-target.
func OnElapse(state string) (Edge, bool) {
	e, ok := onElapse[state]
	return e, ok
}

// ArmedClock is THE THREE-CLOCK SELECTOR (F.4). Exactly ONE clock is armed at a
// time, and WHICH one is decided by which timestamps are set - NEVER by the
// state alone:
//
//	podStartedAt == nil                             -> CLOCK 1 ADMISSION, from
//	                                                   stateEnteredAt, 24h ->
//	                                                   park(admission-starved)
//	podStartedAt != nil && stateWorkStartedAt == nil -> CLOCK 2 READINESS, from
//	                                                   podStartedAt, 5m -> RESPAWN
//	stateWorkStartedAt != nil                       -> CLOCK 3 WORK, from
//	                                                   stateWorkStartedAt, the
//	                                                   budget -> park(stage-deadline)
//
// podStartedAt == nil AND stateWorkStartedAt == nil is CLOCK 1. It is a named
// case, not an inference. The READINESS clock NEVER measures from
// stateEnteredAt: that includes the admission queue, and the queue is where a
// Task in normal steady state sits.
//
// OPERATOR-DRIVEN states run CLOCK 3 ONLY, measured from stateEnteredAt,
// against their OWN budget. They do NOT run clock 1 (fix V7-8): merged with a
// 24h admission-starved clock could never reach merge-timeout, and the bounded
// merge re-entry cycle would never engage at all.
//
// ONE operator-driven refinement, and it is a SPLIT of two things issue #480
// conflated (issue #513). On a merge-timeout/deploy-timeout un-park re-entry
// (merged or deployed with StageElapsedCarrySeconds > 0):
//
//	the DEADLINE           -> a FRESH bounded window per re-entry: from
//	                          stateEnteredAt, no carry subtraction, against
//	                          v1alpha1.TimeoutReentryBudget, not the state budget
//	the REPORTED RESIDENCY -> still CUMULATIVE across the whole round trip, via
//	                          StateElapsedSeconds and its metric readers
//
// A carry-adjusted deadline is self-defeating: the park it resumes only happens
// once the budget is spent, so the re-entry is over budget the instant it
// arrives. The residency number is what #480 measured and it is unchanged.
//
// paused is Project.spec.maxConcurrentAgents == 0. It disarms the ADMISSION
// clock. It is the ONLY deadline exception in the contract: without it the
// pause kill switch is a backlog shredder. It does NOT disarm clocks 2 and 3,
// which measure a pod that already exists.
//
// clock is ClockNone when nothing is armed; since/budget/onElapse are then zero.
func ArmedClock(t *v1alpha1.Task, paused bool) (clock string, since time.Time, budget time.Duration, onElapseEdge Edge) {
	st := t.Status.State

	// THE PARK CLOCK, and it OUTRANKS every state clock: park is what takes the
	// pod down, so a parked Task's only remaining deadline is its retention. It
	// replaces the deleted `parked` STAGE's own ParkRetention row.
	if t.Status.ParkReason != "" {
		// THE ONE EXEMPTION (F.4): parked(backlog-sweep) is not stalled work, it
		// is the durable owner of an Issue CR at zero agent cost. It consumes
		// nothing: no pod, no queue slot, no turn. It NEVER ages out - it is
		// reaped when its Issues close.
		if t.Status.ParkReason == ReasonBacklogSweep || t.Status.ParkedAt == nil {
			return ClockNone, time.Time{}, 0, Edge{}
		}
		return ClockWork, t.Status.ParkedAt.Time, v1alpha1.ParkRetention, Edge{To: Reap}
	}

	budget, ok := Budget(st)
	if !ok || t.Status.StateEnteredAt == nil {
		return ClockNone, time.Time{}, 0, Edge{}
	}
	elapse, ok := OnElapse(st)
	if !ok {
		return ClockNone, time.Time{}, 0, Edge{}
	}

	// THE IDLE CLOCK. A named case, not an inference. A LIVE state runs a pod,
	// so the generic selector below would measure clock 3 from
	// stateWorkStartedAt - which describes pod age, not how long the human has
	// been silent. The budget here is the table default; reconcileClocks
	// substitutes the project's scm.conversationIdleMinutes, which is the only
	// per-project knob in the clock model and is why the substitution lives at
	// the caller rather than in this pure package.
	//
	// It takes over only once a pod EXISTS: a live state that has not been
	// admitted yet is still covered by clock 1, and a live state whose pod is
	// booting is still covered by clock 2. An idle clock on an unadmitted Task
	// would be measuring the silence of a conversation that has not started.
	//
	// IT BOUNDS A CONVERSATION, NOT AGENT WORK, and the two are told apart by
	// whether a turn is IN FLIGHT. #521 promoted `conversing`'s idle clock to all
	// three live states, which deleted the WORK budget each of them used to have
	// (implementing 6h, reviewing 4h) - and ConversationLastEventAt is re-stamped
	// by exactly one thing, a HUMAN webhook comment. Armed unconditionally it
	// therefore parked a silently-working implement agent at 60 minutes, at
	// awaiting-human, where it needed a human comment or aged out at
	// ParkRetention. An agent mid-turn is not idle by any reading of the word.
	//
	// With a turn in flight NOTHING is armed here, and the bounds are the two
	// that are actually about work: the per-turn stall timeout (turnTimedOut,
	// anchored on the wrapper's own reported activity) and ResidencyExceeded.
	//
	// The ABSOLUTE bound this clock cannot provide is ResidencyExceeded, checked
	// separately by reconcileClocks BEFORE this function runs. The two COMPOSE
	// rather than overlap: this one fires when the state is QUIET, residency
	// fires when it has been LONG, and at most one of them can be armed for any
	// given Task at any moment.
	if Live(st) && t.Status.PodStartedAt != nil && t.Status.StateWorkStartedAt != nil {
		if turnInFlight(t) {
			return ClockNone, time.Time{}, 0, Edge{}
		}
		base, ok := idleBase(t)
		if !ok {
			return ClockNone, time.Time{}, 0, Edge{}
		}
		return ClockWork, base, budget, elapse
	}

	if OperatorDriven(st) || !Live(st) {
		// CLOCK 3 ONLY, from stateEnteredAt, against its own budget. `new` is
		// operator triage and belongs here too; done/rejected run their
		// retention clock the same way.
		if paused && elapse.Reason == ReasonAdmissionStarved {
			return ClockNone, time.Time{}, 0, Edge{}
		}
		// A merge-timeout/deploy-timeout un-park RE-ENTRY. carry > 0 IN merged or
		// deployed is the discriminator, and it is exact: stampEnter zeroes the
		// carry on every state transition, so a non-zero carry at merged or
		// deployed can only have come from a park in THAT state - and the only
		// park reason at either that un-parks at all is its own timeout
		// (every other reason legal there is UnparkNever). MergeReentries and
		// DeployReentries are NOT usable for this: they are cumulative counters
		// that stay set across an unrelated later cycle.
		//
		// The re-entry gets its OWN bounded window measured from THIS entry, with
		// NO carry subtraction, against TimeoutReentryBudget (issue #513). Issue
		// #480 conflated two things and this splits them: a carry-adjusted
		// `since` puts the re-entry over budget on arrival and it re-parks on the
		// same reconcile pass (live in #513: re-entry lifetimes of
		// 7.2ms/8.8ms/11.3ms with all three laps spent and zero time bought).
		reentered := st == v1alpha1.StateMerged || st == v1alpha1.StateDeployed
		if reentered && t.Status.StageElapsedCarrySeconds > 0 {
			return ClockWork, t.Status.StateEnteredAt.Time, v1alpha1.TimeoutReentryBudget, elapse
		}
		return ClockWork, t.Status.StateEnteredAt.Time, budget, elapse
	}

	// A LIVE state whose pod is not up yet. Which of the two remaining clocks is
	// armed depends ENTIRELY on the stamps.
	if t.Status.PodStartedAt == nil {
		// CLOCK 1 ADMISSION.
		if paused {
			return ClockNone, time.Time{}, 0, Edge{}
		}
		return ClockAdmission, t.Status.StateEnteredAt.Time, v1alpha1.AdmissionStarvedBudget,
			Edge{To: ParkTarget, Reason: ReasonAdmissionStarved}
	}
	// CLOCK 2 READINESS.
	return ClockReadiness, t.Status.PodStartedAt.Time, v1alpha1.PodReadyTimeout,
		Edge{To: Respawn}
}

// Elapsed reports the edge to take when the armed clock has run out, if it has.
// An Edge.To of Respawn is NOT a transition: call RecordRespawn. An Edge.To of
// Reap is NOT a transition either: the reaper deletes the Task. An Edge.To of
// ParkTarget is NOT a transition: call Park.
func Elapsed(t *v1alpha1.Task, paused bool, now time.Time) (Edge, bool) {
	clock, since, budget, edge := ArmedClock(t, paused)
	if clock == ClockNone {
		return Edge{}, false
	}
	if now.Sub(since) <= budget {
		return Edge{}, false
	}
	return edge, true
}

// StateElapsedSeconds is the carry-adjusted (issue #480) elapsed time since
// Status.StateEnteredAt: now.Sub(StateEnteredAt) plus StageElapsedCarrySeconds.
//
// It is deliberately a REPORTING number AND the residency measure, not the
// deadline: ArmedClock's operator-driven WORK clock stopped subtracting the
// carry in issue #513 (a timeout re-entry gets its own TimeoutReentryBudget
// window instead), so this is the form every caller that reports or compares
// true state RESIDENCY must use instead of a bare now.Sub(StateEnteredAt) -
// operator_task_state_age_seconds and operator_merge_cursor_stalled_seconds both
// do, or they under-report true residency by up to a full budget per re-entry,
// exactly as #480 documented live (a gauge reading 2h20m for a Task that had
// been merging 10h21m). ResidencyExceeded reads it for the same reason.
//
// Returns 0 when StateEnteredAt is nil (no clock armed yet).
func StateElapsedSeconds(t *v1alpha1.Task, now time.Time) float64 {
	if t.Status.StateEnteredAt == nil {
		return 0
	}
	return now.Sub(t.Status.StateEnteredAt.Time).Seconds() + float64(t.Status.StageElapsedCarrySeconds)
}

// ReArmAfterPodLoss puts a LIVE Task back in the admission queue for a fresh
// pod, in place, charging one podRecreations. It is the answer to a pod that
// ENDED WITHOUT THE AGENT SAYING ANYTHING - no handoff note, nothing asked and
// nothing reported - which is not a human gate and must not be parked as one.
//
// Parking such a Task awaiting-human is a dead end by construction:
// awaiting-human is UnparkHuman, so it resumes only on a non-bot comment, and
// nobody replies to a question that was never posed. Measured live in the tatara
// namespace: 45 Tasks parked awaiting-human, only 20 of them carrying a real
// agent handoff; the other 25 were wreckage from pods dying in one 71-minute
// window, each waiting on an answer to nothing.
//
// THREE STAMPS, and every one of them is load-bearing:
//
//   - podStartedAt/stateWorkStartedAt to nil re-arms CLOCK 1 so the next
//     reconcile admits a replacement pod.
//   - stateEnteredAt to now, because CLOCK 1 measures FROM IT against
//     AdmissionStarvedBudget. Leaving a stale value re-elapses clock 1 on the
//     very next pass and parks admission-starved, which is UnparkNever - strictly
//     worse than the awaiting-human this replaces. That is #513's "a retry that
//     provides no retry" shape and it must not be recreated here.
//   - podRecreations is NOT reset (unlike the un-park reArm), and RecordRespawn
//     bumps it first. It NO LONGER BOUNDS THIS LOOP - O3 deleted the terminal -
//     but it is still counted, because it is the input to
//     operator_pod_recreations_total and therefore to the churn alert
//     (sum by (project) (increase(operator_pod_recreations_total[1h])) > 6,
//     critical) that replaced the cap. A boot-crash loop is now bounded only by
//     ResidencyCapAll; the alert is the compensating control.
//
// ConversationLastEventAt is deliberately untouched: it is the HUMAN half of the
// idle base, and the idle clock is not armed while podStartedAt is nil anyway -
// it re-bases off the replacement pod's stateWorkStartedAt at pod-ready.
func ReArmAfterPodLoss(t *v1alpha1.Task, now time.Time) Edge {
	edge := RecordRespawn(t)
	stamp := metav1.NewTime(now)
	t.Status.StateEnteredAt = &stamp
	t.Status.PodStartedAt = nil
	t.Status.StateWorkStartedAt = nil
	return edge
}

// RecordRespawn is the respawn accounting shared by every pod-loss path - a
// never-Ready pod (CLOCK 2), a pod that vanished, and a pod that died in place:
// each RESPAWNS, burning one podRecreations. It does NOT terminate the Task, and
// since O3 it never returns a terminal edge at all - there is no
// maxPodRecreations any more. It KEEPS COUNTING: stats.podRecreations is what
// obs.PodRecreation reports, and that series is the churn alert's only input.
// Stopping the count to match the deleted cap would blind the one control that
// replaced it.
func RecordRespawn(t *v1alpha1.Task) Edge {
	t.Status.Stats.PodRecreations++
	return Edge{To: Respawn}
}

// BudgetExit is what is LEFT of the set of exits every live state used to carry
// on top of its clocks (F.4). It returns no edge for a state that runs no pod.
//
// O3 deleted two of the three: `Turns >= maxTurnsPerTask` and
// `PodRecreations > maxPodRecreations`. Both counted work rather than measuring
// a stall, and both parked agents that were making progress - a 400-turn Task is
// a long job, not a wedged one. Stall is now decided by the probe machinery (O2)
// and, failing that, by ResidencyCapAll.
//
// podStoppedNoOutcome SURVIVES, without its recreation gate. It is not a budget:
// it says the pod RAN and is GONE and the Task never left the state, which is a
// fact about this Task right now rather than a count of how much it has done.
// It parks no-outcome, which is UnparkTimer and re-drives.
func BudgetExit(t *v1alpha1.Task, podStoppedNoOutcome bool) (Edge, bool) {
	if !Live(t.Status.State) {
		return Edge{}, false
	}
	if podStoppedNoOutcome {
		return Edge{To: ParkTarget, Reason: ReasonNoOutcome}, true
	}
	return Edge{}, false
}

// RequestChanges is the awaiting-review exit on submit_outcome(request_changes).
// On a kind=review Task it is park(awaiting-human) - ALWAYS, unconditionally.
// That is the review agent's NORMAL verdict on a bad human PR, and it is the
// PRIMARY path v6 left open into an implement pod spawning against someone
// else's PR with no Issue, no ApprovalEvidence and no C.6 gate anywhere in its
// history (fix V7-1). The review IS posted. The human fixes their own PR.
//
// On any other kind it re-enters under-implementation. It USED TO be bounded by
// maxReviewRounds on the MR (cycle 1); O3 deleted that branch. A review
// round-trip count measures how much conversation has happened, not whether it
// is going anywhere, and parking review-loop-exhausted killed the exact
// implement/review pairs that were converging. status.reviewRounds is still
// incremented by the review-post path - it is observability, and the park-spike
// dashboards read it - it just no longer terminates anything. What bounds the
// loop now is ResidencyCapAll on the state the Task keeps re-entering.
func RequestChanges(t *v1alpha1.Task) (Edge, bool) {
	if t.Spec.Kind == kindReview {
		return Edge{To: ParkTarget, Reason: ReasonAwaitingHuman}, true
	}
	return Edge{To: v1alpha1.StateUnderImplementation}, true
}

// ReenterOnReviewChangesRequested routes a maintainer's changes_requested on a
// Tatara-owned, NOT-yet-merged MR back onto the state machine. The caller has
// already verified no owned MR is merged and the actor is a maintainer.
//
// A NON-parked awaiting-review/merged Task re-enters under-implementation: the
// maintainer wants code changes, so this is a fresh implementation cycle and it
// gets a fresh merge/head budget (F3). Every OTHER non-parked state folds:
// done/rejected are never resurrected, an already-under-implementation Task is a
// redundant no-op, and `new`/`refined` have no re-entry edge because that would
// bypass the #294 approval gate.
//
// A PARKED Task is routed by ParkReason, mirroring Unpark EXACTLY:
//   - merge-timeout -> un-park in place at merged with MergeReentries
//     accounting; NEVER under-implementation (that would recreate deleted
//     branches). A real code change from the maintainer moves the head and the
//     merged head-moved bounce re-reviews it.
//   - no-outcome -> un-park in place, behind Unpark's own guards.
//   - every other reason folds (ok=false).
//
// GUARD 1 still refuses under-implementation/merged for a kind=review Task from
// anywhere, so an adopted human PR is never driven.
func ReenterOnReviewChangesRequested(t *v1alpha1.Task, mrs []v1alpha1.MergeRequest, now time.Time) (ok bool) {
	if now.IsZero() {
		now = time.Now()
	}
	if t.Status.ParkReason != "" {
		return reenterParkedOnReview(t, mrs, now)
	}
	switch t.Status.State {
	case v1alpha1.StateAwaitingReview, v1alpha1.StateMerged:
		return enterFreshImplementing(t, mrs, now)
	default:
		return false
	}
}

// enterFreshImplementing applies the awaiting-review|merged ->
// under-implementation edge for a maintainer-driven fresh implementation, and on
// success zeroes the merge, head-move and red-CI budgets (F3): a fresh
// implementation deserves a fresh merge budget, and this reset is human-gated
// (one maintainer changes_requested per reset), so it is NOT the automatic
// HeadMoved bounce and cannot spin a head-move loop on its own.
func enterFreshImplementing(t *v1alpha1.Task, mrs []v1alpha1.MergeRequest, now time.Time) bool {
	if err := Enter(t, mrs, v1alpha1.StateUnderImplementation, "", now); err != nil {
		return false
	}
	t.Status.HeadMoveReentries = 0
	t.Status.MergeReentries = 0
	t.Status.CIRedReentries = 0
	return true
}

// reenterParkedOnReview is the parked branch of ReenterOnReviewChangesRequested,
// routed by ParkReason to mirror Unpark.
func reenterParkedOnReview(t *v1alpha1.Task, mrs []v1alpha1.MergeRequest, now time.Time) bool {
	switch t.Status.ParkReason {
	case ReasonMergeTimeout:
		if t.Status.MergeReentries >= v1alpha1.MaxMergeReentries {
			// Budget spent. Fold rather than replicate Unpark's re-park to
			// merge-blocked HERE: this only keeps the REVIEW path from
			// terminating the Task. The periodic driveUnparks -> Unpark still
			// drives it on its own cadence.
			return false
		}
		t.Status.MergeReentries++
		reArm(t, now)
		return true
	case ReasonNoOutcome:
		if anyMerged(mrs) {
			return false // a re-implement would duplicate an already-merged change
		}
		// The turn gate that used to sit here ("would bounce straight into
		// park(turn-budget-exhausted)") is gone with the park reason it guarded
		// against: BudgetExit no longer exits on Stats.Turns, so there is nothing
		// left to bounce into. See Unpark's ReasonNoOutcome arm.
		reArm(t, now)
		t.Status.HeadMoveReentries = 0
		t.Status.MergeReentries = 0
		t.Status.CIRedReentries = 0
		return true
	default:
		return false
	}
}

// HeadMoved is the merged exit when the live head has moved off reviewedSHA (or
// Merge 409s "head moved"). It is CYCLE 4 (fix M3-9): the fourth cycle, and the
// ONLY one that SPAWNS A POD every lap. merged -> awaiting-review does NOT touch
// MergeReentries (only the PARKED path does), and reviewRounds increments only
// on request_changes, so a PR whose head keeps moving - a human pushing to the
// branch, a flapping CI autocommit - spun forever, burning a review pod on every
// lap, with no counter anywhere.
//
// It INCREMENTS status.headMoveReentries and caps at maxHeadMoveReentries ->
// park(head-moving).
func HeadMoved(t *v1alpha1.Task, maxHeadMoveReentries int) (Edge, bool) {
	if t.Status.HeadMoveReentries >= maxHeadMoveReentries {
		return Edge{To: ParkTarget, Reason: ReasonHeadMoving}, true
	}
	t.Status.HeadMoveReentries++
	return Edge{To: v1alpha1.StateAwaitingReview}, true
}

// CIRed is the RED-CI exit (issue #476), and it is CYCLE 5. It is taken from
// BOTH sides of the promotion it guards:
//
//   - awaiting-review, instead of advancing an approved-but-red change into
//     merged;
//   - merged, instead of re-polling a red required check every 60s until the 4h
//     budget parks it (and then un-parking straight back, up to
//     maxMergeReentries, for ~16h of re-testing one deterministic verdict).
//
// A failed check on the head that was reviewed is DETERMINISTIC: no amount of
// waiting turns it green, only a new commit does. The Task therefore goes back
// to under-implementation, where an agent can fix it.
//
// It INCREMENTS status.ciRedReentries and caps at maxCIRedReentries ->
// park(ci-blocked), for the same reason every other bounce is capped: each lap
// spawns pods.
//
// TWO refusals sit in front of the re-implement:
//
//   - kind=review: fixing a human's PR is a HUMAN action, so it parks
//     awaiting-human exactly like every other kind=review verdict.
//   - anyMerged(mrs): part of spec.mergeOrder has already landed, and
//     re-implementing would re-propose merged code and recreate deleted
//     branches. It parks ci-red, which has no re-entry: a human decides.
func CIRed(t *v1alpha1.Task, mrs []v1alpha1.MergeRequest, maxCIRedReentries int) (Edge, bool) {
	if t.Spec.Kind == kindReview {
		return Edge{To: ParkTarget, Reason: ReasonAwaitingHuman}, true
	}
	if anyMerged(mrs) {
		return Edge{To: ParkTarget, Reason: ReasonCIRed}, true
	}
	if t.Status.CIRedReentries >= maxCIRedReentries {
		return Edge{To: ParkTarget, Reason: ReasonCIBlocked}, true
	}
	t.Status.CIRedReentries++
	return Edge{To: v1alpha1.StateUnderImplementation, Reason: ReasonCIRed}, true
}

// UnparkInput is everything the un-park rules read.
//
// internal/stage is PURE, and as of the agent-judged approval gate it does NO
// approval reasoning at all. The caller performs NO approval evaluation before
// calling Unpark and passes NO verdict in: this package never sees an approval,
// and no field here can carry one.
//
// That is what makes parked(identity-unverified) re-entry a RESPONSIVENESS
// decision only - "should a live agent be put in front of the human who just
// commented, and is there room for one" - rather than an authorization decision.
// The authorization lives in exactly one place, restapi.verifyApprovalScope, and
// it runs against the operator's own mirror on every
// submit_outcome(action=approved).
type UnparkInput struct {
	Task *v1alpha1.Task
	// Issues and MRs are the Issues / MergeRequests this Task OWNS. A review
	// Task owns ZERO Issues.
	Issues []v1alpha1.Issue
	MRs    []v1alpha1.MergeRequest
	// ActiveTasks is the count of ACTIVE (non-done) Tasks in the project;
	// MaxOpenTasks is Project.spec.maxOpenTasks. A promotion is NOT a mint, and
	// v3 checked the cap only at mint, so a maintainer's bulk comment pass would
	// promote 40 Tasks past a cap of 6.
	ActiveTasks  int
	MaxOpenTasks int
	// BotLogin is Project.spec.scm.botLogin. An event authored by it is a BOT
	// event and can never un-park anything: the operator's own park comment must
	// not un-park the Task it parked.
	BotLogin string
	// LiveHasRoom is the CALLER's answer to "is this project under its live-pod
	// ceiling right now". internal/stage is pure and cannot count live Tasks, so
	// the caller computes it once per pass. False is always SAFE: the un-park
	// declines, the pendingEvent is RETAINED and the next pass retries.
	LiveHasRoom bool
	Now         time.Time
}

// The CLOSED decline vocabulary. Unpark returns exactly one of these, and the
// caller puts it on operator_unpark_declined_total's `kind` label, so the set
// must stay small and stable. A decline that cannot name its condition is the
// defect this vocabulary exists to make impossible: the 2026-07-27
// identity-unverified stall declined silently and no log or counter anywhere
// could say which of the several conditions that arm then checked had refused.
const (
	// DeclineNone means Unpark cleared the park flag.
	DeclineNone = ""
	// DeclineNotParked: the Task was not parked. A caller bug, named rather
	// than swallowed.
	DeclineNotParked = "not-parked"
	// DeclineNoHumanEvent: the rule needs a non-bot pendingEvent and there is none.
	DeclineNoHumanEvent = "no-human-event"
	// DeclineOverCap: backlog-sweep promotion would exceed maxOpenTasks. The
	// promotion DEFERS; the event is retained.
	DeclineOverCap = "over-cap"
	// DeclineNoLiveRoom: the un-park would resume a LIVE state but the project
	// is at its live-pod ceiling. The pendingEvent is RETAINED and the next pass
	// retries; nothing is dropped.
	DeclineNoLiveRoom = "no-live-room"
	// DeclineNoOpenIssues: the empty owned-Issue set is not a licence (fix V6-3).
	DeclineNoOpenIssues = "no-open-issues"
	// DeclineMergedMR: re-entry would spawn a pod against an already-merged MR.
	DeclineMergedMR = "merged-mr"
	// DeclineRoundsExhausted: humanReviewRounds is at MaxHumanReviewRounds.
	DeclineRoundsExhausted = "rounds-exhausted"
	// DeclineTurnsExhausted: stats.turns was at maxTurnsPerTask. RETIRED by O3
	// and returned by nothing - the turn ceiling no longer terminates or vetoes
	// anything. The constant STAYS because internal/controller mirrors this
	// vocabulary onto operator_unpark_declined_total's `kind` label, and a label
	// value that has stopped being emitted is a series going to zero, not a
	// compile error to chase.
	DeclineTurnsExhausted = "turns-exhausted"
	// DeclineWrongParkedFrom: a no-outcome park from a pre-implement state must
	// not auto-escalate into implementation (#406).
	DeclineWrongParkedFrom = "wrong-parked-from"
	// DeclineNoReentry: this park reason has no re-entry rule at all
	// (UnparkNever). It ages out at ParkRetention and is reaped.
	DeclineNoReentry = "no-reentry"
	// DeclineRetryBudgetSpent: a TIMER un-park whose counter is exhausted. The
	// Task is RE-PARKED with the matching blocked reason, which is UnparkNever,
	// so it ages out instead of retrying forever.
	DeclineRetryBudgetSpent = "retry-budget-spent"
)

// UnparkDeclines is the closed decline vocabulary as data, for the metric's
// label allow-list.
var UnparkDeclines = []string{
	DeclineNotParked,
	DeclineNoHumanEvent,
	DeclineOverCap,
	DeclineNoLiveRoom,
	DeclineNoOpenIssues,
	DeclineMergedMR,
	DeclineRoundsExhausted,
	DeclineTurnsExhausted,
	DeclineWrongParkedFrom,
	DeclineNoReentry,
	DeclineRetryBudgetSpent,
}

// Unpark is the re-entry function, and this is its entire body. A parked Task
// does not "resume": it either matches ONE of these narrow rules or it ages out
// at ParkRetention and is reaped.
//
// IT NEVER CHANGES State. Un-parking returns a Task to WHERE IT WAS - that is
// the whole point of park being a flag - so there is no target to derive and no
// parkedFromState to derive it from. What it DOES do is clear the flag and
// RE-ARM the clock (stateEnteredAt = now, podStartedAt = nil,
// stateWorkStartedAt = nil, podRecreations = 0, and the idle stamp on a live
// state), because a Task un-parked with a stale stateEnteredAt would re-park on
// the same reconcile pass.
//
// On DeclineNone the flag is cleared and the caller persists status. On any
// other return the Task is UNTOUCHED except where a re-park is explicitly
// documented, it stays parked, and its pendingEvents are RETAINED, never
// dropped.
func Unpark(in UnparkInput) (decline string) {
	t := in.Task
	now := in.Now
	if now.IsZero() {
		now = time.Now()
	}
	if t.Status.ParkReason == "" {
		return DeclineNotParked
	}
	class, ok := UnparkClassFor(t.Status.ParkReason)
	if !ok || class == UnparkNever || class == UnparkRetired {
		// UnparkRetired lands here ON PURPOSE. Its three reasons are migrated
		// EXACTLY ONCE by controller.driveRetiredUnparks, which does not go
		// through this function at all; giving them an arm here instead would
		// make a one-shot migration into a permanent retry rule, and would also
		// hold every un-migrated leftover alive past ParkRetention because the
		// reaper's unparkFires probe calls this. Treated as UnparkNever, they age
		// out exactly as they did before O3.
		//
		// implement-declined, stage-deadline,
		// admission-starved,
		// fold-adoption-unverified, operator-error, triage-stalled,
		// name-too-long, review-post-refused, object-too-large,
		// merge-order-missing, agent-contract-mismatch, merge-blocked,
		// deploy-blocked, head-moving, ci-red, ci-blocked, ownership-lost.
		//
		// NO re-entry, and NOT a sweep hand-off either: a parked Task keeps its
		// controller ownership, so the sweep sees issue_owned and re-mints
		// nothing until the reaper collects the Task at ParkRetention. What a
		// human reply gets instead is resumeNoReentryParks, which severs the
		// owned issues and re-mints them in the SAME pass - a NEW, gate-
		// respecting Task, never a re-entry of this one. With no reply it ages
		// out at ParkRetention and is reaped, AFTER the operator posts its bot
		// park comment, and the next sweep re-mints the now-orphan issue as a
		// parked(backlog-sweep) Task which OWNS it and costs nothing.
		return DeclineNoReentry
	}
	if class == UnparkHuman && !hasNonBotEvent(t, in.BotLogin) {
		return DeclineNoHumanEvent
	}

	switch t.Status.ParkReason {

	case ReasonBacklogSweep:
		// NOT a park in the failure sense: this Task never ran. It exists to OWN
		// its Issue CRs at zero agent cost (B.4). It NEVER ages out; it is reaped
		// when its Issues close. A refine fold does NOT promote it - the fold
		// DELETES it (B.3).
		if in.ActiveTasks >= in.MaxOpenTasks {
			// OVER CAP: the promotion DEFERS. The Task stays parked and the
			// pendingEvent is RETAINED, never dropped. It promotes as soon as a
			// slot frees.
			return DeclineOverCap
		}
		reArm(t, now)
		return DeclineNone

	case ReasonAwaitingHuman:
		if t.Spec.Kind == kindReview {
			// A review-kind Task may NEVER enter under-implementation or merged.
			// There is no path, no condition, no exception - and un-park no
			// longer moves state at all, so the guard is now structural twice
			// over.
			//
			// humanReviewRounds is a NEW counter and it is NOT mr.reviewRounds,
			// which increments only on request_changes: on the approve path that
			// bound did not exist, and this spawned ONE REVIEW POD PER HUMAN
			// COMMENT, capped only by the (since deleted) maxTurnsPerTask.
			if anyMerged(in.MRs) {
				// A pod spawned into awaiting-review on an already-merged MR has
				// no legal outcome (issue #393). Refuse re-entry; the Task ages
				// out at ParkRetention and is reaped.
				return DeclineMergedMR
			}
			// #511: an externally-owned MR (a stand-down) is not an ordinary
			// review round - it is the ONE state a "take over" comment can arrive
			// in, and the round cap was sized to bound review ping-pong, not to
			// swallow a maintainer's take-over request. Skip the cap and DO NOT
			// spend a round.
			if !anyExternallyOwned(in.MRs) {
				if t.Status.HumanReviewRounds >= v1alpha1.MaxHumanReviewRounds {
					return DeclineRoundsExhausted // STAY PARKED. Do not spawn another review pod.
				}
				if d := liveRoomDecline(t, in); d != DeclineNone {
					return d
				}
				t.Status.HumanReviewRounds++
				reArm(t, now)
				return DeclineNone
			}
			if d := liveRoomDecline(t, in); d != DeclineNone {
				return d
			}
			reArm(t, now)
			return DeclineNone
		}
		// THE EMPTY SET IS NOT A LICENCE (fix V6-3). A Task whose owned Issues
		// have all closed has nothing left to talk about, and resuming it spawns
		// a pod that renders a bundle with no live thread in it. v5's "if EVERY
		// owned Issue is approved -> implementing" promoted a review Task
		// straight into an implement pod on ANY human comment, because
		// all([]) == true; that promotion is gone with the target derivation, but
		// the empty-set refusal it taught is kept.
		if len(openIssues(in.Issues)) == 0 {
			return DeclineNoOpenIssues
		}
		if d := liveRoomDecline(t, in); d != DeclineNone {
			return d
		}
		reArm(t, now)
		return DeclineNone

	case ReasonIdentityUnverified:
		// A comment alone still cannot GRANT anything here. The grant lives
		// EXCLUSIVELY in restapi.verifyApprovalScope, which re-derives the
		// maintainer identity and re-checks the agent's citation against the
		// operator's own mirror on every submit_outcome(action=approved). This
		// arm has exactly one job - put a live agent in front of the human who
		// just commented.
		if d := liveRoomDecline(t, in); d != DeclineNone {
			// D1: the truthful terminus. The live-pod ceiling exists to bound
			// live agent pods, and there is no cheaper state to fall back to
			// under the merged model. The pendingEvent is RETAINED.
			return d
		}
		if t.Spec.Kind == kindReview {
			if anyMerged(in.MRs) {
				return DeclineMergedMR
			}
			if t.Status.HumanReviewRounds >= v1alpha1.MaxHumanReviewRounds {
				return DeclineRoundsExhausted
			}
			t.Status.HumanReviewRounds++
		}
		reArm(t, now)
		return DeclineNone

	case ReasonHandoffStalled:
		// The outcome COMMITTED but the C.5.3 phase-2 drain lost the advance
		// (PR 389: a stale informer cache defeated advanceAfterReview). The work
		// already landed on the forge, so this park is recoverable: a human
		// comment re-arms awaiting-review, where the reconciler's
		// level-triggered re-drive - or, failing that, a fresh review round -
		// completes it. Bounded by humanReviewRounds exactly like the
		// kind=review awaiting-human rule, and for the same reason: each
		// re-entry may spawn a review pod, and a comment storm must not spawn
		// one per comment.
		if anyMerged(in.MRs) {
			return DeclineMergedMR
		}
		if t.Status.HumanReviewRounds >= v1alpha1.MaxHumanReviewRounds {
			return DeclineRoundsExhausted
		}
		if d := liveRoomDecline(t, in); d != DeclineNone {
			return d
		}
		t.Status.HumanReviewRounds++
		reArm(t, now)
		return DeclineNone

	case ReasonMergeTimeout:
		if t.Status.MergeReentries >= v1alpha1.MaxMergeReentries {
			// The bound is spent. RE-PARK with merge-blocked, which is
			// UnparkNever, so it ages out instead of retrying forever. This
			// replaces the deleted failed(merge-blocked) terminal.
			repark(t, ReasonMergeBlocked, now)
			return DeclineRetryBudgetSpent
		}
		t.Status.MergeReentries++
		// Idempotent: mergeCursor resumes and EVERY MR is re-validated against
		// state=merged before any Merge call.
		reArm(t, now)
		return DeclineNone

	case ReasonDeployTimeout:
		if t.Status.DeployReentries >= v1alpha1.MaxDeployReentries {
			repark(t, ReasonDeployBlocked, now)
			return DeclineRetryBudgetSpent
		}
		t.Status.DeployReentries++
		// Idempotent: per-MR deployedAt re-check.
		reArm(t, now)
		return DeclineNone

	case ReasonNoOutcome:
		// #406: only a park reached FROM under-implementation or awaiting-review
		// may re-drive. A no-outcome park from `new` or `refined` means a
		// pre-implement state never terminated (the real mechanism: an
		// SCM-comment error left the outcome claim unreleased, pod-liveness
		// respawned the pod until PodRecreations exhausted) - such a Task must
		// NOT be auto-escalated straight into implementation work.
		if t.Status.ParkedFromState != v1alpha1.StateUnderImplementation &&
			t.Status.ParkedFromState != v1alpha1.StateAwaitingReview {
			return DeclineWrongParkedFrom
		}
		if anyMerged(in.MRs) {
			// A re-implement would duplicate an already-merged change.
			return DeclineMergedMR
		}
		// THE TURN GATE IS GONE (O3), and its removal is the point rather than a
		// side effect. It read `Stats.Turns >= in.MaxTurnsPerTask` and it was the
		// SECOND CLOCK a no-outcome park landed on: clearing the park only to be
		// refused re-entry on a turn count the same release stopped enforcing
		// anywhere else is a Task that can never be recovered by any means the
		// operator has. maxTurnsPerTask no longer terminates a live Task, so it
		// must not veto an un-park either.
		if d := liveRoomDecline(t, in); d != DeclineNone {
			return d
		}
		reArm(t, now)
		return DeclineNone

	default:
		// Unreachable: UnparkClassFor is total over ParkReasons and every
		// non-Never member has an arm above. Named rather than silent.
		return DeclineNoReentry
	}
}

// liveRoomDecline is the live-pod ceiling, applied to every un-park that would
// resume a LIVE state. It is a no-op for an operator-driven or terminal state:
// those run no pod, so the ceiling has nothing to say about them.
func liveRoomDecline(t *v1alpha1.Task, in UnparkInput) string {
	if Live(t.Status.State) && !in.LiveHasRoom {
		return DeclineNoLiveRoom
	}
	return DeclineNone
}

// reArm clears the park flag and restarts the state's clock IN PLACE. It is the
// un-park half of stampEnter: same stamps, same reason for each of them, no
// state change and no reason change.
//
// StageElapsedCarrySeconds is PRESERVED, always: Park folded this state's
// residency into it and reported residency plus ResidencyExceeded must stay
// continuous across the round trip (#480, #513). Only a genuine state
// transition launders it, in stampEnter.
func reArm(t *v1alpha1.Task, now time.Time) {
	clearPark(t)
	stamp := metav1.NewTime(now)
	t.Status.StateEnteredAt = &stamp
	t.Status.PodStartedAt = nil
	t.Status.StateWorkStartedAt = nil
	t.Status.Stats.PodRecreations = 0
	if Live(t.Status.State) {
		t.Status.ConversationLastEventAt = &stamp
	}
}

// hasNonBotEvent reports whether a HUMAN commented. The E.3 enqueue filter
// already drops bot events, but the operator's own park comment must never be
// able to un-park the Task it parked, so the check is repeated here where the
// decision is actually made.
func hasNonBotEvent(t *v1alpha1.Task, botLogin string) bool {
	for i := range t.Status.PendingEvents {
		if t.Status.PendingEvents[i].Author != botLogin {
			return true
		}
	}
	return false
}

func openIssues(issues []v1alpha1.Issue) []v1alpha1.Issue {
	out := make([]v1alpha1.Issue, 0, len(issues))
	for i := range issues {
		if issues[i].Status.State == "open" {
			out = append(out, issues[i])
		}
	}
	return out
}

func anyMerged(mrs []v1alpha1.MergeRequest) bool {
	for i := range mrs {
		if mrs[i].Status.State == "merged" || mrs[i].Status.MergedAt != nil {
			return true
		}
	}
	return false
}

// anyExternallyOwned reports whether any of mrs is currently Ownership ==
// external - a stand-down state (#511) where a human comment is plausibly a
// take-over request, not an ordinary review round.
func anyExternallyOwned(mrs []v1alpha1.MergeRequest) bool {
	for i := range mrs {
		if mrs[i].Status.Ownership == v1alpha1.OwnershipExternal {
			return true
		}
	}
	return false
}
