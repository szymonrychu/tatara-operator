// Package stage is the Task stage machine (contract F). It is a PURE package:
// tables, predicates, and Unpark. It never talks to the API server, the forge,
// or a Kubelet, and it must never import internal/controller.
//
// The tables are DATA, not switch statements with a default. The point of a
// table is that a new stage cannot be added without appearing in it: a switch
// with a default silently accepts anything.
package stage

import (
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// Create is the pseudo-stage a Task is minted FROM. Reap and Respawn are
// pseudo-TARGETS: neither is a stage, and neither is ever written to
// status.stage. Reap means "the reaper deletes this Task"; Respawn means "the
// pod is recreated" (clock 2 is a respawn trigger, not a terminal).
const (
	Create  = "(create)"
	Reap    = "(reap)"
	Respawn = "(respawn)"
)

// The seven agent kinds (contract F.2).
const (
	AgentBrainstorm    = "brainstorm"
	AgentClarify       = "clarify"
	AgentIncident      = "incident"
	AgentRefine        = "refine"
	AgentImplement     = "implement"
	AgentReview        = "review"
	AgentDocumentation = "documentation"
)

// kindReview is the Task.Spec.Kind that may NEVER reach implementing or
// merging. There is no path, no condition, no exception. It does not exist.
const kindReview = "review"

// The three clocks (contract F.4). Exactly ONE is armed at a time, and WHICH
// one is armed is decided by which timestamps are set - never by the stage
// alone.
const (
	ClockNone      = "none"
	ClockAdmission = "admission"
	ClockReadiness = "readiness"
	ClockWork      = "work"
)

// Stage reasons (contract F.5, the CLOSED set).
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
)

// Reasons is the F.5 closed set. A reason not in it is REJECTED by Enter.
// pod-not-ready IS NOT A MEMBER (fix V7-7): it was never a terminal state, it
// was a respawn trigger wearing a terminal's name. A never-Ready pod respawns
// (clock 2) and the terminal, once the recreation budget is spent, is
// pod-recreation-exhausted.
var Reasons = []string{
	ReasonBacklogSweep,
	ReasonTriageStalled,
	ReasonNameTooLong,
	ReasonStageDeadline,
	ReasonAwaitingHuman,
	ReasonIdentityUnverified,
	ReasonImplementDeclined,
	ReasonDeclined,
	ReasonFalsePositive,
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
	ReasonDocTimeout,
	ReasonOperatorError,
	ReasonHeadMoving,
	ReasonHandoffStalled,
}

var reasonSet = func() map[string]bool {
	m := make(map[string]bool, len(Reasons))
	for _, r := range Reasons {
		m[r] = true
	}
	return m
}()

// ValidReason reports whether r is a member of the F.5 closed set.
func ValidReason(r string) bool { return reasonSet[r] }

// Edge is one row of the F.3 transition table. To is a stage, or one of the
// pseudo-targets Reap / Respawn. Reason is the stage reason stamped on To
// (empty when To carries none). Trigger is the contract's own prose.
type Edge struct {
	To      string
	Reason  string
	Trigger string
}

// AllStages returns the 15 members of the F.1 enum. Iteration order is the
// contract's.
func AllStages() []string {
	return []string{
		v1alpha1.StageTriaging,
		v1alpha1.StageBrainstorming,
		v1alpha1.StageClarifying,
		v1alpha1.StageInvestigating,
		v1alpha1.StageRefining,
		v1alpha1.StageApproved,
		v1alpha1.StageImplementing,
		v1alpha1.StageReviewing,
		v1alpha1.StageMerging,
		v1alpha1.StageDeploying,
		v1alpha1.StageDelivered,
		v1alpha1.StageDocumenting,
		v1alpha1.StageRejected,
		v1alpha1.StageFailed,
		v1alpha1.StageParked,
	}
}

// agentKinds is the F.2 table: which agent kind each stage spawns. A stage
// mapping to "" is POD-LESS. This table and v1alpha1.StagePodless are asserted
// to agree, mechanically, in the tests.
var agentKinds = map[string]string{
	v1alpha1.StageTriaging:      "",
	v1alpha1.StageBrainstorming: AgentBrainstorm,
	v1alpha1.StageClarifying:    AgentClarify,
	v1alpha1.StageInvestigating: AgentIncident,
	v1alpha1.StageRefining:      AgentRefine,
	v1alpha1.StageApproved:      "",
	v1alpha1.StageImplementing:  AgentImplement,
	v1alpha1.StageReviewing:     AgentReview,
	v1alpha1.StageMerging:       "",
	v1alpha1.StageDeploying:     "",
	v1alpha1.StageDelivered:     "",
	v1alpha1.StageDocumenting:   AgentDocumentation,
	v1alpha1.StageRejected:      "",
	v1alpha1.StageFailed:        "",
	v1alpha1.StageParked:        "",
}

// AgentKindFor is the F.2 table. It returns "" for a pod-less stage.
func AgentKindFor(stage string) string { return agentKinds[stage] }

// EnforcesMaxTurnsPerPod reports whether maxTurnsPerPod bounds this agent kind.
// The implement kind is EXEMPT (F.4): a long healthy coding run must not be cut
// off. It is bounded instead by maxTurnsPerTask and the implementing stage
// deadline. maxTurnsPerPod never terminates a Task in any case - it stops the
// POD via the G.7 TTL handoff and respawns, spending one podRecreations.
func EnforcesMaxTurnsPerPod(agentKind string) bool {
	return agentKind != "" && agentKind != AgentImplement
}

// Transitions is the F.3 table as data, keyed by the FROM stage (plus the
// Create pseudo-stage). No agent writes status.stage; only the operator does,
// and a transition not in this table is REJECTED (Enter returns
// *IllegalTransitionError, off which the reconciler labels
// operator_illegal_stage_transition_total{from,to}).
var Transitions = map[string][]Edge{
	Create: {
		{To: v1alpha1.StageTriaging, Trigger: "Task minted ACTIVE: webhook-originated, or a human has the last word on the thread (B.4)"},
		{To: v1alpha1.StageParked, Reason: ReasonBacklogSweep, Trigger: "Task minted from a SWEEP-discovered backlog issue (B.4). Spawns no pod, enqueues nothing"},
		{To: v1alpha1.StageDocumenting, Trigger: "the NIGHTLY documentation batch is minted (F.3)"},
	},

	v1alpha1.StageTriaging: {
		{To: v1alpha1.StageBrainstorming, Trigger: "spec.kind = brainstorm"},
		{To: v1alpha1.StageClarifying, Trigger: "spec.kind = clarify"},
		{To: v1alpha1.StageInvestigating, Trigger: "spec.kind = incident"},
		{To: v1alpha1.StageRefining, Trigger: "spec.kind = refine"},
		{To: v1alpha1.StageReviewing, Trigger: "spec.kind = review"},
		{To: v1alpha1.StageDocumenting, Trigger: "spec.kind = documentation"},
		{To: v1alpha1.StageFailed, Reason: ReasonTriageStalled, Trigger: "spec validation fails, or the 5m triage budget elapses"},
		{To: v1alpha1.StageFailed, Reason: ReasonNameTooLong, Trigger: "the 49-char name guard fails"},
		{To: v1alpha1.StageFailed, Reason: ReasonOperatorError, Trigger: "unrecoverable operator error"},
		{To: v1alpha1.StageFailed, Reason: ReasonObjectTooLarge, Trigger: "the A.7 byte-budget pre-write guard refuses"},
	},

	v1alpha1.StageBrainstorming: podStageEdges(
		Edge{To: v1alpha1.StageDelivered, Trigger: "submit_outcome(propose|skip). documentedBy stays empty: no docs Task is spawned (fix 25)"},
	),

	v1alpha1.StageClarifying: podStageEdges(
		Edge{To: v1alpha1.StageApproved, Trigger: "submit_outcome(decision=implement) AND the C.6 grammar passes for EVERY owned Issue"},
		Edge{To: v1alpha1.StageParked, Reason: ReasonIdentityUnverified, Trigger: "decision=implement but the C.6 grammar FAILS"},
		Edge{To: v1alpha1.StageParked, Reason: ReasonAwaitingHuman, Trigger: "decision=discuss, or the 24h clarify budget elapses"},
		Edge{To: v1alpha1.StageRejected, Reason: ReasonDeclined, Trigger: "decision=close (the operator closes the issue)"},
	),

	v1alpha1.StageInvestigating: podStageEdges(
		Edge{To: v1alpha1.StageClarifying, Trigger: "submit_outcome(file_issue): the tracker Issue is created under THIS Task"},
		Edge{To: v1alpha1.StageRejected, Reason: ReasonFalsePositive, Trigger: "submit_outcome(false_positive). No docs Task (fix 25)"},
	),

	v1alpha1.StageRefining: podStageEdges(
		Edge{To: v1alpha1.StageDelivered, Trigger: "folds/closes/links applied AND the fold VERIFIED (B.3)"},
		Edge{To: v1alpha1.StageFailed, Reason: ReasonFoldAdoptionUnverified, Trigger: "B.3 step-3 verification fails"},
	),

	// approved is POD-LESS (the admission gate). Its own 24h budget elapses to
	// parked(admission-starved), which is exactly clock 1 by another name - and
	// that is why the paused-project carve-out covers it too.
	v1alpha1.StageApproved: {
		{To: v1alpha1.StageImplementing, Trigger: "a QueuedEvent for the implement pod is ADMITTED"},
		{To: v1alpha1.StageClarifying, Trigger: "the Task ACQUIRES a new Issue after approval. Approval is not sticky (fix H9)"},
		{To: v1alpha1.StageParked, Reason: ReasonAdmissionStarved, Trigger: "the 24h admission budget elapses (skipped when the project is PAUSED)"},
		{To: v1alpha1.StageFailed, Reason: ReasonOperatorError, Trigger: "unrecoverable operator error"},
		{To: v1alpha1.StageFailed, Reason: ReasonObjectTooLarge, Trigger: "the A.7 byte-budget pre-write guard refuses"},
	},

	v1alpha1.StageImplementing: podStageEdges(
		Edge{To: v1alpha1.StageReviewing, Trigger: "submit_outcome(submitted) and >= 1 owned MR is open"},
		Edge{To: v1alpha1.StageParked, Reason: ReasonImplementDeclined, Trigger: "submit_outcome(declined)"},
	),

	v1alpha1.StageReviewing: podStageEdges(
		// BOTH of these exist ONLY for spec.kind != "review". The guard is in
		// LegalFor, which Enter uses, so it is structurally impossible to bypass.
		Edge{To: v1alpha1.StageImplementing, Trigger: "submit_outcome(request_changes) AND spec.kind != review AND reviewRounds < maxReviewRounds. Gated on pendingReview == nil"},
		Edge{To: v1alpha1.StageMerging, Trigger: "submit_outcome(approve) AND spec.kind != review. Gated on pendingReview == nil"},
		Edge{To: v1alpha1.StageParked, Reason: ReasonAwaitingHuman, Trigger: "submit_outcome(approve|request_changes) on a kind=review Task. The review IS posted. A human's PR is fixed and merged by the human (fixes V7-1, C3-2)"},
		Edge{To: v1alpha1.StageParked, Reason: ReasonReviewLoopExhausted, Trigger: "request_changes at maxReviewRounds, on a non-review Task"},
		Edge{To: v1alpha1.StageParked, Reason: ReasonReviewPostRefused, Trigger: "a structural 4xx from PostReview (fix C1)"},
		Edge{To: v1alpha1.StageParked, Reason: ReasonHandoffStalled, Trigger: "the outcome COMMITTED but the C.5.3 phase-2 drain (DrainPendingReview -> advanceAfterReview) never advanced the Task within HandoffDeadline (5m). ONLY reviewing carries it: every other kind's commit calls stage.Enter in the SAME write, so no other stage can be committed-but-not-advanced"},
	),

	// merging is POD-LESS: clock 3 ONLY, from stageEnteredAt, against ITS OWN 4h
	// budget. It gets NO 24h admission-starved clock (fix V7-8) - if it did, it
	// could never reach merge-timeout and the bounded merge re-entry cycle would
	// never engage at all.
	v1alpha1.StageMerging: {
		{To: v1alpha1.StageDeploying, Trigger: "every repo in mergeOrder merged, in order, each on green CI"},
		{To: v1alpha1.StageReviewing, Trigger: "a live head != reviewedSHA, or Merge 409s head-moved. INCREMENTS status.headMoveReentries (the FOURTH cycle, fix M3-9)"},
		{To: v1alpha1.StageImplementing, Trigger: "a maintainer requested changes on the still-open MR before it merged (F.6-adjacent). kind=review refused by LegalFor"},
		{To: v1alpha1.StageFailed, Reason: ReasonHeadMoving, Trigger: "headMoveReentries at maxHeadMoveReentries"},
		{To: v1alpha1.StageFailed, Reason: ReasonMergeBlocked, Trigger: "mergeReentries at maxMergeReentries (fix H7)"},
		{To: v1alpha1.StageFailed, Reason: ReasonMergeOrderMissing, Trigger: "len(spec.mergeOrder) == 0 on entry (bug-catcher)"},
		{To: v1alpha1.StageFailed, Reason: ReasonOperatorError, Trigger: "unrecoverable operator error"},
		{To: v1alpha1.StageFailed, Reason: ReasonObjectTooLarge, Trigger: "the A.7 byte-budget pre-write guard refuses"},
		{To: v1alpha1.StageParked, Reason: ReasonMergeTimeout, Trigger: "the 4h merging budget elapses"},
	},

	v1alpha1.StageDeploying: {
		{To: v1alpha1.StageDelivered, Trigger: "every owned MR merged AND deployedAt != nil. The OPERATOR closes every owned Issue and stamps deliveredAt (C.4)"},
		{To: v1alpha1.StageFailed, Reason: ReasonDeployBlocked, Trigger: "deployReentries at maxDeployReentries (fix H7)"},
		{To: v1alpha1.StageFailed, Reason: ReasonOperatorError, Trigger: "unrecoverable operator error"},
		{To: v1alpha1.StageFailed, Reason: ReasonObjectTooLarge, Trigger: "the A.7 byte-budget pre-write guard refuses"},
		{To: v1alpha1.StageParked, Reason: ReasonDeployTimeout, Trigger: "the 2h deploying budget elapses"},
	},

	v1alpha1.StageDocumenting: podStageEdges(
		Edge{To: v1alpha1.StageReviewing, Trigger: "submit_outcome(submitted) on the docs MR"},
		Edge{To: v1alpha1.StageDelivered, Reason: ReasonDocTimeout, Trigger: "submit_outcome(declined), or the 2h docStageBudget elapses. documentedBy is stamped on every covered parent either way"},
	),

	// delivered is QUASI-terminal: nothing is spawned per-delivery (documentation
	// is a nightly BATCH, fix F2), and the reaper collects it at 48h. It is not
	// in the terminal set, so operator-error can still fail it.
	v1alpha1.StageDelivered: {
		{To: v1alpha1.StageFailed, Reason: ReasonOperatorError, Trigger: "unrecoverable operator error"},
	},

	// parked is a DEAD END that ages out. Its ONLY exits are the narrow F.6
	// re-entry rules, and Unpark is the ONE function that produces them.
	v1alpha1.StageParked: {
		{To: v1alpha1.StageTriaging, Trigger: "F.6 backlog-sweep: a non-bot pendingEvent AND ACTIVE Tasks < maxOpenTasks"},
		{To: v1alpha1.StageReviewing, Trigger: "F.6 awaiting-human on a kind=review Task, bounded by humanReviewRounds (5)"},
		{To: v1alpha1.StageImplementing, Trigger: "F.6 awaiting-human (every open owned Issue approved) / identity-unverified (the C.6 grammar re-passes) / no-outcome (zero merged MRs)"},
		{To: v1alpha1.StageClarifying, Trigger: "F.6 awaiting-human, not every open owned Issue is approved"},
		{To: v1alpha1.StageMerging, Trigger: "F.6 merge-timeout, under maxMergeReentries. NEVER implementing"},
		{To: v1alpha1.StageDeploying, Trigger: "F.6 deploy-timeout, under maxDeployReentries. NEVER implementing"},
		{To: v1alpha1.StageFailed, Reason: ReasonMergeBlocked, Trigger: "F.6 merge-timeout at maxMergeReentries"},
		{To: v1alpha1.StageFailed, Reason: ReasonDeployBlocked, Trigger: "F.6 deploy-timeout at maxDeployReentries"},
	},

	// rejected and failed are TERMINAL. They have no exits: they age out and the
	// reaper collects them.
	v1alpha1.StageRejected: {},
	v1alpha1.StageFailed:   {},
}

// podStageEdges appends the exits EVERY pod-spawning stage carries (F.3, F.4)
// to that stage's own edges. It exists so a new pod stage cannot be added
// without them.
func podStageEdges(own ...Edge) []Edge {
	common := []Edge{
		{To: v1alpha1.StageParked, Reason: ReasonAdmissionStarved, Trigger: "CLOCK 1: podStartedAt == nil and now > stageEnteredAt + 24h. Skipped when the project is PAUSED"},
		{To: v1alpha1.StageParked, Reason: ReasonStageDeadline, Trigger: "CLOCK 3: the F.4 work budget elapses"},
		{To: v1alpha1.StageParked, Reason: ReasonNoOutcome, Trigger: "a pod stopped having submitted no outcome AND the recreation budget is spent"},
		{To: v1alpha1.StageParked, Reason: ReasonObjectTooLarge, Trigger: "the A.7 byte-budget pre-write guard refuses"},
		{To: v1alpha1.StageFailed, Reason: ReasonTurnBudgetExhausted, Trigger: "stats.turns >= maxTurnsPerTask"},
		{To: v1alpha1.StageFailed, Reason: ReasonPodRecreationExhausted, Trigger: "stats.podRecreations > maxPodRecreations. This is the terminal for a never-Ready pod"},
		{To: v1alpha1.StageFailed, Reason: ReasonAgentContractMismatch, Trigger: "the wrapper's contractVersion != 2 at pod-ready, BEFORE turn-0 is submitted (fix A2)"},
		{To: v1alpha1.StageFailed, Reason: ReasonOperatorError, Trigger: "unrecoverable operator error"},
	}
	return append(append([]Edge{}, own...), common...)
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

// Legal reports whether the from -> to edge exists in the F.3 table. It has no
// Task in scope, so it CANNOT enforce the kind guard: use LegalFor (or Enter,
// which uses it) wherever a Task is available.
func Legal(from, to string) bool { return legalPairs[[2]string{from, to}] }

// LegalFor is Legal plus the two guards that need the Task and its owned MRs.
//
// GUARD 1 (fixes V7-1, V6-3, C3-2). A kind=review Task may NEVER enter
// implementing or merging. Not from reviewing on request_changes (the review
// agent's NORMAL verdict on a bad human PR - the PRIMARY path v6 missed), not
// from reviewing on approve, not from parked by any un-park rule, not from
// anywhere. There is no author check to get wrong because the sweep ignores
// bot-authored non-adoptable PRs, so EVERY review Task is non-bot-authored by
// construction. Merging or fixing a human's PR is a HUMAN action.
//
// GUARD 2 (contract C.5.3). reviewing -> implementing and reviewing -> merging
// BOTH require that every owned MergeRequest has status.pendingReview == nil. A
// non-nil pendingReview means "a review is owed to the forge and the mirror has
// not recorded it yet"; a pod spawned then renders a bundle with no findings in
// it, re-submits, and burns maxReviewRounds. An EMPTY owned-MR set does not
// open the gate either.
func LegalFor(t *v1alpha1.Task, mrs []v1alpha1.MergeRequest, from, to string) bool {
	if !Legal(from, to) {
		return false
	}
	if t != nil && t.Spec.Kind == kindReview &&
		(to == v1alpha1.StageImplementing || to == v1alpha1.StageMerging) {
		return false
	}
	if from == v1alpha1.StageReviewing &&
		(to == v1alpha1.StageImplementing || to == v1alpha1.StageMerging) &&
		!reviewGateOpen(mrs) {
		return false
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

// IllegalTransitionError is returned by Enter when the edge is not in the F.3
// table (or a guard refuses it). From/To are the labels the reconciler puts on
// operator_illegal_stage_transition_total{from,to}.
type IllegalTransitionError struct {
	From string
	To   string
}

func (e *IllegalTransitionError) Error() string {
	return fmt.Sprintf("illegal stage transition %s -> %s", e.From, e.To)
}

// UnknownReasonError is returned by Enter for a reason outside the F.5 closed
// set. pod-not-ready lands here.
type UnknownReasonError struct{ Reason string }

func (e *UnknownReasonError) Error() string {
	return fmt.Sprintf("stage reason %q is not in the F.5 closed set", e.Reason)
}

// MissingReasonError is returned by Enter when a terminal stage is entered with
// no reason. The reason is MANDATORY on parked/failed/rejected.
type MissingReasonError struct{ To string }

func (e *MissingReasonError) Error() string {
	return fmt.Sprintf("stage %s requires a stage reason", e.To)
}

// Enter is the ONE way a stage is entered, so no caller can forget the four
// things EVERY transition does (F.3, fix V7-4):
//
//	status.stageEnteredAt     = now
//	status.podStartedAt       = nil     <- load-bearing
//	status.stageWorkStartedAt = nil
//	stats.podRecreations      = 0
//
// Forgetting podStartedAt = nil leaves a Task covered by NO CLOCK while it
// queues on a re-entry edge (clock 1 is armed only when podStartedAt == nil,
// and clock 2 needs a pod that does not exist yet), and puts the G.7 TTL base
// t0 = podStartedAt + agentPodTTLSeconds ALREADY IN THE PAST for the next pod,
// so the operator TTL-stops it before its first turn.
//
// mrs are the MergeRequests this Task OWNS; they feed the C.5.3 pendingReview
// gate. Pass nil when the Task owns none.
func Enter(t *v1alpha1.Task, mrs []v1alpha1.MergeRequest, to, reason string, now time.Time) error {
	from := t.Status.Stage
	if from == "" {
		from = Create
	}
	if !LegalFor(t, mrs, from, to) {
		return &IllegalTransitionError{From: from, To: to}
	}
	if reason != "" && !ValidReason(reason) {
		return &UnknownReasonError{Reason: reason}
	}
	if reason == "" && reasonRequired(to) {
		return &MissingReasonError{To: to}
	}

	if to == v1alpha1.StageParked {
		t.Status.ParkedFromStage = from
	}
	stamp := metav1.NewTime(now)
	t.Status.Stage = to
	t.Status.StageReason = reason
	t.Status.AgentKind = AgentKindFor(to)
	t.Status.StageEnteredAt = &stamp
	t.Status.PodStartedAt = nil
	t.Status.StageWorkStartedAt = nil
	t.Status.Stats.PodRecreations = 0
	return nil
}

func reasonRequired(to string) bool {
	switch to {
	case v1alpha1.StageParked, v1alpha1.StageFailed, v1alpha1.StageRejected:
		return true
	default:
		return false
	}
}

// budgets is the F.4 WORK-clock table, verbatim. EVERY member of the F.1 enum
// has a row: a new stage cannot be added without one, and a table-driven test
// asserts it. parked(backlog-sweep) is the ONE exemption, and it is a REASON,
// not a stage: the parked STAGE still has its parkRetention row, and ArmedClock
// disarms every clock on that one reason.
var budgets = map[string]time.Duration{
	v1alpha1.StageTriaging:      5 * time.Minute,
	v1alpha1.StageBrainstorming: 2 * time.Hour,
	v1alpha1.StageClarifying:    24 * time.Hour,
	v1alpha1.StageInvestigating: 2 * time.Hour,
	v1alpha1.StageRefining:      2 * time.Hour,
	v1alpha1.StageApproved:      24 * time.Hour,
	v1alpha1.StageImplementing:  6 * time.Hour,
	v1alpha1.StageReviewing:     4 * time.Hour,
	v1alpha1.StageMerging:       4 * time.Hour,
	v1alpha1.StageDeploying:     2 * time.Hour,
	v1alpha1.StageDocumenting:   v1alpha1.DocStageBudget,
	v1alpha1.StageDelivered:     v1alpha1.DeliveredRetention,
	v1alpha1.StageRejected:      v1alpha1.RejectedRetention,
	v1alpha1.StageFailed:        v1alpha1.FailedRetention,
	v1alpha1.StageParked:        v1alpha1.ParkRetention,
}

// onElapse is the other column of the same row: where the WORK clock goes when
// the budget is spent.
var onElapse = map[string]Edge{
	v1alpha1.StageTriaging:      {To: v1alpha1.StageFailed, Reason: ReasonTriageStalled},
	v1alpha1.StageBrainstorming: {To: v1alpha1.StageParked, Reason: ReasonStageDeadline},
	v1alpha1.StageClarifying:    {To: v1alpha1.StageParked, Reason: ReasonAwaitingHuman},
	v1alpha1.StageInvestigating: {To: v1alpha1.StageParked, Reason: ReasonStageDeadline},
	v1alpha1.StageRefining:      {To: v1alpha1.StageParked, Reason: ReasonStageDeadline},
	v1alpha1.StageApproved:      {To: v1alpha1.StageParked, Reason: ReasonAdmissionStarved},
	v1alpha1.StageImplementing:  {To: v1alpha1.StageParked, Reason: ReasonStageDeadline},
	v1alpha1.StageReviewing:     {To: v1alpha1.StageParked, Reason: ReasonStageDeadline},
	v1alpha1.StageMerging:       {To: v1alpha1.StageParked, Reason: ReasonMergeTimeout},
	v1alpha1.StageDeploying:     {To: v1alpha1.StageParked, Reason: ReasonDeployTimeout},
	v1alpha1.StageDocumenting:   {To: v1alpha1.StageDelivered, Reason: ReasonDocTimeout},
	v1alpha1.StageDelivered:     {To: Reap},
	v1alpha1.StageRejected:      {To: Reap},
	v1alpha1.StageFailed:        {To: Reap},
	v1alpha1.StageParked:        {To: Reap},
}

// Budget is the F.4 WORK-clock table. ok is false only for a stage that is not
// in the F.1 enum.
func Budget(stage string) (time.Duration, bool) {
	d, ok := budgets[stage]
	return d, ok
}

// OnElapse is where the WORK clock goes when Budget is spent. Edge.To may be
// the Reap pseudo-target.
func OnElapse(stage string) (Edge, bool) {
	e, ok := onElapse[stage]
	return e, ok
}

// ArmedClock is THE THREE-CLOCK SELECTOR (F.4). Exactly ONE clock is armed at a
// time, and WHICH one is decided by which timestamps are set - NEVER by the
// stage alone:
//
//	podStartedAt == nil                             -> CLOCK 1 ADMISSION, from
//	                                                   stageEnteredAt, 24h ->
//	                                                   parked(admission-starved)
//	podStartedAt != nil && stageWorkStartedAt == nil -> CLOCK 2 READINESS, from
//	                                                   podStartedAt, 5m -> RESPAWN
//	stageWorkStartedAt != nil                       -> CLOCK 3 WORK, from
//	                                                   stageWorkStartedAt, the F.4
//	                                                   budget -> parked(stage-deadline)
//
// podStartedAt == nil AND stageWorkStartedAt == nil is CLOCK 1. It is a named
// case, not an inference. The READINESS clock NEVER measures from
// stageEnteredAt: that includes the admission queue, and the queue is where a
// Task in normal steady state sits.
//
// POD-LESS stages run CLOCK 3 ONLY, measured from stageEnteredAt, against their
// OWN budget. They do NOT run clock 1 (fix V7-8): merging with a 24h
// admission-starved clock could never reach merge-timeout, and the bounded merge
// re-entry cycle would never engage at all.
//
// paused is Project.spec.maxConcurrentAgents == 0. It disarms the ADMISSION
// clock - clock 1 on every pod stage, and the pod-less `approved` stage, whose
// budget elapses to the same admission-starved reason. It is the ONLY deadline
// exception in the contract: without it the pause kill switch is a backlog
// shredder. It does NOT disarm clocks 2 and 3, which measure a pod that already
// exists.
//
// clock is ClockNone when nothing is armed; since/budget/onElapse are then zero.
func ArmedClock(t *v1alpha1.Task, paused bool) (clock string, since time.Time, budget time.Duration, onElapseEdge Edge) {
	stg := t.Status.Stage
	budget, ok := Budget(stg)
	if !ok || t.Status.StageEnteredAt == nil {
		return ClockNone, time.Time{}, 0, Edge{}
	}
	// The ONE exemption (F.4): parked(backlog-sweep) is not stalled work, it is
	// the durable owner of an Issue CR at zero agent cost. It consumes nothing:
	// no pod, no queue slot, no turn. It NEVER ages out - it is reaped when its
	// Issues close.
	if stg == v1alpha1.StageParked && t.Status.StageReason == ReasonBacklogSweep {
		return ClockNone, time.Time{}, 0, Edge{}
	}

	elapse, ok := OnElapse(stg)
	if !ok {
		return ClockNone, time.Time{}, 0, Edge{}
	}

	if v1alpha1.StagePodless(stg) {
		// CLOCK 3 ONLY, from stageEnteredAt, against its own budget.
		if paused && elapse.Reason == ReasonAdmissionStarved {
			return ClockNone, time.Time{}, 0, Edge{}
		}
		return ClockWork, t.Status.StageEnteredAt.Time, budget, elapse
	}

	// A POD stage. Which of the three is armed depends ENTIRELY on the stamps.
	switch {
	case t.Status.PodStartedAt == nil:
		if paused {
			return ClockNone, time.Time{}, 0, Edge{}
		}
		return ClockAdmission, t.Status.StageEnteredAt.Time, v1alpha1.AdmissionStarvedBudget,
			Edge{To: v1alpha1.StageParked, Reason: ReasonAdmissionStarved}
	case t.Status.StageWorkStartedAt == nil:
		return ClockReadiness, t.Status.PodStartedAt.Time, v1alpha1.PodReadyTimeout,
			Edge{To: Respawn}
	default:
		return ClockWork, t.Status.StageWorkStartedAt.Time, budget, elapse
	}
}

// Elapsed reports the edge to take when the armed clock has run out, if it has.
// An Edge.To of Respawn is NOT a transition: call Respawn. An Edge.To of Reap is
// NOT a transition either: the reaper deletes the Task.
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

// Respawn is the CLOCK 2 breach handler, and it mirrors the semantics of the
// existing boot machinery verbatim (handleBootCrash -> resetAgentRun,
// internal/controller/bootcrash.go:138-175): a never-Ready pod RESPAWNS, burning
// one podRecreations. It does NOT terminate the Task. The terminal, once the
// budget is spent, is failed(pod-recreation-exhausted) - pod-not-ready does not
// exist.
//
// RecordRespawn returns terminal=true (with the failed edge) once podRecreations exceeds
// maxPodRecreations; otherwise it returns Edge{To: Respawn} and the caller
// recreates the pod, re-stamping status.podStartedAt.
func RecordRespawn(t *v1alpha1.Task, maxPodRecreations int) (edge Edge, terminal bool) {
	t.Status.Stats.PodRecreations++
	if t.Status.Stats.PodRecreations > maxPodRecreations {
		return Edge{To: v1alpha1.StageFailed, Reason: ReasonPodRecreationExhausted}, true
	}
	return Edge{To: Respawn}, false
}

// BudgetExit is the set of exits EVERY pod-spawning stage carries ON TOP of its
// clocks (F.4). It returns no edge for a pod-less stage.
func BudgetExit(t *v1alpha1.Task, maxTurnsPerTask, maxPodRecreations int, podStoppedNoOutcome bool) (Edge, bool) {
	if v1alpha1.StagePodless(t.Status.Stage) {
		return Edge{}, false
	}
	if t.Status.Stats.Turns >= maxTurnsPerTask {
		return Edge{To: v1alpha1.StageFailed, Reason: ReasonTurnBudgetExhausted}, true
	}
	if t.Status.Stats.PodRecreations > maxPodRecreations {
		return Edge{To: v1alpha1.StageFailed, Reason: ReasonPodRecreationExhausted}, true
	}
	if podStoppedNoOutcome && t.Status.Stats.PodRecreations >= maxPodRecreations {
		return Edge{To: v1alpha1.StageParked, Reason: ReasonNoOutcome}, true
	}
	return Edge{}, false
}

// RequestChanges is the reviewing exit on submit_outcome(request_changes). On a
// kind=review Task it is parked(awaiting-human) - ALWAYS, unconditionally. That
// is the review agent's NORMAL verdict on a bad human PR, and it is the PRIMARY
// path v6 left open into an implement pod spawning against someone else's PR
// with no Issue, no ApprovalEvidence and no C.6 gate anywhere in its history
// (fix V7-1). The review IS posted. The human fixes their own PR.
//
// On any other kind it re-enters implementing, bounded by maxReviewRounds on the
// MR (cycle 1).
func RequestChanges(t *v1alpha1.Task, mrs []v1alpha1.MergeRequest, maxReviewRounds int) (Edge, bool) {
	if t.Spec.Kind == kindReview {
		return Edge{To: v1alpha1.StageParked, Reason: ReasonAwaitingHuman}, true
	}
	for i := range mrs {
		if mrs[i].Status.ReviewRounds >= maxReviewRounds {
			return Edge{To: v1alpha1.StageParked, Reason: ReasonReviewLoopExhausted}, true
		}
	}
	return Edge{To: v1alpha1.StageImplementing}, true
}

// ReenterImplementingOnReview re-enters implementing after a maintainer's
// changes_requested on a Tatara-owned, NOT-yet-merged MR. The caller has already
// verified the MR is not merged (the merged/finished boundary is the caller's,
// per the spec). It respects the F.3 table (only froms with an edge to
// implementing - reviewing, merging, approved, parked - succeed) and the
// kind=review guard (via Enter -> LegalFor). A terminal Task is never
// resurrected, and an already-implementing Task is a redundant no-op.
func ReenterImplementingOnReview(t *v1alpha1.Task, mrs []v1alpha1.MergeRequest, now time.Time) (ok bool) {
	if now.IsZero() {
		now = time.Now()
	}
	switch t.Status.Stage {
	case v1alpha1.StageRejected, v1alpha1.StageFailed, v1alpha1.StageDelivered, v1alpha1.StageImplementing:
		return false
	}
	if err := Enter(t, mrs, v1alpha1.StageImplementing, "", now); err != nil {
		return false
	}
	return true
}

// HeadMoved is the merging exit when the live head has moved off reviewedSHA (or
// Merge 409s "head moved"). It is CYCLE 4 (fix M3-9): the fourth cycle, the one
// v4 missed, and the ONLY one that SPAWNS A POD every lap. merging -> reviewing
// does NOT touch mergeReentries (only the PARKED path does), and reviewRounds
// increments only on request_changes, so a PR whose head keeps moving - a human
// pushing to the branch, a flapping CI autocommit - spun forever, burning a
// review pod on every lap, with no counter anywhere.
//
// It INCREMENTS status.headMoveReentries and caps at maxHeadMoveReentries ->
// failed(head-moving).
func HeadMoved(t *v1alpha1.Task, maxHeadMoveReentries int) (Edge, bool) {
	if t.Status.HeadMoveReentries >= maxHeadMoveReentries {
		return Edge{To: v1alpha1.StageFailed, Reason: ReasonHeadMoving}, true
	}
	t.Status.HeadMoveReentries++
	return Edge{To: v1alpha1.StageReviewing}, true
}

// UnparkInput is everything F.6 reads.
//
// internal/stage is PURE. The caller has ALREADY done every forge read and every
// grammar evaluation before it calls Unpark. In particular, for a
// parked(identity-unverified) Task with a non-bot pendingEvent, the caller MUST,
// in this order (fixes C3-3, M11):
//
//  1. SYNC THAT ISSUE'S COMMENTS FROM THE FORGE. The C.6 grammar's single-use-
//     evidence clause needs the approving comment IN THE MIRROR with its
//     ExternalID, and TaskEvent carries no externalId; B.4's mirror-cadence table
//     does not cover parked(identity-unverified) Tasks. Without the sync the
//     grammar re-runs against a thread that does not contain the comment that
//     triggered it, and silently fails.
//  2. RE-EVALUATE THE C.6 GRAMMAR against the refreshed thread, and on a pass
//     stamp Issue.status.approval and status=approved.
//
// GrammarPassed is that verdict. It is IGNORED for every other stageReason.
type UnparkInput struct {
	Task *v1alpha1.Task
	// Issues and MRs are the Issues / MergeRequests this Task OWNS. A review
	// Task owns ZERO Issues.
	Issues []v1alpha1.Issue
	MRs    []v1alpha1.MergeRequest
	// ActiveTasks is the count of ACTIVE (non-terminal) Tasks in the project;
	// MaxOpenTasks is Project.spec.maxOpenTasks. A promotion is NOT a mint, and
	// v3 checked the cap only at mint, so a maintainer's bulk comment pass would
	// promote 40 Tasks past a cap of 6.
	ActiveTasks  int
	MaxOpenTasks int
	// BotLogin is Project.spec.scm.botLogin. An event authored by it is a BOT
	// event and can never un-park anything: the operator's own park comment must
	// not un-park the Task it parked.
	BotLogin        string
	GrammarPassed   bool
	MaxTurnsPerTask int
	Now             time.Time
}

// Unpark is the F.6 re-entry function, and this is its entire body. A parked
// Task does not "resume": it either matches ONE of these narrow rules or it ages
// out at parkRetention and is reaped.
//
// On ok, the transition has ALREADY BEEN APPLIED through Enter - stage, reason,
// stageEnteredAt, podStartedAt = nil, stageWorkStartedAt = nil,
// podRecreations = 0, and the F.6 counter increments. The caller persists
// status; it never re-derives the target itself. On !ok the Task is UNTOUCHED
// and stays parked, and its pendingEvents are RETAINED, never dropped.
//
// The target is RE-DERIVED FROM STATE, NEVER from status.parkedFromStage (which
// is observability only).
func Unpark(in UnparkInput) (target string, ok bool) {
	t := in.Task
	now := in.Now
	if now.IsZero() {
		now = time.Now()
	}
	enter := func(to, reason string) (string, bool) {
		if err := Enter(t, in.MRs, to, reason, now); err != nil {
			// A guard refused it. LegalFor is what stops a kind=review Task
			// reaching implementing from the no-outcome rule, which F.6 itself
			// writes with no kind guard. It stays parked and ages out.
			return "", false
		}
		return to, true
	}

	switch t.Status.StageReason {

	case ReasonBacklogSweep:
		// NOT a park in the failure sense: this Task never ran. It exists to OWN
		// its Issue CRs at zero agent cost (B.4). It NEVER ages out; it is reaped
		// when its Issues close. A refine fold does NOT promote it - the fold
		// DELETES it (B.3).
		if !hasNonBotEvent(t, in.BotLogin) {
			return "", false
		}
		if in.ActiveTasks >= in.MaxOpenTasks {
			// OVER CAP: the promotion DEFERS. The Task stays parked and the
			// pendingEvent is RETAINED, never dropped. It promotes as soon as a
			// slot frees.
			return "", false
		}
		return enter(v1alpha1.StageTriaging, "")

	case ReasonAwaitingHuman:
		// The ONLY comment-driven re-entry.
		if !hasNonBotEvent(t, in.BotLogin) {
			return "", false
		}
		if t.Spec.Kind == kindReview {
			// A review-kind Task may NEVER enter implementing or merging. There
			// is no path, no condition, no exception. It does not exist.
			//
			// humanReviewRounds is a NEW counter and it is NOT mr.reviewRounds,
			// which increments only on request_changes: on the approve path that
			// bound did not exist, and this spawned ONE REVIEW POD PER HUMAN
			// COMMENT, capped only by maxTurnsPerTask (300).
			if t.Status.HumanReviewRounds >= v1alpha1.MaxHumanReviewRounds {
				return "", false // STAY PARKED. Do not spawn another review pod.
			}
			t.Status.HumanReviewRounds++
			target, ok := enter(v1alpha1.StageReviewing, "")
			if !ok {
				t.Status.HumanReviewRounds--
			}
			return target, ok
		}
		// THE EMPTY SET IS NOT A LICENCE (fix V6-3). A review Task owns ZERO
		// Issues and all([]) == true, so v5's "if EVERY owned Issue is approved
		// -> implementing" promoted it straight into an implement pod against
		// someone else's PR on ANY human comment. And it looped. An empty owned-
		// Issue set must never satisfy a universal quantifier that gates code
		// execution.
		open := openIssues(in.Issues)
		if len(open) == 0 {
			return "", false
		}
		if allApproved(open) {
			return enter(v1alpha1.StageImplementing, "")
		}
		return enter(v1alpha1.StageClarifying, "")

	case ReasonIdentityUnverified:
		// A comment ALONE cannot un-park this - the one park reason sitting
		// directly in front of "write code and merge it to prod". Only a comment
		// that PASSES THE C.6 GRAMMAR can: maintainer identity, anchored
		// whole-line phrase, single-use evidence, re-evaluated by the OPERATOR
		// against a freshly SYNCED thread (see UnparkInput). On a FAIL it stays
		// parked and the bot does NOT comment again.
		if !hasNonBotEvent(t, in.BotLogin) {
			return "", false
		}
		if !in.GrammarPassed {
			return "", false
		}
		open := openIssues(in.Issues)
		if len(open) == 0 || !allApproved(open) {
			// H9: implementing needs EVERY owned Issue approved, and the empty
			// set is not a licence here either.
			return "", false
		}
		return enter(v1alpha1.StageImplementing, "")

	case ReasonMergeTimeout:
		if t.Status.MergeReentries >= v1alpha1.MaxMergeReentries {
			return enter(v1alpha1.StageFailed, ReasonMergeBlocked)
		}
		t.Status.MergeReentries++
		// Idempotent: mergeCursor resumes and EVERY MR is re-validated against
		// state=merged before any Merge call. NEVER implementing - that would
		// recreate deleted branches and re-propose already-merged code.
		target, ok := enter(v1alpha1.StageMerging, "")
		if !ok {
			t.Status.MergeReentries--
		}
		return target, ok

	case ReasonDeployTimeout:
		if t.Status.DeployReentries >= v1alpha1.MaxDeployReentries {
			return enter(v1alpha1.StageFailed, ReasonDeployBlocked)
		}
		t.Status.DeployReentries++
		// Idempotent: per-MR deployedAt re-check. NEVER implementing.
		target, ok := enter(v1alpha1.StageDeploying, "")
		if !ok {
			t.Status.DeployReentries--
		}
		return target, ok

	case ReasonNoOutcome:
		if anyMerged(in.MRs) {
			// A re-implement would duplicate an already-merged change.
			return "", false
		}
		if t.Status.Stats.Turns >= in.MaxTurnsPerTask {
			return "", false
		}
		return enter(v1alpha1.StageImplementing, "")

	default:
		// review-loop-exhausted, implement-declined, declined, false-positive,
		// stage-deadline,
		// admission-starved, turn-budget-exhausted, pod-recreation-exhausted,
		// fold-adoption-unverified, doc-timeout, operator-error, triage-stalled,
		// name-too-long, review-post-refused, object-too-large,
		// merge-order-missing, agent-contract-mismatch, merge-blocked,
		// deploy-blocked, head-moving, handoff-stalled.
		//
		// NO re-entry. It ages out at parkRetention and is reaped, AFTER the
		// operator posts its bot park comment. The next sweep re-mints the
		// still-open issue as a parked(backlog-sweep) Task, which OWNS it and
		// costs nothing. If a human then comments, THAT Task promotes - as a NEW
		// Task, not a zombie one.
		return "", false
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

func allApproved(issues []v1alpha1.Issue) bool {
	for i := range issues {
		if issues[i].Status.Status != "approved" {
			return false
		}
	}
	return true
}

func anyMerged(mrs []v1alpha1.MergeRequest) bool {
	for i := range mrs {
		if mrs[i].Status.State == "merged" || mrs[i].Status.MergedAt != nil {
			return true
		}
	}
	return false
}
