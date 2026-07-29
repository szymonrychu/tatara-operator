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
	ReasonTrackedElsewhere,
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
	ReasonIssueClosed,
	ReasonOwnershipLost,
	ReasonMRMergedExternally,
	ReasonMRClosedExternally,
	ReasonMergeAuthRefused,
	ReasonMRTakenOver,
	ReasonCIRed,
	ReasonCIBlocked,
}

// issueClosedTrigger is the F.3 prose for a WS3-I3 rejected(issue-closed) edge.
const issueClosedTrigger = "a human closed the driving issue mid-flight (WS3-I3): the operator stops the work and the terminal reaper closes the bot PR"

// issueClosedEdge is the rejected(issue-closed) edge added to the ten LIVE,
// non-deploying source stages (WS3-I3). deploying is EXCLUDED on purpose: a
// merged, deploying change is finished work and a late issue close must not
// rewind it (the same boundary the review path enforces). documenting is a
// nightly BATCH with no driving issue to close, so it is excluded too.
func issueClosedEdge() Edge {
	return Edge{To: v1alpha1.StageRejected, Reason: ReasonIssueClosed, Trigger: issueClosedTrigger}
}

// issueClosedStopStages is the closed set of LIVE stages from which a
// human-closed driving issue stops the Task at rejected(issue-closed) (WS3-I3).
// It is the SAME ten stages that carry the edge; ApplyIssueClosedStop reads it
// so the webhook->leader gate and the F.3 table cannot drift apart.
var issueClosedStopStages = map[string]bool{
	v1alpha1.StageTriaging:      true,
	v1alpha1.StageBrainstorming: true,
	v1alpha1.StageClarifying:    true,
	v1alpha1.StageInvestigating: true,
	v1alpha1.StageRefining:      true,
	v1alpha1.StageApproved:      true,
	v1alpha1.StageImplementing:  true,
	v1alpha1.StageReviewing:     true,
	v1alpha1.StageConversing:    true,
	v1alpha1.StageMerging:       true,
}

// AllowsIssueClosedStop reports whether a Task in `stg` may be stopped at
// rejected(issue-closed) by a human closing its driving issue (WS3-I3). It is
// false for deploying (merged work is not rewound), documenting, and every
// terminal/quasi-terminal stage.
func AllowsIssueClosedStop(stg string) bool { return issueClosedStopStages[stg] }

var reasonSet = func() map[string]bool {
	m := make(map[string]bool, len(Reasons))
	for _, r := range Reasons {
		m[r] = true
	}
	return m
}()

// ValidReason reports whether r is a member of the F.5 closed set.
func ValidReason(r string) bool { return reasonSet[r] }

// reentryReasons is the set of PARKED reasons stage.Unpark can re-enter from
// (comment- or time-driven F.6 rules). A parked Task carrying ANY OTHER reason is
// a genuine dead-end with no F.6 re-entry - which is what WS3-I4 acts on: a human
// reply to such a park triggers a fresh gated re-mint, never a smuggled re-entry.
var reentryReasons = map[string]bool{
	ReasonBacklogSweep:       true,
	ReasonAwaitingHuman:      true,
	ReasonIdentityUnverified: true,
	ReasonMergeTimeout:       true,
	ReasonDeployTimeout:      true,
	ReasonNoOutcome:          true,
	ReasonHandoffStalled:     true,
}

// HasReentry reports whether a parked `reason` has an F.6 re-entry rule. It is the
// WS3-I4 discriminator: a parked Task whose reason has NO re-entry is a dead-end a
// human reply resumes through a fresh clarify mint, not an Unpark.
func HasReentry(reason string) bool { return reentryReasons[reason] }

// Edge is one row of the F.3 transition table. To is a stage, or one of the
// pseudo-targets Reap / Respawn. Reason is the stage reason stamped on To
// (empty when To carries none). Trigger is the contract's own prose.
type Edge struct {
	To      string
	Reason  string
	Trigger string
}

// AllStages returns the 16 members of the F.1 enum. Iteration order is the
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
		v1alpha1.StageConversing,
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
//
// conversing maps to AgentClarify deliberately and it is NOT a placeholder: the
// clarify agent is the platform's conversational agent and its outcome
// vocabulary (implement / discuss / close) IS the "where does this conversation
// go next" decision. An eighth agent kind would need matching tool and skill
// profiles in tatara-cli, tatara-agent-skills and the wrapper's installer, none
// of which this change touches.
var agentKinds = map[string]string{
	v1alpha1.StageTriaging:      "",
	v1alpha1.StageBrainstorming: AgentBrainstorm,
	v1alpha1.StageClarifying:    AgentClarify,
	v1alpha1.StageInvestigating: AgentIncident,
	v1alpha1.StageRefining:      AgentRefine,
	v1alpha1.StageApproved:      "",
	v1alpha1.StageImplementing:  AgentImplement,
	v1alpha1.StageReviewing:     AgentReview,
	v1alpha1.StageConversing:    AgentClarify,
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
		{To: v1alpha1.StageApproved, Trigger: "a maintainer-gated takeover mints a full-lifecycle Task bound to an existing MR (MR ownership design)"},
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
		issueClosedEdge(),
	},

	v1alpha1.StageBrainstorming: podStageEdges(
		Edge{To: v1alpha1.StageDelivered, Trigger: "submit_outcome(propose|skip). documentedBy stays empty: no docs Task is spawned (fix 25)"},
		issueClosedEdge(),
	),

	v1alpha1.StageClarifying: podStageEdges(
		Edge{To: v1alpha1.StageApproved, Trigger: "submit_outcome(decision=implement) AND the agent's CITATION verifies for EVERY LIVE owned Issue"},
		Edge{To: v1alpha1.StageParked, Reason: ReasonIdentityUnverified, Trigger: "decision=implement but the citation does NOT verify (uncited, not a maintainer's, quote absent, or evidence replayed)"},
		Edge{To: v1alpha1.StageParked, Reason: ReasonAwaitingHuman, Trigger: "decision=discuss, or the 24h clarify budget elapses"},
		Edge{To: v1alpha1.StageRejected, Reason: ReasonDeclined, Trigger: "decision=close (the operator closes the issue)"},
		Edge{To: v1alpha1.StageConversing, Trigger: "a qualifying comment arrived on an owned thread: the Task moves to a live conversation. The clarify pod of the stage being left is torn down by EnterStage as for any pod stage; the conversing pod's turn-0 bundle carries the whole thread, the notes journal and <events>, and every SUBSEQUENT comment reaches that live pod warm"},
		issueClosedEdge(),
	),

	v1alpha1.StageInvestigating: podStageEdges(
		Edge{To: v1alpha1.StageClarifying, Trigger: "submit_outcome(file_issue): the tracker Issue is created under THIS Task"},
		Edge{To: v1alpha1.StageRejected, Reason: ReasonFalsePositive, Trigger: "submit_outcome(false_positive). No docs Task (fix 25)"},
		issueClosedEdge(),
	),

	v1alpha1.StageRefining: podStageEdges(
		Edge{To: v1alpha1.StageDelivered, Trigger: "folds/closes/links applied AND the fold VERIFIED (B.3)"},
		Edge{To: v1alpha1.StageFailed, Reason: ReasonFoldAdoptionUnverified, Trigger: "B.3 step-3 verification fails"},
		issueClosedEdge(),
	),

	// approved is POD-LESS (the admission gate). Its own 24h budget elapses to
	// parked(admission-starved), which is exactly clock 1 by another name - and
	// that is why the paused-project carve-out covers it too.
	v1alpha1.StageApproved: {
		{To: v1alpha1.StageImplementing, Trigger: "a QueuedEvent for the implement pod is ADMITTED"},
		{To: v1alpha1.StageClarifying, Trigger: "the Task ACQUIRES a new Issue after approval (fix H9). LEGAL BUT UNDRIVEN: applyApprovalStage was the only production caller and it died with the agent-judged approval gate, so nothing re-gates an approved Task today. If it is re-implemented the home is queue_controller.go, not restapi"},
		{To: v1alpha1.StageParked, Reason: ReasonAdmissionStarved, Trigger: "the 24h admission budget elapses (skipped when the project is PAUSED)"},
		{To: v1alpha1.StageParked, Reason: ReasonOwnershipLost, Trigger: "an external commit landed on the MR while approved: a takeover Task mints straight into approved already controller-owning the MR (MintOrUnparkTakeoverTask), so it can be flipped before ever reaching implementing"},
		{To: v1alpha1.StageFailed, Reason: ReasonOperatorError, Trigger: "unrecoverable operator error"},
		{To: v1alpha1.StageFailed, Reason: ReasonObjectTooLarge, Trigger: "the A.7 byte-budget pre-write guard refuses"},
		issueClosedEdge(),
	},

	v1alpha1.StageImplementing: podStageEdges(
		Edge{To: v1alpha1.StageReviewing, Trigger: "submit_outcome(submitted) and >= 1 owned MR is open"},
		Edge{To: v1alpha1.StageParked, Reason: ReasonImplementDeclined, Trigger: "submit_outcome(declined)"},
		Edge{To: v1alpha1.StageParked, Reason: ReasonOwnershipLost, Trigger: "an external commit landed on this MR while implementing: ownership flipped to external"},
		issueClosedEdge(),
	),

	v1alpha1.StageReviewing: podStageEdges(
		// BOTH of these exist ONLY for spec.kind != "review". The guard is in
		// LegalFor, which Enter uses, so it is structurally impossible to bypass.
		Edge{To: v1alpha1.StageImplementing, Trigger: "submit_outcome(request_changes) AND spec.kind != review AND reviewRounds < maxReviewRounds. Gated on pendingReview == nil"},
		Edge{To: v1alpha1.StageMerging, Trigger: "submit_outcome(approve) AND spec.kind != review. Gated on pendingReview == nil, and on the LIVE CI at the reviewed head not being red (issue #476)"},
		Edge{To: v1alpha1.StageImplementing, Reason: ReasonCIRed, Trigger: "an approve whose LIVE CI at the reviewed head has FAILED (issue #476). The approve is real and stays on the forge; the change simply cannot merge, so it goes back to the agent that can fix it instead of being promoted into a pod-less stage whose only exit was the 4h budget. Bounded by maxCIRedReentries"},
		Edge{To: v1alpha1.StageParked, Reason: ReasonCIRed, Trigger: "red CI while part of spec.mergeOrder has already merged: re-implementing would re-propose merged code and recreate deleted branches, so it is a human's call"},
		Edge{To: v1alpha1.StageFailed, Reason: ReasonCIBlocked, Trigger: "ciRedReentries at maxCIRedReentries"},
		Edge{To: v1alpha1.StageParked, Reason: ReasonAwaitingHuman, Trigger: "submit_outcome(approve|request_changes) on a kind=review Task. The review IS posted. A human's PR is fixed and merged by the human (fixes V7-1, C3-2)"},
		Edge{To: v1alpha1.StageParked, Reason: ReasonReviewLoopExhausted, Trigger: "request_changes at maxReviewRounds, on a non-review Task"},
		Edge{To: v1alpha1.StageParked, Reason: ReasonReviewPostRefused, Trigger: "a structural 4xx from PostReview (fix C1)"},
		Edge{To: v1alpha1.StageParked, Reason: ReasonHandoffStalled, Trigger: "the outcome COMMITTED but the C.5.3 phase-2 drain (DrainPendingReview -> advanceAfterReview) never advanced the Task within HandoffDeadline (5m), and the reconciler's level-triggered re-drive could not either. ONLY reviewing carries it: every other kind's commit calls stage.Enter in the SAME write, so no other stage can be committed-but-not-advanced. Recoverable: a human comment re-enters reviewing (F.6)"},
		Edge{To: v1alpha1.StageParked, Reason: ReasonOwnershipLost, Trigger: "an external commit landed on this MR while reviewing: ownership flipped to external"},
		issueClosedEdge(),
		Edge{To: v1alpha1.StageDelivered, Reason: ReasonMRMergedExternally, Trigger: "kind=review Task, every owned MR merged externally before/while reviewing - no open MR to post an outcome against, so the operator finalizes the honest finished work"},
		Edge{To: v1alpha1.StageRejected, Reason: ReasonMRClosedExternally, Trigger: "kind=review Task, every owned MR terminal with at least one closed-unmerged: the review target was abandoned, recorded rejected not delivered"},
		Edge{To: v1alpha1.StageRejected, Reason: ReasonMRTakenOver, Trigger: "kind=review Task whose review target a maintainer TOOK OVER: the MR's controller ownership moved to a takeover Task (own.HandOverController demoted this parent to a plain owner), leaving it controller-owning zero MRs with no outcome to post; it is retired and the takeover Task now owns delivery. NOT delivered: the takeover Task itself reaches delivered for the MR, so the superseded parent must not double-count the one shipped MR as two deliveries. reviewing -> rejected is ungated in LegalFor, unlike reviewing -> delivered which requires AllMRsMerged(ownedMRs) and this parent owns none"},
		Edge{To: v1alpha1.StageConversing, Trigger: "a qualifying comment arrived on the owned MR thread: the Task moves to a live conversation. Bounded by humanReviewRounds, exactly like the awaiting-human re-entry, because each lap can spawn a pod"},
	),

	// conversing is POD-BEARING and NON-TERMINAL. It is where a Task waits with a
	// LIVE agent on the other end of a thread, instead of parked with none. Its
	// clock is the IDLE timer on status.conversationLastEventAt, and its
	// budget-elapsed exit (onElapse) is parked(awaiting-human) after a handoff
	// turn - a conversation that goes quiet becomes an ordinary park, and an
	// ordinary park is re-entered by the ordinary F.6 rules.
	v1alpha1.StageConversing: podStageEdges(
		Edge{To: v1alpha1.StageApproved, Trigger: "submit_outcome(decision=implement) AND verifyApprovalScope's LIVE citation check passes for EVERY LIVE owned Issue, exactly as from clarifying. Refused by LegalFor's GUARD 1 for a kind=review Task (a review Task owns zero Issues, so the citation check can never pass for it anyway; the guard makes that refusal structural rather than merely likely)"},
		Edge{To: v1alpha1.StageParked, Reason: ReasonIdentityUnverified, Trigger: "decision=implement but the citation does NOT verify, exactly as from clarifying"},
		Edge{To: v1alpha1.StageParked, Reason: ReasonAwaitingHuman, Trigger: "the conversation went idle past conversationIdleMinutes (after a handoff turn), the per-project conversing ceiling evicted the longest-idle conversation (after a handoff turn), or decision=discuss"},
		Edge{To: v1alpha1.StageRejected, Reason: ReasonDeclined, Trigger: "decision=close (the operator closes the issue)"},
		Edge{To: v1alpha1.StageReviewing, Trigger: "the conversation was entered FROM reviewing and the agent hands the thread back for a fresh review round. LegalFor's kind guard is untouched: implementing, merging AND approved remain unreachable for a kind=review Task, by any path"},
		issueClosedEdge(),
	),

	// merging is POD-LESS: clock 3 ONLY, from stageEnteredAt, against ITS OWN 4h
	// budget. It gets NO 24h admission-starved clock (fix V7-8) - if it did, it
	// could never reach merge-timeout and the bounded merge re-entry cycle would
	// never engage at all.
	v1alpha1.StageMerging: {
		{To: v1alpha1.StageDeploying, Trigger: "every repo in mergeOrder merged, in order, each on green CI"},
		{To: v1alpha1.StageReviewing, Trigger: "a live head != reviewedSHA, or Merge 409s head-moved. INCREMENTS status.headMoveReentries (the FOURTH cycle, fix M3-9)"},
		{To: v1alpha1.StageImplementing, Trigger: "a maintainer requested changes on the still-open MR before it merged (F.6-adjacent). kind=review refused by LegalFor"},
		{To: v1alpha1.StageImplementing, Reason: ReasonCIRed, Trigger: "the LIVE CI at the reviewed head has FAILED (issue #476). A failed check is DETERMINISTIC - polling it every 60s until the 4h budget elapses re-discovers the same verdict 240 times - so the Task leaves merging immediately for the agent that can fix it. Bounded by maxCIRedReentries"},
		{To: v1alpha1.StageParked, Reason: ReasonCIRed, Trigger: "red CI while an earlier repo in spec.mergeOrder has already merged: re-implementing would re-propose merged code and recreate deleted branches (the same boundary the merge-timeout un-park refuses to cross), so it is a human's call"},
		{To: v1alpha1.StageFailed, Reason: ReasonCIBlocked, Trigger: "ciRedReentries at maxCIRedReentries"},
		{To: v1alpha1.StageFailed, Reason: ReasonHeadMoving, Trigger: "headMoveReentries at maxHeadMoveReentries"},
		{To: v1alpha1.StageFailed, Reason: ReasonMergeBlocked, Trigger: "mergeReentries at maxMergeReentries (fix H7)"},
		{To: v1alpha1.StageFailed, Reason: ReasonMergeOrderMissing, Trigger: "len(spec.mergeOrder) == 0 on entry (bug-catcher)"},
		{To: v1alpha1.StageFailed, Reason: ReasonOperatorError, Trigger: "unrecoverable operator error"},
		{To: v1alpha1.StageFailed, Reason: ReasonObjectTooLarge, Trigger: "the A.7 byte-budget pre-write guard refuses"},
		{To: v1alpha1.StageParked, Reason: ReasonMergeTimeout, Trigger: "the 4h merging budget elapses"},
		{To: v1alpha1.StageParked, Reason: ReasonOwnershipLost, Trigger: "an external commit landed on the MR while merging: the controller-owning Task (takeover or normal) can still be mid-merge when a further unattributable push races it"},
		{To: v1alpha1.StageParked, Reason: ReasonMergeAuthRefused, Trigger: "the forge returned 401/403 (invalid/insufficient credential) on Merge (#404); a bad token never fixes itself on retry, so this fails fast instead of hot-requeueing until merge-timeout"},
		issueClosedEdge(),
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
		{To: v1alpha1.StageReviewing, Trigger: "F.6 awaiting-human on a kind=review Task / handoff-stalled on a human comment (any kind). Both bounded by humanReviewRounds (5)"},
		{To: v1alpha1.StageImplementing, Trigger: "F.6 awaiting-human (every open owned Issue approved) / no-outcome (zero merged MRs). NOT identity-unverified: that reason's only F.6 target is conversing, because F.6 does no approval reasoning at all"},
		{To: v1alpha1.StageClarifying, Trigger: "F.6 awaiting-human, not every open owned Issue is approved"},
		{To: v1alpha1.StageMerging, Trigger: "F.6 merge-timeout, under maxMergeReentries. NEVER implementing"},
		{To: v1alpha1.StageDeploying, Trigger: "F.6 deploy-timeout, under maxDeployReentries. NEVER implementing"},
		{To: v1alpha1.StageFailed, Reason: ReasonMergeBlocked, Trigger: "F.6 merge-timeout at maxMergeReentries"},
		{To: v1alpha1.StageFailed, Reason: ReasonDeployBlocked, Trigger: "F.6 deploy-timeout at maxDeployReentries"},
		{To: v1alpha1.StageApproved, Reason: ReasonOwnershipLost, Trigger: "a maintainer re-took ownership via a takeover comment; the parked takeover Task re-enters the lifecycle at approved to resume pushing"},
		{To: v1alpha1.StageMerging, Reason: ReasonOwnershipLost, Trigger: "an approved review landed on a stood-down (external-push) MR; the parked takeover Task re-enters at merging so the operator merges the approved human head"},
		{To: v1alpha1.StageConversing, Trigger: "F.6 awaiting-human / identity-unverified: a qualifying comment arrived and the conversing ceiling has room. This is the un-park that makes a comment visibly start an agent instead of only queueing an event"},
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
// GUARD 1 (fixes V7-1, V6-3, C3-2; widened 2026-07-28 security review IMPORTANT
// 3). A kind=review Task may NEVER enter implementing, merging, or approved.
// Not from reviewing on request_changes (the review agent's NORMAL verdict on a
// bad human PR - the PRIMARY path v6 missed), not from reviewing on approve,
// not from parked by any un-park rule, not from conversing (Task 9: a
// kind=review Task reaches conversing via reviewing -> conversing, and its
// clarify agent can submit decision=implement same as any other conversing
// Task), not from anywhere. approved is included because it is the admission
// GATE to implementing, not a destination in its own right for a Task that can
// never be admitted: without this a kind=review Task stuck submitting
// decision=implement would reach approved and sit there - unable to advance
// (implementing is still blocked) and unable to un-park on its own - until the
// 24h admission-starved budget elapses, a wedge rather than a crash but still a
// wasted Task no comment can recover. There is no author check to get wrong
// because the sweep ignores bot-authored non-adoptable PRs, so EVERY review
// Task is non-bot-authored by construction. Merging or fixing a human's PR is a
// HUMAN action.
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
		(to == v1alpha1.StageImplementing || to == v1alpha1.StageMerging || to == v1alpha1.StageApproved) {
		return false
	}
	if from == v1alpha1.StageReviewing &&
		(to == v1alpha1.StageImplementing || to == v1alpha1.StageMerging) &&
		!reviewGateOpen(mrs) {
		return false
	}
	// reviewing -> delivered is the kind=review external-merge finalize. It is a
	// brand-new pair, so gating it here is safe (no other edge uses it). NOTE:
	// reviewing -> rejected is deliberately NOT gated - it is shared with the
	// issue-closed stop (issue_apply.go enters it with mrs == nil), and the
	// mr-closed-externally terminal predicate is enforced by terminalMREdge, its
	// only caller.
	if from == v1alpha1.StageReviewing && to == v1alpha1.StageDelivered {
		if t == nil || t.Spec.Kind != kindReview || !AllMRsMerged(mrs) {
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
//
// ENTRY INTO CONVERSING ALSO ARMS THE IDLE CLOCK
// (status.conversationLastEventAt = now), here rather than at each of
// conversing's several entry call sites (controller.EnterConversing for the
// live clarifying/reviewing edge, stage.UnparkDetailed's pure enter() closure
// for the parked(awaiting-human)/parked(identity-unverified) edges). Enter is
// the ONE choke point every one of them already goes through, so this is the
// one place a CALLER THAT USES Enter's OWN OUTPUT cannot forget it -
// AppendTaskEvent stamps the SAME field on every queued event and usually
// runs moments earlier in the same request, but a caller must never depend on
// that ordering: an unarmed clock means ArmedClock returns ClockNone and the
// conversation holds its concurrency slot forever, since there is
// deliberately no absolute lifetime ceiling (decision D6). It is NOT a
// guarantee against a caller that hand-copies Enter's RESULT field-by-field
// instead of using the mutated Task directly (queue_controller.go's admission
// write does exactly this, for a different edge, and must copy this field
// too - it does, but that is a second place, not this one, and the next such
// mirror must remember it independently).
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
	if to == v1alpha1.StageConversing {
		t.Status.ConversationLastEventAt = &stamp
	}
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
	v1alpha1.StageConversing:    v1alpha1.ConversationIdleDefault,
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
	v1alpha1.StageConversing:    {To: v1alpha1.StageParked, Reason: ReasonAwaitingHuman},
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

	// THE IDLE CLOCK (conversing). A named case, not an inference. conversing is a
	// POD stage, so the generic selector below would arm clock 1 or clock 2 or
	// measure clock 3 from stageWorkStartedAt - all three of which describe pod
	// age, and none of which describes how long the human has been silent. The
	// budget here is the table default; reconcileClocks substitutes the project's
	// scm.conversationIdleMinutes, which is the only per-project knob in the F.4
	// clock model and is why the substitution lives at the caller rather than in
	// this pure package.
	if stg == v1alpha1.StageConversing {
		if t.Status.ConversationLastEventAt == nil {
			return ClockNone, time.Time{}, 0, Edge{}
		}
		elapse, ok := OnElapse(stg)
		if !ok {
			return ClockNone, time.Time{}, 0, Edge{}
		}
		return ClockWork, t.Status.ConversationLastEventAt.Time, budget, elapse
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

// ReenterOnReviewChangesRequested routes a maintainer's changes_requested on a
// Tatara-owned, NOT-yet-merged MR back onto the stage machine. The caller has
// already verified no owned MR is merged (the merged/finished boundary is the
// caller's, per the spec) and the actor is a maintainer.
//
// A NON-parked reviewing/merging Task re-enters implementing: the maintainer
// wants code changes, so this is a fresh implementation cycle and it gets a
// fresh merge/head budget (F3). Every OTHER non-parked stage folds: rejected/
// failed/delivered are never resurrected, an already-implementing Task is a
// redundant no-op, and earlier stages (clarifying/brainstorming/approved) have
// no re-entry edge because that would bypass the #294 approval gate.
//
// A PARKED Task is routed by StageReason, mirroring Unpark EXACTLY (the target is
// derived from the reason, never from status.parkedFromStage, which the machine
// treats as observability only):
//   - merge-timeout -> merging with MergeReentries accounting; NEVER implementing
//     (that would recreate deleted branches). A real code change from the
//     maintainer moves the head and the merging head-moved bounce re-reviews it.
//   - no-outcome -> implementing, behind Unpark's own guards (no merged MR, turns
//     under the lifetime cap) so it declines instead of bouncing straight into
//     failed(turn-budget-exhausted) (F4).
//   - every other reason folds (ok=false). awaiting-human / identity-unverified /
//     backlog-sweep are resumed by the webhook's pending-event path
//     (driveCommentUnpark) and by the project reconcile's driveUnparks sweep;
//     for identity-unverified BOTH lead to conversing or a decline and never to
//     implementing, because F.6 does no approval reasoning. The exhaustion
//     reasons (review-loop-exhausted,
//     stage-deadline, ...) age out - re-entering them here would escape their own
//     cap one human review at a time (F1).
//
// The kind=review guard (Enter -> LegalFor) still refuses implementing/merging
// for a kind=review Task from anywhere, so an adopted human PR is never driven.
func ReenterOnReviewChangesRequested(t *v1alpha1.Task, mrs []v1alpha1.MergeRequest, maxTurnsPerTask int, now time.Time) (ok bool) {
	if now.IsZero() {
		now = time.Now()
	}
	switch t.Status.Stage {
	case v1alpha1.StageReviewing, v1alpha1.StageMerging:
		return enterFreshImplementing(t, mrs, now)
	case v1alpha1.StageParked:
		return reenterParkedOnReview(t, mrs, maxTurnsPerTask, now)
	default:
		// rejected/failed/delivered (never resurrected), implementing (redundant),
		// and every earlier stage (no re-entry edge; #294). Fold.
		return false
	}
}

// enterFreshImplementing applies the reviewing|merging|parked -> implementing
// edge for a maintainer-driven fresh implementation, and on success zeroes the
// merge, head-move and red-CI budgets (F3): a fresh implementation deserves a fresh merge
// budget, and this reset is human-gated (one maintainer changes_requested per
// reset), so it is NOT the automatic HeadMoved bounce and cannot spin a head-move
// loop on its own.
func enterFreshImplementing(t *v1alpha1.Task, mrs []v1alpha1.MergeRequest, now time.Time) bool {
	if err := Enter(t, mrs, v1alpha1.StageImplementing, "", now); err != nil {
		return false
	}
	t.Status.HeadMoveReentries = 0
	t.Status.MergeReentries = 0
	t.Status.CIRedReentries = 0
	return true
}

// reenterParkedOnReview is the parked branch of ReenterOnReviewChangesRequested,
// routed by StageReason to mirror Unpark.
func reenterParkedOnReview(t *v1alpha1.Task, mrs []v1alpha1.MergeRequest, maxTurnsPerTask int, now time.Time) bool {
	switch t.Status.StageReason {
	case ReasonMergeTimeout:
		if t.Status.MergeReentries >= v1alpha1.MaxMergeReentries {
			// Budget spent. Fold rather than replicate Unpark's failed(merge-blocked)
			// HERE: this only keeps the REVIEW path from terminating the Task. The Task
			// is NOT immortal - the periodic driveUnparks -> Unpark still drives
			// parked(merge-timeout)-at-cap to failed(merge-blocked) on its own cadence,
			// independent of any human review. Duplicating that terminal on the review
			// path would just race it.
			return false
		}
		t.Status.MergeReentries++
		if err := Enter(t, mrs, v1alpha1.StageMerging, "", now); err != nil {
			t.Status.MergeReentries--
			return false
		}
		return true
	case ReasonNoOutcome:
		if anyMerged(mrs) {
			return false // a re-implement would duplicate an already-merged change
		}
		if t.Status.Stats.Turns >= maxTurnsPerTask {
			return false // would bounce straight into failed(turn-budget-exhausted)
		}
		return enterFreshImplementing(t, mrs, now)
	default:
		return false
	}
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

// CIRed is the RED-CI exit (issue #476), and it is CYCLE 5. It is taken from
// BOTH sides of the promotion it guards:
//
//   - reviewing, instead of advancing an approved-but-red change into merging;
//   - merging, instead of re-polling a red required check every 60s until the 4h
//     budget parks it (and then un-parks straight back into merging, up to
//     maxMergeReentries, for ~16h of re-testing one deterministic verdict).
//
// A failed check on the head that was reviewed is DETERMINISTIC: no amount of
// waiting turns it green, only a new commit does. The Task therefore goes back
// to implementing, where an agent can fix it - which is what the platform would
// have done had the review verdict itself carried the CI state.
//
// It INCREMENTS status.ciRedReentries and caps at maxCIRedReentries ->
// failed(ci-blocked), for the same reason every other bounce is capped: each lap
// spawns pods.
//
// TWO refusals sit in front of the re-implement:
//
//   - kind=review: fixing a human's PR is a HUMAN action, so it parks
//     awaiting-human exactly like every other kind=review verdict. LegalFor would
//     refuse implementing anyway; this keeps that refusal from surfacing as an
//     error-loop instead of an edge.
//   - anyMerged(mrs): part of spec.mergeOrder has already landed, and
//     re-implementing would re-propose merged code and recreate deleted branches
//   - the same boundary reenterParkedOnReview refuses to cross. It parks
//     ci-red, which has no F.6 re-entry: a human decides.
func CIRed(t *v1alpha1.Task, mrs []v1alpha1.MergeRequest, maxCIRedReentries int) (Edge, bool) {
	if t.Spec.Kind == kindReview {
		return Edge{To: v1alpha1.StageParked, Reason: ReasonAwaitingHuman}, true
	}
	if anyMerged(mrs) {
		return Edge{To: v1alpha1.StageParked, Reason: ReasonCIRed}, true
	}
	if t.Status.CIRedReentries >= maxCIRedReentries {
		return Edge{To: v1alpha1.StageFailed, Reason: ReasonCIBlocked}, true
	}
	t.Status.CIRedReentries++
	return Edge{To: v1alpha1.StageImplementing, Reason: ReasonCIRed}, true
}

// UnparkInput is everything F.6 reads.
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
// submit_outcome(decision=implement). Nothing this function returns can grant
// implementing or approved from that park reason.
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
	MaxTurnsPerTask int
	// ConversingHasRoom is the CALLER's answer to "is this project under its
	// conversing ceiling right now". internal/stage is pure and cannot count live
	// Tasks, so the caller computes it once per pass. False is always SAFE: every
	// rule that would have entered conversing falls back to exactly the target it
	// had before this stage existed, so a full ceiling degrades responsiveness and
	// never drops the event.
	ConversingHasRoom bool
	Now               time.Time
}

// The CLOSED decline vocabulary. UnparkDetailed returns exactly one of these,
// and the caller puts it on operator_unpark_declined_total's `kind` label, so
// the set must stay small and stable. A decline that cannot name its condition
// is the defect this vocabulary exists to make impossible: the 2026-07-27
// identity-unverified stall declined silently and no log or counter anywhere
// could say which of the several conditions that arm then checked had refused.
const (
	// DeclineNone means Unpark re-entered (target != "").
	DeclineNone = ""
	// DeclineNoHumanEvent: the rule needs a non-bot pendingEvent and there is none.
	DeclineNoHumanEvent = "no-human-event"
	// DeclineOverCap: backlog-sweep promotion would exceed maxOpenTasks. The
	// promotion DEFERS; the event is retained.
	DeclineOverCap = "over-cap"
	// DeclineNoConversingRoom: identity-unverified with a qualifying human
	// comment, but the project is at its conversing ceiling. The pendingEvent is
	// RETAINED and the next pass retries; nothing is dropped.
	DeclineNoConversingRoom = "no-conversing-room"
	// DeclineNoOpenIssues: the empty owned-Issue set is not a licence (fix V6-3).
	DeclineNoOpenIssues = "no-open-issues"
	// DeclineMergedMR: re-entry would spawn a pod against an already-merged MR.
	DeclineMergedMR = "merged-mr"
	// DeclineRoundsExhausted: humanReviewRounds is at MaxHumanReviewRounds.
	DeclineRoundsExhausted = "rounds-exhausted"
	// DeclineTurnsExhausted: stats.turns is at maxTurnsPerTask.
	DeclineTurnsExhausted = "turns-exhausted"
	// DeclineWrongParkedFrom: a no-outcome park from a pre-implement stage must
	// not auto-escalate into implementing (#406).
	DeclineWrongParkedFrom = "wrong-parked-from"
	// DeclineIllegalEdge: the F.3 table or a LegalFor guard refused the edge.
	DeclineIllegalEdge = "illegal-edge"
	// DeclineNoReentry: this park reason has no F.6 re-entry rule at all.
	DeclineNoReentry = "no-reentry"
)

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
	target, _ = UnparkDetailed(in)
	return target, target != ""
}

// UnparkDetailed is Unpark with the refusal named. decline is DeclineNone
// exactly when target != "". See the decline vocabulary above for why this
// exists: a guard that declines without recording a reason is how a
// high-stakes re-entry rule stalls a Task for a day with zero errors and zero
// logs.
func UnparkDetailed(in UnparkInput) (target string, decline string) {
	t := in.Task
	now := in.Now
	if now.IsZero() {
		now = time.Now()
	}
	enter := func(to, reason string) (string, string) {
		if err := Enter(t, in.MRs, to, reason, now); err != nil {
			// A guard refused it. LegalFor is what stops a kind=review Task
			// reaching implementing from the no-outcome rule, which F.6 itself
			// writes with no kind guard. It stays parked and ages out.
			return "", DeclineIllegalEdge
		}
		return to, DeclineNone
	}

	switch t.Status.StageReason {

	case ReasonBacklogSweep:
		// NOT a park in the failure sense: this Task never ran. It exists to OWN
		// its Issue CRs at zero agent cost (B.4). It NEVER ages out; it is reaped
		// when its Issues close. A refine fold does NOT promote it - the fold
		// DELETES it (B.3).
		if !hasNonBotEvent(t, in.BotLogin) {
			return "", DeclineNoHumanEvent
		}
		if in.ActiveTasks >= in.MaxOpenTasks {
			// OVER CAP: the promotion DEFERS. The Task stays parked and the
			// pendingEvent is RETAINED, never dropped. It promotes as soon as a
			// slot frees.
			return "", DeclineOverCap
		}
		return enter(v1alpha1.StageTriaging, "")

	case ReasonAwaitingHuman:
		// The ONLY comment-driven re-entry.
		if !hasNonBotEvent(t, in.BotLogin) {
			return "", DeclineNoHumanEvent
		}
		if t.Spec.Kind == kindReview {
			// A review-kind Task may NEVER enter implementing or merging. There
			// is no path, no condition, no exception. It does not exist.
			//
			// humanReviewRounds is a NEW counter and it is NOT mr.reviewRounds,
			// which increments only on request_changes: on the approve path that
			// bound did not exist, and this spawned ONE REVIEW POD PER HUMAN
			// COMMENT, capped only by maxTurnsPerTask (300).
			if anyMerged(in.MRs) {
				// A pod spawned into reviewing on an already-merged MR has no
				// legal outcome (issue #393). Refuse re-entry; the Task ages
				// out at parkRetention and is reaped.
				return "", DeclineMergedMR
			}
			if t.Status.HumanReviewRounds >= v1alpha1.MaxHumanReviewRounds {
				return "", DeclineRoundsExhausted // STAY PARKED. Do not spawn another review pod.
			}
			t.Status.HumanReviewRounds++
			target, decline := enter(v1alpha1.StageReviewing, "")
			if decline != DeclineNone {
				t.Status.HumanReviewRounds--
			}
			return target, decline
		}
		// THE EMPTY SET IS NOT A LICENCE (fix V6-3). A review Task owns ZERO
		// Issues and all([]) == true, so v5's "if EVERY owned Issue is approved
		// -> implementing" promoted it straight into an implement pod against
		// someone else's PR on ANY human comment. And it looped. An empty owned-
		// Issue set must never satisfy a universal quantifier that gates code
		// execution.
		open := openIssues(in.Issues)
		if len(open) == 0 {
			return "", DeclineNoOpenIssues
		}
		if allApproved(open) {
			// A maintainer approved. That is a decision, not a conversation.
			return enter(v1alpha1.StageImplementing, "")
		}
		if in.ConversingHasRoom {
			return enter(v1alpha1.StageConversing, "")
		}
		return enter(v1alpha1.StageClarifying, "")

	case ReasonIdentityUnverified:
		// A comment alone still cannot GRANT anything here. What changed is
		// where the grant lives: it is now EXCLUSIVELY
		// restapi.verifyApprovalScope, which re-derives the maintainer identity
		// and re-checks the agent's citation against the operator's own mirror
		// on every submit_outcome(decision=implement). This arm therefore has
		// exactly one job - put a live agent in front of the human who just
		// commented - and it can never reach implementing or approved.
		//
		// #294: this is a DECISION flip at the chokepoint, not a new EDGE. The
		// Task still traverses clarifying|conversing -> approved -> implementing,
		// still faces LegalFor GUARD 1, ticket admission and the 24h budget.
		if !hasNonBotEvent(t, in.BotLogin) {
			return "", DeclineNoHumanEvent
		}
		if !in.ConversingHasRoom {
			// D1: the truthful terminus. NOT a fallthrough to clarifying: the
			// conversing ceiling exists to bound live agent pods, and spawning a
			// clarifying pod instead would route around the very limit being
			// reported. The pendingEvent is RETAINED, so the next comment or the
			// next reconcile retries.
			return "", DeclineNoConversingRoom
		}
		if t.Spec.Kind == kindReview {
			// A kind=review Task owns ZERO Issues (H9's empty-set rule), so
			// verifyApprovalScope can NEVER pass for it - every
			// decision=implement from a review-kind conversation bounces back to
			// parked(identity-unverified). Without these three guards a stuck
			// kind=review Task would re-enter conversing on every subsequent
			// human comment and spawn ONE POD PER COMMENT forever, capped only
			// by maxTurnsPerTask (300).
			if anyMerged(in.MRs) {
				return "", DeclineMergedMR
			}
			if t.Status.HumanReviewRounds >= v1alpha1.MaxHumanReviewRounds {
				return "", DeclineRoundsExhausted // STAY PARKED. Do not spawn another pod.
			}
			t.Status.HumanReviewRounds++
			target, decline := enter(v1alpha1.StageConversing, "")
			if decline != DeclineNone {
				t.Status.HumanReviewRounds--
			}
			return target, decline
		}
		return enter(v1alpha1.StageConversing, "")

	case ReasonMergeTimeout:
		if t.Status.MergeReentries >= v1alpha1.MaxMergeReentries {
			return enter(v1alpha1.StageFailed, ReasonMergeBlocked)
		}
		t.Status.MergeReentries++
		// Idempotent: mergeCursor resumes and EVERY MR is re-validated against
		// state=merged before any Merge call. NEVER implementing - that would
		// recreate deleted branches and re-propose already-merged code.
		target, decline := enter(v1alpha1.StageMerging, "")
		if decline != DeclineNone {
			t.Status.MergeReentries--
		}
		return target, decline

	case ReasonDeployTimeout:
		if t.Status.DeployReentries >= v1alpha1.MaxDeployReentries {
			return enter(v1alpha1.StageFailed, ReasonDeployBlocked)
		}
		t.Status.DeployReentries++
		// Idempotent: per-MR deployedAt re-check. NEVER implementing.
		target, decline := enter(v1alpha1.StageDeploying, "")
		if decline != DeclineNone {
			t.Status.DeployReentries--
		}
		return target, decline

	case ReasonNoOutcome:
		// #406: only a park reached FROM implementing or reviewing may re-drive.
		// A no-outcome park from investigating/brainstorming/clarifying/refining/
		// documenting means a pre-implement stage never terminated (the real
		// mechanism: an SCM-comment error left the outcome claim unreleased,
		// pod-liveness respawned the pod until PodRecreations exhausted) - such a
		// Task must NOT be auto-escalated straight into implementing.
		if t.Status.ParkedFromStage != v1alpha1.StageImplementing &&
			t.Status.ParkedFromStage != v1alpha1.StageReviewing {
			return "", DeclineWrongParkedFrom
		}
		if anyMerged(in.MRs) {
			// A re-implement would duplicate an already-merged change.
			return "", DeclineMergedMR
		}
		if t.Status.Stats.Turns >= in.MaxTurnsPerTask {
			return "", DeclineTurnsExhausted
		}
		return enter(v1alpha1.StageImplementing, "")

	case ReasonHandoffStalled:
		// The outcome COMMITTED but the C.5.3 phase-2 drain lost the advance
		// (PR 389: a stale informer cache defeated advanceAfterReview). The
		// work already landed on the forge, so this park is recoverable: a
		// human comment re-enters reviewing, where the reconciler's
		// level-triggered re-drive - or, failing that, a fresh review round -
		// completes it. Stage-scoped, not kind-scoped: any kind whose review outcome
		// committed can stall this way. Bounded by humanReviewRounds exactly
		// like the kind=review awaiting-human rule, and for the same reason:
		// each re-entry may spawn a review pod, and a comment storm must not
		// spawn one per comment.
		if !hasNonBotEvent(t, in.BotLogin) {
			return "", DeclineNoHumanEvent
		}
		if anyMerged(in.MRs) {
			// A pod spawned into reviewing on an already-merged MR has no
			// legal outcome (issue #393). Refuse re-entry; the Task ages out
			// at parkRetention and is reaped.
			return "", DeclineMergedMR
		}
		if t.Status.HumanReviewRounds >= v1alpha1.MaxHumanReviewRounds {
			return "", DeclineRoundsExhausted // STAY PARKED. Do not spawn another review pod.
		}
		t.Status.HumanReviewRounds++
		target, decline := enter(v1alpha1.StageReviewing, "")
		if decline != DeclineNone {
			t.Status.HumanReviewRounds--
		}
		return target, decline

	default:
		// review-loop-exhausted, implement-declined, declined, false-positive,
		// stage-deadline,
		// admission-starved, turn-budget-exhausted, pod-recreation-exhausted,
		// fold-adoption-unverified, doc-timeout, operator-error, triage-stalled,
		// name-too-long, review-post-refused, object-too-large,
		// merge-order-missing, agent-contract-mismatch, merge-blocked,
		// deploy-blocked, head-moving.
		//
		// NO re-entry. It ages out at parkRetention and is reaped, AFTER the
		// operator posts its bot park comment. The next sweep re-mints the
		// still-open issue as a parked(backlog-sweep) Task, which OWNS it and
		// costs nothing. If a human then comments, THAT Task promotes - as a NEW
		// Task, not a zombie one.
		return "", DeclineNoReentry
	}
}

// UnparkTargetForBindingRepair derives the re-entry target for EXACTLY ONE
// park flavor: the MR-binding watchdog's parked(awaiting-human), taken when an
// interrupted mint left a Source-bearing Task with zero owned refs (the caller
// verifies that empty-refs half of the predicate BEFORE running the bind
// repair, since a successful repair un-empties them). This is the one
// documented exception to "the un-park target is never derived from
// status.parkedFromStage": every normal F.6 rule re-derives its target from
// owned Issue/MR state, but for this park that state IS the stub that was just
// repaired - there is nothing else to derive from, and parkedFromStage names
// the exact stage the mint interruption yanked the Task out of. The edge
// itself still goes through the F.3 table: a parkedFromStage with no legal
// parked-> edge refuses (ok=false, Task untouched), and the caller's
// Enter/EnterStage application re-validates with the kind guard.
func UnparkTargetForBindingRepair(t *v1alpha1.Task) (target string, ok bool) {
	if t.Status.Stage != v1alpha1.StageParked || t.Status.StageReason != ReasonAwaitingHuman {
		return "", false
	}
	from := t.Status.ParkedFromStage
	if from == "" || !Legal(v1alpha1.StageParked, from) {
		return "", false
	}
	return from, true
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
