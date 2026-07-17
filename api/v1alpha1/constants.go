package v1alpha1

import "time"

// Operator-wide constants shared across internal/controller and
// internal/stage (contract A.6, fix L30: "used everywhere, declared nowhere"
// in v3). Exported so both packages consume one definition instead of
// re-declaring their own copies.
const (
	// ParkRetention is how long a park (except backlog-sweep) ages out before
	// the reaper collects it (B.6).
	ParkRetention = 7 * 24 * time.Hour
	// DeliveredRetention ages out a delivered Task (B.6/F.4, fix F1).
	DeliveredRetention = 48 * time.Hour
	// RejectedRetention ages out a rejected Task.
	RejectedRetention = 24 * time.Hour
	// FailedRetention ages out a failed Task.
	FailedRetention = 7 * 24 * time.Hour
	// DocStageBudget bounds a documenting batch: it never pins a parent past
	// this (B.6/F.4).
	DocStageBudget = 2 * time.Hour
	// AdmissionStarvedBudget is CLOCK 1: the ADMISSION deadline, measured from
	// status.stageEnteredAt, armed on EVERY pod stage (podStartedAt == nil).
	// Breach -> parked(admission-starved), EXCEPT it is skipped entirely when
	// the project is PAUSED (MaxConcurrentAgents == 0). This is the
	// generalisation of the old approved-stage admission-starved budget to
	// every pod stage - it is NOT podReadyTimeout (fix V6-1, V7-7).
	AdmissionStarvedBudget = 24 * time.Hour
	// PodReadyTimeout IS agentBootDeadline
	// (internal/controller/task_controller.go:35) - the SAME 5-minute
	// constant, not a second one (fix V7-7). This is CLOCK 2: the pod EXISTS
	// (podStartedAt != nil) but never became Ready within this long of
	// podStartedAt. On breach the pod RESPAWNS (+1 podRecreations) via
	// handleBootCrash -> resetAgentRun; it does NOT fail the Task until
	// MaxPodRecreations attempts are exhausted (bootcrash.go:138-175), one
	// attempt counted per distinct pod UID so a genuinely slow boot gets its
	// full MaxPodRecreations x PodReadyTimeout budget.
	//
	// api/v1alpha1 cannot import internal/controller (layering/import-cycle),
	// so this is a standalone literal. It MUST equal
	// internal/controller.agentBootDeadline; internal/controller carries a
	// test-time equality assertion (TestPodReadyTimeoutMatchesAgentBootDeadline
	// in bootcrash_test.go) pinning them together so they cannot drift.
	PodReadyTimeout = 5 * time.Minute
	// MaxMergeReentries bounds the merging<->reviewing re-entry cycle (fix H7).
	MaxMergeReentries = 3
	// MaxDeployReentries bounds the deploying re-entry cycle (fix H7).
	MaxDeployReentries = 3
	// MaxHeadMoveReentries bounds the head-moving re-entry cycle; the FOURTH
	// cycle is refused (fix M3-9).
	MaxHeadMoveReentries = 3
	// MaxHumanReviewRounds bounds the reviewing<->parked(awaiting-human) cycle
	// on kind=review Tasks (fix V7-9). NOT bounded by AgentSpec.MaxReviewRounds
	// - that counter only moves on request_changes.
	MaxHumanReviewRounds = 5
	// CIPollMinInterval floors how often CI status is re-polled (fix C3).
	CIPollMinInterval = 20 * time.Second
	// ObjectByteBudget is the byte-exact pre-write guard ceiling for a CR
	// (A.7): half the ~1.5MiB etcd object ceiling, the headroom reserved for
	// metadata.managedFields growth we do not control.
	ObjectByteBudget = 800_000
	// OutcomeClaimTTL bounds a BARE /outcome claim - one stamped by
	// claimOutcomeFingerprint and never overwritten by a kind handler's commit.
	// Within it, an identical retry is told the outcome is IN FLIGHT on another
	// replica (409). Past it, the claim is an ORPHANED STUB - the process died
	// between the claim and the commit - and an identical retry RE-CLAIMS it and
	// proceeds. The handler makes no forge WRITE and at most one forge READ
	// (GetPRHead), so 60s is far beyond its worst honest latency.
	OutcomeClaimTTL = 60 * time.Second
	// HandoffDeadline bounds the C.5.3 phase-2 handoff: kind=review is the ONE
	// outcome kind whose commit makes no stage transition (the advance is
	// deferred to MergeRequestReconciler -> DrainPendingReview ->
	// advanceAfterReview, so that a forge outage cannot lose an accepted
	// outcome). That drain normally lands in ~1s. The 4h reviewing work budget is
	// far too loose to surface one that never runs, and the pod caps are
	// suppressed underneath it, so this is what keeps the suppression bounded.
	HandoffDeadline = 5 * time.Minute
)
