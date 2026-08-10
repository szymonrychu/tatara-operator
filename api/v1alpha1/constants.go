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
	// MaxAutoReentries BOUNDS the C.3 automatic pickup of a stranded park: how
	// many times, in total, tatara may collect a Task parked under a reason
	// nothing un-parks and re-mint its issue BY ITSELF, with no human comment.
	//
	// THREE, and the number is the entire safety argument. Making UnparkNever
	// stop meaning permanent removes the property that used to make it safe -
	// that a broken issue eventually stops costing anything - so something else
	// has to provide it. This does: three laps, then a REAL dead end that is
	// labelled tatara-parked and sits in the backlog where a human can see it.
	// An issue that is genuinely broken (a repo that does not build, a task no
	// agent can complete) burns three fresh Tasks and stops; one that was
	// stranded by a transient - a forge outage, an operator crash mid-stage, a
	// CI runner that was down - gets three chances to succeed without anyone
	// having to notice it was stuck.
	//
	// IT IS COUNTED PER ISSUE, NOT PER TASK, and it MUST be: every re-entry
	// deletes the Task and mints a fresh one, so a per-Task counter starts at
	// zero on every lap and bounds nothing at all. That is the same defect
	// carryHumanReviewRounds exists to fix on the review side, and it is why
	// this count is persisted on the Issue mirror (AnnAutoReentries), the one
	// object that outlives the laps.
	MaxAutoReentries = 3
	// ConversationIdleDefault is a LIVE state's IDLE budget when a Project sets
	// no scm.conversationIdleMinutes. It is measured from
	// status.conversationLastEventAt, which every queued event re-stamps, so it
	// is a genuine idle timer and NOT the flat agentPodTTLSeconds cap measured
	// from pod start. Per decision D6 there is no absolute lifetime ceiling while
	// a conversation stays active: the pod rotates at its own TTL and the
	// conversation continues in the replacement.
	//
	// The ABSOLUTE bound this clock deliberately does not provide is
	// stage.ResidencyExceeded, checked separately by reconcileClocks.
	ConversationIdleDefault = 60 * time.Minute
	// DefaultMaxLivePods is the per-project ceiling on simultaneously LIVE
	// Tasks when a Project sets no maxLivePods. Two.
	//
	// It is not a substitute for MaxConcurrentAgents and does not replace it: a
	// live Task holds a real concurrency slot (queueTaskHoldsSlot returns true
	// for it), so the two caps compose. This one exists because a conversation
	// is CHEAP to start and LONG to hold, so without its own bound a handful of
	// chatty threads would occupy every agent slot the project has.
	//
	// MUST STAY STRICTLY BELOW DefaultMaxConcurrentAgents: at 5 versus 3 the
	// ceiling could never bind (three chatty conversations already saturate
	// 100% of the project's agent concurrency before the live-pod cap does
	// anything), starving every implement/review/merge Task indefinitely
	// (2026-07-28 final review IMPORTANT 1). Two leaves at least one slot free
	// for non-conversational work at the defaults. MACHINE-CHECKED by
	// TestMaxLivePodsIsStrictlyBelowMaxConcurrentAgents; it was prose until
	// #521.
	DefaultMaxLivePods = 2
	// DefaultMaxConcurrentAgents is ProjectSpec.MaxConcurrentAgents' kubebuilder
	// default, declared as a Go constant so the strict-inequality invariant
	// above has both sides available to a test.
	DefaultMaxConcurrentAgents = 3
	// DeliveredRetention ages out a delivered Task (B.6/F.4, fix F1).
	DeliveredRetention = 48 * time.Hour
	// RejectedRetention ages out a rejected Task.
	RejectedRetention = 24 * time.Hour
	// DeclinedProposalRetention is the ONE exception to RejectedRetention: a
	// rejected(issue-closed) Task that still owns a RETAINED brainstorm-proposal
	// mirror is held this long instead of 24h. Nothing else uses it, and no other
	// Task shape is affected.
	//
	// The exception exists because the Task's reap is what deletes the mirror: the
	// decline sever keeps the ownerRef on purpose, so the CR cascades with the
	// Task. That mirror is the ONLY record that a proposal was discarded and WHY
	// (the maintainer's comments), and it is what the brainstorm turn-0 bundle's
	// <proposal_history> block renders declined rows from.
	//
	// WHAT BREAKS IF THIS IS LOWERED: at RejectedRetention a decline has a
	// ONE-DAY half-life. The next brainstorm session no longer sees the killed
	// idea, re-proposes it, and the maintainer declines it again - the immortal-PR
	// loop the history block exists to close. Raising it is safe for correctness
	// and costs Issue CRs plus one mirror thread re-read per MirrorCadenceActive
	// per retained mirror, so it is also the CR-growth bound.
	DeclinedProposalRetention = 14 * 24 * time.Hour
	// FailedRetention ages out a failed Task.
	FailedRetention = 7 * 24 * time.Hour
	// DocStageBudget bounds a documenting batch: it never pins a parent past
	// this (B.6/F.4).
	DocStageBudget = 2 * time.Hour
	// FoldInFlightGrace is the window inside which a B.3 fold adoption is a
	// HEALTHY hold and is NOT counted as GC-blocked. It is the same treatment
	// doc_reference got in #424: operator_gc_blocked_total re-counts one EVENT
	// per reconcile pass per held object, so counting the legitimate window made
	// every ordinary fold trip the warning alert. The whole adoption is the body
	// of ONE submit_outcome request and finishes in seconds; five minutes is two
	// orders of magnitude of headroom.
	FoldInFlightGrace = 5 * time.Minute
	// FoldInFlightTTL is the HARD backstop on that same hold: past it the marker
	// is stale by construction and the members are released whatever the
	// umbrella's stage says. It exists because nothing else bounds the hold - the
	// fold is a single request, so an umbrella that is still live an hour later
	// is one whose request died without ever reaching step 5 (issue #467, where
	// 26 members were pinned against an umbrella that self-terminated
	// mid-adoption).
	FoldInFlightTTL = 1 * time.Hour
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
	// PodWatchReconciler.handleNotReady. It does NOT fail the Task at all:
	// O3 deleted MaxPodRecreations, so a boot-crash loop is bounded only by
	// ResidencyCapAll and made visible by the
	// operator_pod_recreations_total{reason="BootTimeout"} churn alert.
	//
	// api/v1alpha1 cannot import internal/controller (layering/import-cycle),
	// so this is a standalone literal. It MUST equal
	// internal/controller.agentBootDeadline; the pin against drift is the
	// literal assertion in project_types_test.go (TestOperatorConstants),
	// which asserts this equals 5 minutes.
	PodReadyTimeout = 5 * time.Minute
	// MaxMergeReentries bounds the merging<->reviewing re-entry cycle (fix H7).
	MaxMergeReentries = 3
	// MaxDeployReentries bounds the deploying re-entry cycle (fix H7).
	MaxDeployReentries = 3
	// TimeoutReentryBudget is the work-clock budget ONE merge-timeout/
	// deploy-timeout un-park re-entry gets, replacing the full stage budget for
	// that attempt (issue #513). The carry from stage.Enter makes the reported
	// residency cumulative; without a fresh window here the re-entry arrives
	// already over budget and re-parks on the same reconcile pass, spending all
	// MaxMergeReentries/MaxDeployReentries laps in milliseconds.
	TimeoutReentryBudget = 30 * time.Minute
	// MaxHeadMoveReentries bounds the head-moving re-entry cycle; the FOURTH
	// cycle is refused (fix M3-9).
	MaxHeadMoveReentries = 3
	// MaxCIRedReentries bounds the red-CI re-entry cycle (issue #476): a red
	// required check routes the Task back to implementing so the agent can fix
	// it, and the FOURTH lap is refused at failed(ci-blocked).
	MaxCIRedReentries = 3
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
	// OutcomeHandlerBudget is the maximum wall-clock a SINGLE /outcome handler may
	// run: postOutcome bounds its request context with it, at the top, before the
	// claim. Nothing else bounds that handler - internal/webhook/server.go sets only
	// ReadHeaderTimeout, no http.Server in the request path sets a WriteTimeout, and
	// the brainstorm path loops CreateIssue (~30s each in internal/scm/github.go)
	// once per proposal. internal/restapi/outcome.go caps brainstorm proposals at 5,
	// so the validator's own worst case is 5 x 30s = 150s (2m30s) - the budget MUST
	// exceed that, or a legal request gets cut mid-loop with some issues already
	// filed. It must also stay STRICTLY under OutcomeClaimTTL (see below), so the
	// budget can never outlive its own claim. 3m clears the 2m30s worst case with
	// 30s to spare and stays 2m under the 5m TTL.
	OutcomeHandlerBudget = 3 * time.Minute
	// OutcomeClaimTTL bounds a BARE /outcome claim - one stamped by
	// claimOutcomeFingerprint and never overwritten by a kind handler's commit.
	// Within it, an identical retry is told the outcome is IN FLIGHT on another
	// replica (409). Past it, the claim is an ORPHANED STUB - the process died
	// between the claim and the commit - and an identical retry RE-CLAIMS it and
	// proceeds.
	//
	// THE INVARIANT THAT MAKES THE LEASE SOUND: OutcomeHandlerBudget <
	// OutcomeClaimTTL. A handler that can outlive its own lease is a handler whose
	// identical retry finds a claim that LOOKS orphaned, is not, re-claims it, and
	// runs every side effect a SECOND time - the exact duplicate the C7 claim-first
	// ordering exists to prevent. With the budget strictly under the TTL a request
	// provably cannot outlive its own claim, so no retry can steal a live request's
	// claim. api/v1alpha1's TestOutcomeHandlerBudgetIsUnderTheClaimTTL asserts it so
	// a future edit to either number cannot silently re-open the hole.
	OutcomeClaimTTL = 5 * time.Minute
	// HandoffDeadline bounds the C.5.3 phase-2 handoff: kind=review is the ONE
	// outcome kind whose commit makes no stage transition (the advance is
	// deferred to MergeRequestReconciler -> DrainPendingReview ->
	// advanceAfterReview, so that a forge outage cannot lose an accepted
	// outcome). That drain normally lands in ~1s. The 4h reviewing work budget is
	// far too loose to surface one that never runs, and the pod caps are
	// suppressed underneath it, so this is what keeps the suppression bounded.
	HandoffDeadline = 5 * time.Minute
	// CIWaitDeadline bounds status.ciWaitSince - the hold PR B puts an accepted
	// implement outcome in while CI at the pushed head is still running.
	//
	// It is a SEPARATE constant from HandoffDeadline and it is six times longer,
	// because the two wait on different things. HandoffDeadline waits on this
	// operator's own reconciler, which lands in about a second; this one waits on
	// a forge's runners, and a terraform plan or a kaniko image build routinely
	// takes fifteen minutes. Five minutes here would expire on every genuinely
	// healthy pipeline, which is the same as not holding at all.
	//
	// EXPIRY IS FAIL-OPEN, NOT A PARK. On expiry reconcileCIWait advances to
	// awaiting-review exactly as the pre-PR-B code did unconditionally, so the
	// worst case of a forge that stops delivering CI events is the behaviour this
	// platform already had. The B3 gate at awaiting-review then still bounces the
	// Task if the pipeline later goes red.
	CIWaitDeadline = 30 * time.Minute
)
