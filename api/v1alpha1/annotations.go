package v1alpha1

// ReingestRequestedAnnotation is the RFC3339 timestamp annotation the M2
// webhook sets to request an incremental re-ingest. The RepositoryReconciler
// reads this to decide whether to launch an ingest Job.
const ReingestRequestedAnnotation = "tatara.dev/reingest-requested"

// Turn-loop annotation keys, shared by the controller (agent-run state) and the
// webhook (reactivation must clear them so a fresh run starts clean).
const (
	AnnCurrentTurn   = "tatara.dev/current-turn"
	AnnTurnComplete  = "tatara.dev/turn-complete"
	AnnTurnStartedAt = "tatara.dev/turn-started-at"
	// AnnTurnLastActivity is the RFC3339 timestamp of the in-flight turn's most
	// recent agent activity (transcript stream event), read from the wrapper's
	// session/turn status and refreshed by the poll backstop. The turn-timeout
	// backstop anchors its deadline on max(turn-started-at, this) so an actively
	// streaming turn is not killed as if it were hung; it is absent until the
	// first backstop GetTurn, and consumers fall back to turn-started-at.
	AnnTurnLastActivity = "tatara.dev/turn-last-activity-at"
	// AnnStallProbeID is the wrapper's probeId for the stall probe currently
	// outstanding against the in-flight turn, and its presence IS "a probe is in
	// flight". Empty means the next inactivity verdict starts a fresh ladder.
	//
	// POD-SCOPED like the four turn annotations above, and cleared with them: a
	// probe belongs to ONE turn inside ONE wrapper pod, and a probeId that
	// outlived its pod names a probe no wrapper has ever heard of - which the new
	// wrapper answers with a 404, i.e. indistinguishable from "this wrapper has no
	// probe support at all" (see agent.ErrProbeUnsupported). Leaving it behind
	// would silently downgrade the next pod to the pre-probe path.
	AnnStallProbeID = "tatara.dev/stall-probe-id"
	// AnnStallProbeAt is the RFC3339 timestamp the outstanding probe was sent at.
	// The grace window (Project.spec.agent.stallProbeGraceSeconds) is measured
	// from it, not from the turn anchor: the probe is delivered at the agent's
	// next TOOL-CALL BOUNDARY, so the clock that matters starts when we asked.
	AnnStallProbeAt = "tatara.dev/stall-probe-at"
	// AnnStallProbeAttempts is how many probes this stall episode has sent,
	// including the outstanding one. It has to be persisted rather than derived:
	// the ladder spans reconciles minutes apart and the operator is free to
	// restart between any two of them, and an attempt count that resets on
	// restart is an escalation that never arrives.
	//
	// Reset (deleted) whenever the probe annotations are cleared - an ANSWERED
	// probe ends the episode, so the next stall starts from attempt 1.
	AnnStallProbeAttempts = "tatara.dev/stall-probe-attempts"
	AnnPodRecreations     = "tatara.dev/pod-recreations"
	// AnnReviewHeadBranch carries the PR/MR head (source) branch on a review Task
	// so its pod checks out the PR head read-only and can run/test it (issue #114
	// decision 4). The review agent never pushes (its TASK_BRANCH stays empty).
	AnnReviewHeadBranch = "tatara.dev/review-head-branch"
	// AnnTakeoverHeadBranch carries the existing MR head (source) branch on a
	// takeover Task so its implement pod PUSHES to that exact branch via
	// TASK_BRANCH. Unlike AnnReviewHeadBranch (read-only CHECKOUT_BRANCH, review
	// kind only), this drives the PUSH checkout, so an arbitrary human/other-bot
	// MR branch (e.g. renovate/*) can be worked without reproducing the derived
	// tatara/* branch name.
	AnnTakeoverHeadBranch = "tatara.dev/takeover-head-branch"
	// AnnRetiredParkMigrated is the ONCE-ONLY LATCH for the O3 retired-park
	// migration (controller.driveRetiredUnparks). Its VALUE is the RFC3339 instant
	// the migration ran; its PRESENCE - not its value - is the guard.
	//
	// It lives on metadata, not status, precisely so it survives everything status
	// does: a re-park, an un-park, a state transition, a spill. A Task migrated
	// once and later parked turn-budget-exhausted again by some other build must
	// NOT be migrated a second time, and a status field would have to be
	// individually preserved by every writer to promise that. Nothing ever removes
	// this annotation.
	AnnRetiredParkMigrated = "tatara.dev/retired-park-migrated"
)

// AnnBrainstormSources is the annotation key carrying the comma-separated
// brainstorm source list stamped on brainstorm Tasks by projectscan and read by
// agent.BuildPod to gate the egress network label. Centralised here so the two
// sites cannot drift.
const AnnBrainstormSources = "tatara.dev/brainstorm-sources"

// AnnBrainstormQuota is the annotation key carrying the per-session proposal
// quota (the computed deficit, clamped to [1, MaxProposalsPerOutcome]) stamped
// on brainstorm Tasks by projectscan and read by internal/restapi/outcome.go,
// which TRUNCATES submit_outcome(action=propose) to it. Operator-side truncation
// is the authority: an agent that ignores the quota cannot overshoot the target.
// The agent-visible copy of the same number is a literal line in Task.Spec.Goal.
const AnnBrainstormQuota = "tatara.dev/brainstorm-quota"

// AnnBrainstormResume is the one-shot resume signal for a paused brainstorm.
// Every resume trigger - the push webhook, an operator merge, a maintainer
// comment or close, and the manual override a human writes by hand - stamps
// this SAME annotation on the Project, and the Project reconcile is the single
// place that clears Status.BrainstormPausedAt and removes it again. The VALUE
// is the trigger name, carried only so the resume is logged with the trigger
// that fired; an empty or unrecognised value reads as a manual override.
// Centralised here so the five writers and the one reader cannot drift.
const AnnBrainstormResume = "tatara.dev/brainstorm-resume"

// Label keys shared between the sweep and the cron scans.
const (
	// LabelSourceKind is the activity kind ("mrScan", "issueScan", etc.).
	LabelSourceKind = "tatara.io/source-kind"
	// LabelActivity is the scan activity name.
	LabelActivity = "tatara.io/activity"
)

const (
	// AnnGrafanaAlert carries the rendered Grafana alert context on an incident Task.
	AnnGrafanaAlert = "tatara.dev/grafana-alert"
)

// Documentation-agent annotations: a documentation Task is repo-scoped to the
// DOCS repo (RepositoryRef), so the triggering component repo and its SHA
// range ride as annotations rather than Source, letting the skill shallow-
// clone the source repo and diff base..head.
const (
	AnnSourceRepo    = "tatara.dev/source-repo"
	AnnSourceBaseSHA = "tatara.dev/source-base-sha"
	AnnSourceHeadSHA = "tatara.dev/source-head-sha"
)
