package v1alpha1

// ReingestRequestedAnnotation is the RFC3339 timestamp annotation the M2
// webhook sets to request an incremental re-ingest. The RepositoryReconciler
// reads this to decide whether to launch an ingest Job.
const ReingestRequestedAnnotation = "tatara.dev/reingest-requested"

// SweepRequestedAnnotation is the RFC3339 instant at which a webhook asked for
// this Repository's issueScan sweep slot to be PULLED FORWARD. reposDueForScan
// treats a repo whose request is newer than the project's last scan as due now,
// regardless of its deterministic phase-shifted slot.
//
// IT EXISTS BECAUSE SOME WORK CANNOT BE MINTED FROM AN HTTP GOROUTINE. A
// dependency-upgrade merge request is adopted under the project-wide
// maxOpenUpgrades cap, and the webhook server runs on EVERY replica
// (HandlerRunnable.NeedLeaderElection() is false) behind a load-balancing
// Service - so a check-then-mint in the handler is a distributed race that no
// in-process lock can close, and three replicas can each independently pass the
// same cap check. The leader's sweep is serialized per project
// (MaxConcurrentReconciles: 1) and already enforces the cap correctly, so the
// webhook's job is reduced to telling it to run NOW. Same idiom as
// ReingestRequestedAnnotation above: a marker write from the HTTP goroutine,
// consumed by a leader-only reconcile (the #353 / F6-1 boundary).
//
// IT IS COMPARED, NEVER CLEARED. Deleting it after a pass would need a
// compare-and-delete against a webhook that may stamp between the list and the
// clear; comparing the instant against the SAME dueBase every other repo slot is
// anchored on costs no write at all, and stampScan advancing LastIssueScan
// retires every request older than the pass that served it. That also makes a
// burst self-debouncing: five deliveries write five instants onto ONE key, and
// one pass serves them all.
const SweepRequestedAnnotation = "tatara.dev/sweep-requested"

// UpgradeDeferredAnnotation records that the most recent sweep pass over this
// Repository found an adoptable dependency-upgrade merge request and could NOT
// take it because the project-wide maxOpenUpgrades was already spent
// (SweepSkipUpgradeHeadroom). Its value is that pass's RFC3339 instant; its
// PRESENCE is what anything reads.
//
// IT IS THE ADDRESS OF A WAIT. SweepRequestedAnnotation above closes the ARRIVAL
// half of the adoption latency - a delivery says "look at THIS repo now". The
// RELEASE half is the mirror image and has no address of its own: an upgrade
// Task frees a lane that is PROJECT-WIDE, and the merge request that lane
// unblocks may sit in any enrolled repository, not the one the finished Task was
// bound to. The two available shortcuts are both wrong. Marking only the
// finished Task's own repository is cheap and silently misses every cross-repo
// deferral. Marking every enrolled repository buys a full forge listing of the
// whole project for every finished Task, which at the observed rate is a
// standing listing load in exchange for information the operator already has.
//
// The pass that DEFERRED is the only actor that knows which repository is
// waiting and why, so it writes it down here, and the freed lane marks exactly
// those. On a project with nothing deferred the release is a cached List and
// zero writes.
//
// IT IS CLEARED, unlike SweepRequestedAnnotation, and by exactly one writer: the
// same sweep arm, on the first pass over this repository that defers nothing.
// That is what bounds the mechanism. A record that outlived its backlog would
// let every subsequent freed lane re-request a sweep of a repository with
// nothing left to adopt, which is the forge-listing loop the whole marker idiom
// is built to avoid. Clearing needs no compare-and-delete race window: this key
// has ONE writer, the leader's serialized sweep, whereas the request marker is
// written by every webhook replica.
const UpgradeDeferredAnnotation = "tatara.dev/upgrade-deferred"

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
	// AnnUpgradeLaneReleased is the ONCE-ONLY LATCH for the lane-release sweep
	// request. Its VALUE is the RFC3339 instant the release ran; its PRESENCE -
	// not its value - is the guard. Nothing ever removes it.
	//
	// IT IS WHAT MAKES THE TRIGGER AN EDGE. A terminal Task is not reconciled
	// once: every informer resync, every event on an object it owns, and every
	// reaper write re-delivers it, and the terminal early return in
	// reconcileStage is reached each time. Without the latch, each of those
	// re-stamps SweepRequestedAnnotation with a fresh instant, which makes the
	// repository due again on the next 30s project reconcile, which lists the
	// forge again - a 30s listing loop driven by Tasks that finished hours ago
	// and are only waiting for the reaper. The freed lane is an EVENT and must be
	// spent exactly once.
	//
	// On metadata rather than status, for the same reason AnnRetiredParkMigrated
	// is: it has to survive everything a status write does to a Task, including
	// an objbudget spill.
	//
	// ONE RELEASE PER TASK IS A DELIBERATE UNDER-COUNT. A Task that parks,
	// unparks (retaking its lane) and parks again frees a lane twice and asks for
	// a sweep once. Re-arming on unpark would need a second writer on the unpark
	// path and would re-open the loop this latch closes, for a shape that is rare
	// and costs only latency: the ordinary four-hourly slot and the webhook
	// fast path both still reach that repository.
	AnnUpgradeLaneReleased = "tatara.dev/upgrade-lane-released"
)

// AnnAutoReentries / AnnAutoReentryExhausted are the C.3 automatic-pickup
// bound, and they live on the ISSUE mirror on purpose.
//
// Every automatic re-entry COLLECTS the parked Task and mints a fresh one, so
// anything stored on the Task resets to zero on every lap and bounds nothing.
// The Issue CR is the only object that survives the laps - which is precisely
// why C.4 stopped cascade-deleting it with its owner - so it is where the count
// has to live. Metadata, not status: a mirror's status is re-derived from the
// forge on every sync, and a counter a sync can clobber is a counter that
// silently un-bounds the loop it exists to bound.
//
// AnnAutoReentries is the decimal count of automatic re-entries SPENT.
// AnnAutoReentryExhausted latches the ONE dead-end notice posted when the count
// reaches MaxAutoReentries; its VALUE is the RFC3339 instant, its PRESENCE is
// the guard. Neither is ever removed: a re-opened issue that genuinely deserves
// a fresh budget gets it from a human, who is the only actor whose judgement
// the bound is protecting against being bypassed.
const (
	AnnAutoReentries        = "tatara.dev/auto-reentries"
	AnnAutoReentryExhausted = "tatara.dev/auto-reentry-exhausted"
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
