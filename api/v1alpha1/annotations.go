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
	// AnnRetryLaneMigrated is the ONCE-ONLY LATCH for the retry-lane migration
	// (controller.driveRetryLaneMigration), and it is the exact analogue of
	// AnnRetiredParkMigrated one class over: ci-red and merge-conflict lost their
	// park writers when stage.CIRed's and stage.MergeConflict's anyMerged arms
	// moved into the lane, so every Task ALREADY carrying one would have kept a
	// verdict from a rule that no longer exists and aged out at ParkRetention.
	// Its VALUE is the RFC3339 instant the migration ran; its PRESENCE is the
	// guard, and nothing ever removes it - a Task re-parked ci-red by a
	// mid-rollout replica is migrated once and never again.
	AnnRetryLaneMigrated = "tatara.dev/retry-lane-migrated"
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
	// AnnRetryExhaustedCommented latches the UnparkRetry escalation comment so
	// the 30s unpark driver cannot post it twice.
	//
	// ITS VALUE IS THE parkedAt OF THE PARK IT ANNOUNCED, and that is what makes
	// it correct rather than merely quiet. A presence-only latch would silence
	// the escalation of every LATER blocker on the same Task, which is the
	// second failure this whole lane exists to remove; keying on the park's own
	// timestamp scopes the silence to the one park that was already announced.
	//
	// It is stamped only AFTER the comment lands, so a forge outage costs a
	// retry on the next pass rather than a lost escalation. It is metadata, not
	// status, for the reason AnnRetiredParkMigrated gives - and because
	// internal/stage cannot write it at all: every persist path behind that
	// package ends in Status().Update, which silently discards annotations.
	AnnRetryExhaustedCommented = "tatara.dev/retry-exhausted-commented"
)

// THE CI-RECOVERY ANNOTATIONS. Four keys, two pairs, and they exist because a
// decline means one of two incompatible things: a verdict on the CHANGE ("this
// bump is wrong / superseded"), which must stay permanent, or a verdict on the
// INFRASTRUCTURE ("I could not submit, CI was red"), which must be re-driven
// when the blocker clears. parked(implement-declined) cannot tell them apart
// after the fact and the free-text decline reason must not be parsed, so the
// discriminating evidence is captured AT DECLINE TIME.
//
// THEY ARE ANNOTATIONS, NOT STATUS FIELDS, for the reason
// AnnRetiredParkMigrated is: they are LATCHES and EVIDENCE about a park, read
// by a driver that lives outside stage.Unpark, and metadata survives every
// status write - a re-park, an un-park, a transition, a spill - without every
// status writer having to individually preserve them. It also keeps the CRD
// schema unchanged.
const (
	// AnnDeclineCI is what CI said at the moment the agent declined: one of
	// CIEvidenceRed / CIEvidenceGreen / CIEvidenceUnknown, aggregated over the
	// merge requests the Task owned (CIDeclineEvidence). ABSENT means the Task
	// owned nothing tatara may act on, which is the ordinary decline and is
	// never re-driven.
	AnnDeclineCI = "tatara.dev/decline-ci"
	// AnnDeclineHeads is the head fingerprint the decline was made against, from
	// the same CIDeclineEvidence call. It is OPAQUE: compared for equality,
	// never parsed. Requiring it to still match is what makes the recovery a
	// re-read of the EXACT code the agent declined at, rather than a re-drive
	// against whatever has been pushed since.
	AnnDeclineHeads = "tatara.dev/decline-heads"
	// AnnCIRecoveryUnparks is the decimal count of CI recoveries this Task has
	// SPENT. It is the ABSOLUTE ceiling (MaxCIRecoveryUnparks), and it is what
	// stops a Task that keeps declining from ping-ponging one fresh head at a
	// time forever.
	AnnCIRecoveryUnparks = "tatara.dev/ci-recovery-unparks"
	// AnnCIRecoveryHeads is the head fingerprint the driver last fired on, and
	// it is the AT-MOST-ONCE-PER-HEAD latch: the same head is never re-driven
	// twice, so a pipeline flapping green -> red -> green at one commit cannot
	// re-open the same decline repeatedly. A fresh push is a genuinely new
	// situation and gets its own lap, charged against the ceiling above.
	//
	// Both are stamped BEFORE the un-park, in one metadata write: a crash
	// between the two leaves a Task that stays parked and is never retried -
	// visible, inert, fixable by hand - which is the same fail-closed ordering
	// driveRetiredUnparks uses and for the same reason.
	AnnCIRecoveryHeads = "tatara.dev/ci-recovery-heads"
)

// AnnMergeStageParked latches the ONE notice posted when an issue's Task parks
// for a MERGE-STAGE reason (stage.IsMergeStagePark) and the automatic driver
// therefore refuses to re-enter it at all. Same shape as
// AnnAutoReentryExhausted - RFC3339 value, PRESENCE is the guard - and
// deliberately NOT that annotation: the two notices say different things (a
// spent budget versus a budget that was never charged), and reusing the marker
// would let a merge-stage stop swallow a later exhaustion notice on the same
// issue, or the reverse.
const AnnMergeStageParked = "tatara.dev/merge-stage-parked"

// AnnMROnlyUnparkRefused latches the ONE notice the MR-only un-park driver posts
// when a maintainer's comment lands on a Task parked AT OR AFTER THE MERGE
// (stage.IsMergeStagePark), where releasing the park would re-drive
// ReconcileMerging against a merge that may have partially succeeded (#597).
//
// ITS VALUE IS THE parkedAt OF THE PARK IT ANSWERED, not a bare presence flag,
// and that is AnnRetryExhaustedCommented's rule for AnnRetryExhaustedCommented's
// reason: a presence-only latch would silence the answer to every LATER park on
// the same Task, and staying silent is the exact defect this driver removes.
// Keying on the park's own timestamp scopes the silence to the one park that was
// already answered.
//
// It is stamped on the TASK, not on the merge request, because it is a fact
// about the park - the same Task can own several merge requests and they all get
// one notice between them, from one park. Metadata, not status, for the reason
// AnnRetiredParkMigrated gives: the subresources are independent, so the
// consume's status write cannot lose it.
const AnnMROnlyUnparkRefused = "tatara.dev/mr-only-unpark-refused"

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

// LabelUpgradeOrigin discriminates the two producers of an `upgrade` Task, and
// it exists so a draining dependency-engine backlog does not silence the upgrade
// CRON. maxOpenUpgrades bounds the cron ALONE (design D2); adopted merge requests
// are bounded by the general pool. Without a discriminator openUpgradeLaneCount
// would read a Renovate batch as "lanes full" and the cron would stop proposing
// bumps for as long as the batch lasted, invisibly.
//
// The prefix is tatara.dev/, NOT the tatara.io/ its neighbours above use. That
// split predates this label (every annotation in this file is tatara.dev/, both
// scan labels are tatara.io/); do not "fix" one to match the other, it would
// orphan every live object carrying the old key.
const (
	LabelUpgradeOrigin = "tatara.dev/upgrade-origin"
	// UpgradeOriginAdopted marks work born from an EXISTING third-party merge
	// request. The cron's own mints carry no upgrade-origin label at all.
	UpgradeOriginAdopted = "adopted"
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
