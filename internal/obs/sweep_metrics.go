package obs

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Sweep cap labels (contract K.1): the two creation budgets that BOTH bind on
// every sweep pass.
const (
	SweepCapMaxNewTasksPerSweep = "maxNewTasksPerSweep"
	SweepCapMaxOpenTasks        = "maxOpenTasks"
)

// TasksMintedPerSweep is observed on EVERY sweep pass, per mint stage, including
// the zero (contract B.4, fix B1/B2). It is what makes the accepted risk of the
// tatara-parked label read a MONITORED one: a project whose parked mints suddenly
// become triaging mints is a project whose park markers stopped landing.
var TasksMintedPerSweep = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "operator_tasks_minted_per_sweep",
	Help:    "Tasks minted by one sweep pass, by project and mint stage (contract B.4).",
	Buckets: []float64{0, 1, 2, 3, 5, 8, 13, 21},
}, []string{"project", "stage"})

// SweepMintCapHitTotal counts sweep passes in which a creation budget BOUND, so
// orphans were left for the next pass. cap is maxNewTasksPerSweep or
// maxOpenTasks. A sustained rate means the backlog is growing faster than the
// platform mints, which is a capacity signal, not an error.
var SweepMintCapHitTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_sweep_mint_cap_hit_total",
	Help: "Sweep passes in which a Task creation budget bound, by project and cap (contract B.4).",
}, []string{"project", "cap"})

// SweepLastSuccessTimestamp is a HEARTBEAT covering two different activity
// families with two different stamp semantics, both intentional for their own
// activity's shape. For "sweep"/"nightlySweep" (sweep.go's B.4 pass) it is
// LIVENESS, not zero-error health: stamped whenever the repos loop RUNS TO
// COMPLETION, even with per-item errors (those are metered separately via
// SweepErrorsTotal) - one stale CR or transient forge error must never
// silence the heartbeat for the whole pass. A sweep that cannot even begin
// (activeTaskCount fails) returns before stamping, so it stays unset. For
// "brainstorm"/"documentation"/"issueScan" (projectscan.go's stampScan) it IS
// zero-error: each is a single Status().Update, not a multi-item loop, so
// success-only stamping is the correct (and simplest) signal there; this is
// also the successor for tatara_scan_items_total, pruned as dead-per-redesign
// (metric-wiring audit, issue #370). Its alert sets noDataState: Alerting,
// because for a heartbeat NoData IS the failure - the gauge is process-local
// and resets on restart, so it is also rehydrated from the persisted
// Status.LastIssueScan/LastBrainstorm/LastDocumentation stamps at the top of
// every runScans reconcile (fix #386), not only stamped on a freshly-run
// pass; an absent series still means that activity has never completed at
// all, never scanned or rehydrated.
//
// `project` is positionally FIRST and is always proj.Name. Without it (issue
// #441) all three Projects wrote one series: the rehydration below runs on every
// Project's reconcile, so the series moved backward and forward - one activity
// cycled through three values within 8 seconds and then held for four hours -
// and a Project whose cron was genuinely dead was masked by a healthy sibling's
// write. The fix is the label set, not backward-movement detection; that is the
// confirmed practice (temporalio/temporal#9600) and no source supports alerting
// on non-monotonicity.
var SweepLastSuccessTimestamp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "operator_sweep_last_success_timestamp_seconds",
	Help: "Unix timestamp of the last completed pass, by project and activity (contract K.1): liveness (per-item-error-tolerant) for sweep/nightlySweep, zero-error for brainstorm/documentation/issueScan.",
}, []string{"project", "activity"})

// SweepNextExpectedTimestamp is the CADENCE half of the heartbeat: the absolute
// unix timestamp of the next expected run for one project's activity, computed
// by the operator from that activity's own cron in the Project CR. It exists so
// the alert rule carries a single grace period instead of a per-activity
// threshold table - one flat 6h threshold breached for ~18h of every 24 on the
// 0 3 * * * / 0 6 * * * nightly crons (tatara-observability#65). This is
// kube-state-metrics' kube_cronjob_next_schedule_time pattern, the only prior art
// that survives adding an activity or a Project without editing the alert rule.
//
// A series exists ONLY for an activity that is actually enabled: publishing one
// for a configured-but-off activity would page for a run that is never going to
// happen. It is refreshed on every Project reconcile from the same persisted
// Status.Last* stamps the heartbeat rehydrates from, so it survives a leader
// change and a pod restart.
var SweepNextExpectedTimestamp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "operator_sweep_next_expected_timestamp_seconds",
	Help: "Unix timestamp of the next expected run for one project's activity, computed from its cron (contract K.1).",
}, []string{"project", "activity"})

// SweepErrorsTotal counts sweep failures by project, activity and reason. Every
// reason is a closed-set string, so the label never takes a forge error message.
var SweepErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_sweep_errors_total",
	Help: "Sweep errors, by project, activity and reason (contract K.1).",
}, []string{"project", "activity", "reason"})

// SweepSkippedTotal counts per-item work the INTAKE FUNNEL deliberately did not
// do, by project, activity and reason. The sweep is one producer
// (activity="sweep"); the webhook is the other (activity="webhook",
// Minter.MintForItem naming the IsOrphanIssue clause that refused a live
// delivery). It is the counterpart to SweepErrorsTotal and
// exists so a benign, self-clearing steady state stays VISIBLE without being
// reported as a fault (issue #477: a PR whose MergeRequest another live Task
// already controller-owns used to raise a hard sweep error on every 4h pass,
// which is what the ERROR-rate alert kept catching). A skip rate that never
// returns to zero is a stuck owning Task - alert on THIS series, not on
// operator_sweep_errors_total and not on ERROR line counts.
var SweepSkippedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_sweep_skipped_total",
	Help: "Intake items deliberately skipped, by project, activity (sweep|webhook) and reason (contract B.4).",
}, []string{"project", "activity", "reason"})

// SweepStaleOwnerRepairedTotal counts Issue AND MergeRequest mirrors whose
// controller ownerRef named a Task the API server no longer has, and which the
// intake funnel therefore REPAIRED by dropping the ref (issue #521). BOTH
// mirror kinds: the reaper releases ownership of them symmetrically
// (releaseOwnership), so the dangling-ref defect existed identically on both.
// It is deliberately NOT a SweepSkippedTotal reason: a repair is work done, and
// it is followed by a mint in the SAME pass, so sharing a series with "I did
// nothing" would hide the difference between the bug being fixed and the bug
// still running.
//
// Expected shape after the #521 rollout: a single burst (one per stranded
// artifact, five issues in the tatara project) and then flat forever. A
// SUSTAINED rate on activity="sweep" is a reap that is not handing over
// ownership - go read B.5, not this counter. activity="webhook" is the same
// repair reached from a live forge delivery instead of a pass; a reap is still
// what produced the stale ref, so the same instruction applies.
var SweepStaleOwnerRepairedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_sweep_stale_owner_repaired_total",
	Help: "Issue/MergeRequest mirrors whose controller ownerRef named a non-existent Task and was dropped, by project and activity (contract B.2/B.4, issue #521).",
}, []string{"project", "activity"})

// SweepTerminalOwnerReleasedTotal counts OPEN Issue mirrors whose controller
// ownerRef named a Task that EXISTS but has FINISHED (state done or rejected),
// and which the intake funnel therefore released by dropping the ref.
//
// IT IS A SEPARATE SERIES FROM SweepStaleOwnerRepairedTotal ON PURPOSE, and the
// reason is that the two have different expected shapes and different runbooks.
// A tombstone ref is a fact about GC ordering: the counter above is documented
// as "a single burst, then flat forever", and a sustained rate on it means a
// reap that is not handing over ownership. A terminal-owner release means a Task
// that is not this issue's work took - and kept - the deed: the live case was
// three `done` backlog-groomer Tasks holding mtg-decks#14 for twelve days
// through the refine outcome's links[] claim. A sustained rate here points at
// THAT path, not at B.5. Folding the two together would have cost the #521
// counter its "flat forever" property, which is the only thing that makes it
// alertable.
//
// THIS COUNTER IS THE VISIBILITY HALF OF THE FIX, and it is not decoration. The
// defect it names ran for twelve days behind ONE line of
// `sweep_skip_issue reason=issue_owned` at INFO, on a path whose deadman
// (SweepOrphanStrandedSeconds) is structurally blind to it: the issue HAD a
// resolvable owner, so it was never stranded. A log line is not a control -
// that is the whole lesson of #521 - and the operator's controllers log through
// logr, which offers INFO and ERROR and no WARN in between. So the alertable
// artifact is this counter, and the log line beside it carries owner_kind and
// owner_state so a non-zero rate names the offending Task KIND without a
// kubectl.
//
// Steady state after a backlog drains is ZERO. Alert on a sustained rate, not
// on a single release: one release per wedged artifact is the fix working.
var SweepTerminalOwnerReleasedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_sweep_terminal_owner_released_total",
	Help: "Open Issue mirrors whose controller ownerRef named a Task that exists but is terminal (done|rejected) and was released, by project and activity (contract B.2/B.4/B.5).",
}, []string{"project", "activity"})

// SweepOrphanStrandedSeconds is the sweep's DEADMAN, and it exists because the
// only sweep alert before it was a LIVENESS heartbeat
// (SweepLastSuccessTimestamp), which reported green for 19 hours while five
// open issues in tatara/tatara-operator had no Task at all (issue #521). A
// heartbeat structurally cannot catch that class: the sweep WAS running,
// correctly, doing nothing. This measures the WORK instead - one series per
// open, allowed-author issue that FINISHED a sweep pass with no live owning
// Task, valued at that issue's age in seconds.
//
// Steady state is ZERO SERIES. Alert on max by (project) over a threshold well
// past one sweep period; the {repo, number} labels NAME the offender, which a
// pre-aggregated max cannot.
//
// It is set for every orphan the pass did not serve - budget-bound, tombstone-
// deleted, AND every per-item ERROR exit. That last group is the one an earlier
// draft missed, and it under-reported exactly when the sweep was failing: an
// issue whose list_comments or mint failed ended the pass with no live owning
// Task, which is the literal definition above.
//
// CARDINALITY (contract K.1). Per-issue labels mean the series MUST be deleted
// when the issue is served, exactly like MergeCursorStalledSeconds: a gauge
// that is never deleted is scraped forever and /metrics grows without bound.
// ClearSweepOrphanStranded is called at the top of every per-repo sweep, and it
// is scoped to (project, repo) rather than project because SweepProject is
// called with dueRepos - clearing by project would wipe series for repos this
// pass never looked at.
var SweepOrphanStrandedSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "operator_sweep_orphan_stranded_seconds",
	Help: "Age in seconds of an open, allowed-author issue that finished a sweep pass with no live owning Task, by project, repo and number (contract B.4/K.1, issue #521).",
}, []string{"project", "repo", "number"})

// ClearSweepOrphanStranded deletes every stranded series for one (project,
// repo). Called at the top of each per-repo sweep, so an issue served by this
// pass loses its series on this pass.
func ClearSweepOrphanStranded(project, repo string) {
	SweepOrphanStrandedSeconds.DeletePartialMatch(prometheus.Labels{"project": project, "repo": repo})
}

// RetainSweepOrphanStranded deletes every stranded series for `project` whose
// repo is NOT in keep. It is the LEAK half of ClearSweepOrphanStranded, and it
// exists because that function only ever runs from inside a sweep pass: a
// project whose sweep is switched off, whose issueScan cron is emptied or made
// unparseable, whose Repository is unenrolled, or which is DELETED outright
// never sweeps that repo again, so its last value is scraped forever. The
// intended alert is `max by (project) (...) > threshold`, so a frozen 19h
// series pages permanently until the pod rolls - a deadman that cannot be
// switched off is not a deadman, it is an alarm nobody will believe.
//
// keep == nil (or empty) retracts the whole project: that IS the disabled /
// no-cron / deleted case.
//
// It enumerates rather than taking the repo set on faith because
// DeletePartialMatch can only express "delete these labels", never "keep only
// these" - and the repos that leak are by definition the ones the caller no
// longer has a name for.
func RetainSweepOrphanStranded(project string, keep []string) {
	kept := make(map[string]bool, len(keep))
	for _, r := range keep {
		kept[r] = true
	}
	ch := make(chan prometheus.Metric, 64)
	go func() {
		SweepOrphanStrandedSeconds.Collect(ch)
		close(ch)
	}()
	var drop []string
	for m := range ch {
		var d dto.Metric
		if err := m.Write(&d); err != nil {
			continue
		}
		var gotProject, gotRepo string
		for _, lp := range d.Label {
			switch lp.GetName() {
			case "project":
				gotProject = lp.GetValue()
			case "repo":
				gotRepo = lp.GetValue()
			}
		}
		if gotProject == project && !kept[gotRepo] {
			drop = append(drop, gotRepo)
		}
	}
	for _, repo := range drop {
		SweepOrphanStrandedSeconds.DeletePartialMatch(prometheus.Labels{"project": project, "repo": repo})
	}
}

// MintOutcomeError is the {outcome} label value for a mint that FAILED. It is
// deliberately not a controller.MintOutcome member - that vocabulary describes
// what a caller must DO NEXT, and an error is already a returned error - but
// without it the series counts only the good news, which is the same
// "green and silent" shape issue #521 spent 19 hours inside.
const MintOutcomeError = "error"

// MintOutcomeTotal counts every intake mint attempt by its typed outcome
// (controller.MintOutcome). It is the metric half of replacing a bare
// `created bool`, which collapsed FOUR states into one bit and let
// resume.go log a re-mint that never happened: "nothing was owed" and "I
// deleted a Task holding this name and the mint is still owed" were the same
// value. A non-zero rate on outcome="tombstone_deleted" is a re-mint-after-reap
// churn signal; a non-zero rate on outcome="existing_live" is the normal
// webhook-beats-sweep backstop.
var MintOutcomeTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_intake_mint_outcome_total",
	Help: "Intake mint attempts by task kind and typed outcome (contract B.4).",
}, []string{"kind", "outcome"})

// AdoptionEnqueuedTotal counts dependency-upgrade merge requests turned into a
// QUEUED adoption, by project and by which producer saw it first. The webhook is
// the fast path and the sweep is the backstop, so a healthy project's `sweep`
// rate is near zero: a sustained `sweep` rate means webhook deliveries are being
// lost, which is invisible on the mint counters (the sweep enqueues them either
// way, just up to a full issueScan period later).
var AdoptionEnqueuedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_adoption_enqueued_total",
	Help: "Dependency-upgrade merge requests enqueued for adoption, by project and producer activity (sweep|webhook).",
}, []string{"project", "activity"})

// adoptionActivities is AdoptionEnqueuedTotal's closed {activity} set: the two
// producers of a queued adoption. Both are seeded even though only the webhook
// produces today, because the pair is the whole point of the label - a `sweep`
// series that only appears once the backstop fires cannot be compared against
// `webhook` on the first lost delivery after a pod roll.
var adoptionActivities = []string{"sweep", WebhookActivity}

// AdoptionEventDroppedTotal counts QUEUED dependency-upgrade adoptions that never
// became a Task, by reason. A queued snapshot is a POINTER to a live forge
// object, and the dispatcher re-asks the adoption question at admit time rather
// than trusting the snapshot - so a non-zero rate is the mechanism WORKING: it is
// the count of agent pods not burned on merge requests that stopped being
// adoptable while they waited.
var AdoptionEventDroppedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "operator_adoption_event_dropped_total",
	Help: "Queued dependency-upgrade adoptions dropped before minting, by project and reason.",
}, []string{"project", "reason"})

// adoptionDropReasons is AdoptionEventDroppedTotal's closed reason set, seeded
// for the same reason every counter here is: a CounterVec with no
// WithLabelValues call has NO series, so increase(...[1h]) is blind to the first
// increment after every pod roll. Kept in sync with the dispatcher's drop(...)
// call sites by TestAdoptionDropReasonsMatchTheDispatcher, which scans them out
// of the source - sweepSkipReasons carried the prose version of that instruction
// and drifted from four call sites anyway (issue #495).
var adoptionDropReasons = []string{"not_adoptable", "repository_gone"}

// sweepSkipReasons is the closed skip-reason set. Keep in sync with sweep.go's
// SweepSkip* constants - enforced by TestSweepSkipReasonsMatchSweepConstants,
// which scans them out of the source, because the identical prose instruction
// on sweepSeedReasons had already drifted from four call sites (issue #495).
var sweepSkipReasons = []string{
	"mr_claimed_by_other_task",
	"issue_not_open",
	"issue_owned",
	"issue_reporter_not_allowed",
	"mint_budget_bound",
	"already_minted",
	"tombstone_deleted",
	"mint_not_owed",
	"upgrade_headroom_bound",
}

// mintOutcomeKinds x mintOutcomes is MintOutcomeTotal's closed label set, seeded
// for the same reason every other counter here is: a CounterVec with no
// WithLabelValues call has NO series at all, so increase(...[1h]) is blind to
// the first mint after every pod roll. Kept literal (no reverse import of
// internal/controller) exactly like sweepSeedReasons.
// takeover and upgrade both reach createTaskRaceSafe - the takeover endpoint
// directly, adoption through MintAdoptedUpgradeTask - so both produce this
// series and both must be seeded, or increase() is blind to the first mint of
// either after every pod roll.
var mintOutcomeKinds = []string{"implement", "review", "takeover", "upgrade"}
var mintOutcomes = []string{"not_owed", "created", "existing_live", "tombstone_deleted", MintOutcomeError}

// sweepActivities is the closed {activity} set for the two intake-funnel
// counters BOTH the sweep and the webhook produce. The webhook half was
// implicitly labelled "sweep" until issue #521's review; a mint driven by a
// live forge delivery is not a sweep pass and must not read as one.
var sweepActivities = []string{"sweep", WebhookActivity}

// ownerActivities is the closed {activity} set for the two OWNER-resolution
// counters, and it is sweepActivities plus the dispatcher. resolveLiveMROwner
// now also runs at ADMIT time (the queued-adoption re-check), and a stale-owner
// ref it repairs there is not a sweep pass and must not read as one - the exact
// mislabelling issue #521 fixed for the webhook half. SweepSkippedTotal is
// deliberately NOT seeded for it: the dispatcher records its refusals on
// AdoptionEventDroppedTotal and emits no sweep skip at all, so seeding one would
// add permanently dead series.
var ownerActivities = []string{"sweep", WebhookActivity, QueueActivity}

// WebhookActivity mirrors controller.WebhookActivity. Literal here for the same
// no-reverse-import reason as sweepSeedReasons; the two are pinned together by
// TestWebhookActivityMatchesTheControllerConstant.
const WebhookActivity = "webhook"

// QueueActivity mirrors controller.QueueActivity: mint-adjacent work the
// leader-elected DISPATCHER does at admission. Pinned by
// TestQueueActivityMatchesTheControllerConstant.
const QueueActivity = "queue"

// sweepSeedReasons is the closed fail(reason, ...) set for sweep.go's B.4 pass
// (SweepActivity), plus list_tasks. Literal here (not imported from
// internal/controller) to avoid a reverse import; kept in sync with sweep.go's
// fail(reason, ...) call sites by TestSweepSeedReasonsCoverEveryFailSite, which
// scans them out of the source - the prose version of that instruction had
// already drifted from four of them.
//
// The drift was not cosmetic (issue #495). An unseeded reason has no series at
// all until its first error, and a counter series born AT its first error has
// no earlier sample to increase from, so increase(...[1h]) is blind to exactly
// the first failure after every pod roll. reconcile_ownership failed once per
// 4-hourly sweep for three days while its own counter read as silent, which is
// why that class had to be alerted on from Loki ERROR lines instead.
var sweepSeedReasons = []string{
	"list_tasks", "owner_repo", "list_issues", "list_prs", "get_issue_cr",
	"list_comments", "get_issue", "mint_issue_task", "clear_webhook_marker",
	"get_owning_task", "get_mr_cr", "adopt_pr", "mint_review_task",
	"reopen_retained_proposal", "get_mr_cr_post_classify", "list_pr_comments",
	"reconcile_ownership", "resolve_live_owner",
	// The adoption arm (a dependency-upgrade merge request minting its own
	// upgrade Task): the mint itself, and the lane count that decides how many
	// this pass may take. An uncountable lane budget adopts NOTHING, so its
	// failure is invisible in the mint counters and only shows up here.
	"adopt_upgrade_mr", "count_upgrade_lanes",
}

// cronReasons/refineReasons are the closed reason sets for projectscan.go's
// cron activities, split per-activity (Task 3, refine re-homing) rather than
// cross-seeded: brainstorm has no cron of its own any more (demand-driven,
// event path only, so only stamp_failed can ever fire on it), issueScan and
// documentation share the plain cron shape (invalid_cron, stamp_failed), and
// refine is now the ONE activity with its own inflight-dedup check, so it
// alone carries refine_inflight_check_failed. The old barrier-era
// refine_barrier_held/refine_barrier_timeout/refine_check_failed reasons are
// gone with the barrier itself - keeping them seeded would leave permanently
// dead series with no producer left anywhere in the codebase.
var cronReasons = []string{"invalid_cron", "stamp_failed"}
var refineReasons = []string{"invalid_cron", "stamp_failed", "refine_inflight_check_failed"}

// upgrade carries the plain cron shape plus its own capacity-count failure: the
// tick reads the live upgrade-Task count before minting, and a failed read is a
// tick that silently minted nothing.
var upgradeReasons = []string{"invalid_cron", "stamp_failed", "upgrade_count_failed"}

// SeedSweepErrorsForProject pre-seeds the closed (activity x reason) label set of
// SweepErrorsTotal for ONE project, so a healthy sweep with zero errors still
// exposes a zero baseline and increase(operator_sweep_errors_total[1h]) is
// well-defined from that project's first reconcile rather than from its first
// error (a CounterVec with no WithLabelValues call has NO series at all;
// metric-wiring audit, issue #370).
//
// This used to run in init(). It cannot any more: `project` joined the label set
// in issue #441 and project names are not known at process start. The Project
// reconciler calls this on every pass; WithLabelValues returns the existing child
// for an already-seeded combination, so it is idempotent and cheap (29 map
// lookups per Project per reconcile).
func SeedSweepErrorsForProject(project string) {
	seed := func(l ...string) { SweepErrorsTotal.WithLabelValues(l...) }
	// "nightlySweep" dropped (boy-scout, this line was already being rewritten
	// for #441): SweepNightlyActivity was deleted as dead in the 2026-07-18
	// #325 change (no nightly sweep planned, no live producer ever existed)
	// but the seed list carried it over verbatim, so 13 of the then-44 seeded
	// series per Project were permanently zero for an activity that can never
	// fire - 13 wasted series process-wide before this branch, 13 per Project
	// after it labelled the metric by project.
	seedLabels(seed, []string{project}, []string{"sweep"}, sweepSeedReasons)
	seedLabels(seed, []string{project}, []string{"brainstorm"}, []string{"stamp_failed"})
	seedLabels(seed, []string{project}, []string{"documentation", "issueScan"}, cronReasons)
	seedLabels(seed, []string{project}, []string{"refine"}, refineReasons)
	seedLabels(seed, []string{project}, []string{"upgrade"}, upgradeReasons)
	skip := func(l ...string) { SweepSkippedTotal.WithLabelValues(l...) }
	seedLabels(skip, []string{project}, sweepActivities, sweepSkipReasons)
	for _, activity := range ownerActivities {
		SweepStaleOwnerRepairedTotal.WithLabelValues(project, activity)
		SweepTerminalOwnerReleasedTotal.WithLabelValues(project, activity)
	}
	enq := func(l ...string) { AdoptionEnqueuedTotal.WithLabelValues(l...) }
	seedLabels(enq, []string{project}, adoptionActivities)
	drop := func(l ...string) { AdoptionEventDroppedTotal.WithLabelValues(l...) }
	seedLabels(drop, []string{project}, adoptionDropReasons)
	// MintOutcomeTotal carries NO project label, so this is process-global and
	// idempotent - seeded here rather than in init() only because this is where
	// its neighbours are, and its absence was the gap the #521 review found.
	mint := func(l ...string) { MintOutcomeTotal.WithLabelValues(l...) }
	seedLabels(mint, mintOutcomeKinds, mintOutcomes)
}

func init() {
	ctrlmetrics.Registry.MustRegister(
		TasksMintedPerSweep,
		SweepMintCapHitTotal,
		SweepLastSuccessTimestamp,
		SweepNextExpectedTimestamp,
		SweepErrorsTotal,
		SweepSkippedTotal,
		SweepStaleOwnerRepairedTotal,
		SweepTerminalOwnerReleasedTotal,
		MintOutcomeTotal,
		AdoptionEnqueuedTotal,
		AdoptionEventDroppedTotal,
		SweepOrphanStrandedSeconds,
	)
}
