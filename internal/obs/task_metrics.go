package obs

import "github.com/prometheus/client_golang/prometheus"

// taskMetrics holds the Task-lifecycle Prometheus collectors (tokens, turns,
// issue state, terminal outcomes), embedded into OperatorMetrics.
type taskMetrics struct {
	taskTokensTotal         *prometheus.CounterVec
	taskTurnsTotal          *prometheus.CounterVec
	taskIssueState          *prometheus.GaugeVec
	taskTerminalTotal       *prometheus.CounterVec
	taskTerminalTokensTotal *prometheus.CounterVec
	taskStage               *prometheus.GaugeVec
	taskStageAge            *prometheus.GaugeVec
	taskParkedTotal         *prometheus.CounterVec
	orphanAdoptedTotal      *prometheus.CounterVec
	unparkDeclinedTotal     *prometheus.CounterVec
	taskUnparkedTotal       *prometheus.CounterVec
	mrOnlyUnparkTotal       *prometheus.CounterVec
	livePods                *prometheus.GaugeVec
	liveEntryDeclined       *prometheus.CounterVec
	liveClosedTotal         *prometheus.CounterVec
	residencyExceededTotal  *prometheus.CounterVec
	parkedLivePodRepaired   *prometheus.CounterVec
	orphanedTurnCleared     *prometheus.CounterVec
	botRounds               *prometheus.GaugeVec
	retryScheduledTotal     *prometheus.CounterVec
	retryExhaustedTotal     *prometheus.CounterVec
	retryBlockerReadTotal   *prometheus.CounterVec
}

// newTaskMetrics registers the task collectors on reg and returns the bundle.
func newTaskMetrics(reg prometheus.Registerer) *taskMetrics {
	m := &taskMetrics{
		taskTokensTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_task_tokens_total",
			Help: "Agent token usage by project, repo, Task kind, issue, model, and type (input|output|cache_read|cache_creation).",
		}, []string{"project", "repo", "kind", "issue", "model", "type"}),
		taskTurnsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_task_turns_total",
			Help: "Agent turns completed by project, repo, Task kind, and issue.",
		}, []string{"project", "repo", "kind", "issue"}),
		taskIssueState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "tatara_issue_state",
			Help: "Current state of open issues tracked by an agent Task, by project, repo, issue, kind, state, and incident flag. Value is always 1; stale series are removed on each recompute.",
		}, []string{"project", "repo", "issue", "kind", "state", "incident"}),
		taskTerminalTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_task_terminal_total",
			Help: "Tasks reaching a TERMINAL STATE by kind, state (done|rejected), and state reason. #521 narrowed the state label from four values to two: `failed` and `parked` are gone as states - a failure now PARKS, and operator_task_parked_total is where a park is counted.",
		}, []string{"kind", "state", "stateReason"}),
		taskTerminalTokensTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_task_terminal_tokens_total",
			Help: "Cumulative agent token usage of terminated Tasks by project, repo, terminal outcome (delivered|churned|abandoned), model, and type (input|output|cache_read|cache_creation). No issue label - churn is outcome-keyed, not issue-keyed.",
		}, []string{"project", "repo", "outcome", "model", "type"}),
		taskStage: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "operator_task_state",
			Help: "Live Tasks currently in a given state, by state and kind (contract K.1). Value is the COUNT of Tasks in that (state,kind) bucket, not per-task.",
		}, []string{"state", "kind"}),
		taskStageAge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "operator_task_state_age_seconds",
			Help: "Seconds since a live Task entered its current state (contract K.1), by task, state, and kind. It is CARRY-ADJUSTED (stage.StateElapsedSeconds), so a park/un-park round trip reads as continuous residency rather than a sawtooth.",
		}, []string{"task", "state", "kind"}),
		taskParkedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_task_parked_total",
			Help: "Parks (contract K.1), by the STATE the Task was in when it parked and the parkReason. #521 made park a flag orthogonal to state, so this is the ONLY counter of a stall - operator_task_terminal_total no longer sees one. Incremented once per park, never on a mint.",
		}, []string{"state", "parkReason"}),
		// Legitimately reads 0 when webhook-primary reactivity is handling
		// intake and the sweep finds no genuine orphan (verified via 7-day
		// Prometheus history during the metric-wiring audit, issue #370:
		// both webhook-driven and sweep-driven mint counts were 0 in the
		// same window - the backlog was fully covered by the webhook path,
		// not silently dropped by a broken sweep). Do not "fix" a flat 0
		// here without first confirming the sweep is genuinely finding
		// zero orphans, not silently failing to adopt real ones.
		orphanAdoptedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_orphan_adopted_total",
			Help: "Orphan work items the sweep minted a Task for (contract K.1), by kind.",
		}, []string{"kind"}),
		unparkDeclinedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_unpark_declined_total",
			Help: "Re-entry declines by ApplyUnpark, by the Task's parkReason and decline kind: " +
				"guard (the live Task had already drifted from what the caller believed was parked - rare, " +
				"anomalous) or rule (stage.Unpark's re-entry rule was not satisfied yet - normal steady state).",
		}, []string{"parkReason", "kind"}),
		taskUnparkedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_task_unparked_total",
			Help: "Parks RELEASED, by the parkReason left behind and the unpark class that released it. " +
				"class=retired is the O3 one-shot migration (driveRetiredUnparks) releasing a park written by a " +
				"deleted ceiling - turn-budget-exhausted, review-loop-exhausted, pod-recreation-exhausted. It is " +
				"EXPECTED to spike once after the O3 rollout and then sit flat at zero forever: each Task is " +
				"latched by the tatara.dev/retired-park-migrated annotation and migrated exactly once, so a " +
				"class=retired rate that does NOT decay to zero means the latch is not sticking and Tasks are " +
				"being re-driven on every pass.",
		}, []string{"reason", "class"}),
		mrOnlyUnparkTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_mr_only_unpark_total",
			Help: "Maintainer comments ANSWERED on a parked Task that owns no Issue mirror, by project, park reason and outcome. " +
				"unparked: the park was released in place and the Task resumed where it stopped. " +
				"refused: the park was written at or after the merge, so the Task was left exactly as it was and the merge request was told why. " +
				"Before this driver existed BOTH outcomes were silence - resumeOne bails unconditionally on a Task with no Issue to sever - " +
				"so any non-zero value is a comment that would previously have been swallowed. Neither value is an error; " +
				"what would be a bug is `unparked` climbing for one Task, since one comment is spent per release (UnparkConsumedAt).",
		}, []string{"project", "parkReason", "outcome"}),
		livePods: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "operator_live_pods",
			Help: "Tasks currently in a LIVE state and un-parked, by project. It is the live reading of the per-project ceiling (Project.spec.maxLivePods, default 2, clamped strictly below maxConcurrentAgents).",
		}, []string{"project"}),
		liveEntryDeclined: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_live_entry_declined_total",
			Help: "Comments that did NOT reach a live agent pod, by project and reason. There is no acknowledgement layer, so this counter is the only thing that makes a refused turn visible to an operator. The reason is a CLOSED vocabulary (controller.LiveEntryDeclineReasons) and never the literal \"unresolved\", which is all the pre-#521 counter ever recorded.",
		}, []string{"project", "reason"}),
		liveClosedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_live_closed_total",
			Help: "Conversations ended, by project and cause (idle | evicted). A rising evicted rate means the ceiling is the binding constraint, not the idle window.",
		}, []string{"project", "cause"}),
		residencyExceededTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_task_residency_exceeded_total",
			Help: "Tasks parked because a live state's ABSOLUTE residency cap was reached even though its idle clock had not. Generalising liveness replaced a live state's work clock with an idle clock, which a chatty reviewer resets forever; this counter is how often that backstop had to fire.",
		}, []string{"state", "kind"}),
		parkedLivePodRepaired: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_task_parked_with_live_pod_repaired_total",
			Help: "Parked Tasks found still holding a live agent pod and repaired. parkReason != \"\" with a live pod is a TRANSIENT by design; a sustained non-zero rate means the park-then-stop sequence is not completing and slots are leaking.",
		}, []string{"project", "park_reason"}),
		orphanedTurnCleared: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_orphaned_turn_annotations_cleared_total",
			Help: "Turn annotations retired by the poll backstop because the pod that was running the turn is gone (the Task is parked, or its pod clocks are nil). This is a REPAIR of an invariant the pod-teardown paths are supposed to maintain themselves, so a sustained non-zero rate means one of them is still leaking - the same reading as operator_task_parked_with_live_pod_repaired_total. A one-off burst after a rollout is the pre-existing backlog draining (issue #566).",
		}, []string{"project"}),
		botRounds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "operator_bot_rounds",
			Help: "Highest consecutive agent-authored comment rounds with no intervening human comment, by project. There is deliberately no ping-pong cap (decision D7); this gauge is the ONLY way a cycling agent pair becomes observable before a human finds it by reading duplicate comments.",
		}, []string{"project"}),
		retryScheduledTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_task_retry_scheduled_total",
			Help: "UnparkRetry laps SCHEDULED, by the park reason that named the blocker. One increment is one backed-off attempt charged against MaxUnparkRetries, so a Task contributes at most five before it escalates. It is the denominator for operator_task_retry_exhausted_total: a rising schedule rate with a flat exhaustion rate is the lane working (blockers clearing themselves); the two rising together means the retries are not clearing anything and the backoff is only delaying a human.",
		}, []string{"reason"}),
		retryExhaustedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_task_retry_exhausted_total",
			Help: "UnparkRetry lanes that ENDED at a human, by the park reason that ended them and the state the Task was stuck in. Each increment is a Task that has been re-parked retry-exhausted and told a human so on the forge, so ANY non-zero value is an approved Task that did not deliver and is waiting on a person. This is the alertable series: before it existed the same event was a silent park nobody found for days. Two reasons produce it and they are different failures: a LANE reason (ci-failed, merge-conflict-retry, ...) is a blocker that outlived MaxUnparkRetries laps, go and look at the merge request; no-outcome is a lane the agent-stop re-arm cap took over AFTER the blocker cleared - the pod came back and the agent asked to stop AgentStopReArmCap times without submitting an outcome - so go and look at what is still unmerged.",
		}, []string{"reason", "state"}),
		retryBlockerReadTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_task_retry_blocker_read_total",
			Help: "LIVE forge confirmations the UnparkRetry lane made before releasing a park, by the park reason and the answer: standing (the blocker is still there, so the lane charges a lap instead of spawning a pod), cleared (it is gone, so the park is released), error (the read did not answer, so nothing is charged or released and the next pass re-reads) and deferred (this pass's maxRetryBlockerReadsPerPass allowance was already spent). A persistently non-zero deferred rate says the per-pass cap is too small for the due backlog; a persistently non-zero error rate says the gate is not gating and every release is happening blind.",
		}, []string{"reason", "result"}),
	}
	reg.MustRegister(
		m.taskTokensTotal,
		m.taskTurnsTotal,
		m.taskIssueState,
		m.taskTerminalTotal,
		m.taskTerminalTokensTotal,
		m.taskStage,
		m.taskStageAge,
		m.taskParkedTotal,
		m.orphanAdoptedTotal,
		m.unparkDeclinedTotal,
		m.taskUnparkedTotal,
		m.mrOnlyUnparkTotal,
		m.livePods,
		m.liveEntryDeclined,
		m.liveClosedTotal,
		m.residencyExceededTotal,
		m.parkedLivePodRepaired,
		m.orphanedTurnCleared,
		m.botRounds,
		m.retryScheduledTotal,
		m.retryExhaustedTotal,
		m.retryBlockerReadTotal,
	)
	return m
}

// TaskRetryScheduled increments operator_task_retry_scheduled_total for one
// armed UnparkRetry lap. Nil-safe: a reconciler wired without metrics is a
// test, not an outage.
func (m *OperatorMetrics) TaskRetryScheduled(reason string) {
	if m == nil || m.retryScheduledTotal == nil {
		return
	}
	m.retryScheduledTotal.WithLabelValues(reason).Inc()
}

// TaskRetryScheduledCounter returns the operator_task_retry_scheduled_total
// counter for reason, for test assertions.
func (m *OperatorMetrics) TaskRetryScheduledCounter(reason string) prometheus.Counter {
	return m.retryScheduledTotal.WithLabelValues(reason)
}

// TaskRetryExhausted increments operator_task_retry_exhausted_total for one
// spent retry lane, by the blocker it failed to clear and the state it is stuck
// in. Nil-safe for the same reason.
func (m *OperatorMetrics) TaskRetryExhausted(reason, state string) {
	if m == nil || m.retryExhaustedTotal == nil {
		return
	}
	m.retryExhaustedTotal.WithLabelValues(reason, state).Inc()
}

// TaskRetryExhaustedCounter returns the operator_task_retry_exhausted_total
// counter for (reason,state), for test assertions.
func (m *OperatorMetrics) TaskRetryExhaustedCounter(reason, state string) prometheus.Counter {
	return m.retryExhaustedTotal.WithLabelValues(reason, state)
}

// The four answers a live blocker read can produce. They are constants because
// the metric's `result` label and the driver's branches must not drift apart.
const (
	RetryBlockerReadStanding = "standing"
	RetryBlockerReadCleared  = "cleared"
	RetryBlockerReadError    = "error"
	RetryBlockerReadDeferred = "deferred"
)

// TaskRetryBlockerRead increments operator_task_retry_blocker_read_total for one
// attempted live confirmation. Nil-safe for the reason the two above are.
func (m *OperatorMetrics) TaskRetryBlockerRead(reason, result string) {
	if m == nil || m.retryBlockerReadTotal == nil {
		return
	}
	m.retryBlockerReadTotal.WithLabelValues(reason, result).Inc()
}

// TaskRetryBlockerReadCounter returns the operator_task_retry_blocker_read_total
// counter for (reason,result), for test assertions.
func (m *OperatorMetrics) TaskRetryBlockerReadCounter(reason, result string) prometheus.Counter {
	return m.retryBlockerReadTotal.WithLabelValues(reason, result)
}

// addPositive adds delta to the vec's counter for the given labels, but only
// when delta is positive, so each series only ever moves forward.
func addPositive(vec *prometheus.CounterVec, delta int64, labels ...string) {
	if delta > 0 {
		vec.WithLabelValues(labels...).Add(float64(delta))
	}
}

// AddTaskTokens increments operator_task_tokens_total by the per-class token
// deltas a single agent turn consumed, labelled by the Task's project, repo,
// kind, issue, and the model that ran. issue is "" for non-issue-scoped tasks
// to bound cardinality; model is "" when unstamped (fail-open). Zero or
// negative deltas are skipped so each series only ever moves forward.
func (m *taskMetrics) AddTaskTokens(project, repo, kind, issue, model string, input, output, cacheRead, cacheCreation int64) {
	addPositive(m.taskTokensTotal, input, project, repo, kind, issue, model, "input")
	addPositive(m.taskTokensTotal, output, project, repo, kind, issue, model, "output")
	addPositive(m.taskTokensTotal, cacheRead, project, repo, kind, issue, model, "cache_read")
	addPositive(m.taskTokensTotal, cacheCreation, project, repo, kind, issue, model, "cache_creation")
}

// AddTerminalTokens increments operator_task_terminal_tokens_total by a
// terminated Task's cumulative per-class token totals, labelled by project,
// repo, the task's classified terminal outcome (delivered|churned|abandoned),
// and the model that ran. No issue label: churn is outcome-keyed, not
// issue-keyed, to bound cardinality. Zero or negative deltas are skipped.
func (m *taskMetrics) AddTerminalTokens(project, repo, outcome, model string, input, output, cacheRead, cacheCreation int64) {
	addPositive(m.taskTerminalTokensTotal, input, project, repo, outcome, model, "input")
	addPositive(m.taskTerminalTokensTotal, output, project, repo, outcome, model, "output")
	addPositive(m.taskTerminalTokensTotal, cacheRead, project, repo, outcome, model, "cache_read")
	addPositive(m.taskTerminalTokensTotal, cacheCreation, project, repo, outcome, model, "cache_creation")
}

// TaskTerminalTokensCounter returns the counter for (project,repo,outcome,model,type) for test assertions.
func (m *taskMetrics) TaskTerminalTokensCounter(project, repo, outcome, model, typ string) prometheus.Counter {
	return m.taskTerminalTokensTotal.WithLabelValues(project, repo, outcome, model, typ)
}

// TaskTokensCounter returns the counter for (project,repo,kind,issue,model,type) for test assertions.
func (m *taskMetrics) TaskTokensCounter(project, repo, kind, issue, model, typ string) prometheus.Counter {
	return m.taskTokensTotal.WithLabelValues(project, repo, kind, issue, model, typ)
}

// TaskTurnsCounter returns the counter for (project,repo,kind,issue) for test assertions.
func (m *taskMetrics) TaskTurnsCounter(project, repo, kind, issue string) prometheus.Counter {
	return m.taskTurnsTotal.WithLabelValues(project, repo, kind, issue)
}

// AddTaskTurn increments operator_task_turns_total by 1 for a completed agent
// turn. Called at the same site as AddTaskTokens (once per turn-complete
// callback), guarded by the same stale/duplicate-callback recorded flag.
func (m *taskMetrics) AddTaskTurn(project, repo, kind, issue string) {
	m.taskTurnsTotal.WithLabelValues(project, repo, kind, issue).Inc()
}

// SetIssueState sets tatara_issue_state{...}=1 for a live issue. Labels:
// project, repo, issue, kind (joins token/turn counters), state, incident.
func (m *taskMetrics) SetIssueState(project, repo, issue, kind, state, incident string) {
	m.taskIssueState.WithLabelValues(project, repo, issue, kind, state, incident).Set(1)
}

// ResetIssueState clears all tatara_issue_state series. Called at the start of
// each updateIssueStateCounts pass so stale (closed/terminal) issues vanish.
func (m *taskMetrics) ResetIssueState() {
	m.taskIssueState.Reset()
}

// DeleteTaskSeries removes the operator_task_tokens_total and
// operator_task_turns_total series for a specific issue-scoped Task when it is
// garbage-collected. Bounds counter cardinality to live + recently-live issues.
// Skip when issue=="" (project-scoped tasks share that label value and must not
// be cleared on any individual task's GC).
//
// Uses DeletePartialMatch on (project,repo,kind,issue) rather than an exact
// DeleteLabelValues match on model+type: a Task's Status.ResolvedModel can
// change across its life (a respawn or stage change may re-resolve a
// different model), so a single Task's token series can be split across
// several model label values. Matching on the model of ONLY the final resolve
// would leak every earlier model's series forever (metric-wiring audit,
// issue #370).
func (m *taskMetrics) DeleteTaskSeries(project, repo, kind, issue string) {
	if issue == "" {
		return
	}
	match := prometheus.Labels{"project": project, "repo": repo, "kind": kind, "issue": issue}
	m.taskTokensTotal.DeletePartialMatch(match)
	m.taskTurnsTotal.DeletePartialMatch(match)
}

// TaskTerminal increments operator_task_terminal_total for a Task reaching a
// terminal stage (delivered|failed|rejected|parked) with the given kind and
// the stage reason recorded on the terminal transition. This is the uniform
// loop success/failure denominator: every terminal transition is metered here
// exactly once, including failure paths (PodLost, TurnTimeout,
// PlanningStalled, ...) that the per-reason fault counters do not all cover.
func (m *taskMetrics) TaskTerminal(kind, state, stateReason string) {
	m.taskTerminalTotal.WithLabelValues(kind, state, stateReason).Inc()
}

// ResetTaskStageGauges clears operator_task_state and
// operator_task_state_age_seconds so a recompute pass leaves no stale series
// for a Task that left its stage or was deleted (contract M22): a per-task
// gauge that is never explicitly reaped grows /metrics without bound, so every
// pass Resets first and re-Sets only live series.
func (m *OperatorMetrics) ResetTaskStageGauges() {
	if m == nil || m.taskStage == nil || m.taskStageAge == nil {
		return
	}
	m.taskStage.Reset()
	m.taskStageAge.Reset()
}

// SetTaskStage sets operator_task_state{stage,kind} to the live COUNT of Tasks
// in that bucket.
func (m *OperatorMetrics) SetTaskStage(state, kind string, count float64) {
	if m == nil || m.taskStage == nil {
		return
	}
	m.taskStage.WithLabelValues(state, kind).Set(count)
}

// SetTaskStageAge sets operator_task_state_age_seconds{task,stage,kind} to
// ageSeconds, the time since that Task entered its current stage.
func (m *OperatorMetrics) SetTaskStageAge(task, state, kind string, ageSeconds float64) {
	if m == nil || m.taskStageAge == nil {
		return
	}
	m.taskStageAge.WithLabelValues(task, state, kind).Set(ageSeconds)
}

// TaskParked increments operator_task_parked_total for one park. state is the
// state the Task was in when it parked; parkReason is the flag it stamped.
// Nil-safe: ParkTask calls it unconditionally, and a reconciler wired without
// metrics is a test, not an outage.
func (m *OperatorMetrics) TaskParked(state, parkReason string) {
	if m == nil || m.taskParkedTotal == nil {
		return
	}
	m.taskParkedTotal.WithLabelValues(state, parkReason).Inc()
}

// OrphanAdopted increments operator_orphan_adopted_total for one orphan work
// item (issue or PR) the sweep minted a Task for.
func (m *OperatorMetrics) OrphanAdopted(kind string) {
	if m == nil || m.orphanAdoptedTotal == nil {
		return
	}
	m.orphanAdoptedTotal.WithLabelValues(kind).Inc()
}

// TaskStageGauge returns the operator_task_state gauge for (stage,kind) for
// test assertions.
func (m *OperatorMetrics) TaskStageGauge(state, kind string) prometheus.Gauge {
	return m.taskStage.WithLabelValues(state, kind)
}

// TaskStageAgeGauge returns the operator_task_state_age_seconds gauge for
// (task,stage,kind) for test assertions.
func (m *OperatorMetrics) TaskStageAgeGauge(task, state, kind string) prometheus.Gauge {
	return m.taskStageAge.WithLabelValues(task, state, kind)
}

// TaskParkedCounter returns the operator_task_parked_total counter for
// (stage,stageReason) for test assertions.
func (m *OperatorMetrics) TaskParkedCounter(state, parkReason string) prometheus.Counter {
	return m.taskParkedTotal.WithLabelValues(state, parkReason)
}

// OrphanAdoptedCounter returns the operator_orphan_adopted_total counter for
// kind for test assertions.
func (m *OperatorMetrics) OrphanAdoptedCounter(kind string) prometheus.Counter {
	return m.orphanAdoptedTotal.WithLabelValues(kind)
}

// UnparkDeclined increments operator_unpark_declined_total for one F.6
// re-entry decline, by the Task's park stageReason and decline kind ("guard"
// or "rule", see UnparkDecline). Nil-safe: a reconciler wired without metrics
// is a test, not an outage.
func (m *OperatorMetrics) UnparkDeclined(stageReason, kind string) {
	if m == nil || m.unparkDeclinedTotal == nil {
		return
	}
	m.unparkDeclinedTotal.WithLabelValues(stageReason, kind).Inc()
}

// UnparkClassRetired is the `class` label value for the O3 retired-park
// migration. It is declared in this package, not aliased from internal/stage,
// so a stage-package identifier can never leak into a label value by accident -
// the same rule internal/controller's UnparkDecline vocabulary follows.
const UnparkClassRetired = "retired"

// UnparkClassCIRecovered is the `class` label value for the CI-recovery driver
// (controller.driveCIRecoveryUnparks): a decline the operator's own submission
// gate forced with a 409 ci-red, released because the pipeline it named has
// since gone green at the same head. Declared here, not aliased, for the reason
// above.
//
// Unlike class=retired this is NOT a one-shot migration and is expected to tick
// at a low rate forever. What it must NOT do is climb: each Task is bounded by
// MaxCIRecoveryUnparks and by an at-most-once-per-head latch, so a rate that
// keeps rising means a Task is ping-ponging and one of those two bounds is not
// sticking.
const UnparkClassCIRecovered = "ci-recovered"

// UnparkClassMROnly is the `class` label value for the MR-only maintainer-
// unpark driver (controller.driveMROnlyUnparks): a park released in place on a
// Task that owns no Issue mirror, by a maintainer comment answered on its
// merge request. Declared here, not aliased, for the reason above.
const UnparkClassMROnly = "mr-only"

// TaskUnparked increments operator_task_unparked_total for one released park.
// Nil-safe: a reconciler wired without metrics is a test, not an outage.
func (m *OperatorMetrics) TaskUnparked(parkReason, class string) {
	if m == nil || m.taskUnparkedTotal == nil {
		return
	}
	m.taskUnparkedTotal.WithLabelValues(parkReason, class).Inc()
}

// TaskUnparkedCounter returns the operator_task_unparked_total counter for
// (parkReason,class) for test assertions.
func (m *OperatorMetrics) TaskUnparkedCounter(parkReason, class string) prometheus.Counter {
	return m.taskUnparkedTotal.WithLabelValues(parkReason, class)
}

// The closed {outcome} vocabulary of operator_mr_only_unpark_total. Constants,
// because the metric's label and the driver's two arms must not drift apart -
// the same rule RetryBlockerRead* and the StrandedPark* values follow.
const (
	MROnlyUnparkReleased = "unparked"
	MROnlyUnparkRefused  = "refused"
)

// MROnlyUnpark increments operator_mr_only_unpark_total for one maintainer
// comment answered on a Task that owns no Issue mirror. Nil-safe: a reconciler
// wired without metrics is a test, not an outage.
func (m *OperatorMetrics) MROnlyUnpark(project, parkReason, outcome string) {
	if m == nil || m.mrOnlyUnparkTotal == nil {
		return
	}
	m.mrOnlyUnparkTotal.WithLabelValues(project, parkReason, outcome).Inc()
}

// MROnlyUnparkCounter returns the operator_mr_only_unpark_total counter for
// (project,parkReason,outcome) for test assertions.
func (m *OperatorMetrics) MROnlyUnparkCounter(project, parkReason, outcome string) prometheus.Counter {
	return m.mrOnlyUnparkTotal.WithLabelValues(project, parkReason, outcome)
}

// UnparkDeclinedCounter returns the operator_unpark_declined_total counter for
// (parkReason,kind) for test assertions.
func (m *OperatorMetrics) UnparkDeclinedCounter(parkReason, kind string) prometheus.Counter {
	return m.unparkDeclinedTotal.WithLabelValues(parkReason, kind)
}

// SetLivePods sets operator_live_pods for one project.
func (m *OperatorMetrics) SetLivePods(project string, n float64) {
	if m == nil || m.livePods == nil {
		return
	}
	m.livePods.WithLabelValues(project).Set(n)
}

// LivePodsGauge returns the operator_live_pods gauge for a project.
func (m *OperatorMetrics) LivePodsGauge(project string) prometheus.Gauge {
	return m.livePods.WithLabelValues(project)
}

// LiveEntryDeclined counts a comment that did not reach a live agent pod. reason
// MUST be a member of controller.LiveEntryDeclineReasons.
func (m *OperatorMetrics) LiveEntryDeclined(project, reason string) {
	if m == nil || m.liveEntryDeclined == nil {
		return
	}
	m.liveEntryDeclined.WithLabelValues(project, reason).Inc()
}

// LiveEntryDeclinedCounter returns the operator_live_entry_declined_total counter.
func (m *OperatorMetrics) LiveEntryDeclinedCounter(project, reason string) prometheus.Counter {
	return m.liveEntryDeclined.WithLabelValues(project, reason)
}

// LiveClosed counts a conversation that ended, by cause.
func (m *OperatorMetrics) LiveClosed(project, cause string) {
	if m == nil || m.liveClosedTotal == nil {
		return
	}
	m.liveClosedTotal.WithLabelValues(project, cause).Inc()
}

// LiveClosedCounter returns the operator_live_closed_total counter.
func (m *OperatorMetrics) LiveClosedCounter(project, cause string) prometheus.Counter {
	return m.liveClosedTotal.WithLabelValues(project, cause)
}

// ResidencyExceeded counts a Task parked by the absolute residency backstop.
func (m *OperatorMetrics) ResidencyExceeded(state, kind string) {
	if m == nil || m.residencyExceededTotal == nil {
		return
	}
	m.residencyExceededTotal.WithLabelValues(state, kind).Inc()
}

// ResidencyExceededCounter returns the operator_task_residency_exceeded_total counter.
func (m *OperatorMetrics) ResidencyExceededCounter(state, kind string) prometheus.Counter {
	return m.residencyExceededTotal.WithLabelValues(state, kind)
}

// ParkedWithLivePodRepaired counts one repair of the park/live invariant.
func (m *OperatorMetrics) ParkedWithLivePodRepaired(project, parkReason string) {
	if m == nil || m.parkedLivePodRepaired == nil {
		return
	}
	m.parkedLivePodRepaired.WithLabelValues(project, parkReason).Inc()
}

// ParkedWithLivePodRepairedCounter returns the
// operator_task_parked_with_live_pod_repaired_total counter.
func (m *OperatorMetrics) ParkedWithLivePodRepairedCounter(project, parkReason string) prometheus.Counter {
	return m.parkedLivePodRepaired.WithLabelValues(project, parkReason)
}

// OrphanedTurnCleared counts one repair of the pod-scoped turn-annotation
// invariant by the poll backstop.
func (m *OperatorMetrics) OrphanedTurnCleared(project string) {
	if m == nil || m.orphanedTurnCleared == nil {
		return
	}
	m.orphanedTurnCleared.WithLabelValues(project).Inc()
}

// OrphanedTurnClearedCounter returns the
// operator_orphaned_turn_annotations_cleared_total counter.
func (m *OperatorMetrics) OrphanedTurnClearedCounter(project string) prometheus.Counter {
	return m.orphanedTurnCleared.WithLabelValues(project)
}

// SetBotRounds sets operator_bot_rounds for one project. The ONE caller is
// updateBotRoundsGauge's periodic per-project-maximum recompute
// (project_controller.go): a webhook-side imperative write here used to be
// last-writer-wins across every Task in a project (the gauge carries no task
// label) and was never revisited when a streak ended, so it latched at
// whatever bot-authored event happened to land last, forever - defeating the
// help text's own "highest" claim (2026-07-28 final review IMPORTANT 5). The
// periodic recompute is the single source of truth now.
func (m *OperatorMetrics) SetBotRounds(project string, n float64) {
	if m == nil || m.botRounds == nil {
		return
	}
	m.botRounds.WithLabelValues(project).Set(n)
}

// ResetBotRounds clears every operator_bot_rounds series. Called at the start
// of each updateBotRoundsGauge pass so a project whose live max has fallen
// (every conversation resolved, or the highest-round Task's streak was reset
// by a human comment) reads its current true value instead of retaining a
// stale high-water mark.
func (m *OperatorMetrics) ResetBotRounds() {
	if m == nil || m.botRounds == nil {
		return
	}
	m.botRounds.Reset()
}

// BotRoundsGauge returns the operator_bot_rounds gauge for a project.
func (m *OperatorMetrics) BotRoundsGauge(project string) prometheus.Gauge {
	return m.botRounds.WithLabelValues(project)
}
