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
			Help: "Tasks reaching a terminal stage by kind, stage (delivered|failed|rejected|parked), and stage reason.",
		}, []string{"kind", "stage", "stageReason"}),
		taskTerminalTokensTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_task_terminal_tokens_total",
			Help: "Cumulative agent token usage of terminated Tasks by project, repo, terminal outcome (delivered|churned|abandoned), model, and type (input|output|cache_read|cache_creation). No issue label - churn is outcome-keyed, not issue-keyed.",
		}, []string{"project", "repo", "outcome", "model", "type"}),
		taskStage: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "operator_task_stage",
			Help: "Live Tasks currently in a given stage, by stage and kind (contract K.1). Value is the COUNT of Tasks in that (stage,kind) bucket, not per-task.",
		}, []string{"stage", "kind"}),
		taskStageAge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "operator_task_stage_age_seconds",
			Help: "Seconds since a live Task entered its current stage (contract K.1), by task, stage, and kind.",
		}, []string{"task", "stage", "kind"}),
		taskParkedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_task_parked_total",
			Help: "Park transitions (contract K.1), by the stage the Task parked FROM (the stalling stage) and the park stageReason. Incremented once per park transition, never on a mint.",
		}, []string{"stage", "stageReason"}),
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
			Help: "F.6 re-entry declines by ApplyUnpark, by the Task's park stageReason and decline kind: " +
				"guard (the live Task had already drifted from what the caller believed was parked - rare, " +
				"anomalous) or rule (stage.Unpark's re-entry rule was not satisfied yet - normal steady state).",
		}, []string{"stageReason", "kind"}),
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
	)
	return m
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
func (m *taskMetrics) TaskTerminal(kind, stage, stageReason string) {
	m.taskTerminalTotal.WithLabelValues(kind, stage, stageReason).Inc()
}

// ResetTaskStageGauges clears operator_task_stage and
// operator_task_stage_age_seconds so a recompute pass leaves no stale series
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

// SetTaskStage sets operator_task_stage{stage,kind} to the live COUNT of Tasks
// in that bucket.
func (m *OperatorMetrics) SetTaskStage(stage, kind string, count float64) {
	if m == nil || m.taskStage == nil {
		return
	}
	m.taskStage.WithLabelValues(stage, kind).Set(count)
}

// SetTaskStageAge sets operator_task_stage_age_seconds{task,stage,kind} to
// ageSeconds, the time since that Task entered its current stage.
func (m *OperatorMetrics) SetTaskStageAge(task, stage, kind string, ageSeconds float64) {
	if m == nil || m.taskStageAge == nil {
		return
	}
	m.taskStageAge.WithLabelValues(task, stage, kind).Set(ageSeconds)
}

// TaskParked increments operator_task_parked_total for one park transition.
// stage is the stage the Task parked FROM (the stalling stage); stageReason is
// the park reason. Nil-safe: EnterStage calls it unconditionally, and a
// reconciler wired without metrics is a test, not an outage.
func (m *OperatorMetrics) TaskParked(stage, stageReason string) {
	if m == nil || m.taskParkedTotal == nil {
		return
	}
	m.taskParkedTotal.WithLabelValues(stage, stageReason).Inc()
}

// OrphanAdopted increments operator_orphan_adopted_total for one orphan work
// item (issue or PR) the sweep minted a Task for.
func (m *OperatorMetrics) OrphanAdopted(kind string) {
	if m == nil || m.orphanAdoptedTotal == nil {
		return
	}
	m.orphanAdoptedTotal.WithLabelValues(kind).Inc()
}

// TaskStageGauge returns the operator_task_stage gauge for (stage,kind) for
// test assertions.
func (m *OperatorMetrics) TaskStageGauge(stage, kind string) prometheus.Gauge {
	return m.taskStage.WithLabelValues(stage, kind)
}

// TaskStageAgeGauge returns the operator_task_stage_age_seconds gauge for
// (task,stage,kind) for test assertions.
func (m *OperatorMetrics) TaskStageAgeGauge(task, stage, kind string) prometheus.Gauge {
	return m.taskStageAge.WithLabelValues(task, stage, kind)
}

// TaskParkedCounter returns the operator_task_parked_total counter for
// (stage,stageReason) for test assertions.
func (m *OperatorMetrics) TaskParkedCounter(stage, stageReason string) prometheus.Counter {
	return m.taskParkedTotal.WithLabelValues(stage, stageReason)
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

// UnparkDeclinedCounter returns the operator_unpark_declined_total counter for
// (stageReason,kind) for test assertions.
func (m *OperatorMetrics) UnparkDeclinedCounter(stageReason, kind string) prometheus.Counter {
	return m.unparkDeclinedTotal.WithLabelValues(stageReason, kind)
}
