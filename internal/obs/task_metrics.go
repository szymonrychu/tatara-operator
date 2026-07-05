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
			Help: "Tasks reaching a terminal phase by kind, phase (Succeeded|Failed), and condition reason.",
		}, []string{"kind", "phase", "reason"}),
		taskTerminalTokensTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_task_terminal_tokens_total",
			Help: "Cumulative agent token usage of terminated Tasks by project, repo, terminal outcome (delivered|churned|abandoned), model, and type (input|output|cache_read|cache_creation). No issue label - churn is outcome-keyed, not issue-keyed.",
		}, []string{"project", "repo", "outcome", "model", "type"}),
	}
	reg.MustRegister(
		m.taskTokensTotal,
		m.taskTurnsTotal,
		m.taskIssueState,
		m.taskTerminalTotal,
		m.taskTerminalTokensTotal,
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
func (m *taskMetrics) DeleteTaskSeries(project, repo, kind, issue, model string) {
	m.taskTokensTotal.DeleteLabelValues(project, repo, kind, issue, model, "input")
	m.taskTokensTotal.DeleteLabelValues(project, repo, kind, issue, model, "output")
	m.taskTokensTotal.DeleteLabelValues(project, repo, kind, issue, model, "cache_read")
	m.taskTokensTotal.DeleteLabelValues(project, repo, kind, issue, model, "cache_creation")
	m.taskTurnsTotal.DeleteLabelValues(project, repo, kind, issue)
}

// TaskTerminal increments operator_task_terminal_total for a Task reaching a
// terminal phase ("Succeeded" or "Failed") with the given kind and the condition
// reason recorded on the terminal transition. This is the uniform loop
// success/failure denominator: every terminal transition is metered here exactly
// once, including failure paths (PodLost, TurnTimeout, PlanningStalled, ...) that
// the per-reason fault counters do not all cover.
func (m *taskMetrics) TaskTerminal(kind, phase, reason string) {
	m.taskTerminalTotal.WithLabelValues(kind, phase, reason).Inc()
}
