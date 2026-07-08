package obs

import "github.com/prometheus/client_golang/prometheus"

// scmMetrics holds the SCM-facing Prometheus collectors, embedded into
// OperatorMetrics.
type scmMetrics struct {
	scmWritesTotal       *prometheus.CounterVec
	scmReqErrorsByStatus *prometheus.CounterVec
}

// newSCMMetrics registers the SCM collectors on reg and returns the bundle.
func newSCMMetrics(reg prometheus.Registerer) *scmMetrics {
	m := &scmMetrics{
		scmWritesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_scm_writes_total",
			Help: "Total SCM operations by provider, verb, kind (read|write), and result (ok|error|blocked|gone|suppressed_last_word|suppressed_bot_mr).",
		}, []string{"provider", "verb", "kind", "result"}),
		scmReqErrorsByStatus: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "operator_scm_request_errors_by_status_total",
			Help: "SCM operations that errored, by provider, verb, and classified status (HTTP code, or \"network\" for connect/timeout failures).",
		}, []string{"provider", "verb", "status"}),
	}
	reg.MustRegister(
		m.scmWritesTotal,
		m.scmReqErrorsByStatus,
	)
	return m
}

// scmReadVerbs are SCM verbs that only read; every other verb is a write. Used
// to label operator_scm_writes_total with kind so the write-failure ratio alert
// is not diluted by read traffic that shares the same credentials.
var scmReadVerbs = map[string]bool{
	"list_open_issues":     true,
	"list_open_prs":        true,
	"get_pr_state":         true,
	"get_commit_ci_status": true,
}

// SCMVerbKind returns "read" or "write" for an SCM verb.
func SCMVerbKind(verb string) string {
	if scmReadVerbs[verb] {
		return "read"
	}
	return "write"
}

// SCMWrite increments operator_scm_writes_total for the given provider, verb,
// and result ("ok", "error", or "blocked"). The kind label (read|write) is
// derived from the verb.
func (m *scmMetrics) SCMWrite(provider, verb, result string) {
	m.scmWritesTotal.WithLabelValues(provider, verb, SCMVerbKind(verb), result).Inc()
}

// SCMRequestErrorByStatus increments operator_scm_request_errors_by_status_total,
// recording the classified status (HTTP code or "network") behind an SCM error.
func (m *scmMetrics) SCMRequestErrorByStatus(provider, verb, status string) {
	m.scmReqErrorsByStatus.WithLabelValues(provider, verb, status).Inc()
}

// SCMWriteCounter returns the counter for (provider, verb, result) for test assertions.
func (m *scmMetrics) SCMWriteCounter(provider, verb, result string) prometheus.Counter {
	return m.scmWritesTotal.WithLabelValues(provider, verb, SCMVerbKind(verb), result)
}
