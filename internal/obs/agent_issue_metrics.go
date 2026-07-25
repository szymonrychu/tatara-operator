package obs

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// AgentInternalIssueTotal counts report_internal_issue calls agents made during a
// turn, as reported by the wrapper's transcript Tailer on the turn-complete
// callback. This is the operator's OWN event, decided in operator code, so hard
// rule 13 requires a counter for it - the alert that used to derive this from a
// `pattern | line_format | json` LogQL pipeline (~9.6s over ~50 lines, paging on
// its own query timeout, tatara-observability#63) now reads this instead. Logs
// remain the drill-down path and keep the free-text description; description is
// NOT a label here, it belongs in the drill-down query, not in a label set.
// category/severity are the wrapper's already-clamped enum values (see
// controller.InternalIssueReport), never raw agent input.
//
// Deliberately NOT pre-seeded: the category enum lives in the wrapper, not here,
// and the alert is `increase(...) > 0` with no `or vector(0)`, so an absent
// series is NoData -> the file's default_no_data_state: "OK", which is the
// correct reading of "no agent reported anything".
var AgentInternalIssueTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "agent_internal_issue_total",
	Help: "Internal issues agents reported during a turn, by category and severity.",
}, []string{"category", "severity"})

func init() {
	ctrlmetrics.Registry.MustRegister(AgentInternalIssueTotal)
}
