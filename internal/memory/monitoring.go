package memory

import (
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// Memory-stack alert thresholds and scrape cadence. Ported verbatim from the
// tatara-memory chart (tatara-memory#58, commit 313834d5): the operator
// provisions the memory stack as native objects and never installs that chart,
// so the alerts it shipped are inert unless the operator emits them here too
// (issue #200). Per-cluster threshold tuning is a deferred follow-up; these
// match the chart's defaults so behaviour is identical to the intended deploy.
const (
	memoryHTTP5xxRatio        = "0.05"
	memoryRetrievalLatencyP99 = "2.5"
	memorySeverityWarning     = "warning"
	memorySeverityCritical    = "critical"
	memoryScrapeInterval      = monitoringv1.Duration("30s")
	memoryScrapeTimeout       = monitoringv1.Duration("10s")
)

// dur returns a *monitoringv1.Duration for a rule "for"/group "interval" field.
func dur(d string) *monitoringv1.Duration {
	v := monitoringv1.Duration(d)
	return &v
}

// MemoryServiceMonitor builds the per-Project ServiceMonitor that scrapes the
// tatara-memory Service /metrics endpoint. Two non-obvious choices:
//
//   - jobLabel pins the scrape `job` label to the Service's
//     app.kubernetes.io/name ("tatara-memory") so the alert exprs
//     (job=~".*tatara-memory.*") match even though the Service is named
//     mem-<project>; without it the default `job` would be the Service name and
//     up{job=~".*tatara-memory.*"} would stay 0.
//   - the selector targets only the memory Service (component=memory): neo4j and
//     lightrag carry the same pin-set labels and also expose a port named "http"
//     (on 7474 / 9621), so a looser selector would scrape their non-metrics
//     ports.
func MemoryServiceMonitor(p *tatarav1alpha1.Project, cfg Config) *monitoringv1.ServiceMonitor {
	n := NamesFor(p.Name)
	return &monitoringv1.ServiceMonitor{
		TypeMeta: metav1.TypeMeta{
			APIVersion: monitoringv1.SchemeGroupVersion.String(),
			Kind:       monitoringv1.ServiceMonitorsKind,
		},
		ObjectMeta: monitorObjectMeta(p, cfg, n.Memory),
		Spec: monitoringv1.ServiceMonitorSpec{
			JobLabel: "app.kubernetes.io/name",
			NamespaceSelector: monitoringv1.NamespaceSelector{
				MatchNames: []string{cfg.Namespace},
			},
			Selector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app.kubernetes.io/instance":  "mem-" + p.Name,
					"app.kubernetes.io/component": "memory",
				},
			},
			Endpoints: []monitoringv1.Endpoint{{
				Port:          "http",
				Path:          "/metrics",
				Interval:      memoryScrapeInterval,
				ScrapeTimeout: memoryScrapeTimeout,
			}},
		},
	}
}

// MemoryPrometheusRule builds the per-Project PrometheusRule carrying the
// tatara-memory alert groups (ported from tatara-memory#58). The cluster
// ruleSelector label is stamped via monitorObjectMeta so the rules are actually
// loaded rather than silently dropped.
func MemoryPrometheusRule(p *tatarav1alpha1.Project, cfg Config) *monitoringv1.PrometheusRule {
	n := NamesFor(p.Name)
	return &monitoringv1.PrometheusRule{
		TypeMeta: metav1.TypeMeta{
			APIVersion: monitoringv1.SchemeGroupVersion.String(),
			Kind:       monitoringv1.PrometheusRuleKind,
		},
		ObjectMeta: monitorObjectMeta(p, cfg, n.Memory),
		Spec: monitoringv1.PrometheusRuleSpec{
			Groups: []monitoringv1.RuleGroup{{
				Name:  "tatara-memory.rules",
				Rules: memoryAlertRules(),
			}},
		},
	}
}

// memoryAlertRules is the ported alert set. Kept as a function (not a package
// var) so each PrometheusRule gets its own slice and callers cannot mutate a
// shared one.
func memoryAlertRules() []monitoringv1.Rule {
	return []monitoringv1.Rule{
		{
			// Class-A deadman: the recall backbone has no scrape target up.
			Alert:  "MemoryDown",
			Expr:   intstr.FromString(`up{job=~".*tatara-memory.*"} == 0`),
			For:    dur("5m"),
			Labels: map[string]string{"severity": memorySeverityCritical},
			Annotations: map[string]string{
				"summary":     "tatara-memory is down (no scrape target up)",
				"description": "No tatara-memory instance has been scrapeable for 5m. The recall/retrieval backbone of the autonomous loop is unavailable.",
			},
		},
		{
			// The `status` label is http.StatusText(code) (a human string), not a
			// numeric code, so match the 5xx StatusText family rather than "5..".
			Alert: "MemoryHigh5xx",
			Expr: intstr.FromString(
				`(sum(rate(http_requests_total{job=~".*tatara-memory.*", status=~"Internal Server Error|Not Implemented|Bad Gateway|Service Unavailable|Gateway Timeout|HTTP Version Not Supported|Variant Also Negotiates|Insufficient Storage|Loop Detected|Not Extended|Network Authentication Required"}[5m]))` +
					` / clamp_min(sum(rate(http_requests_total{job=~".*tatara-memory.*"}[5m])), 1)) > ` + memoryHTTP5xxRatio,
			),
			For:    dur("10m"),
			Labels: map[string]string{"severity": memorySeverityWarning},
			Annotations: map[string]string{
				"summary":     "tatara-memory serving elevated 5xx",
				"description": "More than " + memoryHTTP5xxRatio + " of tatara-memory HTTP responses are server errors over the last 5m (sustained 10m).",
			},
		},
		{
			// Upstream recall failures: lightrag client calls returning result="error".
			Alert:  "MemoryLightragErrors",
			Expr:   intstr.FromString(`sum(increase(lightrag_calls_total{job=~".*tatara-memory.*", result="error"}[15m])) > 0`),
			For:    dur("0m"),
			Labels: map[string]string{"severity": memorySeverityWarning},
			Annotations: map[string]string{
				"summary":     "tatara-memory LightRAG upstream is erroring",
				"description": "tatara-memory recorded LightRAG client errors in the last 15m. The upstream recall path is degraded.",
			},
		},
		{
			Alert:  "MemoryIngestJobsFailing",
			Expr:   intstr.FromString(`sum(increase(ingest_jobs_total{job=~".*tatara-memory.*", status="failed"}[1h])) > 0`),
			For:    dur("0m"),
			Labels: map[string]string{"severity": memorySeverityWarning},
			Annotations: map[string]string{
				"summary":     "tatara-memory ingest jobs are failing",
				"description": "tatara-memory finalized one or more ingest jobs with status=\"failed\" in the last hour. New memories may not be getting indexed.",
			},
		},
		{
			// p99 over DefBuckets (largest finite bucket 10s); keep the threshold
			// below 10s or the quantile saturates and can never exceed it.
			Alert: "MemoryRetrievalLatencyHigh",
			Expr: intstr.FromString(
				`histogram_quantile(0.99, sum by (le) (rate(http_request_duration_seconds_bucket{job=~".*tatara-memory.*"}[5m]))) > ` + memoryRetrievalLatencyP99,
			),
			For:    dur("15m"),
			Labels: map[string]string{"severity": memorySeverityWarning},
			Annotations: map[string]string{
				"summary":     "tatara-memory request latency is high",
				"description": "tatara-memory p99 request latency has exceeded " + memoryRetrievalLatencyP99 + "s for 15m.",
			},
		},
		{
			Alert:  "MemoryHandlerPanics",
			Expr:   intstr.FromString(`sum(increase(http_panics_total{job=~".*tatara-memory.*"}[15m])) > 0`),
			For:    dur("0m"),
			Labels: map[string]string{"severity": memorySeverityWarning},
			Annotations: map[string]string{
				"summary":     "tatara-memory HTTP handler panicked",
				"description": "tatara-memory recovered one or more HTTP handler panics in the last 15m. A code path is wedging requests.",
			},
		},
	}
}
