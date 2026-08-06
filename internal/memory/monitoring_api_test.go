package memory_test

import (
	"strings"
	"testing"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/memory"
)

// apiRules indexes a Project's generated rules by alert name.
func apiRules(t *testing.T, p *tatarav1alpha1.Project) map[string]monitoringv1.Rule {
	t.Helper()
	out := map[string]monitoringv1.Rule{}
	for _, r := range memory.MemoryPrometheusRule(p, testMonitorCfg()).Spec.Groups[0].Rules {
		out[r.Alert] = r
	}
	return out
}

// TestMemoryAPIUnavailable is the replacement page for total API loss.
//
// It exists because deploymentTemplateConverged no longer demands
// AvailableReplicas == Replicas: a Project serving 0 of 1, or 2 of 3, now stays
// Ready as far as the Project phase is concerned, so the only remaining signal
// of API loss is a Prometheus rule.
//
// It keys on kube-state-metrics, NOT on `count(up{...} == 1)`. The API serves
// /metrics from the same HTTP server as /readyz, and the ServiceMonitor keeps
// scraping a pod that has already been dropped from the Service's endpoints, so
// `up == 1` is TRUE for a NotReady replica. Verified against the incident window:
//
//	count(up{job="tatara-memory"} == 1 and on(pod) (kube_pod_status_ready{condition="true"} == 0))
//	2026-08-01T19:10:00Z -> 1
//
// A count(up == 1) rule would have scored the exact 2026-08-01 failure as healthy.
func TestMemoryAPIUnavailable(t *testing.T) {
	r, ok := apiRules(t, testProject("acme"))["MemoryAPIUnavailable"]
	require.True(t, ok, "MemoryAPIUnavailable must be generated at every replica count")

	require.Equal(t, `kube_deployment_status_replicas_available{namespace="tatara", deployment="mem-acme"} == 0`, r.Expr.StrVal)
	require.Equal(t, "critical", r.Labels["severity"])
	require.Equal(t, "10m", string(*r.For))
}

// TestMemoryAPIUnavailable_NotKeyedOnScrapeHealth is a regression guard, not a
// duplicate of the above: it forbids reintroducing the count(up{...} == 1)
// shape MemoryPostgresInstancesBelowDeclared correctly uses for cnpg. That
// shape is right there and wrong here, for the reason quoted above.
func TestMemoryAPIUnavailable_NotKeyedOnScrapeHealth(t *testing.T) {
	rules := apiRules(t, projectWithAPIReplicas("acme", 3))
	for _, name := range []string{"MemoryAPIUnavailable", "MemoryAPIReplicasBelowDeclared"} {
		expr := rules[name].Expr.StrVal
		require.NotContains(t, expr, "count(", "%s must not aggregate: count() returns a LABEL-LESS sample, so all three Projects would collapse onto one Alertmanager fingerprint and dedupe into a single notification", name)
		require.NotContains(t, expr, "up{", "%s must not key on scrape health: up==1 is true for a NotReady replica on this stack", name)
		require.Contains(t, expr, `namespace="tatara"`, "%s must carry KSM's namespace label", name)
		require.Contains(t, expr, `deployment="mem-acme"`, "%s must carry KSM's deployment label so each Project gets its own fingerprint", name)
	}
}

// TestMemoryAPIReplicasBelowDeclared is the PARTIAL-loss signal, and only that.
//
// It is generated only above one replica: at one replica `< 1` is identical to
// MemoryAPIUnavailable's `== 0`, and a rule that is inert-by-construction or a
// verbatim duplicate of its neighbour is tech debt (hard rule 4).
//
// The `> 0` filter keeps total loss out of it, so a dead Project pages critical
// once instead of critical plus warning.
func TestMemoryAPIReplicasBelowDeclared(t *testing.T) {
	t.Run("absent at one replica", func(t *testing.T) {
		_, ok := apiRules(t, testProject("acme"))["MemoryAPIReplicasBelowDeclared"]
		require.False(t, ok, "at one replica this rule is a verbatim duplicate of MemoryAPIUnavailable")
	})

	t.Run("present above one replica", func(t *testing.T) {
		r, ok := apiRules(t, projectWithAPIReplicas("acme", 3))["MemoryAPIReplicasBelowDeclared"]
		require.True(t, ok)
		require.Equal(t, `(kube_deployment_status_replicas_available{namespace="tatara", deployment="mem-acme"} > 0) < 3`, r.Expr.StrVal)
		require.Equal(t, "warning", r.Labels["severity"])
		require.Equal(t, "15m", string(*r.For))
	})
}

// TestMemoryDownIsAggregatedAndPerProject covers the finding that opting into
// the new apiReplicas knob would otherwise turn a correct rule into a false one.
//
// `up{job=~".*tatara-memory.*"} == 0` is unaggregated, so it yields one series
// per scrape target. At apiReplicas: 3 a single replica whose node is rebooting
// - the literal 2026-08-01 scenario - fires a CRITICAL page whose description
// reads "No tatara-memory instance has been scrapeable", while two replicas
// serve every request.
//
// It was also cluster-wide: `jobLabel` pins job="tatara-memory" for all three
// Projects in one namespace (verified live), so a per-Project PrometheusRule was
// evaluating a cluster-wide condition. absent() over a service-scoped selector
// fixes both: it collapses to one series, and it scopes to this Project.
func TestMemoryDownIsAggregatedAndPerProject(t *testing.T) {
	r := apiRules(t, projectWithAPIReplicas("acme", 3))["MemoryDown"]
	require.Equal(t, `absent(up{job=~".*tatara-memory.*", namespace="tatara", service="mem-acme"} == 1)`, r.Expr.StrVal)
	require.Equal(t, "critical", r.Labels["severity"])
	require.False(t, strings.Contains(r.Expr.StrVal, "== 0"),
		"the unaggregated `up == 0` form yields one series per pod and false-pages when one replica of N is down")
}
