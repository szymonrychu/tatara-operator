package memory_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/szymonrychu/tatara-operator/internal/memory"
)

func testMonitorCfg() memory.Config {
	cfg := testCfg()
	cfg.MonitorEnabled = true
	cfg.MonitorLabels = map[string]string{"release": "prometheus"}
	return cfg
}

func TestMemoryServiceMonitor(t *testing.T) {
	p := testProject("acme")
	sm := memory.MemoryServiceMonitor(p, testMonitorCfg())

	require.Equal(t, "mem-acme", sm.Name)
	require.Equal(t, "tatara", sm.Namespace)
	require.Equal(t, "ServiceMonitor", sm.Kind)
	require.Equal(t, "monitoring.coreos.com/v1", sm.APIVersion)
	require.Len(t, sm.OwnerReferences, 1)
	require.True(t, *sm.OwnerReferences[0].Controller)

	// The cluster serviceMonitorSelector label must be stamped on the object so
	// Prometheus discovers it; without it the stack stays unscraped (issue #200).
	require.Equal(t, "prometheus", sm.Labels["release"])
	// The pin-set name label is preserved.
	require.Equal(t, "tatara-memory", sm.Labels["app.kubernetes.io/name"])

	// jobLabel pins the scrape job to "tatara-memory" so up{job=~".*tatara-memory.*"}
	// matches even though the Service is named mem-acme.
	require.Equal(t, "app.kubernetes.io/name", sm.Spec.JobLabel)

	// Selector targets only the memory Service (component=memory), not neo4j/lightrag.
	require.Equal(t, "mem-acme", sm.Spec.Selector.MatchLabels["app.kubernetes.io/instance"])
	require.Equal(t, "memory", sm.Spec.Selector.MatchLabels["app.kubernetes.io/component"])
	require.Equal(t, []string{"tatara"}, sm.Spec.NamespaceSelector.MatchNames)

	require.Len(t, sm.Spec.Endpoints, 1)
	require.Equal(t, "http", sm.Spec.Endpoints[0].Port)
	require.Equal(t, "/metrics", sm.Spec.Endpoints[0].Path)
}

// The MemoryService must carry the component label the ServiceMonitor selects
// on, or the ServiceMonitor matches nothing. Guards the two builders together.
func TestMemoryServiceMonitor_MatchesMemoryService(t *testing.T) {
	p := testProject("acme")
	svc := memory.MemoryService(p, testMonitorCfg())
	sm := memory.MemoryServiceMonitor(p, testMonitorCfg())

	for k, v := range sm.Spec.Selector.MatchLabels {
		require.Equal(t, v, svc.Labels[k], "memory Service is missing selector label %q the ServiceMonitor requires", k)
	}
}

func TestPGPodMonitor(t *testing.T) {
	p := testProject("acme")
	pm := memory.PGPodMonitor(p, testMonitorCfg())

	require.Equal(t, "mem-acme-pg", pm.Name)
	require.Equal(t, "tatara", pm.Namespace)
	require.Equal(t, "PodMonitor", pm.Kind)
	require.Equal(t, "monitoring.coreos.com/v1", pm.APIVersion)
	require.Len(t, pm.OwnerReferences, 1)
	require.True(t, *pm.OwnerReferences[0].Controller)

	// The cluster podMonitorSelector label must be stamped so Prometheus discovers it.
	require.Equal(t, "prometheus", pm.Labels["release"])

	// Selector targets the cnpg pods of THIS cluster (cnpg.io/cluster=<cluster>).
	require.Equal(t, "mem-acme-pg", pm.Spec.Selector.MatchLabels["cnpg.io/cluster"])
	require.Equal(t, []string{"tatara"}, pm.Spec.NamespaceSelector.MatchNames)

	require.Len(t, pm.Spec.PodMetricsEndpoints, 1)
	require.NotNil(t, pm.Spec.PodMetricsEndpoints[0].Port)
	require.Equal(t, "metrics", *pm.Spec.PodMetricsEndpoints[0].Port)
	require.Equal(t, "/metrics", pm.Spec.PodMetricsEndpoints[0].Path)
}

func TestMemoryPrometheusRule(t *testing.T) {
	p := testProject("acme")
	pr := memory.MemoryPrometheusRule(p, testMonitorCfg())

	require.Equal(t, "mem-acme", pr.Name)
	require.Equal(t, "tatara", pr.Namespace)
	require.Equal(t, "PrometheusRule", pr.Kind)
	require.Equal(t, "monitoring.coreos.com/v1", pr.APIVersion)
	require.Len(t, pr.OwnerReferences, 1)

	// ruleSelector label stamped so the rules load rather than being dropped.
	require.Equal(t, "prometheus", pr.Labels["release"])

	require.Len(t, pr.Spec.Groups, 1)
	g := pr.Spec.Groups[0]
	require.Equal(t, "tatara-memory.rules", g.Name)

	bySeverity := map[string]string{}
	for _, r := range g.Rules {
		bySeverity[r.Alert] = r.Labels["severity"]
		require.NotEmpty(t, r.Expr.StrVal, "alert %s has empty expr", r.Alert)
		require.Contains(t, r.Annotations, "summary", "alert %s missing summary", r.Alert)
	}
	// The ported alert set must be present with the chart's severities.
	require.Equal(t, "critical", bySeverity["MemoryDown"], "MemoryDown deadman must be critical")
	for _, a := range []string{"MemoryHigh5xx", "MemoryLightragErrors", "MemoryIngestJobsFailing", "MemoryRetrievalLatencyHigh", "MemoryHandlerPanics"} {
		require.Equal(t, "warning", bySeverity[a], "alert %s must be warning", a)
	}
}

// Empty MonitorLabels (the cluster-agnostic default) must not stamp any extra
// label and must not panic on a nil map.
func TestMemoryMonitoring_NoExtraLabelsByDefault(t *testing.T) {
	p := testProject("acme")
	cfg := testCfg() // MonitorLabels nil
	sm := memory.MemoryServiceMonitor(p, cfg)
	pr := memory.MemoryPrometheusRule(p, cfg)
	require.NotContains(t, sm.Labels, "release")
	require.NotContains(t, pr.Labels, "release")
	// Pin-set labels still present.
	require.Equal(t, "tatara-memory", sm.Labels["app.kubernetes.io/name"])
}
