package memory_test

import (
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

// TestEveryAlertCarriesTheProjectLabel is the structural guard for
// cross-Project notification dedup, and it covers every rule rather than the
// new ones only.
//
// A PrometheusRule is per-Project, but the ALERT is identified by its label set.
// Most of these expressions aggregate (sum, count, histogram_quantile over
// `by (le)`), and an aggregation without `by` returns a LABEL-LESS sample - so
// before this label, `MemoryPostgresInstancesBelowDeclared` firing for Project A
// and for Project B produced two alerts with the identical label set
// {alertname, severity}. Alertmanager sees one fingerprint and notifies once:
// Project B's critical is silently swallowed by Project A's.
//
// Keying that on expression shape is exactly the accident that produced the
// absent() bug. `project` makes it structural and testable instead.
func TestEveryAlertCarriesTheProjectLabel(t *testing.T) {
	// Three replicas and multi-instance postgres so the instance-gated rules
	// (replication topology, MemoryAPIReplicasBelowDeclared) are generated too.
	p := projectWithAPIReplicas("acme", 3)
	p.Spec.Memory.PgInstances = 3

	rules := apiRules(t, p)
	require.Greater(t, len(rules), 10, "sanity: the gated rules should be present")
	for name, r := range rules {
		require.Equal(t, "acme", r.Labels["project"],
			"alert %s has no project label: it would share an Alertmanager fingerprint with the other Projects' copy of the same rule", name)
	}
}

// TestMemoryAPIUnavailable is the page for total loss of a Project's API.
//
// It is NOT justified by "the Project phase no longer tracks this" - that would
// be false. memoryPhase still requires memoryAvail >= 1, so a Project serving
// 0 of 1 still demotes to Provisioning and eventually Degraded exactly as it
// did before; only PARTIAL loss stopped demoting. This rule earns its place on
// two other differences: the Degraded path only pages after the whole
// MemoryProvisioningTimeout (45m by default) has elapsed, and it reports the
// Project as unhealthy rather than naming the API as the failed component.
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
//
// The kube_deployment_created join is a warm-up guard, not decoration: a
// brand-new Project takes cnpg bootstrap plus a LightRAG round-trip before
// /readyz first succeeds, and routinely exceeds 10m. Without it, creating a
// Project is a guaranteed false CRITICAL. The old `up == 0` could not fire for a
// target that did not exist yet, so this is new noise that has to be bounded.
func TestMemoryAPIUnavailable(t *testing.T) {
	r, ok := apiRules(t, testProject("acme"))["MemoryAPIUnavailable"]
	require.True(t, ok, "MemoryAPIUnavailable must be generated at every replica count")

	require.Equal(t,
		`(kube_deployment_status_replicas_available{namespace="tatara", deployment="mem-acme"} == 0)`+
			` and on(namespace, deployment) (time() - kube_deployment_created{namespace="tatara", deployment="mem-acme"} > 2700)`,
		r.Expr.StrVal)
	require.Equal(t, "critical", r.Labels["severity"])
	require.Equal(t, "10m", string(*r.For))
}

// TestMemoryAPIReplicasBelowDeclared is the partial-loss signal.
//
// `< N` with NO `> 0` filter, deliberately. An earlier draft filtered total loss
// out so a dead Project would page critical once rather than critical plus
// warning. That created a blind spot with no signal anywhere: this rule and
// MemoryAPIUnavailable were then mutually exclusive, so an availability count
// oscillating between N-1 and 0 reset one For clock every time it satisfied the
// other and neither ever elapsed - which is the literal #528 mode, /readyz
// intermittently timing out against a cold LightRAG. A monotonic 3 -> 2 -> 0
// slide had the same gap. Overlapping windows and an occasional double page are
// the cheaper mistake.
//
// Still generated only above one replica: at one replica `< 1` IS
// MemoryAPIUnavailable's `== 0`, and a rule that duplicates its neighbour
// verbatim is tech debt (hard rule 4), the same reason the replication-topology
// rules are instance-gated.
func TestMemoryAPIReplicasBelowDeclared(t *testing.T) {
	t.Run("absent at one replica", func(t *testing.T) {
		_, ok := apiRules(t, testProject("acme"))["MemoryAPIReplicasBelowDeclared"]
		require.False(t, ok, "at one replica this rule is a verbatim duplicate of MemoryAPIUnavailable")
	})

	t.Run("present above one replica", func(t *testing.T) {
		r, ok := apiRules(t, projectWithAPIReplicas("acme", 3))["MemoryAPIReplicasBelowDeclared"]
		require.True(t, ok)
		require.Equal(t, `kube_deployment_status_replicas_available{namespace="tatara", deployment="mem-acme"} < 3`, r.Expr.StrVal)
		require.Equal(t, "warning", r.Labels["severity"])
		require.Equal(t, "15m", string(*r.For))
	})
}

// TestMemoryAPIRulesNotKeyedOnScrapeHealth forbids reintroducing the
// count(up{...} == 1) shape MemoryPostgresInstancesBelowDeclared correctly uses
// for cnpg. That shape is right there and wrong here, for the reason quoted on
// TestMemoryAPIUnavailable: on this stack up == 1 for a NotReady replica.
//
// It asserts a property the byte-for-byte expression tests above do not, so it
// is not a dead restatement of them: it holds for whatever those expressions
// become, which is the point of a guard.
func TestMemoryAPIRulesNotKeyedOnScrapeHealth(t *testing.T) {
	p := projectWithAPIReplicas("acme", 3)
	for _, name := range []string{"MemoryAPIUnavailable", "MemoryAPIReplicasBelowDeclared"} {
		r, ok := apiRules(t, p)[name]
		require.True(t, ok)
		require.NotContains(t, r.Expr.StrVal, "up{",
			"%s must not key on scrape health: up==1 is true for a NotReady replica on this stack", name)
	}
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
// evaluating a cluster-wide condition. The `service` label is the only one that
// distinguishes them.
//
// `max by (namespace, service) (...) == 0`, and deliberately NOT
// `absent(up{...} == 1)`, which was the first attempt at this fix and is wrong
// for a reason that is invisible until you run it. absent() derives its output
// labels from its argument only when that argument is a plain vector selector;
// given a filtered expression like `up{...} == 1` it can derive nothing and
// emits a LABEL-LESS sample. Verified live, and confirmed in the Prometheus
// source (createLabelsForAbsentFunction switches on VectorSelector /
// MatrixSelector only, and `up{...} == 1` is a BinaryExpr):
//
//	absent(up{job=~".*tatara-memory.*", namespace="tatara", service="mem-x"} == 1)  ->  {}
//	max by (namespace, service) (up{job=~".*tatara-memory.*", namespace="tatara"})
//	  ->  {namespace="tatara", service="mem-mtg"} 1, {..., service="mem-tatara"} 1, ...
func TestMemoryDownIsAggregatedAndPerProject(t *testing.T) {
	r := apiRules(t, projectWithAPIReplicas("acme", 3))["MemoryDown"]
	require.Equal(t, `max by (namespace, service) (up{job=~".*tatara-memory.*", namespace="tatara", service="mem-acme"}) == 0`, r.Expr.StrVal)
	require.Equal(t, "critical", r.Labels["severity"])
}
