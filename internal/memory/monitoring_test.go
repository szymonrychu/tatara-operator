package memory_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/memory"
)

// testProjectWithInstances is testProject plus an explicit pgInstances, so
// replication-topology tests can exercise a multi-instance HA cluster instead
// of the single-instance default.
func testProjectWithInstances(name string, n int) *tatarav1alpha1.Project {
	p := testProject(name)
	p.Spec.Memory = &tatarav1alpha1.MemorySpec{PgInstances: n}
	return p
}

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
	// The postgres-layer alerts (added for issue #252) must be present and warning.
	for _, a := range []string{"MemoryPostgresVolumeFilling", "MemoryPostgresInstanceRestarting"} {
		require.Equal(t, "warning", bySeverity[a], "alert %s must be warning", a)
	}
	// The replication alerts (added for issues #442, #444, #448) must be present.
	// MemoryPostgresInstancesBelowDeclared applies at any instance count (including
	// the single-instance default "acme" project used here) and is critical: it is
	// the general redundancy-count safety net.
	require.Equal(t, "critical", bySeverity["MemoryPostgresInstancesBelowDeclared"], "MemoryPostgresInstancesBelowDeclared must be critical")
}

// A single-instance project (the CR default, PgInstances unset/1) has no
// standbys and therefore no replication slots or streaming replicas to alert
// on. The three replication-topology alerts must NOT be generated for it - a
// literal "< 0" or absent-series expression would be inert-by-construction
// tech debt (hard rule 4), not a real guard. Only the general
// MemoryPostgresInstancesBelowDeclared alert, which is meaningful even at
// instances=1 (the lone instance itself disappearing), must be present.
func TestMemoryPrometheusRule_SingleInstance_NoReplicationTopologyAlerts(t *testing.T) {
	p := testProject("acme") // defaults to PgInstances=1
	pr := memory.MemoryPrometheusRule(p, testMonitorCfg())

	names := map[string]bool{}
	for _, r := range pr.Spec.Groups[0].Rules {
		names[r.Alert] = true
	}

	require.True(t, names["MemoryPostgresInstancesBelowDeclared"], "MemoryPostgresInstancesBelowDeclared must exist even for a single-instance project")
	for _, a := range []string{
		"MemoryPostgresReplicationSlotInactive",
		"MemoryPostgresStreamingReplicasBelowExpected",
		"MemoryPostgresReplicationSlotWalRetentionHigh",
		"MemoryPostgresStandbyReplayStalled",
	} {
		require.False(t, names[a], "alert %s must NOT be generated for a single-instance project (no standbys, no slots)", a)
	}
	// MemoryPostgresReplicationLagHigh was tried and removed entirely (not
	// merely instance-guarded) - see MEMORY.md 2026-07-26 - so it must never
	// appear regardless of instance count.
	require.False(t, names["MemoryPostgresReplicationLagHigh"], "MemoryPostgresReplicationLagHigh was removed and must never be generated")

	// Full expression equality, not a substring check: `Contains(..., "< 1")`
	// would also pass on "< 10" or "< 12", silently letting a wrong threshold
	// through. up{}==1 is the scrape-reachability primitive (not a cnpg_pg_*
	// metric - see memoryAlertRules' doc comment for why), so it stays live
	// even for a single-instance project with no replication topology at all.
	byName := map[string]string{}
	for _, r := range pr.Spec.Groups[0].Rules {
		byName[r.Alert] = r.Expr.StrVal
	}
	require.Equal(t,
		`(count(up{namespace="tatara", pod=~"mem-acme-pg-.*", container="postgres"} == 1) or vector(0)) < 1`,
		byName["MemoryPostgresInstancesBelowDeclared"],
	)
}

// A multi-instance (HA) project must get the full replication alert set,
// correctly scoped to this cluster and thresholded off its declared instance
// count - the mechanism and metric names are lifted directly from the
// evidence in issues #442, #444 and #448.
func TestMemoryPrometheusRule_ReplicationAlerts_MultiInstance(t *testing.T) {
	p := testProjectWithInstances("acme", 3)
	pr := memory.MemoryPrometheusRule(p, testMonitorCfg())

	byName := map[string]string{}
	bySeverity := map[string]string{}
	byFor := map[string]string{}
	for _, r := range pr.Spec.Groups[0].Rules {
		byName[r.Alert] = r.Expr.StrVal
		bySeverity[r.Alert] = r.Labels["severity"]
		if r.For != nil {
			byFor[r.Alert] = string(*r.For)
		}
	}

	// Full expression equality throughout, not substring checks: a `Contains`
	// check on e.g. "< 2" would also pass on "< 20" or "< 25", silently letting
	// a wrong threshold or a malformed expression through - exactly the
	// "silently never fires" failure class this whole change exists to fix.
	// No PromQL parser is available in this module's dependency graph (checked
	// go.mod/go.sum and the module cache for prometheus/prometheus's
	// promql/parser or an equivalent; none vendored), so expression validity is
	// asserted via exact string equality against a known-good expression
	// instead of a parse check.

	// MemoryPostgresInstancesBelowDeclared: fewer instances of this cluster
	// have a healthy scrape target (up{}==1) than the Project declares (3).
	// Deliberately NOT a cnpg_pg_* metric (see the doc comment on
	// memoryAlertRules for the live-verified reason). `or vector(0)` guards a
	// wholly-absent metric set (every instance gone) comparing as 0.
	require.Equal(t,
		`(count(up{namespace="tatara", pod=~"mem-acme-pg-.*", container="postgres"} == 1) or vector(0)) < 3`,
		byName["MemoryPostgresInstancesBelowDeclared"],
	)
	require.Equal(t, "10m", byFor["MemoryPostgresInstancesBelowDeclared"])
	require.Equal(t, "critical", bySeverity["MemoryPostgresInstancesBelowDeclared"])

	// primaryGuard is the `and on(pod) (...)` clause every slot/streaming-
	// replica rule now carries, restricting the read to whichever pod is
	// CURRENTLY the primary. Live-verified necessary, not gold-plating: an
	// unscoped read picked up a non-primary standby's stale/wrong view of a
	// sibling's slot at >2GiB on a cluster whose real primary reported 0 -
	// see MEMORY.md 2026-07-26 for the live evidence.
	primaryGuard := `and on(pod) (cnpg_pg_replication_in_recovery{namespace="tatara", pod=~"mem-acme-pg-.*", container="postgres"} == 0)`

	// MemoryPostgresReplicationSlotInactive: a declared standby's slot reads
	// inactive - the exact #442/#444/#448 signature. Read on the primary's view
	// of the slot (slot_name label), not on the sick standby's own status.
	require.Equal(t,
		`max by (slot_name) (cnpg_pg_replication_slots_active{namespace="tatara", slot_name=~"_cnpg_mem_acme_pg_.*"} `+primaryGuard+`) == 0`,
		byName["MemoryPostgresReplicationSlotInactive"],
	)
	require.Equal(t, "30m", byFor["MemoryPostgresReplicationSlotInactive"])
	require.Equal(t, "critical", bySeverity["MemoryPostgresReplicationSlotInactive"])

	// MemoryPostgresStreamingReplicasBelowExpected: primary-side streaming
	// count below instances-1 (2 standbys expected for 3 instances).
	require.Equal(t,
		`(max(cnpg_pg_replication_streaming_replicas{namespace="tatara", pod=~"mem-acme-pg-.*", container="postgres"} `+primaryGuard+`) or vector(0)) < 2`,
		byName["MemoryPostgresStreamingReplicasBelowExpected"],
	)
	require.Equal(t, "30m", byFor["MemoryPostgresStreamingReplicasBelowExpected"])
	require.Equal(t, "critical", bySeverity["MemoryPostgresStreamingReplicasBelowExpected"])

	// MemoryPostgresReplicationLagHigh does not exist: tried and removed after
	// live-verifying it false-fires on a write-idle-but-healthy standby
	// (cnpg_pg_replication_lag climbs unbounded with wall-clock time whenever
	// there is nothing new to replay, independent of replication health). See
	// MEMORY.md 2026-07-26 for the full evidence and the redundancy argument
	// against a byte-diff-gated version.
	require.NotContains(t, byName, "MemoryPostgresReplicationLagHigh")

	// MemoryPostgresReplicationSlotWalRetentionHigh: the N1 latent
	// disk-exhaustion trap - a slot pinning WAL beyond a quarter of the
	// project's configured (default 8Gi) WAL volume. 8Gi/4 = 2147483648 bytes.
	require.Equal(t,
		`max by (slot_name) (cnpg_pg_replication_slots_pg_wal_lsn_diff{namespace="tatara", slot_name=~"_cnpg_mem_acme_pg_.*"} `+primaryGuard+`) > 2147483648`,
		byName["MemoryPostgresReplicationSlotWalRetentionHigh"],
	)
	require.Equal(t, "15m", byFor["MemoryPostgresReplicationSlotWalRetentionHigh"])
	require.Equal(t, "warning", bySeverity["MemoryPostgresReplicationSlotWalRetentionHigh"])

	// MemoryPostgresStandbyReplayStalled: closes the third-review-round gap -
	// a standby up/counted/slot-active yet making zero REPLAY progress, which
	// none of the four rules above can see (they all read the FLUSH frontier).
	// cnpg_pg_stat_replication_replay_lag_seconds is emitted ONLY by primaries
	// (live-verified: absent on every standby), so it needs no `and on(pod)`
	// guard - there is no non-primary reporter to be wrong.
	require.Equal(t,
		`max by (application_name) (cnpg_pg_stat_replication_replay_lag_seconds{namespace="tatara", pod=~"mem-acme-pg-.*", container="postgres"}) > 300`,
		byName["MemoryPostgresStandbyReplayStalled"],
	)
	require.Equal(t, "15m", byFor["MemoryPostgresStandbyReplayStalled"])
	require.Equal(t, "critical", bySeverity["MemoryPostgresStandbyReplayStalled"])
}

// Regression test for the third-review-round Critical finding: a standby can
// be up (up{}==1), counted (streaming_replicas), and slot-active, yet have
// made zero replay progress, because the flush frontier every other rule
// reads stays genuinely 0 while only the apply/replay process has stalled.
// Live evidence: mem-tatara-pg-1 read replay_diff_bytes=2266627752 (2.1GiB)
// and a still-climbing replay_lag_seconds on the primary's pg_stat_replication
// view, while every other cnpg_pg_* signal (up, slots_active,
// streaming_replicas, slots_pg_wal_lsn_diff) read healthy. Pins the metric
// family (pg_stat_replication, not the slot/count metrics) and that it
// carries no `and on(pod)` guard, since it is structurally primary-exclusive.
func TestMemoryPrometheusRule_StandbyReplayStalled_UsesReplicationReplayMetric(t *testing.T) {
	p := testProjectWithInstances("acme", 3)
	pr := memory.MemoryPrometheusRule(p, testMonitorCfg())

	var expr string
	for _, r := range pr.Spec.Groups[0].Rules {
		if r.Alert == "MemoryPostgresStandbyReplayStalled" {
			expr = r.Expr.StrVal
		}
	}
	require.Contains(t, expr, "cnpg_pg_stat_replication_replay_lag_seconds", "must read the primary's pg_stat_replication replay frontier, not the slot flush frontier")
	require.Contains(t, expr, "application_name", "must aggregate by application_name for per-standby attribution")
	require.NotContains(t, expr, "and on(pod)", "must NOT need the primary-scoping guard: this metric is structurally primary-exclusive")
	require.NotContains(t, expr, "cnpg_pg_replication_in_recovery", "must not join against in_recovery: no non-primary reporter exists to guard against")
}

// Regression test for the reviewer-found trap: an earlier cut of the two
// slot-keyed rules read cnpg_pg_replication_slots_active /
// _pg_wal_lsn_diff unscoped across every reporting instance, which
// live-verification showed picks up a non-primary standby's stale/wrong view
// of a slot (2GiB+ on mem-tatara-pg-1 for a sibling's slot the real primary
// reported 0 for). Both rules must intersect with the primary's own
// in_recovery==0 report so a future edit cannot silently drop that guard.
func TestMemoryPrometheusRule_SlotAlerts_ScopedToPrimaryOnly(t *testing.T) {
	p := testProjectWithInstances("acme", 3)
	pr := memory.MemoryPrometheusRule(p, testMonitorCfg())

	byName := map[string]string{}
	for _, r := range pr.Spec.Groups[0].Rules {
		byName[r.Alert] = r.Expr.StrVal
	}

	for _, a := range []string{"MemoryPostgresReplicationSlotInactive", "MemoryPostgresReplicationSlotWalRetentionHigh", "MemoryPostgresStreamingReplicasBelowExpected"} {
		require.Contains(t, byName[a], "and on(pod)", "alert %s must scope to the primary's own report", a)
		require.Contains(t, byName[a], "cnpg_pg_replication_in_recovery", "alert %s must join against in_recovery to identify the primary", a)
		require.Contains(t, byName[a], "== 0)", "alert %s must require the joined pod to be in_recovery==0 (the primary)", a)
	}
}

// MemoryPostgresInstancesBelowDeclared must catch a genuinely dead instance
// even on a single-instance project, and must NOT fire on a merely
// collector-degraded one. Regression test for the reviewer-found trap: the
// first cut of this rule keyed on cnpg_pg_replication_in_recovery, which
// disappears whenever CNPG's user-query collector fails - as confirmed live
// on the single-instance mem-mtg-pg cluster, whose pod is up and scrapeable
// (up{}=1) but which reports cnpg_collector_up=0 and zero cnpg_pg_* series.
// That made the rule fire permanently on a healthy instance. This test does
// not re-run PromQL (no evaluator in this module), but pins the exact
// expression so a future edit cannot silently reintroduce a cnpg_pg_* liveness
// dependency without failing a byte-for-byte comparison.
func TestMemoryPrometheusRule_InstancesBelowDeclared_UsesScrapeHealthNotCollectorMetric(t *testing.T) {
	p := testProject("acme")
	pr := memory.MemoryPrometheusRule(p, testMonitorCfg())

	var expr string
	for _, r := range pr.Spec.Groups[0].Rules {
		if r.Alert == "MemoryPostgresInstancesBelowDeclared" {
			expr = r.Expr.StrVal
		}
	}
	require.NotContains(t, expr, "cnpg_pg_", "must not depend on CNPG's user-query collector output, which vanishes independent of instance health")
	require.Contains(t, expr, `up{`, "must key off the raw Prometheus scrape-health primitive")
	require.Contains(t, expr, "== 1", "must count targets that ARE up, not merely present")
}

// The postgres-layer alerts must scope their series to THIS Project's cnpg
// cluster (mem-<proj>-pg) and its namespace, or a per-Project PrometheusRule
// would fire on another Project's postgres cluster sharing the namespace.
func TestMemoryPrometheusRule_PostgresAlertsScopedToCluster(t *testing.T) {
	p := testProject("acme")
	pr := memory.MemoryPrometheusRule(p, testMonitorCfg())

	byName := map[string]string{}
	for _, r := range pr.Spec.Groups[0].Rules {
		byName[r.Alert] = r.Expr.StrVal
	}

	vol := byName["MemoryPostgresVolumeFilling"]
	require.Contains(t, vol, `persistentvolumeclaim=~"mem-acme-pg-.*"`, "volume alert must scope to this cluster's PVCs")
	require.Contains(t, vol, `namespace="tatara"`, "volume alert must scope to the memory namespace")
	require.Contains(t, vol, "kubelet_volume_stats_available_bytes", "volume alert must key off kubelet volume stats")

	restart := byName["MemoryPostgresInstanceRestarting"]
	require.Contains(t, restart, `pod=~"mem-acme-pg-.*"`, "restart alert must scope to this cluster's pods")
	require.Contains(t, restart, `container="postgres"`, "restart alert must target the postgres container")
	require.Contains(t, restart, "kube_pod_container_status_restarts_total", "restart alert must key off kube-state restart counter")
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
