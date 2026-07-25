package memory

import (
	"fmt"
	"strings"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/resource"
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

	// memoryReplayStalledThresholdSeconds is how long a standby may show zero
	// replay progress (cnpg_pg_stat_replication_replay_lag_seconds, read on the
	// PRIMARY via pg_stat_replication) before MemoryPostgresStandbyReplayStalled
	// fires. The removed standby-self-reported lag metric was structurally
	// unsafe (it computes now() - pg_last_xact_replay_timestamp(), which grows
	// without bound on a truly write-idle stream regardless of replication
	// health) - but this stack's live data never actually exercised that idle
	// growth: every non-zero reading found was either a genuine sub-scrape
	// promotion blip or the real ongoing stall this rule now targets. This
	// metric reads 0 for as long as a standby has no pending replication work
	// (live-verified across the ~20.2h retention window this cluster carries),
	// which is a stronger, structural guarantee than "happened to be 0 in the
	// window checked" - so the threshold only needs to bound a REAL stall.
	memoryReplayStalledThresholdSeconds = "300"
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

// PGPodMonitor scrapes the CloudNativePG postgres pods' metrics endpoint (the
// container port named "metrics", 9187) so cnpg_* metrics - WAL volume usage,
// replication lag, database size, ready instances - land in Prometheus. Without
// it the postgres cluster is an observability blind spot: the disk saturation
// and replication divergence behind issue #238 stayed invisible until the
// memory API began returning 5xx. cnpg's own spec.monitoring.enablePodMonitor
// is deprecated in cnpg v1.29.1 ("create a PodMonitor manually"), so the
// PodMonitor is built natively here, mirroring MemoryServiceMonitor.
//
// The selector matches cnpg's per-pod label cnpg.io/cluster=<cluster>; jobLabel
// is left default (no alert rule keys off the cnpg job label). monitorObjectMeta
// stamps the cluster ruleSelector/podMonitorSelector labels so the PodMonitor is
// discovered rather than silently dropped.
func PGPodMonitor(p *tatarav1alpha1.Project, cfg Config) *monitoringv1.PodMonitor {
	n := NamesFor(p.Name)
	metricsPort := "metrics"
	return &monitoringv1.PodMonitor{
		TypeMeta: metav1.TypeMeta{
			APIVersion: monitoringv1.SchemeGroupVersion.String(),
			Kind:       monitoringv1.PodMonitorsKind,
		},
		ObjectMeta: monitorObjectMeta(p, cfg, n.PGCluster),
		Spec: monitoringv1.PodMonitorSpec{
			NamespaceSelector: monitoringv1.NamespaceSelector{
				MatchNames: []string{cfg.Namespace},
			},
			Selector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					"cnpg.io/cluster": n.PGCluster,
				},
			},
			PodMetricsEndpoints: []monitoringv1.PodMetricsEndpoint{{
				Port:          &metricsPort,
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
				Rules: memoryAlertRules(p, n.PGCluster, cfg.Namespace),
			}},
		},
	}
}

// memoryAlertRules is the alert set for the memory stack. Kept as a function
// (not a package var) so each PrometheusRule gets its own slice and callers
// cannot mutate a shared one.
//
// The first group (ported from tatara-memory#58) alerts on the tatara-memory
// API layer (http_requests_total, up, ...). Those only fire once the API is
// already serving 5xx - a reactive, downstream signal. The postgres-layer rules
// appended below fire on the DB failure modes that CAUSE that 5xx, one hop
// upstream, so the cluster degradation is caught before it reaches the API:
//
//   - MemoryPostgresVolumeFilling: a cnpg PVC (PGDATA or the dedicated WAL
//     volume) is running out of space. A full volume stops Postgres writing WAL
//     and the write path (/memories:bulk) starts returning 503 while reads keep
//     working - the disk-exhaustion write-outage of issue #238.
//   - MemoryPostgresInstanceRestarting: a cnpg postgres instance is
//     crash-looping. Repeated primary crash/restart + failover thrash is the
//     shape of both #238 and the ~3.5h #240 outage.
//
// These two intentionally key off kubelet_volume_stats_* (kubelet) and
// kube_pod_container_status_restarts_total (kube-state-metrics) rather than
// cnpg_* metrics: those cluster-standard series are already scraped and present,
// so the rules are live immediately and do not depend on the cnpg PodMonitor
// (PGPodMonitor) scrape, whose cnpg_* series were still absent from Prometheus
// during the issue #252 investigation.
//
// The replication-topology rules below (issues #442, #444, #448) key off
// cnpg_* metrics directly: the deferral note that used to live here is
// resolved - #442/#444/#448 each independently confirmed cnpg_pg_replication_*
// series flowing live off PGPodMonitor's scrape. mem-infrastructure-pg-2 sat
// as a permanently diverged standby - up, crash-looping 165 times, its
// replication slot inactive and its slot's retained WAL flat rather than
// draining - for 8h42m before anything paged, because the only existing
// related alert (in tatara-observability, not here) keys off
// kube_pod_container_status_waiting_reason, which cannot see a standby that is
// up and silently not replicating. Upstream CNPG's own remediation cannot
// self-diagnose this either: its decision is gated on reading the sick
// instance's HTTP status endpoint, which never comes up while the container
// is fatally exiting ~1.5s into every start (issue #448 N2). So every rule
// below is read on metrics observable from the SURVIVING side of the
// connection - the primary's view of its replication slots and streaming
// replica count - never from the sick instance itself:
//
//   - MemoryPostgresInstancesBelowDeclared: fewer instances of this cluster's
//     scrape target report up{}==1 than this Project declares
//     (memory.PgInstances). Deliberately reads the raw Prometheus scrape-health
//     primitive, NOT a cnpg_pg_* metric: cnpg_pg_* series are produced by CNPG's
//     USER-QUERY collector and disappear whenever that collector's query fails,
//     even while the instance itself is up and perfectly scrapeable (confirmed
//     live: the unrelated single-instance mem-mtg-pg cluster reads
//     cnpg_collector_up=0 with zero cnpg_pg_* series at all, while its pod is
//     Ready and up{}=1) - using one as a liveness proxy would page forever on a
//     healthy instance whose collector query merely errors. up{}==1 has no such
//     dependency: it is Prometheus's own record of whether the scrape target
//     answered, independent of anything the target's payload contains.
//     Meaningful even for a single-instance project (the lone instance
//     disappearing).
//   - MemoryPostgresReplicationSlotInactive: a declared standby's replication
//     slot reads inactive, read via the PRIMARY's pg_replication_slots view
//     specifically (see primarySelector below). This is the exact
//     #442/#444/#448 signature and would have paged at the true start of the
//     incident (18:48Z) instead of 8h42m later (03:45Z), when the standby was
//     still up and silently failing to stream. Only generated for
//     instances > 1 (a single-instance cluster has no slots).
//   - MemoryPostgresStreamingReplicasBelowExpected: the primary's own
//     streaming-replica count falls short of instances-1. A second,
//     primary-side corroboration of the same signature, independent of
//     whether the sick standby's own metrics exist at all. Only generated for
//     instances > 1.
//   - MemoryPostgresReplicationSlotWalRetentionHigh: a slot pinning more than a
//     quarter of the Project's configured WAL volume (half of
//     pgMaxSlotWalKeepSize, memory.go) - the N1 latent disk-exhaustion trap:
//     WAL a dead/stuck slot pins reads flat only because the database is idle,
//     and grows unbounded the moment write traffic resumes, well before
//     MemoryPostgresVolumeFilling's 15%-free threshold or cnpg's own
//     max_slot_wal_keep_size would forcibly invalidate the slot. Only
//     generated for instances > 1.
//   - MemoryPostgresStandbyReplayStalled: a standby that IS up, IS counted,
//     and has an ACTIVE slot, yet has made zero replay progress - precisely
//     the #442/#444/#448 signature (a standby that looks healthy on every
//     coarse signal while silently not applying WAL) and the one none of the
//     four rules above can see, because they all read the slot's FLUSH
//     frontier (cnpg_pg_replication_slots_pg_wal_lsn_diff /
//     cnpg_pg_replication_streaming_replicas), which stays genuinely 0 when
//     only REPLAY (apply) has stalled. Reads
//     cnpg_pg_stat_replication_replay_lag_seconds instead - a DIFFERENT
//     metric family (pg_stat_replication on the primary), keyed by
//     application_name=<standby pod>, tracking the REPLAY frontier. Only
//     generated for instances > 1.
//
// primarySelector: cnpg_pg_replication_slots_active/_pg_wal_lsn_diff are NOT
// primary-exclusive metrics - every RUNNING instance's local
// pg_replication_slots view gets scraped, including a standby's, and a
// BROKEN standby's view of a SIBLING's slot can be stale (this does NOT hold
// for standbys generally: mem-tatara-pg's OTHER, healthy standby reports the
// same sibling's slot accurately at 0 the whole time - only the stalled one
// diverges). Live-verified: mem-tatara-pg-1 (itself stalled - see
// MemoryPostgresStandbyReplayStalled above) reported
// cnpg_pg_replication_slots_pg_wal_lsn_diff for a SIBLING standby's slot
// climbing past 2GiB while the actual primary reported 0 for that same
// slot_name. This is NOT an invented false positive on a healthy cluster -
// mem-tatara-pg is NOT healthy, and the 2GiB+ pg-1 reported is a real
// downstream symptom of pg-1's OWN replay stall (its local slot-sync view is
// frozen at the pre-stall LSN while its receive position keeps advancing). An
// unscoped `max by (slot_name)` over ALL reporters picks up that distorted
// reading and would have alarmed on the WRONG symptom (WAL retention) instead
// of the real one (replay stall) - misdiagnosis, not noise. Both slot-keyed
// rules therefore intersect the slot metric with
// `cnpg_pg_replication_in_recovery{...} == 0` via `and on(pod)`, restricting
// the read to whichever pod is CURRENTLY the primary and discarding every
// other reporter's distorted view, live-verified quiet on mem-tatara-pg's
// actual primary (0 for both slots) and still firing correctly on
// mem-infrastructure-pg's genuinely orphaned slot.
//
// KNOWN, ACCEPTED gap (not redesigned, documented): if a PRIMARY's own
// user-query collector degrades (the same failure class
// MemoryPostgresInstancesBelowDeclared's doc entry describes for mem-mtg-pg,
// but on the CURRENT primary of a multi-instance cluster), every cnpg_pg_*
// series - including the `and on(pod)` join partner - disappears from that
// primary. MemoryPostgresReplicationSlotInactive,
// MemoryPostgresReplicationSlotWalRetentionHigh, and
// MemoryPostgresStandbyReplayStalled all go silently dark (their expressions
// evaluate to no data, not a firing alert), while
// MemoryPostgresStreamingReplicasBelowExpected's outer `or vector(0)` fires
// anyway (needed so a wholly-gone cluster still compares as 0) with a
// misleading "fewer streaming replicas than expected" message when the real
// fault is "the primary's collector is down." One page, two dark rules. THIS
// FAILURE CLASS IS LIVE ON THIS CLUSTER RIGHT NOW, just not on an instance
// count where the coupling currently bites: mem-mtg-pg-1's cnpg_collector_up
// has read a sustained 0 for ~11h (live-verified: 1 until ~12:40Z, 0
// continuously from there to now), a genuine mid-window outage, not an edge
// artifact - but mem-mtg-pg is single-instance, so none of the three
// affected rules are generated for it and no misleading page results today.
// The part of this claim that IS load-bearing for the multi-instance rules
// above - that both CURRENT PRIMARIES of the live multi-instance clusters
// (mem-tatara-pg-2, mem-infrastructure-pg-1) have stayed continuously
// scrapeable with a healthy collector for the full window checked - is true
// and separately confirmed. So the gap is real, its risk is not
// hypothetical (a real project on this cluster is in exactly this state
// today), and it is recorded as a known trade-off, not fixed here - closing
// it needs a collector-health precondition on all four cnpg_pg_*-keyed
// rules, a larger change than this alerting-only pass.
//
// Deliberately NOT added: a per-standby cnpg_pg_replication_is_wal_receiver_up
// check (redundant with MemoryPostgresReplicationSlotInactive, but unlike the
// slot check requires the sick instance itself to be alive enough to report -
// exactly the signal source issue #448 says cannot be trusted), and a
// standby-side cnpg_pg_replication_lag ("time since last replay",
// self-reported by the standby) rule. That metric was tried and REMOVED, on
// STRUCTURAL grounds: CNPG's `pg_replication` collector computes it as
// `now() - pg_last_xact_replay_timestamp()`, which genuinely does grow
// without bound on a truly write-idle stream regardless of replication
// health, because "no new transaction" and "stopped replaying" are
// indistinguishable to that formula. NOT because this stack's own data
// showed it misfiring: re-examined, this stack was never actually
// write-idle during the observation window (this cluster's primary kept
// generating new WAL throughout - see the corollary below), and every
// non-zero reading the removed metric produced here was either a genuine
// sub-scrape post-promotion blip (mem-infrastructure-pg-3, one 30s sample at
// 591s, gone by the next scrape, invisible at the rule's own 120s+ scrape
// cadence) or the SAME real, ongoing standby stall MemoryPostgresStandbyReplayStalled
// now targets (mem-tatara-pg-1's reading tracked
// cnpg_pg_stat_replication_replay_lag_seconds in lockstep once the stall
// began) - i.e. the removed rule's one sustained exceedance in this data was
// a TRUE positive, not noise. The removal stands on the structural argument
// alone, which needs no cherry-picked incident to justify it.
// MemoryPostgresStandbyReplayStalled above replaces it with a DIFFERENT
// metric family - cnpg_pg_stat_replication_*, read on the primary via
// pg_stat_replication rather than self-reported by the standby - which is
// safe by construction rather than merely safe in the window checked: it
// measures pending replication WORK (bytes/time actually behind), so it
// reads 0 whenever a standby has nothing outstanding, with no dependency on
// wall-clock time since an arbitrary last event. Corollary worth recording:
// this project is NOT write-idle - cnpg_pg_stat_replication_sent_diff_bytes
// for mem-tatara-pg-1 read a flat 0 throughout (the primary has transmitted
// everything it has generated) while replay_diff_bytes climbed from ~1.2GB
// to ~2.8GB across the same hour, meaning the primary kept producing new WAL
// continuously; the gap is entirely in the standby's own replay/apply step,
// not in write volume. Full evidence trail: MEMORY.md, 2026-07-26.
//
// A rolling restart or a deliberate scale change must not trip any of these:
// CNPG bounces or (re)clones one instance at a time. MemoryPostgresInstancesBelowDeclared
// only measures scrape reachability (up{}==1), which a fresh pod satisfies as
// soon as its container starts and opens the metrics port - well before any
// base backup completes - so its "for" (10m) only needs to ride out a single
// instance's restart bounce, not a resync. The other four measure actual
// replication convergence (a slot activating, streaming replicas catching up,
// replay actually progressing), which a freshly cloned standby only reaches
// once its base backup finishes; their "for" (30m for the hard down/inactive/
// below-expected states, 15m for the two early-warning/progress ones) rides
// that out too - MemoryPostgresStandbyReplayStalled does not need the fuller
// 30m because a routine restart's replay backlog clears in seconds once
// reconnected (no base-backup wait), unlike a slot activating from cold.
// instances is read from THIS Project's current spec at render time, so a
// scale change moves the threshold and the actual desired pod count together
// in the same reconcile - there is no window where the alert compares against
// a stale target.
//
// cluster is the cnpg Cluster name (mem-<proj>-pg) and its pods/PVCs are named
// <cluster>-<n>[-wal]; namespace scopes the series to this Project's cluster
// since several Projects' clusters share a namespace. Every metric used here -
// kube-state-metrics, kubelet, and cnpg_* alike - carries container="postgres"
// on these pods (verified live), so podSelector is shared across all of them;
// there is no separate cnpg selector.
func memoryAlertRules(p *tatarav1alpha1.Project, cluster, namespace string) []monitoringv1.Rule {
	pgSelector := fmt.Sprintf(`namespace=%q, persistentvolumeclaim=~%q`, namespace, cluster+"-.*")
	podSelector := fmt.Sprintf(`namespace=%q, pod=~%q, container="postgres"`, namespace, cluster+"-.*")
	slotSelector := fmt.Sprintf(`namespace=%q, slot_name=~%q`, namespace, "_cnpg_"+strings.ReplaceAll(cluster, "-", "_")+"_.*")
	// onPrimary intersects a slot-keyed metric with whichever pod is CURRENTLY
	// reporting in_recovery==0 (the primary), discarding every other running
	// instance's non-authoritative view of the same slot_name. See the
	// primarySelector paragraph in the doc comment above for the live evidence
	// this guards against.
	onPrimary := fmt.Sprintf(`and on(pod) (cnpg_pg_replication_in_recovery{%s} == 0)`, podSelector)
	instances := PgInstances(p)
	walRetentionWarnBytes := pgSlotWalRetentionWarnBytes(p)

	rules := []monitoringv1.Rule{
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
		{
			// One free-space ratio series per cnpg PVC (PGDATA and WAL). Fires per
			// volume that drops below the headroom threshold, before it fills and
			// stalls WAL writes (issue #238). The dedicated WAL volume (#238) is only
			// 8Gi by default, so 15% is a meaningful early margin, not noise.
			Alert: "MemoryPostgresVolumeFilling",
			Expr: intstr.FromString(fmt.Sprintf(
				`kubelet_volume_stats_available_bytes{%[1]s} / kubelet_volume_stats_capacity_bytes{%[1]s} < 0.15`,
				pgSelector,
			)),
			For:    dur("5m"),
			Labels: map[string]string{"severity": memorySeverityWarning},
			Annotations: map[string]string{
				"summary":     "postgres volume for " + cluster + " is filling up",
				"description": "A cnpg postgres PVC of cluster " + cluster + " has under 15% free space for 5m. A full PGDATA or WAL volume stops Postgres writing WAL and the memory write path (/memories:bulk) returns 503 (issue #238).",
			},
		},
		{
			// One series per crash-looping postgres instance. Repeated primary
			// crash/restart drives failovers and the write path to 503 (#238, #240).
			Alert: "MemoryPostgresInstanceRestarting",
			Expr: intstr.FromString(fmt.Sprintf(
				`increase(kube_pod_container_status_restarts_total{%s}[15m]) > 2`,
				podSelector,
			)),
			For:    dur("5m"),
			Labels: map[string]string{"severity": memorySeverityWarning},
			Annotations: map[string]string{
				"summary":     "postgres instance of " + cluster + " is restarting",
				"description": "A cnpg postgres instance of cluster " + cluster + " has restarted more than twice in 15m. A crash-looping primary drives failover thrash and 503s on the memory write path (issues #238, #240).",
			},
		},
		{
			// count(up{...}==1) is a pure scrape-reachability check on Prometheus's
			// own primitive, deliberately NOT a cnpg_pg_* metric: cnpg_pg_* series
			// come from CNPG's user-query collector and vanish whenever that
			// collector's query fails, even while the instance is up and perfectly
			// scrapeable - verified live against the unrelated single-instance
			// mem-mtg-pg cluster, whose pod is Ready and up{}=1 but which reports
			// cnpg_collector_up=0 and zero cnpg_pg_* series. A rule keyed on any
			// cnpg_pg_* metric here would have paged on that healthy instance
			// forever. `or vector(0)` guards the case where every instance of the
			// cluster is gone at once - count() over an empty vector yields no
			// sample, which would otherwise silently swallow a whole-cluster outage
			// instead of comparing it against the declared count.
			Alert: "MemoryPostgresInstancesBelowDeclared",
			Expr: intstr.FromString(fmt.Sprintf(
				`(count(up{%s} == 1) or vector(0)) < %d`,
				podSelector, instances,
			)),
			For:    dur("10m"),
			Labels: map[string]string{"severity": memorySeverityCritical},
			Annotations: map[string]string{
				"summary":     "postgres cluster " + cluster + " has fewer live instances than declared",
				"description": fmt.Sprintf("Fewer than the declared %d cnpg instance(s) of cluster %s have had a healthy scrape target for 10m - one or more instances are down, crash-looping, or unreachable (issues #442, #444, #448).", instances, cluster),
			},
		},
	}

	if instances > 1 {
		rules = append(rules,
			monitoringv1.Rule{
				// max by (slot_name), scoped onPrimary: collapses to one signal per
				// standby, read ONLY from whichever pod is currently the primary (see
				// the primarySelector doc-comment paragraph above - a non-primary
				// reporter's view of a slot can be stale or simply wrong). A slot
				// inactive for 30m sustained is the #442/#444/#448 signature: the
				// standby's walreceiver never attaches while the pod and the primary
				// both look healthy elsewhere.
				Alert: "MemoryPostgresReplicationSlotInactive",
				Expr: intstr.FromString(fmt.Sprintf(
					`max by (slot_name) (cnpg_pg_replication_slots_active{%s} %s) == 0`,
					slotSelector, onPrimary,
				)),
				For:    dur("30m"),
				Labels: map[string]string{"severity": memorySeverityCritical},
				Annotations: map[string]string{
					"summary":     "a replication slot of postgres cluster " + cluster + " is inactive",
					"description": "A declared replication slot of cluster " + cluster + " has been inactive for 30m - its standby is not streaming (issues #442, #444, #448). 30m rides out a routine standby restart or a freshly scaled-up standby's initial base backup.",
				},
			},
			monitoringv1.Rule{
				// Scoped onPrimary for the same reason as the slot rules: a standby's
				// own streaming_replicas view should always read 0, but scoping
				// explicitly (rather than relying on that convention) keeps this rule
				// consistent with the other primary-keyed ones and immune to a stale
				// non-primary reporter. `or vector(0)` covers the primary itself
				// reporting no series at all, which must still compare as 0 streaming
				// replicas rather than no data.
				Alert: "MemoryPostgresStreamingReplicasBelowExpected",
				Expr: intstr.FromString(fmt.Sprintf(
					`(max(cnpg_pg_replication_streaming_replicas{%s} %s) or vector(0)) < %d`,
					podSelector, onPrimary, instances-1,
				)),
				For:    dur("30m"),
				Labels: map[string]string{"severity": memorySeverityCritical},
				Annotations: map[string]string{
					"summary":     "postgres cluster " + cluster + " has fewer streaming replicas than expected",
					"description": fmt.Sprintf("The primary of cluster %s has reported fewer than %d streaming replica(s) for 30m, the count implied by its declared %d instances (issues #442, #444, #448).", cluster, instances-1, instances),
				},
			},
			monitoringv1.Rule{
				// The N1 latent disk-exhaustion trap: a slot's retained WAL reads
				// flat only because the database is idle, and grows unbounded the
				// moment write traffic resumes. Thresholded off this Project's own
				// configured WAL volume (a quarter of it - half of
				// pgMaxSlotWalKeepSize, memory.go) rather than a hardcoded absolute,
				// so it scales with a custom pgWalStorage and gives lead time before
				// cnpg's own max_slot_wal_keep_size would forcibly invalidate the
				// slot. Scoped onPrimary: LIVE-VERIFIED NECESSARY, not defensive
				// gold-plating - an unscoped max by (slot_name) picked up a BROKEN
				// standby's distorted view of a sibling's slot at >2GiB while the
				// actual primary reported 0 for that slot - a real symptom
				// misattributed to the wrong slot/cause, which would have alarmed
				// on "WAL retention" within ~15 minutes instead of the real fault
				// (see MemoryPostgresStandbyReplayStalled below).
				Alert: "MemoryPostgresReplicationSlotWalRetentionHigh",
				Expr: intstr.FromString(fmt.Sprintf(
					`max by (slot_name) (cnpg_pg_replication_slots_pg_wal_lsn_diff{%s} %s) > %d`,
					slotSelector, onPrimary, walRetentionWarnBytes,
				)),
				For:    dur("15m"),
				Labels: map[string]string{"severity": memorySeverityWarning},
				Annotations: map[string]string{
					"summary":     "a replication slot of postgres cluster " + cluster + " is pinning growing WAL",
					"description": fmt.Sprintf("A replication slot of cluster %s has retained more than %d bytes of WAL for 15m, a quarter of its configured WAL volume - the latent disk-exhaustion trap of issue #448's N1 finding, ahead of the point cnpg's own max_slot_wal_keep_size would forcibly invalidate the slot.", cluster, walRetentionWarnBytes),
				},
			},
			monitoringv1.Rule{
				// The gap none of the other four rules can see: a standby that is
				// up (up{}==1), counted (streaming_replicas), and has an active
				// slot, yet has made zero REPLAY progress - the FLUSH frontier the
				// other rules read stays genuinely 0 because the standby is still
				// receiving and flushing WAL fine; only its apply/replay process has
				// stalled. Reads cnpg_pg_stat_replication_replay_lag_seconds -
				// pg_stat_replication on the PRIMARY, keyed by
				// application_name=<standby pod> - which is emitted ONLY by
				// primaries (absent on every standby, live-verified), so unlike the
				// slot metrics above it needs no `and on(pod)` guard: there is no
				// non-primary reporter to be wrong. Chose the TIME form over the
				// sibling cnpg_pg_stat_replication_replay_diff_bytes metric: both
				// are live-verified idle-immune BY CONSTRUCTION (they measure
				// pending replication work, not wall-clock time since an event),
				// confirmed flat 0 on every healthy standby across the full
				// ~20.2h retention window this cluster carries, so the choice
				// between them is legibility, not safety - "no replay progress
				// for N minutes" reads directly in an alert annotation without
				// needing to know this project's WAL volume or write rate, where
				// a byte threshold would need the same per-project scaling
				// pgSlotWalRetentionWarnBytes already does for a different metric.
				Alert: "MemoryPostgresStandbyReplayStalled",
				Expr: intstr.FromString(fmt.Sprintf(
					`max by (application_name) (cnpg_pg_stat_replication_replay_lag_seconds{%s}) > %s`,
					podSelector, memoryReplayStalledThresholdSeconds,
				)),
				For:    dur("15m"),
				Labels: map[string]string{"severity": memorySeverityCritical},
				Annotations: map[string]string{
					"summary":     "a standby of postgres cluster " + cluster + " has stopped applying WAL",
					"description": "A standby of cluster " + cluster + " has made zero replay progress for more than " + memoryReplayStalledThresholdSeconds + "s - it is up, counted, and its slot is active, but it is silently not applying WAL (issues #442, #444, #448). 15m rides out a routine restart's catch-up, which clears in seconds once reconnected.",
				},
			},
		)
	}

	return rules
}

// pgSlotWalRetentionWarnBytes is the early-warning threshold for a single
// replication slot's retained WAL: one quarter of the Project's configured WAL
// volume, i.e. half of pgMaxSlotWalKeepSize (memory.go) - the point at which
// cnpg itself invalidates a slot rather than let it fill the volume (issue
// #240). Warning here gives an operator time to intervene (drop the stale
// slot, fix the standby) before that self-protection forces a disruptive
// re-clone. Falls back to a quarter of the 8Gi default if the configured size
// cannot be parsed, mirroring pgMaxSlotWalKeepSize's own fallback.
func pgSlotWalRetentionWarnBytes(p *tatarav1alpha1.Project) int64 {
	q, err := resource.ParseQuantity(pgWalStorage(p))
	if err != nil {
		q = resource.MustParse(defaultPgWalStorage)
	}
	return q.Value() / 4
}
