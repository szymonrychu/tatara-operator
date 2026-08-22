package obs

import "github.com/prometheus/client_golang/prometheus"

// accountUsageMetrics holds the Claude account usage (claudeSubscription mode)
// Prometheus collectors, embedded into OperatorMetrics.
type accountUsageMetrics struct {
	accountUsageUtil          *prometheus.GaugeVec
	accountUsageReset         *prometheus.GaugeVec
	accountUsageSnapshotAge   *prometheus.GaugeVec
	accountUsageGateReady     *prometheus.GaugeVec
	accountUsagePollHealth    prometheus.Gauge
	accountUsagePollerEnabled prometheus.Gauge
	accountUsagePollFailures  prometheus.Counter
	accountOveragePercent     prometheus.Gauge
	accountOverageUsed        prometheus.Gauge
	accountOverageLimit       prometheus.Gauge
}

// newAccountUsageMetrics registers the account-usage collectors on reg and
// returns the bundle.
func newAccountUsageMetrics(reg prometheus.Registerer) *accountUsageMetrics {
	m := &accountUsageMetrics{
		// Claude account usage windows (claudeSubscription mode), keyed by window
		// name (five_hour|seven_day|seven_day_opus|seven_day_sonnet).
		accountUsageUtil: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "tatara_account_usage_utilization",
			Help: "Claude account usage utilization percent (0..100) by window.",
		}, []string{"window"}),
		accountUsageReset: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "tatara_account_usage_resets_at_seconds",
			Help: "Unix time each usage window resets.",
		}, []string{"window"}),
		// Snapshot age, labelled by which feed produced the governing snapshot
		// ("poller" | "wrapper"). A dead feed is directly visible here.
		//
		// The source label lives HERE and deliberately NOT on
		// tatara_account_usage_utilization / _resets_at_seconds: both feeds stay
		// wired, so the question is real, but relabelling those established
		// series would break every dashboard query and recording rule already
		// reading them. This gauge answers "which feed is current" without
		// touching them.
		accountUsageSnapshotAge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "tatara_account_usage_snapshot_age_seconds",
			Help: "Age in seconds of the account usage snapshot currently governing admission, by feed source.",
		}, []string{"source"}),
		// 1 when the claudeSubscription gate is enabled AND a fresh snapshot is
		// governing; 0 when it is enabled but has none.
		//
		// A label-less GaugeVec rather than a Gauge, deliberately: a registered
		// Gauge renders 0 from process start, and TataraAccountUsageFeedDead
		// alerts on == 0. The vec's child is created on first Set, so an
		// operator with the budget gate off produces NO series and cannot fire.
		//
		// THIS IS THE ALERT THE OUTAGE NEEDED. The gate ran inert for weeks
		// because the only budget rule watched operator_admission_blocked_total,
		// a derived counter that by construction never increments while the gate
		// evaluates 0%.
		accountUsageGateReady: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "tatara_account_usage_gate_ready",
			Help: "1 when the claudeSubscription gate has a fresh usage snapshot, 0 when it is enabled but has none.",
		}, nil),
		accountUsagePollHealth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "tatara_account_usage_poll_health",
			Help: "1 when the usage poll is healthy, 0 when stale.",
		}),
		// Distinguishes "poller intentionally disabled" (USAGE_ENABLED=false,
		// e.g. claudeSubscription mode's wrapper-reported path) from "poller
		// enabled but unhealthy" - poll_health alone can't tell the two apart
		// since it defaults to 0 whether or not a poll ever ran (issue #339).
		accountUsagePollerEnabled: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "tatara_account_usage_poller_enabled",
			Help: "1 when the usage poller (USAGE_ENABLED) is enabled, 0 when disabled.",
		}),
		// Per-fetch failure counter (issue #189). Increments on every failed poll;
		// the poll-health gauge only flips to 0 once consecutive failures reach the
		// staleness threshold, so this exposes transient blips the gauge hides.
		accountUsagePollFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tatara_account_usage_poll_failures_total",
			Help: "Total Claude account usage poll fetch failures.",
		}),
		// Monthly overage (read-only, never gates admission; non-goal per spec).
		accountOveragePercent: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "tatara_account_overage_percent",
			Help: "Monthly overage utilization percent (0..100+), read-only.",
		}),
		accountOverageUsed: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "tatara_account_overage_used",
			Help: "Monthly overage amount used, read-only.",
		}),
		accountOverageLimit: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "tatara_account_overage_limit",
			Help: "Monthly overage limit, read-only.",
		}),
	}
	reg.MustRegister(
		m.accountUsageUtil,
		m.accountUsageReset,
		m.accountUsageSnapshotAge,
		m.accountUsageGateReady,
		m.accountUsagePollHealth,
		m.accountUsagePollerEnabled,
		m.accountUsagePollFailures,
		m.accountOveragePercent,
		m.accountOverageUsed,
		m.accountOverageLimit,
	)
	return m
}

// SetAccountUsage sets tatara_account_usage_utilization for a usage window
// (five_hour|seven_day|seven_day_opus|seven_day_sonnet) to a percent (0..100).
func (m *accountUsageMetrics) SetAccountUsage(window string, percent float64) {
	m.accountUsageUtil.WithLabelValues(window).Set(percent)
}

// SetAccountUsageReset sets tatara_account_usage_resets_at_seconds for a usage
// window to the Unix time it resets.
func (m *accountUsageMetrics) SetAccountUsageReset(window string, unix float64) {
	m.accountUsageReset.WithLabelValues(window).Set(unix)
}

// SetAccountUsageSnapshotAge sets tatara_account_usage_snapshot_age_seconds for
// one feed source ("poller" | "wrapper") to the age in seconds of the snapshot
// currently governing admission.
func (m *accountUsageMetrics) SetAccountUsageSnapshotAge(source string, seconds float64) {
	m.accountUsageSnapshotAge.WithLabelValues(source).Set(seconds)
}

// SetAccountUsageGateReady sets tatara_account_usage_gate_ready to 1 when the
// claudeSubscription gate has a fresh snapshot governing admission, 0 when it is
// enabled but has none. Calling it is what CREATES the series, so a disabled
// gate must never call it.
func (m *accountUsageMetrics) SetAccountUsageGateReady(ready bool) {
	v := 0.0
	if ready {
		v = 1.0
	}
	m.accountUsageGateReady.WithLabelValues().Set(v)
}

// SetAccountUsagePollHealth sets tatara_account_usage_poll_health to 1 when the
// usage poll is healthy, 0 when stale.
func (m *accountUsageMetrics) SetAccountUsagePollHealth(healthy bool) {
	v := 0.0
	if healthy {
		v = 1.0
	}
	m.accountUsagePollHealth.Set(v)
}

// SetAccountUsagePollerEnabled sets tatara_account_usage_poller_enabled to 1
// when the poller is enabled (USAGE_ENABLED=true), 0 when disabled.
func (m *accountUsageMetrics) SetAccountUsagePollerEnabled(enabled bool) {
	v := 0.0
	if enabled {
		v = 1.0
	}
	m.accountUsagePollerEnabled.Set(v)
}

// IncAccountUsagePollFailure increments tatara_account_usage_poll_failures_total:
// a single Claude account usage poll fetch failed. The poll-health gauge only
// flips to 0 once consecutive failures reach the staleness threshold.
func (m *accountUsageMetrics) IncAccountUsagePollFailure() {
	m.accountUsagePollFailures.Inc()
}

// SetAccountOverage sets the monthly overage read-only gauges (never gates
// admission).
func (m *accountUsageMetrics) SetAccountOverage(percent, used, limit float64) {
	m.accountOveragePercent.Set(percent)
	m.accountOverageUsed.Set(used)
	m.accountOverageLimit.Set(limit)
}
