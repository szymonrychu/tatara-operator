package accountusage

import (
	"context"
	"log/slog"
	"time"
)

type fetcher interface {
	Fetch(context.Context) (Snapshot, error)
}

// PollerMetrics is the metrics surface the poller produces on every poll. The
// poller - not the ConfigMap-mirror hook - is the sole producer so a failed or
// stale poll is always reflected: onUpdate runs only on success and so can never
// flip poll-health to 0 or count a failure (issue #189).
type PollerMetrics interface {
	SetAccountUsage(window string, percent float64)
	SetAccountUsageReset(window string, unix float64)
	SetAccountOverage(percent, used, limit float64)
	SetAccountUsagePollHealth(healthy bool)
	IncAccountUsagePollFailure()
}

type Poller struct {
	Fetcher          fetcher
	Store            *Store
	Metrics          PollerMetrics
	Interval         time.Duration
	FailureThreshold int
	Now              func() time.Time
	// onUpdate, when set, mirrors the freshly-fetched Snapshot to the ConfigMap
	// after each successful poll. Metrics are produced directly by the poller (see
	// PollerMetrics), NOT here, so a dead endpoint is still observable and the
	// ticker stays testable.
	onUpdate func(Snapshot)

	failures int
}

func (p *Poller) NeedLeaderElection() bool { return true }

// SetOnUpdate installs the post-poll hook (ConfigMap mirror), called with the
// freshly-fetched Snapshot after each successful poll.
func (p *Poller) SetOnUpdate(fn func(Snapshot)) {
	p.onUpdate = fn
}

func (p *Poller) Start(ctx context.Context) error {
	if p.Interval < 180*time.Second {
		p.Interval = 180 * time.Second
	}
	if p.Now == nil {
		p.Now = time.Now
	}
	// Seed poll-health to a defined value before the first poll so the gauge is
	// never merely at its registered default: unhealthy until a poll proves
	// otherwise. The immediate first poll below replaces it (true on success).
	if p.Metrics != nil {
		p.Metrics.SetAccountUsagePollHealth(false)
	}
	p.pollOnce(ctx) // immediate first poll
	t := time.NewTicker(p.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			p.pollOnce(ctx)
		}
	}
}

func (p *Poller) pollOnce(ctx context.Context) {
	snap, err := p.Fetcher.Fetch(ctx)
	if err != nil {
		p.failures++
		slog.Warn("accountusage poll failed", "failures", p.failures, "error", err)
		if p.Metrics != nil {
			p.Metrics.IncAccountUsagePollFailure()
		}
		if p.failures >= p.FailureThreshold {
			cur := p.Store.Get()
			cur.Healthy = false // keep last-known windows, mark stale
			p.Store.Set(cur)
			if p.Metrics != nil {
				p.Metrics.SetAccountUsagePollHealth(false)
			}
		}
		return
	}
	p.failures = 0
	snap.Healthy = true
	snap.UpdatedAt = p.Now()
	p.Store.Set(snap)
	slog.Info("accountusage poll ok", "five_hour_pct", snap.FiveHour.Percent, "weekly_pct", snap.Weekly.Percent)
	if p.Metrics != nil {
		p.emitMetrics(snap)
	}
	if p.onUpdate != nil {
		p.onUpdate(snap)
	}
}

// emitMetrics publishes a successful-poll snapshot: per-window utilization and
// reset time for all four windows, the read-only overage pool, and poll-health.
func (p *Poller) emitMetrics(snap Snapshot) {
	resetUnix := func(t time.Time) float64 {
		if t.IsZero() {
			return 0
		}
		return float64(t.Unix())
	}
	p.Metrics.SetAccountUsage("five_hour", snap.FiveHour.Percent)
	p.Metrics.SetAccountUsage("seven_day", snap.Weekly.Percent)
	p.Metrics.SetAccountUsage("seven_day_opus", snap.Opus.Percent)
	p.Metrics.SetAccountUsage("seven_day_sonnet", snap.Sonnet.Percent)
	p.Metrics.SetAccountUsageReset("five_hour", resetUnix(snap.FiveHour.Reset))
	p.Metrics.SetAccountUsageReset("seven_day", resetUnix(snap.Weekly.Reset))
	p.Metrics.SetAccountUsageReset("seven_day_opus", resetUnix(snap.Opus.Reset))
	p.Metrics.SetAccountUsageReset("seven_day_sonnet", resetUnix(snap.Sonnet.Reset))
	p.Metrics.SetAccountOverage(snap.Overage.Percent, snap.Overage.Used, snap.Overage.Limit)
	p.Metrics.SetAccountUsagePollHealth(true)
}
