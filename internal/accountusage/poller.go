package accountusage

import (
	"context"
	"log/slog"
	"time"
)

type fetcher interface {
	Fetch(context.Context) (Snapshot, error)
}

type Poller struct {
	Fetcher          fetcher
	Store            *Store
	Interval         time.Duration
	FailureThreshold int
	Now              func() time.Time
	// onUpdate, when set, is called after each successful poll (ConfigMap mirror +
	// metrics wired in Task A9). Kept as a hook so the ticker stays testable.
	onUpdate func(Snapshot)

	failures int
}

func (p *Poller) NeedLeaderElection() bool { return true }

func (p *Poller) Start(ctx context.Context) error {
	if p.Interval < 180*time.Second {
		p.Interval = 180 * time.Second
	}
	if p.Now == nil {
		p.Now = time.Now
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
		if p.failures >= p.FailureThreshold {
			cur := p.Store.Get()
			cur.Healthy = false // keep last-known windows, mark stale
			p.Store.Set(cur)
		}
		return
	}
	p.failures = 0
	snap.Healthy = true
	snap.UpdatedAt = p.Now()
	p.Store.Set(snap)
	slog.Info("accountusage poll ok", "five_hour_pct", snap.FiveHour.Percent, "weekly_pct", snap.Weekly.Percent)
	if p.onUpdate != nil {
		p.onUpdate(snap)
	}
}
