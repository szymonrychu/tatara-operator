package accountusage

import (
	"sync"
	"time"

	"github.com/szymonrychu/tatara-operator/internal/budget"
)

// Window is one usage window: percent used (0..100) and when it resets.
type Window struct {
	Percent float64
	Reset   time.Time
}

// Overage is the read-only pay-as-you-go overage pool ("monthly").
type Overage struct {
	Enabled bool
	Percent float64
	Used    float64
	Limit   float64
}

// Snapshot is the fleet-wide account usage at a point in time.
type Snapshot struct {
	FiveHour  Window
	Weekly    Window
	Opus      Window // per-model weekly; zero when N/A on the plan
	Sonnet    Window
	Overage   Overage
	Healthy   bool
	UpdatedAt time.Time
}

// Subscription projects the snapshot into the budget gate's input type. The gate
// uses 5h + weekly; per-model + overage ride along for metrics only.
func (s Snapshot) Subscription() budget.Subscription {
	return budget.Subscription{
		FiveHourPercent: s.FiveHour.Percent,
		FiveHourReset:   s.FiveHour.Reset,
		WeeklyPercent:   s.Weekly.Percent,
		WeeklyReset:     s.Weekly.Reset,
		OpusPercent:     s.Opus.Percent,
		OpusReset:       s.Opus.Reset,
		SonnetPercent:   s.Sonnet.Percent,
		SonnetReset:     s.Sonnet.Reset,
		OverageEnabled:  s.Overage.Enabled,
		OveragePercent:  s.Overage.Percent,
	}
}

// Store holds the single fleet-wide snapshot, safe for concurrent reads by every
// project's admission reconcile and writes by the poller.
type Store struct {
	mu   sync.RWMutex
	snap Snapshot
}

func (s *Store) Get() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snap
}

func (s *Store) Set(snap Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snap = snap
}
