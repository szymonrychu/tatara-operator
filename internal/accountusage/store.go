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

// Snapshot sources, used as the label value on the snapshot-age gauge.
const (
	SourcePoller  = "poller"
	SourceWrapper = "wrapper"
)

// Snapshot is the fleet-wide account usage at a point in time.
type Snapshot struct {
	FiveHour  Window
	Weekly    Window
	Opus      Window // per-model weekly; zero when N/A on the plan
	Sonnet    Window
	Overage   Overage
	Healthy   bool
	UpdatedAt time.Time
	// Source names which feed produced this snapshot: "poller" (the
	// /api/oauth/usage poller) or "wrapper" (the agent-pod statusline feed).
	// It labels the snapshot-age gauge so a dead feed names itself.
	Source string
}

// Subscription projects the snapshot into the budget gate's input type. The gate
// uses 5h + weekly; per-model + overage ride along for metrics only.
func (s Snapshot) Subscription() budget.Subscription {
	return budget.Subscription{
		FiveHourPercent: s.FiveHour.Percent,
		FiveHourReset:   s.FiveHour.Reset,
		WeeklyPercent:   s.Weekly.Percent,
		WeeklyReset:     s.Weekly.Reset,
		// UpdatedAt IS the observation time, and carrying it across is what
		// makes budget.Config.MaxSnapshotAge able to fire at all: the
		// dispatcher's only view of the fleet snapshot is this projection, and
		// budget.SnapshotFresh reads a zero ObservedAt as "unknown, so fresh".
		ObservedAt:     s.UpdatedAt,
		OpusPercent:    s.Opus.Percent,
		OpusReset:      s.Opus.Reset,
		SonnetPercent:  s.Sonnet.Percent,
		SonnetReset:    s.Sonnet.Reset,
		OverageEnabled: s.Overage.Enabled,
		OveragePercent: s.Overage.Percent,
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

// SetIfNewer stores snap only when its UpdatedAt is strictly after what is
// held, and reports whether it did. It is the discipline the WRAPPER feed uses:
// snapshots arrive from many agent pods with no ordering guarantee, and the
// fleet-wide value must be the newest observation, not the last to be
// delivered.
//
// The POLLER keeps the unconditional Set: it is a single-goroutine
// authoritative fetch, and it is disabled in prod (USAGE_ENABLED=false). Two
// writers with different disciplines is a real, currently-unreachable sharp
// edge; if the poller is ever activated alongside the wrapper feed, decide
// deliberately which one wins rather than letting arrival order decide.
func (s *Store) SetIfNewer(snap Snapshot) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.snap.UpdatedAt.IsZero() && !snap.UpdatedAt.After(s.snap.UpdatedAt) {
		return false
	}
	s.snap = snap
	return true
}
