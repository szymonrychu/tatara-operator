package accountusage

import (
	"testing"
	"time"
)

func TestStoreGetSetIsConcurrencySafeAndCopies(t *testing.T) {
	s := &Store{}
	if got := s.Get(); got.Healthy || !got.UpdatedAt.IsZero() {
		t.Fatal("zero store must be unhealthy/empty")
	}
	reset := time.Now().Add(time.Hour)
	s.Set(Snapshot{FiveHour: Window{Percent: 55, Reset: reset}, Healthy: true, UpdatedAt: time.Now()})
	got := s.Get()
	if got.FiveHour.Percent != 55 || !got.Healthy {
		t.Fatalf("Get mismatch: %+v", got)
	}
}

func TestStoreSetIfNewer(t *testing.T) {
	t0 := time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		seed    *Snapshot
		in      Snapshot
		wantOK  bool
		wantPct float64
		wantSrc string
	}{
		{
			name:    "empty store accepts",
			in:      Snapshot{UpdatedAt: t0, FiveHour: Window{Percent: 40}, Source: SourceWrapper},
			wantOK:  true,
			wantPct: 40,
			wantSrc: SourceWrapper,
		},
		{
			name:    "newer accepts",
			seed:    &Snapshot{UpdatedAt: t0, FiveHour: Window{Percent: 40}, Source: SourceWrapper},
			in:      Snapshot{UpdatedAt: t0.Add(time.Minute), FiveHour: Window{Percent: 55}, Source: SourceWrapper},
			wantOK:  true,
			wantPct: 55,
			wantSrc: SourceWrapper,
		},
		{
			name:    "older rejects",
			seed:    &Snapshot{UpdatedAt: t0.Add(time.Minute), FiveHour: Window{Percent: 55}, Source: SourceWrapper},
			in:      Snapshot{UpdatedAt: t0, FiveHour: Window{Percent: 40}, Source: SourceWrapper},
			wantOK:  false,
			wantPct: 55,
			wantSrc: SourceWrapper,
		},
		{
			name:    "equal timestamp rejects",
			seed:    &Snapshot{UpdatedAt: t0, FiveHour: Window{Percent: 55}, Source: SourceWrapper},
			in:      Snapshot{UpdatedAt: t0, FiveHour: Window{Percent: 40}, Source: SourceWrapper},
			wantOK:  false,
			wantPct: 55,
			wantSrc: SourceWrapper,
		},
		{
			name:    "a poller snapshot is not displaced by an older wrapper one",
			seed:    &Snapshot{UpdatedAt: t0.Add(time.Minute), FiveHour: Window{Percent: 80}, Source: SourcePoller},
			in:      Snapshot{UpdatedAt: t0, FiveHour: Window{Percent: 10}, Source: SourceWrapper},
			wantOK:  false,
			wantPct: 80,
			wantSrc: SourcePoller,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var s Store
			if tc.seed != nil {
				s.Set(*tc.seed)
			}
			if got := s.SetIfNewer(tc.in); got != tc.wantOK {
				t.Fatalf("SetIfNewer = %v, want %v", got, tc.wantOK)
			}
			got := s.Get()
			if got.FiveHour.Percent != tc.wantPct {
				t.Fatalf("FiveHour.Percent = %v, want %v", got.FiveHour.Percent, tc.wantPct)
			}
			if got.Source != tc.wantSrc {
				t.Fatalf("Source = %q, want %q", got.Source, tc.wantSrc)
			}
		})
	}
}

func TestSnapshotSubscriptionProjection(t *testing.T) {
	reset := time.Now().Add(time.Hour)
	snap := Snapshot{
		FiveHour: Window{Percent: 42, Reset: reset},
		Weekly:   Window{Percent: 71, Reset: reset},
		Opus:     Window{Percent: 80, Reset: reset},
	}
	sub := snap.Subscription()
	if sub.FiveHourPercent != 42 || sub.WeeklyPercent != 71 || sub.OpusPercent != 80 {
		t.Fatalf("projection mismatch: %+v", sub)
	}
	if !sub.FiveHourReset.Equal(reset) {
		t.Fatal("reset not projected")
	}
}

// TestSnapshotSubscriptionProjectsObservedAt is what makes
// budget.Config.MaxSnapshotAge real. The dispatcher's only view of the fleet
// snapshot is Store.Get().Subscription(); if the projection drops UpdatedAt then
// budget.SnapshotFresh always sees a ZERO ObservedAt, reads it as "unknown, so
// fresh", and the staleness bound never fires no matter how it is configured.
func TestSnapshotSubscriptionProjectsObservedAt(t *testing.T) {
	at := time.Now().Add(-3 * time.Hour)
	if got := (Snapshot{UpdatedAt: at}).Subscription().ObservedAt; !got.Equal(at) {
		t.Fatalf("ObservedAt = %v, want %v", got, at)
	}
	// An empty store must stay "unknown", never a spuriously ancient snapshot.
	if got := (Snapshot{}).Subscription().ObservedAt; !got.IsZero() {
		t.Fatalf("ObservedAt = %v for an empty snapshot, want zero", got)
	}
}
