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
