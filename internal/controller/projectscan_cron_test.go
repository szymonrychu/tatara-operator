package controller

import (
	"testing"
	"time"
)

func TestActivityNextFire(t *testing.T) {
	base := time.Date(2026, 6, 11, 10, 0, 0, 0, time.UTC)
	cases := []struct {
		name     string
		schedule string
		last     time.Time
		wantOK   bool
		wantDue  bool // now is at/after next
		now      time.Time
	}{
		{"empty disables", "", base, false, false, base},
		{"hourly not yet due", "0 * * * *", base, true, false, base.Add(30 * time.Minute)},
		{"hourly due", "0 * * * *", base, true, true, base.Add(90 * time.Minute)},
		{"bad cron disabled", "not a cron", base, false, false, base},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			next, ok := activityNextFire(tc.schedule, tc.last)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			due := !tc.now.Before(next)
			if due != tc.wantDue {
				t.Fatalf("due = %v at now=%v next=%v, want %v", due, tc.now, next, tc.wantDue)
			}
		})
	}
}
