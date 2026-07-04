// Copyright 2026 tatara authors.

package controller

import "testing"

func TestTerminalOutcome(t *testing.T) {
	cases := []struct {
		name             string
		to, reason       string
		implementGiveUps int
		want             string
	}{
		{"done delivered", "Done", "", 0, "delivered"},
		{"parked recoverable implement-failed", "Parked", "implement-failed", 0, "churned"},
		{"parked recoverable maxIterations", "Parked", "maxIterations", 0, "churned"},
		{"parked recoverable refused-no-explanation", "Parked", "refused-no-explanation", 0, "churned"},
		{"parked recoverable deadline", "Parked", "deadline", 0, "churned"},
		{"parked giveups nonzero overrides non-recoverable reason", "Parked", "refused", 2, "churned"},
		{"parked non-recoverable deliberate decline", "Parked", "refused", 0, "abandoned"},
		{"parked non-recoverable duplicate", "Parked", "duplicate", 0, "abandoned"},
		{"stopped abandoned", "Stopped", "", 0, "abandoned"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := terminalOutcome(tc.to, tc.reason, tc.implementGiveUps); got != tc.want {
				t.Fatalf("terminalOutcome(%q, %q, %d) = %q, want %q", tc.to, tc.reason, tc.implementGiveUps, got, tc.want)
			}
		})
	}
}
