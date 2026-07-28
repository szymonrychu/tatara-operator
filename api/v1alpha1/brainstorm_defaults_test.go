package v1alpha1

import "testing"

func ptr(i int) *int { return &i }

func TestResolveTarget(t *testing.T) {
	tests := []struct {
		name string
		act  BrainstormActivity
		want int
	}{
		{"both unset falls back to the default", BrainstormActivity{}, DefaultTargetOpenProposals},
		{"explicit target wins", BrainstormActivity{TargetOpenProposals: ptr(7)}, 7},
		{"legacy maxOpenProposals is honoured when target is unset", BrainstormActivity{MaxOpenProposals: 10}, 10},
		{"target beats the legacy alias", BrainstormActivity{TargetOpenProposals: ptr(2), MaxOpenProposals: 10}, 2},
		{"an explicit zero target disables refill", BrainstormActivity{TargetOpenProposals: ptr(0)}, 0},
		{"a negative target clamps to zero", BrainstormActivity{TargetOpenProposals: ptr(-4)}, 0},
		{"a non-positive legacy alias falls back to the default", BrainstormActivity{MaxOpenProposals: 0}, DefaultTargetOpenProposals},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.act.ResolveTarget(); got != tc.want {
				t.Fatalf("ResolveTarget() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestResolveHistoryWindow(t *testing.T) {
	tests := []struct {
		name string
		act  BrainstormActivity
		want int
	}{
		{"unset", BrainstormActivity{}, DefaultHistoryWindow},
		{"explicit", BrainstormActivity{HistoryWindow: ptr(5)}, 5},
		{"zero disables the block", BrainstormActivity{HistoryWindow: ptr(0)}, 0},
		{"negative clamps to zero", BrainstormActivity{HistoryWindow: ptr(-1)}, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.act.ResolveHistoryWindow(); got != tc.want {
				t.Fatalf("ResolveHistoryWindow() = %d, want %d", got, tc.want)
			}
		})
	}
}
