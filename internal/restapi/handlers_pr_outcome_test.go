package restapi

import "testing"

func TestPrOutcomeKindOK(t *testing.T) {
	tests := []struct {
		kind string
		want bool
	}{
		{"issueLifecycle", true},
		{"selfImprove", false},
		{"brainstorm", false},
		{"implement", false},
	}
	for _, tc := range tests {
		if got := prOutcomeKindOK(tc.kind); got != tc.want {
			t.Errorf("prOutcomeKindOK(%q) = %v, want %v", tc.kind, got, tc.want)
		}
	}
}
