package controller

import "testing"

func TestWeakTitle(t *testing.T) {
	tests := []struct {
		name     string
		in       string
		wantWeak bool
	}{
		{"empty", "", true},
		{"bare go", "Go", true},
		{"bare update", "update", true},
		{"too short", "fix bug", true},
		{"two words", "fix everything", true},
		{"denylist wip", "wip", true},
		{"good conventional", "fix(scan): dedup brainstorm proposals by systemic label", false},
		{"good plain", "Add main-branch CI health to the brainstorm survey", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotWeak, guidance := weakTitle(tc.in)
			if gotWeak != tc.wantWeak {
				t.Fatalf("weakTitle(%q) weak = %v, want %v", tc.in, gotWeak, tc.wantWeak)
			}
			if gotWeak && guidance == "" {
				t.Fatalf("weakTitle(%q) weak but empty guidance", tc.in)
			}
			if !gotWeak && guidance != "" {
				t.Fatalf("weakTitle(%q) strong but non-empty guidance %q", tc.in, guidance)
			}
		})
	}
}
