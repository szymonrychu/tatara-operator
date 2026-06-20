package controller

import (
	"testing"
)

func TestCreateScanTask_SetsSourceTitle(t *testing.T) {
	tests := []struct {
		name      string
		cand      candidate
		wantTitle string
	}{
		{name: "issue title threaded", cand: candidate{repo: "o/r", number: 7, title: "Fix flaky CI on push"}, wantTitle: "Fix flaky CI on push"},
		{name: "empty title stays empty", cand: candidate{repo: "o/r", number: 8}, wantTitle: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			src := scanSourceFor("github", tc.cand)
			if src.Title != tc.wantTitle {
				t.Fatalf("Source.Title = %q, want %q", src.Title, tc.wantTitle)
			}
		})
	}
}
