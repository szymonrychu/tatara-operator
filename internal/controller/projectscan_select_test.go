package controller

import (
	"testing"
	"time"

	"github.com/szymonrychu/tatara-operator/internal/scm"
)

func TestSelectPriorityThenStale(t *testing.T) {
	base := time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC)
	cands := []candidate{
		{repo: "o/r", number: 1, labels: nil, updatedAt: base.Add(3 * time.Hour)},
		{repo: "o/r", number: 2, labels: []string{"tatara/priority"}, updatedAt: base.Add(2 * time.Hour)},
		{repo: "o/r", number: 3, labels: nil, updatedAt: base.Add(1 * time.Hour)},
		{repo: "o/r", number: 4, labels: []string{"tatara/priority"}, updatedAt: base.Add(4 * time.Hour)},
	}
	cases := []struct {
		name      string
		priority  string
		n         int
		wantOrder []int
	}{
		{"priority first then stale, cap 3", "tatara/priority", 3, []int{2, 4, 3}},
		{"no priority label = pure stale", "", 2, []int{3, 1}},
		{"cap 1 picks stalest priority", "tatara/priority", 1, []int{2}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := selectCandidates(cands, tc.priority, tc.n)
			if len(got) != len(tc.wantOrder) {
				t.Fatalf("len = %d, want %d (%+v)", len(got), len(tc.wantOrder), got)
			}
			for i, want := range tc.wantOrder {
				if got[i].number != want {
					t.Fatalf("pos %d = #%d, want #%d (%+v)", i, got[i].number, want, got)
				}
			}
		})
	}
}

var _ = scm.PRRef{}
