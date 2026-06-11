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
		{"no priority label = pure stale", "", 2, []int{3, 2}},
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

func TestCandidatesFromPRs(t *testing.T) {
	prs := []scm.PRRef{
		{Repo: "o/r", Number: 5, Author: "alice", HeadSHA: "abc", Labels: []string{"x"}, UpdatedAt: time.Unix(100, 0)},
	}
	got := candidatesFromPRs(prs)
	if len(got) != 1 || got[0].number != 5 || got[0].author != "alice" || got[0].headSHA != "abc" || !got[0].isPR {
		t.Fatalf("candidatesFromPRs = %+v", got)
	}
}

func TestCandidatesFromIssues(t *testing.T) {
	iss := []scm.IssueRef{
		{Repo: "o/r", Number: 7, Labels: []string{"bug"}, UpdatedAt: time.Unix(100, 0), IsPR: false},
		{Repo: "o/r", Number: 8, IsPR: true}, // filtered out
	}
	got := candidatesFromIssues(iss)
	if len(got) != 1 || got[0].number != 7 || got[0].isPR {
		t.Fatalf("candidatesFromIssues should drop IsPR rows: %+v", got)
	}
}
