package controller

import (
	"testing"
	"time"

	"github.com/szymonrychu/tatara-operator/internal/scm"
)

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
