package controller

import (
	"testing"
	"time"

	"github.com/szymonrychu/tatara-operator/internal/scm"
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

// TestMrScanSrcCand_TitleThreaded guards the mrScan MRCI branch: the srcCand
// constructed from a candidate must carry the title so issueLifecycle tasks
// get a non-empty Source.Title for TaskBranch slug generation.
func TestMrScanSrcCand_TitleThreaded(t *testing.T) {
	pr := scm.PRRef{
		Repo: "owner/repo", Number: 42, Author: "alice",
		Body: "Fix the memory leak\n\nLonger description.", UpdatedAt: time.Now(),
	}
	cands := candidatesFromPRs([]scm.PRRef{pr})
	if len(cands) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(cands))
	}
	c := cands[0]
	// This replicates the mrScan MRCI srcCand construction.
	srcCand := candidate{
		repo: c.repo, number: c.number, author: c.author, isPR: true, title: c.title,
	}
	src := scanSourceFor("github", srcCand)
	if src.Title != "Fix the memory leak" {
		t.Fatalf("mrScan srcCand Source.Title = %q, want %q", src.Title, "Fix the memory leak")
	}
}

// TestBackstopCand_TitleThreaded guards the backstop candidate builder: title
// from scm.IssueRef must appear in the resulting TaskSource so lifecycle tasks
// recovered by backstop get a non-empty Source.Title.
func TestBackstopCand_TitleThreaded(t *testing.T) {
	iss := scm.IssueRef{
		Repo: "owner/repo", Number: 99, Title: "Triage stale issue",
		Labels: []string{"bug"}, UpdatedAt: time.Now(),
	}
	slug := iss.Repo
	// Replicates the backstop cand literal.
	cand := candidate{repo: slug, number: iss.Number, labels: iss.Labels, updatedAt: iss.UpdatedAt, title: iss.Title}
	src := scanSourceFor("github", cand)
	if src.Title != iss.Title {
		t.Fatalf("backstop cand Source.Title = %q, want %q", src.Title, iss.Title)
	}
}
