package scm

import "testing"

func TestValueTypesZero(t *testing.T) {
	r := IssueReq{Title: "t", Body: "b", Labels: []string{"x"}}
	if r.Title != "t" || r.Labels[0] != "x" {
		t.Fatalf("IssueReq fields not wired: %+v", r)
	}
	ref := CreatedIssue{Ref: "o/r#1", URL: "https://x/1"}
	if ref.Ref != "o/r#1" || ref.URL == "" {
		t.Fatalf("CreatedIssue fields not wired: %+v", ref)
	}
	st := PRState{Author: "a", HeadSHA: "sha", HeadBranch: "br", Mergeable: true, CIStatus: "success"}
	if !st.Mergeable || st.CIStatus != "success" {
		t.Fatalf("PRState fields not wired: %+v", st)
	}
	s := Suggestion{Path: "a.go", Line: 12, Body: "fix"}
	if s.Line != 12 {
		t.Fatalf("Suggestion fields not wired: %+v", s)
	}
	b := BoardRef{Provider: "github", Owner: "o", GitHubProjectNumber: 3, GitLabBoardID: 0, StatusField: "Status"}
	if b.GitHubProjectNumber != 3 || b.StatusField != "Status" {
		t.Fatalf("BoardRef fields not wired: %+v", b)
	}
	ev := WebhookEvent{AuthorLogin: "bot", ActorLogin: "alice", Action: "labeled", Number: 7, IsPR: true, HeadSHA: "deadbeef", HeadBranch: "feat", ChangedLabel: "tatara/awaiting-approval"}
	if ev.AuthorLogin != "bot" || ev.ActorLogin != "alice" || !ev.IsPR || ev.ChangedLabel == "" {
		t.Fatalf("WebhookEvent new fields not wired: %+v", ev)
	}
}

func TestProvidersSatisfySCMWriter(t *testing.T) {
	var _ SCMWriter = (*GitHub)(nil)
	var _ SCMWriter = (*GitLab)(nil)
}
