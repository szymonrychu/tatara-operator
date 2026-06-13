package v1alpha1

import "testing"

func TestScmSpecFields(t *testing.T) {
	p := ProjectSpec{Scm: &ScmSpec{
		Provider: "github", Owner: "acme", BotLogin: "tatara-bot",
		Board:           &BoardSpec{GitHubProjectNumber: 3, StatusField: "Status"},
		MergePolicy:     "afterApproval",
		PRReactionScope: "labeledOrMentioned",
		ApprovalLabel:   "tatara/awaiting-approval",
	}}
	if p.Scm.Owner != "acme" || p.Scm.Board.GitHubProjectNumber != 3 {
		t.Fatalf("scm spec not wired: %+v", p.Scm)
	}
}

func TestScmSpecLabelFields(t *testing.T) {
	s := ScmSpec{IdeaLabel: "tatara-idea", ApprovedLabel: "tatara-approved", RejectedLabel: "tatara-rejected"}
	if s.IdeaLabel != "tatara-idea" || s.ApprovedLabel != "tatara-approved" || s.RejectedLabel != "tatara-rejected" {
		t.Fatalf("label fields not set: %+v", s)
	}
}

func TestTaskNewFields(t *testing.T) {
	ts := TaskSpec{
		Kind: "review", ApprovalRequired: true,
		ProposedIssue: &ProposedIssueSpec{RepositoryRef: "r", Title: "T", Body: "B", Kind: "bug"},
		Source:        &TaskSource{AuthorLogin: "bob", IsPR: true, Number: 9},
	}
	if ts.Kind != "review" || !ts.ApprovalRequired || ts.ProposedIssue.Kind != "bug" || ts.Source.Number != 9 {
		t.Fatalf("task spec not wired: %+v", ts)
	}
	st := TaskStatus{
		DiscoveredIssues: []string{"https://x/1"},
		ReviewVerdict:    &ReviewVerdict{Decision: "approve", Body: "lgtm", Suggestions: []Suggestion{{Path: "a.go", Line: 1, Body: "x"}}},
		PROutcome:        &PROutcome{Action: "merge", Reason: "green"},
	}
	if st.ReviewVerdict.Decision != "approve" || st.PROutcome.Action != "merge" || len(st.DiscoveredIssues) != 1 {
		t.Fatalf("task status not wired: %+v", st)
	}
	if ConditionApprovalApproved != "ApprovalApproved" {
		t.Fatalf("condition const = %q", ConditionApprovalApproved)
	}
}
