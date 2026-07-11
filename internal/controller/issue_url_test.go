package controller

import "testing"

func TestParseIssueURL_GitHub(t *testing.T) {
	repo, num, ok := parseIssueURL("https://github.com/owner/repo/issues/42")
	if !ok || repo != "owner/repo" || num != 42 {
		t.Fatalf("parseIssueURL() = (%q, %d, %v), want (owner/repo, 42, true)", repo, num, ok)
	}
}

func TestParseIssueURL_GitLabWithSubgroup(t *testing.T) {
	repo, num, ok := parseIssueURL("https://gitlab.com/group/subgroup/project/-/issues/7")
	if !ok || repo != "group/subgroup/project" || num != 7 {
		t.Fatalf("parseIssueURL() = (%q, %d, %v), want (group/subgroup/project, 7, true)", repo, num, ok)
	}
}

func TestParseIssueURL_NotAnIssueURL(t *testing.T) {
	if _, _, ok := parseIssueURL("https://github.com/owner/repo/pull/1"); ok {
		t.Fatal("parseIssueURL() must reject non-issue URLs")
	}
}
