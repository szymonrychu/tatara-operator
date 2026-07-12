package controller

import (
	"strings"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func TestRenderLinksBlock_TwoURLs(t *testing.T) {
	got := RenderLinksBlock([]string{"o/a#1", "o/b#2"})
	want := "<!-- tatara-links:start -->\nRelated issues (same task): o/a#1, o/b#2\n<!-- tatara-links:end -->"
	if got != want {
		t.Fatalf("RenderLinksBlock() = %q, want %q", got, want)
	}
}

func TestSpliceLinksBlock_AppendsWhenAbsent(t *testing.T) {
	body := "original body text"
	block := RenderLinksBlock([]string{"o/a#1"})
	got := SpliceLinksBlock(body, block)
	if !strings.Contains(got, "original body text") || !strings.Contains(got, block) {
		t.Fatalf("SpliceLinksBlock() = %q, want original body + appended block", got)
	}
}

func TestSpliceLinksBlock_IdempotentRewrite(t *testing.T) {
	body := "original body text\n\n" + RenderLinksBlock([]string{"o/a#1"})
	newBlock := RenderLinksBlock([]string{"o/a#1", "o/b#2"})
	got := SpliceLinksBlock(body, newBlock)
	if strings.Count(got, "tatara-links:start") != 1 {
		t.Fatalf("SpliceLinksBlock() must rewrite in place, not duplicate the block: %q", got)
	}
	if !strings.Contains(got, "o/b#2") || !strings.Contains(got, "original body text") {
		t.Fatalf("SpliceLinksBlock() = %q, want rewritten block + preserved surrounding body", got)
	}
}

func TestAllIssueSiblingURLs_UnionsWorkItemsDiscoveredAndCrossRepo(t *testing.T) {
	task := &tatarav1alpha1.Task{
		Status: tatarav1alpha1.TaskStatus{
			WorkItems: []tatarav1alpha1.WorkItemRef{
				{Provider: "github", Repo: "o/r", Number: 7, Kind: tatarav1alpha1.WorkItemIssue},
				{Provider: "github", Repo: "o/r", Number: 91, Kind: tatarav1alpha1.WorkItemPR}, // PR, excluded
			},
			DiscoveredIssues: []string{"https://github.com/o/r/issues/8"},
		},
		Spec: tatarav1alpha1.TaskSpec{
			SystemicGroup: &tatarav1alpha1.SystemicGroup{
				SystemicID: "sg1",
				CrossRepo:  []string{"o/other#3 - the other issue"},
			},
		},
	}
	got := allIssueSiblingURLs(task)
	want := []string{
		"https://github.com/o/r/issues/7",
		"https://github.com/o/r/issues/8",
		"https://github.com/o/other/issues/3",
	}
	if len(got) != len(want) {
		t.Fatalf("allIssueSiblingURLs() = %v, want %v", got, want)
	}
	for i, u := range want {
		if got[i] != u {
			t.Errorf("allIssueSiblingURLs()[%d] = %q, want %q", i, got[i], u)
		}
	}
}

func TestAllIssueSiblingURLs_DedupesAndSkipsBelowThreshold(t *testing.T) {
	task := &tatarav1alpha1.Task{
		Status: tatarav1alpha1.TaskStatus{
			WorkItems: []tatarav1alpha1.WorkItemRef{
				{Provider: "github", Repo: "o/r", Number: 7, Kind: tatarav1alpha1.WorkItemIssue},
			},
			DiscoveredIssues: []string{"https://github.com/o/r/issues/7"}, // duplicate of WorkItems entry
		},
	}
	got := allIssueSiblingURLs(task)
	if len(got) != 1 {
		t.Fatalf("allIssueSiblingURLs() = %v, want exactly 1 deduped URL", got)
	}
}
