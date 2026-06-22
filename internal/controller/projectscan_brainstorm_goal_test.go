package controller

import (
	"strings"
	"testing"
)

func TestBrainstormGoalDropsCommentPathAddsEarlyExit(t *testing.T) {
	g := brainstormGoalProject([]string{"o/a", "o/b"}, "STATE", "")
	if strings.Contains(g, "comment_on_issue") {
		t.Fatal("brainstorm goal must NOT instruct comment_on_issue (path-2 dropped)")
	}
	if !strings.Contains(g, "skip_brainstorm") {
		t.Fatal("brainstorm goal must instruct skip_brainstorm early-exit")
	}
	if !strings.Contains(g, "propose_issue") {
		t.Fatal("brainstorm goal must keep propose_issue path")
	}
	// Proposal must decompose into sub-problems and offer options per piece.
	for _, want := range []string{"sub-problem", "OPTIONS", "recommended"} {
		if !strings.Contains(g, want) {
			t.Fatalf("brainstorm goal must require decomposition+options; missing %q", want)
		}
	}
}
