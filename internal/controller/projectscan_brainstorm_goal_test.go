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
	if !strings.Contains(g, "skip_research") {
		t.Fatal("brainstorm goal must instruct skip_research early-exit")
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

func TestBrainstormGoalPrioritizesHandoffsFirst(t *testing.T) {
	g := brainstormGoalProject([]string{"o/a", "o/b"}, "STATE", "")

	for _, want := range []string{"list_handoffs", "get_handoff"} {
		if !strings.Contains(g, want) {
			t.Fatalf("brainstorm goal missing handoff tool %q", want)
		}
	}
	low := strings.ToLower(g)
	for _, want := range []string{"handoff", "maxopenproposals", "continu"} {
		if !strings.Contains(low, want) {
			t.Fatalf("brainstorm goal missing handoff-prioritize guidance %q", want)
		}
	}
	// Handoff prioritization must precede the fresh-ideas mandate.
	if strings.Index(low, "list_handoffs") > strings.Index(low, "mandate") {
		t.Fatalf("brainstorm goal must list_handoffs BEFORE the fresh-ideas research pass")
	}
}

func TestBrainstormGoalMandatesCouncilSkill(t *testing.T) {
	g := brainstormGoalProject([]string{"o/a", "o/b"}, "STATE", "")
	for _, want := range []string{
		"tatara-council-brainstorm",
		"tatara-deep-research",
		"do NOT call",
	} {
		if !strings.Contains(g, want) {
			t.Fatalf("brainstorm goal must mandate council skill; missing %q", want)
		}
	}
	// The council mandate must precede the existing repo-awareness body.
	if strings.Index(g, "tatara-council-brainstorm") > strings.Index(g, "READ REAL CODE") {
		t.Fatal("council mandate must be prepended before the READ REAL CODE body")
	}
	// Regression guard: must not introduce the lowercase word "mandate" ahead of
	// list_handoffs (TestBrainstormGoalPrioritizesHandoffsFirst relies on order).
	low := strings.ToLower(g)
	if strings.Contains(low, "mandate") && strings.Index(low, "mandate") < strings.Index(low, "list_handoffs") {
		t.Fatal("prepend must not add a 'mandate' keyword before list_handoffs")
	}
}
