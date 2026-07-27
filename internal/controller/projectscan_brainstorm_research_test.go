package controller

import (
	"strings"
	"testing"
)

// TestBrainstormGoalNamesCodeQualitySkillAndGrounding verifies that the brainstorm goal
// instructs the agent to use the code-quality skill backed by real on-disk code and
// the code-graph MCP tools, not just the deep-research/ADR path.
func TestBrainstormGoalNamesCodeQualitySkillAndGrounding(t *testing.T) {
	g := brainstormGoalProject([]string{"o/a", "o/b"}, "STATE", "", 3)

	// Must name the code-quality proposal skill.
	if !strings.Contains(g, "tatara-code-quality-proposal") {
		t.Fatal("brainstorm goal must name tatara-code-quality-proposal skill")
	}

	// Must ground proposals in real code: on-disk clones and code-graph tools.
	// code_context and code_graph are the post-fold names; code_related,
	// code_important, code_cross_repo, code_bridges and code_communities were
	// folded into their rel=/op= arguments and no longer exist as tools.
	for _, want := range []string{
		"workspace/", "code_search", "code_context", "code_graph", "code_explain",
		"simplification", "robustness",
	} {
		if !strings.Contains(g, want) {
			t.Fatalf("brainstorm goal missing code-quality grounding keyword %q", want)
		}
	}

	// The early exit is submit_outcome(action=skip); skip_research never existed
	// as a tool and is in tatara-cli's TestReapedToolsAreGone.
	if !strings.Contains(g, "action=skip") {
		t.Fatal("brainstorm goal must name submit_outcome(action=skip) for the early exit")
	}
	if strings.Contains(g, "skip_research") {
		t.Fatal("brainstorm goal must NOT name the reaped skip_research tool")
	}
}

// TestBrainstormGoalNoDanglingSkipBrainstorm is the regression guard that
// confirms the dangling skip_brainstorm prompt token has been removed.
// This test FAILS on the unmodified codebase.
func TestBrainstormGoalNoDanglingSkipBrainstorm(t *testing.T) {
	g := brainstormGoalProject([]string{"o/a", "o/b"}, "STATE", "", 3)
	if strings.Contains(g, "skip_brainstorm") {
		t.Fatal("brainstorm goal must NOT contain dangling skip_brainstorm token; use submit_outcome(action=skip) instead")
	}
}
