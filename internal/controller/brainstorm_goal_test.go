package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBrainstormGoalProject_CodeQualityGrounding(t *testing.T) {
	goal := brainstormGoalProject([]string{"szymonrychu/tatara-operator", "szymonrychu/tatara-cli"}, "ISSUES: none", "", 3)
	for _, want := range []string{
		"workspace/",     // on-disk clone layout
		"code_search",    // code-graph grounding
		"code_graph",     // code-graph grounding, post-fold tool name
		"simplification", // target
		"robustness",     // target
		"action=propose", // read-only proposer contract, via submit_outcome
		"action=skip",    // early exit preserved, via submit_outcome
	} {
		assert.Contains(t, goal, want, "goal must mention %q", want)
	}
	for _, reaped := range []string{"propose_issue", "skip_research"} {
		assert.NotContains(t, goal, reaped,
			"goal must not name %q: it is a reaped tool, see tatara-cli TestReapedToolsAreGone", reaped)
	}
	assert.Contains(t, goal, "tatara-operator", "must name the target repos")
}
