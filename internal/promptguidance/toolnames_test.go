package promptguidance

import (
	"reflect"
	"testing"
)

// The false-positive cases are not hypothetical: `decks/`, `%ct`, `values.yaml`
// and `workspace/<owner>/<repo>` are all shapes that appear in real agent prose
// on this platform. A checker that flags them is a checker that gets an
// allow-list bolted onto it within a week, and an allow-list is how the
// original list_handoffs bug survived four cycles.
func TestUnknownToolNames(t *testing.T) {
	tests := []struct {
		name  string
		prose string
		want  []string
	}{
		{"real tool passes", "call `task_list` for this project", nil},
		{"real tool in call form passes", "emit `submit_outcome(action=skip, reason=...)`", nil},
		{"real tool with quoted args passes", "read one Task with `task_context(task=<name>, notes=\"all\")`", nil},
		{"bogus tool is reported", "call `list_handoffs` for this project", []string{"list_handoffs"}},
		{"bogus tool in call form is reported", "call `skip_research(reason)` and STOP", []string{"skip_research"}},
		{"reaped tool is reported", "then call `propose_issue`", []string{"propose_issue"}},
		{"folded code-graph tools are reported", "`code_related` and `code_bridges` and `code_related`",
			[]string{"code_bridges", "code_related"}},
		{"trailing-slash path is not a tool", "the most recently updated dir under `decks/`", nil},
		{"format verb is not a tool", "sort by `%ct`", nil},
		{"filename is not a tool", "no plain ENVs in `values.yaml`", nil},
		{"clone path is not a tool", "cloned read-only into `workspace/<owner>/<repo>`", nil},
		{"skill name is not a tool", "invoke the `tatara-council-brainstorm` skill", nil},
		{"arg assignment is not a tool", "then `decision=close` on the issue", nil},
		{"upper-case marker is not a tool", "the tool returns `MEMORY_DEGRADED: ...`", nil},
		{"branch name is not a tool", "branch `task/x` has the fix", nil},
		{"bundle element is not a tool", "read the `<proposal_history>` block", nil},
		{"empty span is not a tool", "an empty `` span", nil},
		// KNOWN LIMIT, asserted so it is a decision and not an accident: the
		// check only sees backticked mentions. internal/refine/goal.go names
		// six reaped tools in unbackticked prose and passes this check
		// vacuously. Fixing that goal is tracked separately.
		{"unbackticked bogus name is NOT caught", "call list_handoffs for this project", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := UnknownToolNames(tc.prose)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("UnknownToolNames(%q) = %v, want %v", tc.prose, got, tc.want)
			}
		})
	}
}

// The mirror is hand-maintained, so its own shape is worth a guard: a typo like
// "task list" or a duplicate would make every conformance test pass for the
// wrong reason.
func TestAgentVisibleToolsIsSortedUniqueAndToolShaped(t *testing.T) {
	seen := map[string]bool{}
	for i, name := range AgentVisibleTools {
		if !toolNameShape.MatchString(name) {
			t.Fatalf("AgentVisibleTools[%d] = %q is not tool-shaped", i, name)
		}
		if seen[name] {
			t.Fatalf("AgentVisibleTools has duplicate %q", name)
		}
		seen[name] = true
		if i > 0 && AgentVisibleTools[i-1] >= name {
			t.Fatalf("AgentVisibleTools is not sorted: %q before %q", AgentVisibleTools[i-1], name)
		}
	}
	if len(AgentVisibleTools) != 21 {
		t.Fatalf("AgentVisibleTools has %d entries, want 21; if tatara-cli added or removed a tool, "+
			"update this count with the list", len(AgentVisibleTools))
	}
}

// The four guidance constants are appended to nearly every agent goal, so a
// stale tool name in one of them reaches every agent kind at once. They are
// checked here directly rather than transitively through each goal builder.
func TestGuidanceConstantsNameOnlyRealTools(t *testing.T) {
	for name, prose := range map[string]string{
		"PlatformProblemGuidance": PlatformProblemGuidance,
		"MemoryDegradedGuidance":  MemoryDegradedGuidance,
		"ToolingNoteGuidance":     ToolingNoteGuidance,
		"ToolingConsumeGuidance":  ToolingConsumeGuidance,
	} {
		t.Run(name, func(t *testing.T) {
			if bad := UnknownToolNames(prose); len(bad) > 0 {
				t.Fatalf("%s names tools that do not exist: %v\n"+
					"Every tool an agent is told to call must be in AgentVisibleTools, "+
					"whose source of truth is tatara-cli's MCP registry.", name, bad)
			}
		})
	}
}
