package refine_test

import (
	"strings"
	"testing"

	"github.com/szymonrychu/tatara-operator/internal/refine"
)

func TestGoalProject_GroomsExistingBacklogOnly(t *testing.T) {
	g := refine.GoalProject([]string{"szymonrychu/tatara-operator", "szymonrychu/tatara-cli"}, 30)

	// Input + the close/dedup/edit action set, in terms of the real tools:
	// scm_read to read the backlog, issue_write to narrow scope in-turn, and
	// submit_outcome's closes/links/folds arrays as the only way to close or
	// cross-reference an issue.
	for _, want := range []string{
		"scm_read", "issue_write", "submit_outcome", "closes", "links", "folds",
		"duplicate", "already", "commit", "30",
		"tatara-operator", "tatara-cli",
	} {
		if !strings.Contains(g, want) {
			t.Fatalf("goal missing %q", want)
		}
	}

	// No stale tool names, and no issue creation: refine grooms the existing
	// backlog, it does not file new issues (followups/splits) that would
	// cascade into triage agents.
	for _, absent := range []string{
		"list_issues", "list_commits", "close_issue", "edit_issue", "submit_turn", "exit_plan_mode",
		"create_issue", "Followup", "Split",
	} {
		if strings.Contains(g, absent) {
			t.Fatalf("goal must not mention %q", absent)
		}
	}

	// Refined actionable issues await the human go/nogo gate; the refiner never
	// escalates them itself (no trigger label, no implementation).
	low := strings.ToLower(g)
	for _, want := range []string{"go/nogo", "trigger label", "do not", "skip", "live"} {
		if !strings.Contains(low, want) {
			t.Fatalf("goal missing approval/non-escalation guidance %q", want)
		}
	}
}

func TestGoalProject_ContainsToolingNoteGuidance(t *testing.T) {
	g := refine.GoalProject([]string{"szymonrychu/tatara-operator"}, 30)
	if !strings.Contains(g, "## Tooling you needed") {
		t.Fatal("refine goal must contain tooling-note guidance so proposer folds mise tools into the issue")
	}
	if !strings.Contains(g, ".mise.toml") {
		t.Fatal("refine goal tooling-note guidance must mention .mise.toml")
	}
}

func TestGoalProject_MandatesGroomerSkill(t *testing.T) {
	g := refine.GoalProject([]string{"szymonrychu/tatara-operator"}, 30)
	if !strings.Contains(g, "tatara-backlog-groomer") {
		t.Fatal("refine goal must mandate the tatara-backlog-groomer skill FIRST")
	}
	if strings.Index(g, "tatara-backlog-groomer") > strings.Index(g, "project refiner") {
		t.Fatal("groomer mandate must be prepended before the refiner body")
	}
}
