package refine_test

import (
	"strings"
	"testing"

	"github.com/szymonrychu/tatara-operator/internal/refine"
)

func TestGoalProject_GroomsExistingBacklogOnly(t *testing.T) {
	g := refine.GoalProject([]string{"szymonrychu/tatara-operator", "szymonrychu/tatara-cli"}, 30)

	// Input + the close/dedup/edit action set.
	for _, want := range []string{
		"list_issues", "list_commits", "close_issue", "edit_issue",
		"duplicate", "already", "commit", "30",
		"tatara-operator", "tatara-cli",
	} {
		if !strings.Contains(g, want) {
			t.Fatalf("goal missing %q", want)
		}
	}

	// No issue creation: refine grooms the existing backlog, it does not file new
	// issues (followups/splits) that would cascade into triage agents.
	for _, absent := range []string{"create_issue", "Followup", "Split"} {
		if strings.Contains(g, absent) {
			t.Fatalf("goal must not mention %q (no issue creation)", absent)
		}
	}

	// Refined actionable issues await the human go/nogo gate; the refiner never
	// escalates them itself (no trigger label, no implementation).
	low := strings.ToLower(g)
	for _, want := range []string{"go/nogo", "trigger label", "do not", "skip", "in flight"} {
		if !strings.Contains(low, want) {
			t.Fatalf("goal missing approval/non-escalation guidance %q", want)
		}
	}
}
