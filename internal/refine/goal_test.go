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
	for _, want := range []string{"go/nogo", "trigger label", "do not", "skip", "live"} {
		if !strings.Contains(low, want) {
			t.Fatalf("goal missing approval/non-escalation guidance %q", want)
		}
	}
}

func TestGoalProject_GaveUpCategory(t *testing.T) {
	g := refine.GoalProject([]string{"szymonrychu/tatara-operator"}, 30)

	// The gave-up category, its three-tier action policy, and the live-task guard
	// must all be present (spec: close-if-resolved / comment-if<3 / escalate-if>=3,
	// never touch a live task).
	for _, want := range []string{
		"implementGiveUps", "Parked",
		"implementGiveUps < 3", "implementGiveUps >= 3",
		"refined", "scope", "escalate",
		"NEVER act on a task in any other lifecycleState",
	} {
		if !strings.Contains(g, want) {
			t.Fatalf("goal missing gave-up guidance %q", want)
		}
	}

	// Under-cap and at-cap tiers must NOT close (close is reserved for
	// already-delivered/duplicate/obsolete); the prompt must say so.
	if !strings.Contains(g, "Do NOT close") {
		t.Fatalf("goal must instruct NOT to close under-cap/at-cap gave-up issues")
	}
}
