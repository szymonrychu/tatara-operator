package incident

import (
	"strings"
	"testing"
)

func TestGoalProject(t *testing.T) {
	g := GoalProject("groupKey=abc status=firing commonLabels={alertname=HighCPU}", []string{"o/api", "o/web"})
	for _, kw := range []string{"o/api", "o/web", "groupKey=abc", "grafana", "submit_outcome", "read-only"} {
		if !strings.Contains(g, kw) {
			t.Fatalf("incident goal missing %q:\n%s", kw, g)
		}
	}
	// Must forbid remediation/write actions.
	if !strings.Contains(g, "Do NOT") || !strings.Contains(strings.ToLower(g), "remediat") {
		t.Fatalf("incident goal must forbid remediation:\n%s", g)
	}
}

// TestGoalProject_NoNonexistentTools is a regression test: the incident goal
// must never name comment_on_issue/propose_issue - tools the incident MCP
// profile does not expose (the #328 root cause B). Linking a genuinely-new-
// but-related issue to an open tracker is expressed through submit_outcome's
// issue.parent, executed by the operator (O8), not a dedicated agent tool.
func TestGoalProject_NoNonexistentTools(t *testing.T) {
	g := GoalProject("ALERTCTX", []string{"o/r1", "o/r2"})
	for _, bad := range []string{"comment_on_issue", "propose_issue"} {
		if strings.Contains(g, bad) {
			t.Fatalf("goal still names nonexistent tool %q", bad)
		}
	}
	for _, want := range []string{"submit_outcome", "file_issue", "false_positive", "parent", "o/r1"} {
		if !strings.Contains(g, want) {
			t.Fatalf("goal missing expected guidance %q", want)
		}
	}
}

func TestGoalProjectSelfReportsPlatformProblems(t *testing.T) {
	g := GoalProject("groupKey=abc status=firing commonLabels={alertname=HighCPU}", []string{"szymonrychu/tatara"})
	if !strings.Contains(g, "report_internal_issue") {
		t.Error("incident goal must instruct self-report via report_internal_issue")
	}
	if !strings.Contains(g, "platform or tooling failure") {
		t.Error("incident goal missing platform-problem block")
	}
	// regression: the old mis-routing sentence must be gone
	if strings.Contains(g, "still file the issue") {
		t.Error("incident goal must NOT tell the agent to file an issue when grafana MCP is unreachable")
	}
}

func TestGoalProjectContainsToolingNoteGuidance(t *testing.T) {
	g := GoalProject("groupKey=abc status=firing commonLabels={alertname=HighCPU}", []string{"szymonrychu/tatara"})
	if !strings.Contains(g, "## Tooling you needed") {
		t.Error("incident goal must contain tooling-note guidance so proposer folds mise tools into the issue")
	}
	if !strings.Contains(g, ".mise.toml") {
		t.Error("incident goal tooling-note guidance must mention .mise.toml")
	}
}

func TestGoalProjectMandatesIncidentSRESkill(t *testing.T) {
	g := GoalProject("groupKey=abc status=firing commonLabels={alertname=HighCPU}", []string{"o/api"})
	for _, want := range []string{"tatara-incident-sre", "tatara-incident-investigation"} {
		if !strings.Contains(g, want) {
			t.Fatalf("incident goal must mandate %q", want)
		}
	}
	if strings.Index(g, "tatara-incident-sre") > strings.Index(g, "A Grafana alert is FIRING") {
		t.Fatal("incident-sre mandate must be prepended before the alert body")
	}
}

// TestGoalProjectContainsDedupInstruction asserts the incident goal instructs the
// agent to survey existing open issues and NOT open a duplicate when the problem
// is already tracked (cross-source dedup, finding #5).
func TestGoalProjectContainsDedupInstruction(t *testing.T) {
	g := GoalProject("groupKey=abc status=firing commonLabels={alertname=HighCPU}", []string{"o/api"})
	low := strings.ToLower(g)
	if !strings.Contains(low, "existing") || !strings.Contains(low, "duplicate") {
		t.Fatalf("incident goal must instruct surveying existing issues to avoid duplicates:\n%s", g)
	}
}

// TestGoalProject_MentionsTaskSurvey asserts the incident goal instructs the
// agent to also survey open incident Tasks (via task_list), not just issues -
// so a recurring alert dedups onto an in-flight investigation before it has
// even filed an issue.
func TestGoalProject_MentionsTaskSurvey(t *testing.T) {
	g := GoalProject("groupKey=abc status=firing commonLabels={alertname=HighCPU}", []string{"o/api"})
	if !strings.Contains(g, "task_list") {
		t.Fatal("incident goal must instruct the agent to survey open incident Tasks via task_list, not just issues")
	}
}
