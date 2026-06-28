package incident

import (
	"strings"
	"testing"
)

func TestGoalProject(t *testing.T) {
	g := GoalProject("groupKey=abc status=firing commonLabels={alertname=HighCPU}", []string{"o/api", "o/web"})
	for _, kw := range []string{"o/api", "o/web", "groupKey=abc", "grafana", "propose_issue", "read-only"} {
		if !strings.Contains(g, kw) {
			t.Fatalf("incident goal missing %q:\n%s", kw, g)
		}
	}
	// Must forbid remediation/write actions.
	if !strings.Contains(g, "Do NOT") || !strings.Contains(strings.ToLower(g), "remediat") {
		t.Fatalf("incident goal must forbid remediation:\n%s", g)
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
