package controller

import (
	"strings"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func sub(name string, order int, phase string) tatarav1alpha1.Subtask {
	s := tatarav1alpha1.Subtask{}
	s.Name = name
	s.Spec.Order = order
	s.Spec.Title = name + "-title"
	s.Spec.Detail = name + "-detail"
	s.Status.Phase = phase
	return s
}

func TestPlanTurnText_MentionsDecompose(t *testing.T) {
	txt := planTurnText("ship the feature", "tatara/task-abc", "proj1", "task-abc")
	if !strings.Contains(txt, "ship the feature") {
		t.Errorf("plan turn missing goal: %q", txt)
	}
	if !strings.Contains(strings.ToLower(txt), "subtask") {
		t.Errorf("plan turn missing subtask MCP instruction: %q", txt)
	}
}

func TestPlanTurnText_AllowsDirectImplementation(t *testing.T) {
	txt := planTurnText("fix a typo", "tatara/task-abc", "proj1", "task-abc")
	low := strings.ToLower(txt)
	if !strings.Contains(low, "implement it directly") {
		t.Errorf("plan turn should let the agent implement small tasks directly: %q", txt)
	}
	if strings.Contains(txt, "Do not start implementation") {
		t.Errorf("plan turn must not forbid implementation outright: %q", txt)
	}
}

func TestPlanTurnText_ContainsBranchDirective(t *testing.T) {
	const branch = "tatara/task-my-task"
	txt := planTurnText("do the thing", branch, "my-project", "my-task")
	if !strings.Contains(txt, branch) {
		t.Errorf("plan turn missing branch directive %q: %q", branch, txt)
	}
	if !strings.Contains(txt, "NEVER commit or push to the default branch") {
		t.Errorf("plan turn missing default-branch prohibition: %q", txt)
	}
}

func TestPlanTurnText_ContainsTaskAndProject(t *testing.T) {
	txt := planTurnText("goal", "tatara/task-foo", "my-project", "task-foo")
	if !strings.Contains(txt, "task-foo") {
		t.Errorf("plan turn missing task name: %q", txt)
	}
	if !strings.Contains(txt, "my-project") {
		t.Errorf("plan turn missing project name: %q", txt)
	}
	if !strings.Contains(txt, "subtask_create(task=`task-foo`") {
		t.Errorf("plan turn missing subtask_create directive: %q", txt)
	}
}

func TestNextPendingSubtask_PicksLowestOrder(t *testing.T) {
	subs := []tatarav1alpha1.Subtask{
		sub("c", 3, "Pending"),
		sub("a", 1, "Done"),
		sub("b", 2, "Pending"),
	}
	got, ok := nextPendingSubtask(subs)
	if !ok {
		t.Fatal("expected a pending subtask")
	}
	if got.Name != "b" {
		t.Errorf("next = %q, want b (lowest-order Pending)", got.Name)
	}
}

func TestNextPendingSubtask_NoneLeft(t *testing.T) {
	subs := []tatarav1alpha1.Subtask{sub("a", 1, "Done"), sub("b", 2, "Done")}
	if _, ok := nextPendingSubtask(subs); ok {
		t.Error("expected no pending subtask")
	}
}

func TestTurnText_TitleAndDetail(t *testing.T) {
	txt := turnText(sub("x", 1, "Pending"), "tatara/task-x", "task-x")
	if !strings.Contains(txt, "x-title") || !strings.Contains(txt, "x-detail") {
		t.Errorf("turn text missing title/detail: %q", txt)
	}
}

func TestTurnText_ContainsBranchReminder(t *testing.T) {
	const branch = "tatara/task-my-task"
	txt := turnText(sub("y", 2, "Pending"), branch, "task-my-task")
	if !strings.Contains(txt, branch) {
		t.Errorf("turn text missing branch reminder %q: %q", branch, txt)
	}
}

func TestTurnText_ContainsTaskReminder(t *testing.T) {
	txt := turnText(sub("z", 1, "Pending"), "tatara/task-z", "task-z")
	if !strings.Contains(txt, "task=`task-z`") {
		t.Errorf("turn text missing task reminder: %q", txt)
	}
}

func TestPlanTurnText_MentionsAllReposCloned(t *testing.T) {
	txt := planTurnText("do x", "tatara/task-abc", "proj1", "task-abc")
	low := strings.ToLower(txt)
	if !strings.Contains(low, "/workspace/") {
		t.Errorf("plan turn missing /workspace/ path: %q", txt)
	}
	if !strings.Contains(low, "each repo you") {
		t.Errorf("plan turn missing 'each repo you' instruction: %q", txt)
	}
}

func TestLifecycleTriageText_ApprovalInstructions(t *testing.T) {
	task := &tatarav1alpha1.Task{Spec: tatarav1alpha1.TaskSpec{Source: &tatarav1alpha1.TaskSource{IssueRef: "o/r#1", URL: "u"}}}
	got := lifecycleTriageText(task, "T", "B")
	if !strings.Contains(got, "human") || !strings.Contains(got, "approval comment") {
		t.Fatalf("triage prompt missing conversation-approval guidance:\n%s", got)
	}
}

func TestReviewText_ReviewAndTestAndVerdict(t *testing.T) {
	txt := reviewText("Review and test PR o/r#5", "proj1", "task-rev")
	low := strings.ToLower(txt)
	if !strings.Contains(low, "review") || !strings.Contains(low, "test") {
		t.Errorf("review prompt must mention review AND test: %q", txt)
	}
	if !strings.Contains(txt, "review_verdict") {
		t.Errorf("review prompt must require the review_verdict tool: %q", txt)
	}
	if !strings.Contains(low, "do not commit") && !strings.Contains(low, "not push") {
		t.Errorf("review prompt must say not to push/commit: %q", txt)
	}
	if !strings.Contains(txt, "task-rev") || !strings.Contains(txt, "proj1") {
		t.Errorf("review prompt missing task/project: %q", txt)
	}
}

func TestLifecyclePhaseGuidance_CommentPhaseNotRestored(t *testing.T) {
	g := lifecyclePhaseGuidance("Triage")
	if !strings.Contains(g, "Lifecycle phase: Triage") {
		t.Errorf("guidance missing phase name: %q", g)
	}
	if !strings.Contains(g, "transient") {
		t.Errorf("guidance missing transient-workspace note: %q", g)
	}
	if !strings.Contains(g, "NOT be restored") {
		t.Errorf("comment-phase guidance must say file edits are not restored: %q", g)
	}
	if strings.Contains(g, "ARE restored") {
		t.Errorf("comment-phase guidance must not promise restored changes: %q", g)
	}
}

func TestLifecyclePhaseGuidance_ImplementPhaseRestored(t *testing.T) {
	g := lifecyclePhaseGuidance("Implement")
	if !strings.Contains(g, "Lifecycle phase: Implement") {
		t.Errorf("guidance missing phase name: %q", g)
	}
	if !strings.Contains(g, "ARE restored") {
		t.Errorf("implement-phase guidance must say committed+pushed changes are restored: %q", g)
	}
	if !strings.Contains(g, "commit and push") {
		t.Errorf("implement-phase guidance must reference commit and push: %q", g)
	}
}

func TestLifecycleTriageText_IncludesPhaseGuidance(t *testing.T) {
	task := &tatarav1alpha1.Task{Spec: tatarav1alpha1.TaskSpec{Source: &tatarav1alpha1.TaskSource{IssueRef: "o/r#1", URL: "u"}}}
	got := lifecycleTriageText(task, "T", "B")
	if !strings.Contains(got, "## Lifecycle phase: Triage") {
		t.Errorf("triage prompt missing phase guidance: %q", got)
	}
	if !strings.Contains(got, "NOT be restored") {
		t.Errorf("triage prompt missing transient-workspace note: %q", got)
	}
}

func TestImplementPrompt_IncludesPhaseGuidance(t *testing.T) {
	task := &tatarav1alpha1.Task{Spec: tatarav1alpha1.TaskSpec{ProjectRef: "proj", Goal: "g", Kind: "issueLifecycle"}}
	task.Name = "task-phase"
	got := implementPrompt(task)
	if !strings.Contains(got, "## Lifecycle phase: Implement") {
		t.Errorf("implement prompt missing phase guidance: %q", got)
	}
	if !strings.Contains(got, "ARE restored") {
		t.Errorf("implement prompt missing restored-changes note: %q", got)
	}
}
