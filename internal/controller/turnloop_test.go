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
