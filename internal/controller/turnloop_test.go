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
	txt := planTurnText("ship the feature")
	if !strings.Contains(txt, "ship the feature") {
		t.Errorf("plan turn missing goal: %q", txt)
	}
	if !strings.Contains(strings.ToLower(txt), "subtask") {
		t.Errorf("plan turn missing subtask MCP instruction: %q", txt)
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
	txt := turnText(sub("x", 1, "Pending"))
	if !strings.Contains(txt, "x-title") || !strings.Contains(txt, "x-detail") {
		t.Errorf("turn text missing title/detail: %q", txt)
	}
}
