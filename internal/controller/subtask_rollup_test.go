package controller

import (
	"strings"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func TestUpsertSubtaskRollup_AddsNewInOrder(t *testing.T) {
	var status tatarav1alpha1.TaskStatus
	upsertSubtaskRollup(&status, tatarav1alpha1.SubtaskRef{Name: "b", Order: 2, Title: "second", Phase: "Pending"})
	upsertSubtaskRollup(&status, tatarav1alpha1.SubtaskRef{Name: "a", Order: 1, Title: "first", Phase: "Pending"})
	if len(status.Subtasks) != 2 || status.Subtasks[0].Name != "a" || status.Subtasks[1].Name != "b" {
		t.Fatalf("Subtasks = %+v, want [a,b] sorted by Order", status.Subtasks)
	}
}

func TestUpsertSubtaskRollup_UpdatesExistingByName(t *testing.T) {
	var status tatarav1alpha1.TaskStatus
	upsertSubtaskRollup(&status, tatarav1alpha1.SubtaskRef{Name: "a", Order: 1, Title: "first", Phase: "Pending"})
	upsertSubtaskRollup(&status, tatarav1alpha1.SubtaskRef{Name: "a", Order: 1, Title: "first", Phase: "Done", Result: "done result"})
	if len(status.Subtasks) != 1 || status.Subtasks[0].Phase != "Done" || status.Subtasks[0].Result != "done result" {
		t.Fatalf("Subtasks = %+v, want single updated entry", status.Subtasks)
	}
}

func TestUpsertSubtaskRollup_TruncatesLongResult(t *testing.T) {
	var status tatarav1alpha1.TaskStatus
	long := strings.Repeat("x", 1000)
	upsertSubtaskRollup(&status, tatarav1alpha1.SubtaskRef{Name: "a", Order: 0, Title: "planning", Phase: "Done", Result: long})
	if len(status.Subtasks[0].Result) > 512 {
		t.Fatalf("Result not truncated: %d chars", len(status.Subtasks[0].Result))
	}
}
