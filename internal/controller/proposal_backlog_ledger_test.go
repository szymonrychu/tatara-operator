package controller

import (
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// mkProposedTask builds a non-terminal brainstorm Task with role:proposed
// ledger entries for the given titles. systemicID is stamped on each entry
// when non-empty.
func mkProposedTask(repo, systemicID string, titles ...string) tatarav1alpha1.Task {
	var t tatarav1alpha1.Task
	t.Name = "brainstorm-" + repo
	t.Spec.Kind = "brainstorm"
	for _, title := range titles {
		entry := tatarav1alpha1.WorkItemRef{
			Repo:  repo,
			Kind:  tatarav1alpha1.WorkItemIssue,
			Role:  tatarav1alpha1.RoleProposed,
			State: tatarav1alpha1.WIProposed,
			Title: title,
		}
		if systemicID != "" {
			entry.Title = systemicID + "|" + title // include id in title for uniqueness
		}
		t.Status.WorkItems = append(t.Status.WorkItems, entry)
	}
	// Grouping keys on Task.Spec.SystemicGroup.SystemicID: all Tasks sharing one
	// SystemicID count as a single proposal in the backlog.
	if systemicID != "" {
		t.Spec.SystemicGroup = &tatarav1alpha1.SystemicGroup{SystemicID: systemicID}
	}
	return t
}

// mkProposedTaskState builds a Task with a role:proposed entry in the given state.
func mkProposedTaskState(repo, state string) tatarav1alpha1.Task {
	var t tatarav1alpha1.Task
	t.Name = "brainstorm-" + repo
	t.Spec.Kind = "brainstorm"
	t.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{
			Repo:  repo,
			Kind:  tatarav1alpha1.WorkItemIssue,
			Role:  tatarav1alpha1.RoleProposed,
			State: state,
			Title: "some proposal",
		},
	}
	return t
}

func TestProposalBacklogFromTasks_StandaloneCount(t *testing.T) {
	tasks := []tatarav1alpha1.Task{
		mkProposedTask("o/a", "", "improve A"),
		mkProposedTask("o/b", "", "improve B"),
		mkProposedTask("o/c", "", "improve C"),
	}
	got := proposalBacklogFromTasks(tasks)
	if got != 3 {
		t.Fatalf("want 3 standalone proposals, got %d", got)
	}
}

func TestProposalBacklogFromTasks_SystemicGroupCountsOne(t *testing.T) {
	// Three tasks all part of systemic group "abc" -> counts as 1.
	tasks := []tatarav1alpha1.Task{
		mkProposedTask("o/a", "abc", "improve A"),
		mkProposedTask("o/b", "abc", "improve B"),
		mkProposedTask("o/c", "abc", "improve C"),
	}
	got := proposalBacklogFromTasks(tasks)
	if got != 1 {
		t.Fatalf("want 1 systemic group, got %d", got)
	}
}

func TestProposalBacklogFromTasks_MixedStandaloneAndGroup(t *testing.T) {
	tasks := []tatarav1alpha1.Task{
		mkProposedTask("o/a", "abc", "improve A"),
		mkProposedTask("o/b", "abc", "improve B"),
		mkProposedTask("o/c", "", "standalone C"),
	}
	got := proposalBacklogFromTasks(tasks)
	if got != 2 { // 1 group + 1 standalone
		t.Fatalf("want 2 (1 group + 1 standalone), got %d", got)
	}
}

func TestProposalBacklogFromTasks_TwoDistinctGroups(t *testing.T) {
	tasks := []tatarav1alpha1.Task{
		mkProposedTask("o/a", "abc", "improve A"),
		mkProposedTask("o/b", "def", "improve B"),
	}
	got := proposalBacklogFromTasks(tasks)
	if got != 2 {
		t.Fatalf("want 2 distinct groups, got %d", got)
	}
}

func TestProposalBacklogFromTasks_SkipsTerminalStates(t *testing.T) {
	tests := []struct {
		name  string
		state string
		want  int
	}{
		{"proposed state counts", tatarav1alpha1.WIProposed, 1},
		{"approved state skipped", tatarav1alpha1.WIApproved, 0},
		{"declined state skipped", tatarav1alpha1.WIDeclined, 0},
		{"implemented state skipped", tatarav1alpha1.WIImplemented, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tasks := []tatarav1alpha1.Task{mkProposedTaskState("o/r", tc.state)}
			got := proposalBacklogFromTasks(tasks)
			if got != tc.want {
				t.Fatalf("want %d for state %q, got %d", tc.want, tc.state, got)
			}
		})
	}
}

func TestProposalBacklogFromTasks_SCMFallbackWhenNoLedger(t *testing.T) {
	// A task with empty WorkItems falls back to SCM count (handled by the
	// caller, not proposalBacklogFromTasks). proposalBacklogFromTasks itself
	// should return 0 for such tasks (no ledger entries).
	var t2 tatarav1alpha1.Task
	t2.Spec.Kind = "brainstorm"
	// No WorkItems set -> proposalBacklogFromTasks returns 0.
	got := proposalBacklogFromTasks([]tatarav1alpha1.Task{t2})
	if got != 0 {
		t.Fatalf("want 0 for task with no WorkItems, got %d", got)
	}
}

func TestProposalBacklogFromTasks_EmptySlice(t *testing.T) {
	if got := proposalBacklogFromTasks(nil); got != 0 {
		t.Fatalf("want 0 for empty slice, got %d", got)
	}
}

func TestProposalBacklogFromTasks_GroupWithOneOpenSiblingCountsOnce(t *testing.T) {
	// A systemic group whose ONLY open (WIProposed) entry lives in one Task while
	// the sibling Tasks in the same group are terminal still counts exactly 1.
	openTask := mkProposedTask("o/a", "grp", "improve A")
	terminalB := mkProposedTaskState("o/b", tatarav1alpha1.WIImplemented)
	terminalB.Spec.SystemicGroup = &tatarav1alpha1.SystemicGroup{SystemicID: "grp"}
	got := proposalBacklogFromTasks([]tatarav1alpha1.Task{openTask, terminalB})
	if got != 1 {
		t.Fatalf("want 1 for a group with one open + one terminal sibling, got %d", got)
	}
}

func TestProposalBacklogFromTasks_TaskWithOpenAndTerminalEntriesCountsOnce(t *testing.T) {
	// A single standalone Task carrying both an open and a terminal role:proposed
	// entry counts once.
	var task tatarav1alpha1.Task
	task.Spec.Kind = "issueLifecycle"
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Repo: "o/r", Number: 1, Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleProposed, State: tatarav1alpha1.WIProposed},
		{Repo: "o/r", Number: 2, Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleProposed, State: tatarav1alpha1.WIDeclined},
	}
	if got := proposalBacklogFromTasks([]tatarav1alpha1.Task{task}); got != 1 {
		t.Fatalf("want 1 for a task with one open + one terminal proposed entry, got %d", got)
	}
}

func TestProposalBacklogFromTasks_SkipsNonProposedRoles(t *testing.T) {
	// source/closes/openedPR entries must not count toward proposals.
	var task tatarav1alpha1.Task
	task.Spec.Kind = "issueLifecycle"
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Repo: "o/r", Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
		{Repo: "o/r", Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleCloses, State: tatarav1alpha1.WIOpen},
		{Repo: "o/r", Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen},
	}
	got := proposalBacklogFromTasks([]tatarav1alpha1.Task{task})
	if got != 0 {
		t.Fatalf("want 0 for non-proposed roles, got %d", got)
	}
}
