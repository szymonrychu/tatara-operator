package controller

import (
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func TestUpsertWorkItem_AddAndUpdate(t *testing.T) {
	task := &tatarav1alpha1.Task{}

	// Add a new entry.
	ref1 := tatarav1alpha1.WorkItemRef{
		Provider: "github", Repo: "o/r", Number: 1,
		Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleSource,
		State: tatarav1alpha1.WIOpen,
	}
	UpsertWorkItem(task, ref1)
	if len(task.Status.WorkItems) != 1 {
		t.Fatalf("expected 1 item after add, got %d", len(task.Status.WorkItems))
	}

	// Add a second distinct item.
	ref2 := tatarav1alpha1.WorkItemRef{
		Provider: "github", Repo: "o/r", Number: 7,
		Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR,
		State: tatarav1alpha1.WIOpen,
	}
	UpsertWorkItem(task, ref2)
	if len(task.Status.WorkItems) != 2 {
		t.Fatalf("expected 2 items after second add, got %d", len(task.Status.WorkItems))
	}

	// Update existing (same repo/number/kind).
	updated := tatarav1alpha1.WorkItemRef{
		Provider: "github", Repo: "o/r", Number: 1,
		Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleSource,
		State: tatarav1alpha1.WIClosed, Title: "updated title",
	}
	UpsertWorkItem(task, updated)
	if len(task.Status.WorkItems) != 2 {
		t.Fatalf("expected still 2 items after update, got %d", len(task.Status.WorkItems))
	}
	if task.Status.WorkItems[0].State != tatarav1alpha1.WIClosed {
		t.Errorf("state not updated: %s", task.Status.WorkItems[0].State)
	}
	if task.Status.WorkItems[0].Title != "updated title" {
		t.Errorf("title not updated: %s", task.Status.WorkItems[0].Title)
	}
}

func TestUpsertWorkItem_ZeroNumberMatchByTitle(t *testing.T) {
	task := &tatarav1alpha1.Task{}
	// Unfiled proposal: Number==0, match by (Repo, Title, Role).
	ref := tatarav1alpha1.WorkItemRef{
		Provider: "github", Repo: "o/r", Number: 0,
		Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleProposed,
		State: tatarav1alpha1.WIProposed, Title: "my proposal",
	}
	UpsertWorkItem(task, ref)
	UpsertWorkItem(task, tatarav1alpha1.WorkItemRef{
		Provider: "github", Repo: "o/r", Number: 0,
		Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleProposed,
		State: tatarav1alpha1.WIApproved, Title: "my proposal",
	})
	if len(task.Status.WorkItems) != 1 {
		t.Fatalf("expected 1 item (same title/role), got %d", len(task.Status.WorkItems))
	}
	if task.Status.WorkItems[0].State != tatarav1alpha1.WIApproved {
		t.Errorf("state not updated to approved: %s", task.Status.WorkItems[0].State)
	}
}

func TestTaskMatchesItem(t *testing.T) {
	tests := []struct {
		name   string
		task   *tatarav1alpha1.Task
		repo   string
		number int
		want   bool
	}{
		{
			name: "matches via Spec.Source IssueRef+Number",
			task: &tatarav1alpha1.Task{Spec: tatarav1alpha1.TaskSpec{
				Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#5", Number: 5},
			}},
			repo: "o/r", number: 5, want: true,
		},
		{
			name: "matches via Spec.Source DedupNumber (bot-PR linked issue)",
			task: &tatarav1alpha1.Task{Spec: tatarav1alpha1.TaskSpec{
				Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#42", Number: 42, DedupNumber: 7},
			}},
			repo: "o/r", number: 7, want: true,
		},
		{
			name: "does not match wrong number",
			task: &tatarav1alpha1.Task{Spec: tatarav1alpha1.TaskSpec{
				Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#5", Number: 5},
			}},
			repo: "o/r", number: 99, want: false,
		},
		{
			name: "matches via ledger entry",
			task: &tatarav1alpha1.Task{
				Status: tatarav1alpha1.TaskStatus{
					WorkItems: []tatarav1alpha1.WorkItemRef{
						{Repo: "o/r2", Number: 10, Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleCloses},
					},
				},
			},
			repo: "o/r2", number: 10, want: true,
		},
		{
			name: "no match no source no ledger",
			task: &tatarav1alpha1.Task{},
			repo: "o/r", number: 1, want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := taskMatchesItem(tc.task, tc.repo, tc.number)
			if got != tc.want {
				t.Errorf("want %v got %v", tc.want, got)
			}
		})
	}
}

func TestReposInScope(t *testing.T) {
	task := &tatarav1alpha1.Task{
		Spec: tatarav1alpha1.TaskSpec{
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r1#1"},
		},
		Status: tatarav1alpha1.TaskStatus{
			WorkItems: []tatarav1alpha1.WorkItemRef{
				{Repo: "o/r2", Number: 2, Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleCloses},
				{Repo: "o/r1", Number: 3, Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleSource},
				{Repo: "o/r2", Number: 4, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR},
			},
		},
	}
	got := reposInScope(task)
	want := []string{"o/r1", "o/r2"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("want %v got %v", want, got)
	}
}

func TestSeedLedgerFromSpec(t *testing.T) {
	now := metav1.Now()
	tests := []struct {
		name     string
		task     *tatarav1alpha1.Task
		wantLen  int
		wantRepo string
		wantRole string
	}{
		{
			name: "seeds from Spec.Source issue",
			task: &tatarav1alpha1.Task{Spec: tatarav1alpha1.TaskSpec{
				Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#5", Number: 5},
			}},
			wantLen: 1, wantRepo: "o/r", wantRole: tatarav1alpha1.RoleSource,
		},
		{
			name: "seeds from Spec.Source PR (IsPR=true)",
			task: &tatarav1alpha1.Task{Spec: tatarav1alpha1.TaskSpec{
				Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#7", Number: 7, IsPR: true},
			}},
			wantLen: 1, wantRepo: "o/r", wantRole: tatarav1alpha1.RoleReviewed,
		},
		{
			name: "seeds with PRNumber as openedPR entry",
			task: &tatarav1alpha1.Task{
				Spec: tatarav1alpha1.TaskSpec{
					Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#5", Number: 5},
				},
				Status: tatarav1alpha1.TaskStatus{PRNumber: 42},
			},
			wantLen: 2, wantRepo: "o/r", wantRole: tatarav1alpha1.RoleSource,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_ = now
			seedLedgerFromSpec(tc.task)
			if len(tc.task.Status.WorkItems) != tc.wantLen {
				t.Errorf("want %d items got %d: %+v", tc.wantLen, len(tc.task.Status.WorkItems), tc.task.Status.WorkItems)
			}
			if tc.task.Status.WorkItems[0].Repo != tc.wantRepo {
				t.Errorf("want repo %s got %s", tc.wantRepo, tc.task.Status.WorkItems[0].Repo)
			}
			if tc.task.Status.WorkItems[0].Role != tc.wantRole {
				t.Errorf("want role %s got %s", tc.wantRole, tc.task.Status.WorkItems[0].Role)
			}

			// Idempotency: re-seeding must not change length.
			lenBefore := len(tc.task.Status.WorkItems)
			seedLedgerFromSpec(tc.task)
			if len(tc.task.Status.WorkItems) != lenBefore {
				t.Errorf("seed is not idempotent: was %d now %d", lenBefore, len(tc.task.Status.WorkItems))
			}
		})
	}
}

func TestSeedLedgerFromSpec_SystemicGroup(t *testing.T) {
	task := &tatarav1alpha1.Task{
		Spec: tatarav1alpha1.TaskSpec{
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r1#1", Number: 1},
			SystemicGroup: &tatarav1alpha1.SystemicGroup{
				SystemicID:       "abc",
				SameRepoSiblings: []int{3, 5},
				CrossRepo:        []string{"o/r2#9 - B"},
			},
		},
	}
	seedLedgerFromSpec(task)
	// Expect: 1 source + 2 closes (same repo) + 1 closes (cross-repo) = 4
	if len(task.Status.WorkItems) != 4 {
		t.Fatalf("expected 4 items, got %d: %+v", len(task.Status.WorkItems), task.Status.WorkItems)
	}
	roleCount := map[string]int{}
	for _, wi := range task.Status.WorkItems {
		roleCount[wi.Role]++
	}
	if roleCount[tatarav1alpha1.RoleSource] != 1 {
		t.Errorf("want 1 source, got %d", roleCount[tatarav1alpha1.RoleSource])
	}
	if roleCount[tatarav1alpha1.RoleCloses] != 3 {
		t.Errorf("want 3 closes, got %d", roleCount[tatarav1alpha1.RoleCloses])
	}
}
