package controller

import (
	"reflect"
	"strings"
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

	// The seeded systemic siblings must surface in the rendered work-item context
	// the agent receives, tying the "include the systemic siblings" criterion to a
	// concrete assertion rather than trusting the seed wiring transitively.
	wctx := tatarav1alpha1.WorkItemsContext(task)
	if !strings.Contains(wctx, "o/r1#1") {
		t.Errorf("context missing source ref o/r1#1: %q", wctx)
	}
	if !strings.Contains(wctx, "o/r1#3") || !strings.Contains(wctx, "o/r1#5") {
		t.Errorf("context missing same-repo siblings: %q", wctx)
	}
	if !strings.Contains(wctx, "o/r2#9") {
		t.Errorf("context missing cross-repo sibling: %q", wctx)
	}
}

// TestWorkItemsContext_GitHubPRUsesHashSeparator: a GitHub PR work item renders
// as repo#N (not the GitLab repo!N), while a GitLab MR renders as repo!N.
func TestWorkItemsContext_GitHubPRUsesHashSeparator(t *testing.T) {
	task := &tatarav1alpha1.Task{
		Status: tatarav1alpha1.TaskStatus{
			WorkItems: []tatarav1alpha1.WorkItemRef{
				{Provider: "github", Repo: "o/r", Number: 10, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen},
				{Provider: "gitlab", Repo: "g/p", Number: 20, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen},
			},
		},
	}
	wctx := tatarav1alpha1.WorkItemsContext(task)
	if !strings.Contains(wctx, "o/r#10") {
		t.Errorf("GitHub PR must render as o/r#10: %q", wctx)
	}
	if strings.Contains(wctx, "o/r!10") {
		t.Errorf("GitHub PR must NOT use the GitLab ! separator: %q", wctx)
	}
	if !strings.Contains(wctx, "g/p!20") {
		t.Errorf("GitLab MR must render as g/p!20: %q", wctx)
	}
}

func TestParseCrossRepoRef(t *testing.T) {
	tests := []struct {
		name     string
		ref      string
		wantRepo string
		wantNum  int
	}{
		{"happy", "o/r2#9 - B", "o/r2", 9},
		{"no title", "o/r2#9", "o/r2", 9},
		{"title contains hash", "o/r2#9 - fix #11 regression", "o/r2", 9},
		{"no separator", "o/r2 plain", "", 0},
		{"non-numeric", "o/r2#x - y", "", 0},
		{"leading hash", "#9 - y", "", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo, num := parseCrossRepoRef(tc.ref)
			if repo != tc.wantRepo || num != tc.wantNum {
				t.Errorf("parseCrossRepoRef(%q) = (%q,%d) want (%q,%d)", tc.ref, repo, num, tc.wantRepo, tc.wantNum)
			}
		})
	}
}

// TestSeedLedgerFromSpec_OpenedPREntry asserts the openedPR entry shape: Kind=pr,
// Role=openedPR, and an EMPTY HeadSHA (a branch name must not masquerade as a SHA
// at seed time; the real SHA lands via the Phase-3 cron backstop).
func TestSeedLedgerFromSpec_OpenedPREntry(t *testing.T) {
	task := &tatarav1alpha1.Task{
		Spec: tatarav1alpha1.TaskSpec{
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#5", Number: 5},
		},
		Status: tatarav1alpha1.TaskStatus{PRNumber: 42, HeadBranch: "tatara/issue-5"},
	}
	seedLedgerFromSpec(task)
	var pr *tatarav1alpha1.WorkItemRef
	for i := range task.Status.WorkItems {
		if task.Status.WorkItems[i].Kind == tatarav1alpha1.WorkItemPR {
			pr = &task.Status.WorkItems[i]
		}
	}
	if pr == nil {
		t.Fatalf("no openedPR entry seeded: %+v", task.Status.WorkItems)
	}
	if pr.Number != 42 {
		t.Errorf("want openedPR number 42, got %d", pr.Number)
	}
	if pr.Role != tatarav1alpha1.RoleOpenedPR {
		t.Errorf("want role %s, got %s", tatarav1alpha1.RoleOpenedPR, pr.Role)
	}
	if pr.HeadSHA != "" {
		t.Errorf("openedPR HeadSHA must be empty at seed time (no branch name), got %q", pr.HeadSHA)
	}
}
