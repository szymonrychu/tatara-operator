package controller

import (
	"strconv"
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func mkCronTask(repo string, number int, kind, headSHA, phase string) tatarav1alpha1.Task {
	tk := tatarav1alpha1.Task{}
	// Set the 3 legacy source dedup labels directly (Phase 1: writes stopped,
	// reads kept for backward-compat with tasks created before the ledger).
	labels := scanTaskLabels(candidate{repo: repo, number: number, headSHA: headSHA}, "mrScan", kind)
	labels[labelSourceRepo] = sanitizeRepoLabel(repo)
	labels[labelSourceNumber] = strconv.Itoa(number)
	if headSHA != "" {
		labels[labelHeadSHA] = headSHA
	}
	tk.Labels = labels
	tk.Status.Phase = phase
	return tk
}

func TestScanTaskLabels(t *testing.T) {
	got := scanTaskLabels(candidate{repo: "o/r", number: 5, headSHA: "abc"}, "mrScan", "review")
	// The three source dedup labels are no longer written (Phase 1).
	for _, key := range []string{"tatara.io/source-repo", "tatara.io/source-number", "tatara.io/head-sha"} {
		if _, ok := got[key]; ok {
			t.Fatalf("scanTaskLabels must not write %q; got %+v", key, got)
		}
	}
	// Kind and activity labels are still expected.
	if got["tatara.io/source-kind"] != "review" {
		t.Fatalf("source-kind = %q (want review); labels = %+v", got["tatara.io/source-kind"], got)
	}
	if got["tatara.io/activity"] != "mrScan" {
		t.Fatalf("activity = %q (want mrScan); labels = %+v", got["tatara.io/activity"], got)
	}
}

func TestDedupPR(t *testing.T) {
	existing := []tatarav1alpha1.Task{
		mkCronTask("o/r", 1, "review", "sha1", "Running"),   // non-terminal -> skip #1
		mkCronTask("o/r", 2, "review", "sha2", "Succeeded"), // terminal at sha2 -> skip same-sha
	}
	cases := []struct {
		name string
		cand candidate
		want bool // true = skipped (deduped)
	}{
		{"in-flight pr skipped", candidate{repo: "o/r", number: 1, headSHA: "shaX", isPR: true}, true},
		{"terminal same sha skipped", candidate{repo: "o/r", number: 2, headSHA: "sha2", isPR: true}, true},
		{"terminal new sha eligible", candidate{repo: "o/r", number: 2, headSHA: "sha9", isPR: true}, false},
		{"unseen pr eligible", candidate{repo: "o/r", number: 3, headSHA: "shaY", isPR: true}, false},
	}
	managed := managedPhaseLabels(nil)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isDeduped(tc.cand, existing, managed, nil); got != tc.want {
				t.Fatalf("isDeduped = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDedupIssue(t *testing.T) {
	created := metav1.Now()
	terminal := mkCronTask("o/r", 7, "triageIssue", "", "Succeeded")
	terminal.CreationTimestamp = created
	existing := []tatarav1alpha1.Task{
		mkCronTask("o/r", 6, "triageIssue", "", "Planning"), // non-terminal -> skip #6
		terminal, // terminal -> skip unless newer activity
	}
	managed := managedPhaseLabels(nil)
	older := candidate{repo: "o/r", number: 7, updatedAt: created.Add(-time.Hour)}
	newer := candidate{repo: "o/r", number: 7, updatedAt: created.Add(time.Hour)}
	if !isDeduped(candidate{repo: "o/r", number: 6}, existing, managed, nil) {
		t.Fatalf("in-flight issue #6 should be deduped")
	}
	if !isDeduped(older, existing, managed, nil) {
		t.Fatalf("terminal issue with no new activity should be deduped")
	}
	if isDeduped(newer, existing, managed, nil) {
		t.Fatalf("terminal issue with newer activity should be eligible")
	}
}

func TestDedupTerminalTaskWithActiveLabelIsDeduped(t *testing.T) {
	created := metav1.Now()
	terminal := mkCronTask("o/r", 5, "issueLifecycle", "", "Succeeded")
	terminal.CreationTimestamp = created
	existing := []tatarav1alpha1.Task{terminal}
	managed := managedPhaseLabels(nil)
	// issue has tatara-implementation label -> managed label present -> deduped
	c := candidate{repo: "o/r", number: 5, labels: []string{"tatara-implementation"}, updatedAt: created.Add(time.Hour)}
	if !isDeduped(c, existing, managed, nil) {
		t.Fatalf("terminal task + active managed label should be deduped (orphan; backstop handles)")
	}
}

func TestDedupTerminalTaskNoLabelNewActivityEligible(t *testing.T) {
	created := metav1.Now()
	terminal := mkCronTask("o/r", 5, "issueLifecycle", "", "Succeeded")
	terminal.CreationTimestamp = created
	existing := []tatarav1alpha1.Task{terminal}
	managed := managedPhaseLabels(nil)
	// no managed label, new activity -> eligible for fresh triage
	c := candidate{repo: "o/r", number: 5, labels: []string{}, updatedAt: created.Add(time.Hour)}
	if isDeduped(c, existing, managed, nil) {
		t.Fatalf("terminal task + no managed label + new activity should be eligible")
	}
}

func TestDedupTerminalTaskNoLabelNoNewActivityDeduped(t *testing.T) {
	created := metav1.Now()
	terminal := mkCronTask("o/r", 5, "issueLifecycle", "", "Succeeded")
	terminal.CreationTimestamp = created
	existing := []tatarav1alpha1.Task{terminal}
	managed := managedPhaseLabels(nil)
	// no managed label, updatedAt == creation -> deduped
	c := candidate{repo: "o/r", number: 5, labels: []string{}, updatedAt: created.Time}
	if !isDeduped(c, existing, managed, nil) {
		t.Fatalf("terminal task + no managed label + no new activity should be deduped")
	}
}

func TestDedupNonTerminalTaskAlwaysDeduped(t *testing.T) {
	existing := []tatarav1alpha1.Task{mkCronTask("o/r", 5, "issueLifecycle", "", "Planning")}
	managed := managedPhaseLabels(nil)
	c := candidate{repo: "o/r", number: 5, labels: []string{"tatara-implementation"}, updatedAt: time.Now()}
	if !isDeduped(c, existing, managed, nil) {
		t.Fatalf("non-terminal task should always be deduped")
	}
}

func TestDedupDeclinedLabelIsDeduped(t *testing.T) {
	created := metav1.Now()
	terminal := mkCronTask("o/r", 5, "issueLifecycle", "", "Succeeded")
	terminal.CreationTimestamp = created
	existing := []tatarav1alpha1.Task{terminal}
	managed := managedPhaseLabels(nil)
	// declined is in managedPhaseLabels -> suppressed
	c := candidate{repo: "o/r", number: 5, labels: []string{"tatara-declined"}, updatedAt: created.Add(time.Hour)}
	if !isDeduped(c, existing, managed, nil) {
		t.Fatalf("terminal task + tatara-declined label should be deduped")
	}
}

func TestDedupPRHeadShaUnchanged(t *testing.T) {
	created := metav1.Now()
	terminal := mkCronTask("o/r", 5, "review", "sha1", "Succeeded")
	terminal.CreationTimestamp = created
	existing := []tatarav1alpha1.Task{terminal}
	managed := managedPhaseLabels(nil)
	// PR same headSHA -> deduped
	c := candidate{repo: "o/r", number: 5, headSHA: "sha1", isPR: true, updatedAt: created.Add(time.Hour)}
	if !isDeduped(c, existing, managed, nil) {
		t.Fatalf("PR terminal task at same headSHA should be deduped")
	}
}

func mkLifecycleKindTask(repo string, number int, lifecycleState string) tatarav1alpha1.Task {
	tk := tatarav1alpha1.Task{}
	tk.Labels = scanTaskLabels(candidate{repo: repo, number: number}, "issueScan", "issueLifecycle")
	tk.Status.LifecycleState = lifecycleState
	// Phase 1: source set so taskMatchesItem can find this task (labels no longer written).
	tk.Spec.Source = &tatarav1alpha1.TaskSource{
		Provider: "github",
		IssueRef: repo + "#" + strconv.Itoa(number),
		Number:   number,
	}
	return tk
}

func TestHasLiveOrAdoptableTask(t *testing.T) {
	cases := []struct {
		name     string
		existing []tatarav1alpha1.Task
		wantName bool // true => a Task is returned (adoptable)
	}{
		{
			name:     "no tasks -> nil",
			existing: nil,
			wantName: false,
		},
		{
			name:     "Parked lifecycle task -> adopt",
			existing: []tatarav1alpha1.Task{mkLifecycleKindTask("o/r", 8, "Parked")},
			wantName: true,
		},
		{
			name:     "Triage (in-flight) -> adopt",
			existing: []tatarav1alpha1.Task{mkLifecycleKindTask("o/r", 8, "Triage")},
			wantName: true,
		},
		{
			name:     "Conversation -> adopt",
			existing: []tatarav1alpha1.Task{mkLifecycleKindTask("o/r", 8, "Conversation")},
			wantName: true,
		},
		{
			name:     "Done -> NOT adoptable",
			existing: []tatarav1alpha1.Task{mkLifecycleKindTask("o/r", 8, "Done")},
			wantName: false,
		},
		{
			name:     "Stopped -> NOT adoptable",
			existing: []tatarav1alpha1.Task{mkLifecycleKindTask("o/r", 8, "Stopped")},
			wantName: false,
		},
		{
			name: "wrong number -> nil",
			existing: []tatarav1alpha1.Task{
				mkLifecycleKindTask("o/r", 9, "Parked"),
			},
			wantName: false,
		},
		{
			name: "review-kind task for same number is ignored",
			existing: []tatarav1alpha1.Task{
				func() tatarav1alpha1.Task {
					tk := tatarav1alpha1.Task{}
					tk.Labels = scanTaskLabels(candidate{repo: "o/r", number: 8}, "mrScan", "review")
					tk.Status.LifecycleState = "Parked"
					return tk
				}(),
			},
			wantName: false,
		},
		{
			name: "Parked preferred over a Done sibling",
			existing: []tatarav1alpha1.Task{
				mkLifecycleKindTask("o/r", 8, "Done"),
				mkLifecycleKindTask("o/r", 8, "Parked"),
			},
			wantName: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := hasLiveOrAdoptableTask(tc.existing, "o/r", 8)
			if (got != nil) != tc.wantName {
				t.Fatalf("hasLiveOrAdoptableTask returned=%v, want adoptable=%v", got != nil, tc.wantName)
			}
		})
	}
}

// --- Phase 2: Task 6 tests - spec/ledger-only identity (no labels) ---

// mkSpecTask builds a Task with ONLY Spec.Source and optional Status.WorkItems.
// No legacy labels. Used to verify that Phase 2 dedup does NOT read labels.
func mkSpecTask(repo string, number int, isPR bool, headSHA, lifecycleState, phase string) tatarav1alpha1.Task {
	tk := tatarav1alpha1.Task{}
	tk.Spec.Source = &tatarav1alpha1.TaskSource{
		Provider: "github",
		IssueRef: repo + "#" + strconv.Itoa(number),
		Number:   number,
		IsPR:     isPR,
	}
	tk.Status.Phase = phase
	tk.Status.LifecycleState = lifecycleState
	if headSHA != "" {
		// Seed the openedPR work-item so headSHAForTask can find it.
		tk.Status.WorkItems = []tatarav1alpha1.WorkItemRef{{
			Provider: "github",
			Repo:     repo,
			Number:   number,
			Kind:     tatarav1alpha1.WorkItemPR,
			Role:     tatarav1alpha1.RoleOpenedPR,
			State:    tatarav1alpha1.WIOpen,
			HeadSHA:  headSHA,
		}}
	}
	return tk
}

// mkSpecTaskWithDedupNumber creates a bot-PR task where DedupNumber is the
// linked issue number (dedup key), not the PR number.
func mkSpecTaskWithDedupNumber(repo string, prNumber, dedupNumber int, phase string) tatarav1alpha1.Task {
	tk := tatarav1alpha1.Task{}
	tk.Spec.Source = &tatarav1alpha1.TaskSource{
		Provider:    "github",
		IssueRef:    repo + "#" + strconv.Itoa(prNumber),
		Number:      prNumber,
		IsPR:        true,
		DedupNumber: dedupNumber,
	}
	tk.Status.Phase = phase
	return tk
}

func TestIsDeduped_SpecOnly_NonTerminalSuppresses(t *testing.T) {
	// A non-terminal task matched by spec identity must dedup the candidate.
	existing := []tatarav1alpha1.Task{
		mkSpecTask("o/r", 5, false, "", "Triage", ""),
	}
	managed := managedPhaseLabels(nil)
	c := candidate{repo: "o/r", number: 5}
	if !isDeduped(c, existing, managed, nil) {
		t.Fatal("non-terminal spec-only task must dedup the candidate")
	}
}

func TestIsDeduped_SpecOnly_TerminalNewActivityEligible(t *testing.T) {
	// Terminal task with newer activity must NOT block re-triage.
	created := metav1.Now()
	tk := mkSpecTask("o/r", 5, false, "", "Done", "Succeeded")
	tk.CreationTimestamp = created
	existing := []tatarav1alpha1.Task{tk}
	managed := managedPhaseLabels(nil)
	c := candidate{repo: "o/r", number: 5, updatedAt: created.Add(time.Hour)}
	if isDeduped(c, existing, managed, nil) {
		t.Fatal("terminal spec-only task + newer activity should be eligible")
	}
}

func TestIsDeduped_SpecOnly_PR_SameHeadSHA_FromLedger(t *testing.T) {
	// Terminal PR task with matching head SHA from the ledger (openedPR entry) must dedup.
	tk := mkSpecTask("o/r", 10, true, "sha-abc", "Done", "Succeeded")
	existing := []tatarav1alpha1.Task{tk}
	managed := managedPhaseLabels(nil)
	c := candidate{repo: "o/r", number: 10, headSHA: "sha-abc", isPR: true}
	if !isDeduped(c, existing, managed, nil) {
		t.Fatal("terminal PR task at same headSHA (from ledger) must dedup")
	}
}

func TestIsDeduped_SpecOnly_PR_DifferentHeadSHA_Eligible(t *testing.T) {
	// Terminal PR task with DIFFERENT head SHA must NOT block a new commit.
	tk := mkSpecTask("o/r", 10, true, "sha-old", "Done", "Succeeded")
	existing := []tatarav1alpha1.Task{tk}
	managed := managedPhaseLabels(nil)
	c := candidate{repo: "o/r", number: 10, headSHA: "sha-new", isPR: true}
	if isDeduped(c, existing, managed, nil) {
		t.Fatal("terminal PR task at different headSHA must be eligible")
	}
}

func TestIsDeduped_SpecOnly_BotPR_DedupNumber_MatchesIssueSlot(t *testing.T) {
	// A bot-PR task with DedupNumber=7 must dedup an issue-slot candidate for issue #7.
	tk := mkSpecTaskWithDedupNumber("o/r", 42, 7, "")
	tk.Status.Phase = "Running"
	existing := []tatarav1alpha1.Task{tk}
	managed := managedPhaseLabels(nil)
	c := candidate{repo: "o/r", number: 7} // issue-slot candidate
	if !isDeduped(c, existing, managed, nil) {
		t.Fatal("bot-PR task with DedupNumber=7 must dedup issue-slot candidate #7")
	}
}

func TestIsDeduped_NoLabels_DifferentRepo_NotDeduped(t *testing.T) {
	// A task for a different repo must not block a candidate for a different repo.
	existing := []tatarav1alpha1.Task{
		mkSpecTask("other/repo", 5, false, "", "Triage", ""),
	}
	managed := managedPhaseLabels(nil)
	c := candidate{repo: "o/r", number: 5}
	if isDeduped(c, existing, managed, nil) {
		t.Fatal("task for a different repo must not dedup a different-repo candidate")
	}
}

func TestHeadSHAForTask_FromLedger(t *testing.T) {
	tk := mkSpecTask("o/r", 10, true, "sha-xyz", "", "")
	if got := headSHAForTask(&tk); got != "sha-xyz" {
		t.Fatalf("headSHAForTask = %q, want sha-xyz", got)
	}
}

func TestHeadSHAForTask_NoLedger_Empty(t *testing.T) {
	tk := tatarav1alpha1.Task{}
	if got := headSHAForTask(&tk); got != "" {
		t.Fatalf("headSHAForTask on empty task = %q, want empty", got)
	}
}

func TestHeadSHAForTask_FallbackMergedHeadSHA(t *testing.T) {
	tk := tatarav1alpha1.Task{}
	tk.Status.MergedHeadSHA = "sha-merged"
	if got := headSHAForTask(&tk); got != "sha-merged" {
		t.Fatalf("headSHAForTask fallback = %q, want sha-merged", got)
	}
}

func TestIsDeduped_BotCommentDoesNotFreeKey(t *testing.T) {
	created := metav1.Now()
	terminal := mkCronTask("o/r", 7, "issueLifecycle", "", "Succeeded")
	terminal.Status.LifecycleState = "Parked"
	terminal.CreationTimestamp = created
	existing := []tatarav1alpha1.Task{terminal}
	managed := managedPhaseLabels(nil)

	// Candidate updatedAt advanced past creation (as a bot comment would do).
	c := candidate{repo: "o/r", number: 7, updatedAt: created.Add(time.Hour)}

	// Legacy nil predicate: updatedAt advanced -> NOT deduped (eligible). Unchanged behavior.
	if isDeduped(c, existing, managed, nil) {
		t.Fatalf("nil predicate: updatedAt advanced should be eligible (legacy behavior)")
	}

	// Human-activity predicate that reports NO human comment since the Task creation
	// (i.e. only the bot commented): the key must stay HELD -> deduped.
	noHuman := func(_ candidate, _ time.Time) bool { return false }
	if !isDeduped(c, existing, managed, noHuman) {
		t.Fatalf("bot-only activity must keep the dedup key held (deduped)")
	}

	// Human comment present after creation -> key freed -> eligible.
	yesHuman := func(_ candidate, _ time.Time) bool { return true }
	if isDeduped(c, existing, managed, yesHuman) {
		t.Fatalf("human activity must free the dedup key (eligible)")
	}
}

// --- Phase 2: Task 7 tests - priorTerminalAttempts + hasLiveLifecycleTaskForIssue (no labels) ---

func TestPriorTerminalAttempts_SpecOnly(t *testing.T) {
	// Build a terminal PR task with ONLY Spec.Source (no legacy labels).
	terminal := mkSpecTask("o/r", 5, true, "", "Parked", "Succeeded")
	inFlight := mkSpecTask("o/r", 5, true, "", "Implement", "")
	wrongPR := mkSpecTask("o/r", 99, true, "", "Parked", "Succeeded")
	wrongRepo := mkSpecTask("other/r", 5, true, "", "Parked", "Succeeded")

	cases := []struct {
		name     string
		existing []tatarav1alpha1.Task
		repo     string
		prNum    int
		want     int
	}{
		{"terminal spec task counts", []tatarav1alpha1.Task{terminal}, "o/r", 5, 1},
		{"in-flight not terminal -> 0", []tatarav1alpha1.Task{inFlight}, "o/r", 5, 0},
		{"wrong PR number -> 0", []tatarav1alpha1.Task{wrongPR}, "o/r", 5, 0},
		{"wrong repo -> 0", []tatarav1alpha1.Task{wrongRepo}, "o/r", 5, 0},
		{"multiple terminal -> count all", []tatarav1alpha1.Task{terminal, terminal}, "o/r", 5, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := priorTerminalAttempts(tc.existing, tc.repo, tc.prNum)
			if got != tc.want {
				t.Fatalf("priorTerminalAttempts = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestHasLiveLifecycleTaskForIssue_SpecOnly(t *testing.T) {
	live := mkSpecTask("o/r", 7, false, "", "Implement", "")
	terminal := mkSpecTask("o/r", 7, false, "", "Done", "Succeeded")
	wrongNum := mkSpecTask("o/r", 99, false, "", "Implement", "")
	wrongRepo := mkSpecTask("other/r", 7, false, "", "Implement", "")

	cases := []struct {
		name     string
		existing []tatarav1alpha1.Task
		slug     string
		number   int
		want     bool
	}{
		{"live task -> true", []tatarav1alpha1.Task{live}, "o/r", 7, true},
		{"terminal task -> false", []tatarav1alpha1.Task{terminal}, "o/r", 7, false},
		{"wrong number -> false", []tatarav1alpha1.Task{wrongNum}, "o/r", 7, false},
		{"wrong repo -> false", []tatarav1alpha1.Task{wrongRepo}, "o/r", 7, false},
		{"no tasks -> false", nil, "o/r", 7, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := hasLiveLifecycleTaskForIssue(tc.existing, tc.slug, tc.number)
			if got != tc.want {
				t.Fatalf("hasLiveLifecycleTaskForIssue = %v, want %v", got, tc.want)
			}
		})
	}
}
