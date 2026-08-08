package v1alpha1_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// TestTaskName covers the 49-char name budget: TaskName truncates only the
// PROJECT segment, never kind/date/uid, and always returns a valid RFC-1123
// label.
func TestTaskName(t *testing.T) {
	ts := time.Date(2026, 7, 12, 0, 0, 0, 0, time.UTC)

	t.Run("long project truncated to fit 49 chars", func(t *testing.T) {
		name := v1alpha1.TaskName("a-very-long-project-name-that-keeps-going", "implement", ts, "m4z8q")
		if len(name) > v1alpha1.MaxTaskNameLength {
			t.Fatalf("len(name) = %d, want <= %d (name=%q)", len(name), v1alpha1.MaxTaskNameLength, name)
		}
		if errs := validation.IsDNS1123Label(name); len(errs) != 0 {
			t.Fatalf("TaskName(%q) is not a valid RFC-1123 label: %v", name, errs)
		}
		if !strings.HasSuffix(name, "-implement-2026-07-12-m4z8q") {
			t.Fatalf("TaskName must truncate only the project segment, never kind/date/uid: got %q", name)
		}
	})

	t.Run("short project left untouched", func(t *testing.T) {
		name := v1alpha1.TaskName("myproj", "implement", ts, "abcde")
		want := "myproj-implement-2026-07-12-abcde"
		if name != want {
			t.Fatalf("TaskName = %q, want %q", name, want)
		}
		if errs := validation.IsDNS1123Label(name); len(errs) != 0 {
			t.Fatalf("TaskName(%q) is not a valid RFC-1123 label: %v", name, errs)
		}
	})
}

// TestTaskNameTooLong covers the reconcile-guard predicate: CRDs cannot
// constrain metadata.name length, so the reconciler (wired in Task 20) calls
// this to fail a Task whose name still exceeds the budget.
func TestTaskNameTooLong(t *testing.T) {
	ok := strings.Repeat("a", v1alpha1.MaxTaskNameLength)
	if v1alpha1.TaskNameTooLong(ok) {
		t.Errorf("TaskNameTooLong(%d chars) = true, want false", len(ok))
	}
	tooLong := strings.Repeat("a", v1alpha1.MaxTaskNameLength+1)
	if !v1alpha1.TaskNameTooLong(tooLong) {
		t.Errorf("TaskNameTooLong(%d chars) = false, want true", len(tooLong))
	}
}

// TestMaxSizedTask is the A.7 mandatory golden byte-budget test, written NOW
// so it guards every later field addition to Task: a Task at every new-field
// cap must marshal to under 1 MiB.
func TestMaxSizedTask(t *testing.T) {
	now := metav1.Now()

	task := &v1alpha1.Task{
		Spec: v1alpha1.TaskSpec{
			ProjectRef: "p",
			Kind:       "implement",
			Goal:       strings.Repeat("g", 16384),
		},
	}

	for i := 0; i < 50; i++ {
		task.Status.Notes = append(task.Status.Notes, v1alpha1.Note{
			At:    now,
			Agent: "implement",
			Kind:  "note",
			Body:  strings.Repeat("n", 4096),
		})
	}
	for i := 0; i < 20; i++ {
		task.Status.PendingEvents = append(task.Status.PendingEvents, v1alpha1.TaskEvent{
			At:     now,
			Kind:   "issue_comment",
			Repo:   "repo",
			Number: i,
			Author: "someone",
			Body:   strings.Repeat("e", 4096),
		})
	}
	for i := 0; i < 50; i++ {
		task.Status.IssueRefs = append(task.Status.IssueRefs, fmt.Sprintf("iss-repo-%d", i))
		task.Status.MRRefs = append(task.Status.MRRefs, fmt.Sprintf("mr-repo-%d", i))
	}
	for i := 0; i < 20; i++ {
		task.Spec.MergeOrder = append(task.Spec.MergeOrder, fmt.Sprintf("repo-%d", i))
	}
	for i := 0; i < 100; i++ {
		task.Spec.DocumentsTasks = append(task.Spec.DocumentsTasks, fmt.Sprintf("project-implement-2026-07-12-uid%02d", i))
	}
	for i := 0; i < 50; i++ {
		task.Status.Stats.AgentsRun = append(task.Status.Stats.AgentsRun, "implement")
		task.Status.Stats.NotesSpilledRefs = append(task.Status.Stats.NotesSpilledRefs, fmt.Sprintf("track-%032d", i))
	}

	b, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	t.Logf("max-sized Task marshaled size: %d bytes", len(b))
	if len(b) >= 1<<20 {
		t.Fatalf("max-sized Task marshaled to %d bytes, want < 1 MiB (1048576)", len(b))
	}
}

// TestA4JSONTags round-trips every contract-A.4 JSON tag on TaskSpec,
// TaskStatus, Note, TaskStats, and TaskEvent through marshal/unmarshal,
// verifying the exact camelCase tag names the contract mandates.
func TestA4JSONTags(t *testing.T) {
	// metav1.Time marshals at 1-second resolution (RFC3339); truncate so the
	// round-trip comparison below is exact.
	now := metav1.NewTime(time.Now().Truncate(time.Second))

	task := &v1alpha1.Task{
		Spec: v1alpha1.TaskSpec{
			ProjectRef:      "proj",
			RepositoryRef:   "repo",
			Goal:            "do the thing",
			Kind:            "implement",
			MergeOrder:      []string{"repo-a", "repo-b"},
			AlertRules:      []string{"alert-a"},
			DedupKey:        "dedup123",
			DocumentsTasks:  []string{"proj-documentation-2026-07-12-abcde"},
			MaxTurnsPerTask: 300,
		},
		Status: v1alpha1.TaskStatus{
			State:              v1alpha1.StateUnderImplementation,
			StateEnteredAt:     &now,
			PodStartedAt:       &now,
			StateWorkStartedAt: &now,
			AgentKind:          "implement",
			PodName:            "proj-implement-2026-07-12-abcde-implement",
			Notes: []v1alpha1.Note{
				{At: now, Agent: "implement", Kind: "note", Body: "did a thing"},
			},
			PendingEvents: []v1alpha1.TaskEvent{
				{At: now, Kind: "issue_comment", Repo: "repo-a", Number: 5, Author: "human", Body: "please fix"},
			},
			Stats: v1alpha1.TaskStats{
				TokensInput:         1,
				TokensOutput:        2,
				TokensCacheRead:     3,
				TokensCacheCreation: 4,
				Turns:               5,
				PodRuns:             6,
				WallSeconds:         7,
				AgentsRun:           []string{"implement"},
				IssueCount:          8,
				MRCount:             9,
				PodRecreations:      1,
				NotesSpilled:        2,
				NotesSpilledRefs:    []string{"track-1"},
			},
			DeliveredAt:       &now,
			DocumentedBy:      "proj-documentation-2026-07-12-xyz01",
			IssueRefs:         []string{"iss-repo-a-5"},
			MRRefs:            []string{"mr-repo-a-6"},
			ParkReason:        "stage-deadline",
			ParkedAt:          &now,
			ParkedFromState:   v1alpha1.StateAwaitingReview,
			MergeCursor:       1,
			MergeReentries:    2,
			DeployReentries:   3,
			HeadMoveReentries: 4,
			HumanReviewRounds: 5,
			FoldInFlight:      []string{"proj-refine-2026-07-12-fold1"},
		},
	}

	b, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	raw := string(b)

	wantKeys := []string{
		`"repositoryRef"`, `"goal"`, `"kind"`, `"mergeOrder"`, `"alertRules"`,
		`"dedupKey"`, `"documentsTasks"`, `"maxTurnsPerTask"`,
		`"state"`, `"stateEnteredAt"`, `"podStartedAt"`, `"stateWorkStartedAt"`,
		`"agentKind"`, `"podName"`, `"notes"`, `"pendingEvents"`, `"stats"`,
		`"deliveredAt"`, `"documentedBy"`, `"issueRefs"`, `"mrRefs"`,
		`"parkReason"`, `"parkedAt"`, `"parkedFromState"`, `"mergeCursor"`, `"mergeReentries"`,
		`"deployReentries"`, `"headMoveReentries"`, `"humanReviewRounds"`,
		`"foldInFlight"`,
		// Note
		`"at"`, `"agent"`, `"body"`,
		// TaskStats
		`"tokensInput"`, `"tokensOutput"`, `"tokensCacheRead"`, `"tokensCacheCreation"`,
		`"turns"`, `"podRuns"`, `"wallSeconds"`, `"agentsRun"`, `"issueCount"`,
		`"mrCount"`, `"podRecreations"`, `"notesSpilled"`, `"notesSpilledRefs"`,
		// TaskEvent
		`"repo"`, `"number"`, `"author"`,
	}
	for _, key := range wantKeys {
		if !strings.Contains(raw, key) {
			t.Errorf("marshaled Task missing JSON key %s\nfull JSON: %s", key, raw)
		}
	}

	var round v1alpha1.Task
	if err := json.Unmarshal(b, &round); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if round.Spec.MaxTurnsPerTask != 300 {
		t.Errorf("round-trip MaxTurnsPerTask = %d, want 300", round.Spec.MaxTurnsPerTask)
	}
	if round.Status.HumanReviewRounds != 5 {
		t.Errorf("round-trip HumanReviewRounds = %d, want 5", round.Status.HumanReviewRounds)
	}
	if round.Status.HeadMoveReentries != 4 {
		t.Errorf("round-trip HeadMoveReentries = %d, want 4", round.Status.HeadMoveReentries)
	}
	if round.Status.PodStartedAt == nil || !round.Status.PodStartedAt.Time.Equal(now.Time) {
		t.Errorf("round-trip PodStartedAt mismatch")
	}
	if len(round.Status.Notes) != 1 || round.Status.Notes[0].Body != "did a thing" {
		t.Errorf("round-trip Notes mismatch: %+v", round.Status.Notes)
	}
	if len(round.Status.Stats.NotesSpilledRefs) != 1 || round.Status.Stats.NotesSpilledRefs[0] != "track-1" {
		t.Errorf("round-trip NotesSpilledRefs mismatch: %+v", round.Status.Stats.NotesSpilledRefs)
	}
}
