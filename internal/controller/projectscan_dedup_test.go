package controller

import (
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func mkCronTask(repo string, number int, kind, headSHA, phase string) tatarav1alpha1.Task {
	tk := tatarav1alpha1.Task{}
	tk.Labels = scanTaskLabels(candidate{repo: repo, number: number, headSHA: headSHA}, "mrScan", kind)
	tk.Status.Phase = phase
	return tk
}

func TestScanTaskLabels(t *testing.T) {
	got := scanTaskLabels(candidate{repo: "o/r", number: 5, headSHA: "abc"}, "mrScan", "review")
	if got["tatara.io/source-repo"] != "o.r" {
		t.Fatalf("source-repo = %q (want sanitized o.r)", got["tatara.io/source-repo"])
	}
	if got["tatara.io/source-number"] != "5" || got["tatara.io/source-kind"] != "review" {
		t.Fatalf("labels = %+v", got)
	}
	if got["tatara.io/head-sha"] != "abc" || got["tatara.io/activity"] != "mrScan" {
		t.Fatalf("labels = %+v", got)
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
			if got := isDeduped(tc.cand, existing, managed); got != tc.want {
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
	if !isDeduped(candidate{repo: "o/r", number: 6}, existing, managed) {
		t.Fatalf("in-flight issue #6 should be deduped")
	}
	if !isDeduped(older, existing, managed) {
		t.Fatalf("terminal issue with no new activity should be deduped")
	}
	if isDeduped(newer, existing, managed) {
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
	if !isDeduped(c, existing, managed) {
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
	if isDeduped(c, existing, managed) {
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
	if !isDeduped(c, existing, managed) {
		t.Fatalf("terminal task + no managed label + no new activity should be deduped")
	}
}

func TestDedupNonTerminalTaskAlwaysDeduped(t *testing.T) {
	existing := []tatarav1alpha1.Task{mkCronTask("o/r", 5, "issueLifecycle", "", "Planning")}
	managed := managedPhaseLabels(nil)
	c := candidate{repo: "o/r", number: 5, labels: []string{"tatara-implementation"}, updatedAt: time.Now()}
	if !isDeduped(c, existing, managed) {
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
	if !isDeduped(c, existing, managed) {
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
	if !isDeduped(c, existing, managed) {
		t.Fatalf("PR terminal task at same headSHA should be deduped")
	}
}

func mkLifecycleKindTask(repo string, number int, lifecycleState string) tatarav1alpha1.Task {
	tk := tatarav1alpha1.Task{}
	tk.Labels = scanTaskLabels(candidate{repo: repo, number: number}, "issueScan", "issueLifecycle")
	tk.Status.LifecycleState = lifecycleState
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
