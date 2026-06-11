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
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isDeduped(tc.cand, existing); got != tc.want {
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
	older := candidate{repo: "o/r", number: 7, updatedAt: created.Add(-time.Hour)}
	newer := candidate{repo: "o/r", number: 7, updatedAt: created.Add(time.Hour)}
	if !isDeduped(candidate{repo: "o/r", number: 6}, existing) {
		t.Fatalf("in-flight issue #6 should be deduped")
	}
	if !isDeduped(older, existing) {
		t.Fatalf("terminal issue with no new activity should be deduped")
	}
	if isDeduped(newer, existing) {
		t.Fatalf("terminal issue with newer activity should be eligible")
	}
}
