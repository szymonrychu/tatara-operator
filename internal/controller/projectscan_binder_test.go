package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestIssueScanCreatesIssueLifecycleKind asserts that issueScan creates Tasks with
// Kind=issueLifecycle (not triageIssue) and labels source-kind=issueLifecycle.
func TestIssueScanCreatesIssueLifecycleKind(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 2}}
	proj, _ := seedScanProject(t, "binder-issue-kind", cron)
	reader := &fakeReader{issues: []scm.IssueRef{
		{Repo: "o/r", Number: 1, UpdatedAt: time.Unix(100, 0)},
		{Repo: "o/r", Number: 2, UpdatedAt: time.Unix(200, 0)},
	}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	b := 99
	r.issueScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{
		{ObjectMeta: metav1.ObjectMeta{Name: "binder-issue-kind-repo", Namespace: testNS},
			Spec: tatarav1alpha1.RepositorySpec{ProjectRef: "binder-issue-kind", URL: "https://github.com/o/r.git", DefaultBranch: "main"}},
	}, nil, cron.IssueScan, &b)

	tasks := listScanTasks(t, "binder-issue-kind")
	if len(tasks) == 0 {
		t.Fatalf("expected tasks to be created")
	}
	for _, tk := range tasks {
		if tk.Spec.Kind != "issueLifecycle" {
			t.Errorf("task %s Kind = %q, want issueLifecycle", tk.Name, tk.Spec.Kind)
		}
		if tk.Labels[labelSourceKind] != "issueLifecycle" {
			t.Errorf("task %s source-kind label = %q, want issueLifecycle", tk.Name, tk.Labels[labelSourceKind])
		}
	}
}

// TestIssueScanLaneOccupancyCountsIssueLifecycle asserts that issueScan uses
// laneOccupancy with "issueLifecycle" kind (not "triageIssue") when determining
// per-repo slot availability.
func TestIssueScanLaneOccupancyCountsIssueLifecycle(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 1}}
	proj, repoA := seedScanProject(t, "binder-lane-lifecycle", cron)

	// Pre-create a Running issueLifecycle task for o/r#1; this should hold the lane.
	pre := &tatarav1alpha1.Task{}
	pre.GenerateName = "scan-"
	pre.Namespace = testNS
	pre.Labels = scanTaskLabels(candidate{repo: "o/r", number: 1}, "issueScan", "issueLifecycle")
	pre.Spec = tatarav1alpha1.TaskSpec{
		ProjectRef: "binder-lane-lifecycle", RepositoryRef: repoA.Name,
		Goal: "g", Kind: "issueLifecycle",
	}
	if err := k8sClient.Create(context.Background(), pre); err != nil {
		t.Fatalf("pre-create: %v", err)
	}
	pre.Status.Phase = "Running"
	_ = k8sClient.Status().Update(context.Background(), pre)

	reader := &fakeReader{issues: []scm.IssueRef{
		{Repo: "o/r", Number: 1, UpdatedAt: time.Unix(100, 0)}, // deduped
		{Repo: "o/r", Number: 2, UpdatedAt: time.Unix(200, 0)}, // blocked by held lane
	}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	b2 := 99
	backlog, _ := r.issueScan(context.Background(), proj, reader,
		[]tatarav1alpha1.Repository{*repoA}, []tatarav1alpha1.Task{*pre}, cron.IssueScan, &b2)
	if !backlog {
		t.Fatalf("want backlog=true (#2 blocked by the Running issueLifecycle #1 lane)")
	}
	// Only the pre-existing task should exist (lane held, no new task for #2).
	tasks := listScanTasks(t, "binder-lane-lifecycle")
	if len(tasks) != 1 {
		t.Fatalf("want only the pre-existing task (lane held), got %d", len(tasks))
	}
}

// TestIssueScanDedupLifecycleTerminals asserts that Done/Stopped/Parked
// lifecycle states are treated as terminal for dedup (freeing the key on
// newer activity), while non-terminal lifecycle Tasks suppress re-creation.
func TestIssueScanDedupLifecycleTerminals(t *testing.T) {
	created := metav1.Now()

	mkLifecycleTask := func(number int, phase, lifecycleState string) tatarav1alpha1.Task {
		tk := tatarav1alpha1.Task{}
		tk.Labels = scanTaskLabels(candidate{repo: "o/r", number: number}, "issueScan", "issueLifecycle")
		tk.Status.Phase = phase
		tk.Status.LifecycleState = lifecycleState
		tk.CreationTimestamp = created
		return tk
	}

	cases := []struct {
		name        string
		cand        candidate
		existing    []tatarav1alpha1.Task
		wantDeduped bool
	}{
		{
			name: "non-terminal lifecycle state (Triage) suppresses",
			cand: candidate{repo: "o/r", number: 1, updatedAt: created.Add(time.Hour)},
			existing: []tatarav1alpha1.Task{
				mkLifecycleTask(1, "Running", "Triage"),
			},
			wantDeduped: true,
		},
		{
			name: "non-terminal lifecycle state (Implement) suppresses",
			cand: candidate{repo: "o/r", number: 2, updatedAt: created.Add(time.Hour)},
			existing: []tatarav1alpha1.Task{
				mkLifecycleTask(2, "Running", "Implement"),
			},
			wantDeduped: true,
		},
		{
			name: "Done lifecycle state frees key on newer activity",
			cand: candidate{repo: "o/r", number: 3, updatedAt: created.Add(time.Hour)},
			existing: []tatarav1alpha1.Task{
				mkLifecycleTask(3, "Succeeded", "Done"),
			},
			wantDeduped: false,
		},
		{
			name: "Stopped lifecycle state frees key on newer activity",
			cand: candidate{repo: "o/r", number: 4, updatedAt: created.Add(time.Hour)},
			existing: []tatarav1alpha1.Task{
				mkLifecycleTask(4, "Succeeded", "Stopped"),
			},
			wantDeduped: false,
		},
		{
			name: "Parked lifecycle state frees key on newer activity",
			cand: candidate{repo: "o/r", number: 5, updatedAt: created.Add(time.Hour)},
			existing: []tatarav1alpha1.Task{
				mkLifecycleTask(5, "Succeeded", "Parked"),
			},
			wantDeduped: false,
		},
		{
			name: "Done lifecycle state suppresses when no newer activity",
			cand: candidate{repo: "o/r", number: 6, updatedAt: created.Add(-time.Hour)},
			existing: []tatarav1alpha1.Task{
				mkLifecycleTask(6, "Succeeded", "Done"),
			},
			wantDeduped: true,
		},
	}

	managed := managedPhaseLabels(nil)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isDeduped(tc.cand, tc.existing, managed)
			if got != tc.wantDeduped {
				t.Fatalf("isDeduped = %v, want %v", got, tc.wantDeduped)
			}
		})
	}
}

// TestMRScanBotPRCreatesIssueLifecycleMRCI asserts that mrScan for a bot-authored PR
// creates Kind=issueLifecycle with LifecycleState=MRCI and prNumber/PrURL set.
func TestMRScanBotPRCreatesIssueLifecycleMRCI(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{MRScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 2}}
	proj, _ := seedScanProject(t, "binder-mr-bot", cron)
	reader := &fakeReader{prs: []scm.PRRef{
		{Repo: "o/r", Number: 9, Author: "tatara-bot", HeadSHA: "abc", UpdatedAt: time.Unix(100, 0)},
		{Repo: "o/r", Number: 10, Author: "human", HeadSHA: "def", UpdatedAt: time.Unix(200, 0)},
	}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	repos := []tatarav1alpha1.Repository{
		{ObjectMeta: metav1.ObjectMeta{Name: "binder-mr-bot-repo", Namespace: testNS},
			Spec: tatarav1alpha1.RepositorySpec{ProjectRef: "binder-mr-bot", URL: "https://github.com/o/r.git", DefaultBranch: "main"}},
	}
	b3 := 99
	r.mrScan(context.Background(), proj, reader, repos, nil, cron.MRScan, &b3)

	tasks := listScanTasks(t, "binder-mr-bot")
	if len(tasks) != 2 {
		t.Fatalf("want 2 tasks (bot+human), got %d", len(tasks))
	}

	var botTask, humanTask *tatarav1alpha1.Task
	for i := range tasks {
		if tasks[i].Spec.Source != nil && tasks[i].Spec.Source.Number == 9 {
			botTask = &tasks[i]
		}
		if tasks[i].Spec.Source != nil && tasks[i].Spec.Source.Number == 10 {
			humanTask = &tasks[i]
		}
	}
	if botTask == nil {
		t.Fatalf("no task found for bot PR #9")
	}
	if humanTask == nil {
		t.Fatalf("no task found for human PR #10")
	}

	// Bot PR -> issueLifecycle with MRCI entry annotation; Spec.Source carries PR identity.
	if botTask.Spec.Kind != "issueLifecycle" {
		t.Errorf("bot PR task Kind = %q, want issueLifecycle", botTask.Spec.Kind)
	}
	// Entry state is now carried by the create-time annotation (FIX 3+5).
	if botTask.Annotations[tatarav1alpha1.LifecycleEntryAnnotation] != "MRCI" {
		t.Errorf("bot PR task lifecycle-entry annotation = %q, want MRCI", botTask.Annotations[tatarav1alpha1.LifecycleEntryAnnotation])
	}
	// PR number is in Spec.Source (set at create time), not Status.PRNumber.
	if botTask.Spec.Source == nil || botTask.Spec.Source.Number != 9 {
		t.Errorf("bot PR task Spec.Source.Number = %d, want 9", func() int {
			if botTask.Spec.Source == nil {
				return 0
			}
			return botTask.Spec.Source.Number
		}())
	}
	if botTask.Spec.Source == nil || !botTask.Spec.Source.IsPR {
		t.Errorf("bot PR task Spec.Source.IsPR must be true")
	}

	// Human PR -> review (unchanged)
	if humanTask.Spec.Kind != "review" {
		t.Errorf("human PR task Kind = %q, want review", humanTask.Spec.Kind)
	}
}

// TestMRScanBotPRClosesIssueKeyedOnLinkedIssue asserts that a bot PR with
// "Closes #N" in its body uses issue N as the dedup key (source-number label).
func TestMRScanBotPRClosesIssueKeyedOnLinkedIssue(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{MRScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 1}}
	proj, _ := seedScanProject(t, "binder-mr-closes", cron)
	reader := &fakeReader{prs: []scm.PRRef{
		{Repo: "o/r", Number: 42, Author: "tatara-bot", HeadSHA: "xyz",
			Body: "Closes #17\n\nsome description", UpdatedAt: time.Unix(100, 0)},
	}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	repos := []tatarav1alpha1.Repository{
		{ObjectMeta: metav1.ObjectMeta{Name: "binder-mr-closes-repo", Namespace: testNS},
			Spec: tatarav1alpha1.RepositorySpec{ProjectRef: "binder-mr-closes", URL: "https://github.com/o/r.git", DefaultBranch: "main"}},
	}
	b4 := 99
	r.mrScan(context.Background(), proj, reader, repos, nil, cron.MRScan, &b4)

	tasks := listScanTasks(t, "binder-mr-closes")
	if len(tasks) != 1 {
		t.Fatalf("want 1 task, got %d", len(tasks))
	}
	tk := tasks[0]
	// source-number label should be the linked issue number (17), not the PR number (42)
	if tk.Labels[labelSourceNumber] != "17" {
		t.Errorf("source-number label = %q, want 17 (linked issue)", tk.Labels[labelSourceNumber])
	}
	// PR number is in Spec.Source.Number (set at create time, not via Status).
	if tk.Spec.Source == nil || tk.Spec.Source.Number != 42 {
		n := 0
		if tk.Spec.Source != nil {
			n = tk.Spec.Source.Number
		}
		t.Errorf("Spec.Source.Number = %d, want 42 (the actual PR)", n)
	}
}

// TestMRScanBotPRNoClosesKeyedOnPRNumber asserts that without "Closes #N",
// the dedup key falls back to the PR number.
func TestMRScanBotPRNoClosesKeyedOnPRNumber(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{MRScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 1}}
	proj, _ := seedScanProject(t, "binder-mr-noclose", cron)
	reader := &fakeReader{prs: []scm.PRRef{
		{Repo: "o/r", Number: 55, Author: "tatara-bot", HeadSHA: "aaa",
			Body: "no close reference here", UpdatedAt: time.Unix(100, 0)},
	}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	repos := []tatarav1alpha1.Repository{
		{ObjectMeta: metav1.ObjectMeta{Name: "binder-mr-noclose-repo", Namespace: testNS},
			Spec: tatarav1alpha1.RepositorySpec{ProjectRef: "binder-mr-noclose", URL: "https://github.com/o/r.git", DefaultBranch: "main"}},
	}
	b5 := 99
	r.mrScan(context.Background(), proj, reader, repos, nil, cron.MRScan, &b5)

	tasks := listScanTasks(t, "binder-mr-noclose")
	if len(tasks) != 1 {
		t.Fatalf("want 1 task, got %d", len(tasks))
	}
	// source-number label should be the PR number (55) when no Closes #N
	if tasks[0].Labels[labelSourceNumber] != "55" {
		t.Errorf("source-number label = %q, want 55 (PR number)", tasks[0].Labels[labelSourceNumber])
	}
}

// TestMRScanLaneOccupancyCountsIssueLifecycle asserts that mrScan uses
// laneOccupancy with "issueLifecycle" + "review" (not "selfImprove").
func TestMRScanLaneOccupancyCountsIssueLifecycle(t *testing.T) {
	existing := []tatarav1alpha1.Task{
		{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{labelSourceRepo: sanitizeRepoLabel("o/r")},
			},
			Spec:   tatarav1alpha1.TaskSpec{Kind: "issueLifecycle"},
			Status: tatarav1alpha1.TaskStatus{Phase: "Running"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{labelSourceRepo: sanitizeRepoLabel("o/r")},
			},
			Spec:   tatarav1alpha1.TaskSpec{Kind: "review"},
			Status: tatarav1alpha1.TaskStatus{Phase: "Planning"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{labelSourceRepo: sanitizeRepoLabel("o/r")},
			},
			Spec:   tatarav1alpha1.TaskSpec{Kind: "selfImprove"}, // legacy - should not count for new binder
			Status: tatarav1alpha1.TaskStatus{Phase: "Running"},
		},
	}
	// mrScan uses "issueLifecycle" + "review" for lane occupancy
	occ := laneOccupancy(existing, "o/r", "issueLifecycle", "review")
	if occ != 2 {
		t.Errorf("laneOccupancy(issueLifecycle+review) = %d, want 2", occ)
	}
	// selfImprove alone should not be counted
	occLegacy := laneOccupancy(existing, "o/r", "selfImprove")
	if occLegacy != 1 {
		t.Errorf("laneOccupancy(selfImprove) = %d, want 1", occLegacy)
	}
}
