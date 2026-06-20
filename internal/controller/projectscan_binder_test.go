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

// TestIssueScanCreatesIssueLifecycleKind asserts that issueScan creates QEs with
// Kind=issueLifecycle and labels source-kind=issueLifecycle.
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

	qes := listScanQEs(t, "binder-issue-kind")
	if len(qes) == 0 {
		t.Fatalf("expected QEs to be created")
	}
	for _, qe := range qes {
		if qe.Spec.Kind != "issueLifecycle" {
			t.Errorf("QE %s Kind = %q, want issueLifecycle", qe.Name, qe.Spec.Kind)
		}
		if qe.Spec.Payload.Labels[labelSourceKind] != "issueLifecycle" {
			t.Errorf("QE %s source-kind label = %q, want issueLifecycle", qe.Name, qe.Spec.Payload.Labels[labelSourceKind])
		}
	}
}

// TestIssueScanDedupBlocksRunningTask asserts that a Running issueLifecycle task for
// issue #1 dedupes that issue; issue #2 (no pre-existing task) gets a QE.
// (Per-repo lane occupancy is removed; dedup still blocks re-creation for the same issue.)
func TestIssueScanDedupBlocksRunningTask(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 1}}
	proj, repoA := seedScanProject(t, "binder-lane-lifecycle", cron)

	// Pre-create a Running issueLifecycle task for o/r#1; this dedupes #1.
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
		{Repo: "o/r", Number: 1, UpdatedAt: time.Unix(100, 0)}, // deduped by running Task
		{Repo: "o/r", Number: 2, UpdatedAt: time.Unix(200, 0)}, // no pre-existing -> QE created
	}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	b2 := 99
	_, _ = r.issueScan(context.Background(), proj, reader,
		[]tatarav1alpha1.Repository{*repoA}, []tatarav1alpha1.Task{*pre}, cron.IssueScan, &b2)

	// #1 is deduped; #2 should get a QE.
	qes := listScanQEs(t, "binder-lane-lifecycle")
	if len(qes) != 1 {
		t.Fatalf("want 1 QE for #2 (no dedup), got %d", len(qes))
	}
	if qes[0].Spec.Payload.Source == nil || qes[0].Spec.Payload.Source.Number != 2 {
		t.Fatalf("want QE for #2, got source=%+v", qes[0].Spec.Payload.Source)
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
// creates a QE with Kind=issueLifecycle and LifecycleEntryAnnotation=MRCI.
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

	qes := listScanQEs(t, "binder-mr-bot")
	if len(qes) != 2 {
		t.Fatalf("want 2 QEs (bot+human), got %d", len(qes))
	}

	var botQE, humanQE *tatarav1alpha1.QueuedEvent
	for i := range qes {
		src := qes[i].Spec.Payload.Source
		if src != nil && src.Number == 9 {
			botQE = &qes[i]
		}
		if src != nil && src.Number == 10 {
			humanQE = &qes[i]
		}
	}
	if botQE == nil {
		t.Fatalf("no QE found for bot PR #9")
	}
	if humanQE == nil {
		t.Fatalf("no QE found for human PR #10")
	}

	// Bot PR -> issueLifecycle with MRCI entry annotation.
	if botQE.Spec.Kind != "issueLifecycle" {
		t.Errorf("bot PR QE Kind = %q, want issueLifecycle", botQE.Spec.Kind)
	}
	if botQE.Spec.Payload.Annotations[tatarav1alpha1.LifecycleEntryAnnotation] != "MRCI" {
		t.Errorf("bot PR QE lifecycle-entry annotation = %q, want MRCI", botQE.Spec.Payload.Annotations[tatarav1alpha1.LifecycleEntryAnnotation])
	}
	src := botQE.Spec.Payload.Source
	if src == nil || src.Number != 9 {
		t.Errorf("bot PR QE Spec.Payload.Source.Number = %d, want 9", func() int {
			if src == nil {
				return 0
			}
			return src.Number
		}())
	}
	if src == nil || !src.IsPR {
		t.Errorf("bot PR QE Spec.Payload.Source.IsPR must be true")
	}

	// Human PR -> review (unchanged)
	if humanQE.Spec.Kind != "review" {
		t.Errorf("human PR QE Kind = %q, want review", humanQE.Spec.Kind)
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

	qes := listScanQEs(t, "binder-mr-closes")
	if len(qes) != 1 {
		t.Fatalf("want 1 QE, got %d", len(qes))
	}
	// source-number label should be the linked issue number (17), not the PR number (42)
	if qes[0].Spec.Payload.Labels[labelSourceNumber] != "17" {
		t.Errorf("source-number label = %q, want 17 (linked issue)", qes[0].Spec.Payload.Labels[labelSourceNumber])
	}
	// PR number is in Payload.Source.Number (set at create time, not via Status).
	src := qes[0].Spec.Payload.Source
	if src == nil || src.Number != 42 {
		n := 0
		if src != nil {
			n = src.Number
		}
		t.Errorf("Payload.Source.Number = %d, want 42 (the actual PR)", n)
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

	qes := listScanQEs(t, "binder-mr-noclose")
	if len(qes) != 1 {
		t.Fatalf("want 1 QE, got %d", len(qes))
	}
	// source-number label should be the PR number (55) when no Closes #N
	if qes[0].Spec.Payload.Labels[labelSourceNumber] != "55" {
		t.Errorf("source-number label = %q, want 55 (PR number)", qes[0].Spec.Payload.Labels[labelSourceNumber])
	}
}
