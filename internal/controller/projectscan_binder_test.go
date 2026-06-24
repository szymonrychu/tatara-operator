package controller

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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

	r.issueScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{
		{ObjectMeta: metav1.ObjectMeta{Name: "binder-issue-kind-repo", Namespace: testNS},
			Spec: tatarav1alpha1.RepositorySpec{ProjectRef: "binder-issue-kind", URL: "https://github.com/o/r.git", DefaultBranch: "main"}},
	}, nil, cron.IssueScan)

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
		Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#1", Number: 1},
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

	_, _ = r.issueScan(context.Background(), proj, reader,
		[]tatarav1alpha1.Repository{*repoA}, []tatarav1alpha1.Task{*pre}, cron.IssueScan)

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
		// Phase 1: source set so taskMatchesItem can find this task.
		tk.Spec.Source = &tatarav1alpha1.TaskSource{
			Provider: "github",
			IssueRef: "o/r#" + strconv.Itoa(number),
			Number:   number,
		}
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
			got := isDeduped(tc.cand, tc.existing, managed, nil)
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
	r.mrScan(context.Background(), proj, reader, repos, nil, cron.MRScan)

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
	r.mrScan(context.Background(), proj, reader, repos, nil, cron.MRScan)

	qes := listScanQEs(t, "binder-mr-closes")
	if len(qes) != 1 {
		t.Fatalf("want 1 QE, got %d", len(qes))
	}
	// source-number label is no longer written (Phase 1: ledger replaces label-based dedup).
	if qes[0].Spec.Payload.Labels[labelSourceNumber] != "" {
		t.Errorf("source-number label must not be written (Phase 1), got %q", qes[0].Spec.Payload.Labels[labelSourceNumber])
	}
	// Linked issue number is recorded as Source.DedupNumber (dedup key for taskMatchesItem).
	src := qes[0].Spec.Payload.Source
	if src == nil || src.DedupNumber != 17 {
		n := 0
		if src != nil {
			n = src.DedupNumber
		}
		t.Errorf("Payload.Source.DedupNumber = %d, want 17 (linked issue)", n)
	}
	// PR number is in Payload.Source.Number (set at create time, not via Status).
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
	r.mrScan(context.Background(), proj, reader, repos, nil, cron.MRScan)

	qes := listScanQEs(t, "binder-mr-noclose")
	if len(qes) != 1 {
		t.Fatalf("want 1 QE, got %d", len(qes))
	}
	// source-number label is no longer written (Phase 1: ledger replaces label-based dedup).
	if qes[0].Spec.Payload.Labels[labelSourceNumber] != "" {
		t.Errorf("source-number label must not be written (Phase 1), got %q", qes[0].Spec.Payload.Labels[labelSourceNumber])
	}
	// DedupNumber is zero when no linked issue (dedup uses Source.Number directly).
	src := qes[0].Spec.Payload.Source
	if src == nil || src.DedupNumber != 0 {
		n := -1
		if src != nil {
			n = src.DedupNumber
		}
		t.Errorf("Payload.Source.DedupNumber = %d, want 0 (no linked issue)", n)
	}
}

// TestIssueScanAdoptsParkedTaskInsteadOfDuplicating asserts the false-refusal fix:
// a Parked issueLifecycle Task for an issue with new human activity is RE-ENTERED to
// Triage (adopted) rather than producing a second Task / QueuedEvent. One Task per issue.
func TestIssueScanAdoptsParkedTaskInsteadOfDuplicating(t *testing.T) {
	ctx := context.Background()
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 2}}
	proj, repoA := seedScanProject(t, "adopt-parked", cron)

	created := metav1.NewTime(time.Now().Add(-2 * time.Hour))

	// Pre-create a Parked issueLifecycle Task for o/r#8 (the duplicate-storm state).
	pre := &tatarav1alpha1.Task{}
	pre.GenerateName = "scan-"
	pre.Namespace = testNS
	pre.Labels = scanTaskLabels(candidate{repo: "o/r", number: 8}, "issueScan", "issueLifecycle")
	pre.Spec = tatarav1alpha1.TaskSpec{
		ProjectRef: "adopt-parked", RepositoryRef: repoA.Name,
		Goal: "g", Kind: "issueLifecycle",
		Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#8", Number: 8},
	}
	if err := k8sClient.Create(ctx, pre); err != nil {
		t.Fatalf("pre-create: %v", err)
	}
	pre.CreationTimestamp = created
	pre.Status.Phase = "Succeeded"
	pre.Status.LifecycleState = "Parked"
	pre.Status.ImplementEmptyRetries = 2
	if err := k8sClient.Status().Update(ctx, pre); err != nil {
		t.Fatalf("pre status: %v", err)
	}

	// Issue updated after the Parked task, with a NEW human comment after creation
	// (so the line-182 human-activity gate lets it through to the adoption branch).
	reader := &fakeReader{
		issues: []scm.IssueRef{
			{Repo: "o/r", Number: 8, UpdatedAt: time.Now()},
		},
		comments: []scm.IssueComment{
			{Author: "szymon", CreatedAt: time.Now()}, // human, after the Parked task creation
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	r.issueScan(ctx, proj, reader, []tatarav1alpha1.Repository{*repoA},
		[]tatarav1alpha1.Task{*pre}, cron.IssueScan)

	// No new QueuedEvent: the Parked Task was adopted, not duplicated.
	qes := listScanQEs(t, "adopt-parked")
	if len(qes) != 0 {
		t.Fatalf("want 0 QEs (adopted, not duplicated), got %d", len(qes))
	}

	// The existing Task is re-entered to Triage with a clean run state.
	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: pre.Name}, got); err != nil {
		t.Fatalf("get adopted task: %v", err)
	}
	if got.Status.LifecycleState != "Triage" {
		t.Fatalf("adopted task LifecycleState = %q, want Triage", got.Status.LifecycleState)
	}
	if got.Status.Phase != "" {
		t.Fatalf("adopted task Phase = %q, want cleared", got.Status.Phase)
	}
	if got.Status.ImplementEmptyRetries != 0 {
		t.Fatalf("adopted task ImplementEmptyRetries = %d, want 0", got.Status.ImplementEmptyRetries)
	}
	if got.Status.LastActivityAt == nil || got.Status.DeadlineAt == nil {
		t.Fatalf("adopted task must stamp LastActivityAt + DeadlineAt")
	}
}

// TestIssueScanBotCommentDoesNotRespawnTask asserts the end-to-end B3 guard: a
// terminal (Parked) issueLifecycle task whose issue updatedAt advanced ONLY because
// of a bot comment must NOT be adopted/respawned (no Triage flip, no QE). A second
// run after a HUMAN comment lands DOES re-enter it.
func TestIssueScanBotCommentDoesNotRespawnTask(t *testing.T) {
	ctx := context.Background()
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 2}}
	proj, repoA := seedScanProject(t, "b3-botcomment", cron)
	// seedScanProject sets BotLogin "tatara-bot".

	created := metav1.NewTime(time.Now().Add(-2 * time.Hour))
	pre := &tatarav1alpha1.Task{}
	pre.GenerateName = "scan-"
	pre.Namespace = testNS
	pre.Labels = scanTaskLabels(candidate{repo: "o/r", number: 8}, "issueScan", "issueLifecycle")
	pre.Spec = tatarav1alpha1.TaskSpec{
		ProjectRef: "b3-botcomment", RepositoryRef: repoA.Name, Goal: "g", Kind: "issueLifecycle",
		Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#8", Number: 8},
	}
	if err := k8sClient.Create(ctx, pre); err != nil {
		t.Fatalf("pre-create: %v", err)
	}
	pre.CreationTimestamp = created
	pre.Status.Phase = "Succeeded"
	pre.Status.LifecycleState = "Parked"
	if err := k8sClient.Status().Update(ctx, pre); err != nil {
		t.Fatalf("pre status: %v", err)
	}

	// Bot-only comment after creation; issue updatedAt advanced.
	botOnly := &fakeReader{
		issues:   []scm.IssueRef{{Repo: "o/r", Number: 8, UpdatedAt: time.Now()}},
		comments: []scm.IssueComment{{Author: "tatara-bot", CreatedAt: time.Now()}},
	}
	r := newScanReconciler(botOnly)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
	r.issueScan(ctx, proj, botOnly, []tatarav1alpha1.Repository{*repoA},
		[]tatarav1alpha1.Task{*pre}, cron.IssueScan)

	if qes := listScanQEs(t, "b3-botcomment"); len(qes) != 0 {
		t.Fatalf("bot-only comment: want 0 QEs, got %d", len(qes))
	}
	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: pre.Name}, got); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.Status.LifecycleState != "Parked" {
		t.Fatalf("bot-only comment: task must stay Parked, got %q", got.Status.LifecycleState)
	}

	// Now a human comments: the task is adopted -> Triage.
	withHuman := &fakeReader{
		issues:   []scm.IssueRef{{Repo: "o/r", Number: 8, UpdatedAt: time.Now()}},
		comments: []scm.IssueComment{{Author: "szymon", CreatedAt: time.Now()}},
	}
	r2 := newScanReconciler(withHuman)
	r2.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
	// Re-list existing (the Parked task is still Parked).
	r2.issueScan(ctx, proj, withHuman, []tatarav1alpha1.Repository{*repoA},
		[]tatarav1alpha1.Task{*got}, cron.IssueScan)

	got2 := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: pre.Name}, got2); err != nil {
		t.Fatalf("get task 2: %v", err)
	}
	if got2.Status.LifecycleState != "Triage" {
		t.Fatalf("human comment: task must be adopted to Triage, got %q", got2.Status.LifecycleState)
	}
}

// TestIssueScanAdoptionDoesNotLoopWithoutNewHumanComment asserts Defect A:
// a Parked task that was already adopted once must NOT be re-adopted on the
// NEXT issueScan cycle when no NEW human comment has arrived since the task's
// LastActivityAt (adoption timestamp). The old human comment predates
// LastActivityAt so it must not re-trigger. The task must stay Parked.
//
// Timeline:
//
//	now+0: task CreationTimestamp (API server)
//	now+1h: human comment (after creation -> isDeduped lets candidate through)
//	now+2h: LastActivityAt (previous adoption; after human comment -> new gate must block)
//
// Without the fix, adoption gates on humanCommentAfter(since=CreationTimestamp)
// which never advances: the +1h human comment is always after creation, so every
// cron cycle re-adopts the Parked task unconditionally. The fix mirrors
// findConvTaskToReactivate by gating on LastActivityAt instead.
func TestIssueScanAdoptionDoesNotLoopWithoutNewHumanComment(t *testing.T) {
	ctx := context.Background()
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 2}}
	proj, repoA := seedScanProject(t, "adopt-noloop", cron)

	// Human comment is in the future relative to wall clock so it is definitely
	// after the task's CreationTimestamp (set by API server to ~now). This ensures
	// isDeduped lets the candidate through to the adoption branch.
	humanCommentTime := time.Now().Add(1 * time.Hour)
	// LastActivityAt simulates a prior adoption: stamped AFTER the human comment.
	// The fix must block re-adoption because the human comment predates LastActivityAt.
	firstAdoptionTime := time.Now().Add(2 * time.Hour)

	// Pre-create a Parked issueLifecycle Task whose LastActivityAt is after the human comment.
	pre := &tatarav1alpha1.Task{}
	pre.GenerateName = "scan-"
	pre.Namespace = testNS
	pre.Labels = scanTaskLabels(candidate{repo: "o/r", number: 77}, "issueScan", "issueLifecycle")
	pre.Spec = tatarav1alpha1.TaskSpec{
		ProjectRef: "adopt-noloop", RepositoryRef: repoA.Name,
		Goal: "g", Kind: "issueLifecycle",
		Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#77", Number: 77},
	}
	if err := k8sClient.Create(ctx, pre); err != nil {
		t.Fatalf("pre-create: %v", err)
	}
	laa := metav1.NewTime(firstAdoptionTime)
	pre.Status.Phase = "Succeeded"
	pre.Status.LifecycleState = "Parked"
	pre.Status.LastActivityAt = &laa
	if err := k8sClient.Status().Update(ctx, pre); err != nil {
		t.Fatalf("pre status: %v", err)
	}

	// The only human comment (at +1h) predates LastActivityAt (+2h): no new activity.
	reader := &fakeReader{
		issues:   []scm.IssueRef{{Repo: "o/r", Number: 77, UpdatedAt: time.Now()}},
		comments: []scm.IssueComment{{Author: "szymon", CreatedAt: humanCommentTime}},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	r.issueScan(ctx, proj, reader, []tatarav1alpha1.Repository{*repoA},
		[]tatarav1alpha1.Task{*pre}, cron.IssueScan)

	// Must produce zero QEs (task not duplicated).
	if qes := listScanQEs(t, "adopt-noloop"); len(qes) != 0 {
		t.Fatalf("no-new-human-comment: want 0 QEs, got %d", len(qes))
	}

	// Task must stay Parked - NOT re-adopted to Triage without new human activity.
	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: pre.Name}, got); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.Status.LifecycleState != "Parked" {
		t.Fatalf("no-new-human-comment: task must stay Parked, got %q (Defect A: adoption re-looped without new human input)", got.Status.LifecycleState)
	}
}
