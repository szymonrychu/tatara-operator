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

// seedBackstopProject creates a Project + Repository for backstop tests.
// Uses issueScan cron so runScans can fire it; project name must be unique per test.
func seedBackstopProject(t *testing.T, name string) (*tatarav1alpha1.Project, tatarav1alpha1.Repository) {
	t.Helper()
	mkSecret(t, name+"-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	proj := &tatarav1alpha1.Project{}
	proj.Name = name
	proj.Namespace = testNS
	proj.Spec.ScmSecretRef = name + "-scm"
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{
		Provider: "github", Owner: "o", BotLogin: "tatara-bot",
		Cron: &tatarav1alpha1.ScmCron{
			IssueScan: tatarav1alpha1.CronActivity{Schedule: "* * * * *", MaxPerRepo: 5},
		},
	}
	if err := k8sClient.Create(context.Background(), proj); err != nil {
		t.Fatalf("create project: %v", err)
	}
	repo := tatarav1alpha1.Repository{}
	repo.Name = name + "-repo"
	repo.Namespace = testNS
	repo.Spec = tatarav1alpha1.RepositorySpec{
		ProjectRef: name, URL: "https://github.com/o/r.git",
		DefaultBranch: "main", ReingestSchedule: "0 6 * * *",
	}
	if err := k8sClient.Create(context.Background(), &repo); err != nil {
		t.Fatalf("create repo: %v", err)
	}
	return proj, repo
}

func TestBackstopRecoversImplementationOrphanToImplement(t *testing.T) {
	proj, repo := seedBackstopProject(t, "backstop-impl")
	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/r": {{Repo: "o/r", Number: 7, Labels: []string{"tatara-implementation"}, UpdatedAt: time.Unix(100, 0)}},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	repos := []tatarav1alpha1.Repository{repo}
	r.recoverOrphans(context.Background(), proj, reader, repos, nil)

	qes := listScanQEs(t, "backstop-impl")
	if len(qes) != 1 {
		t.Fatalf("want 1 recovery QE, got %d", len(qes))
	}
	entry := qes[0].Spec.Payload.Annotations[tatarav1alpha1.LifecycleEntryAnnotation]
	if entry != "Implement" {
		t.Fatalf("entry = %q, want Implement", entry)
	}
}

func TestBackstopRecoversBrainstormingOrphanToTriage(t *testing.T) {
	proj, repo := seedBackstopProject(t, "backstop-bs")
	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/r": {{Repo: "o/r", Number: 8, Labels: []string{"tatara-brainstorming"}, UpdatedAt: time.Unix(100, 0)}},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	repos := []tatarav1alpha1.Repository{repo}
	r.recoverOrphans(context.Background(), proj, reader, repos, nil)

	qes := listScanQEs(t, "backstop-bs")
	if len(qes) != 1 {
		t.Fatalf("want 1 recovery QE, got %d", len(qes))
	}
	entry := qes[0].Spec.Payload.Annotations[tatarav1alpha1.LifecycleEntryAnnotation]
	if entry != "Triage" {
		t.Fatalf("entry = %q, want Triage", entry)
	}
}

func TestBackstopRecoversLegacyIdeaOrphanToTriage(t *testing.T) {
	proj, repo := seedBackstopProject(t, "backstop-legacy")
	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/r": {{Repo: "o/r", Number: 9, Labels: []string{"tatara-idea"}, UpdatedAt: time.Unix(100, 0)}},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	repos := []tatarav1alpha1.Repository{repo}
	r.recoverOrphans(context.Background(), proj, reader, repos, nil)

	qes := listScanQEs(t, "backstop-legacy")
	if len(qes) != 1 {
		t.Fatalf("want 1 recovery QE, got %d", len(qes))
	}
	entry := qes[0].Spec.Payload.Annotations[tatarav1alpha1.LifecycleEntryAnnotation]
	if entry != "Triage" {
		t.Fatalf("entry = %q, want Triage", entry)
	}
}

func TestBackstopSkipsIssueWithLiveTask(t *testing.T) {
	proj, repo := seedBackstopProject(t, "backstop-live")
	// Pre-create a non-terminal task for o/r#7 (simulating mrScan's MRCI task).
	pre := &tatarav1alpha1.Task{}
	pre.GenerateName = "scan-"
	pre.Namespace = testNS
	pre.Labels = scanTaskLabels(candidate{repo: "o/r", number: 7}, "mrScan", "issueLifecycle")
	pre.Spec = tatarav1alpha1.TaskSpec{
		ProjectRef:    "backstop-live",
		RepositoryRef: repo.Name,
		Goal:          "g",
		Kind:          "issueLifecycle",
		Source:        &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#7", Number: 7},
	}
	if err := k8sClient.Create(context.Background(), pre); err != nil {
		t.Fatalf("pre-create: %v", err)
	}
	pre.Status.Phase = "Running"
	_ = k8sClient.Status().Update(context.Background(), pre)

	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/r": {{Repo: "o/r", Number: 7, Labels: []string{"tatara-implementation"}, UpdatedAt: time.Unix(100, 0)}},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	repos := []tatarav1alpha1.Repository{repo}
	r.recoverOrphans(context.Background(), proj, reader, repos, nil)

	// Pre-existing live Task blocks; no new QE created.
	qes := listScanQEs(t, "backstop-live")
	if len(qes) != 0 {
		t.Fatalf("want 0 QEs (live task present), got %d", len(qes))
	}
	tasks := listScanTasks(t, "backstop-live")
	if len(tasks) != 1 || tasks[0].Name != pre.Name {
		t.Fatalf("want only pre-existing task, got %d tasks", len(tasks))
	}
}

// TestBackstopSkipsIssueWithConversationTask is the regression for the #44 wedge:
// a Conversation (human-blocked) lifecycle Task already owns the issue's pod name,
// so the backstop must NOT spawn a second lifecycle Task for the same issue when
// the issue later gains an implementation phase label. Counting Conversation as a
// live lifecycle Task closes the dedup gap that let a duplicate through.
func TestBackstopSkipsIssueWithConversationTask(t *testing.T) {
	proj, repo := seedBackstopProject(t, "backstop-conv")
	pre := &tatarav1alpha1.Task{}
	pre.GenerateName = "scan-"
	pre.Namespace = testNS
	pre.Labels = scanTaskLabels(candidate{repo: "o/r", number: 7}, "issueScan", "issueLifecycle")
	pre.Spec = tatarav1alpha1.TaskSpec{
		ProjectRef:    "backstop-conv",
		RepositoryRef: repo.Name,
		Goal:          "g",
		Kind:          "issueLifecycle",
		Source:        &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#7", Number: 7},
	}
	if err := k8sClient.Create(context.Background(), pre); err != nil {
		t.Fatalf("pre-create: %v", err)
	}
	// Conversation lifecycle state, empty phase: human-blocked, no running pod,
	// but still owns the issue's pod name.
	pre.Status.DeployState = "Conversation"
	_ = k8sClient.Status().Update(context.Background(), pre)

	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/r": {{Repo: "o/r", Number: 7, Labels: []string{"tatara-implementation"}, UpdatedAt: time.Unix(100, 0)}},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	repos := []tatarav1alpha1.Repository{repo}
	r.recoverOrphans(context.Background(), proj, reader, repos, nil)

	qes := listScanQEs(t, "backstop-conv")
	if len(qes) != 0 {
		t.Fatalf("want 0 QEs (Conversation task present, no duplicate), got %d", len(qes))
	}
	tasks := listScanTasks(t, "backstop-conv")
	if len(tasks) != 1 || tasks[0].Name != pre.Name {
		t.Fatalf("want only the pre-existing Conversation task, got %d tasks", len(tasks))
	}
}

func TestBackstopSkipsDeclined(t *testing.T) {
	proj, repo := seedBackstopProject(t, "backstop-declined")
	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/r": {{Repo: "o/r", Number: 11, Labels: []string{"tatara-declined"}, UpdatedAt: time.Unix(100, 0)}},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	repos := []tatarav1alpha1.Repository{repo}
	r.recoverOrphans(context.Background(), proj, reader, repos, nil)

	qes := listScanQEs(t, "backstop-declined")
	if len(qes) != 0 {
		t.Fatalf("declined issue should create no QEs, got %d", len(qes))
	}
}

// TestBackstopAlwaysRuns verifies that recoverOrphans always runs (budget gate
// removed) and enqueues eligible orphaned issues.
func TestBackstopAlwaysRuns(t *testing.T) {
	proj, repo := seedBackstopProject(t, "backstop-budget")
	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/r": {{Repo: "o/r", Number: 12, Labels: []string{"tatara-implementation"}, UpdatedAt: time.Unix(100, 0)}},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	repos := []tatarav1alpha1.Repository{repo}
	r.recoverOrphans(context.Background(), proj, reader, repos, nil)

	// Budget gate removed: the orphaned implementation issue is now enqueued.
	qes := listScanQEs(t, "backstop-budget")
	if len(qes) != 1 {
		t.Fatalf("budget gate removed: want 1 QE for orphaned implementation issue, got %d", len(qes))
	}
}

func TestBackstopApprovedOrphanToImplement(t *testing.T) {
	proj, repo := seedBackstopProject(t, "backstop-approved")
	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/r": {{Repo: "o/r", Number: 13, Labels: []string{"tatara-approved"}, UpdatedAt: time.Unix(100, 0)}},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	repos := []tatarav1alpha1.Repository{repo}
	r.recoverOrphans(context.Background(), proj, reader, repos, nil)

	qes := listScanQEs(t, "backstop-approved")
	if len(qes) != 1 {
		t.Fatalf("want 1 recovery QE for approved orphan, got %d", len(qes))
	}
	entry := qes[0].Spec.Payload.Annotations[tatarav1alpha1.LifecycleEntryAnnotation]
	if entry != "Implement" {
		t.Fatalf("entry = %q, want Implement", entry)
	}
}

// TestRunScans_BackstopFiredAfterIssueScan verifies recoverOrphans is wired into
// runScans and fires after the issueScan pass when issueScan is due.
// Scenario: issue has tatara-implementation label and a terminal task (orphan).
// issueScan dedupes it (managed label present). backstop creates the Implement recovery QE.
func TestRunScans_BackstopFiredAfterIssueScan(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "* * * * *", MaxPerRepo: 5}}
	proj, repo := seedScanProject(t, "backstop-wired", cron)
	past := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	proj.Status.LastIssueScan = &past
	_ = k8sClient.Status().Update(context.Background(), proj)

	// Pre-create a terminal issueLifecycle task for issue #20.
	pre := &tatarav1alpha1.Task{}
	pre.GenerateName = "scan-"
	pre.Namespace = testNS
	pre.Labels = scanTaskLabels(candidate{repo: "o/r", number: 20}, "issueScan", "issueLifecycle")
	pre.Spec = tatarav1alpha1.TaskSpec{
		ProjectRef: "backstop-wired", RepositoryRef: repo.Name,
		Goal: "old triage", Kind: "issueLifecycle",
		Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#20", Number: 20},
	}
	if err := k8sClient.Create(context.Background(), pre); err != nil {
		t.Fatalf("pre-create: %v", err)
	}
	pre.Status.Phase = "Succeeded"
	_ = k8sClient.Status().Update(context.Background(), pre)

	// Issue has tatara-implementation label -> isDeduped (managed label present) -> issueScan skips.
	// backstop should create an Implement recovery QE.
	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/r": {{Repo: "o/r", Number: 20, Labels: []string{"tatara-implementation"}, UpdatedAt: time.Now()}},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	if _, err := r.runScans(context.Background(), proj); err != nil {
		t.Fatalf("runScans: %v", err)
	}

	qes := listScanQEs(t, "backstop-wired")
	if len(qes) != 1 {
		t.Fatalf("want 1 backstop recovery QE (Implement), got %d", len(qes))
	}
	entry := qes[0].Spec.Payload.Annotations[tatarav1alpha1.LifecycleEntryAnnotation]
	if entry != "Implement" {
		t.Fatalf("backstop QE entry = %q, want Implement", entry)
	}
}
