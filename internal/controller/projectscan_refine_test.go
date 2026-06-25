package controller

// TDD tests for C2: refine cron barrier.
// Tests written before implementation; must FAIL until barrier lands.

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// seedRefineProject creates a Project with issueScan due and no LastRefine.
func seedRefineProject(t *testing.T, name string) *tatarav1alpha1.Project {
	t.Helper()
	ctx := context.Background()
	mkSecret(t, name+"-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	// Schedule every minute so it is always due. ClosedLookbackDays>0 opts into
	// the refine barrier.
	cron := &tatarav1alpha1.ScmCron{
		IssueScan: tatarav1alpha1.CronActivity{Schedule: "* * * * *"},
		Refine:    tatarav1alpha1.RefineActivity{ClosedLookbackDays: 30},
	}
	proj := &tatarav1alpha1.Project{}
	proj.Name = name
	proj.Namespace = testNS
	proj.Spec.ScmSecretRef = name + "-scm"
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{
		Provider: "github", Owner: "o", BotLogin: "bot", Cron: cron,
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project %s: %v", name, err)
	}
	// Stamp LastIssueScan 2 minutes in the past so the every-minute schedule is
	// immediately due on the first reconcile.
	past := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	proj.Status.LastIssueScan = &past
	if err := k8sClient.Status().Update(ctx, proj); err != nil {
		t.Fatalf("stamp LastIssueScan: %v", err)
	}
	mkScanRepo(t, name, name+"-repo", "https://github.com/o/r.git")
	return proj
}

// listRefineQEs returns QueuedEvents for the project whose kind == "refine".
func listRefineQEs(t *testing.T, project string) []tatarav1alpha1.QueuedEvent {
	t.Helper()
	qes := listScanQEs(t, project)
	var out []tatarav1alpha1.QueuedEvent
	for _, qe := range qes {
		if qe.Spec.Kind == "refine" || qe.Spec.Payload.Kind == "refine" {
			out = append(out, qe)
		}
	}
	return out
}

// TestRefineBarrier_DefersScansUntilRefineTerminal: first runScans creates a
// refine task and does NOT create issueScan tasks. Once refine is terminal,
// issueScan tasks are created and LastRefine is stamped.
func TestRefineBarrier_DefersScansUntilRefineTerminal(t *testing.T) {
	proj := seedRefineProject(t, "refine-barrier")
	// Return one issue so issueScan creates a QE when the barrier releases.
	reader := &fakeReader{issues: []scm.IssueRef{{Repo: "o/r", Number: 1, Title: "open issue"}}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	ctx := context.Background()

	// First reconcile: refine task should be created; no issueScan task.
	if _, err := r.runScans(ctx, proj); err != nil {
		t.Fatalf("runScans round 1: %v", err)
	}
	refineQEs := listRefineQEs(t, "refine-barrier")
	if len(refineQEs) == 0 {
		t.Fatalf("want refine QueuedEvent after round 1, got none")
	}
	issueScanQEs := listIssueQEs(t, "refine-barrier")
	if len(issueScanQEs) > 0 {
		t.Fatalf("want no issueScan QEs while refine pending, got %d", len(issueScanQEs))
	}

	// Second reconcile with refine task non-terminal: still no issueScan.
	if _, err := r.runScans(ctx, proj); err != nil {
		t.Fatalf("runScans round 2: %v", err)
	}
	issueScanQEs = listIssueQEs(t, "refine-barrier")
	if len(issueScanQEs) > 0 {
		t.Fatalf("want no issueScan QEs while refine in-flight, got %d", len(issueScanQEs))
	}

	// Mark the refine task terminal (Succeeded) by setting Phase on the Task.
	// Find the task created from the QE.
	var refineTask *tatarav1alpha1.Task
	var allTasks tatarav1alpha1.TaskList
	if err := k8sClient.List(ctx, &allTasks); err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	for i := range allTasks.Items {
		if allTasks.Items[i].Spec.ProjectRef == "refine-barrier" && allTasks.Items[i].Spec.Kind == "refine" {
			refineTask = &allTasks.Items[i]
			break
		}
	}
	if refineTask == nil {
		// The task may not be materialized from the QE in envtest; mark the QE consumed
		// and create a terminal task manually.
		refineTask = &tatarav1alpha1.Task{}
		refineTask.GenerateName = "refine-"
		refineTask.Namespace = testNS
		refineTask.Spec = tatarav1alpha1.TaskSpec{ProjectRef: "refine-barrier", Kind: "refine", Goal: "g"}
		if err := k8sClient.Create(ctx, refineTask); err != nil {
			t.Fatalf("create refine task: %v", err)
		}
	}
	refineTask.Status.Phase = "Succeeded"
	if err := k8sClient.Status().Update(ctx, refineTask); err != nil {
		t.Fatalf("mark refine Succeeded: %v", err)
	}

	// Third reconcile with terminal refine: LastRefine stamped, issueScan fires.
	if _, err := r.runScans(ctx, proj); err != nil {
		t.Fatalf("runScans round 3: %v", err)
	}

	var fresh tatarav1alpha1.Project
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "refine-barrier"}, &fresh); err != nil {
		t.Fatalf("get project: %v", err)
	}
	if fresh.Status.LastRefine == nil {
		t.Fatalf("want LastRefine stamped after terminal refine, got nil")
	}

	issueScanQEs = listIssueQEs(t, "refine-barrier")
	if len(issueScanQEs) == 0 {
		t.Fatalf("want issueScan QEs after refine terminates, got none")
	}
}

// TestRefineBarrier_FailedRefineStillReleases: a Failed refine Task stamps
// LastRefine and releases the scan gate (no wedge on failure).
func TestRefineBarrier_FailedRefineStillReleases(t *testing.T) {
	proj := seedRefineProject(t, "refine-failed")
	reader := &fakeReader{}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	ctx := context.Background()

	// First reconcile: refine created.
	if _, err := r.runScans(ctx, proj); err != nil {
		t.Fatalf("runScans: %v", err)
	}
	refineQEs := listRefineQEs(t, "refine-failed")
	if len(refineQEs) == 0 {
		t.Fatalf("want refine QE")
	}

	// Create a terminal Failed refine task.
	ft := &tatarav1alpha1.Task{}
	ft.GenerateName = "refine-"
	ft.Namespace = testNS
	ft.Spec = tatarav1alpha1.TaskSpec{ProjectRef: "refine-failed", Kind: "refine", Goal: "g"}
	if err := k8sClient.Create(ctx, ft); err != nil {
		t.Fatalf("create refine task: %v", err)
	}
	ft.Status.Phase = "Failed"
	if err := k8sClient.Status().Update(ctx, ft); err != nil {
		t.Fatalf("mark refine Failed: %v", err)
	}

	// Reconcile with Failed task: LastRefine stamped, scans released.
	if _, err := r.runScans(ctx, proj); err != nil {
		t.Fatalf("runScans after failed: %v", err)
	}

	var fresh tatarav1alpha1.Project
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "refine-failed"}, &fresh); err != nil {
		t.Fatalf("get project: %v", err)
	}
	if fresh.Status.LastRefine == nil {
		t.Fatalf("want LastRefine stamped even on failed refine")
	}
}

// TestRefine_OnePerProjectPerCycle: with a refine task already in flight, a
// second reconcile does NOT create a second refine QueuedEvent.
func TestRefine_OnePerProjectPerCycle(t *testing.T) {
	proj := seedRefineProject(t, "refine-dedup")
	reader := &fakeReader{}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	ctx := context.Background()

	// First reconcile creates one refine QE.
	if _, err := r.runScans(ctx, proj); err != nil {
		t.Fatalf("runScans 1: %v", err)
	}
	if len(listRefineQEs(t, "refine-dedup")) != 1 {
		t.Fatalf("want exactly 1 refine QE after first run")
	}

	// Second reconcile: refine still in-flight, so no new QE.
	if _, err := r.runScans(ctx, proj); err != nil {
		t.Fatalf("runScans 2: %v", err)
	}
	if len(listRefineQEs(t, "refine-dedup")) != 1 {
		t.Fatalf("want still exactly 1 refine QE (dedup), got more")
	}
}

// TestRefine_LastRefineRecentSkipsNewRefine: when LastRefine is recent (this
// cycle), runScans proceeds directly to scans without creating another refine.
func TestRefine_LastRefineRecentSkipsNewRefine(t *testing.T) {
	proj := seedRefineProject(t, "refine-recent")
	// Stamp LastRefine to now (this cycle already refined).
	now := metav1.NewTime(time.Now())
	proj.Status.LastRefine = &now
	if err := k8sClient.Status().Update(context.Background(), proj); err != nil {
		t.Fatalf("stamp LastRefine: %v", err)
	}

	reader := &fakeReader{}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	// runScans: no refine needed this cycle.
	if _, err := r.runScans(context.Background(), proj); err != nil {
		t.Fatalf("runScans: %v", err)
	}
	refineQEs := listRefineQEs(t, "refine-recent")
	if len(refineQEs) > 0 {
		t.Fatalf("want no refine QE when LastRefine is recent, got %d", len(refineQEs))
	}
}

// listIssueQEs returns QueuedEvents for the project whose activity == issueScan.
func listIssueQEs(t *testing.T, project string) []tatarav1alpha1.QueuedEvent {
	t.Helper()
	qes := listScanQEs(t, project)
	var out []tatarav1alpha1.QueuedEvent
	for _, qe := range qes {
		if qe.Spec.Payload.Labels[labelActivity] == "issueScan" {
			out = append(out, qe)
		}
	}
	return out
}
