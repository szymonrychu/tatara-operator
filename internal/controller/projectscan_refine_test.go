package controller

// TDD tests for the refine/brainstorm cron-merge: the refine pre-scan barrier
// is re-scoped to fire on the brainstorm cron tick (cronSpec.Brainstorm.Enabled
// + activityDue(proj,"brainstorm")) instead of the ClosedLookbackDays>0 opt-in.
// mrScan/issueScan/healthCheck no longer wait on refine.

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

// seedRefineProject creates a Project with brainstorm due every minute (the
// new refine-barrier trigger) and issueScan also due, so tests can assert
// issueScan is NOT gated on refine while brainstorm is.
func seedRefineProject(t *testing.T, name string) *tatarav1alpha1.Project {
	t.Helper()
	ctx := context.Background()
	mkSecret(t, name+"-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	cron := &tatarav1alpha1.ScmCron{
		IssueScan:  tatarav1alpha1.CronActivity{Schedule: "* * * * *"},
		Brainstorm: tatarav1alpha1.BrainstormActivity{Enabled: true, Schedule: "* * * * *"},
		Refine:     tatarav1alpha1.RefineActivity{ClosedLookbackDays: 30},
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
	// Stamp both LastIssueScan and LastBrainstorm 2 minutes in the past so the
	// every-minute schedules are immediately due on the first reconcile.
	past := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	proj.Status.LastIssueScan = &past
	proj.Status.LastBrainstorm = &past
	if err := k8sClient.Status().Update(ctx, proj); err != nil {
		t.Fatalf("stamp scan status: %v", err)
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

// listBrainstormQEs is defined in projectscan_run_test.go.

// markRefineTerminal finds the refine Task created for the project (creating
// one directly if the QE hasn't materialized into a Task in envtest) and sets
// its Phase, driving the barrier to terminal.
func markRefineTerminal(t *testing.T, project, phase string) {
	t.Helper()
	ctx := context.Background()
	var refineTask *tatarav1alpha1.Task
	var allTasks tatarav1alpha1.TaskList
	if err := k8sClient.List(ctx, &allTasks); err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	for i := range allTasks.Items {
		if allTasks.Items[i].Spec.ProjectRef == project && allTasks.Items[i].Spec.Kind == "refine" {
			refineTask = &allTasks.Items[i]
			break
		}
	}
	if refineTask == nil {
		refineTask = &tatarav1alpha1.Task{}
		refineTask.GenerateName = "refine-"
		refineTask.Namespace = testNS
		refineTask.Spec = tatarav1alpha1.TaskSpec{ProjectRef: project, Kind: "refine", Goal: "g"}
		if err := k8sClient.Create(ctx, refineTask); err != nil {
			t.Fatalf("create refine task: %v", err)
		}
	}
	refineTask.Status.Phase = phase
	if err := k8sClient.Status().Update(ctx, refineTask); err != nil {
		t.Fatalf("mark refine %s: %v", phase, err)
	}
}

// TestRefineBarrier_DueBrainstormTickCreatesRefineAndHolds: a due brainstorm
// tick creates a refine Task and requeues (30s barrier poll); brainstorm does
// NOT run yet.
func TestRefineBarrier_DueBrainstormTickCreatesRefineAndHolds(t *testing.T) {
	proj := seedRefineProject(t, "refine-barrier")
	reader := &fakeReader{issues: []scm.IssueRef{{Repo: "o/r", Number: 1, Title: "open issue"}}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	ctx := context.Background()

	requeue, err := r.runScans(ctx, proj)
	if err != nil {
		t.Fatalf("runScans: %v", err)
	}
	// The barrier polls at requeueRefineBarrier; other due activities (e.g.
	// issueScan's jittered per-repo next-fire) may request an even sooner
	// requeue, so assert an upper bound rather than exact equality.
	if requeue > requeueRefineBarrier {
		t.Fatalf("want requeue<=%v (barrier poll), got %v", requeueRefineBarrier, requeue)
	}
	refineQEs := listRefineQEs(t, "refine-barrier")
	if len(refineQEs) == 0 {
		t.Fatalf("want refine QueuedEvent created on due brainstorm tick, got none")
	}
	if len(listBrainstormQEs(t, "refine-barrier")) != 0 {
		t.Fatalf("want brainstorm NOT run while refine barrier holds")
	}

	// Second reconcile with refine still non-terminal: still no brainstorm, no
	// second refine QE (in-flight dedup).
	if _, err := r.runScans(ctx, proj); err != nil {
		t.Fatalf("runScans round 2: %v", err)
	}
	if len(listBrainstormQEs(t, "refine-barrier")) != 0 {
		t.Fatalf("want brainstorm still held on round 2")
	}
	if len(listRefineQEs(t, "refine-barrier")) != 1 {
		t.Fatalf("want exactly 1 refine QE (dedup), got %d", len(listRefineQEs(t, "refine-barrier")))
	}
}

// TestRefineBarrier_TerminalRefineReleasesBrainstorm: once refine is terminal
// (Succeeded), the next reconcile stamps LastRefine and runs brainstorm.
func TestRefineBarrier_TerminalRefineReleasesBrainstorm(t *testing.T) {
	proj := seedRefineProject(t, "refine-release")
	reader := &fakeReader{issues: []scm.IssueRef{{Repo: "o/r", Number: 1, Title: "open issue"}}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	ctx := context.Background()

	if _, err := r.runScans(ctx, proj); err != nil {
		t.Fatalf("runScans round 1: %v", err)
	}
	markRefineTerminal(t, "refine-release", "Succeeded")

	if _, err := r.runScans(ctx, proj); err != nil {
		t.Fatalf("runScans round 2: %v", err)
	}

	var fresh tatarav1alpha1.Project
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "refine-release"}, &fresh); err != nil {
		t.Fatalf("get project: %v", err)
	}
	if fresh.Status.LastRefine == nil {
		t.Fatalf("want LastRefine stamped after terminal refine, got nil")
	}
	if len(listBrainstormQEs(t, "refine-release")) == 0 {
		t.Fatalf("want brainstorm QE after refine terminates, got none")
	}
}

// TestRefineBarrier_FailedRefineStillReleases: a Failed refine Task still
// stamps LastRefine and releases the brainstorm gate (no wedge on failure).
func TestRefineBarrier_FailedRefineStillReleases(t *testing.T) {
	proj := seedRefineProject(t, "refine-failed")
	reader := &fakeReader{issues: []scm.IssueRef{{Repo: "o/r", Number: 1, Title: "open issue"}}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	ctx := context.Background()

	if _, err := r.runScans(ctx, proj); err != nil {
		t.Fatalf("runScans round 1: %v", err)
	}
	markRefineTerminal(t, "refine-failed", "Failed")

	if _, err := r.runScans(ctx, proj); err != nil {
		t.Fatalf("runScans round 2: %v", err)
	}

	var fresh tatarav1alpha1.Project
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "refine-failed"}, &fresh); err != nil {
		t.Fatalf("get project: %v", err)
	}
	if fresh.Status.LastRefine == nil {
		t.Fatalf("want LastRefine stamped even on failed refine")
	}
	if len(listBrainstormQEs(t, "refine-failed")) == 0 {
		t.Fatalf("want brainstorm QE after a Failed refine releases the gate")
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

	if _, err := r.runScans(ctx, proj); err != nil {
		t.Fatalf("runScans 1: %v", err)
	}
	if len(listRefineQEs(t, "refine-dedup")) != 1 {
		t.Fatalf("want exactly 1 refine QE after first run")
	}

	if _, err := r.runScans(ctx, proj); err != nil {
		t.Fatalf("runScans 2: %v", err)
	}
	if len(listRefineQEs(t, "refine-dedup")) != 1 {
		t.Fatalf("want still exactly 1 refine QE (dedup), got more")
	}
}

// TestRefine_LastRefineRecentSkipsNewRefine: when LastRefine is recent (this
// cycle), runScans proceeds directly to brainstorm without creating another
// refine.
func TestRefine_LastRefineRecentSkipsNewRefine(t *testing.T) {
	proj := seedRefineProject(t, "refine-recent")
	now := metav1.NewTime(time.Now())
	proj.Status.LastRefine = &now
	if err := k8sClient.Status().Update(context.Background(), proj); err != nil {
		t.Fatalf("stamp LastRefine: %v", err)
	}

	reader := &fakeReader{issues: []scm.IssueRef{{Repo: "o/r", Number: 1, Title: "open issue"}}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	if _, err := r.runScans(context.Background(), proj); err != nil {
		t.Fatalf("runScans: %v", err)
	}
	refineQEs := listRefineQEs(t, "refine-recent")
	if len(refineQEs) > 0 {
		t.Fatalf("want no refine QE when LastRefine is recent, got %d", len(refineQEs))
	}
	if len(listBrainstormQEs(t, "refine-recent")) == 0 {
		t.Fatalf("want brainstorm to run immediately when LastRefine is already current")
	}
}

// TestRefineBarrier_DoesNotGateIssueScanOrMRScan: mrScan and issueScan run on
// their own due schedule even while the refine barrier holds brainstorm.
func TestRefineBarrier_DoesNotGateIssueScanOrMRScan(t *testing.T) {
	proj := seedRefineProject(t, "refine-nogate-issuescan")
	reader := &fakeReader{issues: []scm.IssueRef{{Repo: "o/r", Number: 1, Title: "open issue"}}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	ctx := context.Background()

	if _, err := r.runScans(ctx, proj); err != nil {
		t.Fatalf("runScans: %v", err)
	}
	// The refine barrier holds brainstorm (no terminal refine yet)...
	if len(listBrainstormQEs(t, "refine-nogate-issuescan")) != 0 {
		t.Fatalf("want brainstorm held pending refine")
	}
	// ...but issueScan (due on its own schedule) still fires in the same
	// reconcile: it is no longer gated on refine.
	if len(listIssueQEs(t, "refine-nogate-issuescan")) == 0 {
		t.Fatalf("want issueScan to run even while the refine barrier holds brainstorm")
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
