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
	"github.com/prometheus/client_golang/prometheus/testutil"
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
func markRefineTerminal(t *testing.T, project, stg string) {
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
	refineTask.Status.Stage = stg
	if err := k8sClient.Status().Update(ctx, refineTask); err != nil {
		t.Fatalf("mark refine %s: %v", stg, err)
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
	markRefineTerminal(t, "refine-release", tatarav1alpha1.StageDelivered)

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
	markRefineTerminal(t, "refine-failed", tatarav1alpha1.StageFailed)

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

// TestLatestTerminalRefineTask_ScopedToCycle: a terminal refine Task from a
// past cycle (created before the current cycle's due-base) must NOT satisfy
// the barrier for a later cycle - only a Task created at/after `since`
// counts. Regression test: before the fix, latestTerminalRefineTask ignored
// `since` entirely, so a single terminal refine Task kept re-satisfying the
// barrier for every brainstorm tick until TaskRetention (7d) GC'd it, instead
// of grooming once per cycle.
func TestLatestTerminalRefineTask_ScopedToCycle(t *testing.T) {
	proj := seedRefineProject(t, "refine-cycle-scope")
	r := newScanReconciler(&fakeReader{})
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
	ctx := context.Background()

	task := &tatarav1alpha1.Task{}
	task.GenerateName = "refine-"
	task.Namespace = testNS
	task.Spec = tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Kind: "refine", Goal: "g"}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create refine task: %v", err)
	}
	task.Status.Stage = tatarav1alpha1.StageDelivered
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("mark refine succeeded: %v", err)
	}
	createdAt := task.CreationTimestamp.Time

	// Cycle whose due-base precedes the task's creation: the task belongs to
	// this cycle and satisfies the barrier.
	sameCycle, err := r.latestTerminalRefineTask(ctx, proj, createdAt.Add(-time.Minute))
	if err != nil {
		t.Fatalf("latestTerminalRefineTask (same cycle): %v", err)
	}
	if sameCycle == nil {
		t.Fatalf("want terminal task to satisfy a barrier whose base precedes its creation")
	}

	// A later cycle's due-base is after the task's creation: the stale task
	// must NOT satisfy this cycle's barrier.
	laterCycle, err := r.latestTerminalRefineTask(ctx, proj, createdAt.Add(time.Minute))
	if err != nil {
		t.Fatalf("latestTerminalRefineTask (later cycle): %v", err)
	}
	if laterCycle != nil {
		t.Fatalf("want stale terminal task NOT to satisfy a later cycle's barrier, got %s", laterCycle.Name)
	}
}

// TestRefineBarrier_HeldEmitsMetricPerTick: issue #401 instrumentation. Every
// scan tick the refine barrier holds brainstorm (refine never terminates)
// must increment SweepErrorsTotal{brainstorm,refine_barrier_held} exactly
// once - previously this early-return emitted zero log/metric, which is how
// the underlying stall went unnoticed until an on-call escalation.
func TestRefineBarrier_HeldEmitsMetricPerTick(t *testing.T) {
	proj := seedRefineProject(t, "refine-held-metric")
	reader := &fakeReader{issues: []scm.IssueRef{{Repo: "o/r", Number: 1, Title: "open issue"}}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
	ctx := context.Background()

	before := testutil.ToFloat64(obs.SweepErrorsTotal.WithLabelValues("brainstorm", "refine_barrier_held"))

	if _, err := r.runScans(ctx, proj); err != nil {
		t.Fatalf("runScans round 1: %v", err)
	}
	afterFirst := testutil.ToFloat64(obs.SweepErrorsTotal.WithLabelValues("brainstorm", "refine_barrier_held"))
	if afterFirst != before+1 {
		t.Fatalf("refine_barrier_held after tick 1 = %v, want %v", afterFirst, before+1)
	}

	if _, err := r.runScans(ctx, proj); err != nil {
		t.Fatalf("runScans round 2: %v", err)
	}
	afterSecond := testutil.ToFloat64(obs.SweepErrorsTotal.WithLabelValues("brainstorm", "refine_barrier_held"))
	if afterSecond != afterFirst+1 {
		t.Fatalf("refine_barrier_held after tick 2 = %v, want %v (once per tick, not cumulative per-repo)", afterSecond, afterFirst+1)
	}
}

// TestRefineBarrier_MaxHoldReleasesBrainstorm: issue #401 release valve. Once
// the refine barrier has held for longer than requeueRefineBarrierMaxHold
// (2h), runScans proceeds to brainstorm anyway - LastBrainstorm advances and a
// brainstorm QueuedEvent is created - even though the refine Task never
// reached a terminal stage, and records refine_barrier_timeout (not
// refine_barrier_held) for that tick.
func TestRefineBarrier_MaxHoldReleasesBrainstorm(t *testing.T) {
	proj := seedRefineProject(t, "refine-maxhold")
	stale := metav1.NewTime(time.Now().Add(-3 * time.Hour))
	proj.Status.LastBrainstorm = &stale
	if err := k8sClient.Status().Update(context.Background(), proj); err != nil {
		t.Fatalf("stamp stale LastBrainstorm: %v", err)
	}
	reader := &fakeReader{issues: []scm.IssueRef{{Repo: "o/r", Number: 1, Title: "open issue"}}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
	ctx := context.Background()

	beforeTimeout := testutil.ToFloat64(obs.SweepErrorsTotal.WithLabelValues("brainstorm", "refine_barrier_timeout"))

	if _, err := r.runScans(ctx, proj); err != nil {
		t.Fatalf("runScans: %v", err)
	}

	if got := testutil.ToFloat64(obs.SweepErrorsTotal.WithLabelValues("brainstorm", "refine_barrier_timeout")); got != beforeTimeout+1 {
		t.Fatalf("refine_barrier_timeout = %v, want %v (max-hold release)", got, beforeTimeout+1)
	}
	if len(listBrainstormQEs(t, "refine-maxhold")) == 0 {
		t.Fatalf("want brainstorm QE created once the max hold releases the barrier")
	}
	var fresh tatarav1alpha1.Project
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "refine-maxhold"}, &fresh); err != nil {
		t.Fatalf("get project: %v", err)
	}
	if fresh.Status.LastBrainstorm == nil || !fresh.Status.LastBrainstorm.After(stale.Time) {
		t.Fatalf("want LastBrainstorm advanced past the stale stamp, got %v", fresh.Status.LastBrainstorm)
	}
}

// TestRefineBarrier_JustUnderMaxHoldDoesNotRelease: a barrier held for just
// under requeueRefineBarrierMaxHold must NOT release prematurely - brainstorm
// stays deferred and refine_barrier_timeout does not increment.
func TestRefineBarrier_JustUnderMaxHoldDoesNotRelease(t *testing.T) {
	proj := seedRefineProject(t, "refine-justunder")
	justUnder := metav1.NewTime(time.Now().Add(-(requeueRefineBarrierMaxHold - time.Minute)))
	proj.Status.LastBrainstorm = &justUnder
	if err := k8sClient.Status().Update(context.Background(), proj); err != nil {
		t.Fatalf("stamp near-max LastBrainstorm: %v", err)
	}
	reader := &fakeReader{issues: []scm.IssueRef{{Repo: "o/r", Number: 1, Title: "open issue"}}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
	ctx := context.Background()

	beforeTimeout := testutil.ToFloat64(obs.SweepErrorsTotal.WithLabelValues("brainstorm", "refine_barrier_timeout"))

	if _, err := r.runScans(ctx, proj); err != nil {
		t.Fatalf("runScans: %v", err)
	}

	if got := testutil.ToFloat64(obs.SweepErrorsTotal.WithLabelValues("brainstorm", "refine_barrier_timeout")); got != beforeTimeout {
		t.Fatalf("refine_barrier_timeout = %v, want unchanged at %v (must not release early)", got, beforeTimeout)
	}
	if len(listBrainstormQEs(t, "refine-justunder")) != 0 {
		t.Fatalf("want brainstorm still held just under the max hold")
	}
}
