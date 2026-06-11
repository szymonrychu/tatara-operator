package controller

import (
	"context"
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
)

type fakeReader struct {
	prs    []scm.PRRef
	issues []scm.IssueRef
	board  []scm.BoardItem
	prErr  error
}

func (f *fakeReader) ListOpenPRs(context.Context, string, string) ([]scm.PRRef, error) {
	return f.prs, f.prErr
}
func (f *fakeReader) ListOpenIssues(context.Context, string, string) ([]scm.IssueRef, error) {
	return f.issues, nil
}
func (f *fakeReader) ListBoardItems(context.Context, scm.BoardRef) ([]scm.BoardItem, error) {
	return f.board, nil
}

func seedScanProject(t *testing.T, name string, cron *tatarav1alpha1.ScmCron) (*tatarav1alpha1.Project, *tatarav1alpha1.Repository) {
	t.Helper()
	ctx := context.Background()
	mkSecret(t, name+"-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	proj := &tatarav1alpha1.Project{}
	proj.Name = name
	proj.Namespace = testNS
	proj.Spec.ScmSecretRef = name + "-scm"
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "tatara-bot", PriorityLabel: "tatara/priority", Cron: cron}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}
	repo := &tatarav1alpha1.Repository{}
	repo.Name = name + "-repo"
	repo.Namespace = testNS
	repo.Spec = tatarav1alpha1.RepositorySpec{ProjectRef: name, URL: "https://github.com/o/r.git", DefaultBranch: "main", ReingestSchedule: "0 6 * * *"}
	if err := k8sClient.Create(ctx, repo); err != nil {
		t.Fatalf("create repo: %v", err)
	}
	return proj, repo
}

func listScanTasks(t *testing.T, project string) []tatarav1alpha1.Task {
	t.Helper()
	var list tatarav1alpha1.TaskList
	if err := k8sClient.List(context.Background(), &list); err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	var out []tatarav1alpha1.Task
	for i := range list.Items {
		if list.Items[i].Spec.ProjectRef == project {
			out = append(out, list.Items[i])
		}
	}
	return out
}

func TestRunScans_MRScanCreatesReviewAndSelfImprove(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{MRScan: tatarav1alpha1.CronActivity{Schedule: "* * * * *", MaxPerCycle: 2}}
	proj, _ := seedScanProject(t, "mrscan-proj", cron)
	// Backdate LastMRScan so the * * * * * schedule fires immediately.
	past := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	proj.Status.LastMRScan = &past
	_ = k8sClient.Status().Update(context.Background(), proj)
	reader := &fakeReader{prs: []scm.PRRef{
		{Repo: "o/r", Number: 1, Author: "tatara-bot", HeadSHA: "a", UpdatedAt: time.Unix(100, 0)},
		{Repo: "o/r", Number: 2, Author: "human", HeadSHA: "b", UpdatedAt: time.Unix(200, 0)},
	}}
	r := newScanReconciler(reader)
	if _, err := r.runScans(context.Background(), proj); err != nil {
		t.Fatalf("runScans: %v", err)
	}
	tasks := listScanTasks(t, "mrscan-proj")
	if len(tasks) != 2 {
		t.Fatalf("want 2 tasks, got %d", len(tasks))
	}
	kinds := map[string]bool{}
	for _, tk := range tasks {
		kinds[tk.Spec.Kind] = true
	}
	if !kinds["selfImprove"] || !kinds["review"] {
		t.Fatalf("want review+selfImprove kinds, got %+v", kinds)
	}
	got := &tatarav1alpha1.Project{}
	_ = k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "mrscan-proj"}, got)
	if got.Status.LastMRScan == nil {
		t.Fatalf("LastMRScan not stamped")
	}
}

func TestRunScans_IssueScanCap(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "* * * * *", MaxPerCycle: 1}}
	proj, _ := seedScanProject(t, "issuescan-proj", cron)
	past := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	proj.Status.LastIssueScan = &past
	_ = k8sClient.Status().Update(context.Background(), proj)
	reader := &fakeReader{issues: []scm.IssueRef{
		{Repo: "o/r", Number: 10, UpdatedAt: time.Unix(100, 0)},
		{Repo: "o/r", Number: 11, UpdatedAt: time.Unix(200, 0)},
	}}
	r := newScanReconciler(reader)
	if _, err := r.runScans(context.Background(), proj); err != nil {
		t.Fatalf("runScans: %v", err)
	}
	tasks := listScanTasks(t, "issuescan-proj")
	if len(tasks) != 1 {
		t.Fatalf("cap=1 should create 1 task, got %d", len(tasks))
	}
	if tasks[0].Spec.Kind != "triageIssue" {
		t.Fatalf("kind = %q, want triageIssue", tasks[0].Spec.Kind)
	}
}

// TestRunScans_RequeueAfterPositiveAfterFire verifies that after a due activity
// fires, runScans returns a RequeueAfter > 0 (the next-fire of the post-stamp
// schedule), not 0, so the activity continues to be scheduled.
func TestRunScans_RequeueAfterPositiveAfterFire(t *testing.T) {
	// Hourly schedule; last ran 2h ago -> due now; next fire ~1h from now.
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerCycle: 1}}
	proj, _ := seedScanProject(t, "requeue-fire-proj", cron)
	past := metav1.NewTime(time.Now().Add(-2 * time.Hour))
	proj.Status.LastIssueScan = &past
	_ = k8sClient.Status().Update(context.Background(), proj)

	reader := &fakeReader{issues: []scm.IssueRef{}} // no issues, just checks requeue
	r := newScanReconciler(reader)
	requeue, err := r.runScans(context.Background(), proj)
	if err != nil {
		t.Fatalf("runScans: %v", err)
	}
	if requeue <= 0 {
		t.Fatalf("RequeueAfter = %v after a due issueScan fire, want > 0", requeue)
	}
	// Sanity: should be roughly 1 hour, definitely <= maxScheduleRequeue.
	if requeue > maxScheduleRequeue {
		t.Fatalf("RequeueAfter = %v exceeds maxScheduleRequeue %v", requeue, maxScheduleRequeue)
	}
}

func TestRunScans_BadCronDisablesNoCrash(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{MRScan: tatarav1alpha1.CronActivity{Schedule: "not a cron", MaxPerCycle: 1}}
	proj, _ := seedScanProject(t, "badcron-proj", cron)
	r := newScanReconciler(&fakeReader{})
	res, err := r.runScans(context.Background(), proj)
	if err != nil {
		t.Fatalf("bad cron must not error: %v", err)
	}
	if len(listScanTasks(t, "badcron-proj")) != 0 {
		t.Fatalf("bad cron must create no tasks")
	}
	_ = res
}

func TestReconcileRequeuesFromScan(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{MRScan: tatarav1alpha1.CronActivity{Schedule: "0 0 1 1 *", MaxPerCycle: 1}} // yearly: never due now
	proj, _ := seedScanProject(t, "requeue-proj", cron)
	r := newScanReconciler(&fakeReader{})
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: "requeue-proj"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter <= 0 || res.RequeueAfter > maxScheduleRequeue {
		t.Fatalf("RequeueAfter = %v, want (0, %v]", res.RequeueAfter, maxScheduleRequeue)
	}
	_ = proj
}

// counterValue reads a counter value by label from a Prometheus registry.
func counterValue(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if labelsMatch(m.GetLabel(), labels) {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func labelsMatch(pairs []*dto.LabelPair, want map[string]string) bool {
	got := map[string]string{}
	for _, p := range pairs {
		got[p.GetName()] = p.GetValue()
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

// TestRunScans_DedupBeforeCap verifies that dedup is applied before the cap so
// a capped stalest-already-in-flight item does not consume the single slot and
// starve the next eligible item.
func TestRunScans_DedupBeforeCap(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "* * * * *", MaxPerCycle: 1}}
	proj, repo := seedScanProject(t, "dedupbefore-proj", cron)
	past := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	proj.Status.LastIssueScan = &past
	_ = k8sClient.Status().Update(context.Background(), proj)

	// Issue #10 is the stalest (updatedAt 100s). Pre-create a Running triageIssue task for it.
	pre := &tatarav1alpha1.Task{}
	pre.GenerateName = "scan-"
	pre.Namespace = testNS
	pre.Labels = scanTaskLabels(candidate{repo: "o/r", number: 10}, "issueScan", "triageIssue")
	pre.Spec = tatarav1alpha1.TaskSpec{ProjectRef: "dedupbefore-proj", RepositoryRef: repo.Name, Goal: "g", Kind: "triageIssue"}
	if err := k8sClient.Create(context.Background(), pre); err != nil {
		t.Fatalf("pre-create: %v", err)
	}
	pre.Status.Phase = "Running"
	_ = k8sClient.Status().Update(context.Background(), pre)

	// Two issues: #10 (stalest, deduped) and #11 (newer, eligible).
	reader := &fakeReader{issues: []scm.IssueRef{
		{Repo: "o/r", Number: 10, UpdatedAt: time.Unix(100, 0)}, // stalest - deduped
		{Repo: "o/r", Number: 11, UpdatedAt: time.Unix(200, 0)}, // second - should be picked
	}}

	reg := prometheus.NewRegistry()
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(reg)

	if _, err := r.runScans(context.Background(), proj); err != nil {
		t.Fatalf("runScans: %v", err)
	}

	// Exactly 1 new task must be created (for #11, not #10).
	tasks := listScanTasks(t, "dedupbefore-proj")
	newTasks := 0
	for _, tk := range tasks {
		if tk.Name != pre.Name {
			newTasks++
			if tk.Spec.Source == nil || tk.Spec.Source.Number != 11 {
				t.Fatalf("expected new task for #11, got source=%+v", tk.Spec.Source)
			}
		}
	}
	if newTasks != 1 {
		t.Fatalf("want exactly 1 new task for #11, got %d new tasks (total %d)", newTasks, len(tasks))
	}

	// skipped_dedup must be incremented for #10.
	dedupCount := counterValue(t, reg, "tatara_scan_items_total", map[string]string{"activity": "issueScan", "outcome": "skipped_dedup"})
	if dedupCount < 1 {
		t.Fatalf("skipped_dedup counter = %v, want >= 1", dedupCount)
	}

	// skipped_cap must be 0 (eligible=1 item after dedup, cap=1 -> no truncation).
	capCount := counterValue(t, reg, "tatara_scan_items_total", map[string]string{"activity": "issueScan", "outcome": "skipped_cap"})
	if capCount != 0 {
		t.Fatalf("skipped_cap counter = %v, want 0", capCount)
	}
}

func TestRunScans_DedupSkipsInFlight(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "* * * * *", MaxPerCycle: 5}}
	proj, repo := seedScanProject(t, "dedup-proj", cron)
	past := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	proj.Status.LastIssueScan = &past
	_ = k8sClient.Status().Update(context.Background(), proj)
	// pre-create an in-flight triageIssue Task for o/r#10
	pre := &tatarav1alpha1.Task{}
	pre.GenerateName = "scan-"
	pre.Namespace = testNS
	pre.Labels = scanTaskLabels(candidate{repo: "o/r", number: 10}, "issueScan", "triageIssue")
	pre.Spec = tatarav1alpha1.TaskSpec{ProjectRef: "dedup-proj", RepositoryRef: repo.Name, Goal: "g", Kind: "triageIssue"}
	if err := k8sClient.Create(context.Background(), pre); err != nil {
		t.Fatalf("pre-create: %v", err)
	}
	pre.Status.Phase = "Running"
	_ = k8sClient.Status().Update(context.Background(), pre)

	reader := &fakeReader{issues: []scm.IssueRef{{Repo: "o/r", Number: 10, UpdatedAt: time.Unix(100, 0)}}}
	r := newScanReconciler(reader)
	if _, err := r.runScans(context.Background(), proj); err != nil {
		t.Fatalf("runScans: %v", err)
	}
	// only the pre-existing one; no new task for #10
	if n := len(listScanTasks(t, "dedup-proj")); n != 1 {
		t.Fatalf("dedup failed: want 1 task, got %d", n)
	}
}
