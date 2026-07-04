package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
)

type fakeReader struct {
	prs      []scm.PRRef
	issues   []scm.IssueRef
	board    []scm.BoardItem
	prErr    error
	comments []scm.IssueComment
	// commentCalls counts ListIssueComments invocations, for tests asserting the
	// per-cycle comment cache dedupes repeated gate reads of the same issue.
	commentCalls int
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
func (f *fakeReader) GetCommitCIStatus(context.Context, string, string, string) (string, error) {
	return "", nil
}
func (f *fakeReader) ListIssueComments(context.Context, string, string, int) ([]scm.IssueComment, error) {
	f.commentCalls++
	return f.comments, nil
}
func (f *fakeReader) GetIssue(context.Context, string, string, int) (scm.IssueContent, error) {
	return scm.IssueContent{}, nil
}
func (f *fakeReader) GetDefaultBranchHeadSHA(context.Context, string, string) (string, error) {
	return "", nil
}
func (f *fakeReader) ListClosedIssues(context.Context, string, string, time.Time) ([]scm.IssueRef, error) {
	return nil, nil
}
func (f *fakeReader) ListCommits(context.Context, string, string, time.Time) ([]scm.CommitRef, error) {
	return nil, nil
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

func mkScanRepo(t *testing.T, project, name, url string) tatarav1alpha1.Repository {
	t.Helper()
	rp := &tatarav1alpha1.Repository{}
	rp.Name = name
	rp.Namespace = testNS
	rp.Spec = tatarav1alpha1.RepositorySpec{ProjectRef: project, URL: url, DefaultBranch: "main", ReingestSchedule: "0 6 * * *"}
	if err := k8sClient.Create(context.Background(), rp); err != nil {
		t.Fatalf("create repo %s: %v", name, err)
	}
	return *rp
}

func listScanQEs(t *testing.T, project string) []tatarav1alpha1.QueuedEvent {
	t.Helper()
	var list tatarav1alpha1.QueuedEventList
	if err := k8sClient.List(context.Background(), &list); err != nil {
		t.Fatalf("list QEs: %v", err)
	}
	var out []tatarav1alpha1.QueuedEvent
	for i := range list.Items {
		if list.Items[i].Spec.ProjectRef == project {
			out = append(out, list.Items[i])
		}
	}
	return out
}

func listBrainstormQEs(t *testing.T, project string) []tatarav1alpha1.QueuedEvent {
	t.Helper()
	qes := listScanQEs(t, project)
	var out []tatarav1alpha1.QueuedEvent
	for _, qe := range qes {
		if qe.Spec.Payload.Labels[labelActivity] == "brainstorm" {
			out = append(out, qe)
		}
	}
	return out
}

func listHealthCheckQEs(t *testing.T, project string) []tatarav1alpha1.QueuedEvent {
	t.Helper()
	qes := listScanQEs(t, project)
	var out []tatarav1alpha1.QueuedEvent
	for _, qe := range qes {
		if qe.Spec.Payload.Labels[labelActivity] == "healthCheck" {
			out = append(out, qe)
		}
	}
	return out
}

func TestIssueScan_PerRepoTopUp(t *testing.T) {
	// Per-repo cap is gone: all 4 eligible issues get QEs; backlog=false.
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 1}}
	proj, _ := seedScanProject(t, "fanout-iss", cron)
	repos := []tatarav1alpha1.Repository{
		mkScanRepo(t, "fanout-iss", "fanout-iss-a", "https://github.com/o/a.git"),
		mkScanRepo(t, "fanout-iss", "fanout-iss-b", "https://github.com/o/b.git"),
	}
	reader := &fakeReader{issues: []scm.IssueRef{
		{Repo: "o/a", Number: 1, UpdatedAt: time.Unix(100, 0)},
		{Repo: "o/a", Number: 2, UpdatedAt: time.Unix(200, 0)},
		{Repo: "o/b", Number: 3, UpdatedAt: time.Unix(100, 0)},
		{Repo: "o/b", Number: 4, UpdatedAt: time.Unix(200, 0)},
	}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	backlog, _ := r.issueScan(context.Background(), proj, reader, repos, nil, cron.IssueScan)
	if backlog {
		t.Fatalf("want backlog=false (no per-repo cap; all 4 issues enqueued)")
	}
	qes := listScanQEs(t, "fanout-iss")
	if len(qes) != 4 {
		t.Fatalf("want 4 QEs (all 4 issues enqueued without per-repo cap), got %d", len(qes))
	}
}

func TestIssueScan_PropagatesAuthorToTaskSource(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 1}}
	proj, _ := seedScanProject(t, "iss-author", cron)
	repos := []tatarav1alpha1.Repository{
		mkScanRepo(t, "iss-author", "iss-author-a", "https://github.com/o/a.git"),
	}
	reader := &fakeReader{issues: []scm.IssueRef{
		{Repo: "o/a", Number: 1, Author: "third-party-dev", UpdatedAt: time.Unix(100, 0)},
	}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	r.issueScan(context.Background(), proj, reader, repos, nil, cron.IssueScan)
	qes := listScanQEs(t, "iss-author")
	if len(qes) != 1 {
		t.Fatalf("want 1 QE, got %d", len(qes))
	}
	src := qes[0].Spec.Payload.Source
	if src == nil || src.AuthorLogin != "third-party-dev" {
		t.Fatalf("want Payload.Source.AuthorLogin=third-party-dev, got %+v", src)
	}
}

func TestCandidatesFromIssues_CarriesAuthor(t *testing.T) {
	cands := candidatesFromIssues([]scm.IssueRef{
		{Repo: "o/a", Number: 1, Author: "alice"},
		{Repo: "o/a", Number: 2, Author: "bob", IsPR: true}, // dropped
	})
	if len(cands) != 1 {
		t.Fatalf("want 1 candidate (PR dropped), got %d", len(cands))
	}
	if cands[0].author != "alice" {
		t.Fatalf("want candidate author=alice, got %q", cands[0].author)
	}
}

func TestIssueScan_ActiveTaskHoldsLane(t *testing.T) {
	// Per-repo lane cap is gone. Issue #1 is deduped (non-terminal Task for it).
	// Issue #2 is NOT blocked -> gets a QE. backlog=false (eligible=1, created=1).
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 1}}
	proj, _ := seedScanProject(t, "fanout-hold", cron)
	repoA := mkScanRepo(t, "fanout-hold", "fanout-hold-a", "https://github.com/o/a.git")

	pre := &tatarav1alpha1.Task{}
	pre.GenerateName = "scan-"
	pre.Namespace = testNS
	pre.Labels = scanTaskLabels(candidate{repo: "o/a", number: 1}, "issueScan", "issueLifecycle")
	pre.Spec = tatarav1alpha1.TaskSpec{ProjectRef: "fanout-hold", RepositoryRef: repoA.Name, Goal: "g", Kind: "issueLifecycle",
		Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/a#1", Number: 1}}
	if err := k8sClient.Create(context.Background(), pre); err != nil {
		t.Fatalf("pre-create: %v", err)
	}
	pre.Status.Phase = "Running"
	_ = k8sClient.Status().Update(context.Background(), pre)

	reader := &fakeReader{issues: []scm.IssueRef{
		{Repo: "o/a", Number: 1, UpdatedAt: time.Unix(100, 0)}, // in-flight -> deduped
		{Repo: "o/a", Number: 2, UpdatedAt: time.Unix(200, 0)}, // eligible -> gets QE
	}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	backlog, _ := r.issueScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{repoA}, []tatarav1alpha1.Task{*pre}, cron.IssueScan)
	if backlog {
		t.Fatalf("want backlog=false (#1 deduped, #2 gets QE)")
	}
	// 1 pre-existing Task + 1 new QE for #2.
	if tasks := listScanTasks(t, "fanout-hold"); len(tasks) != 1 {
		t.Fatalf("want only pre-existing task (no new Tasks; scan creates QEs), got %d tasks", len(tasks))
	}
	qes := listScanQEs(t, "fanout-hold")
	if len(qes) != 1 {
		t.Fatalf("want 1 new QE for #2, got %d", len(qes))
	}
	if qes[0].Spec.Payload.Source == nil || qes[0].Spec.Payload.Source.Number != 2 {
		t.Fatalf("QE source = %+v, want number=2", qes[0].Spec.Payload.Source)
	}
}

func TestMRScan_PerRepoTopUp(t *testing.T) {
	// Per-repo cap is gone: all 4 eligible PRs get QEs; backlog=false.
	cron := &tatarav1alpha1.ScmCron{MRScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 1}}
	proj, _ := seedScanProject(t, "fanout-mr", cron)
	repos := []tatarav1alpha1.Repository{
		mkScanRepo(t, "fanout-mr", "fanout-mr-a", "https://github.com/o/a.git"),
		mkScanRepo(t, "fanout-mr", "fanout-mr-b", "https://github.com/o/b.git"),
	}
	reader := &fakeReader{prs: []scm.PRRef{
		{Repo: "o/a", Number: 1, Author: "tatara-bot", UpdatedAt: time.Unix(100, 0)},
		{Repo: "o/a", Number: 2, Author: "human", UpdatedAt: time.Unix(200, 0)},
		{Repo: "o/b", Number: 3, Author: "human", UpdatedAt: time.Unix(100, 0)},
		{Repo: "o/b", Number: 4, Author: "tatara-bot", UpdatedAt: time.Unix(200, 0)},
	}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	backlog := r.mrScan(context.Background(), proj, reader, repos, nil, cron.MRScan)
	if backlog {
		t.Fatalf("want backlog=false (no per-repo cap; all 4 PRs enqueued)")
	}
	qes := listScanQEs(t, "fanout-mr")
	if len(qes) != 4 {
		t.Fatalf("want 4 QEs (all 4 PRs enqueued), got %d", len(qes))
	}
	for _, qe := range qes {
		k := qe.Spec.Kind
		if k != "review" && k != "issueLifecycle" {
			t.Fatalf("unexpected QE kind %q", k)
		}
	}
}

func TestRunScans_MRScanCreatesReviewAndSelfImprove(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{MRScan: tatarav1alpha1.CronActivity{Schedule: "* * * * *", MaxPerRepo: 2}}
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
	qes := listScanQEs(t, "mrscan-proj")
	if len(qes) != 2 {
		t.Fatalf("want 2 QEs, got %d", len(qes))
	}
	kinds := map[string]bool{}
	for _, qe := range qes {
		kinds[qe.Spec.Kind] = true
	}
	if !kinds["issueLifecycle"] || !kinds["review"] {
		t.Fatalf("want review+issueLifecycle kinds, got %+v", kinds)
	}
	got := &tatarav1alpha1.Project{}
	_ = k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "mrscan-proj"}, got)
	if got.Status.LastMRScan == nil {
		t.Fatalf("LastMRScan not stamped")
	}
}

func TestRunScans_IssueScanCap(t *testing.T) {
	// MaxPerRepo=1 is now ignored; both issues get QEs (bounded only by autonomous cap).
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "* * * * *", MaxPerRepo: 1}}
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
	qes := listScanQEs(t, "issuescan-proj")
	if len(qes) != 2 {
		t.Fatalf("both issues should be enqueued (no per-repo cap), got %d QEs", len(qes))
	}
	for _, qe := range qes {
		if qe.Spec.Kind != "issueLifecycle" {
			t.Fatalf("QE kind = %q, want issueLifecycle", qe.Spec.Kind)
		}
	}
}

// TestRunScans_RequeueAfterPositiveAfterFire verifies that after a due activity
// fires, runScans returns a RequeueAfter > 0 (the next-fire of the post-stamp
// schedule), not 0, so the activity continues to be scheduled.
func TestRunScans_RequeueAfterPositiveAfterFire(t *testing.T) {
	// Hourly schedule; last ran 2h ago -> due now; next fire ~1h from now.
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 1}}
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
	// "x x x x x" passes the CRD schedule pattern (5 whitespace-separated fields)
	// but fails cron parsing at reconcile time, exercising the graceful-disable
	// path. A non-5-field string ("not a cron") is now rejected at admission by
	// the schedule Pattern marker, so it can no longer be stored to reach reconcile.
	cron := &tatarav1alpha1.ScmCron{MRScan: tatarav1alpha1.CronActivity{Schedule: "x x x x x", MaxPerRepo: 1}}
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
	cron := &tatarav1alpha1.ScmCron{MRScan: tatarav1alpha1.CronActivity{Schedule: "0 0 1 1 *", MaxPerRepo: 1}} // yearly: never due now
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

// TestRunScans_DedupBeforeCap verifies that a stalest item already in flight as
// an AwaitingApproval proposal is deduped (not recreated) AND does not occupy the
// repo lane, so the next eligible item is still picked.
func TestRunScans_DedupBeforeCap(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "* * * * *", MaxPerRepo: 1}}
	proj, repo := seedScanProject(t, "dedupbefore-proj", cron)
	past := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	proj.Status.LastIssueScan = &past
	_ = k8sClient.Status().Update(context.Background(), proj)

	// Issue #10 is the stalest (updatedAt 100s). Pre-create an AwaitingApproval
	// triageIssue task for it (a proposal awaiting approval -> does not hold the lane).
	pre := &tatarav1alpha1.Task{}
	pre.GenerateName = "scan-"
	pre.Namespace = testNS
	pre.Labels = scanTaskLabels(candidate{repo: "o/r", number: 10}, "issueScan", "triageIssue")
	pre.Spec = tatarav1alpha1.TaskSpec{ProjectRef: "dedupbefore-proj", RepositoryRef: repo.Name, Goal: "g", Kind: "triageIssue",
		Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#10", Number: 10}}
	if err := k8sClient.Create(context.Background(), pre); err != nil {
		t.Fatalf("pre-create: %v", err)
	}
	pre.Status.Phase = "AwaitingApproval"
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

	// Exactly 1 new QE must be created (for #11, not #10).
	// Pre-existing Task for #10 is not a QE; check tasks still contains only pre.
	tasks := listScanTasks(t, "dedupbefore-proj")
	if len(tasks) != 1 || tasks[0].Name != pre.Name {
		t.Fatalf("want only pre-existing task, got %d tasks", len(tasks))
	}
	qes := listScanQEs(t, "dedupbefore-proj")
	if len(qes) != 1 {
		t.Fatalf("want exactly 1 new QE for #11, got %d", len(qes))
	}
	if qes[0].Spec.Payload.Source == nil || qes[0].Spec.Payload.Source.Number != 11 {
		t.Fatalf("expected QE for #11, got source=%+v", qes[0].Spec.Payload.Source)
	}

	// skipped_dedup must be incremented for #10.
	dedupCount := counterValue(t, reg, "tatara_scan_items_total", map[string]string{"activity": "issueScan", "outcome": "skipped_dedup"})
	if dedupCount < 1 {
		t.Fatalf("skipped_dedup counter = %v, want >= 1", dedupCount)
	}
}

func TestRunScans_DedupSkipsInFlight(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "* * * * *", MaxPerRepo: 5}}
	proj, repo := seedScanProject(t, "dedup-proj", cron)
	past := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	proj.Status.LastIssueScan = &past
	_ = k8sClient.Status().Update(context.Background(), proj)
	// pre-create an in-flight triageIssue Task for o/r#10
	pre := &tatarav1alpha1.Task{}
	pre.GenerateName = "scan-"
	pre.Namespace = testNS
	pre.Labels = scanTaskLabels(candidate{repo: "o/r", number: 10}, "issueScan", "triageIssue")
	pre.Spec = tatarav1alpha1.TaskSpec{ProjectRef: "dedup-proj", RepositoryRef: repo.Name, Goal: "g", Kind: "triageIssue",
		Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#10", Number: 10}}
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
	// only the pre-existing task; no new QE for #10 (deduped by running Task)
	if n := len(listScanTasks(t, "dedup-proj")); n != 1 {
		t.Fatalf("dedup failed: want 1 task, got %d", n)
	}
	if n := len(listScanQEs(t, "dedup-proj")); n != 0 {
		t.Fatalf("dedup failed: want 0 QEs for in-flight issue, got %d", n)
	}
}

// perRepoFakeReader allows scripting per-repo issues and errors for brainstorm
// and backstop tests. It falls through to the base fakeReader for PRs/board.
type perRepoFakeReader struct {
	fakeReader
	// issuesByRepo maps "owner/repo" -> issues to return.
	issuesByRepo map[string][]scm.IssueRef
	// errRepos is the set of "owner/repo" slugs that return an error.
	errRepos map[string]bool
}

func (f *perRepoFakeReader) ListOpenIssues(_ context.Context, owner, repo string) ([]scm.IssueRef, error) {
	slug := owner + "/" + repo
	if f.errRepos[slug] {
		return nil, fmt.Errorf("fake error for %s", slug)
	}
	if iss, ok := f.issuesByRepo[slug]; ok {
		return iss, nil
	}
	return nil, nil
}

// seedBrainstormProject creates a Project with ApprovalLabel set and a brainstorm
// cron, plus the requested repositories (by slug "owner/repo").
func seedBrainstormProject(t *testing.T, name string, repoSlugs []string, maxOpenProposals int) (*tatarav1alpha1.Project, []tatarav1alpha1.Repository) {
	t.Helper()
	ctx := context.Background()
	mkSecret(t, name+"-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	cron := &tatarav1alpha1.ScmCron{
		Brainstorm: tatarav1alpha1.BrainstormActivity{
			Enabled:          true,
			Schedule:         "0 * * * *",
			MaxOpenProposals: maxOpenProposals,
		},
	}
	proj := &tatarav1alpha1.Project{}
	proj.Name = name
	proj.Namespace = testNS
	proj.Spec.ScmSecretRef = name + "-scm"
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{
		Provider:      "github",
		Owner:         "o",
		BotLogin:      "tatara-bot",
		ApprovalLabel: "tatara/awaiting-approval",
		Cron:          cron,
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}
	var repos []tatarav1alpha1.Repository
	for _, slug := range repoSlugs {
		repoName := name + "-" + strings.ReplaceAll(slug, "/", "-")
		rp := &tatarav1alpha1.Repository{}
		rp.Name = repoName
		rp.Namespace = testNS
		rp.Spec = tatarav1alpha1.RepositorySpec{
			ProjectRef:       name,
			URL:              "https://github.com/" + slug + ".git",
			DefaultBranch:    "main",
			ReingestSchedule: "0 6 * * *",
		}
		if err := k8sClient.Create(ctx, rp); err != nil {
			t.Fatalf("create repo %s: %v", slug, err)
		}
		repos = append(repos, *rp)
	}
	return proj, repos
}

// listBrainstormTasks returns brainstorm tasks for the given project.
func listBrainstormTasks(t *testing.T, project string) []tatarav1alpha1.Task {
	t.Helper()
	tasks := listScanTasks(t, project)
	var out []tatarav1alpha1.Task
	for _, tk := range tasks {
		if tk.Labels[labelActivity] == "brainstorm" {
			out = append(out, tk)
		}
	}
	return out
}

// TestBrainstorm_UnderCap_CreatesOneProjectTask: 2 repos, 0 proposals each ->
// exactly ONE brainstorm QueuedEvent (project-level, not per-repo).
func TestBrainstorm_UnderCap_CreatesOneProjectTask(t *testing.T) {
	proj, repos := seedBrainstormProject(t, "bs-undercap", []string{"o/a", "o/b"}, 3)
	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/a": {},
			"o/b": {},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	act := tatarav1alpha1.BrainstormActivity{Enabled: true, MaxOpenProposals: 3}
	r.brainstorm(context.Background(), proj, reader, repos, nil, act)

	qes := listBrainstormQEs(t, "bs-undercap")
	// Project-level: one QE per cycle, not one per repo.
	if len(qes) != 1 {
		t.Fatalf("want 1 brainstorm QE (project-level), got %d", len(qes))
	}
}

// TestBrainstorm_AtCap_SkipsRepo: repo with >= maxOpenProposals open idea-label issues -> no brainstorm task.
func TestBrainstorm_AtCap_SkipsRepo(t *testing.T) {
	proj, repos := seedBrainstormProject(t, "bs-atcap", []string{"o/c"}, 3)
	// 3 open issues with the idea label (default "tatara-idea") -> at cap
	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/c": {
				{Repo: "o/c", Number: 1, Labels: []string{"tatara-idea"}, IsPR: false},
				{Repo: "o/c", Number: 2, Labels: []string{"tatara-idea"}, IsPR: false},
				{Repo: "o/c", Number: 3, Labels: []string{"tatara-idea"}, IsPR: false},
			},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	act := tatarav1alpha1.BrainstormActivity{Enabled: true, MaxOpenProposals: 3}
	r.brainstorm(context.Background(), proj, reader, repos, nil, act)

	qes := listBrainstormQEs(t, "bs-atcap")
	if len(qes) != 0 {
		t.Fatalf("want 0 brainstorm QEs (at cap), got %d", len(qes))
	}
}

// TestBrainstorm_InFlight_SkipsRepo: pre-existing non-terminal brainstorm Task -> no new task.
func TestBrainstorm_InFlight_SkipsRepo(t *testing.T) {
	proj, repos := seedBrainstormProject(t, "bs-inflight", []string{"o/d"}, 3)
	// pre-create a Planning brainstorm Task for this repo
	pre := &tatarav1alpha1.Task{}
	pre.GenerateName = "brainstorm-"
	pre.Namespace = testNS
	pre.Labels = map[string]string{labelActivity: "brainstorm"}
	pre.Spec = tatarav1alpha1.TaskSpec{
		ProjectRef:    "bs-inflight",
		RepositoryRef: repos[0].Name,
		Goal:          "g",
		Kind:          "brainstorm",
	}
	if err := k8sClient.Create(context.Background(), pre); err != nil {
		t.Fatalf("pre-create: %v", err)
	}
	pre.Status.Phase = "Planning"
	_ = k8sClient.Status().Update(context.Background(), pre)

	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{"o/d": {}},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	existing := []tatarav1alpha1.Task{*pre}
	act := tatarav1alpha1.BrainstormActivity{Enabled: true, MaxOpenProposals: 3}
	r.brainstorm(context.Background(), proj, reader, repos, existing, act)

	// Pre-existing Task with labelActivity=brainstorm blocks new QE (in-flight guard).
	tasks := listBrainstormTasks(t, "bs-inflight")
	if len(tasks) != 1 {
		t.Fatalf("want 1 pre-existing task (in-flight guard blocks new QE), got %d", len(tasks))
	}
	qes := listBrainstormQEs(t, "bs-inflight")
	if len(qes) != 0 {
		t.Fatalf("want 0 new QEs (in-flight guard), got %d", len(qes))
	}
}

// TestBrainstorm_ListErrorSkipsBacklog_StillCreatesTask: reader error for repo
// E's backlog is non-fatal; the project task is still created (project-scoped,
// empty RepositoryRef). The goal still includes both repo slugs.
func TestBrainstorm_ListErrorSkipsBacklog_StillCreatesTask(t *testing.T) {
	proj, repos := seedBrainstormProject(t, "bs-isolate", []string{"o/e", "o/f"}, 3)
	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/e": {}, // will error (errRepos)
			"o/f": {},
		},
		errRepos: map[string]bool{"o/e": true},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	act := tatarav1alpha1.BrainstormActivity{Enabled: true, MaxOpenProposals: 3}
	r.brainstorm(context.Background(), proj, reader, repos, nil, act)

	qes := listBrainstormQEs(t, "bs-isolate")
	// One project-level QE is still created; the backlog error for e is non-fatal.
	if len(qes) != 1 {
		t.Fatalf("want 1 brainstorm QE (backlog error non-fatal), got %d", len(qes))
	}
	// Project-scoped: RepositoryRef must be empty.
	if qes[0].Spec.RepositoryRef != "" {
		t.Fatalf("brainstorm QE RepositoryRef = %q, want empty (project-scoped)", qes[0].Spec.RepositoryRef)
	}
	_ = repos // repos used for setup only
}

// ----- C5: Brainstorm goal names the deep-research skill -----

func TestBrainstormGoal_NamesDeepResearchSkill(t *testing.T) {
	g := brainstormGoalProject([]string{"tatara-cli"}, "", "")
	if !strings.Contains(g, "tatara-deep-research") {
		t.Fatalf("brainstorm goal does not invoke tatara-deep-research skill: %s", g)
	}
	if !strings.Contains(g, "tatara-cli") {
		t.Fatalf("brainstorm goal lost the repo slug: %s", g)
	}
}
