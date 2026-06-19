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
func (f *fakeReader) GetCommitCIStatus(context.Context, string, string, string) (string, error) {
	return "", nil
}
func (f *fakeReader) ListIssueComments(context.Context, string, string, int) ([]scm.IssueComment, error) {
	return nil, nil
}
func (f *fakeReader) GetIssue(context.Context, string, string, int) (scm.IssueContent, error) {
	return scm.IssueContent{}, nil
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

func TestIssueScan_PerRepoTopUp(t *testing.T) {
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

	b := 99
	backlog, _ := r.issueScan(context.Background(), proj, reader, repos, nil, cron.IssueScan, &b)
	if !backlog {
		t.Fatalf("want backlog=true (2 of 4 issues remain after per-repo top-up)")
	}
	tasks := listScanTasks(t, "fanout-iss")
	bySlug := map[string]int{}
	for i := range tasks {
		bySlug[tasks[i].Labels[labelSourceRepo]]++
	}
	if len(tasks) != 2 || bySlug[sanitizeRepoLabel("o/a")] != 1 || bySlug[sanitizeRepoLabel("o/b")] != 1 {
		t.Fatalf("want 1 task per repo (o/a, o/b), got %d tasks: %v", len(tasks), bySlug)
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

	b := 99
	r.issueScan(context.Background(), proj, reader, repos, nil, cron.IssueScan, &b)
	tasks := listScanTasks(t, "iss-author")
	if len(tasks) != 1 {
		t.Fatalf("want 1 task, got %d", len(tasks))
	}
	src := tasks[0].Spec.Source
	if src == nil || src.AuthorLogin != "third-party-dev" {
		t.Fatalf("want Source.AuthorLogin=third-party-dev, got %+v", src)
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
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 1}}
	proj, _ := seedScanProject(t, "fanout-hold", cron)
	repoA := mkScanRepo(t, "fanout-hold", "fanout-hold-a", "https://github.com/o/a.git")

	// issueLifecycle (not triageIssue) holds the lane for the new binder.
	pre := &tatarav1alpha1.Task{}
	pre.GenerateName = "scan-"
	pre.Namespace = testNS
	pre.Labels = scanTaskLabels(candidate{repo: "o/a", number: 1}, "issueScan", "issueLifecycle")
	pre.Spec = tatarav1alpha1.TaskSpec{ProjectRef: "fanout-hold", RepositoryRef: repoA.Name, Goal: "g", Kind: "issueLifecycle"}
	if err := k8sClient.Create(context.Background(), pre); err != nil {
		t.Fatalf("pre-create: %v", err)
	}
	pre.Status.Phase = "Running"
	_ = k8sClient.Status().Update(context.Background(), pre)

	reader := &fakeReader{issues: []scm.IssueRef{
		{Repo: "o/a", Number: 1, UpdatedAt: time.Unix(100, 0)}, // in-flight (deduped)
		{Repo: "o/a", Number: 2, UpdatedAt: time.Unix(200, 0)}, // blocked by the held lane
	}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	b2 := 99
	backlog, _ := r.issueScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{repoA}, []tatarav1alpha1.Task{*pre}, cron.IssueScan, &b2)
	if !backlog {
		t.Fatalf("want backlog=true (#2 blocked by the Running #1 lane)")
	}
	if tasks := listScanTasks(t, "fanout-hold"); len(tasks) != 1 {
		t.Fatalf("want only the pre-existing task (lane held by Running #1), got %d", len(tasks))
	}
}

func TestMRScan_PerRepoTopUp(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{MRScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 1}}
	proj, _ := seedScanProject(t, "fanout-mr", cron)
	repos := []tatarav1alpha1.Repository{
		mkScanRepo(t, "fanout-mr", "fanout-mr-a", "https://github.com/o/a.git"),
		mkScanRepo(t, "fanout-mr", "fanout-mr-b", "https://github.com/o/b.git"),
	}
	reader := &fakeReader{prs: []scm.PRRef{
		{Repo: "o/a", Number: 1, Author: "tatara-bot", UpdatedAt: time.Unix(100, 0)}, // o/a stalest -> issueLifecycle/MRCI
		{Repo: "o/a", Number: 2, Author: "human", UpdatedAt: time.Unix(200, 0)},
		{Repo: "o/b", Number: 3, Author: "human", UpdatedAt: time.Unix(100, 0)}, // o/b stalest -> review
		{Repo: "o/b", Number: 4, Author: "tatara-bot", UpdatedAt: time.Unix(200, 0)},
	}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	b3 := 99
	backlog := r.mrScan(context.Background(), proj, reader, repos, nil, cron.MRScan, &b3)
	if !backlog {
		t.Fatalf("want backlog=true (2 of 4 PRs remain after per-repo top-up)")
	}
	tasks := listScanTasks(t, "fanout-mr")
	bySlug := map[string]int{}
	for i := range tasks {
		bySlug[tasks[i].Labels[labelSourceRepo]]++
		if k := tasks[i].Spec.Kind; k != "review" && k != "issueLifecycle" {
			t.Fatalf("unexpected kind %q", k)
		}
	}
	if len(tasks) != 2 || bySlug[sanitizeRepoLabel("o/a")] != 1 || bySlug[sanitizeRepoLabel("o/b")] != 1 {
		t.Fatalf("want 1 mr task per repo (o/a, o/b), got %d tasks: %v", len(tasks), bySlug)
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
	tasks := listScanTasks(t, "mrscan-proj")
	if len(tasks) != 2 {
		t.Fatalf("want 2 tasks, got %d", len(tasks))
	}
	kinds := map[string]bool{}
	for _, tk := range tasks {
		kinds[tk.Spec.Kind] = true
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
	tasks := listScanTasks(t, "issuescan-proj")
	if len(tasks) != 1 {
		t.Fatalf("cap=1 should create 1 task, got %d", len(tasks))
	}
	if tasks[0].Spec.Kind != "issueLifecycle" {
		t.Fatalf("kind = %q, want issueLifecycle", tasks[0].Spec.Kind)
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
	pre.Spec = tatarav1alpha1.TaskSpec{ProjectRef: "dedupbefore-proj", RepositoryRef: repo.Name, Goal: "g", Kind: "triageIssue"}
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
// exactly ONE brainstorm task (project-level, not per-repo).
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
	bbs := 99
	r.brainstorm(context.Background(), proj, reader, repos, nil, act, &bbs)

	tasks := listBrainstormTasks(t, "bs-undercap")
	// Project-level: one task per cycle, not one per repo.
	if len(tasks) != 1 {
		t.Fatalf("want 1 brainstorm task (project-level), got %d", len(tasks))
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
	bbs2 := 99
	r.brainstorm(context.Background(), proj, reader, repos, nil, act, &bbs2)

	tasks := listBrainstormTasks(t, "bs-atcap")
	if len(tasks) != 0 {
		t.Fatalf("want 0 brainstorm tasks (at cap), got %d", len(tasks))
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
	bbs3 := 99
	r.brainstorm(context.Background(), proj, reader, repos, existing, act, &bbs3)

	tasks := listBrainstormTasks(t, "bs-inflight")
	if len(tasks) != 1 {
		t.Fatalf("want 1 task (pre-existing only, in-flight guard), got %d", len(tasks))
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
	bbs4 := 99
	r.brainstorm(context.Background(), proj, reader, repos, nil, act, &bbs4)

	tasks := listBrainstormTasks(t, "bs-isolate")
	// One project-level task is still created; the backlog error for e is non-fatal.
	if len(tasks) != 1 {
		t.Fatalf("want 1 brainstorm task (backlog error non-fatal), got %d", len(tasks))
	}
	// Project-scoped: no single primary repo pinned.
	if tasks[0].Spec.RepositoryRef != "" {
		t.Fatalf("brainstorm task RepositoryRef = %q, want empty (project-scoped)", tasks[0].Spec.RepositoryRef)
	}
	_ = repos // repos used for setup only
}

// ----- C5: Brainstorm goal names the deep-research skill -----

func TestBrainstormGoal_NamesDeepResearchSkill(t *testing.T) {
	g := brainstormGoalProject([]string{"tatara-cli"}, "")
	if !strings.Contains(g, "tatara-deep-research") {
		t.Fatalf("brainstorm goal does not invoke tatara-deep-research skill: %s", g)
	}
	if !strings.Contains(g, "tatara-cli") {
		t.Fatalf("brainstorm goal lost the repo slug: %s", g)
	}
}
