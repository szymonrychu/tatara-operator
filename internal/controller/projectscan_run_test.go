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

	backlog := r.issueScan(context.Background(), proj, reader, repos, nil, cron.IssueScan)
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

	backlog := r.issueScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{repoA}, []tatarav1alpha1.Task{*pre}, cron.IssueScan)
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

	backlog := r.mrScan(context.Background(), proj, reader, repos, nil, cron.MRScan)
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
	cron := &tatarav1alpha1.ScmCron{MRScan: tatarav1alpha1.CronActivity{Schedule: "not a cron", MaxPerRepo: 1}}
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
		parts := strings.SplitN(slug, "/", 2)
		repoName := name + "-" + strings.ReplaceAll(slug, "/", "-")
		rp := &tatarav1alpha1.Repository{}
		rp.Name = repoName
		rp.Namespace = testNS
		_ = parts
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

// TestBrainstorm_UnderCap_CreatesOnePerRepo: 2 repos, 0 proposals each -> 2 brainstorm tasks.
func TestBrainstorm_UnderCap_CreatesOnePerRepo(t *testing.T) {
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

	tasks := listBrainstormTasks(t, "bs-undercap")
	if len(tasks) != 2 {
		t.Fatalf("want 2 brainstorm tasks (one per repo), got %d", len(tasks))
	}
	repoRefs := map[string]bool{}
	for _, tk := range tasks {
		repoRefs[tk.Spec.RepositoryRef] = true
	}
	for _, rp := range repos {
		if !repoRefs[rp.Name] {
			t.Fatalf("no brainstorm task for repo %s", rp.Name)
		}
	}
}

// TestBrainstorm_AtCap_SkipsRepo: repo with >= maxOpenProposals open approval-label issues -> no brainstorm task.
func TestBrainstorm_AtCap_SkipsRepo(t *testing.T) {
	proj, repos := seedBrainstormProject(t, "bs-atcap", []string{"o/c"}, 3)
	// 3 open issues with the approval label -> at cap
	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/c": {
				{Repo: "o/c", Number: 1, Labels: []string{"tatara/awaiting-approval"}, IsPR: false},
				{Repo: "o/c", Number: 2, Labels: []string{"tatara/awaiting-approval"}, IsPR: false},
				{Repo: "o/c", Number: 3, Labels: []string{"tatara/awaiting-approval"}, IsPR: false},
			},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	act := tatarav1alpha1.BrainstormActivity{Enabled: true, MaxOpenProposals: 3}
	r.brainstorm(context.Background(), proj, reader, repos, nil, act)

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
	r.brainstorm(context.Background(), proj, reader, repos, existing, act)

	tasks := listBrainstormTasks(t, "bs-inflight")
	if len(tasks) != 1 {
		t.Fatalf("want 1 task (pre-existing only, in-flight guard), got %d", len(tasks))
	}
}

// TestBrainstorm_ListErrorIsolatesRepo: reader error for repo A does not block repo B.
func TestBrainstorm_ListErrorIsolatesRepo(t *testing.T) {
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

	tasks := listBrainstormTasks(t, "bs-isolate")
	// Only repo f should get a task; repo e errored and was skipped.
	if len(tasks) != 1 {
		t.Fatalf("want 1 brainstorm task (for repo f only), got %d", len(tasks))
	}
	// Find repo for o/f
	var repoF *tatarav1alpha1.Repository
	for i := range repos {
		if strings.Contains(repos[i].Spec.URL, "o/f") {
			repoF = &repos[i]
		}
	}
	if repoF == nil {
		t.Fatal("could not find repo for o/f")
	}
	if tasks[0].Spec.RepositoryRef != repoF.Name {
		t.Fatalf("task RepositoryRef = %q, want %q", tasks[0].Spec.RepositoryRef, repoF.Name)
	}
}

// mkAwaitingApprovalTask creates an AwaitingApproval proposal Task bound to a
// project and repo, with a TaskSource.Number so the backstop can look it up.
func mkAwaitingApprovalTask(t *testing.T, project, repoName, issueRef string, issueNumber int) *tatarav1alpha1.Task {
	t.Helper()
	task := &tatarav1alpha1.Task{}
	task.GenerateName = "proposal-"
	task.Namespace = testNS
	task.Labels = map[string]string{labelActivity: "issueScan"}
	task.Spec = tatarav1alpha1.TaskSpec{
		ProjectRef:    project,
		RepositoryRef: repoName,
		Goal:          "proposal",
		Kind:          "brainstorm",
		ProposedIssue: &tatarav1alpha1.ProposedIssueSpec{
			RepositoryRef: repoName,
			Title:         "test proposal",
			Body:          "body",
			Kind:          "improvement",
		},
		Source: &tatarav1alpha1.TaskSource{
			Provider: "github",
			IssueRef: issueRef,
			Number:   issueNumber,
		},
	}
	if err := k8sClient.Create(context.Background(), task); err != nil {
		t.Fatalf("create proposal task: %v", err)
	}
	task.Status.Phase = "AwaitingApproval"
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("set AwaitingApproval: %v", err)
	}
	return task
}

// TestApprovalBackstop_FlipsStuckApproved: AwaitingApproval task, issue open
// without approval label -> ApprovalApproved condition flipped True.
func TestApprovalBackstop_FlipsStuckApproved(t *testing.T) {
	proj, repos := seedBrainstormProject(t, "backstop-flip", []string{"o/g"}, 3)
	task := mkAwaitingApprovalTask(t, "backstop-flip", repos[0].Name, "o/g#10", 10)

	// Issue 10 is open but approval label is absent -> approved on SCM but webhook missed.
	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/g": {{Repo: "o/g", Number: 10, Labels: []string{}, IsPR: false}},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	existing := []tatarav1alpha1.Task{*task}
	r.approvalBackstop(context.Background(), proj, reader, repos, existing)

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, got); err != nil {
		t.Fatalf("get task: %v", err)
	}
	var found bool
	for _, c := range got.Status.Conditions {
		if c.Type == tatarav1alpha1.ConditionApprovalApproved && c.Status == "True" {
			found = true
		}
	}
	if !found {
		t.Fatalf("ApprovalApproved condition not set True; conditions: %+v", got.Status.Conditions)
	}
}

// TestApprovalBackstop_NotApproved_NoOp: approval label still present -> no flip.
func TestApprovalBackstop_NotApproved_NoOp(t *testing.T) {
	proj, repos := seedBrainstormProject(t, "backstop-noop", []string{"o/h"}, 3)
	task := mkAwaitingApprovalTask(t, "backstop-noop", repos[0].Name, "o/h#20", 20)

	// Label still present -> not yet approved.
	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/h": {{Repo: "o/h", Number: 20, Labels: []string{"tatara/awaiting-approval"}, IsPR: false}},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	existing := []tatarav1alpha1.Task{*task}
	r.approvalBackstop(context.Background(), proj, reader, repos, existing)

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, got); err != nil {
		t.Fatalf("get task: %v", err)
	}
	for _, c := range got.Status.Conditions {
		if c.Type == tatarav1alpha1.ConditionApprovalApproved && c.Status == "True" {
			t.Fatalf("ApprovalApproved should NOT be set when label still present")
		}
	}
}

// TestApprovalBackstop_ImplRunning_NoOp: implementation already running -> no flip.
func TestApprovalBackstop_ImplRunning_NoOp(t *testing.T) {
	proj, repos := seedBrainstormProject(t, "backstop-impl", []string{"o/i"}, 3)
	task := mkAwaitingApprovalTask(t, "backstop-impl", repos[0].Name, "o/i#30", 30)

	// Pre-create a running implementation Task for the same issue.
	implTask := &tatarav1alpha1.Task{}
	implTask.GenerateName = "impl-"
	implTask.Namespace = testNS
	implTask.Labels = map[string]string{labelActivity: "issueScan"}
	implTask.Spec = tatarav1alpha1.TaskSpec{
		ProjectRef:    "backstop-impl",
		RepositoryRef: repos[0].Name,
		Goal:          "implement o/i#30",
		Kind:          "issueLifecycle",
		Source: &tatarav1alpha1.TaskSource{
			Provider: "github",
			IssueRef: "o/i#30",
			Number:   30,
		},
	}
	if err := k8sClient.Create(context.Background(), implTask); err != nil {
		t.Fatalf("create impl task: %v", err)
	}
	implTask.Status.Phase = "Running"
	_ = k8sClient.Status().Update(context.Background(), implTask)

	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/i": {{Repo: "o/i", Number: 30, Labels: []string{}, IsPR: false}},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	existing := []tatarav1alpha1.Task{*task, *implTask}
	r.approvalBackstop(context.Background(), proj, reader, repos, existing)

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, got); err != nil {
		t.Fatalf("get task: %v", err)
	}
	for _, c := range got.Status.Conditions {
		if c.Type == tatarav1alpha1.ConditionApprovalApproved && c.Status == "True" {
			t.Fatalf("ApprovalApproved should NOT be set when impl already running")
		}
	}
}
