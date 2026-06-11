package controller

import (
	"context"
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
