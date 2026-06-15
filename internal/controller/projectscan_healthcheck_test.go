package controller

// Repo-health-check cycle tests, modeled on the project-level brainstorm tests:
// one healthCheck Task per project per cycle, targeting ONE repo chosen
// stale-first, with the same in-flight and backlog backpressure guards.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func seedHealthCheckProject(t *testing.T, name string, repoSlugs []string, maxOpenFindings int) (*tatarav1alpha1.Project, []tatarav1alpha1.Repository) {
	t.Helper()
	ctx := context.Background()
	mkSecret(t, name+"-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	cron := &tatarav1alpha1.ScmCron{
		HealthCheck: tatarav1alpha1.HealthCheckActivity{
			Enabled:         true,
			Schedule:        "0 * * * *",
			MaxOpenFindings: maxOpenFindings,
		},
	}
	proj := &tatarav1alpha1.Project{}
	proj.Name = name
	proj.Namespace = testNS
	proj.Spec.ScmSecretRef = name + "-scm"
	proj.Spec.Scm = &tatarav1alpha1.ScmSpec{
		Provider: "github",
		Owner:    "o",
		BotLogin: "tatara-bot",
		Cron:     cron,
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

func listHealthCheckTasks(t *testing.T, project string) []tatarav1alpha1.Task {
	t.Helper()
	var out []tatarav1alpha1.Task
	for _, tk := range listScanTasks(t, project) {
		if tk.Labels[labelActivity] == "healthCheck" {
			out = append(out, tk)
		}
	}
	return out
}

// TestHealthCheck_OneTask: 2 repos, 0 open proposals -> exactly ONE healthCheck
// Task and the shared budget is decremented by one.
func TestHealthCheck_OneTask(t *testing.T) {
	proj, repos := seedHealthCheckProject(t, "hc-one", []string{"o/alpha", "o/beta"}, 5)
	reader := &perRepoFakeReader{issuesByRepo: map[string][]scm.IssueRef{"o/alpha": {}, "o/beta": {}}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	act := tatarav1alpha1.HealthCheckActivity{Enabled: true, MaxOpenFindings: 5}
	budget := 99
	r.healthCheck(context.Background(), proj, reader, repos, nil, act, &budget)

	tasks := listHealthCheckTasks(t, "hc-one")
	if len(tasks) != 1 {
		t.Fatalf("want exactly 1 healthCheck task for 2-repo project, got %d", len(tasks))
	}
	if budget != 98 {
		t.Fatalf("budget = %d after 1 create, want 98", budget)
	}
	if tasks[0].Spec.Kind != "healthCheck" {
		t.Fatalf("task kind = %q, want healthCheck", tasks[0].Spec.Kind)
	}
}

// TestHealthCheck_InFlight_Blocks: any non-terminal healthCheck Task in the
// project blocks a new cycle.
func TestHealthCheck_InFlight_Blocks(t *testing.T) {
	proj, repos := seedHealthCheckProject(t, "hc-inflight", []string{"o/x", "o/y"}, 5)

	pre := &tatarav1alpha1.Task{}
	pre.GenerateName = "healthcheck-"
	pre.Namespace = testNS
	pre.Labels = map[string]string{labelActivity: "healthCheck"}
	pre.Spec = tatarav1alpha1.TaskSpec{ProjectRef: "hc-inflight", RepositoryRef: repos[0].Name, Goal: "g", Kind: "healthCheck"}
	if err := k8sClient.Create(context.Background(), pre); err != nil {
		t.Fatalf("pre-create: %v", err)
	}
	pre.Status.Phase = "Planning"
	_ = k8sClient.Status().Update(context.Background(), pre)

	reader := &perRepoFakeReader{issuesByRepo: map[string][]scm.IssueRef{"o/x": {}, "o/y": {}}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	existing := []tatarav1alpha1.Task{*pre}
	act := tatarav1alpha1.HealthCheckActivity{Enabled: true, MaxOpenFindings: 5}
	budget := 99
	r.healthCheck(context.Background(), proj, reader, repos, existing, act, &budget)

	if tasks := listHealthCheckTasks(t, "hc-inflight"); len(tasks) != 1 {
		t.Fatalf("want 1 task (pre-existing only; in-flight guard), got %d", len(tasks))
	}
}

// TestHealthCheck_Backlog_AtCap_Skips: open proposals across repos sum to the
// cap -> no new task (shared brainstorming-labeled backlog).
func TestHealthCheck_Backlog_AtCap_Skips(t *testing.T) {
	proj, repos := seedHealthCheckProject(t, "hc-cap", []string{"o/m", "o/n"}, 5)
	reader := &perRepoFakeReader{issuesByRepo: map[string][]scm.IssueRef{
		"o/m": {
			{Repo: "o/m", Number: 1, Labels: []string{"tatara-idea"}},
			{Repo: "o/m", Number: 2, Labels: []string{"tatara-idea"}},
			{Repo: "o/m", Number: 3, Labels: []string{"tatara-idea"}},
		},
		"o/n": {
			{Repo: "o/n", Number: 4, Labels: []string{"tatara-idea"}},
			{Repo: "o/n", Number: 5, Labels: []string{"tatara-idea"}},
		},
	}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	act := tatarav1alpha1.HealthCheckActivity{Enabled: true, MaxOpenFindings: 5}
	budget := 99
	r.healthCheck(context.Background(), proj, reader, repos, nil, act, &budget)

	if tasks := listHealthCheckTasks(t, "hc-cap"); len(tasks) != 0 {
		t.Fatalf("want 0 tasks (backlog >= maxOpenFindings), got %d", len(tasks))
	}
}

// TestHealthCheck_Backlog_UnderCap_Creates: 3+1 = 4 < 5 -> create 1.
func TestHealthCheck_Backlog_UnderCap_Creates(t *testing.T) {
	proj, repos := seedHealthCheckProject(t, "hc-under", []string{"o/p", "o/q"}, 5)
	reader := &perRepoFakeReader{issuesByRepo: map[string][]scm.IssueRef{
		"o/p": {
			{Repo: "o/p", Number: 1, Labels: []string{"tatara-idea"}},
			{Repo: "o/p", Number: 2, Labels: []string{"tatara-idea"}},
			{Repo: "o/p", Number: 3, Labels: []string{"tatara-idea"}},
		},
		"o/q": {{Repo: "o/q", Number: 4, Labels: []string{"tatara-idea"}}},
	}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	act := tatarav1alpha1.HealthCheckActivity{Enabled: true, MaxOpenFindings: 5}
	budget := 99
	r.healthCheck(context.Background(), proj, reader, repos, nil, act, &budget)

	if tasks := listHealthCheckTasks(t, "hc-under"); len(tasks) != 1 {
		t.Fatalf("want 1 task (sum=4 < maxOpenFindings=5), got %d", len(tasks))
	}
}

// mkRepo builds an in-memory Repository for the pure pick tests.
func mkHCRepo(name, slug string) tatarav1alpha1.Repository {
	rp := tatarav1alpha1.Repository{}
	rp.Name = name
	rp.Spec.URL = "https://github.com/" + slug + ".git"
	return rp
}

// hcTask builds an in-memory healthCheck Task for a repo with a creation time.
func hcTask(repoName string, created time.Time) tatarav1alpha1.Task {
	tk := tatarav1alpha1.Task{}
	tk.Labels = map[string]string{labelActivity: "healthCheck"}
	tk.CreationTimestamp = metav1.NewTime(created)
	tk.Spec = tatarav1alpha1.TaskSpec{RepositoryRef: repoName, Kind: "healthCheck"}
	return tk
}

// TestPickHealthCheckRepo_NeverCheckedWins: a repo with no prior healthCheck
// Task is the most stale and wins over a recently-checked repo.
func TestPickHealthCheckRepo_NeverCheckedWins(t *testing.T) {
	a := mkHCRepo("repo-a", "o/a")
	b := mkHCRepo("repo-b", "o/b")
	repos := []tatarav1alpha1.Repository{a, b}
	existing := []tatarav1alpha1.Task{hcTask("repo-a", time.Now())}

	got := pickHealthCheckRepo(repos, existing)
	if got == nil || got.Name != "repo-b" {
		t.Fatalf("want repo-b (never checked), got %v", got)
	}
}

// TestPickHealthCheckRepo_OldestWins: when both repos have been checked, the
// one checked longest ago wins.
func TestPickHealthCheckRepo_OldestWins(t *testing.T) {
	a := mkHCRepo("repo-a", "o/a")
	b := mkHCRepo("repo-b", "o/b")
	repos := []tatarav1alpha1.Repository{a, b}
	now := time.Now()
	existing := []tatarav1alpha1.Task{
		hcTask("repo-a", now.Add(-72*time.Hour)),
		hcTask("repo-b", now.Add(-1*time.Hour)),
	}

	got := pickHealthCheckRepo(repos, existing)
	if got == nil || got.Name != "repo-a" {
		t.Fatalf("want repo-a (oldest check), got %v", got)
	}
}

// TestPickHealthCheckRepo_TieBreakByName: with no history, selection is the
// name-sorted first repo (deterministic).
func TestPickHealthCheckRepo_TieBreakByName(t *testing.T) {
	repos := []tatarav1alpha1.Repository{mkHCRepo("zzz", "o/zzz"), mkHCRepo("aaa", "o/aaa"), mkHCRepo("mmm", "o/mmm")}
	got := pickHealthCheckRepo(repos, nil)
	if got == nil || got.Name != "aaa" {
		t.Fatalf("want aaa (first sorted), got %v", got)
	}
}

// TestHealthCheckGoalRepo: the goal targets the single repo, names the
// tatara-repo-health skill, and mandates a single propose_issue call.
func TestHealthCheckGoalRepo(t *testing.T) {
	g := healthCheckGoalRepo("o/alpha")
	for _, want := range []string{"o/alpha", "tatara-repo-health", "propose_issue"} {
		if !strings.Contains(g, want) {
			t.Fatalf("goal missing %q: %s", want, g)
		}
	}
}
