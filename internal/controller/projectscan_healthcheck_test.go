package controller

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
	"k8s.io/apimachinery/pkg/types"
)

// seedHealthCheckProject creates a Project with a healthCheck cron plus the
// requested repositories (by slug "owner/repo"). Mirrors seedBrainstormProject.
func seedHealthCheckProject(t *testing.T, name string, repoSlugs []string, maxOpenProposals int) (*tatarav1alpha1.Project, []tatarav1alpha1.Repository) {
	t.Helper()
	ctx := context.Background()
	mkSecret(t, name+"-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	cron := &tatarav1alpha1.ScmCron{
		HealthCheck: tatarav1alpha1.HealthCheckActivity{
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
	tasks := listScanTasks(t, project)
	var out []tatarav1alpha1.Task
	for _, tk := range tasks {
		if tk.Labels[labelActivity] == "healthCheck" {
			out = append(out, tk)
		}
	}
	return out
}

// TestHealthCheck_UnderCap_CreatesOneProjectTask: 2 repos, 0 proposals each ->
// exactly ONE project-level healthCheck task driving the tatara-health-check skill.
func TestHealthCheck_UnderCap_CreatesOneProjectTask(t *testing.T) {
	proj, repos := seedHealthCheckProject(t, "hc-undercap", []string{"o/a", "o/b"}, 3)
	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{"o/a": {}, "o/b": {}},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	act := tatarav1alpha1.HealthCheckActivity{Enabled: true, MaxOpenProposals: 3}
	r.healthCheck(context.Background(), proj, reader, repos, nil, act)

	qes := listHealthCheckQEs(t, "hc-undercap")
	if len(qes) != 1 {
		t.Fatalf("want 1 healthCheck QE (project-level), got %d", len(qes))
	}
	qe := qes[0]
	if qe.Spec.Kind != "brainstorm" {
		t.Fatalf("healthCheck QE Kind = %q, want brainstorm (reused)", qe.Spec.Kind)
	}
	if !strings.Contains(qe.Spec.Payload.Goal, "tatara-health-check") {
		t.Fatalf("healthCheck goal does not invoke tatara-health-check skill: %s", qe.Spec.Payload.Goal)
	}
}

// TestHealthCheck_AtCap_SkipsRepo: repo with >= maxOpenProposals open idea-label
// issues -> no healthCheck task.
func TestHealthCheck_AtCap_SkipsRepo(t *testing.T) {
	proj, repos := seedHealthCheckProject(t, "hc-atcap", []string{"o/c"}, 3)
	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/c": {
				{Repo: "o/c", Number: 1, Labels: []string{"tatara-idea"}},
				{Repo: "o/c", Number: 2, Labels: []string{"tatara-idea"}},
				{Repo: "o/c", Number: 3, Labels: []string{"tatara-idea"}},
			},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	act := tatarav1alpha1.HealthCheckActivity{Enabled: true, MaxOpenProposals: 3}
	r.healthCheck(context.Background(), proj, reader, repos, nil, act)

	if qes := listHealthCheckQEs(t, "hc-atcap"); len(qes) != 0 {
		t.Fatalf("want 0 healthCheck QEs (at cap), got %d", len(qes))
	}
}

// TestHealthCheck_InFlight_SkipsRepo: a pre-existing non-terminal healthCheck Task
// blocks a new cycle (project-scoped in-flight guard, independent from brainstorm).
func TestHealthCheck_InFlight_SkipsRepo(t *testing.T) {
	proj, repos := seedHealthCheckProject(t, "hc-inflight", []string{"o/d"}, 3)
	pre := &tatarav1alpha1.Task{}
	pre.GenerateName = "healthcheck-"
	pre.Namespace = testNS
	pre.Labels = map[string]string{labelActivity: "healthCheck"}
	pre.Spec = tatarav1alpha1.TaskSpec{
		ProjectRef:    "hc-inflight",
		RepositoryRef: repos[0].Name,
		Goal:          "g",
		Kind:          "brainstorm",
	}
	if err := k8sClient.Create(context.Background(), pre); err != nil {
		t.Fatalf("pre-create: %v", err)
	}
	pre.Status.Phase = "Planning"
	_ = k8sClient.Status().Update(context.Background(), pre)

	reader := &perRepoFakeReader{issuesByRepo: map[string][]scm.IssueRef{"o/d": {}}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	existing := []tatarav1alpha1.Task{*pre}
	act := tatarav1alpha1.HealthCheckActivity{Enabled: true, MaxOpenProposals: 3}
	r.healthCheck(context.Background(), proj, reader, repos, existing, act)

	if tasks := listHealthCheckTasks(t, "hc-inflight"); len(tasks) != 1 {
		t.Fatalf("want 1 task (pre-existing only, in-flight guard), got %d", len(tasks))
	}
	if qes := listHealthCheckQEs(t, "hc-inflight"); len(qes) != 0 {
		t.Fatalf("want 0 new QEs (in-flight guard), got %d", len(qes))
	}
}

// TestHealthCheck_DoesNotBlockBrainstorm: an in-flight brainstorm Task must NOT
// block a healthCheck cycle (the two activities are independent).
func TestHealthCheck_DoesNotBlockBrainstorm(t *testing.T) {
	proj, repos := seedHealthCheckProject(t, "hc-indep", []string{"o/g"}, 3)
	bs := &tatarav1alpha1.Task{}
	bs.GenerateName = "brainstorm-"
	bs.Namespace = testNS
	bs.Labels = map[string]string{labelActivity: "brainstorm"}
	bs.Spec = tatarav1alpha1.TaskSpec{ProjectRef: "hc-indep", RepositoryRef: repos[0].Name, Goal: "g", Kind: "brainstorm"}
	if err := k8sClient.Create(context.Background(), bs); err != nil {
		t.Fatalf("pre-create brainstorm: %v", err)
	}
	bs.Status.Phase = "Planning"
	_ = k8sClient.Status().Update(context.Background(), bs)

	reader := &perRepoFakeReader{issuesByRepo: map[string][]scm.IssueRef{"o/g": {}}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	act := tatarav1alpha1.HealthCheckActivity{Enabled: true, MaxOpenProposals: 3}
	r.healthCheck(context.Background(), proj, reader, repos, []tatarav1alpha1.Task{*bs}, act)

	if qes := listHealthCheckQEs(t, "hc-indep"); len(qes) != 1 {
		t.Fatalf("want 1 healthCheck QE (brainstorm in-flight must not block), got %d", len(qes))
	}
}

func TestHealthCheckGoal_NamesHealthCheckSkill(t *testing.T) {
	g := healthCheckGoalProject([]string{"tatara-cli"}, "", "")
	if !strings.Contains(g, "tatara-health-check") {
		t.Fatalf("healthCheck goal does not invoke tatara-health-check skill: %s", g)
	}
	if !strings.Contains(g, "tatara-cli") {
		t.Fatalf("healthCheck goal lost the repo slug: %s", g)
	}
}

// TestRunScans_HealthCheckStampsLastHealthCheck: a due healthCheck activity is
// dispatched by runScans, creates one task, and records Status.LastHealthCheck.
func TestRunScans_HealthCheckStampsLastHealthCheck(t *testing.T) {
	proj, _ := seedHealthCheckProject(t, "hc-stamp", []string{"o/h"}, 3)
	// Make the activity due: a past last-fire so the hourly next-fire is in the past.
	past := metav1.NewTime(time.Now().Add(-2 * time.Hour))
	proj.Status.LastHealthCheck = &past
	if err := k8sClient.Status().Update(context.Background(), proj); err != nil {
		t.Fatalf("seed last-healthcheck: %v", err)
	}

	reader := &perRepoFakeReader{issuesByRepo: map[string][]scm.IssueRef{"o/h": {}}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	if _, err := r.runScans(context.Background(), proj); err != nil {
		t.Fatalf("runScans: %v", err)
	}

	if qes := listHealthCheckQEs(t, "hc-stamp"); len(qes) != 1 {
		t.Fatalf("want 1 healthCheck QE created by runScans, got %d", len(qes))
	}
	var got tatarav1alpha1.Project
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "hc-stamp"}, &got); err != nil {
		t.Fatalf("get project: %v", err)
	}
	if got.Status.LastHealthCheck == nil || !got.Status.LastHealthCheck.After(past.Time) {
		t.Fatalf("LastHealthCheck not advanced: %+v (seed %v)", got.Status.LastHealthCheck, past.Time)
	}
}
