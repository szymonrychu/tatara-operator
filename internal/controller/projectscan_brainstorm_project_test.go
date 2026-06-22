package controller

// Project-level brainstorm tests (TDD - written before implementation).
// These tests define the NEW behavior: one brainstorm Task per project per
// cycle, not one per repo. They must FAIL until the implementation lands.

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// TestBrainstorm_ProjectLevel_MultiRepo_OneTask: 2 repos, 0 proposals across
// the project -> exactly ONE brainstorm Task created, not two.
func TestBrainstorm_ProjectLevel_MultiRepo_OneTask(t *testing.T) {
	proj, repos := seedBrainstormProject(t, "bs-proj-one", []string{"o/alpha", "o/beta"}, 5)
	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/alpha": {},
			"o/beta":  {},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	act := tatarav1alpha1.BrainstormActivity{Enabled: true, MaxOpenProposals: 5}
	r.brainstorm(context.Background(), proj, reader, repos, nil, act)

	qes := listBrainstormQEs(t, "bs-proj-one")
	if len(qes) != 1 {
		t.Fatalf("want exactly 1 brainstorm QE for 2-repo project, got %d", len(qes))
	}
}

// TestBrainstorm_ProjectLevel_InFlight_AnyRepo_Blocks: a non-terminal
// brainstorm Task for ANY repo in the project blocks a new one.
func TestBrainstorm_ProjectLevel_InFlight_AnyRepo_Blocks(t *testing.T) {
	proj, repos := seedBrainstormProject(t, "bs-proj-inflight", []string{"o/x", "o/y"}, 5)

	// Pre-create an in-flight brainstorm Task for repo x (not y).
	pre := &tatarav1alpha1.Task{}
	pre.GenerateName = "brainstorm-"
	pre.Namespace = testNS
	pre.Labels = map[string]string{labelActivity: "brainstorm"}
	pre.Spec = tatarav1alpha1.TaskSpec{
		ProjectRef:    "bs-proj-inflight",
		RepositoryRef: repos[0].Name, // o/x
		Goal:          "g",
		Kind:          "brainstorm",
	}
	if err := k8sClient.Create(context.Background(), pre); err != nil {
		t.Fatalf("pre-create: %v", err)
	}
	pre.Status.Phase = "Planning"
	_ = k8sClient.Status().Update(context.Background(), pre)

	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/x": {},
			"o/y": {},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	existing := []tatarav1alpha1.Task{*pre}
	act := tatarav1alpha1.BrainstormActivity{Enabled: true, MaxOpenProposals: 5}
	r.brainstorm(context.Background(), proj, reader, repos, existing, act)

	tasks := listBrainstormTasks(t, "bs-proj-inflight")
	// Only the pre-existing Task; no new QE created because ANY inflight Task blocks.
	if len(tasks) != 1 {
		t.Fatalf("want 1 task (pre-existing only; project-level in-flight guard), got %d", len(tasks))
	}
	qes := listBrainstormQEs(t, "bs-proj-inflight")
	if len(qes) != 0 {
		t.Fatalf("want 0 new QEs (in-flight guard), got %d", len(qes))
	}
}

// TestBrainstorm_ProjectLevel_SummedBacklog_AtCap_Skips: ideas are spread
// across repos but their sum >= maxOpenProposals -> no new task.
func TestBrainstorm_ProjectLevel_SummedBacklog_AtCap_Skips(t *testing.T) {
	// maxOpenProposals=5; spread 3+2 ideas across two repos = 5 total -> skip.
	proj, repos := seedBrainstormProject(t, "bs-proj-sumcap", []string{"o/m", "o/n"}, 5)
	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/m": {
				{Repo: "o/m", Number: 1, Labels: []string{"tatara-idea"}},
				{Repo: "o/m", Number: 2, Labels: []string{"tatara-idea"}},
				{Repo: "o/m", Number: 3, Labels: []string{"tatara-idea"}},
			},
			"o/n": {
				{Repo: "o/n", Number: 4, Labels: []string{"tatara-idea"}},
				{Repo: "o/n", Number: 5, Labels: []string{"tatara-idea"}},
			},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	act := tatarav1alpha1.BrainstormActivity{Enabled: true, MaxOpenProposals: 5}
	r.brainstorm(context.Background(), proj, reader, repos, nil, act)

	qes := listBrainstormQEs(t, "bs-proj-sumcap")
	if len(qes) != 0 {
		t.Fatalf("want 0 brainstorm QEs (summed backlog >= maxOpenProposals), got %d", len(qes))
	}
}

// TestBrainstorm_ProjectLevel_SummedBacklog_UnderCap_Creates: 3+1 = 4 < 5 -> create 1.
func TestBrainstorm_ProjectLevel_SummedBacklog_UnderCap_Creates(t *testing.T) {
	proj, repos := seedBrainstormProject(t, "bs-proj-undersum", []string{"o/p", "o/q"}, 5)
	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/p": {
				{Repo: "o/p", Number: 1, Labels: []string{"tatara-idea"}},
				{Repo: "o/p", Number: 2, Labels: []string{"tatara-idea"}},
				{Repo: "o/p", Number: 3, Labels: []string{"tatara-idea"}},
			},
			"o/q": {
				{Repo: "o/q", Number: 4, Labels: []string{"tatara-idea"}},
			},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	act := tatarav1alpha1.BrainstormActivity{Enabled: true, MaxOpenProposals: 5}
	r.brainstorm(context.Background(), proj, reader, repos, nil, act)

	qes := listBrainstormQEs(t, "bs-proj-undersum")
	if len(qes) != 1 {
		t.Fatalf("want 1 brainstorm QE (sum=4 < maxOpenProposals=5), got %d", len(qes))
	}
}

// TestBrainstorm_ProjectLevel_DeterministicPrimaryRepo: brainstorm tasks are
// project-scoped (empty RepositoryRef); the goal encodes all repos sorted by
// name for determinism across cycles.
func TestBrainstorm_ProjectLevel_DeterministicPrimaryRepo(t *testing.T) {
	// Seed repos with names that have a non-trivial sort order.
	proj, repos := seedBrainstormProject(t, "bs-proj-det", []string{"o/zzz", "o/aaa", "o/mmm"}, 5)
	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/zzz": {},
			"o/aaa": {},
			"o/mmm": {},
		},
	}

	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	act := tatarav1alpha1.BrainstormActivity{Enabled: true, MaxOpenProposals: 5}
	r.brainstorm(context.Background(), proj, reader, repos, nil, act)

	qes := listBrainstormQEs(t, "bs-proj-det")
	if len(qes) != 1 {
		t.Fatalf("want 1 QE, got %d", len(qes))
	}
	// Project-scoped: no single primary repo pinned.
	if qes[0].Spec.RepositoryRef != "" {
		t.Fatalf("brainstorm QE RepositoryRef = %q, want empty (project-scoped)", qes[0].Spec.RepositoryRef)
	}
	// Goal must mention all three repos.
	for _, slug := range []string{"o/aaa", "o/mmm", "o/zzz"} {
		if !strings.Contains(qes[0].Spec.Payload.Goal, slug) {
			t.Fatalf("goal missing slug %q", slug)
		}
	}
}

// TestBrainstormGoal_ProjectSpanning: the goal must NOT contain a single
// hard-coded repo slug; it must reference all repos and instruct the agent
// to pick the best repo via propose_issue's repo arg.
func TestBrainstormGoal_ProjectSpanning(t *testing.T) {
	slugs := []string{"o/alpha", "o/beta", "o/gamma"}
	g := brainstormGoalProject(slugs, "", "")

	// Must mention all repos.
	for _, slug := range slugs {
		if !strings.Contains(g, slug) {
			t.Fatalf("goal missing slug %q: %s", slug, g)
		}
	}
	// Must still call the deep-research skill.
	if !strings.Contains(g, "tatara-deep-research") {
		t.Fatalf("goal does not reference tatara-deep-research skill: %s", g)
	}
	// Must instruct agent to pass repo arg to propose_issue.
	if !strings.Contains(g, "propose_issue") {
		t.Fatalf("goal does not mention propose_issue: %s", g)
	}
	// Must NOT be scoped to a single repo (old single-slug format).
	// The old format was "for repo <slug>" - new one covers the whole project.
	if strings.Contains(g, "for repo o/alpha") {
		t.Fatalf("goal still uses old single-repo phrasing: %s", g)
	}
}

// TestBrainstorm_ProjectLevel_ShortCircuit_Backlog: backlog summation stops
// early once total >= maxProp (avoids unnecessary SCM calls for remaining repos).
func TestBrainstorm_ProjectLevel_ShortCircuit_Backlog(t *testing.T) {
	// 3 repos; first alone has maxProp=3 ideas -> short-circuit, never query others.
	proj, repos := seedBrainstormProject(t, "bs-proj-sc", []string{"o/sc1", "o/sc2", "o/sc3"}, 3)

	queriedRepos := map[string]int{}
	reader := &countingReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/sc1": {
				{Repo: "o/sc1", Number: 1, Labels: []string{"tatara-idea"}},
				{Repo: "o/sc1", Number: 2, Labels: []string{"tatara-idea"}},
				{Repo: "o/sc1", Number: 3, Labels: []string{"tatara-idea"}},
			},
			"o/sc2": {{Repo: "o/sc2", Number: 4, Labels: []string{"tatara-idea"}}},
			"o/sc3": {},
		},
		queried: queriedRepos,
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	act := tatarav1alpha1.BrainstormActivity{Enabled: true, MaxOpenProposals: 3}
	r.brainstorm(context.Background(), proj, reader, repos, nil, act)

	// sc1 hits cap -> sc2 and sc3 should NOT be queried.
	if queriedRepos["o/sc2"] > 0 || queriedRepos["o/sc3"] > 0 {
		t.Fatalf("short-circuit failed: queried %v after hitting cap on sc1", queriedRepos)
	}
	qes := listBrainstormQEs(t, "bs-proj-sc")
	if len(qes) != 0 {
		t.Fatalf("want 0 QEs (at cap after sc1), got %d", len(qes))
	}
}

// TestBrainstorm_ProjectLevel_EmptyRepositoryRef: brainstorm creates a Task with
// an empty RepositoryRef (project-scoped, no single-repo pin).
func TestBrainstorm_ProjectLevel_EmptyRepositoryRef(t *testing.T) {
	proj, repos := seedBrainstormProject(t, "bs-proj-emptyref", []string{"o/alpha", "o/beta"}, 5)
	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/alpha": {},
			"o/beta":  {},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	act := tatarav1alpha1.BrainstormActivity{Enabled: true, MaxOpenProposals: 5}
	r.brainstorm(context.Background(), proj, reader, repos, nil, act)

	qes := listBrainstormQEs(t, "bs-proj-emptyref")
	if len(qes) != 1 {
		t.Fatalf("want 1 brainstorm QE, got %d", len(qes))
	}
	if qes[0].Spec.RepositoryRef != "" {
		t.Fatalf("brainstorm QE RepositoryRef = %q, want empty (project-scoped)", qes[0].Spec.RepositoryRef)
	}
}

// TestHealthCheck_ProjectLevel_EmptyRepositoryRef: healthCheck creates a Task with
// an empty RepositoryRef (project-scoped).
func TestHealthCheck_ProjectLevel_EmptyRepositoryRef(t *testing.T) {
	proj, repos := seedHealthCheckProject(t, "hc-emptyref", []string{"o/a", "o/b"}, 3)
	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{"o/a": {}, "o/b": {}},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	act := tatarav1alpha1.HealthCheckActivity{Enabled: true, MaxOpenProposals: 3}
	r.healthCheck(context.Background(), proj, reader, repos, nil, act)

	qes := listHealthCheckQEs(t, "hc-emptyref")
	if len(qes) != 1 {
		t.Fatalf("want 1 healthCheck QE, got %d", len(qes))
	}
	if qes[0].Spec.RepositoryRef != "" {
		t.Fatalf("healthCheck QE RepositoryRef = %q, want empty (project-scoped)", qes[0].Spec.RepositoryRef)
	}
}

// TestBrainstorm_ProjectLevel_ProjectScopedPodName: brainstorm QE is project-scoped.
// PodRepo is empty because pod-name is stamped at admit time, not at enqueue time.
func TestBrainstorm_ProjectLevel_ProjectScopedPodName(t *testing.T) {
	proj, repos := seedBrainstormProject(t, "bs-proj-podname", []string{"o/alpha", "o/beta"}, 5)
	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{"o/alpha": {}, "o/beta": {}},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	act := tatarav1alpha1.BrainstormActivity{Enabled: true, MaxOpenProposals: 5}
	r.brainstorm(context.Background(), proj, reader, repos, nil, act)

	qes := listBrainstormQEs(t, "bs-proj-podname")
	if len(qes) != 1 {
		t.Fatalf("want 1 brainstorm QE, got %d", len(qes))
	}
	// Pod-name is NOT stamped at enqueue time; PodRepo must be empty (project-scoped).
	if qes[0].Spec.Payload.PodRepo != "" {
		t.Fatalf("brainstorm QE PodRepo = %q, want empty (project-scoped, stamped at admit)", qes[0].Spec.Payload.PodRepo)
	}
}

// TestBrainstormDefaultProposalCapIsTen: MaxOpenProposals=0 (unset) should
// default to 10. 9 open proposals must NOT cap-skip; 10 must cap-skip.
func TestBrainstormDefaultProposalCapIsTen(t *testing.T) {
	tests := []struct {
		name      string
		projName  string
		openCount int
		wantSkip  bool
	}{
		{"nine_under_default_cap", "bs-defcap-9", 9, false},
		{"ten_at_default_cap", "bs-defcap-10", 10, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			proj, repos := seedBrainstormProject(t, tc.projName, []string{"o/r1"}, 0)

			var issues []scm.IssueRef
			for i := 1; i <= tc.openCount; i++ {
				issues = append(issues, scm.IssueRef{Repo: "o/r1", Number: i, Labels: []string{"tatara-idea"}})
			}
			reader := &perRepoFakeReader{issuesByRepo: map[string][]scm.IssueRef{"o/r1": issues}}
			r := newScanReconciler(reader)
			r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

			// MaxOpenProposals=0 -> falls back to default.
			act := tatarav1alpha1.BrainstormActivity{Enabled: true, MaxOpenProposals: 0}
			r.brainstorm(context.Background(), proj, reader, repos, nil, act)

			qes := listBrainstormQEs(t, tc.projName)
			if tc.wantSkip && len(qes) != 0 {
				t.Fatalf("want cap-skip (0 QEs), got %d", len(qes))
			}
			if !tc.wantSkip && len(qes) != 1 {
				t.Fatalf("want 1 QE (under cap), got %d", len(qes))
			}
		})
	}
}

// countingReader wraps perRepoFakeReader and records which repos were queried.
type countingReader struct {
	issuesByRepo map[string][]scm.IssueRef
	queried      map[string]int
	fakeReader
}

func (c *countingReader) ListOpenIssues(_ context.Context, owner, repo string) ([]scm.IssueRef, error) {
	slug := owner + "/" + repo
	c.queried[slug]++
	if iss, ok := c.issuesByRepo[slug]; ok {
		return iss, nil
	}
	return nil, nil
}
