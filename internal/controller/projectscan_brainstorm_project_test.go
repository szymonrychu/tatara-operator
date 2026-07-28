package controller

// Project-level brainstorm tests: one brainstorm Task per project per cycle,
// not one per repo, and project-scoped (no repo pinned).
//
// The former SCM-label backlog cases (summed-backlog at/under cap, the
// short-circuit that stopped listing issues once the cap was hit, and the
// default-cap-of-ten case) are gone with the cap itself. The backlog level now
// comes from Issue CRs and the refill decision is the control law's, so their
// replacements live in projectscan_brainstorm_target_test.go.

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
	proj, repos := seedBrainstormProject(t, "bs-proj-one", []string{"o/alpha", "o/beta"}, ptrInt(5))
	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/alpha": {},
			"o/beta":  {},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	r.brainstorm(context.Background(), proj, reader, repos, nil, proj.Spec.Scm.Cron.Brainstorm)

	qes := listBrainstormQEs(t, "bs-proj-one")
	if len(qes) != 1 {
		t.Fatalf("want exactly 1 brainstorm QE for 2-repo project, got %d", len(qes))
	}
}

// TestBrainstorm_ProjectLevel_InFlight_AnyRepo_Blocks: a non-terminal
// brainstorm Task for ANY repo in the project blocks a new one.
func TestBrainstorm_ProjectLevel_InFlight_AnyRepo_Blocks(t *testing.T) {
	proj, repos := seedBrainstormProject(t, "bs-proj-inflight", []string{"o/x", "o/y"}, ptrInt(1))

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
	pre.Status.Stage = tatarav1alpha1.StageBrainstorming
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
	r.brainstorm(context.Background(), proj, reader, repos, existing, proj.Spec.Scm.Cron.Brainstorm)

	tasks := listBrainstormTasks(t, "bs-proj-inflight")
	// Only the pre-existing Task; no new QE created because the in-flight Task
	// consumes the only slot of a target-1 project.
	if len(tasks) != 1 {
		t.Fatalf("want 1 task (pre-existing only; project-level in-flight guard), got %d", len(tasks))
	}
	qes := listBrainstormQEs(t, "bs-proj-inflight")
	if len(qes) != 0 {
		t.Fatalf("want 0 new QEs (in-flight guard), got %d", len(qes))
	}
}

// TestBrainstorm_ProjectLevel_DeterministicPrimaryRepo: brainstorm tasks are
// project-scoped (empty RepositoryRef); the goal encodes all repos sorted by
// name for determinism across cycles.
func TestBrainstorm_ProjectLevel_DeterministicPrimaryRepo(t *testing.T) {
	// Seed repos with names that have a non-trivial sort order.
	proj, repos := seedBrainstormProject(t, "bs-proj-det", []string{"o/zzz", "o/aaa", "o/mmm"}, ptrInt(5))
	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/zzz": {},
			"o/aaa": {},
			"o/mmm": {},
		},
	}

	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	r.brainstorm(context.Background(), proj, reader, repos, nil, proj.Spec.Scm.Cron.Brainstorm)

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
// hard-coded repo slug; it must reference all repos and instruct the agent to
// pick the best repo via each proposal's repo field on submit_outcome.
func TestBrainstormGoal_ProjectSpanning(t *testing.T) {
	slugs := []string{"o/alpha", "o/beta", "o/gamma"}
	g := brainstormGoalProject(slugs, "", "", 3)

	// Must mention all repos.
	for _, slug := range slugs {
		if !strings.Contains(g, slug) {
			t.Fatalf("goal missing slug %q: %s", slug, g)
		}
	}
	// Must still reference the code-quality skill.
	if !strings.Contains(g, "tatara-code-quality-proposal") {
		t.Fatalf("goal does not reference tatara-code-quality-proposal skill: %s", g)
	}
	// Must instruct the agent to set each proposal's repo on submit_outcome.
	if !strings.Contains(g, "action=propose") {
		t.Fatalf("goal does not mention submit_outcome(action=propose): %s", g)
	}
	if !strings.Contains(g, "proposal's `repo`") {
		t.Fatalf("goal does not tell the agent to set each proposal's repo: %s", g)
	}
	// Must NOT be scoped to a single repo (old single-slug format).
	// The old format was "for repo <slug>" - new one covers the whole project.
	if strings.Contains(g, "for repo o/alpha") {
		t.Fatalf("goal still uses old single-repo phrasing: %s", g)
	}
}

// TestBrainstorm_ProjectLevel_EmptyRepositoryRef: brainstorm creates a Task with
// an empty RepositoryRef (project-scoped, no single-repo pin).
func TestBrainstorm_ProjectLevel_EmptyRepositoryRef(t *testing.T) {
	proj, repos := seedBrainstormProject(t, "bs-proj-emptyref", []string{"o/alpha", "o/beta"}, ptrInt(5))
	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{
			"o/alpha": {},
			"o/beta":  {},
		},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	r.brainstorm(context.Background(), proj, reader, repos, nil, proj.Spec.Scm.Cron.Brainstorm)

	qes := listBrainstormQEs(t, "bs-proj-emptyref")
	if len(qes) != 1 {
		t.Fatalf("want 1 brainstorm QE, got %d", len(qes))
	}
	if qes[0].Spec.RepositoryRef != "" {
		t.Fatalf("brainstorm QE RepositoryRef = %q, want empty (project-scoped)", qes[0].Spec.RepositoryRef)
	}
}

// TestBrainstorm_ProjectLevel_ProjectScopedPodName: brainstorm QE is project-scoped.
// PodRepo is empty because pod-name is stamped at admit time, not at enqueue time.
func TestBrainstorm_ProjectLevel_ProjectScopedPodName(t *testing.T) {
	proj, repos := seedBrainstormProject(t, "bs-proj-podname", []string{"o/alpha", "o/beta"}, ptrInt(5))
	reader := &perRepoFakeReader{
		issuesByRepo: map[string][]scm.IssueRef{"o/alpha": {}, "o/beta": {}},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	r.brainstorm(context.Background(), proj, reader, repos, nil, proj.Spec.Scm.Cron.Brainstorm)

	qes := listBrainstormQEs(t, "bs-proj-podname")
	if len(qes) != 1 {
		t.Fatalf("want 1 brainstorm QE, got %d", len(qes))
	}
	// Pod-name is NOT stamped at enqueue time; PodRepo must be empty (project-scoped).
	if qes[0].Spec.Payload.PodRepo != "" {
		t.Fatalf("brainstorm QE PodRepo = %q, want empty (project-scoped, stamped at admit)", qes[0].Spec.Payload.PodRepo)
	}
}

// TestBrainstormUnsetTargetFallsBackToTheDeprecatedAlias: an unmigrated Project
// leaves TargetOpenProposals unset, and the CRD defaults MaxOpenProposals to 5,
// so ResolveTarget yields 5 - the documented default. It is emphatically NOT the
// unreachable 10 fallback the old cycle carried (plan conflict C7), which this
// task deleted: at 5 pending there is no refill, where a 10 ceiling would give
// a deficit of 5.
func TestBrainstormUnsetTargetFallsBackToTheDeprecatedAlias(t *testing.T) {
	tests := []struct {
		name      string
		projName  string
		pending   int
		wantQuota string
		wantQE    int
	}{
		{"one_below_the_alias_default", "bs-defcap-4", 4, "1", 1},
		{"at_the_alias_default", "bs-defcap-5", 5, "", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			proj, repos := seedBrainstormProject(t, tc.projName, []string{"o/r1"}, nil)
			reader := &perRepoFakeReader{issuesByRepo: map[string][]scm.IssueRef{"o/r1": nil}}
			r := newScanReconciler(reader)
			r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
			for i := 1; i <= tc.pending; i++ {
				seedProposalIssue(t, r, proj, tc.projName+"-r1", i, "brainstorm", "open", "new")
			}

			r.brainstorm(context.Background(), proj, reader, repos, nil, proj.Spec.Scm.Cron.Brainstorm)

			qes := listBrainstormQEs(t, tc.projName)
			if len(qes) != tc.wantQE {
				t.Fatalf("want %d QE, got %d", tc.wantQE, len(qes))
			}
			if tc.wantQE == 1 {
				if got := qes[0].Spec.Payload.Annotations[tatarav1alpha1.AnnBrainstormQuota]; got != tc.wantQuota {
					t.Fatalf("quota annotation = %q, want %q", got, tc.wantQuota)
				}
			}
		})
	}
}
