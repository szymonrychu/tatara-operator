package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// --- unit tests for helpers ---

func TestTaskOpen_TerminalPhases(t *testing.T) {
	cases := []struct {
		phase     string
		lifecycle string
		want      bool
	}{
		{"Succeeded", "", false},
		{"Failed", "", false},
		{"Running", "", true},
		{"Planning", "", true},
		{"Pending", "", true},
		{"Running", "Triage", true},
		{"Running", "MRCI", true},
		{"Running", "Done", false},
		{"Running", "Stopped", false},
		{"Running", "Parked", false},
		{"Succeeded", "Done", false},
		// Conversation = awaiting-human; must NOT consume creation budget.
		{"Running", "Conversation", false},
		{"Planning", "Conversation", false},
	}
	for _, tc := range cases {
		tk := &tatarav1alpha1.Task{}
		tk.Status.Phase = tc.phase
		tk.Status.LifecycleState = tc.lifecycle
		got := taskOpen(tk)
		if got != tc.want {
			t.Errorf("taskOpen(phase=%q, lifecycle=%q) = %v, want %v", tc.phase, tc.lifecycle, got, tc.want)
		}
	}
}

func TestTaskActive_ExcludesTerminalLifecycle(t *testing.T) {
	cases := []struct {
		phase     string
		lifecycle string
		want      bool
	}{
		{"Running", "", true},
		{"Planning", "", true},
		{"Pending", "", false},
		{"Succeeded", "", false},
		{"Failed", "", false},
		{"Running", "Triage", true},
		// Deadlock cases: an active phase but a TERMINAL lifecycle must NOT
		// occupy a concurrency slot. A Task Parked at maxIterations keeps a
		// stale Planning phase; counting it deadlocks the cap.
		{"Planning", "Parked", false},
		{"Running", "Parked", false},
		{"Running", "Done", false},
		{"Running", "Stopped", false},
	}
	for _, tc := range cases {
		tk := &tatarav1alpha1.Task{}
		tk.Status.Phase = tc.phase
		tk.Status.LifecycleState = tc.lifecycle
		if got := taskActive(tk); got != tc.want {
			t.Errorf("taskActive(phase=%q, lifecycle=%q) = %v, want %v", tc.phase, tc.lifecycle, got, tc.want)
		}
	}
}

func TestOpenTaskCount(t *testing.T) {
	tasks := []tatarav1alpha1.Task{
		{Status: tatarav1alpha1.TaskStatus{Phase: "Running"}},
		{Status: tatarav1alpha1.TaskStatus{Phase: "Succeeded"}},
		{Status: tatarav1alpha1.TaskStatus{Phase: "Failed"}},
		{Status: tatarav1alpha1.TaskStatus{Phase: "Planning", LifecycleState: "Triage"}},
		{Status: tatarav1alpha1.TaskStatus{Phase: "Running", LifecycleState: "Done"}},
	}
	// Running + Planning/Triage open = 2; Succeeded/Failed/Done terminal = 3
	got := openTaskCount(tasks)
	if got != 2 {
		t.Fatalf("openTaskCount = %d, want 2", got)
	}
}

// TestOpenTaskCount_ConversationExcluded verifies that Conversation (awaiting-human)
// issueLifecycle tasks do NOT consume the creation budget.  This is the core of the
// brainstorm-starvation fix: 3 Conversation tasks with MaxOpenTasks=3 must leave
// budget=3 for autonomous creation (brainstorm, issueScan, mrScan).
func TestOpenTaskCount_ConversationExcluded(t *testing.T) {
	tasks := []tatarav1alpha1.Task{
		// Three Conversation tasks - the "all slots pinned" scenario that broke brainstorm.
		{Status: tatarav1alpha1.TaskStatus{Phase: "Running", LifecycleState: "Conversation"}},
		{Status: tatarav1alpha1.TaskStatus{Phase: "Planning", LifecycleState: "Conversation"}},
		{Status: tatarav1alpha1.TaskStatus{Phase: "Pending", LifecycleState: "Conversation"}},
		// One genuinely active task: must still count.
		{Status: tatarav1alpha1.TaskStatus{Phase: "Running", LifecycleState: "Implement"}},
		// Terminal tasks: must not count.
		{Status: tatarav1alpha1.TaskStatus{Phase: "Running", LifecycleState: "Done"}},
		{Status: tatarav1alpha1.TaskStatus{Phase: "Succeeded"}},
	}
	// Only the Implement task should count; Conversation + terminal = excluded.
	got := openTaskCount(tasks)
	if got != 1 {
		t.Fatalf("openTaskCount with 3 Conversation tasks = %d, want 1 (only Implement counts)", got)
	}
}

func TestMaxOpenTasks_Default(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	if n := maxOpenTasks(proj); n != 3 {
		t.Fatalf("maxOpenTasks(0) = %d, want 3 (default)", n)
	}
	proj.Spec.MaxOpenTasks = 5
	if n := maxOpenTasks(proj); n != 5 {
		t.Fatalf("maxOpenTasks(5) = %d, want 5", n)
	}
}

// --- integration tests: issueScan respects budget ---

// TestIssueScan_BudgetZero_CreatesNoTasks: budget=0 -> no tasks created.
func TestIssueScan_BudgetZero_CreatesNoTasks(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 5}}
	proj, _ := seedScanProject(t, "budget-zero-iss", cron)
	reader := &fakeReader{issues: []scm.IssueRef{
		{Repo: "o/r", Number: 1, UpdatedAt: time.Unix(100, 0)},
		{Repo: "o/r", Number: 2, UpdatedAt: time.Unix(200, 0)},
		{Repo: "o/r", Number: 3, UpdatedAt: time.Unix(300, 0)},
	}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	repos := []tatarav1alpha1.Repository{
		{ObjectMeta: metav1.ObjectMeta{Name: "budget-zero-iss-repo", Namespace: testNS},
			Spec: tatarav1alpha1.RepositorySpec{ProjectRef: "budget-zero-iss", URL: "https://github.com/o/r.git", DefaultBranch: "main"}},
	}
	budget := 0
	r.issueScan(context.Background(), proj, reader, repos, nil, cron.IssueScan, &budget)

	tasks := listScanTasks(t, "budget-zero-iss")
	if len(tasks) != 0 {
		t.Fatalf("budget=0: want 0 tasks created, got %d", len(tasks))
	}
}

// TestIssueScan_BudgetTwo_CreatesTwoMax: budget=2 -> at most 2 tasks created.
func TestIssueScan_BudgetTwo_CreatesAtMostTwo(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 5}}
	proj, _ := seedScanProject(t, "budget-two-iss", cron)
	reader := &fakeReader{issues: []scm.IssueRef{
		{Repo: "o/r", Number: 10, UpdatedAt: time.Unix(100, 0)},
		{Repo: "o/r", Number: 11, UpdatedAt: time.Unix(200, 0)},
		{Repo: "o/r", Number: 12, UpdatedAt: time.Unix(300, 0)},
	}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	repos := []tatarav1alpha1.Repository{
		{ObjectMeta: metav1.ObjectMeta{Name: "budget-two-iss-repo", Namespace: testNS},
			Spec: tatarav1alpha1.RepositorySpec{ProjectRef: "budget-two-iss", URL: "https://github.com/o/r.git", DefaultBranch: "main"}},
	}
	budget := 2
	r.issueScan(context.Background(), proj, reader, repos, nil, cron.IssueScan, &budget)

	tasks := listScanTasks(t, "budget-two-iss")
	if len(tasks) > 2 {
		t.Fatalf("budget=2: want at most 2 tasks, got %d", len(tasks))
	}
	if len(tasks) == 0 {
		t.Fatalf("budget=2: want at least 1 task created, got 0")
	}
	if budget != 0 {
		t.Fatalf("budget pointer must be 0 after 2 creations, got %d", budget)
	}
}

// TestRunScans_MaxOpenTasks_Cap: project with MaxOpenTasks=1, 1 open task ->
// runScans must create 0 additional tasks.
//
// Note: MaxOpenTasks is set directly on the in-memory proj pointer after the
// k8s status update, because the CRD schema in the envtest environment is loaded
// from the charts directory which may not yet have the field (pre-manifest-regen).
// runScans uses the passed *Project directly so the in-memory value is authoritative.
func TestRunScans_MaxOpenTasks_Cap(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "* * * * *", MaxPerRepo: 5}}
	proj, repo := seedScanProject(t, "maxopen-cap", cron)

	// Pre-create one open (Running) issueLifecycle task.
	pre := &tatarav1alpha1.Task{}
	pre.GenerateName = "open-"
	pre.Namespace = testNS
	pre.Labels = scanTaskLabels(candidate{repo: "o/r", number: 99}, "issueScan", "issueLifecycle")
	pre.Spec = tatarav1alpha1.TaskSpec{ProjectRef: "maxopen-cap", RepositoryRef: repo.Name, Goal: "g", Kind: "issueLifecycle"}
	if err := k8sClient.Create(context.Background(), pre); err != nil {
		t.Fatalf("pre-create: %v", err)
	}
	pre.Status.Phase = "Running"
	_ = k8sClient.Status().Update(context.Background(), pre)

	// Backdate LastIssueScan so the * * * * * schedule fires immediately.
	past := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	proj.Status.LastIssueScan = &past
	_ = k8sClient.Status().Update(context.Background(), proj)

	// Set MaxOpenTasks in-memory after status update; runScans uses the pointer
	// directly so the field does not need to survive a k8s roundtrip.
	proj.Spec.MaxOpenTasks = 1

	reader := &fakeReader{issues: []scm.IssueRef{
		{Repo: "o/r", Number: 1, UpdatedAt: time.Unix(100, 0)},
		{Repo: "o/r", Number: 2, UpdatedAt: time.Unix(200, 0)},
	}}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	if _, err := r.runScans(context.Background(), proj); err != nil {
		t.Fatalf("runScans: %v", err)
	}

	tasks := listScanTasks(t, "maxopen-cap")
	if len(tasks) != 1 {
		t.Fatalf("MaxOpenTasks=1 with 1 open task: want 0 new tasks (total 1), got %d total", len(tasks))
	}
}
