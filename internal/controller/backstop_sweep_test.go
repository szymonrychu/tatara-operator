package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// seedBackstopSweepProject creates a Project + Repository for sweep tests.
func seedBackstopSweepProject(t *testing.T, name string) (*tatarav1alpha1.Project, tatarav1alpha1.Repository) {
	t.Helper()
	cron := &tatarav1alpha1.ScmCron{
		IssueScan: tatarav1alpha1.CronActivity{Schedule: "* * * * *", MaxPerRepo: 5},
	}
	proj, repoPtr := seedScanProject(t, name, cron)
	return proj, *repoPtr
}

// makeStrandedTask creates a Task in k8s with a work-item ledger describing a
// stalled MRCI scenario (open-MR + open-source-issue, no pod).
func makeStrandedTask(t *testing.T, projName, repoName string, prNumber, issueNumber int) *tatarav1alpha1.Task {
	t.Helper()
	ctx := context.Background()
	task := &tatarav1alpha1.Task{}
	task.GenerateName = "sweep-"
	task.Namespace = testNS
	task.Labels = map[string]string{
		labelSourceKind: "issueLifecycle",
		labelActivity:   "mrScan",
	}
	task.Spec = tatarav1alpha1.TaskSpec{
		ProjectRef:    projName,
		RepositoryRef: repoName,
		Goal:          "fix issue o/r#" + itoa(issueNumber),
		Kind:          "issueLifecycle",
		Source: &tatarav1alpha1.TaskSource{
			Provider: "github",
			IssueRef: "o/r#" + itoa(prNumber),
			Number:   prNumber,
			IsPR:     true,
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create stranded task: %v", err)
	}
	// Set ledger via status update.
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: issueNumber, Kind: tatarav1alpha1.WorkItemIssue,
			Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
		{Provider: "github", Repo: "o/r", Number: prNumber, Kind: tatarav1alpha1.WorkItemPR,
			Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadSHA: "sha1"},
	}
	task.Status.PodName = ""
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("status update stranded task: %v", err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), task) })
	return task
}

// itoa converts int to string for task naming.
func itoa(n int) string {
	return fmt.Sprintf("%d", n)
}

// TestBackstopSweep_ReactivatesStrandedTask: a project with a stranded open-MR
// Task (no pod, only 1 prior terminal attempt) -> sweep creates exactly one
// reactivation MRCI QueuedEvent (not a second task).
func TestBackstopSweep_ReactivatesStrandedTask(t *testing.T) {
	proj, repo := seedBackstopSweepProject(t, "sweep-reactivate")

	// Stranded task: open PR #50, open issue #7, no pod.
	task := makeStrandedTask(t, "sweep-reactivate", repo.Name, 50, 7)

	// One prior terminal task (under maxRecoveryAttempts=3) to prove the bound is not hit.
	priorTask := &tatarav1alpha1.Task{}
	priorTask.GenerateName = "prior-"
	priorTask.Namespace = testNS
	priorTask.Labels = map[string]string{labelSourceKind: "issueLifecycle", labelActivity: "mrScan"}
	priorTask.Spec = tatarav1alpha1.TaskSpec{
		ProjectRef:    "sweep-reactivate",
		RepositoryRef: repo.Name,
		Goal:          "prior attempt",
		Kind:          "issueLifecycle",
		Source:        &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#50", Number: 50, IsPR: true},
	}
	require.NoError(t, k8sClient.Create(context.Background(), priorTask))
	priorTask.Status.LifecycleState = "Parked"
	require.NoError(t, k8sClient.Status().Update(context.Background(), priorTask))
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), priorTask) })

	// SCM reader: issue #7 still open, PR #50 still open.
	reader := &backstopFakeReader{
		openIssues: map[string][]scm.IssueRef{"o/r": {{Repo: "o/r", Number: 7}}},
		openPRs:    map[string][]scm.PRRef{"o/r": {{Repo: "o/r", Number: 50, HeadSHA: "sha1"}}},
	}

	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	repos := []tatarav1alpha1.Repository{repo}
	_ = task // created above; the sweep lists tasks by project ref
	r.backstopSweep(context.Background(), proj, reader, repos)

	// Must create exactly one MRCI QueuedEvent for the stranded PR.
	qes := listScanQEs(t, "sweep-reactivate")
	require.Len(t, qes, 1, "want 1 reactivation QE for stranded open-MR task")
	ann := qes[0].Spec.Payload.Annotations[tatarav1alpha1.LifecycleEntryAnnotation]
	require.Equal(t, "MRCI", ann, "QE lifecycle entry must be MRCI")
}

// TestBackstopSweep_ExhaustedClosePR: 3 prior terminal attempts for the same bot PR
// -> sweep closes the PR and creates no new task.
func TestBackstopSweep_ExhaustedClosePR(t *testing.T) {
	proj, repo := seedBackstopSweepProject(t, "sweep-exhausted")
	fw := &fullFakeSCMWriter{}

	// Stranded task: open PR #51, open issue #8, no pod.
	task := makeStrandedTask(t, "sweep-exhausted", repo.Name, 51, 8)

	// 3 prior terminal tasks (hits maxRecoveryAttempts=3).
	for i := range 3 {
		pt := &tatarav1alpha1.Task{}
		pt.GenerateName = "prior-"
		pt.Namespace = testNS
		pt.Labels = map[string]string{labelSourceKind: "issueLifecycle", labelActivity: "mrScan"}
		pt.Spec = tatarav1alpha1.TaskSpec{
			ProjectRef:    "sweep-exhausted",
			RepositoryRef: repo.Name,
			Goal:          "prior " + itoa(i),
			Kind:          "issueLifecycle",
			Source:        &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#51", Number: 51, IsPR: true},
		}
		require.NoError(t, k8sClient.Create(context.Background(), pt))
		pt.Status.LifecycleState = "Parked"
		require.NoError(t, k8sClient.Status().Update(context.Background(), pt))
		t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), pt) })
	}

	reader := &backstopFakeReader{
		openIssues: map[string][]scm.IssueRef{"o/r": {{Repo: "o/r", Number: 8}}},
		openPRs:    map[string][]scm.PRRef{"o/r": {{Repo: "o/r", Number: 51, HeadSHA: "sha1"}}},
	}

	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	repos := []tatarav1alpha1.Repository{repo}
	_ = task
	r.backstopSweep(context.Background(), proj, reader, repos)

	// ClosePR must have been called.
	require.True(t, fw.closePRCalled, "expected ClosePR for exhausted recovery")
	require.Equal(t, 51, fw.closePRNumber)

	// No new QE (close path, not create path).
	qes := listScanQEs(t, "sweep-exhausted")
	require.Empty(t, qes, "no QE expected when recovery is exhausted")
}

// TestBackstopSweep_CloseObsoleteTask: all source/closes issues are closed
// (Tier-1 refreshed them) and an open MR remains -> sweep closes the MR, no new task.
func TestBackstopSweep_CloseObsoleteTask(t *testing.T) {
	proj, repo := seedBackstopSweepProject(t, "sweep-obsolete")
	fw := &fullFakeSCMWriter{}

	// Task with open PR #52 but source issue #9 already closed in ledger.
	task := &tatarav1alpha1.Task{}
	task.GenerateName = "sweep-"
	task.Namespace = testNS
	task.Labels = map[string]string{labelSourceKind: "issueLifecycle", labelActivity: "mrScan"}
	task.Spec = tatarav1alpha1.TaskSpec{
		ProjectRef:    "sweep-obsolete",
		RepositoryRef: repo.Name,
		Goal:          "fix issue",
		Kind:          "issueLifecycle",
		Source:        &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#52", Number: 52, IsPR: true},
	}
	require.NoError(t, k8sClient.Create(context.Background(), task))
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 9, Kind: tatarav1alpha1.WorkItemIssue,
			Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIClosed}, // already closed
		{Provider: "github", Repo: "o/r", Number: 52, Kind: tatarav1alpha1.WorkItemPR,
			Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadSHA: "sha2"},
	}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), task) })

	// SCM reader: issue #9 not in open list (closed), PR #52 still open.
	reader := &backstopFakeReader{
		openIssues: map[string][]scm.IssueRef{"o/r": {}},
		openPRs:    map[string][]scm.PRRef{"o/r": {{Repo: "o/r", Number: 52, HeadSHA: "sha2"}}},
	}

	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	repos := []tatarav1alpha1.Repository{repo}
	r.backstopSweep(context.Background(), proj, reader, repos)

	// ClosePR must have been called.
	require.True(t, fw.closePRCalled, "expected ClosePR for obsolete MR")
	require.Equal(t, 52, fw.closePRNumber)

	// No new QE created.
	qes := listScanQEs(t, "sweep-obsolete")
	require.Empty(t, qes, "no QE expected for close-obsolete action")
}

// TestBackstopSweep_Tier1DriftNoTask: Tier-1 drift only (issue state changes in
// ledger) without any open MR -> NO task created. Ledger must NOT create tasks
// from pure state refresh.
func TestBackstopSweep_Tier1DriftNoTask(t *testing.T) {
	proj, repo := seedBackstopSweepProject(t, "sweep-drift")

	// Task with open issue #10, no PR at all.
	task := &tatarav1alpha1.Task{}
	task.GenerateName = "sweep-"
	task.Namespace = testNS
	task.Labels = map[string]string{labelSourceKind: "issueLifecycle", labelActivity: "issueScan"}
	task.Spec = tatarav1alpha1.TaskSpec{
		ProjectRef:    "sweep-drift",
		RepositoryRef: repo.Name,
		Goal:          "triage issue",
		Kind:          "issueLifecycle",
		Source:        &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#10", Number: 10, IsPR: false},
	}
	require.NoError(t, k8sClient.Create(context.Background(), task))
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 10, Kind: tatarav1alpha1.WorkItemIssue,
			Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
	}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), task) })

	// SCM reader: issue #10 is now closed (Tier-1 drift).
	reader := &backstopFakeReader{
		openIssues: map[string][]scm.IssueRef{"o/r": {}}, // issue #10 closed
		openPRs:    map[string][]scm.PRRef{},
	}

	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	repos := []tatarav1alpha1.Repository{repo}
	r.backstopSweep(context.Background(), proj, reader, repos)

	// No new QE must be created.
	qes := listScanQEs(t, "sweep-drift")
	require.Empty(t, qes, "Tier-1-only drift must NOT create tasks")
}

// TestRunScans_BackstopSweepFiredAfterIssueScan verifies that backstopSweep is
// wired into runScans and fires when issueScan is due. A stranded open-MR Task
// with a live issue -> sweep creates an MRCI reactivation QE.
func TestRunScans_BackstopSweepFiredAfterIssueScan(t *testing.T) {
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "* * * * *", MaxPerRepo: 5}}
	proj, repoPtr := seedScanProject(t, "sweep-wired", cron)
	past := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	proj.Status.LastIssueScan = &past
	require.NoError(t, k8sClient.Status().Update(context.Background(), proj))

	// Stranded task: open PR #60, open issue #11, no pod, no prior terminal tasks.
	strandedTask := &tatarav1alpha1.Task{}
	strandedTask.GenerateName = "stranded-"
	strandedTask.Namespace = testNS
	strandedTask.Labels = map[string]string{labelSourceKind: "issueLifecycle", labelActivity: "mrScan"}
	strandedTask.Spec = tatarav1alpha1.TaskSpec{
		ProjectRef:    "sweep-wired",
		RepositoryRef: repoPtr.Name,
		Goal:          "fix issue",
		Kind:          "issueLifecycle",
		Source:        &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#60", Number: 60, IsPR: true},
	}
	require.NoError(t, k8sClient.Create(context.Background(), strandedTask))
	strandedTask.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 11, Kind: tatarav1alpha1.WorkItemIssue,
			Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
		{Provider: "github", Repo: "o/r", Number: 60, Kind: tatarav1alpha1.WorkItemPR,
			Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadSHA: "sha3"},
	}
	require.NoError(t, k8sClient.Status().Update(context.Background(), strandedTask))
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), strandedTask) })

	reader := &backstopFakeReader{
		openIssues: map[string][]scm.IssueRef{"o/r": {{Repo: "o/r", Number: 11}}},
		openPRs:    map[string][]scm.PRRef{"o/r": {{Repo: "o/r", Number: 60, HeadSHA: "sha3"}}},
	}

	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	_, err := r.runScans(context.Background(), proj)
	require.NoError(t, err)

	// backstopSweep must have created an MRCI QE for the stranded task.
	qes := listScanQEs(t, "sweep-wired")
	require.Len(t, qes, 1, "want 1 MRCI QE from backstopSweep wired into runScans")
	ann := qes[0].Spec.Payload.Annotations[tatarav1alpha1.LifecycleEntryAnnotation]
	require.Equal(t, "MRCI", ann)
}
