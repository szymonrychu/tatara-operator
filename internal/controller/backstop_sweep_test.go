package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	// Production-realistic: a stranded task that already opened an MR has a
	// non-empty PodName (stamped once, never cleared on park) and is itself Parked
	// (terminal lifecycle). The pod object does NOT exist (long gone). The sweep
	// must treat this as not-live via an actual pod Get, not short-circuit on
	// PodName presence, and must EXCLUDE itself from priorTerminalAttempts.
	task.Status.PodName = agent.PodName(task)
	task.Status.DeployState = "Parked"
	task.Status.ParkReason = "BootCrashLoop"
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
	priorTask.Status.DeployState = "Parked"
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

// TestBackstopSweep_NewModelRoutesToReview is the U-E regression: a stalled
// NEW-MODEL task (Kind=implement umbrella, not legacy issueLifecycle) with an open
// bot PR routes backstop reactivation through the discrete `review` kind (carrying
// the PR head branch as AnnReviewHeadBranch), NOT the issueLifecycle bridge.
func TestBackstopSweep_NewModelRoutesToReview(t *testing.T) {
	proj, repo := seedBackstopSweepProject(t, "sweep-newmodel")
	ctx := context.Background()

	task := &tatarav1alpha1.Task{}
	task.GenerateName = "sweep-nm-"
	task.Namespace = testNS
	task.Labels = map[string]string{labelSourceKind: "implement", labelActivity: "issueScan"}
	task.Spec = tatarav1alpha1.TaskSpec{
		ProjectRef:    "sweep-newmodel",
		RepositoryRef: "", // umbrella
		Goal:          "cross-repo change",
		Kind:          "implement",
		Source:        &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#70", Number: 70},
	}
	require.NoError(t, k8sClient.Create(ctx, task))
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 70, Kind: tatarav1alpha1.WorkItemIssue,
			Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
		{Provider: "github", Repo: "o/r", Number: 71, Kind: tatarav1alpha1.WorkItemPR,
			Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen, HeadSHA: "sha1", HeadBranch: "tatara/task-nm"},
	}
	task.Status.PodName = agent.PodName(task)
	task.Status.DeployState = "Parked"
	require.NoError(t, k8sClient.Status().Update(ctx, task))
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), task) })

	reader := &backstopFakeReader{
		openIssues: map[string][]scm.IssueRef{"o/r": {{Repo: "o/r", Number: 70}}},
		openPRs:    map[string][]scm.PRRef{"o/r": {{Repo: "o/r", Number: 71, HeadSHA: "sha1"}}},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
	r.backstopSweep(ctx, proj, reader, []tatarav1alpha1.Repository{repo})

	qes := listScanQEs(t, "sweep-newmodel")
	require.Len(t, qes, 1, "want 1 recovery QE for stranded new-model task")
	require.Equal(t, "review", qes[0].Spec.Payload.Kind, "new-model recovery must route to review, not issueLifecycle")
	require.Equal(t, "tatara/task-nm", qes[0].Spec.Payload.Annotations[tatarav1alpha1.AnnReviewHeadBranch],
		"review recovery must carry the PR head branch")
	require.NotEqual(t, "MRCI", qes[0].Spec.Payload.Annotations[tatarav1alpha1.LifecycleEntryAnnotation],
		"new-model recovery must NOT use the issueLifecycle MRCI bridge")
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
		pt.Status.DeployState = "Parked"
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

	// ClosePR must have been called with a superseded note (NOT the exhausted-
	// recovery "after N attempts" body), and the recovery-exhausted label must NOT
	// be stamped on an obsoleted PR.
	require.True(t, fw.closePRCalled, "expected ClosePR for obsolete MR")
	require.Equal(t, 52, fw.closePRNumber)
	require.Contains(t, fw.closePRBody, "superseded", "obsolete close must post a superseded note")
	require.Contains(t, fw.closePRBody, "no longer needed", "obsolete body must explain source issues are resolved")
	require.NotContains(t, fw.closePRBody, "recovery", "obsolete close must NOT use the recovery-exhausted framing")
	require.NotEqual(t, labelRecoveryExhausted, fw.addLabelLabel, "obsolete close must NOT stamp recovery-exhausted label")

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
	strandedTask.Status.PodName = agent.PodName(strandedTask)
	strandedTask.Status.DeployState = "Parked"
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

// TestBackstopSweep_Idempotent: running the sweep twice in a row against the same
// stranded task must NOT create a duplicate QE. The second sweep finds the
// reactivation task already in flight; createScanTask's dedup suppresses the
// duplicate. Duplicate-task creation is the exact false-refusal/duplication class
// this platform has been burned by.
func TestBackstopSweep_Idempotent(t *testing.T) {
	proj, repo := seedBackstopSweepProject(t, "sweep-idem")
	makeStrandedTask(t, "sweep-idem", repo.Name, 53, 12)

	reader := &backstopFakeReader{
		openIssues: map[string][]scm.IssueRef{"o/r": {{Repo: "o/r", Number: 12}}},
		openPRs:    map[string][]scm.PRRef{"o/r": {{Repo: "o/r", Number: 53, HeadSHA: "sha1"}}},
	}

	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
	repos := []tatarav1alpha1.Repository{repo}

	r.backstopSweep(context.Background(), proj, reader, repos)
	require.Len(t, listScanQEs(t, "sweep-idem"), 1, "first sweep creates one QE")

	r.backstopSweep(context.Background(), proj, reader, repos)
	require.Len(t, listScanQEs(t, "sweep-idem"), 1, "second sweep must NOT duplicate the QE")
}

// TestBackstopSweep_SharesDedupKeyWithMrScan: mrScan and backstopSweep run in the
// same tick (runScans order). For a bot MR whose body links a tracker issue
// (issueNum != PRnum), both paths must compute the SAME createScanTask dedup key
// (issueLifecycle\x00<repo>#<issueNum>) so only ONE reactivation QE is created -
// never two agents on one MR. The backstop must key on the linked issue (derived
// from its ledger), not the PR number.
func TestBackstopSweep_SharesDedupKeyWithMrScan(t *testing.T) {
	proj, repo := seedBackstopSweepProject(t, "sweep-xdedup")

	// Stranded bot MR #70 closing issue #13. Ledger carries both. makeStrandedTask
	// seeds the PR ledger HeadSHA="sha1".
	makeStrandedTask(t, "sweep-xdedup", repo.Name, 70, 13)

	// Bot-authored open PR #70 whose body links issue #13 ("Closes #13"). The head
	// SHA has ADVANCED ("sha2" != the stranded task's "sha1") so mrScan's same-head
	// terminal dedup does NOT suppress it and mrScan creates its own MRCI QE keyed
	// on the linked issue #13. The backstop must key on #13 too (not PR #70), else
	// both fire and two QEs (two agents) land on one MR.
	reader := &backstopFakeReader{
		openIssues: map[string][]scm.IssueRef{"o/r": {{Repo: "o/r", Number: 13}}},
		openPRs: map[string][]scm.PRRef{"o/r": {{
			Repo: "o/r", Number: 70, Author: "tatara-bot", HeadSHA: "sha2", Body: "Closes #13",
		}}},
	}

	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
	repos := []tatarav1alpha1.Repository{repo}

	// mrScan first (as runScans orders it), then backstopSweep in the same tick.
	existing, err := r.existingScanTasks(context.Background(), proj)
	require.NoError(t, err)
	r.mrScan(context.Background(), proj, reader, repos, existing, tatarav1alpha1.CronActivity{})
	r.backstopSweep(context.Background(), proj, reader, repos)

	// Exactly ONE reactivation QE for the linked issue, not two.
	qes := listScanQEs(t, "sweep-xdedup")
	require.Len(t, qes, 1, "mrScan + backstop must share a dedup key -> single QE for one MR")
}

// TestBackstopSweep_NoActionOnFetchError: a transient ListOpenPRs error for the
// PR's repo must NOT drive any Tier-2 action. This guards the migration hazard:
// ~1148 lazily-seeded tasks default openedPR to WIOpen, and a single list error
// must not let never-confirmed seed-open state spawn spurious Reactivate/Close.
func TestBackstopSweep_NoActionOnFetchError(t *testing.T) {
	proj, repo := seedBackstopSweepProject(t, "sweep-fetcherr")
	fw := &fullFakeSCMWriter{}
	makeStrandedTask(t, "sweep-fetcherr", repo.Name, 54, 14)

	// Issue list succeeds (issue #14 shows closed) but the PR list ERRORS, so the
	// PR repo is never confirmed. Without the gate this would look like
	// close-obsolete (all source issues closed + open PR) and close a live MR.
	reader := &backstopFakeReader{
		openIssues: map[string][]scm.IssueRef{"o/r": {}},
		prListErr:  map[string]bool{"o/r": true},
	}

	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }
	repos := []tatarav1alpha1.Repository{repo}

	r.backstopSweep(context.Background(), proj, reader, repos)

	require.False(t, fw.closePRCalled, "must NOT close a PR whose repo fetch failed")
	require.Empty(t, listScanQEs(t, "sweep-fetcherr"), "must NOT reactivate on unconfirmed seed-open state")
}

// TestBackstopSweep_Tier1PersistsRefresh: when Tier-1 refresh changes ledger state
// it must be written back to the CR. Flip issue #15 to closed in SCM; after the
// sweep, Re-Get the Task and assert Status.WorkItems[issue].State==WIClosed and
// LastRefreshedAt is set, proving the RetryOnConflict Status().Update round-trips.
func TestBackstopSweep_Tier1PersistsRefresh(t *testing.T) {
	proj, repo := seedBackstopSweepProject(t, "sweep-persist")

	// Issue-only task (no PR): pure Tier-1 drift, no Tier-2 action.
	task := &tatarav1alpha1.Task{}
	task.GenerateName = "sweep-"
	task.Namespace = testNS
	task.Labels = map[string]string{labelSourceKind: "issueLifecycle", labelActivity: "issueScan"}
	task.Spec = tatarav1alpha1.TaskSpec{
		ProjectRef:    "sweep-persist",
		RepositoryRef: repo.Name,
		Goal:          "triage",
		Kind:          "issueLifecycle",
		Source:        &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#15", Number: 15, IsPR: false},
	}
	require.NoError(t, k8sClient.Create(context.Background(), task))
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: 15, Kind: tatarav1alpha1.WorkItemIssue,
			Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
	}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), task) })

	reader := &backstopFakeReader{
		openIssues: map[string][]scm.IssueRef{"o/r": {}}, // issue #15 now closed
		openPRs:    map[string][]scm.PRRef{},
	}

	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
	r.backstopSweep(context.Background(), proj, reader, []tatarav1alpha1.Repository{repo})

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(task), &got))
	require.Len(t, got.Status.WorkItems, 1)
	require.Equal(t, tatarav1alpha1.WIClosed, got.Status.WorkItems[0].State, "refreshed State must persist to the CR")
	require.NotNil(t, got.Status.WorkItems[0].LastRefreshedAt, "LastRefreshedAt must persist to the CR")
}
