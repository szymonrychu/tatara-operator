package controller

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// --- pure helpers ---

func TestPushCDEligible(t *testing.T) {
	cases := []struct {
		name string
		cs   *tatarav1alpha1.ChangeSummary
		want bool
	}{
		{"nil change summary", nil, false},
		{"summary without significance", &tatarav1alpha1.ChangeSummary{PRTitle: "x"}, false},
		{"summary with significance", &tatarav1alpha1.ChangeSummary{Significance: "minor"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := &tatarav1alpha1.Task{Status: tatarav1alpha1.TaskStatus{ChangeSummary: tc.cs}}
			require.Equal(t, tc.want, pushCDEligible(task))
		})
	}
}

func TestDeployBudget(t *testing.T) {
	proj := &tatarav1alpha1.Project{Spec: tatarav1alpha1.ProjectSpec{
		DeployBudgetSeconds: 3300, DeploySingleHopBudgetSeconds: 2100,
	}}
	require.Equal(t, 3300*time.Second, deployBudget(proj, "tatara-cli"), "cli is multi-hop")
	require.Equal(t, 3300*time.Second, deployBudget(proj, "tatara-agent-skills"), "skills is multi-hop")
	require.Equal(t, 2100*time.Second, deployBudget(proj, "tatara-operator"), "operator is single-hop")
	require.Equal(t, 2100*time.Second, deployBudget(proj, "tatara-memory"), "memory is single-hop")

	// Fallbacks when the spec leaves the budgets unset.
	bare := &tatarav1alpha1.Project{}
	require.Equal(t, 3300*time.Second, deployBudget(bare, "tatara-cli"))
	require.Equal(t, 2100*time.Second, deployBudget(bare, "tatara-operator"))
}

func TestPinCarriesVersion(t *testing.T) {
	imgState := "  tag: \"v1.4.0\"\n"
	chartState := "  version: 1.4.0\n" // chart pins drop the v-prefix
	require.True(t, pinCarriesVersion(imgState, "v1.4.0"), "image pin (v-prefixed) matches")
	require.True(t, pinCarriesVersion(chartState, "v1.4.0"), "chart pin (bare) matches the bare form")
	require.False(t, pinCarriesVersion(imgState, "v1.5.0"), "absent version does not match")
	require.False(t, pinCarriesVersion(imgState, ""), "empty version never matches")
}

// TestPoolInflight_ExcludesDeploying covers the lane-exclusion seam: a pod-less
// Deploying Task must NOT consume a pool slot, or it re-creates the lane
// starvation trap.
func TestPoolInflight_ExcludesDeploying(t *testing.T) {
	r := &DispatcherReconciler{}
	deploying := preQueueTask("dep", "Running", "issueLifecycle", "")
	deploying.Status.Phase = tatarav1alpha1.PhaseDeploying
	deploying.Status.LifecycleState = tatarav1alpha1.LifecycleStateDeploying
	tasks := []tatarav1alpha1.Task{
		preQueueTask("live", "Running", "issueLifecycle", ""), // counts
		deploying, // excluded (pod-less)
	}
	got := r.poolInflight(nil, tasks, tatarav1alpha1.QueueClassNormal)
	require.Equal(t, 1, got, "Deploying Task must not count toward the pool")
}

// --- envtest scene + fakes ---

type deployFakeWriter struct {
	scm.SCMWriter
	mu         sync.Mutex
	closeCalls []string // repo|number|comment
}

func (f *deployFakeWriter) CloseIssue(_ context.Context, _, repo string, number int, comment string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeCalls = append(f.closeCalls, fmt.Sprintf("%s|%d|%s", repo, number, comment))
	return nil
}

type deployFakeReader struct {
	scm.SCMReader
	tag      string
	tagFound bool
	run      scm.WorkflowRun
	runFound bool
	files    map[string]string
}

func (f *deployFakeReader) LatestSemverTag(_ context.Context, _, _ string) (string, bool, error) {
	return f.tag, f.tagFound, nil
}

func (f *deployFakeReader) LatestWorkflowRun(_ context.Context, _, _, _, _ string) (scm.WorkflowRun, bool, error) {
	return f.run, f.runFound, nil
}

func (f *deployFakeReader) GetFileContent(_ context.Context, _, _, path, _ string) (string, error) {
	return f.files[path], nil
}

func newDeployReconciler(fw scm.SCMWriter, rd scm.SCMReader) *TaskReconciler {
	return &TaskReconciler{
		Client:           k8sClient,
		Scheme:           k8sClient.Scheme(),
		Metrics:          obs.NewOperatorMetrics(prometheus.NewRegistry()),
		LifecycleMetrics: obs.NewLifecycleMetrics(prometheus.NewRegistry()),
		Session:          newFakeSession(),
		PodConfig:        agent.PodConfig{Namespace: testNS, CallbackURL: "http://op-internal.tatara.svc:8082"},
		SCMFor:           func(string) (scm.SCMWriter, error) { return fw, nil },
		ReaderFor:        func(_, _ string) (scm.SCMReader, error) { return rd, nil },
	}
}

// seedDeployScene creates the secret, project (with deploy budgets), the
// component repo, and the terminal tatara-helmfile repo.
func seedDeployScene(t *testing.T, suffix, compRepoSlug string) *tatarav1alpha1.Project {
	t.Helper()
	ctx := context.Background()
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "dep-scm-" + suffix, Namespace: testNS},
		Data:       map[string][]byte{"token": []byte("pat"), "webhookSecret": []byte("w")},
	}
	require.NoError(t, k8sClient.Create(ctx, sec))

	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "dep-proj-" + suffix, Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef:                 sec.Name,
			TriggerLabel:                 "tatara",
			DeployBudgetSeconds:          3300,
			DeploySingleHopBudgetSeconds: 2100,
			Scm:                          &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "szymonrychu", BotLogin: "tatara-bot"},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, proj))

	comp := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "dep-comp-" + suffix, Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef: proj.Name, URL: "https://github.com/szymonrychu/" + compRepoSlug + ".git",
			DefaultBranch: "main", ReingestSchedule: "0 6 * * *",
		},
	}
	require.NoError(t, k8sClient.Create(ctx, comp))

	hf := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "dep-hf-" + suffix, Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef: proj.Name, URL: "https://github.com/szymonrychu/tatara-helmfile.git",
			DefaultBranch: "main", ReingestSchedule: "0 6 * * *",
		},
	}
	require.NoError(t, k8sClient.Create(ctx, hf))
	return proj
}

func seedDeployingTask(t *testing.T, name, project, compRepo, issueRef string, deadline time.Time, version string) *tatarav1alpha1.Task {
	t.Helper()
	ctx := context.Background()
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: project, RepositoryRef: compRepo, Kind: "issueLifecycle", Goal: "ship it",
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: issueRef, Number: 7},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, task))
	task.Status.Phase = tatarav1alpha1.PhaseDeploying
	task.Status.LifecycleState = tatarav1alpha1.LifecycleStateDeploying
	dl := metav1.NewTime(deadline)
	task.Status.DeployDeadline = &dl
	task.Status.MergeCommitSHA = "abcdef1234567"
	if version != "" {
		task.Status.DeployedVersion = version
		task.Status.DeployArtifact = "comp@" + version
	}
	require.NoError(t, k8sClient.Status().Update(ctx, task))
	return task
}

func deployCtx() context.Context {
	return logf.IntoContext(context.Background(), logf.Log)
}

// TestReconcileDeploying_LearnsVersionAndResolves: a Deploying Task with no
// recorded version learns the cut tag, sees a successful apply carrying it, and
// resolves Done with the issue closed + deployed-version comment.
func TestReconcileDeploying_LearnsVersionAndResolves(t *testing.T) {
	proj := seedDeployScene(t, "resolve", "tatara-operator")
	task := seedDeployingTask(t, "dep-resolve", proj.Name, "dep-comp-resolve", "szymonrychu/tatara-operator#7",
		time.Now().Add(30*time.Minute), "")

	fw := &deployFakeWriter{}
	rd := &deployFakeReader{
		tag: "v1.4.0", tagFound: true,
		run:      scm.WorkflowRun{HeadSHA: "feedface00", Status: "completed", Conclusion: "success", HTMLURL: "https://run/1"},
		runFound: true,
		files:    map[string]string{"helmfile.yaml.gotmpl": "  version: 1.4.0\n", "values/tatara-operator/common.yaml": "  tag: \"v1.4.0\"\n"},
	}
	r := newDeployReconciler(fw, rd)

	_, err := r.reconcileDeploying(deployCtx(), proj, getTask(t, task.Name))
	require.NoError(t, err)

	got := getTask(t, task.Name)
	require.Equal(t, "Done", got.Status.LifecycleState)
	require.Equal(t, "", got.Status.Phase, "Deploying phase cleared on resolve")
	require.True(t, tatarav1alpha1.TaskTerminal(got))
	require.Len(t, fw.closeCalls, 1)
	require.Contains(t, fw.closeCalls[0], "szymonrychu/tatara-operator|7|")
	require.Contains(t, fw.closeCalls[0], "Deployed tatara-operator@v1.4.0")
	require.Contains(t, fw.closeCalls[0], "tatara-helmfile@feedfac")

	entries, err := (&DeployLedger{Client: k8sClient, Namespace: testNS}).List(deployCtx(), proj.Name)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, DeployStateApplied, entries[0].State)
}

// TestReconcileDeploying_DedupSweep: two Deploying Tasks publishing the same
// applied version resolve in a SINGLE reconcile pass (N tasks, one watcher).
func TestReconcileDeploying_DedupSweep(t *testing.T) {
	proj := seedDeployScene(t, "dedup", "tatara-operator")
	t1 := seedDeployingTask(t, "dep-dedup-1", proj.Name, "dep-comp-dedup", "szymonrychu/tatara-operator#11",
		time.Now().Add(30*time.Minute), "v2.0.0")
	t2 := seedDeployingTask(t, "dep-dedup-2", proj.Name, "dep-comp-dedup", "szymonrychu/tatara-operator#12",
		time.Now().Add(30*time.Minute), "v2.0.0")

	ledger := &DeployLedger{Client: k8sClient, Namespace: testNS}
	require.NoError(t, ledger.Add(deployCtx(), proj.Name, DeployLedgerEntry{Artifact: "tatara-operator", Version: "v2.0.0", SourceTaskRef: t1.Name, IssueRef: "szymonrychu/tatara-operator#11", State: DeployStateDeploying}))
	require.NoError(t, ledger.Add(deployCtx(), proj.Name, DeployLedgerEntry{Artifact: "tatara-operator", Version: "v2.0.0", SourceTaskRef: t2.Name, IssueRef: "szymonrychu/tatara-operator#12", State: DeployStateDeploying}))

	fw := &deployFakeWriter{}
	rd := &deployFakeReader{
		tag: "v2.0.0", tagFound: true,
		run:      scm.WorkflowRun{HeadSHA: "applied99", Status: "completed", Conclusion: "success", HTMLURL: "https://run/2"},
		runFound: true,
		files:    map[string]string{"values/tatara-operator/common.yaml": "  tag: \"v2.0.0\"\n"},
	}
	r := newDeployReconciler(fw, rd)

	// Reconcile only ONE task; the sweep must resolve BOTH.
	_, err := r.reconcileDeploying(deployCtx(), proj, getTask(t, t1.Name))
	require.NoError(t, err)

	require.Equal(t, "Done", getTask(t, t1.Name).Status.LifecycleState)
	require.Equal(t, "Done", getTask(t, t2.Name).Status.LifecycleState)
	require.Len(t, fw.closeCalls, 2, "both issues closed in one sweep")
}

// TestReconcileDeploying_SuccessPredatesPin: a successful apply whose state does
// NOT carry this Task's version is ignored (stays Deploying).
func TestReconcileDeploying_SuccessPredatesPin(t *testing.T) {
	proj := seedDeployScene(t, "predate", "tatara-operator")
	task := seedDeployingTask(t, "dep-predate", proj.Name, "dep-comp-predate", "szymonrychu/tatara-operator#7",
		time.Now().Add(30*time.Minute), "v3.0.0")
	fw := &deployFakeWriter{}
	rd := &deployFakeReader{
		tag: "v3.0.0", tagFound: true,
		run:      scm.WorkflowRun{HeadSHA: "old123", Status: "completed", Conclusion: "success"},
		runFound: true,
		files:    map[string]string{"values/tatara-operator/common.yaml": "  tag: \"v2.9.0\"\n"}, // older pin
	}
	r := newDeployReconciler(fw, rd)

	res, err := r.reconcileDeploying(deployCtx(), proj, getTask(t, task.Name))
	require.NoError(t, err)
	require.Equal(t, deployPollRequeue, res.RequeueAfter)
	require.Equal(t, tatarav1alpha1.LifecycleStateDeploying, getTask(t, task.Name).Status.LifecycleState)
	require.Empty(t, fw.closeCalls)
}

// TestReconcileDeploying_TimeoutReroll: a cascade past its budget deadline
// rerolls to Implement, consuming one auto-reroll attempt.
func TestReconcileDeploying_TimeoutReroll(t *testing.T) {
	proj := seedDeployScene(t, "timeout", "tatara-operator")
	task := seedDeployingTask(t, "dep-timeout", proj.Name, "dep-comp-timeout", "szymonrychu/tatara-operator#7",
		time.Now().Add(-time.Minute), "v1.0.0") // deadline already passed
	fw := &deployFakeWriter{}
	r := newDeployReconciler(fw, &deployFakeReader{})

	_, err := r.reconcileDeploying(deployCtx(), proj, getTask(t, task.Name))
	require.NoError(t, err)

	got := getTask(t, task.Name)
	require.Equal(t, "Implement", got.Status.LifecycleState)
	require.Equal(t, "", got.Status.Phase)
	require.Equal(t, 1, got.Status.ImplementGiveUps)
	require.NotEmpty(t, got.Status.ImplementContext)
}

// TestReconcileDeploying_ApplyFailureReroll: a failed apply carrying this Task's
// version rerolls to Implement to fix the cascade.
func TestReconcileDeploying_ApplyFailureReroll(t *testing.T) {
	proj := seedDeployScene(t, "applyfail", "tatara-operator")
	task := seedDeployingTask(t, "dep-applyfail", proj.Name, "dep-comp-applyfail", "szymonrychu/tatara-operator#7",
		time.Now().Add(30*time.Minute), "v1.1.0")
	fw := &deployFakeWriter{}
	rd := &deployFakeReader{
		tag: "v1.1.0", tagFound: true,
		run:      scm.WorkflowRun{HeadSHA: "badsha", Status: "completed", Conclusion: "failure", HTMLURL: "https://run/fail"},
		runFound: true,
		files:    map[string]string{"values/tatara-operator/common.yaml": "  tag: \"v1.1.0\"\n"},
	}
	r := newDeployReconciler(fw, rd)

	_, err := r.reconcileDeploying(deployCtx(), proj, getTask(t, task.Name))
	require.NoError(t, err)

	got := getTask(t, task.Name)
	require.Equal(t, "Implement", got.Status.LifecycleState)
	require.Contains(t, got.Status.ImplementContext, "https://run/fail")
}

// TestCDScan_RerollsStalled: the backstop rerolls a Deploying Task stalled past
// 1.5x its budget.
func TestCDScan_RerollsStalled(t *testing.T) {
	proj := seedDeployScene(t, "cdscan", "tatara-operator")
	// Single-hop budget 2100s; 1.5x threshold = deadline + 1050s. Put the deadline
	// 2000s in the past so we are well past the 1.5x backstop.
	task := seedDeployingTask(t, "dep-cdscan", proj.Name, "dep-comp-cdscan", "szymonrychu/tatara-operator#7",
		time.Now().Add(-2000*time.Second), "v1.0.0")

	pr := &ProjectReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry())}
	pr.cdScan(deployCtx(), proj, []tatarav1alpha1.Task{*getTask(t, task.Name)})

	got := getTask(t, task.Name)
	require.Equal(t, "Implement", got.Status.LifecycleState)
	require.Equal(t, "", got.Status.Phase)
	require.Equal(t, 1, got.Status.ImplementGiveUps)
}

// TestCDScan_SkipsWithinThreshold: a Deploying Task still within 1.5x budget is
// left alone by the backstop.
func TestCDScan_SkipsWithinThreshold(t *testing.T) {
	proj := seedDeployScene(t, "cdscanok", "tatara-operator")
	task := seedDeployingTask(t, "dep-cdscanok", proj.Name, "dep-comp-cdscanok", "szymonrychu/tatara-operator#7",
		time.Now().Add(30*time.Minute), "v1.0.0") // deadline in the future
	pr := &ProjectReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry())}
	pr.cdScan(deployCtx(), proj, []tatarav1alpha1.Task{*getTask(t, task.Name)})

	require.Equal(t, tatarav1alpha1.LifecycleStateDeploying, getTask(t, task.Name).Status.LifecycleState)
}
