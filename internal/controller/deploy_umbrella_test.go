package controller

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// Discrete-implement umbrella deploy supervision (task-kind redesign): the deploy
// cascade + issue-auto-close must drive off the Task's role:openedPR WorkItem
// members (N repos, N PRs), not the single Status.PRNumber scalar. The issue
// closes only when EVERY merged member is confirmed applied (confirm-all); a
// stuck (unmerged) member past the merge-wait deadline parks the stream with an
// issue comment naming it.

// --- fakes ---

type umbWriter struct {
	scm.SCMWriter
	mu          sync.Mutex
	merged      map[int]bool
	closed      map[int]bool
	mergeCalled bool
	closeCalls  []string // repoSlug|number|comment
	comments    []string // ref|body
}

func (w *umbWriter) GetPRState(_ context.Context, _, _ string, number int) (scm.PRState, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return scm.PRState{Merged: w.merged[number], Closed: w.closed[number], CIStatus: "success"}, nil
}

func (w *umbWriter) Merge(_ context.Context, _, _ string, _ int, _ string) (string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.mergeCalled = true
	return "mergedsha", nil
}

func (w *umbWriter) CloseIssue(_ context.Context, _, repoSlug string, number int, comment string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closeCalls = append(w.closeCalls, fmt.Sprintf("%s|%d|%s", repoSlug, number, comment))
	return nil
}

func (w *umbWriter) Comment(_ context.Context, _, ref, body string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.comments = append(w.comments, ref+"|"+body)
	return nil
}

type umbReader struct {
	scm.SCMReader
	tags     map[string]string // repo name -> tag
	run      scm.WorkflowRun
	runFound bool
	files    map[string]string
}

func (r *umbReader) LatestSemverTag(_ context.Context, _, repo string) (string, bool, error) {
	v, ok := r.tags[repo]
	return v, ok, nil
}

func (r *umbReader) LatestWorkflowRun(_ context.Context, _, _, _, _ string) (scm.WorkflowRun, bool, error) {
	return r.run, r.runFound, nil
}

func (r *umbReader) GetFileContent(_ context.Context, _, _, path, _ string) (string, error) {
	return r.files[path], nil
}

func newUmbProjectReconciler(w *umbWriter) *ProjectReconciler {
	return &ProjectReconciler{
		Client:    k8sClient,
		Scheme:    k8sClient.Scheme(),
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
		SCMFor:    func(string) (scm.SCMWriter, error) { return w, nil },
		ReaderFor: func(_, _ string) (scm.SCMReader, error) { return &umbReader{}, nil },
	}
}

func newUmbTaskReconciler(w *umbWriter, rd *umbReader) *TaskReconciler {
	return &TaskReconciler{
		Client:           k8sClient,
		Scheme:           k8sClient.Scheme(),
		Metrics:          obs.NewOperatorMetrics(prometheus.NewRegistry()),
		LifecycleMetrics: obs.NewLifecycleMetrics(prometheus.NewRegistry()),
		Session:          newFakeSession(),
		PodConfig:        agent.PodConfig{Namespace: testNS, CallbackURL: "http://op-internal.tatara.svc:8082"},
		SCMFor:           func(string) (scm.SCMWriter, error) { return w, nil },
		ReaderFor:        func(_, _ string) (scm.SCMReader, error) { return rd, nil },
	}
}

// seedUmbrellaScene creates the secret, project (deploy budgets), one Repository
// per component slug, plus the terminal tatara-helmfile repo.
func seedUmbrellaScene(t *testing.T, suffix string, comps ...string) *tatarav1alpha1.Project {
	t.Helper()
	ctx := context.Background()
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "umb-scm-" + suffix, Namespace: testNS},
		Data:       map[string][]byte{"token": []byte("pat"), "webhookSecret": []byte("w")},
	}
	require.NoError(t, k8sClient.Create(ctx, sec))
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "umb-proj-" + suffix, Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef:                 sec.Name,
			TriggerLabel:                 "tatara",
			DeployBudgetSeconds:          3300,
			DeploySingleHopBudgetSeconds: 2100,
			MergeWaitBudgetMinutes:       720,
			Scm:                          &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "szymonrychu", BotLogin: "tatara-bot"},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, proj))
	for i, comp := range comps {
		repo := &tatarav1alpha1.Repository{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("umb-%s-c%d", suffix, i), Namespace: testNS},
			Spec: tatarav1alpha1.RepositorySpec{
				ProjectRef: proj.Name, URL: "https://github.com/szymonrychu/" + comp + ".git",
				DefaultBranch: "main", ReingestSchedule: "0 6 * * *",
			},
		}
		require.NoError(t, k8sClient.Create(ctx, repo))
	}
	hf := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "umb-" + suffix + "-hf", Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef: proj.Name, URL: "https://github.com/szymonrychu/tatara-helmfile.git",
			DefaultBranch: "main", ReingestSchedule: "0 6 * * *",
		},
	}
	require.NoError(t, k8sClient.Create(ctx, hf))
	return proj
}

// seedUmbrellaTask creates a discrete-implement umbrella Task (empty RepositoryRef)
// with a role:openedPR member per (repo-slug, number). NO Status.PRNumber is set:
// the whole point is that supervision drives off the ledger members.
func seedUmbrellaTask(t *testing.T, proj *tatarav1alpha1.Project, name string, members map[string]int) *tatarav1alpha1.Task {
	t.Helper()
	ctx := context.Background()
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: proj.Name, RepositoryRef: "", Kind: "implement",
			Goal:   "Issue #5: cross-repo change",
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "szymonrychu/tatara-operator#5", Number: 5, IsPR: false},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, task))
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), task) })
	task.Status.Phase = tatarav1alpha1.PhaseSucceeded
	task.Status.ChangeSummary = &tatarav1alpha1.ChangeSummary{Significance: "minor"}
	for slug, num := range members {
		task.Status.WorkItems = append(task.Status.WorkItems, tatarav1alpha1.WorkItemRef{
			Provider: "github", Repo: slug, Number: num,
			Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR,
			State: tatarav1alpha1.WIOpen, HeadBranch: "tatara/task-" + name,
		})
	}
	require.NoError(t, k8sClient.Status().Update(ctx, task))
	return task
}

func umbMember(task *tatarav1alpha1.Task, slug string) *tatarav1alpha1.WorkItemRef {
	for i := range task.Status.WorkItems {
		if task.Status.WorkItems[i].Repo == slug {
			return &task.Status.WorkItems[i]
		}
	}
	return nil
}

// --- item 1: superviseMergedPRs reaches Deploying, per-member, no PRNumber ---

func TestUmbrella_SuperviseMergedPRs_EntersDeployingWhenAllMembersMerged(t *testing.T) {
	proj := seedUmbrellaScene(t, "enter", "tatara-operator", "tatara-chat")
	var repos tatarav1alpha1.RepositoryList
	require.NoError(t, k8sClient.List(context.Background(), &repos))
	seedUmbrellaTask(t, proj, "umb-enter", map[string]int{
		"szymonrychu/tatara-operator": 101,
		"szymonrychu/tatara-chat":     102,
	})
	w := &umbWriter{merged: map[int]bool{101: true, 102: true}}
	r := newUmbProjectReconciler(w)

	r.superviseMergedPRs(deployCtx(), proj, filterRepos(repos.Items, proj.Name))

	require.False(t, w.mergeCalled, "sweep must never call Merge (single merge egress preserved)")
	got := getTask(t, "umb-enter")
	require.True(t, tatarav1alpha1.TaskDeploying(got), "umbrella with all members merged must enter Deploying")
	require.NotNil(t, got.Status.DeployDeadline)
	// Both members flipped to merged + seeded deploying.
	require.Equal(t, tatarav1alpha1.WIMerged, umbMember(got, "szymonrychu/tatara-operator").State)
	require.Equal(t, tatarav1alpha1.WIMerged, umbMember(got, "szymonrychu/tatara-chat").State)
}

func TestUmbrella_SuperviseMergedPRs_WaitsWhenAMemberUnmerged(t *testing.T) {
	proj := seedUmbrellaScene(t, "wait", "tatara-operator", "tatara-chat")
	var repos tatarav1alpha1.RepositoryList
	require.NoError(t, k8sClient.List(context.Background(), &repos))
	seedUmbrellaTask(t, proj, "umb-wait", map[string]int{
		"szymonrychu/tatara-operator": 201,
		"szymonrychu/tatara-chat":     202,
	})
	w := &umbWriter{merged: map[int]bool{201: true}} // 202 not merged
	r := newUmbProjectReconciler(w)

	r.superviseMergedPRs(deployCtx(), proj, filterRepos(repos.Items, proj.Name))

	got := getTask(t, "umb-wait")
	require.False(t, tatarav1alpha1.TaskDeploying(got), "must not enter Deploying while a member is unmerged")
	require.NotNil(t, got.Status.MergeWaitDeadline, "merge-wait clock must be stamped on first sight")
	require.NotEqual(t, "Parked", got.Status.DeployState, "still within the merge-wait budget: not parked")
}

// --- item 3: pre-merge deadline parks the stuck stream with an issue comment ---

func TestUmbrella_SuperviseMergedPRs_ParksStuckMemberPastDeadline(t *testing.T) {
	proj := seedUmbrellaScene(t, "stuck", "tatara-operator", "tatara-chat")
	var repos tatarav1alpha1.RepositoryList
	require.NoError(t, k8sClient.List(context.Background(), &repos))
	task := seedUmbrellaTask(t, proj, "umb-stuck", map[string]int{
		"szymonrychu/tatara-operator": 301,
		"szymonrychu/tatara-chat":     302,
	})
	// Merge-wait deadline already elapsed; operator merged, chat stuck unmerged.
	past := metav1.NewTime(time.Now().Add(-time.Minute))
	cur := getTask(t, task.Name)
	cur.Status.MergeWaitDeadline = &past
	require.NoError(t, k8sClient.Status().Update(context.Background(), cur))

	w := &umbWriter{merged: map[int]bool{301: true}} // 302 still open
	r := newUmbProjectReconciler(w)

	r.superviseMergedPRs(deployCtx(), proj, filterRepos(repos.Items, proj.Name))

	got := getTask(t, "umb-stuck")
	require.False(t, tatarav1alpha1.TaskDeploying(got), "a stuck member keeps the stream out of Deploying")
	require.Equal(t, "Parked", got.Status.DeployState)
	require.Equal(t, mergeParkReason, got.Status.ParkReason)
	require.Len(t, w.comments, 1, "the stuck stream must be surfaced with one issue comment")
	require.Contains(t, w.comments[0], "tatara-chat", "the comment must name the stuck member repo")
	require.Empty(t, w.closeCalls, "the originating issue must NOT be closed while a member is stuck")
}

// --- item 2: reconcileDeploying (umbrella) closes only after ALL members applied ---

// seedDeployingUmbrella creates an umbrella Task already in Deploying with both
// members merged (deploy candidates), no version learned yet.
func seedDeployingUmbrella(t *testing.T, proj *tatarav1alpha1.Project, name string, members map[string]int, deadline time.Time) *tatarav1alpha1.Task {
	t.Helper()
	ctx := context.Background()
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: proj.Name, RepositoryRef: "", Kind: "implement", Goal: "ship it",
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "szymonrychu/tatara-operator#9", Number: 9, IsPR: false},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, task))
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), task) })
	task.Status.Phase = tatarav1alpha1.PhaseDeploying
	task.Status.DeployState = tatarav1alpha1.DeployStateDeploying
	dl := metav1.NewTime(deadline)
	task.Status.DeployDeadline = &dl
	task.Status.ChangeSummary = &tatarav1alpha1.ChangeSummary{Significance: "minor"}
	for slug, num := range members {
		task.Status.WorkItems = append(task.Status.WorkItems, tatarav1alpha1.WorkItemRef{
			Provider: "github", Repo: slug, Number: num,
			Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR,
			State: tatarav1alpha1.WIMerged, DeployState: "deploying", HeadBranch: "tatara/task-" + name,
		})
	}
	require.NoError(t, k8sClient.Status().Update(ctx, task))
	return task
}

func TestUmbrella_ReconcileDeploying_StaysOpenUntilAllMembersApplied(t *testing.T) {
	proj := seedUmbrellaScene(t, "partial", "tatara-operator", "tatara-chat")
	task := seedDeployingUmbrella(t, proj, "umb-partial", map[string]int{
		"szymonrychu/tatara-operator": 401,
		"szymonrychu/tatara-chat":     402,
	}, time.Now().Add(30*time.Minute))

	w := &umbWriter{}
	// Apply carries ONLY the operator pin; chat's version is not yet applied.
	rd := &umbReader{
		tags:     map[string]string{"tatara-operator": "v1.0.0", "tatara-chat": "v2.0.0"},
		run:      scm.WorkflowRun{HeadSHA: "sha1", Status: "completed", Conclusion: "success", HTMLURL: "https://run/1"},
		runFound: true,
		files: map[string]string{
			"helmfile.yaml.gotmpl":               "releases:\n  - name: tatara-operator\n    version: 1.0.0\n",
			"values/tatara-operator/common.yaml": "  tag: \"v1.0.0\"\n",
		},
	}
	r := newUmbTaskReconciler(w, rd)

	res, err := r.reconcileDeploying(deployCtx(), proj, getTask(t, task.Name))
	require.NoError(t, err)
	require.Equal(t, deployPollRequeue, res.RequeueAfter)

	got := getTask(t, task.Name)
	require.Equal(t, tatarav1alpha1.DeployStateDeploying, got.Status.DeployState, "not all members applied: issue stays open")
	require.Empty(t, w.closeCalls, "issue must not close while a member is unconfirmed")
	require.Equal(t, "applied", umbMember(got, "szymonrychu/tatara-operator").DeployState, "operator confirmed applied")
	require.NotEqual(t, "applied", umbMember(got, "szymonrychu/tatara-chat").DeployState, "chat not yet applied")
}

func TestUmbrella_ReconcileDeploying_ClosesIssueWhenAllMembersApplied(t *testing.T) {
	proj := seedUmbrellaScene(t, "allapplied", "tatara-operator", "tatara-chat")
	task := seedDeployingUmbrella(t, proj, "umb-allapplied", map[string]int{
		"szymonrychu/tatara-operator": 501,
		"szymonrychu/tatara-chat":     502,
	}, time.Now().Add(30*time.Minute))
	// operator already confirmed applied from a prior sweep (per-member persistence).
	cur := getTask(t, task.Name)
	umbMember(cur, "szymonrychu/tatara-operator").DeployState = "applied"
	umbMember(cur, "szymonrychu/tatara-operator").DeployedVersion = "v1.0.0"
	require.NoError(t, k8sClient.Status().Update(context.Background(), cur))

	w := &umbWriter{}
	rd := &umbReader{
		tags:     map[string]string{"tatara-operator": "v1.0.0", "tatara-chat": "v2.0.0"},
		run:      scm.WorkflowRun{HeadSHA: "sha2", Status: "completed", Conclusion: "success", HTMLURL: "https://run/2"},
		runFound: true,
		files: map[string]string{
			"helmfile.yaml.gotmpl":               "releases:\n  - name: tatara-operator\n    version: 1.0.0\n  - name: tatara-chat\n    version: 2.0.0\n",
			"values/tatara-operator/common.yaml": "  tag: \"v1.0.0\"\n",
		},
	}
	r := newUmbTaskReconciler(w, rd)

	_, err := r.reconcileDeploying(deployCtx(), proj, getTask(t, task.Name))
	require.NoError(t, err)

	got := getTask(t, task.Name)
	require.Equal(t, "Done", got.Status.DeployState, "all members applied: Task resolves Done")
	require.Equal(t, "", got.Status.Phase, "Deploying phase cleared on resolve")
	require.True(t, tatarav1alpha1.TaskTerminal(got))
	require.Len(t, w.closeCalls, 1, "the originating issue closes exactly once, after all N members applied")
	require.Contains(t, w.closeCalls[0], "szymonrychu/tatara-operator|9|")
	// The close comment spans every member artifact@version.
	require.True(t, strings.Contains(w.closeCalls[0], "tatara-operator@v1.0.0"), "close comment names operator version")
	require.True(t, strings.Contains(w.closeCalls[0], "tatara-chat@v2.0.0"), "close comment names chat version")
}

// --- item 4: cdScan defaults on at the CRD level ---

func TestCDScan_DefaultsOnWhenOmitted(t *testing.T) {
	ctx := context.Background()
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "cdscan-def-scm", Namespace: testNS},
		Data:       map[string][]byte{"token": []byte("pat"), "webhookSecret": []byte("w")},
	}
	require.NoError(t, k8sClient.Create(ctx, sec))
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "cdscan-def-proj", Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: sec.Name,
			TriggerLabel: "tatara",
			Scm: &tatarav1alpha1.ScmSpec{
				Provider: "github", Owner: "szymonrychu", BotLogin: "tatara-bot",
				// A cron block that sets mrScan but deliberately OMITS cdScan.
				Cron: &tatarav1alpha1.ScmCron{
					MRScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *"},
				},
			},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, proj))
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), proj) })

	var got tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(proj), &got))
	require.Equal(t, "*/10 * * * *", got.Spec.Scm.Cron.CDScan.Schedule,
		"a Project without an explicit cdScan.schedule must get the defaulted backstop cron")
}
