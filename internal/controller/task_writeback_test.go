package controller

import (
	"context"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

type fakeWriter struct {
	scm.SCMWriter
	mu          sync.Mutex
	openCalls   int
	commentArgs []string // issueRef|body
	prURL       string
	openErr     error
}

func (f *fakeWriter) OpenChange(_ context.Context, _, _, _, _, _, _ string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.openCalls++
	if f.openErr != nil {
		return "", f.openErr
	}
	if f.prURL == "" {
		f.prURL = "https://example/pr/1"
	}
	return f.prURL, nil
}

func (f *fakeWriter) Comment(_ context.Context, _, issueRef, body string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commentArgs = append(f.commentArgs, issueRef+"|"+body)
	return nil
}

func newWriteBackReconciler(t *testing.T, fw *fakeWriter) *TaskReconciler {
	t.Helper()
	return &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Session: newFakeSession(),
		PodConfig: agent.PodConfig{
			Namespace:           testNS,
			CallbackURL:         "http://op-internal.tatara.svc:8082",
			OIDCIssuer:          "https://keycloak.tatara.svc/realms/master",
			AnthropicSecretName: "anthropic",
			CLIOIDCSecretName:   "tatara-cli-oidc",
		},
		SCMFor: func(string) (scm.SCMWriter, error) { return fw, nil },
	}
}

func reconcileWriteback(t *testing.T, r *TaskReconciler, name string) (ctrl.Result, error) {
	t.Helper()
	return r.Reconcile(logf.IntoContext(context.Background(), logf.Log), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: name},
	})
}

// seedWritebackPending sets a Task into the WritebackPending state that M4's
// terminate() produces for a Succeeded task.
func seedWritebackPending(t *testing.T, name, scmSecret, project, repo string) *tatarav1alpha1.Task {
	t.Helper()
	ctx := context.Background()

	// Secret with token + webhookSecret
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: scmSecret, Namespace: testNS},
		Data:       map[string][]byte{"token": []byte("pat"), "webhookSecret": []byte("w")},
	}
	if err := k8sClient.Create(ctx, sec); err != nil {
		t.Fatalf("create secret %s: %v", scmSecret, err)
	}

	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: project, Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: scmSecret,
			TriggerLabel: "tatara",
		},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project %s: %v", project, err)
	}

	r := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: repo, Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef:       project,
			URL:              "https://github.com/o/r.git",
			DefaultBranch:    "main",
			ReingestSchedule: "0 6 * * *",
		},
	}
	if err := k8sClient.Create(ctx, r); err != nil {
		t.Fatalf("create repository %s: %v", repo, err)
	}

	src := &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#7", URL: "https://github.com/o/r/issues/7"}
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    project,
			RepositoryRef: repo,
			Goal:          "Fix the bug",
			Source:        src,
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task %s: %v", name, err)
	}

	// Directly set the status to what M4's terminate() produces.
	task.Status.Phase = "Succeeded"
	task.Status.ResultSummary = "did the thing"
	apimeta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
		Type:   "WritebackPending",
		Status: metav1.ConditionTrue,
		Reason: "AwaitingM5",
	})
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed writeback pending status on %s: %v", name, err)
	}
	// Reload to get server-side state.
	var fresh tatarav1alpha1.Task
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, &fresh); err != nil {
		t.Fatalf("reload task %s: %v", name, err)
	}
	return &fresh
}

func TestTaskWriteBackOpensPRAndComments(t *testing.T) {
	fw := &fakeWriter{prURL: "https://github.com/o/r/pull/5"}
	r := newWriteBackReconciler(t, fw)
	task := seedWritebackPending(t, "wb-task1", "wb-scm1", "wb-proj1", "wb-repo1")

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(
		context.Background(),
		types.NamespacedName{Namespace: testNS, Name: task.Name},
		&got,
	))
	require.Equal(t, "https://github.com/o/r/pull/5", got.Status.PrURL)
	// WritebackPending must be cleared (False).
	cond := findCond(got.Status.Conditions, "WritebackPending")
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status)

	fw.mu.Lock()
	defer fw.mu.Unlock()
	require.Equal(t, 1, fw.openCalls)
	require.Len(t, fw.commentArgs, 1)
	require.Contains(t, fw.commentArgs[0], "o/r#7|")
}

func TestTaskWriteBackNoCommentWhenNoSource(t *testing.T) {
	fw := &fakeWriter{prURL: "https://github.com/o/r/pull/6"}
	r := newWriteBackReconciler(t, fw)

	// No source - manually create without IssueRef.
	ctx := context.Background()
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "wb-scm2", Namespace: testNS},
		Data:       map[string][]byte{"token": []byte("pat"), "webhookSecret": []byte("w")},
	}
	require.NoError(t, k8sClient.Create(ctx, sec))
	require.NoError(t, k8sClient.Create(ctx, &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "wb-proj2", Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{ScmSecretRef: "wb-scm2"},
	}))
	require.NoError(t, k8sClient.Create(ctx, &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "wb-repo2", Namespace: testNS},
		Spec:       tatarav1alpha1.RepositorySpec{ProjectRef: "wb-proj2", URL: "https://github.com/o/r2.git", DefaultBranch: "main", ReingestSchedule: "0 6 * * *"},
	}))
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "wb-task2", Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "wb-proj2", RepositoryRef: "wb-repo2", Goal: "no-source task"},
	}
	require.NoError(t, k8sClient.Create(ctx, task))
	task.Status.Phase = "Succeeded"
	task.Status.ResultSummary = "done"
	apimeta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
		Type: "WritebackPending", Status: metav1.ConditionTrue, Reason: "AwaitingM5",
	})
	require.NoError(t, k8sClient.Status().Update(ctx, task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	require.NotEmpty(t, got.Status.PrURL)

	fw.mu.Lock()
	defer fw.mu.Unlock()
	require.Equal(t, 1, fw.openCalls)
	require.Empty(t, fw.commentArgs)
}

func TestWriteback_CommentsResultWhenNoPR(t *testing.T) {
	// Report/question task: no repo has the branch (all 422), so no PR opens,
	// but the agent's result must still be posted to the issue.
	fw := &fakeWriter{openErr: &scm.HTTPError{Status: 422, Body: "no diff", Path: "/pulls"}}
	r := newWriteBackReconciler(t, fw)
	task := seedWritebackPending(t, "wb-nopr", "wb-scm-nopr", "wb-proj-nopr", "wb-repo-nopr")

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	fw.mu.Lock()
	defer fw.mu.Unlock()
	require.GreaterOrEqual(t, fw.openCalls, 1)
	require.Len(t, fw.commentArgs, 1, "report-only task must still comment its result on the issue")
	require.Contains(t, fw.commentArgs[0], "o/r#7|")
	require.Contains(t, fw.commentArgs[0], "did the thing") // ResultSummary from the seed
}

func TestTaskWriteBackIdempotent(t *testing.T) {
	fw := &fakeWriter{prURL: "https://github.com/o/r/pull/7"}
	r := newWriteBackReconciler(t, fw)
	task := seedWritebackPending(t, "wb-task3", "wb-scm3", "wb-proj3", "wb-repo3")

	// First reconcile: write-back.
	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	// Second reconcile: should be noop (prURL already set).
	_, err = reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	fw.mu.Lock()
	defer fw.mu.Unlock()
	require.Equal(t, 1, fw.openCalls, "OpenChange must not be called twice")
}

// TestTaskWriteBackAlreadyExists tests that a 4xx HTTPError from OpenChange
// clears WritebackPending with a neutral reason and does not requeue.
func TestTaskWriteBackAlreadyExists(t *testing.T) {
	task := seedWritebackPending(t, "wb-task4", "wb-scm4", "wb-proj4", "wb-repo4")

	fw := &fakeWriter{openErr: &scm.HTTPError{Status: 422, Body: "already exists", Path: "/repos/o/r/pulls"}}
	r := newWriteBackReconciler(t, fw)

	res, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)
	// Must not requeue forever on permanent error.
	require.Equal(t, ctrl.Result{}, res)

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(
		context.Background(),
		types.NamespacedName{Namespace: testNS, Name: task.Name},
		&got,
	))
	// WritebackPending must be cleared with neutral reason, not an error reason.
	cond := findCond(got.Status.Conditions, "WritebackPending")
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status)
	require.Equal(t, "WritebackSkipped", cond.Reason)
	require.Contains(t, cond.Message, "422")
}

// fakeWriterPerRepo returns a configurable PR URL per repoURL, and an HTTPError
// for repos in the 422 set.
type fakeWriterPerRepo struct {
	scm.SCMWriter
	mu          sync.Mutex
	openCalls   int
	commentArgs []string
	prURLs      map[string]string // repoURL -> pr URL
	errRepos    map[string]bool   // repoURL -> return 422
}

func (f *fakeWriterPerRepo) OpenChange(_ context.Context, repoURL, _, _, _, _, _ string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.openCalls++
	if f.errRepos[repoURL] {
		return "", &scm.HTTPError{Status: 422, Body: "branch not found", Path: "/pulls"}
	}
	url, ok := f.prURLs[repoURL]
	if !ok {
		url = "https://example/pr/" + repoURL
	}
	return url, nil
}

func (f *fakeWriterPerRepo) Comment(_ context.Context, _, issueRef, body string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commentArgs = append(f.commentArgs, issueRef+"|"+body)
	return nil
}

// seedWritebackPendingMultiRepo creates a project with two repos (r1=primary, r2=secondary)
// plus the task seeded in WritebackPending state.
func seedWritebackPendingMultiRepo(t *testing.T, name, scmSecret, project, primaryRepo, secondaryRepo string) *tatarav1alpha1.Task {
	t.Helper()
	ctx := context.Background()

	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: scmSecret, Namespace: testNS},
		Data:       map[string][]byte{"token": []byte("pat"), "webhookSecret": []byte("w")},
	}
	if err := k8sClient.Create(ctx, sec); err != nil {
		t.Fatalf("create secret %s: %v", scmSecret, err)
	}

	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: project, Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{ScmSecretRef: scmSecret, TriggerLabel: "tatara"},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project %s: %v", project, err)
	}

	r1 := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: primaryRepo, Namespace: testNS},
		Spec:       tatarav1alpha1.RepositorySpec{ProjectRef: project, URL: "https://github.com/o/r1.git", DefaultBranch: "main", ReingestSchedule: "0 6 * * *"},
	}
	if err := k8sClient.Create(ctx, r1); err != nil {
		t.Fatalf("create primary repository %s: %v", primaryRepo, err)
	}

	r2 := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: secondaryRepo, Namespace: testNS},
		Spec:       tatarav1alpha1.RepositorySpec{ProjectRef: project, URL: "https://github.com/o/r2.git", DefaultBranch: "main", ReingestSchedule: "0 6 * * *"},
	}
	if err := k8sClient.Create(ctx, r2); err != nil {
		t.Fatalf("create secondary repository %s: %v", secondaryRepo, err)
	}

	src := &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r1#9", URL: "https://github.com/o/r1/issues/9"}
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    project,
			RepositoryRef: primaryRepo,
			Goal:          "Cross-repo fix",
			Source:        src,
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task %s: %v", name, err)
	}

	task.Status.Phase = "Succeeded"
	task.Status.ResultSummary = "fixed both repos"
	apimeta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
		Type:   "WritebackPending",
		Status: metav1.ConditionTrue,
		Reason: "AwaitingM5",
	})
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed writeback pending status on %s: %v", name, err)
	}
	var fresh tatarav1alpha1.Task
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, &fresh); err != nil {
		t.Fatalf("reload task %s: %v", name, err)
	}
	return &fresh
}

func TestWriteback_OpensPRPerRepoWithBranch(t *testing.T) {
	fw := &fakeWriterPerRepo{
		prURLs: map[string]string{
			"https://github.com/o/r1.git": "https://github.com/o/r1/pull/10",
			"https://github.com/o/r2.git": "https://github.com/o/r2/pull/11",
		},
	}
	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Session: newFakeSession(),
		PodConfig: agent.PodConfig{
			Namespace:           testNS,
			CallbackURL:         "http://op-internal.tatara.svc:8082",
			OIDCIssuer:          "https://keycloak.tatara.svc/realms/master",
			AnthropicSecretName: "anthropic",
			CLIOIDCSecretName:   "tatara-cli-oidc",
		},
		SCMFor: func(string) (scm.SCMWriter, error) { return fw, nil },
	}
	task := seedWritebackPendingMultiRepo(t, "wb-mr-task1", "wb-mr-scm1", "wb-mr-proj1", "wb-mr-repo1", "wb-mr-repo2")

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))

	// WritebackPending must be cleared.
	cond := findCond(got.Status.Conditions, "WritebackPending")
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status)

	// Both PRs were opened.
	fw.mu.Lock()
	defer fw.mu.Unlock()
	require.Equal(t, 2, fw.openCalls)
	// Issue was commented with both PR links.
	require.Len(t, fw.commentArgs, 1)
	require.Contains(t, fw.commentArgs[0], "o/r1#9|")
	require.Contains(t, fw.commentArgs[0], "https://github.com/o/r1/pull/10")
	require.Contains(t, fw.commentArgs[0], "https://github.com/o/r2/pull/11")
	// PrURL on status contains at least the primary PR.
	require.Contains(t, got.Status.PrURL, "https://github.com/o/r1/pull/10")
}

func TestWriteback_SkipsRepoWith422(t *testing.T) {
	fw := &fakeWriterPerRepo{
		prURLs:   map[string]string{"https://github.com/o/r1.git": "https://github.com/o/r1/pull/20"},
		errRepos: map[string]bool{"https://github.com/o/r2.git": true},
	}
	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Session: newFakeSession(),
		PodConfig: agent.PodConfig{
			Namespace:           testNS,
			CallbackURL:         "http://op-internal.tatara.svc:8082",
			OIDCIssuer:          "https://keycloak.tatara.svc/realms/master",
			AnthropicSecretName: "anthropic",
			CLIOIDCSecretName:   "tatara-cli-oidc",
		},
		SCMFor: func(string) (scm.SCMWriter, error) { return fw, nil },
	}
	task := seedWritebackPendingMultiRepo(t, "wb-mr-task2", "wb-mr-scm2", "wb-mr-proj2", "wb-mr-repo3", "wb-mr-repo4")

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	cond := findCond(got.Status.Conditions, "WritebackPending")
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status)

	fw.mu.Lock()
	defer fw.mu.Unlock()
	require.Equal(t, 2, fw.openCalls) // called for both repos
	// Only one PR URL in comment (r2 was 422-skipped)
	require.Len(t, fw.commentArgs, 1)
	require.Contains(t, fw.commentArgs[0], "https://github.com/o/r1/pull/20")
	require.NotContains(t, fw.commentArgs[0], "r2")
}

// fullFakeSCMWriter records calls to all SCMWriter methods used by review/selfImprove/implement paths.
type fullFakeSCMWriter struct {
	scm.SCMWriter
	// implement path
	openCalls    int
	openCallBody string
	// review path
	approveCalled        bool
	approveNumber        int
	approveBody          string
	requestChangesCalled bool
	requestChangesNumber int
	requestChangesBody   string
	suggestCalled        bool
	suggestSuggs         []scm.Suggestion
	commentCalled        bool
	commentIssueRef      string
	commentBody          string
	// selfImprove path
	mergeCalled   bool
	mergeNumber   int
	mergeMethod   string
	closePRCalled bool
	closePRNumber int
	// triageIssue path
	closeIssueCalled bool
	closeIssueNumber int
	// GetPRState
	prState    scm.PRState
	prStateErr error
}

func (f *fullFakeSCMWriter) OpenChange(_ context.Context, _, _, _, _, title, body string) (string, error) {
	f.openCalls++
	f.openCallBody = body
	return "https://example/pr/99", nil
}
func (f *fullFakeSCMWriter) Comment(_ context.Context, _, issueRef, body string) error {
	f.commentCalled = true
	f.commentIssueRef = issueRef
	f.commentBody = body
	return nil
}
func (f *fullFakeSCMWriter) Approve(_ context.Context, _, _ string, number int, body string) error {
	f.approveCalled = true
	f.approveNumber = number
	f.approveBody = body
	return nil
}
func (f *fullFakeSCMWriter) RequestChanges(_ context.Context, _, _ string, number int, body string) error {
	f.requestChangesCalled = true
	f.requestChangesNumber = number
	f.requestChangesBody = body
	return nil
}
func (f *fullFakeSCMWriter) Suggest(_ context.Context, _, _ string, _ int, sugg []scm.Suggestion) error {
	f.suggestCalled = true
	f.suggestSuggs = sugg
	return nil
}
func (f *fullFakeSCMWriter) Merge(_ context.Context, _, _ string, number int, method string) error {
	f.mergeCalled = true
	f.mergeNumber = number
	f.mergeMethod = method
	return nil
}
func (f *fullFakeSCMWriter) ClosePR(_ context.Context, _, _ string, number int, _ string) error {
	f.closePRCalled = true
	f.closePRNumber = number
	return nil
}
func (f *fullFakeSCMWriter) GetPRState(_ context.Context, _, _ string, _ int) (scm.PRState, error) {
	return f.prState, f.prStateErr
}
func (f *fullFakeSCMWriter) CloseIssue(_ context.Context, _, _ string, number int, _ string) error {
	f.closeIssueCalled = true
	f.closeIssueNumber = number
	return nil
}

// seedWritebackKindTask creates the minimal project+repo+secret+task for write-back Kind tests.
// scmSpec may be nil for implement tasks; provided for selfImprove policy tests.
func seedWritebackKindTask(t *testing.T, name, project, repo, scmSecret string, spec tatarav1alpha1.TaskSpec, scmSpec *tatarav1alpha1.ScmSpec) *tatarav1alpha1.Task {
	t.Helper()
	ctx := context.Background()

	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: scmSecret, Namespace: testNS},
		Data:       map[string][]byte{"token": []byte("pat"), "webhookSecret": []byte("w")},
	}
	if err := k8sClient.Create(ctx, sec); err != nil {
		t.Fatalf("create secret %s: %v", scmSecret, err)
	}
	projSpec := tatarav1alpha1.ProjectSpec{
		ScmSecretRef: scmSecret,
		TriggerLabel: "tatara",
		Agent: tatarav1alpha1.AgentSpec{
			Model: "claude-x", Image: "wrapper:1", PermissionMode: "bypassPermissions",
			MaxTurnsPerTask: 50, TurnTimeoutSeconds: 1800,
		},
	}
	if scmSpec != nil {
		projSpec.Scm = scmSpec
	}
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: project, Namespace: testNS},
		Spec:       projSpec,
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project %s: %v", project, err)
	}
	proj.Status.Memory = &tatarav1alpha1.MemoryStatus{Phase: "Ready", Endpoint: "http://mem.svc"}
	if err := k8sClient.Status().Update(ctx, proj); err != nil {
		t.Fatalf("set project memory ready: %v", err)
	}

	r := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: repo, Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef:       project,
			URL:              "https://github.com/o/r.git",
			DefaultBranch:    "main",
			ReingestSchedule: "0 6 * * *",
		},
	}
	if err := k8sClient.Create(ctx, r); err != nil {
		t.Fatalf("create repo %s: %v", repo, err)
	}

	spec.ProjectRef = project
	spec.RepositoryRef = repo
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec:       spec,
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task %s: %v", name, err)
	}

	// Seed WritebackPending so doWriteBack is entered.
	task.Status.Phase = "Succeeded"
	task.Status.ResultSummary = "done"
	apimeta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
		Type: "WritebackPending", Status: metav1.ConditionTrue, Reason: "AwaitingM5",
	})
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed writeback status: %v", err)
	}
	var fresh tatarav1alpha1.Task
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, &fresh); err != nil {
		t.Fatalf("reload task: %v", err)
	}
	return &fresh
}

func newFullFakeReconciler(t *testing.T, fw *fullFakeSCMWriter) *TaskReconciler {
	t.Helper()
	return &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Session: newFakeSession(),
		PodConfig: agent.PodConfig{
			Namespace:           testNS,
			CallbackURL:         "http://op-internal.tatara.svc:8082",
			OIDCIssuer:          "https://keycloak.tatara.svc/realms/master",
			AnthropicSecretName: "anthropic",
			CLIOIDCSecretName:   "tatara-cli-oidc",
		},
		SCMFor: func(string) (scm.SCMWriter, error) { return fw, nil },
	}
}

func TestDoWriteBackKind(t *testing.T) {
	t.Run("review/approve", func(t *testing.T) {
		fw := &fullFakeSCMWriter{}
		r := newFullFakeReconciler(t, fw)
		task := seedWritebackKindTask(t, "wbk-rev-approve", "wbk-proj-ra", "wbk-repo-ra", "wbk-scm-ra",
			tatarav1alpha1.TaskSpec{
				Goal: "review a PR",
				Kind: "review",
				Source: &tatarav1alpha1.TaskSource{
					Provider: "github", IssueRef: "o/r#9", IsPR: true, Number: 9,
				},
			}, nil)
		task.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{Decision: "approve", Body: "lgtm"}
		require.NoError(t, k8sClient.Status().Update(context.Background(), task))

		_, err := reconcileWriteback(t, r, task.Name)
		require.NoError(t, err)

		require.True(t, fw.approveCalled, "Approve must be called")
		require.Equal(t, 9, fw.approveNumber)
		require.Equal(t, "lgtm", fw.approveBody)
		require.Zero(t, fw.openCalls, "OpenChange must NOT be called for review kind")
	})

	t.Run("review/request_changes with suggestions", func(t *testing.T) {
		fw := &fullFakeSCMWriter{}
		r := newFullFakeReconciler(t, fw)
		task := seedWritebackKindTask(t, "wbk-rev-rc", "wbk-proj-rc", "wbk-repo-rc", "wbk-scm-rc",
			tatarav1alpha1.TaskSpec{
				Goal: "review a PR",
				Kind: "review",
				Source: &tatarav1alpha1.TaskSource{
					Provider: "github", IssueRef: "o/r#10", IsPR: true, Number: 10,
				},
			}, nil)
		task.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{
			Decision:    "request_changes",
			Body:        "nope",
			Suggestions: []tatarav1alpha1.Suggestion{{Path: "a.go", Line: 5, Body: "x := 1"}},
		}
		require.NoError(t, k8sClient.Status().Update(context.Background(), task))

		_, err := reconcileWriteback(t, r, task.Name)
		require.NoError(t, err)

		require.True(t, fw.requestChangesCalled, "RequestChanges must be called")
		require.Equal(t, 10, fw.requestChangesNumber)
		require.True(t, fw.suggestCalled, "Suggest must be called when suggestions present")
		require.Len(t, fw.suggestSuggs, 1)
		require.Equal(t, "a.go", fw.suggestSuggs[0].Path)
		require.Zero(t, fw.openCalls, "OpenChange must NOT be called for review kind")
	})

	t.Run("review/comment", func(t *testing.T) {
		fw := &fullFakeSCMWriter{}
		r := newFullFakeReconciler(t, fw)
		task := seedWritebackKindTask(t, "wbk-rev-cmt", "wbk-proj-cmt", "wbk-repo-cmt", "wbk-scm-cmt",
			tatarav1alpha1.TaskSpec{
				Goal: "comment on a PR",
				Kind: "review",
				Source: &tatarav1alpha1.TaskSource{
					Provider: "github", IssueRef: "o/r#11", IsPR: true, Number: 11,
				},
			}, nil)
		task.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{Decision: "comment", Body: "nice work"}
		require.NoError(t, k8sClient.Status().Update(context.Background(), task))

		_, err := reconcileWriteback(t, r, task.Name)
		require.NoError(t, err)

		require.True(t, fw.commentCalled, "Comment must be called for decision=comment")
		require.Equal(t, "o/r#11", fw.commentIssueRef)
		require.Equal(t, "nice work", fw.commentBody)
		require.Zero(t, fw.openCalls, "OpenChange must NOT be called")
	})

	t.Run("selfImprove/merge afterApproval", func(t *testing.T) {
		fw := &fullFakeSCMWriter{prState: scm.PRState{Author: "tatara-bot", CIStatus: ""}}
		r := newFullFakeReconciler(t, fw)
		task := seedWritebackKindTask(t, "wbk-si-merge", "wbk-proj-sim", "wbk-repo-sim", "wbk-scm-sim",
			tatarav1alpha1.TaskSpec{
				Goal: "self-improve",
				Kind: "selfImprove",
				Source: &tatarav1alpha1.TaskSource{
					Provider: "github", IssueRef: "o/r#12", IsPR: true, Number: 12,
				},
			},
			&tatarav1alpha1.ScmSpec{
				Provider:    "github",
				Owner:       "o",
				BotLogin:    "tatara-bot",
				MergePolicy: "afterApproval",
			})
		task.Status.PROutcome = &tatarav1alpha1.PROutcome{Action: "merge", Reason: "approved"}
		require.NoError(t, k8sClient.Status().Update(context.Background(), task))

		_, err := reconcileWriteback(t, r, task.Name)
		require.NoError(t, err)

		require.True(t, fw.mergeCalled, "Merge must be called for afterApproval pr_outcome=merge")
		require.False(t, fw.closePRCalled, "ClosePR must NOT be called")
		require.Equal(t, 12, fw.mergeNumber)
		require.Equal(t, "squash", fw.mergeMethod)
	})

	t.Run("selfImprove/merge autoMergeOnGreenCI success", func(t *testing.T) {
		fw := &fullFakeSCMWriter{prState: scm.PRState{Author: "tatara-bot", CIStatus: "success"}}
		r := newFullFakeReconciler(t, fw)
		task := seedWritebackKindTask(t, "wbk-si-maci", "wbk-proj-maci", "wbk-repo-maci", "wbk-scm-maci",
			tatarav1alpha1.TaskSpec{
				Goal: "self-improve CI",
				Kind: "selfImprove",
				Source: &tatarav1alpha1.TaskSource{
					Provider: "github", IssueRef: "o/r#13", IsPR: true, Number: 13,
				},
			},
			&tatarav1alpha1.ScmSpec{
				Provider:    "github",
				Owner:       "o",
				BotLogin:    "tatara-bot",
				MergePolicy: "autoMergeOnGreenCI",
			})
		task.Status.PROutcome = &tatarav1alpha1.PROutcome{Action: "merge"}
		require.NoError(t, k8sClient.Status().Update(context.Background(), task))

		_, err := reconcileWriteback(t, r, task.Name)
		require.NoError(t, err)

		require.True(t, fw.mergeCalled, "Merge must be called when CI=success")
	})

	t.Run("selfImprove/merge autoMergeOnGreenCI CI absent -> afterApproval", func(t *testing.T) {
		fw := &fullFakeSCMWriter{prState: scm.PRState{Author: "tatara-bot", CIStatus: ""}}
		r := newFullFakeReconciler(t, fw)
		task := seedWritebackKindTask(t, "wbk-si-noci", "wbk-proj-noci", "wbk-repo-noci", "wbk-scm-noci",
			tatarav1alpha1.TaskSpec{
				Goal: "self-improve no CI",
				Kind: "selfImprove",
				Source: &tatarav1alpha1.TaskSource{
					Provider: "github", IssueRef: "o/r#14", IsPR: true, Number: 14,
				},
			},
			&tatarav1alpha1.ScmSpec{
				Provider:    "github",
				Owner:       "o",
				BotLogin:    "tatara-bot",
				MergePolicy: "autoMergeOnGreenCI",
			})
		task.Status.PROutcome = &tatarav1alpha1.PROutcome{Action: "merge"}
		require.NoError(t, k8sClient.Status().Update(context.Background(), task))

		_, err := reconcileWriteback(t, r, task.Name)
		require.NoError(t, err)

		// CI absent -> falls back to afterApproval which trusts pr_outcome=merge -> merges.
		require.True(t, fw.mergeCalled, "CI absent falls back to afterApproval which trusts pr_outcome=merge")
	})

	t.Run("selfImprove/close", func(t *testing.T) {
		fw := &fullFakeSCMWriter{prState: scm.PRState{Author: "tatara-bot"}}
		r := newFullFakeReconciler(t, fw)
		task := seedWritebackKindTask(t, "wbk-si-close", "wbk-proj-close", "wbk-repo-close", "wbk-scm-close",
			tatarav1alpha1.TaskSpec{
				Goal: "self-improve close",
				Kind: "selfImprove",
				Source: &tatarav1alpha1.TaskSource{
					Provider: "github", IssueRef: "o/r#15", IsPR: true, Number: 15,
				},
			},
			&tatarav1alpha1.ScmSpec{
				Provider: "github", Owner: "o", BotLogin: "tatara-bot", MergePolicy: "afterApproval",
			})
		task.Status.PROutcome = &tatarav1alpha1.PROutcome{Action: "close", Reason: "rejecting"}
		require.NoError(t, k8sClient.Status().Update(context.Background(), task))

		_, err := reconcileWriteback(t, r, task.Name)
		require.NoError(t, err)

		require.True(t, fw.closePRCalled, "ClosePR must be called")
		require.Equal(t, 15, fw.closePRNumber)
		require.False(t, fw.mergeCalled, "Merge must NOT be called")
	})

	t.Run("implement body carries marker", func(t *testing.T) {
		fw := &fullFakeSCMWriter{}
		r := newFullFakeReconciler(t, fw)
		task := seedWritebackKindTask(t, "wbk-impl-marker", "wbk-proj-impl", "wbk-repo-impl", "wbk-scm-impl",
			tatarav1alpha1.TaskSpec{
				Goal: "fix bug",
				Kind: "implement",
				Source: &tatarav1alpha1.TaskSource{
					Provider: "github", IssueRef: "o/r#16", IsPR: false, Number: 0,
				},
			}, nil)

		_, err := reconcileWriteback(t, r, task.Name)
		require.NoError(t, err)

		require.GreaterOrEqual(t, fw.openCalls, 1, "OpenChange must be called for implement kind")
		require.Contains(t, fw.openCallBody, "<!-- tatara-authored -->", "implement body must carry marker")
	})
}

func TestWriteBackIssue_IsPRRefused(t *testing.T) {
	// CloseIssue must NOT be called when the source is a PR.
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "wbi-ispr", "wbi-proj-ispr", "wbi-repo-ispr", "wbi-scm-ispr",
		tatarav1alpha1.TaskSpec{
			Goal: "triage issue that is actually a PR",
			Kind: "triageIssue",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#20", IsPR: true, Number: 20,
			},
		}, nil)
	task.Status.IssueOutcome = &tatarav1alpha1.IssueOutcome{Action: "close", Comment: "out of scope"}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	require.False(t, fw.closeIssueCalled, "CloseIssue must NOT be called when source.IsPR is true")

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	cond := findCond(got.Status.Conditions, "WritebackPending")
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status)
	require.Equal(t, "IssueRefusedPR", cond.Reason)
}

func TestWriteBackIssue_CloseIssue(t *testing.T) {
	// CloseIssue must be called for a real issue (IsPR=false) with action=close.
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "wbi-close", "wbi-proj-close", "wbi-repo-close", "wbi-scm-close",
		tatarav1alpha1.TaskSpec{
			Goal: "triage issue",
			Kind: "triageIssue",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#21", IsPR: false, Number: 21,
			},
		}, nil)
	task.Status.IssueOutcome = &tatarav1alpha1.IssueOutcome{Action: "close", Comment: "out of scope"}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	require.True(t, fw.closeIssueCalled, "CloseIssue must be called for IsPR=false, action=close")
	require.Equal(t, 21, fw.closeIssueNumber)
}

func TestWriteBackIssue_ImplementCallsOpenChange(t *testing.T) {
	// action=implement must call OpenChange so the pushed branch becomes a PR.
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "wbi-impl", "wbi-proj-impl2", "wbi-repo-impl2", "wbi-scm-impl2",
		tatarav1alpha1.TaskSpec{
			Goal: "triage issue implement",
			Kind: "triageIssue",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#22", IsPR: false, Number: 22,
			},
		}, nil)
	task.Status.IssueOutcome = &tatarav1alpha1.IssueOutcome{Action: "implement"}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	require.GreaterOrEqual(t, fw.openCalls, 1, "OpenChange must be called for triageIssue action=implement")
	require.False(t, fw.closeIssueCalled, "CloseIssue must NOT be called for implement")
}

func TestProviderForRemote(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	tests := []struct {
		remote string
		want   string
	}{
		{"https://gitlab.com/org/repo.git", "gitlab"},
		{"https://self-hosted.gitlab.example.com/org/repo.git", "gitlab"},
		{"https://github.com/org/repo.git", "github"},
		{"https://github.example.com/org/repo.git", "github"},
		{"https://internal.example.com/org/repo.git", "github"}, // unknown -> defaults to github
	}
	for _, tc := range tests {
		got := providerForRemote(ctx, tc.remote)
		if got != tc.want {
			t.Errorf("providerForRemote(%q) = %q, want %q", tc.remote, got, tc.want)
		}
	}
}
