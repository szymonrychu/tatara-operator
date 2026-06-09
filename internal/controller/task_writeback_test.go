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
		SCMFor: func(string) (Writer, error) { return fw, nil },
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
			ProjectRef:    project,
			URL:           "https://github.com/o/r.git",
			DefaultBranch: "main",
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
		Spec:       tatarav1alpha1.RepositorySpec{ProjectRef: "wb-proj2", URL: "https://github.com/o/r2.git", DefaultBranch: "main"},
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
		Spec:       tatarav1alpha1.RepositorySpec{ProjectRef: project, URL: "https://github.com/o/r1.git", DefaultBranch: "main"},
	}
	if err := k8sClient.Create(ctx, r1); err != nil {
		t.Fatalf("create primary repository %s: %v", primaryRepo, err)
	}

	r2 := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: secondaryRepo, Namespace: testNS},
		Spec:       tatarav1alpha1.RepositorySpec{ProjectRef: project, URL: "https://github.com/o/r2.git", DefaultBranch: "main"},
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
		SCMFor: func(string) (Writer, error) { return fw, nil },
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
		SCMFor: func(string) (Writer, error) { return fw, nil },
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
