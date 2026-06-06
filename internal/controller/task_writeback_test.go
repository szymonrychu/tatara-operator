package controller

import (
	"context"
	"fmt"
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

// permanentSCMError is a fake permanent error simulating scm.HTTPError{Status:422}.
type permanentSCMError struct{ status int }

func (e *permanentSCMError) Error() string     { return fmt.Sprintf("scm: permanent %d", e.status) }
func (e *permanentSCMError) IsPermanent() bool { return e.status >= 400 && e.status < 500 }

// TestTaskWriteBackAlreadyExists tests that a permanent 422 from OpenChange
// clears WritebackPending and does not requeue infinitely.
func TestTaskWriteBackAlreadyExists(t *testing.T) {
	task := seedWritebackPending(t, "wb-task4", "wb-scm4", "wb-proj4", "wb-repo4")

	fw := &fakeWriter{openErr: &permanentSCMError{status: 422}}
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
	// WritebackPending must be cleared even on permanent error.
	cond := findCond(got.Status.Conditions, "WritebackPending")
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status)
}
