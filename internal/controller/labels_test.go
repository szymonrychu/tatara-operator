package controller

import (
	"context"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type labelWriter struct {
	scm.SCMWriter
	mu      sync.Mutex
	added   []string
	removed []string
}

func (w *labelWriter) AddLabel(_ context.Context, _, _, label string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.added = append(w.added, label)
	return nil
}
func (w *labelWriter) RemoveLabel(_ context.Context, _, _, label string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.removed = append(w.removed, label)
	return nil
}

type labelReader struct {
	fakeProposalReader
	current []string
}

func (r *labelReader) ListOpenIssues(_ context.Context, _, _ string) ([]scm.IssueRef, error) {
	return []scm.IssueRef{{Repo: "o/r", Number: 7, Labels: r.current}}, nil
}

func seedLabelTask(t *testing.T, suffix string, currentLabels []string) (*TaskReconciler, *tatarav1alpha1.Task, *labelWriter) {
	t.Helper()
	ctx := context.Background()
	sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "lbl-scm-" + suffix, Namespace: testNS}, Data: map[string][]byte{"token": []byte("tok")}}
	require.NoError(t, k8sClient.Create(ctx, sec))
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "lbl-proj-" + suffix, Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{ScmSecretRef: "lbl-scm-" + suffix, Scm: &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "tatara-bot"}},
	}
	require.NoError(t, k8sClient.Create(ctx, proj))
	repo := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "lbl-repo-" + suffix, Namespace: testNS},
		Spec:       tatarav1alpha1.RepositorySpec{ProjectRef: proj.Name, URL: "https://github.com/o/r.git", DefaultBranch: "main", ReingestSchedule: "0 6 * * *"},
	}
	require.NoError(t, k8sClient.Create(ctx, repo))
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "lbl-task-" + suffix, Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, RepositoryRef: repo.Name, Kind: "issueLifecycle",
			Source: &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#7", Number: 7, AuthorLogin: "human"}},
	}
	require.NoError(t, k8sClient.Create(ctx, task))
	var fresh tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: task.Name}, &fresh))
	w := &labelWriter{}
	rdr := &labelReader{current: currentLabels}
	r := &TaskReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		SCMFor:    func(string) (scm.SCMWriter, error) { return w, nil },
		ReaderFor: func(_, _ string) (scm.SCMReader, error) { return rdr, nil }}
	return r, &fresh, w
}

func TestSetLifecycleLabel_AddsDesiredRemovesOthers(t *testing.T) {
	r, task, w := seedLabelTask(t, "addrm", []string{"tatara-idea", "unrelated"})
	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))
	require.NoError(t, r.setLifecycleLabel(context.Background(), &proj, task, "tatara-approved"))
	require.Equal(t, []string{"tatara-approved"}, w.added)
	require.Equal(t, []string{"tatara-idea"}, w.removed)
}

func TestSetLifecycleLabel_NoopWhenAlreadySet(t *testing.T) {
	r, task, w := seedLabelTask(t, "noop", []string{"tatara-approved"})
	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))
	require.NoError(t, r.setLifecycleLabel(context.Background(), &proj, task, "tatara-approved"))
	require.Empty(t, w.added)
	require.Empty(t, w.removed)
}

func TestSetLifecycleLabel_NeverTouchesTriggerOrPriority(t *testing.T) {
	r, task, w := seedLabelTask(t, "scope", []string{"tatara", "priority/high", "tatara-idea"})
	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))
	require.NoError(t, r.setLifecycleLabel(context.Background(), &proj, task, "tatara-rejected"))
	require.Equal(t, []string{"tatara-rejected"}, w.added)
	require.Equal(t, []string{"tatara-idea"}, w.removed)
}

type commentReader struct {
	fakeProposalReader
	comments []scm.IssueComment
}

func (r *commentReader) ListIssueComments(_ context.Context, _, _ string, _ int) ([]scm.IssueComment, error) {
	return r.comments, nil
}

func newReconcilerWithReader(rdr scm.SCMReader) *TaskReconciler {
	return &TaskReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(),
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
		SCMFor:    func(string) (scm.SCMWriter, error) { return &labelWriter{}, nil },
		ReaderFor: func(_, _ string) (scm.SCMReader, error) { return rdr, nil }}
}

func TestHasHumanComment(t *testing.T) {
	_, task, _ := seedLabelTask(t, "humancmt", nil)
	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))

	r1 := newReconcilerWithReader(&commentReader{comments: []scm.IssueComment{{Author: "tatara-bot", Body: "proposal"}}})
	got, err := r1.hasHumanComment(context.Background(), &proj, task)
	require.NoError(t, err)
	require.False(t, got)

	r2 := newReconcilerWithReader(&commentReader{comments: []scm.IssueComment{{Author: "tatara-bot", Body: "x"}, {Author: "szymon", Body: "looks good, go"}}})
	got, err = r2.hasHumanComment(context.Background(), &proj, task)
	require.NoError(t, err)
	require.True(t, got)
}

// TestSetLifecycleLabel_UnknownLabels_RemovesUnconditionally verifies the
// read-failure fallback: when the current label set cannot be read (the issue
// is not in the open list, e.g. just-closed, or the reader returns nothing),
// the desired label is added and BOTH other managed labels are removed
// best-effort, preserving the "exactly one managed label" contract.
func TestSetLifecycleLabel_UnknownLabels_RemovesUnconditionally(t *testing.T) {
	_, task, w := seedLabelTask(t, "unknown", []string{"tatara-idea"})
	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))
	// commentReader.ListOpenIssues returns nil -> issue never matched -> known=false.
	r := &TaskReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(),
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
		SCMFor:    func(string) (scm.SCMWriter, error) { return w, nil },
		ReaderFor: func(_, _ string) (scm.SCMReader, error) { return &commentReader{}, nil }}
	require.NoError(t, r.setLifecycleLabel(context.Background(), &proj, task, "tatara-approved"))
	require.Equal(t, []string{"tatara-approved"}, w.added)
	require.ElementsMatch(t, []string{"tatara-idea", "tatara-rejected"}, w.removed)
}
