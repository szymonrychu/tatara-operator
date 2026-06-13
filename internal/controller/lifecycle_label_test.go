package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"k8s.io/apimachinery/pkg/types"
)

// errGetIssueReader fails GetIssue so the authorship check errors (fail-closed test).
type errGetIssueReader struct{ fakeProposalReader }

func (errGetIssueReader) GetIssue(_ context.Context, _, _ string, _ int) (scm.IssueContent, error) {
	return scm.IssueContent{}, fmt.Errorf("get issue boom")
}

// labelWriter (defined in labels_test.go) overrides only AddLabel/RemoveLabel.
// finishTriage also calls Comment (discuss) and CloseIssue (close); add no-op
// overrides so the embedded nil SCMWriter is never dereferenced.
func (w *labelWriter) Comment(_ context.Context, _, _, _ string) error { return nil }
func (w *labelWriter) CloseIssue(_ context.Context, _, _ string, number int, _ string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = append(w.closed, number)
	return nil
}

func reconcilerFor(w scm.SCMWriter, rdr scm.SCMReader) *TaskReconciler {
	return &TaskReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(),
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
		SCMFor:    func(string) (scm.SCMWriter, error) { return w, nil },
		ReaderFor: func(_, _ string) (scm.SCMReader, error) { return rdr, nil }}
}

func markSucceededWithOutcome(t *testing.T, name, action string) {
	t.Helper()
	ctx := context.Background()
	var fresh tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, &fresh))
	fresh.Status.Phase = "Succeeded"
	fresh.Status.IssueOutcome = &tatarav1alpha1.IssueOutcome{Action: action, Comment: "c"}
	require.NoError(t, k8sClient.Status().Update(ctx, &fresh))
}

func getTaskByName(t *testing.T, name string) *tatarav1alpha1.Task {
	t.Helper()
	var fresh tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, &fresh))
	return &fresh
}

func projOf(t *testing.T, task *tatarav1alpha1.Task) *tatarav1alpha1.Project {
	t.Helper()
	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))
	return &proj
}

func TestFinishTriage_HumanFiledImplement_Approved(t *testing.T) {
	_, task, w := seedLabelTask(t, "hf-impl", []string{"tatara-idea"})
	r := reconcilerFor(w, &commentReader{})
	proj := projOf(t, task)
	markSucceededWithOutcome(t, task.Name, "implement")
	_, err := r.finishTriage(context.Background(), proj, getTaskByName(t, task.Name))
	require.NoError(t, err)
	require.Equal(t, []string{"tatara-approved"}, w.added)
	require.Equal(t, "Implement", getTaskByName(t, task.Name).Status.LifecycleState)
}

func TestFinishTriage_Close_Rejected(t *testing.T) {
	_, task, w := seedLabelTask(t, "close-rej", []string{"tatara-idea"})
	r := reconcilerFor(w, &commentReader{})
	proj := projOf(t, task)
	markSucceededWithOutcome(t, task.Name, "close")
	_, err := r.finishTriage(context.Background(), proj, getTaskByName(t, task.Name))
	require.NoError(t, err)
	require.Equal(t, []string{"tatara-declined"}, w.added)
	require.Equal(t, "Done", getTaskByName(t, task.Name).Status.LifecycleState)
}

func TestFinishTriage_Discuss_Idea(t *testing.T) {
	_, task, w := seedLabelTask(t, "disc-idea", nil)
	r := reconcilerFor(w, &commentReader{})
	proj := projOf(t, task)
	markSucceededWithOutcome(t, task.Name, "discuss")
	_, err := r.finishTriage(context.Background(), proj, getTaskByName(t, task.Name))
	require.NoError(t, err)
	require.Equal(t, []string{"tatara-brainstorming"}, w.added)
	require.Equal(t, "Conversation", getTaskByName(t, task.Name).Status.LifecycleState)
}

// Authorship is detected via the tataraAuthoredMarker in the issue body, NOT
// Source.AuthorLogin (which issueScan leaves empty). seedLabelTask sets
// AuthorLogin="human", so these tests prove the guard fires on the cron path
// purely from the marker.
func TestFinishTriage_TataraAuthoredImplement_NoHumanComment_ParksIdea(t *testing.T) {
	_, task, w := seedLabelTask(t, "auth-noh", nil)
	r := reconcilerFor(w, &commentReader{body: "an idea\n\n" + tataraAuthoredMarker})
	proj := projOf(t, task)
	markSucceededWithOutcome(t, task.Name, "implement")
	_, err := r.finishTriage(context.Background(), proj, getTaskByName(t, task.Name))
	require.NoError(t, err)
	require.Equal(t, []string{"tatara-brainstorming"}, w.added)
	require.Equal(t, "Conversation", getTaskByName(t, task.Name).Status.LifecycleState)
}

func TestFinishTriage_TataraAuthoredImplement_WithHumanComment_Approved(t *testing.T) {
	_, task, w := seedLabelTask(t, "auth-h", nil)
	r := reconcilerFor(w, &commentReader{
		body:     "an idea\n\n" + tataraAuthoredMarker,
		comments: []scm.IssueComment{{Author: "szymon", Body: "approved, go"}},
	})
	proj := projOf(t, task)
	markSucceededWithOutcome(t, task.Name, "implement")
	_, err := r.finishTriage(context.Background(), proj, getTaskByName(t, task.Name))
	require.NoError(t, err)
	require.Equal(t, []string{"tatara-approved"}, w.added)
	require.Equal(t, "Implement", getTaskByName(t, task.Name).Status.LifecycleState)
}

// Fail-closed: when the authorship check errors, treat the issue as tatara-authored
// and park it (never auto-approve on an unknown).
func TestFinishTriage_AuthorshipCheckError_FailsClosed_ParksIdea(t *testing.T) {
	_, task, w := seedLabelTask(t, "auth-err", nil)
	r := reconcilerFor(w, &errGetIssueReader{})
	proj := projOf(t, task)
	markSucceededWithOutcome(t, task.Name, "implement")
	_, err := r.finishTriage(context.Background(), proj, getTaskByName(t, task.Name))
	require.NoError(t, err)
	require.Equal(t, []string{"tatara-brainstorming"}, w.added)
	require.Equal(t, "Conversation", getTaskByName(t, task.Name).Status.LifecycleState)
}

// TestTriageEntrySetsBrainstormingLabel: a fresh issueLifecycle task (LifecycleState "")
// with a Triage entry annotation (or no annotation, which defaults to Triage) should
// have tatara-brainstorming added when reconcileLifecycle processes the case "" block.
func TestTriageEntrySetsBrainstormingLabel(t *testing.T) {
	r, task, w := seedLabelTask(t, "triage-entry", nil)
	setProjectMemoryReady(t, task.Spec.ProjectRef, "http://mem-triage-entry.tatara.svc:8080")
	// task is fresh (LifecycleState ""), no entry annotation -> defaults to Triage.
	ctx := context.Background()
	_, err := r.reconcileLifecycle(ctx, getTaskByName(t, task.Name))
	require.NoError(t, err)
	require.Contains(t, w.added, "tatara-brainstorming", "brainstorming label must be set on Triage entry")
	// No other managed phase labels should have been added.
	for _, lbl := range []string{"tatara-implementation", "tatara-approved", "tatara-declined"} {
		require.NotContains(t, w.added, lbl, "only brainstorming should be added, not %s", lbl)
	}
}

// TestImplementEntrySetsImplementationLabel: a task at LifecycleState "Implement" with
// Phase "" (fresh spawn) and an issue source should have tatara-implementation added.
func TestImplementEntrySetsImplementationLabel(t *testing.T) {
	r, task, w := seedLabelTask(t, "impl-entry", nil)
	setProjectMemoryReady(t, task.Spec.ProjectRef, "http://mem-impl-entry.tatara.svc:8080")
	// Set LifecycleState to Implement.
	ctx := context.Background()
	var fresh tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: task.Name}, &fresh))
	fresh.Status.LifecycleState = "Implement"
	require.NoError(t, k8sClient.Status().Update(ctx, &fresh))

	proj := projOf(t, &fresh)
	refreshed := getTaskByName(t, task.Name)
	// handleImplement is the code path; phase "" -> fresh spawn -> ensurePhaseLabel.
	_, _ = r.handleImplement(ctx, proj, refreshed)
	require.Contains(t, w.added, "tatara-implementation", "implementation label must be set on Implement entry")
	for _, lbl := range []string{"tatara-brainstorming", "tatara-approved", "tatara-declined"} {
		require.NotContains(t, w.added, lbl, "only implementation should be added, not %s", lbl)
	}
}

// TestPRSourceTaskSkipsPhaseLabel: a task whose source is a PR (IsPR=true) must
// not trigger any phase label, since phase labels are issue-only.
func TestPRSourceTaskSkipsPhaseLabel(t *testing.T) {
	ctx := context.Background()
	r, task, w := seedLabelTask(t, "pr-src-skip", nil)
	setProjectMemoryReady(t, task.Spec.ProjectRef, "http://mem-pr-src-skip.tatara.svc:8080")
	// Patch the task source to IsPR=true.
	var fresh tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: task.Name}, &fresh))
	fresh.Spec.Source.IsPR = true
	fresh.Spec.Source.URL = "https://github.com/o/r/pull/9"
	require.NoError(t, k8sClient.Update(ctx, &fresh))
	// Set LifecycleState to Implement.
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: task.Name}, &fresh))
	fresh.Status.LifecycleState = "Implement"
	require.NoError(t, k8sClient.Status().Update(ctx, &fresh))

	proj := projOf(t, &fresh)
	refreshed := getTaskByName(t, task.Name)
	_, _ = r.handleImplement(ctx, proj, refreshed)
	for _, lbl := range []string{"tatara-brainstorming", "tatara-implementation", "tatara-approved", "tatara-declined"} {
		require.NotContains(t, w.added, lbl, "PR-source task must not set phase label %s", lbl)
	}
}
