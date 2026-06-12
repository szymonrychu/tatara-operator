package controller

// Tests for Fix 4: createProposal duplicate-issue prevention.
//
// Three layers:
//   (A) source-set idempotency guard: if task.Spec.Source already has a URL,
//       CreateIssue must NOT be called again.
//   (B) RetryOnConflict on the Spec.Source record update: even when the first
//       r.Update returns a Conflict, CreateIssue must be called exactly once
//       and Source must be recorded.
//   (C) Title-level idempotency: if the reader returns an existing open issue
//       with the same title, CreateIssue must NOT be called; Source is set to
//       the existing issue.  When ReaderFor is nil the title check is skipped
//       and CreateIssue proceeds normally.

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// fakeProposalWriter counts CreateIssue calls and returns a fixed ref.
type fakeProposalWriter struct {
	scm.SCMWriter
	mu          sync.Mutex
	createCalls int
}

func (f *fakeProposalWriter) CreateIssue(_ context.Context, _, _ string, _ scm.IssueReq) (scm.CreatedIssue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCalls++
	return scm.CreatedIssue{Ref: "o/r#99", URL: "https://github.com/o/r/issues/99"}, nil
}

func (f *fakeProposalWriter) AddBoardItem(_ context.Context, _ string, _ scm.BoardRef, _ string) error {
	return nil
}

func (f *fakeProposalWriter) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.createCalls
}

// fakeProposalReader returns a configurable list of open issues.
type fakeProposalReader struct {
	issues []scm.IssueRef
}

func (f *fakeProposalReader) ListOpenIssues(_ context.Context, _, _ string) ([]scm.IssueRef, error) {
	return f.issues, nil
}

func (f *fakeProposalReader) ListOpenPRs(_ context.Context, _, _ string) ([]scm.PRRef, error) {
	return nil, nil
}

func (f *fakeProposalReader) ListBoardItems(_ context.Context, _ scm.BoardRef) ([]scm.BoardItem, error) {
	return nil, nil
}

func (f *fakeProposalReader) GetCommitCIStatus(_ context.Context, _, _, _ string) (string, error) {
	return "", nil
}

// seedProposalTask creates the minimum objects for createProposal: a secret,
// project with scm spec + scmSecretRef, a repository, and a Task with
// ProposedIssue set.  Returns the seeded Task (server-round-tripped).
func seedProposalTask(t *testing.T, name, proj, repo, secret, proposalTitle string) *tatarav1alpha1.Task {
	t.Helper()
	ctx := context.Background()

	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secret, Namespace: testNS},
		Data:       map[string][]byte{"token": []byte("tok"), "webhookSecret": []byte("w")},
	}
	require.NoError(t, k8sClient.Create(ctx, sec))

	project := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: proj, Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: secret,
			Scm: &tatarav1alpha1.ScmSpec{
				Provider: "github",
				Owner:    "o",
				BotLogin: "tatara-bot",
			},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, project))
	project.Status.Memory = &tatarav1alpha1.MemoryStatus{Phase: "Ready", Endpoint: "http://mem.svc"}
	require.NoError(t, k8sClient.Status().Update(ctx, project))

	r := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: repo, Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef:       proj,
			URL:              "https://github.com/o/r.git",
			DefaultBranch:    "main",
			ReingestSchedule: "0 6 * * *",
		},
	}
	require.NoError(t, k8sClient.Create(ctx, r))

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    proj,
			RepositoryRef: repo,
			Kind:          "implement",
			Goal:          proposalTitle,
			ProposedIssue: &tatarav1alpha1.ProposedIssueSpec{
				RepositoryRef: repo,
				Title:         proposalTitle,
				Body:          "description of the proposal",
				Kind:          "improvement",
			},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, task))

	var fresh tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, &fresh))
	return &fresh
}

// newProposalReconciler builds a TaskReconciler wired with the given writer and
// optional reader.  Pass nil readerFor to simulate no-reader (tests that
// ReaderFor nil does not panic).
func newProposalReconciler(t *testing.T, fw scm.SCMWriter, readerFor func(provider, token string) (scm.SCMReader, error)) *TaskReconciler {
	t.Helper()
	return &TaskReconciler{
		Client:    k8sClient,
		Scheme:    k8sClient.Scheme(),
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
		SCMFor:    func(string) (scm.SCMWriter, error) { return fw, nil },
		ReaderFor: readerFor,
	}
}

// --- (A) Source-already-set guard ---

// TestCreateProposal_SourceAlreadySet verifies that if task.Spec.Source is
// already populated (URL non-empty) createProposal returns without calling
// CreateIssue, and advances the Task to AwaitingApproval.
func TestCreateProposal_SourceAlreadySet(t *testing.T) {
	fw := &fakeProposalWriter{}
	r := newProposalReconciler(t, fw, nil)

	task := seedProposalTask(t, "prop-src-set", "prop-src-proj", "prop-src-repo", "prop-src-scm", "My Proposal")

	// Pre-set Source to simulate a prior successful createProposal call.
	task.Spec.Source = &tatarav1alpha1.TaskSource{
		Provider: "github",
		IssueRef: "o/r#99",
		URL:      "https://github.com/o/r/issues/99",
	}
	require.NoError(t, k8sClient.Update(context.Background(), task))
	// Reload server state.
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, task))

	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))

	_, err := r.createProposal(context.Background(), &proj, task)
	require.NoError(t, err)

	// CreateIssue must NOT have been called.
	require.Zero(t, fw.calls(), "CreateIssue must not be called when Source.URL is already set")

	// Task status should be AwaitingApproval.
	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	require.Equal(t, "AwaitingApproval", got.Status.Phase, "phase must be AwaitingApproval")

	// WritebackPending must be False.
	cond := findCond(got.Status.Conditions, "WritebackPending")
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status, "WritebackPending must be False after source-set guard")
}

// --- (B) RetryOnConflict on Spec.Source record ---

// conflictOnceTaskSpecClient injects one Conflict on the first r.Update (spec
// update) for Task objects.
type conflictOnceTaskSpecClient struct {
	client.Client
	calls *atomic.Int32
}

func (c *conflictOnceTaskSpecClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if _, ok := obj.(*tatarav1alpha1.Task); ok {
		if c.calls.Add(1) == 1 {
			return apierrors.NewConflict(schema.GroupResource{Group: "tatara.dev", Resource: "tasks"}, obj.GetName(), nil)
		}
	}
	return c.Client.Update(ctx, obj, opts...)
}

// TestCreateProposal_ConflictOnSourceRecord verifies that when the first
// r.Update (recording Spec.Source) returns a Conflict, createProposal retries
// and CreateIssue is called exactly once.
func TestCreateProposal_ConflictOnSourceRecord(t *testing.T) {
	fw := &fakeProposalWriter{}

	var calls atomic.Int32
	cc := &conflictOnceTaskSpecClient{Client: k8sClient, calls: &calls}

	r := &TaskReconciler{
		Client:  cc,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		SCMFor:  func(string) (scm.SCMWriter, error) { return fw, nil },
	}

	task := seedProposalTask(t, "prop-conflict", "prop-conflict-proj", "prop-conflict-repo", "prop-conflict-scm", "Conflict Proposal")

	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))

	_, err := r.createProposal(context.Background(), &proj, task)
	require.NoError(t, err)

	// CreateIssue called exactly once (no duplicate).
	require.Equal(t, 1, fw.calls(), "CreateIssue must be called exactly once despite conflict")

	// Source must be recorded on the task.
	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	require.NotNil(t, got.Spec.Source, "Spec.Source must be set after conflict retry")
	require.NotEmpty(t, got.Spec.Source.URL, "Spec.Source.URL must be non-empty after conflict retry")

	require.GreaterOrEqual(t, calls.Load(), int32(2), "Update must have been called at least twice (once conflict, once success)")
}

// --- (C) Title-level idempotency ---

// TestCreateProposal_TitleDuplicateSkipsCreate verifies that when the reader
// returns an open issue with the same title, CreateIssue is NOT called and the
// task's Source is set to the existing issue.
func TestCreateProposal_TitleDuplicateSkipsCreate(t *testing.T) {
	fw := &fakeProposalWriter{}

	existingIssue := scm.IssueRef{
		Repo:   "o/r",
		Number: 42,
		Title:  "My Duplicate Proposal",
	}
	reader := &fakeProposalReader{issues: []scm.IssueRef{existingIssue}}

	r := newProposalReconciler(t, fw, func(_, _ string) (scm.SCMReader, error) {
		return reader, nil
	})

	task := seedProposalTask(t, "prop-title-dup", "prop-title-proj", "prop-title-repo", "prop-title-scm", "My Duplicate Proposal")

	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))

	_, err := r.createProposal(context.Background(), &proj, task)
	require.NoError(t, err)

	// CreateIssue must NOT be called.
	require.Zero(t, fw.calls(), "CreateIssue must not be called when a duplicate title exists")

	// Source must point at the existing issue.
	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	require.NotNil(t, got.Spec.Source, "Spec.Source must be set to the existing issue")
	require.Equal(t, 42, got.Spec.Source.Number, "Source.Number must match the existing issue")

	// Phase must be AwaitingApproval.
	require.Equal(t, "AwaitingApproval", got.Status.Phase)

	// WritebackPending must be False.
	cond := findCond(got.Status.Conditions, "WritebackPending")
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status)
}

// TestCreateProposal_ReaderForNilProceedsNormally verifies that when ReaderFor
// is nil the title-check is skipped and CreateIssue proceeds normally.
func TestCreateProposal_ReaderForNilProceedsNormally(t *testing.T) {
	fw := &fakeProposalWriter{}
	r := newProposalReconciler(t, fw, nil) // nil readerFor

	task := seedProposalTask(t, "prop-no-reader", "prop-no-reader-proj", "prop-no-reader-repo", "prop-no-reader-scm", "No Reader Proposal")

	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))

	_, err := r.createProposal(context.Background(), &proj, task)
	require.NoError(t, err)

	// CreateIssue must be called when reader is nil.
	require.Equal(t, 1, fw.calls(), "CreateIssue must be called when ReaderFor is nil")
}

// TestCreateProposal_TitleNoMatchProceedsNormally verifies that when the reader
// returns issues but none match the title, CreateIssue proceeds normally.
func TestCreateProposal_TitleNoMatchProceedsNormally(t *testing.T) {
	fw := &fakeProposalWriter{}

	// Different title - no match.
	reader := &fakeProposalReader{issues: []scm.IssueRef{
		{Repo: "o/r", Number: 11, Title: "Some Unrelated Issue"},
	}}

	r := newProposalReconciler(t, fw, func(_, _ string) (scm.SCMReader, error) {
		return reader, nil
	})

	task := seedProposalTask(t, "prop-no-match", "prop-no-match-proj", "prop-no-match-repo", "prop-no-match-scm", "Brand New Proposal")

	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))

	_, err := r.createProposal(context.Background(), &proj, task)
	require.NoError(t, err)

	// No title match -> CreateIssue proceeds.
	require.Equal(t, 1, fw.calls(), "CreateIssue must be called when no title match")
}

// --- verify fix 3 still intact: createProposal routes through Phase transition ---

// TestCreateProposal_SetsAwaitingApproval verifies the happy path: a fresh Task
// with ProposedIssue gets CreateIssue called once and transitions to AwaitingApproval.
func TestCreateProposal_HappyPath(t *testing.T) {
	fw := &fakeProposalWriter{}
	r := newProposalReconciler(t, fw, nil)

	task := seedProposalTask(t, "prop-happy", "prop-happy-proj", "prop-happy-repo", "prop-happy-scm", "Happy Proposal")

	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))

	_, err := r.createProposal(context.Background(), &proj, task)
	require.NoError(t, err)

	require.Equal(t, 1, fw.calls(), "CreateIssue must be called once on happy path")

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	require.NotNil(t, got.Spec.Source, "Spec.Source must be set on happy path")
	require.Equal(t, "https://github.com/o/r/issues/99", got.Spec.Source.URL)
	require.Equal(t, "AwaitingApproval", got.Status.Phase)

	// ApprovalApproved condition must be False.
	cond := apimeta.FindStatusCondition(got.Status.Conditions, tatarav1alpha1.ConditionApprovalApproved)
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status)
}
