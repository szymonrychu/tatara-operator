package controller

// Tests for audit-r2 findings in writeback.go.
// Each test block is prefixed with the finding number it covers.

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// --- Finding 1: clearWritebackPending returns error ---

// errOnClearClient injects a non-conflict error on the first Status().Update
// so clearWritebackPending's RetryOnConflict exhausts and returns the error.
type errOnClearClient struct {
	client.Client
	calls *atomic.Int32
}

func (c *errOnClearClient) Status() client.SubResourceWriter {
	return &errOnClearWriter{SubResourceWriter: c.Client.Status(), calls: c.calls}
}

type errOnClearWriter struct {
	client.SubResourceWriter
	calls *atomic.Int32
}

func (w *errOnClearWriter) Update(_ context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if w.calls.Add(1) <= 1 {
		// Return a non-conflict error - RetryOnConflict does NOT retry these.
		return fmt.Errorf("injected transient error")
	}
	return w.SubResourceWriter.Update(context.Background(), obj, opts...)
}

// TestClearWritebackPending_ReturnsError verifies finding 1: clearWritebackPending
// returns an error when the status update fails so callers can propagate it.
func TestClearWritebackPending_ReturnsError(t *testing.T) {
	ctx := context.Background()

	mkSecret(t, "cwp-err-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "cwp-err-proj", Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{ScmSecretRef: "cwp-err-scm"},
	}
	require.NoError(t, k8sClient.Create(ctx, proj))
	repo := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "cwp-err-repo", Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef: "cwp-err-proj", URL: "https://github.com/o/r.git",
			DefaultBranch: "main", ReingestSchedule: "0 6 * * *",
		},
	}
	require.NoError(t, k8sClient.Create(ctx, repo))
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "cwp-err-task", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "cwp-err-proj", RepositoryRef: "cwp-err-repo", Goal: "g",
		},
	}
	require.NoError(t, k8sClient.Create(ctx, task))

	var calls atomic.Int32
	cc := &errOnClearClient{Client: k8sClient, calls: &calls}
	r := &TaskReconciler{
		Client:  cc,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Session: newFakeSession(),
	}

	err := r.clearWritebackPending(ctx, task, "TestReason", "msg")
	require.Error(t, err, "clearWritebackPending must return error when status update fails")
}

// TestDoWriteBack_ClearErrorPropagated verifies that when clearWritebackPending
// fails on an AlreadyWritten guard, doWriteBack surfaces the error so the
// reconciler requeues rather than returning nil and silently dropping the failure.
func TestDoWriteBack_ClearErrorPropagated(t *testing.T) {
	var calls atomic.Int32
	cc := &errOnClearClient{Client: k8sClient, calls: &calls}

	fw := &fullFakeSCMWriter{}
	r := &TaskReconciler{
		Client:  cc,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Session: newFakeSession(),
		PodConfig: agent.PodConfig{
			Namespace:           testNS,
			CallbackURL:         "http://op.svc:8082",
			OIDCIssuer:          "https://kc.svc/realms/master",
			AnthropicSecretName: "anthropic",
			CLIOIDCSecretName:   "tatara-cli-oidc",
		},
		SCMFor: func(string) (scm.SCMWriter, error) { return fw, nil },
	}
	// Seed a task that already has PrURL set (AlreadyWritten guard path).
	task := seedWritebackKindTask(t, "cwperr-alreadywritten", "cwperr-proj", "cwperr-repo", "cwperr-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "g", Kind: "implement",
			Source: &tatarav1alpha1.TaskSource{Provider: "github"},
		}, nil)
	task.Status.PrURL = "https://github.com/o/r/pull/1"
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.Error(t, err, "doWriteBack must propagate clearWritebackPending error from AlreadyWritten guard")
}

// --- Finding 2: 422 "already exists" recovery ---

// alreadyExistsFakeWriter returns a 422 "A pull request already exists" on
// OpenChange and allows GetPRState + ListOpenPRs for recovery.
type alreadyExistsFakeWriter struct {
	scm.SCMWriter
	mu         sync.Mutex
	openCalls  int
	getPRCalls int
	headBranch string // the branch GetPRState returns
	openErr    error
}

func (f *alreadyExistsFakeWriter) OpenChange(_ context.Context, _, _, _, _, _, _ string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.openCalls++
	return "", f.openErr
}

func (f *alreadyExistsFakeWriter) GetPRState(_ context.Context, _, _ string, _ int) (scm.PRState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getPRCalls++
	return scm.PRState{Author: "bot", HeadBranch: f.headBranch}, nil
}

func (f *alreadyExistsFakeWriter) GetIssueState(_ context.Context, _, _ string, _ int) (scm.IssueState, error) {
	return scm.IssueState{}, nil
}

func (f *alreadyExistsFakeWriter) Comment(_ context.Context, _, _, _ string) error { return nil }

// fakePRReader is a minimal SCMReader that returns one open PR.
type fakePRReader struct {
	prs []scm.PRRef
}

func (r *fakePRReader) ListOpenPRs(_ context.Context, _, _ string) ([]scm.PRRef, error) {
	return r.prs, nil
}
func (r *fakePRReader) ListOpenIssues(_ context.Context, _, _ string) ([]scm.IssueRef, error) {
	return nil, nil
}
func (r *fakePRReader) ListBoardItems(_ context.Context, _ scm.BoardRef) ([]scm.BoardItem, error) {
	return nil, nil
}
func (r *fakePRReader) GetCommitCIStatus(_ context.Context, _, _, _ string) (string, error) {
	return "", nil
}
func (r *fakePRReader) ListIssueComments(_ context.Context, _, _ string, _ int) ([]scm.IssueComment, error) {
	return nil, nil
}
func (r *fakePRReader) GetIssue(_ context.Context, _, _ string, _ int) (scm.IssueContent, error) {
	return scm.IssueContent{}, nil
}
func (r *fakePRReader) GetDefaultBranchHeadSHA(_ context.Context, _, _ string) (string, error) {
	return "", nil
}
func (r *fakePRReader) ListClosedIssues(_ context.Context, _, _ string, _ time.Time) ([]scm.IssueRef, error) {
	return nil, nil
}
func (r *fakePRReader) ListCommits(_ context.Context, _, _ string, _ time.Time) ([]scm.CommitRef, error) {
	return nil, nil
}

// TestWriteback_422AlreadyExists_RecoversPRURL verifies finding 2: when OpenChange
// returns 422 "A pull request already exists", the controller recovers the
// existing PR URL via ListOpenPRs+GetPRState so PrURL is set and the lifecycle
// path is not mis-routed into the empty-implement / 'refused' branch.
func TestWriteback_422AlreadyExists_RecoversPRURL(t *testing.T) {
	sourceBranch := "tatara/task-ae-task"
	fw := &alreadyExistsFakeWriter{
		openErr:    &scm.HTTPError{Status: 422, Body: "A pull request already exists for o:tatara/task-ae-task", Path: "/repos/o/r/pulls"},
		headBranch: sourceBranch,
	}
	reader := &fakePRReader{
		prs: []scm.PRRef{{Repo: "o/r", Number: 55, Author: "bot", HeadBranch: sourceBranch}},
	}

	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Session: newFakeSession(),
		PodConfig: agent.PodConfig{
			Namespace:           testNS,
			CallbackURL:         "http://op.svc:8082",
			OIDCIssuer:          "https://kc.svc/realms/master",
			AnthropicSecretName: "anthropic",
			CLIOIDCSecretName:   "tatara-cli-oidc",
		},
		SCMFor: func(string) (scm.SCMWriter, error) { return fw, nil },
		ReaderFor: func(_, _ string) (scm.SCMReader, error) {
			return reader, nil
		},
	}

	task := seedWritebackPending(t, "ae-task", "ae-scm", "ae-proj", "ae-repo")

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))

	// PrURL must be set to the recovered PR URL, not empty.
	require.NotEmpty(t, got.Status.PrURL, "PrURL must be recovered from existing PR on 422 already-exists")
	require.Contains(t, got.Status.PrURL, "55", "PrURL must reference PR #55")
}

// --- Finding 3: findOpenIssueByTitle uses GitLabProjectPath ---

// gitlabProjectPathReader records the owner/repo passed to ListOpenIssues.
type gitlabProjectPathReader struct {
	owner string
	repo  string
}

func (r *gitlabProjectPathReader) ListOpenIssues(_ context.Context, owner, repo string) ([]scm.IssueRef, error) {
	r.owner = owner
	r.repo = repo
	return nil, nil
}
func (r *gitlabProjectPathReader) ListOpenPRs(_ context.Context, _, _ string) ([]scm.PRRef, error) {
	return nil, nil
}
func (r *gitlabProjectPathReader) ListBoardItems(_ context.Context, _ scm.BoardRef) ([]scm.BoardItem, error) {
	return nil, nil
}
func (r *gitlabProjectPathReader) GetCommitCIStatus(_ context.Context, _, _, _ string) (string, error) {
	return "", nil
}
func (r *gitlabProjectPathReader) ListIssueComments(_ context.Context, _, _ string, _ int) ([]scm.IssueComment, error) {
	return nil, nil
}
func (r *gitlabProjectPathReader) GetIssue(_ context.Context, _, _ string, _ int) (scm.IssueContent, error) {
	return scm.IssueContent{}, nil
}
func (r *gitlabProjectPathReader) GetDefaultBranchHeadSHA(_ context.Context, _, _ string) (string, error) {
	return "", nil
}
func (r *gitlabProjectPathReader) ListClosedIssues(_ context.Context, _, _ string, _ time.Time) ([]scm.IssueRef, error) {
	return nil, nil
}
func (r *gitlabProjectPathReader) ListCommits(_ context.Context, _, _ string, _ time.Time) ([]scm.CommitRef, error) {
	return nil, nil
}

// TestFindOpenIssueByTitle_GitLabProjectPath verifies finding 3: for GitLab
// repos findOpenIssueByTitle must pass the full project path (e.g.
// "group/sub/proj") as owner and "" as repo, not "owner/" which 404s.
func TestFindOpenIssueByTitle_GitLabProjectPath(t *testing.T) {
	glReader := &gitlabProjectPathReader{}
	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Session: newFakeSession(),
		ReaderFor: func(_, _ string) (scm.SCMReader, error) {
			return glReader, nil
		},
	}
	proj := &tatarav1alpha1.Project{
		Spec: tatarav1alpha1.ProjectSpec{
			Scm: &tatarav1alpha1.ScmSpec{Provider: "gitlab", Owner: "topgroup"},
		},
	}

	// Subgroup GitLab URL: OwnerRepo returns last two segments ("sub/proj"),
	// dropping "topgroup" - the old code would call ListOpenIssues("topgroup", "")
	// which builds "topgroup/" as the project path.
	repoURL := "https://gitlab.example.com/topgroup/sub/proj.git"
	_, _, _ = r.findOpenIssueByTitle(context.Background(), proj, repoURL, "tok", "Idea")

	require.Equal(t, "topgroup/sub/proj", glReader.owner,
		"GitLab: owner must be the full project path, not just topgroup")
	require.Equal(t, "", glReader.repo,
		"GitLab: repo must be empty when full path is passed as owner")
}

// --- Finding 5: writeBackReview 'comment' uses PR number, not IssueRef ---

// commentTargetFakeWriter records what issueRef the Comment call received.
type commentTargetFakeWriter struct {
	scm.SCMWriter
	mu              sync.Mutex
	commentIssueRef string
}

func (f *commentTargetFakeWriter) GetIssueState(_ context.Context, _, _ string, _ int) (scm.IssueState, error) {
	return scm.IssueState{}, nil
}
func (f *commentTargetFakeWriter) GetPRState(_ context.Context, _, _ string, _ int) (scm.PRState, error) {
	return scm.PRState{}, nil
}
func (f *commentTargetFakeWriter) Comment(_ context.Context, _, issueRef, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commentIssueRef = issueRef
	return nil
}

// TestWriteBackReview_CommentUsesNumber verifies finding 5: a 'comment' review
// verdict must be posted to owner/repo#number (same addressing as approve/RC),
// not to Source.IssueRef (which may be the originating issue or empty).
func TestWriteBackReview_CommentUsesNumber(t *testing.T) {
	fw := &commentTargetFakeWriter{}
	r := newFullFakeReconciler(t, &fullFakeSCMWriter{}) // for type; we override SCMFor below
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	task := seedWritebackKindTask(t, "rev-comment-num", "rcn-proj", "rcn-repo", "rcn-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "review",
			Kind: "review",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github",
				// IssueRef points to the originating issue, not the PR.
				IssueRef: "o/r#10",
				IsPR:     true,
				Number:   42,
			},
		}, nil)
	task.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{Decision: "comment", Body: "looks good"}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	fw.mu.Lock()
	ref := fw.commentIssueRef
	fw.mu.Unlock()

	// Must address the PR (number=42), not the issue (IssueRef="o/r#10").
	require.Contains(t, ref, "#42",
		"comment verdict must target PR number, not Source.IssueRef")
}

// --- Finding 7: writeback skip/no-change/no-PR business outcomes have metrics ---

// TestWriteback_Metrics_NoChange verifies finding 7: when OpenChange returns 422
// "No commits between", the no_change metric must be incremented.
func TestWriteback_Metrics_NoChange(t *testing.T) {
	reg := prometheus.NewRegistry()
	fw := &trackingFakeWriter{
		openErr: &scm.HTTPError{Status: 422, Body: "No commits between main and tatara/task-nc", Path: "/repos/o/r/pulls"},
	}
	r := newTrackingReconciler(t, fw, reg)
	task := seedWritebackPending(t, "m7-nochange", "m7nc-scm", "m7nc-proj", "m7nc-repo")

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	cnt := testutil.ToFloat64(r.Metrics.WritebackOutcomeCounter("no_change"))
	require.Equal(t, float64(1), cnt, "no_change metric must be emitted on 422 no-commits")
}

// TestWriteback_Metrics_NoPR verifies finding 7: when no PR is opened (all repos
// skip), the no_pr metric must be incremented.
func TestWriteback_Metrics_NoPR(t *testing.T) {
	reg := prometheus.NewRegistry()
	fw := &trackingFakeWriter{
		openErr: &scm.HTTPError{Status: 422, Body: "Some other 422", Path: "/repos/o/r/pulls"},
	}
	r := newTrackingReconciler(t, fw, reg)
	task := seedWritebackPending(t, "m7-nopr", "m7np-scm", "m7np-proj", "m7np-repo")

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	cnt := testutil.ToFloat64(r.Metrics.WritebackOutcomeCounter("no_pr"))
	require.Equal(t, float64(1), cnt, "no_pr metric must be emitted when no PR is opened")
}

// TestWriteback_Metrics_Opened verifies finding 7: when a PR is successfully
// opened, the opened metric must be incremented.
func TestWriteback_Metrics_Opened(t *testing.T) {
	reg := prometheus.NewRegistry()
	fw := &trackingFakeWriter{}
	r := newTrackingReconciler(t, fw, reg)
	task := seedWritebackPending(t, "m7-opened", "m7op-scm", "m7op-proj", "m7op-repo")

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	cnt := testutil.ToFloat64(r.Metrics.WritebackOutcomeCounter("opened"))
	require.Equal(t, float64(1), cnt, "opened metric must be emitted on successful PR open")
}

// --- Finding 8: mergeAllowed has no unused parameters ---

// TestMergeAllowed_SignatureKISS verifies finding 8: mergeAllowed compiles
// with only (proj, st) params (compile-time check via call in test).
func TestMergeAllowed_SignatureKISS(t *testing.T) {
	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
	}
	proj := &tatarav1alpha1.Project{
		Spec: tatarav1alpha1.ProjectSpec{
			Scm: &tatarav1alpha1.ScmSpec{MergePolicy: "autoMergeOnGreenCI"},
		},
	}
	// CI green -> allowed.
	ok := r.mergeAllowed(proj, scm.PRState{CIStatus: "success"})
	require.True(t, ok, "autoMergeOnGreenCI must allow merge when CI is green")

	// CI failure -> not allowed.
	ok = r.mergeAllowed(proj, scm.PRState{CIStatus: "failure"})
	require.False(t, ok, "autoMergeOnGreenCI must deny merge when CI is failure")
}

// --- Finding 9: writeBackReview drops _ = proj ---

// TestWriteBackReview_NoProjBinding verifies finding 9: writeBackReview does
// not reference proj at all (no dead binding). This is a compile-time structural
// check; we verify the approve path works without any proj dependency.
func TestWriteBackReview_NoProjBinding(t *testing.T) {
	// no-test: finding 9 is a dead-binding removal (_=proj dropped); verifying
	// via compile success + the approve/request_changes tests that exercise the
	// full writeBackReview path without needing proj.
	// The approve test in TestDoWriteBackKind already exercises this path; we
	// add a dedicated assertion that approve posts to the correct PR number.
	fw := &fullFakeSCMWriter{}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "f9-nodrop", "f9-proj", "f9-repo", "f9-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "review",
			Kind: "review",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IsPR: true, Number: 88,
			},
		}, nil)
	task.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{Decision: "approve", Body: "lgtm"}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)
	require.True(t, fw.approveCalled, "Approve must be called")
	require.Equal(t, 88, fw.approveNumber, "Approve must target the correct PR number")
}

// --- Compile-time interface checks ---

var _ scm.SCMReader = (*fakePRReader)(nil)
var _ scm.SCMReader = (*gitlabProjectPathReader)(nil)

// --- errOnClearClient must satisfy client.Client ---

// Satisfy client.Client interface methods not overridden.
func (c *errOnClearClient) Get(ctx context.Context, key types.NamespacedName, obj client.Object, opts ...client.GetOption) error {
	return c.Client.Get(ctx, key, obj, opts...)
}

// errOnClearWriter must satisfy client.SubResourceWriter.
var _ client.SubResourceWriter = (*errOnClearWriter)(nil)

// apierrors conflict helper for errOnClearWriter - unused but kept for potential reuse.
var _ = apierrors.NewConflict(schema.GroupResource{}, "", nil)
var _ = apimeta.SetStatusCondition
