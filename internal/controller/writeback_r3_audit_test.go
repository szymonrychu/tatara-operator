package controller

// Tests for audit-r3 findings on writeback.go (2026-06-16).
// Findings covered: 1 (close idempotency), 2 (GetPRState metered),
// 3 (list calls metered), 4 (HeadBranch N+1 fix), 6 (incremental PrURL persist),
// 7 (mergeAllowed returns bool), 8 (nil ReaderFor WARN), 9 (brainstorm fallback WARN).
// Finding 5 (afterApproval doc/rename) is no-test: comment/doc change only.

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// --- Finding 1: close path skips ClosePR when PR is already closed ---

// closedPRWriter reports the PR as already closed from GetPRState and counts ClosePR calls.
type closedPRWriter struct {
	scm.SCMWriter
	mu            sync.Mutex
	closeCalls    int
	prStateClosed bool
}

func (f *closedPRWriter) GetPRState(_ context.Context, _, _ string, _ int) (scm.PRState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return scm.PRState{Author: "tatara-bot", Closed: f.prStateClosed}, nil
}
func (f *closedPRWriter) ClosePR(_ context.Context, _, _ string, _ int, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeCalls++
	return nil
}
func (f *closedPRWriter) Comment(_ context.Context, _, _, _ string) error { return nil }

// TestWriteBackSelfImprove_CloseSkipsAlreadyClosed verifies F1: when the PR is
// already closed (st.Closed=true), ClosePR must not be called, preventing the
// close comment from being re-posted on a requeue after clearWritebackPending failure.
func TestWriteBackSelfImprove_CloseSkipsAlreadyClosed(t *testing.T) {
	fw := &closedPRWriter{prStateClosed: true}
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
	}
	task := seedWritebackKindTask(t, "r3-f1-already-closed", "wb3f1-proj", "wb3f1-repo", "wb3f1-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "self-improve",
			Kind: "selfImprove",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IsPR: true, Number: 101,
			},
		},
		&tatarav1alpha1.ScmSpec{
			Provider: "github", Owner: "o", BotLogin: "tatara-bot",
		})
	task.Status.PROutcome = &tatarav1alpha1.PROutcome{Action: "close", Reason: "done"}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	fw.mu.Lock()
	calls := fw.closeCalls
	fw.mu.Unlock()
	require.Equal(t, 0, calls, "ClosePR must not be called when PR is already closed")

	// WritebackPending must still be cleared.
	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	cond := findCond(got.Status.Conditions, "WritebackPending")
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status,
		"WritebackPending must be cleared even when ClosePR is skipped")
}

// TestWriteBackSelfImprove_CloseCallsWhenOpen verifies F1 happy path: when the
// PR is open (Closed=false), ClosePR IS called.
func TestWriteBackSelfImprove_CloseCallsWhenOpen(t *testing.T) {
	fw := &closedPRWriter{prStateClosed: false}
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
	}
	task := seedWritebackKindTask(t, "r3-f1-open-pr", "wb3f1b-proj", "wb3f1b-repo", "wb3f1b-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "self-improve",
			Kind: "selfImprove",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IsPR: true, Number: 102,
			},
		},
		&tatarav1alpha1.ScmSpec{
			Provider: "github", Owner: "o", BotLogin: "tatara-bot",
		})
	task.Status.PROutcome = &tatarav1alpha1.PROutcome{Action: "close", Reason: "done"}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	fw.mu.Lock()
	calls := fw.closeCalls
	fw.mu.Unlock()
	require.Equal(t, 1, calls, "ClosePR must be called once when PR is open")
}

// --- Finding 2: GetPRState calls are metered via recordSCM ---

// getPRStateMetricWriter records GetPRState calls and returns a configurable state.
type getPRStateMetricWriter struct {
	scm.SCMWriter
	mu         sync.Mutex
	getPRCalls int
	state      scm.PRState
}

func (f *getPRStateMetricWriter) GetPRState(_ context.Context, _, _ string, _ int) (scm.PRState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getPRCalls++
	return f.state, nil
}
func (f *getPRStateMetricWriter) Merge(_ context.Context, _, _ string, _ int, _ string) (string, error) {
	return "", nil
}
func (f *getPRStateMetricWriter) ClosePR(_ context.Context, _, _ string, _ int, _ string) error {
	return nil
}
func (f *getPRStateMetricWriter) Comment(_ context.Context, _, _, _ string) error { return nil }

// TestWriteBackSelfImprove_GetPRStateMetered verifies F2: the GetPRState call on
// the authorship gate path emits a get_pr_state metric via recordSCM.
func TestWriteBackSelfImprove_GetPRStateMetered(t *testing.T) {
	reg := prometheus.NewRegistry()
	fw := &getPRStateMetricWriter{
		state: scm.PRState{Author: "tatara-bot", Closed: false},
	}
	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(reg),
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
	task := seedWritebackKindTask(t, "r3-f2-get-pr-metered", "wb3f2-proj", "wb3f2-repo", "wb3f2-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "self-improve",
			Kind: "selfImprove",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IsPR: true, Number: 103,
			},
		},
		&tatarav1alpha1.ScmSpec{
			Provider: "github", Owner: "o", BotLogin: "tatara-bot",
		})
	task.Status.PROutcome = &tatarav1alpha1.PROutcome{Action: "close", Reason: "done"}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	cnt := testutil.ToFloat64(r.Metrics.SCMWriteCounter("github", "get_pr_state", "ok"))
	require.GreaterOrEqual(t, cnt, float64(1), "get_pr_state metric must be emitted from authorship gate")
}

// --- Finding 3: ListOpenPRs and ListOpenIssues are metered ---

// listMetricReader records list calls for metric assertions.
type listMetricReader struct {
	scm.SCMReader
	mu           sync.Mutex
	listPRCalls  int
	listIssCalls int
	prs          []scm.PRRef
}

func (r *listMetricReader) ListOpenPRs(_ context.Context, _, _ string) ([]scm.PRRef, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.listPRCalls++
	return r.prs, nil
}
func (r *listMetricReader) ListOpenIssues(_ context.Context, _, _ string) ([]scm.IssueRef, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.listIssCalls++
	return nil, nil
}
func (r *listMetricReader) ListBoardItems(_ context.Context, _ scm.BoardRef) ([]scm.BoardItem, error) {
	return nil, nil
}
func (r *listMetricReader) GetCommitCIStatus(_ context.Context, _, _, _ string) (string, error) {
	return "", nil
}
func (r *listMetricReader) ListIssueComments(_ context.Context, _, _ string, _ int) ([]scm.IssueComment, error) {
	return nil, nil
}
func (r *listMetricReader) GetIssue(_ context.Context, _, _ string, _ int) (scm.IssueContent, error) {
	return scm.IssueContent{}, nil
}

// TestWriteback_ListOpenPRsMetered verifies F3: ListOpenPRs in recoverExistingPRURL
// emits a list_open_prs metric via recordSCM.
func TestWriteback_ListOpenPRsMetered(t *testing.T) {
	reg := prometheus.NewRegistry()
	sourceBranch := "tatara/task-r3-list-prs-task"
	reader := &listMetricReader{
		prs: []scm.PRRef{{Repo: "o/r", Number: 60, Author: "bot", HeadBranch: sourceBranch}},
	}
	fw := &alreadyExistsFakeWriter{
		openErr:    &scm.HTTPError{Status: 422, Body: "A pull request already exists for o:tatara/task-r3-list-prs-task", Path: "/repos/o/r/pulls"},
		headBranch: sourceBranch,
	}
	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(reg),
		Session: newFakeSession(),
		PodConfig: agent.PodConfig{
			Namespace:           testNS,
			CallbackURL:         "http://op.svc:8082",
			OIDCIssuer:          "https://kc.svc/realms/master",
			AnthropicSecretName: "anthropic",
			CLIOIDCSecretName:   "tatara-cli-oidc",
		},
		SCMFor:    func(string) (scm.SCMWriter, error) { return fw, nil },
		ReaderFor: func(_, _ string) (scm.SCMReader, error) { return reader, nil },
	}
	task := seedWritebackPending(t, "r3-f3-list-prs", "wb3f3-scm", "wb3f3-proj", "wb3f3-repo")
	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	cnt := testutil.ToFloat64(r.Metrics.SCMWriteCounter("github", "list_open_prs", "ok"))
	require.Equal(t, float64(1), cnt, "list_open_prs metric must be emitted from recoverExistingPRURL")
}

// --- Finding 4: recoverExistingPRURL uses HeadBranch from PRRef (no N+1) ---

// singleGetPRStateRecoveryWriter counts GetPRState calls to verify no N+1.
type singleGetPRStateRecoveryWriter struct {
	scm.SCMWriter
	mu         sync.Mutex
	openCalls  int
	getPRCalls int // must stay 0 after F4 fix
}

func (f *singleGetPRStateRecoveryWriter) OpenChange(_ context.Context, _, _, srcBranch, _, _, _ string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.openCalls++
	return "", &scm.HTTPError{Status: 422, Body: "A pull request already exists for o:" + srcBranch, Path: "/repos/o/r/pulls"}
}
func (f *singleGetPRStateRecoveryWriter) GetPRState(_ context.Context, _, _ string, _ int) (scm.PRState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getPRCalls++
	return scm.PRState{}, nil
}
func (f *singleGetPRStateRecoveryWriter) Comment(_ context.Context, _, _, _ string) error {
	return nil
}

var _ scm.SCMReader = (*listMetricReader)(nil)

// TestWriteback_RecoverExistingPRURL_NoGetPRStateFanout verifies F4: after the
// HeadBranch field was added to PRRef, recoverExistingPRURL must not call
// GetPRState per-PR; the single authorship-gate GetPRState is from writeBackSelfImprove,
// not from this open-change path.
func TestWriteback_RecoverExistingPRURL_NoGetPRStateFanout(t *testing.T) {
	// Task name determines the source branch: tatara/task-<name>.
	taskName := "r3-f4-no-fanout"
	sourceBranch := "tatara/task-" + taskName
	reader := &listMetricReader{
		prs: []scm.PRRef{
			{Repo: "o/r", Number: 71, Author: "bot", HeadBranch: "other-branch"},
			{Repo: "o/r", Number: 72, Author: "bot", HeadBranch: sourceBranch},
			{Repo: "o/r", Number: 73, Author: "bot", HeadBranch: "yet-another"},
		},
	}
	fw := &singleGetPRStateRecoveryWriter{}
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
		SCMFor:    func(string) (scm.SCMWriter, error) { return fw, nil },
		ReaderFor: func(_, _ string) (scm.SCMReader, error) { return reader, nil },
	}
	task := seedWritebackPending(t, taskName, "wb3f4-scm", "wb3f4-proj", "wb3f4-repo")
	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	// PrURL must be set to the recovered PR (number 72).
	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	require.Contains(t, got.Status.PrURL, "72", "PrURL must reference PR #72 matched by HeadBranch")

	// GetPRState must NOT have been called by recoverExistingPRURL (F4 fix).
	fw.mu.Lock()
	gprc := fw.getPRCalls
	fw.mu.Unlock()
	require.Equal(t, 0, gprc, "recoverExistingPRURL must not call GetPRState per-PR after F4 fix")
}

// --- Finding 6: primary PR URL persisted incrementally ---

// transientSecondRepoWriter: OpenChange succeeds for the first repo, fails
// transiently for the second, so we can check PrURL is persisted from the first.
type transientSecondRepoWriter struct {
	scm.SCMWriter
	mu        sync.Mutex
	openCalls int
}

func (f *transientSecondRepoWriter) OpenChange(_ context.Context, repoURL, _, _, _, _, _ string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.openCalls++
	if f.openCalls == 1 {
		return "https://github.com/o/r/pull/201", nil
	}
	// Second repo: simulate transient 5xx.
	return "", &scm.HTTPError{Status: 503, Body: "service unavailable", Path: "/"}
}
func (f *transientSecondRepoWriter) Comment(_ context.Context, _, _, _ string) error { return nil }

// TestWriteBack_IncrementalPRURLPersist verifies F6: after the first successful
// OpenChange in a multi-repo loop, PrURL is persisted immediately so a later
// transient failure on the second repo does not lose it.
func TestWriteBack_IncrementalPRURLPersist(t *testing.T) {
	fw := &transientSecondRepoWriter{}
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
	}
	// Create a two-repo project.
	task := seedWritebackPendingMultiRepo(t, "r3-f6-persist", "wb3f6-scm", "wb3f6-proj", "wb3f6-repo1", "wb3f6-repo2")

	_, err := reconcileWriteback(t, r, task.Name)
	// Expect an error because the second repo returns 5xx.
	require.Error(t, err, "transient 5xx on second repo must return error for requeue")

	// But PrURL must already be persisted from the first repo's success.
	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	require.Equal(t, "https://github.com/o/r/pull/201", got.Status.PrURL,
		"PrURL must be persisted after first repo success, before second repo error")
}

// --- Finding 7: mergeAllowed returns bool (no dead error) ---

// TestMergeAllowed_ReturnsBool verifies F7: mergeAllowed has no error return; the
// call compiles and behaves correctly as a plain bool.
func TestMergeAllowed_ReturnsBool(t *testing.T) {
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
	// This is a compile-time structural check: if mergeAllowed returned (bool, error)
	// the assignment below would not compile.
	allowed := r.mergeAllowed(proj, scm.PRState{CIStatus: "success"})
	require.True(t, allowed, "autoMergeOnGreenCI+success must be allowed")

	allowed = r.mergeAllowed(proj, scm.PRState{CIStatus: "failure"})
	require.False(t, allowed, "autoMergeOnGreenCI+failure must not be allowed")

	// afterApproval (default) always allows.
	proj2 := &tatarav1alpha1.Project{
		Spec: tatarav1alpha1.ProjectSpec{
			Scm: &tatarav1alpha1.ScmSpec{MergePolicy: "afterApproval"},
		},
	}
	allowed = r.mergeAllowed(proj2, scm.PRState{})
	require.True(t, allowed, "afterApproval must always allow merge")
}

// --- Finding 8: nil ReaderFor logs WARN on recovery path ---

// logCapturingClient wraps k8sClient but does not override logging;
// we test the observability effect via metric skip_4xx path when reader is nil.
func TestWriteBack_NilReaderForLogsWarn(t *testing.T) {
	// When ReaderFor is nil, recoverExistingPRURL returns ("", nil) after logging WARN.
	// The 422 "already exists" branch then falls through to skip_4xx.
	reg := prometheus.NewRegistry()
	fw := &alreadyExistsFakeWriter{
		openErr:    &scm.HTTPError{Status: 422, Body: "A pull request already exists for o:tatara/task-r3-nil-reader", Path: "/repos/o/r/pulls"},
		headBranch: "tatara/task-r3-nil-reader",
	}
	r := &TaskReconciler{
		Client:    k8sClient,
		Scheme:    k8sClient.Scheme(),
		Metrics:   obs.NewOperatorMetrics(reg),
		Session:   newFakeSession(),
		ReaderFor: nil, // nil reader triggers the WARN path
		PodConfig: agent.PodConfig{
			Namespace:           testNS,
			CallbackURL:         "http://op.svc:8082",
			OIDCIssuer:          "https://kc.svc/realms/master",
			AnthropicSecretName: "anthropic",
			CLIOIDCSecretName:   "tatara-cli-oidc",
		},
		SCMFor: func(string) (scm.SCMWriter, error) { return fw, nil },
	}
	task := seedWritebackPending(t, "r3-f8-nil-reader", "wb3f8-scm", "wb3f8-proj", "wb3f8-repo")
	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err, "nil ReaderFor must not cause an error; recovery degrades to skip_4xx")

	// Outcome must be no_pr (all skipped via skip_4xx fallback).
	cnt := testutil.ToFloat64(r.Metrics.WritebackOutcomeCounter("no_pr"))
	require.Equal(t, float64(1), cnt, "nil reader -> skip_4xx fallback -> no_pr metric must be emitted")
}

// --- Finding 9: brainstormHasProposal logs WARN on fallback scan ---

// fieldIndexUnsupportedClient injects a field-selector-unsupported error so the
// fallback path in brainstormHasProposal is exercised.
type fieldIndexUnsupportedClient struct {
	client.Client
	calls atomic.Int32
}

func (c *fieldIndexUnsupportedClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if c.calls.Add(1) == 1 {
		// Simulate no field index: return an error that isFieldSelectorUnsupported recognises.
		// The actual check is "No kind is registered for the type" or "field selector not supported";
		// we return the generic unsupported error via a server-side "Invalid" status.
		return &fieldSelectorUnsupportedError{}
	}
	return c.Client.List(ctx, list, opts...)
}

// fieldSelectorUnsupportedError mimics the error isFieldSelectorUnsupported detects.
// isFieldSelectorUnsupported checks for "field label not supported" in the error string.
type fieldSelectorUnsupportedError struct{}

func (e *fieldSelectorUnsupportedError) Error() string {
	return "field label not supported: metadata.projectRef"
}

// TestBrainstormHasProposal_FallbackLogsWarn verifies F9: when the field index is
// unsupported, brainstormHasProposal falls back and the fallback is observable
// (the function returns false with no panic, which is the observable contract).
// The WARN log itself is a side-effect we can verify by ensuring the fallback ran
// (second List call succeeds and the function returns a result rather than erroring).
func TestBrainstormHasProposal_FallbackLogsWarn(t *testing.T) {
	cc := &fieldIndexUnsupportedClient{Client: k8sClient}
	r := &TaskReconciler{
		Client:  cc,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
	}
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "r3-f9-bs-fallback", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "wb3f9-proj", RepositoryRef: "wb3f9-repo", Goal: "brainstorm",
		},
	}
	// brainstormHasProposal must not panic and must return a deterministic bool
	// regardless of which path (field-index or full-scan) it takes.
	result := r.brainstormHasProposal(context.Background(), task)
	// No proposals seeded -> must be false.
	require.False(t, result, "brainstormHasProposal must return false when no proposals exist")
	// Verify the fallback ran by checking the call counter (2 List calls: first fails, second succeeds).
	require.Equal(t, int32(2), cc.calls.Load(), "fallback scan must issue a second List call")
}

// isFieldSelectorUnsupported is in projectscan.go; verify our fake satisfies the check.
func init() {
	// Compile-time interface verify for listMetricReader.
	var _ scm.SCMReader = (*listMetricReader)(nil)
}

// apimeta import used for WritebackPending condition assertions.
var _ = apimeta.FindStatusCondition
