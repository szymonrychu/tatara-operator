package controller

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// seedDisarmTask creates a kind=implement Task with an already-open PR and a
// declared RemainingScope, so reconcile enters checkRemainingScopeHardFail's
// disarm path (F2).
func seedDisarmTask(t *testing.T, name, project, repo, scmSecret string) *tatarav1alpha1.Task {
	t.Helper()
	task := seedWritebackKindTask(t, name, project, repo, scmSecret,
		tatarav1alpha1.TaskSpec{
			Goal: "Implement issue 200",
			Kind: "implement",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#200", URL: "https://github.com/o/r/issues/200", Number: 200,
			},
		}, nil)
	task.Status.PrURL = "https://github.com/o/r/pull/201"
	task.Status.ChangeSummary = &tatarav1alpha1.ChangeSummary{
		PRTitle:        "feat: partial",
		PRBody:         "Partial.",
		DeliveredScope: "half",
		RemainingScope: "the other half",
		Significance:   "minor",
	}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))
	return task
}

// TestDisarm_TransientError_RequeuesWithoutTerminating is the F2 regression:
// a transient SCM error during disarmOpenChanges (ClosePR 500) must NOT let
// the Task terminate - the previous fail-open behavior swallowed every
// disarm error and terminated Failed unconditionally, leaving an armed PR
// open with nothing tracking that the disarm never actually verified.
func TestDisarm_TransientError_RequeuesWithoutTerminating(t *testing.T) {
	fw := &fullFakeSCMWriter{closePRErrs: []error{&scm.HTTPError{Status: 500, Body: "boom"}}}
	r := newFullFakeReconciler(t, fw)
	task := seedDisarmTask(t, "wbk-disarm-transient", "wbk-disarm-tp", "wbk-disarm-tr", "wbk-disarm-ts")

	res, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)
	require.Equal(t, disarmRetryRequeue, res.RequeueAfter, "a dirty disarm sweep under the cap must requeue, not terminate")

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	require.NotEqual(t, "Failed", got.Status.Phase, "the Task must not terminate while the disarm is unverified")
	require.Equal(t, 1, got.Status.DisarmFailures)
	cond := apimeta.FindStatusCondition(got.Status.Conditions, "WritebackPending")
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionTrue, cond.Status, "WritebackPending must stay armed so the next reconcile retries the disarm")
	require.True(t, fw.closePRCalled)
	require.Nil(t, apimeta.FindStatusCondition(got.Status.Conditions, "DisarmFailed"))
}

// TestDisarm_CapExhausted_TerminatesLoudly verifies that after disarmFailureCap
// consecutive dirty sweeps the Task terminates anyway (it cannot retry
// forever) but records a distinct DisarmFailed condition and increments the
// operator_writeback_outcome_total{result="disarm_failed"} counter, so an
// armed-PR-that-could-not-be-disarmed is alertable instead of silently
// dropped.
func TestDisarm_CapExhausted_TerminatesLoudly(t *testing.T) {
	fw := &fullFakeSCMWriter{closePRErrs: []error{&scm.HTTPError{Status: 500, Body: "boom"}}}
	r := newFullFakeReconciler(t, fw)
	task := seedDisarmTask(t, "wbk-disarm-capped", "wbk-disarm-cp", "wbk-disarm-cr", "wbk-disarm-cs")

	require.Zero(t, testutil.ToFloat64(r.Metrics.WritebackOutcomeCounter("disarm_failed")))

	for i := 0; i < disarmFailureCap; i++ {
		_, err := reconcileWriteback(t, r, task.Name)
		require.NoError(t, err)
	}

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	require.Equal(t, "Failed", got.Status.Phase, "the cap must eventually let the Task terminate rather than retry forever")
	readyCond := apimeta.FindStatusCondition(got.Status.Conditions, "Ready")
	require.NotNil(t, readyCond)
	require.Equal(t, "IncompleteImplementation", readyCond.Reason)
	disarmCond := apimeta.FindStatusCondition(got.Status.Conditions, "DisarmFailed")
	require.NotNil(t, disarmCond, "an exhausted disarm budget must record a distinct DisarmFailed condition")
	require.Equal(t, metav1.ConditionTrue, disarmCond.Status)
	require.Equal(t, "DisarmCapReached", disarmCond.Reason)
	require.Equal(t, float64(1), testutil.ToFloat64(r.Metrics.WritebackOutcomeCounter("disarm_failed")),
		"the give-up must be alertable via a counter, not just a log line")
	require.Equal(t, disarmFailureCap, fw.closePRCallCount)
}

// TestDisarm_PermanentAlreadyClosed_TreatedAsSuccess verifies that a permanent
// SCM error (404, the PR is already gone/closed) counts as a clean disarm, not
// a failure: the Task terminates immediately on the first attempt, with no
// DisarmFailed condition and no retry.
func TestDisarm_PermanentAlreadyClosed_TreatedAsSuccess(t *testing.T) {
	fw := &fullFakeSCMWriter{closePRErrs: []error{&scm.HTTPError{Status: 404, Body: "Not Found"}}}
	r := newFullFakeReconciler(t, fw)
	task := seedDisarmTask(t, "wbk-disarm-gone", "wbk-disarm-gp", "wbk-disarm-gr", "wbk-disarm-gs")

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	require.Equal(t, "Failed", got.Status.Phase, "a permanently-gone target must count as disarmed and let the Task terminate on the first attempt")
	readyCond := apimeta.FindStatusCondition(got.Status.Conditions, "Ready")
	require.NotNil(t, readyCond)
	require.Equal(t, "IncompleteImplementation", readyCond.Reason)
	require.Nil(t, apimeta.FindStatusCondition(got.Status.Conditions, "DisarmFailed"))
	require.Equal(t, 0, got.Status.DisarmFailures)
	require.Equal(t, 1, fw.closePRCallCount, "a permanent 404 must not be retried")
}

// TestDisarm_TargetAlreadyMerged_ShoutsDistinctlyInsteadOfSilentClean is the C1
// regression: a disarm target that already MERGED before the sweep runs must
// not be silently folded into an ordinary clean disarm via ClosePR's
// close-of-an-already-closed no-op. It must skip the (now pointless) disarm
// actions, never post the misleading "must not merge" close note, and instead
// emit a distinct ERROR log, a distinct disarm_merged metric, a distinct
// IncompleteChangeMerged condition, and an explanatory comment on the
// originating issue - while still terminating the Task Failed (the operator
// cannot un-merge the change).
func TestDisarm_TargetAlreadyMerged_ShoutsDistinctlyInsteadOfSilentClean(t *testing.T) {
	fw := &fullFakeSCMWriter{prState: scm.PRState{Merged: true}}
	r := newFullFakeReconciler(t, fw)
	task := seedDisarmTask(t, "wbk-disarm-merged", "wbk-disarm-mp", "wbk-disarm-mr", "wbk-disarm-ms")

	require.Zero(t, testutil.ToFloat64(r.Metrics.WritebackOutcomeCounter("disarm_merged")))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	require.False(t, fw.closePRCalled, "an already-merged target must not go through ClosePR - nothing left to close")
	require.False(t, fw.disableAutoMergeCalled, "an already-merged target has nothing left to disarm")

	require.True(t, fw.commentCalled, "the operator must comment distinctly that the change already merged")
	require.Contains(t, fw.commentBody, "MERGED")
	require.NotContains(t, fw.commentBody, "must not merge",
		"the misleading close-comment wording must be suppressed for an already-merged target")

	require.Equal(t, float64(1), testutil.ToFloat64(r.Metrics.WritebackOutcomeCounter("disarm_merged")),
		"the already-merged case must be alertable via its own counter, distinct from disarm_failed")

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	mergedCond := apimeta.FindStatusCondition(got.Status.Conditions, "IncompleteChangeMerged")
	require.NotNil(t, mergedCond, "an already-merged disarm target must set a distinct condition")
	require.Equal(t, metav1.ConditionTrue, mergedCond.Status)
	require.Nil(t, apimeta.FindStatusCondition(got.Status.Conditions, "DisarmFailed"),
		"the merged case is distinct from the generic disarm-failed cap path")
	require.Equal(t, "Failed", got.Status.Phase, "the Task must still terminate Failed - the operator cannot un-merge the change")
	readyCond := apimeta.FindStatusCondition(got.Status.Conditions, "Ready")
	require.NotNil(t, readyCond)
	require.Equal(t, "IncompleteImplementation", readyCond.Reason)
}

// pruneDisarmFailuresClient wraps client.Client so Status().Update() succeeds
// (nil error) but silently drops any change to Status.DisarmFailures before
// forwarding to the real client - simulating a deployed CRD that predates the
// field (server-side structural-schema pruning): the write appears to succeed
// but the counter never actually advances (C2 regression).
type pruneDisarmFailuresClient struct {
	client.Client
}

func (c *pruneDisarmFailuresClient) Status() client.SubResourceWriter {
	return &pruneDisarmFailuresWriter{SubResourceWriter: c.Client.Status()}
}

type pruneDisarmFailuresWriter struct {
	client.SubResourceWriter
}

func (w *pruneDisarmFailuresWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if task, ok := obj.(*tatarav1alpha1.Task); ok {
		task.Status.DisarmFailures = 0
	}
	if err := w.SubResourceWriter.Update(ctx, obj, opts...); err != nil {
		return err
	}
	if task, ok := obj.(*tatarav1alpha1.Task); ok {
		task.Status.DisarmFailures = 0
	}
	return nil
}

// TestDisarm_CounterWriteDoesNotPersist_TerminatesLoudlyInsteadOfLivelocking is
// the C2 regression: when the Status.DisarmFailures write appears to succeed
// (no error) but does not actually stick, requeuing blind would livelock -
// every reconcile re-reads the unadvanced counter, retries the disarm sweep,
// and never reaches disarmFailureCap. The fix must detect the unpersisted
// write and terminate loudly (DisarmFailed condition + disarm_failed metric)
// on the very first pass instead.
func TestDisarm_CounterWriteDoesNotPersist_TerminatesLoudlyInsteadOfLivelocking(t *testing.T) {
	fw := &fullFakeSCMWriter{closePRErrs: []error{&scm.HTTPError{Status: 500, Body: "boom"}}}
	task := seedDisarmTask(t, "wbk-disarm-nopersist", "wbk-disarm-npp", "wbk-disarm-npr", "wbk-disarm-nps")

	r := &TaskReconciler{
		Client:  &pruneDisarmFailuresClient{Client: k8sClient},
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

	res, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)
	require.Equal(t, ctrl.Result{}, res, "must not requeue blind when the counter write did not persist - that would livelock")

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	require.Equal(t, "Failed", got.Status.Phase, "an unpersistable counter must still terminate the Task, not livelock retrying forever")
	disarmCond := apimeta.FindStatusCondition(got.Status.Conditions, "DisarmFailed")
	require.NotNil(t, disarmCond)
	require.Equal(t, metav1.ConditionTrue, disarmCond.Status)
	require.Equal(t, float64(1), testutil.ToFloat64(r.Metrics.WritebackOutcomeCounter("disarm_failed")))
}

// TestDisarm_CapNotResetBeforeTerminateSucceeds is the C4 regression: the
// terminate-loudly patch used to reset DisarmFailures=0 before terminate()
// ran, so if terminate() itself then failed, the next reconcile would restart
// a fresh disarmFailureCap-sweep cycle instead of re-terminating immediately.
// This verifies the counter stays at its cap-reached value once the
// DisarmFailed condition has been recorded, regardless of what terminate()
// does afterward - i.e. the reset is no longer coupled to termination at all.
func TestDisarm_CapNotResetBeforeTerminateSucceeds(t *testing.T) {
	fw := &fullFakeSCMWriter{closePRErrs: []error{&scm.HTTPError{Status: 500, Body: "boom"}}}
	r := newFullFakeReconciler(t, fw)
	task := seedDisarmTask(t, "wbk-disarm-c4", "wbk-disarm-c4p", "wbk-disarm-c4r", "wbk-disarm-c4s")

	for i := 0; i < disarmFailureCap; i++ {
		_, err := reconcileWriteback(t, r, task.Name)
		require.NoError(t, err)
	}

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	require.Equal(t, "Failed", got.Status.Phase)
	// The counter's exact value at the cap-exceeded branch is whatever the last
	// successfully-persisted attempt count was (disarmFailureCap-1: the cap
	// check itself fires before ever writing "attempts"==cap) - what matters is
	// that it is no longer force-reset to 0 as a side effect of giving up.
	require.NotZero(t, got.Status.DisarmFailures,
		"the cap-reached counter must stay monotonic (not reset to 0) once DisarmFailed has been recorded, "+
			"so a failed terminate() cannot restart a fresh retry budget")
	require.Equal(t, disarmFailureCap-1, got.Status.DisarmFailures)
}

// TestDisarm_DisableAutoMergeRealError_BotLoginSet_NotClean is C3 regression 1:
// a real/unexpected DisableAutoMerge failure on a PR the operator itself armed
// (BotLogin configured) must NOT be treated as clean - ClosePR succeeding is
// not sufficient proof the PR can never auto-merge again after a reopen.
func TestDisarm_DisableAutoMergeRealError_BotLoginSet_NotClean(t *testing.T) {
	fw := &fullFakeSCMWriter{disableAutoMergeErr: &scm.HTTPError{Status: 500, Body: "rate limited"}}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "wbk-c3-real", "wbk-c3-real-p", "wbk-c3-real-r", "wbk-c3-real-s",
		tatarav1alpha1.TaskSpec{
			Goal: "Implement issue 300",
			Kind: "implement",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#300", URL: "https://github.com/o/r/issues/300", Number: 300,
			},
		}, &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "tatara-bot"})
	task.Status.PrURL = "https://github.com/o/r/pull/301"
	task.Status.ChangeSummary = &tatarav1alpha1.ChangeSummary{
		PRTitle: "feat: partial", PRBody: "partial", DeliveredScope: "half",
		RemainingScope: "the other half", Significance: "minor",
	}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	res, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)
	require.Equal(t, disarmRetryRequeue, res.RequeueAfter,
		"a real DisableAutoMerge failure on an operator-armed PR must not be treated as clean")

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	require.NotEqual(t, "Failed", got.Status.Phase, "the Task must not terminate while DisableAutoMerge is unverified")
	require.Equal(t, 1, got.Status.DisarmFailures)
	require.True(t, fw.closePRCalled, "ClosePR still runs even though DisableAutoMerge failed")
}

// TestDisarm_DisableAutoMergeNoopError_StillClean is C3 regression 2: the
// documented "nothing to disable" no-op (auto-merge was never armed or
// already off) must remain non-fatal and not gate clean, even with BotLogin
// configured - C3's stricter gating only applies to real/unexpected errors.
func TestDisarm_DisableAutoMergeNoopError_StillClean(t *testing.T) {
	fw := &fullFakeSCMWriter{disableAutoMergeErr: errors.New("github: graphql error: Pull request Auto merge is not enabled.")}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "wbk-c3-noop", "wbk-c3-noop-p", "wbk-c3-noop-r", "wbk-c3-noop-s",
		tatarav1alpha1.TaskSpec{
			Goal: "Implement issue 310",
			Kind: "implement",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#310", URL: "https://github.com/o/r/issues/310", Number: 310,
			},
		}, &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "tatara-bot"})
	task.Status.PrURL = "https://github.com/o/r/pull/311"
	task.Status.ChangeSummary = &tatarav1alpha1.ChangeSummary{
		PRTitle: "feat: partial", PRBody: "partial", DeliveredScope: "half",
		RemainingScope: "the other half", Significance: "minor",
	}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	require.Equal(t, "Failed", got.Status.Phase,
		"the documented auto-merge-already-off no-op must still count as a clean disarm")
	require.Equal(t, 0, got.Status.DisarmFailures)
}

// c5DisarmReader is a minimal SCMReader (every method stubbed to a benign
// zero value except ListIssueComments) that serves the bot comments seeded by
// the C5 test for the disarm close-comment dedup check. It fully implements
// the interface (rather than embedding scm.SCMReader for the unused methods)
// because checkRemainingScopeHardFail's m9 comment/label path also reads
// through this reader (e.g. ensurePhaseLabel -> currentIssueLabels ->
// ListOpenIssues) before disarmOpenChanges ever runs, and an embedded nil
// interface panics the moment any of those get called.
type c5DisarmReader struct {
	comments map[string][]scm.IssueComment
}

func (r *c5DisarmReader) ListOpenPRs(_ context.Context, _, _ string) ([]scm.PRRef, error) {
	return nil, nil
}
func (r *c5DisarmReader) ListOpenIssues(_ context.Context, _, _ string) ([]scm.IssueRef, error) {
	return nil, nil
}
func (r *c5DisarmReader) ListBoardItems(_ context.Context, _ scm.BoardRef) ([]scm.BoardItem, error) {
	return nil, nil
}
func (r *c5DisarmReader) GetCommitCIStatus(_ context.Context, _, _, _ string) (string, error) {
	return "", nil
}
func (r *c5DisarmReader) ListIssueComments(_ context.Context, owner, name string, number int) ([]scm.IssueComment, error) {
	return r.comments[fmt.Sprintf("%s/%s#%d", owner, name, number)], nil
}
func (r *c5DisarmReader) GetIssue(_ context.Context, _, _ string, _ int) (scm.IssueContent, error) {
	return scm.IssueContent{}, nil
}
func (r *c5DisarmReader) GetDefaultBranchHeadSHA(_ context.Context, _, _ string) (string, error) {
	return "", nil
}
func (r *c5DisarmReader) ListClosedIssues(_ context.Context, _, _ string, _ time.Time) ([]scm.IssueRef, error) {
	return nil, nil
}
func (r *c5DisarmReader) ListCommits(_ context.Context, _, _ string, _ time.Time) ([]scm.CommitRef, error) {
	return nil, nil
}

var _ scm.SCMReader = (*c5DisarmReader)(nil)

// TestDisarm_MultiRepo_CleanTargetCloseNoteNotDuplicatedAcrossRetries is the C5
// regression: on a multi-repo umbrella disarm sweep where target A stays dirty
// (forcing retries) and target B closes cleanly, B's "must not merge" close
// note must be posted exactly once across the retries, not re-posted on every
// pass just because ClosePR itself is an idempotent no-op on an
// already-closed PR.
func TestDisarm_MultiRepo_CleanTargetCloseNoteNotDuplicatedAcrossRetries(t *testing.T) {
	const dirtyNumber = 401
	const cleanNumber = 402
	fw := &fullFakeSCMWriter{
		closePRErrByNumber: map[int]error{dirtyNumber: &scm.HTTPError{Status: 500, Body: "boom"}},
	}
	reader := &c5DisarmReader{comments: map[string][]scm.IssueComment{}}
	r := newFullFakeReconciler(t, fw)
	r.ReaderFor = func(_, _ string) (scm.SCMReader, error) { return reader, nil }

	task := seedWritebackKindTask(t, "wbk-c5-task", "wbk-c5-proj", "wbk-c5-r1", "wbk-c5-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "cross-repo partial change",
			Kind: "implement",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#400", URL: "https://github.com/o/r/issues/400", Number: 400,
			},
		}, &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "tatara-bot"})

	require.NoError(t, k8sClient.Create(context.Background(), &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "wbk-c5-r2", Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef: "wbk-c5-proj", URL: "https://github.com/o/c5r2", DefaultBranch: "main", ReingestSchedule: "0 6 * * *",
		},
	}))

	task.Status.PrURL = "https://github.com/o/r/pull/401"
	task.Status.ChangeSummary = &tatarav1alpha1.ChangeSummary{
		PRTitle: "feat: partial", PRBody: "partial", DeliveredScope: "half",
		RemainingScope: "the other half", Significance: "minor",
	}
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/r", Number: dirtyNumber, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen},
		{Provider: "github", Repo: "o/c5r2", Number: cleanNumber, Kind: tatarav1alpha1.WorkItemPR, Role: tatarav1alpha1.RoleOpenedPR, State: tatarav1alpha1.WIOpen},
	}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	// Pass 1: repo A (401) stays dirty, repo B (402) closes cleanly and posts
	// the close note.
	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)
	require.Len(t, fw.closePRBodyByNumber[cleanNumber], 1)
	require.NotEmpty(t, fw.closePRBodyByNumber[cleanNumber][0], "the first clean close must post the close note")

	// Simulate the SCM now reflecting that posted comment (what a real
	// ClosePR's embedded Comment() call would have delivered) so pass 2's
	// dedup read sees it.
	reader.comments["o/c5r2#402"] = []scm.IssueComment{{Author: "tatara-bot", Body: fw.closePRBodyByNumber[cleanNumber][0]}}

	// Pass 2: repo A is still dirty (forces a retry), repo B is re-swept too
	// (ClosePR's PATCH is an idempotent no-op) but must NOT re-post the note.
	_, err = reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	require.Len(t, fw.closePRBodyByNumber[cleanNumber], 2, "ClosePR is still called on retry (idempotent PATCH no-op)")
	require.Empty(t, fw.closePRBodyByNumber[cleanNumber][1], "the close note for the already-clean target must not be re-posted on retry")
}
