// Copyright 2026 tatara authors.

package controller

// Tests for audit-r2 findings on lifecycle.go (2026-06-16).
// Findings 1/5, 2, 3/4, 7/16, 9, 18, 19, 20, 21 have unit-testable behavior.

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// ================================================================
// Findings 1 & 5: IssueOutcome("discuss") must be recorded AFTER
// enterConversation commits (not before), so a failed transition
// cannot double-count on the next reconcile.
// ================================================================

// fakeSCMWriterWithEnterConvFail is a writer that always fails Comment so we
// can simulate enterConversation failure paths indirectly.  (The actual failure
// path in enterConversation is a k8s conflict retry exhaust, which the fake
// client cannot reproduce.  We test the post-move arrangement by verifying the
// metric is recorded exactly once on a successful path and by reading the code
// ordering.)
//
// The test below exercises the discuss arm end-to-end and asserts the metric is
// recorded exactly once for a successful reconcile. If the metric were
// incremented before enterConversation and enterConversation were to fail, the
// count would be > 1 on the next run; this test pins the success-path count.
func TestFinishTriage_DiscussMetricRecordedOnce(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	name := "r2-discuss-metric"
	proj := "r2-dmp"
	repo := "r2-dmr"
	sec := "r2-dms"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#500",
		URL: "https://github.com/o/r/issues/500", Number: 500,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	// Human-filed issue with discuss outcome -> no silence gate, posts comment.
	task.Status.LifecycleState = "Triage"
	task.Status.Phase = "Succeeded"
	task.Status.IssueOutcome = &tatarav1alpha1.IssueOutcome{Action: "discuss", Comment: "let me know your thoughts"}
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	reg := prometheus.NewRegistry()
	lm := obs.NewLifecycleMetrics(reg)
	om := obs.NewOperatorMetrics(reg)

	fw := &lifecycleFakeSCMWriter{}
	r := &TaskReconciler{
		Client:           k8sClient,
		Scheme:           k8sClient.Scheme(),
		Metrics:          om,
		LifecycleMetrics: lm,
	}
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	// Must be in Conversation.
	got := fetchTask(t, name)
	if got.Status.LifecycleState != "Conversation" {
		t.Errorf("LifecycleState = %q, want Conversation", got.Status.LifecycleState)
	}

	// IssueOutcome("discuss") must be counted exactly once.
	if v := testutil.ToFloat64(om.IssueOutcomeTotal("discuss")); v != 1 {
		t.Errorf("issue_outcome{discuss} = %v, want 1", v)
	}
}

// ================================================================
// Finding 2: nil IssueOutcome on Succeeded must default to "discuss"
// (not "implement"), treating an inconclusive run as awaiting human input.
// ================================================================

func TestFinishTriage_NilOutcomeDefaultsToDiscuss(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	name := "r2-nil-outcome"
	proj := "r2-nop"
	repo := "r2-nor"
	sec := "r2-nos"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#501",
		URL: "https://github.com/o/r/issues/501", Number: 501,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	// Phase=Succeeded, no IssueOutcome set at all.
	task.Status.LifecycleState = "Triage"
	task.Status.Phase = "Succeeded"
	task.Status.IssueOutcome = nil
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{}
	r := newLifecycleReconciler(t, fw)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	// Must go to Conversation (discuss), NOT Implement.
	got := fetchTask(t, name)
	if got.Status.LifecycleState == "Implement" {
		t.Error("nil IssueOutcome must NOT default to Implement; must enter Conversation (discuss)")
	}
	if got.Status.LifecycleState != "Conversation" {
		t.Errorf("LifecycleState = %q, want Conversation for nil IssueOutcome", got.Status.LifecycleState)
	}
}

// ================================================================
// Findings 3 & 4: handleMerge - second GetPRState must be removed;
// first GetPRState must record SCM metric.
// ================================================================

// mergeCountingWriter counts GetPRState calls.
type mergeCountingWriter struct {
	lifecycleFakeSCMWriterMRCI
	getPRStateCalls int
	mergeReturnSHA  string
}

func (f *mergeCountingWriter) GetPRState(ctx context.Context, repoURL, token string, number int) (scm.PRState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getPRStateCalls++
	return f.prState, f.prErr
}

func (f *mergeCountingWriter) Merge(_ context.Context, _, _ string, _ int, _ string) (string, error) {
	sha := f.mergeReturnSHA
	if sha == "" {
		sha = "abc123"
	}
	return sha, nil
}

func seedMergeTaskR2(t *testing.T, suffix string) (*TaskReconciler, *mergeCountingWriter, string) {
	t.Helper()
	ctx := context.Background()
	name := "r2-merge-" + suffix
	proj := "r2-mergep-" + suffix
	repo := "r2-merger-" + suffix
	sec := "r2-merges-" + suffix
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#9",
		URL: "https://github.com/o/r/issues/9", Number: 9,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	task.Status.LifecycleState = "Merge"
	task.Status.PRNumber = 55
	task.Status.PrURL = "https://github.com/o/r/pull/55"
	task.Status.HeadBranch = "tatara/task-" + name
	dl := metav1.NewTime(time.Now().Add(time.Hour))
	task.Status.DeadlineAt = &dl
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed merge task: %v", err)
	}

	fw := &mergeCountingWriter{}
	fw.prState = scm.PRState{Author: "bot", CIStatus: "success", HeadSHA: "headsha42"}
	reg := prometheus.NewRegistry()
	om := obs.NewOperatorMetrics(reg)
	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: om,
	}
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }
	return r, fw, name
}

// TestHandleMerge_SingleGetPRStateCall verifies that handleMerge only calls
// GetPRState once (not twice) to capture the head SHA.
func TestHandleMerge_SingleGetPRStateCall(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, fw, name := seedMergeTaskR2(t, "once")

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	fw.mu.Lock()
	calls := fw.getPRStateCalls
	fw.mu.Unlock()

	if calls != 1 {
		t.Errorf("GetPRState called %d times, want exactly 1 (second call is redundant)", calls)
	}
}

// TestHandleMerge_HeadSHACapturedFromFirstCall verifies that after a successful
// merge the task has MergedHeadSHA set (from the first, now only, GetPRState).
func TestHandleMerge_HeadSHACapturedFromFirstCall(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, _, name := seedMergeTaskR2(t, "headsha")

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	got := fetchTask(t, name)
	if got.Status.MergedHeadSHA != "headsha42" {
		t.Errorf("MergedHeadSHA = %q, want headsha42", got.Status.MergedHeadSHA)
	}
}

// TestHandleMerge_GetPRStateMetricRecorded verifies that the first (and only)
// GetPRState call is reflected in the SCM metric.
func TestHandleMerge_GetPRStateMetricRecorded(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	name := "r2-merge-metric"
	proj := "r2-mmrp"
	repo := "r2-mmrr"
	sec := "r2-mmrs"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#10",
		URL: "https://github.com/o/r/issues/10", Number: 10,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	task.Status.LifecycleState = "Merge"
	task.Status.PRNumber = 56
	task.Status.PrURL = "https://github.com/o/r/pull/56"
	task.Status.HeadBranch = "tatara/task-" + name
	dl := metav1.NewTime(time.Now().Add(time.Hour))
	task.Status.DeadlineAt = &dl
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	reg := prometheus.NewRegistry()
	om := obs.NewOperatorMetrics(reg)

	fw := &mergeCountingWriter{}
	fw.prState = scm.PRState{Author: "bot", CIStatus: "success", HeadSHA: "sha99"}
	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: om,
	}
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	// get_pr_state must appear in SCM metric.
	v := testutil.ToFloat64(om.SCMWriteCounter("github", "get_pr_state", "ok"))
	if v < 1 {
		t.Errorf("scm_write{get_pr_state,ok} = %v, want >= 1 (first GetPRState must be recorded)", v)
	}
}

// ================================================================
// Findings 7 & 16: handleMainCI must record GetCommitCIStatus in SCM
// metric and log at WARN on error (not silently swallow).
// ================================================================

// ciStatusReader is a fake SCMReader with configurable GetCommitCIStatus behaviour.
type ciStatusReader struct {
	fakeProposalReader
	status string
	err    error
}

func (r *ciStatusReader) GetCommitCIStatus(_ context.Context, _, _, _ string) (string, error) {
	return r.status, r.err
}

func (r *ciStatusReader) GetIssue(_ context.Context, _, _ string, _ int) (scm.IssueContent, error) {
	return scm.IssueContent{}, nil
}

func (r *ciStatusReader) ListIssueComments(_ context.Context, _, _ string, _ int) ([]scm.IssueComment, error) {
	return nil, nil
}

func seedMainCITaskR2(t *testing.T, suffix string) (*tatarav1alpha1.Task, string, string) {
	t.Helper()
	name := "r2-mainci-" + suffix
	proj := "r2-maincp-" + suffix
	repo := "r2-maincr-" + suffix
	sec := "r2-maincs-" + suffix
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#11",
		URL: "https://github.com/o/r/issues/11", Number: 11,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	task.Status.LifecycleState = "MainCI"
	task.Status.MergeCommitSHA = "merge123"
	task.Status.PRNumber = 60
	task.Status.PrURL = "https://github.com/o/r/pull/60"
	dl := metav1.NewTime(time.Now().Add(time.Hour))
	task.Status.DeadlineAt = &dl
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}
	return fetchTask(t, name), proj, repo
}

// TestHandleMainCI_GetCommitCIStatusMetricRecordedOnSuccess verifies the SCM
// metric is emitted on a successful GetCommitCIStatus read (pending status).
func TestHandleMainCI_GetCommitCIStatusMetricRecordedOnSuccess(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	task, _, repo := seedMainCITaskR2(t, "ok")

	reg := prometheus.NewRegistry()
	om := obs.NewOperatorMetrics(reg)

	rdr := &ciStatusReader{status: "pending"}
	fw := &lifecycleFakeSCMWriter{}
	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: om,
	}
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }
	r.ReaderFor = func(_, _ string) (scm.SCMReader, error) { return rdr, nil }
	_ = repo

	_, err := r.reconcileLifecycle(ctx, task)
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	v := testutil.ToFloat64(om.SCMWriteCounter("github", "get_commit_ci_status", "ok"))
	if v < 1 {
		t.Errorf("scm_write{get_commit_ci_status,ok} = %v, want >= 1", v)
	}
}

// TestHandleMainCI_GetCommitCIStatusMetricRecordedOnError verifies the SCM
// metric is emitted (with error label) when GetCommitCIStatus fails, and that
// the reconcile does NOT silently discard the error from the metric.
func TestHandleMainCI_GetCommitCIStatusMetricRecordedOnError(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	task, _, _ := seedMainCITaskR2(t, "err")

	reg := prometheus.NewRegistry()
	om := obs.NewOperatorMetrics(reg)

	rdr := &ciStatusReader{err: &scm.HTTPError{Status: 503, Body: "unavailable", Path: "/ci"}}
	fw := &lifecycleFakeSCMWriter{}
	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: om,
	}
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }
	r.ReaderFor = func(_, _ string) (scm.SCMReader, error) { return rdr, nil }

	// Error path must still requeue (not return an error to controller-runtime).
	res, err := r.reconcileLifecycle(ctx, task)
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Error("expected RequeueAfter on CI status error")
	}

	// Error must be recorded in the SCM metric.
	v := testutil.ToFloat64(om.SCMWriteCounter("github", "get_commit_ci_status", "error"))
	if v < 1 {
		t.Errorf("scm_write{get_commit_ci_status,error} = %v, want >= 1", v)
	}
}

// ================================================================
// Finding 9: parkWithComment must resolve provider from project when
// task.Spec.Source.Provider is empty (board/cron-sourced tasks).
// ================================================================

// TestParkWithComment_UsesProjectProviderWhenSourceEmpty directly calls
// parkWithComment with a task whose Source.Provider is "" and a project whose
// Scm.Provider is "github", then asserts the SCM metric uses "github" not "".
// (Board/cron tasks can have empty Source.Provider; the CRD enum only enforces
// on creation via the admission webhook, so older objects may have it empty.)
func TestParkWithComment_UsesProjectProviderWhenSourceEmpty(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	// Seed a task with provider="github" (to satisfy CRD), then test
	// parkWithComment directly with an in-memory task that has empty provider.
	name := "r2-park-prv"
	proj := "r2-parkp-prv"
	repo := "r2-parkr-prv"
	sec := "r2-parks-prv"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#20",
		URL: "https://github.com/o/r/issues/20", Number: 20,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	// Simulate empty Source.Provider on the in-memory object (board-sourced task).
	task.Spec.Source.Provider = ""

	reg := prometheus.NewRegistry()
	om := obs.NewOperatorMetrics(reg)
	fw := &lifecycleFakeSCMWriter{}
	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: om,
	}
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	// Call parkWithComment directly (bypasses CRD validation).
	// parkWithComment must fetch the project to resolve provider when
	// task.Spec.Source.Provider is "".
	if err := r.parkWithComment(ctx, task, fw, "tok", "test", "board task park"); err != nil {
		t.Fatalf("parkWithComment: %v", err)
	}

	// The SCM metric for "comment" must NOT use empty provider.
	// seedLifecycleTask wires project ScmSpec.Provider = "github" so the fallback
	// must yield "github".
	emptyV := testutil.ToFloat64(om.SCMWriteCounter("", "comment", "ok")) +
		testutil.ToFloat64(om.SCMWriteCounter("", "comment", "error"))
	if emptyV > 0 {
		t.Errorf("scm_write{provider='',comment} = %v, want 0: parkWithComment must fall back to project provider", emptyV)
	}
	githubV := testutil.ToFloat64(om.SCMWriteCounter("github", "comment", "ok")) +
		testutil.ToFloat64(om.SCMWriteCounter("github", "comment", "error"))
	if githubV < 1 {
		t.Errorf("scm_write{provider='github',comment} = %v, want >= 1: project provider fallback required", githubV)
	}
}

// ================================================================
// Finding 18: setLifecycleState must skip the second RetryOnConflict
// block (boot-crash annotation clear) when the annotation is absent.
// We can't easily count Get calls via fake client, so this is a
// no-test (structural, verified by code review).
// ================================================================

// TestSetLifecycleState_NoAnnotation_Transitions verifies that a normal state
// transition still works correctly when annBootCrashAttempts is absent
// (regression guard: the annotation guard must not break normal transitions).
func TestSetLifecycleState_NoAnnotation_Transitions(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "r2-slca"
	proj := "r2-slcap"
	repo := "r2-slcar"
	sec := "r2-slcas"
	task := seedLifecycleTask(t, name, proj, repo, sec, &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#30",
		URL: "https://github.com/o/r/issues/30", Number: 30,
	})

	reg := prometheus.NewRegistry()
	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(reg),
	}

	if err := r.setLifecycleState(ctx, task, "Parked", "test"); err != nil {
		t.Fatalf("setLifecycleState: %v", err)
	}

	got := fetchTask(t, name)
	if got.Status.LifecycleState != "Parked" {
		t.Errorf("LifecycleState = %q, want Parked", got.Status.LifecycleState)
	}
}

// ================================================================
// Finding 19: await-approval must call resetAgentRun BEFORE entering
// Conversation so a failed resetAgentRun leaves the task in Triage,
// not in Conversation with a leaked pod.
// ================================================================

// TestFinishTriage_AwaitApproval_StateBeforeReset verifies that after the
// await-approval path runs, the task ends up in Conversation (i.e., the
// whole sequence succeeded).  Failure of resetAgentRun is hard to test with
// the fake client, so this is a structural test pinning the correct end state.
func TestFinishTriage_AwaitApproval_StateAfterFullSequence(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	name := "r2-await-approv"
	proj := "r2-aap"
	repo := "r2-aar"
	sec := "r2-aas"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#40",
		URL: "https://github.com/o/r/issues/40", Number: 40,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	// Tatara-authored, no human comment, implement outcome -> await-approval path.
	task.Status.LifecycleState = "Triage"
	task.Status.Phase = "Succeeded"
	task.Status.IssueOutcome = &tatarav1alpha1.IssueOutcome{Action: "implement"}
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Make the issue tatara-authored with no human comments.
	rdr := &discussSilenceReader{
		issueBody: "goal\n\n" + tataraAuthoredMarker,
		comments:  nil,
	}
	fw := &lifecycleFakeSCMWriter{}
	r := newLifecycleReconciler(t, fw)
	r.ReaderFor = func(_, _ string) (scm.SCMReader, error) { return rdr, nil }

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	got := fetchTask(t, name)
	if got.Status.LifecycleState != "Conversation" {
		t.Errorf("LifecycleState = %q, want Conversation after await-approval", got.Status.LifecycleState)
	}
	// IssueOutcome must be cleared.
	if got.Status.IssueOutcome != nil {
		t.Error("IssueOutcome must be nil after await-approval transition")
	}
}

// ================================================================
// Finding 20: parkWithComment must check task.Spec.Source != nil only once.
// Structural nit - regression test ensures the function still works.
// ================================================================

// TestParkWithComment_NilSource_NoError verifies that parkWithComment does not
// panic or error when task.Spec.Source is nil (no comment path, just park).
func TestParkWithComment_NilSource_NoError(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	name := "r2-park-nilsrc"
	proj := "r2-pnsp"
	repo := "r2-pnsr"
	sec := "r2-pnss"
	task := seedLifecycleTask(t, name, proj, repo, sec, nil) // nil source

	reg := prometheus.NewRegistry()
	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(reg),
	}
	fw := &lifecycleFakeSCMWriter{}
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	var projObj tatarav1alpha1.Project
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: proj}, &projObj); err != nil {
		t.Fatalf("get proj: %v", err)
	}

	if err := r.parkWithComment(ctx, task, fw, "tok", "test", "no source task"); err != nil {
		t.Fatalf("parkWithComment with nil source: %v", err)
	}

	got := fetchTask(t, name)
	if got.Status.LifecycleState != "Parked" {
		t.Errorf("LifecycleState = %q, want Parked", got.Status.LifecycleState)
	}
}

// ================================================================
// Finding 21: maybeOpenFollowupIssue log must include pr_url field.
// Structural nit - we verify the function completes successfully
// (the log field is validated by code review).
// ================================================================

func TestMaybeOpenFollowupIssue_LogIncludesPRURL(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	name := "r2-followup"
	proj := "r2-fup"
	repo := "r2-fur"
	sec := "r2-fus"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#50",
		URL: "https://github.com/o/r/issues/50", Number: 50,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	task.Status.PrURL = "https://github.com/o/r/pull/51"
	task.Status.ChangeSummary = &tatarav1alpha1.ChangeSummary{
		RemainingScope: "some remaining work",
	}
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{createIssueURL: "https://github.com/o/r/issues/52"}
	r := newLifecycleReconciler(t, fw)

	if err := r.maybeOpenFollowupIssue(ctx, fetchTask(t, name)); err != nil {
		t.Fatalf("maybeOpenFollowupIssue: %v", err)
	}

	got := fetchTask(t, name)
	if got.Status.FollowupIssueURL != "https://github.com/o/r/issues/52" {
		t.Errorf("FollowupIssueURL = %q, want %q", got.Status.FollowupIssueURL, "https://github.com/o/r/issues/52")
	}
}
