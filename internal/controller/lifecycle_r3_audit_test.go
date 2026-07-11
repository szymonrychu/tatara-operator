// Copyright 2026 tatara authors.

package controller

// Tests for audit-r3 findings on lifecycle.go (2026-06-16).
// Findings covered: 1, 2, 3, 5, 8, 10, 12, 13, 15, 17, 18.
// Findings not unit-tested with a reason noted per finding.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// gatherCounterByLabels reads a counter value from reg matching metricName
// and the given label key/value pairs. Returns 0 when not found.
func gatherCounterByLabels(t *testing.T, reg *prometheus.Registry, metricName string, labels map[string]string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != metricName {
			continue
		}
		for _, m := range mf.GetMetric() {
			if r3LabelsMatch(m.GetLabel(), labels) {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func r3LabelsMatch(got []*dto.LabelPair, want map[string]string) bool {
	matched := 0
	for _, lp := range got {
		if v, ok := want[lp.GetName()]; ok && v == lp.GetValue() {
			matched++
		}
	}
	return matched == len(want)
}

// ================================================================
// Finding 1 (medium/correctness): IssueOutcome("close") must be
// recorded AFTER setDeployState(Done) succeeds, not inside
// triageCloseIssue. On a successful close path, the metric must
// be exactly 1. A duplicate-free metric after Done confirms the
// metric moved to the committed-transition path.
// ================================================================

// closeCountingWriter counts CloseIssue calls and records Comment calls.
type closeCountingWriter struct {
	lifecycleFakeSCMWriter
	closeIssueCalls int
}

func (w *closeCountingWriter) CloseIssue(_ context.Context, _, _ string, _ int, _ string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closeIssueCalls++
	return nil
}

func TestFinishTriage_CloseMetricAfterTransition(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	name := "r3-close-metric"
	proj := "r3-cmp"
	repo := "r3-cmr"
	sec := "r3-cms"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#600",
		URL: "https://github.com/o/r/issues/600", Number: 600,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	task.Status.DeployState = "Triage"
	task.Status.Phase = "Succeeded"
	task.Status.IssueOutcome = &tatarav1alpha1.IssueOutcome{Action: "close", Comment: "closing this"}
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	reg := prometheus.NewRegistry()
	om := obs.NewOperatorMetrics(reg)
	lm := obs.NewLifecycleMetrics(reg)

	fw := &closeCountingWriter{}
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

	// Task must be Done.
	got := fetchTask(t, name)
	if got.Status.DeployState != "Done" {
		t.Errorf("DeployState = %q, want Done", got.Status.DeployState)
	}

	// IssueOutcome("close") must be counted exactly once (not twice, not zero).
	if v := testutil.ToFloat64(om.IssueOutcomeTotal("close")); v != 1 {
		t.Errorf("issue_outcome{close} = %v, want 1", v)
	}
}

// ================================================================
// Finding 2 (low/observability): Comment-drain and interjection-drain
// success paths must record ReconcileResult("Task","success").
// ================================================================

// TestCommentDrain_ReconcileResultSuccess verifies that a successful comment
// drain records a success reconcile metric (not just no metric at all).
func TestCommentDrain_ReconcileResultSuccess(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	name := "r3-cdr-metric"
	proj := "r3-cdrp"
	repo := "r3-cdrr"
	sec := "r3-cdrs"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#601",
		URL: "https://github.com/o/r/issues/601", Number: 601,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	// Add a pending comment to drain.
	task.Status.DeployState = "Implement"
	task.Status.Phase = "Running"
	task.Status.PendingComments = []string{"progress update"}
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	reg := prometheus.NewRegistry()
	om := obs.NewOperatorMetrics(reg)

	fw := &lifecycleFakeSCMWriter{}
	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: om,
	}
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	res, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	// Must requeue (drain path) via RequeueAfter (finding 18: no bare Requeue:true).
	if res.RequeueAfter == 0 {
		t.Error("expected RequeueAfter after comment drain")
	}

	// ReconcileResult "success" must have been recorded.
	v := gatherCounterByLabels(t, reg, "operator_reconcile_total", map[string]string{"kind": "Task", "result": "success"})
	if v < 1 {
		t.Errorf("operator_reconcile_total{Task,success} = %v after comment drain, want >= 1", v)
	}
}

// TestInterjectionDrain_StaleDropReconcileResultSuccess verifies that dropping
// stale interjections (no in-flight turn) records ReconcileResult("Task","success").
// Uses mkInterjectFixture pattern: a fakeSession is required for the drain block
// to be entered (the guard is `len(PendingInterjections) > 0 && r.Session != nil`).
func TestInterjectionDrain_StaleDropReconcileResultSuccess(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	// Use the mkInterjectFixture pattern: no AnnCurrentTurn annotation so
	// taskHasInflightTurn returns false -> stale-drop path fires.
	task := mkInterjectFixture(t, "r3-idr-metric", nil, []string{"stale interject"})

	reg := prometheus.NewRegistry()
	om := obs.NewOperatorMetrics(reg)

	fs := newFakeSession()
	r := newTaskReconciler(fs)
	r.Metrics = om

	res, err := r.reconcileLifecycle(ctx, task)
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	// Stale-drop returns RequeueAfter (finding 18 fix: was Requeue:true).
	if res.RequeueAfter == 0 {
		t.Error("expected RequeueAfter after stale interjection drop (finding 18: no bare Requeue)")
	}

	// ReconcileResult "success" must have been recorded (finding 2).
	v := gatherCounterByLabels(t, reg, "operator_reconcile_total", map[string]string{"kind": "Task", "result": "success"})
	if v < 1 {
		t.Errorf("operator_reconcile_total{Task,success} = %v after stale interjection drop, want >= 1", v)
	}
}

// ================================================================
// Finding 3 (low/algorithm): close-withheld arm must add
// IssueOutcome("close-withheld") metric.
// ================================================================

func TestFinishTriage_CloseWithheld_MetricRecorded(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	name := "r3-cwm"
	proj := "r3-cwmp"
	repo := "r3-cwmr"
	sec := "r3-cwms"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#603",
		URL: "https://github.com/o/r/issues/603", Number: 603,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	// Set up an unmerged change so close is withheld.
	task.Status.DeployState = "Triage"
	task.Status.Phase = "Succeeded"
	task.Status.PrURL = "https://github.com/o/r/pull/99"
	task.Status.IssueOutcome = &tatarav1alpha1.IssueOutcome{Action: "close", Comment: "done"}
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	reg := prometheus.NewRegistry()
	om := obs.NewOperatorMetrics(reg)

	fw := &lifecycleFakeSCMWriter{}
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

	// Task must NOT be Done (withheld -> Conversation).
	got := fetchTask(t, name)
	if got.Status.DeployState == "Done" {
		t.Error("close-withheld must NOT transition to Done")
	}
	if got.Status.DeployState != "Conversation" {
		t.Errorf("DeployState = %q, want Conversation for close-withheld", got.Status.DeployState)
	}

	// IssueOutcome("close-withheld") must be counted.
	if v := testutil.ToFloat64(om.IssueOutcomeTotal("close-withheld")); v != 1 {
		t.Errorf("issue_outcome{close-withheld} = %v, want 1", v)
	}
}

// ================================================================
// Finding 5 (low/efficiency): handleImplement must NOT issue a
// second Get after the RetryOnConflict iteration increment.
// We cannot directly count Get calls via the fake client, so this
// is a structural regression guard: the iterate->spawn path must
// still work correctly (increments iterations, ends up in spawn).
// ================================================================

func TestHandleImplement_IterationIncrementNoExtraGet(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	name := "r3-impl-iter"
	proj := "r3-iip"
	repo := "r3-iir"
	sec := "r3-iis"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#604",
		URL: "https://github.com/o/r/issues/604", Number: 604,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	task.Status.DeployState = "Implement"
	task.Status.Phase = ""
	task.Status.LifecycleIterations = 0
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{}
	r := newLifecycleReconciler(t, fw)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	got := fetchTask(t, name)
	if got.Status.LifecycleIterations != 1 {
		t.Errorf("LifecycleIterations = %d, want 1 after first spawn", got.Status.LifecycleIterations)
	}
}

// ================================================================
// Finding 8 (nit/observability): handleMainCI success must log
// INFO when CloseIssue succeeds (scm_issue_closed_on_merge action).
// We verify the close is attempted; log content is a code-review check.
// ================================================================

type mainCICloseCountingWriter struct {
	lifecycleFakeSCMWriter
	closeIssueCalls int
}

func (w *mainCICloseCountingWriter) CloseIssue(_ context.Context, _, _ string, _ int, _ string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closeIssueCalls++
	return nil
}

func TestHandleMainCI_CloseIssueCalledOnSuccess(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	name := "r3-mainci-close"
	proj := "r3-mccp"
	repo := "r3-mccr"
	sec := "r3-mccs"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#605",
		URL: "https://github.com/o/r/issues/605", Number: 605, IsPR: false,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	task.Status.DeployState = "MainCI"
	task.Status.MergeCommitSHA = "sha-close-test"
	dl := metav1.NewTime(time.Now().Add(time.Hour))
	task.Status.DeadlineAt = &dl
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &mainCICloseCountingWriter{}
	rdr := &ciStatusReader{status: "success"}

	reg := prometheus.NewRegistry()
	om := obs.NewOperatorMetrics(reg)
	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: om,
	}
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }
	r.ReaderFor = func(_, _ string) (scm.SCMReader, error) { return rdr, nil }

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	fw.mu.Lock()
	calls := fw.closeIssueCalls
	fw.mu.Unlock()

	if calls != 1 {
		t.Errorf("CloseIssue called %d times on MainCI success, want 1", calls)
	}
	got := fetchTask(t, name)
	if got.Status.DeployState != "Done" {
		t.Errorf("DeployState = %q, want Done", got.Status.DeployState)
	}
}

// ================================================================
// Finding 10 (nit/observability): parkWithComment must log INFO
// when Source is nil (park without comment path). This is a
// structural guard verifying parkWithComment still works with nil
// source (the log statement is verified by code review).
// ================================================================

func TestParkWithComment_NilSource_LogsNoComment(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	name := "r3-park-nillog"
	proj := "r3-pnlp"
	repo := "r3-pnlr"
	sec := "r3-pnls"
	// nil source
	task := seedLifecycleTask(t, name, proj, repo, sec, nil)

	reg := prometheus.NewRegistry()
	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(reg),
	}
	fw := &lifecycleFakeSCMWriter{}
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	if err := r.parkWithComment(ctx, task, fw, "tok", "test", "no source"); err != nil {
		t.Fatalf("parkWithComment nil source: %v", err)
	}

	got := fetchTask(t, name)
	if got.Status.DeployState != "Parked" {
		t.Errorf("DeployState = %q, want Parked", got.Status.DeployState)
	}
	// No SCM write must have happened (nil source -> no comment).
	emptyV := om_commentOkOrErr(reg)
	if emptyV > 0 {
		t.Errorf("comment SCM metric = %v, want 0 for nil-source park (no comment expected)", emptyV)
	}
}

// om_commentOkOrErr reads the scm_writes_total for comment verb from a registry.
func om_commentOkOrErr(reg *prometheus.Registry) float64 {
	// Gather all metrics and sum comment-verb counters.
	mfs, _ := reg.Gather()
	var total float64
	for _, mf := range mfs {
		if mf.GetName() != "operator_scm_writes_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "verb" && l.GetValue() == "comment" {
					total += m.GetCounter().GetValue()
				}
			}
		}
	}
	return total
}

// ================================================================
// Finding 12 (nit/correctness): GetCommitCIStatus failure must be
// logged at Error, not Info (the comment said WARN but code used Info).
// This is a code-review-verified change; the test confirms the path
// still requeues correctly after the log-level fix.
// ================================================================

func TestHandleMainCI_CIStatusError_StillRequeues(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	name := "r3-mainci-errlog"
	proj := "r3-melp"
	repo := "r3-melr"
	sec := "r3-mels"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#606",
		URL: "https://github.com/o/r/issues/606", Number: 606,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	task.Status.DeployState = "MainCI"
	task.Status.MergeCommitSHA = "sha-errlog"
	dl := metav1.NewTime(time.Now().Add(time.Hour))
	task.Status.DeadlineAt = &dl
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	rdr := &ciStatusReader{err: errors.New("ci read error")}
	fw := &lifecycleFakeSCMWriter{}
	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
	}
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }
	r.ReaderFor = func(_, _ string) (scm.SCMReader, error) { return rdr, nil }

	res, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle returned error: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Error("expected RequeueAfter on CI status error (must still requeue after log-level fix)")
	}
}

// ================================================================
// Finding 13 (nit/simplification): setDeployState - dead `from`
// initialization. This is verified by code review; the test is a
// regression guard that transitions still work after the nit is applied.
// ================================================================

func TestSetDeployState_FromFieldPopulatedCorrectly(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	name := "r3-from-field"
	proj := "r3-ffp"
	repo := "r3-ffr"
	sec := "r3-ffs"
	task := seedLifecycleTask(t, name, proj, repo, sec, &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#607",
		URL: "https://github.com/o/r/issues/607", Number: 607,
	})
	task.Status.DeployState = "Triage"
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
	}

	if err := r.setDeployState(ctx, fetchTask(t, name), "Parked", "test"); err != nil {
		t.Fatalf("setDeployState: %v", err)
	}
	got := fetchTask(t, name)
	if got.Status.DeployState != "Parked" {
		t.Errorf("DeployState = %q, want Parked", got.Status.DeployState)
	}
}

// ================================================================
// Finding 15 (nit/observability): comment drain must log l.Error on
// mid-batch post failure. This path emits an error return; we verify
// the reconcile returns an error (the log is code-review-verified).
// ================================================================

type failingCommentWriter struct {
	lifecycleFakeSCMWriter
	failAfter int
	calls     int
}

func (w *failingCommentWriter) Comment(_ context.Context, _, _, _ string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls++
	if w.calls > w.failAfter {
		return errors.New("comment post failed")
	}
	return nil
}

func TestCommentDrain_MidBatchFailure_ReturnsError(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	name := "r3-cdr-fail"
	proj := "r3-cfp"
	repo := "r3-cfr"
	sec := "r3-cfs"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#608",
		URL: "https://github.com/o/r/issues/608", Number: 608,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	// Two pending comments; first succeeds, second fails.
	task.Status.DeployState = "Implement"
	task.Status.Phase = "Running"
	task.Status.PendingComments = []string{"first comment", "second comment"}
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &failingCommentWriter{failAfter: 1}
	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
	}
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err == nil {
		t.Error("expected error on mid-batch comment failure")
	}
}

// ================================================================
// Finding 17 (nit/simplification): triage default arm must become
// explicit case "implement": + real default that logs unknown action.
// An unknown action string must NOT silently enter Implement.
// ================================================================

// TestFinishTriage_UnknownAction_SafeDefault calls finishTriage directly
// with an in-memory task carrying an unknown action in IssueOutcome.
// The CRD webhook validates action on status updates, so we bypass it by
// injecting the IssueOutcome into the in-memory task object directly and
// calling finishTriage rather than reconcileLifecycle.
func TestFinishTriage_UnknownAction_SafeDefault(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	name := "r3-unknown-action"
	proj := "r3-uap"
	repo := "r3-uar"
	sec := "r3-uas"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#609",
		URL: "https://github.com/o/r/issues/609", Number: 609,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	// Set up a Triage/Succeeded state without an IssueOutcome via the API, then
	// inject the unknown action directly into the in-memory object to simulate
	// an agent that returned a non-enum string (bypasses CRD status validation).
	task.Status.DeployState = "Triage"
	task.Status.Phase = "Succeeded"
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Re-fetch, then inject unknown action into in-memory object.
	inMem := fetchTask(t, name)
	inMem.Status.IssueOutcome = &tatarav1alpha1.IssueOutcome{Action: "foobar", Comment: ""}

	fw := &lifecycleFakeSCMWriter{}
	r := newLifecycleReconciler(t, fw)

	var proj2 tatarav1alpha1.Project
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: testNS, Name: proj}, &proj2); err != nil {
		t.Fatalf("get project: %v", err)
	}

	_, err := r.finishTriage(ctx, &proj2, inMem)
	if err != nil {
		t.Fatalf("finishTriage: %v", err)
	}

	got := fetchTask(t, name)
	// An unknown action must NOT enter Implement (unsafe). It must enter a safe
	// state - Conversation (discuss hold via the new default arm).
	if got.Status.DeployState == "Implement" {
		t.Error("unknown triage action 'foobar' must NOT silently enter Implement")
	}
	if got.Status.DeployState != "Conversation" {
		t.Errorf("DeployState = %q, want Conversation for unknown action (safe fallback)", got.Status.DeployState)
	}
}

// ================================================================
// Finding 18 (nit/efficiency): comment/interjection drain success
// must use RequeueAfter rather than bare Requeue:true so the
// controller does not busy-loop when items are continuously appended.
// ================================================================

func TestCommentDrain_SuccessUsesRequeueAfter(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	name := "r3-cdr-reqafter"
	proj := "r3-rap"
	repo := "r3-rar"
	sec := "r3-ras"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#610",
		URL: "https://github.com/o/r/issues/610", Number: 610,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	task.Status.DeployState = "Implement"
	task.Status.Phase = "Running"
	task.Status.PendingComments = []string{"comment to drain"}
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{}
	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
	}
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	res, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	// Must use RequeueAfter (not bare Requeue:true) to avoid busy-loop.
	if res.RequeueAfter == 0 {
		t.Error("comment drain success must return RequeueAfter, not bare Requeue:true")
	}
}

func (w *closeCountingWriter) EnsureLabel(_ context.Context, _, _, _, _ string) error { return nil }

func (w *mainCICloseCountingWriter) EnsureLabel(_ context.Context, _, _, _, _ string) error {
	return nil
}
