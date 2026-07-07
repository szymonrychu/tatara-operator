// Copyright 2026 tatara authors.

package controller

// Tests for audit findings on lifecycle.go (2026-06-15).
// Each section matches the finding number from the spec.

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// newAuditReconciler builds a TaskReconciler with a dedicated registry and
// returns both so tests can assert on metrics directly.
func newAuditReconciler(t *testing.T, fw scm.SCMWriter) (*TaskReconciler, *obs.LifecycleMetrics, *obs.OperatorMetrics) {
	t.Helper()
	reg := prometheus.NewRegistry()
	lm := obs.NewLifecycleMetrics(reg)
	om := obs.NewOperatorMetrics(reg)
	r := &TaskReconciler{
		Client:           k8sClient,
		Scheme:           k8sClient.Scheme(),
		Metrics:          om,
		LifecycleMetrics: lm,
		Session:          newFakeSession(),
		PodConfig: agent.PodConfig{
			Namespace:           testNS,
			CallbackURL:         "http://op-internal.tatara.svc:8082",
			OIDCIssuer:          "https://keycloak.tatara.svc/realms/master",
			AnthropicSecretName: "anthropic",
			CLIOIDCSecretName:   "tatara-cli-oidc",
		},
	}
	if fw != nil {
		r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }
	}
	return r, lm, om
}

// ============================================================
// Findings 1 & 3: RecordGiveup on all terminal park paths
// ============================================================

// TestRecordGiveup_TriageFailed verifies Triage/Phase=Failed parks with
// RecordGiveup("triage-failed").
func TestRecordGiveup_TriageFailed(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "audit-giveup-triagefail"
	proj := "audit-gtf-proj"
	repo := "audit-gtf-repo"
	sec := "audit-gtf-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#401",
		URL: "https://github.com/o/r/issues/401", Number: 401,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.LifecycleState = "Triage"
	task.Status.Phase = "Failed"
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{}
	r, lm, _ := newAuditReconciler(t, fw)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	if v := testutil.ToFloat64(lm.GiveupTotal("triage-failed")); v != 1 {
		t.Errorf("giveup{triage-failed} = %v, want 1", v)
	}
	if got := fetchTask(t, name); got.Status.LifecycleState != "Parked" {
		t.Errorf("LifecycleState = %q, want Parked", got.Status.LifecycleState)
	}
}

// TestRecordGiveup_ImplementFailed verifies Implement/Phase=Failed parks with
// RecordGiveup("implement-failed").
func TestRecordGiveup_ImplementFailed(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "audit-giveup-implfail"
	proj := "audit-gif-proj"
	repo := "audit-gif-repo"
	sec := "audit-gif-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#402",
		URL: "https://github.com/o/r/issues/402", Number: 402,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.LifecycleState = "Implement"
	task.Status.Phase = "Failed"
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{}
	r, lm, _ := newAuditReconciler(t, fw)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	if v := testutil.ToFloat64(lm.GiveupTotal("implement-failed")); v != 1 {
		t.Errorf("giveup{implement-failed} = %v, want 1", v)
	}
	if got := fetchTask(t, name); got.Status.LifecycleState != "Parked" {
		t.Errorf("LifecycleState = %q, want Parked", got.Status.LifecycleState)
	}
}

// TestRecordGiveup_Refused verifies the codified-refusal park increments
// RecordGiveup("refused").
func TestRecordGiveup_Refused(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "audit-giveup-refused"
	proj := "audit-gr-proj"
	repo := "audit-gr-repo"
	sec := "audit-gr-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#403",
		URL: "https://github.com/o/r/issues/403", Number: 403,
		IsPR: false,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.LifecycleState = "Implement"
	task.Status.Phase = "Succeeded"
	task.Status.ImplementOutcome = &tatarav1alpha1.ImplementOutcome{
		Action: "declined", Reason: "already done in PR #100",
	}
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &noChangeRecordingSCMWriter{}
	r, lm, _ := newAuditReconciler(t, fw)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	if v := testutil.ToFloat64(lm.GiveupTotal("refused-declined")); v != 1 {
		t.Errorf("giveup{refused-declined} = %v, want 1", v)
	}
	if got := fetchTask(t, name); got.Status.LifecycleState != "Parked" {
		t.Errorf("LifecycleState = %q, want Parked", got.Status.LifecycleState)
	}
}

// TestRecordGiveup_AlreadyDone verifies an already_done outcome parks via the
// codified-terminal path with giveup reason "refused-already-done" and
// LifecycleState park reason "refused-already-done".
func TestRecordGiveup_AlreadyDone(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "audit-giveup-alreadydone"
	proj := "audit-gad-proj"
	repo := "audit-gad-repo"
	sec := "audit-gad-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#405",
		URL: "https://github.com/o/r/issues/405", Number: 405,
		IsPR: false,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.LifecycleState = "Implement"
	task.Status.Phase = "Succeeded"
	task.Status.ImplementOutcome = &tatarav1alpha1.ImplementOutcome{
		Action: "already_done", Reason: "fix already committed on the shared branch in PR #101",
	}
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &noChangeRecordingSCMWriter{}
	r, lm, _ := newAuditReconciler(t, fw)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	if v := testutil.ToFloat64(lm.GiveupTotal("refused-already-done")); v != 1 {
		t.Errorf("giveup{refused-already-done} = %v, want 1", v)
	}
	if got := fetchTask(t, name); got.Status.LifecycleState != "Parked" {
		t.Errorf("LifecycleState = %q, want Parked", got.Status.LifecycleState)
	}
}

// TestRecordGiveup_RefusedDeclinedLabel verifies the declined codified path
// now records giveup label "refused-declined" (split from the old "refused").
func TestRecordGiveup_RefusedDeclinedLabel(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "audit-giveup-refdecl"
	proj := "audit-grd-proj"
	repo := "audit-grd-repo"
	sec := "audit-grd-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#406",
		URL: "https://github.com/o/r/issues/406", Number: 406,
		IsPR: false,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.LifecycleState = "Implement"
	task.Status.Phase = "Succeeded"
	task.Status.ImplementOutcome = &tatarav1alpha1.ImplementOutcome{
		Action: "declined", Reason: "out of scope, tracked elsewhere",
	}
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &noChangeRecordingSCMWriter{}
	r, lm, _ := newAuditReconciler(t, fw)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	if v := testutil.ToFloat64(lm.GiveupTotal("refused-declined")); v != 1 {
		t.Errorf("giveup{refused-declined} = %v, want 1", v)
	}
}

// TestRecordGiveup_RefusedNoExplanation verifies the no-PR-and-no-explanation
// park increments RecordGiveup("refused-no-explanation").
func TestRecordGiveup_RefusedNoExplanation(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "audit-giveup-rne"
	proj := "audit-grne-proj"
	repo := "audit-grne-repo"
	sec := "audit-grne-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#404",
		URL: "https://github.com/o/r/issues/404", Number: 404,
		IsPR: false,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.LifecycleState = "Implement"
	task.Status.Phase = "Succeeded"
	// No ImplementOutcome (no declared decline) + retry cap already exhausted.
	task.Status.ImplementEmptyRetries = 2
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &noChangeRecordingSCMWriter{}
	r, lm, _ := newAuditReconciler(t, fw)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	if v := testutil.ToFloat64(lm.GiveupTotal("refused-no-explanation")); v != 1 {
		t.Errorf("giveup{refused-no-explanation} = %v, want 1", v)
	}
	if got := fetchTask(t, name); got.Status.LifecycleState != "Parked" {
		t.Errorf("LifecycleState = %q, want Parked", got.Status.LifecycleState)
	}
}

// TestRecordGiveup_NoPRNumber verifies MRCI with PR number 0 parks with
// RecordGiveup("no-pr-number").
func TestRecordGiveup_NoPRNumber(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "audit-giveup-nopr"
	proj := "audit-gnp-proj"
	repo := "audit-gnp-repo"
	sec := "audit-gnp-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#405",
		URL: "https://github.com/o/r/issues/405", Number: 405,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.LifecycleState = "MRCI"
	task.Status.PRNumber = 0
	dl := metav1.NewTime(time.Now().Add(time.Hour))
	task.Status.DeadlineAt = &dl
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &fakeWriterPark{prState: scm.PRState{Author: "bot"}}
	r, lm, _ := newAuditReconciler(t, &fw.lifecycleFakeSCMWriter)
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	if v := testutil.ToFloat64(lm.GiveupTotal("no-pr-number")); v != 1 {
		t.Errorf("giveup{no-pr-number} = %v, want 1", v)
	}
	if got := fetchTask(t, name); got.Status.LifecycleState != "Parked" {
		t.Errorf("LifecycleState = %q, want Parked", got.Status.LifecycleState)
	}
}

// ============================================================
// Finding 4: IssueOutcome metric for discuss and implement
// ============================================================

// TestIssueOutcomeMetric_Discuss verifies the "discuss" triage arm records
// IssueOutcome("discuss").
func TestIssueOutcomeMetric_Discuss(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, _, name := seedTriageSucceeded(t, "audit-discuss-metric", &tatarav1alpha1.IssueOutcome{
		Action: "discuss", Comment: "need more info",
	})

	reg := prometheus.NewRegistry()
	lm := obs.NewLifecycleMetrics(reg)
	om := obs.NewOperatorMetrics(reg)
	r.LifecycleMetrics = lm
	r.Metrics = om

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	if v := testutil.ToFloat64(om.IssueOutcomeTotal("discuss")); v != 1 {
		t.Errorf("issue_outcome{discuss} = %v, want 1", v)
	}
}

// TestIssueOutcomeMetric_Implement verifies the "implement" triage arm records
// IssueOutcome("implement"). Uses a third-party-authored task to bypass the
// self-approve gate.
func TestIssueOutcomeMetric_Implement(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	name := "audit-impl-outcome-metric"
	proj := "audit-iom-proj"
	repo := "audit-iom-repo"
	sec := "audit-iom-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#406",
		URL: "https://github.com/o/r/issues/406", Number: 406,
		// AuthorLogin set to a non-bot, non-maintainer to satisfy thirdPartyAuthor.
		AuthorLogin: "external-user",
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.LifecycleState = "Triage"
	task.Status.Phase = "Succeeded"
	task.Status.IssueOutcome = &tatarav1alpha1.IssueOutcome{Action: "implement"}
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{}
	r, _, om := newAuditReconciler(t, fw)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	if v := testutil.ToFloat64(om.IssueOutcomeTotal("implement")); v != 1 {
		t.Errorf("issue_outcome{implement} = %v, want 1", v)
	}
}

// ============================================================
// Finding 5: annBootCrashAttempts cleared on lifecycle-state transition
// ============================================================

// TestSetLifecycleState_ClearsBootCrashAttempts verifies that setLifecycleState
// deletes the annBootCrashAttempts annotation so a new state does not inherit
// the prior state's boot-crash budget.
func TestSetLifecycleState_ClearsBootCrashAttempts(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "audit-bootcrash-clear"
	proj := "audit-bcc-proj"
	repo := "audit-bcc-repo"
	sec := "audit-bcc-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#407",
		URL: "https://github.com/o/r/issues/407", Number: 407,
		// AuthorLogin set so that third-party autoapprove fires, bypassing self-approve gate.
		AuthorLogin: "external-user",
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	// Set annBootCrashAttempts to 3 in annotations before the transition.
	if task.Annotations == nil {
		task.Annotations = map[string]string{}
	}
	task.Annotations[annBootCrashAttempts] = "3"
	if err := k8sClient.Update(context.Background(), task); err != nil {
		t.Fatalf("seed annotations: %v", err)
	}
	task = fetchTask(t, name)
	task.Status.LifecycleState = "Triage"
	task.Status.Phase = "Succeeded"
	task.Status.IssueOutcome = &tatarav1alpha1.IssueOutcome{Action: "implement"}
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed status: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{}
	r := newLifecycleReconciler(t, fw)

	// Transition will be Triage->Implement via setLifecycleState.
	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	got := fetchTask(t, name)
	if v, ok := got.Annotations[annBootCrashAttempts]; ok && v != "" {
		t.Errorf("annBootCrashAttempts = %q after setLifecycleState, want cleared (empty or absent)", v)
	}
}

// ============================================================
// Finding 8: maxIterations comment error must not be silently discarded
// ============================================================

// TestMaxIterations_CommentErrorDoesNotBlock verifies that when the
// maxLifecycleIterations backstop comment fails, the task still parks (the
// error must not prevent the park transition).
func TestMaxIterations_CommentErrorDoesNotBlock(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "audit-maxiter-commenterr"
	proj := "audit-mice-proj"
	repo := "audit-mice-repo"
	sec := "audit-mice-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#408",
		URL: "https://github.com/o/r/issues/408", Number: 408,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.LifecycleState = "Implement"
	task.Status.Phase = ""
	task.Status.LifecycleIterations = 10 // at maxIter
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Writer that returns error on Comment.
	fw := &errCommentWriter{}
	r := newLifecycleReconciler(t, nil)
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle returned error (must be non-fatal): %v", err)
	}

	got := fetchTask(t, name)
	if got.Status.LifecycleState != "Parked" {
		t.Errorf("LifecycleState = %q, want Parked (comment error must not block park)", got.Status.LifecycleState)
	}
}

// errCommentWriter is an SCMWriter whose Comment always fails.
type errCommentWriter struct {
	lifecycleFakeSCMWriter
}

func (e *errCommentWriter) Comment(_ context.Context, _, _, _ string) error {
	return errTestComment
}

var errTestComment = &scm.HTTPError{Status: 500, Body: "server error", Path: "/comments"}

// ============================================================
// Finding 10 & 14: handleConversation nil-deadline must use configured idle minutes
// ============================================================

// TestHandleConversation_NilDeadlineUsesConversationIdleMinutes verifies that
// when DeadlineAt is nil the safety-net path uses ConversationIdleMinutes from
// the project config, not a hardcoded 60.
func TestHandleConversation_NilDeadlineUsesConversationIdleMinutes(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "audit-conv-nildeadline"
	proj := "audit-cnd-proj"
	repo := "audit-cnd-repo"
	sec := "audit-cnd-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#409",
		URL: "https://github.com/o/r/issues/409", Number: 409,
	}
	mkSecret(t, sec, map[string][]byte{"token": []byte("tok"), "webhookSecret": []byte("wh")})
	p := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: proj, Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: sec,
			Scm: &tatarav1alpha1.ScmSpec{
				Provider:                "github",
				Owner:                   "o",
				BotLogin:                "bot",
				ConversationIdleMinutes: 30, // non-default: 30 instead of 60
			},
			Agent: tatarav1alpha1.AgentSpec{
				Model: "claude-x", Image: "wrapper:1", PermissionMode: "bypassPermissions",
				MaxTurnsPerTask: 50, TurnTimeoutSeconds: 1800,
			},
		},
	}
	if err := k8sClient.Create(context.Background(), p); err != nil {
		t.Fatalf("create project: %v", err)
	}
	p.Status.Memory = stableMemStatus("http://mem.svc:8080")
	if err := k8sClient.Status().Update(context.Background(), p); err != nil {
		t.Fatalf("set memory ready: %v", err)
	}

	repoObj := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: repo, Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef:       proj,
			URL:              "https://github.com/o/r.git",
			DefaultBranch:    "main",
			ReingestSchedule: "0 6 * * *",
		},
	}
	if err := k8sClient.Create(context.Background(), repoObj); err != nil {
		t.Fatalf("create repo: %v", err)
	}

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: proj, RepositoryRef: repo,
			Goal: "test", Kind: "issueLifecycle",
			Source: src,
		},
	}
	if err := k8sClient.Create(context.Background(), task); err != nil {
		t.Fatalf("create task: %v", err)
	}
	task.Status.LifecycleState = "Conversation"
	task.Status.DeadlineAt = nil // no deadline set
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed status: %v", err)
	}

	before := time.Now()
	fw := &lifecycleFakeSCMWriter{}
	r := newLifecycleReconciler(t, fw)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	got := fetchTask(t, name)
	if got.Status.DeadlineAt == nil {
		t.Fatal("DeadlineAt must be set after nil-deadline safety-net path")
	}
	// The deadline should be ~30 min from now, not ~60.
	dl := got.Status.DeadlineAt.Time
	elapsed := dl.Sub(before)
	// Allow 2-minute margin around 30 minutes.
	if elapsed < 28*time.Minute || elapsed > 32*time.Minute {
		t.Errorf("DeadlineAt set to %v from now, want ~30min (ConversationIdleMinutes=30), not hardcoded 60",
			elapsed.Round(time.Minute))
	}
}

// ============================================================
// Finding 11: parsePRNumber returns 0 for non-numeric trailing segment
// ============================================================

// TestParsePRNumber_MalformedURL verifies that a malformed PR URL returns 0
// (the caller logs at ERROR for non-empty URLs that parse to 0).
func TestParsePRNumber_MalformedURL(t *testing.T) {
	cases := []struct {
		url  string
		want int
	}{
		{"https://github.com/o/r/pull/42", 42},
		{"https://github.com/o/r/pull/abc", 0},
		{"https://github.com/o/r/pull/", 0},
		{"", 0},
	}
	for _, c := range cases {
		got := parsePRNumber(c.url)
		if got != c.want {
			t.Errorf("parsePRNumber(%q) = %d, want %d", c.url, got, c.want)
		}
	}
}

// ============================================================
// Finding 12: maybeMarkHandoverResume safer update ordering
// ============================================================

// TestMaybeMarkHandoverResume_SafeOrdering verifies that after the
// handover-resume path runs, both Status.Handover and the annotation are
// consistently set.
func TestMaybeMarkHandoverResume_SafeOrdering(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	// Use MRCI failure path to trigger maybeMarkHandoverResume.
	// Token threshold: 50%, window: 100, lastTokens: 80 (80%) -> triggers.
	r, _, name := seedMRCITaskWithTokens(t, "audit-handover-order", 100, 50, 80,
		scm.PRState{Author: "bot", CIStatus: "failure"})

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	got := fetchTask(t, name)
	// Both Handover and annotation must be set (consistent state).
	if got.Status.Handover == "" {
		t.Error("Status.Handover must be set when context guard trips")
	}
	if got.Annotations[annPendingHandoverResume] != "true" {
		t.Errorf("annotation %q = %q, want 'true'", annPendingHandoverResume, got.Annotations[annPendingHandoverResume])
	}
}

// ============================================================
// Finding 13: ImplementEmptyRetry metric incremented in retry branch
// ============================================================

// TestImplementEmptyRetry_MetricIncremented verifies the empty-retry branch
// calls LifecycleMetrics.ImplementEmptyRetry() each time it re-spawns.
func TestImplementEmptyRetry_MetricIncremented(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "audit-emptyretry-metric"
	proj := "audit-erm-proj"
	repo := "audit-erm-repo"
	sec := "audit-erm-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#410",
		URL: "https://github.com/o/r/issues/410", Number: 410,
		IsPR: false,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.LifecycleState = "Implement"
	task.Status.Phase = "Succeeded"
	task.Status.ImplementEmptyRetries = 0 // first retry
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &noChangeRecordingSCMWriter{}
	r, lm, _ := newAuditReconciler(t, fw)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	// ImplementEmptyRetry must have been incremented once.
	if v := testutil.ToFloat64(lm.ImplementEmptyRetryTotal()); v != 1 {
		t.Errorf("implement_empty_retry_total = %v, want 1", v)
	}
}
