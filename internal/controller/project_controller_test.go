package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

func newProjectReconciler() *ProjectReconciler {
	r, _ := newProjectReconcilerWithReg()
	return r
}

func newProjectReconcilerWithReg() (*ProjectReconciler, *prometheus.Registry) {
	reg := prometheus.NewRegistry()
	return &ProjectReconciler{
		Client:              k8sClient,
		Scheme:              k8sClient.Scheme(),
		Metrics:             obs.NewOperatorMetrics(reg),
		LifecycleMetrics:    obs.NewLifecycleMetrics(reg),
		ExternalWebhookBase: "https://tatara.example/operator/webhooks",
		MemoryConfig: memory.Config{
			Namespace:        testNS,
			MemoryImage:      "harbor.example/tatara-memory:test",
			LightragImage:    "harbor.example/lightrag:test",
			Neo4jImage:       "neo4j:5-community",
			OpenAISecretName: "openai-shared",
			OIDCIssuer:       "https://keycloak.example/realms/tatara",
			OIDCAudience:     "tatara-memory",
		},
	}, reg
}

func reconcileProject(t *testing.T, name string) (ctrl.Result, error) {
	t.Helper()
	r := newProjectReconciler()
	return r.Reconcile(logf.IntoContext(context.Background(), logf.Log), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: name},
	})
}

func mkSecret(t *testing.T, name string, data map[string][]byte) {
	t.Helper()
	s := &corev1.Secret{}
	s.Name = name
	s.Namespace = testNS
	s.Data = data
	if err := k8sClient.Create(context.Background(), s); err != nil {
		t.Fatalf("create secret %s: %v", name, err)
	}
}

func getProject(t *testing.T, name string) *tataradevv1alpha1.Project {
	t.Helper()
	p := &tataradevv1alpha1.Project{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, p); err != nil {
		t.Fatalf("get project %s: %v", name, err)
	}
	return p
}

func waitProjectReady(t *testing.T, name string, want metav1.ConditionStatus) *tataradevv1alpha1.Project {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		p := getProject(t, name)
		c := apierrors.FindStatusCondition(p.Status.Conditions, "Ready")
		if c != nil && c.Status == want {
			return p
		}
		time.Sleep(interval)
	}
	t.Fatalf("project %s Ready never reached %s", name, want)
	return nil
}

func TestProjectReconcile_ValidSecret(t *testing.T) {
	ctx := context.Background()
	mkSecret(t, "valid-scm", map[string][]byte{
		"token":         []byte("ghp_x"),
		"webhookSecret": []byte("hmac"),
	})
	p := &tataradevv1alpha1.Project{}
	p.Name = "proj-valid"
	p.Namespace = testNS
	p.Spec.ScmSecretRef = "valid-scm"
	if err := k8sClient.Create(ctx, p); err != nil {
		t.Fatalf("create project: %v", err)
	}

	if _, err := reconcileProject(t, "proj-valid"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := waitProjectReady(t, "proj-valid", metav1.ConditionTrue)
	want := "https://tatara.example/operator/webhooks/proj-valid"
	if got.Status.WebhookURL != want {
		t.Errorf("webhookURL = %q, want %q", got.Status.WebhookURL, want)
	}
}

func TestProjectReconcile_MissingSecret(t *testing.T) {
	ctx := context.Background()
	p := &tataradevv1alpha1.Project{}
	p.Name = "proj-nosecret"
	p.Namespace = testNS
	p.Spec.ScmSecretRef = "does-not-exist"
	if err := k8sClient.Create(ctx, p); err != nil {
		t.Fatalf("create project: %v", err)
	}

	if _, err := reconcileProject(t, "proj-nosecret"); err != nil {
		t.Fatalf("reconcile returned error, want nil (status carries failure): %v", err)
	}
	got := waitProjectReady(t, "proj-nosecret", metav1.ConditionFalse)
	c := apierrors.FindStatusCondition(got.Status.Conditions, "Ready")
	if c.Reason != "SecretNotFound" {
		t.Errorf("reason = %q, want SecretNotFound", c.Reason)
	}
}

func TestProjectReconcile_MissingKeys(t *testing.T) {
	ctx := context.Background()
	mkSecret(t, "partial-scm", map[string][]byte{"token": []byte("ghp_x")})
	p := &tataradevv1alpha1.Project{}
	p.Name = "proj-partialkeys"
	p.Namespace = testNS
	p.Spec.ScmSecretRef = "partial-scm"
	if err := k8sClient.Create(ctx, p); err != nil {
		t.Fatalf("create project: %v", err)
	}

	if _, err := reconcileProject(t, "proj-partialkeys"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := waitProjectReady(t, "proj-partialkeys", metav1.ConditionFalse)
	c := apierrors.FindStatusCondition(got.Status.Conditions, "Ready")
	if c.Reason != "SecretMissingKeys" {
		t.Errorf("reason = %q, want SecretMissingKeys", c.Reason)
	}
}

// TestGaugeRecomputeThrottled verifies that maybeRecomputeGauges skips the
// expensive ProjectList+TaskList scans when called within the throttle interval
// and runs them once the interval has elapsed.
func TestGaugeRecomputeThrottled(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	// Two reconcilers with different intervals to test behaviour precisely.
	r := newProjectReconciler()
	// Short interval: first call should fire; second immediate call should skip.
	r.GaugeRecomputeInterval = 5 * time.Minute

	// First call: lastGaugeRecompute is zero, so recompute must run.
	before := r.lastGaugeRecompute
	r.maybeRecomputeGauges(ctx)
	if !r.lastGaugeRecompute.After(before) {
		t.Fatal("first maybeRecomputeGauges call did not update lastGaugeRecompute")
	}
	after := r.lastGaugeRecompute

	// Immediate second call: interval not elapsed, so lastGaugeRecompute must NOT change.
	r.maybeRecomputeGauges(ctx)
	if !r.lastGaugeRecompute.Equal(after) {
		t.Fatal("second immediate maybeRecomputeGauges call updated lastGaugeRecompute; expected skip")
	}

	// Backdate lastGaugeRecompute past the interval and confirm a third call fires.
	r.lastGaugeRecompute = time.Now().Add(-r.GaugeRecomputeInterval - time.Second)
	r.maybeRecomputeGauges(ctx)
	if !r.lastGaugeRecompute.After(after) {
		t.Fatal("third maybeRecomputeGauges call (interval elapsed) did not update lastGaugeRecompute")
	}
}

// gatherIssueState reads tatara_issue_state for the given issue from reg.
// Returns the gauge value if the series exists, or -1 when absent.
func gatherIssueState(t *testing.T, reg *prometheus.Registry, issue, state, incident string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "tatara_issue_state" {
			continue
		}
		for _, m := range mf.GetMetric() {
			var gotIssue, gotState, gotIncident string
			for _, lp := range m.GetLabel() {
				switch lp.GetName() {
				case "issue":
					gotIssue = lp.GetValue()
				case "state":
					gotState = lp.GetValue()
				case "incident":
					gotIncident = lp.GetValue()
				}
			}
			if gotIssue == issue && gotState == state && gotIncident == incident {
				return m.GetGauge().GetValue()
			}
		}
	}
	return -1
}

// TestUpdateIssueStateCounts_EmitsPerIssue verifies that a non-terminal
// issueLifecycle Task with an issue-scoped Source emits a gauge series.
func TestUpdateIssueStateCounts_EmitsPerIssue(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, reg := newProjectReconcilerWithReg()

	task := &tataradevv1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "isc-task-1", Namespace: testNS},
		Spec: tataradevv1alpha1.TaskSpec{
			ProjectRef:    "isc-proj",
			RepositoryRef: "isc-repo",
			Kind:          "issueLifecycle",
			Goal:          "test",
			Source: &tataradevv1alpha1.TaskSource{
				Provider: "github",
				IssueRef: "acme/repo#42",
				Number:   42,
			},
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task: %v", err)
	}
	task.Status.LifecycleState = "Implement"
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("set lifecycle state: %v", err)
	}

	r.updateIssueStateCounts(ctx)

	if got := gatherIssueState(t, reg, "acme/repo#42", "implementing", "false"); got != 1 {
		t.Fatalf("tatara_issue_state{issue=acme/repo#42,state=implementing,incident=false} = %v, want 1", got)
	}
}

// TestUpdateIssueStateCounts_MapsConversationToAwaitingApproval verifies the
// LifecycleState=Conversation -> state="awaiting-approval" mapping.
func TestUpdateIssueStateCounts_MapsConversationToAwaitingApproval(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, reg := newProjectReconcilerWithReg()

	task := &tataradevv1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "isc-task-conv", Namespace: testNS},
		Spec: tataradevv1alpha1.TaskSpec{
			ProjectRef:    "isc-proj-conv",
			RepositoryRef: "isc-repo-conv",
			Kind:          "issueLifecycle",
			Goal:          "test",
			Source: &tataradevv1alpha1.TaskSource{
				Provider: "github",
				IssueRef: "acme/repo#99",
				Number:   99,
			},
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task: %v", err)
	}
	task.Status.LifecycleState = "Conversation"
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("set lifecycle state: %v", err)
	}

	r.updateIssueStateCounts(ctx)

	if got := gatherIssueState(t, reg, "acme/repo#99", "awaiting-approval", "false"); got != 1 {
		t.Fatalf("tatara_issue_state{state=awaiting-approval} = %v, want 1", got)
	}
}

// TestUpdateIssueStateCounts_IncidentLabel verifies that a Task bearing the
// tatara.io/incident label emits incident="true".
func TestUpdateIssueStateCounts_IncidentLabel(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, reg := newProjectReconcilerWithReg()

	task := &tataradevv1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "isc-task-inc",
			Namespace: testNS,
			Labels:    map[string]string{labelIncident: "true"},
		},
		Spec: tataradevv1alpha1.TaskSpec{
			ProjectRef:    "isc-proj-inc",
			RepositoryRef: "isc-repo-inc",
			Kind:          "issueLifecycle",
			Goal:          "test",
			Source: &tataradevv1alpha1.TaskSource{
				Provider: "github",
				IssueRef: "acme/repo#101",
				Number:   101,
			},
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task: %v", err)
	}
	task.Status.LifecycleState = "Triage"
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("set lifecycle state: %v", err)
	}

	r.updateIssueStateCounts(ctx)

	if got := gatherIssueState(t, reg, "acme/repo#101", "triage", "true"); got != 1 {
		t.Fatalf("tatara_issue_state{issue=acme/repo#101,incident=true} = %v, want 1", got)
	}
}

// TestUpdateIssueStateCounts_SkipsTerminalAndEmptyIssue verifies that terminal
// Tasks and project-scoped Tasks (empty issue) produce no gauge series.
func TestUpdateIssueStateCounts_SkipsTerminalAndEmptyIssue(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, reg := newProjectReconcilerWithReg()

	// Terminal lifecycle Task (Done).
	done := &tataradevv1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "isc-task-done", Namespace: testNS},
		Spec: tataradevv1alpha1.TaskSpec{
			ProjectRef:    "isc-skip-proj",
			RepositoryRef: "isc-skip-repo",
			Kind:          "issueLifecycle",
			Goal:          "test",
			Source:        &tataradevv1alpha1.TaskSource{Provider: "github", IssueRef: "acme/repo#200", Number: 200},
		},
	}
	if err := k8sClient.Create(ctx, done); err != nil {
		t.Fatalf("create done task: %v", err)
	}
	done.Status.LifecycleState = "Done"
	if err := k8sClient.Status().Update(ctx, done); err != nil {
		t.Fatalf("set lifecycle state Done: %v", err)
	}

	// Project-scoped incident Task (no Source -> empty issue).
	incident := &tataradevv1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "isc-task-prj", Namespace: testNS},
		Spec: tataradevv1alpha1.TaskSpec{
			ProjectRef: "isc-skip-proj",
			Kind:       "incident",
			Goal:       "test",
		},
	}
	if err := k8sClient.Create(ctx, incident); err != nil {
		t.Fatalf("create incident task: %v", err)
	}

	r.updateIssueStateCounts(ctx)

	// Neither task must produce a series.
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "tatara_issue_state" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "issue" {
					v := lp.GetValue()
					if v == "acme/repo#200" || v == "" {
						t.Errorf("unexpected tatara_issue_state series with issue=%q (terminal or empty-issue task must be excluded)", v)
					}
				}
			}
		}
	}
}

// TestUpdateIssueStateCounts_ResetsClosedIssue verifies that an issue that
// was emitted on pass 1 but whose Task is terminal on pass 2 vanishes after
// the second pass (Reset+Set-only-live semantics).
func TestUpdateIssueStateCounts_ResetsClosedIssue(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, reg := newProjectReconcilerWithReg()

	task := &tataradevv1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "isc-task-close", Namespace: testNS},
		Spec: tataradevv1alpha1.TaskSpec{
			ProjectRef:    "isc-close-proj",
			RepositoryRef: "isc-close-repo",
			Kind:          "issueLifecycle",
			Goal:          "test",
			Source:        &tataradevv1alpha1.TaskSource{Provider: "github", IssueRef: "acme/repo#300", Number: 300},
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task: %v", err)
	}
	task.Status.LifecycleState = "Triage"
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("set lifecycle state Triage: %v", err)
	}

	// Pass 1: issue is live.
	r.updateIssueStateCounts(ctx)
	if got := gatherIssueState(t, reg, "acme/repo#300", "triage", "false"); got != 1 {
		t.Fatalf("pass 1: tatara_issue_state = %v, want 1", got)
	}

	// Transition to terminal (Done).
	task = &tataradevv1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "isc-task-close"}, task); err != nil {
		t.Fatalf("get task: %v", err)
	}
	task.Status.LifecycleState = "Done"
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("set lifecycle state Done: %v", err)
	}

	// Pass 2: issue is terminal -> no series.
	r.updateIssueStateCounts(ctx)
	if got := gatherIssueState(t, reg, "acme/repo#300", "triage", "false"); got != -1 {
		t.Fatalf("pass 2: tatara_issue_state for done issue = %v, want absent (-1)", got)
	}
}

// TestIssueStateFor_Blocked verifies that an issueLifecycle Task in Parked with
// a recoverable ParkReason and ImplementGiveUps >= maxImplGiveUps returns "blocked"
// so the metric surface reflects the at-cap state.
func TestIssueStateFor_Blocked(t *testing.T) {
	// At cap with recoverable reason -> blocked.
	task := &tataradevv1alpha1.Task{}
	task.Spec.Kind = "issueLifecycle"
	task.Status.LifecycleState = "Parked"
	task.Status.ParkReason = "implement-failed"
	task.Status.ImplementGiveUps = maxImplGiveUps
	if got := issueStateFor(task); got != "blocked" {
		t.Errorf("issueStateFor at-cap give-up = %q, want blocked", got)
	}

	// Under cap (giveUps=1) -> empty (still terminal, reaper spares it).
	task2 := &tataradevv1alpha1.Task{}
	task2.Spec.Kind = "issueLifecycle"
	task2.Status.LifecycleState = "Parked"
	task2.Status.ParkReason = "maxIterations"
	task2.Status.ImplementGiveUps = 1
	if got := issueStateFor(task2); got != "" {
		t.Errorf("issueStateFor under-cap give-up = %q, want empty (not yet blocked)", got)
	}

	// Non-recoverable reason at giveUps=3 -> empty (normal terminal).
	task3 := &tataradevv1alpha1.Task{}
	task3.Spec.Kind = "issueLifecycle"
	task3.Status.LifecycleState = "Parked"
	task3.Status.ParkReason = "refused-declined"
	task3.Status.ImplementGiveUps = maxImplGiveUps
	if got := issueStateFor(task3); got != "" {
		t.Errorf("issueStateFor non-recoverable parked = %q, want empty (not blocked)", got)
	}
}

// TestUpdateIssueStateCounts_BlockedEmitsSeries verifies that an at-cap give-up
// Parked task emits a tatara_issue_state series with state="blocked".
func TestUpdateIssueStateCounts_BlockedEmitsSeries(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, reg := newProjectReconcilerWithReg()

	task := &tataradevv1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "isc-task-blocked", Namespace: testNS},
		Spec: tataradevv1alpha1.TaskSpec{
			ProjectRef:    "isc-blocked-proj",
			RepositoryRef: "isc-blocked-repo",
			Kind:          "issueLifecycle",
			Goal:          "test",
			Source: &tataradevv1alpha1.TaskSource{
				Provider: "github",
				IssueRef: "acme/repo#999",
				Number:   999,
			},
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task: %v", err)
	}
	task.Status.LifecycleState = "Parked"
	task.Status.ParkReason = "implement-failed"
	task.Status.ImplementGiveUps = maxImplGiveUps
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("set status: %v", err)
	}

	r.updateIssueStateCounts(ctx)

	if got := gatherIssueState(t, reg, "acme/repo#999", "blocked", "false"); got != 1 {
		t.Fatalf("tatara_issue_state{issue=acme/repo#999,state=blocked} = %v, want 1", got)
	}
}

// TestIssueStateFor covers the pure state-mapping table for all LifecycleState
// and Phase/Kind combos, including terminal (must return "") and review tasks.
func TestIssueStateFor(t *testing.T) {
	cases := []struct {
		name      string
		kind      string
		lifecycle string
		phase     string
		want      string
	}{
		{"lifecycle Triage", "issueLifecycle", "Triage", "", "triage"},
		{"lifecycle Conversation", "issueLifecycle", "Conversation", "", "awaiting-approval"},
		{"lifecycle Implement", "issueLifecycle", "Implement", "", "implementing"},
		{"lifecycle MRCI", "issueLifecycle", "MRCI", "", "mr-ci"},
		{"lifecycle Merge", "issueLifecycle", "Merge", "", "merging"},
		{"lifecycle MainCI", "issueLifecycle", "MainCI", "", "main-ci"},
		{"lifecycle Done (terminal)", "issueLifecycle", "Done", "", ""},
		{"lifecycle Stopped (terminal)", "issueLifecycle", "Stopped", "", ""},
		{"lifecycle Parked (terminal)", "issueLifecycle", "Parked", "", ""},
		{"lifecycle empty state", "issueLifecycle", "", "", ""},
		{"review Planning", "review", "", "Planning", "reviewing"},
		{"review Running", "review", "", "Running", "reviewing"},
		{"review Succeeded (terminal)", "review", "", "Succeeded", ""},
		{"review Failed (terminal)", "review", "", "Failed", ""},
		{"other kind", "brainstorm", "", "", ""},
		{"incident kind", "incident", "", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := &tataradevv1alpha1.Task{}
			task.Spec.Kind = tc.kind
			task.Status.LifecycleState = tc.lifecycle
			task.Status.Phase = tc.phase
			got := issueStateFor(task)
			if got != tc.want {
				t.Errorf("issueStateFor{kind=%q, lifecycle=%q, phase=%q} = %q, want %q",
					tc.kind, tc.lifecycle, tc.phase, got, tc.want)
			}
		})
	}
}

var _ = client.IgnoreNotFound
