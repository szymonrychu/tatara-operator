package controller

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// ----- lifecycle SCM fake -----

type lifecycleFakeSCMWriter struct {
	scm.SCMWriter
	mu           sync.Mutex
	closeCalls   []struct{ repo, comment string }
	commentCalls []struct{ issueRef, body string }
	openCalls    []struct{ sourceBranch string }
	openPRURL    string
}

func (f *lifecycleFakeSCMWriter) CloseIssue(_ context.Context, _, repo string, _ int, comment string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeCalls = append(f.closeCalls, struct{ repo, comment string }{repo, comment})
	return nil
}

func (f *lifecycleFakeSCMWriter) Comment(_ context.Context, _, issueRef, body string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commentCalls = append(f.commentCalls, struct{ issueRef, body string }{issueRef, body})
	return nil
}

func (f *lifecycleFakeSCMWriter) OpenChange(_ context.Context, _, _, sourceBranch, _, _, _ string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.openCalls = append(f.openCalls, struct{ sourceBranch string }{sourceBranch})
	url := f.openPRURL
	if url == "" {
		url = "https://github.com/o/r/pull/42"
	}
	return url, nil
}

// newLifecycleReconciler builds a TaskReconciler wired with the given SCM writer.
func newLifecycleReconciler(t *testing.T, fw *lifecycleFakeSCMWriter) *TaskReconciler {
	t.Helper()
	reg := prometheus.NewRegistry()
	r := &TaskReconciler{
		Client:           k8sClient,
		Scheme:           k8sClient.Scheme(),
		Metrics:          obs.NewOperatorMetrics(reg),
		LifecycleMetrics: obs.NewLifecycleMetrics(reg),
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
	return r
}

// seedLifecycleTask creates the project+repo+secret+issueLifecycle task needed by Triage/Implement tests.
func seedLifecycleTask(t *testing.T, name, project, repo, scmSecret string, source *tatarav1alpha1.TaskSource) *tatarav1alpha1.Task {
	t.Helper()
	ctx := context.Background()

	mkSecret(t, scmSecret, map[string][]byte{"token": []byte("tok"), "webhookSecret": []byte("wh")})

	scmSpec := &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "bot"}
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: project, Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: scmSecret,
			Scm:          scmSpec,
			Agent: tatarav1alpha1.AgentSpec{
				Model: "claude-x", Image: "wrapper:1", PermissionMode: "bypassPermissions",
				MaxTurnsPerTask: 50, TurnTimeoutSeconds: 1800,
			},
		},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project %s: %v", project, err)
	}
	proj.Status.Memory = &tatarav1alpha1.MemoryStatus{Phase: "Ready", Endpoint: "http://mem.svc:8080"}
	if err := k8sClient.Status().Update(ctx, proj); err != nil {
		t.Fatalf("set memory ready: %v", err)
	}

	r := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: repo, Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef: project, URL: "https://github.com/o/r.git",
			DefaultBranch: "main", ReingestSchedule: "0 6 * * *",
		},
	}
	if err := k8sClient.Create(ctx, r); err != nil {
		t.Fatalf("create repo %s: %v", repo, err)
	}

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    project,
			RepositoryRef: repo,
			Goal:          "Issue #5: fix the login bug",
			Kind:          "issueLifecycle",
			Source:        source,
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task %s: %v", name, err)
	}
	return task
}

// ----- Task 3: setLifecycleState + metrics -----

func TestSetLifecycleState_TransitionsStateAndIncrementMetric(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	reg := prometheus.NewRegistry()
	m := obs.NewLifecycleMetrics(reg)

	mkSecret(t, "lc-state-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})

	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "lc-state-proj", Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{ScmSecretRef: "lc-state-scm"},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}

	repo := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "lc-state-repo", Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef:       "lc-state-proj",
			URL:              "https://github.com/o/r.git",
			DefaultBranch:    "main",
			ReingestSchedule: "0 6 * * *",
		},
	}
	if err := k8sClient.Create(ctx, repo); err != nil {
		t.Fatalf("create repo: %v", err)
	}

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "lc-state-task", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    "lc-state-proj",
			RepositoryRef: "lc-state-repo",
			Goal:          "test lifecycle state",
			Kind:          "issueLifecycle",
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task: %v", err)
	}

	r := &TaskReconciler{
		Client:           k8sClient,
		Scheme:           k8sClient.Scheme(),
		Metrics:          obs.NewOperatorMetrics(prometheus.NewRegistry()),
		LifecycleMetrics: m,
		Session:          newFakeSession(),
		PodConfig: agent.PodConfig{
			Namespace:           testNS,
			CallbackURL:         "http://op-internal.tatara.svc:8082",
			OIDCIssuer:          "https://keycloak.tatara.svc/realms/master",
			AnthropicSecretName: "anthropic",
			CLIOIDCSecretName:   "tatara-cli-oidc",
		},
	}

	if err := r.setLifecycleState(ctx, task, "Triage", "initial"); err != nil {
		t.Fatalf("setLifecycleState: %v", err)
	}

	// Verify state persisted.
	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "lc-state-task"}, got); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.Status.LifecycleState != "Triage" {
		t.Errorf("LifecycleState = %q, want Triage", got.Status.LifecycleState)
	}

	// Verify counter incremented.
	counter := testutil.ToFloat64(m.TransitionTotal("", "Triage"))
	if counter != 1 {
		t.Errorf("tatara_lifecycle_transition_total{from='',to=Triage} = %v, want 1", counter)
	}
}

// ----- Task 4: reconcileLifecycle skeleton dispatch -----

func TestReconcileLifecycle_EmptyStateInitializesToTriage(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	mkSecret(t, "lc-init-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})

	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "lc-init-proj", Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "lc-init-scm",
		},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}
	// Set memory ready so the gate passes.
	proj.Status.Memory = &tatarav1alpha1.MemoryStatus{Phase: "Ready", Endpoint: "http://mem.svc:8080"}
	if err := k8sClient.Status().Update(ctx, proj); err != nil {
		t.Fatalf("set memory ready: %v", err)
	}

	repo := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "lc-init-repo", Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef:       "lc-init-proj",
			URL:              "https://github.com/o/r.git",
			DefaultBranch:    "main",
			ReingestSchedule: "0 6 * * *",
		},
	}
	if err := k8sClient.Create(ctx, repo); err != nil {
		t.Fatalf("create repo: %v", err)
	}

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "lc-init-task", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    "lc-init-proj",
			RepositoryRef: "lc-init-repo",
			Goal:          "issue #1",
			Kind:          "issueLifecycle",
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task: %v", err)
	}

	reg := prometheus.NewRegistry()
	r := &TaskReconciler{
		Client:           k8sClient,
		Scheme:           k8sClient.Scheme(),
		Metrics:          obs.NewOperatorMetrics(reg),
		LifecycleMetrics: obs.NewLifecycleMetrics(reg),
		Session:          newFakeSession(),
		PodConfig: agent.PodConfig{
			Namespace:           testNS,
			CallbackURL:         "http://op-internal.tatara.svc:8082",
			OIDCIssuer:          "https://keycloak.tatara.svc/realms/master",
			AnthropicSecretName: "anthropic",
			CLIOIDCSecretName:   "tatara-cli-oidc",
		},
	}

	res, err := r.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: "lc-init-task"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Error("expected requeue after Triage initialization")
	}

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "lc-init-task"}, got); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.Status.LifecycleState != "Triage" {
		t.Errorf("LifecycleState = %q, want Triage", got.Status.LifecycleState)
	}
}

// TestReconcileLifecycle_UnknownStateReturnsError verifies that reconcileLifecycle
// returns a descriptive error for an unrecognised LifecycleState. The CRD enum
// prevents this through the API, so we call reconcileLifecycle directly on an
// in-memory task with a bogus state that bypasses CRD validation.
func TestReconcileLifecycle_UnknownStateReturnsError(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	mkSecret(t, "lc-unk-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})

	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "lc-unk-proj", Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "lc-unk-scm",
		},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}
	proj.Status.Memory = &tatarav1alpha1.MemoryStatus{Phase: "Ready", Endpoint: "http://mem.svc:8080"}
	if err := k8sClient.Status().Update(ctx, proj); err != nil {
		t.Fatalf("set memory ready: %v", err)
	}

	repo := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "lc-unk-repo", Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef:       "lc-unk-proj",
			URL:              "https://github.com/o/r.git",
			DefaultBranch:    "main",
			ReingestSchedule: "0 6 * * *",
		},
	}
	if err := k8sClient.Create(ctx, repo); err != nil {
		t.Fatalf("create repo: %v", err)
	}

	reg := prometheus.NewRegistry()
	r := &TaskReconciler{
		Client:           k8sClient,
		Scheme:           k8sClient.Scheme(),
		Metrics:          obs.NewOperatorMetrics(reg),
		LifecycleMetrics: obs.NewLifecycleMetrics(reg),
		Session:          newFakeSession(),
		PodConfig: agent.PodConfig{
			Namespace:           testNS,
			CallbackURL:         "http://op-internal.tatara.svc:8082",
			OIDCIssuer:          "https://keycloak.tatara.svc/realms/master",
			AnthropicSecretName: "anthropic",
			CLIOIDCSecretName:   "tatara-cli-oidc",
		},
	}

	// Construct a task in-memory with a bogus state (bypasses CRD enum validation).
	bogusTask := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "lc-unk-proj", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    "lc-unk-proj",
			RepositoryRef: "lc-unk-repo",
			Goal:          "issue #2",
			Kind:          "issueLifecycle",
		},
		Status: tatarav1alpha1.TaskStatus{
			LifecycleState: "NotAValidState",
		},
	}

	_, err := r.reconcileLifecycle(ctx, bogusTask)
	if err == nil {
		t.Error("expected error for unknown lifecycle state, got nil")
	}
}

// ----- Task 5: Triage state handler -----

// seedTriageSucceeded seeds a task in LifecycleState=Triage/Phase=Succeeded
// with the given IssueOutcome, then returns the reconciler and task name.
func seedTriageSucceeded(t *testing.T, nameSuffix string, outcome *tatarav1alpha1.IssueOutcome) (r *TaskReconciler, fw *lifecycleFakeSCMWriter, taskName string) {
	t.Helper()
	ctx := context.Background()
	name := "lc-triage-" + nameSuffix
	proj := "lc-tp-" + nameSuffix
	repo := "lc-tr-" + nameSuffix
	sec := "lc-ts-" + nameSuffix
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#5", URL: "https://github.com/o/r/issues/5",
		Number: 5,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	// Seed the task as if a Triage agent run completed: LifecycleState=Triage, Phase=Succeeded.
	task.Status.LifecycleState = "Triage"
	task.Status.Phase = "Succeeded"
	task.Status.IssueOutcome = outcome
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed triage succeeded status: %v", err)
	}
	fw = &lifecycleFakeSCMWriter{}
	r = newLifecycleReconciler(t, fw)
	return r, fw, name
}

func TestLifecycleTriage_Close(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, fw, name := seedTriageSucceeded(t, "close", &tatarav1alpha1.IssueOutcome{
		Action: "close", Comment: "out of scope",
	})

	_, err := r.reconcileLifecycle(ctx, func() *tatarav1alpha1.Task {
		tk := &tatarav1alpha1.Task{}
		if e := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, tk); e != nil {
			t.Fatalf("get task: %v", e)
		}
		return tk
	}())
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	fw.mu.Lock()
	defer fw.mu.Unlock()
	if len(fw.closeCalls) != 1 {
		t.Fatalf("CloseIssue call count = %d, want 1; closeCalls=%+v", len(fw.closeCalls), fw.closeCalls)
	}
	if fw.closeCalls[0].comment != "out of scope" {
		t.Errorf("CloseIssue comment = %q, want %q", fw.closeCalls[0].comment, "out of scope")
	}

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, got); err != nil {
		t.Fatalf("get task after: %v", err)
	}
	if got.Status.LifecycleState != "Done" {
		t.Errorf("LifecycleState = %q, want Done", got.Status.LifecycleState)
	}
	if got.Status.IssueOutcome != nil {
		t.Error("IssueOutcome must be cleared after consuming")
	}
}

func TestLifecycleTriage_Discuss(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, fw, name := seedTriageSucceeded(t, "discuss", &tatarav1alpha1.IssueOutcome{
		Action: "discuss", Comment: "I have two design questions...",
	})

	_, err := r.reconcileLifecycle(ctx, func() *tatarav1alpha1.Task {
		tk := &tatarav1alpha1.Task{}
		if e := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, tk); e != nil {
			t.Fatalf("get task: %v", e)
		}
		return tk
	}())
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	fw.mu.Lock()
	defer fw.mu.Unlock()
	if len(fw.commentCalls) != 1 {
		t.Fatalf("Comment call count = %d, want 1", len(fw.commentCalls))
	}
	if !strings.Contains(fw.commentCalls[0].body, "design questions") {
		t.Errorf("Comment body = %q, want to contain %q", fw.commentCalls[0].body, "design questions")
	}

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, got); err != nil {
		t.Fatalf("get task after: %v", err)
	}
	if got.Status.LifecycleState != "Conversation" {
		t.Errorf("LifecycleState = %q, want Conversation", got.Status.LifecycleState)
	}
	if got.Status.DeadlineAt == nil {
		t.Error("DeadlineAt must be set after discuss transition")
	}
	if got.Status.IssueOutcome != nil {
		t.Error("IssueOutcome must be cleared after consuming")
	}
}

func TestLifecycleTriage_Implement(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, fw, name := seedTriageSucceeded(t, "impl", &tatarav1alpha1.IssueOutcome{
		Action: "implement",
	})

	_, err := r.reconcileLifecycle(ctx, func() *tatarav1alpha1.Task {
		tk := &tatarav1alpha1.Task{}
		if e := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, tk); e != nil {
			t.Fatalf("get task: %v", e)
		}
		return tk
	}())
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	fw.mu.Lock()
	defer fw.mu.Unlock()
	if len(fw.closeCalls) != 0 {
		t.Error("CloseIssue must NOT be called for implement outcome")
	}
	if len(fw.commentCalls) != 0 {
		t.Error("Comment must NOT be called for implement outcome")
	}

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, got); err != nil {
		t.Fatalf("get task after: %v", err)
	}
	if got.Status.LifecycleState != "Implement" {
		t.Errorf("LifecycleState = %q, want Implement", got.Status.LifecycleState)
	}
	if got.Status.IssueOutcome != nil {
		t.Error("IssueOutcome must be cleared after consuming")
	}
}

func TestLifecycleTriage_NilOutcomeTreatedAsImplement(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, _, name := seedTriageSucceeded(t, "nilout", nil)

	_, err := r.reconcileLifecycle(ctx, func() *tatarav1alpha1.Task {
		tk := &tatarav1alpha1.Task{}
		if e := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, tk); e != nil {
			t.Fatalf("get task: %v", e)
		}
		return tk
	}())
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, got); err != nil {
		t.Fatalf("get task after: %v", err)
	}
	if got.Status.LifecycleState != "Implement" {
		t.Errorf("LifecycleState = %q, want Implement (nil outcome defaults to implement)", got.Status.LifecycleState)
	}
}

func TestLifecycleTriage_FailedTransitionsToParked(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-triage-failed"
	proj := "lc-tp-failed"
	repo := "lc-tr-failed"
	sec := "lc-ts-failed"
	src := &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#5", Number: 5}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.LifecycleState = "Triage"
	task.Status.Phase = "Failed"
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed failed status: %v", err)
	}

	r := newLifecycleReconciler(t, nil)
	_, err := r.reconcileLifecycle(ctx, func() *tatarav1alpha1.Task {
		tk := &tatarav1alpha1.Task{}
		if e := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, tk); e != nil {
			t.Fatalf("get task: %v", e)
		}
		return tk
	}())
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, got); err != nil {
		t.Fatalf("get task after: %v", err)
	}
	if got.Status.LifecycleState != "Parked" {
		t.Errorf("LifecycleState = %q, want Parked", got.Status.LifecycleState)
	}
}
