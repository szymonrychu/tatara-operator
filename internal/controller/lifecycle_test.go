package controller

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// ----- lifecycle SCM fake -----

type lifecycleFakeSCMWriter struct {
	scm.SCMWriter
	mu           sync.Mutex
	closeCalls   []struct{ repo, comment string }
	commentCalls []struct{ issueRef, body string }
	openCalls    []struct {
		repoURL, sourceBranch, body string
	}
	openPRURL string
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

func (f *lifecycleFakeSCMWriter) OpenChange(_ context.Context, repoURL, _, sourceBranch, _, _, body string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.openCalls = append(f.openCalls, struct {
		repoURL, sourceBranch, body string
	}{repoURL, sourceBranch, body})
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

// ----- Task 6: Implement state handler -----

func TestLifecycleImplement_SucceededOpensMRAndEntersMRCI(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-impl-ok"
	proj := "lc-ip-ok"
	repo := "lc-ir-ok"
	sec := "lc-is-ok"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#10", URL: "https://github.com/o/r/issues/10",
		Number: 10,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	// Seed: Implement agent run completed successfully.
	// LifecycleIterations=1: spawn already incremented it when Phase was "".
	task.Status.LifecycleState = "Implement"
	task.Status.Phase = "Succeeded"
	task.Status.LifecycleIterations = 1
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed implement succeeded: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{openPRURL: "https://github.com/o/r/pull/42"}
	r := newLifecycleReconciler(t, fw)

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
	if len(fw.openCalls) == 0 {
		t.Fatal("OpenChange must be called for Implement succeeded")
	}
	wantBranch := taskBranch(&tatarav1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS}})
	if fw.openCalls[0].sourceBranch != wantBranch {
		t.Errorf("OpenChange sourceBranch = %q, want %q", fw.openCalls[0].sourceBranch, wantBranch)
	}

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, got); err != nil {
		t.Fatalf("get task after: %v", err)
	}
	if got.Status.LifecycleState != "MRCI" {
		t.Errorf("LifecycleState = %q, want MRCI", got.Status.LifecycleState)
	}
	if got.Status.PrURL != "https://github.com/o/r/pull/42" {
		t.Errorf("PrURL = %q, want https://github.com/o/r/pull/42", got.Status.PrURL)
	}
	if got.Status.PRNumber != 42 {
		t.Errorf("PRNumber = %d, want 42", got.Status.PRNumber)
	}
	if got.Status.HeadBranch == "" {
		t.Error("HeadBranch must be set")
	}
	if got.Status.LifecycleIterations != 1 {
		t.Errorf("LifecycleIterations = %d, want 1", got.Status.LifecycleIterations)
	}
}

// noChangeSCMWriter returns 422 for OpenChange, simulating no-diff / branch absent.
type noChangeSCMWriter struct{ scm.SCMWriter }

func (n *noChangeSCMWriter) OpenChange(_ context.Context, _, _, _, _, _, _ string) (string, error) {
	return "", &scm.HTTPError{Status: 422, Body: "no diff", Path: "/pulls"}
}

func (n *noChangeSCMWriter) Comment(_ context.Context, _, _, _ string) error { return nil }

func TestLifecycleImplement_NoPRTransitionsToParked(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-impl-nopr"
	proj := "lc-ip-nopr"
	repo := "lc-ir-nopr"
	sec := "lc-is-nopr"
	src := &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#11", Number: 11}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.LifecycleState = "Implement"
	task.Status.Phase = "Succeeded"
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{}
	r := newLifecycleReconciler(t, fw)
	// Override SCMFor to return a writer that returns 422.
	r.SCMFor = func(string) (scm.SCMWriter, error) {
		return &noChangeSCMWriter{}, nil
	}

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
		t.Errorf("LifecycleState = %q, want Parked (no-change)", got.Status.LifecycleState)
	}
}

func TestLifecycleImplement_FailedTransitionsToParked(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-impl-fail"
	proj := "lc-ip-fail"
	repo := "lc-ir-fail"
	sec := "lc-is-fail"
	src := &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#12", Number: 12}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.LifecycleState = "Implement"
	task.Status.Phase = "Failed"
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
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
		t.Errorf("LifecycleState = %q, want Parked (implement-failed)", got.Status.LifecycleState)
	}
}

// ----- FIX 1: concurrency gate must not block terminal-phase outcome consumption -----

// TestLifecycleTriage_ConcurrencyCapDoesNotBlockFinishTriage asserts that a Triage
// task with Phase=Succeeded still runs finishTriage (consumes outcome, transitions
// to Implement) even when the project is at the MaxConcurrentTasks concurrency cap.
// The gates must only fire on the SPAWN path, not when finishing a completed run.
func TestLifecycleTriage_ConcurrencyCapDoesNotBlockFinishTriage(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	// Suffix to keep resources unique in the shared envtest namespace.
	const suffix = "capgate"
	name := "lc-triage-" + suffix
	projName := "lc-tp-" + suffix
	repoName := "lc-tr-" + suffix
	sec := "lc-ts-" + suffix

	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#99", URL: "https://github.com/o/r/issues/99",
		Number: 99,
	}
	task := seedLifecycleTask(t, name, projName, repoName, sec, src)

	// Put the task into Triage / Succeeded with an implement outcome.
	task.Status.LifecycleState = "Triage"
	task.Status.Phase = "Succeeded"
	task.Status.IssueOutcome = &tatarav1alpha1.IssueOutcome{Action: "implement"}
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed triage succeeded: %v", err)
	}

	// Fill the concurrency cap: create MaxConcurrentTasks (default 3) sibling tasks
	// in active "Running" phase so atConcurrencyCap returns true.
	proj := &tatarav1alpha1.Project{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: projName}, proj); err != nil {
		t.Fatalf("get project: %v", err)
	}
	maxConc := proj.Spec.MaxConcurrentTasks
	if maxConc <= 0 {
		maxConc = 3
	}
	for i := range maxConc {
		blockerName := "blocker-capgate-" + string(rune('a'+i))
		blocker := &tatarav1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{
				Name:      blockerName,
				Namespace: testNS,
			},
			Spec: tatarav1alpha1.TaskSpec{
				ProjectRef:    projName,
				RepositoryRef: repoName,
				Goal:          "blocker",
				Kind:          "implement",
			},
		}
		if err := k8sClient.Create(ctx, blocker); err != nil {
			t.Fatalf("create blocker task %d: %v", i, err)
		}
		blocker.Status.Phase = "Running"
		if err := k8sClient.Status().Update(ctx, blocker); err != nil {
			t.Fatalf("set blocker running %d: %v", i, err)
		}
	}

	r := newLifecycleReconciler(t, &lifecycleFakeSCMWriter{})

	// Reconcile: despite being at cap the terminal-phase Triage task must finish.
	_, err := r.reconcileLifecycle(ctx, func() *tatarav1alpha1.Task {
		tk := &tatarav1alpha1.Task{}
		if e := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, tk); e != nil {
			t.Fatalf("get task: %v", e)
		}
		return tk
	}())
	if err != nil {
		t.Fatalf("reconcileLifecycle at cap: %v", err)
	}

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, got); err != nil {
		t.Fatalf("get task after: %v", err)
	}
	if got.Status.LifecycleState != "Implement" {
		t.Errorf("LifecycleState = %q, want Implement; concurrency cap must not block finishTriage", got.Status.LifecycleState)
	}
}

// ----- FIX 3+5: reconcileLifecycle initializes LifecycleState from lifecycle-entry annotation -----

// TestReconcileLifecycle_AnnotationEntryImplement asserts that when LifecycleState
// is empty but the tatara.dev/lifecycle-entry annotation is "Implement", the first
// reconcile sets LifecycleState=Implement (not the default Triage).
func TestReconcileLifecycle_AnnotationEntryImplement(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	mkSecret(t, "lc-annimpl-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})

	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "lc-annimpl-proj", Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{ScmSecretRef: "lc-annimpl-scm"},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}
	proj.Status.Memory = &tatarav1alpha1.MemoryStatus{Phase: "Ready", Endpoint: "http://mem.svc:8080"}
	if err := k8sClient.Status().Update(ctx, proj); err != nil {
		t.Fatalf("set memory ready: %v", err)
	}
	repo := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "lc-annimpl-repo", Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef: "lc-annimpl-proj", URL: "https://github.com/o/r.git",
			DefaultBranch: "main", ReingestSchedule: "0 6 * * *",
		},
	}
	if err := k8sClient.Create(ctx, repo); err != nil {
		t.Fatalf("create repo: %v", err)
	}

	// Task with lifecycle-entry=Implement annotation, empty LifecycleState.
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "lc-annimpl-task",
			Namespace:   testNS,
			Annotations: map[string]string{"tatara.dev/lifecycle-entry": "Implement"},
		},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "lc-annimpl-proj", RepositoryRef: "lc-annimpl-repo",
			Goal: "issue #3", Kind: "issueLifecycle",
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
			Namespace: testNS, CallbackURL: "http://op-internal.tatara.svc:8082",
			OIDCIssuer:          "https://keycloak.tatara.svc/realms/master",
			AnthropicSecretName: "anthropic", CLIOIDCSecretName: "tatara-cli-oidc",
		},
	}

	_, err := r.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: "lc-annimpl-task"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "lc-annimpl-task"}, got); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.Status.LifecycleState != "Implement" {
		t.Errorf("LifecycleState = %q, want Implement (from annotation); default would be Triage", got.Status.LifecycleState)
	}
}

// TestReconcileLifecycle_NoAnnotationDefaultsTriage asserts that without the
// lifecycle-entry annotation, LifecycleState is still initialized to Triage.
func TestReconcileLifecycle_NoAnnotationDefaultsTriage(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	mkSecret(t, "lc-noann-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})

	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "lc-noann-proj", Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{ScmSecretRef: "lc-noann-scm"},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}
	proj.Status.Memory = &tatarav1alpha1.MemoryStatus{Phase: "Ready", Endpoint: "http://mem.svc:8080"}
	if err := k8sClient.Status().Update(ctx, proj); err != nil {
		t.Fatalf("set memory ready: %v", err)
	}
	repo := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "lc-noann-repo", Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef: "lc-noann-proj", URL: "https://github.com/o/r.git",
			DefaultBranch: "main", ReingestSchedule: "0 6 * * *",
		},
	}
	if err := k8sClient.Create(ctx, repo); err != nil {
		t.Fatalf("create repo: %v", err)
	}

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "lc-noann-task", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "lc-noann-proj", RepositoryRef: "lc-noann-repo",
			Goal: "issue #4", Kind: "issueLifecycle",
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
			Namespace: testNS, CallbackURL: "http://op-internal.tatara.svc:8082",
			OIDCIssuer:          "https://keycloak.tatara.svc/realms/master",
			AnthropicSecretName: "anthropic", CLIOIDCSecretName: "tatara-cli-oidc",
		},
	}

	_, err := r.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: "lc-noann-task"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "lc-noann-task"}, got); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.Status.LifecycleState != "Triage" {
		t.Errorf("LifecycleState = %q, want Triage (default when no annotation)", got.Status.LifecycleState)
	}
}

// ----- Task 2: Implement re-entry context prompt + field clear -----

// TestLifecycleImplementPlanText_PlainWhenContextEmpty verifies that when
// ImplementContext is empty the prompt is the plain planTurnText output.
func TestLifecycleImplementPlanText_PlainWhenContextEmpty(t *testing.T) {
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "task-plain", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "proj", RepositoryRef: "repo",
			Goal: "fix the bug", Kind: "issueLifecycle",
		},
		Status: tatarav1alpha1.TaskStatus{ImplementContext: ""},
	}
	got := implementPrompt(task)
	want := planTurnText(task.Spec.Goal, taskBranch(task), task.Spec.ProjectRef, task.Name)
	if got != want {
		t.Errorf("implementPrompt with empty context:\ngot:  %q\nwant: %q", got, want)
	}
}

// TestLifecycleImplementPlanText_IncludesContextBlockWhenSet verifies that
// when ImplementContext is non-empty the prompt contains both the base plan
// text and a "## Re-entry context" block with the context value.
func TestLifecycleImplementPlanText_IncludesContextBlockWhenSet(t *testing.T) {
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "task-reentry", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "proj", RepositoryRef: "repo",
			Goal: "fix the bug", Kind: "issueLifecycle",
		},
		Status: tatarav1alpha1.TaskStatus{ImplementContext: "CI failed: test_login timed out"},
	}
	got := implementPrompt(task)
	if !strings.Contains(got, planTurnText(task.Spec.Goal, taskBranch(task), task.Spec.ProjectRef, task.Name)) {
		t.Error("implementPrompt with context must include the base plan text")
	}
	if !strings.Contains(got, "## Re-entry context") {
		t.Errorf("implementPrompt with context must contain '## Re-entry context'; got: %q", got)
	}
	if !strings.Contains(got, "CI failed: test_login timed out") {
		t.Errorf("implementPrompt with context must contain the context detail; got: %q", got)
	}
}

// TestLifecycleImplement_ContextClearedAfterRunStarts verifies that after
// handleImplement spawns the first pod and sets Phase=Planning, ImplementContext
// is cleared on the persisted Task so a later fresh entry is not re-prompted
// with the old context.
func TestLifecycleImplement_ContextClearedAfterRunStarts(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-impl-ctx-clear"
	proj := "lc-icc-proj"
	repo := "lc-icc-repo"
	sec := "lc-icc-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#30", URL: "https://github.com/o/r/issues/30",
		Number: 30,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	// State: ready to spawn a fresh implement run; ImplementContext is set (re-entry).
	task.Status.LifecycleState = "Implement"
	task.Status.Phase = ""
	task.Status.ImplementContext = "CI failed: test_auth timed out"
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed implement re-entry: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{}
	r := newLifecycleReconciler(t, fw)

	// First reconcile: ensurePodAndService creates pod, sets Phase=Planning, requeues.
	_, err := r.reconcileLifecycle(ctx, func() *tatarav1alpha1.Task {
		tk := &tatarav1alpha1.Task{}
		if e := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, tk); e != nil {
			t.Fatalf("get task: %v", e)
		}
		return tk
	}())
	if err != nil {
		t.Fatalf("reconcileLifecycle (spawn): %v", err)
	}

	// After the spawn reconcile, ImplementContext must be cleared on the persisted task.
	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, got); err != nil {
		t.Fatalf("get task after spawn: %v", err)
	}
	if got.Status.ImplementContext != "" {
		t.Errorf("ImplementContext = %q after spawn, want empty (must be cleared when run starts)", got.Status.ImplementContext)
	}
}

// TestLifecycleImplement_ContextInPromptWhenPodReady verifies that when a
// task with ImplementContext set reaches the driveTurns step (pod ready, no
// current turn), the submitted turn text contains the re-entry context block.
func TestLifecycleImplement_ContextInPromptWhenPodReady(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-impl-ctx-prompt"
	proj := "lc-icp-proj"
	repo := "lc-icp-repo"
	sec := "lc-icp-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#31", URL: "https://github.com/o/r/issues/31",
		Number: 31,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	// State: pod exists and is ready, Phase=Planning, ImplementContext set.
	task.Status.LifecycleState = "Implement"
	task.Status.Phase = "Planning"
	task.Status.PodName = agent.PodName(task)
	task.Status.ImplementContext = "CI failed: build timed out"
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Create a ready pod so podReady returns true.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      agent.PodName(task),
			Namespace: testNS,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "wrapper", Image: "wrapper:1"}},
		},
	}
	if err := k8sClient.Create(ctx, pod); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	pod.Status.Conditions = []corev1.PodCondition{{
		Type: corev1.PodReady, Status: corev1.ConditionTrue,
	}}
	if err := k8sClient.Status().Update(ctx, pod); err != nil {
		t.Fatalf("set pod ready: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{}
	sess := newFakeSession()
	r := newLifecycleReconciler(t, fw)
	r.Session = sess

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

	sub, ok := sess.lastSubmit()
	if !ok {
		t.Fatal("expected a SubmitTurn call; none recorded")
	}
	if !strings.Contains(sub.Text, "## Re-entry context") {
		t.Errorf("submitted turn text missing re-entry context block; text=%q", sub.Text)
	}
	if !strings.Contains(sub.Text, "CI failed: build timed out") {
		t.Errorf("submitted turn text missing context detail; text=%q", sub.Text)
	}
}

// ----- Task 7: Closes #N on lifecycle MR body (primary repo only) -----

// seedLifecycleTaskWithSecondaryRepo creates the same objects as seedLifecycleTask
// plus a second Repository in the same project. Returns the task and secondary repo name.
func seedLifecycleTaskWithSecondaryRepo(t *testing.T, name, proj, primaryRepo, secondaryRepo, scmSecret string, source *tatarav1alpha1.TaskSource) *tatarav1alpha1.Task {
	t.Helper()
	task := seedLifecycleTask(t, name, proj, primaryRepo, scmSecret, source)
	// Add a second repo to the same project.
	r2 := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: secondaryRepo, Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef: proj, URL: "https://github.com/o/r2.git",
			DefaultBranch: "main", ReingestSchedule: "0 6 * * *",
		},
	}
	if err := k8sClient.Create(context.Background(), r2); err != nil {
		t.Fatalf("create secondary repo %s: %v", secondaryRepo, err)
	}
	return task
}

// TestLifecycleImplement_ClosesIssueInPrimaryRepoMRBody verifies that an
// issue-linked lifecycle task's MR body for the PRIMARY repo contains
// "Closes #<issueNumber>".
func TestLifecycleImplement_ClosesIssueInPrimaryRepoMRBody(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-closes-primary"
	proj := "lc-cp-proj"
	primaryRepo := "lc-cp-repo"
	sec := "lc-cp-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#50",
		URL: "https://github.com/o/r/issues/50", Number: 50,
		IsPR: false,
	}
	task := seedLifecycleTask(t, name, proj, primaryRepo, sec, src)
	task.Status.LifecycleState = "Implement"
	task.Status.Phase = "Succeeded"
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{}
	r := newLifecycleReconciler(t, fw)

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
	if len(fw.openCalls) == 0 {
		t.Fatal("expected OpenChange to be called")
	}
	primaryBody := fw.openCalls[0].body
	wantCloses := "Closes #50"
	if !strings.Contains(primaryBody, wantCloses) {
		t.Errorf("primary repo MR body = %q, want to contain %q", primaryBody, wantCloses)
	}
}

// TestLifecycleImplement_ClosesIssueNotInSecondaryRepoMRBody verifies that the
// "Closes #N" line does NOT appear in secondary-repo MR bodies.
func TestLifecycleImplement_ClosesIssueNotInSecondaryRepoMRBody(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-closes-secondary"
	proj := "lc-cs-proj"
	primaryRepo := "lc-cs-repo1"
	secondaryRepo := "lc-cs-repo2"
	sec := "lc-cs-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#51",
		URL: "https://github.com/o/r/issues/51", Number: 51,
		IsPR: false,
	}
	task := seedLifecycleTaskWithSecondaryRepo(t, name, proj, primaryRepo, secondaryRepo, sec, src)
	task.Status.LifecycleState = "Implement"
	task.Status.Phase = "Succeeded"
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{}
	r := newLifecycleReconciler(t, fw)

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
	if len(fw.openCalls) < 2 {
		t.Fatalf("expected 2 OpenChange calls (primary+secondary), got %d", len(fw.openCalls))
	}
	// Primary repo must have Closes #51.
	if !strings.Contains(fw.openCalls[0].body, "Closes #51") {
		t.Errorf("primary repo MR body = %q, must contain 'Closes #51'", fw.openCalls[0].body)
	}
	// Secondary repo must NOT have Closes #51.
	if strings.Contains(fw.openCalls[1].body, "Closes #51") {
		t.Errorf("secondary repo MR body = %q, must NOT contain 'Closes #51'", fw.openCalls[1].body)
	}
}

// TestLifecycleImplement_NoPREntryLifecycleTaskDoesNotClose verifies that a
// lifecycle Task entered from a PR (Source.IsPR=true) does NOT emit Closes #N.
func TestLifecycleImplement_NoPREntryLifecycleTaskDoesNotClose(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-closes-pr-entry"
	proj := "lc-cpe-proj"
	repo := "lc-cpe-repo"
	sec := "lc-cpe-sec"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#52",
		URL: "https://github.com/o/r/pull/52", Number: 52,
		IsPR: true,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	task.Status.LifecycleState = "Implement"
	task.Status.Phase = "Succeeded"
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{}
	r := newLifecycleReconciler(t, fw)

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
	if len(fw.openCalls) == 0 {
		t.Fatal("expected OpenChange to be called")
	}
	if strings.Contains(fw.openCalls[0].body, "Closes #") {
		t.Errorf("PR-entry lifecycle task MR body = %q, must NOT contain 'Closes #'", fw.openCalls[0].body)
	}
}

// TestLifecycleImplement_LegacyImplementTaskDoesNotClose verifies that a
// generic (non-lifecycle) implement Task's MR body does NOT contain Closes #N.
func TestLifecycleImplement_LegacyImplementTaskDoesNotClose(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	// Use the existing writeBackOpenChange test infrastructure.
	mkSecret(t, "lc-legacy-sec", map[string][]byte{"token": []byte("tok"), "webhookSecret": []byte("w")})
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "lc-legacy-proj", Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: "lc-legacy-sec",
			Scm:          &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o"},
			Agent: tatarav1alpha1.AgentSpec{
				Model: "claude-x", Image: "wrapper:1", PermissionMode: "bypassPermissions",
				MaxTurnsPerTask: 50, TurnTimeoutSeconds: 1800,
			},
		},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}
	proj.Status.Memory = &tatarav1alpha1.MemoryStatus{Phase: "Ready", Endpoint: "http://mem.svc:8080"}
	if err := k8sClient.Status().Update(ctx, proj); err != nil {
		t.Fatalf("set memory ready: %v", err)
	}
	r2 := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "lc-legacy-repo", Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef:    "lc-legacy-proj",
			URL:           "https://github.com/o/r.git",
			DefaultBranch: "main", ReingestSchedule: "0 6 * * *",
		},
	}
	if err := k8sClient.Create(ctx, r2); err != nil {
		t.Fatalf("create repo: %v", err)
	}
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "lc-legacy-task", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    "lc-legacy-proj",
			RepositoryRef: "lc-legacy-repo",
			Goal:          "improve the login flow",
			Kind:          "implement", // NOT issueLifecycle
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#53",
				URL: "https://github.com/o/r/issues/53", Number: 53, IsPR: false,
			},
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task: %v", err)
	}
	// Seed WritebackPending=True to trigger doWriteBack -> writeBackOpenChange.
	apimeta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
		Type: "WritebackPending", Status: metav1.ConditionTrue,
		Reason: "AgentDone", ObservedGeneration: task.Generation,
	})
	task.Status.Phase = "Succeeded"
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed legacy task: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{}
	r := newLifecycleReconciler(t, fw)

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: "lc-legacy-task"}}
	_, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	fw.mu.Lock()
	defer fw.mu.Unlock()
	if len(fw.openCalls) == 0 {
		t.Fatal("expected OpenChange to be called for legacy implement task")
	}
	if strings.Contains(fw.openCalls[0].body, "Closes #") {
		t.Errorf("legacy implement MR body = %q, must NOT contain 'Closes #'", fw.openCalls[0].body)
	}
}

// ----- FIX 2: idempotent writeBackOpenChange + atomic implement-finish -----

// TestLifecycleImplement_IdempotentOnRetry verifies that calling the implement-finish
// path twice (Phase Succeeded, PrURL already set from a previous reconcile) opens
// the PR exactly once and still ends in LifecycleState=MRCI.
func TestLifecycleImplement_IdempotentOnRetry(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-impl-idem"
	proj := "lc-ip-idem"
	repo := "lc-ir-idem"
	sec := "lc-is-idem"
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#20", URL: "https://github.com/o/r/issues/20",
		Number: 20,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	// Simulate: first reconcile opened the PR and set PrURL, but then errored
	// before finishing the state transition. So: Phase=Succeeded, PrURL already set,
	// LifecycleState still Implement.
	task.Status.LifecycleState = "Implement"
	task.Status.Phase = "Succeeded"
	task.Status.PrURL = "https://github.com/o/r/pull/77"
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{openPRURL: "https://github.com/o/r/pull/77"}
	r := newLifecycleReconciler(t, fw)

	_, err := r.reconcileLifecycle(ctx, func() *tatarav1alpha1.Task {
		tk := &tatarav1alpha1.Task{}
		if e := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, tk); e != nil {
			t.Fatalf("get task: %v", e)
		}
		return tk
	}())
	if err != nil {
		t.Fatalf("reconcileLifecycle (retry): %v", err)
	}

	fw.mu.Lock()
	openCount := len(fw.openCalls)
	fw.mu.Unlock()
	if openCount != 0 {
		t.Errorf("OpenChange called %d times on retry; want 0 (PR already open)", openCount)
	}

	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, got); err != nil {
		t.Fatalf("get task after: %v", err)
	}
	if got.Status.LifecycleState != "MRCI" {
		t.Errorf("LifecycleState = %q, want MRCI after idempotent retry", got.Status.LifecycleState)
	}
	if got.Status.PrURL != "https://github.com/o/r/pull/77" {
		t.Errorf("PrURL = %q, want unchanged", got.Status.PrURL)
	}
}

// ============================================================
// Task 6 - Iteration backstop
// ============================================================

// seedImplementReadyTask seeds a task in LifecycleState=Implement, Phase=""
// (ready to spawn a fresh run). Extra status fields set via the returned task
// pointer before calling reconcileLifecycle.
func seedImplementReadyTask(t *testing.T, suffix string, iterations int) (*TaskReconciler, *lifecycleFakeSCMWriter, *tatarav1alpha1.Task) {
	t.Helper()
	ctx := context.Background()
	name := "lc-backstop-" + suffix
	proj := "lc-bsp-" + suffix
	repo := "lc-bsr-" + suffix
	sec := "lc-bss-" + suffix
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#7", URL: "https://github.com/o/r/issues/7",
		Number: 7,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	// Set MaxLifecycleIterations on the project to 3 for deterministic tests.
	projObj := &tatarav1alpha1.Project{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: proj}, projObj); err != nil {
		t.Fatalf("get project: %v", err)
	}
	projObj.Spec.Agent.MaxLifecycleIterations = 3
	if err := k8sClient.Update(ctx, projObj); err != nil {
		t.Fatalf("update project MaxLifecycleIterations: %v", err)
	}

	task.Status.LifecycleState = "Implement"
	task.Status.Phase = ""
	task.Status.LifecycleIterations = iterations
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed implement ready: %v", err)
	}
	fw := &lifecycleFakeSCMWriter{}
	r := newLifecycleReconciler(t, fw)
	// Wire the reader so GetPRState works (not needed for backstop but keeps
	// the reconciler consistent).
	return r, fw, task
}

func fetchTask(t *testing.T, name string) *tatarav1alpha1.Task {
	t.Helper()
	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, got); err != nil {
		t.Fatalf("get task %s: %v", name, err)
	}
	return got
}

// TestLifecycleImplement_BackstopParksWhenMaxIterationsReached verifies that
// entering Implement with LifecycleIterations >= MaxLifecycleIterations parks the
// task without spawning a pod, increments giveup metric, and posts a comment.
func TestLifecycleImplement_BackstopParksWhenMaxIterationsReached(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, fw, task := seedImplementReadyTask(t, "max", 3) // 3 >= max(3) -> park

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, task.Name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	got := fetchTask(t, task.Name)
	if got.Status.LifecycleState != "Parked" {
		t.Errorf("LifecycleState = %q, want Parked (backstop)", got.Status.LifecycleState)
	}
	// No pod spawned.
	pods := &corev1.PodList{}
	if err := k8sClient.List(ctx, pods, client.InNamespace(testNS), client.MatchingFields{"metadata.name": agent.PodName(task)}); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	// Pod count check via comment - backstop must post comment.
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if len(fw.commentCalls) == 0 {
		t.Error("backstop must post a comment on the issue/PR")
	}
	found := false
	for _, c := range fw.commentCalls {
		if strings.Contains(c.body, "max lifecycle iterations") || strings.Contains(c.body, "human") {
			found = true
		}
	}
	if !found {
		t.Errorf("backstop comment must mention max iterations; got %+v", fw.commentCalls)
	}
}

// TestLifecycleImplement_BackstopAllowsSpawnBelowMax verifies that with iterations
// below max, Implement still spawns (transitions away from Implement).
func TestLifecycleImplement_BackstopAllowsSpawnBelowMax(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, _, task := seedImplementReadyTask(t, "below", 2) // 2 < max(3) -> spawn

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, task.Name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	got := fetchTask(t, task.Name)
	// Phase should advance (Planning) meaning spawn occurred, not Parked.
	if got.Status.LifecycleState == "Parked" {
		t.Error("LifecycleState must not be Parked below max iterations")
	}
	// LifecycleIterations must be incremented.
	if got.Status.LifecycleIterations != 3 {
		t.Errorf("LifecycleIterations = %d, want 3 (incremented on spawn)", got.Status.LifecycleIterations)
	}
}

// ============================================================
// Task 3 - MRCI poll state
// ============================================================

// lifecycleFakeSCMWriterMRCI extends lifecycleFakeSCMWriter with GetPRState control.
type lifecycleFakeSCMWriterMRCI struct {
	lifecycleFakeSCMWriter
	prState scm.PRState
	prErr   error
}

func (f *lifecycleFakeSCMWriterMRCI) GetPRState(_ context.Context, _, _ string, _ int) (scm.PRState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.prState, f.prErr
}

func (f *lifecycleFakeSCMWriterMRCI) Merge(_ context.Context, _, _ string, _ int, _ string) (string, error) {
	return "", nil
}

func seedMRCITask(t *testing.T, suffix string, prState scm.PRState, deadlineOffset time.Duration) (*TaskReconciler, *lifecycleFakeSCMWriterMRCI, string) {
	t.Helper()
	ctx := context.Background()
	name := "lc-mrci-" + suffix
	proj := "lc-mrcip-" + suffix
	repo := "lc-mrcir-" + suffix
	sec := "lc-mrcis-" + suffix
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#8", URL: "https://github.com/o/r/issues/8",
		Number: 8,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	task.Status.LifecycleState = "MRCI"
	task.Status.PRNumber = 42
	task.Status.PrURL = "https://github.com/o/r/pull/42"
	task.Status.HeadBranch = "tatara/task-" + name
	if deadlineOffset != 0 {
		dl := metav1.NewTime(time.Now().Add(deadlineOffset))
		task.Status.DeadlineAt = &dl
	}
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed mrci task: %v", err)
	}

	fw := &lifecycleFakeSCMWriterMRCI{prState: prState}
	r := newLifecycleReconciler(t, &fw.lifecycleFakeSCMWriter)
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }
	return r, fw, name
}

func TestLifecycleMRCI_PendingRequeues(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, _, name := seedMRCITask(t, "pending", scm.PRState{Author: "bot", CIStatus: "pending"}, time.Hour)

	res, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Error("pending CI must requeue")
	}
	got := fetchTask(t, name)
	if got.Status.LifecycleState != "MRCI" {
		t.Errorf("LifecycleState = %q, want MRCI on pending CI", got.Status.LifecycleState)
	}
}

func TestLifecycleMRCI_SuccessTransitionsToMerge(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, _, name := seedMRCITask(t, "success", scm.PRState{Author: "bot", CIStatus: "success"}, time.Hour)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	got := fetchTask(t, name)
	if got.Status.LifecycleState != "Merge" {
		t.Errorf("LifecycleState = %q, want Merge on CI success", got.Status.LifecycleState)
	}
	if got.Status.DeadlineAt != nil {
		t.Error("DeadlineAt must be cleared on transition out of MRCI")
	}
}

func TestLifecycleMRCI_FailureSetsContextAndReentersImplement(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, _, name := seedMRCITask(t, "failure", scm.PRState{Author: "bot", CIStatus: "failure"}, time.Hour)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	got := fetchTask(t, name)
	if got.Status.LifecycleState != "Implement" {
		t.Errorf("LifecycleState = %q, want Implement on CI failure", got.Status.LifecycleState)
	}
	if got.Status.ImplementContext == "" {
		t.Error("ImplementContext must be set on MRCI failure")
	}
	if !strings.Contains(got.Status.ImplementContext, "pipeline") && !strings.Contains(got.Status.ImplementContext, "MR") && !strings.Contains(got.Status.ImplementContext, "CI") {
		t.Errorf("ImplementContext = %q, should mention pipeline/CI failure", got.Status.ImplementContext)
	}
}

func TestLifecycleMRCI_NoCITransitionsToMerge(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	// CIStatus="" means no CI configured
	r, _, name := seedMRCITask(t, "noci", scm.PRState{Author: "bot", CIStatus: ""}, time.Hour)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	got := fetchTask(t, name)
	if got.Status.LifecycleState != "Merge" {
		t.Errorf("LifecycleState = %q, want Merge when no CI", got.Status.LifecycleState)
	}
}

func TestLifecycleMRCI_DeadlineParks(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	// deadline already passed (negative offset)
	r, fw, name := seedMRCITask(t, "deadline", scm.PRState{Author: "bot", CIStatus: "pending"}, -time.Minute)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	got := fetchTask(t, name)
	if got.Status.LifecycleState != "Parked" {
		t.Errorf("LifecycleState = %q, want Parked on deadline", got.Status.LifecycleState)
	}
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if len(fw.commentCalls) == 0 {
		t.Error("deadline park must post a comment")
	}
}

func TestLifecycleMRCI_NonBotAuthorParks(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, fw, name := seedMRCITask(t, "notbot", scm.PRState{Author: "someuser", CIStatus: "pending"}, time.Hour)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	got := fetchTask(t, name)
	if got.Status.LifecycleState != "Parked" {
		t.Errorf("LifecycleState = %q, want Parked (non-bot author)", got.Status.LifecycleState)
	}
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if len(fw.commentCalls) == 0 {
		t.Error("non-bot author park must post a comment")
	}
}

// ============================================================
// Task 4 - Merge state + 405 regression guard
// ============================================================

// lifecycleFakeSCMWriterMerge extends the base fake with controlled Merge behaviour.
type lifecycleFakeSCMWriterMerge struct {
	lifecycleFakeSCMWriter
	prState  scm.PRState
	mergeSHA string
	mergeErr error
}

func (f *lifecycleFakeSCMWriterMerge) GetPRState(_ context.Context, _, _ string, _ int) (scm.PRState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.prState, nil
}

func (f *lifecycleFakeSCMWriterMerge) Merge(_ context.Context, _, _ string, _ int, _ string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mergeSHA, f.mergeErr
}

func seedMergeTask(t *testing.T, suffix string, fw *lifecycleFakeSCMWriterMerge, deadlineOffset time.Duration) (*TaskReconciler, string) {
	t.Helper()
	ctx := context.Background()
	name := "lc-merge-" + suffix
	proj := "lc-mergep-" + suffix
	repo := "lc-merger-" + suffix
	sec := "lc-merges-" + suffix
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#9", URL: "https://github.com/o/r/issues/9",
		Number: 9,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	task.Status.LifecycleState = "Merge"
	task.Status.PRNumber = 42
	task.Status.PrURL = "https://github.com/o/r/pull/42"
	task.Status.HeadBranch = "tatara/task-" + name
	if deadlineOffset != 0 {
		dl := metav1.NewTime(time.Now().Add(deadlineOffset))
		task.Status.DeadlineAt = &dl
	}
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed merge task: %v", err)
	}

	r := newLifecycleReconciler(t, &fw.lifecycleFakeSCMWriter)
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }
	return r, name
}

func TestLifecycleMerge_AllowedOK_TransitionsToMainCIWithSHA(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	fw := &lifecycleFakeSCMWriterMerge{
		prState:  scm.PRState{Author: "bot", Mergeable: true, CIStatus: "success"},
		mergeSHA: "abc123sha",
	}
	r, name := seedMergeTask(t, "ok", fw, time.Hour)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	got := fetchTask(t, name)
	if got.Status.LifecycleState != "MainCI" {
		t.Errorf("LifecycleState = %q, want MainCI on successful merge", got.Status.LifecycleState)
	}
	if got.Status.MergeCommitSHA != "abc123sha" {
		t.Errorf("MergeCommitSHA = %q, want abc123sha", got.Status.MergeCommitSHA)
	}
	if got.Status.DeadlineAt != nil {
		t.Error("DeadlineAt must be cleared after merge")
	}
}

// TestLifecycleMerge_405ConflictSpawnsResolveAttempt_ErrNil is the explicit
// live-loop guard: a 405 from Merge must NOT return an error to controller-runtime
// (which would trigger exponential backoff), and must transition to Implement with
// ImplementContext set.
func TestLifecycleMerge_405ConflictSpawnsResolveAttempt_ErrNil(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	fw := &lifecycleFakeSCMWriterMerge{
		prState:  scm.PRState{Author: "bot", Mergeable: true, CIStatus: "success"},
		mergeErr: &scm.HTTPError{Status: 405, Body: "merge conflict", Path: "/merge"},
	}
	r, name := seedMergeTask(t, "405", fw, time.Hour)

	result, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	// THE CRITICAL ASSERTION: err must be nil (no controller-runtime backoff loop).
	if err != nil {
		t.Errorf("405 conflict must NOT return error (live-loop guard): got err = %v", err)
	}
	_ = result

	got := fetchTask(t, name)
	if got.Status.LifecycleState != "Implement" {
		t.Errorf("LifecycleState = %q, want Implement (spawn resolve attempt)", got.Status.LifecycleState)
	}
	if got.Status.ImplementContext == "" {
		t.Error("ImplementContext must be set for conflict resolve instruction")
	}
	if !strings.Contains(got.Status.ImplementContext, "conflict") && !strings.Contains(got.Status.ImplementContext, "rebase") {
		t.Errorf("ImplementContext = %q; should mention conflict/rebase", got.Status.ImplementContext)
	}
}

func TestLifecycleMerge_NotAllowedRequeues(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	// autoMergeOnGreenCI with pending CI -> mergeAllowed=false
	fw := &lifecycleFakeSCMWriterMerge{
		prState: scm.PRState{Author: "bot", CIStatus: "pending"},
	}
	r, name := seedMergeTask(t, "notallowed", fw, time.Hour)
	// Set autoMergeOnGreenCI policy on project.
	proj := &tatarav1alpha1.Project{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "lc-mergep-notallowed"}, proj); err != nil {
		t.Fatalf("get project: %v", err)
	}
	proj.Spec.Scm.MergePolicy = "autoMergeOnGreenCI"
	if err := k8sClient.Update(context.Background(), proj); err != nil {
		t.Fatalf("update project policy: %v", err)
	}

	res, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Error("not-allowed merge must requeue")
	}
	got := fetchTask(t, name)
	if got.Status.LifecycleState != "Merge" {
		t.Errorf("LifecycleState = %q, want Merge (not allowed, requeue)", got.Status.LifecycleState)
	}
}

func TestLifecycleMerge_TransientErrorRequeues(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	fw := &lifecycleFakeSCMWriterMerge{
		prState:  scm.PRState{Author: "bot", CIStatus: "success"},
		mergeErr: &scm.HTTPError{Status: 503, Body: "service unavailable", Path: "/merge"},
	}
	r, name := seedMergeTask(t, "transient", fw, time.Hour)

	res, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Error("transient merge error must requeue")
	}
	got := fetchTask(t, name)
	if got.Status.LifecycleState != "Merge" {
		t.Errorf("LifecycleState = %q, want Merge (transient)", got.Status.LifecycleState)
	}
}

func TestLifecycleMerge_TransientDeadlineParks(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	fw := &lifecycleFakeSCMWriterMerge{
		prState:  scm.PRState{Author: "bot", CIStatus: "success"},
		mergeErr: &scm.HTTPError{Status: 503, Body: "unavailable", Path: "/merge"},
	}
	r, name := seedMergeTask(t, "trans-dl", fw, -time.Minute) // deadline already passed

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	got := fetchTask(t, name)
	if got.Status.LifecycleState != "Parked" {
		t.Errorf("LifecycleState = %q, want Parked (transient error + deadline)", got.Status.LifecycleState)
	}
}

// ============================================================
// Task 5 - MainCI poll + close
// ============================================================

// lifecycleFakeSCMWriterMainCI controls commit CI and close calls.
type lifecycleFakeSCMWriterMainCI struct {
	lifecycleFakeSCMWriter
	ciStatus     string
	ciErr        error
	closeIssueFn func() error // optional override; nil = success
}

func (f *lifecycleFakeSCMWriterMainCI) GetPRState(_ context.Context, _, _ string, _ int) (scm.PRState, error) {
	return scm.PRState{Author: "bot", CIStatus: "success"}, nil
}

func (f *lifecycleFakeSCMWriterMainCI) Merge(_ context.Context, _, _ string, _ int, _ string) (string, error) {
	return "", nil
}

func (f *lifecycleFakeSCMWriterMainCI) GetCommitCIStatus(_ context.Context, _, _, _ string) (string, error) {
	return f.ciStatus, f.ciErr
}

func (f *lifecycleFakeSCMWriterMainCI) CloseIssue(_ context.Context, _, _ string, _ int, comment string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeCalls = append(f.closeCalls, struct{ repo, comment string }{"", comment})
	if f.closeIssueFn != nil {
		return f.closeIssueFn()
	}
	return nil
}

// SCMReaderMainCI satisfies SCMReader for GetCommitCIStatus in the reconciler.
// (The reconciler calls GetCommitCIStatus via ReaderFor.)
type fakeReaderMainCI struct {
	ciStatus string
	ciErr    error
}

func (f *fakeReaderMainCI) ListOpenPRs(_ context.Context, _, _ string) ([]scm.PRRef, error) {
	return nil, nil
}
func (f *fakeReaderMainCI) ListOpenIssues(_ context.Context, _, _ string) ([]scm.IssueRef, error) {
	return nil, nil
}
func (f *fakeReaderMainCI) ListBoardItems(_ context.Context, _ scm.BoardRef) ([]scm.BoardItem, error) {
	return nil, nil
}
func (f *fakeReaderMainCI) GetCommitCIStatus(_ context.Context, _, _, _ string) (string, error) {
	return f.ciStatus, f.ciErr
}

func seedMainCITask(t *testing.T, suffix string, fw *lifecycleFakeSCMWriterMainCI, deadlineOffset time.Duration) (*TaskReconciler, string) {
	t.Helper()
	ctx := context.Background()
	name := "lc-mainci-" + suffix
	proj := "lc-mcp-" + suffix
	repo := "lc-mcr-" + suffix
	sec := "lc-mcs-" + suffix
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#11", URL: "https://github.com/o/r/issues/11",
		Number: 11,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	task.Status.LifecycleState = "MainCI"
	task.Status.MergeCommitSHA = "deadbeef"
	task.Status.PRNumber = 55
	task.Status.PrURL = "https://github.com/o/r/pull/55"
	if deadlineOffset != 0 {
		dl := metav1.NewTime(time.Now().Add(deadlineOffset))
		task.Status.DeadlineAt = &dl
	}
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed mainci task: %v", err)
	}

	r := newLifecycleReconciler(t, &fw.lifecycleFakeSCMWriter)
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }
	r.ReaderFor = func(_, _ string) (scm.SCMReader, error) {
		return &fakeReaderMainCI{ciStatus: fw.ciStatus, ciErr: fw.ciErr}, nil
	}
	return r, name
}

func TestLifecycleMainCI_PendingRequeues(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	fw := &lifecycleFakeSCMWriterMainCI{ciStatus: "pending"}
	r, name := seedMainCITask(t, "pending", fw, time.Hour)

	res, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Error("pending MainCI must requeue")
	}
	got := fetchTask(t, name)
	if got.Status.LifecycleState != "MainCI" {
		t.Errorf("LifecycleState = %q, want MainCI on pending", got.Status.LifecycleState)
	}
}

func TestLifecycleMainCI_SuccessClosesDoneIdempotent(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	// closeIssueFn returns nil (idempotent - issue may already be closed)
	fw := &lifecycleFakeSCMWriterMainCI{ciStatus: "success"}
	r, name := seedMainCITask(t, "success", fw, time.Hour)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	got := fetchTask(t, name)
	if got.Status.LifecycleState != "Done" {
		t.Errorf("LifecycleState = %q, want Done on MainCI success", got.Status.LifecycleState)
	}
}

func TestLifecycleMainCI_SuccessCloseIssueIdempotentOnNotFound(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	// Simulate already-closed: CloseIssue returns a 404 HTTPError (Closes #N
	// in the MR body may have already closed it).
	fw := &lifecycleFakeSCMWriterMainCI{
		ciStatus: "success",
		closeIssueFn: func() error {
			return &scm.HTTPError{Status: 404, Body: "not found", Path: "/issues/close"}
		},
	}
	r, name := seedMainCITask(t, "idem", fw, time.Hour)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle on idempotent close: %v", err)
	}
	got := fetchTask(t, name)
	if got.Status.LifecycleState != "Done" {
		t.Errorf("LifecycleState = %q, want Done (idempotent CloseIssue)", got.Status.LifecycleState)
	}
}

func TestLifecycleMainCI_FailureReentersImplement(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	fw := &lifecycleFakeSCMWriterMainCI{ciStatus: "failure"}
	r, name := seedMainCITask(t, "failure", fw, time.Hour)

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	got := fetchTask(t, name)
	if got.Status.LifecycleState != "Implement" {
		t.Errorf("LifecycleState = %q, want Implement on MainCI failure", got.Status.LifecycleState)
	}
	if got.Status.ImplementContext == "" {
		t.Error("ImplementContext must be set on MainCI failure")
	}
}

func TestLifecycleMainCI_DeadlineParks(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	fw := &lifecycleFakeSCMWriterMainCI{ciStatus: "pending"}
	r, name := seedMainCITask(t, "deadline", fw, -time.Minute) // already past

	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	got := fetchTask(t, name)
	if got.Status.LifecycleState != "Parked" {
		t.Errorf("LifecycleState = %q, want Parked on MainCI deadline", got.Status.LifecycleState)
	}
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if len(fw.commentCalls) == 0 {
		t.Error("deadline park must post comment")
	}
}
