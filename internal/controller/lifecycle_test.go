package controller

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// ----- lifecycle SCM fake -----

type lifecycleFakeSCMWriter struct {
	scm.SCMWriter
	mu            sync.Mutex
	failOnComment string
	closeCalls    []struct{ repo, comment string }
	commentCalls  []struct{ issueRef, body string }
	openCalls     []struct {
		repoURL, sourceBranch, title, body string
	}
	openPRURL      string
	createIssues   []struct{ url, title, body string }
	createIssueURL string
	issueClosed    bool
}

func (f *lifecycleFakeSCMWriter) GetIssueState(_ context.Context, _, _ string, _ int) (scm.IssueState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return scm.IssueState{Closed: f.issueClosed}, nil
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
	if f.failOnComment != "" && body == f.failOnComment {
		return fmt.Errorf("simulated comment failure")
	}
	f.commentCalls = append(f.commentCalls, struct{ issueRef, body string }{issueRef, body})
	return nil
}

func (f *lifecycleFakeSCMWriter) OpenChange(_ context.Context, repoURL, _, sourceBranch, _, title, body string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.openCalls = append(f.openCalls, struct {
		repoURL, sourceBranch, title, body string
	}{repoURL, sourceBranch, title, body})
	url := f.openPRURL
	if url == "" {
		url = "https://github.com/o/r/pull/42"
	}
	return url, nil
}

func (f *lifecycleFakeSCMWriter) CreateIssue(_ context.Context, _, _ string, req scm.IssueReq) (scm.CreatedIssue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	url := f.createIssueURL
	if url == "" {
		url = "https://github.com/o/r/issues/99"
	}
	f.createIssues = append(f.createIssues, struct{ url, title, body string }{url, req.Title, req.Body})
	return scm.CreatedIssue{Ref: "o/r#99", URL: url}, nil
}

func (f *lifecycleFakeSCMWriter) AddLabel(_ context.Context, _, _, _ string) error    { return nil }
func (f *lifecycleFakeSCMWriter) RemoveLabel(_ context.Context, _, _, _ string) error { return nil }
func (f *lifecycleFakeSCMWriter) EnsureLabel(_ context.Context, _, _, _, _ string) error {
	return nil
}
func (f *lifecycleFakeSCMWriter) EnableAutoMerge(_ context.Context, _, _, _, _ string) error {
	return nil
}

// commentBodies returns the comment bodies posted to the given issueRef, in order.
func (f *lifecycleFakeSCMWriter) commentBodies(issueRef string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, c := range f.commentCalls {
		if c.issueRef == issueRef {
			out = append(out, c.body)
		}
	}
	return out
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
	proj.Status.Memory = stableMemStatus("http://mem.svc:8080")
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

// ----- Task 3: setDeployState + metrics -----

func TestSetDeployState_TransitionsStateAndIncrementMetric(t *testing.T) {
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

	if err := r.setDeployState(ctx, task, "Triage", "initial"); err != nil {
		t.Fatalf("setDeployState: %v", err)
	}

	// Verify state persisted.
	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "lc-state-task"}, got); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.Status.DeployState != "Triage" {
		t.Errorf("DeployState = %q, want Triage", got.Status.DeployState)
	}

	// Verify counter incremented.
	counter := testutil.ToFloat64(m.TransitionTotal("", "Triage"))
	if counter != 1 {
		t.Errorf("tatara_lifecycle_transition_total{from='',to=Triage} = %v, want 1", counter)
	}
}

// TestSetDeployState_TerminalDeletesWrapper verifies that transitioning into
// a terminal lifecycle state (Parked/Done/Stopped) tears down the wrapper
// Pod+Service so idle agent sessions do not accumulate.
func TestSetDeployState_TerminalDeletesWrapper(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-term-cleanup"
	src := &tatarav1alpha1.TaskSource{Provider: "github", IssueRef: "o/r#7", Number: 7}
	task := seedLifecycleTask(t, name, "lc-tc-proj", "lc-tc-repo", "lc-tc-sec", src)

	// Stand up the wrapper Pod + Service the running session would have created.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: agent.PodName(task), Namespace: testNS},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "wrapper", Image: "wrapper:1"}}},
	}
	if err := k8sClient.Create(ctx, pod); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: agent.PodName(task), Namespace: testNS},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 8080}}},
	}
	if err := k8sClient.Create(ctx, svc); err != nil {
		t.Fatalf("create service: %v", err)
	}

	r := newLifecycleReconciler(t, nil)
	if err := r.setDeployState(ctx, task, "Parked", "test-terminal"); err != nil {
		t.Fatalf("setDeployState: %v", err)
	}

	// envtest has no kubelet, so a deleted Pod may linger with a DeletionTimestamp
	// rather than vanishing; treat either as deleted (same as the terminate test).
	gotPod := &corev1.Pod{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: agent.PodName(task)}, gotPod); err == nil && gotPod.DeletionTimestamp == nil {
		t.Error("wrapper pod should be deleted on terminal transition")
	}
	gotSvc := &corev1.Service{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: agent.PodName(task)}, gotSvc); !apierrors.IsNotFound(err) {
		t.Errorf("wrapper service should be deleted on terminal transition, got err=%v", err)
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
	proj.Status.Memory = stableMemStatus("http://mem.svc:8080")
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
	if got.Status.DeployState != "Triage" {
		t.Errorf("DeployState = %q, want Triage", got.Status.DeployState)
	}
}

// TestReconcileLifecycle_UnknownStateReturnsError verifies that reconcileLifecycle
// returns a descriptive error for an unrecognised DeployState. The CRD enum
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
	proj.Status.Memory = stableMemStatus("http://mem.svc:8080")
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
			DeployState: "NotAValidState",
		},
	}

	_, err := r.reconcileLifecycle(ctx, bogusTask)
	if err == nil {
		t.Error("expected error for unknown lifecycle state, got nil")
	}
}

// ----- FIX 3+5: reconcileLifecycle initializes DeployState from lifecycle-entry annotation -----

// TestReconcileLifecycle_AnnotationEntryImplement asserts that when DeployState
// is empty but the tatara.dev/lifecycle-entry annotation is "Implement", the first
// reconcile sets DeployState=Implement (not the default Triage).
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
	proj.Status.Memory = stableMemStatus("http://mem.svc:8080")
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

	// Task with lifecycle-entry=Implement annotation, empty DeployState.
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
	if got.Status.DeployState != "Implement" {
		t.Errorf("DeployState = %q, want Implement (from annotation); default would be Triage", got.Status.DeployState)
	}
}

// TestReconcileLifecycle_NoAnnotationDefaultsTriage asserts that without the
// lifecycle-entry annotation, DeployState is still initialized to Triage.
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
	proj.Status.Memory = stableMemStatus("http://mem.svc:8080")
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
	if got.Status.DeployState != "Triage" {
		t.Errorf("DeployState = %q, want Triage (default when no annotation)", got.Status.DeployState)
	}
}

// ----- C3: Drain PendingComments -----

func TestReconcileLifecycle_DrainsPendingComments(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "owner/repo#7",
		URL: "https://github.com/owner/repo/issues/7", Number: 7,
	}
	task := seedLifecycleTask(t,
		"lc-drain-task", "lc-drain-proj", "lc-drain-repo", "lc-drain-scm", src)

	// Set PendingComments on the task status.
	task.Status.PendingComments = []string{"first", "second"}
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("set PendingComments: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{}
	r := newLifecycleReconciler(t, fw)

	if _, err := r.reconcileLifecycle(ctx, task); err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}

	// Comments must have been posted to the issue ref in order.
	got := fw.commentBodies("owner/repo#7")
	if len(got) != 2 || got[0] != "first" || got[1] != "second" {
		t.Fatalf("posted comments = %#v, want [first second]", got)
	}

	// PendingComments must be cleared in the persisted task.
	var reloaded tatarav1alpha1.Task
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: testNS, Name: "lc-drain-task"}, &reloaded); err != nil {
		t.Fatalf("reload task: %v", err)
	}
	if len(reloaded.Status.PendingComments) != 0 {
		t.Fatalf("PendingComments not cleared: %#v", reloaded.Status.PendingComments)
	}
}

func TestReconcileLifecycle_DrainPartialFailureKeepsRemaining(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "owner/repo#8",
		URL: "https://github.com/owner/repo/issues/8", Number: 8,
	}
	task := seedLifecycleTask(t,
		"lc-drain-fail-task", "lc-drain-fail-proj", "lc-drain-fail-repo", "lc-drain-fail-scm", src)

	task.Status.PendingComments = []string{"ok1", "BOOM", "ok2"}
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("set PendingComments: %v", err)
	}

	fw := &lifecycleFakeSCMWriter{failOnComment: "BOOM"}
	r := newLifecycleReconciler(t, fw)

	// The drain must return an error (so the reconcile requeues) once a comment
	// fails to post.
	if _, err := r.reconcileLifecycle(ctx, task); err == nil {
		t.Fatal("reconcileLifecycle: expected error on comment post failure")
	}

	// Only the comment before the failure is delivered; BOOM is never recorded.
	got := fw.commentBodies("owner/repo#8")
	if len(got) != 1 || got[0] != "ok1" {
		t.Fatalf("posted comments = %#v, want [ok1]", got)
	}

	// The delivered comment is dequeued; the failed one and everything after it
	// remain so a retry does not re-post ok1.
	var reloaded tatarav1alpha1.Task
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: testNS, Name: "lc-drain-fail-task"}, &reloaded); err != nil {
		t.Fatalf("reload task: %v", err)
	}
	want := []string{"BOOM", "ok2"}
	if len(reloaded.Status.PendingComments) != len(want) ||
		reloaded.Status.PendingComments[0] != want[0] ||
		reloaded.Status.PendingComments[1] != want[1] {
		t.Fatalf("PendingComments = %#v, want %#v", reloaded.Status.PendingComments, want)
	}
}
