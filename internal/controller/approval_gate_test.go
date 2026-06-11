package controller

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// fakeSCMWriter records calls to CreateIssue, AddBoardItem, SetBoardColumn.
// It embeds scm.SCMWriter so it satisfies the full interface; only the three
// methods exercised by the proposal path are overridden.
type fakeSCMWriter struct {
	scm.SCMWriter
	createdLabels  []string
	boardColumn    string
	addedBoardItem bool
}

func (f *fakeSCMWriter) CreateIssue(_ context.Context, _, _ string, req scm.IssueReq) (scm.CreatedIssue, error) {
	f.createdLabels = req.Labels
	return scm.CreatedIssue{Ref: "o/r#1", URL: "https://gh/o/r/issues/1"}, nil
}

func (f *fakeSCMWriter) AddBoardItem(_ context.Context, _ string, _ scm.BoardRef, _ string) error {
	f.addedBoardItem = true
	return nil
}

func (f *fakeSCMWriter) SetBoardColumn(_ context.Context, _ string, _ scm.BoardRef, _, column string) error {
	f.boardColumn = column
	return nil
}

// newApprovalReconciler builds a TaskReconciler wired to the given fake SCM writer.
func newApprovalReconciler(t *testing.T, fw *fakeSCMWriter) *TaskReconciler {
	t.Helper()
	return &TaskReconciler{
		Client:  k8sClient,
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
}

// seedApprovalTask creates the project, scm secret, repo, and a task with
// ApprovalRequired=true and a ProposedIssue but no Source yet.
func seedApprovalTask(t *testing.T, name, project, repo, scmSecret string) *tatarav1alpha1.Task {
	t.Helper()
	ctx := context.Background()

	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: scmSecret, Namespace: testNS},
		Data:       map[string][]byte{"token": []byte("pat"), "webhookSecret": []byte("w")},
	}
	if err := k8sClient.Create(ctx, sec); err != nil {
		t.Fatalf("create secret %s: %v", scmSecret, err)
	}

	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: project, Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: scmSecret,
			TriggerLabel: "tatara",
			Agent: tatarav1alpha1.AgentSpec{
				Model: "claude-x", Image: "wrapper:1", PermissionMode: "bypassPermissions",
				MaxTurnsPerTask: 50, TurnTimeoutSeconds: 1800,
			},
			Scm: &tatarav1alpha1.ScmSpec{
				Provider:      "github",
				Owner:         "acme",
				BotLogin:      "tatara-bot",
				ApprovalLabel: "tatara/awaiting-approval",
				Board: &tatarav1alpha1.BoardSpec{
					GitHubProjectNumber: 3,
					StatusField:         "Status",
				},
			},
		},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project %s: %v", project, err)
	}

	// Mark project memory ready so the reconciler does not gate on that.
	proj.Status.Memory = &tatarav1alpha1.MemoryStatus{Phase: "Ready", Endpoint: "http://mem.svc"}
	if err := k8sClient.Status().Update(ctx, proj); err != nil {
		t.Fatalf("set project memory ready: %v", err)
	}

	r := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: repo, Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef:       project,
			URL:              "https://github.com/acme/myrepo.git",
			DefaultBranch:    "main",
			ReingestSchedule: "0 6 * * *",
		},
	}
	if err := k8sClient.Create(ctx, r); err != nil {
		t.Fatalf("create repo %s: %v", repo, err)
	}

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:       project,
			RepositoryRef:    repo,
			Goal:             "fix the bug",
			Kind:             "implement",
			ApprovalRequired: true,
			ProposedIssue: &tatarav1alpha1.ProposedIssueSpec{
				RepositoryRef: repo,
				Title:         "Fix the bug",
				Body:          "We found a bug.",
				Kind:          "bug",
			},
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task %s: %v", name, err)
	}
	var fresh tatarav1alpha1.Task
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, &fresh); err != nil {
		t.Fatalf("reload task: %v", err)
	}
	return &fresh
}

func doReconcileApproval(t *testing.T, r *TaskReconciler, name string) ctrl.Result {
	t.Helper()
	ctx := logf.IntoContext(context.Background(), logf.Log)
	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: name}})
	require.NoError(t, err)
	return res
}

func TestApprovalGate(t *testing.T) {
	t.Run("proposal creates Source + holds AwaitingApproval", func(t *testing.T) {
		fw := &fakeSCMWriter{}
		r := newApprovalReconciler(t, fw)
		task := seedApprovalTask(t, "ap-task1", "ap-proj1", "ap-repo1", "ap-scm1")

		doReconcileApproval(t, r, task.Name)

		var got tatarav1alpha1.Task
		require.NoError(t, k8sClient.Get(context.Background(),
			types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))

		// CreateIssue was called with the approval label.
		require.Equal(t, []string{"tatara/awaiting-approval"}, fw.createdLabels)
		// Board item was added and column was set.
		require.True(t, fw.addedBoardItem)
		require.Equal(t, "Proposed", fw.boardColumn)
		// Task Source was populated.
		require.NotNil(t, got.Spec.Source)
		require.Equal(t, "tatara-bot", got.Spec.Source.AuthorLogin)
		require.False(t, got.Spec.Source.IsPR)
		// DiscoveredIssues has the URL.
		require.Contains(t, got.Status.DiscoveredIssues, "https://gh/o/r/issues/1")
		// Phase is AwaitingApproval.
		require.Equal(t, "AwaitingApproval", got.Status.Phase)
		// No pod was created for this task (proposal holds execution).
		podName := agent.PodName(&got)
		var pod corev1.Pod
		err := k8sClient.Get(context.Background(),
			types.NamespacedName{Namespace: testNS, Name: podName}, &pod)
		require.True(t, err != nil, "pod should not exist for approval-gated task, but got no error")
	})

	t.Run("gate releases on ApprovalApproved=True", func(t *testing.T) {
		fw := &fakeSCMWriter{}
		r := newApprovalReconciler(t, fw)
		task := seedApprovalTask(t, "ap-task2", "ap-proj2", "ap-repo2", "ap-scm2")

		// First reconcile: triggers proposal creation, stays AwaitingApproval.
		doReconcileApproval(t, r, task.Name)

		var got tatarav1alpha1.Task
		require.NoError(t, k8sClient.Get(context.Background(),
			types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
		require.Equal(t, "AwaitingApproval", got.Status.Phase)

		// Simulate human approval: set ApprovalApproved=True.
		apimeta.SetStatusCondition(&got.Status.Conditions, metav1.Condition{
			Type:   tatarav1alpha1.ConditionApprovalApproved,
			Status: metav1.ConditionTrue,
			Reason: "HumanApproved",
		})
		require.NoError(t, k8sClient.Status().Update(context.Background(), &got))

		// Second reconcile: gate released; reconciler should proceed past AwaitingApproval.
		doReconcileApproval(t, r, task.Name)

		require.NoError(t, k8sClient.Get(context.Background(),
			types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
		// Phase must have advanced past AwaitingApproval.
		require.NotEqual(t, "AwaitingApproval", got.Status.Phase)
	})
}
