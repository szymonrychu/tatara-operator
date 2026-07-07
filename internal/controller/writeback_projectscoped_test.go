package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// seedProjectScopedTask seeds the minimal objects needed for a project-scoped
// Task (incident/healthCheck) whose RepositoryRef is intentionally empty.
// Unlike seedWritebackKindTask it does NOT create a Repository object (there
// is none by contract for these kinds).
func seedProjectScopedTask(t *testing.T, name, project, scmSecret string, spec tatarav1alpha1.TaskSpec) *tatarav1alpha1.Task {
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
		},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project %s: %v", project, err)
	}
	proj.Status.Memory = stableMemStatus("http://mem.svc")
	if err := k8sClient.Status().Update(ctx, proj); err != nil {
		t.Fatalf("set project memory ready: %v", err)
	}

	// RepositoryRef is intentionally empty for project-scoped kinds.
	spec.ProjectRef = project
	spec.RepositoryRef = ""
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec:       spec,
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task %s: %v", name, err)
	}

	task.Status.Phase = "Succeeded"
	task.Status.ResultSummary = "done"
	apimeta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
		Type: "WritebackPending", Status: metav1.ConditionTrue, Reason: "AwaitingM5",
	})
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed writeback status: %v", err)
	}
	var fresh tatarav1alpha1.Task
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, &fresh); err != nil {
		t.Fatalf("reload task: %v", err)
	}
	return &fresh
}

func newProjectScopedReconciler(t *testing.T, fw *fullFakeSCMWriter) *TaskReconciler {
	t.Helper()
	return &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Session: newFakeSession(),
		SCMFor: func(provider string) (Writer, error) {
			return fw, nil
		},
	}
}

// kindSlug returns a lowercase, RFC-1123-safe slug for a Task kind string,
// used for constructing unique test resource names.
func kindSlug(kind string) string {
	return strings.ToLower(kind)
}

// TestDoWriteBack_ProjectScoped_NoOp verifies that incident and healthCheck Tasks
// with an empty RepositoryRef do NOT call OpenChange (which would error-loop on
// Repository "" not found) and instead clear WritebackPending cleanly.
//
// Regression: before the fix, both kinds fell through the doWriteBack default:
// case into writeBackOpenChange which resolved a Repository by empty name,
// returned "Repository.tatara.dev \"\" not found", and never cleared
// WritebackPending -> controller-runtime requeued at ~1/s forever.
func TestDoWriteBack_ProjectScoped_NoOp(t *testing.T) {
	for _, kind := range []string{"incident", "healthCheck"} {
		kind := kind
		slug := kindSlug(kind)
		t.Run(kind, func(t *testing.T) {
			fw := &fullFakeSCMWriter{}
			r := newProjectScopedReconciler(t, fw)
			task := seedProjectScopedTask(t,
				"ps-noop-"+slug,
				"ps-noop-proj-"+slug,
				"ps-noop-scm-"+slug,
				tatarav1alpha1.TaskSpec{
					Goal: kind + " investigation",
					Kind: kind,
				})

			// Must NOT error (regression assertion: not "Repository \"\" not found").
			_, err := reconcileWriteback(t, r, task.Name)
			require.NoError(t, err, "%s: doWriteBack must not error on empty RepositoryRef", kind)

			// Must NOT call OpenChange (no SCM repo resolved).
			require.Zero(t, fw.openCalls, "%s: must not call OpenChange", kind)

			// WritebackPending must be cleared.
			var got tatarav1alpha1.Task
			require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
			cond := findCond(got.Status.Conditions, "WritebackPending")
			require.NotNil(t, cond, "%s: WritebackPending condition must exist", kind)
			require.Equal(t, metav1.ConditionFalse, cond.Status, "%s: WritebackPending must be False", kind)
			require.Equal(t, "NoWriteback", cond.Reason, "%s: no-proposal run must use NoWriteback", kind)
		})
	}
}

// TestDoWriteBack_ProjectScoped_WithProposal verifies that incident and
// healthCheck Tasks that produced a child proposal Task (via propose_issue)
// clear WritebackPending with reason ProposalFiled.
func TestDoWriteBack_ProjectScoped_WithProposal(t *testing.T) {
	for _, kind := range []string{"incident", "healthCheck"} {
		kind := kind
		slug := kindSlug(kind)
		t.Run(kind, func(t *testing.T) {
			fw := &fullFakeSCMWriter{}
			r := newProjectScopedReconciler(t, fw)
			proj := "ps-prop-proj-" + slug
			childRepo := "ps-prop-repo-" + slug
			task := seedProjectScopedTask(t,
				"ps-prop-task-"+slug,
				proj,
				"ps-prop-scm-"+slug,
				tatarav1alpha1.TaskSpec{
					Goal: kind + " investigation",
					Kind: kind,
				})

			// Seed a child proposal Task created by propose_issue.
			proposalTask := &tatarav1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "ps-prop-child-" + slug,
					Namespace: testNS,
				},
				Spec: tatarav1alpha1.TaskSpec{
					ProjectRef:    proj,
					RepositoryRef: childRepo,
					Goal:          "implement: fix found in " + kind,
					Kind:          "implement",
					ProposedIssue: &tatarav1alpha1.ProposedIssueSpec{
						RepositoryRef: childRepo,
						Title:         "Issue found by " + kind,
						Body:          "Proposal body",
						Kind:          "improvement",
					},
				},
			}
			require.NoError(t, k8sClient.Create(context.Background(), proposalTask))

			_, err := reconcileWriteback(t, r, task.Name)
			require.NoError(t, err, "%s: doWriteBack with proposal must not error", kind)

			require.Zero(t, fw.openCalls, "%s: must not call OpenChange even with proposal child", kind)

			var got tatarav1alpha1.Task
			require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
			cond := findCond(got.Status.Conditions, "WritebackPending")
			require.NotNil(t, cond, "%s: WritebackPending condition must exist", kind)
			require.Equal(t, metav1.ConditionFalse, cond.Status, "%s: WritebackPending must be False", kind)
			require.Equal(t, "ProposalFiled", cond.Reason, "%s: run with proposal child must use ProposalFiled", kind)
		})
	}
}
