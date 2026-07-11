package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// TestDoWriteBack_UmbrellaImplement_MultiRepo verifies the 7-kind redesign
// multi-repo writeback: an implement Task with an EMPTY RepositoryRef and a
// WorkItems ledger spanning two repos opens one PR per repo (not error-looping on
// a missing primary repo) and comments the umbrella's originating issue with the
// links.
func TestDoWriteBack_UmbrellaImplement_MultiRepo(t *testing.T) {
	ctx := context.Background()
	const project = "umb-mr-proj"
	const scmSecret = "umb-mr-scm"

	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: scmSecret, Namespace: testNS},
		Data:       map[string][]byte{"token": []byte("pat"), "webhookSecret": []byte("w")},
	}
	if err := k8sClient.Create(ctx, sec); err != nil {
		t.Fatalf("create secret: %v", err)
	}
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: project, Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: scmSecret,
			TriggerLabel: "tatara",
			Scm:          &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "tatara-bot"},
			Agent: tatarav1alpha1.AgentSpec{
				Model: "claude-x", Image: "wrapper:1", PermissionMode: "bypassPermissions",
				MaxTurnsPerTask: 50, TurnTimeoutSeconds: 1800,
			},
		},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}
	for _, r := range []struct{ name, url string }{
		{"umb-mr-r1", "https://github.com/o/umbmr1"},
		{"umb-mr-r2", "https://github.com/o/umbmr2"},
	} {
		repo := &tatarav1alpha1.Repository{
			ObjectMeta: metav1.ObjectMeta{Name: r.name, Namespace: testNS},
			Spec:       tatarav1alpha1.RepositorySpec{ProjectRef: project, URL: r.url, DefaultBranch: "main", ReingestSchedule: "0 6 * * *"},
		}
		if err := k8sClient.Create(ctx, repo); err != nil {
			t.Fatalf("create repo %s: %v", r.name, err)
		}
	}

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "umb-mr-task", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    project,
			RepositoryRef: "", // umbrella: no primary repo
			Kind:          "implement",
			Goal:          "cross-repo change",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github",
				IssueRef: "o/umbmr1#5",
				URL:      "https://github.com/o/umbmr1/issues/5",
				Number:   5,
			},
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task: %v", err)
	}
	task.Status.Phase = "Succeeded"
	task.Status.ResultSummary = "did cross-repo work"
	task.Status.WorkItems = []tatarav1alpha1.WorkItemRef{
		{Provider: "github", Repo: "o/umbmr1", Number: 5, Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
		{Provider: "github", Repo: "o/umbmr2", Number: 0, Kind: tatarav1alpha1.WorkItemIssue, Role: tatarav1alpha1.RoleSource, State: tatarav1alpha1.WIOpen},
	}
	apimeta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
		Type: "WritebackPending", Status: metav1.ConditionTrue, Reason: "AwaitingM5",
	})
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	var fresh tatarav1alpha1.Task
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "umb-mr-task"}, &fresh); err != nil {
		t.Fatalf("reload: %v", err)
	}

	fw := &fullFakeSCMWriter{}
	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Session: newFakeSession(),
		SCMFor:  func(string) (Writer, error) { return fw, nil },
	}

	if _, err := r.doWriteBack(ctx, &fresh); err != nil {
		t.Fatalf("doWriteBack (umbrella empty ref) must not error: %v", err)
	}
	if fw.openCalls != 2 {
		t.Fatalf("openCalls = %d, want 2 (one PR per umbrella repo)", fw.openCalls)
	}
	// The umbrella's originating issue is commented with the PR links.
	if !fw.commentCalled || fw.commentIssueRef != "o/umbmr1#5" {
		t.Fatalf("expected comment on umbrella issue o/umbmr1#5, got called=%v ref=%q", fw.commentCalled, fw.commentIssueRef)
	}
	var after tatarav1alpha1.Task
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "umb-mr-task"}, &after); err != nil {
		t.Fatalf("reload after: %v", err)
	}
	if after.Status.PrURL == "" {
		t.Fatalf("PrURL not recorded on umbrella task")
	}
	if !strings.HasPrefix(after.Status.PrURL, "https://") {
		t.Fatalf("PrURL = %q, want an https url", after.Status.PrURL)
	}
}
