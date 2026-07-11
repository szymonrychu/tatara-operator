package controller

// Tests for the item-1 root-cause adjacent fix (PR #295): a review turn must
// never be spawned, and a review verdict must never be posted, against a PR/MR
// that is already merged or closed - the deploy supervisor may merge the PR
// between scan-time task creation and this reconcile.

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// seedReviewTask seeds a repo-scoped review Task with a PR source, ready to
// spawn (Phase=="").
func seedReviewTask(t *testing.T, suffix string) *tatarav1alpha1.Task {
	t.Helper()
	proj := "rtc-proj-" + suffix
	repo := "rtc-repo-" + suffix
	scmSecret := "rtc-scm-" + suffix
	mkSecret(t, scmSecret, map[string][]byte{"token": []byte("tok")})
	mkTaskProject(t, proj, 3)
	// mkTaskProject does not set ScmSecretRef; patch it in.
	p := &tatarav1alpha1.Project{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: proj}, p); err != nil {
		t.Fatalf("get project: %v", err)
	}
	p.Spec.ScmSecretRef = scmSecret
	p.Spec.Scm = &tatarav1alpha1.ScmSpec{Provider: "github", BotLogin: "tatara-bot"}
	if err := k8sClient.Update(context.Background(), p); err != nil {
		t.Fatalf("update project scm: %v", err)
	}
	setProjectMemoryReady(t, proj, "http://mem."+proj+".svc:8080")
	mkTaskRepository(t, repo, proj)

	name := "rtc-task-" + suffix
	tk := &tatarav1alpha1.Task{}
	tk.Name = name
	tk.Namespace = testNS
	tk.Spec.ProjectRef = proj
	tk.Spec.RepositoryRef = repo
	tk.Spec.Kind = "review"
	tk.Spec.Goal = "review the PR"
	tk.Spec.Source = &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#9", IsPR: true, Number: 9,
	}
	if err := k8sClient.Create(context.Background(), tk); err != nil {
		t.Fatalf("create task: %v", err)
	}
	return tk
}

// TestTaskReconcile_ReviewTargetMerged_SkipsSpawn verifies that a fresh review
// Task whose PR is already merged terminates without spawning a pod.
func TestTaskReconcile_ReviewTargetMerged_SkipsSpawn(t *testing.T) {
	task := seedReviewTask(t, "merged")

	fs := newFakeSession()
	r := newTaskReconciler(fs)
	r.SCMFor = func(string) (scm.SCMWriter, error) {
		return &fullFakeSCMWriter{prState: scm.PRState{Merged: true}}, nil
	}

	if _, err := reconcileTask(t, r, task.Name); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := getTask(t, task.Name)
	if got.Status.Phase != "Succeeded" {
		t.Errorf("Phase = %q, want Succeeded (skipped, not spawned)", got.Status.Phase)
	}

	pod := &corev1.Pod{}
	if err := k8sClient.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: agent.PodName(got)}, pod); err == nil {
		t.Fatalf("expected no pod spawned for an already-merged review target")
	} else if !apierrors.IsNotFound(err) {
		t.Fatalf("unexpected error checking for pod: %v", err)
	}
}

// TestWriteBackReview_TargetMerged_SkipsVerdictPost verifies that a review
// verdict is never posted (Approve/Comment/RequestChanges) once the target
// PR/MR is already merged or closed - the item-1 root cause of PR #295's
// double-post.
func TestWriteBackReview_TargetMerged_SkipsVerdictPost(t *testing.T) {
	fw := &fullFakeSCMWriter{prState: scm.PRState{Merged: true}}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "wbk-rev-merged", "wbk-proj-merged", "wbk-repo-merged", "wbk-scm-merged",
		tatarav1alpha1.TaskSpec{
			Goal: "comment on a PR",
			Kind: "review",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#11", IsPR: true, Number: 11,
			},
		}, nil)
	task.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{Decision: "comment", Body: "nice work"}
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("status update: %v", err)
	}

	if _, err := reconcileWriteback(t, r, task.Name); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if fw.commentCalled {
		t.Fatal("review verdict must not be posted on an already-merged PR")
	}
	got := getTask(t, task.Name)
	cond := findCond(got.Status.Conditions, "WritebackPending")
	if cond == nil || cond.Status != metav1.ConditionFalse {
		t.Fatal("WritebackPending must be cleared even when the verdict is withheld")
	}
}
