// Copyright 2026 tatara authors.

package controller

// M3 Task 5 tests: resume implement from handover doc.

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// TestContextGuard_ResumeFromHandover_InjectsTurnZero verifies that when the
// pending-handover-resume annotation is set, the turn-0 prompt includes
// the handover doc and the annotation is cleared after the run starts.
func TestContextGuard_ResumeFromHandover_InjectsTurnZero(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	name := "lc-m3-resume"
	proj := "lc-m3-rp"
	repo := "lc-m3-rr"
	sec := "lc-m3-rs"

	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#202",
		URL: "https://github.com/o/r/issues/202", Number: 202,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)
	handoverDoc := "Prior work is on branch feat/login-fix; read its diff. OAuth2 login is done."
	task.Status.DeployState = "Implement"
	task.Status.Phase = ""
	task.Status.LifecycleIterations = 1
	task.Status.Handover = handoverDoc
	if err := k8sClient.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	// Set the pending-handover-resume annotation.
	annotate(t, name, map[string]string{annPendingHandoverResume: "true"})

	sess := newFakeSession()
	fw := &lifecycleFakeSCMWriter{}
	r := newLifecycleReconciler(t, fw)
	r.Session = sess

	// First reconcile: spawn (Phase="") -> Planning phase, no turn submitted yet.
	_, err := r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle spawn: %v", err)
	}
	setTaskPhase(t, name, "Planning")

	// The first reconcile already created the pod via driveAgentRun; just mark it ready.
	pod := &corev1.Pod{}
	if err := k8sClient.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: agent.PodName(fetchTask(t, name))}, pod); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if err := k8sClient.Status().Update(context.Background(), pod); err != nil {
		t.Fatalf("set pod ready: %v", err)
	}

	// Second reconcile: pod ready -> submit turn-0 prompt.
	_, err = r.reconcileLifecycle(ctx, fetchTask(t, name))
	if err != nil {
		t.Fatalf("reconcileLifecycle turn-0: %v", err)
	}

	// Turn-0 must contain handover doc.
	sub, ok := sess.lastSubmit()
	if !ok {
		t.Fatal("expected a SubmitTurn call; none recorded")
	}
	if !strings.Contains(sub.Text, handoverDoc) {
		t.Errorf("turn-0 prompt missing handover doc;\nfull text: %q", sub.Text)
	}
	if !strings.Contains(sub.Text, "## Resume from handover") {
		t.Errorf("turn-0 prompt missing '## Resume from handover' section;\nfull text: %q", sub.Text)
	}

	// Annotation must be cleared after turn-0 submission.
	got := fetchTask(t, name)
	if got.Annotations[annPendingHandoverResume] != "" {
		t.Errorf("annotation %q still set after resume; want cleared", annPendingHandoverResume)
	}
	// Status.Handover must remain for audit.
	if got.Status.Handover == "" {
		t.Error("Status.Handover must remain after resume (audit trail)")
	}
}

// Ensure types import is used.
var _ = metav1.ObjectMeta{}
