package controller

import (
	"context"
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// getClarifyProject fetches the project seeded by seedClarifyTask for the suffix.
func getClarifyProject(t *testing.T, ctx context.Context, suffix string) *tatarav1alpha1.Project {
	t.Helper()
	pj := &tatarav1alpha1.Project{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "clf-p-" + suffix}, pj); err != nil {
		t.Fatalf("get project: %v", err)
	}
	return pj
}

// setClarifyConversation moves a seeded clarify task into the live discuss state
// with the given deadline offset (negative = already past) and interjections.
func setClarifyConversation(t *testing.T, ctx context.Context, name string, deadlineOffset time.Duration, interjections []string) {
	t.Helper()
	tk := getClarifyTask(t, ctx, name)
	tk.Status.DeployState = "Conversation"
	tk.Status.Phase = ""
	dl := metav1.NewTime(time.Now().Add(deadlineOffset))
	tk.Status.DeadlineAt = &dl
	tk.Status.PendingInterjections = interjections
	if err := k8sClient.Status().Update(ctx, tk); err != nil {
		t.Fatalf("set conversation status: %v", err)
	}
}

// TestClarifyTimeout_KillsAtDeadline: a clarify Task in discuss past its 1h
// wall-clock deadline with no pending interjection is stopped with reason
// "clarify-timeout" (the live pod is torn down).
func TestClarifyTimeout_KillsAtDeadline(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, _, name := seedClarifyTask(t, "to-kill", "human", nil)
	setClarifyConversation(t, ctx, name, -time.Minute, nil)
	proj := getClarifyProject(t, ctx, "to-kill")

	if _, err := r.handleClarifyConversation(ctx, proj, getClarifyTask(t, ctx, name)); err != nil {
		t.Fatalf("handleClarifyConversation: %v", err)
	}

	got := getClarifyTask(t, ctx, name)
	if got.Status.DeployState != "Parked" {
		t.Errorf("DeployState = %q, want Parked", got.Status.DeployState)
	}
	if got.Status.ParkReason != "clarify-timeout" {
		t.Errorf("ParkReason = %q, want clarify-timeout", got.Status.ParkReason)
	}
}

// TestClarifyTimeout_RequeuesLiveBeforeDeadline: before the deadline the task
// stays live in Conversation and requeues.
func TestClarifyTimeout_RequeuesLiveBeforeDeadline(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, _, name := seedClarifyTask(t, "to-live", "human", nil)
	setClarifyConversation(t, ctx, name, time.Hour, nil)
	proj := getClarifyProject(t, ctx, "to-live")

	res, err := r.handleClarifyConversation(ctx, proj, getClarifyTask(t, ctx, name))
	if err != nil {
		t.Fatalf("handleClarifyConversation: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Errorf("expected a live requeue before deadline, got %v", res.RequeueAfter)
	}
	got := getClarifyTask(t, ctx, name)
	if got.Status.DeployState != "Conversation" {
		t.Errorf("DeployState = %q, want Conversation (still live)", got.Status.DeployState)
	}
}

// TestClarifyTimeout_ExtendsOnPendingInterjection: a human comment that arrived
// just before the deadline (waiting, not stalled) extends the window rather than
// killing the pod.
func TestClarifyTimeout_ExtendsOnPendingInterjection(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, _, name := seedClarifyTask(t, "to-ext", "human", nil)
	setClarifyConversation(t, ctx, name, -time.Minute, []string{"here is my answer"})
	proj := getClarifyProject(t, ctx, "to-ext")

	if _, err := r.handleClarifyConversation(ctx, proj, getClarifyTask(t, ctx, name)); err != nil {
		t.Fatalf("handleClarifyConversation: %v", err)
	}

	got := getClarifyTask(t, ctx, name)
	if got.Status.DeployState != "Conversation" {
		t.Errorf("DeployState = %q, want Conversation (extended, not killed)", got.Status.DeployState)
	}
	if got.Status.DeadlineAt == nil || !got.Status.DeadlineAt.After(time.Now()) {
		t.Errorf("expected extended future deadline, got %v", got.Status.DeadlineAt)
	}
}
