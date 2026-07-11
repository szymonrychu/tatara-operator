package controller

import (
	"context"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// seedAutoapproveTriage seeds a Triage/Succeeded issueLifecycle task with an
// implement outcome and the given issue author, under a project whose Scm
// carries BotLogin "bot" plus the given maintainer logins. The reconciler's
// ReaderFor returns a commentReader whose GetIssue body carries the
// tataraAuthoredMarker and which reports NO human comments. With the
// maintainer-approval gate in force, the task holds in Conversation unless a
// verified maintainer approval has been recorded on its status
// (Status.ApprovedByMaintainer); authorship no longer changes the outcome.
// Returns the reconciler and task name.
func seedAutoapproveTriage(t *testing.T, suffix, author string, maintainers []string) (*TaskReconciler, string) {
	t.Helper()
	ctx := context.Background()
	name := "lc-aa-" + suffix
	proj := "lc-aap-" + suffix
	repo := "lc-aar-" + suffix
	sec := "lc-aas-" + suffix
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#5", URL: "https://github.com/o/r/issues/5",
		Number: 5, AuthorLogin: author,
	}
	task := seedLifecycleTask(t, name, proj, repo, sec, src)

	var p tatarav1alpha1.Project
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: proj}, &p); err != nil {
		t.Fatalf("get project: %v", err)
	}
	p.Spec.Scm.MaintainerLogins = maintainers
	if err := k8sClient.Update(ctx, &p); err != nil {
		t.Fatalf("update project scm: %v", err)
	}

	task.Status.DeployState = "Triage"
	task.Status.Phase = "Succeeded"
	task.Status.IssueOutcome = &tatarav1alpha1.IssueOutcome{Action: "implement"}
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed triage succeeded: %v", err)
	}

	r := newLifecycleReconciler(t, &lifecycleFakeSCMWriter{})
	rdr := &commentReader{body: tataraAuthoredMarker} // marker present, no human comments
	r.ReaderFor = func(_, _ string) (scm.SCMReader, error) { return rdr, nil }
	return r, name
}

func reconcileTriageState(t *testing.T, r *TaskReconciler, name string) string {
	t.Helper()
	ctx := logf.IntoContext(context.Background(), logf.Log)
	tk := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, tk); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if _, err := r.reconcileLifecycle(ctx, tk); err != nil {
		t.Fatalf("reconcileLifecycle: %v", err)
	}
	got := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, got); err != nil {
		t.Fatalf("get task after: %v", err)
	}
	return got.Status.DeployState
}

// TestTriageGate_MaintainerAuthorHolds: an issue authored by a maintainer with
// no recorded approval still holds in Conversation - authorship is not approval.
func TestTriageGate_MaintainerAuthorHolds(t *testing.T) {
	r, name := seedAutoapproveTriage(t, "maintainer", "szymon", []string{"szymon"})
	if got := reconcileTriageState(t, r, name); got != "Conversation" {
		t.Fatalf("DeployState = %q, want Conversation (maintainer author is not approval)", got)
	}
}

// TestTriageGate_BotAuthoredHolds: a bot-authored issue holds in Conversation.
func TestTriageGate_BotAuthoredHolds(t *testing.T) {
	r, name := seedAutoapproveTriage(t, "botauthored", "bot", nil)
	if got := reconcileTriageState(t, r, name); got != "Conversation" {
		t.Fatalf("DeployState = %q, want Conversation (bot-authored holds)", got)
	}
}

// TestTriageGate_EmptyAuthorHolds: an issue with no captured author holds.
func TestTriageGate_EmptyAuthorHolds(t *testing.T) {
	r, name := seedAutoapproveTriage(t, "noauthor", "", []string{"szymon"})
	if got := reconcileTriageState(t, r, name); got != "Conversation" {
		t.Fatalf("DeployState = %q, want Conversation (empty author holds)", got)
	}
}

// TestTriageGate_NonApproverCommentHolds: a comment from a NON-maintainer does
// not release the gate - the issue stays in Conversation.
func TestTriageGate_NonApproverCommentHolds(t *testing.T) {
	r, name := seedAutoapproveTriage(t, "apprgatenon", "szymon", []string{"szymon"})
	r.ReaderFor = func(_, _ string) (scm.SCMReader, error) {
		return &commentReader{body: tataraAuthoredMarker,
			comments: []scm.IssueComment{{Author: "random-human", Body: "do it"}}}, nil
	}
	if got := reconcileTriageState(t, r, name); got != "Conversation" {
		t.Fatalf("DeployState = %q, want Conversation (non-maintainer comment must not release)", got)
	}
}
