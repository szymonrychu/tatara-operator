package controller

import (
	"context"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// clarifyFakeWriter captures label + comment + close calls for the clarify
// handoff assertions (the shared lifecycleFakeSCMWriter no-ops AddLabel/RemoveLabel).
type clarifyFakeWriter struct {
	scm.SCMWriter
	mu           sync.Mutex
	addLabels    []struct{ issueRef, label string }
	removeLabels []struct{ issueRef, label string }
	comments     []struct{ issueRef, body string }
	closes       []struct{ repo, comment string }
}

func (f *clarifyFakeWriter) GetIssueState(_ context.Context, _, _ string, _ int) (scm.IssueState, error) {
	return scm.IssueState{}, nil
}
func (f *clarifyFakeWriter) AddLabel(_ context.Context, _, issueRef, label string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.addLabels = append(f.addLabels, struct{ issueRef, label string }{issueRef, label})
	return nil
}

func (f *clarifyFakeWriter) RemoveLabel(_ context.Context, _, issueRef, label string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removeLabels = append(f.removeLabels, struct{ issueRef, label string }{issueRef, label})
	return nil
}

func (f *clarifyFakeWriter) Comment(_ context.Context, _, issueRef, body string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.comments = append(f.comments, struct{ issueRef, body string }{issueRef, body})
	return nil
}

func (f *clarifyFakeWriter) CloseIssue(_ context.Context, _, repo string, _ int, comment string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closes = append(f.closes, struct{ repo, comment string }{repo, comment})
	return nil
}

func (f *clarifyFakeWriter) addedLabels() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.addLabels))
	for i, a := range f.addLabels {
		out[i] = a.label
	}
	return out
}

func (f *clarifyFakeWriter) removedLabels() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.removeLabels))
	for i, a := range f.removeLabels {
		out[i] = a.label
	}
	return out
}

func containsStr(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// seedClarifyTask creates a project+repo+secret and a clarify Task at
// DeployState=Triage/Phase=Succeeded with the given outcome and author, then
// returns a reconciler wired with the capturing writer plus the task name.
func seedClarifyTask(t *testing.T, suffix, author string, outcome *tatarav1alpha1.IssueOutcome) (*TaskReconciler, *clarifyFakeWriter, string) {
	t.Helper()
	ctx := context.Background()
	name := "clf-" + suffix
	proj := "clf-p-" + suffix
	repo := "clf-r-" + suffix
	sec := "clf-s-" + suffix
	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#5", URL: "https://github.com/o/r/issues/5",
		Number: 5, AuthorLogin: author,
	}

	mkSecret(t, sec, map[string][]byte{"token": []byte("tok"), "webhookSecret": []byte("wh")})
	pj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: proj, Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: sec,
			Scm:          &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "bot"},
			Agent: tatarav1alpha1.AgentSpec{
				Model: "claude-x", Image: "wrapper:1", PermissionMode: "bypassPermissions",
				MaxTurnsPerTask: 50, TurnTimeoutSeconds: 1800,
			},
		},
	}
	if err := k8sClient.Create(ctx, pj); err != nil {
		t.Fatalf("create project: %v", err)
	}
	pj.Status.Memory = stableMemStatus("http://mem.svc:8080")
	if err := k8sClient.Status().Update(ctx, pj); err != nil {
		t.Fatalf("set memory ready: %v", err)
	}
	rp := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: repo, Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef: proj, URL: "https://github.com/o/r.git",
			DefaultBranch: "main", ReingestSchedule: "0 6 * * *",
		},
	}
	if err := k8sClient.Create(ctx, rp); err != nil {
		t.Fatalf("create repo: %v", err)
	}
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: proj, RepositoryRef: repo,
			Goal: "Issue #5: fix the login bug", Kind: "clarify", Source: src,
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task: %v", err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(context.Background(), task) })
	task.Status.DeployState = "Triage"
	task.Status.Phase = "Succeeded"
	task.Status.IssueOutcome = outcome
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed clarify status: %v", err)
	}

	fw := &clarifyFakeWriter{}
	reg := prometheus.NewRegistry()
	r := &TaskReconciler{
		Client:           k8sClient,
		Scheme:           k8sClient.Scheme(),
		Metrics:          obs.NewOperatorMetrics(reg),
		LifecycleMetrics: obs.NewLifecycleMetrics(reg),
		Session:          newFakeSession(),
		PodConfig:        agent.PodConfig{Namespace: testNS, CallbackURL: "http://op-internal.tatara.svc:8082"},
		SCMFor:           func(string) (scm.SCMWriter, error) { return fw, nil },
	}
	return r, fw, name
}

func getClarifyTask(t *testing.T, ctx context.Context, name string) *tatarav1alpha1.Task {
	t.Helper()
	tk := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, tk); err != nil {
		t.Fatalf("get task %s: %v", name, err)
	}
	return tk
}

// TestClarify_ImplementHandoff: a clarify Task whose agent chose implement flips
// the managed labels (remove brainstorming, add implementation), populates the
// warm-resume handoff artifact, and terminates the clarify Task (Done) - it must
// NOT enter the Implement lifecycle state.
func TestClarify_ImplementHandoff(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, fw, name := seedClarifyTask(t, "impl", "human", &tatarav1alpha1.IssueOutcome{
		Action: "implement", Comment: "Agreed plan: rework the auth middleware.",
	})
	recordApproval(t, name, "szymon") // verified maintainer approval gates the handoff

	if _, err := r.reconcileClarify(ctx, getClarifyTask(t, ctx, name)); err != nil {
		t.Fatalf("reconcileClarify: %v", err)
	}

	if !containsStr(fw.addedLabels(), "tatara-implementation") {
		t.Errorf("expected tatara-implementation added; got adds=%v", fw.addedLabels())
	}
	if !containsStr(fw.removedLabels(), "tatara-brainstorming") {
		t.Errorf("expected tatara-brainstorming removed; got removes=%v", fw.removedLabels())
	}
	if containsStr(fw.addedLabels(), "tatara-approved") {
		t.Error("clarify handoff must NOT add tatara-approved (that is the review path)")
	}

	got := getClarifyTask(t, ctx, name)
	if got.Status.DeployState != "Done" {
		t.Errorf("DeployState = %q, want Done (clarify terminates on handoff)", got.Status.DeployState)
	}
	if got.Status.DeployState == "Implement" {
		t.Error("clarify must NOT enter the Implement lifecycle state")
	}
	if got.Status.Handover == "" {
		t.Error("clarify handoff must populate Status.Handover (warm-resume artifact)")
	}
}

func TestClarify_ImplementHandoff_LockedSetsStatus(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, _, name := seedClarifyTask(t, "impl-locked", "human", &tatarav1alpha1.IssueOutcome{
		Action: "implement", Comment: "Agreed plan.", Locked: true,
	})
	recordApproval(t, name, "szymon")

	if _, err := r.reconcileClarify(ctx, getClarifyTask(t, ctx, name)); err != nil {
		t.Fatalf("reconcileClarify: %v", err)
	}

	got := getClarifyTask(t, ctx, name)
	if !got.Status.ImplementationLocked {
		t.Error("ImplementationLocked = false, want true when issue_outcome carried locked=true")
	}
}

func TestClarify_ImplementHandoff_UnlockedLeavesStatusFalse(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, _, name := seedClarifyTask(t, "impl-unlocked", "human", &tatarav1alpha1.IssueOutcome{
		Action: "implement", Comment: "Agreed plan.", // Locked defaults to false
	})
	recordApproval(t, name, "szymon")

	if _, err := r.reconcileClarify(ctx, getClarifyTask(t, ctx, name)); err != nil {
		t.Fatalf("reconcileClarify: %v", err)
	}

	got := getClarifyTask(t, ctx, name)
	if got.Status.ImplementationLocked {
		t.Error("ImplementationLocked = true, want false when issue_outcome did not carry locked")
	}
}

// TestClarify_DiscussStaysLive: a clarify Task whose agent chose discuss posts
// the comment and stays live in Conversation with a deadline.
func TestClarify_DiscussStaysLive(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, fw, name := seedClarifyTask(t, "discuss", "human", &tatarav1alpha1.IssueOutcome{
		Action: "discuss", Comment: "I have two design questions before we implement.",
	})

	if _, err := r.reconcileClarify(ctx, getClarifyTask(t, ctx, name)); err != nil {
		t.Fatalf("reconcileClarify: %v", err)
	}

	fw.mu.Lock()
	nComments := len(fw.comments)
	fw.mu.Unlock()
	if nComments != 1 {
		t.Fatalf("expected 1 comment posted, got %d", nComments)
	}

	got := getClarifyTask(t, ctx, name)
	if got.Status.DeployState != "Conversation" {
		t.Errorf("DeployState = %q, want Conversation", got.Status.DeployState)
	}
	if got.Status.DeadlineAt == nil {
		t.Error("DeadlineAt must be set after discuss (live pod idle window)")
	}
}

// TestClarify_CloseTerminates: a clarify close outcome closes the issue and marks
// the Task Done.
func TestClarify_CloseTerminates(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)
	r, fw, name := seedClarifyTask(t, "close", "human", &tatarav1alpha1.IssueOutcome{
		Action: "close", Comment: "duplicate of #3",
	})

	if _, err := r.reconcileClarify(ctx, getClarifyTask(t, ctx, name)); err != nil {
		t.Fatalf("reconcileClarify: %v", err)
	}

	fw.mu.Lock()
	nCloses := len(fw.closes)
	fw.mu.Unlock()
	if nCloses != 1 {
		t.Fatalf("expected 1 CloseIssue, got %d", nCloses)
	}

	got := getClarifyTask(t, ctx, name)
	if got.Status.DeployState != "Done" {
		t.Errorf("DeployState = %q, want Done", got.Status.DeployState)
	}
}
