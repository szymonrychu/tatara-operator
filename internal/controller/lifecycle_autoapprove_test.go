package controller

import (
	"context"
	"testing"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// seedAutoapproveTriage seeds a Triage/Succeeded issueLifecycle task with an
// implement outcome and the given issue author, under a project whose Scm
// carries BotLogin "bot" plus the given maintainer logins. The reconciler's
// ReaderFor returns a commentReader whose GetIssue body carries the
// tataraAuthoredMarker and which reports NO human comments - so the self-approve
// guard holds (-> Conversation) unless the author-tier bypass approves first
// (-> Implement). Returns the reconciler and task name.
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

	task.Status.LifecycleState = "Triage"
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
	return got.Status.LifecycleState
}

// TestTriageAutoapprove_ThirdPartyBypassesHold: an issue opened by a contributor
// who is neither the bot nor a maintainer is approved straight to Implement even
// though the body carries the tatara-authored marker and no human has commented
// (which would otherwise hold the issue in Conversation).
func TestTriageAutoapprove_ThirdPartyBypassesHold(t *testing.T) {
	r, name := seedAutoapproveTriage(t, "thirdparty", "third-party-dev", []string{"szymon"})
	if got := reconcileTriageState(t, r, name); got != "Implement" {
		t.Fatalf("LifecycleState = %q, want Implement (third-party autoapprove bypasses hold)", got)
	}
}

// TestTriageAutoapprove_MaintainerKeepsGate: an issue authored by a maintainer
// is NOT third-party, so the self-approve guard still applies; with the marker
// present and no human comment it holds in Conversation.
func TestTriageAutoapprove_MaintainerKeepsGate(t *testing.T) {
	r, name := seedAutoapproveTriage(t, "maintainer", "szymon", []string{"szymon"})
	if got := reconcileTriageState(t, r, name); got != "Conversation" {
		t.Fatalf("LifecycleState = %q, want Conversation (maintainer keeps self-approve gate)", got)
	}
}

// TestTriageAutoapprove_BotAuthoredKeepsGate: a bot-authored issue (author ==
// BotLogin) keeps the existing self-approve guard and holds in Conversation.
func TestTriageAutoapprove_BotAuthoredKeepsGate(t *testing.T) {
	r, name := seedAutoapproveTriage(t, "botauthored", "bot", nil)
	if got := reconcileTriageState(t, r, name); got != "Conversation" {
		t.Fatalf("LifecycleState = %q, want Conversation (bot-authored keeps self-approve gate)", got)
	}
}

// TestTriageAutoapprove_EmptyAuthorKeepsGate: when no author was captured the
// issue is not treated as third-party, so the marker-based self-approve guard
// still holds it in Conversation.
func TestTriageAutoapprove_EmptyAuthorKeepsGate(t *testing.T) {
	r, name := seedAutoapproveTriage(t, "noauthor", "", []string{"szymon"})
	if got := reconcileTriageState(t, r, name); got != "Conversation" {
		t.Fatalf("LifecycleState = %q, want Conversation (empty author keeps self-approve gate)", got)
	}
}

// TestThirdPartyAuthor_Classification unit-checks the author tier directly.
func TestThirdPartyAuthor_Classification(t *testing.T) {
	mk := func(author string, maintainers []string) (*tatarav1alpha1.Project, *tatarav1alpha1.Task) {
		p := &tatarav1alpha1.Project{Spec: tatarav1alpha1.ProjectSpec{
			Scm: &tatarav1alpha1.ScmSpec{BotLogin: "bot", MaintainerLogins: maintainers}}}
		tk := &tatarav1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: "x"},
			Spec: tatarav1alpha1.TaskSpec{Source: &tatarav1alpha1.TaskSource{AuthorLogin: author}}}
		return p, tk
	}
	cases := []struct {
		name        string
		author      string
		maintainers []string
		want        bool
	}{
		{"third party", "carol", []string{"szymon"}, true},
		{"bot", "bot", []string{"szymon"}, false},
		{"maintainer", "szymon", []string{"szymon"}, false},
		{"second maintainer", "alex", []string{"szymon", "alex"}, false},
		{"empty author", "", []string{"szymon"}, false},
		{"third party no maintainers", "carol", nil, true},
	}
	for _, c := range cases {
		p, tk := mk(c.author, c.maintainers)
		if got := thirdPartyAuthor(p, tk); got != c.want {
			t.Errorf("%s: thirdPartyAuthor = %v, want %v", c.name, got, c.want)
		}
	}
}
