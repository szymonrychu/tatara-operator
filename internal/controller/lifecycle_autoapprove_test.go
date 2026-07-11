package controller

import (
	"context"
	"errors"
	"strings"
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

// ----- Path (c): auto-approve (item 4a) -----

// setAutoApproveFlag flips Project.Spec.AutoApproveTataraProposals.
func setAutoApproveFlag(t *testing.T, projName string, on bool) {
	t.Helper()
	ctx := context.Background()
	var p tatarav1alpha1.Project
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: projName}, &p); err != nil {
		t.Fatalf("get project: %v", err)
	}
	p.Spec.AutoApproveTataraProposals = on
	if err := k8sClient.Update(ctx, &p); err != nil {
		t.Fatalf("set auto-approve flag: %v", err)
	}
}

// setProjectProvider flips Project.Spec.Scm.Provider AND the seeded Task's
// Spec.Source.Provider (scmContext prefers the latter when set), so both
// agree on the target provider.
func setProjectProvider(t *testing.T, projName, taskName, provider string) {
	t.Helper()
	ctx := context.Background()
	var p tatarav1alpha1.Project
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: projName}, &p); err != nil {
		t.Fatalf("get project: %v", err)
	}
	p.Spec.Scm.Provider = provider
	if err := k8sClient.Update(ctx, &p); err != nil {
		t.Fatalf("set provider: %v", err)
	}
	var tk tatarav1alpha1.Task
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: taskName}, &tk); err != nil {
		t.Fatalf("get task: %v", err)
	}
	tk.Spec.Source.Provider = provider
	if err := k8sClient.Update(ctx, &tk); err != nil {
		t.Fatalf("set task source provider: %v", err)
	}
}

// ----- FIX-1: a human closing the tracked issue vetoes auto-approve -----

// TestLifecycleTriage_AutoApprove_ClosedTrackedIssue_Withheld: the tracked
// proposal issue is CLOSED (a human veto) even though every other auto-approve
// precondition holds. Auto-approve must be withheld and no audit comment posted.
func TestLifecycleTriage_AutoApprove_ClosedTrackedIssue_Withheld(t *testing.T) {
	r, name := seedAutoapproveTriage(t, "closed-veto", "bot", nil)
	setAutoApproveFlag(t, "lc-aap-closed-veto", true)
	fw := &lifecycleFakeSCMWriter{issueClosed: true}
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }
	r.ReaderFor = func(_, _ string) (scm.SCMReader, error) {
		return &commentReader{body: tataraAuthoredMarker + "\n" + tataraProposedByMarker("incident")}, nil
	}

	if got := reconcileTriageState(t, r, name); got != "Conversation" {
		t.Fatalf("DeployState = %q, want Conversation (a closed tracked issue must veto auto-approve)", got)
	}
	got := getTaskByName(t, name)
	if got.Status.ApprovedByMaintainer != "" {
		t.Errorf("ApprovedByMaintainer = %q, want empty (closed issue must not auto-approve)", got.Status.ApprovedByMaintainer)
	}
	if got.Status.AutoApproved {
		t.Error("AutoApproved must stay false when withheld by the closed-issue veto")
	}
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if len(fw.commentCalls) != 0 {
		t.Errorf("no audit comment must post when withheld by the closed-issue veto; got %+v", fw.commentCalls)
	}
}

// TestLifecycleTriage_AutoApprove_IssueStateReadError_Withheld: an SCM read
// error resolving the tracked issue's state must fail closed - never approve
// on unread state.
func TestLifecycleTriage_AutoApprove_IssueStateReadError_Withheld(t *testing.T) {
	r, name := seedAutoapproveTriage(t, "state-err", "bot", nil)
	setAutoApproveFlag(t, "lc-aap-state-err", true)
	fw := &lifecycleFakeSCMWriter{issueStateErr: errors.New("scm down")}
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }
	r.ReaderFor = func(_, _ string) (scm.SCMReader, error) {
		return &commentReader{body: tataraAuthoredMarker + "\n" + tataraProposedByMarker("incident")}, nil
	}

	if got := reconcileTriageState(t, r, name); got != "Conversation" {
		t.Fatalf("DeployState = %q, want Conversation (an issue-state read error must fail closed)", got)
	}
	got := getTaskByName(t, name)
	if got.Status.ApprovedByMaintainer != "" {
		t.Errorf("ApprovedByMaintainer = %q, want empty", got.Status.ApprovedByMaintainer)
	}
}

// ----- FIX-4: isBotAuthoredProposal must verify the author live on GitLab -----

// TestLifecycleTriage_AutoApprove_GitLab_SpoofedActorHint_Withheld: on GitLab,
// Source.AuthorLogin is the webhook ACTOR, not the resource author. An actor
// whose login happens to equal the bot login must NOT auto-approve when the
// live GetIssueState author is a human.
func TestLifecycleTriage_AutoApprove_GitLab_SpoofedActorHint_Withheld(t *testing.T) {
	r, name := seedAutoapproveTriage(t, "gl-spoof", "bot", nil)
	setAutoApproveFlag(t, "lc-aap-gl-spoof", true)
	setProjectProvider(t, "lc-aap-gl-spoof", name, "gitlab")
	fw := &lifecycleFakeSCMWriter{issueAuthor: "human"}
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }
	r.ReaderFor = func(_, _ string) (scm.SCMReader, error) {
		return &commentReader{body: tataraAuthoredMarker + "\n" + tataraProposedByMarker("incident")}, nil
	}

	if got := reconcileTriageState(t, r, name); got != "Conversation" {
		t.Fatalf("DeployState = %q, want Conversation (gitlab must ignore the actor hint and verify live author)", got)
	}
	got := getTaskByName(t, name)
	if got.Status.ApprovedByMaintainer != "" {
		t.Errorf("ApprovedByMaintainer = %q, want empty (spoofed actor hint must not auto-approve on gitlab)", got.Status.ApprovedByMaintainer)
	}
}

// TestLifecycleTriage_AutoApprove_GitLab_LiveAuthorBot_Releases: on GitLab, a
// human actor hint must not block auto-approve when the live GetIssueState
// author IS the bot.
func TestLifecycleTriage_AutoApprove_GitLab_LiveAuthorBot_Releases(t *testing.T) {
	r, name := seedAutoapproveTriage(t, "gl-live", "human-actor", nil)
	setAutoApproveFlag(t, "lc-aap-gl-live", true)
	setProjectProvider(t, "lc-aap-gl-live", name, "gitlab")
	fw := &lifecycleFakeSCMWriter{issueAuthor: "bot"}
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }
	r.ReaderFor = func(_, _ string) (scm.SCMReader, error) {
		return &commentReader{body: tataraAuthoredMarker + "\n" + tataraProposedByMarker("incident")}, nil
	}

	if got := reconcileTriageState(t, r, name); got != "Implement" {
		t.Fatalf("DeployState = %q, want Implement (gitlab live author is the bot -> auto-approve)", got)
	}
	got := getTaskByName(t, name)
	if got.Status.ApprovedByMaintainer != "<tatara:auto:incident>" {
		t.Errorf("ApprovedByMaintainer = %q, want the auto sentinel", got.Status.ApprovedByMaintainer)
	}
}

// ----- FIX-3: recordAutoApproval must not clobber a concurrent approval -----

// TestRecordAutoApproval_ConcurrentApproval_NoOverwrite: a concurrent label
// (or conversational) approval landing between the caller's approvingMaintainer
// scan and this call must win - recordAutoApproval must re-check inside the
// retry closure and no-op rather than overwrite the audit attribution.
func TestRecordAutoApproval_ConcurrentApproval_NoOverwrite(t *testing.T) {
	r, name := seedAutoapproveTriage(t, "race", "bot", nil)
	ctx := context.Background()
	tk := getTaskByName(t, name)
	tk.Status.ApprovedByMaintainer = "szymon" // simulates a concurrent label approval
	if err := k8sClient.Status().Update(ctx, tk); err != nil {
		t.Fatalf("seed concurrent approval: %v", err)
	}

	recorded, err := r.recordAutoApproval(ctx, tk, "incident")
	if err != nil {
		t.Fatalf("recordAutoApproval: %v", err)
	}
	if recorded {
		t.Fatal("recordAutoApproval must report recorded=false when a concurrent approval already landed")
	}
	if tk.Status.ApprovedByMaintainer != "szymon" {
		t.Errorf("ApprovedByMaintainer = %q, want szymon (must not be overwritten)", tk.Status.ApprovedByMaintainer)
	}
	if tk.Status.AutoApproved {
		t.Error("AutoApproved must stay false when a concurrent non-auto approval won the race")
	}

	got := getTaskByName(t, name)
	if got.Status.ApprovedByMaintainer != "szymon" {
		t.Errorf("persisted ApprovedByMaintainer = %q, want szymon", got.Status.ApprovedByMaintainer)
	}
}

// TestRecordAutoApproval_Empty_Records is the regression guard: the ordinary
// clean auto-approve (no concurrent writer) still records the sentinel.
func TestRecordAutoApproval_Empty_Records(t *testing.T) {
	r, name := seedAutoapproveTriage(t, "race-ok", "bot", nil)
	ctx := context.Background()
	tk := getTaskByName(t, name)

	recorded, err := r.recordAutoApproval(ctx, tk, "incident")
	if err != nil {
		t.Fatalf("recordAutoApproval: %v", err)
	}
	if !recorded {
		t.Fatal("recordAutoApproval must report recorded=true on a clean approve")
	}
	if tk.Status.ApprovedByMaintainer != "<tatara:auto:incident>" {
		t.Errorf("ApprovedByMaintainer = %q, want the auto sentinel", tk.Status.ApprovedByMaintainer)
	}
	if !tk.Status.AutoApproved {
		t.Error("AutoApproved must be true")
	}
}

// TestLifecycleTriage_AutoApprove_BotAuthored_FlagOn_Releases: a bot-authored,
// tatara-proposed issue under the project flag auto-approves - the incident
// investigation itself served as review.
func TestLifecycleTriage_AutoApprove_BotAuthored_FlagOn_Releases(t *testing.T) {
	r, name := seedAutoapproveTriage(t, "aa-on", "bot", nil)
	setAutoApproveFlag(t, "lc-aap-aa-on", true)
	fw := &lifecycleFakeSCMWriter{}
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }
	r.ReaderFor = func(_, _ string) (scm.SCMReader, error) {
		return &commentReader{body: tataraAuthoredMarker + "\n" + tataraProposedByMarker("incident")}, nil
	}

	if got := reconcileTriageState(t, r, name); got != "Implement" {
		t.Fatalf("DeployState = %q, want Implement (bot-authored + tatara-proposed + flag on must auto-approve)", got)
	}
	got := getTaskByName(t, name)
	if got.Status.ApprovedByMaintainer != "<tatara:auto:incident>" {
		t.Fatalf("ApprovedByMaintainer = %q, want the auto sentinel", got.Status.ApprovedByMaintainer)
	}
	if !got.Status.AutoApproved {
		t.Error("AutoApproved must be true")
	}
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if len(fw.commentCalls) != 1 {
		t.Fatalf("want exactly 1 audit comment, got %d", len(fw.commentCalls))
	}
	if !strings.Contains(fw.commentCalls[0].body, "auto-approved") {
		t.Errorf("audit comment = %q, want to mention auto-approved", fw.commentCalls[0].body)
	}
}

// TestLifecycleTriage_AutoApprove_HumanAuthored_NeverReleases: a human-authored
// issue must NEVER auto-approve, even with the marker present (adversarial:
// marker text injected into a human-authored issue body).
func TestLifecycleTriage_AutoApprove_HumanAuthored_NeverReleases(t *testing.T) {
	r, name := seedAutoapproveTriage(t, "aa-human", "random-human", nil)
	setAutoApproveFlag(t, "lc-aap-aa-human", true)
	r.ReaderFor = func(_, _ string) (scm.SCMReader, error) {
		return &commentReader{body: tataraAuthoredMarker + "\n" + tataraProposedByMarker("incident")}, nil
	}

	if got := reconcileTriageState(t, r, name); got != "Conversation" {
		t.Fatalf("DeployState = %q, want Conversation (human-authored must never auto-approve)", got)
	}
	got := getTaskByName(t, name)
	if got.Status.ApprovedByMaintainer != "" {
		t.Errorf("ApprovedByMaintainer = %q, want empty", got.Status.ApprovedByMaintainer)
	}
	if got.Status.AutoApproved {
		t.Error("AutoApproved must stay false")
	}
}

// TestLifecycleTriage_AutoApprove_FlagOff_ParksAsToday: bot-authored + marker
// present but the project flag is off (default) - parks exactly as today.
func TestLifecycleTriage_AutoApprove_FlagOff_ParksAsToday(t *testing.T) {
	r, name := seedAutoapproveTriage(t, "aa-flagoff", "bot", nil)
	r.ReaderFor = func(_, _ string) (scm.SCMReader, error) {
		return &commentReader{body: tataraAuthoredMarker + "\n" + tataraProposedByMarker("brainstorm")}, nil
	}

	if got := reconcileTriageState(t, r, name); got != "Conversation" {
		t.Fatalf("DeployState = %q, want Conversation (flag off must not auto-approve)", got)
	}
}

// TestLifecycleTriage_AutoApprove_NoMarker_NeverReleases: bot-authored, flag on,
// but the body has no tatara-proposed-by marker (e.g. a plain tataraAuthoredMarker-
// only follow-up issue) - must not auto-approve.
func TestLifecycleTriage_AutoApprove_NoMarker_NeverReleases(t *testing.T) {
	r, name := seedAutoapproveTriage(t, "aa-nomarker", "bot", nil)
	setAutoApproveFlag(t, "lc-aap-aa-nomarker", true)
	// seedAutoapproveTriage's default reader body carries only tataraAuthoredMarker.

	if got := reconcileTriageState(t, r, name); got != "Conversation" {
		t.Fatalf("DeployState = %q, want Conversation (no kind marker must not auto-approve)", got)
	}
}

// TestLifecycleTriage_AutoApprove_ComposesWithLabelPath_NoDoubleApprove: when
// path (a) (label approval) already recorded ApprovedByMaintainer, the whole
// gate block is skipped - no double-approve, no audit comment, no metric.
func TestLifecycleTriage_AutoApprove_ComposesWithLabelPath_NoDoubleApprove(t *testing.T) {
	r, name := seedAutoapproveTriage(t, "aa-label", "bot", nil)
	setAutoApproveFlag(t, "lc-aap-aa-label", true)
	fw := &lifecycleFakeSCMWriter{}
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }
	r.ReaderFor = func(_, _ string) (scm.SCMReader, error) {
		return &commentReader{body: tataraAuthoredMarker + "\n" + tataraProposedByMarker("incident")}, nil
	}
	recordApproval(t, name, "szymon")

	if got := reconcileTriageState(t, r, name); got != "Implement" {
		t.Fatalf("DeployState = %q, want Implement (already approved via label path)", got)
	}
	got := getTaskByName(t, name)
	if got.Status.ApprovedByMaintainer != "szymon" {
		t.Errorf("ApprovedByMaintainer = %q, want szymon (must not be overwritten by auto-approve)", got.Status.ApprovedByMaintainer)
	}
	if got.Status.AutoApproved {
		t.Error("AutoApproved must stay false - path (a) already released, path (c) never ran")
	}
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if len(fw.commentCalls) != 0 {
		t.Errorf("want no audit comment when already approved via label path, got %d", len(fw.commentCalls))
	}
}

// TestLifecycleTriage_AutoApprove_ComposesWithConversationalPath_PrefersConversational:
// when a verified maintainer conversational approval (path b) is ALSO available,
// it wins over auto-approve (path c) - a human's live approval takes precedence.
func TestLifecycleTriage_AutoApprove_ComposesWithConversationalPath_PrefersConversational(t *testing.T) {
	r, name := seedAutoapproveTriage(t, "aa-conv", "bot", []string{"szymon"})
	setAutoApproveFlag(t, "lc-aap-aa-conv", true)
	r.ReaderFor = func(_, _ string) (scm.SCMReader, error) {
		return &commentReader{body: tataraAuthoredMarker + "\n" + tataraProposedByMarker("incident"),
			comments: []scm.IssueComment{{Author: "szymon", Body: "approved, go"}}}, nil
	}

	if got := reconcileTriageState(t, r, name); got != "Implement" {
		t.Fatalf("DeployState = %q, want Implement", got)
	}
	got := getTaskByName(t, name)
	if got.Status.ApprovedByMaintainer != "szymon" {
		t.Errorf("ApprovedByMaintainer = %q, want szymon (conversational approval takes precedence)", got.Status.ApprovedByMaintainer)
	}
	if got.Status.AutoApproved {
		t.Error("AutoApproved must stay false when conversational approval released it")
	}
}
