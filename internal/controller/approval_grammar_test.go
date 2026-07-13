package controller

import (
	"context"
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TestGrammarMatchesApprovalPhrase is the C.6 clause (c) ANCHORED WHOLE-LINE
// matcher. The comment must CONSIST OF the phrase, not contain it: the v3
// substring match approved "I can't approve this until the tests pass", and
// because clause (b) takes the maintainer's MOST RECENT comment, their own
// corrective follow-up approved the work too.
func TestGrammarMatchesApprovalPhrase(t *testing.T) {
	def := tatarav1alpha1.DefaultApprovalPhrases()

	tests := []struct {
		name    string
		body    string
		phrases []string
		want    string
		ok      bool
	}{
		{"negated approve does not approve", "I can't approve this until tests pass", def, "", false},
		{"negated go-ahead does not approve", "don't go ahead with this", def, "", false},
		{"go ahead approves", "go ahead", def, "go ahead", true},
		{"lgtm with bang approves", "LGTM!", def, "lgtm", true},
		{"padded approved approves", "  approved.  ", def, "approved", true},
		{"bold lgtm approves", "**LGTM**", def, "lgtm", true},
		{"bold lgtm with period approves", "**LGTM**.", def, "lgtm", true},
		{"backticked lgtm approves", "`lgtm`", def, "lgtm", true},
		{"underscored approved approves", "_approved_", def, "approved", true},
		{"lgtm with emoji shortcode approves", "LGTM :rocket:", def, "lgtm", true},
		{"lgtm with unicode emoji approves", "LGTM \U0001F680", def, "lgtm", true},
		{"phrase on its own line among prose approves", "thanks for the writeup\n\nlgtm\n", def, "lgtm", true},
		{"phrase inside a fenced code block does not approve", "here is the marker:\n```\nlgtm\n```\n", def, "", false},
		{"phrase in a quoted line does not approve", "> lgtm\n\nnot yet, see above", def, "", false},
		{"phrase with a trailing clause does not approve", "lgtm, but fix the tests first", def, "", false},
		{"ship it approves", "Ship it.", def, "ship it", true},
		{"empty body does not approve", "", def, "", false},
		{"regex metacharacters in a phrase are quoted, not interpolated", "a+b", []string{"a+b"}, "a+b", true},
		{"a quoted metacharacter phrase does not match its regex expansion", "aab", []string{"a+b"}, "", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := MatchesApprovalPhrase(tc.body, tc.phrases)
			if ok != tc.ok || got != tc.want {
				t.Fatalf("MatchesApprovalPhrase(%q) = (%q, %v), want (%q, %v)", tc.body, got, ok, tc.want, tc.ok)
			}
		})
	}
}

// approvalProject builds the C.6 project: a bot login, a maintainer set, and
// the default (closed) approval wordlist.
func approvalProject(maintainers ...string) *tatarav1alpha1.Project {
	p := mirrorProject("tatara-bot")
	p.Spec.Scm.MaintainerLogins = maintainers
	return p
}

// approvalIssue builds a live (state=open) Issue CR owned by a Task.
func approvalIssue(repo string, number int, comments ...tatarav1alpha1.Comment) *tatarav1alpha1.Issue {
	return &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: tatarav1alpha1.IssueName(repo, number), Namespace: testNS},
		Spec: tatarav1alpha1.IssueSpec{
			RepositoryRef: repo, Number: number, ProjectRef: "proj",
			URL: "https://github.com/szymonrychu/tatara-operator/issues/1",
		},
		Status: tatarav1alpha1.IssueStatus{State: "open", Status: "new", Comments: comments},
	}
}

// approvalComment is one mirrored thread comment. isBot mirrors the STRUCTURAL
// exclusion the mirror computes from Project.spec.scm.botLogin.
func approvalComment(id, author, body string, at time.Time, isBot bool) tatarav1alpha1.Comment {
	return tatarav1alpha1.Comment{
		ExternalID: id, Author: author, Body: body,
		CreatedAt: metav1.NewTime(at.UTC().Truncate(time.Second)), IsBot: isBot,
	}
}

func approvalTask(name string, issueRefs ...string) *tatarav1alpha1.Task {
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{Kind: "clarify", ProjectRef: "proj"},
		Status: tatarav1alpha1.TaskStatus{
			Stage:     tatarav1alpha1.StageClarifying,
			IssueRefs: issueRefs,
		},
	}
}

func getTaskCR(t *testing.T, c client.Client, name string) *tatarav1alpha1.Task {
	t.Helper()
	var task tatarav1alpha1.Task
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: testNS, Name: name}, &task); err != nil {
		t.Fatalf("get task %s: %v", name, err)
	}
	return &task
}

// TestVerifyApprovalScopeIsEveryLiveIssue is fix H9: clarifying -> approved
// never said WHICH owned Issue was approved, so one "lgtm" on one issue
// approved a Task spanning every repo in mergeOrder. EVERY live issue needs its
// own evidence.
func TestVerifyApprovalScopeIsEveryLiveIssue(t *testing.T) {
	ctx := context.Background()
	proj, repo := approvalProject("szymonrychu"), mirrorRepo()
	now := time.Now()

	i1 := approvalIssue(repo.Name, 1, approvalComment("c1", "szymonrychu", "lgtm", now, false))
	i2 := approvalIssue(repo.Name, 2)
	i3 := approvalIssue(repo.Name, 3)
	task := approvalTask("t-scope", i1.Name, i2.Name, i3.Name)
	c := newMirrorClient(t, proj, repo, i1, i2, i3, task)

	ev, err := VerifyApproval(ctx, c, &mirrorSpiller{}, proj, task)
	if err != nil {
		t.Fatalf("VerifyApproval: %v", err)
	}
	if len(ev) != 3 {
		t.Fatalf("evidence entries = %d, want 3 (one per LIVE issue)", len(ev))
	}
	if ev[i1.Name] == nil {
		t.Fatal("the issue carrying lgtm has no evidence")
	}
	if ev[i2.Name] != nil || ev[i3.Name] != nil {
		t.Fatal("an issue with NO maintainer phrase produced evidence")
	}
	if ApprovalPassed(ev) {
		t.Fatal("one lgtm on one of three issues passed the gate")
	}
	if got := getTaskCR(t, c, task.Name).Status.Stage; got != tatarav1alpha1.StageClarifying {
		t.Fatalf("task stage = %q, want clarifying (the gate did NOT pass)", got)
	}
	if got := getIssueCR(t, c, i2.Name).Status.Status; got == "approved" {
		t.Fatal("an issue with no evidence was marked approved")
	}

	// Now approve the other two: the Task reaches approved.
	for _, name := range []string{i2.Name, i3.Name} {
		iss := getIssueCR(t, c, name)
		iss.Status.Comments = []tatarav1alpha1.Comment{approvalComment("c9", "szymonrychu", "go ahead", now, false)}
		if err := c.Status().Update(ctx, iss); err != nil {
			t.Fatalf("seed comment on %s: %v", name, err)
		}
	}
	task = getTaskCR(t, c, task.Name)
	ev, err = VerifyApproval(ctx, c, &mirrorSpiller{}, proj, task)
	if err != nil {
		t.Fatalf("VerifyApproval (2): %v", err)
	}
	if !ApprovalPassed(ev) {
		t.Fatalf("every live issue carries evidence but the gate did not pass: %+v", ev)
	}
	if got := getTaskCR(t, c, task.Name).Status.Stage; got != tatarav1alpha1.StageApproved {
		t.Fatalf("task stage = %q, want approved", got)
	}
	for _, name := range []string{i1.Name, i2.Name, i3.Name} {
		if got := getIssueCR(t, c, name).Status.Status; got != "approved" {
			t.Fatalf("issue %s status = %q, want approved", name, got)
		}
	}
}

// TestVerifyApprovalClosedIssueIsOutOfScope: a human closing ONE issue of a
// multi-issue Task must not make approval require a phrase on a CLOSED thread,
// forever (fix L3-14).
func TestVerifyApprovalClosedIssueIsOutOfScope(t *testing.T) {
	ctx := context.Background()
	proj, repo := approvalProject("szymonrychu"), mirrorRepo()
	now := time.Now()

	i1 := approvalIssue(repo.Name, 1, approvalComment("c1", "szymonrychu", "lgtm", now, false))
	i2 := approvalIssue(repo.Name, 2, approvalComment("c2", "szymonrychu", "lgtm", now, false))
	closed := approvalIssue(repo.Name, 3)
	closed.Status.State = "closed"
	done := approvalIssue(repo.Name, 4)
	done.Status.Status = "done"
	task := approvalTask("t-closed", i1.Name, i2.Name, closed.Name, done.Name)
	c := newMirrorClient(t, proj, repo, i1, i2, closed, done, task)

	ev, err := VerifyApproval(ctx, c, &mirrorSpiller{}, proj, task)
	if err != nil {
		t.Fatalf("VerifyApproval: %v", err)
	}
	if len(ev) != 2 {
		t.Fatalf("evidence entries = %d, want 2 (closed and done issues are OUT of scope)", len(ev))
	}
	if !ApprovalPassed(ev) {
		t.Fatal("the two LIVE issues are approved; a closed issue must not hold the gate shut forever")
	}
	if got := getTaskCR(t, c, task.Name).Status.Stage; got != tatarav1alpha1.StageApproved {
		t.Fatalf("task stage = %q, want approved", got)
	}
}

// TestVerifyApprovalBotCannotApprove: the bot is excluded STRUCTURALLY, before
// IsMaintainer runs, so a bot misconfigured into maintainerLogins still cannot
// approve. The operator's own park comment must never un-park the Task it parked.
func TestVerifyApprovalBotCannotApprove(t *testing.T) {
	ctx := context.Background()
	proj, repo := approvalProject("szymonrychu", "tatara-bot"), mirrorRepo()

	i1 := approvalIssue(repo.Name, 1, approvalComment("c1", "tatara-bot", "lgtm", time.Now(), true))
	task := approvalTask("t-bot", i1.Name)
	c := newMirrorClient(t, proj, repo, i1, task)

	ev, refusals, err := VerifyApprovalDetailed(ctx, c, &mirrorSpiller{}, proj, task)
	if err != nil {
		t.Fatalf("VerifyApprovalDetailed: %v", err)
	}
	if ApprovalPassed(ev) {
		t.Fatal("the BOT approved its own work")
	}
	if refusals[i1.Name] != ApprovalRefusedNoMaintainer {
		t.Fatalf("refusal = %q, want %q", refusals[i1.Name], ApprovalRefusedNoMaintainer)
	}
	if got := getTaskCR(t, c, task.Name).Status.Stage; got != tatarav1alpha1.StageClarifying {
		t.Fatalf("task stage = %q, want clarifying", got)
	}
}

// TestVerifyApprovalMostRecentMaintainerCommentGoverns is clause (b): an older
// approving maintainer comment behind a NEWER non-approving maintainer comment
// does NOT approve. The maintainer's corrective follow-up is the one that counts.
func TestVerifyApprovalMostRecentMaintainerCommentGoverns(t *testing.T) {
	ctx := context.Background()
	proj, repo := approvalProject("szymonrychu"), mirrorRepo()
	now := time.Now()

	i1 := approvalIssue(repo.Name, 1,
		approvalComment("c1", "szymonrychu", "lgtm", now.Add(-2*time.Hour), false),
		approvalComment("c2", "rando", "go ahead", now.Add(-time.Hour), false),
		approvalComment("c3", "szymonrychu", "actually hold on, this breaks the reaper", now, false),
	)
	task := approvalTask("t-recent", i1.Name)
	c := newMirrorClient(t, proj, repo, i1, task)

	ev, refusals, err := VerifyApprovalDetailed(ctx, c, &mirrorSpiller{}, proj, task)
	if err != nil {
		t.Fatalf("VerifyApprovalDetailed: %v", err)
	}
	if ApprovalPassed(ev) {
		t.Fatal("an older lgtm approved despite a NEWER non-approving maintainer comment")
	}
	if refusals[i1.Name] != ApprovalRefusedNoPhrase {
		t.Fatalf("refusal = %q, want %q", refusals[i1.Name], ApprovalRefusedNoPhrase)
	}
}

// TestVerifyApprovalNonMaintainerCannotApprove: closed-by-default. A phrase from
// a non-maintainer is not consent.
func TestVerifyApprovalNonMaintainerCannotApprove(t *testing.T) {
	ctx := context.Background()
	proj, repo := approvalProject("szymonrychu"), mirrorRepo()

	i1 := approvalIssue(repo.Name, 1, approvalComment("c1", "rando", "lgtm", time.Now(), false))
	task := approvalTask("t-rando", i1.Name)
	c := newMirrorClient(t, proj, repo, i1, task)

	ev, err := VerifyApproval(ctx, c, &mirrorSpiller{}, proj, task)
	if err != nil {
		t.Fatalf("VerifyApproval: %v", err)
	}
	if ApprovalPassed(ev) {
		t.Fatal("a non-maintainer approved")
	}
}

// TestVerifyApprovalSingleUseEvidence is clause (d): a REPLAYED evidence
// commentId cannot approve twice. A consumed comment is consumed.
func TestVerifyApprovalSingleUseEvidence(t *testing.T) {
	ctx := context.Background()
	proj, repo := approvalProject("szymonrychu"), mirrorRepo()
	now := time.Now().UTC().Truncate(time.Second)

	i1 := approvalIssue(repo.Name, 1, approvalComment("c1", "szymonrychu", "lgtm", now, false))
	// The comment was already consumed, and the Issue was subsequently reset out
	// of approved (a re-clarify). The SAME comment must not approve it again.
	i1.Status.Status = "new"
	i1.Status.Approval = &tatarav1alpha1.ApprovalEvidence{
		Login: "szymonrychu", CommentID: "c1", CreatedAt: metav1.NewTime(now), Phrase: "lgtm",
	}
	task := approvalTask("t-replay", i1.Name)
	c := newMirrorClient(t, proj, repo, i1, task)

	ev, refusals, err := VerifyApprovalDetailed(ctx, c, &mirrorSpiller{}, proj, task)
	if err != nil {
		t.Fatalf("VerifyApprovalDetailed: %v", err)
	}
	if ApprovalPassed(ev) {
		t.Fatal("a REPLAYED evidence commentId re-approved")
	}
	if refusals[i1.Name] != ApprovalRefusedEvidenceReplayed {
		t.Fatalf("refusal = %q, want %q", refusals[i1.Name], ApprovalRefusedEvidenceReplayed)
	}

	// A NEWER maintainer comment approves.
	iss := getIssueCR(t, c, i1.Name)
	iss.Status.Comments = append(iss.Status.Comments,
		approvalComment("c2", "szymonrychu", "go ahead", now.Add(time.Hour), false))
	if err := c.Status().Update(ctx, iss); err != nil {
		t.Fatalf("seed newer comment: %v", err)
	}
	ev, err = VerifyApproval(ctx, c, &mirrorSpiller{}, proj, getTaskCR(t, c, task.Name))
	if err != nil {
		t.Fatalf("VerifyApproval (2): %v", err)
	}
	if !ApprovalPassed(ev) {
		t.Fatal("a NEW maintainer comment failed to approve")
	}
	got := getIssueCR(t, c, i1.Name).Status.Approval
	if got == nil || got.CommentID != "c2" || got.Phrase != "go ahead" || got.Login != "szymonrychu" {
		t.Fatalf("evidence = %+v, want {login: szymonrychu, commentId: c2, phrase: go ahead}", got)
	}
}

// TestVerifyApprovalAutoEvidenceSurvivesAReRun: autoApproveTataraProposals is
// the ONLY other path into approved, and it writes ApprovalEvidence{Auto: true,
// Login: "<tatara:auto>", CommentID: ""} - evidence with NO comment to re-match.
// A re-run of the grammar must not refuse it and bounce the Task out of approved.
func TestVerifyApprovalAutoEvidenceSurvivesAReRun(t *testing.T) {
	ctx := context.Background()
	proj, repo := approvalProject("szymonrychu"), mirrorRepo()

	i1 := approvalIssue(repo.Name, 1)
	i1.Status.Status = "approved"
	i1.Status.Approval = &tatarav1alpha1.ApprovalEvidence{
		Auto: true, Login: "<tatara:auto>", CommentID: "", CreatedAt: metav1.Now(),
	}
	task := approvalTask("t-auto", i1.Name)
	c := newMirrorClient(t, proj, repo, i1, task)

	ev, err := VerifyApproval(ctx, c, &mirrorSpiller{}, proj, task)
	if err != nil {
		t.Fatalf("VerifyApproval: %v", err)
	}
	if !ApprovalPassed(ev) {
		t.Fatal("an AUTO-approved issue was refused on re-verification")
	}
	if got := ev[i1.Name]; got == nil || !got.Auto || got.Login != "<tatara:auto>" {
		t.Fatalf("evidence = %+v, want the auto evidence preserved", got)
	}
	if got := getTaskCR(t, c, task.Name).Status.Stage; got != tatarav1alpha1.StageApproved {
		t.Fatalf("task stage = %q, want approved", got)
	}
}

// TestVerifyApprovalIsNotRevokedByLaterChat: approval is un-stuck by ACQUIRING an
// Issue (clause 2), never by a maintainer's later chatter. A "thanks!" after an
// "lgtm" must not revoke the approval it already granted.
func TestVerifyApprovalIsNotRevokedByLaterChat(t *testing.T) {
	ctx := context.Background()
	proj, repo := approvalProject("szymonrychu"), mirrorRepo()
	now := time.Now()

	i1 := approvalIssue(repo.Name, 1, approvalComment("c1", "szymonrychu", "lgtm", now, false))
	task := approvalTask("t-chat", i1.Name)
	c := newMirrorClient(t, proj, repo, i1, task)

	if ev, err := VerifyApproval(ctx, c, &mirrorSpiller{}, proj, task); err != nil || !ApprovalPassed(ev) {
		t.Fatalf("lgtm did not approve: ev=%+v err=%v", ev, err)
	}

	iss := getIssueCR(t, c, i1.Name)
	iss.Status.Comments = append(iss.Status.Comments,
		approvalComment("c2", "szymonrychu", "thanks!", now.Add(time.Hour), false))
	if err := c.Status().Update(ctx, iss); err != nil {
		t.Fatalf("seed later chat: %v", err)
	}

	ev, err := VerifyApproval(ctx, c, &mirrorSpiller{}, proj, getTaskCR(t, c, task.Name))
	if err != nil {
		t.Fatalf("VerifyApproval (2): %v", err)
	}
	if !ApprovalPassed(ev) {
		t.Fatal("a later non-approving maintainer comment REVOKED an approval already granted")
	}
	if got := getTaskCR(t, c, task.Name).Status.Stage; got != tatarav1alpha1.StageApproved {
		t.Fatalf("task stage = %q, want approved", got)
	}
}

// TestVerifyApprovalIsNotSticky (fix H9): a Task that ACQUIRES a new Issue after
// approval goes back to clarifying, because clause (2) no longer holds. An agent
// cannot widen its own mandate by adopting work after the gate.
func TestVerifyApprovalIsNotSticky(t *testing.T) {
	ctx := context.Background()
	proj, repo := approvalProject("szymonrychu"), mirrorRepo()
	now := time.Now()

	issues := []*tatarav1alpha1.Issue{
		approvalIssue(repo.Name, 1, approvalComment("c1", "szymonrychu", "lgtm", now, false)),
		approvalIssue(repo.Name, 2, approvalComment("c2", "szymonrychu", "lgtm", now, false)),
		approvalIssue(repo.Name, 3, approvalComment("c3", "szymonrychu", "lgtm", now, false)),
	}
	task := approvalTask("t-sticky", issues[0].Name, issues[1].Name, issues[2].Name)
	objs := []client.Object{proj, repo, task}
	for _, i := range issues {
		objs = append(objs, i)
	}
	c := newMirrorClient(t, objs...)

	if ev, err := VerifyApproval(ctx, c, &mirrorSpiller{}, proj, task); err != nil || !ApprovalPassed(ev) {
		t.Fatalf("three approved issues did not pass the gate: ev=%+v err=%v", ev, err)
	}
	if got := getTaskCR(t, c, task.Name).Status.Stage; got != tatarav1alpha1.StageApproved {
		t.Fatalf("task stage = %q, want approved", got)
	}

	// The agent adopts a FOURTH issue after the gate.
	i4 := approvalIssue(repo.Name, 4)
	if err := c.Create(ctx, i4); err != nil {
		t.Fatalf("create issue 4: %v", err)
	}
	task = getTaskCR(t, c, task.Name)
	task.Status.IssueRefs = append(task.Status.IssueRefs, i4.Name)
	if err := c.Status().Update(ctx, task); err != nil {
		t.Fatalf("acquire issue 4: %v", err)
	}

	ev, err := VerifyApproval(ctx, c, &mirrorSpiller{}, proj, getTaskCR(t, c, task.Name))
	if err != nil {
		t.Fatalf("VerifyApproval (2): %v", err)
	}
	if ApprovalPassed(ev) {
		t.Fatal("an UNAPPROVED fourth issue still passed the gate")
	}
	if got := getTaskCR(t, c, task.Name).Status.Stage; got != tatarav1alpha1.StageClarifying {
		t.Fatalf("task stage = %q, want clarifying (approval is NOT sticky)", got)
	}
}

// TestVerifyApprovalEmptySetIsNotALicence: a Task owning ZERO live Issues does
// not pass the gate. all([]) == true must never gate code execution.
func TestVerifyApprovalEmptySetIsNotALicence(t *testing.T) {
	ctx := context.Background()
	proj, repo := approvalProject("szymonrychu"), mirrorRepo()
	task := approvalTask("t-empty")
	c := newMirrorClient(t, proj, repo, task)

	ev, err := VerifyApproval(ctx, c, &mirrorSpiller{}, proj, task)
	if err != nil {
		t.Fatalf("VerifyApproval: %v", err)
	}
	if ApprovalPassed(ev) {
		t.Fatal("a Task owning ZERO live Issues passed the approval gate")
	}
	if got := getTaskCR(t, c, task.Name).Status.Stage; got != tatarav1alpha1.StageClarifying {
		t.Fatalf("task stage = %q, want clarifying", got)
	}
}

// TestVerifyApprovalRefusalNamesWhatWasMissing: the park comment the operator
// posts on a refusal names what was missing. It is BOT-authored, so it can never
// un-park the Task (E.3 enqueue filter + F.6).
func TestVerifyApprovalRefusalNamesWhatWasMissing(t *testing.T) {
	phrases := tatarav1alpha1.DefaultApprovalPhrases()
	for _, reason := range []string{
		ApprovalRefusedNoMaintainer, ApprovalRefusedNoPhrase, ApprovalRefusedEvidenceReplayed,
	} {
		msg := ApprovalRefusedComment(reason, phrases)
		if msg == "" {
			t.Fatalf("refusal %q rendered an EMPTY comment", reason)
		}
		if !containsAll(msg, "lgtm", "go ahead") {
			t.Fatalf("refusal comment does not name the accepted phrases: %q", msg)
		}
	}
}

func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		found := false
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// approvalEvent builds the non-bot pendingEvent that re-drives the grammar.
func approvalEvent(repo string, number int, author, body string) tatarav1alpha1.TaskEvent {
	return tatarav1alpha1.TaskEvent{
		At: metav1.Now(), Kind: "issue_comment", Repo: repo, Number: number, Author: author, Body: body,
	}
}

// parkedTask is a Task parked at identity-unverified with the non-bot event that
// triggers the C3-3 re-verification.
func parkedTask(name string, ev tatarav1alpha1.TaskEvent, issueRefs ...string) *tatarav1alpha1.Task {
	t := approvalTask(name, issueRefs...)
	t.Status.Stage = tatarav1alpha1.StageParked
	t.Status.StageReason = stage.ReasonIdentityUnverified
	t.Status.PendingEvents = []tatarav1alpha1.TaskEvent{ev}
	return t
}

// TestReVerifyParkedSyncsTheThreadFirst is the C3-3 path, and the ordering is
// mandatory. A Task parked at identity-unverified, given a maintainer comment
// "go ahead", reaches implementing in ONE comment - not two comments and 7 days.
//
// The mirror here is DELIBERATELY ONE DAY STALE (the parked cadence is DAILY):
// without the on-demand sync the grammar re-runs against a thread that does not
// contain the comment that triggered it, clause (d) has no ExternalID to check
// against, and the re-verification silently fails.
func TestReVerifyParkedSyncsTheThreadFirst(t *testing.T) {
	ctx := context.Background()
	proj, repo := approvalProject("szymonrychu"), mirrorRepo()
	dayOld := metav1.NewTime(time.Now().Add(-25 * time.Hour).UTC().Truncate(time.Second))

	i1 := approvalIssue(repo.Name, 291,
		approvalComment("b1", "tatara-bot", "tatara: I cannot start work on this yet", dayOld.Time, true))
	i1.Status.LastSyncedAt = &dayOld
	ev := approvalEvent(repo.Name, 291, "szymonrychu", "go ahead")
	task := parkedTask("t-reverify", ev, i1.Name)
	c := newMirrorClient(t, proj, repo, i1, task)

	// The FORGE has the approving comment; the stale mirror does not.
	rd := &mirrorReader{comments: []scm.IssueComment{
		{ExternalID: "b1", Author: "tatara-bot", Body: "tatara: I cannot start work on this yet", CreatedAt: dayOld.Time},
		{ExternalID: "c9", Author: "szymonrychu", Body: "go ahead", CreatedAt: time.Now().UTC().Truncate(time.Second)},
	}}

	passed, err := ReVerifyParked(ctx, c, &mirrorSpiller{}, rd, proj, task, ev)
	if err != nil {
		t.Fatalf("ReVerifyParked: %v", err)
	}
	if rd.calls != 1 {
		t.Fatalf("forge reads = %d, want EXACTLY 1 (sync that issue's thread, once)", rd.calls)
	}
	if !passed {
		t.Fatal("a maintainer 'go ahead' on a parked Task did not pass the re-verified grammar")
	}
	iss := getIssueCR(t, c, i1.Name)
	if iss.Status.Status != "approved" {
		t.Fatalf("issue status = %q, want approved", iss.Status.Status)
	}
	if iss.Status.Approval == nil || iss.Status.Approval.CommentID != "c9" {
		t.Fatalf("evidence = %+v, want the SYNCED comment c9 (its ExternalID did not exist in the stale mirror)", iss.Status.Approval)
	}

	// ONE comment reaches implementing: feed the grammar verdict into F.6.
	fresh := getTaskCR(t, c, task.Name)
	target, ok := stage.Unpark(stage.UnparkInput{
		Task:          fresh,
		Issues:        []tatarav1alpha1.Issue{*iss},
		BotLogin:      "tatara-bot",
		GrammarPassed: passed,
		MaxOpenTasks:  10,
		Now:           time.Now(),
	})
	if !ok || target != tatarav1alpha1.StageImplementing {
		t.Fatalf("Unpark = (%q, %v), want (implementing, true)", target, ok)
	}
}

// TestReVerifyParkedRefusesANonApprovingComment: "not yet" keeps the Task parked.
func TestReVerifyParkedRefusesANonApprovingComment(t *testing.T) {
	ctx := context.Background()
	proj, repo := approvalProject("szymonrychu"), mirrorRepo()

	i1 := approvalIssue(repo.Name, 291)
	ev := approvalEvent(repo.Name, 291, "szymonrychu", "not yet")
	task := parkedTask("t-notyet", ev, i1.Name)
	c := newMirrorClient(t, proj, repo, i1, task)

	rd := &mirrorReader{comments: []scm.IssueComment{
		{ExternalID: "c9", Author: "szymonrychu", Body: "not yet", CreatedAt: time.Now()},
	}}
	passed, err := ReVerifyParked(ctx, c, &mirrorSpiller{}, rd, proj, task, ev)
	if err != nil {
		t.Fatalf("ReVerifyParked: %v", err)
	}
	if passed {
		t.Fatal("'not yet' passed the approval grammar")
	}
	fresh := getTaskCR(t, c, task.Name)
	iss := getIssueCR(t, c, i1.Name)
	if _, ok := stage.Unpark(stage.UnparkInput{
		Task: fresh, Issues: []tatarav1alpha1.Issue{*iss}, BotLogin: "tatara-bot",
		GrammarPassed: passed, MaxOpenTasks: 10, Now: time.Now(),
	}); ok {
		t.Fatal("a refused grammar un-parked the Task")
	}
	if fresh.Status.Stage != tatarav1alpha1.StageParked {
		t.Fatalf("task stage = %q, want parked", fresh.Status.Stage)
	}
}

// TestReVerifyParkedIgnoresABotEvent: the operator's own park comment is
// bot-authored. It must not even cost a forge read.
func TestReVerifyParkedIgnoresABotEvent(t *testing.T) {
	ctx := context.Background()
	proj, repo := approvalProject("szymonrychu"), mirrorRepo()

	i1 := approvalIssue(repo.Name, 291)
	ev := approvalEvent(repo.Name, 291, "tatara-bot", "lgtm")
	task := parkedTask("t-botevent", ev, i1.Name)
	c := newMirrorClient(t, proj, repo, i1, task)

	rd := &mirrorReader{}
	passed, err := ReVerifyParked(ctx, c, &mirrorSpiller{}, rd, proj, task, ev)
	if err != nil {
		t.Fatalf("ReVerifyParked: %v", err)
	}
	if passed {
		t.Fatal("a BOT comment passed the approval grammar")
	}
	if rd.calls != 0 {
		t.Fatalf("forge reads = %d, want 0 (a bot event is never re-verified)", rd.calls)
	}
	if getIssueCR(t, c, i1.Name).Status.Status == "approved" {
		t.Fatal("a BOT comment approved an issue")
	}
}
