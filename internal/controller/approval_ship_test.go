package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// THIS FILE PINS THE SECOND HOLE #639 NAMED. The gate could be skipped
// altogether: mrOpen read head-branch, idempotency and taskOwesOpenWork, and
// submit_outcome(action=submitted) read only the plan hash. Neither ever looked
// at Issue.status.approval, so an implement agent that never called the gate at
// all still opened the PR and still shipped. ApprovalShipVerdict is the read
// they both now do.

func shipIssue(repoRef, author string, number int) *tatarav1alpha1.Issue {
	iss := autoProposalIssue(repoRef, author, tatarav1alpha1.ProposalKindBrainstorm, number)
	return iss
}

func shipComment(author string, bot bool) tatarav1alpha1.Comment {
	return tatarav1alpha1.Comment{
		ExternalID: "c1", Author: author, Body: "go ahead", IsBot: bot,
		CreatedAt: metav1.NewTime(time.Now()),
	}
}

func shipClient(t *testing.T, repo *tatarav1alpha1.Repository) client.Client {
	t.Helper()
	return newMirrorClient(t, repo)
}

// TestApprovalShipVerdict_NoEvidenceNoComment is the #639 primary case: an
// implement agent that skipped the gate on a thread nobody has spoken on.
func TestApprovalShipVerdict_NoEvidenceNoComment(t *testing.T) {
	proj, repo := approvalProject("szymonrychu"), mirrorRepo()
	iss := shipIssue(repo.Name, "tatara-bot", 7)

	got := ApprovalShipVerdict(context.Background(), shipClient(t, repo), proj,
		[]tatarav1alpha1.Issue{*iss}, "")

	if len(got) != 1 {
		t.Fatalf("want exactly one blocker, got %v", got)
	}
	if got[0].Detail != ShipBlockedNeedsMaintainerComment {
		t.Fatalf("detail = %q, want %q", got[0].Detail, ShipBlockedNeedsMaintainerComment)
	}
	if got[0].Repo != repo.Name || got[0].Number != 7 {
		t.Fatalf("blocker must name the issue it is about, got %+v", got[0])
	}
}

// TestApprovalShipVerdict_MaintainerSpokeButGateUnused is the SECOND detail, and
// the reason there are two: the remedies differ. Nobody has commented -> go ask.
// Somebody HAS -> the agent has everything it needs and simply has not called
// submit_outcome(action=approved).
func TestApprovalShipVerdict_MaintainerSpokeButGateUnused(t *testing.T) {
	proj, repo := approvalProject("szymonrychu"), mirrorRepo()
	iss := shipIssue(repo.Name, "tatara-bot", 7)
	iss.Status.Comments = []tatarav1alpha1.Comment{shipComment("szymonrychu", false)}

	got := ApprovalShipVerdict(context.Background(), shipClient(t, repo), proj,
		[]tatarav1alpha1.Issue{*iss}, "")

	if len(got) != 1 || got[0].Detail != ShipBlockedNeedsApprovalTool {
		t.Fatalf("want one %q blocker, got %v", ShipBlockedNeedsApprovalTool, got)
	}
}

// A BOT comment is not a maintainer comment. The operator posts its own plan
// echo and its own approval confirmation on the thread; if either counted, every
// bot-authored proposal would report needs-approval-tool the moment the operator
// spoke on it, which is a remedy the agent cannot act on.
func TestApprovalShipVerdict_BotCommentIsNotAMaintainerComment(t *testing.T) {
	proj, repo := approvalProject("szymonrychu"), mirrorRepo()
	iss := shipIssue(repo.Name, "tatara-bot", 7)
	iss.Status.Comments = []tatarav1alpha1.Comment{shipComment("tatara-bot", true)}

	got := ApprovalShipVerdict(context.Background(), shipClient(t, repo), proj,
		[]tatarav1alpha1.Issue{*iss}, "")

	if len(got) != 1 || got[0].Detail != ShipBlockedNeedsMaintainerComment {
		t.Fatalf("want one %q blocker, got %v", ShipBlockedNeedsMaintainerComment, got)
	}
}

// TestApprovalShipVerdict_HumanCitedEvidenceShips: the gate granted on a real
// citation, so nothing blocks - AT ANY SEVERITY. A human who said go ahead is
// never severity-limited; the ceiling exists to bound what tatara approves for
// ITSELF.
func TestApprovalShipVerdict_HumanCitedEvidenceShips(t *testing.T) {
	proj, repo := approvalProject("szymonrychu"), mirrorRepo()
	proj.Spec.AutoApproveMaxSignificance = tatarav1alpha1.AutoApproveOff
	iss := shipIssue(repo.Name, "tatara-bot", 7)
	iss.Status.Comments = []tatarav1alpha1.Comment{shipComment("szymonrychu", false)}
	iss.Status.Status = "approved"
	iss.Status.Approval = &tatarav1alpha1.ApprovalEvidence{
		Login: "szymonrychu", CommentID: "c1", Phrase: "go ahead", CreatedAt: metav1.Now(),
	}

	if got := ApprovalShipVerdict(context.Background(), shipClient(t, repo), proj,
		[]tatarav1alpha1.Issue{*iss}, "major"); len(got) != 0 {
		t.Fatalf("a human-cited approval must ship at any significance, got %v", got)
	}
}

// TestApprovalShipVerdict_AutoEvidenceAgainstTheCeiling is the severity rule.
func TestApprovalShipVerdict_AutoEvidenceAgainstTheCeiling(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ceiling string
		sig     string
		blocked bool
	}{
		{"minor ceiling passes patch", "minor", "patch", false},
		{"minor ceiling passes minor", "minor", "minor", false},
		{"minor ceiling refuses major", "minor", "major", true},
		{"major ceiling passes major", "major", "major", false},
		{"ceiling lowered to off after the grant refuses", tatarav1alpha1.AutoApproveOff, "patch", true},
		{"mr_write(open) declares no significance and is never ceiling-blocked", "minor", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proj, repo := approvalProject("szymonrychu"), mirrorRepo()
			proj.Spec.AutoApproveMaxSignificance = tc.ceiling
			iss := shipIssue(repo.Name, "tatara-bot", 7)
			iss.Status.Status = "approved"
			iss.Status.Approval = &tatarav1alpha1.ApprovalEvidence{
				Auto: true, Login: tatarav1alpha1.AutoApproveLogin, CreatedAt: metav1.Now(),
			}

			got := ApprovalShipVerdict(context.Background(), shipClient(t, repo), proj,
				[]tatarav1alpha1.Issue{*iss}, tc.sig)

			if tc.blocked {
				if len(got) != 1 || got[0].Detail != ShipBlockedOverCeiling {
					t.Fatalf("want one %q blocker, got %v", ShipBlockedOverCeiling, got)
				}
				return
			}
			if len(got) != 0 {
				t.Fatalf("want no blocker, got %v", got)
			}
		})
	}
}

// TestApprovalShipVerdict_OutOfScopeIssuesAreNotBlockers keeps the gate's own
// scope filter: a human closing one Issue of a multi-issue Task must not strand
// the rest, and a Task whose Issues are ALL closed is ungated here rather than
// permanently blocked. The gate itself (verifyApprovalScope) already refuses an
// approval over an empty live set; this function's job is the SHIP, not the
// grant.
func TestApprovalShipVerdict_OutOfScopeIssuesAreNotBlockers(t *testing.T) {
	proj, repo := approvalProject("szymonrychu"), mirrorRepo()
	closed := shipIssue(repo.Name, "tatara-bot", 7)
	closed.Status.State = "closed"
	rejected := shipIssue(repo.Name, "tatara-bot", 8)
	rejected.Status.Status = "rejected"

	if got := ApprovalShipVerdict(context.Background(), shipClient(t, repo), proj,
		[]tatarav1alpha1.Issue{*closed, *rejected}, "patch"); len(got) != 0 {
		t.Fatalf("out-of-scope issues must produce no blocker, got %v", got)
	}
}

// TestApprovalShipVerdict_ZeroLiveIssuesIsUngated is the takeover and
// adopted-upgrade carve-out, stated as a test so it cannot be closed by
// accident. A takeover Task owns NO Issue and was authorised at the takeover
// endpoint; an adopted upgrade Task owns an MR and no Issue. Blocking either on
// "no issue carries approval evidence" would wedge both permanently.
func TestApprovalShipVerdict_ZeroLiveIssuesIsUngated(t *testing.T) {
	proj, repo := approvalProject("szymonrychu"), mirrorRepo()
	if got := ApprovalShipVerdict(context.Background(), shipClient(t, repo), proj, nil, "major"); len(got) != 0 {
		t.Fatalf("a Task owning no Issue must be ungated, got %v", got)
	}
}

// TestApprovalShipVerdict_OneBlockerPerLiveIssue: the refusal names EVERY issue
// that is holding the ship, not the first. An agent told about one of three
// fixes one and is refused again.
func TestApprovalShipVerdict_OneBlockerPerLiveIssue(t *testing.T) {
	proj, repo := approvalProject("szymonrychu"), mirrorRepo()
	a := shipIssue(repo.Name, "tatara-bot", 7)
	b := shipIssue(repo.Name, "tatara-bot", 8)
	b.Status.Comments = []tatarav1alpha1.Comment{shipComment("szymonrychu", false)}

	got := ApprovalShipVerdict(context.Background(), shipClient(t, repo), proj,
		[]tatarav1alpha1.Issue{*a, *b}, "")

	if len(got) != 2 {
		t.Fatalf("want a blocker per live issue, got %v", got)
	}
	if got[0].Detail != ShipBlockedNeedsMaintainerComment || got[1].Detail != ShipBlockedNeedsApprovalTool {
		t.Fatalf("each issue must be judged on its own state, got %v", got)
	}
}

// TestShipBlockerGuidance_IsTotal pins the guidance map over the closed
// vocabulary. #639's ask is that EVERY tool output, confirming or denying,
// guides the agent through the process - a blocker with no guidance is the
// refusal that made an agent burn a turn guessing.
func TestShipBlockerGuidance_IsTotal(t *testing.T) {
	for _, d := range ShipBlockedDetails {
		if ShipBlockerGuidance(d) == "" {
			t.Fatalf("ship blocker %q has no guidance", d)
		}
	}
	if ShipBlockerGuidance("not-a-blocker") == "" {
		t.Fatal("an unknown detail must still produce guidance; a silent refusal is the defect")
	}
}

// TestApprovalRefusalGuidance_IsTotal is the same totality pin over the GATE's
// refusal vocabulary. #639: all tool outputs, confirming or denying, must guide
// the agent through this process - and a `granted:false` with a bare reason
// constant is the shape an agent has historically mis-read as "ask a human"
// when the remedy was in its own hands.
func TestApprovalRefusalGuidance_IsTotal(t *testing.T) {
	for _, r := range ApprovalRefusals {
		if ApprovalRefusalGuidance(r) == "" {
			t.Fatalf("approval refusal %q has no guidance", r)
		}
	}
	if ApprovalRefusalGuidance("citation-not-verified") == "" {
		t.Fatal("the scope check's fallback reason must carry guidance too; it is a reachable value")
	}
	if ApprovalRefusalGuidance("") == "" {
		t.Fatal("guidance must be total: an unknown reason still needs a next step")
	}
}

// TestApprovalGrantGuidance_NamesWhatUnblocks: the GRANT carries guidance too.
// It is the one answer that says "now go", and until #639 it said nothing at
// all - the grant returned a bare Task DTO with no `granted` key, though both
// the prompt and the skill tell the agent to read one.
func TestApprovalGrantGuidance_NamesWhatUnblocks(t *testing.T) {
	g := ApprovalGrantGuidance()
	for _, want := range []string{"mr_write(action=open)", "unblocked"} {
		if !strings.Contains(g, want) {
			t.Fatalf("grant guidance must mention %q, got %q", want, g)
		}
	}
}
