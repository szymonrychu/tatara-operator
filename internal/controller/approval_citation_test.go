// Copyright 2026 tatara authors.

package controller

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// cit is a one-element citation slice: the common shape.
func cit(id, quote string) []tatarav1alpha1.ApprovalCitation {
	return []tatarav1alpha1.ApprovalCitation{{ID: id, Quote: quote}}
}

func citIssue(comments ...tatarav1alpha1.Comment) *tatarav1alpha1.Issue {
	return &tatarav1alpha1.Issue{
		Status: tatarav1alpha1.IssueStatus{
			State: "open", Status: "new", Comments: comments,
		},
	}
}

func cmt(id, author, body string, bot bool, at time.Time) tatarav1alpha1.Comment {
	return tatarav1alpha1.Comment{
		ExternalID: id, Author: author, Body: body, IsBot: bot,
		CreatedAt: metav1.NewTime(at),
	}
}

// citProject carries autoApproveTataraProposals ON, so the carve-out row is a
// real carve-out and every other row proves the gate refuses DESPITE the flag.
func citProject(botLogin string, maintainers ...string) *tatarav1alpha1.Project {
	p := mirrorProject(botLogin)
	p.Spec.Scm.MaintainerLogins = maintainers
	p.Spec.AutoApproveTataraProposals = true
	return p
}

func citRepo() *tatarav1alpha1.Repository {
	return mirrorRepo()
}

// citAutoApprovableIssue is the shape autoApproveApplies grants on: open,
// bot-authored, carrying a valid provenance marker anchored to Spec.
func citAutoApprovableIssue(t *testing.T, botLogin string) *tatarav1alpha1.Issue {
	t.Helper()
	iss := citIssue()
	iss.Status.Author = botLogin
	iss.Status.Body = tatarav1alpha1.StampProposalMarker("do the proposed work",
		tatarav1alpha1.ProposalKindBrainstorm)
	iss.Spec.ProposalBodyHash = tatarav1alpha1.ComputeProposalContentHash(iss.Status.Body)
	return iss
}

// TestVerifyOneIssue_CitationFailClosedMatrix is the artefact that proves no
// citation shape reaches implementing without a real, non-bot maintainer
// comment. Every row that is not an explicit PASS must refuse FOR ITS OWN
// REASON - a row that refuses for a different reason proves nothing - and a
// refusal returns nil evidence, never a partial grant.
func TestVerifyOneIssue_CitationFailClosedMatrix(t *testing.T) {
	t0 := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)
	proj := citProject("bot-1", "maintainer-1")
	repo := citRepo()

	tests := []struct {
		name        string
		iss         *tatarav1alpha1.Issue
		citations   []tatarav1alpha1.ApprovalCitation
		maintainers []string // nil means the shared proj
		wantReason  string   // "" means PASS
		wantAuto    bool
	}{
		// --- PASS rows -----------------------------------------------------
		{
			name: "cited comment is a maintainer comment and the quote occurs in it",
			iss: citIssue(
				cmt("c-1", "randomer", "please do this", false, t0),
				cmt("c-2", "maintainer-1", "sure, go ahead, I approve!", false, t0.Add(time.Minute)),
			),
			citations:  cit("c-2", "go ahead, I approve!"),
			wantReason: "",
		},
		{
			// The AMENDMENT row. Requiring the NEWEST maintainer comment
			// deadlocks this exact thread: consent is unambiguous, but the
			// newest maintainer comment is not itself a go-ahead, so the agent
			// could cite nothing, would submit discuss every turn, and the Task
			// would park at awaiting-human forever with no signal.
			name: "cited comment is an EARLIER maintainer comment and a newer maintainer comment exists",
			iss: citIssue(
				cmt("c-1", "maintainer-1", "go ahead, I approve!", false, t0),
				cmt("c-2", "maintainer-1", "thanks - ping me when the PR is up", false, t0.Add(time.Minute)),
			),
			citations:  cit("c-1", "go ahead, I approve!"),
			wantReason: "",
		},
		{
			// PASSES AT THIS LAYER ON PURPOSE. The operator has no recency
			// clause, so it cannot and must not catch a withdrawal - "is this
			// later comment a withdrawal?" is an intent question, and intent is
			// the AGENT's side of the split. The agent reads the whole thread
			// and must submit discuss, not implement, here. That obligation is
			// covered by the SKILLS, not by this matrix. Do not "fix" this row
			// by adding an operator check.
			name: "cited comment is older and a NEWER maintainer comment withdraws it",
			iss: citIssue(
				cmt("c-1", "maintainer-1", "go ahead", false, t0),
				cmt("c-2", "maintainer-1", "actually, hold off", false, t0.Add(time.Minute)),
			),
			citations:  cit("c-1", "go ahead"),
			wantReason: "",
		},
		{
			name: "issue already approved with evidence short-circuits regardless of citation",
			iss: func() *tatarav1alpha1.Issue {
				i := citIssue(cmt("c-2", "maintainer-1", "ok", false, t0))
				i.Status.Status = "approved"
				i.Status.Approval = &tatarav1alpha1.ApprovalEvidence{Login: "maintainer-1", CommentID: "c-old"}
				return i
			}(),
			citations:  nil,
			wantReason: "",
		},
		{
			name:       "auto-approve carve-out: no maintainer comment, empty citation",
			iss:        citAutoApprovableIssue(t, "bot-1"),
			citations:  nil,
			wantReason: "",
			wantAuto:   true,
		},

		// --- REFUSE rows ---------------------------------------------------
		{
			name:       "no maintainer comment, no auto-approve, empty citation",
			iss:        citIssue(cmt("c-1", "randomer", "lgtm", false, t0)),
			citations:  nil,
			wantReason: ApprovalRefusedNoMaintainer,
		},
		{
			name: "auto-approve WOULD apply but a maintainer HAS commented and nothing is cited",
			iss: func() *tatarav1alpha1.Issue {
				i := citAutoApprovableIssue(t, "bot-1")
				i.Status.Comments = []tatarav1alpha1.Comment{cmt("c-2", "maintainer-1", "hold on", false, t0)}
				return i
			}(),
			citations:  nil,
			wantReason: ApprovalRefusedNoCitation,
		},
		{
			name:       "maintainer comment exists, citation slice empty",
			iss:        citIssue(cmt("c-2", "maintainer-1", "go ahead", false, t0)),
			citations:  nil,
			wantReason: ApprovalRefusedNoCitation,
		},
		{
			name: "cited comment is a NON-maintainer's",
			iss: citIssue(
				cmt("c-1", "randomer", "go ahead", false, t0),
				cmt("c-2", "maintainer-1", "let me look", false, t0.Add(time.Minute)),
			),
			citations:  cit("c-1", "go ahead"),
			wantReason: ApprovalRefusedCitationNotMaintainer,
		},
		{
			name: "cited comment is the BOT's own",
			iss: citIssue(
				cmt("c-1", "bot-1", "go ahead", true, t0),
				cmt("c-2", "maintainer-1", "let me look", false, t0.Add(time.Minute)),
			),
			citations:  cit("c-1", "go ahead"),
			wantReason: ApprovalRefusedCitationNotMaintainer,
		},
		{
			// The bot login is misconfigured INTO maintainerLogins. The
			// structural bot exclusion runs BEFORE IsMaintainer, so it still
			// cannot approve.
			name: "cited comment is the BOT's own and the bot is in maintainerLogins",
			iss: citIssue(
				cmt("c-1", "bot-1", "go ahead", true, t0),
				cmt("c-2", "maintainer-1", "let me look", false, t0.Add(time.Minute)),
			),
			citations:   cit("c-1", "go ahead"),
			maintainers: []string{"maintainer-1", "bot-1"},
			wantReason:  ApprovalRefusedCitationNotMaintainer,
		},
		{
			name:       "cited comment id does not exist on the issue at all",
			iss:        citIssue(cmt("c-2", "maintainer-1", "go ahead", false, t0)),
			citations:  cit("c-999", "go ahead"),
			wantReason: ApprovalRefusedCitationNotMaintainer,
		},
		{
			name:       "cited id is empty and the mirror's ExternalID is also empty",
			iss:        citIssue(cmt("", "maintainer-1", "go ahead", false, t0)),
			citations:  cit("", "go ahead"),
			wantReason: ApprovalRefusedCitationNotMaintainer,
		},
		{
			name:       "quote is absent from the cited comment's body",
			iss:        citIssue(cmt("c-2", "maintainer-1", "let me think about it", false, t0)),
			citations:  cit("c-2", "go ahead, I approve!"),
			wantReason: ApprovalRefusedQuoteAbsent,
		},
		{
			name: "quote is present but in a DIFFERENT comment",
			iss: citIssue(
				cmt("c-1", "randomer", "go ahead, I approve!", false, t0),
				cmt("c-2", "maintainer-1", "not yet", false, t0.Add(time.Minute)),
			),
			citations:  cit("c-2", "go ahead, I approve!"),
			wantReason: ApprovalRefusedQuoteAbsent,
		},
		{
			name:       "quote is the empty string (a trivially-matching substring)",
			iss:        citIssue(cmt("c-2", "maintainer-1", "go ahead", false, t0)),
			citations:  cit("c-2", ""),
			wantReason: ApprovalRefusedQuoteAbsent,
		},
		{
			name:       "quote is whitespace only",
			iss:        citIssue(cmt("c-2", "maintainer-1", "go ahead", false, t0)),
			citations:  cit("c-2", "   \n\t "),
			wantReason: ApprovalRefusedQuoteAbsent,
		},
		{
			name: "the cited comment was already consumed as evidence",
			iss: func() *tatarav1alpha1.Issue {
				i := citIssue(cmt("c-2", "maintainer-1", "go ahead", false, t0))
				i.Status.Status = "new" // no longer approved
				i.Status.Approval = &tatarav1alpha1.ApprovalEvidence{
					Login: "maintainer-1", CommentID: "c-2", Phrase: "go ahead",
				}
				return i
			}(),
			citations:  cit("c-2", "go ahead"),
			wantReason: ApprovalRefusedEvidenceReplayed,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := proj
			if tc.maintainers != nil {
				p = citProject("bot-1", tc.maintainers...)
			}
			ev, reason := verifyOneIssue(tc.iss, p, repo, "bot-1", tc.citations)
			if reason != tc.wantReason {
				t.Fatalf("reason = %q, want %q", reason, tc.wantReason)
			}
			if tc.wantReason != "" {
				if ev != nil {
					t.Fatalf("a refusal returned evidence %#v; every refusal MUST return nil", ev)
				}
				return
			}
			if ev == nil {
				t.Fatal("a pass returned nil evidence")
			}
			if ev.Auto != tc.wantAuto {
				t.Fatalf("ev.Auto = %v, want %v", ev.Auto, tc.wantAuto)
			}
		})
	}
}

// TestVerifyOneIssue_EvidenceRecordsTheAgentsQuote: on a pass the operator
// stores the agent's VERBATIM quote in ApprovalEvidence.Phrase, alongside the
// cited comment's own id and author. Phrase is reused rather than a new field
// added: it has always been "the text that justified the approval".
func TestVerifyOneIssue_EvidenceRecordsTheAgentsQuote(t *testing.T) {
	t0 := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)
	iss := citIssue(cmt("c-7", "maintainer-1", "sure, go ahead, I approve!", false, t0))

	ev, reason := verifyOneIssue(iss, citProject("bot-1", "maintainer-1"), citRepo(), "bot-1",
		cit("c-7", " go ahead, I approve! "))
	if reason != "" {
		t.Fatalf("reason = %q, want a pass", reason)
	}
	if ev.Login != "maintainer-1" || ev.CommentID != "c-7" {
		t.Fatalf("evidence = %+v, want {login: maintainer-1, commentId: c-7}", ev)
	}
	if ev.Phrase != "go ahead, I approve!" {
		t.Fatalf("evidence.Phrase = %q, want the trimmed verbatim quote", ev.Phrase)
	}
	if ev.Auto {
		t.Fatal("a human citation produced Auto evidence")
	}
}

// TestVerifyOneIssue_MultipleCitations: an agent may cite several comments. A
// citation naming a comment that is NOT a maintainer's is simply not citable, so
// it does not poison a good citation alongside it. (A citation naming a comment
// that IS citable but fails clause (c) or (d) does refuse - see the ordering note
// on verifyOneIssue.)
func TestVerifyOneIssue_MultipleCitations(t *testing.T) {
	t0 := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)
	iss := citIssue(
		cmt("c-1", "randomer", "go ahead", false, t0),
		cmt("c-2", "maintainer-1", "yes please, approved", false, t0.Add(time.Minute)),
	)
	citations := []tatarav1alpha1.ApprovalCitation{
		{ID: "c-1", Quote: "go ahead"},
		{ID: "c-2", Quote: "yes please, approved"},
	}
	ev, reason := verifyOneIssue(iss, citProject("bot-1", "maintainer-1"), citRepo(), "bot-1", citations)
	if reason != "" {
		t.Fatalf("reason = %q, want a pass on the maintainer-authored citation", reason)
	}
	if ev.CommentID != "c-2" {
		t.Fatalf("evidence commentId = %q, want c-2 (the only maintainer-authored citation)", ev.CommentID)
	}
}
