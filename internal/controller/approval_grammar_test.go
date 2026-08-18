package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// THIS FILE TESTS THE ONE SURVIVING ENTRY POINT, GrammarVerifier.VerifyApproval.
//
// It used to test a SECOND, Task-level one (VerifyApproval /
// VerifyApprovalDetailed / applyApprovalStage / ReVerifyParked) that looped over
// every owned Issue, wrote the evidence and moved clarifying -> approved itself.
// That twin lost its last production caller and was deleted; the loop, the write
// and the stage edge all belong to restapi's verifyApprovalScope +
// submit_outcome writer, and are tested there (TestOutcome_Clarify_*). Tests for
// the deleted twin were pinning ordering inside dead code, so they went with it
// rather than being retargeted - a scope assertion against a function nothing
// calls certifies nothing.
//
// The per-Issue citation clauses themselves are in approval_citation_test.go
// (verifyOneIssue, the pure core). What is left here is the carve-out matrix and
// the seam's own two arms.

// approvalProject builds the project: a bot login and a maintainer set. There is
// no wordlist any more - the agent judges the wording, the operator judges who
// wrote the comment it cited.
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

// cites builds the agent's citation set: one {id, quote} pair per argument pair.
func cites(pairs ...string) []tatarav1alpha1.ApprovalCitation {
	out := make([]tatarav1alpha1.ApprovalCitation, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		out = append(out, tatarav1alpha1.ApprovalCitation{ID: pairs[i], Quote: pairs[i+1]})
	}
	return out
}

func getTaskCR(t *testing.T, c client.Client, name string) *tatarav1alpha1.Task {
	t.Helper()
	var task tatarav1alpha1.Task
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: testNS, Name: name}, &task); err != nil {
		t.Fatalf("get task %s: %v", name, err)
	}
	return &task
}

// verifyOne runs the production seam over ONE Issue and hands the metrics
// registry back with the verdict, because the refusal REASON is not in the
// return: the seam answers (nil, false) and labels
// operator_approval_refused_total{reason} on its way out. Asserting that counter
// is how a test at this layer names the reason it expected, and it pins hard
// rule 13 in the same assertion.
func verifyOne(t *testing.T, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository,
	iss *tatarav1alpha1.Issue, citations []tatarav1alpha1.ApprovalCitation) (
	*tatarav1alpha1.ApprovalEvidence, bool, *obs.OperatorMetrics) {
	t.Helper()
	metrics := obs.NewOperatorMetrics(prometheus.NewRegistry())
	g := &GrammarVerifier{Client: newMirrorClient(t, proj, repo, iss), Metrics: metrics}
	ev, ok, _ := g.VerifyApprovalDeclared(context.Background(), proj, iss, citations, "")
	return ev, ok, metrics
}

// autoProposalIssue builds a bot-authored, tatara-proposed issue (the shape a
// brainstorm proposal / incident tracker issue has after mintIssueCR): open,
// Author = the bot login, body carrying the provenance marker.
func autoProposalIssue(repo, botLogin, kind string, number int, comments ...tatarav1alpha1.Comment) *tatarav1alpha1.Issue {
	iss := approvalIssue(repo, number, comments...)
	iss.Status.Author = botLogin
	iss.Status.Body = tatarav1alpha1.StampProposalMarker("do the proposed work", kind)
	// The operator writes the integrity anchor into Spec at mint (see mintIssueCR).
	iss.Spec.ProposalBodyHash = tatarav1alpha1.ComputeProposalContentHash(iss.Status.Body)
	return iss
}

// TestAutoApprove_FailClosedMatrix is the security-critical carve-out matrix: the
// autoApproveTataraProposals path removes the last human gate before prod, so
// every fail-closed branch is asserted explicitly. Auto-approval is granted ONLY
// on the all-green row (flag on + bot author + valid marker + open + no
// maintainer comment) and it is the one arm where NO citation is required,
// because there is no comment to cite; every other row must refuse.
func TestAutoApprove_FailClosedMatrix(t *testing.T) {
	const bot = "tatara-bot"
	now := time.Now()

	tests := []struct {
		name        string
		flagOn      bool
		mutate      func(iss *tatarav1alpha1.Issue)
		mutateProj  func(p *tatarav1alpha1.Project)
		wantAuto    bool
		wantRefusal string // "" when not asserted (auto pass)
		// wantAxis is the auto-approve axis that refused, and it is asserted on
		// EVERY no-maintainer-comment row: that one reason is the terminal
		// symptom of five different causes, and until the axis was a label the
		// live refusal (helmfile#32, 2026-08-16) could not be attributed to any
		// of them. It is "" on the rows that never reach the carve-out.
		wantAxis string
	}{
		{
			name:     "flag on + bot + marker + open => Auto:true",
			flagOn:   true,
			wantAuto: true,
		},
		{
			name:        "flag OFF => today's behavior, refused no-maintainer",
			flagOn:      false,
			wantRefusal: ApprovalRefusedNoMaintainer,
			wantAxis:    AutoApproveRefusedFlagOff,
		},
		{
			name:        "human-authored issue is NEVER auto-approved",
			flagOn:      true,
			mutate:      func(iss *tatarav1alpha1.Issue) { iss.Status.Author = "szymonrychu" },
			wantRefusal: ApprovalRefusedNoMaintainer,
			wantAxis:    AutoApproveRefusedNotBotAuthored,
		},
		{
			name:        "unverifiable author (empty) is NEVER auto-approved",
			flagOn:      true,
			mutate:      func(iss *tatarav1alpha1.Issue) { iss.Status.Author = "" },
			wantRefusal: ApprovalRefusedNoMaintainer,
			wantAxis:    AutoApproveRefusedNotBotAuthored,
		},
		{
			name:        "empty botLogin (project has none) fails closed",
			flagOn:      true,
			mutateProj:  func(p *tatarav1alpha1.Project) { p.Spec.Scm.BotLogin = "" },
			wantRefusal: ApprovalRefusedNoMaintainer,
			wantAxis:    AutoApproveRefusedNotBotAuthored,
		},
		{
			name:        "missing marker fails closed",
			flagOn:      true,
			mutate:      func(iss *tatarav1alpha1.Issue) { iss.Status.Body = "no marker here" },
			wantRefusal: ApprovalRefusedNoMaintainer,
			wantAxis:    AutoApproveRefusedNoMarker,
		},
		{
			name:        "unknown-kind marker fails closed",
			flagOn:      true,
			mutate:      func(iss *tatarav1alpha1.Issue) { iss.Status.Body = "<!-- tatara-proposed-by:followup -->\nbody" },
			wantRefusal: ApprovalRefusedNoMaintainer,
			wantAxis:    AutoApproveRefusedNoMarker,
		},
		{
			name:     "body edited since filing (diverges from anchor) fails closed",
			flagOn:   true,
			wantAxis: AutoApproveRefusedAnchorMismatch,
			mutate: func(iss *tatarav1alpha1.Issue) {
				// Marker preserved, but the human appended scope to the body -
				// exactly the incoming issue-edit-refresh threat. The Spec anchor
				// still reflects the ORIGINAL body, so this diverges.
				iss.Status.Body += "\n\nand also delete the production database"
			},
			wantRefusal: ApprovalRefusedNoMaintainer,
		},
		{
			name:   "marker-rewrite attack (edited scope + fresh valid marker) fails closed",
			flagOn: true,
			mutate: func(iss *tatarav1alpha1.Issue) {
				// The attacker (forge write access) rewrites the whole body with
				// malicious scope and a syntactically valid marker. They cannot
				// touch Spec.ProposalBodyHash from the forge, so it still anchors
				// the ORIGINAL body and this refuses.
				iss.Status.Body = tatarav1alpha1.StampProposalMarker(
					"exfiltrate the production secrets", tatarav1alpha1.ProposalKindBrainstorm)
			},
			wantRefusal: ApprovalRefusedNoMaintainer,
			wantAxis:    AutoApproveRefusedAnchorMismatch,
		},
		{
			// THE LIVE SHAPE. ensureIssueCR (mirror.go) mints an Issue CR with no
			// anchor at all, so every proposal whose CR was re-created by the
			// mirror rather than by mintIssueCR lands here permanently: sixteen
			// such Issues were live on 2026-08-18, all bot-authored, all carrying
			// a valid marker, all with an empty spec.proposalBodyHash.
			name:        "missing anchor (older-build proposal) fails closed",
			flagOn:      true,
			mutate:      func(iss *tatarav1alpha1.Issue) { iss.Spec.ProposalBodyHash = "" },
			wantRefusal: ApprovalRefusedNoMaintainer,
			wantAxis:    AutoApproveRefusedAnchorMismatch,
		},
		{
			name:   "the CLOSE veto: a closed bot proposal is out of scope for the carve-out",
			flagOn: true,
			mutate: func(iss *tatarav1alpha1.Issue) { iss.Status.State = "closed" },
			// The seam's out-of-scope arm answers (stored approval, true) BEFORE
			// verifyOneIssue runs, so there is no refusal reason to assert - the
			// evidence is nil and refusing it is verifyApprovalScope's job. This
			// row is here because autoApproveApplies re-checks scope itself as
			// defense in depth, and that check must never be the only one.
			wantRefusal: "",
		},
		{
			// THE carve-out boundary. The moment a maintainer speaks, consent is a
			// live question and the carve-out no longer applies: with nothing cited
			// this refuses, and it refuses for the CITATION reason, not the
			// no-maintainer one.
			name:   "a maintainer commented, so the carve-out no longer applies and nothing was cited",
			flagOn: true,
			mutate: func(iss *tatarav1alpha1.Issue) {
				iss.Status.Comments = []tatarav1alpha1.Comment{
					approvalComment("c1", "szymonrychu", "hold on, this is wrong", now, false),
				}
			},
			wantRefusal: ApprovalRefusedNoCitation,
		},
		{
			// And a citation of that maintainer's own words does NOT rescue it
			// when the words are not in the comment: the anti-fabrication check
			// runs on the carve-out path too.
			name:   "a maintainer commented and the agent fabricated a quote",
			flagOn: true,
			mutate: func(iss *tatarav1alpha1.Issue) {
				iss.Status.Comments = []tatarav1alpha1.Comment{
					approvalComment("c1", "szymonrychu", "hold on, this is wrong", now, false),
				}
			},
			wantRefusal: ApprovalRefusedQuoteAbsent,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			proj, repo := approvalProject("szymonrychu"), mirrorRepo()
			proj.Spec.AutoApproveTataraProposals = tc.flagOn
			if tc.mutateProj != nil {
				tc.mutateProj(proj)
			}
			iss := autoProposalIssue(repo.Name, bot, tatarav1alpha1.ProposalKindBrainstorm, 1)
			if tc.mutate != nil {
				tc.mutate(iss)
			}

			var citations []tatarav1alpha1.ApprovalCitation
			if tc.wantRefusal == ApprovalRefusedQuoteAbsent {
				citations = cites("c1", "go ahead")
			}

			ev, ok, metrics := verifyOne(t, proj, repo, iss, citations)
			switch {
			case tc.wantAuto:
				if !ok || ev == nil || !ev.Auto ||
					ev.Login != tatarav1alpha1.AutoApproveLogin || ev.CommentID != "" {
					t.Fatalf("verdict = (%+v, %v), want Auto:true, Login:%q, empty CommentID",
						ev, ok, tatarav1alpha1.AutoApproveLogin)
				}
			case tc.wantRefusal == "":
				// The out-of-scope arm: permissive by design, and nil evidence.
				if !ok || ev != nil {
					t.Fatalf("verdict = (%+v, %v), want the permissive out-of-scope arm with NO evidence", ev, ok)
				}
			default:
				if ok || ev != nil {
					t.Fatalf("a fail-closed row granted: verdict = (%+v, %v)", ev, ok)
				}
				if got := testutil.ToFloat64(metrics.ApprovalRefusedCounter(tc.wantRefusal)); got != 1 {
					t.Fatalf("operator_approval_refused_total{reason=%q} = %v, want 1", tc.wantRefusal, got)
				}
			}
			// THE AXIS. no-maintainer-comment is the ONLY refusal the carve-out
			// can produce, and it says nothing about WHICH of the five gates
			// refused, so it is counted per-axis as well as per-reason. Every
			// other refusal reached the maintainer-comment arm and must move NO
			// axis series at all.
			if tc.wantRefusal == ApprovalRefusedNoMaintainer && tc.wantAxis == "" {
				t.Fatal("a no-maintainer-comment row must name the auto-approve axis that refused")
			}
			for _, axis := range AutoApproveRefusals {
				want := 0.0
				if axis == tc.wantAxis {
					want = 1
				}
				if got := testutil.ToFloat64(metrics.AutoApproveRefusedCounter(axis)); got != want {
					t.Fatalf("operator_auto_approve_refused_total{axis=%q} = %v, want %v", axis, got, want)
				}
			}
			// operator_auto_approve_total counts the auto-approval TRANSITION, so
			// it belongs to the WRITER (restapi's submit_outcome), never to this
			// pure reader: a reader cannot tell a first grant from the hundredth
			// re-read of the same approved Issue, and counting here would inflate
			// the one series that says "the last human gate was removed".
			if got := testutil.ToFloat64(
				metrics.AutoApproveCounter(tatarav1alpha1.ProposalKindBrainstorm)); got != 0 {
				t.Fatalf("operator_auto_approve_total{kind=brainstorm} = %v, want 0 from the reader", got)
			}
		})
	}
}

// TestVerifyApproval_OutOfScopeIssueIsPermissiveAndEvidenceless documents the
// arm that is NOT fail-closed, because relying on it alone was a real gate hole.
// An out-of-scope (closed / done / rejected) Issue is not pending approval and
// must not block the others, so the seam answers ok=true with whatever the Issue
// already carries - which is routinely NOTHING. The safety lives in the caller:
// restapi's verifyApprovalScope filters these out with the same predicate and
// then refuses an empty remainder (TestOutcome_Clarify_ClosedIssueIsNotALicence).
func TestVerifyApproval_OutOfScopeIssueIsPermissiveAndEvidenceless(t *testing.T) {
	proj, repo := approvalProject("szymonrychu"), mirrorRepo()

	for _, tc := range []struct {
		name   string
		mutate func(iss *tatarav1alpha1.Issue)
	}{
		{"closed", func(iss *tatarav1alpha1.Issue) { iss.Status.State = "closed" }},
		{"done", func(iss *tatarav1alpha1.Issue) { iss.Status.Status = "done" }},
		{"rejected", func(iss *tatarav1alpha1.Issue) { iss.Status.Status = "rejected" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			iss := approvalIssue(repo.Name, 1)
			tc.mutate(iss)
			ev, ok, _ := verifyOne(t, proj, repo, iss, nil)
			if !ok {
				t.Fatal("an out-of-scope Issue was refused; it would block every live Issue on the Task")
			}
			if ev != nil {
				t.Fatalf("evidence = %+v, want nil - the caller must not read this as an approval", ev)
			}
			if ApprovalInScope(iss) {
				t.Fatal("ApprovalInScope still calls this Issue live; the two must agree or the caller's filter drifts")
			}
		})
	}
}

// TestVerifyApproval_StoredEvidenceNeedsNoCitation is clause-2 idempotence at
// the seam. Approval is a question about what an Issue CARRIES, not about
// whether it can be re-derived on this request, and two things depend on that:
// the autoApproveTataraProposals evidence has NO comment to re-match, and a
// maintainer's later "thanks!" must not revoke an approval already granted.
func TestVerifyApproval_StoredEvidenceNeedsNoCitation(t *testing.T) {
	proj, repo := approvalProject("szymonrychu"), mirrorRepo()
	now := time.Now()

	t.Run("auto evidence survives a re-read with no citation", func(t *testing.T) {
		iss := approvalIssue(repo.Name, 1)
		iss.Status.Status = "approved"
		iss.Status.Approval = &tatarav1alpha1.ApprovalEvidence{
			Auto: true, Login: tatarav1alpha1.AutoApproveLogin, CommentID: "", CreatedAt: metav1.Now(),
		}
		ev, ok, _ := verifyOne(t, proj, repo, iss, nil)
		if !ok || ev == nil || !ev.Auto || ev.Login != tatarav1alpha1.AutoApproveLogin {
			t.Fatalf("verdict = (%+v, %v), want the stored auto evidence preserved", ev, ok)
		}
	})

	t.Run("later maintainer chatter does not revoke a granted approval", func(t *testing.T) {
		iss := approvalIssue(repo.Name, 2,
			approvalComment("c1", "szymonrychu", "go ahead", now, false),
			approvalComment("c2", "szymonrychu", "thanks!", now.Add(time.Hour), false))
		iss.Status.Status = "approved"
		iss.Status.Approval = &tatarav1alpha1.ApprovalEvidence{
			Login: "szymonrychu", CommentID: "c1", Phrase: "go ahead", CreatedAt: metav1.NewTime(now),
		}
		ev, ok, _ := verifyOne(t, proj, repo, iss, nil)
		if !ok || ev == nil || ev.CommentID != "c1" {
			t.Fatalf("verdict = (%+v, %v), want the stored human evidence preserved", ev, ok)
		}
	})
}

// TestVerifyApprovalRefusalReasonsAreDistinct: the refusal reasons the operator
// can return name what the OPERATOR could not establish, and each is its own
// distinct label - they are the `reason` label on
// operator_approval_refused_total, so a collision would merge two failure modes
// into one unreadable series. None of them says what wording to use: there is no
// wordlist any more, and telling a human to type a magic phrase was the failure
// this redesign removes.
//
// It walks ApprovalRefusals rather than a hand-copied list. The hand-copied one
// covered five of eleven, so the six added since (no-live-issue, the two
// approver checks, the three plan-pin ones) were pinned by nothing.
func TestVerifyApprovalRefusalReasonsAreDistinct(t *testing.T) {
	if len(ApprovalRefusals) != 11 {
		t.Fatalf("ApprovalRefusals has %d entries, want 11: a new refusal must join the closed vocabulary, "+
			"which is the `reason` label's only definition", len(ApprovalRefusals))
	}
	seen := map[string]bool{}
	for _, reason := range ApprovalRefusals {
		if reason == "" {
			t.Fatal("a refusal reason is EMPTY")
		}
		if seen[reason] {
			t.Fatalf("refusal reason %q is duplicated", reason)
		}
		seen[reason] = true
	}
}

// TestAutoApproveRefusalIsExhaustiveOverTheAxes pins the axis attribution
// itself: every axis in the closed vocabulary is PRODUCIBLE by a real Issue
// shape, and a fully-satisfying proposal produces none.
//
// It calls autoApproveRefusal directly rather than through the seam because one
// axis - issue-not-in-scope - is unreachable from there by construction:
// VerifyApprovalDeclared returns on its own ApprovalInScope check before
// verifyOneIssue runs. That axis exists precisely so the security decision is
// self-contained rather than caller-trusting, and a defence-in-depth branch no
// test can reach is a branch free to rot.
func TestAutoApproveRefusalIsExhaustiveOverTheAxes(t *testing.T) {
	const bot = "tatara-bot"

	tests := []struct {
		name       string
		mutate     func(iss *tatarav1alpha1.Issue)
		mutateProj func(p *tatarav1alpha1.Project)
		want       string
	}{
		{name: "every axis satisfied", want: ""},
		{
			name:       "flag off",
			mutateProj: func(p *tatarav1alpha1.Project) { p.Spec.AutoApproveTataraProposals = false },
			want:       AutoApproveRefusedFlagOff,
		},
		{
			name:   "closed issue (the human veto)",
			mutate: func(iss *tatarav1alpha1.Issue) { iss.Status.State = "closed" },
			want:   AutoApproveRefusedNotInScope,
		},
		{
			name:   "issue already rejected",
			mutate: func(iss *tatarav1alpha1.Issue) { iss.Status.Status = "rejected" },
			want:   AutoApproveRefusedNotInScope,
		},
		{
			name:   "human author",
			mutate: func(iss *tatarav1alpha1.Issue) { iss.Status.Author = "szymonrychu" },
			want:   AutoApproveRefusedNotBotAuthored,
		},
		{
			name:   "no marker",
			mutate: func(iss *tatarav1alpha1.Issue) { iss.Status.Body = "plain body" },
			want:   AutoApproveRefusedNoMarker,
		},
		{
			name:   "empty anchor",
			mutate: func(iss *tatarav1alpha1.Issue) { iss.Spec.ProposalBodyHash = "" },
			want:   AutoApproveRefusedAnchorMismatch,
		},
		{
			name:   "body diverged from the anchor",
			mutate: func(iss *tatarav1alpha1.Issue) { iss.Status.Body += "\nextra scope" },
			want:   AutoApproveRefusedAnchorMismatch,
		},
	}

	produced := map[string]bool{}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			proj, repo := approvalProject("szymonrychu"), mirrorRepo()
			proj.Spec.AutoApproveTataraProposals = true
			if tc.mutateProj != nil {
				tc.mutateProj(proj)
			}
			iss := autoProposalIssue(repo.Name, bot, tatarav1alpha1.ProposalKindIncident, 1)
			if tc.mutate != nil {
				tc.mutate(iss)
			}
			got := autoApproveRefusal(iss, proj, bot)
			if got != tc.want {
				t.Fatalf("autoApproveRefusal = %q, want %q", got, tc.want)
			}
			// THE DECISION IS THE REFUSAL'S EMPTINESS, and nothing else. This is
			// the no-behaviour-change pin: instrumenting the carve-out must not
			// move one approval verdict.
			if applies := autoApproveApplies(iss, proj, bot); applies != (got == "") {
				t.Fatalf("autoApproveApplies = %v but autoApproveRefusal = %q; the axis split changed the DECISION", applies, got)
			}
			produced[got] = true
		})
	}

	for _, axis := range AutoApproveRefusals {
		if !produced[axis] {
			t.Fatalf("axis %q is in AutoApproveRefusals but no row produces it; an unproducible label is a series nobody can read", axis)
		}
	}
}

// TestAutoApproveRefusalsAreDistinct is the axis vocabulary's own collision
// check, for the same reason its reason twin above exists: axis is a Prometheus
// label on operator_auto_approve_refused_total.
func TestAutoApproveRefusalsAreDistinct(t *testing.T) {
	seen := map[string]bool{}
	for _, axis := range AutoApproveRefusals {
		if axis == "" {
			t.Fatal("an auto-approve axis is EMPTY; the empty string is the GRANT")
		}
		if seen[axis] {
			t.Fatalf("auto-approve axis %q is duplicated", axis)
		}
		seen[axis] = true
	}
}
