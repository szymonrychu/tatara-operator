// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"html"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// THE APPROVAL GATE (contract C.6). PRESENCE IS NOT CONSENT; A CITATION IS.
//
// The pre-redesign approvingMaintainer() returned a maintainer-authored comment
// WITHOUT READING IT, so a maintainer saying "I can't approve this until the
// tests pass" released the autonomous implement -> review -> merge -> deploy
// chain. The literal wordlist that replaced it failed the other way: it could
// not read "go ahead, I approve!" and parked a Task whose maintainer intent was
// unambiguous.
//
// The gate is now SPLIT. The AGENT judges what a maintainer comment MEANS and
// CITES it. The OPERATOR judges WHO, structurally, against its own mirror, and
// never reads intent at all:
//
//	a. a maintainer-authored, structurally-non-bot comment exists on this Issue
//	b. the agent CITED one of THEM (set membership, NOT recency)
//	c. the agent's verbatim quote OCCURS in the body the operator holds
//	d. the evidence is SINGLE-USE: a consumed commentId cannot approve twice
//
// and the SCOPE is EVERY LIVE owned Issue (state=open, status not in
// done/rejected), never just one: one citation on one issue must not approve a
// Task spanning every repo in mergeOrder.
//
// THERE IS EXACTLY ONE ENTRY POINT: GrammarVerifier.VerifyApproval, a per-Issue
// READER. The scope loop, the evidence write and the clarifying -> approved edge
// all belong to restapi's verifyApprovalScope + submit_outcome writer
// (internal/restapi/outcome.go), which is the only place the agent's claim
// exists. This package used to carry a second, Task-level twin
// (VerifyApproval/VerifyApprovalDetailed/applyApprovalStage) that looped, wrote
// and moved the stage itself; it lost its last production caller with the
// re-verify-on-unpark path and was deleted rather than left as a second gate
// free to drift from the one that runs.

// Approval refusal reasons. They name what the OPERATOR could not establish -
// never what the comment meant, which is the agent's job. They are the `reason`
// label on operator_approval_refused_total and the `reason` field of the
// action=approval_refused INFO log. NOTHING renders them to the human: the
// refusal parks the Task at identity-unverified and the operator posts no
// comment saying why (ApprovalRefusedComment, which did, had no production
// caller and was deleted with the wordlist).
const (
	// ApprovalRefusedNoMaintainer: there is no maintainer-authored, non-bot
	// comment on the thread at all, and the auto-approve carve-out does not apply.
	ApprovalRefusedNoMaintainer = "no-maintainer-comment"
	// ApprovalRefusedNoCitation: a maintainer HAS commented, so consent is a
	// live question, and the agent cited nothing. Fail closed.
	ApprovalRefusedNoCitation = "no-citation"
	// ApprovalRefusedCitationNotMaintainer: the cited id names no
	// maintainer-authored, non-bot comment on this Issue - it is a
	// non-maintainer's, it is the bot's, it is an unknown id, or it is not on
	// this Issue at all. It is NOT a recency complaint: an EARLIER approving
	// comment is perfectly citable while newer maintainer comments exist. A
	// later comment that withdraws the approval is the agent's call, not this
	// constant's.
	ApprovalRefusedCitationNotMaintainer = "citation-not-maintainer"
	// ApprovalRefusedQuoteAbsent: the agent's verbatim substring does not occur
	// in the body the OPERATOR holds. This is the anti-fabrication check.
	ApprovalRefusedQuoteAbsent = "quote-not-in-comment"
	// ApprovalRefusedEvidenceReplayed: that comment was already consumed as
	// evidence once. A later approval must cite a DIFFERENT comment.
	ApprovalRefusedEvidenceReplayed = "evidence-replayed"
	// ApprovalRefusedNoLiveIssue: the Task has NO live owned Issue to approve -
	// it owns none at all, or a human has closed/done/rejected every one of them.
	//
	// THE ONLY REASON EMITTED OUTSIDE verifyOneIssue. It is a property of the
	// SCOPE, not of any one Issue, so there is nothing for the per-Issue verifier
	// to blame it on; restapi's verifyApprovalScope emits and counts it. Before
	// it existed those two paths refused SILENTLY - no reason, no counter, no
	// action=approval_refused line - because they return before ever calling
	// VerifyApproval, where all three live. A human closing the only Issue of a
	// clarify Task, which is the strongest veto they have and the exact gate hole
	// fix L3-14 closed, was therefore the one refusal with no attribution at all.
	ApprovalRefusedNoLiveIssue = "no-live-issue"
)

// ApprovalInScope is C.6 clause (2), narrowed to LIVE issues (fix L3-14): a
// human closing one issue of a multi-issue Task must not make approval require a
// citation on a CLOSED thread, forever.
//
// EXPORTED because restapi's verifyApprovalScope must apply the IDENTICAL
// filter, and did not. Without it, a Task whose only Issue a human had CLOSED
// produced an all-nil evidence map that verifyApprovalScope still reported as
// granted, and decision=implement reached approved with no citation, no
// maintainer comment and no evidence at all. One definition of "a live Issue",
// not two free to drift - the same reason TaskOwnsIssue is exported.
func ApprovalInScope(iss *tatarav1alpha1.Issue) bool {
	if iss.Status.State != "open" {
		return false
	}
	return iss.Status.Status != "done" && iss.Status.Status != "rejected"
}

// isMaintainerComment is the operator's WHO check on one comment: a verified
// maintainer wrote it and it is structurally NOT the bot.
//
// THREE LAYERS REFUSE A BOT-AUTHORED APPROVAL, AND THIS FILE USED TO CREDIT THE
// WRONG ONE. For "the bot login is misconfigured INTO maintainerLogins", the
// layer that actually refuses is neither of the two here: EffectiveMaintainerLogins
// runs withoutBotLogin (api/v1alpha1/logins.go), which strips the bot from the
// project list AND from a per-Repository override before IsMaintainer ever looks.
// IsMaintainer then rejects the bot login again on its own. Delete BOTH explicit
// checks and the misconfiguration is still refused - verified by mutation.
//
// The botLogin disjunct below is therefore DEFENCE IN DEPTH, not the operative
// check, and it is kept as such: at the one production call site
// (GrammarVerifier.VerifyApproval) botLogin is proj.Spec.Scm.BotLogin, making it
// byte-equivalent to IsMaintainer's own test, but it is the parameter this
// function is given rather than a value it derives, so it is the layer that
// survives a change to either of the other two. TestVerifyOneIssue_BotLoginDisjunctIsTheLastLayer
// pins it by passing a botLogin the other two layers cannot see.
//
// Ordering still matters for the OTHER bot shape: c.IsBot is the mirror's
// structural flag, and checking it before IsMaintainer is what refuses a
// bot-authored comment on a project that names no maintainers at all.
func isMaintainerComment(c *tatarav1alpha1.Comment, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, botLogin string) bool {
	if c.IsBot || c.Author == "" || (botLogin != "" && c.Author == botLogin) {
		return false
	}
	return tatarav1alpha1.IsMaintainer(proj, repo, c.Author)
}

// quoteOccursIn is clause (c), the anti-fabrication check, and it is
// ENTITY-TOLERANT ON PURPOSE. It returns the form of the quote that actually
// occurs in body, and whether one does.
//
// THE AGENT DOES NOT SEE THE RAW BODY. It copies its quote out of the turn-0
// bundle, and prompt.EscapeText has already replaced & < > " ' with their XML
// entities in every comment body rendered there (contract E.1). Matching the
// quote against the RAW mirror body therefore refused a maintainer who wrote
// "let's ship it" - the bundle says "let&apos;s ship it", the agent cites that
// verbatim exactly as its prompt tells it to, and strings.Contains fails.
// Apostrophes and ampersands are ordinary in approving comments, so that was a
// routine refusal, not an edge case: the Task parked at identity-unverified and
// the next turn plausibly cited the same escaped span and looped.
//
// BOTH forms are tried, in this order, and neither is a weakening: each is a
// LITERAL containment test against the body the operator holds itself, so a
// fabricated quote still fails both.
//
//  1. the quote AS SUBMITTED - a re-verification reading off the mirror rather
//     than the bundle submits the raw text, and a maintainer who literally typed
//     "&amp;" must match on that form (unescaping it would yield "&", which is
//     NOT what the body contains);
//  2. the quote UNESCAPED - the bundle form. html.UnescapeString is the stdlib
//     inverse and is used rather than hand-rolling escapeXML's reverse, which
//     would be a second definition free to drift from the first.
//
// It is fixed HERE, in the one place that compares, and never by instructing the
// agent to submit an unescaped quote: an instruction repeated across the clarify
// prompt and two agent skills will drift, a single comparison cannot.
func quoteOccursIn(body, quoted string) (string, bool) {
	quote := strings.TrimSpace(quoted)
	if quote == "" {
		return "", false
	}
	if strings.Contains(body, quote) {
		return quote, true
	}
	if un := strings.TrimSpace(html.UnescapeString(quote)); un != "" && un != quote &&
		strings.Contains(body, un) {
		return un, true
	}
	return "", false
}

// verifyOneIssue is the per-Issue approval decision and the SINGLE definition
// of it. It is PURE: it derives the verdict and NEVER writes; the caller
// persists.
//
// SPLIT OF DUTIES. The agent judges WHAT THE COMMENT MEANS - that is the whole
// point of this design, because the literal wordlist could not read
// "go ahead, I approve!" and parked a Task with the maintainer's intent
// unambiguous. The operator judges WHO, all deterministic, all against its own
// mirror:
//
//	a. a maintainer-authored, structurally-non-bot comment exists on this Issue
//	   (isMaintainerComment: the bot exclusion runs BEFORE IsMaintainer, so a bot
//	   login misconfigured into maintainerLogins still cannot approve);
//	b. the agent CITED one of THEM - the cited id resolves to a member of that
//	   SET;
//	c. the agent's verbatim Quote OCCURS in the body the operator itself holds,
//	   which closes fabrication;
//	d. the evidence is SINGLE-USE: a consumed commentId cannot approve twice.
//
// Clause (b) is a set membership test and NOT a recency test. The cited comment
// need not be the newest maintainer comment, because requiring that deadlocks an
// ordinary thread: "go ahead, I approve!" followed by "thanks - ping me when the
// PR is up" leaves consent unambiguous but nothing citable, so the agent would
// submit discuss every turn and the Task would park at awaiting-human forever
// with no signal. A later maintainer comment that WITHDRAWS the approval is the
// AGENT's call - it reads the whole thread and submits discuss instead of
// implement. That is an intent question, and intent is the agent's side of this
// split. The consequence, accepted deliberately: the operator's backstop against
// a MISJUDGING agent is weaker here than the wordlist's was, because a stale
// "go ahead" can now out-vote a fresh "hold off" if the agent misreads.
//
// It does NOT check the comment against Task.status.stageEnteredAt either.
// stage.Enter re-stamps that on EVERY transition, so the primary flow - park,
// human comments, un-park to conversing (re-stamp), agent cites the comment that
// caused the un-park - would refuse itself forever. Clause (d) and the agent's
// own reading of the thread already give everything recency-vs-park would have.
//
// The caller has already established the Issue is in scope (ApprovalInScope).
// An Issue that ALREADY CARRIES VALID EVIDENCE is approved: clause (2) asks
// whether every live Issue CARRIES evidence, not whether it can be re-derived
// right now. That idempotence keeps the autoApproveTataraProposals path
// (ApprovalEvidence{Auto: true, CommentID: ""}) alive and stops a maintainer's
// later "thanks!" from REVOKING an approval already given.
func verifyOneIssue(iss *tatarav1alpha1.Issue, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, botLogin string,
	citations []tatarav1alpha1.ApprovalCitation) (*tatarav1alpha1.ApprovalEvidence, string) {

	if iss.Status.Status == "approved" && iss.Status.Approval != nil {
		return iss.Status.Approval, ""
	}

	// Clause (a). Does ANY maintainer-authored non-bot comment exist at all?
	maintainerSpoke := false
	for i := range iss.Status.Comments {
		if isMaintainerComment(&iss.Status.Comments[i], proj, repo, botLogin) {
			maintainerSpoke = true
			break
		}
	}
	if !maintainerSpoke {
		// THE AUTO-APPROVE CARVE-OUT (autoApproveTataraProposals). It sits ONLY
		// in the no-maintainer-comment arm on purpose: there is no comment to
		// cite, which is exactly why the citation fields are NOT unconditionally
		// required. A maintainer who DID comment falls through below and blocks
		// the release. Fail-closed on every axis; see autoApproveApplies.
		if autoApproveApplies(iss, proj, botLogin) {
			return autoApprovalEvidence(), ""
		}
		return nil, ApprovalRefusedNoMaintainer
	}

	// A maintainer has spoken, so consent is a live question and SOMETHING must
	// be cited. An empty citation here is a refusal, not a fallback to
	// auto-approve.
	if len(citations) == 0 {
		return nil, ApprovalRefusedNoCitation
	}

	// Clause (b): SET MEMBERSHIP, not recency. Any maintainer-authored non-bot
	// comment on this Issue is citable, including an older one with newer
	// maintainer comments after it. An empty ExternalID on either side can never
	// match: a mirror row with no id is not citable, and a citation with no id
	// cites nothing.
	//
	// The FIRST citable comment in thread order is the one verified, and clauses
	// (c)/(d) then either grant on it or refuse - there is no falling through to
	// the next citation. That is deliberate and fail-closed: an agent that cites
	// several comments gets a verdict on the first one it named that exists, not
	// the most convenient one.
	var cited *tatarav1alpha1.Comment
	var quoted string
	for i := range iss.Status.Comments {
		c := &iss.Status.Comments[i]
		if c.ExternalID == "" || !isMaintainerComment(c, proj, repo, botLogin) {
			continue
		}
		for j := range citations {
			if citations[j].ID == c.ExternalID {
				cited, quoted = c, citations[j].Quote
				break
			}
		}
		if cited != nil {
			break
		}
	}
	if cited == nil {
		return nil, ApprovalRefusedCitationNotMaintainer
	}

	// Clause (c). TrimSpace first: an empty or whitespace-only quote is a
	// trivially-matching substring and must be refused explicitly, not accepted
	// by strings.Contains's empty-needle rule.
	quote, ok := quoteOccursIn(cited.Body, quoted)
	if !ok {
		return nil, ApprovalRefusedQuoteAbsent
	}

	// Clause (d), SINGLE-USE.
	if iss.Status.Approval != nil && iss.Status.Approval.CommentID == cited.ExternalID {
		return nil, ApprovalRefusedEvidenceReplayed
	}

	return &tatarav1alpha1.ApprovalEvidence{
		Login:     cited.Author,
		CommentID: cited.ExternalID,
		CreatedAt: cited.CreatedAt,
		// The agent's quote, in the form that ACTUALLY OCCURS in the body the
		// operator holds (quoteOccursIn returns the matched form, not the
		// submitted one). A human reads this in `kubectl get issue -o yaml`, so
		// it must not be the entity soup the agent copied out of its bundle.
		Phrase: quote,
	}, ""
}

// autoApproveApplies is the autoApproveTataraProposals carve-out predicate, and
// EVERY branch of it is a security gate on the last human veto before prod. It is
// fail-closed on all four axes and grants auto-approval ONLY when every one holds:
//
//  1. the per-project flag is on (default false => exactly today's behavior);
//  2. the Issue is in scope - open, not done/rejected. A human's CLOSE is the
//     veto, and a closed Issue is refused here even though the callers already
//     filter it, so the security decision is self-contained, not caller-trusting;
//  3. the Issue is BOT-authored: Status.Author (SCM truth, mirror-refreshed) equals
//     a NON-EMPTY botLogin. A human-authored issue, or one whose author cannot be
//     verified (empty author / empty botLogin), is NEVER auto-approved;
//  4. the body carries a valid tatara-proposed-by marker (brainstorm / incident)
//     AND still matches its filing-time integrity anchor. A missing/malformed
//     marker fails closed; so does a body that diverges from the anchor.
//
// Axis 4 is the tamper guard for a coupling that is safe today but will not stay
// that way. Auto-approve releases the body in Status.Body as reviewed-by-
// construction. Today a human body edit is invisible: Task.Spec.Goal is frozen at
// mint and issues.edited webhooks are ignored, so Status.Body only ever holds the
// tatara-proposed content. A parallel workstream's approved design WILL wire
// issue-edit body refresh, after which the mirror would carry a human edit into
// Status.Body. The anchor makes that fail closed: the hash of record lives in
// IssueSpec.ProposalBodyHash, written once by the operator at mint into the ONE
// field the mirror never overwrites, so a party with forge write access can edit
// the body and even rewrite the in-body marker, but cannot rewrite the CR Spec -
// so any post-filing body change (scope edit, marker forgery) diverges from the
// anchor and refuses. Fail-closed on a missing anchor (older-build proposal) too.
func autoApproveApplies(iss *tatarav1alpha1.Issue, proj *tatarav1alpha1.Project, botLogin string) bool {
	if !proj.Spec.AutoApproveTataraProposals {
		return false
	}
	if !ApprovalInScope(iss) {
		return false
	}
	if botLogin == "" || iss.Status.Author == "" || iss.Status.Author != botLogin {
		return false
	}
	if tatarav1alpha1.ProposalKindFromBody(iss.Status.Body) == "" {
		return false
	}
	return tatarav1alpha1.ProposalBodyMatchesAnchor(iss.Status.Body, iss.Spec.ProposalBodyHash)
}

// autoApprovalEvidence is the ApprovalEvidence the carve-out records: Auto=true,
// the sentinel Login, and NO CommentID (there is no maintainer comment to cite).
// The clause-2 idempotency in verifyOneIssue keeps it alive across re-verification.
func autoApprovalEvidence() *tatarav1alpha1.ApprovalEvidence {
	return &tatarav1alpha1.ApprovalEvidence{
		Auto:      true,
		Login:     tatarav1alpha1.AutoApproveLogin,
		CreatedAt: metav1.Now(),
	}
}

// GrammarVerifier is the PRODUCTION restapi.ApprovalVerifier (fix W1). Before it
// was wired, restapi.Config.Approval was nil, so verifyApprovalScope failed
// closed on EVERY clarify decision=implement and the platform could never
// implement anything. It runs the per-Issue citation check against the Issue
// CR's MIRRORED comments, so the REST clarify path verifies a real maintainer
// approval rather than failing closed. It is a pure READER; outcome.go persists
// the returned evidence.
//
// Metrics may be nil (every accessor is nil-safe).
type GrammarVerifier struct {
	Client  client.Client
	Metrics *obs.OperatorMetrics
}

// VerifyApproval implements the restapi.ApprovalVerifier seam for ONE owned
// Issue. An out-of-scope (closed / done / rejected) Issue is not pending
// approval and never blocks the scope check: it passes with whatever evidence it
// already carries.
//
// THAT ARM IS PERMISSIVE, NOT FAIL-CLOSED, and the safety is NOT here: the
// evidence it returns is routinely nil. It is the CALLER that must not treat
// that as an approval - restapi's verifyApprovalScope filters out-of-scope
// Issues out of the scope loop entirely (ApprovalInScope, the same predicate)
// and refuses both an empty remainder and any in-scope Issue that grants nil
// evidence. Relying on this arm alone is precisely the hole that let a Task
// whose only Issue a human had CLOSED reach approved with no evidence at all.
func (g *GrammarVerifier) VerifyApproval(ctx context.Context, proj *tatarav1alpha1.Project,
	iss *tatarav1alpha1.Issue,
	citations []tatarav1alpha1.ApprovalCitation) (*tatarav1alpha1.ApprovalEvidence, bool) {
	if !ApprovalInScope(iss) {
		return iss.Status.Approval, true
	}
	botLogin := ""
	if proj.Spec.Scm != nil {
		botLogin = proj.Spec.Scm.BotLogin
	}
	repo := approvalRepo(ctx, g.Client, iss)
	ev, reason := verifyOneIssue(iss, proj, repo, botLogin, citations)
	if reason != "" {
		log.FromContext(ctx).Info("approval refused",
			"action", "approval_refused", "issue", iss.Name, "reason", reason)
		g.Metrics.ApprovalRefused(reason)
		return nil, false
	}
	return ev, true
}

// approvalRepo resolves the Issue's Repository for the per-repository
// maintainerLogins override. A missing Repository resolves to the project list.
func approvalRepo(ctx context.Context, c client.Client, iss *tatarav1alpha1.Issue) *tatarav1alpha1.Repository {
	if iss.Spec.RepositoryRef == "" {
		return nil
	}
	var repo tatarav1alpha1.Repository
	if err := c.Get(ctx, types.NamespacedName{Namespace: iss.Namespace, Name: iss.Spec.RepositoryRef}, &repo); err != nil {
		return nil
	}
	return &repo
}

// TaskOwnsIssue reports whether the Task owns the Issue the event landed on.
// An event on a thread this Task does not own buys no forge read. Exported
// because the webhook comment path scopes its own on-demand mirror sync with
// exactly this predicate (internal/webhook/pending_events.go) - one definition
// of "this Task's thread", not two that can drift.
func TaskOwnsIssue(task *tatarav1alpha1.Task, repoRef string, number int) bool {
	want := tatarav1alpha1.IssueName(repoRef, number)
	for _, name := range task.Status.IssueRefs {
		if name == want {
			return true
		}
	}
	return false
}
