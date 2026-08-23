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
// label on operator_approval_refused_total, the `reason` field of the
// action=approval_refused INFO log, and the `reason` key of the 200 refusal
// body.
//
// NOTHING RENDERS THEM TO THE HUMAN, and nothing should: they are diagnostic
// labels, not prose. What reaches the AGENT alongside the reason is
// ApprovalRefusalGuidance (approval_ship.go), a total map from reason to the
// next step - because the reason names the fault and says nothing about the
// remedy, and the remedies differ per reason.
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
	// ApprovalRefusedApproverNotMaintainer: the agent DECLARED an approving
	// maintainer who is not one. Before #521 this collapsed into the generic
	// citation-not-maintainer, which reports the wrong thing: the citation may
	// be fine and the declared login is what is wrong. This is the
	// reporter-self-approval case, made legible.
	//
	// IT IS GATED ON declared != "". On the auto-approve carve-out path the
	// field is legitimately absent and ev.Login is the <tatara:auto> sentinel;
	// ungated, this refusal would kill every auto-approved proposal on every
	// Project whose autoApproveMaxSignificance is above `off`.
	ApprovalRefusedApproverNotMaintainer = "approver-not-maintainer"
	// ApprovalRefusedApproverMismatch: the declared approver is not the author
	// of the comment that was cited. This is what stops the username becoming a
	// second, weaker authority - the citation stays the SOLE authority and the
	// username is a declaration that must AGREE with it. Gated on
	// declared != "" for the same reason as the constant above.
	ApprovalRefusedApproverMismatch = "approver-mismatch"
	// ApprovalRefusedPlanNoteMissing: this Task has NO plan note to pin - it
	// never wrote one, or the one it wrote has been spilled out of status.notes
	// and its body can no longer be hashed.
	ApprovalRefusedPlanNoteMissing = "plan-note-missing"
	// ApprovalRefusedPlanNoteNotPlan: planNoteId does not name THE plan note.
	//
	// planNoteId is CLIENT-SUPPLIED and the wire says nothing about the note's
	// kind, so an agent may send the id of any note it has written. The gate
	// resolves the plan note ITSELF - the newest note of kind `plan`, which is
	// the same note the submit-time re-check re-hashes - and the declared id is
	// a DECLARATION THAT MUST AGREE with it, exactly as approvingMaintainer is a
	// declaration that must agree with the citation.
	//
	// It fires on a note of another kind (a handoff note, a turn note) and on a
	// SUPERSEDED plan note, because both are the same defect: a hash taken over
	// a note the re-check will never read. The first defeats the pin outright -
	// nothing to compare, so no drift is ever detected - and the second fires it
	// spuriously, bouncing the next untouched submit back to the gate.
	ApprovalRefusedPlanNoteNotPlan = "plan-note-not-plan"
	// ApprovalRefusedPlanHashMismatch: the plan note's body changed between the
	// grant and the attempt to write code against it.
	ApprovalRefusedPlanHashMismatch = "plan-hash-mismatch"
)

// ApprovalRefusals is the CLOSED vocabulary. It is the `reason` label on
// operator_approval_refused_total and the `reason` field in the 200 refusal
// body, so it must stay small, stable, total, and Prometheus-label-safe.
var ApprovalRefusals = []string{
	ApprovalRefusedNoMaintainer,
	ApprovalRefusedNoCitation,
	ApprovalRefusedCitationNotMaintainer,
	ApprovalRefusedQuoteAbsent,
	ApprovalRefusedEvidenceReplayed,
	ApprovalRefusedNoLiveIssue,
	ApprovalRefusedApproverNotMaintainer,
	ApprovalRefusedApproverMismatch,
	ApprovalRefusedPlanNoteMissing,
	ApprovalRefusedPlanNoteNotPlan,
	ApprovalRefusedPlanHashMismatch,
}

// The AUTO-APPROVE AXES. autoApproveApplies is fail-closed on five gates and
// every one of them collapses into the single ApprovalRefusedNoMaintainer
// reason, so the refusal names the SYMPTOM ("no maintainer commented") and never
// the CAUSE. These are the causes, and they are the `axis` label on
// operator_auto_approve_refused_total plus the `axis` field of the
// action=auto_approve_refused INFO log. Like ApprovalRefusals they are a CLOSED,
// Prometheus-label-safe vocabulary; the empty string is the GRANT.
const (
	// AutoApproveRefusedCeilingOff: the Project's autoApproveMaxSignificance is
	// `off` - the default, and exactly the pre-carve-out behaviour. The remedy is
	// a Project config change, and it is the only axis whose remedy is a decision
	// rather than a repair.
	//
	// IT IS THE ONLY CEILING CHECK IN THIS FILE, and deliberately so. The other
	// three ceiling values grant HERE and are re-checked at submit against the
	// declared change_significance (ApprovalShipVerdict), because that level does
	// not exist on the wire until submit_outcome(action=submitted).
	AutoApproveRefusedCeilingOff = "ceiling-off"
	// AutoApproveRefusedNotInScope: the Issue is closed, done or rejected. On
	// the production path this is UNREACHABLE - VerifyApprovalDeclared returns
	// on the same predicate first - and it is kept because autoApproveApplies
	// must not trust its caller for a decision that removes the last human gate.
	// A human's CLOSE is that human's veto.
	AutoApproveRefusedNotInScope = "issue-not-in-scope"
	// AutoApproveRefusedNotBotAuthored: Status.Author is not the project's
	// botLogin, or either of the two is empty. It covers a genuinely
	// human-authored issue (correct refusal), a project with no botLogin
	// configured, and a mirror that has not yet learned the author.
	AutoApproveRefusedNotBotAuthored = "not-bot-authored"
	// AutoApproveRefusedNoMarker: the body carries no valid tatara-proposed-by
	// marker. The usual cause is not tampering but PROVENANCE THAT WAS NEVER
	// CLAIMED: an issue filed through the generic issue_write action=create path
	// declares no proposalKind, deliberately (handlers_v2.go argues the
	// prompt-injection case), so it is not a tatara proposal and never
	// auto-approves. The remedy is to give the filer a declared kind, NEVER to
	// sniff the marker out of an agent-written body.
	AutoApproveRefusedNoMarker = "no-proposal-marker"
	// AutoApproveRefusedAnchorMismatch: the body no longer matches
	// Spec.ProposalBodyHash, INCLUDING the empty-anchor case
	// (ProposalBodyMatchesAnchor fails closed on ""). Two very different causes
	// share it, and telling them apart needs the CR: a body that diverged is the
	// tamper guard doing its job, while an EMPTY anchor is a proposal whose
	// Issue CR was minted by ensureIssueCR (the mirror path, which writes no
	// anchor) rather than by mintIssueCR - re-created after its filing Task was
	// reaped, or filed by a build older than the anchor. The empty-anchor shape
	// is permanent: nothing ever back-fills the anchor, and nothing may, since
	// an anchor derived from the mirrored body would anchor the body to itself
	// and delete the tamper guard entirely.
	AutoApproveRefusedAnchorMismatch = "anchor-mismatch"
)

// AutoApproveRefusals is the CLOSED axis vocabulary, in the order
// autoApproveRefusal evaluates them.
var AutoApproveRefusals = []string{
	AutoApproveRefusedCeilingOff,
	AutoApproveRefusedNotInScope,
	AutoApproveRefusedNotBotAuthored,
	AutoApproveRefusedNoMarker,
	AutoApproveRefusedAnchorMismatch,
}

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
// right now. That idempotence keeps the auto-approve carve-out path
// (ApprovalEvidence{Auto: true, CommentID: ""}) alive and stops a maintainer's
// later "thanks!" from REVOKING an approval already given.
func verifyOneIssue(iss *tatarav1alpha1.Issue, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, botLogin string,
	citations []tatarav1alpha1.ApprovalCitation, declared string) (*tatarav1alpha1.ApprovalEvidence, string) {

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
		// THE AUTO-APPROVE CARVE-OUT (autoApproveMaxSignificance above `off`). It sits ONLY
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

	// THE DECLARED APPROVER, checked against the CITED comment's author, in this
	// order: not-a-maintainer first (the more specific, more actionable failure),
	// then mismatch. It lives HERE and not in verifyApprovalScope because both
	// checks need the cited comment in hand.
	//
	// BOTH ARE SKIPPED ENTIRELY WHEN declared == "". That is not laxity, it is
	// the auto-approve carve-out: on that path there is no comment
	// author to name, the field is legitimately absent, and refusing here would
	// make the carve-out unreachable on the two Projects that have it live. The
	// PAIR RULE in restapi's gate is what stops an agent simply omitting the
	// field to dodge these two - a citation with no declared approver is a 400
	// before this function is ever reached.
	//
	// The declaration is a CROSS-CHECK, never a second authority: the citation
	// stays the sole authority and this only ever REFUSES.
	if declared != "" {
		if !isMaintainerLogin(proj, repo, declared, botLogin) {
			return nil, ApprovalRefusedApproverNotMaintainer
		}
		if !strings.EqualFold(declared, cited.Author) {
			return nil, ApprovalRefusedApproverMismatch
		}
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

// autoApproveApplies is the auto-approve carve-out predicate, and EVERY branch of
// it is a security gate on the last human veto before prod. It is fail-closed on
// all four axes and grants auto-approval ONLY when every one holds:
//
//  1. the per-project ceiling autoApproveMaxSignificance is above `off` (the
//     default => exactly the pre-carve-out behaviour). This is a BINARY test
//     here: the ceiling's LEVEL is enforced at submit, by ApprovalShipVerdict,
//     because change_significance does not exist yet at gate time. A grant on
//     this path is therefore PROVISIONAL - see ApprovalShipVerdict;
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
	return autoApproveRefusal(iss, proj, botLogin) == ""
}

// autoApproveRefusal is autoApproveApplies with its verdict SPELLED OUT: it
// returns the axis that refused, or "" for a grant. The decision is byte-
// identical - autoApproveApplies is now defined as this function's emptiness -
// and the only new thing is that the refusal has a name.
//
// IT EXISTS BECAUSE THE REFUSAL WAS UNATTRIBUTABLE IN PRODUCTION. Every failing
// axis collapses into the single ApprovalRefusedNoMaintainer reason, which reads
// as "no maintainer has commented" - true, but not the cause. On 2026-08-16 an
// implement agent on task mt-i-helmfile-27 was refused exactly that on
// iss-helmfile-32, could not tell which of the five gates had closed, concluded
// it needed a maintainer comment it had no way to obtain, and spent its turn on
// action=discuss instead. The Task parked awaiting-human and delivery stalled
// for a day. The five causes have five different remedies - a project flag, a
// human's close, a botLogin config, a filer that declared no proposalKind, an
// anchor that was never written - and no amount of reading the grammar
// distinguishes them after the fact, because the Issue CR the verdict was about
// is routinely reaped before anyone looks.
//
// THE AXES ARE ORDERED CHEAPEST-AND-MOST-STRUCTURAL FIRST, so the reported axis
// is the most actionable one: a project whose ceiling is off is reported as
// ceiling-off even if its issue is also anchorless, because raising the ceiling
// is the thing to do first.
func autoApproveRefusal(iss *tatarav1alpha1.Issue, proj *tatarav1alpha1.Project, botLogin string) string {
	if tatarav1alpha1.AutoApproveCeiling(proj) == tatarav1alpha1.AutoApproveOff {
		return AutoApproveRefusedCeilingOff
	}
	if !ApprovalInScope(iss) {
		return AutoApproveRefusedNotInScope
	}
	if botLogin == "" || iss.Status.Author == "" || iss.Status.Author != botLogin {
		return AutoApproveRefusedNotBotAuthored
	}
	if tatarav1alpha1.ProposalKindFromBody(iss.Status.Body) == "" {
		return AutoApproveRefusedNoMarker
	}
	if !tatarav1alpha1.ProposalBodyMatchesAnchor(iss.Status.Body, iss.Spec.ProposalBodyHash) {
		return AutoApproveRefusedAnchorMismatch
	}
	return ""
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
func (g *GrammarVerifier) VerifyApprovalDeclared(ctx context.Context, proj *tatarav1alpha1.Project,
	iss *tatarav1alpha1.Issue,
	citations []tatarav1alpha1.ApprovalCitation, declared string) (*tatarav1alpha1.ApprovalEvidence, bool, string) {
	if !ApprovalInScope(iss) {
		return iss.Status.Approval, true, ""
	}
	botLogin := ""
	if proj.Spec.Scm != nil {
		botLogin = proj.Spec.Scm.BotLogin
	}
	repo := approvalRepo(ctx, g.Client, iss)
	ev, reason := verifyOneIssue(iss, proj, repo, botLogin, citations, declared)
	if reason != "" {
		// WHICH AUTO-APPROVE AXIS REFUSED, and only on the one reason the
		// carve-out can produce: no-maintainer-comment is emitted BOTH when a
		// thread genuinely has no maintainer comment and when the carve-out
		// declined to stand in for one, and those are different failures with
		// different remedies. The axis is RECOMPUTED here rather than threaded
		// out of verifyOneIssue because that function is pure, holds no metrics
		// handle, and is called from nowhere else - recomputing costs one hash
		// of a body already in memory and buys no signature churn.
		if reason == ApprovalRefusedNoMaintainer {
			axis := autoApproveRefusal(iss, proj, botLogin)
			log.FromContext(ctx).Info("auto-approve carve-out refused",
				"action", "auto_approve_refused", "issue", iss.Name, "axis", axis)
			g.Metrics.AutoApproveRefused(axis)
		}
		log.FromContext(ctx).Info("approval refused",
			"action", "approval_refused", "issue", iss.Name, "reason", reason, "declared", declared)
		g.Metrics.ApprovalRefused(reason)
		return nil, false, reason
	}
	return ev, true, ""
}

// isMaintainerLogin reports whether login is a verified, non-bot maintainer of
// this project/repo. It is the DECLARED-approver half of the gate and it is
// deliberately the same list isMaintainerComment consults, so a login that
// could never have authored a citable comment can never be declared either.
func isMaintainerLogin(proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository,
	login, botLogin string) bool {
	if login == "" {
		return false
	}
	if botLogin != "" && strings.EqualFold(login, botLogin) {
		return false
	}
	return tatarav1alpha1.IsMaintainer(proj, repo, login)
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
