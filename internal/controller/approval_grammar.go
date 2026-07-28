// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
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

// Approval refusal reasons. They name what the OPERATOR could not establish -
// never what the comment meant, which is the agent's job. They are the `reason`
// label on operator_approval_refused_total and the text the operator's park
// comment reports back to the human.
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
)

// ApprovalPassed reports whether EVERY live owned Issue carries evidence.
//
// THE EMPTY SET IS NOT A LICENCE: an evidence map with no entries - a Task
// owning ZERO live Issues - is a REFUSAL, never a pass. all([]) == true must
// never gate code execution.
func ApprovalPassed(evidence map[string]*tatarav1alpha1.ApprovalEvidence) bool {
	if len(evidence) == 0 {
		return false
	}
	for _, e := range evidence {
		if e == nil {
			return false
		}
	}
	return true
}

// approvalInScope is C.6 clause (2), narrowed to LIVE issues (fix L3-14): a
// human closing one issue of a multi-issue Task must not make approval require a
// phrase on a CLOSED thread, forever.
func approvalInScope(iss *tatarav1alpha1.Issue) bool {
	if iss.Status.State != "open" {
		return false
	}
	return iss.Status.Status != "done" && iss.Status.Status != "rejected"
}

// isMaintainerComment is the operator's WHO check on one comment: a verified
// maintainer wrote it and it is structurally NOT the bot. The bot exclusion runs
// BEFORE IsMaintainer, so a bot login misconfigured into maintainerLogins still
// cannot approve.
func isMaintainerComment(c *tatarav1alpha1.Comment, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, botLogin string) bool {
	if c.IsBot || c.Author == "" || (botLogin != "" && c.Author == botLogin) {
		return false
	}
	return tatarav1alpha1.IsMaintainer(proj, repo, c.Author)
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
// The caller has already established the Issue is in scope (approvalInScope).
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
	quote := strings.TrimSpace(quoted)
	if quote == "" || !strings.Contains(cited.Body, quote) {
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
		Phrase:    quote, // the agent's verbatim quote, re-verified by the operator
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
	if !approvalInScope(iss) {
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
// already carries, mirroring VerifyApprovalDetailed's skip.
func (g *GrammarVerifier) VerifyApproval(ctx context.Context, proj *tatarav1alpha1.Project,
	iss *tatarav1alpha1.Issue,
	citations []tatarav1alpha1.ApprovalCitation) (*tatarav1alpha1.ApprovalEvidence, bool) {
	if !approvalInScope(iss) {
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

// VerifyApproval runs the C.6 grammar over EVERY LIVE owned Issue and writes the
// verified evidence. It is called from TWO places: clarify
// submit_outcome(decision=implement), and the parked(identity-unverified) un-park
// path on a non-bot pendingEvent (via ReVerifyParked).
//
// On a pass, per issue: status.approval = {login, commentId, createdAt, phrase}
// and status.status = approved. Once EVERY live issue is approved, a Task sitting
// in clarifying enters approved. Approval is NOT sticky: a Task in approved that
// no longer satisfies clause (2) - because it ACQUIRED an Issue after the gate -
// goes back to clarifying.
//
// It never enters a stage from parked: that edge belongs to stage.Unpark, which
// as of agent-judged-approval-gate step C takes NO verdict from this function or
// any other - stage.UnparkInput has no verdict field at all any more, so a
// parked identity-unverified Task can only reach conversing. This function has
// no production caller either (only tests); it is deleted in step 7.
func VerifyApproval(ctx context.Context, c client.Client, sp objbudget.Spiller,
	proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task,
	citations []tatarav1alpha1.ApprovalCitation) (map[string]*tatarav1alpha1.ApprovalEvidence, error) {
	evidence, _, err := VerifyApprovalDetailed(ctx, c, sp, proj, task, citations, nil)
	return evidence, err
}

// VerifyApprovalDetailed is VerifyApproval plus the per-issue refusal reason -
// one of the ApprovalRefused* constants above. The evidence map has an entry for
// every LIVE owned Issue; a nil value is a refusal.
//
// citations are the agent's approval citations for THIS verification, applied to
// every live owned Issue - the same set restapi's verifyApprovalScope offers each
// Issue in the scope loop.
//
// metrics may be nil; when set, an auto-approval TRANSITION (the last human gate
// removed) increments operator_auto_approve_total{kind} so the path is queryable
// without log-scraping (hard rule 13), and every refusal increments
// operator_approval_refused_total{reason}.
func VerifyApprovalDetailed(ctx context.Context, c client.Client, sp objbudget.Spiller,
	proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task,
	citations []tatarav1alpha1.ApprovalCitation, metrics *obs.OperatorMetrics) (
	map[string]*tatarav1alpha1.ApprovalEvidence, map[string]string, error) {
	l := log.FromContext(ctx)
	evidence := make(map[string]*tatarav1alpha1.ApprovalEvidence, len(task.Status.IssueRefs))
	refusals := make(map[string]string, len(task.Status.IssueRefs))

	botLogin := ""
	if proj.Spec.Scm != nil {
		botLogin = proj.Spec.Scm.BotLogin
	}

	for _, name := range task.Status.IssueRefs {
		var iss tatarav1alpha1.Issue
		if err := c.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: name}, &iss); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, nil, fmt.Errorf("approval: get issue %s: %w", name, err)
		}
		if !approvalInScope(&iss) {
			continue
		}

		repo := approvalRepo(ctx, c, &iss)
		ev, reason := verifyOneIssue(&iss, proj, repo, botLogin, citations)
		if reason != "" {
			evidence[name] = nil
			refusals[name] = reason
			l.Info("approval refused",
				"action", "approval_refused", "task", task.Name, "issue", name, "reason", reason)
			metrics.ApprovalRefused(reason)
			continue
		}

		// A NEWLY DERIVED evidence is persisted; an already-approved Issue
		// short-circuits inside verifyOneIssue with its stored evidence and needs
		// no re-write (clause 2 idempotency, autoApprove and single-use liveness).
		if ev != nil && (iss.Status.Status != "approved" || iss.Status.Approval == nil) {
			key := types.NamespacedName{Namespace: iss.Namespace, Name: iss.Name}
			if err := objbudget.FitIssue(ctx, c, sp, key, func(cur *tatarav1alpha1.Issue) {
				cur.Status.Approval = ev.DeepCopy()
				cur.Status.Status = "approved"
			}); err != nil {
				return nil, nil, fmt.Errorf("approval: record evidence on %s: %w", name, err)
			}
			l.Info("approval verified",
				"action", "approval_verified", "task", task.Name, "issue", name,
				"maintainer_login", ev.Login, "cited_comment_id", ev.CommentID,
				"auto", ev.Auto)
			if ev.Auto && metrics != nil {
				if kind := tatarav1alpha1.ProposalKindFromBody(iss.Status.Body); kind != "" {
					metrics.AutoApproveTotal(kind)
				}
			}
		}
		evidence[name] = ev
	}

	if err := applyApprovalStage(ctx, c, sp, task, ApprovalPassed(evidence)); err != nil {
		return nil, nil, err
	}
	return evidence, refusals, nil
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

// applyApprovalStage moves the Task across the ONE edge the gate owns.
// clarifying -> approved on a pass; approved -> clarifying when the gate no
// longer holds (approval is NOT sticky: an agent cannot widen its own mandate by
// adopting work after the gate). Every other stage - notably parked - is left
// alone: stage.Unpark owns the un-park edge, and as of
// agent-judged-approval-gate step C it takes no verdict at all -
// stage.UnparkInput has no verdict field left to carry one, so nothing this gate
// computes can move a parked Task.
func applyApprovalStage(ctx context.Context, c client.Client, sp objbudget.Spiller,
	task *tatarav1alpha1.Task, passed bool) error {
	var to string
	switch {
	case passed && task.Status.Stage == tatarav1alpha1.StageClarifying:
		to = tatarav1alpha1.StageApproved
	case !passed && task.Status.Stage == tatarav1alpha1.StageApproved:
		to = tatarav1alpha1.StageClarifying
	default:
		return nil
	}

	now := time.Now()
	var enterErr error
	key := types.NamespacedName{Namespace: task.Namespace, Name: task.Name}
	if err := objbudget.FitTask(ctx, c, sp, key, func(cur *tatarav1alpha1.Task) {
		enterErr = stage.Enter(cur, nil, to, "", now)
	}); err != nil {
		return fmt.Errorf("approval: enter %s on %s: %w", to, task.Name, err)
	}
	if enterErr != nil {
		return fmt.Errorf("approval: enter %s on %s: %w", to, task.Name, enterErr)
	}
	if err := c.Get(ctx, key, task); err != nil {
		return fmt.Errorf("approval: refresh task %s: %w", task.Name, err)
	}
	log.FromContext(ctx).Info("approval gate moved the task",
		"action", "approval_stage", "task", task.Name, "stage", to, "passed", passed)
	return nil
}

// ReVerifyParked is the C3-3 un-park path, and its ordering is MANDATORY: it
// SYNCS THAT ISSUE'S THREAD FROM THE FORGE FIRST (one forge read, on a human
// comment, on a parked Task - cheap), and only THEN runs the grammar.
//
// The mirror cadence for a parked Task is DAILY, clause (d) enforces single-use
// evidence against Comment.ExternalID, and a TaskEvent carries no externalId. So
// without the sync the grammar re-runs against a thread that does NOT contain the
// comment that triggered it, and silently fails - restoring the exact 7-day dead
// end this redesign removes.
//
// It returns the grammar verdict, which the caller USED TO feed to stage.Unpark
// as UnparkInput.GrammarPassed. As of agent-judged-approval-gate step A it has
// NO CALLER AT ALL - the webhook limb that called it is gone, step B removed the
// reader of the field it fed, and step C removed the field itself - so the
// verdict it returns goes nowhere. Kept
// only until step 7 deletes it; do not wire a new caller to it. A BOT-authored
// event is refused before any forge read: the operator's own park comment can
// never un-park the Task it parked.
func ReVerifyParked(ctx context.Context, c client.Client, sp objbudget.Spiller, reader scm.SCMReader,
	proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task, ev tatarav1alpha1.TaskEvent,
	citations []tatarav1alpha1.ApprovalCitation, metrics *obs.OperatorMetrics) (bool, error) {
	passed, _, err := ReVerifyParkedDetailed(ctx, c, sp, reader, proj, task, ev, citations, metrics)
	return passed, err
}

// ReVerifyParkedDetailed is ReVerifyParked plus the EVIDENCE the pass was built
// from, keyed by Issue CR name. The caller USED TO persist that evidence as a
// durable Task.status.approvalVerdict, on the reasoning that the grammar verdict
// was the one input the periodic un-park backstop could never reconstruct. That
// field was deleted in agent-judged-approval-gate step D and the evidence now
// goes nowhere: this function is test-only, like ReVerifyParked above it.
func ReVerifyParkedDetailed(ctx context.Context, c client.Client, sp objbudget.Spiller, reader scm.SCMReader,
	proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task, ev tatarav1alpha1.TaskEvent,
	citations []tatarav1alpha1.ApprovalCitation,
	metrics *obs.OperatorMetrics) (bool, map[string]*tatarav1alpha1.ApprovalEvidence, error) {

	botLogin := ""
	if proj.Spec.Scm != nil {
		botLogin = proj.Spec.Scm.BotLogin
	}
	if ev.Author == "" || (botLogin != "" && ev.Author == botLogin) {
		return false, nil, nil
	}
	if ev.Kind == "issue_comment" && ev.Repo != "" && ev.Number > 0 {
		key := IssueKey(ev.Repo, ev.Number)
		if TaskOwnsIssue(task, ev.Repo, ev.Number) {
			if err := SyncIssueOnDemand(ctx, c, sp, reader, proj, key); err != nil {
				return false, nil, fmt.Errorf("approval: on-demand sync of %s: %w", key, err)
			}
		}
	}
	evidence, _, err := VerifyApprovalDetailed(ctx, c, sp, proj, task, citations, metrics)
	if err != nil {
		return false, nil, err
	}
	return ApprovalPassed(evidence), evidence, nil
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
