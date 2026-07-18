// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// THE APPROVAL GATE (contract C.6). PRESENCE IS NOT CONSENT; TEXT IS.
//
// The pre-redesign approvingMaintainer() returned a maintainer-authored comment
// WITHOUT READING IT, so a maintainer saying "I can't approve this until the
// tests pass" released the autonomous implement -> review -> merge -> deploy
// chain. The grammar below is the fix, and every clause of it is load-bearing:
//
//	a. the author is a maintainer AND is structurally NOT the bot
//	b. it is the MOST RECENT maintainer-authored comment on that thread
//	c. an ANCHORED WHOLE-LINE match: the comment CONSISTS OF an approval phrase
//	d. the evidence is SINGLE-USE: a consumed commentId cannot approve twice
//
// and the SCOPE is EVERY LIVE owned Issue (state=open, status not in
// done/rejected), never just one: one "lgtm" on one issue must not approve a
// Task spanning every repo in mergeOrder.
//
// A negation BLOCKLIST was rejected outright: blocklists lose. That is the same
// argument that makes C.7's close-directive filter an ALLOWLIST.

// Approval refusal reasons. They name what was MISSING, and they are what the
// operator's park comment reports back to the human.
const (
	// ApprovalRefusedNoMaintainer: clauses (a)/(b) - no maintainer-authored,
	// non-bot comment on the thread at all.
	ApprovalRefusedNoMaintainer = "no-maintainer-comment"
	// ApprovalRefusedNoPhrase: clause (c) - the MOST RECENT maintainer comment
	// does not CONSIST OF an approval phrase.
	ApprovalRefusedNoPhrase = "no-approval-phrase"
	// ApprovalRefusedEvidenceReplayed: clause (d) - that comment was already
	// consumed as evidence once.
	ApprovalRefusedEvidenceReplayed = "evidence-replayed"
)

var (
	// approvalFenceRe opens/closes a fenced code block. A phrase inside a fence
	// is a QUOTATION of the grammar, not an utterance of it.
	approvalFenceRe = regexp.MustCompile("^\\s*(```|~~~)")
	// approvalQuoteRe is a markdown block quote: quoting the bot's own park
	// comment back at it must not approve.
	approvalQuoteRe = regexp.MustCompile(`^\s*>`)
	// approvalShortcodeRe strips TRAILING emoji shortcodes (":rocket:").
	approvalShortcodeRe = regexp.MustCompile(`(?:\s*:[a-z0-9_+-]+:)+\s*$`)
)

// isApprovalEmojiRune reports whether r is a trailing decoration rather than
// text: emoji planes, the symbol/arrow/dingbat blocks, and the joiners.
func isApprovalEmojiRune(r rune) bool {
	switch {
	case r >= 0x1F000 && r <= 0x1FAFF:
		return true
	case r >= 0x2190 && r <= 0x2BFF:
		return true
	case r == 0xFE0F || r == 0x200D || r == 0x20E3:
		return true
	}
	return false
}

// stripApprovalEmphasis removes the markdown emphasis delimiters (* _ `) that
// wrap a run (fix L3-13). It is NOT cosmetic: "**LGTM**" is how humans actually
// write the single most common approval token, and without this it fails the
// anchor and drops the Task into the identity-unverified dead end. An
// intra-word underscore (snake_case) is kept: it is text, not emphasis.
func stripApprovalEmphasis(s string) string {
	runes := []rune(s)
	var b strings.Builder
	for i, r := range runes {
		if r != '*' && r != '_' && r != '`' {
			b.WriteRune(r)
			continue
		}
		if r == '_' && i > 0 && i < len(runes)-1 &&
			isApprovalWordRune(runes[i-1]) && isApprovalWordRune(runes[i+1]) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func isApprovalWordRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r)
}

// normalizeApprovalLines is the C.6 clause (c) normalisation, in the contract's
// order: lowercase, strip fenced code blocks, strip quoted lines, strip
// markdown emphasis, strip trailing emoji and trailing whitespace.
func normalizeApprovalLines(body string) []string {
	var out []string
	inFence := false
	for _, raw := range strings.Split(strings.ToLower(body), "\n") {
		line := strings.TrimRight(raw, "\r")
		if approvalFenceRe.MatchString(line) {
			inFence = !inFence
			continue
		}
		if inFence || approvalQuoteRe.MatchString(line) {
			continue
		}
		line = stripApprovalEmphasis(line)
		line = trimApprovalTrailer(line)
		line = approvalShortcodeRe.ReplaceAllString(line, "")
		line = trimApprovalTrailer(line)
		out = append(out, line)
	}
	return out
}

func trimApprovalTrailer(s string) string {
	return strings.TrimRightFunc(s, func(r rune) bool {
		return unicode.IsSpace(r) || isApprovalEmojiRune(r)
	})
}

// MatchesApprovalPhrase is the ANCHORED WHOLE-LINE matcher (C.6 clause c,
// USER DECISION D-B). SOME LINE of the normalised body must match
// ^\s*(<phrase>)[\s.!]*$: the comment must CONSIST OF the phrase, not contain
// it. It returns the matched phrase EXACTLY as the project configured it (it is
// what lands in ApprovalEvidence.Phrase).
//
// The phrase is quoted with regexp.QuoteMeta before interpolation: a project may
// configure a phrase carrying regex metacharacters and that must not become an
// injection into the operator's own gate.
func MatchesApprovalPhrase(body string, phrases []string) (string, bool) {
	lines := normalizeApprovalLines(body)
	if len(lines) == 0 {
		return "", false
	}
	for _, phrase := range phrases {
		norm := strings.ToLower(strings.TrimSpace(phrase))
		if norm == "" {
			continue
		}
		re, err := regexp.Compile(`^\s*(` + regexp.QuoteMeta(norm) + `)[\s.!]*$`)
		if err != nil {
			continue
		}
		for _, line := range lines {
			if re.MatchString(line) {
				return phrase, true
			}
		}
	}
	return "", false
}

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

// ApprovalRefusedComment renders the comment the operator posts on an Issue
// whose approval grammar failed, naming what was missing. It is BOT-authored,
// so it can never un-park the Task it parked (E.3 enqueue filter + F.6).
func ApprovalRefusedComment(reason string, phrases []string) string {
	quoted := make([]string, 0, len(phrases))
	for _, p := range phrases {
		quoted = append(quoted, "`"+p+"`")
	}
	list := strings.Join(quoted, ", ")

	var missing string
	switch reason {
	case ApprovalRefusedNoMaintainer:
		missing = "no maintainer has commented on this thread"
	case ApprovalRefusedNoPhrase:
		missing = "the most recent maintainer comment is not an approval"
	case ApprovalRefusedEvidenceReplayed:
		missing = "the most recent maintainer comment was already used to approve this issue once"
	default:
		missing = "the approval could not be verified"
	}
	return fmt.Sprintf("tatara: I am not starting work on this yet - %s. "+
		"A maintainer can approve it with a comment that CONSISTS OF one of: %s "+
		"(the comment must be the phrase alone, not a sentence containing it).", missing, list)
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

// mostRecentMaintainerComment is C.6 clauses (a) and (b): the LATEST comment on
// the thread whose author is a verified maintainer and is structurally NOT the
// bot. The bot exclusion runs BEFORE IsMaintainer, so a bot login misconfigured
// into maintainerLogins still cannot approve.
func mostRecentMaintainerComment(iss *tatarav1alpha1.Issue, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, botLogin string) *tatarav1alpha1.Comment {
	var best *tatarav1alpha1.Comment
	for i := range iss.Status.Comments {
		c := &iss.Status.Comments[i]
		if c.IsBot || c.Author == "" || (botLogin != "" && c.Author == botLogin) {
			continue
		}
		if !tatarav1alpha1.IsMaintainer(proj, repo, c.Author) {
			continue
		}
		if best == nil || !c.CreatedAt.Time.Before(best.CreatedAt.Time) {
			best = c
		}
	}
	return best
}

// verifyOneIssue is the C.6 per-Issue grammar body, and it is the SINGLE
// definition of the per-Issue verdict - shared by VerifyApprovalDetailed's
// per-Task scope loop and by GrammarVerifier (the production restapi
// ApprovalVerifier the /outcome clarify path calls). It is PURE: it derives the
// verdict and NEVER writes. The caller (grammar loop OR outcome.go) persists.
//
// The caller has already established the Issue is in scope (approvalInScope).
// An Issue that ALREADY CARRIES VALID EVIDENCE is approved - clause (2) asks
// whether every live Issue CARRIES evidence, not whether it can be re-derived
// from the thread right now. That idempotence keeps the autoApproveTataraProposals
// path (ApprovalEvidence{Auto: true, CommentID: ""}) alive and stops a
// maintainer's later "thanks!" from REVOKING an approval already given.
func verifyOneIssue(iss *tatarav1alpha1.Issue, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, phrases []string, botLogin string) (*tatarav1alpha1.ApprovalEvidence, string) {
	if iss.Status.Status == "approved" && iss.Status.Approval != nil {
		return iss.Status.Approval, ""
	}
	cmt := mostRecentMaintainerComment(iss, proj, repo, botLogin)
	if cmt == nil {
		// THE AUTO-APPROVE CARVE-OUT (autoApproveTataraProposals). It sits ONLY in
		// the no-maintainer-comment arm on purpose: it fires when the bot proposed
		// the work and no human has spoken, but a maintainer who DID comment
		// something that is not an approval still falls through to
		// ApprovalRefusedNoPhrase below and blocks the release. It is fail-closed on
		// every axis (flag / bot-authorship / marker / scope); see autoApproveApplies.
		if autoApproveApplies(iss, proj, botLogin) {
			return autoApprovalEvidence(), ""
		}
		return nil, ApprovalRefusedNoMaintainer
	}
	phrase, ok := MatchesApprovalPhrase(cmt.Body, phrases)
	if !ok {
		return nil, ApprovalRefusedNoPhrase
	}
	if iss.Status.Approval != nil && iss.Status.Approval.CommentID == cmt.ExternalID {
		// Clause (d), SINGLE-USE: this comment was consumed as evidence once
		// already and the Issue is no longer approved. It cannot approve a second
		// time; a later approval must cite a NEWER comment.
		return nil, ApprovalRefusedEvidenceReplayed
	}
	return &tatarav1alpha1.ApprovalEvidence{
		Login:     cmt.Author,
		CommentID: cmt.ExternalID,
		CreatedAt: cmt.CreatedAt,
		Phrase:    phrase,
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
//  4. the body carries a valid tatara-proposed-by marker (brainstorm / incident).
//     A missing or malformed marker fails closed.
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
	return tatarav1alpha1.ProposalKindFromBody(iss.Status.Body) != ""
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
// implement anything. It runs the C.6 per-Issue grammar against the Issue CR's
// MIRRORED comments - the same grammar VerifyApprovalDetailed runs per Task - so
// the REST clarify path verifies a real maintainer approval rather than failing
// closed. It is a pure READER; outcome.go persists the returned evidence.
type GrammarVerifier struct {
	Client client.Client
}

// VerifyApproval implements the restapi.ApprovalVerifier seam for ONE owned
// Issue. An out-of-scope (closed / done / rejected) Issue is not pending
// approval and never blocks the scope check: it passes with whatever evidence it
// already carries, mirroring VerifyApprovalDetailed's skip.
func (g *GrammarVerifier) VerifyApproval(ctx context.Context, proj *tatarav1alpha1.Project,
	iss *tatarav1alpha1.Issue) (*tatarav1alpha1.ApprovalEvidence, bool) {
	if !approvalInScope(iss) {
		return iss.Status.Approval, true
	}
	botLogin := ""
	if proj.Spec.Scm != nil {
		botLogin = proj.Spec.Scm.BotLogin
	}
	repo := approvalRepo(ctx, g.Client, iss)
	ev, reason := verifyOneIssue(iss, proj, repo, tatarav1alpha1.EffectiveApprovalPhrases(proj), botLogin)
	if reason != "" {
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
// takes this function's verdict as UnparkInput.GrammarPassed.
func VerifyApproval(ctx context.Context, c client.Client, sp objbudget.Spiller,
	proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (map[string]*tatarav1alpha1.ApprovalEvidence, error) {
	evidence, _, err := VerifyApprovalDetailed(ctx, c, sp, proj, task)
	return evidence, err
}

// VerifyApprovalDetailed is VerifyApproval plus the per-issue refusal reason the
// operator's park comment names (ApprovalRefusedComment). The evidence map has an
// entry for every LIVE owned Issue; a nil value is a refusal.
func VerifyApprovalDetailed(ctx context.Context, c client.Client, sp objbudget.Spiller,
	proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (
	map[string]*tatarav1alpha1.ApprovalEvidence, map[string]string, error) {
	l := log.FromContext(ctx)
	evidence := make(map[string]*tatarav1alpha1.ApprovalEvidence, len(task.Status.IssueRefs))
	refusals := make(map[string]string, len(task.Status.IssueRefs))

	phrases := tatarav1alpha1.EffectiveApprovalPhrases(proj)
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
		ev, reason := verifyOneIssue(&iss, proj, repo, phrases, botLogin)
		if reason != "" {
			evidence[name] = nil
			refusals[name] = reason
			l.Info("approval refused",
				"action", "approval_refused", "task", task.Name, "issue", name, "reason", reason)
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
				"maintainer_login", ev.Login, "comment_external_id", ev.CommentID,
				"matched_phrase", ev.Phrase, "auto", ev.Auto)
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
// alone: stage.Unpark owns the un-park edge and takes the verdict as
// GrammarPassed.
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
// It returns the grammar verdict, which the caller feeds to stage.Unpark as
// UnparkInput.GrammarPassed. A BOT-authored event is refused before any forge
// read: the operator's own park comment can never un-park the Task it parked.
func ReVerifyParked(ctx context.Context, c client.Client, sp objbudget.Spiller, reader scm.SCMReader,
	proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task, ev tatarav1alpha1.TaskEvent) (bool, error) {
	botLogin := ""
	if proj.Spec.Scm != nil {
		botLogin = proj.Spec.Scm.BotLogin
	}
	if ev.Author == "" || (botLogin != "" && ev.Author == botLogin) {
		return false, nil
	}
	if ev.Kind == "issue_comment" && ev.Repo != "" && ev.Number > 0 {
		key := IssueKey(ev.Repo, ev.Number)
		if approvalOwnsIssue(task, ev.Repo, ev.Number) {
			if err := SyncIssueOnDemand(ctx, c, sp, reader, proj, key); err != nil {
				return false, fmt.Errorf("approval: on-demand sync of %s: %w", key, err)
			}
		}
	}
	evidence, err := VerifyApproval(ctx, c, sp, proj, task)
	if err != nil {
		return false, err
	}
	return ApprovalPassed(evidence), nil
}

// approvalOwnsIssue reports whether the Task owns the Issue the event landed on.
// An event on a thread this Task does not own buys no forge read.
func approvalOwnsIssue(task *tatarav1alpha1.Task, repoRef string, number int) bool {
	want := tatarav1alpha1.IssueName(repoRef, number)
	for _, name := range task.Status.IssueRefs {
		if name == want {
			return true
		}
	}
	return false
}
