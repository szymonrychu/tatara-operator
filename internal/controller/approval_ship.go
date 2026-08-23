// Copyright 2026 tatara authors.

package controller

import (
	"context"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// THE SHIP GATE (#639). The APPROVAL gate - submit_outcome(action=approved),
// verifyApprovalScope, verifyOneIssue - decides whether an Issue may be worked
// on. Nothing decided whether a change may LEAVE: mrOpen checked head-branch,
// idempotency and taskOwesOpenWork, and submit_outcome(action=submitted) checked
// only the plan hash. Neither read Issue.status.approval at all, so an implement
// agent that never called the gate still opened the PR, and the reviewer and the
// merge corridor downstream have no approval check of their own.
//
// This file is that read, and it is deliberately a SEPARATE decision from the
// grant. The grant asks "may this agent start?"; the ship asks "is the approval
// that authorised this work still good enough to release it?" - which is where
// the severity ceiling can be enforced at all, because change_significance does
// not exist on the wire until action=submitted.

const (
	// ShipBlockedNeedsMaintainerComment: the Issue carries no approval evidence
	// and no maintainer has said anything on the thread. The remedy is a HUMAN,
	// not a tool call, and no amount of retrying reaches it.
	ShipBlockedNeedsMaintainerComment = "needs-maintainer-comment"
	// ShipBlockedNeedsApprovalTool: the Issue carries no approval evidence but a
	// maintainer HAS commented. Everything needed to open the gate is already in
	// the agent's turn-0 bundle; it simply has not called the tool. This is the
	// #639 self-approval shape inverted - the agent skipping the gate rather than
	// the operator skipping the human - and it is the one blocker whose remedy is
	// entirely in the agent's hands.
	ShipBlockedNeedsApprovalTool = "needs-approval-tool"
	// ShipBlockedOverCeiling: the Issue was auto-approved - no human ever spoke -
	// and the change the agent now declares is larger than the project's
	// autoApproveMaxSignificance. The grant was PROVISIONAL and this is where it
	// is settled. Remedy: a maintainer comment plus a real citation, or a smaller
	// change.
	ShipBlockedOverCeiling = "over-auto-approve-ceiling"
)

// ShipBlockedDetails is the CLOSED vocabulary, in the order ApprovalShipVerdict
// evaluates it. ShipBlockerGuidance is total over it, pinned by test.
var ShipBlockedDetails = []string{
	ShipBlockedNeedsMaintainerComment,
	ShipBlockedNeedsApprovalTool,
	ShipBlockedOverCeiling,
}

// ShipBlocker is ONE live owned Issue that is holding the change back. Its JSON
// shape is the one tatara-cli's refusalGuidance already decodes for
// reason=pr-not-ready, so a new refusal renders in the agent's transcript with
// no client change beyond learning the reason string.
type ShipBlocker struct {
	// Repo is the Repository CR name - the same string the agent passes as
	// `repo` to mr_write and issue_write, so the remedy is directly actionable.
	Repo   string `json:"repo"`
	Number int    `json:"number"`
	// Detail is a ShipBlockedDetails member.
	Detail string `json:"detail"`
	// Guidance is the sentence that tells the agent what to DO. It rides the
	// blocker rather than being looked up client-side because the cli is not the
	// trust boundary and must not hold a second copy of this vocabulary.
	Guidance string `json:"guidance"`
}

// ApprovalShipVerdict returns one ShipBlocker per LIVE owned Issue that is not
// clear to ship, and nil when the change may leave.
//
// significance is the agent's DECLARED change_significance, and it is EMPTY on
// the mr_write(action=open) path - the level does not exist on that wire. An
// empty significance therefore enforces the two evidence blockers and skips the
// ceiling; the ceiling bites at submit. That asymmetry is the design, not an
// oversight: refusing the OPEN would cost the agent its pushed branch for a
// value it has no way to supply yet, and the PR cannot merge without passing the
// submit check anyway.
//
// ZERO LIVE OWNED ISSUES IS ZERO BLOCKERS, and that is load-bearing. A takeover
// Task owns no Issue at all and was authorised more strictly than this gate
// would, at the takeover endpoint, by a maintainer comment the operator
// verified. An adopted upgrade Task owns a merge request and no Issue. Both must
// stay ungated; requiring evidence from an empty set would wedge them forever.
// The GRANT path has the opposite rule - verifyApprovalScope refuses an empty
// live set, because there an empty set means "nothing was approved" - and the
// two are different questions.
//
// OUT-OF-SCOPE ISSUES ARE FILTERED, not required to produce evidence, through
// the same ApprovalInScope predicate the grant uses. A human closing one Issue
// of a multi-issue Task must not strand the rest.
//
// THE UNIT IS THE TASK'S GATE, NOT EACH ISSUE, AND THAT DISTINCTION IS A WEDGE
// FIX. Requiring evidence on every live owned Issue reads as the stricter rule
// and is in fact unshippable: `issue_write(action=create)` is in the implement
// profile and the workflow skill tells the agent to file follow-ups for
// out-of-scope work it finds, mintIssueCR makes the CALLING Task the controller
// owner, and the new Issue is seeded live with no approval and no comments. That
// Issue then blocks a Task the gate had already granted, and the remedy is
// unreachable: `action=approved` from `under-implementation` is not a legal
// transition, so the Task can never ship anything again.
//
// So the question asked is "did the gate run over this Task's scope?", answered
// by any live owned Issue carrying evidence. That is not a weaker test than it
// looks: verifyApprovalScope refuses the WHOLE request unless EVERY live Issue
// in scope produced evidence, so one Issue carrying evidence means the whole
// scope was granted at the moment of the grant. An Issue that arrives AFTER it
// is a late arrival, and nothing has ever re-gated a late arrival - the
// implement workflow's own ownership-is-not-approval rule says so and tells the
// agent to leave it out of scope. This function now agrees with that rule
// instead of contradicting it.
func ApprovalShipVerdict(ctx context.Context, c client.Client, proj *tatarav1alpha1.Project,
	issues []tatarav1alpha1.Issue, significance string) []ShipBlocker {

	botLogin := ""
	if proj != nil && proj.Spec.Scm != nil {
		botLogin = proj.Spec.Scm.BotLogin
	}

	live := make([]*tatarav1alpha1.Issue, 0, len(issues))
	granted := false
	for i := range issues {
		iss := &issues[i]
		if !ApprovalInScope(iss) {
			continue
		}
		live = append(live, iss)
		if iss.Status.Approval != nil {
			granted = true
		}
	}
	if len(live) == 0 {
		return nil
	}

	var out []ShipBlocker
	if !granted {
		// THE GATE NEVER RAN over this Task's scope. This is #639's headline
		// shape - the agent skipped the gate, or was refused by it and wrote the
		// code anyway - and every live Issue is named, because an agent told
		// about one of three fixes it and is refused again.
		for _, iss := range live {
			out = append(out, shipBlocker(iss, noEvidenceDetail(ctx, c, iss, proj, botLogin)))
		}
		return out
	}

	// The gate ran. What is left is the SEVERITY CEILING, and it applies only to
	// evidence the carve-out produced: a maintainer who read the plan and said go
	// ahead has already made the judgement the ceiling stands in for, so a cited
	// approval ships at any significance.
	for _, iss := range live {
		ev := iss.Status.Approval
		if ev == nil || !ev.Auto {
			continue
		}
		if tatarav1alpha1.AutoApproveOverCeiling(proj, significance) {
			out = append(out, shipBlocker(iss, ShipBlockedOverCeiling))
		}
	}
	return out
}

func shipBlocker(iss *tatarav1alpha1.Issue, detail string) ShipBlocker {
	return ShipBlocker{
		Repo:     iss.Spec.RepositoryRef,
		Number:   iss.Spec.Number,
		Detail:   detail,
		Guidance: ShipBlockerGuidance(detail),
	}
}

// noEvidenceDetail splits the no-evidence case in two, because the remedies
// differ: one needs a human and no amount of retrying reaches it, the other is a
// tool call the agent can make in the same turn. Collapsing them is what made
// the pre-#639 refusals unactionable.
func noEvidenceDetail(ctx context.Context, c client.Client, iss *tatarav1alpha1.Issue,
	proj *tatarav1alpha1.Project, botLogin string) string {

	repo := approvalRepo(ctx, c, iss)
	for j := range iss.Status.Comments {
		if isMaintainerComment(&iss.Status.Comments[j], proj, repo, botLogin) {
			return ShipBlockedNeedsApprovalTool
		}
	}
	return ShipBlockedNeedsMaintainerComment
}

// ShipBlockerGuidance is the closed map from a blocker to the sentence that
// tells the agent what to do about it. #639: every tool output, confirming or
// denying, must guide the agent through this process.
//
// THE DEFAULT ARM IS NOT DEAD CODE. A detail added to the vocabulary without a
// guidance entry would otherwise ship an empty string to the agent, which is the
// unactionable refusal this whole change exists to remove; the totality test
// catches that in CI and this arm catches it in production.
func ShipBlockerGuidance(detail string) string {
	switch detail {
	case ShipBlockedNeedsMaintainerComment:
		return "No maintainer has commented on this issue, so there is nothing to cite and no approval to " +
			"report. Post your plan on the thread and end the turn with " +
			"`submit_outcome(kind=implement, action=discuss, reason=...)`. Any code you write before a " +
			"maintainer comments is LOST: no merge request can be opened to carry it."
	case ShipBlockedNeedsApprovalTool:
		return "A maintainer HAS commented on this issue but you have not passed the gate. Read the thread, " +
			"and if the comment is a go-ahead call `submit_outcome(kind=implement, action=approved, " +
			"approving_maintainer=..., plan_note_id=..., approval_citations=[{id, quote}])` and wait for " +
			"`granted:true`. Until it returns granted, `mr_write(action=open)` stays refused."
	case ShipBlockedOverCeiling:
		return "This issue was auto-approved with no maintainer comment, and the change you declared is " +
			"larger than this project's autoApproveMaxSignificance ceiling. Auto-approval is provisional " +
			"at this size: ask on the thread with `submit_outcome(kind=implement, action=discuss, " +
			"reason=...)` and cite the maintainer's reply, or split the change down to the ceiling."
	default:
		return "This issue is not clear to ship. Read its approval state with `task_context()` and pass " +
			"the gate with `submit_outcome(kind=implement, action=approved, ...)` before opening a merge " +
			"request."
	}
}

// ApprovalRefusalGuidance is the closed map from a GATE refusal to the next
// step. It covers ApprovalRefusals plus the two reasons restapi can synthesise
// on the scope path, and it is total: an unknown reason still gets a next step.
//
// IT EXISTS BECAUSE THE REASON CONSTANT IS NOT A REMEDY. `no-maintainer-comment`
// and `plan-note-not-plan` need completely different actions - one waits for a
// human, the other re-writes a note and retries in the same turn - and an agent
// handed only the constant has historically guessed, spent the turn on
// action=discuss, and parked the Task on a problem it could have fixed itself
// (approval_grammar.go's autoApproveRefusal doc records exactly that incident).
func ApprovalRefusalGuidance(reason string) string {
	switch reason {
	case ApprovalRefusedNoMaintainer:
		return "No maintainer has commented on this issue, so there is nothing to cite. Post your plan on " +
			"the thread and end the turn with `action=discuss`. Do not write code: " +
			"`mr_write(action=open)` is refused until this gate grants, so nothing can carry it."
	case ApprovalRefusedNoCitation:
		return "A maintainer HAS commented, so a citation is required. Re-read the thread and send " +
			"approving_maintainer plus approval_citations=[{id, quote}] naming the comment you judged " +
			"to be the go-ahead, or use `action=discuss` if none of them is one."
	case ApprovalRefusedCitationNotMaintainer:
		return "The comment id you cited is not a verified non-bot maintainer's comment on this issue. " +
			"Take the id from the `<comment external_id=\"...\">` attribute in your own bundle; do not " +
			"re-crawl the forge, and never cite the bot's own comments."
	case ApprovalRefusedQuoteAbsent:
		return "Your quote does not occur in the comment body the operator holds. Copy a VERBATIM " +
			"substring of that comment - a paraphrase is refused as a fabrication."
	case ApprovalRefusedEvidenceReplayed:
		return "That comment was already spent as approval evidence. A later approval must cite a " +
			"DIFFERENT comment; if the maintainer has not said anything since, use `action=discuss`."
	case ApprovalRefusedNoLiveIssue:
		return "This task owns no live issue to approve - every one it owns is closed, done or rejected. " +
			"A human closing the issue is the strongest veto there is: finish with `action=declined` " +
			"naming that, rather than looking for something else to approve."
	case ApprovalRefusedApproverNotMaintainer:
		return "The approving_maintainer you declared is not a verified maintainer of this project. It " +
			"must be the LOGIN of the author of the comment you cited, and that author must be a " +
			"maintainer; it is a declaration that has to agree with the citation, never a second authority."
	case ApprovalRefusedApproverMismatch:
		return "The approving_maintainer you declared is not the author of the comment you cited. Set it " +
			"to that comment's author, or cite the comment the person you named actually wrote."
	case ApprovalRefusedPlanNoteMissing:
		return "This task has no plan note to pin. Write one with `task_note(kind=\"plan\", ...)`, keep " +
			"the id it returns, and send that id as plan_note_id."
	case ApprovalRefusedPlanNoteNotPlan:
		return "plan_note_id does not name the NEWEST note of kind `plan` - it names a note of another " +
			"kind, or a superseded plan. Write the plan you actually want approved as a fresh " +
			"`task_note(kind=\"plan\")` and send THAT id."
	case ApprovalRefusedPlanHashMismatch:
		return "The plan note changed after the grant, so the change you are submitting is not the one " +
			"that was approved. Say so on the thread, write a NEW `task_note(kind=\"plan\")`, and go " +
			"back through `submit_outcome(action=approved)` before submitting again."
	default:
		return "The gate refused. Re-read the issue thread, correct the citation or the plan note the " +
			"reason names, and retry - or use `action=discuss` to ask the maintainer. Do not write " +
			"code until this call returns granted:true."
	}
}

// ApprovalGrantGuidance is what the GRANT says. The grant used to say nothing:
// it returned a bare Task DTO with no `granted` key at all, while the prompt and
// the skill both told the agent to read one. An agent that cannot tell a grant
// from a refusal is an agent that guesses, and #639's ask is that BOTH answers
// guide it.
func ApprovalGrantGuidance() string {
	return "Gate open. `mr_write(action=open)` and `submit_outcome(action=submitted)` are now unblocked " +
		"for this task. Implement the plan note you were approved on, in full, in one change; do NOT " +
		"rewrite that note - the operator re-checks its hash at submit and a rewritten plan sends you " +
		"back here."
}
