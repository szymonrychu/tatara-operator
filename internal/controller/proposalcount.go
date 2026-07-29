package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// systemicLabelPrefix marks a multi-repo proposal group. Issues sharing one
// count as a SINGLE backlog slot so a systemic improvement filed against three
// repos does not consume three slots of the target.
const systemicLabelPrefix = "tatara/systemic-"

// issueAuthoredByBot is the STRUCTURAL bot check, the same shape the approval
// citation check and the pendingEvents enqueue filter rely on (mirror.go
// mirrorCommentFrom): an EMPTY author, or an unconfigured bot login, is NEVER the
// bot. Both halves must be non-empty, so it fails CLOSED.
func issueAuthoredByBot(iss *tatarav1alpha1.Issue, botLogin string) bool {
	return botLogin != "" && iss.Status.Author != "" && iss.Status.Author == botLogin
}

// effectiveProposalKind is the provenance of iss: the Spec stamp when set,
// otherwise the in-body marker on a BOT-AUTHORED issue.
//
// The precedence is EXACT and load-bearing. Spec.ProposalKind is the
// integrity-bearing field - the mirror only ever writes Status, so nothing
// SCM-side can reach it - and it therefore WINS whenever it is set, with no
// authorship corroboration needed. The body marker lives in the forge-editable
// body and is consulted ONLY on an unstamped Issue, where there is no stamped
// value to override; a forge-side body edit can never flip or clear a value the
// operator already wrote.
//
// The fallback is the ROLLOUT property, not a convenience. Every proposal open
// when this build ships was minted before the Spec field existed: it carries the
// tatara-proposed-by marker in its mirrored body and nothing in Spec. Reading
// only Spec would make pendingProposalCount zero for every project on the first
// deploy, so the deficit would be the full target on top of a backlog that is
// already full. The reconciler backfill (issue_controller.go stampProposalKind)
// converts those to stamped Issues as they reconcile, so this branch is
// self-liquidating rather than a permanent second source of truth.
//
// The fallback carries TWO corroborating gates, both Spec-side or forge-immutable,
// because the marker itself is neither.
//
//  1. BOT AUTHORSHIP. Anyone with forge write access to ANY issue on a tracked
//     repo can paste "<!-- tatara-proposed-by:brainstorm -->" into a body they
//     control. Ungated, that alone would buy a permanent backlog slot and
//     suppress legitimate refills (a refill-cadence DoS). An attacker who can
//     edit a body cannot change its author, and a genuine proposal is ALWAYS
//     filed under the bot account.
//
//  2. A NON-EMPTY ProposalBodyHash. Bot authorship alone is not enough, because
//     mintIssueCR files AGENT-authored issues (issue_write action=create) under
//     the bot account too, and that body is agent-written - a prompt-injection
//     surface. The anchor is written by mintIssueCR only when the CALLER declared
//     a proposal kind, so its presence means "the operator declared this a
//     proposal", which is the exact thing being asserted. This costs the
//     migration path NOTHING: the anchor and the marker were introduced in the
//     same commit (f83e8f3), so every proposal that carries a marker carries an
//     anchor too. Presence only - never a MATCH, which would hand a body editor a
//     way to deflate the count.
//
// A stamped Spec.ProposalKind needs neither gate: Spec is unreachable from the
// forge and is only ever written by the operator.
func effectiveProposalKind(iss *tatarav1alpha1.Issue, botLogin string) string {
	if iss.Spec.ProposalKind != "" {
		return iss.Spec.ProposalKind
	}
	if !issueAuthoredByBot(iss, botLogin) || iss.Spec.ProposalBodyHash == "" {
		return ""
	}
	return tatarav1alpha1.ProposalKindFromBody(iss.Status.Body)
}

// proposalPending reports whether iss is a brainstorm proposal still AWAITING a
// maintainer decision. This is the counting predicate for the backlog target.
//
// It reads the Issue CR mirror in etcd, NEVER the forge. That is the whole point
// (design section 2): webhook delivery lag, lost deliveries and search-index
// staleness only bite a controller that counts by calling the forge, and the CR
// mirror is already the operator's strongly consistent source of truth.
//
// An APPROVED proposal is deliberately not pending: it is no longer awaiting a
// decision, so it frees its slot immediately even though its forge issue stays
// open through implementation. That is what makes "maintainer approves -> next
// brainstorm launches" literally true.
//
// botLogin is Project.spec.scm.botLogin, threaded in by the caller so the
// forgeable body-marker fallback has an unforgeable authorship anchor. It is
// deliberately a plain string and not the Project: this predicate makes no API
// call and no SCM call.
func proposalPending(iss *tatarav1alpha1.Issue, botLogin string) bool {
	if effectiveProposalKind(iss, botLogin) != tatarav1alpha1.ProposalKindBrainstorm {
		return false
	}
	if iss.Status.State != "open" {
		return false
	}
	switch iss.Status.Status {
	case "approved", "rejected", "done":
		return false
	}
	return true
}

// proposalDisplayStatus derives the THREE display statuses the <proposal_history>
// block renders. Issue.Status.Status alone cannot distinguish "discarded" from
// "never triaged", so a closed-and-not-approved issue reads as declined: a
// maintainer who just closes the issue has discarded the proposal.
//
// "done" MUST be folded into approved and MUST be tested before the closed
// check. CloseIssuesOnDelivery (merge.go) stamps a SHIPPED proposal
// State="closed" Status="done", which would otherwise fall through to the
// closed arm and render every successfully delivered proposal as "declined" for
// the whole of DeliveredRetention - at the TOP of the newest-first window, where
// the pod prompt tells the agent a declined proposal is a killed idea. The agent
// would read "we built and shipped this" as "the maintainer killed this" and
// infer the exact opposite maintainer preference from the block whose entire
// purpose is carrying verdict rationale. The rest of the branch already knows
// done is not a decline (retainProposalDecline, proposalPending); only this
// mapping missed it.
//
// The three-status contract (open|approved|declined) is FROZEN: the shipped
// tatara-agent-skills text on the other side consumes exactly these three, so a
// fourth status for "delivered" is not available here.
func proposalDisplayStatus(iss *tatarav1alpha1.Issue) string {
	if iss.Status.Status == "approved" || iss.Status.Status == "done" {
		return "approved"
	}
	if iss.Status.Status == "rejected" || iss.Status.State == "closed" {
		return "declined"
	}
	return "open"
}

// systemicGroup returns the tatara/systemic-<id> label on iss, or "".
func systemicGroup(iss *tatarav1alpha1.Issue) string {
	for _, l := range iss.Status.Labels {
		if strings.HasPrefix(l, systemicLabelPrefix) {
			return l
		}
	}
	return ""
}

// pendingProposalCount is the project-wide backlog level the control law
// compares against the target. Systemic groups collapse to one slot each.
func pendingProposalCount(issues []tatarav1alpha1.Issue, botLogin string) int {
	groups := map[string]bool{}
	standalone := 0
	for i := range issues {
		if !proposalPending(&issues[i], botLogin) {
			continue
		}
		if g := systemicGroup(&issues[i]); g != "" {
			groups[g] = true
			continue
		}
		standalone++
	}
	return standalone + len(groups)
}

// pendingProposalCountByRepo is the per-repo split that feeds the existing
// operator_open_proposals{repo} gauge. It is deliberately NOT systemic-collapsed:
// a group spans repos, so collapsing it would attribute its one slot to an
// arbitrary repo. The project-wide gauge (operator_brainstorm_pending_proposals)
// carries the collapsed number.
func pendingProposalCountByRepo(issues []tatarav1alpha1.Issue, botLogin string) map[string]int {
	out := map[string]int{}
	for i := range issues {
		if proposalPending(&issues[i], botLogin) {
			out[issues[i].Spec.RepositoryRef]++
		}
	}
	return out
}

// projectProposalIssues lists every Issue CR in the project's namespace whose
// spec.projectRef names proj. Plain List + filter, matching ownedIssueCRs
// (merge.go): the Issue CR set per namespace is small and there is no
// projectRef field index to piggyback on.
//
// It logs nothing on purpose. Listing is not a business action; the refill
// DECISION is, and brainstorm() logs that one with the full input set
// (target/pending/inflight/deficit/trigger/reason). A second INFO line here
// would fire on every list and read as a decision that was never made.
func (r *ProjectReconciler) projectProposalIssues(ctx context.Context, proj *tatarav1alpha1.Project) ([]tatarav1alpha1.Issue, error) {
	var list tatarav1alpha1.IssueList
	if err := r.List(ctx, &list, client.InNamespace(proj.Namespace)); err != nil {
		return nil, fmt.Errorf("brainstorm: list issues for %s: %w", proj.Name, err)
	}
	out := make([]tatarav1alpha1.Issue, 0, len(list.Items))
	for i := range list.Items {
		if list.Items[i].Spec.ProjectRef == proj.Name {
			out = append(out, list.Items[i])
		}
	}
	return out, nil
}

// brainstormDeficit is the control law:
//
//	deficit = max(0, target - pending - inflight)
//
// It is CLAMPED AT 0 on purpose. When pending exceeds the target - after
// lowering N, or a long stretch with no maintainer verdicts - the controller
// stops refilling and lets the backlog drain naturally. It NEVER closes
// proposals to reconcile downward: autonomously destroying work product a human
// has not read is out of scope, and a level-triggered controller in both
// directions is the wrong shape when the "replicas" are human-reviewable
// artifacts.
func brainstormDeficit(target, pending, inflight int) int {
	return max(target-pending-inflight, 0)
}

// brainstormRefillDecision is the whole control law. quota is the number of
// proposals the session may file, clamped to [1, MaxProposalsPerOutcome] when
// refill is true. reason is "" when refilling, otherwise the log-and-metric
// reason the cycle was suppressed.
//
// There is no trigger-dependent branch and no circuit breaker any more. The
// breaker suppressed the EVENT path once consecutiveSkips crossed a threshold
// and ONLY a cron tick reset it, which made the two mechanisms load-bearing for
// each other: the breaker wedged the fast path and the cron was the only
// unwedge. Worse, it counted CORRECT behaviour - an agent reporting "nothing
// worth proposing" - toward a brake, so a working system switched its own fast
// path off. Pause is now an explicit state an agent asks for (action=exhausted,
// internal/restapi/outcome.go) and five triggers clear (Task 4), not an
// inference from a counter.
//
// paused is proj.Status.BrainstormPausedAt != nil at the call site. It is
// passed as a bool rather than the Project so this stays a pure function with a
// table test.
//
// THE COOLDOWN GATE (C2 fix round). paused/exhausted is LLM judgment and must
// not be the only defense against a busy loop: a skip files no Issue, so the
// deficit this law reacts to stays positive, the Task still reaches a terminal
// stage, the brainstormCycleFinishedPredicate wake fires immediately, and this
// law would recompute the same positive deficit and refill again - with no
// floor, back to back, bounded only by the 30s self-requeue. now/lastBrainstorm
// give this a durable, trigger-agnostic floor: when a prior session is known
// (lastBrainstorm != nil) and less than act.ResolveMinSessionInterval() has
// elapsed since it, refill is refused with the distinct reason "cooling-down"
// so it is observable apart from "paused" and "at-target". lastBrainstorm==nil
// (no session has ever run) never gates - the floor delays the NEXT session,
// it does not hold up the first one. This is a RATE LIMIT, not a breaker: it
// never inspects why the prior session ended and it never suppresses
// permanently, only until the floor elapses.
func brainstormRefillDecision(act tatarav1alpha1.BrainstormActivity,
	pending, inflight int, paused bool, now time.Time, lastBrainstorm *time.Time) (quota int, refill bool, reason string) {

	if paused {
		return 0, false, "paused"
	}
	if lastBrainstorm != nil {
		if minInterval := act.ResolveMinSessionInterval(); minInterval > 0 && now.Sub(*lastBrainstorm) < minInterval {
			return 0, false, "cooling-down"
		}
	}
	deficit := brainstormDeficit(act.ResolveTarget(), pending, inflight)
	if deficit <= 0 {
		return 0, false, "at-target"
	}
	return min(deficit, tatarav1alpha1.MaxProposalsPerOutcome), true, ""
}
