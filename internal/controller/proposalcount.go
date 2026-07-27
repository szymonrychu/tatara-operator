package controller

import (
	"context"
	"fmt"
	"strings"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// systemicLabelPrefix marks a multi-repo proposal group. Issues sharing one
// count as a SINGLE backlog slot so a systemic improvement filed against three
// repos does not consume three slots of the target.
const systemicLabelPrefix = "tatara/systemic-"

// issueAuthoredByBot is the STRUCTURAL bot check, the same shape the approval
// grammar and the pendingEvents enqueue filter rely on (mirror.go
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
// The AUTHORSHIP gate on that fallback is a security control, not a filter.
// Anyone with forge write access to ANY issue on a tracked repo can paste
// "<!-- tatara-proposed-by:brainstorm -->" into a body they control. Ungated,
// that alone would buy a permanent backlog slot and suppress legitimate refills
// (a refill-cadence DoS; it can never reach auto-approve, which fails closed on
// the absent ProposalBodyHash anchor). An attacker who can edit a body cannot
// change its author, and a genuine proposal is ALWAYS filed under the bot
// account, so requiring bot authorship closes the vector while leaving the
// migration path for real pre-existing proposals fully intact.
func effectiveProposalKind(iss *tatarav1alpha1.Issue, botLogin string) string {
	if iss.Spec.ProposalKind != "" {
		return iss.Spec.ProposalKind
	}
	if !issueAuthoredByBot(iss, botLogin) {
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
func proposalDisplayStatus(iss *tatarav1alpha1.Issue) string {
	if iss.Status.Status == "approved" {
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

// resetBrainstormSkips zeroes the skip breaker. The cron tick is the ONLY thing
// that re-enables the event-driven refill path after the breaker trips, so this
// is the liveness guarantee of the whole design: a dry idea space costs one
// session per cron period instead of wedging brainstorm forever.
func (r *ProjectReconciler) resetBrainstormSkips(ctx context.Context, proj *tatarav1alpha1.Project) error {
	if proj.Status.BrainstormConsecutiveSkips == 0 {
		return nil
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Project{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: proj.Namespace, Name: proj.Name}, fresh); err != nil {
			return fmt.Errorf("brainstorm: get project %s: %w", proj.Name, err)
		}
		if fresh.Status.BrainstormConsecutiveSkips == 0 {
			proj.Status.BrainstormConsecutiveSkips = 0
			return nil
		}
		fresh.Status.BrainstormConsecutiveSkips = 0
		proj.Status.BrainstormConsecutiveSkips = 0
		return r.Status().Update(ctx, fresh)
	})
}

// TriggerEvent and TriggerCron name the two refill paths. They are the value of
// the "trigger" log field and the operator_brainstorm_refill_total label.
const (
	TriggerEvent = "event"
	TriggerCron  = "cron"
)

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

// brainstormRefillDecision applies the control law plus the skip circuit
// breaker. quota is the number of proposals the session may file, clamped to
// [1, MaxProposalsPerOutcome] when refill is true. reason is "" when refilling,
// otherwise the log-and-metric reason the cycle was suppressed.
//
// The breaker suppresses ONLY the event path. The cron tick is the backstop that
// repairs the backlog after a dropped event or a tripped breaker, and it resets
// the counter, so a genuinely dry idea space costs one session per cron period
// instead of one per reconcile.
func brainstormRefillDecision(act tatarav1alpha1.BrainstormActivity,
	pending, inflight, consecutiveSkips int, trigger string) (quota int, refill bool, reason string) {

	if maxSkips := act.ResolveMaxConsecutiveSkips(); trigger == TriggerEvent &&
		maxSkips > 0 && consecutiveSkips >= maxSkips {
		return 0, false, "breaker-tripped"
	}
	deficit := brainstormDeficit(act.ResolveTarget(), pending, inflight)
	if deficit <= 0 {
		return 0, false, "at-target"
	}
	return min(deficit, tatarav1alpha1.MaxProposalsPerOutcome), true, ""
}
