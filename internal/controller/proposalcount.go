package controller

import (
	"context"
	"fmt"
	"strings"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// systemicLabelPrefix marks a multi-repo proposal group. Issues sharing one
// count as a SINGLE backlog slot so a systemic improvement filed against three
// repos does not consume three slots of the target.
const systemicLabelPrefix = "tatara/systemic-"

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
func proposalPending(iss *tatarav1alpha1.Issue) bool {
	if iss.Spec.ProposalKind != tatarav1alpha1.ProposalKindBrainstorm {
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
func pendingProposalCount(issues []tatarav1alpha1.Issue) int {
	groups := map[string]bool{}
	standalone := 0
	for i := range issues {
		if !proposalPending(&issues[i]) {
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
func pendingProposalCountByRepo(issues []tatarav1alpha1.Issue) map[string]int {
	out := map[string]int{}
	for i := range issues {
		if proposalPending(&issues[i]) {
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
//nolint:unused // transitional; wired into the brainstorm cycle in O4 (rewire onto target law)
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
	log.FromContext(ctx).Info("brainstorm: counted pending proposals",
		"action", "brainstorm_backlog_counted", "resource_id", proj.Name,
		"pending", pendingProposalCount(out), "issues_seen", len(out))
	return out, nil
}
