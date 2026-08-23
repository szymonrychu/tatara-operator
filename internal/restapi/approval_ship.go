// Copyright 2026 tatara authors.

package restapi

import (
	"context"
	"fmt"
	"net/http"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/controller"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// THE SHIP GATE'S HTTP HALF (#639). controller.ApprovalShipVerdict decides;
// this file asks it at the two places a change can LEAVE - mr_write(action=open)
// and submit_outcome(action=submitted) - and renders the refusal.

// approvalRequiredReason is the `reason` discriminator on the refusal body. It
// is the string tatara-cli's refusalGuidance switches on, so it is wire contract
// and not a log label.
const approvalRequiredReason = "approval-required"

// approvalRequiredBody is the 409 the two ship sites return.
//
// IT CARRIES BOTH `error` AND `message`. `message` is what refusalGuidance
// renders for an agent whose cli knows this reason; `error` is what EVERY other
// reader falls back to - an older cli, a hand-rolled client, the pod's own
// transport logging - because writeError's one-key shape is what the rest of
// this API returns and a body without it reads as an empty failure.
type approvalRequiredBody struct {
	Error   string                   `json:"error"`
	Reason  string                   `json:"reason"`
	Message string                   `json:"message"`
	Blocked []controller.ShipBlocker `json:"blocked"`
}

// shipVerdict reads the Task's owned Issues and asks for the verdict. It is the
// ONE entry point for both ship sites, so the two cannot drift.
//
// IT RESOLVES THE PROJECT FROM THE TASK, never from the caller. `mrOpen` already
// holds a *Project, but that one comes from the {p} URL parameter and callerTask
// does not check that the named Task belongs to it. The maintainer set and the
// bot login decide which blocker this gate reports and, once a significance is
// declared, which ceiling it is measured against - so taking them from a
// caller-supplied parameter puts an input to a security decision in the caller's
// hands. One extra Get buys the gate its own answer.
//
// A READ FAILURE FAILS OPEN, deliberately, and it is the one place in this
// change that does. The alternative refuses every open and every submit during a
// transient apiserver blip, which strands finished work behind an error the
// agent cannot act on and cannot distinguish from a real refusal; the same
// reasoning planPinRefusal already applies to its own read. The grant itself was
// gated, the Issue write is durable, and the failure is loud on the ordinary
// client-error path.
func (s *Server) shipVerdict(ctx context.Context, task *tatarav1alpha1.Task,
	significance string) []controller.ShipBlocker {

	if !shipGateApplies(task) {
		return nil
	}
	proj, err := s.getProjectCR(ctx, task.Spec.ProjectRef)
	if err != nil {
		s.log.WarnContext(ctx, "restapi: could not read the task's project for the approval ship gate; allowing",
			"task", task.Name, "project", task.Spec.ProjectRef, "error", err)
		return nil
	}
	issues, err := s.ownedIssues(ctx, task)
	if err != nil {
		s.log.WarnContext(ctx, "restapi: could not read owned issues for the approval ship gate; allowing",
			"task", task.Name, "error", err)
		return nil
	}
	return controller.ApprovalShipVerdict(ctx, s.c, proj, issues, significance)
}

// shipGateApplies is the kind filter. ONLY the implement agent meets this gate:
// it is the one kind that passes through submit_outcome's approval actions and
// therefore the one kind that can have skipped them. A review pod's mr_write, a
// documentation batch and an upgrade Task carry no approval of their own and
// would be refused forever.
//
// A TAKEOVER TASK SATISFIES THIS FILTER TOO, and that is safe only because of an
// invariant living somewhere else. stage.originAgentKinds maps BOTH `implement`
// and `takeover` onto AgentImplement, so a takeover pod's Status.AgentKind is
// "implement" - yet it is minted straight into under-implementation, `refined`
// is unreachable for it, and it therefore can never pass the gate. What saves it
// is that it owns ZERO Issue CRs by construction, so ApprovalShipVerdict returns
// no blockers. Stated here rather than only in that function's doc because it is
// this filter that lets a takeover through: any future kind mapped onto
// AgentImplement that DOES own an Issue is wedged on arrival, with no remedy.
func shipGateApplies(task *tatarav1alpha1.Task) bool {
	return task != nil && task.Status.AgentKind == "implement"
}

// refuseApprovalRequired writes the 409 and counts it.
func (s *Server) refuseApprovalRequired(w http.ResponseWriter, r *http.Request,
	task *tatarav1alpha1.Task, kind, action string, blockers []controller.ShipBlocker) {

	// COUNTED ON BOTH SERIES, deliberately. RestOwnershipRefusedTotal is where
	// every other structural refusal on a write endpoint lands, and the
	// mr_write refusal has nowhere else to go. But an outcome refusal that never
	// touched RestOutcomeRejectedTotal is invisible to every outcome-rejection
	// dashboard and alert, which is where an operator looks when submits start
	// failing - so the submit path (kind != "") is counted there as well.
	obs.RestOwnershipRefusedTotal.WithLabelValues(approvalRequiredReason).Inc()
	if kind != "" {
		obs.RestOutcomeRejectedTotal.WithLabelValues(kind, approvalRequiredReason).Inc()
	}
	msg := fmt.Sprintf("%s is refused: %d issue(s) this task owns have not passed the implement gate. "+
		"Work done before the gate grants is LOST - no merge request can carry it. "+
		"Fix each issue below, then retry.", action, len(blockers))
	details := make([]any, 0, len(blockers)*2)
	for _, b := range blockers {
		details = append(details, "blocked_issue", fmt.Sprintf("%s#%d=%s", b.Repo, b.Number, b.Detail))
	}
	s.log.InfoContext(r.Context(), "restapi: ship refused, approval required",
		append(append(reqLogFields(r), "action", "approval_required_refused", "task", task.Name,
			"reason", approvalRequiredReason, "blocked", len(blockers)), details...)...)
	writeJSON(w, http.StatusConflict, approvalRequiredBody{
		Error:   msg,
		Reason:  approvalRequiredReason,
		Message: msg,
		Blocked: blockers,
	})
}
