package controller

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// THE #521 TERMINAL-RESET GUARD.
//
// #521 replaced status.stage with status.state and REMOVED the legacy field
// from the CRD. CRD structural pruning is applied on the READ path, not only on
// write: the moment the narrowed schema is served, no GET returns status.stage
// on any object, however old. There is therefore NOTHING a migrator can read,
// and the ruling is to SKIP THE MIGRATION AND ACCEPT THE RESET - every
// pre-#521 Task comes back stateless and re-derives through the create edge in
// reconcileStage. That is now the INTENDED behaviour, not a defect.
//
// ONE PART OF THAT RESET IS NOT ACCEPTABLE: a Task that had already FINISHED
// must not re-enter the live lifecycle. A delivered or rejected Task rewound to
// `new` re-triages, reopens its issues, respawns an agent and can redo merge
// requests that already landed. terminalResetTarget is the guard against
// exactly that, and it reads ONLY fields that survive pruning:
//
//   - status.deliveredAt, status.documentedBy and every other status field
//     except stage/stageReason are still declared in the narrowed schema and
//     come back intact;
//   - the owned Issue mirrors are SEPARATE OBJECTS with their own CRD, which
//     #521 did not narrow, so nothing about them is pruned at all.
//
// THE BIAS IS DELIBERATE AND IT IS ONE-DIRECTIONAL. Ambiguous evidence returns
// ("", "") and the Task stays stateless: re-triage is NOISY BUT HARMLESS, while
// a wrong terminal STRANDS REAL WORK behind a state that has no exit. Every
// signal below is therefore keyed on a value with exactly one production
// writer, and that writer is named.
//
// IT DOES NOT FIRE operator_task_terminal_total, and that is correct rather
// than an oversight: TaskTerminalEntry drops every edge whose `from` is "",
// because a mint is not an outcome. Counting these 17 as fresh terminals would
// inject a one-off spike into the counter 29 tatara-observability rules divide
// by. They ARE visible - as action=terminal_reset_guard in the log and as
// operator_task_state{state="done"|"rejected"} once the Project reconciler
// recomputes the gauge - which is what the post-deploy check reads.
//
// It returns ("", "", nil) when the Task is not PROVABLY terminal.
func terminalResetTarget(ctx context.Context, c client.Client,
	task *tatarav1alpha1.Task) (state, reason string, err error) {

	// SIGNAL 1: THE DELIVERY STAMP. status.deliveredAt has exactly two writers -
	// CloseIssuesOnDelivery (merge.go) and forceDocTimeout (docbatch.go) - and
	// both set it in the SAME FitTask mutation as EnterStage(..., done, ...).
	// Nothing clears it and nothing else writes it, so its presence is a
	// same-transaction record that this Task reached done.
	if task.Status.DeliveredAt != nil {
		return tatarav1alpha1.StateDone, "", nil
	}

	// SIGNAL 2: THE DOC-BATCH COVER STAMP. status.documentedBy is written only
	// over batch.Spec.DocumentsTasks, and mintDocBatch builds that list from
	// Tasks it reads at Status.State == done, skipping every other state. Its
	// presence therefore proves the OPERATOR observed this Task as done, which
	// is independent evidence from signal 1 (a done Task reached through
	// refined -> done stamps no deliveredAt).
	if task.Status.DocumentedBy != "" {
		return tatarav1alpha1.StateDone, "", nil
	}

	// SIGNAL 3: THE OWNED ISSUE MIRRORS. A live Task drives at least one open
	// issue; every owned mirror being terminal means there is no longer anything
	// for it to drive.
	issues, err := resetGuardIssues(ctx, c, task)
	if err != nil {
		return "", "", err
	}
	// NO MIRRORS IS NOT EVIDENCE. A kind=review Task owns zero Issues by
	// construction, an issue-refs entry whose CR was reaped resolves to nothing,
	// and a mint interrupted before intake owns nothing yet. An absence proves
	// nothing in any of those cases, so it falls through to the reset.
	if len(issues) == 0 {
		return "", "", nil
	}

	declined := false
	stamped := true
	for i := range issues {
		st := issues[i].Status
		switch st.Status {
		case "rejected":
			// recordProposalDecline (issue_apply.go) is the ONLY writer of
			// Issue.status.status = "rejected", and it runs on the issue-closed
			// stop path that terminates the Task at rejected.
			declined = true
		case "done":
			// CloseIssuesOnDelivery (merge.go) is the ONLY writer of
			// Issue.status.status = "done": it is the delivery stamp.
		default:
			// new / approved: the platform never reached a verdict on this issue.
			stamped = false
		}
		// SCM truth. An issue still open on the forge means the work is still
		// wanted, whatever the platform last stamped, and that is enough on its
		// own to make the whole population ambiguous.
		if st.State != "closed" {
			return "", "", nil
		}
	}

	switch {
	case declined:
		// ANY declined mirror wins over the rest: a Task cannot have been
		// delivered and declined, and the decline is the later verdict.
		return tatarav1alpha1.StateRejected, stage.ReasonDeclined, nil
	case stamped:
		// Every mirror carries the delivery stamp. This is the delivered
		// population whose deliveredAt somehow did not survive its own write.
		return tatarav1alpha1.StateDone, "", nil
	default:
		// Closed on the forge, never stamped by the platform: a HUMAN closed it.
		// That is precisely what ApplyIssueClosedStop terminates at, and it
		// stops the Task at rejected(issue-closed) from every state the machine
		// admits it from.
		//
		// THE ONE KNOWN IMPRECISION, and it is terminal-vs-terminal rather than
		// terminal-vs-live: a Task that was sitting at `deployed` when a human
		// closed its issues lands here as rejected, whereas the live machine
		// EXCLUDES `deployed` from the issue-closed stop and would have let it
		// finish at done. Nothing is stranded - its MRs are merged and its
		// deploy has happened; what is lost is the deliveredAt stamp and the
		// doc-batch coverage. Distinguishing the two needs the state, which is
		// exactly what pruning took away, and `deployed` is a 2h window.
		return tatarav1alpha1.StateRejected, stage.ReasonIssueClosed, nil
	}
}

// resetGuardIssues collects the Issue mirrors this Task owns, from BOTH
// bindings: the controller ownerRef (authoritative, B.2 rule 1) and
// status.issueRefs (what the reaper and the unpark rules read). The UNION is
// deliberate and it is the conservative choice - the guard refuses on a single
// non-terminal mirror, so seeing MORE mirrors can only ever move the answer
// towards "leave it stateless".
//
// A ref whose CR is gone is skipped, exactly as ProjectReconciler.ownedIssues
// does: the mirror is a rebuildable projection and its absence is not a verdict.
func resetGuardIssues(ctx context.Context, c client.Client,
	task *tatarav1alpha1.Task) ([]tatarav1alpha1.Issue, error) {

	owned, err := ownedIssueCRs(ctx, c, task)
	if err != nil {
		return nil, fmt.Errorf("reset guard: list owned issues of %s: %w", task.Name, err)
	}
	seen := make(map[string]bool, len(owned))
	for i := range owned {
		seen[owned[i].Name] = true
	}
	for _, name := range task.Status.IssueRefs {
		if seen[name] {
			continue
		}
		var iss tatarav1alpha1.Issue
		err := c.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: name}, &iss)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("reset guard: get issue %s: %w", name, err)
		}
		seen[name] = true
		owned = append(owned, iss)
	}
	return owned, nil
}
