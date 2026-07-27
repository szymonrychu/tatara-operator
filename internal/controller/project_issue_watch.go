package controller

import (
	"context"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// brainstormResyncInterval is the PERIODIC BACKSTOP for the event-driven refill:
// a lost watch event degrades refill LATENCY, never correctness (the next
// resync catches it). Per the reconcile.Result godoc this is an explicit poll
// interval for an external condition, deliberately NOT the workqueue error
// rate limiter - see the Reconcile fold below.
const brainstormResyncInterval = 15 * time.Minute

// proposalPendingChanged reports whether an Issue update moved a brainstorm
// proposal INTO or OUT OF the pending set. Any other update (a mirror resync, a
// comment append, a label projection) is not a backlog-level change and must not
// wake the Project reconciler: the mirror re-syncs on its own cadence and would
// otherwise make this the chattiest edge in the operator.
//
// botLogin is unavailable in a watch predicate - only the two Issue diffs are
// given, never the owning Project - so this reads proposalPending's STAMPED
// provenance gate only (Spec.ProposalKind), never its forgeable body-marker
// fallback (which requires bot-authorship corroboration this callsite cannot
// perform). That is deliberately narrower than the counting predicate: a
// legacy, still-unstamped proposal's verdict is still caught by the
// brainstormResyncInterval cron backstop, and O5's reconciler backfill stamps
// the issue on its own next pass, so the gap is rollout-bounded, not
// permanent.
func proposalPendingChanged(oldIss, newIss *tatarav1alpha1.Issue) bool {
	if oldIss.Spec.ProposalKind != tatarav1alpha1.ProposalKindBrainstorm &&
		newIss.Spec.ProposalKind != tatarav1alpha1.ProposalKindBrainstorm {
		return false
	}
	return proposalPending(oldIss, "") != proposalPending(newIss, "")
}

// issueToProject maps a proposal Issue onto its owning Project. A non-brainstorm
// Issue (unstamped, or stamped "incident") maps to nothing: only a brainstorm
// proposal's verdict can change the backlog level this watch exists to react
// to.
func issueToProject(_ context.Context, obj client.Object) []reconcile.Request {
	iss, ok := obj.(*tatarav1alpha1.Issue)
	if !ok || iss.Spec.ProposalKind != tatarav1alpha1.ProposalKindBrainstorm || iss.Spec.ProjectRef == "" {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{Namespace: iss.Namespace, Name: iss.Spec.ProjectRef},
	}}
}

// proposalVerdictPredicate admits only the events that can change the backlog
// level: a create that lands already pending (a webhook-driven catch-up
// creating the mirror after the fact), an update that crosses the pending
// boundary, and a delete (a severed proposal mirror frees its slot). Every
// other Issue write on a project with a large issue backlog - state mirror
// resyncs, comment-count bumps, label projections - is filtered out here,
// BEFORE it ever reaches issueToProject or the workqueue: this predicate is
// what keeps the watch from amplifying into a hot loop.
func proposalVerdictPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			iss, ok := e.Object.(*tatarav1alpha1.Issue)
			return ok && proposalPending(iss, "")
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldIss, ok1 := e.ObjectOld.(*tatarav1alpha1.Issue)
			newIss, ok2 := e.ObjectNew.(*tatarav1alpha1.Issue)
			return ok1 && ok2 && proposalPendingChanged(oldIss, newIss)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			iss, ok := e.Object.(*tatarav1alpha1.Issue)
			return ok && iss.Spec.ProposalKind == tatarav1alpha1.ProposalKindBrainstorm
		},
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}

// brainstormCronSpec returns the project's BrainstormActivity, nil-safe against
// both spec.scm and spec.scm.cron being unset (both are +optional pointers -
// runScans guards the identical pair at projectscan.go, and taking this
// field's address through either nil pointer would panic).
func brainstormCronSpec(proj *tatarav1alpha1.Project) *tatarav1alpha1.BrainstormActivity {
	if proj.Spec.Scm == nil || proj.Spec.Scm.Cron == nil {
		return nil
	}
	return &proj.Spec.Scm.Cron.Brainstorm
}
