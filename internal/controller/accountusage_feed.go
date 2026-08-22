package controller

import (
	"context"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/accountusage"
)

// AccountUsageFeedReconciler folds each Task's reported Claude subscription
// usage snapshot into the fleet-wide in-process accountusage.Store, newest
// wins. It is the LEADER-ONLY half of the wrapper feed.
//
// WHY THIS EXISTS AS ITS OWN CONTROLLER, and why neither obvious alternative
// works:
//
//  1. NOT in the turn-complete callback handler. callbackRunnable is
//     deliberately NOT leader-elected (cmd/manager/wire.go), so it runs on all
//     3 replicas. accountusage.Store is per-process and the dispatcher that
//     reads it is leader-only, so a Set there lands on a non-leader ~2/3 of the
//     time and is silently discarded - no error, no log.
//
//  2. NOT in the dispatcher. DispatcherReconciler.doReconcile lists via
//     listProjectVia, which returns ONLY that Project's Tasks. A max over that
//     slice is a PER-PROJECT snapshot, which is exactly the defect dd40ee4
//     retired: Claude subscription usage is ACCOUNT-WIDE, so a Project that
//     falls quiet stops contributing while its neighbours keep burning the same
//     shared windows. The dispatcher is also the hot path (one reconcile per
//     QueuedEvent), and a fleet-wide scan there would be O(all tasks) per event.
//
// Controllers built with ctrl.NewControllerManagedBy are leader-elected by
// default, which is precisely the property this needs.
//
// THE STORE WRITE LIVES IN Reconcile, NEVER IN A MAP FUNC OR EventHandler. A
// controller-runtime map func runs TWICE per Update, so a side effect placed
// there double-fires, and a predicate does NOT protect it. Do not "simplify"
// this into a handler.
type AccountUsageFeedReconciler struct {
	client.Client
	Store     *accountusage.Store
	Namespace string
}

func (r *AccountUsageFeedReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var task tatarav1alpha1.Task
	if err := r.Get(ctx, req.NamespacedName, &task); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	au := task.Status.AccountUsage
	// An absent snapshot is NORMAL, not an error: the statusline reports
	// nothing until a session's first API response, so a Task can legitimately
	// carry none. Seeding a zero-valued snapshot here would read to the gate as
	// "0% used", which is the silent failure this feed exists to fix.
	if au == nil || au.ObservedAt.IsZero() || r.Store == nil {
		return ctrl.Result{}, nil
	}
	if r.Store.SetIfNewer(SnapshotFromTaskAccountUsage(au)) {
		log.FromContext(ctx).Info("account usage snapshot adopted",
			"action", "account_usage_feed", "resource_id", req.Name,
			"observed_at", au.ObservedAt.Time.UTC().Format(time.RFC3339),
			"five_hour_percent", au.FiveHourPercent, "weekly_percent", au.WeeklyPercent)
	}
	return ctrl.Result{}, nil
}

// SnapshotFromTaskAccountUsage maps a Task-reported observation into the fleet
// store's Snapshot. Exported because cmd/manager's leader-start seed shares it -
// there must be exactly ONE mapping, not two that can drift.
//
// Healthy is true because the observation IS a successful read of the account's
// own rate-limit state; staleness is governed separately, by
// budget.Config.MaxSnapshotAge against UpdatedAt.
//
// A nil reset stays the zero time: the window's reset is UNKNOWN, which the
// gate treats as inactive, not as long expired.
func SnapshotFromTaskAccountUsage(au *tatarav1alpha1.TaskAccountUsage) accountusage.Snapshot {
	s := accountusage.Snapshot{
		FiveHour:  accountusage.Window{Percent: float64(au.FiveHourPercent)},
		Weekly:    accountusage.Window{Percent: float64(au.WeeklyPercent)},
		Healthy:   true,
		Source:    accountusage.SourceWrapper,
		UpdatedAt: au.ObservedAt.UTC(),
	}
	if au.FiveHourReset != nil {
		s.FiveHour.Reset = au.FiveHourReset.UTC()
	}
	if au.WeeklyReset != nil {
		s.Weekly.Reset = au.WeeklyReset.UTC()
	}
	return s
}

func (r *AccountUsageFeedReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("accountusagefeed").
		For(&tatarav1alpha1.Task{}, builder.WithPredicates(accountUsageChanged())).
		Complete(r)
}

// accountUsageChanged passes only the Task writes that actually moved
// status.accountUsage. Tasks are written on nearly every reconcile in the
// platform; without this the feed controller would wake on all of them. A
// PREDICATE is pure and safe here - unlike a map func, it has no side effect to
// double-fire.
func accountUsageChanged() predicate.Predicate {
	observedAt := func(o client.Object) time.Time {
		t, ok := o.(*tatarav1alpha1.Task)
		if !ok || t.Status.AccountUsage == nil {
			return time.Time{}
		}
		return t.Status.AccountUsage.ObservedAt.Time
	}
	return predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return !observedAt(e.Object).IsZero() },
		UpdateFunc:  func(e event.UpdateEvent) bool { return observedAt(e.ObjectNew).After(observedAt(e.ObjectOld)) },
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}
