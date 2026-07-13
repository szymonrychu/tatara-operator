package controller

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/own"
)

// StageReconciler drives the TWO POD-LESS OPERATOR STAGES: merging and
// deploying (F.2's "none (operator)" rows). Every other stage is driven by a
// pod, by the mirror reconcilers, or by /outcome; this one is the operator's own
// hands on the forge.
//
// It is a SEPARATE controller from TaskReconciler on purpose: the merge and the
// delivery are irreversible writes to a human's repository, and they run through
// StageDriver - the single merge egress and the single review poster - and
// nothing else.
//
// It also watches MergeRequests: the deploying stage advances when an owned MR
// is stamped deployed, and that write happens on the MR, not on the Task.
type StageReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// Driver is the operator egress (merge, delivery). Nil disables the
	// controller entirely.
	Driver *StageDriver
}

// +kubebuilder:rbac:groups=tatara.dev,resources=tasks,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=tatara.dev,resources=tasks/status,verbs=get;update;patch

// Reconcile drives one Task through merging or deploying. Every other stage is
// a no-op here.
func (r *StageReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if r.Driver == nil {
		return ctrl.Result{}, nil
	}
	var task tatarav1alpha1.Task
	if err := r.Get(ctx, req.NamespacedName, &task); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !task.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	switch task.Status.Stage {
	case tatarav1alpha1.StageMerging, tatarav1alpha1.StageDeploying:
	default:
		return ctrl.Result{}, nil
	}

	var proj tatarav1alpha1.Project
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.ProjectRef}, &proj); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("stage: get project %s: %w", task.Spec.ProjectRef, err)
	}

	if task.Status.Stage == tatarav1alpha1.StageMerging {
		return r.Driver.ReconcileMerging(ctx, &proj, &task)
	}
	return r.Driver.ReconcileDeploying(ctx, &proj, &task)
}

// SetupWithManager registers the stage driver's controller. The MergeRequest
// watch maps to the CONTROLLER-owning Task: a review drain that lands the Task
// in merging, and a deployedAt stamp that makes delivery satisfiable, both show
// up on the MR first.
func (r *StageReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&tatarav1alpha1.Task{}).
		Watches(&tatarav1alpha1.MergeRequest{}, handler.EnqueueRequestsFromMapFunc(ownerTaskRequests)).
		Named("taskstage").
		Complete(r)
}

// ownerTaskRequests maps an owned object onto its CONTROLLER-owning Task.
func ownerTaskRequests(_ context.Context, obj client.Object) []reconcile.Request {
	name, ok := own.ControllerOwner(obj)
	if !ok {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: name},
	}}
}
