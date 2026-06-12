// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"fmt"

	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// setLifecycleState updates task.Status.LifecycleState to `to`, retrying on
// conflict (same pattern as clearWritebackPending). It logs the transition at
// INFO and increments tatara_lifecycle_transition_total{from,to}.
func (r *TaskReconciler) setLifecycleState(ctx context.Context, task *tatarav1alpha1.Task, to, reason string) error {
	l := log.FromContext(ctx)
	from := task.Status.LifecycleState

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		from = fresh.Status.LifecycleState
		fresh.Status.LifecycleState = to
		return r.Status().Update(ctx, fresh)
	}); err != nil {
		return fmt.Errorf("setLifecycleState: %w", err)
	}

	l.Info("lifecycle transition",
		"action", "lifecycle_transition",
		"resource_id", task.Name,
		"from", from,
		"to", to,
		"reason", reason,
	)

	if r.LifecycleMetrics != nil {
		r.LifecycleMetrics.RecordTransition(from, to)
	}

	task.Status.LifecycleState = to
	return nil
}

// reconcileLifecycle is the dispatch function for issueLifecycle Tasks. It
// applies the same memory-ready and concurrency gates as the generic path,
// then switches on Status.LifecycleState.
func (r *TaskReconciler) reconcileLifecycle(ctx context.Context, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var project tatarav1alpha1.Project
	if err := r.Get(ctx, client.ObjectKey{Namespace: task.Namespace, Name: task.Spec.ProjectRef}, &project); err != nil {
		r.Metrics.ReconcileResult("Task", "error")
		return ctrl.Result{}, fmt.Errorf("lifecycle: get project: %w", err)
	}

	// Memory gate.
	if project.Status.Memory == nil || project.Status.Memory.Phase != "Ready" {
		l.Info("lifecycle task gated: project memory not ready",
			"action", "task_memory_gate", "resource_id", task.Name, "project", project.Name)
		return ctrl.Result{RequeueAfter: capRequeue}, nil
	}

	// Concurrency gate (only when not already active).
	if !isActive(task.Status.Phase) {
		atCap, err := r.atConcurrencyCap(ctx, &project, task.Name)
		if err != nil {
			r.Metrics.ReconcileResult("Task", "error")
			return ctrl.Result{}, err
		}
		if atCap {
			l.Info("lifecycle task gated at concurrency cap",
				"action", "task_gate", "resource_id", task.Name, "project", project.Name)
			return ctrl.Result{RequeueAfter: capRequeue}, nil
		}
	}

	switch task.Status.LifecycleState {
	case "":
		// First reconcile: initialize to Triage.
		if err := r.setLifecycleState(ctx, task, "Triage", "initial"); err != nil {
			r.Metrics.ReconcileResult("Task", "error")
			return ctrl.Result{}, err
		}
		r.Metrics.ReconcileResult("Task", "success")
		return ctrl.Result{RequeueAfter: pollRequeue}, nil

	case "Triage", "Implement":
		// M0b: real agent-run handler
		l.Info("lifecycle agent-run state (stub)",
			"action", "lifecycle_agent_run_stub", "resource_id", task.Name,
			"state", task.Status.LifecycleState)
		r.Metrics.ReconcileResult("Task", "success")
		return ctrl.Result{RequeueAfter: pollRequeue}, nil

	case "Conversation", "MRCI", "Merge", "MainCI":
		// M1/M2: poll states - stub requeue
		r.Metrics.ReconcileResult("Task", "success")
		return ctrl.Result{RequeueAfter: pollRequeue}, nil

	case "Done", "Stopped", "Parked":
		r.Metrics.ReconcileResult("Task", "success")
		return ctrl.Result{}, nil

	default:
		r.Metrics.ReconcileResult("Task", "error")
		return ctrl.Result{}, fmt.Errorf("lifecycle: unknown lifecycleState %q for task %s", task.Status.LifecycleState, task.Name)
	}
}
