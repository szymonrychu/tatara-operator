package controller

import (
	"context"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// DrainStandDownMerge keeps merge-on-approve alive after a stand-down. A
// tatara-owned MR that a human pushed to flips to external+external-push, and
// its pushing-capable owner Task parks ownership-lost (flipToExternal),
// control handed back to the review Task. Merge-on-approve CONTINUES after a
// stand-down (spec Section 1: external + external-push keeps review + merge),
// so when the review Task's next approve verdict lands on the human's CURRENT
// head, this drain - running on every MergeRequest reconcile - re-drives the
// parked owner Task into merging and moves control back to it, so the
// existing task-stage-driven reconcileMerge merges it under
// mergeAllowedForOwnership (OP10).
//
// The re-driven owner Task is found by SCANNING mr's owner refs for one that
// is currently parked(ownership-lost) - NOT by assuming the takeover Task's
// deterministic name. flipToExternal parks ANY pushing-capable owner
// (takeover-kind AND a normal full-lifecycle Task widened by kind != review,
// not kind == takeover), so this must match either shape.
//
// It is a no-op for tatara MRs (their own reviewing->merging handles merge)
// and for external+initial MRs (human-merged only, never taken over).
func (d *StageDriver) DrainStandDownMerge(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, mr *tatarav1alpha1.MergeRequest) error {

	if mr.Status.State != "open" ||
		mr.Status.Ownership != tatarav1alpha1.OwnershipExternal ||
		!strings.HasPrefix(mr.Status.OwnershipReason, "external-push:") {
		return nil
	}
	// Merge only an APPROVED review pinned to the CURRENT external head.
	if mr.Status.Status != "approved" || mr.Status.ReviewedSHA == "" || mr.Status.ReviewedSHA != mr.Status.HeadSHA {
		return nil
	}

	tk, err := d.parkedOwnershipLostOwner(ctx, proj, mr)
	if err != nil {
		return err
	}
	if tk == nil {
		return nil // no parked owner task (never taken over, or already re-driven/reaped)
	}

	// Move control back to the parked owner Task so reconcileMerge finds the MR.
	prev, _ := own.ControllerOwner(mr)
	var from *tatarav1alpha1.Task
	if prev != "" && prev != tk.Name {
		from = &tatarav1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: prev, Namespace: proj.Namespace}}
	}
	if err := d.mutateOwnerRefs(ctx, mr, func(fresh *tatarav1alpha1.MergeRequest) error {
		own.AddPlainOwner(fresh, tk)
		return own.HandOverController(fresh, from, tk)
	}); err != nil {
		return fmt.Errorf("stand-down merge: hand control to owner task %s: %w", tk.Name, err)
	}

	// Reason is passed EXPLICITLY: the parked->merging edge collapses by
	// (from, to) in the F.3 table, so the Reason carried here is not what makes
	// the transition legal - it is the audit trail stamped onto
	// status.stageReason, and it must read ownership-lost, not an empty string.
	if err := d.enterStage(ctx, proj, tk, tatarav1alpha1.StageMerging, stage.ReasonOwnershipLost, nil); err != nil {
		return fmt.Errorf("stand-down merge: re-drive %s to merging: %w", tk.Name, err)
	}
	log.FromContext(ctx).Info("stand-down merge re-drive", "action", "standdown_merge",
		"resource_id", mr.Name, "task", tk.Name, "kind", tk.Spec.Kind)
	return nil
}

// parkedOwnershipLostOwner finds the plain owner Task on mr, of ANY kind, that
// is currently parked(ownership-lost): the pushing-capable owner the last
// flip-to-external parked, left behind as a plain (non-controller) ref by
// hand-back to the review Task. Owner refs are append-only and
// creation-ordered (internal/own), so the LAST matching ref is the most
// recently parked owner, in case the MR has outlived more than one flip round.
// Returns (nil, nil) when no owner ref currently matches.
func (d *StageDriver) parkedOwnershipLostOwner(ctx context.Context, proj *tatarav1alpha1.Project,
	mr *tatarav1alpha1.MergeRequest) (*tatarav1alpha1.Task, error) {

	var found *tatarav1alpha1.Task
	for _, ref := range mr.GetOwnerReferences() {
		if ref.Kind != "Task" {
			continue
		}
		var tk tatarav1alpha1.Task
		if err := d.Get(ctx, client.ObjectKey{Namespace: proj.Namespace, Name: ref.Name}, &tk); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("stand-down merge: get owner task %s: %w", ref.Name, err)
		}
		if tk.Status.Stage == tatarav1alpha1.StageParked && tk.Status.StageReason == stage.ReasonOwnershipLost {
			t := tk
			found = &t
		}
	}
	return found, nil
}
