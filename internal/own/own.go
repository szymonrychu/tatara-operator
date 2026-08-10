// Package own implements the ownership rules the platform's garbage
// collection leans on (contract B.1, B.2, B.3, B.5).
//
// The invariant is the API server's, not ours: the GC deletes a dependent only
// when EVERY owner reference resolves to gone, and an object with ZERO owner
// references is never collected. So an Issue/MergeRequest that any live Task
// still references survives, and an artifact with no Task at all leaks rather
// than vanishing - which is what makes "no SCM artifact without a Task"
// structural instead of aspirational.
//
// The package is deliberately dumb: every function except RepairZeroController
// mutates an object IN MEMORY and returns. The CALLER owns the Update and the
// RetryOnConflict. That matters most for HandOverController: the API server
// rejects an object carrying two controller=true refs, so a controller handover
// can never be two PUTs (demote, then promote) - it is ONE mutation that the
// caller writes back in ONE Update.
package own

import (
	"context"
	"fmt"

	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// taskKind is the ownerRef Kind of a Task. A Task NEVER owns a Task (B.2 rule
// 4), so on an Issue/MergeRequest every Task-kind ref is one of the 1..N Task
// owners and every other ref (a Project, say) is out of scope for this package.
const taskKind = "Task"

func boolPtr(b bool) *bool { return &b }

// AddPlainOwner appends a plain owner ref for task to obj: controller unset,
// blockOwnerDeletion EXPLICITLY true (B.2 rules 2 and 3 - a custom controller
// does not get blockOwnerDeletion for free; setting it is what requires
// tasks/finalizers update RBAC). A plain owner's only job is to hold the GC
// open.
//
// It is idempotent: if task already owns obj (controller ref or plain), the
// object is left untouched and false is returned. The caller can therefore
// skip its Update when nothing changed.
func AddPlainOwner(obj client.Object, task *tataradevv1alpha1.Task) bool {
	refs := obj.GetOwnerReferences()
	for _, r := range refs {
		if isTaskRef(r) && r.Name == task.Name {
			return false
		}
	}
	obj.SetOwnerReferences(append(refs, metav1.OwnerReference{
		APIVersion:         tataradevv1alpha1.GroupVersion.String(),
		Kind:               taskKind,
		Name:               task.Name,
		UID:                task.UID,
		BlockOwnerDeletion: boolPtr(true),
	}))
	return true
}

// HandOverController is the ATOMIC single-PUT controller swap used by the
// refine fold (B.3) and by the reaper (B.5). It rewrites obj's owner refs in
// memory so that `to` is the ONLY ref carrying controller=true, demoting
// `from` (and defensively any other controller ref: B.2 rule 1 admits exactly
// one, and the API server enforces it at admission with a 422). The caller
// issues ONE Update.
//
// `to` MUST already be an owner - append it with AddPlainOwner first. Promoting
// a non-owner would mint a controller ref with no plain ref behind it and skip
// the append half of the fold, so it is refused.
//
// `from` may be nil (mint: there is no incumbent) and may name a Task that is
// no longer an owner (a retried handover whose first Update landed). Both are
// no-ops on the demote half, which makes the swap idempotent.
func HandOverController(obj client.Object, from, to *tataradevv1alpha1.Task) error {
	if to == nil {
		return fmt.Errorf("handover to a nil Task on %s/%s", obj.GetNamespace(), obj.GetName())
	}
	refs := obj.GetOwnerReferences()
	target := -1
	for i, r := range refs {
		if isTaskRef(r) && r.Name == to.Name {
			target = i
			break
		}
	}
	if target < 0 {
		return fmt.Errorf("cannot hand the controller flag to %q: it is not an owner of %s/%s (append a plain owner ref first)",
			to.Name, obj.GetNamespace(), obj.GetName())
	}
	if from != nil && from.Name == to.Name {
		return fmt.Errorf("controller handover from %q to itself on %s/%s", from.Name, obj.GetNamespace(), obj.GetName())
	}

	for i := range refs {
		if !isTaskRef(refs[i]) {
			continue
		}
		refs[i].BlockOwnerDeletion = boolPtr(true)
		if i == target {
			refs[i].Controller = boolPtr(true)
			continue
		}
		refs[i].Controller = boolPtr(false)
	}
	obj.SetOwnerReferences(refs)
	return nil
}

// ControllerOwner returns the name of the Task carrying controller=true on
// obj. It is the anti-race lock, the TASK printer column, and the
// AUTHORIZATION for every SCM write against the artifact (B.2 rule 1).
func ControllerOwner(obj client.Object) (string, bool) {
	for _, r := range obj.GetOwnerReferences() {
		if isTaskRef(r) && r.Controller != nil && *r.Controller {
			return r.Name, true
		}
	}
	return "", false
}

// OldestSurvivingOwner returns the oldest Task owner of obj that is still
// alive. Owner refs are APPENDED, so ownerRef order IS creation order and the
// first live ref is the oldest survivor. `live` maps Task name to liveness;
// a name absent from the map, or present as false, is dead.
//
// LIVE MEANS "CAN STILL WORK", NOT "EXISTS". Every caller builds this map, and
// every caller must exclude a Task that is TERMINAL (v1alpha1.TaskDone). An heir
// that has already finished cannot act on the artifact it inherits and cannot be
// woken, so promoting one does not transfer ownership - it PARKS the artifact
// behind a name that resolves. That is the mtg-decks#14 shape: each reap handed
// the deed from one `done` backlog-groomer to the next while the sweep's orphan
// predicate read every one of them as an owner, and the issue went twelve days
// with no Task.
func OldestSurvivingOwner(obj client.Object, live map[string]bool) (string, bool) {
	for _, r := range obj.GetOwnerReferences() {
		if isTaskRef(r) && live[r.Name] {
			return r.Name, true
		}
	}
	return "", false
}

// DropOwner removes EVERY Task-kind owner ref naming task from obj, IN MEMORY,
// and reports whether anything changed so the caller can skip its Update.
//
// It exists for the ONE case RepairZeroController does not cover, and the
// distinction is issue #521: RepairZeroController handles "no ref carries
// controller=true"; this handles "a ref names a Task that DOES NOT EXIST". A
// dangling controller ref is not ownership, it is a string, and it must be
// REMOVED rather than read past - the same ref also misroutes ownerTaskRequests
// (stage_controller.go), the reaper cascade and ourMR, so a caller that merely
// ignored it would leave three other consumers still wrong.
//
// LIVENESS IS NOT DECIDED HERE. This package is memory-only by its own package
// doc; the caller Gets the Task and decides. See Minter.resolveLiveOwner.
func DropOwner(obj client.Object, task string) bool {
	refs := obj.GetOwnerReferences()
	kept := make([]metav1.OwnerReference, 0, len(refs))
	for _, r := range refs {
		if isTaskRef(r) && r.Name == task {
			continue
		}
		kept = append(kept, r)
	}
	if len(kept) == len(refs) {
		return false
	}
	obj.SetOwnerReferences(kept)
	return true
}

// RepairZeroController is B.2 rule 5's guard. An Issue/MergeRequest must NEVER
// have zero controller owners: with plain owners and no controller owner it is
// worked by nobody and re-minted by nobody, because the sweep's orphan
// predicate sees an OWNED Issue. Every path that removes a controller owner
// (fold B.3, reap B.5) is supposed to hand the flag over FIRST; this is the
// belt to those braces.
//
// It promotes the OLDEST surviving Task owner to controller=true, logs at
// ERROR (every firing is a fold or reap bug) and increments
// operator_orphan_no_controller_total. It reports repaired=false, with no
// error, when obj already has a controller owner, when it has no Task owners
// at all (a zero-owner artifact is the sweep's business, not this guard's),
// and when no owner survives (the artifact cascades with its last owner).
//
// A TERMINAL OWNER IS NOT AN HEIR (see OldestSurvivingOwner). This guard runs at
// the top of EVERY Issue and MergeRequest reconcile, so promoting a `done` Task
// re-installs, within milliseconds, exactly the ref the intake funnel's terminal
// release had just removed - the two would disagree about what an owner is, and
// the guard would win. When every owner has finished, the honest answer is that
// this artifact has no controller and the SWEEP owns it: that is a designed
// hand-off, not the fold/reap bug the ERROR line is about, so it is logged as
// one.
//
// This is the ONE function in the package that talks to the API server.
func RepairZeroController(ctx context.Context, c client.Client, obj client.Object) (bool, error) {
	if _, ok := ControllerOwner(obj); ok {
		return false, nil
	}

	var owners []string
	for _, r := range obj.GetOwnerReferences() {
		if isTaskRef(r) {
			owners = append(owners, r.Name)
		}
	}
	if len(owners) == 0 {
		return false, nil
	}

	l := log.FromContext(ctx)
	obs.OrphanNoControllerTotal.Inc()

	live := make(map[string]bool, len(owners))
	anyExists := false
	for _, name := range owners {
		var task tataradevv1alpha1.Task
		err := c.Get(ctx, types.NamespacedName{Namespace: obj.GetNamespace(), Name: name}, &task)
		switch {
		case err == nil:
			anyExists = true
			live[name] = !tataradevv1alpha1.TaskDone(&task)
		case apierrors.IsNotFound(err):
			live[name] = false
		default:
			return false, fmt.Errorf("resolve owner Task %q of %s/%s: %w", name, obj.GetNamespace(), obj.GetName(), err)
		}
	}

	promote, ok := OldestSurvivingOwner(obj, live)
	if !ok {
		if anyExists {
			l.Info("artifact has owner refs but no controller owner and every owner has finished; the sweep re-mints and adopts it",
				"namespace", obj.GetNamespace(), "name", obj.GetName(), "owners", owners)
			return false, nil
		}
		l.Error(nil, "artifact has owner refs but no controller owner and no surviving owner; it cascades with its last owner",
			"namespace", obj.GetNamespace(), "name", obj.GetName(), "owners", owners)
		return false, nil
	}

	if err := HandOverController(obj, nil, &tataradevv1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: promote, Namespace: obj.GetNamespace()},
	}); err != nil {
		return false, fmt.Errorf("promote %q on %s/%s: %w", promote, obj.GetNamespace(), obj.GetName(), err)
	}
	if err := c.Update(ctx, obj); err != nil {
		return false, fmt.Errorf("update %s/%s after promoting %q: %w", obj.GetNamespace(), obj.GetName(), promote, err)
	}

	l.Error(nil, "repaired an artifact with zero controller owners by promoting its oldest surviving owner (contract B.2 rule 5); a fold or a reap dropped the flag",
		"namespace", obj.GetNamespace(), "name", obj.GetName(), "owners", owners, "promoted", promote)
	return true, nil
}

// isTaskRef reports whether r is an ownerRef to a tatara.dev Task.
func isTaskRef(r metav1.OwnerReference) bool {
	return r.Kind == taskKind && r.APIVersion == tataradevv1alpha1.GroupVersion.String()
}
