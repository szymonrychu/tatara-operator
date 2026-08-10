package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// workspaceBindDeadline bounds how long a Task may wait on an un-Bound
// workspace volume before it parks.
//
// rook-ceph-rwx is volumeBindingMode: Immediate, so binding does NOT wait for a
// consumer pod: the claim either binds within seconds of the create or
// something is genuinely wrong (the CSI provisioner is down, the pool is full,
// the class was renamed). Ten minutes is therefore generous rather than tight,
// and the happy path never observes it at all.
const workspaceBindDeadline = 10 * time.Minute

// ensureWorkspacePVC creates the per-Task workspace PVC when needed and reports
// whether EVERY volume the wrapper pod will mount is Bound.
//
// THE BOUND GATE IS THE POINT OF THIS FUNCTION, and it is the single most
// important safety property of the persistent workspace. handleNotReady
// respawns any pod that has not become Ready inside the 5 minute boot deadline,
// and the maxPodRecreations ceiling was DELETED, so a pod left Pending on an
// unbound claim is roughly 288 pod creations for one Task over 24 hours. By
// returning "not ready" instead of letting ensureStagePod proceed, an
// unprovisionable volume costs ZERO pods.
//
// It never blocks forever either: past workspaceBindDeadline the Task parks with
// stage.ReasonOperatorError, whose UnparkClass is UnparkNever - a human has to
// look at the storage before this Task is worth running again, which is exactly
// the semantics of the situation.
//
// Returns (true, nil) when the feature is off for this Project, so a cluster
// that has not opted in behaves exactly as it did before.
func (r *TaskReconciler) ensureWorkspacePVC(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task) (bool, error) {

	if !agent.WorkspacePVCEnabled(proj, r.PodConfig) {
		return true, nil
	}

	name := agent.WorkspacePVCName(task)
	pvc := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: name}, pvc)
	switch {
	case apierrors.IsNotFound(err):
		want, buildErr := agent.BuildWorkspacePVC(proj, task, r.PodConfig)
		if buildErr != nil {
			return false, buildErr
		}
		// Idempotent for the same reason the pod create is: two reconciles can
		// race here and the loser must not fail the pass.
		if createErr := r.Create(ctx, want); createErr != nil && !apierrors.IsAlreadyExists(createErr) {
			return false, fmt.Errorf("create workspace pvc %s: %w", name, createErr)
		}
		log.FromContext(ctx).Info("created the task workspace volume",
			"action", "workspace_pvc_create", "resource_id", task.Name, "pvc", name,
			"storage_class", tatarav1alpha1.WorkspaceStorageClass(proj),
			"size", tatarav1alpha1.WorkspaceSize(proj))
		// Never Bound on the pass that created it; the caller requeues.
		return false, nil
	case err != nil:
		return false, fmt.Errorf("get workspace pvc %s: %w", name, err)
	}

	// The workspace claim's own age is the deadline clock for BOTH volumes. It is
	// created first and always exists by the time anything can be waiting, so it
	// needs no extra annotation to remember when the wait began.
	since := pvc.CreationTimestamp.Time

	if pvc.Status.Phase != corev1.ClaimBound {
		return false, r.parkOnStuckWorkspaceVolume(ctx, proj, task, since, name, string(pvc.Status.Phase))
	}

	// The CACHE claim is owned by the PROJECT and provisioned by the project
	// reconcile, not here - but the pod MOUNTS it, so a pod created before it
	// exists is Pending on a missing volume and hits the same respawn loop the
	// gate above exists to prevent. Observe it; never create it here, or it would
	// carry a Task ownerRef and die with the first Task that used it.
	if agent.CachePVCEnabled(proj, r.PodConfig) {
		cacheName := agent.CachePVCName(proj.Name)
		cache := &corev1.PersistentVolumeClaim{}
		cacheErr := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: cacheName}, cache)
		switch {
		case apierrors.IsNotFound(cacheErr):
			return false, r.parkOnStuckWorkspaceVolume(ctx, proj, task, since, cacheName, "Absent")
		case cacheErr != nil:
			return false, fmt.Errorf("get cache pvc %s: %w", cacheName, cacheErr)
		case cache.Status.Phase != corev1.ClaimBound:
			return false, r.parkOnStuckWorkspaceVolume(ctx, proj, task, since, cacheName, string(cache.Status.Phase))
		}
	}

	return true, nil
}

// parkOnStuckWorkspaceVolume parks the Task once the bind deadline has passed,
// and does nothing at all before then (the caller requeues). It returns an error
// only when the park itself fails: an ordinary wait is not an error.
func (r *TaskReconciler) parkOnStuckWorkspaceVolume(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, since time.Time, pvcName, phase string) error {

	waited := time.Since(since)
	if waited < workspaceBindDeadline {
		log.FromContext(ctx).Info("waiting for the workspace volume to bind; no pod is created until it does",
			"action", "workspace_pvc_wait", "resource_id", task.Name, "pvc", pvcName,
			"phase", phase, "waited_seconds", int(waited.Seconds()))
		return nil
	}
	if tatarav1alpha1.Parked(task) {
		return nil
	}
	log.FromContext(ctx).Error(nil, "workspace volume never bound; parking the task",
		"action", "workspace_pvc_stuck", "resource_id", task.Name, "pvc", pvcName,
		"phase", phase, "waited_seconds", int(waited.Seconds()))
	return r.park(ctx, proj, task, stage.ReasonOperatorError, time.Now())
}

// deleteWorkspacePVC removes a Task's workspace volume. It is called from ONE
// place - the terminal-outcome arm of enterStage - and never from the park
// teardown; see the call site for why that separation is load-bearing.
func deleteWorkspacePVC(ctx context.Context, c client.Client, namespace string,
	task *tatarav1alpha1.Task) error {
	name := agent.WorkspacePVCName(task)
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
	}
	if err := c.Delete(ctx, pvc); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete workspace pvc %s: %w", name, err)
	}
	log.FromContext(ctx).Info("deleted the task workspace volume",
		"action", "workspace_pvc_delete", "resource_id", task.Name, "pvc", name)
	return nil
}

// ensureCachePVC provisions the per-PROJECT build-cache volume, owner-referenced
// to the PROJECT so it outlives every Task that uses it and is cascade-deleted
// with the Project itself. It is deleted when cacheEnabled goes false, and NEVER
// when a Task reaches a terminal outcome.
//
// The RBAC for persistentvolumeclaims (full verbs) and the
// Owns(&corev1.PersistentVolumeClaim{}) watch were both already registered by
// the memory stack; this adds neither.
func (r *ProjectReconciler) ensureCachePVC(ctx context.Context, p *tatarav1alpha1.Project) error {
	if !p.DeletionTimestamp.IsZero() {
		return nil
	}
	// The operator-wide switch is a ROLLBACK lever, not a delete. Flipping it off
	// must stop new provisioning without destroying volumes that already hold
	// real caches, so that flipping it back on is a cheap and lossless recovery.
	// Only the PROJECT's own fields retire a cache.
	if !r.PodConfig.WorkspacePVCEnabled {
		return nil
	}

	name := agent.CachePVCName(p.Name)

	if !p.WorkspaceEnabled() || !p.WorkspaceCacheEnabled() {
		pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: r.PodConfig.Namespace}}
		if err := r.Delete(ctx, pvc); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete cache pvc %s: %w", name, err)
		}
		return nil
	}

	existing := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, types.NamespacedName{Namespace: r.PodConfig.Namespace, Name: name}, existing)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get cache pvc %s: %w", name, err)
	}
	want, buildErr := agent.BuildCachePVC(p, r.PodConfig, projectOwnerRef(p))
	if buildErr != nil {
		return buildErr
	}
	if createErr := r.Create(ctx, want); createErr != nil && !apierrors.IsAlreadyExists(createErr) {
		return fmt.Errorf("create cache pvc %s: %w", name, createErr)
	}
	log.FromContext(ctx).Info("created the project build-cache volume",
		"action", "cache_pvc_create", "resource_id", p.Name, "pvc", name,
		"storage_class", tatarav1alpha1.WorkspaceStorageClass(p),
		"size", tatarav1alpha1.WorkspaceCacheSize(p))
	return nil
}

// projectOwnerRef is the controller OwnerReference to a Project, matching the
// one internal/memory stamps on the memory stack.
func projectOwnerRef(p *tatarav1alpha1.Project) metav1.OwnerReference {
	t := true
	return metav1.OwnerReference{
		APIVersion:         tatarav1alpha1.GroupVersion.String(),
		Kind:               "Project",
		Name:               p.Name,
		UID:                p.UID,
		Controller:         &t,
		BlockOwnerDeletion: &t,
	}
}
