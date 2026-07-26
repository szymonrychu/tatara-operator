package controller

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"sort"
	"strings"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// ensureNeo4jPassword returns the neo4j password for the Project's memory
// stack, generating a random one and persisting it to the mem-<proj>-neo4j
// Secret on first reconcile. On subsequent reconciles it reads the existing
// Secret back so the password is never rotated.
func (r *ProjectReconciler) ensureNeo4jPassword(ctx context.Context, p *tataradevv1alpha1.Project) (string, error) {
	names := memory.NamesFor(p.Name)
	var existing corev1.Secret
	key := types.NamespacedName{Namespace: r.MemoryConfig.Namespace, Name: names.Neo4jSecret}
	err := r.Get(ctx, key, &existing)
	switch {
	case err == nil:
		return passwordFromSecret(&existing, names.Neo4jSecret)
	case !apierrors.IsNotFound(err):
		return "", fmt.Errorf("get neo4j secret: %w", err)
	}

	pw, err := randomPassword(32)
	if err != nil {
		return "", fmt.Errorf("generate neo4j password: %w", err)
	}
	sec := memory.Neo4jPasswordSecret(p, r.MemoryConfig, pw)
	if err := r.Create(ctx, sec); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// Lost a race; read the winner back.
			if err := r.Get(ctx, key, &existing); err != nil {
				return "", fmt.Errorf("get neo4j secret after race: %w", err)
			}
			return passwordFromSecret(&existing, names.Neo4jSecret)
		}
		return "", fmt.Errorf("create neo4j secret: %w", err)
	}
	return pw, nil
}

// passwordFromSecret extracts and validates the "password" key from a Secret.
// It returns an error if the key is absent or empty, applying the same
// invariant on every read path (primary and race-loser).
func passwordFromSecret(sec *corev1.Secret, secretName string) (string, error) {
	pw := string(sec.Data["password"])
	if pw == "" {
		return "", fmt.Errorf("neo4j secret %s missing password key", secretName)
	}
	return pw, nil
}

// randomPassword returns a URL-safe base64 string with at least nBytes of
// entropy.
func randomPassword(nBytes int) (string, error) {
	b := make([]byte, nBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// memoryFieldOwner is the SSA field-manager name the operator owns for the
// per-project memory stack.
const memoryFieldOwner = "tatara-operator"

// applyMemoryStack server-side-applies every object in the Project's memory
// stack (owner-ref'd by the N1 builders). The neo4j password Secret is created
// separately by ensureNeo4jPassword and is NOT applied here, so it is never
// rotated.
func (r *ProjectReconciler) applyMemoryStack(ctx context.Context, p *tataradevv1alpha1.Project) error {
	l := log.FromContext(ctx)
	cfg := r.MemoryConfig
	pgCluster, err := r.guardPGStorageShrink(ctx, p, memory.PGCluster(p, cfg))
	if err != nil {
		return err
	}
	// A half-configured object store renders NO backup stanza at all
	// (memory.PGBackupStatus fails closed, because a broken archive_command fills
	// the WAL volume - issue #240). Surface it loudly: the alternative is a
	// cluster the deployer believes is backed up and that silently is not.
	if warning := memoryBackupWarning(cfg); warning != "" {
		l.Error(nil, warning,
			"action", "memory_backup_misconfigured",
			"resource_id", p.Name)
	}
	objs := []client.Object{
		pgCluster,
		memory.Neo4jStatefulSet(p, cfg),
		memory.Neo4jService(p, cfg),
		memory.LightragPVC(p, cfg),
		memory.LightragDeployment(p, cfg),
		memory.LightragService(p, cfg),
		memory.MemoryConfigMap(p, cfg),
		memory.MemoryDeployment(p, cfg),
		memory.MemoryService(p, cfg),
	}
	if ing := memory.Ingress(p, cfg); ing != nil {
		objs = append(objs, ing)
	}
	// The ScheduledBackup is what actually TAKES base backups; the Cluster's
	// spec.backup retention policy only prunes an existing catalogue. When the
	// feature is off the object must be DELETED, not merely skipped: a leftover
	// ScheduledBackup would keep firing against a cluster that no longer has an
	// object store, failing every run forever.
	if sb := memory.PGScheduledBackup(p, cfg); sb != nil {
		objs = append(objs, sb)
	} else if err := r.deleteScheduledBackup(ctx, p); err != nil {
		return err
	}
	// ServiceMonitor + PrometheusRule are gated behind MonitorEnabled: a cluster
	// without the prometheus-operator CRDs must not have the whole memory
	// reconcile fail on an unknown kind. When enabled they make the stack
	// scraped (up{job=~".*tatara-memory.*"}=1) and load the memory alerts.
	if cfg.MonitorEnabled {
		objs = append(objs,
			memory.MemoryServiceMonitor(p, cfg),
			memory.PGPodMonitor(p, cfg),
			memory.MemoryPrometheusRule(p, cfg),
		)
	}
	for _, obj := range objs {
		// client.Apply (the Patch variant) is deprecated in controller-runtime
		// v0.24.1 with no stated removal version. Migration to the typed
		// r.Apply(ctx, applyconfig) API requires generated applyconfiguration
		// types for all 10 stack objects (incl. cnpg Cluster) and is tracked
		// for N4. The Patch path is functionally identical in the interim.
		if err := r.Patch(ctx, obj, client.Apply, //nolint:staticcheck
			client.FieldOwner(memoryFieldOwner), client.ForceOwnership); err != nil {
			return fmt.Errorf("apply %T %s: %w", obj, obj.GetName(), err)
		}
	}
	return nil
}

// memoryBackupWarning returns the operator-level backup misconfiguration
// warning, or "" when the config is either cleanly off or complete.
func memoryBackupWarning(cfg memory.Config) string {
	_, warning := memory.PGBackupStatus(cfg)
	return warning
}

// deleteScheduledBackup removes the Project's cnpg ScheduledBackup, tolerating
// an already-absent object. Called on every reconcile where the object-store
// backup config is off or incomplete, which is the steady state on a cluster
// with no object store - so the "nothing to delete" path is the normal one, not
// the exception. Issued as a direct Delete rather than a cached Get-then-Delete
// deliberately: a Get would spin up an informer (and a permanent watch) on
// ScheduledBackup just to answer "still absent?" on every reconcile.
//
// A no-kind-match is tolerated alongside NotFound. The scheduledbackups CRD is
// absent on any cluster whose cnpg install predates it or that has no cnpg at
// all, and there a Delete returns "no matches for kind" rather than NotFound. A
// missing CRD means there is definitively no object to remove, so failing the
// whole memory reconcile over it would take the entire stack down for a feature
// that is switched off - which is exactly what envtest (no cnpg CRDs installed)
// reproduced.
func (r *ProjectReconciler) deleteScheduledBackup(ctx context.Context, p *tataradevv1alpha1.Project) error {
	sb := &cnpgv1.ScheduledBackup{}
	sb.Name = memory.NamesFor(p.Name).PGScheduledBackup
	sb.Namespace = r.MemoryConfig.Namespace
	err := r.Delete(ctx, sb)
	if err == nil || apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
		return nil
	}
	return fmt.Errorf("delete scheduled backup %s: %w", sb.Name, err)
}

// guardPGStorageShrink clamps the rendered cnpg Cluster's PGDATA and WAL storage
// sizes up to the already-provisioned sizes before it is applied, so a render
// whose size drifted below the live volume never asks cnpg to shrink storage.
// cnpg's admission webhook rejects any shrink, and before this guard that
// rejection failed every apply and wedged the whole project memory reconcile to
// Failed (issue #248). The provisioned floor is the max of the live Cluster
// .spec size and the live PVC capacity, so a PVC manually expanded beyond the
// recorded spec is also caught before the reconcile hits the webhook (issue
// #258). A NotFound existing cluster (first provision) or an unset provisioned
// size leaves the render untouched.
func (r *ProjectReconciler) guardPGStorageShrink(ctx context.Context, p *tataradevv1alpha1.Project, desired *cnpgv1.Cluster) (*cnpgv1.Cluster, error) {
	l := log.FromContext(ctx)
	var existing cnpgv1.Cluster
	key := types.NamespacedName{Namespace: r.MemoryConfig.Namespace, Name: desired.Name}
	if err := r.Get(ctx, key, &existing); err != nil {
		if apierrors.IsNotFound(err) {
			return desired, nil
		}
		// A non-NotFound read is a transient API/cache blip, not a real failure.
		// Fail-open: apply the render unclamped rather than flipping the whole
		// stack to Failed on a blip (the same non-flapping stance the health path
		// takes). In steady state the render equals the provisioned size, so no
		// shrink is requested; if the spec has genuinely drifted below the live
		// volume, cnpg's admission webhook still rejects the shrink and the next
		// reconcile re-reads and clamps. The other stack objects still apply.
		l.Info("shrink guard: transient error reading existing pg cluster, applying render unclamped",
			"action", "memory_storage_shrink_guard_read_error",
			"resource_id", desired.Name,
			"error", err.Error())
		return desired, nil
	}
	// Also read the live PVC capacities. cnpg's webhook validates a shrink
	// against the stored Cluster .spec, but a PVC manually expanded beyond that
	// spec still cannot be shrunk, so a render at/above the spec yet below the
	// live PVC would be rejected downstream (issue #258). PVC reads are
	// best-effort hardening: on a read error, fall back to the Cluster-spec floor
	// (the #248 incident path) rather than failing the whole apply.
	pgDataPVCCap, walPVCCap, err := r.provisionedPGPVCCapacity(ctx, desired.Name)
	if err != nil {
		l.Info("shrink guard: could not read live pg pvc capacity, clamping against cluster spec only",
			"action", "memory_storage_shrink_guard_pvc_read_error",
			"resource_id", desired.Name,
			"error", err.Error())
		pgDataPVCCap, walPVCCap = "", ""
	}
	prov := memory.ProvisionedPGStorage{
		PGDataSpecSize:    existing.Spec.StorageConfiguration.Size,
		PGDataPVCCapacity: pgDataPVCCap,
		WALSpecSize:       walSize(&existing),
		WALPVCCapacity:    walPVCCap,
	}
	raised, err := memory.ClampPGStorageToProvisioned(desired, prov)
	if err != nil {
		return nil, fmt.Errorf("guard pg storage shrink: %w", err)
	}
	if raised {
		r.Metrics.MemoryStorageShrinkGuarded(p.Name)
		l.Info("raised rendered pg storage to provisioned size to avoid a cnpg shrink rejection",
			"action", "memory_storage_shrink_guard",
			"resource_id", desired.Name,
			"pgdata_size", desired.Spec.StorageConfiguration.Size,
			"wal_size", walSize(desired))
	}
	return desired, nil
}

// walSize returns the cluster's WAL volume size, or "" when it declares none.
// Used for the shrink-guard log line and as the existing-cluster WAL spec floor.
func walSize(c *cnpgv1.Cluster) string {
	if c.Spec.WalStorage == nil {
		return ""
	}
	return c.Spec.WalStorage.Size
}

// cnpg labels its per-instance PVCs so the data and WAL volumes can be told
// apart. These mirror pkg/utils.{ClusterLabelName,PvcRoleLabelName} and their
// PG_DATA/PG_WAL role values from cloudnative-pg; hardcoded here to avoid pulling
// the whole cnpg utils package in for two string constants.
const (
	cnpgClusterLabel  = "cnpg.io/cluster"
	cnpgPVCRoleLabel  = "cnpg.io/pvcRole"
	cnpgPVCRolePGData = "PG_DATA"
	cnpgPVCRolePGWAL  = "PG_WAL"
)

// provisionedPGPVCCapacity returns the largest live capacity across the cnpg
// cluster's PGDATA and WAL PVCs, as resource-quantity strings ("" when no such
// PVC exists yet). It reads status.capacity - the actually-provisioned size that
// reflects a manual expansion - falling back to the spec request when status is
// not yet populated. The max across a multi-instance cluster's per-replica PVCs
// is the floor the render must not drop below (issue #258).
func (r *ProjectReconciler) provisionedPGPVCCapacity(ctx context.Context, clusterName string) (pgData, wal string, err error) {
	var pvcs corev1.PersistentVolumeClaimList
	if err := r.List(ctx, &pvcs,
		client.InNamespace(r.MemoryConfig.Namespace),
		client.MatchingLabels{cnpgClusterLabel: clusterName}); err != nil {
		return "", "", fmt.Errorf("list pg pvcs for %s: %w", clusterName, err)
	}
	for i := range pvcs.Items {
		pvc := &pvcs.Items[i]
		capacity := pvcCapacity(pvc)
		if capacity == "" {
			continue
		}
		switch pvc.Labels[cnpgPVCRoleLabel] {
		case cnpgPVCRolePGData:
			if pgData, err = maxSizeString(pgData, capacity); err != nil {
				return "", "", fmt.Errorf("pgdata pvc %s: %w", pvc.Name, err)
			}
		case cnpgPVCRolePGWAL:
			if wal, err = maxSizeString(wal, capacity); err != nil {
				return "", "", fmt.Errorf("wal pvc %s: %w", pvc.Name, err)
			}
		}
	}
	return pgData, wal, nil
}

// pvcCapacity returns the PVC's live storage capacity as a quantity string,
// preferring status.capacity (the provisioned size, updated after an expansion)
// and falling back to the spec request. Returns "" when neither is set.
func pvcCapacity(pvc *corev1.PersistentVolumeClaim) string {
	if q, ok := pvc.Status.Capacity[corev1.ResourceStorage]; ok && !q.IsZero() {
		return q.String()
	}
	if q, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; ok && !q.IsZero() {
		return q.String()
	}
	return ""
}

// maxSizeString returns the larger of two resource-quantity strings by magnitude;
// an empty current value is replaced by the candidate.
func maxSizeString(current, candidate string) (string, error) {
	if current == "" {
		return candidate, nil
	}
	curQty, err := resource.ParseQuantity(current)
	if err != nil {
		return "", fmt.Errorf("parse %q: %w", current, err)
	}
	candQty, err := resource.ParseQuantity(candidate)
	if err != nil {
		return "", fmt.Errorf("parse %q: %w", candidate, err)
	}
	if candQty.Cmp(curQty) > 0 {
		return candidate, nil
	}
	return current, nil
}

// memoryHealth carries the readiness inputs memoryPhase gates on, plus the
// CNPG-side detail the MemoryReady condition and status.memory surface for
// diagnosis. Everything read here is observable while an instance's container is
// dead - CNPG's own remediation is gated on the instance-manager HTTP endpoint
// (:8000/pg/status), which is never up on a crash-looping member, so a diverged
// standby is invisible to it by construction (issue #442).
type memoryHealth struct {
	pgReady       int
	pgWant        int
	pgPrimary     string
	pgUnhealthy   []string
	pgDanglingPVC []string
	neo4jReady    int32
	lightragAvail int32
	memoryAvail   int32
}

// notReadyComponents names the stack components below their readiness gate, in
// a stable order. It is empty exactly when memoryPhase would return "Ready".
// Issue #425: a stack could sit Provisioning for hours with no record of WHICH
// of the four backends held it.
func (h memoryHealth) notReadyComponents() []string {
	var out []string
	if h.pgReady < memoryQuorum(h.pgWant) {
		out = append(out, "postgres")
	}
	if h.neo4jReady < 1 {
		out = append(out, "neo4j")
	}
	if h.lightragAvail < 1 {
		out = append(out, "lightrag")
	}
	if h.memoryAvail < 1 {
		out = append(out, "memory-api")
	}
	return out
}

// pgDegradedReason describes how the CNPG cluster is impaired, or "" when it is
// fully healthy. It is deliberately independent of memoryQuorum: a cluster can
// be quorate (so memoryPhase reads Ready and the fleet keeps serving) while a
// standby sits permanently diverged, which is the exact state that went
// undetected for 8h42m in issue #442. Callers surface this on the MemoryReady
// condition, never on the phase - tightening the phase gate flaps the whole
// fleet (issues #215, #355).
func pgDegradedReason(h memoryHealth) string {
	var parts []string
	if h.pgPrimary == "" {
		parts = append(parts, "no primary elected")
	}
	if h.pgReady < h.pgWant {
		s := fmt.Sprintf("%d/%d instances ready", h.pgReady, h.pgWant)
		if len(h.pgUnhealthy) > 0 {
			s += " (not healthy: " + strings.Join(h.pgUnhealthy, ", ") + ")"
		}
		parts = append(parts, s)
	}
	if len(h.pgDanglingPVC) > 0 {
		parts = append(parts, "dangling PVCs: "+strings.Join(h.pgDanglingPVC, ", "))
	}
	return strings.Join(parts, "; ")
}

// pgUnhealthyInstances returns the names CNPG lists under any status other than
// healthy, sorted so the derived condition message does not churn on Go's
// randomised map iteration order.
func pgUnhealthyInstances(st map[cnpgv1.PodStatus][]string) []string {
	var out []string
	for status, names := range st {
		if status == cnpgv1.PodHealthy {
			continue
		}
		out = append(out, names...)
	}
	sort.Strings(out)
	return out
}

// memoryStackHealth reads the owned objects' statuses and returns the readiness
// inputs for memoryPhase plus the CNPG replication detail.
func (r *ProjectReconciler) memoryStackHealth(ctx context.Context, p *tataradevv1alpha1.Project) (memoryHealth, error) {
	names := memory.NamesFor(p.Name)
	ns := r.MemoryConfig.Namespace
	h := memoryHealth{pgWant: memory.PgInstances(p)}

	// A NotFound read means the object was SSA-applied moments ago and is not
	// yet visible in the informer cache (or has not been created yet). That is
	// not-yet-ready, not a failure: leave the count at zero so memoryPhase
	// reports Provisioning. Only a genuine (non-NotFound) read error is returned.
	var cluster cnpgv1.Cluster
	if e := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: names.PGCluster}, &cluster); e != nil {
		if !apierrors.IsNotFound(e) {
			return memoryHealth{}, fmt.Errorf("get cnpg cluster: %w", e)
		}
	} else {
		h.pgReady = cluster.Status.ReadyInstances
		h.pgPrimary = cluster.Status.CurrentPrimary
		h.pgUnhealthy = pgUnhealthyInstances(cluster.Status.InstancesStatus)
		h.pgDanglingPVC = cluster.Status.DanglingPVC
	}

	var sts appsv1.StatefulSet
	if e := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: names.Neo4j}, &sts); e != nil {
		if !apierrors.IsNotFound(e) {
			return memoryHealth{}, fmt.Errorf("get neo4j statefulset: %w", e)
		}
	} else {
		h.neo4jReady = sts.Status.ReadyReplicas
	}

	var lightrag appsv1.Deployment
	if e := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: names.Lightrag}, &lightrag); e != nil {
		if !apierrors.IsNotFound(e) {
			return memoryHealth{}, fmt.Errorf("get lightrag deployment: %w", e)
		}
	} else if deploymentRolloutConverged(&lightrag) {
		h.lightragAvail = lightrag.Status.AvailableReplicas
	}

	var mem appsv1.Deployment
	if e := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: names.Memory}, &mem); e != nil {
		if !apierrors.IsNotFound(e) {
			return memoryHealth{}, fmt.Errorf("get memory deployment: %w", e)
		}
	} else if deploymentRolloutConverged(&mem) {
		h.memoryAvail = mem.Status.AvailableReplicas
	}

	return h, nil
}

// deploymentRolloutConverged reports whether d's rollout has fully landed: the
// controller has observed the latest spec generation, and every replica is on
// the current pod template (UpdatedReplicas) and Available. Without this, a
// Deployment mid-rollout with one stale-generation replica still Available
// reads identically to a converged one, so memoryStackHealth would count it
// ready while an old pod runs alongside the new one (issue #355 - the
// mem-tatara-lightrag rollout never converged during the incident, old+new
// pods coexisting, and nothing detected it). A never-applied/status-not-yet-
// populated Deployment (all fields zero) is correctly NOT converged.
func deploymentRolloutConverged(d *appsv1.Deployment) bool {
	return d.Status.ObservedGeneration == d.Generation &&
		d.Status.UpdatedReplicas == d.Status.Replicas &&
		d.Status.AvailableReplicas == d.Status.Replicas
}

// memoryQuorum is the minimum number of cnpg instances that must be Ready for
// the cluster to be treated as serving: a strict majority (wantInstances/2 + 1),
// floored at 1. For a single-instance cluster (the default) quorum is 1; for a
// 3-node HA cluster it is 2, so losing one replica still leaves a healthy
// primary plus quorum rather than flipping the whole stack to Provisioning.
func memoryQuorum(wantInstances int) int {
	if wantInstances < 1 {
		return 1
	}
	return wantInstances/2 + 1
}

// memoryPhase returns "Ready" when cnpg has a serving quorum of Ready instances
// (memoryQuorum) AND neo4j, lightrag and memory each report at least one ready /
// available replica; otherwise "Provisioning". Gating on a quorum rather than on
// every instance keeps a degraded-but-primary-serving HA cluster available: a
// single replica loss must not take memory fully not-ready (issue #215).
func memoryPhase(readyInstances, wantInstances int, neo4jReady, lightragAvail, memoryAvail int32) string {
	if readyInstances >= memoryQuorum(wantInstances) && neo4jReady >= 1 && lightragAvail >= 1 && memoryAvail >= 1 {
		return "Ready"
	}
	return "Provisioning"
}

// memoryRequeue is how often the reconciler re-checks a Provisioning stack.
const memoryRequeue = 10 * time.Second

// memoryReadyStabilizationWindow is how long the memory stack must hold Phase==Ready
// before controllers treat it as stably ready and release gated work. This matches
// the ~3-minute window of the existing retrieval-probe unhealthy threshold, so a
// new leader does not release the task backlog before the retrieval surface is
// confirmed healthy. 3 min chosen to mirror memoryRetrievalUnhealthyThreshold
// (3 cycles * 60s probe interval).
const memoryReadyStabilizationWindow = 3 * time.Minute

// memoryStablyReady reports whether p's memory stack has been continuously Ready
// for at least memoryReadyStabilizationWindow. Use this instead of a bare Phase==Ready
// check at task/lifecycle/ingest gate sites to prevent herd-release on return-to-healthy.
func memoryStablyReady(p *tataradevv1alpha1.Project, now time.Time) bool {
	if p.Status.Memory == nil || p.Status.Memory.Phase != "Ready" {
		return false
	}
	if p.Status.Memory.ReadySince == nil {
		return false
	}
	return now.Sub(p.Status.Memory.ReadySince.Time) >= memoryReadyStabilizationWindow
}

// reconcileMemory provisions the Project's memory stack and sets
// project.Status.Memory + the MemoryReady condition (it does NOT persist;
// the caller does one status update). It returns the requeue interval (set
// while Provisioning) and a non-nil error on a hard apply/password failure
// (recorded as phase=Failed + MemoryReady=False) or a transient health read
// error (which leaves the phase unchanged and requeues with backoff rather
// than flapping to Failed).
func (r *ProjectReconciler) reconcileMemory(ctx context.Context, p *tataradevv1alpha1.Project) (time.Duration, error) {
	l := log.FromContext(ctx)
	if !p.DeletionTimestamp.IsZero() {
		l.Info("project being deleted, skipping memory stack apply",
			"action", "memory_retire",
			"resource_id", p.Name)
		return 0, nil
	}

	p.Status.Memory = ensureMemoryStatus(p)
	prevPhase := p.Status.Memory.Phase
	p.Status.Memory.Endpoint = memory.Endpoint(p.Name, r.MemoryConfig.Namespace)
	p.Status.Memory.ExternalEndpoint = memory.ExternalMemoryURL(p.Name, r.MemoryConfig)

	if _, err := r.ensureNeo4jPassword(ctx, p); err != nil {
		return 0, r.failMemory(p, "PasswordError", err)
	}
	if err := r.applyMemoryStack(ctx, p); err != nil {
		return 0, r.failMemory(p, "ApplyError", err)
	}

	h, err := r.memoryStackHealth(ctx, p)
	if err != nil {
		// A non-NotFound read is a transient API/cache blip, not a real failure
		// (NotFound is already handled as not-yet-ready inside memoryStackHealth).
		// Leave the phase and MemoryReady condition as they are so a healthy
		// stack does not flap to Failed on a 30s blip. Return nil so the caller
		// preserves the 10s memoryRequeue cadence; returning an error here would
		// cause the caller to discard requeueAfter and fall back to exponential
		// backoff instead of the intended fixed poll.
		// Failed is reserved for genuine apply/password errors.
		if p.Status.Memory.Phase == "" {
			p.Status.Memory.Phase = "Provisioning"
		}
		r.Metrics.MemoryHealthReadError()
		l.Info("transient memory health read error, will retry",
			"action", "memory_health_retry",
			"resource_id", p.Name,
			"error", err.Error())
		return memoryRequeue, nil
	}

	phase := memoryPhase(h.pgReady, h.pgWant, h.neo4jReady, h.lightragAvail, h.memoryAvail)

	// Record which component is holding the stack and the CNPG instance/primary
	// detail, so a Provisioning or degraded-but-quorate stack is diagnosable from
	// the Project alone (issues #425, #442).
	p.Status.Memory.NotReady = h.notReadyComponents()
	p.Status.Memory.PgReadyInstances = h.pgReady
	p.Status.Memory.PgWantInstances = h.pgWant
	p.Status.Memory.PgPrimary = h.pgPrimary

	// Capture the current provisioning episode's start before the block below
	// clears it on reaching Ready; the provision-duration histogram measures
	// from here, not from the Project's creation.
	provisioningSince := p.Status.Memory.ProvisioningSince

	// Maintain ReadySince/ProvisioningSince for the stabilization debounce
	// (memoryStablyReady) and the Provisioning->Degraded timeout (issue #355).
	// ReadySince is set once on the Provisioning->Ready edge and cleared
	// whenever the phase leaves Ready. ProvisioningSince is the mirror: set on
	// any Ready/Failed/""->Provisioning edge, PRESERVED across a
	// Provisioning<->Degraded episode (so a stack does not get a fresh clock
	// every 10s poll while stuck), and cleared on reaching Ready.
	now := metav1.Now()
	if phase == "Ready" {
		if prevPhase != "Ready" {
			p.Status.Memory.ReadySince = &now
		}
		// else preserve existing ReadySince (steady-state; do not reset the clock)
		p.Status.Memory.ProvisioningSince = nil
	} else {
		p.Status.Memory.ReadySince = nil
		if prevPhase != "Provisioning" && prevPhase != "Degraded" {
			p.Status.Memory.ProvisioningSince = &now
		} else if p.Status.Memory.ProvisioningSince == nil {
			// Defensive: should be unreachable (prevPhase Provisioning/Degraded
			// implies a prior ProvisioningSince stamp), but never leave the timeout
			// check comparing against a nil pointer.
			p.Status.Memory.ProvisioningSince = &now
		}
		if r.MemoryConfig.ProvisioningTimeout > 0 &&
			now.Sub(p.Status.Memory.ProvisioningSince.Time) >= r.MemoryConfig.ProvisioningTimeout {
			phase = "Degraded"
		}
	}
	p.Status.Memory.Phase = phase

	// waiting names the components still below their gate, appended to every
	// not-Ready condition message so the reason is legible without a second
	// kubectl call (issue #425).
	waiting := ""
	if len(p.Status.Memory.NotReady) > 0 {
		waiting = " (waiting on: " + strings.Join(p.Status.Memory.NotReady, ", ") + ")"
	}
	condStatus := metav1.ConditionFalse
	reason := "Provisioning"
	msg := "memory stack provisioning" + waiting
	switch phase {
	case "Ready":
		condStatus = metav1.ConditionTrue
		reason = "Ready"
		msg = "memory stack ready at " + p.Status.Memory.Endpoint
		if prevPhase != "Ready" {
			// Measure the provisioning episode, not the Project's lifetime. Using
			// CreationTimestamp made every Provisioning->Ready RE-transition record
			// how old the Project was, which on a long-lived project swamped the
			// histogram. ProvisioningSince is nil only on a first provision that
			// never passed through Provisioning, where creation IS the episode start.
			start := p.CreationTimestamp.Time
			if provisioningSince != nil {
				start = provisioningSince.Time
			}
			r.Metrics.ObserveMemoryProvisionDuration(now.Sub(start).Seconds())
		}
		// Fold a sustained retrieval-probe failure into the condition. Replica
		// readiness alone cannot see a memory pod that is Available but serving a
		// stale or broken HTTP contract; updateMemoryRetrievalProbe meters that and
		// counts consecutive unhealthy cycles per project. Once that run reaches
		// memoryRetrievalUnhealthyThreshold (~3 min), a replica-Available stack
		// reads MemoryReady=False/RetrievalUnreachable instead of falsely green.
		// The replica gate stays the precondition (a still-Provisioning stack is
		// never probed) and phase stays "Ready", so the probe keeps running and the
		// condition clears itself once the surface recovers.
		//
		// A degraded-but-quorate postgres demotes the condition the same way
		// (issue #442: a standby sat permanently diverged on an orphaned timeline
		// for 8h42m and nothing detected it, because the quorum gate was still
		// satisfied). RetrievalUnreachable is checked first and wins: it means the
		// stack is not serving at all, the stronger statement, and keeping it
		// first leaves the pre-existing #355 behaviour byte-for-byte unchanged.
		switch {
		case r.memoryUnhealthyCycles[p.Name] >= memoryRetrievalUnhealthyThreshold:
			condStatus = metav1.ConditionFalse
			reason = "RetrievalUnreachable"
			msg = "memory replicas available but retrieval surface unreachable at " + p.Status.Memory.Endpoint
		default:
			if degraded := pgDegradedReason(h); degraded != "" {
				condStatus = metav1.ConditionFalse
				reason = "PostgresDegraded"
				msg = "memory serving on a quorum but postgres is degraded: " + degraded
				l.Info("memory postgres degraded while still quorate",
					"action", "memory_pg_degraded",
					"resource_id", p.Name,
					"pg_ready_instances", h.pgReady,
					"pg_want_instances", h.pgWant,
					"pg_primary", h.pgPrimary,
					"detail", degraded)
			}
		}
	case "Degraded":
		// Issue #355: a stuck backend must surface as a failing condition after a
		// bounded timeout instead of staying Provisioning indefinitely (the live
		// incident sat Provisioning for 7h+ with no signal at all). Phase stays
		// queryable/distinguishable from an ordinary in-flight Provisioning; the
		// stack keeps polling at memoryRequeue so it self-clears the moment the
		// backend actually becomes healthy.
		elapsed := now.Sub(p.Status.Memory.ProvisioningSince.Time).Round(time.Second)
		reason = "ProvisioningTimeout"
		msg = fmt.Sprintf("memory stack still provisioning after %s (exceeds %s timeout)%s",
			elapsed, r.MemoryConfig.ProvisioningTimeout, waiting)
	}
	meta.SetStatusCondition(&p.Status.Conditions, metav1.Condition{
		Type:               "MemoryReady",
		Status:             condStatus,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: p.Generation,
	})

	if phase == "Ready" {
		return 0, nil
	}
	return memoryRequeue, nil
}

// ensureMemoryStatus returns the existing status.memory or a fresh one.
func ensureMemoryStatus(p *tataradevv1alpha1.Project) *tataradevv1alpha1.MemoryStatus {
	if p.Status.Memory != nil {
		return p.Status.Memory
	}
	return &tataradevv1alpha1.MemoryStatus{}
}

// failMemory records phase=Failed + MemoryReady=False on the Project status
// and returns the wrapped error for the caller to surface. p.Status.Memory is
// always non-nil when called from reconcileMemory (set at entry), so no
// nil-guard is needed here.
func (r *ProjectReconciler) failMemory(p *tataradevv1alpha1.Project, reason string, err error) error {
	p.Status.Memory.Phase = "Failed"
	meta.SetStatusCondition(&p.Status.Conditions, metav1.Condition{
		Type:               "MemoryReady",
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            err.Error(),
		ObservedGeneration: p.Generation,
	})
	return fmt.Errorf("reconcile memory: %w", err)
}
