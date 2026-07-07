package controller

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
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
	cfg := r.MemoryConfig
	pgCluster, err := r.guardPGStorageShrink(ctx, p, memory.PGCluster(p, cfg))
	if err != nil {
		return err
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

// memoryStackHealth reads the owned objects' statuses and returns the readiness
// inputs for memoryPhase: cnpg readyInstances, neo4j readyReplicas, lightrag
// availableReplicas, memory availableReplicas.
func (r *ProjectReconciler) memoryStackHealth(ctx context.Context, p *tataradevv1alpha1.Project) (readyInstances int, neo4jReady, lightragAvail, memoryAvail int32, err error) {
	names := memory.NamesFor(p.Name)
	ns := r.MemoryConfig.Namespace

	// A NotFound read means the object was SSA-applied moments ago and is not
	// yet visible in the informer cache (or has not been created yet). That is
	// not-yet-ready, not a failure: leave the count at zero so memoryPhase
	// reports Provisioning. Only a genuine (non-NotFound) read error is returned.
	var cluster cnpgv1.Cluster
	if e := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: names.PGCluster}, &cluster); e != nil {
		if !apierrors.IsNotFound(e) {
			return 0, 0, 0, 0, fmt.Errorf("get cnpg cluster: %w", e)
		}
	} else {
		readyInstances = cluster.Status.ReadyInstances
	}

	var sts appsv1.StatefulSet
	if e := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: names.Neo4j}, &sts); e != nil {
		if !apierrors.IsNotFound(e) {
			return 0, 0, 0, 0, fmt.Errorf("get neo4j statefulset: %w", e)
		}
	} else {
		neo4jReady = sts.Status.ReadyReplicas
	}

	var lightrag appsv1.Deployment
	if e := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: names.Lightrag}, &lightrag); e != nil {
		if !apierrors.IsNotFound(e) {
			return 0, 0, 0, 0, fmt.Errorf("get lightrag deployment: %w", e)
		}
	} else {
		lightragAvail = lightrag.Status.AvailableReplicas
	}

	var mem appsv1.Deployment
	if e := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: names.Memory}, &mem); e != nil {
		if !apierrors.IsNotFound(e) {
			return 0, 0, 0, 0, fmt.Errorf("get memory deployment: %w", e)
		}
	} else {
		memoryAvail = mem.Status.AvailableReplicas
	}

	return readyInstances, neo4jReady, lightragAvail, memoryAvail, nil
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

	readyInstances, neo4jReady, lightragAvail, memoryAvail, err := r.memoryStackHealth(ctx, p)
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

	phase := memoryPhase(readyInstances, memory.PgInstances(p), neo4jReady, lightragAvail, memoryAvail)
	p.Status.Memory.Phase = phase

	// Maintain ReadySince for the stabilization debounce (memoryStablyReady).
	// Set once on the Provisioning->Ready edge; preserve on steady-state Ready;
	// clear whenever the phase leaves Ready so the clock resets on re-entry.
	now := metav1.Now()
	if phase == "Ready" {
		if prevPhase != "Ready" {
			p.Status.Memory.ReadySince = &now
		}
		// else preserve existing ReadySince (steady-state; do not reset the clock)
	} else {
		p.Status.Memory.ReadySince = nil
	}

	condStatus := metav1.ConditionFalse
	reason := "Provisioning"
	msg := "memory stack provisioning"
	if phase == "Ready" {
		condStatus = metav1.ConditionTrue
		reason = "Ready"
		msg = "memory stack ready at " + p.Status.Memory.Endpoint
		if prevPhase != "Ready" {
			r.Metrics.ObserveMemoryProvisionDuration(time.Since(p.CreationTimestamp.Time).Seconds())
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
		if r.memoryUnhealthyCycles[p.Name] >= memoryRetrievalUnhealthyThreshold {
			condStatus = metav1.ConditionFalse
			reason = "RetrievalUnreachable"
			msg = "memory replicas available but retrieval surface unreachable at " + p.Status.Memory.Endpoint
		}
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
