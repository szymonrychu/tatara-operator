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
	objs := []client.Object{
		memory.PGCluster(p, cfg),
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

// memoryPhase returns "Ready" when cnpg has at least the wanted ready
// instances AND neo4j, lightrag and memory each report at least one ready /
// available replica; otherwise "Provisioning".
func memoryPhase(readyInstances, wantInstances int, neo4jReady, lightragAvail, memoryAvail int32) string {
	if readyInstances >= wantInstances && neo4jReady >= 1 && lightragAvail >= 1 && memoryAvail >= 1 {
		return "Ready"
	}
	return "Provisioning"
}

// memoryRequeue is how often the reconciler re-checks a Provisioning stack.
const memoryRequeue = 10 * time.Second

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
