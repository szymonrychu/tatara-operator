package controller

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
		pw := string(existing.Data["password"])
		if pw == "" {
			return "", fmt.Errorf("neo4j secret %s missing password key", names.Neo4jSecret)
		}
		return pw, nil
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
			return string(existing.Data["password"]), nil
		}
		return "", fmt.Errorf("create neo4j secret: %w", err)
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

// effectivePGInstances returns the configured pg instance count for the
// Project, defaulting to 1 when spec.memory is unset or zero.
func effectivePGInstances(p *tataradevv1alpha1.Project) int {
	if p.Spec.Memory != nil && p.Spec.Memory.PgInstances > 0 {
		return p.Spec.Memory.PgInstances
	}
	return 1
}

// applyMemoryStack server-side-applies every object in the Project's memory
// stack (owner-ref'd by the N1 builders). The neo4j password Secret is created
// separately by ensureNeo4jPassword and is NOT applied here, so it is never
// rotated.
func (r *ProjectReconciler) applyMemoryStack(ctx context.Context, p *tataradevv1alpha1.Project, neo4jPassword string) error {
	cfg := r.MemoryConfig
	objs := []client.Object{
		memory.PGCluster(p, cfg),
		memory.Neo4jStatefulSet(p, cfg),
		memory.Neo4jService(p, cfg),
		memory.LightragPVC(p, cfg),
		memory.LightragDeployment(p, cfg),
		memory.LightragService(p, cfg),
		memory.MemoryConfigMap(p, cfg),
		memory.MemorySecret(p, cfg),
		memory.MemoryDeployment(p, cfg),
		memory.MemoryService(p, cfg),
	}
	for _, obj := range objs {
		if err := r.Patch(ctx, obj, client.Apply,
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

	var cluster cnpgv1.Cluster
	if e := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: names.PGCluster}, &cluster); e != nil {
		return 0, 0, 0, 0, fmt.Errorf("get cnpg cluster: %w", e)
	}
	readyInstances = cluster.Status.ReadyInstances

	var sts appsv1.StatefulSet
	if e := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: names.Neo4j}, &sts); e != nil {
		return 0, 0, 0, 0, fmt.Errorf("get neo4j statefulset: %w", e)
	}
	neo4jReady = sts.Status.ReadyReplicas

	var lightrag appsv1.Deployment
	if e := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: names.Lightrag}, &lightrag); e != nil {
		return 0, 0, 0, 0, fmt.Errorf("get lightrag deployment: %w", e)
	}
	lightragAvail = lightrag.Status.AvailableReplicas

	var mem appsv1.Deployment
	if e := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: names.Memory}, &mem); e != nil {
		return 0, 0, 0, 0, fmt.Errorf("get memory deployment: %w", e)
	}
	memoryAvail = mem.Status.AvailableReplicas

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
