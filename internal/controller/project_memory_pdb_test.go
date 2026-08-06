package controller

import (
	"context"
	"testing"

	"github.com/szymonrychu/tatara-operator/internal/memory"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/types"
)

// TestApplyMemoryStack_EmitsBothPDBs verifies that applyMemoryStack creates
// both memory and lightrag PodDisruptionBudgets owned by the Project.
func TestApplyMemoryStack_EmitsBothPDBs(t *testing.T) {
	ctx := context.Background()
	r := newMemoryReconciler()
	p := mkMemoryProject(t, "stack-pdb")

	if _, err := r.ensureNeo4jPassword(ctx, p); err != nil {
		t.Fatalf("ensureNeo4jPassword: %v", err)
	}
	if err := r.applyMemoryStack(ctx, p); err != nil {
		t.Fatalf("applyMemoryStack: %v", err)
	}

	names := memory.NamesFor(p.Name)

	// Memory PDB must exist and be owned by the Project.
	var memPDB policyv1.PodDisruptionBudget
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: names.Memory}, &memPDB); err != nil {
		t.Fatalf("get memory PDB: %v", err)
	}
	assertOwnedByProject(t, memPDB.GetOwnerReferences(), p.Name)

	// Lightrag PDB must exist and be owned by the Project.
	var lightragPDB policyv1.PodDisruptionBudget
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: names.Lightrag}, &lightragPDB); err != nil {
		t.Fatalf("get lightrag PDB: %v", err)
	}
	assertOwnedByProject(t, lightragPDB.GetOwnerReferences(), p.Name)
}
