package memory_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func TestMemoryPDB(t *testing.T) {
	p := testProject("acme")
	cfg := testCfg()
	pdb := memory.MemoryPDB(p, cfg)

	// Name must follow the mem-<project> pattern.
	require.Equal(t, "mem-acme", pdb.Name)
	require.Equal(t, "tatara", pdb.Namespace)

	// TypeMeta must be set explicitly for SSA.
	require.Equal(t, "policy/v1", pdb.APIVersion)
	require.Equal(t, "PodDisruptionBudget", pdb.Kind)

	// Must have exactly one controller ownerRef to the Project.
	require.Len(t, pdb.OwnerReferences, 1)
	require.True(t, *pdb.OwnerReferences[0].Controller)
	require.Equal(t, "Project", pdb.OwnerReferences[0].Kind)
	require.Equal(t, "acme", pdb.OwnerReferences[0].Name)

	// Spec: maxUnavailable=1, minAvailable=nil (minAvailable=1 at replicas=1
	// denies every eviction and blocks node drain, strictly worse than today
	// for the node-reboot incident this fixes).
	require.Equal(t, intstr.FromInt32(1), *pdb.Spec.MaxUnavailable)
	require.Nil(t, pdb.Spec.MinAvailable)

	// UnhealthyPodEvictionPolicy must be AlwaysAllow (with maxUnavailable and
	// desired=1, disruptionsAllowed==currentHealthy, so an already-unready
	// pod would otherwise block the drain).
	require.NotNil(t, pdb.Spec.UnhealthyPodEvictionPolicy)
	require.Equal(t, policyv1.AlwaysAllow, *pdb.Spec.UnhealthyPodEvictionPolicy)

	// Selector must match the MemoryDeployment pod selector.
	dep := memory.MemoryDeployment(p, cfg)
	require.Equal(t, dep.Spec.Selector.MatchLabels, pdb.Spec.Selector.MatchLabels)
}

func TestLightragPDB(t *testing.T) {
	p := testProject("acme")
	cfg := testCfg()
	pdb := memory.LightragPDB(p, cfg)

	// Name must follow the mem-<project>-lightrag pattern.
	require.Equal(t, "mem-acme-lightrag", pdb.Name)
	require.Equal(t, "tatara", pdb.Namespace)

	// TypeMeta must be set explicitly for SSA.
	require.Equal(t, "policy/v1", pdb.APIVersion)
	require.Equal(t, "PodDisruptionBudget", pdb.Kind)

	// Must have exactly one controller ownerRef to the Project.
	require.Len(t, pdb.OwnerReferences, 1)
	require.True(t, *pdb.OwnerReferences[0].Controller)
	require.Equal(t, "Project", pdb.OwnerReferences[0].Kind)
	require.Equal(t, "acme", pdb.OwnerReferences[0].Name)

	// Spec: maxUnavailable=1, minAvailable=nil.
	require.Equal(t, intstr.FromInt32(1), *pdb.Spec.MaxUnavailable)
	require.Nil(t, pdb.Spec.MinAvailable)

	// UnhealthyPodEvictionPolicy must be AlwaysAllow.
	require.NotNil(t, pdb.Spec.UnhealthyPodEvictionPolicy)
	require.Equal(t, policyv1.AlwaysAllow, *pdb.Spec.UnhealthyPodEvictionPolicy)

	// Selector must match the LightragDeployment pod selector.
	dep := memory.LightragDeployment(p, cfg)
	require.Equal(t, dep.Spec.Selector.MatchLabels, pdb.Spec.Selector.MatchLabels)
}
