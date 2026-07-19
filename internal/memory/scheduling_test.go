package memory_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// nativePodSpecs returns the pod template spec each native memory-stack builder
// (memory/neo4j/lightrag) produces for project acme under cfg, keyed by the
// component label the spreading rules must select on.
func nativePodSpecs(cfg memory.Config) map[string]corev1.PodSpec {
	p := testProject("acme")
	return map[string]corev1.PodSpec{
		"memory":   memory.MemoryDeployment(p, cfg).Spec.Template.Spec,
		"neo4j":    memory.Neo4jStatefulSet(p, cfg).Spec.Template.Spec,
		"lightrag": memory.LightragDeployment(p, cfg).Spec.Template.Spec,
	}
}

// assertComponentAffinity asserts the soft two-term pod-anti-affinity: weight
// 100 on this project's own component replicas, weight 50 on any OTHER project's
// memory-stack pod (the #327 cross-project term). Both terms use wantTopoKey.
func assertComponentAffinity(t *testing.T, aff *corev1.Affinity, component, wantTopoKey string) {
	t.Helper()
	require.NotNil(t, aff, "Affinity must be set")
	require.NotNil(t, aff.PodAntiAffinity, "PodAntiAffinity must be set")
	terms := aff.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	require.Len(t, terms, 2)

	// weight 100: spread this project's own replicas of this component.
	require.Equal(t, int32(100), terms[0].Weight)
	require.Equal(t, wantTopoKey, terms[0].PodAffinityTerm.TopologyKey)
	require.Equal(t, map[string]string{
		"app.kubernetes.io/instance":  "mem-acme",
		"app.kubernetes.io/component": component,
	}, terms[0].PodAffinityTerm.LabelSelector.MatchLabels)
	require.Empty(t, terms[0].PodAffinityTerm.LabelSelector.MatchExpressions)

	// weight 50: avoid co-locating with any other project's memory-stack pod.
	assertCrossProjectTerm(t, terms[1], wantTopoKey)
}

// assertCrossProjectTerm asserts the weight-50 cross-project anti-affinity term:
// name=tatara-memory AND project NotIn [acme] on wantTopoKey.
func assertCrossProjectTerm(t *testing.T, wt corev1.WeightedPodAffinityTerm, wantTopoKey string) {
	t.Helper()
	require.Equal(t, int32(50), wt.Weight)
	require.Equal(t, wantTopoKey, wt.PodAffinityTerm.TopologyKey)
	sel := wt.PodAffinityTerm.LabelSelector
	require.Equal(t, map[string]string{"app.kubernetes.io/name": "tatara-memory"}, sel.MatchLabels)
	require.Len(t, sel.MatchExpressions, 1)
	require.Equal(t, "tatara.dev/project", sel.MatchExpressions[0].Key)
	require.Equal(t, metav1.LabelSelectorOpNotIn, sel.MatchExpressions[0].Operator)
	require.Equal(t, []string{"acme"}, sel.MatchExpressions[0].Values)
}

// assertTopologySpread asserts the single soft spread constraint fanning this
// project's own component replicas across wantTopoKey.
func assertTopologySpread(t *testing.T, tsc []corev1.TopologySpreadConstraint, component, wantTopoKey string) {
	t.Helper()
	require.Len(t, tsc, 1)
	require.Equal(t, int32(1), tsc[0].MaxSkew)
	require.Equal(t, wantTopoKey, tsc[0].TopologyKey)
	require.Equal(t, corev1.ScheduleAnyway, tsc[0].WhenUnsatisfiable)
	require.Equal(t, map[string]string{
		"app.kubernetes.io/instance":  "mem-acme",
		"app.kubernetes.io/component": component,
	}, tsc[0].LabelSelector.MatchLabels)
}

// TestNativeWorkloadSpreading covers the three native builders: default topology
// key when cfg.TopologyKey is empty, and honoring an override.
func TestNativeWorkloadSpreading(t *testing.T) {
	t.Run("defaults to kubernetes.io/hostname", func(t *testing.T) {
		for component, spec := range nativePodSpecs(memory.Config{}) {
			t.Run(component, func(t *testing.T) {
				assertComponentAffinity(t, spec.Affinity, component, "kubernetes.io/hostname")
				assertTopologySpread(t, spec.TopologySpreadConstraints, component, "kubernetes.io/hostname")
			})
		}
	})

	t.Run("honors TopologyKey override", func(t *testing.T) {
		cfg := testCfg()
		cfg.TopologyKey = "topology.kubernetes.io/zone"
		for component, spec := range nativePodSpecs(cfg) {
			t.Run(component, func(t *testing.T) {
				assertComponentAffinity(t, spec.Affinity, component, "topology.kubernetes.io/zone")
				assertTopologySpread(t, spec.TopologySpreadConstraints, component, "topology.kubernetes.io/zone")
			})
		}
	})
}

// TestPGClusterSpreading covers the cnpg Cluster: cnpg's own within-cluster
// anti-affinity (PodAntiAffinityType preferred + TopologyKey) satisfies spreading
// this project's pg instances, and AdditionalPodAntiAffinity carries the
// cross-project (#327) term so different projects' pg clusters avoid co-locating.
func TestPGClusterSpreading(t *testing.T) {
	t.Run("defaults to kubernetes.io/hostname", func(t *testing.T) {
		c := memory.PGCluster(testProject("acme"), memory.Config{})
		require.Equal(t, "kubernetes.io/hostname", c.Spec.Affinity.TopologyKey)
		require.Equal(t, "preferred", c.Spec.Affinity.PodAntiAffinityType)
		require.NotNil(t, c.Spec.Affinity.AdditionalPodAntiAffinity)
		terms := c.Spec.Affinity.AdditionalPodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution
		require.Len(t, terms, 1)
		assertCrossProjectTerm(t, terms[0], "kubernetes.io/hostname")
	})

	t.Run("honors TopologyKey override", func(t *testing.T) {
		cfg := testCfg()
		cfg.TopologyKey = "topology.kubernetes.io/zone"
		c := memory.PGCluster(testProject("acme"), cfg)
		require.Equal(t, "topology.kubernetes.io/zone", c.Spec.Affinity.TopologyKey)
		require.Equal(t, "preferred", c.Spec.Affinity.PodAntiAffinityType)
		require.NotNil(t, c.Spec.Affinity.AdditionalPodAntiAffinity)
		terms := c.Spec.Affinity.AdditionalPodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution
		require.Len(t, terms, 1)
		assertCrossProjectTerm(t, terms[0], "topology.kubernetes.io/zone")
	})
}
