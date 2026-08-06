package memory_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
)

// projectWithAPIReplicas builds a Project whose memory API is sized to n.
func projectWithAPIReplicas(name string, n int32) *tatarav1alpha1.Project {
	p := testProject(name)
	p.Spec.Memory = &tatarav1alpha1.MemorySpec{APIReplicas: n}
	return p
}

// TestMemorySpreadIsHardAboveOneReplica pins the half of #528 that makes the
// apiReplicas knob mean anything.
//
// MaxSkew 1 + ScheduleAnyway is a scheduler SCORE, not a constraint: the
// resource-balance and image-locality plugins, and the weight-50 cross-project
// anti-affinity term, can all outvote it, so all N replicas can land on one
// node. A node reboot is then still a 100% outage - exactly what #528 filed.
// Above one replica the spread has to be DoNotSchedule to be worth anything.
//
// At one replica it stays ScheduleAnyway so a single-node dev cluster still
// schedules, and so every existing Project's pod template is byte-identical.
func TestMemorySpreadIsHardAboveOneReplica(t *testing.T) {
	cfg := testCfg()

	t.Run("soft at one replica", func(t *testing.T) {
		tsc := memory.MemoryDeployment(testProject("acme"), cfg).Spec.Template.Spec.TopologySpreadConstraints
		require.Len(t, tsc, 1)
		require.Equal(t, corev1.ScheduleAnyway, tsc[0].WhenUnsatisfiable)
		require.Nil(t, tsc[0].NodeTaintsPolicy, "no taints policy on the soft form: it must not rewrite existing pod templates")
	})

	t.Run("hard above one replica", func(t *testing.T) {
		tsc := memory.MemoryDeployment(projectWithAPIReplicas("acme", 3), cfg).Spec.Template.Spec.TopologySpreadConstraints
		require.Len(t, tsc, 1)
		require.Equal(t, corev1.DoNotSchedule, tsc[0].WhenUnsatisfiable)
		// nodeTaintsPolicy defaults to Ignore, which counts a cordoned node as an
		// eligible domain. Mid-drain that makes the drained node the only domain
		// with room under MaxSkew 1, so the replacement replica strands Pending.
		require.NotNil(t, tsc[0].NodeTaintsPolicy)
		require.Equal(t, corev1.NodeInclusionPolicyHonor, *tsc[0].NodeTaintsPolicy)
	})

	// lightrag and neo4j are structurally single-replica (RWO PVC + Recreate,
	// and a 1-replica StatefulSet). A hard constraint there could only wedge
	// them, so they stay soft whatever the API is sized to.
	t.Run("dependencies stay soft regardless of apiReplicas", func(t *testing.T) {
		p := projectWithAPIReplicas("acme", 3)
		for name, tsc := range map[string][]corev1.TopologySpreadConstraint{
			"lightrag": memory.LightragDeployment(p, cfg).Spec.Template.Spec.TopologySpreadConstraints,
			"neo4j":    memory.Neo4jStatefulSet(p, cfg).Spec.Template.Spec.TopologySpreadConstraints,
		} {
			t.Run(name, func(t *testing.T) {
				require.Len(t, tsc, 1)
				require.Equal(t, corev1.ScheduleAnyway, tsc[0].WhenUnsatisfiable)
			})
		}
	})
}

// TestStartupProbeTimeoutIsExplicit closes the last unset probe timeout.
//
// The startupProbe is the one covering the exact window #528 is about: the
// 60s waitForDB retry, OIDC discovery and schema migrations. Leaving
// timeoutSeconds unset gives it the Kubernetes default of 1s, so a slow
// dependency fails the probe it exists to tolerate.
func TestStartupProbeTimeoutIsExplicit(t *testing.T) {
	c := memory.MemoryDeployment(testProject("acme"), testCfg()).Spec.Template.Spec.Containers[0]
	require.Equal(t, int32(5), c.StartupProbe.TimeoutSeconds)
	require.Equal(t, int32(5), c.LivenessProbe.TimeoutSeconds)
	require.Equal(t, int32(5), c.ReadinessProbe.TimeoutSeconds)
}

// TestPDBUnhealthyEvictionPolicyIsGatedOnReplicaCount.
//
// With maxUnavailable=1, DesiredHealthy = replicas-1.
//
//   - At 1 replica DesiredHealthy is 0, and IfHealthyBudget's fast path requires
//     DesiredHealthy > 0, so an unready pod falls through to checkAndDecrement
//     with disruptionsAllowed = currentHealthy = 0 and the drain is refused
//     outright. AlwaysAllow is required to avoid being worse than no PDB.
//   - Above 1 replica AlwaysAllow makes the eviction API skip the budget for
//     every Running-but-unready pod, so a rolling multi-node reboot can evict
//     all N still-cold-starting replicas at once - the exact population a reboot
//     churns, and the one the PDB exists to serialise. IfHealthyBudget keeps the
//     desired=1 deadlock closed (DesiredHealthy > 0 there) while refusing that.
func TestPDBUnhealthyEvictionPolicyIsGatedOnReplicaCount(t *testing.T) {
	cfg := testCfg()

	t.Run("AlwaysAllow at one replica", func(t *testing.T) {
		pdb := memory.MemoryPDB(testProject("acme"), cfg)
		require.NotNil(t, pdb.Spec.UnhealthyPodEvictionPolicy)
		require.Equal(t, policyv1.AlwaysAllow, *pdb.Spec.UnhealthyPodEvictionPolicy)
	})

	t.Run("IfHealthyBudget above one replica", func(t *testing.T) {
		pdb := memory.MemoryPDB(projectWithAPIReplicas("acme", 3), cfg)
		require.NotNil(t, pdb.Spec.UnhealthyPodEvictionPolicy)
		require.Equal(t, policyv1.IfHealthyBudget, *pdb.Spec.UnhealthyPodEvictionPolicy)
	})

	// lightrag is pinned to one replica by its RWO PVC whatever the API says.
	t.Run("lightrag is always the one-replica case", func(t *testing.T) {
		pdb := memory.LightragPDB(projectWithAPIReplicas("acme", 3), cfg)
		require.Equal(t, policyv1.AlwaysAllow, *pdb.Spec.UnhealthyPodEvictionPolicy)
	})
}
