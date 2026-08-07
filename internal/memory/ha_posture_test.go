package memory_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
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

// TestPDBUnhealthyEvictionPolicyIsAlwaysAllow pins AlwaysAllow at EVERY replica
// count, and exists mostly to stop the obvious "tighten this" edit.
//
// The tempting change is to gate it - AlwaysAllow at one replica, the
// IfHealthyBudget default above - on the grounds that above one replica
// AlwaysAllow lets a drain evict every not-yet-Ready replica at once. That
// eviction is real, but it is the lesser failure, and gating deadlocks drains
// on this stack specifically. Both halves verified against kubernetes
// release-1.33:
//
//		// pkg/registry/core/pod/storage/eviction.go
//		if !podutil.IsPodReady(pod) {
//		    if policy == policyv1.AlwaysAllow { return nil }
//		    if pdb.Status.CurrentHealthy >= pdb.Status.DesiredHealthy &&
//		       pdb.Status.DesiredHealthy > 0 { return nil }
//		}
//		// pkg/controller/disruption/disruption.go
//		desiredHealthy = expectedCount - maxUnavailable
//		disruptionsAllowed := currentHealthy - desiredHealthy
//		if expectedCount <= 0 || disruptionsAllowed <= 0 { disruptionsAllowed = 0 }
//
//	  - At 1 replica: DesiredHealthy is 0, so IfHealthyBudget's unready fast path
//	    is skipped by its own `DesiredHealthy > 0` guard and checkAndDecrement sees
//	    disruptionsAllowed = currentHealthy = 0. Every eviction 429s and the drain
//	    of the node the pod is stuck on is refused outright - strictly worse than
//	    shipping no PDB, on an issue about node-reboot tolerance.
//	  - Above 1 replica: whenever currentHealthy < DesiredHealthy (= replicas-1),
//	    the same two branches BOTH refuse, for healthy and unhealthy pods alike,
//	    and nothing in the budget can clear it. On this stack that state is not
//	    exotic: /readyz round-trips LightRAG, which is single-replica by storage
//	    (RWO PVC + Recreate), so rebooting LightRAG's node takes all N API
//	    replicas NotReady together - currentHealthy 0, DesiredHealthy 2. A drain
//	    on any node holding an API pod then wedges permanently, and the thing that
//	    would clear it is itself waiting for a node to come back. Opting into HA
//	    would convert a self-healing 25-minute outage into a stuck cluster reboot.
//
// AlwaysAllow bounds the damage instead: it only ever skips the budget for pods
// that are NOT Ready, i.e. pods already out of the Service's endpoints and
// serving nothing. Ready pods stay gated by checkAndDecrement identically under
// both policies. Losing cold-starting replicas costs a restart; a budget that
// can never be satisfied costs the whole node drain.
func TestPDBUnhealthyEvictionPolicyIsAlwaysAllow(t *testing.T) {
	cfg := testCfg()

	for name, p := range map[string]*tatarav1alpha1.Project{
		"one replica":    testProject("acme"),
		"three replicas": projectWithAPIReplicas("acme", 3),
		"maximum of ten": projectWithAPIReplicas("acme", 10),
	} {
		t.Run(name, func(t *testing.T) {
			pdb := memory.MemoryPDB(p, cfg)
			require.NotNil(t, pdb.Spec.UnhealthyPodEvictionPolicy)
			require.Equal(t, policyv1.AlwaysAllow, *pdb.Spec.UnhealthyPodEvictionPolicy,
				"IfHealthyBudget wedges the drain whenever currentHealthy < replicas-1, which /readyz's LightRAG coupling makes routine")
			// maxUnavailable must stay 1 whatever the replica count: it is what
			// serialises disruption, and it is what makes DesiredHealthy replicas-1.
			require.Equal(t, intstr.FromInt32(1), *pdb.Spec.MaxUnavailable)
			require.Nil(t, pdb.Spec.MinAvailable)
		})
	}

	t.Run("lightrag", func(t *testing.T) {
		pdb := memory.LightragPDB(projectWithAPIReplicas("acme", 3), cfg)
		require.Equal(t, policyv1.AlwaysAllow, *pdb.Spec.UnhealthyPodEvictionPolicy)
	})
}
