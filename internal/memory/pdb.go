package memory

import (
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// componentPDB builds the PodDisruptionBudget for one memory-stack component.
//
// maxUnavailable, never minAvailable: minAvailable=1 against a 1-replica
// Deployment computes disruptionsAllowed=0 for a healthy pod, so the API
// server denies EVERY eviction and a node drain blocks outright. For an
// issue whose whole subject is node-reboot tolerance that would be strictly
// worse than shipping no PDB at all. maxUnavailable=1 is a no-op at one
// replica (it permits the single eviction, exactly today's behaviour) and
// serialises disruption once apiReplicas goes above 1, so drains take one
// replica at a time instead of all of them.
//
// unhealthyPodEvictionPolicy is AlwaysAllow at EVERY replica count. The obvious
// tightening - AlwaysAllow at one replica, the IfHealthyBudget default above,
// since above one replica AlwaysAllow lets a drain evict every not-yet-Ready
// replica at once - was tried and reverted. It deadlocks drains on this stack.
//
// The relevant upstream code (kubernetes release-1.33; the semantics below are
// NOT in the policyv1 godoc, which omits the `DesiredHealthy > 0` guard, so read
// the source and not the vendored comment before "correcting" this):
//
//	// pkg/registry/core/pod/storage/eviction.go
//	if !podutil.IsPodReady(pod) {
//	    if policy == policyv1.AlwaysAllow { return nil }
//	    if pdb.Status.CurrentHealthy >= pdb.Status.DesiredHealthy &&
//	       pdb.Status.DesiredHealthy > 0 { return nil }
//	}
//	// pkg/controller/disruption/disruption.go
//	desiredHealthy = expectedCount - maxUnavailable
//	disruptionsAllowed := currentHealthy - desiredHealthy
//	if expectedCount <= 0 || disruptionsAllowed <= 0 { disruptionsAllowed = 0 }
//
// With maxUnavailable=1, DesiredHealthy is replicas-1, and:
//
//   - At replicas == 1, DesiredHealthy is 0. IfHealthyBudget's unready fast path
//     is skipped by its own `DesiredHealthy > 0` guard, and checkAndDecrement
//     sees disruptionsAllowed = currentHealthy = 0. Every eviction 429s, so the
//     drain of the node an unready pod is stuck on is refused outright -
//     strictly worse than shipping no PDB, on an issue about reboot tolerance.
//   - At replicas > 1, whenever currentHealthy < DesiredHealthy BOTH branches
//     refuse, for healthy and unhealthy pods alike, and nothing in the budget
//     can clear it. On this stack that state is routine rather than exotic:
//     /readyz round-trips LightRAG, which is single-replica by storage (an RWO
//     PVC plus a Recreate strategy), so rebooting LightRAG's node takes all N
//     API replicas NotReady together - currentHealthy 0 against DesiredHealthy 2. A drain on
//     any node holding an API pod then wedges permanently, and the thing that
//     would clear it is itself waiting for a node to come back. Opting into HA
//     would turn a self-healing 25-minute outage into a stuck cluster reboot
//     that also blocks every unrelated workload on those nodes.
//
// AlwaysAllow bounds the damage: it only ever skips the budget for pods that are
// NOT Ready - already out of the Service's endpoints, serving nothing - while
// Ready pods stay gated by checkAndDecrement identically under either policy.
// Losing a cold-starting replica costs a restart; an unsatisfiable budget costs
// the drain. Requires Kubernetes 1.27+ (GA since 1.31); this operator targets
// 1.33.
//
// Scope limit, stated because it is easy to over-read: maxUnavailable=1
// serialises evictions of READY pods above one replica. It does not make a node
// drain never stall, and it does not protect a replica that is merely cold.
//
// Emitted unconditionally, not gated on apiReplicas > 1: because
// maxUnavailable=1 is harmless at one replica, an unconditional apply needs
// no delete path (contrast the ScheduledBackup in applyMemoryStack, which
// must be deleted when its feature is turned off).
func componentPDB(p *tatarav1alpha1.Project, cfg Config, name, component string) *policyv1.PodDisruptionBudget {
	maxUnavailable := intstr.FromInt32(1)
	policy := policyv1.AlwaysAllow
	return &policyv1.PodDisruptionBudget{
		TypeMeta:   metav1.TypeMeta{APIVersion: "policy/v1", Kind: "PodDisruptionBudget"},
		ObjectMeta: objectMeta(p, cfg, name),
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable:             &maxUnavailable,
			Selector:                   &metav1.LabelSelector{MatchLabels: selectorLabels(p.Name, component)},
			UnhealthyPodEvictionPolicy: &policy,
		},
	}
}

// MemoryPDB builds the PodDisruptionBudget for the tatara-memory Deployment.
func MemoryPDB(p *tatarav1alpha1.Project, cfg Config) *policyv1.PodDisruptionBudget {
	return componentPDB(p, cfg, NamesFor(p.Name).Memory, "memory")
}

// LightragPDB builds the PodDisruptionBudget for the lightrag Deployment.
//
// lightrag's Deployment is pinned to one replica by an RWO data PVC plus a
// Recreate strategy, so this PDB is documentation of intent and a
// drain-safety no-op today rather than active protection.
func LightragPDB(p *tatarav1alpha1.Project, cfg Config) *policyv1.PodDisruptionBudget {
	return componentPDB(p, cfg, NamesFor(p.Name).Lightrag, "lightrag")
}
