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
// unhealthyPodEvictionPolicy is gated on the replica count, because the two
// cases want opposite things. With maxUnavailable=1 the eviction API's
// DesiredHealthy is replicas-1:
//
//   - replicas == 1 -> AlwaysAllow. DesiredHealthy is 0, and IfHealthyBudget's
//     fast path for an unready pod requires DesiredHealthy > 0, so the request
//     falls through to checkAndDecrement with disruptionsAllowed =
//     currentHealthy = 0. An already-unready pod would then pin the budget at 0
//     and block the drain of the node it is stuck on outright - strictly worse
//     than shipping no PDB at all, on an issue about node-reboot tolerance.
//   - replicas > 1 -> IfHealthyBudget (the API default). AlwaysAllow there makes
//     the eviction API skip the budget entirely for every Running-but-unready
//     pod, so a rolling multi-node reboot can evict all N still-cold-starting
//     replicas at once. Cold-starting replicas are exactly the population a
//     reboot churns, and serialising their disruption is what this object is
//     for. IfHealthyBudget still admits an unready pod once currentHealthy >=
//     DesiredHealthy, so it does not reintroduce the deadlock above.
//
// Requires Kubernetes 1.27+ (the field is GA since 1.31); this operator
// targets 1.33.
//
// Scope limit, stated because it is easy to over-read: above one replica this
// serialises evictions, it does not make a node drain never stall. A drain that
// cannot respect the budget still blocks, which is the point.
//
// Emitted unconditionally, not gated on apiReplicas > 1: because
// maxUnavailable=1 is harmless at one replica, an unconditional apply needs
// no delete path (contrast the ScheduledBackup in applyMemoryStack, which
// must be deleted when its feature is turned off).
func componentPDB(p *tatarav1alpha1.Project, cfg Config, name, component string, replicas int32) *policyv1.PodDisruptionBudget {
	maxUnavailable := intstr.FromInt32(1)
	policy := policyv1.AlwaysAllow
	if replicas > 1 {
		policy = policyv1.IfHealthyBudget
	}
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
	return componentPDB(p, cfg, NamesFor(p.Name).Memory, "memory", APIReplicas(p))
}

// LightragPDB builds the PodDisruptionBudget for the lightrag Deployment.
//
// lightrag's Deployment is pinned to one replica by an RWO data PVC plus a
// Recreate strategy, so this PDB is documentation of intent and a
// drain-safety no-op today rather than active protection. It passes a literal 1
// for the same reason: it is always the one-replica case, whatever the API is
// sized to.
func LightragPDB(p *tatarav1alpha1.Project, cfg Config) *policyv1.PodDisruptionBudget {
	return componentPDB(p, cfg, NamesFor(p.Name).Lightrag, "lightrag", 1)
}
