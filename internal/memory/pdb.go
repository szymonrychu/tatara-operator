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
// unhealthyPodEvictionPolicy=AlwaysAllow: with maxUnavailable set and
// desired=1, disruptionsAllowed reduces to currentHealthy, so an
// already-unready pod would pin disruptionsAllowed at 0 and block the drain
// of the node it is stuck on. AlwaysAllow lets a Running-but-unready pod
// always be evicted, which removes that deadlock while keeping the
// healthy-pod budget intact. Requires Kubernetes 1.27+ (the field is GA
// since 1.31); this operator targets 1.33.
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
