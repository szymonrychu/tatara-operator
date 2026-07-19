package memory

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// defaultTopologyKey is the node-identity topology domain the memory-stack
// spreading defaults to when the operator sets no MEMORY_TOPOLOGY_KEY. Applied
// here (not in config.Load) so a zero-value memory.Config{} still spreads
// correctly in unit tests and any caller that builds Config directly.
const defaultTopologyKey = "kubernetes.io/hostname"

// topologyKey resolves the topology domain used by every memory-stack spreading
// rule: cfg.TopologyKey when the operator supplied one (MEMORY_TOPOLOGY_KEY),
// else the per-node default. Kept cluster-agnostic (rule 14): the deploying
// helmfile picks the real domain (e.g. a rack/zone label).
func topologyKey(cfg Config) string {
	if cfg.TopologyKey != "" {
		return cfg.TopologyKey
	}
	return defaultTopologyKey
}

// crossProjectAntiAffinityTerm is the pod-anti-affinity term that keeps a
// project's memory-stack pod off any node already hosting ANOTHER project's
// memory-stack pod of ANY component. It matches the shared cross-project pin-set
// label (app.kubernetes.io/name=tatara-memory) AND excludes this project's own
// pods (tatara.dev/project NotIn [project]).
//
// This is the #327 fix: the incident was a single node hosting lightrag+neo4j+pg
// for BOTH projects, so isolating that node took both projects' memory API down.
// A component-scoped rule would not have caught it (the co-located pods were
// different components); this is deliberately component-agnostic.
func crossProjectAntiAffinityTerm(project, topoKey string) corev1.PodAffinityTerm {
	return corev1.PodAffinityTerm{
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"app.kubernetes.io/name": "tatara-memory"},
			MatchExpressions: []metav1.LabelSelectorRequirement{{
				Key:      "tatara.dev/project",
				Operator: metav1.LabelSelectorOpNotIn,
				Values:   []string{project},
			}},
		},
		TopologyKey: topoKey,
	}
}

// componentAffinity builds the soft pod-anti-affinity applied to a memory-stack
// workload's pod template. Two weighted preferred terms, both soft so a
// single-node (dev) cluster never wedges:
//
//   - weight 100: avoid co-locating this project's own replicas of this
//     component (belt-and-suspenders with topologySpreadConstraints; matches the
//     "where replica count allows" wording since it is soft).
//   - weight 50: the cross-project term (crossProjectAntiAffinityTerm) - avoid
//     co-locating with any other project's memory-stack pod. This is the #327 fix.
func componentAffinity(project, component string, cfg Config) *corev1.Affinity {
	tk := topologyKey(cfg)
	return &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
				{
					Weight: 100,
					PodAffinityTerm: corev1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{MatchLabels: selectorLabels(project, component)},
						TopologyKey:   tk,
					},
				},
				{
					Weight:          50,
					PodAffinityTerm: crossProjectAntiAffinityTerm(project, tk),
				},
			},
		},
	}
}

// crossProjectAntiAffinity wraps the cross-project term as a standalone
// *corev1.PodAntiAffinity. cnpg owns the within-cluster (own-instances)
// anti-affinity for the pg Cluster via PodAntiAffinityType, so its
// AdditionalPodAntiAffinity only needs the cross-project (#327) term.
func crossProjectAntiAffinity(project string, cfg Config) *corev1.PodAntiAffinity {
	return &corev1.PodAntiAffinity{
		PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{
			Weight:          50,
			PodAffinityTerm: crossProjectAntiAffinityTerm(project, topologyKey(cfg)),
		}},
	}
}

// topologySpreadConstraints builds the soft spread rule that fans a project's
// own replicas of one component across nodes (the topologyKey domain). MaxSkew 1
// with WhenUnsatisfiable=ScheduleAnyway: on a cluster that cannot satisfy the
// spread (dev/single-node) the pod still schedules rather than staying Pending.
func topologySpreadConstraints(project, component string, cfg Config) []corev1.TopologySpreadConstraint {
	return []corev1.TopologySpreadConstraint{{
		MaxSkew:           1,
		TopologyKey:       topologyKey(cfg),
		WhenUnsatisfiable: corev1.ScheduleAnyway,
		LabelSelector:     &metav1.LabelSelector{MatchLabels: selectorLabels(project, component)},
	}}
}
