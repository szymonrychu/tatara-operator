package agent_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// TestParseScheduling_Empty asserts that an empty or whitespace-only document
// parses to a zero Scheduling (no constraints), keeping the chart cluster-agnostic.
func TestParseScheduling_Empty(t *testing.T) {
	for _, in := range []string{"", "   ", "{}"} {
		sc, err := agent.ParseScheduling(in)
		require.NoError(t, err, "input %q", in)
		require.Nil(t, sc.NodeSelector)
		require.Nil(t, sc.Tolerations)
		require.Nil(t, sc.Affinity)
	}
}

// TestParseScheduling_Full asserts that a populated JSON document parses into
// the typed NodeSelector/Tolerations/Affinity the agent Pod builder consumes.
func TestParseScheduling_Full(t *testing.T) {
	doc := `{
		"nodeSelector": {"kubernetes.io/os": "linux"},
		"tolerations": [
			{"key": "node-role.kubernetes.io/control-plane", "operator": "Exists", "effect": "NoSchedule"}
		],
		"affinity": {
			"nodeAffinity": {
				"requiredDuringSchedulingIgnoredDuringExecution": {
					"nodeSelectorTerms": [
						{"matchExpressions": [{"key": "disktype", "operator": "In", "values": ["ssd"]}]}
					]
				}
			}
		}
	}`
	sc, err := agent.ParseScheduling(doc)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"kubernetes.io/os": "linux"}, sc.NodeSelector)
	require.Len(t, sc.Tolerations, 1)
	require.Equal(t, "node-role.kubernetes.io/control-plane", sc.Tolerations[0].Key)
	require.Equal(t, corev1.TolerationOpExists, sc.Tolerations[0].Operator)
	require.Equal(t, corev1.TaintEffectNoSchedule, sc.Tolerations[0].Effect)
	require.NotNil(t, sc.Affinity)
	require.NotNil(t, sc.Affinity.NodeAffinity)
}

// TestParseScheduling_Malformed asserts that an invalid JSON document returns an
// error (caught at startup) rather than silently dropping scheduling.
func TestParseScheduling_Malformed(t *testing.T) {
	_, err := agent.ParseScheduling(`{"nodeSelector": [not json`)
	require.Error(t, err)
}
