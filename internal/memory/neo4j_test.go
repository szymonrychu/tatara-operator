package memory_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	corev1 "k8s.io/api/core/v1"
)

func envByName(env []corev1.EnvVar, name string) (corev1.EnvVar, bool) {
	for _, e := range env {
		if e.Name == name {
			return e, true
		}
	}
	return corev1.EnvVar{}, false
}

func TestNeo4jStatefulSet(t *testing.T) {
	p := testProject("acme")
	ss := memory.Neo4jStatefulSet(p, testCfg())

	require.Equal(t, "mem-acme-neo4j", ss.Name)
	require.Equal(t, "tatara", ss.Namespace)
	require.Equal(t, "mem-acme-neo4j", ss.Spec.ServiceName)
	require.EqualValues(t, 1, *ss.Spec.Replicas)
	require.Len(t, ss.OwnerReferences, 1)
	require.True(t, *ss.OwnerReferences[0].Controller)

	c := ss.Spec.Template.Spec.Containers[0]
	require.Equal(t, "neo4j:5-community", c.Image)

	// NEO4J_AUTH from the generated secret.
	auth, ok := envByName(c.Env, "NEO4J_AUTH")
	require.True(t, ok)
	require.NotNil(t, auth.ValueFrom)
	require.NotNil(t, auth.ValueFrom.SecretKeyRef)
	require.Equal(t, "mem-acme-neo4j", auth.ValueFrom.SecretKeyRef.Name)
	require.Equal(t, "NEO4J_AUTH", auth.ValueFrom.SecretKeyRef.Key)

	// Ports.
	ports := map[string]int32{}
	for _, pt := range c.Ports {
		ports[pt.Name] = pt.ContainerPort
	}
	require.Equal(t, int32(7687), ports["bolt"])
	require.Equal(t, int32(7474), ports["http"])

	// Data volume claim sized from default.
	require.Len(t, ss.Spec.VolumeClaimTemplates, 1)
	vct := ss.Spec.VolumeClaimTemplates[0]
	require.Equal(t, "10Gi", vct.Spec.Resources.Requests.Storage().String())

	// /data mount present.
	var mounted bool
	for _, m := range c.VolumeMounts {
		if m.MountPath == "/data" {
			mounted = true
		}
	}
	require.True(t, mounted)
}

func TestNeo4jStatefulSet_StorageOverride(t *testing.T) {
	p := testProject("acme")
	p.Spec.Memory = &tatarav1alpha1.MemorySpec{Neo4jStorage: "25Gi"}
	ss := memory.Neo4jStatefulSet(p, testCfg())
	require.Equal(t, "25Gi",
		ss.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests.Storage().String())
}

func TestNeo4jService(t *testing.T) {
	p := testProject("acme")
	svc := memory.Neo4jService(p, testCfg())
	require.Equal(t, "mem-acme-neo4j", svc.Name)
	require.Len(t, svc.OwnerReferences, 1)
	ports := map[string]int32{}
	for _, pt := range svc.Spec.Ports {
		ports[pt.Name] = pt.Port
	}
	require.Equal(t, int32(7687), ports["bolt"])
	require.Equal(t, int32(7474), ports["http"])
	require.Equal(t, "mem-acme", svc.Spec.Selector["app.kubernetes.io/instance"])
}
