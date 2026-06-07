package memory_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	corev1 "k8s.io/api/core/v1"
)

func TestLightragDeployment(t *testing.T) {
	p := testProject("acme")
	d := memory.LightragDeployment(p, testCfg())

	require.Equal(t, "mem-acme-lightrag", d.Name)
	require.Equal(t, "tatara", d.Namespace)
	require.Len(t, d.OwnerReferences, 1)
	require.True(t, *d.OwnerReferences[0].Controller)
	require.Equal(t, appsv1RecreateName(), string(d.Spec.Strategy.Type))

	c := d.Spec.Template.Spec.Containers[0]
	require.Equal(t, "ghcr.io/hkuds/lightrag:v1.4.16", c.Image)
	require.Equal(t, int32(9621), c.Ports[0].ContainerPort)

	env := map[string]corev1.EnvVar{}
	for _, e := range c.Env {
		env[e.Name] = e
	}

	// Non-secret wiring.
	require.Equal(t, "mem-acme-pg-rw", env["POSTGRES_HOST"].Value)
	require.Equal(t, "5432", env["POSTGRES_PORT"].Value)
	require.Equal(t, "tatara_memory", env["POSTGRES_DATABASE"].Value)
	require.Equal(t, "tatara_memory", env["POSTGRES_USER"].Value)
	require.Equal(t, "bolt://mem-acme-neo4j:7687", env["NEO4J_URI"].Value)
	require.Equal(t, "neo4j", env["NEO4J_USERNAME"].Value)
	require.Equal(t, "PGVectorStorage", env["LIGHTRAG_VECTOR_STORAGE"].Value)
	require.Equal(t, "Neo4JStorage", env["LIGHTRAG_GRAPH_STORAGE"].Value)

	// Secret wiring.
	require.Equal(t, "tatara-openai", env["LLM_BINDING_API_KEY"].ValueFrom.SecretKeyRef.Name)
	require.Equal(t, "LLM_BINDING_API_KEY", env["LLM_BINDING_API_KEY"].ValueFrom.SecretKeyRef.Key)
	require.Equal(t, "mem-acme-pg-app", env["POSTGRES_PASSWORD"].ValueFrom.SecretKeyRef.Name)
	require.Equal(t, "password", env["POSTGRES_PASSWORD"].ValueFrom.SecretKeyRef.Key)
	require.Equal(t, "mem-acme-neo4j", env["NEO4J_PASSWORD"].ValueFrom.SecretKeyRef.Name)
	require.Equal(t, "password", env["NEO4J_PASSWORD"].ValueFrom.SecretKeyRef.Key)
}

func TestLightragService(t *testing.T) {
	p := testProject("acme")
	svc := memory.LightragService(p, testCfg())
	require.Equal(t, "mem-acme-lightrag", svc.Name)
	require.Equal(t, int32(9621), svc.Spec.Ports[0].Port)
	require.Equal(t, "mem-acme", svc.Spec.Selector["app.kubernetes.io/instance"])
	require.Len(t, svc.OwnerReferences, 1)
}

func TestLightragPVC(t *testing.T) {
	p := testProject("acme")
	pvc := memory.LightragPVC(p, testCfg())
	require.Equal(t, "mem-acme-lightrag-data", pvc.Name)
	require.Equal(t, "10Gi", pvc.Spec.Resources.Requests.Storage().String())
	require.Len(t, pvc.OwnerReferences, 1)
}

func appsv1RecreateName() string { return "Recreate" }
