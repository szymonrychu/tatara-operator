package memory_test

import (
	"strings"
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
	// LightRAG's processing pipeline reads the raw OPENAI_API_KEY env var; without
	// it, entity extraction fails KeyError 'OPENAI_API_KEY' and docs never process.
	require.Equal(t, "tatara-openai", env["OPENAI_API_KEY"].ValueFrom.SecretKeyRef.Name)
	require.Equal(t, "LLM_BINDING_API_KEY", env["OPENAI_API_KEY"].ValueFrom.SecretKeyRef.Key)
	require.Equal(t, "mem-acme-pg-app", env["POSTGRES_PASSWORD"].ValueFrom.SecretKeyRef.Name)
	require.Equal(t, "password", env["POSTGRES_PASSWORD"].ValueFrom.SecretKeyRef.Key)
	require.Equal(t, "mem-acme-neo4j", env["NEO4J_PASSWORD"].ValueFrom.SecretKeyRef.Name)
	require.Equal(t, "password", env["NEO4J_PASSWORD"].ValueFrom.SecretKeyRef.Key)
}

func TestLightragDeployment_ImagePullSecrets(t *testing.T) {
	p := testProject("acme")

	// Set: imagePullSecrets present.
	d := memory.LightragDeployment(p, testCfg())
	require.Len(t, d.Spec.Template.Spec.ImagePullSecrets, 1)
	require.Equal(t, "regcred", d.Spec.Template.Spec.ImagePullSecrets[0].Name)

	// Unset: imagePullSecrets absent.
	dNoIPS := memory.LightragDeployment(p, testCfgNoIPS())
	require.Empty(t, dNoIPS.Spec.Template.Spec.ImagePullSecrets)
}

func TestLightragDeployment_WaitsForNeo4j(t *testing.T) {
	p := testProject("acme")
	d := memory.LightragDeployment(p, testCfg())

	// Upstream LightRAG exits fatally (no retry) if Neo4j is unreachable at
	// boot, so a readiness/liveness probe on the main container cannot help -
	// the process is already dead. Gate it with an initContainer instead, one
	// per pod so it blocks the main container from starting at all.
	require.Len(t, d.Spec.Template.Spec.InitContainers, 1, "lightrag needs exactly one dependency-gate initContainer (neo4j only - postgres already has its own upstream retry)")
	init := d.Spec.Template.Spec.InitContainers[0]

	require.Equal(t, "wait-for-neo4j", init.Name)
	// Reuse the lightrag image itself (it ships python3) rather than pulling in
	// a new tool/image for the wait loop.
	require.Equal(t, "ghcr.io/hkuds/lightrag:v1.4.16", init.Image)

	env := map[string]corev1.EnvVar{}
	for _, e := range init.Env {
		env[e.Name] = e
	}
	require.Equal(t, "mem-acme-neo4j", env["NEO4J_HOST"].Value)

	script := strings.Join(init.Args, "\n")
	require.Contains(t, script, "7687", "must target neo4j's bolt port")
	require.Contains(t, script, "NEO4J_HOST")
	// Bounded failure mode: must give up and exit non-zero rather than
	// blocking forever silently, so a permanently-down Neo4j surfaces as a
	// diagnosable Init:CrashLoopBackOff instead of a pod that never starts.
	require.Contains(t, script, "exit 1")
	require.Contains(t, script, "max_attempts")

	require.Equal(t, corev1.TerminationMessageFallbackToLogsOnError, init.TerminationMessagePolicy)
}

func TestLightragDeployment_InitContainerDoesNotGatePostgres(t *testing.T) {
	p := testProject("acme")
	d := memory.LightragDeployment(p, testCfg())

	// Postgres is deliberately NOT gated: upstream LightRAG already retries
	// postgres ~10 times on boot (evidenced in the incident that motivated
	// this fix), so an initContainer wait on postgres would duplicate
	// existing, working behaviour rather than fixing anything.
	for _, init := range d.Spec.Template.Spec.InitContainers {
		for _, e := range init.Env {
			require.NotEqual(t, "POSTGRES_HOST", e.Name, "postgres must not be gated - upstream's own retry already covers it")
		}
	}
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
