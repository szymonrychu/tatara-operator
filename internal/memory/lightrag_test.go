package memory_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
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

func TestLightragDeployment_DataPathProbes(t *testing.T) {
	p := testProject("acme")
	d := memory.LightragDeployment(p, testCfg())

	c := d.Spec.Template.Spec.Containers[0]

	// All three probes must be present. A wedged LightRAG that cannot connect to
	// Postgres is never restarted without the LivenessProbe - the incident showed
	// 8h of uptime with 0 restarts and 100% data-path failure.
	require.NotNil(t, c.StartupProbe, "StartupProbe must exist to gate initial health")
	require.NotNil(t, c.LivenessProbe, "LivenessProbe must exist: a wedged LightRAG is never restarted without it (incident: 8h uptime, 0 restarts, 100% data-path failure)")
	require.NotNil(t, c.ReadinessProbe, "ReadinessProbe must exist to shed traffic from unhealthy replicas")

	// All probes must use HTTPGet and have no TCPSocket. A TCP-accept check cannot
	// distinguish 'serving' from 'accepting and hanging forever', which is exactly
	// what happened in the incident.
	for _, probe := range []*corev1.Probe{c.StartupProbe, c.LivenessProbe, c.ReadinessProbe} {
		require.NotNil(t, probe.HTTPGet, "probe must use HTTPGet handler")
		require.Nil(t, probe.TCPSocket, "probe must not use TCPSocket - a TCP-accept check cannot distinguish serving from hanging forever, which is exactly what happened")
	}

	// Test the probe paths and ports
	probePath := "/documents/status_counts"
	expectedPort := intstr.FromString("http")

	for _, probe := range []*corev1.Probe{c.StartupProbe, c.LivenessProbe, c.ReadinessProbe} {
		require.Equal(t, probePath, probe.HTTPGet.Path, "probe must use correct data-path endpoint")
		require.Equal(t, expectedPort, probe.HTTPGet.Port, "probe must target http port")
	}

	// Explicitly assert each probe's path is NOT /health. The /health endpoint answered
	// 200 for 8h from the main event-loop thread while every worker was futex-blocked
	// with no PostgreSQL connection open, so it cannot detect this failure mode.
	require.NotEqual(t, "/health", c.StartupProbe.HTTPGet.Path, "startup probe path must not be /health - /health answered 200 for 8h from the main event-loop thread while workers were futex-blocked, so it cannot detect data-path failure")
	require.NotEqual(t, "/health", c.LivenessProbe.HTTPGet.Path, "liveness probe path must not be /health - /health answered 200 for 8h from the main event-loop thread while workers were futex-blocked, so it cannot detect data-path failure")
	require.NotEqual(t, "/health", c.ReadinessProbe.HTTPGet.Path, "readiness probe path must not be /health - /health answered 200 for 8h from the main event-loop thread while workers were futex-blocked, so it cannot detect data-path failure")

	// Verify timing configurations for each probe
	type probeSpec struct {
		name            string
		probe           *corev1.Probe
		expectedPeriod  int32
		expectedTimeout int32
		expectedFailure int32
	}

	probes := []probeSpec{
		{
			name:            "StartupProbe",
			probe:           c.StartupProbe,
			expectedPeriod:  10,
			expectedTimeout: 5,
			expectedFailure: 30,
		},
		{
			name:            "LivenessProbe",
			probe:           c.LivenessProbe,
			expectedPeriod:  30,
			expectedTimeout: 10,
			expectedFailure: 10,
		},
		{
			name:            "ReadinessProbe",
			probe:           c.ReadinessProbe,
			expectedPeriod:  10,
			expectedTimeout: 5,
			expectedFailure: 6,
		},
	}

	for _, spec := range probes {
		require.Equal(t, spec.expectedPeriod, spec.probe.PeriodSeconds,
			"%s: PeriodSeconds must match probe configuration", spec.name)
		require.Equal(t, spec.expectedTimeout, spec.probe.TimeoutSeconds,
			"%s: TimeoutSeconds must match probe configuration", spec.name)
		require.Equal(t, spec.expectedFailure, spec.probe.FailureThreshold,
			"%s: FailureThreshold must match probe configuration", spec.name)
	}

	// LivenessProbe must have TerminationGracePeriodSeconds set to 15. A wedged
	// process cannot complete uvicorn's graceful shutdown so SIGTERM would otherwise
	// hang for the full default 30s pod grace period on every liveness kill.
	require.NotNil(t, c.LivenessProbe.TerminationGracePeriodSeconds,
		"LivenessProbe must have TerminationGracePeriodSeconds set")
	require.Equal(t, int64(15), *c.LivenessProbe.TerminationGracePeriodSeconds,
		"LivenessProbe: TerminationGracePeriodSeconds must be 15s (wedged process cannot complete uvicorn graceful shutdown so SIGTERM would otherwise hang 30s on every liveness kill)")

	// InitialDelaySeconds must be 0 on liveness and readiness. The StartupProbe gates both
	// so an initial delay would be dead config.
	require.Equal(t, int32(0), c.LivenessProbe.InitialDelaySeconds,
		"LivenessProbe: InitialDelaySeconds must be 0 (StartupProbe gates startup so initial delay is dead config)")
	require.Equal(t, int32(0), c.ReadinessProbe.InitialDelaySeconds,
		"ReadinessProbe: InitialDelaySeconds must be 0 (StartupProbe gates startup so initial delay is dead config)")

	// Readiness must fire strictly before liveness: shed traffic first, then kill.
	readinessBudget := c.ReadinessProbe.PeriodSeconds * c.ReadinessProbe.FailureThreshold
	livenessBudget := c.LivenessProbe.PeriodSeconds * c.LivenessProbe.FailureThreshold
	require.Less(t, readinessBudget, livenessBudget,
		"readiness budget (%d*%d=%ds) must be strictly less than liveness budget (%d*%d=%ds) - correct ordering is shed traffic first, then kill",
		c.ReadinessProbe.PeriodSeconds, c.ReadinessProbe.FailureThreshold, readinessBudget,
		c.LivenessProbe.PeriodSeconds, c.LivenessProbe.FailureThreshold, livenessBudget)

	// Liveness budget must be exactly 300s. This must exceed the ~120s worst legitimate
	// CNPG failover window but stay far below the 2.5h of unnecessary outage the incident
	// produced after Postgres recovered.
	require.Equal(t, int32(300), livenessBudget,
		"liveness budget must be exactly 300s (%d*%d): must exceed ~120s worst CNPG failover but stay far below 2.5h incident outage",
		c.LivenessProbe.PeriodSeconds, c.LivenessProbe.FailureThreshold)

	// Startup budget must be >= LightRAG's ~180s upstream Postgres boot-retry budget.
	startupBudget := c.StartupProbe.PeriodSeconds * c.StartupProbe.FailureThreshold
	require.GreaterOrEqual(t, startupBudget, int32(180),
		"startup budget (%d*%d=%ds) must be >= LightRAG's ~180s upstream Postgres boot-retry budget",
		c.StartupProbe.PeriodSeconds, c.StartupProbe.FailureThreshold, startupBudget)
}

func TestLightragDeployment_SingleUvicornWorker(t *testing.T) {
	p := testProject("acme")
	d := memory.LightragDeployment(p, testCfg())

	c := d.Spec.Template.Spec.Containers[0]

	// Verify WORKERS is not present in environment variables. The probe design assumes
	// a single uvicorn process so every probe hits the same event loop. With WORKERS>1,
	// a per-worker wedge makes the probe probabilistic and the consecutive-failure
	// counter resets on any request routed to a healthy worker, silently disabling the
	// liveness probe.
	for _, e := range c.Env {
		require.NotEqual(t, "WORKERS", e.Name,
			"WORKERS env var must not be present: probe design assumes single uvicorn process; with WORKERS>1 a per-worker wedge makes probes probabilistic and resets failure counters, silently disabling liveness detection")
	}
}
