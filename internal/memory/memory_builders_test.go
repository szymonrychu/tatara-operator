package memory_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	corev1 "k8s.io/api/core/v1"
)

func TestMemoryDeployment(t *testing.T) {
	p := testProject("acme")
	d := memory.MemoryDeployment(p, testCfg())

	require.Equal(t, "mem-acme", d.Name)
	require.Equal(t, "tatara", d.Namespace)
	require.Len(t, d.OwnerReferences, 1)
	require.True(t, *d.OwnerReferences[0].Controller)

	c := d.Spec.Template.Spec.Containers[0]
	require.Equal(t, "harbor/tatara-memory:0.2.0", c.Image)
	require.Equal(t, int32(8080), c.Ports[0].ContainerPort)

	// envFrom references ONLY the ConfigMap - no empty speculative Secret (KISS rule).
	var cmRef, secRef bool
	for _, ef := range c.EnvFrom {
		if ef.ConfigMapRef != nil && ef.ConfigMapRef.Name == "mem-acme" {
			cmRef = true
		}
		if ef.SecretRef != nil && ef.SecretRef.Name == "mem-acme" {
			secRef = true
		}
	}
	require.True(t, cmRef, "configMapRef mem-acme missing from envFrom")
	require.False(t, secRef, "empty speculative SecretRef mem-acme must not appear in envFrom")

	// Env vars: PG_DSN and OPENAI_API_KEY must both be present.
	envByName := make(map[string]corev1.EnvVar)
	for _, e := range c.Env {
		envByName[e.Name] = e
	}

	dsn, found := envByName["PG_DSN"]
	require.True(t, found, "PG_DSN env missing")
	require.Equal(t, "mem-acme-pg-app", dsn.ValueFrom.SecretKeyRef.Name)
	require.Equal(t, "uri", dsn.ValueFrom.SecretKeyRef.Key)

	// OPENAI_API_KEY must be wired to the shared OpenAI secret so the
	// tatara-memory community labeler can use LLM labels (not degrade silently).
	oai, found := envByName["OPENAI_API_KEY"]
	require.True(t, found, "OPENAI_API_KEY env missing from MemoryDeployment")
	require.Equal(t, "tatara-openai", oai.ValueFrom.SecretKeyRef.Name)
	require.Equal(t, "LLM_BINDING_API_KEY", oai.ValueFrom.SecretKeyRef.Key)
}

func TestMemoryDeployment_ImagePullSecrets(t *testing.T) {
	p := testProject("acme")

	// Set: imagePullSecrets present.
	d := memory.MemoryDeployment(p, testCfg())
	require.Len(t, d.Spec.Template.Spec.ImagePullSecrets, 1)
	require.Equal(t, "regcred", d.Spec.Template.Spec.ImagePullSecrets[0].Name)

	// Unset: imagePullSecrets absent.
	dNoIPS := memory.MemoryDeployment(p, testCfgNoIPS())
	require.Empty(t, dNoIPS.Spec.Template.Spec.ImagePullSecrets)
}

func TestMemoryDeployment_StartupProbe(t *testing.T) {
	p := testProject("acme")
	c := memory.MemoryDeployment(p, testCfg()).Spec.Template.Spec.Containers[0]

	// A startupProbe must gate liveness so the multi-stage blocking startup
	// (up to 60s waitForDB + OIDC discovery + migrations) cannot be
	// liveness-killed mid-boot into an unrecoverable CrashLoopBackOff.
	require.NotNil(t, c.StartupProbe, "startupProbe missing - liveness can kill a slow boot")
	require.Equal(t, "/healthz", c.StartupProbe.HTTPGet.Path)

	// Budget must exceed the app's worst-case 60s DB wait.
	budget := c.StartupProbe.PeriodSeconds * c.StartupProbe.FailureThreshold
	require.GreaterOrEqual(t, budget, int32(90), "startup budget must clear the 60s waitForDB plus OIDC/migrations")

	require.NotNil(t, c.LivenessProbe)
	require.Equal(t, "/healthz", c.LivenessProbe.HTTPGet.Path)
}

// TestMemoryDeployment_ProbeTimeoutsAreExplicit pins issue #528's second defect.
// None of the three probes set timeoutSeconds, so all three inherited the
// Kubernetes default of 1 second - including the readinessProbe, whose /readyz
// handler pings Postgres AND does a full HTTP round-trip to LightRAG, returning
// the first error with no retry, grace or warm-up.
//
// During the 2026-08-01 node reboot that cost ~95 seconds of entirely
// self-inflicted NotReady: mem-infrastructure logged nine consecutive
// `readyz failed / lightrag: context canceled` lines, one per 10s period, each
// with duration_s pinned at 1.000008544 / 0.999578 / 0.999913 - a 1.000s wall,
// and `context canceled` (the kubelet hung up) rather than `context deadline
// exceeded` (a server-side deadline). LightRAG was merely cold-starting from its
// own restart and answered fine moments later.
//
// The same 1s budget also flaps the pod NotReady on any dependency blip over a
// second, which for a probe that makes two network calls is routine.
func TestMemoryDeployment_ProbeTimeoutsAreExplicit(t *testing.T) {
	c := memory.MemoryDeployment(testProject("acme"), testCfg()).Spec.Template.Spec.Containers[0]

	for _, tc := range []struct {
		name  string
		probe *corev1.Probe
	}{
		{"readinessProbe", c.ReadinessProbe},
		{"livenessProbe", c.LivenessProbe},
		{"startupProbe", c.StartupProbe},
	} {
		require.NotNilf(t, tc.probe, "%s missing", tc.name)
		require.Greaterf(t, tc.probe.TimeoutSeconds, int32(1),
			"%s leaves timeoutSeconds unset, so the kubelet cancels it at 1s (#528)", tc.name)
	}

	// Readiness is the one that actually costs an outage: it fans out to Postgres
	// and LightRAG, so its budget must clear two network round-trips with margin.
	require.GreaterOrEqual(t, c.ReadinessProbe.TimeoutSeconds, int32(5),
		"/readyz pings Postgres AND LightRAG; 1-2s cannot cover both (#528)")
}

func TestMemoryConfigMap(t *testing.T) {
	p := testProject("acme")
	cm := memory.MemoryConfigMap(p, testCfg())
	require.Equal(t, "mem-acme", cm.Name)
	require.Equal(t, ":8080", cm.Data["HTTP_ADDR"])
	require.Equal(t, "http://mem-acme-lightrag:9621", cm.Data["LIGHTRAG_BASE_URL"])
	require.Equal(t, "https://auth.example/realms/master", cm.Data["OIDC_ISSUER"])
	require.Equal(t, "tatara-memory", cm.Data["OIDC_AUDIENCE"])
	require.Equal(t, "info", cm.Data["LOG_LEVEL"])
	require.Contains(t, cm.Data, "WORKER_POOL_SIZE")
	require.Len(t, cm.OwnerReferences, 1)
}

func TestMemoryService(t *testing.T) {
	p := testProject("acme")
	svc := memory.MemoryService(p, testCfg())
	require.Equal(t, "mem-acme", svc.Name)
	require.Equal(t, int32(8080), svc.Spec.Ports[0].Port)
	require.Equal(t, "mem-acme", svc.Spec.Selector["app.kubernetes.io/instance"])
	// The component metadata label lets the ServiceMonitor target only this
	// Service (neo4j/lightrag share the pin-set labels and a "http" port).
	require.Equal(t, "memory", svc.Labels["app.kubernetes.io/component"])
	require.Len(t, svc.OwnerReferences, 1)
}
