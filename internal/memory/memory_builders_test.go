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

	// envFrom references the ConfigMap and Secret.
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
	require.True(t, secRef, "secretRef mem-acme missing from envFrom")

	// PG_DSN from the cnpg app secret key uri.
	var dsn corev1.EnvVar
	var found bool
	for _, e := range c.Env {
		if e.Name == "PG_DSN" {
			dsn, found = e, true
		}
	}
	require.True(t, found)
	require.Equal(t, "mem-acme-pg-app", dsn.ValueFrom.SecretKeyRef.Name)
	require.Equal(t, "uri", dsn.ValueFrom.SecretKeyRef.Key)
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

func TestMemorySecret(t *testing.T) {
	p := testProject("acme")
	s := memory.MemorySecret(p, testCfg())
	require.Equal(t, "mem-acme", s.Name)
	require.Equal(t, corev1.SecretTypeOpaque, s.Type)
	require.Len(t, s.OwnerReferences, 1)
}

func TestMemoryService(t *testing.T) {
	p := testProject("acme")
	svc := memory.MemoryService(p, testCfg())
	require.Equal(t, "mem-acme", svc.Name)
	require.Equal(t, int32(8080), svc.Spec.Ports[0].Port)
	require.Equal(t, "mem-acme", svc.Spec.Selector["app.kubernetes.io/instance"])
	require.Len(t, svc.OwnerReferences, 1)
}
