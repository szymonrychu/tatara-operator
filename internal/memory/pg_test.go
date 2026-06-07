package memory_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/memory"
)

func testCfg() memory.Config {
	return memory.Config{
		Namespace:        "tatara",
		MemoryImage:      "harbor/tatara-memory:0.2.0",
		LightragImage:    "ghcr.io/hkuds/lightrag:v1.4.16",
		Neo4jImage:       "neo4j:5-community",
		OpenAISecretName: "tatara-openai",
		OIDCIssuer:       "https://auth.example/realms/master",
		OIDCAudience:     "tatara-memory",
	}
}

func TestPGCluster_DefaultsAndShape(t *testing.T) {
	p := testProject("acme")
	c := memory.PGCluster(p, testCfg())

	require.Equal(t, "mem-acme-pg", c.Name)
	require.Equal(t, "tatara", c.Namespace)
	require.Equal(t, "tatara-memory", c.Labels["app.kubernetes.io/name"])
	require.Equal(t, "acme", c.Labels["tatara.dev/project"])

	require.Len(t, c.OwnerReferences, 1)
	require.Equal(t, "Project", c.OwnerReferences[0].Kind)
	require.Equal(t, "acme", c.OwnerReferences[0].Name)
	require.NotNil(t, c.OwnerReferences[0].Controller)
	require.True(t, *c.OwnerReferences[0].Controller)

	require.Equal(t, 1, c.Spec.Instances)
	require.Equal(t, "10Gi", c.Spec.StorageConfiguration.Size)

	require.NotNil(t, c.Spec.Bootstrap)
	require.NotNil(t, c.Spec.Bootstrap.InitDB)
	require.Equal(t, "tatara_memory", c.Spec.Bootstrap.InitDB.Database)
	require.Equal(t, "tatara_memory", c.Spec.Bootstrap.InitDB.Owner)
	require.Contains(t, c.Spec.Bootstrap.InitDB.PostInitApplicationSQL,
		"CREATE EXTENSION IF NOT EXISTS vector")
}

func TestPGCluster_SpecOverrides(t *testing.T) {
	p := testProject("acme")
	p.Spec.Memory = &tatarav1alpha1.MemorySpec{PgInstances: 3, PgStorage: "50Gi"}
	c := memory.PGCluster(p, testCfg())
	require.Equal(t, 3, c.Spec.Instances)
	require.Equal(t, "50Gi", c.Spec.StorageConfiguration.Size)
}
