package memory_test

import (
	"testing"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/memory"
)

func testCfg() memory.Config {
	return memory.Config{
		Namespace:        "tatara",
		MemoryImage:      "harbor/tatara-memory:0.2.0",
		LightragImage:    "ghcr.io/hkuds/lightrag:v1.4.16",
		Neo4jImage:       "neo4j:2026.04.0",
		OpenAISecretName: "tatara-openai",
		OIDCIssuer:       "https://auth.example/realms/master",
		OIDCAudience:     "tatara-memory",
		ImagePullSecret:  "regcred",
	}
}

func testCfgNoIPS() memory.Config {
	cfg := testCfg()
	cfg.ImagePullSecret = ""
	return cfg
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

	require.NotNil(t, c.Spec.WalStorage)
	require.Equal(t, "8Gi", c.Spec.WalStorage.Size)

	// WAL retention is bounded to half the 8Gi WAL volume so a lagging replica's
	// slot cannot fill the disk and stall failover (issue #240).
	require.Equal(t, "4096MB", c.Spec.PostgresConfiguration.Parameters["max_slot_wal_keep_size"])

	require.NotNil(t, c.Spec.Bootstrap)
	require.NotNil(t, c.Spec.Bootstrap.InitDB)
	require.Equal(t, "tatara_memory", c.Spec.Bootstrap.InitDB.Database)
	require.Equal(t, "tatara_memory", c.Spec.Bootstrap.InitDB.Owner)
	require.Contains(t, c.Spec.Bootstrap.InitDB.PostInitApplicationSQL,
		"CREATE EXTENSION IF NOT EXISTS vector")
}

func pgClusterWithStorage(pgdata, wal string) *cnpgv1.Cluster {
	c := &cnpgv1.Cluster{}
	c.Spec.StorageConfiguration.Size = pgdata
	if wal != "" {
		c.Spec.WalStorage = &cnpgv1.StorageConfiguration{Size: wal}
	}
	return c
}

func TestClampPGStorageToExisting(t *testing.T) {
	t.Run("nil existing leaves desired untouched", func(t *testing.T) {
		desired := pgClusterWithStorage("10Gi", "2Gi")
		raised, err := memory.ClampPGStorageToExisting(desired, nil)
		require.NoError(t, err)
		require.False(t, raised)
		require.Equal(t, "10Gi", desired.Spec.StorageConfiguration.Size)
		require.Equal(t, "2Gi", desired.Spec.WalStorage.Size)
	})

	t.Run("shrink is clamped up to provisioned size", func(t *testing.T) {
		// The issue #248 case: default 10Gi rendered against a provisioned 20Gi.
		desired := pgClusterWithStorage("10Gi", "2Gi")
		existing := pgClusterWithStorage("20Gi", "5Gi")
		raised, err := memory.ClampPGStorageToExisting(desired, existing)
		require.NoError(t, err)
		require.True(t, raised)
		require.Equal(t, "20Gi", desired.Spec.StorageConfiguration.Size)
		require.Equal(t, "5Gi", desired.Spec.WalStorage.Size)
	})

	t.Run("equal sizes are not raised", func(t *testing.T) {
		desired := pgClusterWithStorage("20Gi", "5Gi")
		existing := pgClusterWithStorage("20Gi", "5Gi")
		raised, err := memory.ClampPGStorageToExisting(desired, existing)
		require.NoError(t, err)
		require.False(t, raised)
		require.Equal(t, "20Gi", desired.Spec.StorageConfiguration.Size)
	})

	t.Run("growth request is honored, not clamped down", func(t *testing.T) {
		desired := pgClusterWithStorage("50Gi", "10Gi")
		existing := pgClusterWithStorage("20Gi", "5Gi")
		raised, err := memory.ClampPGStorageToExisting(desired, existing)
		require.NoError(t, err)
		require.False(t, raised)
		require.Equal(t, "50Gi", desired.Spec.StorageConfiguration.Size)
		require.Equal(t, "10Gi", desired.Spec.WalStorage.Size)
	})

	t.Run("only WAL shrinks", func(t *testing.T) {
		desired := pgClusterWithStorage("20Gi", "2Gi")
		existing := pgClusterWithStorage("20Gi", "8Gi")
		raised, err := memory.ClampPGStorageToExisting(desired, existing)
		require.NoError(t, err)
		require.True(t, raised)
		require.Equal(t, "20Gi", desired.Spec.StorageConfiguration.Size)
		require.Equal(t, "8Gi", desired.Spec.WalStorage.Size)
	})

	t.Run("different-unit sizes compare by magnitude", func(t *testing.T) {
		// 10240Mi == 10Gi; provisioned 20Gi must still win.
		desired := pgClusterWithStorage("10240Mi", "2Gi")
		existing := pgClusterWithStorage("20Gi", "2Gi")
		raised, err := memory.ClampPGStorageToExisting(desired, existing)
		require.NoError(t, err)
		require.True(t, raised)
		require.Equal(t, "20Gi", desired.Spec.StorageConfiguration.Size)
	})

	t.Run("existing without WAL leaves desired WAL untouched", func(t *testing.T) {
		desired := pgClusterWithStorage("10Gi", "2Gi")
		existing := pgClusterWithStorage("20Gi", "")
		raised, err := memory.ClampPGStorageToExisting(desired, existing)
		require.NoError(t, err)
		require.True(t, raised)
		require.Equal(t, "20Gi", desired.Spec.StorageConfiguration.Size)
		require.Equal(t, "2Gi", desired.Spec.WalStorage.Size)
	})

	t.Run("unparseable existing size is an error", func(t *testing.T) {
		desired := pgClusterWithStorage("10Gi", "2Gi")
		existing := pgClusterWithStorage("garbage", "2Gi")
		_, err := memory.ClampPGStorageToExisting(desired, existing)
		require.Error(t, err)
	})
}

func TestPGCluster_ImagePullSecrets(t *testing.T) {
	p := testProject("acme")

	// Set: imagePullSecrets present.
	c := memory.PGCluster(p, testCfg())
	require.Len(t, c.Spec.ImagePullSecrets, 1)
	require.Equal(t, "regcred", c.Spec.ImagePullSecrets[0].Name)

	// Unset: imagePullSecrets absent.
	cNoIPS := memory.PGCluster(p, testCfgNoIPS())
	require.Empty(t, cNoIPS.Spec.ImagePullSecrets)
}

func TestPGCluster_SpecOverrides(t *testing.T) {
	p := testProject("acme")
	p.Spec.Memory = &tatarav1alpha1.MemorySpec{PgInstances: 3, PgStorage: "50Gi", PgWalStorage: "10Gi"}
	c := memory.PGCluster(p, testCfg())
	require.Equal(t, 3, c.Spec.Instances)
	require.Equal(t, "50Gi", c.Spec.StorageConfiguration.Size)
	require.NotNil(t, c.Spec.WalStorage)
	require.Equal(t, "10Gi", c.Spec.WalStorage.Size)
	// max_slot_wal_keep_size tracks the WAL volume: half of 10Gi.
	require.Equal(t, "5120MB", c.Spec.PostgresConfiguration.Parameters["max_slot_wal_keep_size"])
}
