package memory_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/szymonrychu/tatara-operator/internal/memory"
)

// testBackupCfg is testCfg plus a complete object-store backup configuration,
// shaped like the tatara-helmfile-owned rook-ceph bucket the feature targets.
func testBackupCfg() memory.Config {
	cfg := testCfg()
	cfg.Backup = memory.BackupConfig{
		Enabled:               true,
		EndpointURL:           "https://rook-ceph-rgw-tatara.rook-ceph.svc",
		Bucket:                "tatara-pg-backups-4f2a",
		PathPrefix:            "cnpg",
		CredentialsSecretName: "tatara-pg-backups",
		RetentionPolicy:       "7d",
		ScheduleCron:          "0 0 2 * * *",
	}
	return cfg
}

// The whole feature must be invisible when it is off. Not "no crash" - the
// rendered cnpg Cluster has to be byte-identical to what a build without the
// feature produces, or every existing cluster gets a spec diff (and cnpg a
// reconcile) for a feature nobody enabled.
func TestPGCluster_BackupDisabled_RenderIsUnchanged(t *testing.T) {
	p := testProject("acme")

	// A config whose backup block is fully populated but NOT enabled must render
	// exactly the same as one that has never heard of backups.
	off := testBackupCfg()
	off.Backup.Enabled = false

	withoutFeature, err := json.Marshal(memory.PGCluster(p, testCfg()))
	require.NoError(t, err)
	withFeatureOff, err := json.Marshal(memory.PGCluster(p, off))
	require.NoError(t, err)
	require.JSONEq(t, string(withoutFeature), string(withFeatureOff))

	require.Nil(t, memory.PGCluster(p, testCfg()).Spec.Backup)
	require.Nil(t, memory.PGScheduledBackup(p, testCfg()))

	enabled, warning := memory.PGBackupStatus(testCfg())
	require.False(t, enabled)
	require.Empty(t, warning, "a cleanly disabled feature must not warn")
}

func TestPGCluster_BackupStanza(t *testing.T) {
	c := memory.PGCluster(testProject("acme"), testBackupCfg())

	require.NotNil(t, c.Spec.Backup)
	require.Equal(t, "7d", c.Spec.Backup.RetentionPolicy)

	store := c.Spec.Backup.BarmanObjectStore
	require.NotNil(t, store)
	require.Equal(t, "s3://tatara-pg-backups-4f2a/cnpg", store.DestinationPath)
	require.Equal(t, "https://rook-ceph-rgw-tatara.rook-ceph.svc", store.EndpointURL)

	// ServerName is deliberately unset: cnpg defaults it to the Cluster name,
	// which is already unique per Project, so the per-cluster archive folders
	// cannot collide and there is no second name to keep in sync.
	require.Empty(t, store.ServerName)

	require.NotNil(t, store.AWS)
	require.Equal(t, "tatara-pg-backups", store.AWS.AccessKeyIDReference.Name)
	require.Equal(t, "AWS_ACCESS_KEY_ID", store.AWS.AccessKeyIDReference.Key)
	require.Equal(t, "tatara-pg-backups", store.AWS.SecretAccessKeyReference.Name)
	require.Equal(t, "AWS_SECRET_ACCESS_KEY", store.AWS.SecretAccessKeyReference.Key)
}

// The Wal and Data compression enums are NOT the same set, and Data is the
// narrower one: barman-cloud v0.5.0 pkg/api/config.go marks Wal
// bzip2;gzip;lz4;snappy;xz;zstd and Data bzip2;gzip;snappy. A value legal on Wal
// but not on Data (lz4, xz, zstd) is a CRD validation rejection at apply time,
// which no unit test would otherwise catch.
func TestPGCluster_BackupCompression_RespectsTheNarrowerDataEnum(t *testing.T) {
	store := memory.PGCluster(testProject("acme"), testBackupCfg()).Spec.Backup.BarmanObjectStore

	walEnum := []string{"bzip2", "gzip", "lz4", "snappy", "xz", "zstd"}
	dataEnum := []string{"bzip2", "gzip", "snappy"}

	require.NotNil(t, store.Wal)
	require.Contains(t, walEnum, string(store.Wal.Compression))
	require.NotNil(t, store.Data)
	require.Contains(t, dataEnum, string(store.Data.Compression),
		"data compression must come from the NARROWER data enum, not the wal one")
}

// A half-configured object store must render NO stanza rather than a broken
// one: a failing archive command makes PostgreSQL retain WAL until the volume
// fills (issue #240). The warning is what keeps that visible.
func TestPGBackupStatus_PartialConfigFailsClosed(t *testing.T) {
	p := testProject("acme")
	cases := []struct {
		name     string
		mutate   func(*memory.BackupConfig)
		wantWarn string
	}{
		{"no bucket", func(b *memory.BackupConfig) { b.Bucket = "" }, "bucket"},
		{"no credentials secret", func(b *memory.BackupConfig) { b.CredentialsSecretName = "" }, "credentials secret"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testBackupCfg()
			tc.mutate(&cfg.Backup)

			enabled, warning := memory.PGBackupStatus(cfg)
			require.False(t, enabled)
			require.Contains(t, warning, tc.wantWarn)

			require.Nil(t, memory.PGCluster(p, cfg).Spec.Backup)
			require.Nil(t, memory.PGScheduledBackup(p, cfg))
		})
	}
}

func TestPGScheduledBackup(t *testing.T) {
	p := testProject("acme")
	sb := memory.PGScheduledBackup(p, testBackupCfg())

	require.NotNil(t, sb)
	require.Equal(t, "mem-acme-pg-backup", sb.Name)
	require.Equal(t, "tatara", sb.Namespace)
	require.Equal(t, "ScheduledBackup", sb.Kind)
	require.Equal(t, "postgresql.cnpg.io/v1", sb.APIVersion)

	require.Equal(t, "acme", sb.Labels["tatara.dev/project"])
	require.Len(t, sb.OwnerReferences, 1)
	require.Equal(t, "acme", sb.OwnerReferences[0].Name)
	require.True(t, *sb.OwnerReferences[0].Controller)

	require.Equal(t, "mem-acme-pg", sb.Spec.Cluster.Name)
	require.Equal(t, "0 0 2 * * *", sb.Spec.Schedule)
	// Defaults to "none", which orphans every Backup object it produces.
	require.Equal(t, "cluster", sb.Spec.BackupOwnerReference)
	require.Equal(t, "barmanObjectStore", string(sb.Spec.Method))
}

// cnpg schedules are robfig/cron, which carries a leading SECONDS field: six
// fields, not the five-field Kubernetes CronJob form. A five-field string is
// accepted by the API server and silently means something else ("0 2 * * *"
// becomes second 0, minute 2, hour *, i.e. hourly), so it is rejected and
// replaced rather than passed through.
func TestPGScheduledBackup_ScheduleIsSixFields(t *testing.T) {
	p := testProject("acme")

	require.Len(t, strings.Fields(memory.PGScheduledBackup(p, testBackupCfg()).Spec.Schedule), 6)

	unset := testBackupCfg()
	unset.Backup.ScheduleCron = ""
	require.Equal(t, "0 0 2 * * *", memory.PGScheduledBackup(p, unset).Spec.Schedule)

	fiveField := testBackupCfg()
	fiveField.Backup.ScheduleCron = "0 2 * * *"
	require.Equal(t, "0 0 2 * * *", memory.PGScheduledBackup(p, fiveField).Spec.Schedule,
		"a five-field CronJob schedule must not be passed through to cnpg")

	enabled, warning := memory.PGBackupStatus(fiveField)
	require.True(t, enabled, "a bad schedule must not disable WAL archiving too")
	require.Contains(t, warning, "6-field")
}

// spec.bootstrap is WRITE-ONCE and fails SILENTLY: cnpg reads it only in
// createPrimaryInstance, reached only while Status.Instances == 0. So the
// steady-state render must keep initdb even with archiving on - a recovery
// stanza here would be an accepted-and-ignored no-op on every live cluster, and
// would then become active (and fail, against an empty archive) on the next
// fresh Project.
func TestPGCluster_BootstrapStaysInitDBWhenBackupEnabled(t *testing.T) {
	c := memory.PGCluster(testProject("acme"), testBackupCfg())

	require.NotNil(t, c.Spec.Bootstrap)
	require.NotNil(t, c.Spec.Bootstrap.InitDB)
	require.Nil(t, c.Spec.Bootstrap.Recovery)
	require.Empty(t, c.Spec.ExternalClusters)
}

func TestPGClusterFromBackup(t *testing.T) {
	p := testProject("acme")
	c := memory.PGClusterFromBackup(p, testBackupCfg(), "mem-acme-pg", "mem-acme-pg-restored")

	require.NotNil(t, c)
	require.Equal(t, "mem-acme-pg", c.Name)

	require.NotNil(t, c.Spec.Bootstrap)
	require.Nil(t, c.Spec.Bootstrap.InitDB)
	require.NotNil(t, c.Spec.Bootstrap.Recovery)
	require.Equal(t, "mem-acme-pg", c.Spec.Bootstrap.Recovery.Source)
	require.Equal(t, "tatara_memory", c.Spec.Bootstrap.Recovery.Database)
	require.Equal(t, "tatara_memory", c.Spec.Bootstrap.Recovery.Owner)

	// The external cluster names the server that WROTE the archive.
	require.Len(t, c.Spec.ExternalClusters, 1)
	ext := c.Spec.ExternalClusters[0]
	require.Equal(t, "mem-acme-pg", ext.Name)
	require.NotNil(t, ext.BarmanObjectStore)
	require.Equal(t, "mem-acme-pg", ext.BarmanObjectStore.ServerName)
	require.Equal(t, "s3://tatara-pg-backups-4f2a/cnpg", ext.BarmanObjectStore.DestinationPath)

	// ... while the recovered cluster archives somewhere else, so it cannot
	// overwrite the very archive it is restoring from.
	require.Equal(t, "mem-acme-pg-restored", c.Spec.Backup.BarmanObjectStore.ServerName)
	require.NotEqual(t, ext.BarmanObjectStore.ServerName, c.Spec.Backup.BarmanObjectStore.ServerName)

	// Everything else is the steady-state render.
	require.Equal(t, 1, c.Spec.Instances)
	require.NotNil(t, c.Spec.WalStorage)
}

func TestPGClusterFromBackup_RefusesUnusableInputs(t *testing.T) {
	p := testProject("acme")

	require.Nil(t, memory.PGClusterFromBackup(p, testCfg(), "mem-acme-pg", "mem-acme-pg-restored"),
		"no object store configured")
	require.Nil(t, memory.PGClusterFromBackup(p, testBackupCfg(), "", "mem-acme-pg-restored"))
	require.Nil(t, memory.PGClusterFromBackup(p, testBackupCfg(), "mem-acme-pg", ""))
	require.Nil(t, memory.PGClusterFromBackup(p, testBackupCfg(), "mem-acme-pg", "mem-acme-pg"),
		"recovering into the source's own archive folder would destroy it")
}

func TestPGCluster_BackupDefaults(t *testing.T) {
	cfg := testBackupCfg()
	cfg.Backup.PathPrefix = ""
	cfg.Backup.RetentionPolicy = ""

	c := memory.PGCluster(testProject("acme"), cfg)
	require.Equal(t, "s3://tatara-pg-backups-4f2a/cnpg", c.Spec.Backup.BarmanObjectStore.DestinationPath)
	require.Equal(t, "7d", c.Spec.Backup.RetentionPolicy)
}
