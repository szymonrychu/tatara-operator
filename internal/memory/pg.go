package memory

import (
	"fmt"
	"strings"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ProvisionedPGStorage is the already-provisioned PGDATA and WAL sizes observed
// for a live cnpg cluster, gathered from both the Cluster .spec and the backing
// PVC capacities. Any field is "" when that source declares or holds nothing
// (e.g. a cluster with no separate WAL volume, or a PVC not yet bound). The
// shrink guard clamps a render up to the larger of the two sources per volume.
type ProvisionedPGStorage struct {
	PGDataSpecSize    string
	PGDataPVCCapacity string
	WALSpecSize       string
	WALPVCCapacity    string
}

// ClampPGStorageToProvisioned raises the freshly rendered cluster's PGDATA and
// WAL storage sizes up to the largest already-provisioned size so a server-side
// apply never asks cnpg to shrink a volume.
//
// cnpg's admission webhook rejects every storage-size reduction. Before this
// guard, a render whose size had drifted below the live volume - e.g. the 10Gi
// defaultPgStorage rendered against a mem-<proj>-pg PVC that a prior spec had
// grown to 20Gi - made every apply fail. That wedged the whole project memory
// reconcile to Failed and blocked the entire agent fleet from memory (issue
// #248). Clamping upward makes storage monotonic: it only ever grows.
//
// For each volume the floor is the max of the live Cluster .spec size and the
// live PVC capacity. Reading the PVC capacity, not just the spec size, catches a
// volume that was manually expanded beyond the recorded spec size: cnpg's
// webhook validates against the stored spec, but the underlying PVC still cannot
// be shrunk, so a render at/above the spec yet below the live PVC would be
// rejected downstream (issue #258). An empty floor (nothing provisioned) or a
// desired render without a WAL volume leaves that size untouched. It mutates
// desired in place and returns whether either size was raised so the caller can
// log/meter it.
func ClampPGStorageToProvisioned(desired *cnpgv1.Cluster, prov ProvisionedPGStorage) (bool, error) {
	pgDataFloor, err := maxProvisionedSize(prov.PGDataSpecSize, prov.PGDataPVCCapacity)
	if err != nil {
		return false, fmt.Errorf("pgdata storage: %w", err)
	}
	raised, err := clampStorageSize(&desired.Spec.StorageConfiguration.Size, pgDataFloor)
	if err != nil {
		return false, fmt.Errorf("pgdata storage: %w", err)
	}
	// WAL lives on its own volume with the same shrink constraint. Only clamp
	// when the desired render declares a WAL volume: a render without walStorage
	// is a separate (also cnpg-rejected) change this guard does not cover. An
	// empty WAL floor (no provisioned WAL volume) leaves it untouched.
	if desired.Spec.WalStorage != nil {
		walFloor, err := maxProvisionedSize(prov.WALSpecSize, prov.WALPVCCapacity)
		if err != nil {
			return false, fmt.Errorf("wal storage: %w", err)
		}
		walRaised, err := clampStorageSize(&desired.Spec.WalStorage.Size, walFloor)
		if err != nil {
			return false, fmt.Errorf("wal storage: %w", err)
		}
		raised = raised || walRaised
	}
	return raised, nil
}

// maxProvisionedSize returns the larger of two provisioned-size strings as a
// resource-quantity string. An empty input counts as "nothing provisioned" and
// loses to any non-empty value; the result is "" only when both are empty. Sizes
// are compared by magnitude (Kubernetes resource quantities), so "10Gi" and
// "10240Mi" are equal and "20Gi" wins over both.
func maxProvisionedSize(a, b string) (string, error) {
	switch {
	case a == "":
		return b, nil
	case b == "":
		return a, nil
	}
	aQty, err := resource.ParseQuantity(a)
	if err != nil {
		return "", fmt.Errorf("parse %q: %w", a, err)
	}
	bQty, err := resource.ParseQuantity(b)
	if err != nil {
		return "", fmt.Errorf("parse %q: %w", b, err)
	}
	if bQty.Cmp(aQty) > 0 {
		return b, nil
	}
	return a, nil
}

// clampStorageSize sets *desired to existing when the existing provisioned size
// is strictly larger, returning whether it raised the value. An empty existing
// size (nothing provisioned) leaves desired untouched. Both sizes are parsed as
// Kubernetes resource quantities so "10Gi" vs "20Gi" compare by magnitude, not
// lexically.
func clampStorageSize(desired *string, existing string) (bool, error) {
	if existing == "" {
		return false, nil
	}
	existingQty, err := resource.ParseQuantity(existing)
	if err != nil {
		return false, fmt.Errorf("parse existing %q: %w", existing, err)
	}
	desiredQty, err := resource.ParseQuantity(*desired)
	if err != nil {
		return false, fmt.Errorf("parse desired %q: %w", *desired, err)
	}
	if existingQty.Cmp(desiredQty) > 0 {
		*desired = existing
		return true, nil
	}
	return false, nil
}

// pgImagePullSecrets converts the operator ImagePullSecret config into the cnpg
// LocalObjectReference slice expected by ClusterSpec.ImagePullSecrets. cnpg's
// LocalObjectReference is a type alias for github.com/cloudnative-pg/machinery/pkg/api.LocalObjectReference
// (not corev1.LocalObjectReference), so we build the slice here rather than
// reusing the corev1-typed imagePullSecrets helper.
func pgImagePullSecrets(cfg Config) []cnpgv1.LocalObjectReference {
	if cfg.ImagePullSecret == "" {
		return nil
	}
	return []cnpgv1.LocalObjectReference{{Name: cfg.ImagePullSecret}}
}

// PGCluster builds the per-Project cnpg Cluster. cnpg's controller derives the
// mem-<proj>-pg-rw Service and the mem-<proj>-pg-app Secret (key "uri") that
// lightrag and tatara-memory consume. The vector extension is installed via
// postInitApplicationSQL on the tatara_memory database for lightrag's
// PGVectorStorage.
//
// cnpg v1.29.1 field adaptations vs the plan (written for v1.27.x):
//   - Struct field names match exactly: Instances, StorageConfiguration.Size,
//     Bootstrap.InitDB.{Database,Owner,PostInitApplicationSQL}. No changes needed.
//   - GroupVersion export: cnpgv1 exposes SchemeGroupVersion (not GroupVersion);
//     TypeMeta.APIVersion uses cnpgv1.SchemeGroupVersion.String().
func PGCluster(p *tatarav1alpha1.Project, cfg Config) *cnpgv1.Cluster {
	n := NamesFor(p.Name)
	return &cnpgv1.Cluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: cnpgv1.SchemeGroupVersion.String(),
			Kind:       "Cluster",
		},
		ObjectMeta: objectMeta(p, cfg, n.PGCluster),
		Spec: cnpgv1.ClusterSpec{
			Instances:        PgInstances(p),
			ImagePullSecrets: pgImagePullSecrets(cfg),
			// Spread the pg stack the same way as the native workloads (issue #365).
			// cnpg owns the WITHIN-cluster anti-affinity (this project's own pg
			// instances) via PodAntiAffinityType "preferred" on the shared
			// topologyKey - soft, so a single-node dev cluster still schedules.
			// AdditionalPodAntiAffinity carries the CROSS-project term so a
			// different project's pg cluster avoids co-locating too (the #327 fix,
			// which a single node hosting both projects' backends caused).
			Affinity: cnpgv1.AffinityConfiguration{
				TopologyKey:               topologyKey(cfg),
				PodAntiAffinityType:       "preferred",
				AdditionalPodAntiAffinity: crossProjectAntiAffinity(p.Name, cfg),
			},
			StorageConfiguration: cnpgv1.StorageConfiguration{
				Size: pgStorage(p),
			},
			// WAL lives on its own PVC, separate from PGDATA. Without this a WAL
			// burst - or WAL retained for a lagging/re-syncing standby - fills the
			// single shared data volume, Postgres can no longer write WAL, and the
			// write path (/memories:bulk) starts returning 503 while reads on
			// replicas keep working (issue #238). A dedicated WAL volume isolates
			// that growth and is resized independently. cnpg permits adding
			// walStorage to a cluster that never had it; only disabling or shrinking
			// it later is rejected.
			WalStorage: &cnpgv1.StorageConfiguration{
				Size: pgWalStorage(p),
			},
			// Bound how much WAL a replication slot can pin on the primary. A
			// lagging/stuck standby holding a slot open forces the primary to
			// retain WAL until the WAL volume fills; the primary can then no
			// longer write WAL and cnpg fails over, but with every standby's
			// volume equally full the failover thrashes with no writable primary
			// (the ~3.5h mem-tatara-pg outage, issue #240). max_slot_wal_keep_size
			// caps that retention: past the cap postgres invalidates the slot
			// (that standby re-syncs) instead of filling the disk. Derived as
			// half the WAL volume in pgMaxSlotWalKeepSize.
			PostgresConfiguration: cnpgv1.PostgresConfiguration{
				Parameters: map[string]string{
					"max_slot_wal_keep_size": pgMaxSlotWalKeepSize(p),
				},
			},
			// Continuous WAL archiving + retention to an object store (issue #432).
			// nil unless the operator is configured with a complete object-store
			// config, so a cluster with no store renders exactly what it renders
			// today. Adding this stanza to a LIVE cluster needs no rebuild and no
			// restart: cnpg already runs every instance with archive_mode=on and an
			// archive_command pointing at the instance manager (verified in cnpg
			// v1.29.1 pkg/postgres/configuration.go), which is why pg_stat_archiver
			// already counts successful archivals on these clusters today. Only
			// where the instance manager ships the segment changes.
			Backup: pgBackup(cfg),
			// Bootstrap is WRITE-ONCE and fails SILENTLY. No webhook rejects a
			// change to it, but cnpg only consults spec.bootstrap in
			// createPrimaryInstance, which the cluster controller calls solely when
			// Status.Instances == 0 (cnpg v1.29.1
			// internal/controller/cluster_controller.go:1063). So this MUST stay
			// InitDB even once archiving is on: swapping in a recovery stanza would
			// be an accepted-and-ignored no-op on every existing cluster, and on a
			// future FRESH Project it would become active and fail, because that
			// Project's archive is empty and there is nothing to recover from.
			// Restoring from the archive is PGClusterFromBackup, a separate,
			// human-triggered code path with no automatic caller.
			Bootstrap: &cnpgv1.BootstrapConfiguration{
				InitDB: &cnpgv1.BootstrapInitDB{
					Database: "tatara_memory",
					Owner:    "tatara_memory",
					PostInitApplicationSQL: []string{
						"CREATE EXTENSION IF NOT EXISTS vector",
					},
				},
			},
		},
	}
}

// PGBackupStatus reports whether cfg carries a usable cnpg object-store backup
// configuration, plus a non-empty warning when part of it was rejected or
// corrected. Callers log the warning; nothing else consumes it.
//
// It fails CLOSED on a partial config (enabled but missing the bucket or the
// credentials Secret): a half-configured store would make every archive attempt
// fail, and a failing archive_command makes PostgreSQL retain WAL in pg_wal
// forever until the volume fills - the issue #240 disk-full outage, on every
// cluster at once. Rendering no stanza at all is strictly safer than rendering a
// broken one, and the warning keeps the misconfiguration visible rather than
// leaving the cluster silently unbacked.
func PGBackupStatus(cfg Config) (enabled bool, warning string) {
	b := cfg.Backup
	if !b.Enabled {
		return false, ""
	}
	switch {
	case b.Bucket == "":
		return false, "memory backup is enabled but no bucket is configured; WAL archiving stays OFF"
	case b.CredentialsSecretName == "":
		return false, "memory backup is enabled but no credentials secret is configured; WAL archiving stays OFF"
	}
	if b.ScheduleCron != "" && len(strings.Fields(b.ScheduleCron)) != backupScheduleFields {
		return true, fmt.Sprintf(
			"memory backup scheduleCron %q is not a %d-field cnpg schedule (seconds minutes hours dom month dow); using %q instead",
			b.ScheduleCron, backupScheduleFields, defaultBackupSchedule)
	}
	return true, ""
}

// pgBackup builds the cnpg Cluster's spec.backup stanza, or nil when the
// operator carries no usable object-store config (see PGBackupStatus).
//
// Deliberately does NOT set ServerName: cnpg defaults it to the Cluster name,
// which is already unique per Project (mem-tatara-pg / mem-infrastructure-pg /
// mem-mtg-pg), so the per-cluster archive folders are collision-proof by
// construction and there is no second name to keep in sync.
//
// Compression: the Wal and Data enums are NOT the same, and the Data one is the
// narrower of the two. In barman-cloud v0.5.0 (the module cnpg v1.29.1 pulls in)
// pkg/api/config.go declares Wal as bzip2;gzip;lz4;snappy;xz;zstd and Data as
// bzip2;gzip;snappy ONLY - so lz4, xz and zstd are all CRD validation
// rejections on Data. gzip is used for both: it is the one value legal on both
// sides, and it is the codec barman's own default path exercises, so it cannot
// fail at runtime for a missing binary in the operand image. A failing
// archive_command is the exact WAL-pileup outage this feature otherwise guards
// against, which makes "boring and certainly present" the right trade here over
// a better ratio.
func pgBackup(cfg Config) *cnpgv1.BackupConfiguration {
	enabled, _ := PGBackupStatus(cfg)
	if !enabled {
		return nil
	}
	b := cfg.Backup
	return &cnpgv1.BackupConfiguration{
		RetentionPolicy: valueOrDefault(b.RetentionPolicy, defaultBackupRetention),
		BarmanObjectStore: &cnpgv1.BarmanObjectStoreConfiguration{
			DestinationPath: pgBackupDestinationPath(b),
			EndpointURL:     b.EndpointURL,
			BarmanCredentials: cnpgv1.BarmanCredentials{
				AWS: &cnpgv1.S3Credentials{
					AccessKeyIDReference:     backupSecretKey(b.CredentialsSecretName, backupAccessKeyIDKey),
					SecretAccessKeyReference: backupSecretKey(b.CredentialsSecretName, backupSecretAccessKeyKey),
				},
			},
			Wal:  &cnpgv1.WalBackupConfiguration{Compression: "gzip"},
			Data: &cnpgv1.DataBackupConfiguration{Compression: "gzip"},
		},
	}
}

// pgBackupDestinationPath is the s3://<bucket>/<pathPrefix> archive root shared
// by every Project's cluster. cnpg appends its own per-cluster serverName
// folder underneath.
func pgBackupDestinationPath(b BackupConfig) string {
	return "s3://" + b.Bucket + "/" + valueOrDefault(b.PathPrefix, defaultBackupPathPrefix)
}

// backupSecretKey points at one key of the object-store credentials Secret.
func backupSecretKey(secretName, key string) *cnpgv1.SecretKeySelector {
	return &cnpgv1.SecretKeySelector{
		LocalObjectReference: cnpgv1.LocalObjectReference{Name: secretName},
		Key:                  key,
	}
}

// valueOrDefault returns v, or def when v is empty.
func valueOrDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// pgBackupSchedule resolves the base-backup schedule, falling back to the
// default when unset OR when the configured value does not have the six fields
// cnpg requires. cnpg schedules come from robfig/cron and carry a leading
// SECONDS field, so the familiar five-field Kubernetes CronJob string is
// accepted by the API server and then silently means something else entirely
// ("0 2 * * *" parses as second 0, minute 2, hour *, i.e. hourly). Correcting
// it here rather than passing it through keeps a copy-pasted CronJob expression
// from quietly turning a daily backup into an hourly one; PGBackupStatus
// surfaces the correction as a warning.
func pgBackupSchedule(b BackupConfig) string {
	if len(strings.Fields(b.ScheduleCron)) != backupScheduleFields {
		return defaultBackupSchedule
	}
	return b.ScheduleCron
}

// PGScheduledBackup builds the cnpg ScheduledBackup that takes the Project's
// periodic base backup, or nil when the operator carries no usable object-store
// config. Callers delete the object by Names.PGScheduledBackup when this
// returns nil, so turning the feature off does not leave an orphan pointing at
// a cluster with no store.
//
// A ScheduledBackup is REQUIRED, not optional decoration: spec.backup only
// enables continuous WAL archiving plus a retention policy, and retention only
// PRUNES an existing catalogue - it never creates a base backup. Without one,
// the archive accumulates WAL with no base to replay it onto and is
// unrestorable.
//
// BackupOwnerReference is set explicitly because it defaults to "none", which
// leaves every produced Backup object orphaned. "cluster" ties each Backup's
// lifetime to the cnpg Cluster (and so, transitively, to the Project) rather
// than to this ScheduledBackup, so toggling the schedule off does not
// garbage-collect the catalogue of backups already taken.
func PGScheduledBackup(p *tatarav1alpha1.Project, cfg Config) *cnpgv1.ScheduledBackup {
	enabled, _ := PGBackupStatus(cfg)
	if !enabled {
		return nil
	}
	n := NamesFor(p.Name)
	return &cnpgv1.ScheduledBackup{
		TypeMeta: metav1.TypeMeta{
			APIVersion: cnpgv1.SchemeGroupVersion.String(),
			Kind:       "ScheduledBackup",
		},
		ObjectMeta: objectMeta(p, cfg, n.PGScheduledBackup),
		Spec: cnpgv1.ScheduledBackupSpec{
			Schedule:             pgBackupSchedule(cfg.Backup),
			Cluster:              cnpgv1.LocalObjectReference{Name: n.PGCluster},
			BackupOwnerReference: "cluster",
			// Explicit rather than relying on the CRD default, so the object keeps
			// meaning barmanObjectStore if a future cnpg changes that default while
			// migrating to the Barman Cloud Plugin.
			Method: cnpgv1.BackupMethodBarmanObjectStore,
		},
	}
}

// PGClusterFromBackup builds a RECOVERY render of the Project's cnpg Cluster:
// same shape as PGCluster, but bootstrapping from the object-store archive
// instead of running InitDB. It returns nil when the operator carries no usable
// object-store config, or when newServerName equals sourceServerName.
//
// NOTHING CALLS THIS AUTOMATICALLY, BY DESIGN. Recovering means the existing
// PVCs are gone; an operator that decided that on its own would be a data-loss
// machine. This is the builder a human drives during a deliberate restore.
//
// It is also the only place a recovery stanza may appear, because spec.bootstrap
// is write-once and fails SILENTLY: cnpg only reads it in createPrimaryInstance,
// reached only while Status.Instances == 0 (cnpg v1.29.1
// internal/controller/cluster_controller.go:1063). Adding recovery to a LIVE
// cluster is accepted by the API server and then ignored forever - no event, no
// condition, no rejection. A restore therefore has to start from a cluster that
// has no instances yet.
//
// sourceServerName is the serverName the archive was WRITTEN under, i.e. the
// name of the cluster that produced it (cnpg defaults serverName to the Cluster
// name, so for a same-Project restore this is Names.PGCluster). newServerName is
// the serverName the RECOVERED cluster archives under from then on; it must
// differ, or the fresh cluster would write its own timeline into the very folder
// it is restoring from and destroy the recovery source.
func PGClusterFromBackup(p *tatarav1alpha1.Project, cfg Config, sourceServerName, newServerName string) *cnpgv1.Cluster {
	enabled, _ := PGBackupStatus(cfg)
	if !enabled || sourceServerName == "" || newServerName == "" || sourceServerName == newServerName {
		return nil
	}
	c := PGCluster(p, cfg)
	// The recovered cluster must not reuse the source's archive folder.
	c.Spec.Backup.BarmanObjectStore.ServerName = newServerName
	// Replace, never extend: initdb and recovery are mutually exclusive.
	c.Spec.Bootstrap = &cnpgv1.BootstrapConfiguration{
		Recovery: &cnpgv1.BootstrapRecovery{
			Source:   sourceServerName,
			Database: "tatara_memory",
			Owner:    "tatara_memory",
		},
	}
	source := pgBackup(cfg).BarmanObjectStore
	source.ServerName = sourceServerName
	c.Spec.ExternalClusters = []cnpgv1.ExternalCluster{{
		Name:              sourceServerName,
		BarmanObjectStore: source,
	}}
	return c
}
