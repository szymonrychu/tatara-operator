package memory

import (
	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

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
