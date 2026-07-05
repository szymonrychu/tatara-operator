package memory

import (
	"fmt"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClampPGStorageToExisting raises the freshly rendered cluster's PGDATA and WAL
// storage sizes to match an already-provisioned cluster whenever the render is
// smaller, so a server-side apply never asks cnpg to shrink a volume.
//
// cnpg's admission webhook rejects every storage-size reduction. Before this
// guard, a render whose size had drifted below the live volume - e.g. the 10Gi
// defaultPgStorage rendered against a mem-<proj>-pg PVC that a prior spec had
// grown to 20Gi - made every apply fail. That wedged the whole project memory
// reconcile to Failed and blocked the entire agent fleet from memory (issue
// #248). Clamping upward makes storage monotonic: it only ever grows.
//
// existing is the live cluster read from the API server (nil when it does not
// yet exist, in which case desired is left untouched). It mutates desired in
// place and returns whether either size was raised so the caller can log/meter it.
func ClampPGStorageToExisting(desired, existing *cnpgv1.Cluster) (bool, error) {
	if existing == nil {
		return false, nil
	}
	raised, err := clampStorageSize(&desired.Spec.StorageConfiguration.Size, existing.Spec.StorageConfiguration.Size)
	if err != nil {
		return false, fmt.Errorf("pgdata storage: %w", err)
	}
	// WAL lives on its own volume with the same shrink constraint. Only compare
	// when both sides declare a WAL volume: a desired render without walStorage
	// is a separate (also cnpg-rejected) change this guard does not cover, and an
	// existing cluster without walStorage has nothing to clamp against.
	if desired.Spec.WalStorage != nil && existing.Spec.WalStorage != nil {
		walRaised, err := clampStorageSize(&desired.Spec.WalStorage.Size, existing.Spec.WalStorage.Size)
		if err != nil {
			return false, fmt.Errorf("wal storage: %w", err)
		}
		raised = raised || walRaised
	}
	return raised, nil
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
