package controller

import (
	"context"
	"testing"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

// backupReconciler is newMemoryReconciler with a complete object-store backup
// configuration (issue #432).
func backupReconciler() *ProjectReconciler {
	r := newMemoryReconciler()
	r.MemoryConfig.Backup = memory.BackupConfig{
		Enabled:               true,
		EndpointURL:           "https://rook-ceph-rgw-tatara.rook-ceph.svc",
		Bucket:                "tatara-pg-backups-4f2a",
		PathPrefix:            "cnpg",
		CredentialsSecretName: "tatara-pg-backups",
		RetentionPolicy:       "7d",
		ScheduleCron:          "0 0 2 * * *",
	}
	return r
}

// This is the test the unit tests cannot be: envtest installs the REAL vendored
// cnpg CRDs (charts/tatara-operator/crds), so the apply below is validated by
// the same OpenAPI schema the live cluster enforces - the retentionPolicy
// pattern, the six-field schedule, the backupOwnerReference/method enums, and
// above all the compression enums, where the data field accepts a strictly
// narrower set (bzip2;gzip;snappy) than wal (bzip2;gzip;lz4;snappy;xz;zstd) and
// a wal-legal value silently written to data is rejected only at apply time.
func TestApplyMemoryStack_BackupStanzaAndScheduledBackupAreAccepted(t *testing.T) {
	ctx := context.Background()
	r := backupReconciler()
	p := mkMemoryProject(t, "stack-backup-on")

	if _, err := r.ensureNeo4jPassword(ctx, p); err != nil {
		t.Fatalf("password: %v", err)
	}
	if err := r.applyMemoryStack(ctx, p); err != nil {
		t.Fatalf("applyMemoryStack: %v", err)
	}
	names := memory.NamesFor(p.Name)

	var cluster cnpgv1.Cluster
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: names.PGCluster}, &cluster); err != nil {
		t.Fatalf("get cnpg cluster: %v", err)
	}
	if cluster.Spec.Backup == nil || cluster.Spec.Backup.BarmanObjectStore == nil {
		t.Fatal("backup stanza missing from the applied cluster")
	}
	if got := cluster.Spec.Backup.BarmanObjectStore.DestinationPath; got != "s3://tatara-pg-backups-4f2a/cnpg" {
		t.Fatalf("destinationPath = %q", got)
	}
	// Bootstrap is write-once and fails silently, so the live render must keep
	// initdb even with archiving on.
	if cluster.Spec.Bootstrap == nil || cluster.Spec.Bootstrap.InitDB == nil {
		t.Fatal("bootstrap.initdb must survive enabling backups")
	}
	if cluster.Spec.Bootstrap.Recovery != nil {
		t.Fatal("bootstrap.recovery must never appear in the steady-state render")
	}

	var sb cnpgv1.ScheduledBackup
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: names.PGScheduledBackup}, &sb); err != nil {
		t.Fatalf("get scheduled backup: %v", err)
	}
	assertOwnedByProject(t, sb.GetOwnerReferences(), p.Name)
	if sb.Spec.Cluster.Name != names.PGCluster {
		t.Fatalf("scheduled backup points at %q, want %q", sb.Spec.Cluster.Name, names.PGCluster)
	}
	if sb.Spec.Schedule != "0 0 2 * * *" {
		t.Fatalf("schedule = %q", sb.Spec.Schedule)
	}
	if sb.Spec.BackupOwnerReference != "cluster" {
		t.Fatalf("backupOwnerReference = %q, want cluster (the CRD default 'none' orphans every Backup)", sb.Spec.BackupOwnerReference)
	}
}

// Turning the feature off must REMOVE the ScheduledBackup, not merely stop
// re-applying it: a leftover would keep firing against a cluster that no longer
// has an object store, failing every single run.
func TestApplyMemoryStack_ScheduledBackupDeletedWhenBackupTurnedOff(t *testing.T) {
	ctx := context.Background()
	r := backupReconciler()
	p := mkMemoryProject(t, "stack-backup-off")

	if _, err := r.ensureNeo4jPassword(ctx, p); err != nil {
		t.Fatalf("password: %v", err)
	}
	if err := r.applyMemoryStack(ctx, p); err != nil {
		t.Fatalf("applyMemoryStack with backup on: %v", err)
	}
	names := memory.NamesFor(p.Name)
	key := types.NamespacedName{Namespace: testNS, Name: names.PGScheduledBackup}
	var sb cnpgv1.ScheduledBackup
	if err := k8sClient.Get(ctx, key, &sb); err != nil {
		t.Fatalf("scheduled backup not created: %v", err)
	}

	r.MemoryConfig.Backup.Enabled = false
	if err := r.applyMemoryStack(ctx, p); err != nil {
		t.Fatalf("applyMemoryStack with backup off: %v", err)
	}
	if err := k8sClient.Get(ctx, key, &sb); !apierrors.IsNotFound(err) {
		t.Fatalf("scheduled backup must be deleted when the feature is off, get returned %v", err)
	}

	// The apply must also stay clean when there was never anything to delete.
	if err := r.applyMemoryStack(ctx, p); err != nil {
		t.Fatalf("applyMemoryStack with backup already off: %v", err)
	}
}

// A half-configured object store renders no stanza at all and no
// ScheduledBackup: a broken archive command makes PostgreSQL retain every WAL
// segment until the volume fills (issue #240), which is strictly worse than
// having no archiving.
func TestApplyMemoryStack_PartialBackupConfigFailsClosed(t *testing.T) {
	ctx := context.Background()
	r := backupReconciler()
	r.MemoryConfig.Backup.CredentialsSecretName = ""
	p := mkMemoryProject(t, "stack-backup-partial")

	if _, err := r.ensureNeo4jPassword(ctx, p); err != nil {
		t.Fatalf("password: %v", err)
	}
	if err := r.applyMemoryStack(ctx, p); err != nil {
		t.Fatalf("applyMemoryStack: %v", err)
	}
	names := memory.NamesFor(p.Name)

	var cluster cnpgv1.Cluster
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: names.PGCluster}, &cluster); err != nil {
		t.Fatalf("get cnpg cluster: %v", err)
	}
	if cluster.Spec.Backup != nil {
		t.Fatalf("a partial backup config must render no stanza, got %+v", cluster.Spec.Backup)
	}
	var sb cnpgv1.ScheduledBackup
	err := k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: names.PGScheduledBackup}, &sb)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("a partial backup config must create no ScheduledBackup, get returned %v", err)
	}
	if warning := memoryBackupWarning(r.MemoryConfig); warning == "" {
		t.Fatal("a partial backup config must produce a warning so it is not silently unbacked")
	}
}
