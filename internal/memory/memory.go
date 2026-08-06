// Package memory holds pure builder functions that produce the per-Project
// memory stack (cnpg postgres, neo4j, lightrag, tatara-memory) as native
// Kubernetes objects. Every object is named from Names, carries the pin-set
// labels, and is owner-referenced to the Project for cascade delete. No
// function performs any client call; callers (the ProjectReconciler, N2)
// server-side-apply the returned objects.
package memory

import (
	"fmt"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Defaults for an empty or partial spec.memory. Applied in the builders, not
// as kubebuilder defaults, so an absent spec.memory still provisions.
const (
	defaultPgInstances  = 1
	defaultPgStorage    = "10Gi"
	defaultPgWalStorage = "8Gi"
	defaultNeo4jStorage = "10Gi"
	defaultAPIReplicas  = 1
)

// Defaults for the cnpg object-store backup stanza (issue #432). Applied in the
// builders so an empty chart value still renders a valid object; the chart sets
// the same literals explicitly so a deployer can see them without reading Go.
const (
	// defaultBackupPathPrefix is the folder inside the bucket every cluster's
	// archive lives under. Each cluster then gets its own subfolder from cnpg's
	// default serverName (the Cluster name), which is already unique per Project.
	defaultBackupPathPrefix = "cnpg"
	// defaultBackupRetention matches cnpg's ^[1-9][0-9]*[dwm]$ retention grammar.
	defaultBackupRetention = "7d"
	// defaultBackupSchedule is 02:00 daily. cnpg schedules are SIX fields
	// (seconds minutes hours dom month dow, robfig/cron), NOT the five-field
	// Kubernetes CronJob form - a five-field value is accepted and silently means
	// something else. See pgBackupSchedule.
	defaultBackupSchedule = "0 0 2 * * *"
	// backupScheduleFields is the field count a cnpg ScheduledBackup schedule
	// must have.
	backupScheduleFields = 6
)

// Secret keys the object-store credentials Secret must carry. These are the
// AWS/S3 (and rook-ceph ObjectBucketClaim) convention, not a configurable knob:
// every S3-compatible store and every OBC-provisioned Secret uses exactly these
// names, so making them configurable would only add a way to get them wrong.
const (
	backupAccessKeyIDKey     = "AWS_ACCESS_KEY_ID"
	backupSecretAccessKeyKey = "AWS_SECRET_ACCESS_KEY"
)

// BackupConfig is the operator-level object-store configuration for cnpg WAL
// archiving and scheduled base backups (issue #432). Every field defaults to
// empty/off so the chart stays cluster-agnostic (rule 14): a cluster with no
// object store renders exactly the same cnpg Cluster it renders today.
//
// The bucket itself is created and owned by tatara-helmfile, NEVER by the
// operator. The rook-ceph-bucket StorageClass has reclaimPolicy: Delete and the
// whole memory stack carries the Project ownerRef, so an operator-created
// ObjectBucketClaim would be garbage-collected together with its Project and
// rook would then delete the bucket and every backup inside it.
type BackupConfig struct {
	// Enabled gates the whole feature. False (default) means no backup stanza,
	// no ScheduledBackup and no archiving alerts are rendered at all.
	Enabled bool
	// EndpointURL is the S3-compatible endpoint (e.g. the in-cluster rook-ceph
	// RGW service URL). Empty lets barman use the AWS default endpoint.
	EndpointURL string
	// Bucket is the object-store bucket the archive is written to. Required.
	Bucket string
	// PathPrefix is the folder inside Bucket. Defaults to defaultBackupPathPrefix.
	PathPrefix string
	// CredentialsSecretName is the Secret carrying AWS_ACCESS_KEY_ID and
	// AWS_SECRET_ACCESS_KEY. Required.
	CredentialsSecretName string
	// RetentionPolicy is cnpg's XXu retention string (days/weeks/months).
	// Defaults to defaultBackupRetention.
	RetentionPolicy string
	// ScheduleCron is the SIX-field robfig/cron schedule of the base backup.
	// Defaults to defaultBackupSchedule.
	ScheduleCron string
}

// Config is the operator-level (non-per-Project) input the builders need.
// The manager maps config.Config into this in N2/N3.
type Config struct {
	Namespace        string
	MemoryImage      string
	LightragImage    string
	Neo4jImage       string
	OpenAISecretName string
	OIDCIssuer       string
	OIDCAudience     string
	ImagePullSecret  string
	IngressHost      string
	IngressClassName string
	// IngressRewriteTarget is the value of the nginx-specific
	// nginx.ingress.kubernetes.io/rewrite-target annotation. Empty (default)
	// means the annotation is NOT emitted, so the chart stays cluster-agnostic
	// (rule 14): a non-nginx ingress controller is not handed nginx annotations.
	IngressRewriteTarget string
	MemoryPathPrefix     string
	// MonitorEnabled gates emission of the per-Project memory-stack
	// ServiceMonitor + PrometheusRule. Default true; set false on a cluster
	// without the prometheus-operator CRDs so the memory reconcile does not fail
	// applying an unknown kind.
	MonitorEnabled bool
	// MonitorLabels are extra labels stamped onto the memory-stack ServiceMonitor
	// and PrometheusRule so the cluster Prometheus serviceMonitorSelector /
	// ruleSelector discovers them (e.g. release: prometheus). Empty by default so
	// the chart stays cluster-agnostic (rule 14); the deploying helmfile sets the
	// label the cluster actually matches on.
	MonitorLabels map[string]string
	// TopologyKey is the node-topology domain the memory-stack spreading rules
	// (pod anti-affinity + topologySpreadConstraints) fan pods across. Empty
	// (default) resolves to "kubernetes.io/hostname" in topologyKey(), so a
	// zero-value Config still spreads per node. Sourced from the operator env var
	// MEMORY_TOPOLOGY_KEY (mirrors the AGENT_SCHEDULING precedent); kept
	// cluster-agnostic (rule 14) so the deploying helmfile picks a rack/zone label
	// when the cluster has one.
	TopologyKey string
	// ProvisioningTimeout bounds how long a stack may stay phase Provisioning
	// before reconcileMemory reports it Degraded (issue #355). Zero disables
	// the bound. Set from config.Config.MemoryProvisioningTimeout in wire.go.
	ProvisioningTimeout time.Duration
	// Backup is the cnpg WAL-archiving / scheduled-base-backup object-store
	// configuration (issue #432). Disabled by default (rule 14).
	Backup BackupConfig
}

// Names holds every object name in the mem-<proj>-* family for one Project.
type Names struct {
	PGCluster         string // cnpg Cluster
	PGScheduledBackup string // cnpg ScheduledBackup (base backup cadence)
	PGService         string // cnpg-managed read-write Service
	PGAppSecret       string // cnpg-managed app Secret (key "uri")
	Neo4j             string // StatefulSet + Service
	Neo4jSecret       string // generated password Secret
	Lightrag          string // Deployment + Service
	LightragPVC       string // lightrag data PVC
	Memory            string // tatara-memory Deployment + Service + ConfigMap + Secret
}

// NamesFor returns the name family for a project.
// Note: pin set wrote this as Names(project) but Names is also the struct type;
// renamed to NamesFor to avoid a Go compile error (func and type cannot share a name).
func NamesFor(project string) Names {
	p := "mem-" + project
	return Names{
		PGCluster:         p + "-pg",
		PGScheduledBackup: p + "-pg-backup",
		PGService:         p + "-pg-rw",
		PGAppSecret:       p + "-pg-app",
		Neo4j:             p + "-neo4j",
		Neo4jSecret:       p + "-neo4j",
		Lightrag:          p + "-lightrag",
		LightragPVC:       p + "-lightrag-data",
		Memory:            p,
	}
}

// Endpoint is the canonical in-cluster URL of a Project's tatara-memory
// service. This is the value the reconciler writes to status.memory.endpoint
// and every other component reads.
func Endpoint(project, namespace string) string {
	return fmt.Sprintf("http://mem-%s.%s.svc:8080", project, namespace)
}

// labels returns the four pin-set labels carried by every object.
func labels(project string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":     "tatara-memory",
		"app.kubernetes.io/instance": "mem-" + project,
		"tatara.dev/project":         project,
	}
}

// ownerRef returns the single controller OwnerReference to the Project.
func ownerRef(p *tatarav1alpha1.Project) metav1.OwnerReference {
	t := true
	return metav1.OwnerReference{
		APIVersion:         tatarav1alpha1.GroupVersion.String(),
		Kind:               "Project",
		Name:               p.Name,
		UID:                p.UID,
		Controller:         &t,
		BlockOwnerDeletion: &t,
	}
}

// objectMeta builds the shared ObjectMeta for an object named name owned by p.
func objectMeta(p *tatarav1alpha1.Project, cfg Config, name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:            name,
		Namespace:       cfg.Namespace,
		Labels:          labels(p.Name),
		OwnerReferences: []metav1.OwnerReference{ownerRef(p)},
	}
}

// monitorObjectMeta is objectMeta plus the cluster-selector labels stamped on
// the monitoring objects (ServiceMonitor / PrometheusRule) so the cluster
// Prometheus serviceMonitorSelector / ruleSelector match them. The extra labels
// are merged on top of the shared pin-set labels. objectMeta returns a fresh
// label map per call, so mutating it here is safe.
func monitorObjectMeta(p *tatarav1alpha1.Project, cfg Config, name string) metav1.ObjectMeta {
	m := objectMeta(p, cfg, name)
	for k, v := range cfg.MonitorLabels {
		m.Labels[k] = v
	}
	return m
}

// imagePullSecrets returns a one-element slice when cfg.ImagePullSecret is set,
// or nil when empty (omitted from the pod spec).
func imagePullSecrets(cfg Config) []corev1.LocalObjectReference {
	if cfg.ImagePullSecret == "" {
		return nil
	}
	return []corev1.LocalObjectReference{{Name: cfg.ImagePullSecret}}
}

// PgInstances resolves the postgres instance count from spec, defaulting to 1.
// Exported so the controller's readiness gate and the builder derive the count
// from one source of truth (hard rule 2: KISS/DRY).
func PgInstances(p *tatarav1alpha1.Project) int {
	if p.Spec.Memory != nil && p.Spec.Memory.PgInstances > 0 {
		return p.Spec.Memory.PgInstances
	}
	return defaultPgInstances
}

// APIReplicas resolves the tatara-memory API replica count from spec,
// defaulting to 1. Exported for the same reason as PgInstances: the builder and
// any caller reasoning about the stack's HA posture read one source of truth.
func APIReplicas(p *tatarav1alpha1.Project) int32 {
	if p.Spec.Memory != nil && p.Spec.Memory.APIReplicas > 0 {
		return int32(p.Spec.Memory.APIReplicas)
	}
	return defaultAPIReplicas
}

// pgStorage resolves the postgres storage size from spec, defaulting.
func pgStorage(p *tatarav1alpha1.Project) string {
	if p.Spec.Memory != nil && p.Spec.Memory.PgStorage != "" {
		return p.Spec.Memory.PgStorage
	}
	return defaultPgStorage
}

// pgWalStorage resolves the dedicated postgres WAL volume size from spec,
// defaulting.
func pgWalStorage(p *tatarav1alpha1.Project) string {
	if p.Spec.Memory != nil && p.Spec.Memory.PgWalStorage != "" {
		return p.Spec.Memory.PgWalStorage
	}
	return defaultPgWalStorage
}

// pgMaxSlotWalKeepSize is the value for postgres' max_slot_wal_keep_size: the
// cap on how much WAL a replication slot may force the primary to retain for a
// lagging standby. It is derived as half the dedicated WAL volume.
//
// The ~3.5h mem-tatara-pg outage (issue #240) was a stuck/lagging replica
// holding a replication slot open, forcing the primary to retain WAL until the
// volume filled. A full WAL volume makes the primary unable to write WAL, cnpg
// marks it unhealthy and fails over - but every candidate standby's volume was
// equally full, so the failover thrashed for hours with no writable primary and
// the memory API stayed NotReady. Bounding slot retention converts that
// cluster-fatal disk-full into a bounded, self-healing degradation: once a
// slot's retained WAL exceeds this cap postgres invalidates the slot (that one
// standby must re-sync) rather than filling the disk. Half the volume leaves
// headroom for active/checkpoint WAL and any archive backlog. Falls back to
// half the 8Gi default when the configured size cannot be parsed.
func pgMaxSlotWalKeepSize(p *tatarav1alpha1.Project) string {
	q, err := resource.ParseQuantity(pgWalStorage(p))
	if err != nil {
		q = resource.MustParse(defaultPgWalStorage)
	}
	mb := q.Value() / (2 * 1024 * 1024)
	if mb < 1 {
		mb = 1
	}
	return fmt.Sprintf("%dMB", mb)
}

// neo4jStorage resolves the neo4j storage size from spec, defaulting.
func neo4jStorage(p *tatarav1alpha1.Project) string {
	if p.Spec.Memory != nil && p.Spec.Memory.Neo4jStorage != "" {
		return p.Spec.Memory.Neo4jStorage
	}
	return defaultNeo4jStorage
}
