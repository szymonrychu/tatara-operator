// Package memory holds pure builder functions that produce the per-Project
// memory stack (cnpg postgres, neo4j, lightrag, tatara-memory) as native
// Kubernetes objects. Every object is named from Names, carries the pin-set
// labels, and is owner-referenced to the Project for cascade delete. No
// function performs any client call; callers (the ProjectReconciler, N2)
// server-side-apply the returned objects.
package memory

import (
	"fmt"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Defaults for an empty or partial spec.memory. Applied in the builders, not
// as kubebuilder defaults, so an absent spec.memory still provisions.
const (
	defaultPgInstances  = 1
	defaultPgStorage    = "10Gi"
	defaultPgWalStorage = "2Gi"
	defaultNeo4jStorage = "10Gi"
)

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
	ChatPathPrefix       string
	ChatImage            string
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
}

// Names holds every object name in the mem-<proj>-* family for one Project.
type Names struct {
	PGCluster   string // cnpg Cluster
	PGService   string // cnpg-managed read-write Service
	PGAppSecret string // cnpg-managed app Secret (key "uri")
	Neo4j       string // StatefulSet + Service
	Neo4jSecret string // generated password Secret
	Lightrag    string // Deployment + Service
	LightragPVC string // lightrag data PVC
	Memory      string // tatara-memory Deployment + Service + ConfigMap + Secret
}

// NamesFor returns the name family for a project.
// Note: pin set wrote this as Names(project) but Names is also the struct type;
// renamed to NamesFor to avoid a Go compile error (func and type cannot share a name).
func NamesFor(project string) Names {
	p := "mem-" + project
	return Names{
		PGCluster:   p + "-pg",
		PGService:   p + "-pg-rw",
		PGAppSecret: p + "-pg-app",
		Neo4j:       p + "-neo4j",
		Neo4jSecret: p + "-neo4j",
		Lightrag:    p + "-lightrag",
		LightragPVC: p + "-lightrag-data",
		Memory:      p,
	}
}

// Endpoint is the canonical in-cluster URL of a Project's tatara-memory
// service. This is the value the reconciler writes to status.memory.endpoint
// and every other component reads.
func Endpoint(project, namespace string) string {
	return fmt.Sprintf("http://mem-%s.%s.svc:8080", project, namespace)
}

// ChatServiceName is the shared tatara-chat Service. Unlike memory (mem-<project>,
// one stack per Project), chat is deployed once per cluster as a single
// tatara-chat release, so every Project's agents, ingress backend, and the
// tool-surface probe all address this one Service.
const ChatServiceName = "tatara-chat"

// ChatEndpoint is the canonical in-cluster URL of the shared chat Service (the
// value agent pods receive as TATARA_CHAT_URL and the tool-surface probe dials).
// Sibling of Endpoint, but not project-scoped: chat is a single shared service.
func ChatEndpoint(namespace string) string {
	return fmt.Sprintf("http://%s.%s.svc:8080", ChatServiceName, namespace)
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

// neo4jStorage resolves the neo4j storage size from spec, defaulting.
func neo4jStorage(p *tatarav1alpha1.Project) string {
	if p.Spec.Memory != nil && p.Spec.Memory.Neo4jStorage != "" {
		return p.Spec.Memory.Neo4jStorage
	}
	return defaultNeo4jStorage
}
