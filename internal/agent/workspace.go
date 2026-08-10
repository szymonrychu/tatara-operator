package agent

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// ComponentAgentCache is the component label on the per-PROJECT cache PVC. It
// is deliberately NOT ComponentAgent: WrapperPodSelector matches managed-by AND
// component=agent, and the reaper's PVC pass reaps by that selector on the
// per-TASK claims. The cache PVC has no Task and must never be a candidate.
const ComponentAgentCache = "agent-cache"

// LabelProjectKey stamps the owning Project on the cache PVC. Same key the
// memory stack uses (internal/memory.ProjectLabel); duplicated rather than
// imported because internal/agent must not depend on internal/memory.
const LabelProjectKey = "tatara.dev/project"

// Volume names for the two operator-owned workspace volumes. They are prefixed
// so a Project's extraVolumes cannot collide with them by accident, and they are
// PREPENDED to the extras in BuildPod so a collision that happens anyway cannot
// shadow the operator's mount.
const (
	workspaceVolumeName = "tatara-workspace"
	cacheVolumeName     = "tatara-cache"
)

// cachePVCPrefix leads the per-PROJECT build-cache PVC name.
const cachePVCPrefix = "tatara-cache-"

// WorkspacePVCName is the per-TASK workspace PVC. It rides on PodName, which is
// already stable per Task and capped at 63 chars for the DNS-1123 LABEL a
// Service name must be; a PVC name is a DNS-1123 SUBDOMAIN (253), so the suffix
// is free. Stability across stages and across respawns therefore comes for
// free too: the same Task always resolves to the same claim, which is the whole
// point - a review stage inherits the implement stage's workspace, and a
// respawned pod re-attaches to the work its predecessor had already done.
func WorkspacePVCName(task *tatarav1alpha1.Task) string {
	return PodName(task) + "-ws"
}

// CachePVCName is the per-PROJECT build-cache PVC.
//
// Cache scoping is per-project DELIBERATELY, and it is where the entire win of
// this feature lives. GOCACHE is keyed by action-ID hash and GOMODCACHE by
// module@version verified against go.sum, so sharing across Tasks is safe by
// construction. A per-Task cache would be COLD on every new Task - which is
// most Tasks - and would buy nothing.
func CachePVCName(projectName string) string {
	return cachePVCPrefix + projectName
}

// WorkspacePVCEnabled reports whether the persistent workspace is in play for
// this Project: the operator-wide switch AND the Project's own escape hatch.
// The operator-wide switch defaults OFF so the CRD and the controller can land
// before any PVC is created anywhere.
func WorkspacePVCEnabled(project *tatarav1alpha1.Project, cfg PodConfig) bool {
	return cfg.WorkspacePVCEnabled && project.WorkspaceEnabled()
}

// CachePVCEnabled reports whether the shared build-cache volume is in play. It
// is subordinate to the workspace switch: the operator-wide flag gates the whole
// feature, and a project with no workspace has nowhere useful to put a cache.
func CachePVCEnabled(project *tatarav1alpha1.Project, cfg PodConfig) bool {
	return WorkspacePVCEnabled(project, cfg) && project.WorkspaceCacheEnabled()
}

// workspaceMounts is the per-TASK volume's mount map. /workspace is the reason
// the feature exists; ~/.cache/pre-commit is the ONE cache that is deliberately
// NOT shared across the project. pre-commit's hook environments have no
// atomicity guarantee - an env killed mid-install is left half-populated with no
// marker, and pre-commit happily reuses it - so keeping it per-Task bounds the
// blast radius of a mid-install OOMKill to a single Task.
var workspaceMounts = []corev1.VolumeMount{
	{Name: workspaceVolumeName, MountPath: "/workspace", SubPath: "workspace"},
	{Name: workspaceVolumeName, MountPath: "/home/agent/.cache/pre-commit", SubPath: "pre-commit"},
}

// cacheMounts is the per-PROJECT volume's mount map.
//
// THE MISE TRAP, and it is why only `downloads` appears here. The wrapper image
// bakes python and pre-commit with `mise use -g` into
// /home/agent/.local/share/mise/installs (121 MB measured) and puts
// .../mise/shims on PATH. Mounting an empty volume at ~/.local/share/mise, or at
// .../installs, HIDES both: the pod comes up with no python3 and no pre-commit,
// and the wrapper then runs `pre-commit install` in every clone. Mount ONLY the
// sibling `downloads` directory. pod_workspace_test.go asserts nothing ever
// mounts at installs or shims.
var cacheMounts = []corev1.VolumeMount{
	{Name: cacheVolumeName, MountPath: "/home/agent/.cache/go-build", SubPath: "go-build"},
	{Name: cacheVolumeName, MountPath: "/home/agent/go/pkg/mod", SubPath: "go-mod"},
	{Name: cacheVolumeName, MountPath: "/home/agent/.cache/pip", SubPath: "pip"},
	{Name: cacheVolumeName, MountPath: "/home/agent/.npm", SubPath: "npm"},
	{Name: cacheVolumeName, MountPath: "/home/agent/.local/share/mise/downloads", SubPath: "mise-downloads"},
}

// workspaceVolumesAndMounts returns the operator-owned volumes and mounts for
// this Project/Task pair, or nils when the feature is off.
func workspaceVolumesAndMounts(project *tatarav1alpha1.Project, task *tatarav1alpha1.Task,
	cfg PodConfig) ([]corev1.Volume, []corev1.VolumeMount) {

	if !WorkspacePVCEnabled(project, cfg) {
		return nil, nil
	}
	vols := []corev1.Volume{pvcVolume(workspaceVolumeName, WorkspacePVCName(task))}
	mounts := append([]corev1.VolumeMount(nil), workspaceMounts...)
	if CachePVCEnabled(project, cfg) {
		vols = append(vols, pvcVolume(cacheVolumeName, CachePVCName(project.Name)))
		mounts = append(mounts, cacheMounts...)
	}
	return vols, mounts
}

func pvcVolume(name, claim string) corev1.Volume {
	return corev1.Volume{
		Name: name,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim},
		},
	}
}

// BuildWorkspacePVC returns the per-TASK workspace PVC, owner-referenced to the
// Task via the SAME ownerRef the Pod and Service use, so it is cascade-deleted
// with the Task and needs no separate finalizer.
//
// Labels are podLabels MINUS the agent-kind label: that label is per-STAGE and
// the whole point of this volume is that it OUTLIVES a stage. Everything else
// is kept so the reaper's existing wrapper selector finds it.
func BuildWorkspacePVC(project *tatarav1alpha1.Project, task *tatarav1alpha1.Task,
	cfg PodConfig) (*corev1.PersistentVolumeClaim, error) {

	q, err := parseWorkspaceQuantity("workspace.size", tatarav1alpha1.WorkspaceSize(project))
	if err != nil {
		return nil, err
	}
	return buildPVC(WorkspacePVCName(task), cfg.Namespace, workspacePVCLabels(task),
		[]metav1.OwnerReference{ownerRef(task)}, tatarav1alpha1.WorkspaceStorageClass(project), q), nil
}

// BuildCachePVC returns the per-PROJECT build-cache PVC. Its owner is the
// PROJECT, not any Task: it is shared by every Task in the project and must
// survive all of them. It is deleted with the Project, or when cacheEnabled
// goes false - never on a Task reaching a terminal outcome.
func BuildCachePVC(project *tatarav1alpha1.Project, cfg PodConfig,
	owner metav1.OwnerReference) (*corev1.PersistentVolumeClaim, error) {

	q, err := parseWorkspaceQuantity("workspace.cacheSize", tatarav1alpha1.WorkspaceCacheSize(project))
	if err != nil {
		return nil, err
	}
	labels := map[string]string{
		"app.kubernetes.io/name": "tatara-operator",
		LabelComponent:           ComponentAgentCache,
		LabelManagedBy:           ManagedByValue,
		LabelProjectKey:          project.Name,
	}
	return buildPVC(CachePVCName(project.Name), cfg.Namespace, labels,
		[]metav1.OwnerReference{owner}, tatarav1alpha1.WorkspaceStorageClass(project), q), nil
}

// workspacePVCLabels is podLabels minus LabelAgentKind. See BuildWorkspacePVC.
func workspacePVCLabels(task *tatarav1alpha1.Task) map[string]string {
	l := podLabels(task)
	delete(l, LabelAgentKind)
	return l
}

func buildPVC(name, namespace string, labels map[string]string, owners []metav1.OwnerReference,
	storageClass string, size resource.Quantity) *corev1.PersistentVolumeClaim {

	sc := storageClass
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       namespace,
			Labels:          labels,
			OwnerReferences: owners,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			// ReadWriteMany, always. A respawned pod can land on a different node
			// while the old pod's volume is still detaching, and RWO would stall
			// it in Multi-Attach for as long as that takes.
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
			StorageClassName: &sc,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: size},
			},
		},
	}
}

// ValidateWorkspaceQuantities returns an error when either workspace size on a
// Project is not a valid, positive Kubernetes quantity. Call it before building
// the PVCs, next to ValidatePodSecretRefs.
//
// These strings come from a PER-PROJECT CRD field, so resource.MustParse is not
// an option: a single typo in one Project's spec would panic the reconcile hot
// path for every Project. Same reasoning as buildResourceRequirements, with the
// extra rule that a PVC request must be positive - resource.ParseQuantity
// accepts "0" and "-1Gi" quite happily and the API server would reject the PVC
// far downstream of anything readable.
func ValidateWorkspaceQuantities(project *tatarav1alpha1.Project) error {
	for _, p := range []struct{ field, raw string }{
		{"workspace.size", tatarav1alpha1.WorkspaceSize(project)},
		{"workspace.cacheSize", tatarav1alpha1.WorkspaceCacheSize(project)},
	} {
		if _, err := parseWorkspaceQuantity(p.field, p.raw); err != nil {
			return err
		}
	}
	return nil
}

func parseWorkspaceQuantity(field, raw string) (resource.Quantity, error) {
	q, err := resource.ParseQuantity(raw)
	if err != nil {
		return resource.Quantity{}, fmt.Errorf("agent: %s=%q is not a valid resource quantity: %w", field, raw, err)
	}
	if q.Sign() <= 0 {
		return resource.Quantity{}, fmt.Errorf("agent: %s=%q must be positive", field, raw)
	}
	return q, nil
}
