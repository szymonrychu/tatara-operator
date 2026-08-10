package agent_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// workspaceInputs is sampleInputs with the operator-wide workspace switch ON,
// which is the only state in which BuildPod mounts anything.
func workspaceInputs() (*tatarav1alpha1.Project, *tatarav1alpha1.Repository, *tatarav1alpha1.Task, agent.PodConfig) {
	proj, repo, task, cfg := sampleInputs()
	cfg.WorkspacePVCEnabled = true
	return proj, repo, task, cfg
}

func mountByPath(c corev1.Container, path string) (corev1.VolumeMount, bool) {
	for _, m := range c.VolumeMounts {
		if m.MountPath == path {
			return m, true
		}
	}
	return corev1.VolumeMount{}, false
}

func volumeByName(pod *corev1.Pod, name string) (corev1.Volume, bool) {
	for _, v := range pod.Spec.Volumes {
		if v.Name == name {
			return v, true
		}
	}
	return corev1.Volume{}, false
}

// The PVC names ride on PodName, which is stable per Task and capped at 63.
// PVC names are DNS-1123 SUBDOMAINS (253), so the "-ws" suffix is free, and
// stability across stages and respawns comes with it.
func TestWorkspacePVCName_IsPodNamePlusSuffix(t *testing.T) {
	_, _, task, _ := workspaceInputs()
	require.Equal(t, agent.PodName(task)+"-ws", agent.WorkspacePVCName(task))

	task.Annotations = map[string]string{agent.PodNameAnnotation: "imp-demo-repo1-i37"}
	require.Equal(t, "imp-demo-repo1-i37-ws", agent.WorkspacePVCName(task))
}

func TestCachePVCName_IsPerProject(t *testing.T) {
	require.Equal(t, "tatara-cache-demo", agent.CachePVCName("demo"))
}

// ONE workspace PVC and ONE cache PVC, split by subPath. The exact map is the
// contract with the wrapper image's layout, so it is pinned here.
func TestBuildPod_WorkspaceAndCacheMounts(t *testing.T) {
	proj, repo, task, cfg := workspaceInputs()
	pod := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg)
	c := pod.Spec.Containers[0]

	want := []struct {
		path, subPath, claim string
	}{
		{"/workspace", "workspace", agent.WorkspacePVCName(task)},
		{"/home/agent/.cache/pre-commit", "pre-commit", agent.WorkspacePVCName(task)},
		{"/home/agent/.cache/go-build", "go-build", agent.CachePVCName(proj.Name)},
		{"/home/agent/go/pkg/mod", "go-mod", agent.CachePVCName(proj.Name)},
		{"/home/agent/.cache/pip", "pip", agent.CachePVCName(proj.Name)},
		{"/home/agent/.npm", "npm", agent.CachePVCName(proj.Name)},
		{"/home/agent/.local/share/mise/downloads", "mise-downloads", agent.CachePVCName(proj.Name)},
	}
	for _, w := range want {
		m, ok := mountByPath(c, w.path)
		require.True(t, ok, "no mount at %s", w.path)
		require.Equal(t, w.subPath, m.SubPath, "subPath at %s", w.path)
		v, ok := volumeByName(pod, m.Name)
		require.True(t, ok, "mount %s names a volume that does not exist", w.path)
		require.NotNil(t, v.PersistentVolumeClaim, "volume %s must be a PVC", v.Name)
		require.Equal(t, w.claim, v.PersistentVolumeClaim.ClaimName, "claim behind %s", w.path)
	}

	// pre-commit is the ONE cache deliberately kept on the per-TASK volume: it
	// has no atomicity guarantee, so a hook env killed mid-install is left half
	// populated with no marker and pre-commit reuses it. Per-Task bounds the
	// blast radius to one Task instead of the whole project.
	pc, _ := mountByPath(c, "/home/agent/.cache/pre-commit")
	gb, _ := mountByPath(c, "/home/agent/.cache/go-build")
	require.NotEqual(t, pc.Name, gb.Name, "pre-commit must not sit on the shared cache volume")
}

// THE MISE TRAP. The Dockerfile bakes python and pre-commit via `mise use -g`
// into .../mise/installs (121 MB measured) and puts .../mise/shims on PATH.
// Mounting an empty volume at ~/.local/share/mise, or at .../installs, HIDES
// both: no python3, no pre-commit, and the wrapper runs `pre-commit install` in
// every clone. Only the sibling `downloads` directory may be mounted.
func TestBuildPod_NeverMountsOverMiseInstallsOrShims(t *testing.T) {
	proj, repo, task, cfg := workspaceInputs()
	c := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.Containers[0]

	forbidden := []string{
		"/home/agent/.local/share/mise",
		"/home/agent/.local/share/mise/installs",
		"/home/agent/.local/share/mise/shims",
	}
	for _, m := range c.VolumeMounts {
		for _, bad := range forbidden {
			require.NotEqual(t, bad, strings.TrimSuffix(m.MountPath, "/"),
				"mounting %s hides the baked mise toolchain", bad)
		}
	}
}

// Keeping the downloaded archives is what makes the downloads cache worth
// having: mise deletes them after install unless told otherwise.
func TestBuildPod_SetsMiseAlwaysKeepDownload(t *testing.T) {
	proj, repo, task, cfg := workspaceInputs()
	c := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.Containers[0]
	v, ok := envValue(c, "MISE_ALWAYS_KEEP_DOWNLOAD")
	require.True(t, ok, "MISE_ALWAYS_KEEP_DOWNLOAD must be set when the cache is mounted")
	require.Equal(t, "1", v)
}

// The operator's own volumes and mounts are PREPENDED, so an extraVolumeMount
// cannot shadow one of them - the same first-wins rationale as the env block.
func TestBuildPod_OperatorVolumesPrecedeExtras(t *testing.T) {
	proj, repo, task, cfg := workspaceInputs()
	proj.Spec.Agent.ExtraVolumes = []corev1.Volume{{
		Name:         "extra",
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	}}
	proj.Spec.Agent.ExtraVolumeMounts = []corev1.VolumeMount{{Name: "extra", MountPath: "/workspace"}}

	pod := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg)
	require.Equal(t, "extra", pod.Spec.Volumes[len(pod.Spec.Volumes)-1].Name)

	mounts := pod.Spec.Containers[0].VolumeMounts
	require.Equal(t, "extra", mounts[len(mounts)-1].Name)
	// First occurrence of /workspace is the operator's, not the extra's.
	first, ok := mountByPath(pod.Spec.Containers[0], "/workspace")
	require.True(t, ok)
	require.NotEqual(t, "extra", first.Name)
}

// Three independent off-switches, all of which must leave the pod exactly as it
// was before this feature existed.
func TestBuildPod_NoWorkspaceMountsWhenDisabled(t *testing.T) {
	fa := false

	t.Run("operator-wide switch off (the default)", func(t *testing.T) {
		proj, repo, task, cfg := sampleInputs() // WorkspacePVCEnabled false
		pod := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg)
		require.Empty(t, pod.Spec.Volumes)
		require.Empty(t, pod.Spec.Containers[0].VolumeMounts)
	})

	t.Run("per-project escape hatch off", func(t *testing.T) {
		proj, repo, task, cfg := workspaceInputs()
		proj.Spec.Workspace = &tatarav1alpha1.WorkspaceSpec{Enabled: &fa}
		pod := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg)
		require.Empty(t, pod.Spec.Volumes)
		require.Empty(t, pod.Spec.Containers[0].VolumeMounts)
	})

	t.Run("cache off leaves the workspace mounted", func(t *testing.T) {
		proj, repo, task, cfg := workspaceInputs()
		proj.Spec.Workspace = &tatarav1alpha1.WorkspaceSpec{CacheEnabled: &fa}
		pod := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg)
		c := pod.Spec.Containers[0]
		_, ok := mountByPath(c, "/workspace")
		require.True(t, ok, "the workspace volume is independent of the cache")
		_, ok = mountByPath(c, "/home/agent/.cache/go-build")
		require.False(t, ok, "no cache mounts when the cache is disabled")
		_, ok = envValue(c, "MISE_ALWAYS_KEEP_DOWNLOAD")
		require.False(t, ok, "no downloads cache means nothing to keep")
		require.Len(t, pod.Spec.Volumes, 1, "only the workspace PVC")
	})
}

// A fresh CephFS subvolume is root:root 0755 and the agent runs as uid 10001,
// so without an fsGroup the first clone fails on permission denied. The CHANGE
// POLICY is equally load-bearing: the CephFS CSI driver reports
// fsGroupPolicy: File, so kubelet recursively chowns the whole volume on EVERY
// mount, and on a 51k-inode Go cache that stall makes the cache a net loss.
func TestBuildPodSecurityContext_FSGroupAndChangePolicy(t *testing.T) {
	proj, repo, task, cfg := workspaceInputs()
	gid := int64(10001)
	cfg.FSGroup = &gid

	sc := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.SecurityContext
	require.NotNil(t, sc)
	require.NotNil(t, sc.FSGroup)
	require.Equal(t, int64(10001), *sc.FSGroup)
	require.NotNil(t, sc.FSGroupChangePolicy)
	require.Equal(t, corev1.FSGroupChangeOnRootMismatch, *sc.FSGroupChangePolicy)
}

// An explicit policy from the chart still wins; the constant above is only the
// fallback for an empty/pruned config value.
func TestBuildPodSecurityContext_ExplicitChangePolicyWins(t *testing.T) {
	proj, repo, task, cfg := workspaceInputs()
	gid := int64(10001)
	cfg.FSGroup = &gid
	cfg.FSGroupChangePolicy = string(corev1.FSGroupChangeAlways)

	sc := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.SecurityContext
	require.NotNil(t, sc.FSGroupChangePolicy)
	require.Equal(t, corev1.FSGroupChangeAlways, *sc.FSGroupChangePolicy)
}

// No fsGroup configured is still no pod-level SecurityContext at all: a change
// policy with nothing to change is meaningless.
func TestBuildPodSecurityContext_NilWithoutFSGroup(t *testing.T) {
	proj, repo, task, cfg := workspaceInputs()
	cfg.FSGroupChangePolicy = string(corev1.FSGroupChangeOnRootMismatch)
	require.Nil(t, agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg).Spec.SecurityContext)
}

// These come from a per-project CRD string, so they must never reach
// resource.MustParse - a typo would panic the reconcile hot path.
func TestValidateWorkspaceQuantities(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	require.NoError(t, agent.ValidateWorkspaceQuantities(proj), "the defaults must always parse")

	proj.Spec.Workspace = &tatarav1alpha1.WorkspaceSpec{Size: "10 gigabytes"}
	err := agent.ValidateWorkspaceQuantities(proj)
	require.Error(t, err)
	require.Contains(t, err.Error(), "workspace.size")

	proj.Spec.Workspace = &tatarav1alpha1.WorkspaceSpec{CacheSize: "-1Gi"}
	err = agent.ValidateWorkspaceQuantities(proj)
	require.Error(t, err)
	require.Contains(t, err.Error(), "workspace.cacheSize")
}
