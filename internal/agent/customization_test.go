package agent_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
)

func volumeByName(vols []corev1.Volume, name string) (corev1.Volume, bool) {
	for _, v := range vols {
		if v.Name == name {
			return v, true
		}
	}
	return corev1.Volume{}, false
}

func mountByName(mounts []corev1.VolumeMount, name string) (corev1.VolumeMount, bool) {
	for _, m := range mounts {
		if m.Name == name {
			return m, true
		}
	}
	return corev1.VolumeMount{}, false
}

func cmByName(cms []*corev1.ConfigMap, name string) (*corev1.ConfigMap, bool) {
	for _, c := range cms {
		if c.Name == name {
			return c, true
		}
	}
	return nil, false
}

func wrapper(pod *corev1.Pod) corev1.Container { return pod.Spec.Containers[0] }

// TestNoCustomization_NoVolumesOrConfigMaps asserts a Project with no AgentSpec
// customization yields no extra volumes/mounts/env and no ConfigMaps (existing
// behaviour preserved).
func TestNoCustomization_NoVolumesOrConfigMaps(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	require.Empty(t, agent.BuildAgentConfigMaps(proj, task, cfg))

	pod := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg)
	require.Empty(t, pod.Spec.Volumes)
	require.Empty(t, wrapper(pod).VolumeMounts)
	_, has := envValue(wrapper(pod), "EXTRA_SETTINGS_PATH")
	require.False(t, has)
	_, has = envValue(wrapper(pod), "PLUGINS_PATH")
	require.False(t, has)
}

func TestSystemPrompt_RendersConfigMapAndMount(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	proj.Spec.Agent.SystemPrompt = "be careful"

	cms := agent.BuildAgentConfigMaps(proj, task, cfg)
	cm, ok := cmByName(cms, agent.PodName(task)+"-prompt")
	require.True(t, ok, "prompt ConfigMap missing")
	require.Equal(t, "be careful", cm.Data["project-claude.md"])
	require.Equal(t, cfg.Namespace, cm.Namespace)
	require.Equal(t, task.Name, cm.OwnerReferences[0].Name)
	require.Equal(t, task.UID, cm.OwnerReferences[0].UID)

	pod := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg)
	v, ok := volumeByName(pod.Spec.Volumes, "agent-prompt")
	require.True(t, ok)
	require.Equal(t, cm.Name, v.ConfigMap.Name)
	m, ok := mountByName(wrapper(pod).VolumeMounts, "agent-prompt")
	require.True(t, ok)
	require.Equal(t, "/etc/wrapper/project-claude.md", m.MountPath)
	require.Equal(t, "project-claude.md", m.SubPath)
	require.True(t, m.ReadOnly)
}

func TestMCPServers_RenderOverlayFragments(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	proj.Spec.Agent.MCPServers = []tatarav1alpha1.MCPServer{
		{Name: "github", ConfigJSON: `{"command":"npx","args":["-y","srv"]}`},
		{Name: "files", ConfigJSON: `{"type":"http","url":"http://x"}`},
	}

	cms := agent.BuildAgentConfigMaps(proj, task, cfg)
	cm, ok := cmByName(cms, agent.PodName(task)+"-mcp")
	require.True(t, ok)
	require.Len(t, cm.Data, 2)
	// Each fragment must be valid JSON with the server under .mcpServers.
	frag := cm.Data["0-github.json"]
	require.NotEmpty(t, frag)
	var doc struct {
		MCPServers map[string]json.RawMessage `json:"mcpServers"`
	}
	require.NoError(t, json.Unmarshal([]byte(frag), &doc))
	require.Contains(t, doc.MCPServers, "github")

	pod := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg)
	m, ok := mountByName(wrapper(pod).VolumeMounts, "agent-mcp")
	require.True(t, ok)
	require.Equal(t, "/etc/wrapper/mcp.d", m.MountPath)
	require.Empty(t, m.SubPath, "mcp overlay is a whole-dir mount")
}

func TestSettings_RenderConfigMapMountAndEnv(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	proj.Spec.Agent.Settings = &apiextensionsv1.JSON{Raw: []byte(`{"maxParallelism":4}`)}

	cms := agent.BuildAgentConfigMaps(proj, task, cfg)
	cm, ok := cmByName(cms, agent.PodName(task)+"-settings")
	require.True(t, ok)
	require.Equal(t, `{"maxParallelism":4}`, cm.Data["settings-extra.json"])

	pod := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg)
	m, ok := mountByName(wrapper(pod).VolumeMounts, "agent-settings")
	require.True(t, ok)
	require.Equal(t, "/etc/wrapper/settings-extra.json", m.MountPath)
	require.Equal(t, "settings-extra.json", m.SubPath)
	v, ok := envValue(wrapper(pod), "EXTRA_SETTINGS_PATH")
	require.True(t, ok)
	require.Equal(t, "/etc/wrapper/settings-extra.json", v)
}

func TestPlugins_RenderConfigMapMountAndEnv(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	proj.Spec.Agent.Plugins = []tatarav1alpha1.Plugin{
		{Name: "p1", Source: "https://market"},
		{Name: "p2"},
	}

	cms := agent.BuildAgentConfigMaps(proj, task, cfg)
	cm, ok := cmByName(cms, agent.PodName(task)+"-plugins")
	require.True(t, ok)
	var got []tatarav1alpha1.Plugin
	require.NoError(t, json.Unmarshal([]byte(cm.Data["plugins.json"]), &got))
	require.Equal(t, proj.Spec.Agent.Plugins, got)

	pod := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg)
	m, ok := mountByName(wrapper(pod).VolumeMounts, "agent-plugins")
	require.True(t, ok)
	require.Equal(t, "/etc/wrapper/plugins.json", m.MountPath)
	v, ok := envValue(wrapper(pod), "PLUGINS_PATH")
	require.True(t, ok)
	require.Equal(t, "/etc/wrapper/plugins.json", v)
}

func TestSkills_MountUserConfigMaps(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	proj.Spec.Agent.Skills = []tatarav1alpha1.SkillSource{
		{Name: "deploy", ConfigMapRef: "skill-deploy-cm"},
		{Name: "audit", ConfigMapRef: "skill-audit-cm"},
	}

	// Skills are user-provided ConfigMaps: the operator mounts but never
	// generates them.
	require.Empty(t, agent.BuildAgentConfigMaps(proj, task, cfg))

	pod := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg)
	v0, ok := volumeByName(pod.Spec.Volumes, "agent-skill-0")
	require.True(t, ok)
	require.Equal(t, "skill-deploy-cm", v0.ConfigMap.Name)
	m0, ok := mountByName(wrapper(pod).VolumeMounts, "agent-skill-0")
	require.True(t, ok)
	require.Equal(t, "/etc/wrapper/skills/deploy", m0.MountPath)
	m1, ok := mountByName(wrapper(pod).VolumeMounts, "agent-skill-1")
	require.True(t, ok)
	require.Equal(t, "/etc/wrapper/skills/audit", m1.MountPath)
}

func TestEnv_AppendedWithSecretRefPreserved(t *testing.T) {
	proj, repo, task, cfg := sampleInputs()
	proj.Spec.Agent.Env = []corev1.EnvVar{
		{Name: "MY_FLAG", Value: "on"},
		{Name: "MY_SECRET", ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "s"},
				Key:                  "k",
			},
		}},
	}

	pod := agent.BuildPod(proj, repo, task, nil, testMemoryEndpoint, cfg)
	c := wrapper(pod)
	v, ok := envValue(c, "MY_FLAG")
	require.True(t, ok)
	require.Equal(t, "on", v)
	// secretKeyRef must be passed through verbatim (operator never sees plaintext).
	var found *corev1.EnvVar
	for i := range c.Env {
		if c.Env[i].Name == "MY_SECRET" {
			found = &c.Env[i]
		}
	}
	require.NotNil(t, found)
	require.NotNil(t, found.ValueFrom)
	require.Equal(t, "s", found.ValueFrom.SecretKeyRef.Name)
	require.Equal(t, "k", found.ValueFrom.SecretKeyRef.Key)

	// Operator's own vars must still come first (MODEL precedes the custom ones).
	var modelIdx, flagIdx = -1, -1
	for i, e := range c.Env {
		if e.Name == "MODEL" {
			modelIdx = i
		}
		if e.Name == "MY_FLAG" {
			flagIdx = i
		}
	}
	require.Greater(t, flagIdx, modelIdx, "Project env must be appended after operator env")
}
