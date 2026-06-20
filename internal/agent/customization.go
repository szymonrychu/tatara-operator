package agent

import (
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// Wrapper mount paths for the per-Project agent customization (issue #74). The
// file/dir targets match the wrapper's own config defaults (PROJECT_CLAUDE_MD_PATH,
// MCP_OVERLAY_DIR, the /etc/wrapper/skills entry of SKILLS_SRC_DIRS), so the
// operator only needs to place files there; the two paths with no existing
// wrapper default (settings overlay, plugins list) are also advertised to the
// wrapper via env (EXTRA_SETTINGS_PATH, PLUGINS_PATH) for subtask 3 to consume.
const (
	wrapperEtcDir = "/etc/wrapper"

	projectClaudeMdPath = wrapperEtcDir + "/project-claude.md"
	mcpOverlayPath      = wrapperEtcDir + "/mcp.d"
	skillsPath          = wrapperEtcDir + "/skills"
	extraSettingsPath   = wrapperEtcDir + "/settings-extra.json"
	pluginsPath         = wrapperEtcDir + "/plugins.json"

	projectClaudeMdKey = "project-claude.md"
	extraSettingsKey   = "settings-extra.json"
	pluginsKey         = "plugins.json"
)

// ConfigMap-name helpers for the operator-generated agent customization
// ConfigMaps. Each is derived from the Pod name so it is deterministic and
// unique per Task, and owner-referenced to the Task so it is garbage-collected
// with it. Skills are NOT generated here: they reference user-provided
// ConfigMaps (SkillSource.ConfigMapRef) the operator only mounts.
func promptConfigMapName(task *tatarav1alpha1.Task) string   { return PodName(task) + "-prompt" }
func mcpConfigMapName(task *tatarav1alpha1.Task) string      { return PodName(task) + "-mcp" }
func settingsConfigMapName(task *tatarav1alpha1.Task) string { return PodName(task) + "-settings" }
func pluginsConfigMapName(task *tatarav1alpha1.Task) string  { return PodName(task) + "-plugins" }

func agentConfigMap(name string, task *tatarav1alpha1.Task, cfg PodConfig, data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       cfg.Namespace,
			Labels:          podLabels(task),
			OwnerReferences: []metav1.OwnerReference{ownerRef(task)},
		},
		Data: data,
	}
}

// BuildAgentConfigMaps returns the operator-generated ConfigMaps that carry the
// Project's agent customization for one Task: the system prompt, the additional
// MCP server overlay fragments, the extra claude settings, and the plugin list.
// Only the ConfigMaps for set AgentSpec fields are returned (an unconfigured
// Project yields none). It is a pure builder; the caller creates the objects.
func BuildAgentConfigMaps(project *tatarav1alpha1.Project, task *tatarav1alpha1.Task, cfg PodConfig) []*corev1.ConfigMap {
	a := project.Spec.Agent
	var cms []*corev1.ConfigMap

	if a.SystemPrompt != "" {
		cms = append(cms, agentConfigMap(promptConfigMapName(task), task, cfg, map[string]string{
			projectClaudeMdKey: a.SystemPrompt,
		}))
	}

	if len(a.MCPServers) > 0 {
		data := make(map[string]string, len(a.MCPServers))
		for i, srv := range a.MCPServers {
			// One overlay fragment per server; the wrapper unions every *.json in
			// MCP_OVERLAY_DIR into .mcp.json. The filename only needs a .json
			// extension and uniqueness (the real key is inside the JSON), so an
			// index prefix guarantees no collision when two names sanitize alike.
			fname := fmt.Sprintf("%d-%s.json", i, sanitizeDNS1123(srv.Name))
			data[fname] = fmt.Sprintf(`{"mcpServers":{%q:%s}}`, srv.Name, srv.ConfigJSON)
		}
		cms = append(cms, agentConfigMap(mcpConfigMapName(task), task, cfg, data))
	}

	if a.Settings != nil && len(a.Settings.Raw) > 0 {
		cms = append(cms, agentConfigMap(settingsConfigMapName(task), task, cfg, map[string]string{
			extraSettingsKey: string(a.Settings.Raw),
		}))
	}

	if len(a.Plugins) > 0 {
		// repoEntry-style: only string fields, json.Marshal cannot fail.
		buf, _ := json.Marshal(a.Plugins)
		cms = append(cms, agentConfigMap(pluginsConfigMapName(task), task, cfg, map[string]string{
			pluginsKey: string(buf),
		}))
	}

	return cms
}

// agentCustomization returns the Volumes, VolumeMounts and extra env vars the
// wrapper container needs to consume the Project's agent customization. It
// mirrors BuildAgentConfigMaps: a volume is added only for a set field. Returns
// all-nil when the Project carries no customization (existing behaviour).
func agentCustomization(project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) ([]corev1.Volume, []corev1.VolumeMount, []corev1.EnvVar) {
	a := project.Spec.Agent
	var vols []corev1.Volume
	var mounts []corev1.VolumeMount
	var env []corev1.EnvVar

	cmVolume := func(volName, cmName string) {
		vols = append(vols, corev1.Volume{
			Name: volName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: cmName},
				},
			},
		})
	}

	if a.SystemPrompt != "" {
		cmVolume("agent-prompt", promptConfigMapName(task))
		// subPath mounts the single file into /etc/wrapper without shadowing the
		// image's other baked files in that dir.
		mounts = append(mounts, corev1.VolumeMount{
			Name: "agent-prompt", MountPath: projectClaudeMdPath, SubPath: projectClaudeMdKey, ReadOnly: true,
		})
	}

	if len(a.MCPServers) > 0 {
		cmVolume("agent-mcp", mcpConfigMapName(task))
		// Whole-dir mount: MCP_OVERLAY_DIR is its own subdirectory, so this does
		// not shadow sibling files in /etc/wrapper.
		mounts = append(mounts, corev1.VolumeMount{
			Name: "agent-mcp", MountPath: mcpOverlayPath, ReadOnly: true,
		})
	}

	if a.Settings != nil && len(a.Settings.Raw) > 0 {
		cmVolume("agent-settings", settingsConfigMapName(task))
		mounts = append(mounts, corev1.VolumeMount{
			Name: "agent-settings", MountPath: extraSettingsPath, SubPath: extraSettingsKey, ReadOnly: true,
		})
		env = append(env, corev1.EnvVar{Name: "EXTRA_SETTINGS_PATH", Value: extraSettingsPath})
	}

	if len(a.Plugins) > 0 {
		cmVolume("agent-plugins", pluginsConfigMapName(task))
		mounts = append(mounts, corev1.VolumeMount{
			Name: "agent-plugins", MountPath: pluginsPath, SubPath: pluginsKey, ReadOnly: true,
		})
		env = append(env, corev1.EnvVar{Name: "PLUGINS_PATH", Value: pluginsPath})
	}

	for i, sk := range a.Skills {
		// One volume per user-provided skill ConfigMap, mounted as its own
		// subdirectory under the wrapper's skills source dir. The wrapper copies
		// the whole tree, so the ConfigMap's keys become the skill's files
		// (e.g. SKILL.md) under /etc/wrapper/skills/<name>.
		volName := fmt.Sprintf("agent-skill-%d", i)
		vols = append(vols, corev1.Volume{
			Name: volName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: sk.ConfigMapRef},
				},
			},
		})
		mounts = append(mounts, corev1.VolumeMount{
			Name: volName, MountPath: skillsPath + "/" + sk.Name, ReadOnly: true,
		})
	}

	return vols, mounts, env
}
