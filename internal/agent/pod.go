package agent

import (
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// wrapperPort is the wrapper's in-pod HTTP listener.
const wrapperPort = 8080

// PodConfig holds the operator-level inputs the Pod/Service builders need that
// do not come from the CRDs.
type PodConfig struct {
	Namespace           string
	CallbackURL         string // full routable in-cluster base URL, e.g. http://tatara-operator-internal.tatara.svc:8082
	OIDCIssuer          string // OIDC issuer URL passed to the wrapper for token verification
	AnthropicSecretName string
	CLIOIDCSecretName   string
}

// PodName returns the deterministic wrapper Pod (and Service) name for a Task.
func PodName(task *tatarav1alpha1.Task) string {
	return "wrapper-" + task.Name
}

func podLabels(task *tatarav1alpha1.Task) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      "tatara-operator",
		"app.kubernetes.io/component": "agent",
		"tatara.dev/managed-by":       "tatara-operator",
		"tatara.dev/task":             task.Name,
	}
}

func ownerRef(task *tatarav1alpha1.Task) metav1.OwnerReference {
	controller := true
	return metav1.OwnerReference{
		APIVersion: tatarav1alpha1.GroupVersion.String(),
		Kind:       "Task",
		Name:       task.Name,
		UID:        task.UID,
		Controller: &controller,
	}
}

func secretEnv(name, secretName, key string) corev1.EnvVar {
	return corev1.EnvVar{
		Name: name,
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
				Key:                  key,
			},
		},
	}
}

// BuildPod returns the wrapper Pod for a Task, owner-referenced to the Task.
func BuildPod(project *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, task *tatarav1alpha1.Task, memoryEndpoint string, cfg PodConfig) *corev1.Pod {
	env := []corev1.EnvVar{
		{Name: "REPO_URL", Value: repo.Spec.URL},
		{Name: "REPO_BRANCH", Value: repo.Spec.DefaultBranch},
		{Name: "MODEL", Value: project.Spec.Agent.Model},
		{Name: "PERMISSION_MODE", Value: project.Spec.Agent.PermissionMode},
		{Name: "TURN_TIMEOUT_SECONDS", Value: strconv.Itoa(project.Spec.Agent.TurnTimeoutSeconds)},
		{Name: "DEFAULT_CALLBACK_URL", Value: strings.TrimSuffix(cfg.CallbackURL, "/") + "/internal/turn-complete"},
		// Task identity: lets the agent address MCP tools without repeating args.
		{Name: "TATARA_TASK", Value: task.Name},
		{Name: "TATARA_PROJECT", Value: project.Name},
		// Per-project memory endpoint: the agent's tatara-cli memory MCP reads
		// TATARA_MEMORY_URL to reach this Project's tatara-memory service.
		{Name: "TATARA_MEMORY_URL", Value: memoryEndpoint},
		// OIDC config: the wrapper enforces bearer tokens with this issuer and audience.
		{Name: "OIDC_ISSUER", Value: cfg.OIDCIssuer},
		{Name: "OIDC_AUDIENCE", Value: "tatara-claude-code-wrapper"},
		secretEnv("CLAUDE_CODE_OAUTH_TOKEN", cfg.AnthropicSecretName, "oauth-token"),
		secretEnv("GIT_TOKEN", project.Spec.ScmSecretRef, "token"),
		secretEnv("CLI_OIDC_CLIENT_ID", cfg.CLIOIDCSecretName, "client-id"),
		secretEnv("CLI_OIDC_CLIENT_SECRET", cfg.CLIOIDCSecretName, "client-secret"),
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            PodName(task),
			Namespace:       cfg.Namespace,
			Labels:          podLabels(task),
			OwnerReferences: []metav1.OwnerReference{ownerRef(task)},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:  "wrapper",
				Image: project.Spec.Agent.Image,
				Env:   env,
				Ports: []corev1.ContainerPort{{ContainerPort: wrapperPort}},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/readyz",
							Port: intstr.FromInt(wrapperPort),
						},
					},
				},
			}},
		},
	}
}

// BuildService returns the ClusterIP Service fronting the wrapper Pod. Its name
// equals the Pod name so the operator can address the wrapper at
// http://<name>.<ns>.svc:8080.
func BuildService(project *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, task *tatarav1alpha1.Task, cfg PodConfig) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            PodName(task),
			Namespace:       cfg.Namespace,
			Labels:          podLabels(task),
			OwnerReferences: []metav1.OwnerReference{ownerRef(task)},
		},
		Spec: corev1.ServiceSpec{
			Selector: podLabels(task),
			Ports: []corev1.ServicePort{{
				Port:       wrapperPort,
				TargetPort: intstr.FromInt(wrapperPort),
			}},
		},
	}
}

// BaseURL returns the in-cluster wrapper address for a Task's Service.
func BaseURL(task *tatarav1alpha1.Task, namespace string) string {
	return "http://" + PodName(task) + "." + namespace + ".svc:" + strconv.Itoa(wrapperPort)
}
