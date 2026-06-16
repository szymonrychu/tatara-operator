package memory

import (
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// MemoryConfigMap holds the tatara-memory non-secret env (ported from the
// chart envConfig), rewired to the per-Project lightrag URL and the operator
// OIDC config.
func MemoryConfigMap(p *tatarav1alpha1.Project, cfg Config) *corev1.ConfigMap {
	n := NamesFor(p.Name)
	return &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: objectMeta(p, cfg, n.Memory),
		Data: map[string]string{
			"HTTP_ADDR":         ":8080",
			"LIGHTRAG_BASE_URL": "http://" + n.Lightrag + ":9621",
			"OIDC_ISSUER":       cfg.OIDCIssuer,
			"OIDC_AUDIENCE":     cfg.OIDCAudience,
			"WORKER_POOL_SIZE":  "4",
			"LOG_LEVEL":         "info",
		},
	}
}

// MemorySecret is the per-Project tatara-memory Secret. The real PG DSN comes
// from the cnpg app Secret via an inline PG_DSN env (see MemoryDeployment);
// this Secret exists for envFrom symmetry and future non-cnpg secret env.
func MemorySecret(p *tatarav1alpha1.Project, cfg Config) *corev1.Secret {
	n := NamesFor(p.Name)
	return &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: objectMeta(p, cfg, n.Memory),
		Type:       corev1.SecretTypeOpaque,
	}
}

// MemoryDeployment builds the per-Project tatara-memory Deployment (port 8080).
// Non-secret env via the ConfigMap, the per-Project Secret via envFrom, and
// PG_DSN inline from the cnpg app Secret key uri.
func MemoryDeployment(p *tatarav1alpha1.Project, cfg Config) *appsv1.Deployment {
	n := NamesFor(p.Name)
	replicas := int32(1)
	sel := selectorLabels(p.Name, "memory")
	podLabels := labels(p.Name)
	podLabels["app.kubernetes.io/component"] = "memory"

	return &appsv1.Deployment{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: objectMeta(p, cfg, n.Memory),
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: sel},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
				Spec: corev1.PodSpec{
					ImagePullSecrets: imagePullSecrets(cfg),
					Containers: []corev1.Container{{
						Name:  "tatara-memory",
						Image: cfg.MemoryImage,
						Ports: []corev1.ContainerPort{
							{Name: "http", ContainerPort: 8080, Protocol: corev1.ProtocolTCP},
						},
						EnvFrom: []corev1.EnvFromSource{
							{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: n.Memory}}},
							{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: n.Memory}}},
						},
						Env: []corev1.EnvVar{
							secretEnv("PG_DSN", n.PGAppSecret, "uri"),
							// Wire the shared OpenAI secret so the tatara-memory community
							// labeler (NewOpenAILabelerFromEnv) finds OPENAI_API_KEY and
							// uses LLM labels instead of silently falling back to member names.
							secretEnv("OPENAI_API_KEY", cfg.OpenAISecretName, "LLM_BINDING_API_KEY"),
						},
						LivenessProbe: &corev1.Probe{
							ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromString("http")}},
							InitialDelaySeconds: 5,
							PeriodSeconds:       10,
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/readyz", Port: intstr.FromString("http")}},
							InitialDelaySeconds: 5,
							PeriodSeconds:       10,
						},
					}},
				},
			},
		},
	}
}

// MemoryService exposes tatara-memory on 8080 (ClusterIP).
func MemoryService(p *tatarav1alpha1.Project, cfg Config) *corev1.Service {
	n := NamesFor(p.Name)
	return &corev1.Service{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: objectMeta(p, cfg, n.Memory),
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: selectorLabels(p.Name, "memory"),
			Ports: []corev1.ServicePort{
				{Name: "http", Port: 8080, TargetPort: intstr.FromString("http"), Protocol: corev1.ProtocolTCP},
			},
		},
	}
}
