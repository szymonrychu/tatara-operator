package grafanamcp

import (
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const grafanaRunAsUser int64 = 65532 // distroless nonroot; image runs as nonroot

func imagePullSecrets(cfg Config) []corev1.LocalObjectReference {
	if cfg.ImagePullSecret == "" {
		return nil
	}
	return []corev1.LocalObjectReference{{Name: cfg.ImagePullSecret}}
}

// Deployment builds the per-Project read-only grafana-mcp Deployment.
// streamable-http on :8000, --disable-write, Grafana Viewer token mounted from
// the project's grafana secret (key serviceAccountToken) and read via
// GRAFANA_SERVICE_ACCOUNT_TOKEN_FILE (re-read per request).
// Precondition: p.Spec.Grafana must be non-nil (caller guards with Spec.Grafana.Enabled check).
func Deployment(p *tatarav1alpha1.Project, cfg Config) *appsv1.Deployment {
	name := Name(p.Name)
	replicas := int32(1)
	runAsNonRoot := true
	noPrivEsc := false
	runAsUser := grafanaRunAsUser
	sel := map[string]string{"app.kubernetes.io/instance": name}
	podLabels := labels(p.Name)

	return &appsv1.Deployment{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: objectMeta(p, cfg, name),
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: sel},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
				Spec: corev1.PodSpec{
					ImagePullSecrets: imagePullSecrets(cfg),
					Volumes: []corev1.Volume{{
						Name: "grafana-token",
						VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
							SecretName: p.Spec.Grafana.SecretRef,
							Items:      []corev1.KeyToPath{{Key: "serviceAccountToken", Path: "token"}},
						}},
					}},
					Containers: []corev1.Container{{
						Name:  "grafana-mcp",
						Image: cfg.Image,
						Args:  []string{"-t", "streamable-http", "--disable-write"},
						Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8000, Protocol: corev1.ProtocolTCP}},
						Env: []corev1.EnvVar{
							{Name: "GRAFANA_URL", Value: p.Spec.Grafana.URL},
							{Name: "GRAFANA_SERVICE_ACCOUNT_TOKEN_FILE", Value: "/etc/grafana/token"},
						},
						VolumeMounts: []corev1.VolumeMount{{
							Name: "grafana-token", MountPath: "/etc/grafana", ReadOnly: true,
						}},
						SecurityContext: &corev1.SecurityContext{
							RunAsNonRoot:             &runAsNonRoot,
							RunAsUser:                &runAsUser,
							AllowPrivilegeEscalation: &noPrivEsc,
						},
					}},
				},
			},
		},
	}
}

// Service exposes grafana-mcp on 8000 (ClusterIP).
// Precondition: p.Spec.Grafana must be non-nil (caller guards with Spec.Grafana.Enabled check).
func Service(p *tatarav1alpha1.Project, cfg Config) *corev1.Service {
	name := Name(p.Name)
	return &corev1.Service{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: objectMeta(p, cfg, name),
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: map[string]string{"app.kubernetes.io/instance": name},
			Ports:    []corev1.ServicePort{{Name: "http", Port: 8000, TargetPort: intstr.FromString("http"), Protocol: corev1.ProtocolTCP}},
		},
	}
}
