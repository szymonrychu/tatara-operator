package memory

import (
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// selectorLabels are the subset used as pod/Service selectors (immutable on a
// StatefulSet, so kept minimal and stable).
func selectorLabels(project, component string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/instance":  "mem-" + project,
		"app.kubernetes.io/component": component,
	}
}

// Neo4jStatefulSet builds the single-node community neo4j StatefulSet. It is a
// native build, NOT the upstream neo4j Helm chart: one replica, NEO4J_AUTH
// from the generated Secret, bolt 7687 / http 7474, data PVC at /data.
func Neo4jStatefulSet(p *tatarav1alpha1.Project, cfg Config) *appsv1.StatefulSet {
	n := NamesFor(p.Name)
	replicas := int32(1)
	sel := selectorLabels(p.Name, "neo4j")
	podLabels := labels(p.Name)
	podLabels["app.kubernetes.io/component"] = "neo4j"

	return &appsv1.StatefulSet{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "StatefulSet"},
		ObjectMeta: objectMeta(p, cfg, n.Neo4j),
		Spec: appsv1.StatefulSetSpec{
			ServiceName: n.Neo4j,
			Replicas:    &replicas,
			Selector:    &metav1.LabelSelector{MatchLabels: sel},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
				Spec: corev1.PodSpec{
					ImagePullSecrets:          imagePullSecrets(cfg),
					Affinity:                  componentAffinity(p.Name, "neo4j", cfg),
					TopologySpreadConstraints: topologySpreadConstraints(p.Name, "neo4j", cfg),
					Containers: []corev1.Container{{
						Name:  "neo4j",
						Image: cfg.Neo4jImage,
						Env: []corev1.EnvVar{
							{
								Name: "NEO4J_AUTH",
								ValueFrom: &corev1.EnvVarSource{
									SecretKeyRef: &corev1.SecretKeySelector{
										LocalObjectReference: corev1.LocalObjectReference{Name: n.Neo4jSecret},
										Key:                  "NEO4J_AUTH",
									},
								},
							},
						},
						Ports: []corev1.ContainerPort{
							{Name: "bolt", ContainerPort: 7687, Protocol: corev1.ProtocolTCP},
							{Name: "http", ContainerPort: 7474, Protocol: corev1.ProtocolTCP},
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString("bolt")},
							},
							PeriodSeconds: 10,
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "data", MountPath: "/data"},
						},
					}},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: "data"},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse(neo4jStorage(p)),
						},
					},
				},
			}},
		},
	}
}

// Neo4jService exposes bolt and http for the neo4j StatefulSet (ClusterIP).
// lightrag connects to bolt://mem-<proj>-neo4j:7687.
func Neo4jService(p *tatarav1alpha1.Project, cfg Config) *corev1.Service {
	n := NamesFor(p.Name)
	return &corev1.Service{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: objectMeta(p, cfg, n.Neo4j),
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: selectorLabels(p.Name, "neo4j"),
			Ports: []corev1.ServicePort{
				{Name: "bolt", Port: 7687, TargetPort: intstr.FromString("bolt"), Protocol: corev1.ProtocolTCP},
				{Name: "http", Port: 7474, TargetPort: intstr.FromString("http"), Protocol: corev1.ProtocolTCP},
			},
		},
	}
}
