package memory

import (
	"fmt"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

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

// lightragEnv is the lightrag container environment, ported from the chart's
// configKeys (non-secret defaults) and secret refs, rewired to per-Project
// postgres (mem-<proj>-pg-rw / app Secret), neo4j (mem-<proj>-neo4j), and the
// shared OpenAI Secret.
func lightragEnv(p *tatarav1alpha1.Project, cfg Config) []corev1.EnvVar {
	n := NamesFor(p.Name)
	lit := func(k, v string) corev1.EnvVar { return corev1.EnvVar{Name: k, Value: v} }
	return []corev1.EnvVar{
		lit("LLM_BINDING", "openai"),
		lit("LLM_MODEL", "gpt-4.1-mini"),
		lit("EMBEDDING_BINDING", "openai"),
		lit("EMBEDDING_MODEL", "text-embedding-3-small"),
		lit("EMBEDDING_DIM", "1536"),
		lit("LIGHTRAG_KV_STORAGE", "PGKVStorage"),
		lit("LIGHTRAG_VECTOR_STORAGE", "PGVectorStorage"),
		lit("LIGHTRAG_GRAPH_STORAGE", "Neo4JStorage"),
		lit("LIGHTRAG_DOC_STATUS_STORAGE", "PGDocStatusStorage"),
		lit("NEO4J_URI", "bolt://"+n.Neo4j+":7687"),
		lit("NEO4J_USERNAME", "neo4j"),
		lit("MAX_ASYNC", "8"),
		lit("MAX_PARALLEL_INSERT", "8"),
		lit("EMBEDDING_FUNC_MAX_ASYNC", "8"),
		lit("POSTGRES_HOST", n.PGService),
		lit("POSTGRES_PORT", "5432"),
		lit("POSTGRES_DATABASE", "tatara_memory"),
		lit("POSTGRES_USER", "tatara_memory"),
		secretEnv("LLM_BINDING_API_KEY", cfg.OpenAISecretName, "LLM_BINDING_API_KEY"),
		// LightRAG's openai LLM/embedding paths fall back to the raw OPENAI_API_KEY
		// env var; without it document processing fails KeyError 'OPENAI_API_KEY'.
		secretEnv("OPENAI_API_KEY", cfg.OpenAISecretName, "LLM_BINDING_API_KEY"),
		secretEnv("POSTGRES_PASSWORD", n.PGAppSecret, "password"),
		secretEnv("NEO4J_PASSWORD", n.Neo4jSecret, "password"),
	}
}

// neo4jWaitScript polls Neo4j's bolt port with a plain python3 TCP connect
// attempt until it accepts a connection, or gives up after maxNeo4jWaitAttempts
// and exits non-zero. python3 ships inside the lightrag image itself (it is
// LightRAG's own runtime), so this deliberately reuses cfg.LightragImage as the
// initContainer image rather than pulling in a new tool/image for the wait.
//
// Bounded failure mode: this does NOT poll forever. After maxNeo4jWaitAttempts
// (5s apart, ~5 minutes total) it exits 1, which leaves the initContainer -
// and therefore the whole pod - in a standard, diagnosable Init:CrashLoopBackOff
// rather than hanging silently forever. `kubectl get pods` shows that state
// directly and `kubectl logs -c wait-for-neo4j <pod>` (or `--previous`) shows
// exactly which attempt failed and against which host:port, because every
// attempt (success or failure) is logged.
//
// Only Neo4j is gated. Upstream LightRAG already retries Postgres ~10 times on
// boot (the incident's own Loki evidence shows those retries working: 9
// attempts over ~3 minutes before Postgres came back), so a Postgres wait here
// would duplicate behaviour that already works rather than fixing anything.
// Neo4j has no retry at all - one failed connection is a fatal
// "Application startup failed. Exiting." - which is the actual root cause and
// the only thing a probe on the main container cannot compensate for (the
// process is already dead, not merely unready).
const (
	// maxNeo4jWaitAttempts and neo4jWaitIntervalSeconds are interpolated into
	// neo4jWaitScript below, so the comment above and the shell text can never
	// drift out of sync with each other again.
	maxNeo4jWaitAttempts     = 60
	neo4jWaitIntervalSeconds = 5
)

var neo4jWaitScript = fmt.Sprintf(`set -eu
attempt=0
max_attempts=%d
until python3 -c "import socket; socket.create_connection(('${NEO4J_HOST}', 7687), timeout=5).close()" 2>/dev/null; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge "$max_attempts" ]; then
    echo "wait-for-neo4j: giving up after ${attempt} attempts, ${NEO4J_HOST}:7687 still unreachable" >&2
    exit 1
  fi
  echo "wait-for-neo4j: ${NEO4J_HOST}:7687 not reachable yet (attempt ${attempt}/${max_attempts}), retrying in %ds"
  sleep %d
done
echo "wait-for-neo4j: ${NEO4J_HOST}:7687 reachable after ${attempt} attempt(s)"
`, maxNeo4jWaitAttempts, neo4jWaitIntervalSeconds, neo4jWaitIntervalSeconds)

// lightragInitContainers gates lightrag's startup on Neo4j being reachable.
// See neo4jWaitScript for the mechanism and why only Neo4j (not Postgres) is
// gated.
func lightragInitContainers(n Names, cfg Config) []corev1.Container {
	return []corev1.Container{{
		Name:                     "wait-for-neo4j",
		Image:                    cfg.LightragImage,
		Command:                  []string{"/bin/sh", "-c"},
		Args:                     []string{neo4jWaitScript},
		Env:                      []corev1.EnvVar{{Name: "NEO4J_HOST", Value: n.Neo4j}},
		TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
	}}
}

// lightragDataPlaneProbe builds a probe against a LightRAG endpoint that
// actually touches the storage backends, rather than the liveness-only
// /health used by tatara-memory's own /readyz. See #502: /health kept
// answering in ~3ms while the data plane was wedged for 121s at a time, so a
// probe on /health (tcpSocket or httpGet) cannot detect this failure mode.
// timeoutSeconds bounds how long a stalled backend can hold up the probe
// itself; failureThreshold*periodSeconds sets how long a stall must persist
// before the probe trips.
func lightragDataPlaneProbe(failureThreshold, periodSeconds int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/documents/status_counts",
				Port: intstr.FromString("http"),
			},
		},
		TimeoutSeconds:   5,
		PeriodSeconds:    periodSeconds,
		FailureThreshold: failureThreshold,
	}
}

// LightragDeployment builds the per-Project lightrag Deployment (port 9621,
// Recreate strategy because the data PVC is RWO with one replica).
func LightragDeployment(p *tatarav1alpha1.Project, cfg Config) *appsv1.Deployment {
	n := NamesFor(p.Name)
	replicas := int32(1)
	sel := selectorLabels(p.Name, "lightrag")
	podLabels := labels(p.Name)
	podLabels["app.kubernetes.io/component"] = "lightrag"

	return &appsv1.Deployment{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: objectMeta(p, cfg, n.Lightrag),
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Strategy: appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType},
			Selector: &metav1.LabelSelector{MatchLabels: sel},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
				Spec: corev1.PodSpec{
					ImagePullSecrets:          imagePullSecrets(cfg),
					Affinity:                  componentAffinity(p.Name, "lightrag", cfg),
					TopologySpreadConstraints: topologySpreadConstraints(p.Name, "lightrag", cfg),
					InitContainers:            lightragInitContainers(n, cfg),
					Containers: []corev1.Container{{
						Name:  "lightrag",
						Image: cfg.LightragImage,
						Ports: []corev1.ContainerPort{
							{Name: "http", ContainerPort: 9621, Protocol: corev1.ProtocolTCP},
						},
						Env: lightragEnv(p, cfg),
						// #502 root cause: a tcpSocket readinessProbe (and an httpGet
						// /health probe, which was measured returning 200 in ~3ms
						// mid-incident) cannot see a wedged data plane - LightRAG kept
						// accepting TCP and answering /health while /query and
						// /documents/* hung for up to 121s. /documents/status_counts is
						// a GET that actually touches the storage backends, so a stall
						// there reflects real data-plane health.
						ReadinessProbe: lightragDataPlaneProbe(3, 10),
						// No livenessProbe existed at all, so the wedged pod (Ready=true,
						// 0 restarts) was never restarted and never routed around.
						// failureThreshold*periodSeconds=180s tolerates the real busy
						// stretches observed in the incident (up to 0.30 cores of genuine
						// work) before concluding the process is actually wedged.
						LivenessProbe: lightragDataPlaneProbe(6, 30),
						Resources: corev1.ResourceRequirements{
							// #502 Q6: no resources at all meant BestEffort QoS - first in
							// line for eviction, lowest CPU shares under contention.
							// Requests/limits sized against the incident's own measurements
							// (idle ~0.010-0.011 cores, 877MB working set, bursts to 0.30
							// cores) with headroom for real ingest work.
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("250m"),
								corev1.ResourceMemory: resource.MustParse("512Mi"),
							},
							Limits: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("2"),
								corev1.ResourceMemory: resource.MustParse("2Gi"),
							},
						},
						VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/app/data"}},
					}},
					Volumes: []corev1.Volume{{
						Name: "data",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: n.LightragPVC},
						},
					}},
				},
			},
		},
	}
}

// LightragService exposes lightrag on 9621 (ClusterIP).
func LightragService(p *tatarav1alpha1.Project, cfg Config) *corev1.Service {
	n := NamesFor(p.Name)
	return &corev1.Service{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: objectMeta(p, cfg, n.Lightrag),
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: selectorLabels(p.Name, "lightrag"),
			Ports: []corev1.ServicePort{
				{Name: "http", Port: 9621, TargetPort: intstr.FromString("http"), Protocol: corev1.ProtocolTCP},
			},
		},
	}
}

// LightragPVC is the lightrag data volume (RWO, sized 10Gi by default; lightrag
// storage is not separately configurable in spec.memory, so it uses the fixed
// chart default).
func LightragPVC(p *tatarav1alpha1.Project, cfg Config) *corev1.PersistentVolumeClaim {
	n := NamesFor(p.Name)
	return &corev1.PersistentVolumeClaim{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaim"},
		ObjectMeta: objectMeta(p, cfg, n.LightragPVC),
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
			},
		},
	}
}
