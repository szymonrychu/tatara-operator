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

const (
	// lightragHealthPath is LightRAG's LIVENESS surface. Upstream's GET /health
	// handler only reads in-process shared state (the pipeline-status namespace
	// dict) plus static config; it never touches Postgres or Neo4j. It is also on
	// LightRAG's default WHITELIST_PATHS, so it answers unauthenticated whatever
	// the auth config becomes.
	//
	// That makes it the RIGHT liveness check and a USELESS readiness check. During
	// issue #502 it answered in 2-23ms for hours while every data-plane call hung -
	// tatara-memory's own /readyz calls it and stayed green throughout, which is
	// why the issue warns that "just make the probe httpGet /health" would not
	// have caught the incident.
	lightragHealthPath = "/health"

	// lightragDataPlanePath is LightRAG's READINESS surface: the cheapest
	// unauthenticated GET that actually round-trips the storage backend. Upstream's
	// handler awaits doc_status.get_all_status_counts() on every call with no
	// caching, which with LIGHTRAG_DOC_STATUS_STORAGE=PGDocStatusStorage is a
	// single indexed `SELECT status, COUNT(*) ... GROUP BY status` against the
	// asyncpg pool. Constant cost regardless of corpus size, and it returns
	// {"status_counts":{"all":0}} on an empty corpus rather than erroring.
	//
	// A wedged data plane - the #502 failure, where the async worker pool is
	// permanently occupied and nothing progresses - makes this either hang past
	// timeoutSeconds or return 500. Either fails the probe, the pod leaves the
	// Service endpoints instead of black-holing traffic, AvailableReplicas drops,
	// and memoryPhase() demotes the stack so ingest is gated and the stack-not-ready
	// alert can finally see it. None of that was possible with a tcpSocket check.
	//
	// Verified against the pinned image (ghcr.io/hkuds/lightrag:v1.4.16): the route
	// exists on a router mounted at prefix /documents, takes no path or query
	// parameters, and passes auth unauthenticated because LIGHTRAG_API_KEY and
	// AUTH_ACCOUNTS are both unset in lightragEnv. If either is ever set, this path
	// is NOT on the default whitelist and the probe needs an X-API-Key header
	// (widening WHITELIST_PATHS instead would also re-open POST /api/chat).
	lightragDataPlanePath = "/documents/status_counts"
)

// httpGet builds an HTTPGetAction against the lightrag container's named http
// port, so the three probes cannot drift on port wiring.
func httpGet(path string) *corev1.HTTPGetAction {
	return &corev1.HTTPGetAction{Path: path, Port: intstr.FromString("http")}
}

// LightragDeployment builds the per-Project lightrag Deployment (port 9621,
// Recreate strategy because the data PVC is RWO with one replica).
//
// Probe design (issue #502) - the split between the three is the whole point:
//
//   - startupProbe /health, 5s x 60 = 5m. Gates liveness while lightrag boots
//     (storage init, schema setup, embedding config) so a slow cold start is not
//     killed mid-startup.
//   - livenessProbe /health. Restarts a process that is dead or hung outright.
//     There was NO livenessProbe at all before, so nothing ever restarted
//     lightrag under any circumstances.
//   - readinessProbe /documents/status_counts. Drains the pod from the Service
//     when the DATA PLANE stops working, which a tcpSocket connect and /health
//     both report as healthy.
//
// Liveness deliberately does NOT use the data-plane path. The #502 wedge was
// TRIGGERED by a ~2h CNPG outage, and no failure budget separates "upstream
// Postgres is down" from "this process is permanently wedged". A data-plane
// liveness probe would therefore restart-loop lightrag through every Postgres
// outage - the exact anti-pattern tatara-memory removed from its own startup
// path. Draining endpoints is the right answer to a dead dependency; restarting
// is the right answer to a dead process.
//
// Not fixed here, deliberately: the container still declares no resources, so it
// runs BestEffort. Requests/limits are a capacity decision that needs live
// measurement, and a wrong guess (unschedulable, or OOMKilled at 877MB working
// set) is worse on a re-enable than BestEffort is.
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
						// See lightragDataPlanePath / lightragHealthPath for why these
						// three probes target the paths they do (issue #502).
						StartupProbe: &corev1.Probe{
							ProbeHandler:     corev1.ProbeHandler{HTTPGet: httpGet(lightragHealthPath)},
							PeriodSeconds:    5,
							TimeoutSeconds:   5,
							FailureThreshold: 60,
						},
						LivenessProbe: &corev1.Probe{
							ProbeHandler:     corev1.ProbeHandler{HTTPGet: httpGet(lightragHealthPath)},
							PeriodSeconds:    10,
							TimeoutSeconds:   5,
							FailureThreshold: 6,
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler:     corev1.ProbeHandler{HTTPGet: httpGet(lightragDataPlanePath)},
							PeriodSeconds:    15,
							TimeoutSeconds:   5,
							FailureThreshold: 3,
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
