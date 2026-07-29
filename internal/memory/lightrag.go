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

// lightragProbePath is the endpoint all three lightrag probes hit. It is
// deliberately NOT /health.
//
// /health is served straight off the asyncio event loop from in-process config
// and returns 200 without touching any backend at all. That is exactly how the
// incident stayed invisible for eight hours: a CNPG switchover killed lightrag's
// in-flight asyncpg connections, its pool-close path timed out after 5s leaving
// the pool lock held, and every uvicorn worker thread then blocked on that futex
// permanently, with no PostgreSQL connection left open. The main thread kept
// accepting sockets and answering /health 200 throughout, so a TCPSocket
// readiness check (the probe this replaces) and an httpGet /health check would
// both have passed while insert_text, query and track_status were at 100% error
// with zero successes.
//
// GET /documents/status_counts is the only endpoint that is simultaneously GET
// (kubelet cannot issue POST), cheap, and Postgres-backed. It is already the one
// lightrag route this operator calls (fetchLightragDocCounts), so it is proven
// to work in-cluster with no auth header. Upstream v1.4.16 routes it to
// PGDocStatusStorage.get_all_status_counts, a single workspace-scoped
// "SELECT status, COUNT(*) ... GROUP BY status" awaited on the shared pool, and
// wraps failures in HTTPException(500). Crucially, upstream's ClientManager
// hands every PG storage class (KV, vector, doc-status) the same process-wide
// asyncpg pool, so a wedge in any of them is visible here. That shared pool is
// what makes this probe meaningful rather than decorative.
//
// Known blind spot: kubelet counts CONSECUTIVE failures, so a partial wedge in
// which one unblocked worker occasionally answers defeats this (and any other)
// probe. The incident's wedge was total, which is why this works. Related, the
// design assumes a single uvicorn process: lightragEnv sets no WORKERS, so every
// probe hits the same event loop. Setting WORKERS>1 would make a per-worker
// wedge probabilistic and silently disable liveness detection.
const lightragProbePath = "/documents/status_counts"

// lightragProbe builds one data-path probe. Kubelet suspends both liveness and
// readiness until the startup probe first succeeds, so none of them set
// InitialDelaySeconds - it would be dead config.
func lightragProbe(periodSeconds, timeoutSeconds, failureThreshold int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
			Path: lightragProbePath,
			Port: intstr.FromString("http"),
		}},
		PeriodSeconds:    periodSeconds,
		TimeoutSeconds:   timeoutSeconds,
		FailureThreshold: failureThreshold,
	}
}

// LightragDeployment builds the per-Project lightrag Deployment (port 9621,
// Recreate strategy because the data PVC is RWO with one replica).
func LightragDeployment(p *tatarav1alpha1.Project, cfg Config) *appsv1.Deployment {
	n := NamesFor(p.Name)
	replicas := int32(1)
	livenessGrace := int64(15)
	liveness := lightragProbe(30, 10, 10)
	liveness.TerminationGracePeriodSeconds = &livenessGrace
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
						// Boot budget 300s (10s * 30). The initContainer has already
						// waited out Neo4j, so this only has to cover lightrag's own
						// ~180s upstream Postgres boot-retry plus first-boot schema and
						// index creation. Its real job is to decouple the boot budget
						// from the wedge-detection budget below: without it, liveness
						// would have to serve both, and anyone later tightening liveness
						// would make a slow dependency-bound boot liveness-killable
						// without noticing the connection. Same rationale as the
						// startupProbe on the tatara-memory container.
						StartupProbe: lightragProbe(10, 5, 30),
						// Wedge budget 300s (30s * 10). This is the number the incident
						// turns on, so both bounds are deliberate.
						//
						// Lower bound: it must ride out every legitimate window in which
						// the whole data path fails at once. A planned CNPG switchover is
						// seconds, an unplanned failover 30-120s, and a rolling restart
						// or node drain relocating the primary up to ~3 min. 300s clears
						// the worst of those with margin.
						//
						// Upper bound: it must be small enough that a permanent wedge
						// self-heals. The incident cost 2.5 HOURS of total data-plane
						// failure AFTER Postgres had fully recovered, ending only when a
						// human ran rollout restart, because nothing could ever restart
						// the pod.
						//
						// A restart during a genuinely long Postgres outage is close to
						// free: KV, vector and doc-status storage are all PG-backed, so
						// with Postgres down nothing is making progress and there is no
						// in-flight work to lose. The resulting CrashLoopBackOff is loud,
						// standard and alertable, and recovers within one backoff cycle
						// once Postgres returns - strictly better than a silent wedge
						// reporting Ready with 0 restarts.
						//
						// timeoutSeconds 10 leaves headroom for asyncpg pool-acquire
						// contention under MAX_PARALLEL_INSERT=8; a single slow probe is
						// plausible during a large ingest, but ten consecutive ones
						// spanning five continuous minutes with no success in between is
						// not, since any success resets the counter.
						//
						// terminationGracePeriodSeconds 15 because a wedged process
						// cannot complete uvicorn's graceful shutdown: without the
						// probe-level override, SIGTERM hangs and kubelet waits the full
						// default 30s pod grace period before SIGKILL on every kill.
						LivenessProbe: liveness,
						// Shed budget 60s (10s * 6), deliberately ordered to fire well
						// before liveness: shed traffic first, kill only if it persists.
						//
						// With one replica there is no second endpoint to shed to, so
						// this probe's real effects are that callers fail fast instead of
						// hanging (tatara-memory's lightrag client burned 60s and 121s
						// per call awaiting headers that never came) and that the Project
						// memory phase, which gates on this Deployment's availableReplicas,
						// finally reflects data-path health. That gate is what released
						// seven repo ingests into the dead backend during the incident.
						//
						// 60s rather than 30s because a Ready->NotReady->Ready round trip
						// costs roughly four minutes of ingest gating via the 3-minute
						// MemoryStablyReady window, and a routine CNPG failover should not
						// pay that. 60s rides one out while still firing five times sooner
						// than liveness.
						ReadinessProbe: lightragProbe(10, 5, 6),
						VolumeMounts:   []corev1.VolumeMount{{Name: "data", MountPath: "/app/data"}},
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
