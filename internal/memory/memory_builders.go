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

// MemoryDeployment builds the per-Project tatara-memory Deployment (port 8080).
// Non-secret env via the ConfigMap; PG_DSN and OPENAI_API_KEY inline from
// their respective Secrets.
//
// replicas stays 1 and NO PodDisruptionBudget is emitted, deliberately - issue
// #528 asks for both and neither is currently safe to ship:
//
//   - tatara-memory is not multi-replica safe today. Its analytics recompute
//     worker guards concurrency with an in-process sync.Mutex + inflight map and
//     claims work with a bare SELECT over dirty repos, so a second replica
//     recomputes the same repo, collides on code_communities' (repo, community)
//     primary key, never clears dirty=true, and repeats that every 30s forever -
//     while paying a duplicate OpenAI labelling call per community. Its ingest
//     ResetRunningItems is global and unfiltered, so one replica's periodic
//     resume flips the OTHER replica's in-flight items back to pending and they
//     run twice. The per-process DB pool budget also has no notion of replica
//     count, so two replicas double the connection draw against a cluster that
//     has exhausted its slots before. The ingest item claim itself is a correct
//     FOR UPDATE SKIP LOCKED, and job state lives entirely in Postgres, so this
//     is fixable - but it is fixable in tatara-memory, not here.
//   - a PDB is meaningless until then. At replicas=1, minAvailable:1 makes
//     disruptionsAllowed 0 and blocks every node drain permanently, and
//     maxUnavailable:1 permits full disruption and protects nothing. The
//     incident that motivated the ask also had 4 nodes rebooting inside 6.5
//     minutes with no PDB honoured at all, so a correct PDB would not have
//     helped it either.
//
// What IS fixed here is the probe budget - see the timeoutSeconds comment below.
// topologySpreadConstraints and podAntiAffinity are already wired and become
// effective the moment replicas rises.
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
					ImagePullSecrets:          imagePullSecrets(cfg),
					Affinity:                  componentAffinity(p.Name, "memory", cfg),
					TopologySpreadConstraints: topologySpreadConstraints(p.Name, "memory", cfg),
					Containers: []corev1.Container{{
						Name:  "tatara-memory",
						Image: cfg.MemoryImage,
						Ports: []corev1.ContainerPort{
							{Name: "http", ContainerPort: 8080, Protocol: corev1.ProtocolTCP},
						},
						EnvFrom: []corev1.EnvFromSource{
							{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: n.Memory}}},
						},
						Env: []corev1.EnvVar{
							secretEnv("PG_DSN", n.PGAppSecret, "uri"),
							// Wire the shared OpenAI secret so the tatara-memory community
							// labeler (NewOpenAILabelerFromEnv) finds OPENAI_API_KEY and
							// uses LLM labels instead of silently falling back to member names.
							secretEnv("OPENAI_API_KEY", cfg.OpenAISecretName, "LLM_BINDING_API_KEY"),
						},
						// tatara-memory does a multi-stage blocking startup before its
						// HTTP listener (hence /healthz) comes up: waitForDB retries the
						// postgres ping for up to 60s, then OIDC discovery, then schema
						// migrations. That worst-case budget exceeds the liveness probe's
						// ~35s kill window, so without a startupProbe a slow-dependency
						// boot is liveness-killed mid-startup into an unrecoverable
						// CrashLoopBackOff. The startupProbe gives startup 100s
						// (5s * 20) and gates liveness until /healthz first answers 200.
						//
						// Every probe sets timeoutSeconds EXPLICITLY. Leaving it unset
						// means 1s, which is the #528 defect: /readyz pings Postgres and
						// then does a full HTTP round-trip to LightRAG, so a 1s budget
						// cannot cover the work the handler does. After the 2026-08-01
						// node reboot the API pod logged nine consecutive
						// "readyz failed / lightrag: context canceled" at duration_s
						// 1.000008544, one per 10s period - ~95s of NotReady entirely
						// self-inflicted, against a LightRAG that was merely cold-starting
						// and answered fine moments later. `context canceled` (kubelet hung
						// up) rather than `context deadline exceeded` is the proof it was
						// the probe budget and not an app-level deadline. The same 1s wall
						// flaps the pod NotReady on any dependency blip over a second.
						StartupProbe: &corev1.Probe{
							ProbeHandler:     corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromString("http")}},
							PeriodSeconds:    5,
							TimeoutSeconds:   5,
							FailureThreshold: 20,
						},
						LivenessProbe: &corev1.Probe{
							ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromString("http")}},
							InitialDelaySeconds: 5,
							PeriodSeconds:       10,
							TimeoutSeconds:      5,
						},
						// 10s: two dependency round-trips plus margin, still well inside
						// the 10s period so probes never overlap.
						ReadinessProbe: &corev1.Probe{
							ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/readyz", Port: intstr.FromString("http")}},
							InitialDelaySeconds: 5,
							PeriodSeconds:       15,
							TimeoutSeconds:      10,
						},
					}},
				},
			},
		},
	}
}

// MemoryService exposes tatara-memory on 8080 (ClusterIP). It carries the
// app.kubernetes.io/component=memory metadata label so the per-Project
// ServiceMonitor can target only this Service: neo4j and lightrag share the
// pin-set labels and also expose a port named "http", so without the component
// label a ServiceMonitor would also scrape their non-metrics ports.
func MemoryService(p *tatarav1alpha1.Project, cfg Config) *corev1.Service {
	n := NamesFor(p.Name)
	meta := objectMeta(p, cfg, n.Memory)
	meta.Labels["app.kubernetes.io/component"] = "memory"
	return &corev1.Service{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: meta,
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: selectorLabels(p.Name, "memory"),
			Ports: []corev1.ServicePort{
				{Name: "http", Port: 8080, TargetPort: intstr.FromString("http"), Protocol: corev1.ProtocolTCP},
			},
		},
	}
}
