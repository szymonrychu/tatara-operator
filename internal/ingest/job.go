// Package ingest builds the Kubernetes Job that clones a repository and runs
// tatara-ingest against tatara-memory.
package ingest

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
)

// Config is the subset of operator configuration the Job builder needs.
type Config struct {
	IngesterImage string
	OIDCIssuer    string
	OIDCClientID  string
	// OIDCSecretName is the name of the Secret that holds the OIDC client
	// secret under the key "OPERATOR_OIDC_CLIENT_SECRET". The ingest Job
	// sources OIDC_CLIENT_SECRET via SecretKeyRef rather than embedding the
	// plaintext value in the Job/Pod spec.
	OIDCSecretName   string
	OIDCAudience     string
	Namespace        string
	ImagePullSecret  string
	OpenAISecretName string
	SemanticModel    string
	// CallbackURL is the operator's internal callback base URL (the same value
	// the agent Pod gets as OPERATOR_PUSH_URL's prefix). The ingest Job uses it
	// to build METRICS_PUSH_URL so the short-lived Job can push its Prometheus
	// metrics to the operator's pushmetrics receiver. When empty the ingester
	// runs with metrics push disabled (a no-op).
	CallbackURL string
}

// semanticEnv returns the env vars that drive the ingester's Phase 2 semantic
// extraction stage: the OpenAI key (sourced from the shared OpenAI Secret, same
// secret/key pair lightrag uses), the model, and the per-Repository opt-out.
// The key is omitted when no OpenAI Secret is configured so the ingester falls
// back to AST-only ingest. SEMANTIC_MODEL defaults to gpt-4o-mini.
func semanticEnv(repo *tataradevv1alpha1.Repository, cfg Config) []corev1.EnvVar {
	model := cfg.SemanticModel
	if model == "" {
		model = "gpt-4o-mini"
	}
	env := []corev1.EnvVar{
		{Name: "SEMANTIC_MODEL", Value: model},
		{Name: "SEMANTIC_INGEST", Value: strconv.FormatBool(tataradevv1alpha1.BoolVal(repo.Spec.SemanticIngest, true))},
	}
	if cfg.OpenAISecretName != "" {
		env = append(env, corev1.EnvVar{
			Name: "OPENAI_API_KEY",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: cfg.OpenAISecretName},
					Key:                  "LLM_BINDING_API_KEY",
				},
			},
		})
	}
	return env
}

// metricsPushEnv returns the env that lets the short-lived ingest Job push its
// Prometheus metrics to the operator's pushmetrics receiver. The ingester reads
// METRICS_PUSH_URL and POSTs the gathered text to it verbatim (it does not
// append query parameters), so the full receiver URL - including the required
// run_id and the job label - is built here. run_id is the Job name so the up-to
// BackoffLimit pod retries of one Job overwrite a single run's series instead of
// leaving evicted partials. When CallbackURL is unset the env is omitted and the
// ingester's push is a no-op.
func metricsPushEnv(jobName string, cfg Config) []corev1.EnvVar {
	if cfg.CallbackURL == "" {
		return nil
	}
	q := url.Values{}
	q.Set("run_id", jobName)
	q.Set("job", "tatara-ingest")
	pushURL := strings.TrimSuffix(cfg.CallbackURL, "/") + "/internal/metrics/push?" + q.Encode()
	return []corev1.EnvVar{{Name: "METRICS_PUSH_URL", Value: pushURL}}
}

// imagePullSecrets returns a one-element slice when cfg.ImagePullSecret is set,
// else nil, so the ingest Job can pull the ingester image from a private registry.
func imagePullSecrets(cfg Config) []corev1.LocalObjectReference {
	if cfg.ImagePullSecret == "" {
		return nil
	}
	return []corev1.LocalObjectReference{{Name: cfg.ImagePullSecret}}
}

// ResultConfigMapName returns the name of the ConfigMap an ingest Job patches
// with the resolved HEAD SHA for the given Repository.
func ResultConfigMapName(repo *tataradevv1alpha1.Repository) string {
	return repo.Name + "-ingest-result"
}

const (
	workspaceVolume = "workspace"
	workspaceMount  = "/workspace"

	// ingestHTTPTimeout is passed to the ingester as HTTP_TIMEOUT. The ingester
	// defaults to 60s; during transient LLM-extraction windows (external OpenAI
	// round-trips for semantic ingest) a single /code-graph:bulk call can exceed
	// 60s, causing HTTP 499 client-abort and ingest pod failure. 300s covers the
	// worst observed extraction latency with margin. (2026-06-20 incident)
	ingestHTTPTimeout = "300s"
)

// BuildJob returns the *batchv1.Job that ingests repo for project. When since
// is non-empty the ingest is incremental (--since since); otherwise it is a
// full ingest. The Job is owner-referenced to repo. It clones with the
// Project SCM token in an init container into an emptyDir, runs tatara-ingest
// in the main container, then writes the cloned HEAD SHA into the repo's
// result ConfigMap via the in-cluster API.
func BuildJob(project *tataradevv1alpha1.Project, repo *tataradevv1alpha1.Repository, since, baseURL string, cfg Config) *batchv1.Job {
	// Incremental jobs use BackoffLimit=0: a missing-since-SHA failure (e.g. after
	// a force-push) is deterministic and not retryable at the pod level. Zero retries
	// means the controller reaches its full-ingest fallback after one pod attempt
	// instead of burning 3 pod runs before escalating (hard rule 13, finding 4).
	// Full ingests keep BackoffLimit=2 (transient clone/network failures can self-heal).
	backoff := int32(2)
	if since != "" {
		backoff = 0
	}
	ttl := int32(600)
	controller := true

	// jobName is computed once and reused as both the Job's name and the
	// metrics-push run_id so the BackoffLimit pod retries of one Job overwrite a
	// single run's pushed series.
	jobName := repo.Name + "-ingest-" + rand.String(5)

	// Clone into a directory that mirrors the repo namespace (owner/.../repo),
	// not a flat "/workspace/repo", so concurrent clones never collide.
	// namespacePath may return an empty or host-only string for degenerate
	// URLs; fall back to the Repository name so clones never collide at root.
	nsPath := namespacePath(repo.Spec.URL)
	if nsPath == "" || !strings.Contains(nsPath, "/") {
		nsPath = repo.Name
	}
	repoDir := workspaceMount + "/" + nsPath

	// URL, branch, and repoDir are passed as env vars (GIT_CLONE_URL,
	// GIT_BRANCH, GIT_REPO_DIR) and referenced quoted in the shell command.
	// This prevents shell injection from Repository spec fields regardless
	// of their content. repoDir is derived from the URL by the operator but
	// could carry shell metacharacters if the URL path is malformed.
	// Full-history clone (no --depth): the incremental diff needs <since> in
	// history, and a shallow clone exits 128 when <since> is absent.
	cloneCmd := `set -e; git -c "credential.helper=!f() { echo username=x-access-token; echo password=${SCM_TOKEN}; }; f" ` +
		`clone --branch "$GIT_BRANCH" "$GIT_CLONE_URL" "$GIT_REPO_DIR"`

	ingestArgs := fmt.Sprintf(
		`tatara-ingest --repo-root "$GIT_REPO_DIR" --repo-name %s --base-url %s`,
		repo.Name, baseURL)
	if since != "" {
		ingestArgs += " --since " + since
	}
	// After a successful ingest, resolve HEAD and patch the result ConfigMap
	// via the in-cluster API (the Job ServiceAccount has patch on it).
	resultCM := ResultConfigMapName(repo)
	mainScript := fmt.Sprintf(
		`set -e; %s; `+
			`SHA=$(git -C "$GIT_REPO_DIR" rev-parse HEAD); `+
			`kubectl -n %s patch configmap %s --type merge `+
			`-p "{\"data\":{\"sha\":\"${SHA}\"}}"`,
		ingestArgs, cfg.Namespace, resultCM)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: cfg.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "tatara-operator",
				"app.kubernetes.io/component": "ingest",
				"tatara.dev/managed-by":       "tatara-operator",
				"tatara.dev/repository":       repo.Name,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: tataradevv1alpha1.GroupVersion.String(),
				Kind:       "Repository",
				Name:       repo.Name,
				UID:        repo.UID,
				Controller: &controller,
			}},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoff,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app.kubernetes.io/name":      "tatara-operator",
						"app.kubernetes.io/component": "ingest",
						"tatara.dev/managed-by":       "tatara-operator",
						"tatara.dev/repository":       repo.Name,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy:      corev1.RestartPolicyNever,
					ServiceAccountName: "tatara-ingest",
					ImagePullSecrets:   imagePullSecrets(cfg),
					Volumes: []corev1.Volume{{
						Name:         workspaceVolume,
						VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
					}},
					InitContainers: []corev1.Container{{
						Name:    "clone",
						Image:   cfg.IngesterImage,
						Command: []string{"/bin/sh", "-c"},
						Args:    []string{cloneCmd},
						Env: []corev1.EnvVar{
							{
								Name: "SCM_TOKEN",
								ValueFrom: &corev1.EnvVarSource{
									SecretKeyRef: &corev1.SecretKeySelector{
										LocalObjectReference: corev1.LocalObjectReference{Name: project.Spec.ScmSecretRef},
										Key:                  "token",
									},
								},
							},
							// GIT_CLONE_URL, GIT_BRANCH, and GIT_REPO_DIR are
							// injected as env vars so the shell command
							// references them quoted, preventing injection
							// from Repository spec fields.
							{Name: "GIT_CLONE_URL", Value: repo.Spec.URL},
							{Name: "GIT_BRANCH", Value: repo.Spec.DefaultBranch},
							{Name: "GIT_REPO_DIR", Value: repoDir},
						},
						VolumeMounts: []corev1.VolumeMount{{Name: workspaceVolume, MountPath: workspaceMount}},
					}},
					Containers: []corev1.Container{{
						Name:    "ingest",
						Image:   cfg.IngesterImage,
						Command: []string{"/bin/sh", "-c"},
						Args:    []string{mainScript},
						Env: append([]corev1.EnvVar{
							{Name: "GIT_REPO_DIR", Value: repoDir},
							{Name: "BASE_URL", Value: baseURL},
							{Name: "OIDC_ISSUER", Value: cfg.OIDCIssuer},
							{Name: "OIDC_CLIENT_ID", Value: cfg.OIDCClientID},
							// OIDC_CLIENT_SECRET is sourced via SecretKeyRef so
							// the value is never embedded in the Job/Pod spec
							// stored in etcd or visible in a kubectl get job -o yaml.
							{
								Name: "OIDC_CLIENT_SECRET",
								ValueFrom: &corev1.EnvVarSource{
									SecretKeyRef: &corev1.SecretKeySelector{
										LocalObjectReference: corev1.LocalObjectReference{Name: cfg.OIDCSecretName},
										Key:                  "OPERATOR_OIDC_CLIENT_SECRET",
									},
								},
							},
							{Name: "OIDC_AUDIENCE", Value: cfg.OIDCAudience},
							// HTTP_TIMEOUT overrides the ingester's 60s default; semantic
							// ingest POSTs /code-graph:bulk which can exceed 60s during
							// transient LLM-extraction windows (2026-06-20 incident).
							{Name: "HTTP_TIMEOUT", Value: ingestHTTPTimeout},
						}, append(semanticEnv(repo, cfg), metricsPushEnv(jobName, cfg)...)...),
						VolumeMounts: []corev1.VolumeMount{{Name: workspaceVolume, MountPath: workspaceMount}},
					}},
				},
			},
		},
	}
}
