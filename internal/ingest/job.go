// Package ingest builds the Kubernetes Job that clones a repository and runs
// tatara-ingest against tatara-memory.
package ingest

import (
	"fmt"
	"strconv"

	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
)

// Config is the subset of operator configuration the Job builder needs.
type Config struct {
	IngesterImage    string
	OIDCIssuer       string
	OIDCClientID     string
	OIDCClientSecret string
	OIDCAudience     string
	Namespace        string
	ImagePullSecret  string
	OpenAISecretName string
	SemanticModel    string
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
)

// BuildJob returns the *batchv1.Job that ingests repo for project. When since
// is non-empty the ingest is incremental (--since since); otherwise it is a
// full ingest. The Job is owner-referenced to repo. It clones with the
// Project SCM token in an init container into an emptyDir, runs tatara-ingest
// in the main container, then writes the cloned HEAD SHA into the repo's
// result ConfigMap via the in-cluster API.
func BuildJob(project *tataradevv1alpha1.Project, repo *tataradevv1alpha1.Repository, since, baseURL string, cfg Config) *batchv1.Job {
	backoff := int32(2)
	ttl := int32(600)
	controller := true

	// Clone into a directory that mirrors the repo namespace (owner/.../repo),
	// not a flat "/workspace/repo", so concurrent clones never collide.
	repoDir := workspaceMount + "/" + namespacePath(repo.Spec.URL)

	// Use git credential helper to inject SCM_TOKEN without embedding it in
	// the URL string. The full URL appears literally in the command so tests
	// can assert on it; the token is supplied via SecretKeyRef env var.
	// Full-history clone (no --depth): the incremental diff needs <since> in
	// history, and a shallow clone exits 128 when <since> is absent.
	cloneCmd := fmt.Sprintf(
		`set -e; git -c "credential.helper=!f() { echo username=x-access-token; echo password=${SCM_TOKEN}; }; f" `+
			`clone --branch %s %s %s`,
		repo.Spec.DefaultBranch, repo.Spec.URL, repoDir)

	ingestArgs := fmt.Sprintf(
		"tatara-ingest --repo-root %s --repo-name %s --base-url %s",
		repoDir, repo.Name, baseURL)
	if since != "" {
		ingestArgs += " --since " + since
	}
	// After a successful ingest, resolve HEAD and patch the result ConfigMap
	// via the in-cluster API (the Job ServiceAccount has patch on it).
	resultCM := ResultConfigMapName(repo)
	mainScript := fmt.Sprintf(
		"set -e; %s; "+
			"SHA=$(git -C %s rev-parse HEAD); "+
			"kubectl -n %s patch configmap %s --type merge "+
			"-p \"{\\\"data\\\":{\\\"sha\\\":\\\"${SHA}\\\"}}\"",
		ingestArgs, repoDir, cfg.Namespace, resultCM)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      repo.Name + "-ingest-" + rand.String(5),
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
						},
						VolumeMounts: []corev1.VolumeMount{{Name: workspaceVolume, MountPath: workspaceMount}},
					}},
					Containers: []corev1.Container{{
						Name:    "ingest",
						Image:   cfg.IngesterImage,
						Command: []string{"/bin/sh", "-c"},
						Args:    []string{mainScript},
						Env: append([]corev1.EnvVar{
							{Name: "BASE_URL", Value: baseURL},
							{Name: "OIDC_ISSUER", Value: cfg.OIDCIssuer},
							{Name: "OIDC_CLIENT_ID", Value: cfg.OIDCClientID},
							{Name: "OIDC_CLIENT_SECRET", Value: cfg.OIDCClientSecret},
							{Name: "OIDC_AUDIENCE", Value: cfg.OIDCAudience},
						}, semanticEnv(repo, cfg)...),
						VolumeMounts: []corev1.VolumeMount{{Name: workspaceVolume, MountPath: workspaceMount}},
					}},
				},
			},
		},
	}
}
