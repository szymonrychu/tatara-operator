package ingest

import (
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
		OIDCClientSecret: "s3cr3t",
		OIDCAudience:     "tatara-memory",
		Namespace:        "tatara",
		ImagePullSecret:  "regcred",
		OpenAISecretName: "tatara-openai",
		SemanticModel:    "gpt-4o-mini",
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
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 2 {
		t.Errorf("backoffLimit = %v, want 2", job.Spec.BackoffLimit)
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
	if !strings.Contains(cloneCmd, "https://github.com/acme/widgets.git") {
		t.Errorf("clone cmd missing url: %q", cloneCmd)
	}
	if !strings.Contains(cloneCmd, "--branch main") {
		t.Errorf("clone cmd missing branch: %q", cloneCmd)
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
	if !strings.Contains(cmd, "tatara-ingest --repo-root /workspace/acme/widgets --repo-name widgets --base-url http://mem-acme.tatara.svc:8080") {
		t.Errorf("ingest cmd wrong: %q", cmd)
	}
	if strings.Contains(cmd, "--since") {
		t.Errorf("full ingest must not pass --since: %q", cmd)
	}
	if !strings.Contains(cmd, "widgets-ingest-result") {
		t.Errorf("ingest cmd must write result configmap: %q", cmd)
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
	if v := envValue(main, "OIDC_CLIENT_SECRET"); v != "s3cr3t" {
		t.Errorf("OIDC_CLIENT_SECRET = %q", v)
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
	if !strings.Contains(cloneCmd, "--branch main") {
		t.Errorf("clone cmd missing branch: %q", cloneCmd)
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

	clone := job.Spec.Template.Spec.InitContainers[0]
	cloneCmd := strings.Join(clone.Command, " ") + " " + strings.Join(clone.Args, " ")
	if !strings.Contains(cloneCmd, wantDir) {
		t.Errorf("clone must target namespace dir %q: %q", wantDir, cloneCmd)
	}

	main := job.Spec.Template.Spec.Containers[0]
	cmd := strings.Join(main.Command, " ") + " " + strings.Join(main.Args, " ")
	if !strings.Contains(cmd, "--repo-root "+wantDir) {
		t.Errorf("ingest cmd must use namespace repo-root %q: %q", wantDir, cmd)
	}
	if !strings.Contains(cmd, "git -C "+wantDir+" rev-parse HEAD") {
		t.Errorf("HEAD resolution must run in namespace dir %q: %q", wantDir, cmd)
	}
}
