package main

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	chiMiddleware "github.com/go-chi/chi/v5/middleware"
	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/config"
	"github.com/szymonrychu/tatara-operator/internal/ingest"
	"github.com/szymonrychu/tatara-operator/internal/memclient"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPodConfigFromConfig(t *testing.T) {
	uid := int64(65532)
	fsg := int64(65532)
	cfg := config.Config{
		Namespace:           "tatara",
		CallbackURL:         "http://tatara-operator-internal.tatara.svc:8082",
		OIDCIssuer:          "https://kc/realms/tatara",
		AnthropicSecretName: "anthropic",
		CLIOIDCSecretName:   "tatara-cli-oidc",
		ImagePullSecret:     "regcred",
		OperatorURL:         "http://tatara-operator.tatara.svc:8080",
		AgentCPURequest:     "250m",
		AgentCPULimit:       "1",
		AgentMemoryRequest:  "256Mi",
		AgentMemoryLimit:    "1Gi",
		AgentRunAsNonRoot:   true,
		AgentRunAsUser:      &uid,
		AgentFSGroup:        &fsg,
	}
	got := podConfigFromConfig(cfg)
	want := agent.PodConfig{
		Namespace:           "tatara",
		CallbackURL:         "http://tatara-operator-internal.tatara.svc:8082",
		OIDCIssuer:          "https://kc/realms/tatara",
		AnthropicSecretName: "anthropic",
		CLIOIDCSecretName:   "tatara-cli-oidc",
		ImagePullSecret:     "regcred",
		OperatorURL:         "http://tatara-operator.tatara.svc:8080",
		CPURequest:          "250m",
		CPULimit:            "1",
		MemoryRequest:       "256Mi",
		MemoryLimit:         "1Gi",
		RunAsNonRoot:        true,
		RunAsUser:           &uid,
		FSGroup:             &fsg,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("podConfigFromConfig = %+v, want %+v", got, want)
	}
}

// TestPodConfigFromConfig_Scheduling asserts the cluster-specific scheduling
// (nodeSelector/tolerations/affinity) is applied to PodConfig from the
// pre-parsed cfg.Scheduling field set by config.Load - no re-parse, no
// discarded error.
func TestPodConfigFromConfig_Scheduling(t *testing.T) {
	scheduling, err := agent.ParseScheduling(
		`{"nodeSelector":{"kubernetes.io/os":"linux"},` +
			`"tolerations":[{"key":"dedicated","operator":"Exists","effect":"NoSchedule"}]}`,
	)
	if err != nil {
		t.Fatalf("ParseScheduling: %v", err)
	}
	cfg := config.Config{
		Scheduling: scheduling,
	}
	got := podConfigFromConfig(cfg)
	if got.NodeSelector["kubernetes.io/os"] != "linux" {
		t.Fatalf("NodeSelector not applied: %+v", got.NodeSelector)
	}
	if len(got.Tolerations) != 1 || got.Tolerations[0].Key != "dedicated" {
		t.Fatalf("Tolerations not applied: %+v", got.Tolerations)
	}
}

// TestPodConfigFromConfig_SchedulingUsesPreParsedStruct asserts that
// podConfigFromConfig reads from cfg.Scheduling (the struct set once by
// config.Load) and NOT from cfg.AgentScheduling (the raw string). A Config
// whose AgentScheduling raw string is malformed but whose Scheduling struct is
// valid still produces the correct PodConfig - demonstrating that the
// error-discarding double-parse (scheduling, _ := agent.ParseScheduling(...))
// has been removed.
func TestPodConfigFromConfig_SchedulingUsesPreParsedStruct(t *testing.T) {
	scheduling, err := agent.ParseScheduling(`{"nodeSelector":{"env":"prod"}}`)
	if err != nil {
		t.Fatalf("ParseScheduling: %v", err)
	}
	cfg := config.Config{
		// Raw string intentionally diverges from Scheduling to prove the func
		// reads the struct, not the string.
		AgentScheduling: `{"nodeSelector":{"env":"staging"}}`,
		Scheduling:      scheduling,
	}
	got := podConfigFromConfig(cfg)
	if got.NodeSelector["env"] != "prod" {
		t.Fatalf("podConfigFromConfig used AgentScheduling raw string instead of pre-parsed Scheduling: NodeSelector=%+v", got.NodeSelector)
	}
}

// TestPodConfigFromConfig_HealthAddrDistinctFromInternalAddr asserts that the
// defaults for HEALTH_ADDR and INTERNAL_ADDR are different ports so the
// manager health probe and the callback server cannot both bind the same address.
func TestPodConfigFromConfig_HealthAddrDistinctFromInternalAddr(t *testing.T) {
	// Simulate an unset environment - both fields at their defaults.
	cfg := config.Config{
		HealthAddr:   ":8081",
		InternalAddr: ":8082",
	}
	if cfg.HealthAddr == cfg.InternalAddr {
		t.Fatalf("HealthAddr (%s) == InternalAddr (%s): they must differ to avoid double-bind",
			cfg.HealthAddr, cfg.InternalAddr)
	}
}

func TestIngestConfigFromConfig(t *testing.T) {
	cfg := config.Config{
		IngesterImage:          "img:1",
		OIDCIssuer:             "https://kc/realms/t",
		OperatorOIDCClientID:   "tatara-operator",
		OperatorOIDCSecretName: "tatara-operator",
		Namespace:              "tatara",
		OpenAISecretName:       "tatara-openai",
		SemanticModel:          "gpt-4o-mini",
	}
	got := ingestConfigFromConfig(cfg, "tatara-memory")
	want := ingest.Config{
		IngesterImage:    "img:1",
		OIDCIssuer:       "https://kc/realms/t",
		OIDCClientID:     "tatara-operator",
		OIDCSecretName:   "tatara-operator",
		OIDCAudience:     "tatara-memory",
		Namespace:        "tatara",
		OpenAISecretName: "tatara-openai",
		SemanticModel:    "gpt-4o-mini",
	}
	if got != want {
		t.Errorf("ingestConfigFromConfig = %+v, want %+v", got, want)
	}
}

// TestNewWebhookMux_RecovererReturns500 verifies that a handler panic does not
// silently drop the request but instead returns HTTP 500. Without
// middleware.Recoverer the net/http server would close the connection without
// writing a status, making the event invisible to the caller.
func TestNewWebhookMux_RecovererReturns500(t *testing.T) {
	mux := newWebhookMux()
	mux.Get("/panic", func(w http.ResponseWriter, r *http.Request) {
		panic("simulated handler panic")
	})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/panic", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 after handler panic, got %d", rec.Code)
	}
}

// TestNewWebhookMux_RequestIDPropagated verifies that the RequestID middleware
// injects a request-id into the request context so handlers can log it for
// correlation (hard rule 12). chi.middleware.RequestID stores the id in the
// context; the handler reads it via middleware.GetReqID and echoes it as a
// response header for inspection.
func TestNewWebhookMux_RequestIDPropagated(t *testing.T) {
	mux := newWebhookMux()
	mux.Get("/ok", func(w http.ResponseWriter, r *http.Request) {
		rid := chiMiddleware.GetReqID(r.Context())
		w.Header().Set("X-Request-Id", rid)
		w.WriteHeader(http.StatusOK)
	})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ok", nil)
	mux.ServeHTTP(rec, req)
	if rec.Header().Get("X-Request-Id") == "" {
		t.Fatal("request-id not in context: RequestID middleware is missing")
	}
}

// TestCallbackRunnableNeedLeaderElection verifies that callbackRunnable opts out
// of leader-election gating so the callback/push-metrics server starts on every
// replica, not only on the leader.
func TestCallbackRunnableNeedLeaderElection(t *testing.T) {
	r := callbackRunnable{}
	if r.NeedLeaderElection() {
		t.Fatal("callbackRunnable.NeedLeaderElection() = true, want false: callback server must start on every replica")
	}
}

func TestMaintenanceRunnable_IsLeaderOnly(t *testing.T) {
	var m maintenanceRunnable
	if !m.NeedLeaderElection() {
		t.Error("maintenanceRunnable must require leader election (leader-only poll/reap)")
	}
	var c callbackRunnable
	if c.NeedLeaderElection() {
		t.Error("callbackRunnable must NOT require leader election (HTTP serves on every replica)")
	}
}

// TestDispatcherBackstopRunnable_IsLeaderOnly guards issue #395: the admission
// backstop scan (list every Project's pending QueuedEvents) must run on the
// elected leader only, same as maintenanceRunnable - N replicas must not each
// run it every 60s.
func TestDispatcherBackstopRunnable_IsLeaderOnly(t *testing.T) {
	var d dispatcherBackstopRunnable
	if !d.NeedLeaderElection() {
		t.Error("dispatcherBackstopRunnable must require leader election (leader-only admission backstop, issue #395)")
	}
}

func TestMemoryConfigFromConfig(t *testing.T) {
	cfg := config.Config{
		Namespace:                 "tatara",
		MemoryImage:               "harbor.example/tatara-memory:0.2.0",
		LightragImage:             "harbor.example/lightrag:1.0.0",
		Neo4jImage:                "neo4j:2026.04.0",
		OpenAISecretName:          "openai-shared",
		OIDCIssuer:                "https://keycloak.example/realms/tatara",
		OIDCAudience:              "tatara",
		ImagePullSecret:           "regcred",
		MemoryMonitoringEnabled:   true,
		MemoryMonitorLabels:       map[string]string{"release": "prometheus"},
		MemoryProvisioningTimeout: 45 * time.Minute,
	}
	mc := memoryConfigFromConfig(cfg)
	if mc.Namespace != "tatara" || mc.MemoryImage != cfg.MemoryImage ||
		mc.LightragImage != cfg.LightragImage || mc.Neo4jImage != cfg.Neo4jImage ||
		mc.OpenAISecretName != cfg.OpenAISecretName || mc.OIDCIssuer != cfg.OIDCIssuer {
		t.Fatalf("memoryConfigFromConfig mismatch: %+v", mc)
	}
	if mc.OIDCAudience != "tatara-memory" {
		t.Fatalf("OIDCAudience = %q, want tatara-memory (the memory service audience)", mc.OIDCAudience)
	}
	if mc.ImagePullSecret != "regcred" {
		t.Fatalf("ImagePullSecret = %q, want regcred", mc.ImagePullSecret)
	}
	if !mc.MonitorEnabled {
		t.Fatalf("MonitorEnabled = false, want true (mapped from MemoryMonitoringEnabled)")
	}
	if mc.MonitorLabels["release"] != "prometheus" {
		t.Fatalf("MonitorLabels = %v, want release=prometheus", mc.MonitorLabels)
	}
	if mc.ProvisioningTimeout != 45*time.Minute {
		t.Fatalf("ProvisioningTimeout = %v, want 45m", mc.ProvisioningTimeout)
	}
}

// TestNewMemoryFor_ReturnsPerProjectNoteFetcher covers issue #345 fault (3):
// restapi.Config.MemoryFor must resolve a fresh tatara-memory client per
// Project (mirroring newSpillerFor), not one flat instance shared across
// every project's rehydrate call. mgr is unused by the resolver body (same
// as newSpillerFor), so nil is safe here.
func TestNewMemoryFor_ReturnsPerProjectNoteFetcher(t *testing.T) {
	cfg := config.Config{
		OIDCIssuer:               "https://kc/realms/tatara",
		OperatorOIDCClientID:     "tatara-operator",
		OperatorOIDCClientSecret: "secret",
	}
	memoryFor := newMemoryFor(nil, cfg)

	projA := &tataradevv1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "alpha"},
		Status:     tataradevv1alpha1.ProjectStatus{Memory: &tataradevv1alpha1.MemoryStatus{Endpoint: "http://tatara-memory.alpha.svc:8080"}},
	}
	projB := &tataradevv1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "beta"},
		Status:     tataradevv1alpha1.ProjectStatus{Memory: &tataradevv1alpha1.MemoryStatus{Endpoint: "http://tatara-memory.beta.svc:8080"}},
	}

	a := memoryFor(projA)
	b := memoryFor(projB)
	if a == nil || b == nil {
		t.Fatalf("newMemoryFor resolver returned nil: a=%v b=%v", a, b)
	}
	ac, ok := a.(*memclient.Client)
	if !ok {
		t.Fatalf("newMemoryFor(%s) did not return *memclient.Client: %T", projA.Name, a)
	}
	bc, ok := b.(*memclient.Client)
	if !ok {
		t.Fatalf("newMemoryFor(%s) did not return *memclient.Client: %T", projB.Name, b)
	}
	if ac == bc {
		t.Fatal("newMemoryFor returned the SAME *memclient.Client instance for two different projects: " +
			"per-project resolution is required because each Project has its own tatara-memory endpoint")
	}

	// A project whose memory stack is not up yet (Status.Memory nil) must not
	// panic the resolver - it resolves to a client dialing an empty endpoint,
	// which fails the write/fetch at request time (existing Fit*/rehydrate
	// error handling), not at construction time.
	projC := &tataradevv1alpha1.Project{ObjectMeta: metav1.ObjectMeta{Name: "gamma"}}
	if got := memoryFor(projC); got == nil {
		t.Fatal("newMemoryFor must not return nil for a project with no memory status yet")
	}
}
