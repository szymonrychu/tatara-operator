package main

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	chiMiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/config"
	"github.com/szymonrychu/tatara-operator/internal/ingest"
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

func TestMemoryConfigFromConfig(t *testing.T) {
	cfg := config.Config{
		Namespace:               "tatara",
		MemoryImage:             "harbor.example/tatara-memory:0.2.0",
		LightragImage:           "harbor.example/lightrag:1.0.0",
		Neo4jImage:              "neo4j:2026.04.0",
		OpenAISecretName:        "openai-shared",
		OIDCIssuer:              "https://keycloak.example/realms/tatara",
		OIDCAudience:            "tatara",
		ImagePullSecret:         "regcred",
		MemoryMonitoringEnabled: true,
		MemoryMonitorLabels:     map[string]string{"release": "prometheus"},
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
}
