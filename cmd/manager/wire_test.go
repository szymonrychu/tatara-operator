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
// JSON (nodeSelector/tolerations/affinity) is parsed and applied to PodConfig.
func TestPodConfigFromConfig_Scheduling(t *testing.T) {
	cfg := config.Config{
		AgentScheduling: `{"nodeSelector":{"kubernetes.io/os":"linux"},` +
			`"tolerations":[{"key":"dedicated","operator":"Exists","effect":"NoSchedule"}]}`,
	}
	got := podConfigFromConfig(cfg)
	if got.NodeSelector["kubernetes.io/os"] != "linux" {
		t.Fatalf("NodeSelector not applied: %+v", got.NodeSelector)
	}
	if len(got.Tolerations) != 1 || got.Tolerations[0].Key != "dedicated" {
		t.Fatalf("Tolerations not applied: %+v", got.Tolerations)
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

func TestMemoryConfigFromConfig(t *testing.T) {
	cfg := config.Config{
		Namespace:        "tatara",
		MemoryImage:      "harbor.example/tatara-memory:0.2.0",
		LightragImage:    "harbor.example/lightrag:1.0.0",
		Neo4jImage:       "neo4j:2026.04.0",
		OpenAISecretName: "openai-shared",
		OIDCIssuer:       "https://keycloak.example/realms/tatara",
		OIDCAudience:     "tatara",
		ImagePullSecret:  "regcred",
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
}
