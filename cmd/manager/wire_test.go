package main

import (
	"testing"

	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/config"
	"github.com/szymonrychu/tatara-operator/internal/ingest"
)

func TestPodConfigFromConfig(t *testing.T) {
	cfg := config.Config{
		Namespace:           "tatara",
		CallbackURL:         "http://tatara-operator-internal.tatara.svc:8082",
		AnthropicSecretName: "anthropic",
		CLIOIDCSecretName:   "tatara-cli-oidc",
	}
	got := podConfigFromConfig(cfg)
	want := agent.PodConfig{
		Namespace:           "tatara",
		CallbackURL:         "http://tatara-operator-internal.tatara.svc:8082",
		AnthropicSecretName: "anthropic",
		CLIOIDCSecretName:   "tatara-cli-oidc",
	}
	if got != want {
		t.Errorf("podConfigFromConfig = %+v, want %+v", got, want)
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
		IngesterImage:            "img:1",
		OIDCIssuer:               "https://kc/realms/t",
		OperatorOIDCClientID:     "tatara-operator",
		OperatorOIDCClientSecret: "secret",
		Namespace:                "tatara",
	}
	got := ingestConfigFromConfig(cfg, "tatara-memory")
	// MemoryBaseURL is no longer sourced from global config; it stays empty
	// here and is set per-Project in N3 (project.Status.Memory.Endpoint).
	want := ingest.Config{
		IngesterImage:    "img:1",
		OIDCIssuer:       "https://kc/realms/t",
		OIDCClientID:     "tatara-operator",
		OIDCClientSecret: "secret",
		OIDCAudience:     "tatara-memory",
		Namespace:        "tatara",
	}
	if got != want {
		t.Errorf("ingestConfigFromConfig = %+v, want %+v", got, want)
	}
}

func TestMemoryConfigFromConfig(t *testing.T) {
	cfg := config.Config{
		Namespace:        "tatara",
		MemoryImage:      "harbor.example/tatara-memory:0.2.0",
		LightragImage:    "harbor.example/lightrag:1.0.0",
		Neo4jImage:       "neo4j:5-community",
		OpenAISecretName: "openai-shared",
		OIDCIssuer:       "https://keycloak.example/realms/tatara",
		OIDCAudience:     "tatara",
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
}
