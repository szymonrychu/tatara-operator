package config_test

import (
	"testing"
	"time"

	"github.com/szymonrychu/tatara-operator/internal/config"
)

func TestLoad(t *testing.T) {
	env := map[string]string{
		"HTTP_ADDR":                   ":8080",
		"METRICS_ADDR":                ":9090",
		"HEALTH_ADDR":                 ":8081",
		"INTERNAL_ADDR":               ":8082",
		"CALLBACK_URL":                "http://tatara-operator-internal.tatara.svc:8082",
		"OIDC_ISSUER":                 "https://kc/realms/tatara",
		"OIDC_AUDIENCE":               "tatara-operator",
		"MEMORY_IMAGE":                "harbor/tatara-memory:0.2.0",
		"LIGHTRAG_IMAGE":              "ghcr.io/hkuds/lightrag:v1.4.16",
		"NEO4J_IMAGE":                 "neo4j:5-community",
		"OPENAI_SECRET_NAME":          "tatara-openai",
		"SEMANTIC_MODEL":              "gpt-4o-mini",
		"INGESTER_IMAGE":              "harbor/ingester:1",
		"EXTERNAL_WEBHOOK_BASE":       "https://ops.example",
		"OPERATOR_OIDC_CLIENT_ID":     "tatara-operator",
		"OPERATOR_OIDC_CLIENT_SECRET": "shh",
		"ANTHROPIC_SECRET_NAME":       "anthropic",
		"CLI_OIDC_SECRET_NAME":        "cli-oidc",
		"IMAGE_PULL_SECRET":           "regcred",
		"LOG_LEVEL":                   "debug",
	}
	for k, v := range env {
		t.Setenv(k, v)
	}

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	tests := []struct {
		name string
		got  string
		want string
	}{
		{"HTTPAddr", cfg.HTTPAddr, ":8080"},
		{"MetricsAddr", cfg.MetricsAddr, ":9090"},
		{"HealthAddr", cfg.HealthAddr, ":8081"},
		{"InternalAddr", cfg.InternalAddr, ":8082"},
		{"CallbackURL", cfg.CallbackURL, "http://tatara-operator-internal.tatara.svc:8082"},
		{"OIDCIssuer", cfg.OIDCIssuer, "https://kc/realms/tatara"},
		{"OIDCAudience", cfg.OIDCAudience, "tatara-operator"},
		{"MemoryImage", cfg.MemoryImage, "harbor/tatara-memory:0.2.0"},
		{"LightragImage", cfg.LightragImage, "ghcr.io/hkuds/lightrag:v1.4.16"},
		{"Neo4jImage", cfg.Neo4jImage, "neo4j:5-community"},
		{"OpenAISecretName", cfg.OpenAISecretName, "tatara-openai"},
		{"SemanticModel", cfg.SemanticModel, "gpt-4o-mini"},
		{"IngesterImage", cfg.IngesterImage, "harbor/ingester:1"},
		{"ExternalWebhookBase", cfg.ExternalWebhookBase, "https://ops.example"},
		{"OperatorOIDCClientID", cfg.OperatorOIDCClientID, "tatara-operator"},
		{"OperatorOIDCClientSecret", cfg.OperatorOIDCClientSecret, "shh"},
		{"AnthropicSecretName", cfg.AnthropicSecretName, "anthropic"},
		{"CLIOIDCSecretName", cfg.CLIOIDCSecretName, "cli-oidc"},
		{"ImagePullSecret", cfg.ImagePullSecret, "regcred"},
		{"LogLevel", cfg.LogLevel, "debug"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("%s = %q, want %q", tt.name, tt.got, tt.want)
			}
		})
	}
}

func TestLoad_MissingRequired(t *testing.T) {
	t.Setenv("OIDC_ISSUER", "")
	t.Setenv("OIDC_AUDIENCE", "")
	if _, err := config.Load(); err == nil {
		t.Fatal("expected error for missing required OIDC_ISSUER/OIDC_AUDIENCE")
	}
}

// TestLoad_Defaults asserts that HealthAddr and InternalAddr have distinct
// defaults so they cannot both bind the same port (which would cause
// "address already in use" at startup).
func TestLoad_SemanticModelDefault(t *testing.T) {
	t.Setenv("OIDC_ISSUER", "https://kc/realms/tatara")
	t.Setenv("OIDC_AUDIENCE", "tatara-operator")
	t.Setenv("SEMANTIC_MODEL", "")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.SemanticModel != "gpt-4o-mini" {
		t.Fatalf("SemanticModel default = %q, want gpt-4o-mini", cfg.SemanticModel)
	}
}

func TestLoad_LeaderElectionDefaultsOn(t *testing.T) {
	t.Setenv("OIDC_ISSUER", "https://kc/realms/tatara")
	t.Setenv("OIDC_AUDIENCE", "tatara-operator")
	t.Setenv("LEADER_ELECTION", "")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.LeaderElection {
		t.Fatal("LeaderElection default = false, want true")
	}
}

func TestLoad_LeaderElectionDisabled(t *testing.T) {
	t.Setenv("OIDC_ISSUER", "https://kc/realms/tatara")
	t.Setenv("OIDC_AUDIENCE", "tatara-operator")
	t.Setenv("LEADER_ELECTION", "false")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.LeaderElection {
		t.Fatal("LeaderElection = true with LEADER_ELECTION=false, want false")
	}
}

func TestLoad_PushMetricsTTLDefault(t *testing.T) {
	t.Setenv("OIDC_ISSUER", "https://kc/realms/tatara")
	t.Setenv("OIDC_AUDIENCE", "tatara-operator")
	t.Setenv("PUSH_METRICS_TTL", "")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.PushMetricsTTL != 5*time.Minute {
		t.Fatalf("PushMetricsTTL default = %v, want 5m", cfg.PushMetricsTTL)
	}
}

func TestLoad_PushMetricsTTLOverride(t *testing.T) {
	t.Setenv("OIDC_ISSUER", "https://kc/realms/tatara")
	t.Setenv("OIDC_AUDIENCE", "tatara-operator")
	t.Setenv("PUSH_METRICS_TTL", "90s")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.PushMetricsTTL != 90*time.Second {
		t.Fatalf("PushMetricsTTL = %v, want 90s", cfg.PushMetricsTTL)
	}
}

func TestLoad_Defaults(t *testing.T) {
	t.Setenv("OIDC_ISSUER", "https://kc/realms/tatara")
	t.Setenv("OIDC_AUDIENCE", "tatara-operator")
	// Do not set HEALTH_ADDR or INTERNAL_ADDR so defaults apply.

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.HealthAddr == cfg.InternalAddr {
		t.Fatalf("HealthAddr (%s) == InternalAddr (%s): they must be distinct to avoid double-bind",
			cfg.HealthAddr, cfg.InternalAddr)
	}
}
