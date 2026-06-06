package config_test

import (
	"testing"

	"github.com/szymonrychu/tatara-operator/internal/config"
)

func TestLoad(t *testing.T) {
	env := map[string]string{
		"HTTP_ADDR":                   ":8080",
		"METRICS_ADDR":                ":9090",
		"INTERNAL_ADDR":               ":8081",
		"OIDC_ISSUER":                 "https://kc/realms/tatara",
		"OIDC_AUDIENCE":               "tatara-operator",
		"MEMORY_BASE_URL":             "http://tatara-memory:8080",
		"INGESTER_IMAGE":              "harbor/ingester:1",
		"EXTERNAL_WEBHOOK_BASE":       "https://ops.example",
		"OPERATOR_OIDC_CLIENT_ID":     "tatara-operator",
		"OPERATOR_OIDC_CLIENT_SECRET": "shh",
		"ANTHROPIC_SECRET_NAME":       "anthropic",
		"CLI_OIDC_SECRET_NAME":        "cli-oidc",
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
		{"InternalAddr", cfg.InternalAddr, ":8081"},
		{"OIDCIssuer", cfg.OIDCIssuer, "https://kc/realms/tatara"},
		{"OIDCAudience", cfg.OIDCAudience, "tatara-operator"},
		{"MemoryBaseURL", cfg.MemoryBaseURL, "http://tatara-memory:8080"},
		{"IngesterImage", cfg.IngesterImage, "harbor/ingester:1"},
		{"ExternalWebhookBase", cfg.ExternalWebhookBase, "https://ops.example"},
		{"OperatorOIDCClientID", cfg.OperatorOIDCClientID, "tatara-operator"},
		{"OperatorOIDCClientSecret", cfg.OperatorOIDCClientSecret, "shh"},
		{"AnthropicSecretName", cfg.AnthropicSecretName, "anthropic"},
		{"CLIOIDCSecretName", cfg.CLIOIDCSecretName, "cli-oidc"},
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
