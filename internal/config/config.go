package config

import (
	"fmt"
	"os"
)

// Config holds the env-scalar configuration for the operator. Each field is
// populated from an env var injected via the chart ConfigMap/Secret (rule 6).
type Config struct {
	HTTPAddr                 string
	MetricsAddr              string
	InternalAddr             string
	OIDCIssuer               string
	OIDCAudience             string
	MemoryBaseURL            string
	IngesterImage            string
	ExternalWebhookBase      string
	OperatorOIDCClientID     string
	OperatorOIDCClientSecret string
	AnthropicSecretName      string
	CLIOIDCSecretName        string
	Namespace                string
	LogLevel                 string
}

func getDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Load reads the operator configuration from the environment, applying
// defaults for the listener addresses and log level. OIDC issuer and
// audience are required.
func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:                 getDefault("HTTP_ADDR", ":8080"),
		MetricsAddr:              getDefault("METRICS_ADDR", ":9090"),
		InternalAddr:             getDefault("INTERNAL_ADDR", ":8081"),
		OIDCIssuer:               os.Getenv("OIDC_ISSUER"),
		OIDCAudience:             os.Getenv("OIDC_AUDIENCE"),
		MemoryBaseURL:            os.Getenv("MEMORY_BASE_URL"),
		IngesterImage:            os.Getenv("INGESTER_IMAGE"),
		ExternalWebhookBase:      os.Getenv("EXTERNAL_WEBHOOK_BASE"),
		OperatorOIDCClientID:     os.Getenv("OPERATOR_OIDC_CLIENT_ID"),
		OperatorOIDCClientSecret: os.Getenv("OPERATOR_OIDC_CLIENT_SECRET"),
		AnthropicSecretName:      os.Getenv("ANTHROPIC_SECRET_NAME"),
		CLIOIDCSecretName:        os.Getenv("CLI_OIDC_SECRET_NAME"),
		Namespace:                getDefault("NAMESPACE", "tatara"),
		LogLevel:                 getDefault("LOG_LEVEL", "info"),
	}
	if cfg.OIDCIssuer == "" {
		return Config{}, fmt.Errorf("config: OIDC_ISSUER is required")
	}
	if cfg.OIDCAudience == "" {
		return Config{}, fmt.Errorf("config: OIDC_AUDIENCE is required")
	}
	return cfg, nil
}
