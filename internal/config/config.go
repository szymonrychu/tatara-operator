package config

import (
	"fmt"
	"os"
)

// Config holds the env-scalar configuration for the operator. Each field is
// populated from an env var injected via the chart ConfigMap/Secret (rule 6).
//
// Four distinct listen addresses:
//   - HealthAddr    (HEALTH_ADDR,   :8081) - manager health/readyz probe bind
//   - InternalAddr  (INTERNAL_ADDR, :8082) - callback server bind (not exposed via ingress)
//   - MetricsAddr   (METRICS_ADDR,  :9090) - Prometheus /metrics bind
//   - HTTPAddr      (HTTP_ADDR,     :8080) - SCM webhook + REST API bind
//
// CallbackURL (CALLBACK_URL) is the full routable in-cluster base URL the
// wrapper Pod POSTs to, e.g. http://tatara-operator-internal.tatara.svc:8082.
// M6 MUST set this to the operator's callback Service DNS and expose
// INTERNAL_ADDR on that Service.
type Config struct {
	HTTPAddr                 string
	MetricsAddr              string
	HealthAddr               string
	InternalAddr             string
	CallbackURL              string
	OIDCIssuer               string
	OIDCAudience             string
	MemoryImage              string
	LightragImage            string
	Neo4jImage               string
	OpenAISecretName         string
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
		HealthAddr:               getDefault("HEALTH_ADDR", ":8081"),
		InternalAddr:             getDefault("INTERNAL_ADDR", ":8082"),
		CallbackURL:              os.Getenv("CALLBACK_URL"),
		OIDCIssuer:               os.Getenv("OIDC_ISSUER"),
		OIDCAudience:             os.Getenv("OIDC_AUDIENCE"),
		MemoryImage:              os.Getenv("MEMORY_IMAGE"),
		LightragImage:            os.Getenv("LIGHTRAG_IMAGE"),
		Neo4jImage:               os.Getenv("NEO4J_IMAGE"),
		OpenAISecretName:         os.Getenv("OPENAI_SECRET_NAME"),
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
