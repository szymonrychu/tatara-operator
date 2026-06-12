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
	OperatorURL              string
	MemoryImage              string
	LightragImage            string
	Neo4jImage               string
	OpenAISecretName         string
	SemanticModel            string
	IngesterImage            string
	ExternalWebhookBase      string
	OperatorOIDCClientID     string
	OperatorOIDCClientSecret string
	AnthropicSecretName      string
	CLIOIDCSecretName        string
	ImagePullSecret          string
	Namespace                string
	LogLevel                 string
	IngressHost              string
	IngressClassName         string
	MemoryPathPrefix         string
	ChatPathPrefix           string
	ChatImage                string
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
		OperatorURL:              getDefault("OPERATOR_URL", "http://tatara-operator.tatara.svc:8080"),
		MemoryImage:              os.Getenv("MEMORY_IMAGE"),
		LightragImage:            os.Getenv("LIGHTRAG_IMAGE"),
		Neo4jImage:               os.Getenv("NEO4J_IMAGE"),
		OpenAISecretName:         os.Getenv("OPENAI_SECRET_NAME"),
		SemanticModel:            getDefault("SEMANTIC_MODEL", "gpt-4o-mini"),
		IngesterImage:            os.Getenv("INGESTER_IMAGE"),
		ExternalWebhookBase:      os.Getenv("EXTERNAL_WEBHOOK_BASE"),
		OperatorOIDCClientID:     os.Getenv("OPERATOR_OIDC_CLIENT_ID"),
		OperatorOIDCClientSecret: os.Getenv("OPERATOR_OIDC_CLIENT_SECRET"),
		AnthropicSecretName:      os.Getenv("ANTHROPIC_SECRET_NAME"),
		CLIOIDCSecretName:        os.Getenv("CLI_OIDC_SECRET_NAME"),
		ImagePullSecret:          os.Getenv("IMAGE_PULL_SECRET"),
		Namespace:                getDefault("NAMESPACE", "tatara"),
		LogLevel:                 getDefault("LOG_LEVEL", "info"),
		IngressHost:              os.Getenv("INGRESS_HOST"),
		IngressClassName:         getDefault("INGRESS_CLASS_NAME", "nginx"),
		MemoryPathPrefix:         getDefault("MEMORY_PATH_PREFIX", "/api/v1/memory"),
		ChatPathPrefix:           getDefault("CHAT_PATH_PREFIX", "/api/v1/chat"),
		ChatImage:                os.Getenv("CHAT_IMAGE"),
	}
	if cfg.OIDCIssuer == "" {
		return Config{}, fmt.Errorf("config: OIDC_ISSUER is required")
	}
	if cfg.OIDCAudience == "" {
		return Config{}, fmt.Errorf("config: OIDC_AUDIENCE is required")
	}
	return cfg, nil
}
