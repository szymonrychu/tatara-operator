package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/szymonrychu/tatara-operator/internal/agent"
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
	// OperatorOIDCSecretName is the name of the Kubernetes Secret that holds
	// OPERATOR_OIDC_CLIENT_SECRET. Ingest Jobs source the OIDC client secret
	// via SecretKeyRef from this Secret rather than embedding the plaintext
	// value in the Job spec.
	OperatorOIDCSecretName string
	AnthropicSecretName    string
	CLIOIDCSecretName      string
	ImagePullSecret        string
	// Agent wrapper Pod resource + securityContext scalars (rule 6: camelCase
	// chart value -> SCREAMING_SNAKE ConfigMap key -> manager via envFrom). Empty
	// resource strings mean no constraint. AgentRunAsUser/AgentFSGroup are
	// pointers so "unset" is distinguishable from UID/GID 0.
	AgentCPURequest    string
	AgentCPULimit      string
	AgentMemoryRequest string
	AgentMemoryLimit   string
	AgentRunAsNonRoot  bool
	AgentRunAsUser     *int64
	AgentFSGroup       *int64
	// AgentScheduling is the cluster-specific Pod-placement JSON document
	// (nodeSelector/tolerations/affinity) for spawned agent Pods. Delivered as a
	// single ConfigMap key (rule 6 list-shaped data), kept empty in the chart so
	// the chart stays cluster-agnostic (rule 14). Validated at load.
	AgentScheduling  string
	Namespace        string
	LogLevel         string
	IngressHost      string
	IngressClassName string
	// IngressRewriteTarget feeds the per-Project memory Ingress's
	// nginx.ingress.kubernetes.io/rewrite-target annotation. Empty (default) =
	// annotation omitted (cluster-agnostic, rule 14).
	IngressRewriteTarget string
	MemoryPathPrefix     string
	ChatPathPrefix       string
	ChatImage            string
	// LeaderElection guards against two managers reconciling concurrently
	// during a rolling-update surge. Defaults on; set LEADER_ELECTION=false
	// for envtest/local single-process runs.
	LeaderElection bool
	// PushMetricsTTL is how long a short-lived pod's pushed metric series are
	// re-exposed on the operator's /metrics after the pod's last push, before
	// they age out. Backstop for pods that exit without best-effort cleanup.
	PushMetricsTTL time.Duration
}

func getDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getDurationDefault(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q is not a valid duration: %w", key, v, err)
	}
	return d, nil
}

func getBoolDefault(key string, def bool) (bool, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("config: %s=%q is not a valid bool: %w", key, v, err)
	}
	return b, nil
}

// getInt64Ptr parses key into a *int64, returning nil when unset and an error
// when present but not a valid integer (so a typo fails startup loudly).
func getInt64Ptr(key string) (*int64, error) {
	v := os.Getenv(key)
	if v == "" {
		return nil, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("config: %s=%q is not a valid integer: %w", key, v, err)
	}
	return &n, nil
}

// Load reads the operator configuration from the environment, applying
// defaults for the listener addresses and log level. OIDC issuer and
// audience are required.
func Load() (Config, error) {
	leaderElection, err := getBoolDefault("LEADER_ELECTION", true)
	if err != nil {
		return Config{}, err
	}
	pushMetricsTTL, err := getDurationDefault("PUSH_METRICS_TTL", 5*time.Minute)
	if err != nil {
		return Config{}, err
	}
	agentRunAsNonRoot, err := getBoolDefault("AGENT_RUN_AS_NON_ROOT", false)
	if err != nil {
		return Config{}, err
	}
	agentRunAsUser, err := getInt64Ptr("AGENT_RUN_AS_USER")
	if err != nil {
		return Config{}, err
	}
	agentFSGroup, err := getInt64Ptr("AGENT_FS_GROUP")
	if err != nil {
		return Config{}, err
	}
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
		OperatorOIDCSecretName:   os.Getenv("OPERATOR_OIDC_SECRET_NAME"),
		AnthropicSecretName:      os.Getenv("ANTHROPIC_SECRET_NAME"),
		CLIOIDCSecretName:        os.Getenv("CLI_OIDC_SECRET_NAME"),
		ImagePullSecret:          os.Getenv("IMAGE_PULL_SECRET"),
		AgentCPURequest:          os.Getenv("AGENT_CPU_REQUEST"),
		AgentCPULimit:            os.Getenv("AGENT_CPU_LIMIT"),
		AgentMemoryRequest:       os.Getenv("AGENT_MEMORY_REQUEST"),
		AgentMemoryLimit:         os.Getenv("AGENT_MEMORY_LIMIT"),
		AgentRunAsNonRoot:        agentRunAsNonRoot,
		AgentRunAsUser:           agentRunAsUser,
		AgentFSGroup:             agentFSGroup,
		AgentScheduling:          os.Getenv("AGENT_SCHEDULING"),
		Namespace:                getDefault("NAMESPACE", "tatara"),
		LogLevel:                 getDefault("LOG_LEVEL", "info"),
		IngressHost:              os.Getenv("INGRESS_HOST"),
		IngressClassName:         os.Getenv("INGRESS_CLASS_NAME"),
		IngressRewriteTarget:     os.Getenv("INGRESS_REWRITE_TARGET"),
		MemoryPathPrefix:         getDefault("MEMORY_PATH_PREFIX", "/api/v1/memory"),
		ChatPathPrefix:           getDefault("CHAT_PATH_PREFIX", "/api/v1/chat"),
		ChatImage:                os.Getenv("CHAT_IMAGE"),
		LeaderElection:           leaderElection,
		PushMetricsTTL:           pushMetricsTTL,
	}
	if cfg.OIDCIssuer == "" {
		return Config{}, fmt.Errorf("config: OIDC_ISSUER is required")
	}
	if cfg.OIDCAudience == "" {
		return Config{}, fmt.Errorf("config: OIDC_AUDIENCE is required")
	}
	// The ingest Job sources OIDC_CLIENT_SECRET via SecretKeyRef{name:
	// OperatorOIDCSecretName}. An empty name renders an invalid secretKeyRef that
	// the API rejects (or resolves to a missing Secret), so require it like the
	// other secret-name inputs.
	if cfg.OperatorOIDCSecretName == "" {
		return Config{}, fmt.Errorf("config: OPERATOR_OIDC_SECRET_NAME is required")
	}
	// Validate the cluster-specific scheduling JSON at load so a malformed
	// document fails startup loudly instead of silently dropping placement.
	if _, err := agent.ParseScheduling(cfg.AgentScheduling); err != nil {
		return Config{}, fmt.Errorf("config: AGENT_SCHEDULING: %w", err)
	}
	return cfg, nil
}
