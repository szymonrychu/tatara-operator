package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/budget"
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
	GrafanaMCPImage          string
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
	// S3 conversation persistence (issue #114). Empty S3Bucket disables the
	// feature: BuildPod injects no S3 env, so wrapper pods behave as before.
	// S3SecretName holds the AWS creds (AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY);
	// empty means the wrapper uses the default credential chain (e.g. IRSA).
	S3Endpoint       string
	S3Bucket         string
	S3Region         string
	S3KeyPrefix      string
	S3ForcePathStyle bool
	S3SecretName     string
	// S3ConversationRetention is the grace after a conversation's whole batch
	// (brainstorm + sibling issues) goes terminal before the reaper deletes its
	// S3 objects. Kept well under TaskRetention so conversations GC before their
	// Tasks (which carry the keys) are reaped. Zero disables conversation GC.
	S3ConversationRetention time.Duration
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
	// the chart stays cluster-agnostic (rule 14). Validated and parsed at load;
	// callers consume Scheduling directly rather than re-parsing the raw string.
	AgentScheduling string
	// Scheduling is the result of parsing AgentScheduling. Populated by Load so
	// callers never need to parse (and cannot silently discard a parse error).
	Scheduling       agent.Scheduling
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
	// PushMetricsAllowedPrefixes is the metric-name prefix allowlist the
	// push-receiver enforces (PUSH_METRICS_ALLOWED_PREFIXES, comma-separated).
	// Nil/empty leaves the receiver on pushmetrics.DefaultAllowedPrefixes; the
	// chart default carries the real names the ephemeral producers push (ccw_,
	// tatara_wrapper_ from the wrapper; ingest_,analyzer_,semantic_,scip_,llm_
	// from the ingester - issue #129). Widen it as new pushed families appear so
	// they reach Prometheus without a receiver code change.
	PushMetricsAllowedPrefixes []string
	// TaskRetention is how long a terminal (Succeeded/Failed/Done/Stopped/Parked)
	// Task is kept before the reaper garbage-collects it (its Subtasks cascade via
	// owner reference). Loaded from TASK_RETENTION_HOURS as an integer hour count.
	// Defaults to DefaultTaskRetention and is clamped up to MinTaskRetention so GC
	// can never delete a Task that still anchors a dedup/cooldown window.
	TaskRetention time.Duration
	// SerenaURL, when non-empty, is the in-cluster URL of the Serena
	// code-intelligence MCP server. Read from TATARA_SERENA_URL. Empty by default
	// (Phase 1: code path wired, no server deployed). Passed through to PodConfig
	// so BuildPod can inject it as TATARA_SERENA_URL into agent pods.
	SerenaURL string

	// CallbackHMACSecret, when non-empty, activates HMAC-SHA256 verification on
	// the /internal/turn-complete callback endpoint. Set from
	// CALLBACK_HMAC_SECRET (the operator reads the raw value via SecretKeyRef so
	// the controller can verify inbound signatures). Empty = no verification
	// (backward-compatible default).
	CallbackHMACSecret string
	// CallbackHMACSecretName is the name of the Secret holding the callback HMAC
	// shared secret under the key callback-hmac-secret. Set from
	// CALLBACK_HMAC_SECRET_NAME. Wrapper Pods reference this Secret via
	// SecretKeyRef (NOT a literal env value) so the secret never appears in a
	// Pod spec / etcd object in plaintext, matching every other agent secret
	// (anthropic, scm, cli-oidc). Empty = HMAC injection disabled (finding 1/r3).
	CallbackHMACSecretName string

	// Token-budget admission gate operator-wide defaults (issue #189). These set
	// the fleet-wide baseline; a Project's spec.tokenBudget overrides them per
	// project. Off by default (TokenBudgetEnabled=false) so the gate is inert
	// until explicitly enabled. camelCase chart value -> SCREAMING_SNAKE ConfigMap
	// key -> consumed here via envFrom (rule 6). Read out as a budget.Config via
	// BudgetDefaults().
	TokenBudgetEnabled          bool
	TokenBudgetMode             string
	TokenBudgetProactivePercent int
	TokenBudgetEmergencyPercent int
	TokenBudgetResetSchedule    string
	TokenBudgetWindowDuration   time.Duration
	TokenBudgetTokenLimit       int64
}

// BudgetDefaults returns the operator-wide token-budget configuration as a
// budget.Config. A Project with no spec.tokenBudget inherits this verbatim; a
// Project that sets the block overrides these per project (see
// Project.BudgetConfig).
func (c Config) BudgetDefaults() budget.Config {
	return budget.Config{
		Enabled:          c.TokenBudgetEnabled,
		Mode:             budget.Mode(c.TokenBudgetMode),
		ProactivePercent: c.TokenBudgetProactivePercent,
		EmergencyPercent: c.TokenBudgetEmergencyPercent,
		ResetSchedule:    c.TokenBudgetResetSchedule,
		WindowDuration:   c.TokenBudgetWindowDuration,
		TokenLimit:       c.TokenBudgetTokenLimit,
	}
}

// DefaultTaskRetention is the default age after which a terminal Task is
// garbage-collected. A week comfortably exceeds every dedup/cooldown window the
// loop relies on (the longest is the 1h incident-alert cooldown) and preserves a
// week of Task history for human debugging.
const DefaultTaskRetention = 168 * time.Hour

// DefaultConversationRetention is the default grace after a conversation's batch
// fully closes before the reaper deletes its S3 objects (issue #114 decision 5).
// Well under DefaultTaskRetention so conversations GC before their Tasks.
const DefaultConversationRetention = 72 * time.Hour

// MinTaskRetention is the hard lower bound on TaskRetention. Terminal-Task GC
// must never delete a Task that still anchors a dedup/cooldown window; the
// longest such window is the 1h incident-alert cooldown (internal/webhook). A
// 2h floor keeps GC clear of that window even if an operator configures an
// aggressively short retention. TaskRetention is clamped up to this in Load.
const MinTaskRetention = 2 * time.Hour

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

// getHoursDefault parses key as an integer number of hours into a Duration,
// returning def when unset and an error when present but not a valid integer.
func getHoursDefault(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q is not a valid integer hours: %w", key, v, err)
	}
	return time.Duration(n) * time.Hour, nil
}

// getCSVList parses key as a comma-separated list, trimming whitespace and
// dropping empty entries. Returns nil when unset or all-empty so callers fall
// back to their own default.
func getCSVList(key string) []string {
	v := os.Getenv(key)
	if v == "" {
		return nil
	}
	var out []string
	for _, part := range strings.Split(v, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
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

// getIntDefault parses key as an int, returning def when unset and an error when
// present but not a valid integer.
func getIntDefault(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q is not a valid integer: %w", key, v, err)
	}
	return n, nil
}

// getInt64Default parses key as an int64, returning def when unset and an error
// when present but not a valid integer.
func getInt64Default(key string, def int64) (int64, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q is not a valid integer: %w", key, v, err)
	}
	return n, nil
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
	taskRetention, err := getHoursDefault("TASK_RETENTION_HOURS", DefaultTaskRetention)
	if err != nil {
		return Config{}, err
	}
	// Clamp up to the floor: GC must never delete a Task that still anchors a
	// dedup/cooldown window (see MinTaskRetention).
	if taskRetention < MinTaskRetention {
		taskRetention = MinTaskRetention
	}
	agentRunAsNonRoot, err := getBoolDefault("AGENT_RUN_AS_NON_ROOT", false)
	if err != nil {
		return Config{}, err
	}
	conversationRetention, err := getHoursDefault("S3_CONVERSATION_RETENTION_HOURS", DefaultConversationRetention)
	if err != nil {
		return Config{}, err
	}
	s3ForcePathStyle, err := getBoolDefault("S3_FORCE_PATH_STYLE", false)
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
	tokenBudgetEnabled, err := getBoolDefault("TOKEN_BUDGET_ENABLED", false)
	if err != nil {
		return Config{}, err
	}
	tokenBudgetProactive, err := getIntDefault("TOKEN_BUDGET_PROACTIVE_PERCENT", budget.DefaultProactivePercent)
	if err != nil {
		return Config{}, err
	}
	tokenBudgetEmergency, err := getIntDefault("TOKEN_BUDGET_EMERGENCY_PERCENT", budget.DefaultEmergencyPercent)
	if err != nil {
		return Config{}, err
	}
	tokenBudgetWindow, err := getDurationDefault("TOKEN_BUDGET_WINDOW", 0)
	if err != nil {
		return Config{}, err
	}
	tokenBudgetLimit, err := getInt64Default("TOKEN_BUDGET_TOKEN_LIMIT", 0)
	if err != nil {
		return Config{}, err
	}
	cfg := Config{
		HTTPAddr:                   getDefault("HTTP_ADDR", ":8080"),
		MetricsAddr:                getDefault("METRICS_ADDR", ":9090"),
		HealthAddr:                 getDefault("HEALTH_ADDR", ":8081"),
		InternalAddr:               getDefault("INTERNAL_ADDR", ":8082"),
		CallbackURL:                os.Getenv("CALLBACK_URL"),
		OIDCIssuer:                 os.Getenv("OIDC_ISSUER"),
		OIDCAudience:               os.Getenv("OIDC_AUDIENCE"),
		OperatorURL:                getDefault("OPERATOR_URL", "http://tatara-operator.tatara.svc:8080"),
		MemoryImage:                os.Getenv("MEMORY_IMAGE"),
		LightragImage:              os.Getenv("LIGHTRAG_IMAGE"),
		GrafanaMCPImage:            os.Getenv("GRAFANA_MCP_IMAGE"),
		Neo4jImage:                 os.Getenv("NEO4J_IMAGE"),
		OpenAISecretName:           os.Getenv("OPENAI_SECRET_NAME"),
		SemanticModel:              getDefault("SEMANTIC_MODEL", "gpt-4o-mini"),
		IngesterImage:              os.Getenv("INGESTER_IMAGE"),
		ExternalWebhookBase:        os.Getenv("EXTERNAL_WEBHOOK_BASE"),
		OperatorOIDCClientID:       os.Getenv("OPERATOR_OIDC_CLIENT_ID"),
		OperatorOIDCClientSecret:   os.Getenv("OPERATOR_OIDC_CLIENT_SECRET"),
		OperatorOIDCSecretName:     os.Getenv("OPERATOR_OIDC_SECRET_NAME"),
		AnthropicSecretName:        os.Getenv("ANTHROPIC_SECRET_NAME"),
		CLIOIDCSecretName:          os.Getenv("CLI_OIDC_SECRET_NAME"),
		ImagePullSecret:            os.Getenv("IMAGE_PULL_SECRET"),
		S3Endpoint:                 os.Getenv("S3_ENDPOINT"),
		S3Bucket:                   os.Getenv("S3_BUCKET"),
		S3Region:                   os.Getenv("S3_REGION"),
		S3KeyPrefix:                os.Getenv("S3_KEY_PREFIX"),
		S3ForcePathStyle:           s3ForcePathStyle,
		S3SecretName:               os.Getenv("S3_SECRET_NAME"),
		S3ConversationRetention:    conversationRetention,
		AgentCPURequest:            os.Getenv("AGENT_CPU_REQUEST"),
		AgentCPULimit:              os.Getenv("AGENT_CPU_LIMIT"),
		AgentMemoryRequest:         os.Getenv("AGENT_MEMORY_REQUEST"),
		AgentMemoryLimit:           os.Getenv("AGENT_MEMORY_LIMIT"),
		AgentRunAsNonRoot:          agentRunAsNonRoot,
		AgentRunAsUser:             agentRunAsUser,
		AgentFSGroup:               agentFSGroup,
		AgentScheduling:            os.Getenv("AGENT_SCHEDULING"),
		Namespace:                  getDefault("NAMESPACE", "tatara"),
		LogLevel:                   getDefault("LOG_LEVEL", "info"),
		IngressHost:                os.Getenv("INGRESS_HOST"),
		IngressClassName:           os.Getenv("INGRESS_CLASS_NAME"),
		IngressRewriteTarget:       os.Getenv("INGRESS_REWRITE_TARGET"),
		MemoryPathPrefix:           getDefault("MEMORY_PATH_PREFIX", "/api/v1/memory"),
		ChatPathPrefix:             getDefault("CHAT_PATH_PREFIX", "/api/v1/chat"),
		ChatImage:                  os.Getenv("CHAT_IMAGE"),
		LeaderElection:             leaderElection,
		PushMetricsTTL:             pushMetricsTTL,
		PushMetricsAllowedPrefixes: getCSVList("PUSH_METRICS_ALLOWED_PREFIXES"),
		TaskRetention:              taskRetention,
		CallbackHMACSecret:         os.Getenv("CALLBACK_HMAC_SECRET"),
		CallbackHMACSecretName:     os.Getenv("CALLBACK_HMAC_SECRET_NAME"),
		SerenaURL:                  os.Getenv("TATARA_SERENA_URL"),

		TokenBudgetEnabled:          tokenBudgetEnabled,
		TokenBudgetMode:             getDefault("TOKEN_BUDGET_MODE", string(budget.ModeCustomWindow)),
		TokenBudgetProactivePercent: tokenBudgetProactive,
		TokenBudgetEmergencyPercent: tokenBudgetEmergency,
		TokenBudgetResetSchedule:    os.Getenv("TOKEN_BUDGET_RESET_SCHEDULE"),
		TokenBudgetWindowDuration:   tokenBudgetWindow,
		TokenBudgetTokenLimit:       tokenBudgetLimit,
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
	// Parse the cluster-specific scheduling JSON once at load. Storing the
	// parsed struct on Config means callers never re-parse the raw string and
	// cannot silently discard a parse error.
	scheduling, err := agent.ParseScheduling(cfg.AgentScheduling)
	if err != nil {
		return Config{}, fmt.Errorf("config: AGENT_SCHEDULING: %w", err)
	}
	cfg.Scheduling = scheduling
	// Fail fast on a bad operator-wide token-budget default so a misconfigured
	// fleet baseline surfaces at startup rather than silently disabling the gate.
	// Validate is a no-op when the budget is disabled (issue #189).
	if err := cfg.BudgetDefaults().Validate(); err != nil {
		return Config{}, fmt.Errorf("config: TOKEN_BUDGET: %w", err)
	}
	return cfg, nil
}
