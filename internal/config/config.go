package config

import (
	"encoding/json"
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
	HTTPAddr        string
	MetricsAddr     string
	HealthAddr      string
	InternalAddr    string
	CallbackURL     string
	OIDCIssuer      string
	OIDCAudience    string
	OperatorURL     string
	MemoryImage     string
	LightragImage   string
	GrafanaMCPImage string
	Neo4jImage      string
	// MemoryMonitoringEnabled (MEMORY_MONITORING_ENABLED, default true) gates the
	// ServiceMonitor + PrometheusRule the operator emits per provisioned memory
	// stack. Set false on a cluster without the prometheus-operator CRDs.
	MemoryMonitoringEnabled bool
	// MemoryMonitorLabels (MEMORY_MONITOR_LABELS, JSON object, default empty) are
	// extra labels stamped on that ServiceMonitor + PrometheusRule so the cluster
	// Prometheus serviceMonitorSelector / ruleSelector match them. Empty keeps the
	// chart cluster-agnostic (rule 14); the deploying helmfile sets the label.
	MemoryMonitorLabels      map[string]string
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
	// IdlePodReapAfter is how long an agent pod may sit with no live turn before
	// the reaper deletes it as a leaked wrapper (issue #237). Loaded from
	// IDLE_POD_REAP_MINUTES as an integer minute count. Defaults to
	// DefaultIdlePodReap and is clamped up to MinIdlePodReap when positive; a
	// zero/negative value disables the idle backstop.
	IdlePodReapAfter time.Duration
	// MemoryProvisioningTimeout bounds how long a project's memory stack may
	// stay in phase Provisioning before reconcileMemory flips it to Degraded
	// (issue #355: a wedged backend sat Provisioning for 7h+ with no bounded
	// failure signal). Loaded from MEMORY_PROVISIONING_TIMEOUT_MINUTES as an
	// integer minute count. Defaults to DefaultMemoryProvisioningTimeout and is
	// clamped up to MinMemoryProvisioningTimeout when positive; a zero/negative
	// value disables the timeout (Provisioning stays unbounded, pre-#355
	// behaviour).
	MemoryProvisioningTimeout time.Duration
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

	// Claude account usage poller (claudeSubscription mode). UsageAuthMode
	// selects the /api/oauth/usage auth header ("bearer" default or
	// "x-api-key", per the Task 0 spike). UsagePollInterval is clamped up to
	// the Poller's 180s floor. UsageUserAgent is sent as the request's
	// User-Agent. UsageBaseURL overrides the Anthropic API base (empty =
	// production default). UsageEnabled activates the poller: off by default so
	// the operator ships inert until the Task 0 auth spike confirms the header;
	// when off, the claudeSubscription gate reads an empty store (fail-open).
	UsageEnabled      bool
	UsageAuthMode     string
	UsagePollInterval time.Duration
	UsageUserAgent    string
	UsageBaseURL      string
	// UsageSecretName, when set, switches the poller's token source to a
	// self-refreshing OAuth source that reads an access+refresh+expiry bundle
	// from that Secret (keys oauth-access-token/oauth-refresh-token/
	// oauth-expires-at) and refreshes it via the OAuth token endpoint, persisting
	// the rotated bundle back. This is REQUIRED for a real deploy: the shared
	// fleet setup-token lacks the user:profile scope /api/oauth/usage needs, so
	// the poller must run on an interactive-login token (which expires ~hourly).
	// When empty, the poller reads the static "oauth-token" key from
	// AnthropicSecretName (back-compat; only works with a user:profile token).
	// UsageOAuthClientID and UsageTokenURL are the (undocumented, reverse-
	// engineered) Claude Code OAuth client id + refresh endpoint - kept as config
	// so a provider change is a helmfile flip, not a code change.
	UsageSecretName    string
	UsageOAuthClientID string
	UsageTokenURL      string
	UsageRefreshMargin time.Duration

	// IncidentDedupVolatileLabels overrides the per-series label denylist that is
	// stripped from the incident dedup key (webhook.defaultVolatileDenylist).
	// Empty (nil) means use the operator default. From INCIDENT_DEDUP_VOLATILE_LABELS
	// (CSV).
	IncidentDedupVolatileLabels []string
	// IncidentRefireCommentCooldown rate-limits the coalesced refire comment on an
	// open incident tracker. From INCIDENT_REFIRE_COMMENT_COOLDOWN_MINUTES.
	IncidentRefireCommentCooldown time.Duration
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

// DefaultIdlePodReap is the default age past which the reaper deletes an agent
// pod that holds no live turn (issue #237). A wrapper whose turn timed out or
// completed parks idle in epoll waiting for the next PTY input; if the operator
// never submits a next turn or tears it down, the pod leaks a Task slot + node
// resources for hours. 30m comfortably exceeds any healthy between-turn gap, so
// only a wedged pod is ever reaped. The bundle IS the continuation state, so a
// still-live Task re-spawns a fresh pod and resumes: reaping is safe.
const DefaultIdlePodReap = 30 * time.Minute

// MinIdlePodReap is the hard lower bound on IdlePodReapAfter. It keeps a
// misconfigured short value from reaping a pod in the brief window between a
// turn completing and the reconcile submitting the next turn. A positive
// IdlePodReapAfter below this floor is clamped up to it in Load; zero disables
// the idle backstop entirely.
const MinIdlePodReap = 5 * time.Minute

// DefaultMemoryProvisioningTimeout is the default bound on how long a memory
// stack may stay Provisioning before reconcileMemory reports it Degraded
// (issue #355). 45m comfortably exceeds a normal cnpg/neo4j/lightrag cold
// start while still catching a wedged backend far sooner than the 7h+ the
// live incident ran unbounded.
const DefaultMemoryProvisioningTimeout = 45 * time.Minute

// MinMemoryProvisioningTimeout is the hard lower bound on
// MemoryProvisioningTimeout. It keeps a misconfigured short value from
// flipping a stack to Degraded during an ordinary provisioning run. A
// positive MemoryProvisioningTimeout below this floor is clamped up to it in
// Load; zero disables the bound entirely.
const MinMemoryProvisioningTimeout = 5 * time.Minute

// DefaultIncidentRefireCooldown is how long the operator waits between coalesced
// refire comments on one open incident tracker (A4). The recurrence counter
// still increments on every suppressed refire; only the COMMENT is rate-limited.
const DefaultIncidentRefireCooldown = 30 * time.Minute

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
// getMinutesDefault parses key as an integer number of minutes into a Duration,
// returning def when unset and an error when present but not a valid integer.
func getMinutesDefault(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q is not a valid integer minutes: %w", key, v, err)
	}
	return time.Duration(n) * time.Minute, nil
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

// getJSONStringMap parses key as a JSON object of string->string, returning nil
// when unset or "{}" and an error when present but not a valid JSON object (so a
// malformed label map fails startup loudly rather than silently dropping the
// cluster's ruleSelector label).
func getJSONStringMap(key string) (map[string]string, error) {
	v := os.Getenv(key)
	if v == "" || v == "{}" {
		return nil, nil
	}
	m := map[string]string{}
	if err := json.Unmarshal([]byte(v), &m); err != nil {
		return nil, fmt.Errorf("config: %s=%q is not a valid JSON object: %w", key, v, err)
	}
	return m, nil
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
	memoryMonitoringEnabled, err := getBoolDefault("MEMORY_MONITORING_ENABLED", true)
	if err != nil {
		return Config{}, err
	}
	memoryMonitorLabels, err := getJSONStringMap("MEMORY_MONITOR_LABELS")
	if err != nil {
		return Config{}, err
	}
	pushMetricsTTL, err := getDurationDefault("PUSH_METRICS_TTL", 5*time.Minute)
	if err != nil {
		return Config{}, err
	}
	idlePodReapAfter, err := getMinutesDefault("IDLE_POD_REAP_MINUTES", DefaultIdlePodReap)
	if err != nil {
		return Config{}, err
	}
	// Clamp up a positive value to the floor so a short misconfiguration cannot
	// reap a pod in the brief between-turns window (see MinIdlePodReap). A
	// zero/negative value disables the idle backstop and is left as-is.
	if idlePodReapAfter > 0 && idlePodReapAfter < MinIdlePodReap {
		idlePodReapAfter = MinIdlePodReap
	}
	memoryProvisioningTimeout, err := getMinutesDefault("MEMORY_PROVISIONING_TIMEOUT_MINUTES", DefaultMemoryProvisioningTimeout)
	if err != nil {
		return Config{}, err
	}
	// Clamp up a positive value to the floor for the same reason as
	// idlePodReapAfter: a short misconfiguration must not flip a healthy,
	// still-provisioning stack to Degraded. Zero/negative disables the bound.
	if memoryProvisioningTimeout > 0 && memoryProvisioningTimeout < MinMemoryProvisioningTimeout {
		memoryProvisioningTimeout = MinMemoryProvisioningTimeout
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
	tokenBudgetEnabled, err := getBoolDefault("TOKEN_BUDGET_ENABLED", false)
	if err != nil {
		return Config{}, err
	}
	usageEnabled, err := getBoolDefault("USAGE_ENABLED", false)
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
	usagePollInterval, err := getDurationDefault("USAGE_POLL_INTERVAL", 180*time.Second)
	if err != nil {
		return Config{}, err
	}
	usageRefreshMargin, err := getDurationDefault("USAGE_REFRESH_MARGIN", 5*time.Minute)
	if err != nil {
		return Config{}, err
	}
	incidentRefireCooldown, err := getMinutesDefault("INCIDENT_REFIRE_COMMENT_COOLDOWN_MINUTES", DefaultIncidentRefireCooldown)
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
		LeaderElection:             leaderElection,
		MemoryMonitoringEnabled:    memoryMonitoringEnabled,
		MemoryMonitorLabels:        memoryMonitorLabels,
		PushMetricsTTL:             pushMetricsTTL,
		PushMetricsAllowedPrefixes: getCSVList("PUSH_METRICS_ALLOWED_PREFIXES"),
		IdlePodReapAfter:           idlePodReapAfter,
		MemoryProvisioningTimeout:  memoryProvisioningTimeout,
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

		UsageEnabled:      usageEnabled,
		UsageAuthMode:     getDefault("USAGE_AUTH_MODE", "bearer"),
		UsagePollInterval: usagePollInterval,
		UsageUserAgent:    getDefault("USAGE_USER_AGENT", "claude-code/1.0.0"),
		UsageBaseURL:      os.Getenv("USAGE_BASE_URL"),
		UsageSecretName:   os.Getenv("USAGE_SECRET_NAME"),
		// Public Claude Code OAuth client id (embedded in the CLI, not a secret).
		UsageOAuthClientID: getDefault("USAGE_OAUTH_CLIENT_ID", "9d1c250a-e61b-44d9-88ed-5944d1962f5e"), // gitleaks:allow
		UsageTokenURL:      getDefault("USAGE_TOKEN_URL", "https://platform.claude.com/v1/oauth/token"),
		UsageRefreshMargin: usageRefreshMargin,

		IncidentDedupVolatileLabels:   getCSVList("INCIDENT_DEDUP_VOLATILE_LABELS"),
		IncidentRefireCommentCooldown: incidentRefireCooldown,
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
