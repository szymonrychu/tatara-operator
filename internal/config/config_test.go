package config_test

import (
	"testing"

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
		"OPERATOR_OIDC_SECRET_NAME":   "tatara-operator",
		"AGENT_CPU_REQUEST":           "250m",
		"AGENT_CPU_LIMIT":             "1",
		"AGENT_MEMORY_REQUEST":        "256Mi",
		"AGENT_MEMORY_LIMIT":          "1Gi",
		"AGENT_RUN_AS_NON_ROOT":       "true",
		"AGENT_RUN_AS_USER":           "65532",
		"AGENT_FS_GROUP":              "65532",
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
		{"OperatorOIDCSecretName", cfg.OperatorOIDCSecretName, "tatara-operator"},
		{"AgentCPURequest", cfg.AgentCPURequest, "250m"},
		{"AgentCPULimit", cfg.AgentCPULimit, "1"},
		{"AgentMemoryRequest", cfg.AgentMemoryRequest, "256Mi"},
		{"AgentMemoryLimit", cfg.AgentMemoryLimit, "1Gi"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("%s = %q, want %q", tt.name, tt.got, tt.want)
			}
		})
	}
	if !cfg.AgentRunAsNonRoot {
		t.Fatal("AgentRunAsNonRoot = false, want true")
	}
	if cfg.AgentRunAsUser == nil || *cfg.AgentRunAsUser != 65532 {
		t.Fatalf("AgentRunAsUser = %v, want 65532", cfg.AgentRunAsUser)
	}
	if cfg.AgentFSGroup == nil || *cfg.AgentFSGroup != 65532 {
		t.Fatalf("AgentFSGroup = %v, want 65532", cfg.AgentFSGroup)
	}
}

// TestLoad_RequiresOperatorOIDCSecretName asserts Load fails fast when
// OPERATOR_OIDC_SECRET_NAME is empty: the ingest Job's secretKeyRef.name would
// otherwise render empty and resolve to a missing Secret (residual #2).
func TestLoad_RequiresOperatorOIDCSecretName(t *testing.T) {
	t.Setenv("OIDC_ISSUER", "https://kc/realms/tatara")
	t.Setenv("OIDC_AUDIENCE", "tatara-operator")
	t.Setenv("OPERATOR_OIDC_SECRET_NAME", "")
	_, err := config.Load()
	if err == nil {
		t.Fatal("expected error for empty OPERATOR_OIDC_SECRET_NAME, got nil")
	}
}

// TestLoad_AgentSchedulingDefaultEmpty asserts the scheduling document defaults
// to empty (chart stays cluster-agnostic, rule 14).
func TestLoad_AgentSchedulingDefaultEmpty(t *testing.T) {
	t.Setenv("OIDC_ISSUER", "https://kc/realms/tatara")
	t.Setenv("OIDC_AUDIENCE", "tatara-operator")
	t.Setenv("OPERATOR_OIDC_SECRET_NAME", "tatara-operator")
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.AgentScheduling != "" {
		t.Fatalf("AgentScheduling default = %q, want empty", cfg.AgentScheduling)
	}
}

// TestLoad_AgentSchedulingMalformed asserts Load fails fast when AGENT_SCHEDULING
// is not valid scheduling JSON, rather than silently dropping placement.
func TestLoad_AgentSchedulingMalformed(t *testing.T) {
	t.Setenv("OIDC_ISSUER", "https://kc/realms/tatara")
	t.Setenv("OIDC_AUDIENCE", "tatara-operator")
	t.Setenv("OPERATOR_OIDC_SECRET_NAME", "tatara-operator")
	t.Setenv("AGENT_SCHEDULING", `{"nodeSelector": [bad`)
	if _, err := config.Load(); err == nil {
		t.Fatal("expected error for malformed AGENT_SCHEDULING, got nil")
	}
}

// TestLoad_AgentRunAsUserMalformed asserts a non-integer AGENT_RUN_AS_USER fails
// fast at load.
func TestLoad_AgentRunAsUserMalformed(t *testing.T) {
	t.Setenv("OIDC_ISSUER", "https://kc/realms/tatara")
	t.Setenv("OIDC_AUDIENCE", "tatara-operator")
	t.Setenv("OPERATOR_OIDC_SECRET_NAME", "tatara-operator")
	t.Setenv("AGENT_RUN_AS_USER", "root")
	if _, err := config.Load(); err == nil {
		t.Fatal("expected error for malformed AGENT_RUN_AS_USER, got nil")
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
	t.Setenv("OPERATOR_OIDC_SECRET_NAME", "tatara-operator")
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
	t.Setenv("OPERATOR_OIDC_SECRET_NAME", "tatara-operator")
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
	t.Setenv("OPERATOR_OIDC_SECRET_NAME", "tatara-operator")
	t.Setenv("LEADER_ELECTION", "false")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.LeaderElection {
		t.Fatal("LeaderElection = true with LEADER_ELECTION=false, want false")
	}
}

func TestLoad_Defaults(t *testing.T) {
	t.Setenv("OIDC_ISSUER", "https://kc/realms/tatara")
	t.Setenv("OIDC_AUDIENCE", "tatara-operator")
	t.Setenv("OPERATOR_OIDC_SECRET_NAME", "tatara-operator")
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

// TestLoad_IngressClassNameDefaultEmpty asserts that when INGRESS_CLASS_NAME is
// unset the field defaults to "" (let K8s use the cluster default IngressClass)
// rather than hard-coding "nginx", which would violate hard rule 14.
func TestLoad_IngressClassNameDefaultEmpty(t *testing.T) {
	t.Setenv("OIDC_ISSUER", "https://kc/realms/tatara")
	t.Setenv("OIDC_AUDIENCE", "tatara-operator")
	t.Setenv("OPERATOR_OIDC_SECRET_NAME", "tatara-operator")
	t.Setenv("INGRESS_CLASS_NAME", "")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.IngressClassName != "" {
		t.Fatalf("IngressClassName default = %q, want \"\" (cluster default)", cfg.IngressClassName)
	}
}

// TestLoad_MalformedLeaderElection asserts that a non-bool LEADER_ELECTION value
// causes Load() to return an error rather than silently falling back to the default.
func TestLoad_MalformedLeaderElection(t *testing.T) {
	t.Setenv("OIDC_ISSUER", "https://kc/realms/tatara")
	t.Setenv("OIDC_AUDIENCE", "tatara-operator")
	t.Setenv("LEADER_ELECTION", "yes") // not a valid bool

	if _, err := config.Load(); err == nil {
		t.Fatal("expected error for malformed LEADER_ELECTION=yes, got nil")
	}
}

// TestLoad_MalformedPushMetricsTTL asserts that an invalid PUSH_METRICS_TTL value
// causes Load() to return an error rather than silently falling back to the default.
func TestLoad_MalformedPushMetricsTTL(t *testing.T) {
	t.Setenv("OIDC_ISSUER", "https://kc/realms/tatara")
	t.Setenv("OIDC_AUDIENCE", "tatara-operator")
	t.Setenv("PUSH_METRICS_TTL", "5minutes") // not a valid duration

	if _, err := config.Load(); err == nil {
		t.Fatal("expected error for malformed PUSH_METRICS_TTL=5minutes, got nil")
	}
}

// TestLoad_AgentRunAsNonRootDefaultFalse asserts that when AGENT_RUN_AS_NON_ROOT is
// unset the Go default is false, matching the chart values.yaml default (agentRunAsNonRoot:
// false). The two must agree: a divergence means running the binary outside the chart
// yields the opposite securityContext from what the chart intends.
func TestLoad_AgentRunAsNonRootDefaultFalse(t *testing.T) {
	t.Setenv("OIDC_ISSUER", "https://kc/realms/tatara")
	t.Setenv("OIDC_AUDIENCE", "tatara-operator")
	t.Setenv("OPERATOR_OIDC_SECRET_NAME", "tatara-operator")
	// AGENT_RUN_AS_NON_ROOT intentionally not set.

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.AgentRunAsNonRoot {
		t.Fatal("AgentRunAsNonRoot default = true, want false (must match chart agentRunAsNonRoot: false)")
	}
}
