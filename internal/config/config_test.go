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

// TestLoad_MemoryMonitoringDefaults asserts the per-Project memory monitoring
// defaults: emission on, no cluster-selector labels (chart stays
// cluster-agnostic, rule 14).
func TestLoad_MemoryMonitoringDefaults(t *testing.T) {
	t.Setenv("OIDC_ISSUER", "https://kc/realms/tatara")
	t.Setenv("OIDC_AUDIENCE", "tatara-operator")
	t.Setenv("OPERATOR_OIDC_SECRET_NAME", "tatara-operator")
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.MemoryMonitoringEnabled {
		t.Fatal("MemoryMonitoringEnabled must default true")
	}
	if len(cfg.MemoryMonitorLabels) != 0 {
		t.Fatalf("MemoryMonitorLabels must default empty, got %v", cfg.MemoryMonitorLabels)
	}
}

func TestLoad_IncidentCorrelationAndEscalationDefaults(t *testing.T) {
	t.Setenv("OIDC_ISSUER", "https://kc/realms/tatara")
	t.Setenv("OIDC_AUDIENCE", "tatara-operator")
	t.Setenv("OPERATOR_OIDC_SECRET_NAME", "tatara-operator")
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.IncidentCorrelationLabels; len(got) != 2 || got[0] != "namespace" || got[1] != "cluster" {
		t.Fatalf("IncidentCorrelationLabels default = %v, want [namespace cluster]", got)
	}
	if cfg.IncidentEscalateRefireThreshold != config.DefaultIncidentEscalateRefires {
		t.Fatalf("IncidentEscalateRefireThreshold = %d, want %d",
			cfg.IncidentEscalateRefireThreshold, config.DefaultIncidentEscalateRefires)
	}
	if cfg.IncidentEscalateStaleAge != config.DefaultIncidentEscalateStaleAge {
		t.Fatalf("IncidentEscalateStaleAge = %v, want %v",
			cfg.IncidentEscalateStaleAge, config.DefaultIncidentEscalateStaleAge)
	}
}

func TestLoad_IncidentCorrelationLabelsOverride(t *testing.T) {
	t.Setenv("OIDC_ISSUER", "https://kc/realms/tatara")
	t.Setenv("OIDC_AUDIENCE", "tatara-operator")
	t.Setenv("OPERATOR_OIDC_SECRET_NAME", "tatara-operator")
	t.Setenv("INCIDENT_CORRELATION_LABELS", "namespace,service,team")
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.IncidentCorrelationLabels; len(got) != 3 || got[1] != "service" {
		t.Fatalf("IncidentCorrelationLabels override = %v, want [namespace service team]", got)
	}
}

// TestLoad_IdlePodReapAfter covers the issue #237 idle-backstop knob: the
// default, an explicit override, the min-floor clamp for a short positive value,
// and disabling via zero.
func TestLoad_IdlePodReapAfter(t *testing.T) {
	base := func(t *testing.T) {
		t.Setenv("OIDC_ISSUER", "https://kc/realms/tatara")
		t.Setenv("OIDC_AUDIENCE", "tatara-operator")
		t.Setenv("OPERATOR_OIDC_SECRET_NAME", "tatara-operator")
	}
	t.Run("default", func(t *testing.T) {
		base(t)
		cfg, err := config.Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.IdlePodReapAfter != config.DefaultIdlePodReap {
			t.Fatalf("IdlePodReapAfter = %v, want default %v", cfg.IdlePodReapAfter, config.DefaultIdlePodReap)
		}
	})
	t.Run("explicit", func(t *testing.T) {
		base(t)
		t.Setenv("IDLE_POD_REAP_MINUTES", "45")
		cfg, err := config.Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.IdlePodReapAfter != 45*time.Minute {
			t.Fatalf("IdlePodReapAfter = %v, want 45m", cfg.IdlePodReapAfter)
		}
	})
	t.Run("clamped to floor", func(t *testing.T) {
		base(t)
		t.Setenv("IDLE_POD_REAP_MINUTES", "1")
		cfg, err := config.Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.IdlePodReapAfter != config.MinIdlePodReap {
			t.Fatalf("IdlePodReapAfter = %v, want clamped %v", cfg.IdlePodReapAfter, config.MinIdlePodReap)
		}
	})
	t.Run("disabled by zero", func(t *testing.T) {
		base(t)
		t.Setenv("IDLE_POD_REAP_MINUTES", "0")
		cfg, err := config.Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.IdlePodReapAfter != 0 {
			t.Fatalf("IdlePodReapAfter = %v, want 0 (disabled)", cfg.IdlePodReapAfter)
		}
	})
	t.Run("invalid", func(t *testing.T) {
		base(t)
		t.Setenv("IDLE_POD_REAP_MINUTES", "notanumber")
		if _, err := config.Load(); err == nil {
			t.Fatal("expected error for non-integer IDLE_POD_REAP_MINUTES, got nil")
		}
	})
}

// TestLoad_MemoryProvisioningTimeout covers the issue #355 provisioning-bound
// knob: the default, an explicit override, the min-floor clamp for a short
// positive value, and disabling via zero.
func TestLoad_MemoryProvisioningTimeout(t *testing.T) {
	base := func(t *testing.T) {
		t.Setenv("OIDC_ISSUER", "https://kc/realms/tatara")
		t.Setenv("OIDC_AUDIENCE", "tatara-operator")
		t.Setenv("OPERATOR_OIDC_SECRET_NAME", "tatara-operator")
	}
	t.Run("default", func(t *testing.T) {
		base(t)
		cfg, err := config.Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.MemoryProvisioningTimeout != config.DefaultMemoryProvisioningTimeout {
			t.Fatalf("MemoryProvisioningTimeout = %v, want default %v", cfg.MemoryProvisioningTimeout, config.DefaultMemoryProvisioningTimeout)
		}
	})
	t.Run("explicit", func(t *testing.T) {
		base(t)
		t.Setenv("MEMORY_PROVISIONING_TIMEOUT_MINUTES", "90")
		cfg, err := config.Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.MemoryProvisioningTimeout != 90*time.Minute {
			t.Fatalf("MemoryProvisioningTimeout = %v, want 90m", cfg.MemoryProvisioningTimeout)
		}
	})
	t.Run("clamped to floor", func(t *testing.T) {
		base(t)
		t.Setenv("MEMORY_PROVISIONING_TIMEOUT_MINUTES", "1")
		cfg, err := config.Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.MemoryProvisioningTimeout != config.MinMemoryProvisioningTimeout {
			t.Fatalf("MemoryProvisioningTimeout = %v, want clamped %v", cfg.MemoryProvisioningTimeout, config.MinMemoryProvisioningTimeout)
		}
	})
	t.Run("disabled by zero", func(t *testing.T) {
		base(t)
		t.Setenv("MEMORY_PROVISIONING_TIMEOUT_MINUTES", "0")
		cfg, err := config.Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.MemoryProvisioningTimeout != 0 {
			t.Fatalf("MemoryProvisioningTimeout = %v, want 0 (disabled)", cfg.MemoryProvisioningTimeout)
		}
	})
	t.Run("invalid", func(t *testing.T) {
		base(t)
		t.Setenv("MEMORY_PROVISIONING_TIMEOUT_MINUTES", "notanumber")
		if _, err := config.Load(); err == nil {
			t.Fatal("expected error for non-integer MEMORY_PROVISIONING_TIMEOUT_MINUTES, got nil")
		}
	})
}

// TestLoad_MemoryMonitorLabelsFromEnv asserts the JSON-object label map parses,
// and that the enable flag can be turned off.
func TestLoad_MemoryMonitorLabelsFromEnv(t *testing.T) {
	t.Setenv("OIDC_ISSUER", "https://kc/realms/tatara")
	t.Setenv("OIDC_AUDIENCE", "tatara-operator")
	t.Setenv("OPERATOR_OIDC_SECRET_NAME", "tatara-operator")
	t.Setenv("MEMORY_MONITORING_ENABLED", "false")
	t.Setenv("MEMORY_MONITOR_LABELS", `{"release":"prometheus"}`)
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MemoryMonitoringEnabled {
		t.Fatal("MemoryMonitoringEnabled = true, want false from env")
	}
	if cfg.MemoryMonitorLabels["release"] != "prometheus" {
		t.Fatalf("MemoryMonitorLabels = %v, want release=prometheus", cfg.MemoryMonitorLabels)
	}
}

// TestLoad_MemoryMonitorLabelsInvalid asserts a malformed JSON object fails
// startup loudly rather than silently dropping the cluster's ruleSelector label.
func TestLoad_MemoryMonitorLabelsInvalid(t *testing.T) {
	t.Setenv("OIDC_ISSUER", "https://kc/realms/tatara")
	t.Setenv("OIDC_AUDIENCE", "tatara-operator")
	t.Setenv("OPERATOR_OIDC_SECRET_NAME", "tatara-operator")
	t.Setenv("MEMORY_MONITOR_LABELS", `not-json`)
	if _, err := config.Load(); err == nil {
		t.Fatal("expected error for malformed MEMORY_MONITOR_LABELS, got nil")
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
	if cfg.Scheduling.NodeSelector != nil || cfg.Scheduling.Tolerations != nil || cfg.Scheduling.Affinity != nil {
		t.Fatalf("Scheduling should be zero value when AgentScheduling is empty: %+v", cfg.Scheduling)
	}
}

// TestLoad_MemoryTopologyKey asserts MEMORY_TOPOLOGY_KEY is empty by default
// (the memory builders then resolve it to kubernetes.io/hostname) and is passed
// through verbatim when the deploying helmfile sets it (issue #365).
func TestLoad_MemoryTopologyKey(t *testing.T) {
	t.Run("default empty", func(t *testing.T) {
		t.Setenv("OIDC_ISSUER", "https://kc/realms/tatara")
		t.Setenv("OIDC_AUDIENCE", "tatara-operator")
		t.Setenv("OPERATOR_OIDC_SECRET_NAME", "tatara-operator")
		cfg, err := config.Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.MemoryTopologyKey != "" {
			t.Fatalf("MemoryTopologyKey default = %q, want empty", cfg.MemoryTopologyKey)
		}
	})

	t.Run("from env", func(t *testing.T) {
		t.Setenv("OIDC_ISSUER", "https://kc/realms/tatara")
		t.Setenv("OIDC_AUDIENCE", "tatara-operator")
		t.Setenv("OPERATOR_OIDC_SECRET_NAME", "tatara-operator")
		t.Setenv("MEMORY_TOPOLOGY_KEY", "topology.kubernetes.io/zone")
		cfg, err := config.Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.MemoryTopologyKey != "topology.kubernetes.io/zone" {
			t.Fatalf("MemoryTopologyKey = %q, want topology.kubernetes.io/zone", cfg.MemoryTopologyKey)
		}
	})
}

// TestLoad_AgentSchedulingParsedIntoStruct asserts that Load parses
// AGENT_SCHEDULING once and stores the result in cfg.Scheduling so callers
// never re-parse the raw string and cannot silently discard a parse error.
func TestLoad_AgentSchedulingParsedIntoStruct(t *testing.T) {
	t.Setenv("OIDC_ISSUER", "https://kc/realms/tatara")
	t.Setenv("OIDC_AUDIENCE", "tatara-operator")
	t.Setenv("OPERATOR_OIDC_SECRET_NAME", "tatara-operator")
	t.Setenv("AGENT_SCHEDULING", `{"nodeSelector":{"kubernetes.io/os":"linux"}}`)

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Scheduling.NodeSelector["kubernetes.io/os"] != "linux" {
		t.Fatalf("Scheduling.NodeSelector not populated by Load: %+v", cfg.Scheduling)
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

// TestLoad_PushMetricsAllowedPrefixes asserts the comma-separated allowlist is
// parsed (trimmed, empties dropped) and defaults to nil when unset so the
// receiver keeps its built-in default.
func TestLoad_PushMetricsAllowedPrefixes(t *testing.T) {
	t.Setenv("OIDC_ISSUER", "https://kc/realms/tatara")
	t.Setenv("OIDC_AUDIENCE", "tatara-operator")
	t.Setenv("OPERATOR_OIDC_SECRET_NAME", "tatara-operator")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.PushMetricsAllowedPrefixes != nil {
		t.Fatalf("unset PUSH_METRICS_ALLOWED_PREFIXES = %v, want nil", cfg.PushMetricsAllowedPrefixes)
	}

	t.Setenv("PUSH_METRICS_ALLOWED_PREFIXES", " wrapper_, agent_ ,, memory_ ")
	cfg, err = config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := []string{"wrapper_", "agent_", "memory_"}
	if len(cfg.PushMetricsAllowedPrefixes) != len(want) {
		t.Fatalf("prefixes = %v, want %v", cfg.PushMetricsAllowedPrefixes, want)
	}
	for i, p := range want {
		if cfg.PushMetricsAllowedPrefixes[i] != p {
			t.Fatalf("prefixes[%d] = %q, want %q", i, cfg.PushMetricsAllowedPrefixes[i], p)
		}
	}
}

// TestLoad_AgentRunAsNonRootDefaultFalse asserts that when AGENT_RUN_AS_NON_ROOT is
// unset the Go default is false, matching the chart values.yaml default (agentRunAsNonRoot:
// false). The two must agree: a divergence means running the binary outside the chart
// yields the opposite securityContext from what the chart intends.
// TestLoad_IncidentDedupAndRefireDefaults asserts the refire comment cooldown
// defaults to DefaultIncidentRefireCooldown (30m).
func TestLoad_IncidentDedupAndRefireDefaults(t *testing.T) {
	t.Setenv("OIDC_ISSUER", "https://kc/realms/tatara")
	t.Setenv("OIDC_AUDIENCE", "tatara-operator")
	t.Setenv("OPERATOR_OIDC_SECRET_NAME", "tatara-operator")

	c, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.IncidentRefireCommentCooldown != config.DefaultIncidentRefireCooldown {
		t.Fatalf("cooldown default = %v, want %v", c.IncidentRefireCommentCooldown, config.DefaultIncidentRefireCooldown)
	}
}

// TestLoad_IncidentDedupAndRefireFromEnv asserts the cooldown parses minutes.
func TestLoad_IncidentDedupAndRefireFromEnv(t *testing.T) {
	t.Setenv("OIDC_ISSUER", "https://kc/realms/tatara")
	t.Setenv("OIDC_AUDIENCE", "tatara-operator")
	t.Setenv("OPERATOR_OIDC_SECRET_NAME", "tatara-operator")
	t.Setenv("INCIDENT_REFIRE_COMMENT_COOLDOWN_MINUTES", "15")

	c, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.IncidentRefireCommentCooldown != 15*time.Minute {
		t.Fatalf("cooldown = %v, want 15m", c.IncidentRefireCommentCooldown)
	}
}

// TestLoad_IncidentRefireCooldownMalformed asserts a non-integer minutes value
// fails startup loudly.
func TestLoad_IncidentRefireCooldownMalformed(t *testing.T) {
	t.Setenv("OIDC_ISSUER", "https://kc/realms/tatara")
	t.Setenv("OIDC_AUDIENCE", "tatara-operator")
	t.Setenv("OPERATOR_OIDC_SECRET_NAME", "tatara-operator")
	t.Setenv("INCIDENT_REFIRE_COMMENT_COOLDOWN_MINUTES", "soon")
	if _, err := config.Load(); err == nil {
		t.Fatal("expected error for non-integer INCIDENT_REFIRE_COMMENT_COOLDOWN_MINUTES, got nil")
	}
}

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
