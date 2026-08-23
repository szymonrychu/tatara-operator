package v1alpha1

import (
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/szymonrychu/tatara-operator/internal/budget"
)

// MemorySpec configures the per-Project memory stack footprint. Defaults are
// declared via +kubebuilder:default so they are enforced at admission and
// visible in the persisted object; the internal/memory builders no longer need
// to carry fallback constants.
type MemorySpec struct {
	// Enabled gates the WHOLE per-Project memory stack (cnpg postgres, neo4j,
	// lightrag, tatara-memory, and the stack's own ServiceMonitor/PodMonitor/
	// PrometheusRule).
	//
	// It is a *bool with deliberately NO kubebuilder:default so nil is
	// distinguishable from an explicit false: nil (the state of every Project
	// written before this field existed, and of every Project that never mentions
	// spec.memory) and true both mean ENABLED. Only an explicit false disables.
	// Read it through Project.MemoryEnabled / MemoryDisabled - never open-code the
	// nil check, and never gate on "== the default" (the DocumentationSpec.Enabled
	// MEMORY trap).
	//
	// Disabling tears the compute and monitoring objects down. What happens to the
	// data is deliberately NOT uniform (see reconcileMemory's teardown):
	//   - the postgres (PGDATA + WAL) and neo4j volumes are RETAINED. The
	//     object-store backup path is off by default and has no automatic restore,
	//     so a cascade delete there would be unrecoverable loss. Re-enabling
	//     reattaches them by name.
	//   - the lightrag volume is DELETED. That data removal is an explicit owner
	//     decision, and the lightrag index is derived data rebuilt by re-ingesting.
	// Do not "make these consistent" in either direction.
	// +optional
	Enabled *bool `json:"enabled,omitempty"`
	// +kubebuilder:default=1
	// +optional
	PgInstances int `json:"pgInstances,omitempty"`
	// +kubebuilder:default="10Gi"
	// +optional
	PgStorage string `json:"pgStorage,omitempty"`
	// PgWalStorage sizes the dedicated CloudNativePG WAL volume. WAL is kept on
	// its own PVC (separate from PGDATA) so a WAL burst - or WAL retained for a
	// lagging/re-syncing standby - cannot fill the data volume and take writes
	// down (issue #238). Defaults to 8Gi: max_slot_wal_keep_size is half the
	// volume, and a 2Gi WAL volume left the other half (1Gi) unable to hold a
	// standby-resync WAL burst, crashlooping replicas on the WAL relocation.
	// +kubebuilder:default="8Gi"
	// +optional
	PgWalStorage string `json:"pgWalStorage,omitempty"`
	// +kubebuilder:default="10Gi"
	// +optional
	Neo4jStorage string `json:"neo4jStorage,omitempty"`
}

// MemoryStatus reports the observed state of the per-Project memory stack.
// Endpoint is the canonical in-cluster URL every other component reads.
type MemoryStatus struct {
	// +optional
	Phase string `json:"phase,omitempty"`
	// +optional
	Endpoint string `json:"endpoint,omitempty"`
	// +optional
	ExternalEndpoint string `json:"externalEndpoint,omitempty"`
	// ReadySince records when the memory stack last transitioned into Phase==Ready.
	// It is set on the Provisioning->Ready edge and cleared whenever the stack
	// leaves Ready. Controllers use it to debounce herd-release on return-to-healthy.
	// +optional
	ReadySince *metav1.Time `json:"readySince,omitempty"`
	// ProvisioningSince records when the memory stack last transitioned INTO a
	// non-Ready phase (Provisioning or Degraded). Set on the Ready/Failed/""->
	// Provisioning edge, preserved across a Provisioning<->Degraded episode, and
	// cleared whenever the stack reaches Ready. reconcileMemory compares it
	// against MemoryConfig.ProvisioningTimeout to bound how long a stuck backend
	// may sit Provisioning before reporting Degraded (issue #355 - a wedged
	// stack sat Provisioning for 7h+ with no bounded failure signal).
	// +optional
	ProvisioningSince *metav1.Time `json:"provisioningSince,omitempty"`
	// NotReady names the stack components currently below their readiness gate
	// ("postgres", "neo4j", "lightrag", "memory-api"), in a stable order. Empty
	// when the stack is Ready. Issue #425: a stack could sit Provisioning for
	// hours with no record of WHICH of the four backends was holding it, which
	// made the incident undiagnosable from the Project alone.
	// +optional
	NotReady []string `json:"notReady,omitempty"`
	// PgReadyInstances / PgWantInstances are the observed and declared CNPG
	// instance counts. They are recorded even while the stack reads Ready, so a
	// degraded-but-quorate cluster (2 of 3 instances, issue #442) is visible
	// without querying CNPG directly.
	// +optional
	PgReadyInstances int `json:"pgReadyInstances,omitempty"`
	// +optional
	PgWantInstances int `json:"pgWantInstances,omitempty"`
	// PgPrimary is the pod CNPG currently reports as the primary. An empty value
	// on an otherwise-quorate cluster means no primary is elected - the cluster
	// accepts no writes. Read from Cluster.Status.CurrentPrimary because it stays
	// observable while an instance's container is dead, unlike CNPG's own
	// instance-manager endpoint (see MEMORY.md 2026-07-26).
	// +optional
	PgPrimary string `json:"pgPrimary,omitempty"`
	// DisabledGeneration is the Project generation whose disable teardown has
	// already completed. It is the idempotence marker for the memory-disabled
	// path: reconcileMemory issues the teardown deletes exactly once per
	// generation and every later pass on the same generation is a cheap no-op
	// instead of a delete storm against objects that are already gone. Cleared
	// whenever memory is re-enabled.
	// +optional
	DisabledGeneration int64 `json:"disabledGeneration,omitempty"`
}

// MemoryPhaseDisabled is the terminal Status.Memory.Phase of a Project whose
// spec.memory.enabled is false. It is deliberately distinct from Provisioning /
// Degraded / Failed: a disabled stack is CONFIGURED that way, not broken, and
// the memory alert set keys on the broken phases only.
const MemoryPhaseDisabled = "Disabled"

// MemoryEnabled reports whether p's memory stack should be provisioned.
// nil spec.memory, or a spec.memory with enabled unset, means enabled - so
// every Project written before spec.memory.enabled existed keeps its stack.
func (p *Project) MemoryEnabled() bool {
	if p == nil || p.Spec.Memory == nil {
		return true
	}
	return BoolVal(p.Spec.Memory.Enabled, true)
}

// MemoryDisabled is the negation of Project.MemoryEnabled, as a free function so
// packages that already read MemoryStablyReady (internal/agent, the controllers)
// can express "configured off" the same way they express "not ready".
func MemoryDisabled(p *Project) bool { return !p.MemoryEnabled() }

// MemoryReadyStabilizationWindow is how long the memory stack must hold
// Phase==Ready before callers treat it as stably ready. It mirrors the
// retrieval probe's unhealthy threshold (3 cycles x 60s), so a freshly elected
// leader does not declare the retrieval surface healthy before it has been
// confirmed.
const MemoryReadyStabilizationWindow = 3 * time.Minute

// MemoryStablyReady reports whether p's memory stack has been continuously
// Ready for at least MemoryReadyStabilizationWindow.
//
// It is NOT a spawn gate. Agent pods spawn and submit turns whatever the memory
// stack's phase; a memory outage that could not be bounded held every Task
// indefinitely and the platform stopped instead of degrading. What this
// predicate drives is (a) the repository INGEST gate, where a partial corpus
// would actually be written, (b) the TATARA_MEMORY_DEGRADED pod env, and (c)
// the degraded turn-0 prompt appendix.
//
// It lives in the API package - like InfraIncidentExempt did - because
// internal/agent builds the pod env and cannot import internal/controller.
func MemoryStablyReady(p *Project, now time.Time) bool {
	if p == nil || p.Status.Memory == nil || p.Status.Memory.Phase != "Ready" {
		return false
	}
	if p.Status.Memory.ReadySince == nil {
		return false
	}
	return now.Sub(p.Status.Memory.ReadySince.Time) >= MemoryReadyStabilizationWindow
}

// GrafanaSpec configures the optional per-project Grafana incident-response
// feature: an operator-provisioned read-only grafana-mcp and an alert-webhook
// receiver. The feature is inert unless Enabled.
type GrafanaSpec struct {
	Enabled bool `json:"enabled"`
	// URL is the Grafana base URL grafana-mcp queries (non-secret).
	// +optional
	URL string `json:"url,omitempty"`
	// SecretRef names a Secret holding the Grafana credentials. Keys:
	//   serviceAccountToken - Grafana Viewer SA token (mounted into grafana-mcp)
	//   webhookSecret       - static bearer the alert webhook must present
	// +optional
	SecretRef string `json:"secretRef,omitempty"`
	// CooldownSeconds is DEPRECATED and no longer used: the per-alert-group refire
	// window was replaced by in-flight dedup (admission-time idempotency).
	// Retained for API compatibility; the value has no effect.
	// +kubebuilder:default=3600
	// +optional
	CooldownSeconds int `json:"cooldownSeconds,omitempty"`
}

// GrafanaStatus reports the observed state of the per-Project grafana-mcp.
type GrafanaStatus struct {
	// +optional
	Phase string `json:"phase,omitempty"`
	// +optional
	Endpoint string `json:"endpoint,omitempty"`
}

// DocumentationSpec configures the optional post-merge documentation agent:
// a merge to any enrolled component's default branch spawns a documentation
// Task that updates the central docs repo if the change warrants it. Inert
// unless Enabled.
type DocumentationSpec struct {
	// Enabled has no kubebuilder:default -> false; do NOT gate behavior on
	// == default (MEMORY trap).
	Enabled bool `json:"enabled"`
	// Repo is the central documentation repo the agent maintains (git URL).
	// It must also be enrolled as a Repository CR under this Project so the
	// bot has push access and mkdocs CI runs.
	// +optional
	Repo string `json:"repo,omitempty"`
}

// LifecycleHooks holds optional shell commands the claude-code wrapper runs at
// fixed points in an agent session. Each is a command string executed via
// `sh -c`; an empty field is skipped. Hooks are best-effort: a non-zero exit is
// logged and counted but never aborts the agent run. preClone receives the repo
// URL and postClone the clone destination (passed as a positional arg and via
// env); the conversation/turn hooks receive the task context already present in
// the pod env (TATARA_TASK, TATARA_PROJECT).
type LifecycleHooks struct {
	// PreClone runs before each repository clone, with the repo URL as argument.
	// +optional
	PreClone string `json:"preClone,omitempty"`
	// PostClone runs after each successful clone+checkout, with the clone
	// destination directory as argument.
	// +optional
	PostClone string `json:"postClone,omitempty"`
	// ConversationStart runs once after the agent session boots successfully.
	// +optional
	ConversationStart string `json:"conversationStart,omitempty"`
	// ConversationRestart runs each time the session is relaunched/resumed after
	// a crash (the --continue path).
	// +optional
	ConversationRestart string `json:"conversationRestart,omitempty"`
	// AgentTurnFinished runs after each agent turn completes (after the work is
	// committed and pushed).
	// +optional
	AgentTurnFinished string `json:"agentTurnFinished,omitempty"`
	// ConversationFinished runs once during session teardown.
	// +optional
	ConversationFinished string `json:"conversationFinished,omitempty"`
}

// AgentMCPServer declares one extra MCP server injected into the agent's
// .mcp.json by the wrapper. Fully generic: the operator neither knows nor
// validates which servers exist. Reserved-name collisions with platform-owned
// servers are resolved by the wrapper, not here.
type AgentMCPServer struct {
	// Name is the .mcp.json server key. Must match ^[a-z0-9-]+$.
	// +kubebuilder:validation:Pattern=`^[a-z0-9-]+$`
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`
	// URL is the server endpoint (e.g. http://svc.ns.svc.cluster.local:8080/mcp).
	// +kubebuilder:validation:MinLength=1
	URL string `json:"url"`
	// Type is the MCP transport; defaults to http.
	// +kubebuilder:validation:Enum=http;sse
	// +kubebuilder:default=http
	// +optional
	Type string `json:"type,omitempty"`
}

// AgentSkillSource declares one extra skill repository the wrapper clones and
// installs skills from, into every agent pod of the project. Fully generic
// (mirrors AgentMCPServer): the operator neither knows nor validates which
// skills exist. Same-host private sources authenticate with the project's
// scmSecretRef token via the wrapper's global GIT_TOKEN credential helper - the
// same auth path as the repo clones - so no extra secret wiring is needed here.
type AgentSkillSource struct {
	// Name is a stable identifier (clone dir + logs). Must match ^[a-z0-9-]+$.
	// +kubebuilder:validation:Pattern=`^[a-z0-9-]+$`
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`
	// URL is the git repository URL to clone.
	// +kubebuilder:validation:MinLength=1
	URL string `json:"url"`
	// Ref is the git ref (branch, tag, or SHA) to clone; empty defaults to main.
	// +optional
	Ref string `json:"ref,omitempty"`
	// Subdir is the path within the clone holding the skill dirs; empty = repo root.
	// +optional
	Subdir string `json:"subdir,omitempty"`
}

// AgentSpec configures the wrapper agent session a Task runs.
type AgentSpec struct {
	// +optional
	Model string `json:"model,omitempty"`
	// +optional
	Image string `json:"image,omitempty"`
	// +kubebuilder:default="bypassPermissions"
	// +optional
	PermissionMode string `json:"permissionMode,omitempty"`
	// Deprecated: RETIRED BY O3 AND READ BY NOTHING. It is retained ONLY so that
	// helmfile values already setting it keep validating; deleting the field is a
	// breaking API change that needs a lockstep helmfile PR, and a later
	// semver:major removes it once helmfile is clean. Setting it has NO EFFECT.
	//
	// MaxTurnsPerTask WAS the LIFETIME turn backstop across every pod of a Task
	// (contract A.6). It parked turn-budget-exhausted, which killed long healthy
	// runs: a turn count measures how much an agent has done, not whether it is
	// stuck. Stall is now decided by the probe machinery and, failing that, by the
	// 24h stage.ResidencyCapAll dead-man switch.
	// +kubebuilder:default=300
	// +optional
	MaxTurnsPerTask int `json:"maxTurnsPerTask,omitempty"`
	// TurnTimeoutSeconds is the per-turn stall (inactivity) window in seconds: a
	// turn is failed only after this long with no agent activity, not at a fixed
	// wall-clock age, so a turn that keeps streaming output is not killed mid-work.
	// The name is kept for CRD compatibility.
	// +kubebuilder:default=1800
	// +optional
	TurnTimeoutSeconds int `json:"turnTimeoutSeconds,omitempty"`
	// StallProbeGraceSeconds is how long the operator waits for a stall probe to
	// be ANSWERED before counting that attempt as unanswered.
	//
	// It is not a second turn timeout. The probe is delivered at the agent's next
	// TOOL-CALL BOUNDARY, so a healthy agent inside one long tool call answers
	// late rather than never: a measured 70s sleep buffered the probe 58.2s. The
	// floor of 60s exists so a grace shorter than a single ordinary tool call
	// cannot be configured, which would turn every long tool call into a stall.
	//
	// Read by nothing in this release. The schema lands FIRST and deliberately
	// ahead of any consumer: structural-CRD pruning drops unknown fields SILENTLY,
	// so a values file that set this before the schema existed would be discarded
	// with no error anywhere.
	// +kubebuilder:validation:Minimum=60
	// +kubebuilder:default=300
	// +optional
	StallProbeGraceSeconds int `json:"stallProbeGraceSeconds,omitempty"`
	// StallProbeMaxAttempts is how many unanswered probes the operator sends
	// before escalating. Bounded at 5 because each attempt costs a full
	// StallProbeGraceSeconds, so the escalation delay is the product of the two
	// and an unbounded attempt count would push a genuinely hung turn past its
	// pod TTL, where the stall handling stops mattering.
	//
	// Read by nothing in this release; see StallProbeGraceSeconds.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=5
	// +kubebuilder:default=2
	// +optional
	StallProbeMaxAttempts int `json:"stallProbeMaxAttempts,omitempty"`
	// Deprecated: RETIRED BY O3 AND READ BY NOTHING. It is retained ONLY so that
	// helmfile values already setting it keep validating; deleting the field is a
	// breaking API change that needs a lockstep helmfile PR, and a later
	// semver:major removes it once helmfile is clean. Setting it has NO EFFECT.
	//
	// MaxTurnsPerPod bounded turns within ONE pod's life. It had NO enforcement
	// reader even before O3 (stage.EnforcesMaxTurnsPerPod, its only gate, had zero
	// callers and is deleted); the pod's runaway bounds are agentPodTTLSeconds and
	// the boot-crash watchdog.
	// +kubebuilder:default=40
	// +optional
	MaxTurnsPerPod int `json:"maxTurnsPerPod,omitempty"`
	// Deprecated: RETIRED BY O3 AND READ BY NOTHING. It is retained ONLY so that
	// helmfile values already setting it keep validating; deleting the field is a
	// breaking API change that needs a lockstep helmfile PR, and a later
	// semver:major removes it once helmfile is clean. Setting it has NO EFFECT.
	//
	// MaxReviewRounds bounded request_changes round-trips. It parked
	// review-loop-exhausted, which killed implement/review pairs that were
	// converging - a round count says how much conversation happened, not whether
	// it is going anywhere. status.reviewRounds is still incremented for
	// observability.
	// +kubebuilder:default=3
	// +optional
	MaxReviewRounds int `json:"maxReviewRounds,omitempty"`
	// Deprecated: RETIRED BY O3 AND READ BY NOTHING. It is retained ONLY so that
	// helmfile values already setting it keep validating; deleting the field is a
	// breaking API change that needs a lockstep helmfile PR, and a later
	// semver:major removes it once helmfile is clean. Setting it has NO EFFECT.
	//
	// MaxPodRecreations bounded boot-crash respawns of one Task's agent pod. It
	// parked pod-recreation-exhausted. Its REPLACEMENT IS AN ALERT, not a cap:
	// sum by (project) (increase(operator_pod_recreations_total[1h])) > 6,
	// critical (tatara-observability). stats.podRecreations is still counted and
	// still exported - that counter is the alert's only input.
	// +kubebuilder:default=3
	// +optional
	MaxPodRecreations int `json:"maxPodRecreations,omitempty"`
	// +kubebuilder:default=200000
	// +optional
	ContextWindowTokens int `json:"contextWindowTokens,omitempty"`
	// HandoverThresholdPercent is the share of the context window (LastTurnInput
	// tokens) past which the lifecycle compacts instead of replaying the full
	// conversation: below it the next pod resumes the full transcript (issue #114
	// full resume), at/above it it falls back to the compacted text Handover. 25%
	// per issue #114 decision 2.
	// +kubebuilder:default=25
	// +optional
	HandoverThresholdPercent int `json:"handoverThresholdPercent,omitempty"`
	// +kubebuilder:validation:Minimum=3
	// +kubebuilder:default=10
	// +optional
	MaxLifecycleIterations int `json:"maxLifecycleIterations,omitempty"`
	// Effort is the reasoning-effort level passed to the wrapper agent as the
	// EFFORT env var (the "ultracode" lever). Highest by default.
	// +kubebuilder:validation:Enum=low;medium;high;xhigh;max
	// +kubebuilder:default="xhigh"
	// +optional
	Effort string `json:"effort,omitempty"`
	// MaxTaskTokens is a per-Task cumulative output-token ceiling for the
	// otherwise turn-uncapped implementation kinds (implement, issueLifecycle): a
	// runaway backstop, not a cost lever. 0 disables it (the default); opt in via
	// the Project values. When Status.CumulativeTokens crosses it the Task is
	// failed with reason TokenBudgetExceeded. TUNE from the component-6 per-kind
	// token telemetry once a healthy-run distribution is known.
	// +optional
	MaxTaskTokens int64 `json:"maxTaskTokens,omitempty"`
	// ModelByKind overrides the project-wide Model per Task Kind. Keys are the
	// Task.Spec.Kind enum values (clarify, triageIssue, review, brainstorm, refine,
	// implement, incident, issueLifecycle, selfImprove, documentation, upgrade) plus
	// the "healthCheck"
	// pseudo-key: healthCheck shares Kind=brainstorm but is resolved against this
	// key first (falling back to the brainstorm entry when absent), letting
	// healthCheck's recurring classification work be tiered separately from
	// brainstorm's creative work. A missing or empty entry falls back to Model.
	// Values are authoritative model IDs (claude-opus-5, claude-sonnet-5).
	// +kubebuilder:validation:MaxProperties=12
	// +kubebuilder:validation:XValidation:rule="self.all(k, k in ['implement','review','clarify','triageIssue','brainstorm','issueLifecycle','incident','selfImprove','refine','healthCheck','documentation','upgrade'])",message="modelByKind keys must be one of: implement, review, clarify, triageIssue, brainstorm, issueLifecycle, incident, selfImprove, refine, healthCheck, documentation, upgrade"
	// +kubebuilder:validation:XValidation:rule="self.all(k, self[k].startsWith('claude-') && self[k].size() <= 64)",message="modelByKind values must be a claude model ID (start with 'claude-', max 64 chars)"
	// +optional
	ModelByKind map[string]string `json:"modelByKind,omitempty"`
	// EffortByKind overrides the project-wide Effort per Task Kind. Same keying as
	// ModelByKind (including the "healthCheck" pseudo-key); a missing or empty
	// entry falls back to Effort. Values are the effort enum (low|medium|high|xhigh|max).
	// +kubebuilder:validation:MaxProperties=12
	// +kubebuilder:validation:XValidation:rule="self.all(k, k in ['implement','review','clarify','triageIssue','brainstorm','issueLifecycle','incident','selfImprove','refine','healthCheck','documentation','upgrade'])",message="effortByKind keys must be one of: implement, review, clarify, triageIssue, brainstorm, issueLifecycle, incident, selfImprove, refine, healthCheck, documentation, upgrade"
	// +kubebuilder:validation:XValidation:rule="self.all(k, self[k] in ['low','medium','high','xhigh','max'])",message="effortByKind values must be one of: low, medium, high, xhigh, max"
	// +optional
	EffortByKind map[string]string `json:"effortByKind,omitempty"`
	// SkillsRef is the git ref (branch, tag, or SHA) of the tatara-agent-skills
	// repo to clone at boot. Empty defaults to "main".
	// +optional
	SkillsRef string `json:"skillsRef,omitempty"`
	// Hooks are optional lifecycle commands the wrapper runs at fixed points
	// (clone, conversation start/restart, turn finished, conversation finished).
	// +optional
	Hooks *LifecycleHooks `json:"hooks,omitempty"`
	// ExtraEnvs are appended to the wrapper container's env, after the operator's
	// own variables (so a stray extra cannot shadow a required one).
	// +optional
	ExtraEnvs []corev1.EnvVar `json:"extraEnvs,omitempty"`
	// ExtraEnvsFrom populates the wrapper container's envFrom (ConfigMap/Secret refs).
	// +optional
	ExtraEnvsFrom []corev1.EnvFromSource `json:"extraEnvsFrom,omitempty"`
	// ExtraVolumeMounts are appended to the wrapper container's volumeMounts.
	// +optional
	ExtraVolumeMounts []corev1.VolumeMount `json:"extraVolumeMounts,omitempty"`
	// ExtraVolumes are appended to the agent Pod's volumes.
	// +optional
	ExtraVolumes []corev1.Volume `json:"extraVolumes,omitempty"`
	// ExtraSidecarContainers are appended to the agent Pod's containers, after the wrapper.
	// +optional
	ExtraSidecarContainers []corev1.Container `json:"extraSidecarContainers,omitempty"`
	// ExtraInitContainers populate the agent Pod's initContainers.
	// +optional
	ExtraInitContainers []corev1.Container `json:"extraInitContainers,omitempty"`
	// MCPServers are extra MCP servers merged into the agent's .mcp.json by the
	// wrapper, after repo overlay fragments but before the platform-owned
	// servers (tatara/grafana/serena), which always win a name collision.
	// +optional
	MCPServers []AgentMCPServer `json:"mcpServers,omitempty"`
	// SkillSources are extra skill repositories installed into every agent pod
	// of the project (into <workspace>/.claude/skills), alongside the baked
	// tatara-agent-skills. Serialized to TATARA_EXTRA_SKILL_SOURCES for the wrapper.
	// +optional
	// +kubebuilder:validation:MaxItems=16
	SkillSources []AgentSkillSource `json:"skillSources,omitempty"`
	// PromptAppendByKind appends project-specific instruction text AFTER the
	// built-in per-kind agentJob prompt (internal/controller/assignment.go). Keys
	// are agent kinds (implement, review, clarify, brainstorm, incident, refine,
	// documentation, upgrade) plus the "*" wildcard, which is appended to every kind BEFORE
	// that kind's own entry. This is TRUSTED project config (maintainer-supplied
	// via helmfile), never user/issue text, so assignment.go may interpolate it.
	// +optional
	// +kubebuilder:validation:MaxProperties=12
	PromptAppendByKind map[string]string `json:"promptAppendByKind,omitempty"`
}

// WorkspaceSpec configures the PERSISTENT agent workspace: a per-Task volume
// mounted at /workspace plus a per-PROJECT build-cache volume.
//
// It exists because /workspace was the container's writable layer, which is
// both VOLATILE and UNBOUNDED. Volatile cost 6 minutes of already-committed
// agent work to a single OOMKill; unbounded means nothing guarantees a node has
// room for every repo a project clones and builds. A quota-backed volume fixes
// both at once. It is NOT a speed feature: a resumed repo costs about 5.1s
// against 2.4s for a fresh depth-1 clone, so the workspace volume itself is a
// small latency COST that is accepted deliberately. The WIN is the build
// caches - a cold `go build ./...` measured 30.9s against 5.5s warm, and a
// -race test-binary compile 29.1s against 4.0s, many times per turn.
type WorkspaceSpec struct {
	// Enabled gates BOTH volumes and every mount they carry.
	//
	// It is an OPERATIONAL ESCAPE HATCH, not a tuning knob: set it false to take
	// one project back to the old ephemeral-overlay behaviour when a rollout of
	// this feature goes wrong, and then take it out again. Nothing about normal
	// operation should ever set it.
	//
	// It is a *bool with deliberately NO kubebuilder:default so nil is
	// distinguishable from an explicit false: nil (the state of every Project
	// written before this field existed, and of every Project that never mentions
	// spec.workspace) and true both mean ENABLED - the same rationale as
	// MemorySpec.Enabled. Read it through Project.WorkspaceEnabled - never
	// open-code the nil check.
	// +optional
	Enabled *bool `json:"enabled,omitempty"`
	// StorageClass is PINNED rather than left to the cluster default, and that is
	// the whole point of the field having a default at all. The workspace is
	// mounted ReadWriteMany; if the cluster default silently changed to an RBD
	// (block) class, every PVC this operator creates would be RWO and every
	// respawn onto a different node would stall in Multi-Attach until the old
	// pod's volume detached. Naming the CephFS class makes that impossible to
	// happen by accident.
	// +kubebuilder:default="rook-ceph-rwx"
	// +optional
	StorageClass string `json:"storageClass,omitempty"`
	// Size is the per-Task workspace request. On CephFS this is a subvolume
	// QUOTA, not a preallocation: the pool is charged only for what is actually
	// written, so this bounds a runaway clone/build rather than reserving 10Gi
	// per Task up front.
	// +kubebuilder:default="10Gi"
	// +optional
	Size string `json:"size,omitempty"`
	// CacheEnabled gates the per-PROJECT build-cache volume independently of the
	// workspace itself. Same tri-state semantics as Enabled: nil means on. Read
	// it through Project.WorkspaceCacheEnabled.
	// +optional
	CacheEnabled *bool `json:"cacheEnabled,omitempty"`
	// CacheSize is the per-PROJECT cache request, and the same CephFS quota
	// caveat as Size applies. It is larger than Size because it is shared across
	// every Task in the project and holds whole toolchain caches (tatara-operator
	// alone measured ~2.4GB GOCACHE plus ~630MB GOMODCACHE).
	// +kubebuilder:default="50Gi"
	// +optional
	CacheSize string `json:"cacheSize,omitempty"`
}

// Defaults for an empty, absent or PRUNED spec.workspace. They are declared as
// kubebuilder markers above AND as Go constants here on purpose, following
// internal/memory: a CRD default only applies if the LIVE CRD carries the
// field, and a field the live CRD does not know is pruned to "" on the way in.
// The accessors below are the single read path and they close that gap.
const (
	DefaultWorkspaceStorageClass = "rook-ceph-rwx"
	DefaultWorkspaceSize         = "10Gi"
	DefaultWorkspaceCacheSize    = "50Gi"
)

// WorkspaceEnabled reports whether p gets a persistent workspace volume.
// nil spec.workspace, or a spec.workspace with enabled unset, means enabled.
func (p *Project) WorkspaceEnabled() bool {
	if p == nil || p.Spec.Workspace == nil {
		return true
	}
	return BoolVal(p.Spec.Workspace.Enabled, true)
}

// WorkspaceCacheEnabled reports whether p gets the shared build-cache volume.
func (p *Project) WorkspaceCacheEnabled() bool {
	if p == nil || p.Spec.Workspace == nil {
		return true
	}
	return BoolVal(p.Spec.Workspace.CacheEnabled, true)
}

// WorkspaceStorageClass is the StorageClass both workspace volumes are
// provisioned from, falling back to DefaultWorkspaceStorageClass.
func WorkspaceStorageClass(p *Project) string {
	if p == nil || p.Spec.Workspace == nil || p.Spec.Workspace.StorageClass == "" {
		return DefaultWorkspaceStorageClass
	}
	return p.Spec.Workspace.StorageClass
}

// WorkspaceSize is the per-Task workspace request, falling back to
// DefaultWorkspaceSize.
func WorkspaceSize(p *Project) string {
	if p == nil || p.Spec.Workspace == nil || p.Spec.Workspace.Size == "" {
		return DefaultWorkspaceSize
	}
	return p.Spec.Workspace.Size
}

// WorkspaceCacheSize is the per-Project cache request, falling back to
// DefaultWorkspaceCacheSize.
func WorkspaceCacheSize(p *Project) string {
	if p == nil || p.Spec.Workspace == nil || p.Spec.Workspace.CacheSize == "" {
		return DefaultWorkspaceCacheSize
	}
	return p.Spec.Workspace.CacheSize
}

// ModelFor resolves the model for the given AGENT kind (brainstorm, incident,
// clarify, refine, review, documentation, implement) - NOT the Task origin
// kind (fix H9). ModelByKind is keyed on the agent kind; a missing or empty
// entry falls back to Model.
func (a AgentSpec) ModelFor(agentKind string) string {
	if m, ok := a.ModelByKind[agentKind]; ok && m != "" {
		return m
	}
	return a.Model
}

// EffortFor resolves the effort for the given AGENT kind. Same keying and
// fallback rule as ModelFor.
func (a AgentSpec) EffortFor(agentKind string) string {
	if e, ok := a.EffortByKind[agentKind]; ok && e != "" {
		return e
	}
	return a.Effort
}

// PromptAppendFor returns the wildcard ("*") append text followed by the kind-
// specific append text, separated by a blank line, skipping empty entries. Empty
// string when neither is set (the common case; no behavior change).
func (a AgentSpec) PromptAppendFor(agentKind string) string {
	var parts []string
	if w := a.PromptAppendByKind["*"]; w != "" {
		parts = append(parts, w)
	}
	if k := a.PromptAppendByKind[agentKind]; k != "" {
		parts = append(parts, k)
	}
	return strings.Join(parts, "\n\n")
}

// BoardSpec configures the project board tatara participates in.
type BoardSpec struct {
	// +optional
	GitHubProjectNumber int `json:"githubProjectNumber,omitempty"`
	// +optional
	GitLabBoardID int `json:"gitlabBoardId,omitempty"`
	// +kubebuilder:default="Status"
	// +optional
	StatusField string `json:"statusField,omitempty"`
}

// CronActivity schedules one Project scan activity (issueScan, healthCheck).
type CronActivity struct {
	// Schedule is a 5-field cron (robfig ParseStandard). Empty disables this activity.
	// +kubebuilder:validation:Pattern=`^$|^(\S+\s+){4}\S+$`
	// +optional
	Schedule string `json:"schedule,omitempty"`
	// MaxPerRepo caps the number of in-progress Tasks per repo (one lane per repo).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	// +optional
	MaxPerRepo int `json:"maxPerRepo,omitempty"`
}

// BrainstormActivity schedules the opt-in self-driven issue-proposal scan.
type BrainstormActivity struct {
	// +optional
	Enabled bool `json:"enabled,omitempty"`
	// +kubebuilder:validation:Pattern=`^$|^(\S+\s+){4}\S+$`
	// +optional
	Schedule string `json:"schedule,omitempty"`
	// Deprecated: MaxPerCycle is retained for API compatibility only. The controller
	// hard-caps brainstorm at one Task per project per cycle regardless of this value.
	// Setting it has no effect. See MEMORY.md for rationale.
	// +kubebuilder:default=1
	// +optional
	MaxPerCycle int `json:"maxPerCycle,omitempty"`
	// TargetOpenProposals is the TARGET number of brainstorm proposals kept open
	// and awaiting a maintainer decision across ALL repos in the project. The
	// controller refills toward it: it never closes a proposal to reconcile
	// downward. Unset falls back to MaxOpenProposals (deprecated alias) and then
	// to DefaultTargetOpenProposals. An explicit 0 disables refill entirely.
	// +optional
	TargetOpenProposals *int `json:"targetOpenProposals,omitempty"`
	// Deprecated: MaxOpenProposals was the pre-target CEILING. It is retained as
	// a working alias, honoured ONLY when TargetOpenProposals is unset, so an
	// unmigrated Project keeps working. Set TargetOpenProposals instead.
	// +kubebuilder:default=5
	// +optional
	MaxOpenProposals int `json:"maxOpenProposals,omitempty"`
	// HistoryWindow is how many recent brainstorm proposals are rendered into the
	// brainstorm pod's turn-0 bundle as the <proposal_history> block. Unset uses
	// DefaultHistoryWindow. An explicit 0 omits the block.
	// +optional
	HistoryWindow *int `json:"historyWindow,omitempty"`
	// StaleProposalDays configures the staleness reaper that auto-closes
	// bot-authored proposals with no human engagement (no human comment, no live
	// work) for at least that many days, clearing dead proposals out of the
	// TargetOpenProposals backlog. Semantics (liveness finding #8): a POSITIVE value
	// sets an explicit window; the UNSET default (0) enables the reaper with a
	// generous-but-finite default window (defaultStaleProposalDays) so un-approved
	// proposals do not accumulate unboundedly; a NEGATIVE value is the explicit
	// opt-out that disables the reaper entirely.
	// +optional
	StaleProposalDays int `json:"staleProposalDays,omitempty"`
	// +kubebuilder:validation:items:Enum=docs;memory;internet
	// +optional
	Sources []string `json:"sources,omitempty"`
	// MinSessionIntervalMinutes floors the wall-clock gap between two
	// brainstorm SESSIONS for this project, regardless of which path
	// dispatched the prior one (a due cron tick or the event-driven wake).
	// This is a RATE LIMIT, not a breaker: it only delays a refill until the
	// floor has elapsed, it never suppresses one permanently and it never
	// inspects why the prior session ended (a skip and a propose are throttled
	// identically). It exists because a skip files no Issue, so the backlog
	// deficit the event-driven wake reacts to stays positive and the wake
	// fires again immediately - with no floor, the only remaining brake was
	// the LLM's own `exhausted` judgment call, which must not be the sole
	// defense against a busy-loop. Semantics mirror StaleProposalDays: a
	// POSITIVE value sets an explicit floor in minutes; the UNSET default (0)
	// enables the floor at DefaultBrainstormMinSessionIntervalMinutes; a
	// NEGATIVE value is the explicit opt-out that disables the floor entirely.
	// +optional
	MinSessionIntervalMinutes int `json:"minSessionIntervalMinutes,omitempty"`
}

// Brainstorm backlog defaults. They live here, not at the kubebuilder-default
// layer, because all three fields are POINTERS: a kubebuilder default would make
// "unset" indistinguishable from an explicit value, and an explicit 0 is
// meaningful for every one of them.
const (
	DefaultTargetOpenProposals = 3
	DefaultHistoryWindow       = 20
	// MaxProposalsPerOutcome mirrors the submit_outcome schema ceiling enforced
	// in internal/restapi/outcome.go. The quota is clamped to it so a large
	// targetOpenProposals cannot instruct an obedient agent to emit a payload
	// the operator will refuse with a 400.
	MaxProposalsPerOutcome = 5
	// DefaultBrainstormMinSessionIntervalMinutes is the floor MinSessionIntervalMinutes
	// resolves to when unset: generous enough that a healthy project refills at most
	// a few times an hour, finite enough that a genuinely short backlog still drains
	// promptly. See MinSessionIntervalMinutes for the full rationale.
	DefaultBrainstormMinSessionIntervalMinutes = 12
)

// ResolveTarget resolves the backlog target: the explicit field, else the
// deprecated MaxOpenProposals alias, else the default. Never negative. It is
// named Resolve* because a Go method cannot share a name with a field.
func (a BrainstormActivity) ResolveTarget() int {
	if a.TargetOpenProposals != nil {
		return max(*a.TargetOpenProposals, 0)
	}
	if a.MaxOpenProposals > 0 {
		return a.MaxOpenProposals
	}
	return DefaultTargetOpenProposals
}

// ResolveHistoryWindow resolves how many prior proposals ride in the turn-0
// bundle as the <proposal_history> block. 0 omits the block.
func (a BrainstormActivity) ResolveHistoryWindow() int {
	if a.HistoryWindow != nil {
		return max(*a.HistoryWindow, 0)
	}
	return DefaultHistoryWindow
}

// ResolveMinSessionInterval resolves the floor between two brainstorm
// sessions. Semantics mirror StaleProposalDays: positive is an explicit
// floor, zero (unset) is the default floor, negative disables it (0
// duration, i.e. no gate).
func (a BrainstormActivity) ResolveMinSessionInterval() time.Duration {
	switch {
	case a.MinSessionIntervalMinutes > 0:
		return time.Duration(a.MinSessionIntervalMinutes) * time.Minute
	case a.MinSessionIntervalMinutes < 0:
		return 0
	default:
		return DefaultBrainstormMinSessionIntervalMinutes * time.Minute
	}
}

// RefineActivity configures the periodic project refiner. It is NOT a
// brainstorm pre-step any more: brainstorm is demand-driven and has no cron to
// hang a barrier off, so refine carries its own schedule. Grooming the backlog
// is genuinely periodic work; refilling it is not.
type RefineActivity struct {
	// Schedule is a 5-field cron (robfig ParseStandard). Empty disables refine.
	// +kubebuilder:validation:Pattern=`^$|^(\S+\s+){4}\S+$`
	// +optional
	Schedule string `json:"schedule,omitempty"`
	// ClosedLookbackDays bounds how far back closed issues are loaded for
	// already-implemented detection. Default 30 when zero.
	// +optional
	ClosedLookbackDays int `json:"closedLookbackDays,omitempty"`
}

// UpgradeActivity schedules the dependency-upgrade cron. Each due tick mints AT
// MOST ONE upgrade Task, and only while the project's OPEN UPGRADE LANES (live
// upgrade Tasks plus enqueued events that have not been minted into one yet)
// are below MaxOpenUpgrades. Throughput is therefore the cron FREQUENCY, not a fan-out:
// "0 */4 * * *" yields up to six upgrade Tasks a day with at most
// MaxOpenUpgrades in flight at once.
//
// Minting N Tasks per fire was rejected: each would self-scan and race for the
// same top candidate, and there is no agent-side task-minting tool to partition
// the work with (create_subtask was deleted in the #521 redesign).
type UpgradeActivity struct {
	// Schedule is a 5-field cron (robfig ParseStandard). Empty disables upgrade,
	// matching refine. Default off for every project.
	// +kubebuilder:validation:Pattern=`^$|^(\S+\s+){4}\S+$`
	// +optional
	Schedule string `json:"schedule,omitempty"`
	// MaxOpenUpgrades caps the project's concurrent open upgrade lanes (live
	// Tasks plus not-yet-minted enqueued events).
	//
	// SET IT EXPLICITLY IN THE ENROLLMENT VALUES. A kubebuilder default is
	// applied on WRITE and NEVER retroactively, so raising this default later
	// does not reach a Project CR that already exists.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=10
	// +optional
	MaxOpenUpgrades int `json:"maxOpenUpgrades,omitempty"`
}

// ReleaseAgeSpec is the minimum age, IN DAYS, a released version must have
// before the upgrade agent will propose it, per semver level. Zero means
// bleeding edge: take it the moment it publishes.
//
// Bleeding edge is a deliberate, accepted trade, not an oversight: it means a
// broken release reaches the cluster (grafana v13.0.0 shipped a dashboard-losing
// migration bug and was pulled; the fix was v13.0.1). This field exists so a
// project can be made conservative without a code change.
type ReleaseAgeSpec struct {
	// +kubebuilder:validation:Minimum=0
	// +optional
	Major int `json:"major,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +optional
	Minor int `json:"minor,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +optional
	Patch int `json:"patch,omitempty"`
}

// UpgradePolicySpec is the resolved policy the upgrade agent is handed in its
// turn-0 assignment. The operator does not ACT on it: it RENDERS it. Every
// decision it describes is the AGENT's, made against release notes the operator
// cannot read.
//
// THE CEL RULE TIES THE ENGINE ALLOWLIST TO THE ADOPTION SWITCH. UpgradeEngineLogins
// widens TWO permissions on its own - ownershipForAuthor classifies that login's
// merge requests `tatara` (which mergeAllowedForOwnership then merges), and
// isUpgradeEngineActor lets that login's pushes re-anchor the bot-head baseline
// instead of standing the merge request down. Both are meaningless without
// adoption and both are live the moment the field is set, so a values file that
// lists the engine and forgets adoptBranchPrefix hands merge authority to an
// account for no purpose. Requiring the prefix makes the two arm together.
// +kubebuilder:validation:XValidation:rule="!has(self.upgradeEngineLogins) || size(self.upgradeEngineLogins) == 0 || (has(self.adoptBranchPrefix) && self.adoptBranchPrefix != \"\")",message="upgradeEngineLogins requires a non-empty adoptBranchPrefix: the allowlist widens merge ownership and head-baseline attribution for those logins and is meaningless without adoption"
type UpgradePolicySpec struct {
	// Engine selects the candidate-discovery mechanism. `renovate` runs the
	// Renovate CLI read-only inside the pod and reads its report as a HINT;
	// `none` means the agent enumerates candidates itself, which is right for a
	// repo with no dependency manifests at all.
	// +kubebuilder:validation:Enum=renovate;none
	// +kubebuilder:default=none
	// +optional
	Engine string `json:"engine,omitempty"`
	// MajorStrategy is how far a single Task may jump. `nextHopOnly` proposes the
	// next mandatory release and nothing beyond it, walking a multi-hop chain one
	// deployed Task at a time (the repo's current pin IS the cursor; no chain
	// state is persisted anywhere). `latest` jumps straight to the newest release.
	// +kubebuilder:validation:Enum=nextHopOnly;latest
	// +kubebuilder:default=nextHopOnly
	// +optional
	MajorStrategy string `json:"majorStrategy,omitempty"`
	// AdoptBranchPrefix is the head-branch prefix that marks a dependency-upgrade
	// merge request this project adopts into its own upgrade Task. Empty (the
	// default) disables adoption entirely.
	//
	// THE PREFIX IS HALF THE TEST. The other half is the AUTHOR, which must be
	// scm.botLogin or an UpgradeEngineLogins entry (see below and
	// AdoptUpgradeMR). Prefix alone would adopt any branch anyone chose to name
	// renovate/something, and an adopted merge request is one the platform will
	// merge. The author is also what makes the merge LEGAL without any operator
	// intervention: ownershipForAuthor classifies a botLogin-authored merge
	// request `tatara`, so it is platform-owned from its first reconcile and
	// nothing has to flip it.
	//
	// DEFAULT EMPTY ON PURPOSE, and set explicitly in the enrollment values. A
	// structural-schema default is applied on EVERY write, including an
	// unrelated helm upgrade, so a "renovate/" default would arm adoption on
	// whichever apply happened next rather than at a moment somebody chose - and
	// it would arm it on every enrolled project at once, which makes the
	// Renovate-stops-merging cutover unschedulable. Same rule as
	// MaxOpenUpgrades above, for the same reason.
	//
	// The trailing slash is enforced: a bare "renovate" would also match a human
	// branch named renovate-experiment.
	// +kubebuilder:validation:Pattern=`^$|^[A-Za-z0-9][A-Za-z0-9._-]*/$`
	// +kubebuilder:validation:MaxLength=63
	// +optional
	AdoptBranchPrefix string `json:"adoptBranchPrefix,omitempty"`
	// UpgradeEngineLogins are FORGE LOGINS, beyond scm.botLogin, that this
	// project accepts as its dependency-upgrade engine. They mean two things,
	// and both are identity statements the forge authenticated:
	//
	//  1. a merge request they AUTHORED under AdoptBranchPrefix is adoptable
	//     (AdoptUpgradeMR). Prefix alone is not enough: anyone can push a branch
	//     called renovate/anything, and an adopted merge request is one the
	//     platform will merge.
	//  2. a push they made to such a branch RE-ANCHORS the head baseline instead
	//     of standing the merge request down (internal/webhook isUpgradeEngineActor).
	//     A routine engine rebase is not a human taking the branch back.
	//
	// LEAVE IT EMPTY WHEN THE ENGINE RUNS WITH THE PLATFORM BOT'S OWN TOKEN,
	// which is the shape project-infrastructure uses: botLogin already covers
	// both meanings, and listing the bot again buys nothing.
	//
	// THESE ARE LOGINS, NOT GIT AUTHORS, AND THE DIFFERENCE IS THE WHOLE POINT.
	// A forge login is the authenticated account behind a token; a git commit's
	// author is a string the pusher chose, is not verified by anything, and
	// survives `git commit --amend` unchanged - so a human amending an engine's
	// commit reads as the engine. This repo has refused to key decisions on it
	// three times already (MergeRequestStatus.MergedSHA, OperatorLandedSHA,
	// brainstormResumeKind). Do not add a git-author variant of this field.
	//
	// NEVER put a human maintainer here. It would make every push that human
	// makes to a prefixed branch re-anchor rather than hand the merge request
	// back, which is the one thing the ownership state machine exists to do.
	// +kubebuilder:validation:MaxItems=8
	// +optional
	UpgradeEngineLogins []string `json:"upgradeEngineLogins,omitempty"`
	// +optional
	MinimumReleaseAge *ReleaseAgeSpec `json:"minimumReleaseAge,omitempty"`
}

// ScmCron groups the cron-driven scan activities.
type ScmCron struct {
	// +optional
	IssueScan CronActivity `json:"issueScan,omitempty"`
	// +optional
	Brainstorm BrainstormActivity `json:"brainstorm,omitempty"`
	// Documentation is the scheduled documentation-sync cron (replaces the retired
	// per-merge push trigger): each tick spawns a documentation Task, scoped to the
	// docs repo, for every enrolled component repo that advanced since the last run
	// (Status.LastDocumentation). Requires Spec.Documentation.Enabled + Repo. Empty
	// Schedule disables it.
	// +optional
	Documentation CronActivity `json:"documentation,omitempty"`
	// Refine configures the periodic project refiner. It has its OWN cron
	// (RefineActivity.Schedule) - it is not a pre-scan barrier and does not
	// piggyback on any other activity's cadence. Empty Schedule disables it.
	// +optional
	Refine RefineActivity `json:"refine,omitempty"`
	// Upgrade is the dependency-upgrade cron. DEFAULT OFF (empty Schedule) for
	// every project: enabling it lets an agent open merge requests that change
	// deployed versions, so it is opt-in per project in the enrollment values.
	// +optional
	Upgrade UpgradeActivity `json:"upgrade,omitempty"`
}

// ScmSpec binds a Project to one SCM provider and its board/merge policy.
// +kubebuilder:validation:XValidation:rule="!has(self.maintainerLogins) || self.maintainerLogins.all(m, m != self.botLogin)",message="maintainerLogins must not contain botLogin (the bot is structurally excluded from maintainer approval)"
// +kubebuilder:validation:XValidation:rule="!has(self.reporterLogins) || self.reporterLogins.all(r, r != self.botLogin)",message="reporterLogins must not contain botLogin (the bot is trusted implicitly; listing it is a misconfiguration)"
type ScmSpec struct {
	// +kubebuilder:validation:Enum=github;gitlab
	Provider string `json:"provider"`
	Owner    string `json:"owner"`
	// BotLogin is the SCM login of the platform bot. Required and non-empty: it
	// is the identity structurally excluded from the maintainer-approval gate, so
	// an empty value would collapse the bot exclusion. Must not appear in
	// maintainerLogins/reporterLogins (enforced by the ScmSpec CEL rules).
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=100
	BotLogin string `json:"botLogin"`
	// BotEmail is the git commit author email for agent commits (the bot's
	// noreply/commit email). When empty the wrapper's default identity stands.
	// +optional
	BotEmail string `json:"botEmail,omitempty"`
	// MaintainerLogins are the human maintainer accounts. They are the unified
	// trusted-insider AND approver set (issue #102): together with BotLogin they
	// form the "trusted insider" set used for issue #56 autoapprove, and - when
	// non-empty - a thread comment counts as the human approval go-ahead only if
	// its author is in this list. Empty preserves the historical behavior (any
	// non-bot human reply releases the self-approve hold; only BotLogin is
	// excluded from #56 autoapprove). Overridable per-repository via
	// RepositorySpec.MaintainerLogins.
	// +optional
	// +kubebuilder:validation:MaxItems=100
	// +kubebuilder:validation:items:MaxLength=100
	MaintainerLogins []string `json:"maintainerLogins,omitempty"`
	// ReporterLogins gates issue/issue-comment intake (issue #102). When non-empty
	// the operator only acts on issues and issue-comments authored by the bot, a
	// maintainer, or an account in this list; everything else is dropped at intake
	// (cron scan and webhook) so unknown third parties cannot drive the lifecycle
	// via prompt injection. Empty preserves the historical open behavior (any
	// author is accepted). Overridable per-repository via
	// RepositorySpec.ReporterLogins.
	// +optional
	// +kubebuilder:validation:MaxItems=100
	// +kubebuilder:validation:items:MaxLength=100
	ReporterLogins []string `json:"reporterLogins,omitempty"`
	// +optional
	Board *BoardSpec `json:"board,omitempty"`
	// +kubebuilder:validation:Enum=afterApproval;autoMergeOnGreenCI
	// +kubebuilder:default="afterApproval"
	// +optional
	MergePolicy string `json:"mergePolicy,omitempty"`
	// PRReactionScope gates which PRs/MRs the B.4 sweep's review path reacts to.
	// Empty (the default) reviews every open human PR/MR (historical open
	// behavior). "labeledOrMentioned" restricts reviews to PRs carrying the
	// project TriggerLabel or @-mentioning the bot, so unlabeled, un-mentioned
	// MRs are not re-reviewed every scan cycle. "all" is an explicit synonym for
	// the open behavior. The default is intentionally NOT "labeledOrMentioned":
	// a defaulted value is indistinguishable from an explicit one, so defaulting
	// it would silently gate every project; opt in explicitly instead.
	// +kubebuilder:validation:Enum=labeledOrMentioned;all
	// +optional
	PRReactionScope string `json:"prReactionScope,omitempty"`
	// ApprovedLabel marks an issue approved for implementation.
	// +kubebuilder:default="tatara-approved"
	// +optional
	ApprovedLabel string `json:"approvedLabel,omitempty"`
	// BrainstormingLabel marks an issue tatara is triaging / discussing (pre-approval).
	// +kubebuilder:default="tatara-brainstorming"
	// +optional
	BrainstormingLabel string `json:"brainstormingLabel,omitempty"`
	// IncidentLabel marks a proposal issue that originated from an incident
	// investigation. Additive: applied alongside BrainstormingLabel, never
	// swept by the phase-label reconciler. Defaults to "tatara-incident".
	// +optional
	IncidentLabel string `json:"incidentLabel,omitempty"`
	// ImplementationLabel marks an issue whose implementation is in flight.
	// +kubebuilder:default="tatara-implementation"
	// +optional
	ImplementationLabel string `json:"implementationLabel,omitempty"`
	// DeclinedLabel marks an issue declined before implementation (triage reject).
	// +kubebuilder:default="tatara-declined"
	// +optional
	DeclinedLabel string `json:"declinedLabel,omitempty"`
	// +optional
	PriorityLabel string `json:"priorityLabel,omitempty"`
	// +optional
	Cron *ScmCron `json:"cron,omitempty"`
	// Guidance is free-form project charter text appended verbatim to the
	// brainstorm and healthCheck goal context. Empty leaves the goal unchanged.
	// +optional
	Guidance string `json:"guidance,omitempty"`
	// +kubebuilder:default=60
	// +optional
	BabysitDeadlineMinutes int `json:"babysitDeadlineMinutes,omitempty"`
	// ConversationIdleMinutes is the conversing stage's IDLE budget: how long a
	// conversation may go without a new event before the operator takes a handoff
	// turn and parks it at awaiting-human. It is measured from
	// Task.status.conversationLastEventAt, which every queued event re-stamps, so
	// it is a genuine idle timer and NOT agentPodTTLSeconds, which is a flat cap
	// from pod start with no reset.
	//
	// The two clocks answer different questions and do not conflict: this one says
	// when the CONVERSATION ends, agentPodTTLSeconds says when ONE POD rotates. A
	// conversing pod that hits its TTL takes the G.7 handoff turn and is replaced
	// in the SAME stage, so an actively-replied-to conversation keeps going
	// indefinitely (decision D6).
	//
	// This field survived the task-centric redesign as dead config, referenced
	// only by its own round-trip test. It is live again as of the conversing
	// stage. Zero means ConversationIdleDefault (60 minutes). A non-zero value
	// below ConversationIdleFloor (5 minutes) is CLAMPED to the floor: this
	// value becomes the conversing stage's ENTIRE pod TTL (agent.PodTTLSeconds),
	// and anything below PodReadyTimeout (5 minutes) TTL-stops the pod before
	// it can ever become Ready - an unbounded pod-recreation loop with no
	// podRecreations budget to stop it.
	// +kubebuilder:default=60
	// +kubebuilder:validation:Minimum=0
	// +optional
	ConversationIdleMinutes int `json:"conversationIdleMinutes,omitempty"`
}

// ConversationIdleFloor is the minimum idle budget ConversationIdle ever
// returns for a non-zero scm.conversationIdleMinutes. agent.PodTTLSeconds
// resolves the conversing stage's ENTIRE pod TTL from this value (it IS the
// pod's lifetime budget for that stage, unlike every other stage which uses
// AgentPodTTLSeconds), and PodReadyTimeout is 5 minutes - so an unfloored
// small value (conversationIdleMinutes: 1 gives a 60s TTL) TTL-stops the pod
// before it can ever become Ready. ttlStop does not charge podRecreations
// (task_stage.go), so that shape is unbounded pod churn with no budget to
// stop it (2026-07-28 final review IMPORTANT 3). Matches
// ScmSpec.AgentPodTTLSeconds' own +kubebuilder:validation:Minimum=300 floor,
// which exists for the identical reason on the flat-TTL stages.
//
// This is a CLAMP, not a CRD Minimum on the field itself: zero is a load-bearing
// sentinel for "use ConversationIdleDefault" (kubebuilder:validation:Minimum=0
// on the field), and a schema Minimum=5 would reject that legitimate explicit
// zero along with the genuinely-too-small values this guards against.
const ConversationIdleFloor = 5 * time.Minute

// ConversationIdle is the conversing stage's idle budget for p: the project's
// scm.conversationIdleMinutes, or ConversationIdleDefault when it is unset or
// non-positive, floored at ConversationIdleFloor otherwise. It is the ONE
// place the minutes-to-Duration conversion happens.
func ConversationIdle(p *Project) time.Duration {
	if p != nil && p.Spec.Scm != nil && p.Spec.Scm.ConversationIdleMinutes > 0 {
		d := time.Duration(p.Spec.Scm.ConversationIdleMinutes) * time.Minute
		if d < ConversationIdleFloor {
			return ConversationIdleFloor
		}
		return d
	}
	return ConversationIdleDefault
}

// MaxConcurrentAgents is p's agent-pod concurrency cap, defaulted to
// DefaultMaxConcurrentAgents when unset.
//
// IT DEFAULTS, SO IT MUST NEVER BE USED TO DETECT A PAUSE. MaxConcurrentAgents
// == 0 is the full-project pause kill switch and this helper turns that 0 into
// 3. Its only legitimate consumer is the MaxLivePods clamp below, which needs a
// concrete ceiling to compare against. Every pause check is a direct
// p.Spec.MaxConcurrentAgents == 0 read (see TestQueueCapacity_PauseMustNotUseFloor
// for the same trap on QueueCapacity).
func MaxConcurrentAgents(p *Project) int {
	if p != nil && p.Spec.MaxConcurrentAgents > 0 {
		return p.Spec.MaxConcurrentAgents
	}
	return DefaultMaxConcurrentAgents
}

// MaxLivePods is the per-project ceiling on simultaneously LIVE agent pods.
//
// IT CLAMPS. A configured value at or above MaxConcurrentAgents can never bind
// - the agent-concurrency cap saturates first - so a project configured that
// way silently has no live-pod ceiling at all. Clamping to
// MaxConcurrentAgents-1 keeps at least one slot for non-conversational work,
// whatever a maintainer types.
//
// THE STRICTLY-BELOW PROPERTY HOLDS FOR maxConcurrentAgents >= 2 ONLY, and the
// exception is deliberate. At a cap of 1 the clamp would produce a ceiling of
// 0, which deadlocks the project outright: no Task could ever enter a live
// state, so no conversation could ever start or finish. The floor of 1 wins
// there and the ceiling EQUALS the cap - one live pod, zero slots left for
// non-conversational work - because a one-agent project has no better trade to
// make. A deadlock is worse than a starved queue that at least drains.
//
// TestMaxLivePodsIsStrictlyBelowMaxConcurrentAgentsForAnyProject checks exactly
// that split contract: strict inequality at every cap >= 2, equality at 1.
func MaxLivePods(p *Project) int {
	want := DefaultMaxLivePods
	if p != nil && p.Spec.MaxLivePods > 0 {
		want = p.Spec.MaxLivePods
	}
	if ceiling := MaxConcurrentAgents(p) - 1; want > ceiling {
		want = ceiling
	}
	if want < 1 {
		want = 1
	}
	return want
}

// ProjectSpec defines the desired state of a Project.
type ProjectSpec struct {
	ScmSecretRef string `json:"scmSecretRef"`
	// +kubebuilder:default="tatara"
	// +optional
	TriggerLabel string `json:"triggerLabel,omitempty"`
	// MaxConcurrentAgents gates AGENT PODS (the admission unit is the pod-spawn,
	// not the Task). ZERO IS THE FULL-PROJECT PAUSE KILL SWITCH: at 0, admission
	// short-circuits and NO QueuedEvent is ever admitted, so no pod and no Task
	// is created. There is deliberately NO Minimum=1 (fix S2).
	//
	// It REPLACES the pre-redesign maxConcurrentTasks, which was PRUNED rather
	// than kept alongside: a stale helmfile value for the old key would otherwise
	// be silently ignored (structural pruning drops it) and concurrency would
	// quietly fall back to this field's default instead of erroring.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxConcurrentAgents int `json:"maxConcurrentAgents,omitempty"`
	// AgentPodTTLSeconds bounds ONE pod's life. The Task persists.
	// +kubebuilder:default=3600
	// +kubebuilder:validation:Minimum=300
	// +optional
	AgentPodTTLSeconds int `json:"agentPodTTLSeconds,omitempty"`
	// MaxLivePods caps how many Tasks in this project may sit in a LIVE state
	// at once. A live Task holds a live agent pod and therefore a REAL
	// MaxConcurrentAgents slot for as long as the conversation stays open
	// (queueTaskHoldsSlot counts it) - the two caps COMPOSE, so this one MUST
	// stay strictly below MaxConcurrentAgents or a handful of chatty threads can
	// occupy the project's entire agent concurrency and starve every
	// implement/review/merge Task indefinitely (2026-07-28 final review
	// IMPORTANT 1; see DefaultMaxLivePods for the default-pair reasoning).
	// v1alpha1.MaxLivePods CLAMPS a value that violates it rather than trusting
	// the typist.
	//
	// Reaching the ceiling DECLINES a new conversation ("live-ceiling-full"; the
	// event stays queued in PendingEvents and rides the Task's next turn) - it
	// does NOT evict an existing one. Eviction is a separate, rarer path
	// (enforceLivePodCeiling, the project-reconcile backstop) that only
	// fires once live pods EXCEED the ceiling, which reaching it
	// exactly never does.
	// Zero means DefaultMaxLivePods (2).
	// +kubebuilder:default=2
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxLivePods int `json:"maxLivePods,omitempty"`
	// MaxNewTasksPerSweep caps how many Tasks ONE sweep pass may mint (fix B1).
	// +kubebuilder:default=5
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxNewTasksPerSweep int `json:"maxNewTasksPerSweep,omitempty"`
	// MaxOpenTasks caps ACTIVE Tasks: every Task whose stage is pod-eligible
	// (NOT parked/delivered/rejected/failed). It is a Task CREATION budget and
	// it is NOT the same lever as MaxConcurrentAgents (a concurrency budget) -
	// a sweep that would exceed it mints nothing this pass. PARKED backlog
	// Tasks (stageReason=backlog-sweep) do NOT count: they hold ownership, not
	// work. Prod runs 6 today.
	// +kubebuilder:default=6
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxOpenTasks int `json:"maxOpenTasks,omitempty"`
	// MaxBundleBytes is the HARD byte budget for a rendered context bundle
	// (fix D1 - USER DECISION). Oldest comments elide first, behind an
	// explicit marker. Default 400 KB (~100k tokens).
	// +kubebuilder:default=400000
	// +kubebuilder:validation:Minimum=50000
	// +optional
	MaxBundleBytes int `json:"maxBundleBytes,omitempty"`
	// +optional
	Agent AgentSpec `json:"agent,omitempty"`
	// +optional
	Memory *MemorySpec `json:"memory,omitempty"`
	// +optional
	Workspace *WorkspaceSpec `json:"workspace,omitempty"`
	// +optional
	Scm *ScmSpec `json:"scm,omitempty"`
	// +optional
	Grafana *GrafanaSpec `json:"grafana,omitempty"`
	// +optional
	Documentation *DocumentationSpec `json:"documentation,omitempty"`
	// +optional
	Queue *QueueSpec `json:"queue,omitempty"`
	// UpgradePolicy configures the dependency-upgrade agent. Nil is the
	// default-off shape (engine none, nextHopOnly, no minimum release age) - a
	// kubebuilder default inside a nil struct pointer is never applied, so the
	// goal renderer resolves those defaults itself rather than reading them back.
	// +optional
	UpgradePolicy *UpgradePolicySpec `json:"upgradePolicy,omitempty"`
	// TokenBudget configures the token-budget admission gate (issue #189). Nil
	// inherits the operator-wide defaults verbatim; a present block is the
	// project's explicit budget config (its Enabled field is authoritative).
	// +optional
	TokenBudget *TokenBudgetSpec `json:"tokenBudget,omitempty"`
	// DeployBudgetSeconds is the Deploying-phase deadline budget for a push-CD
	// cascade along the LONGEST path to a tatara-helmfile apply (2 tag-cut hops,
	// e.g. cli -> wrapper -> helmfile): 1.2x the summed per-stage p95 durations.
	// On exceed, a Deploying Task parks recoverable with reason deploy-timeout.
	// +kubebuilder:default=3300
	// +optional
	DeployBudgetSeconds int `json:"deployBudgetSeconds,omitempty"`
	// DeploySingleHopBudgetSeconds is the tighter deadline budget for artifacts
	// one hop from tatara-helmfile (operator, memory, ingester, chat): no
	// intermediate parent rebuild. Deploy-supervision picks this over
	// DeployBudgetSeconds for single-hop artifacts.
	// +kubebuilder:default=2100
	// +optional
	DeploySingleHopBudgetSeconds int `json:"deploySingleHopBudgetSeconds,omitempty"`
	// MergeWaitBudgetMinutes bounds how long a discrete-implement umbrella waits
	// for its member PRs to be reviewed + merged before it parks recoverable with
	// an issue comment naming the stuck member(s) (item 3: the pre-merge deadline).
	// Default 720 (12h): generous enough for human review, bounded so a
	// permanently-stuck member surfaces instead of sitting open+approved forever.
	// +kubebuilder:default=720
	// +optional
	MergeWaitBudgetMinutes int `json:"mergeWaitBudgetMinutes,omitempty"`
	// AutoApproveMaxSignificance is the SEVERITY CEILING on the auto-approve
	// carve-out: the largest change a bot-authored, tatara-proposed issue (marked
	// <!-- tatara-proposed-by:<kind> -->) may ship without a maintainer comment.
	// `off` disables the carve-out entirely and is the default.
	//
	// It replaced the boolean autoApproveTataraProposals, which was all-or-nothing
	// and therefore licensed a `major` on the strength of the same provenance that
	// licensed a `patch`. The carve-out itself is unchanged in every other respect:
	// never a human-authored issue, marker or not - the bot-authorship check is
	// independent and mandatory - and never a body edited since filing (the Issue
	// Spec.ProposalBodyHash anchor, set at mint from the SCM-unreachable spec, must
	// still match the current body's fingerprint).
	//
	// THE CEILING BITES AT SUBMIT, NOT AT THE GATE. `change_significance` does not
	// exist on the wire until submit_outcome(action=submitted), so an auto-approved
	// Issue's grant is PROVISIONAL: a declared level above this ceiling is refused
	// there, with the work intact and the thread sent back to a human. Approvals a
	// maintainer actually cited are never severity-limited - a human who said go
	// ahead needs no second ceiling.
	//
	// It gates ONLY the approval carve-out, not the marker. Proposal filers stamp
	// the marker UNCONDITIONALLY, so a project at `off` still gets the marker in the
	// stored body (present but inert) - intentional, so a later raise can
	// auto-approve proposals filed while it was off.
	//
	// The EMPTY string reads as `off` (AutoApproveCeiling), which is what makes the
	// CRD upgrade fail closed: every Project CR written by a build that had the
	// boolean carries no value for this field.
	// +kubebuilder:validation:Enum=off;patch;minor;major
	// +kubebuilder:default=off
	// +optional
	AutoApproveMaxSignificance string `json:"autoApproveMaxSignificance,omitempty"`
}

// TokenBudgetSpec configures the per-Project token-budget admission gate (issue
// #189): pause proactive work (normal pool) at ProactivePercent and incident
// work (alert pool) at EmergencyPercent of the window usage. Off by default.
type TokenBudgetSpec struct {
	// Enabled turns the gate on for this project. When the block is present this
	// field is authoritative (it is NOT inherited from the operator-wide default).
	// +optional
	Enabled bool `json:"enabled,omitempty"`
	// Mode selects how usage is measured: customWindow meters the operator's own
	// per-turn token accounting against TokenLimit within a cron-anchored reset
	// window; claudeSubscription gates on the wrapper-reported Claude 5h/weekly
	// usage percentages.
	// +kubebuilder:validation:Enum=customWindow;claudeSubscription
	// +kubebuilder:default=customWindow
	// +optional
	Mode string `json:"mode,omitempty"`
	// ProactivePercent pauses the normal pool (brainstorm, implement, review, ...)
	// at this percentage of the window. Default 50.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=50
	// +optional
	ProactivePercent int `json:"proactivePercent,omitempty"`
	// EmergencyPercent pauses the alert pool (incidents) at this percentage of the
	// window. Ordered >= ProactivePercent at evaluation. Default 80.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=80
	// +optional
	EmergencyPercent int `json:"emergencyPercent,omitempty"`
	// FiveHourProactivePercent / FiveHourEmergencyPercent /
	// WeeklyProactivePercent / WeeklyEmergencyPercent gate each Claude
	// subscription window against its OWN thresholds (claudeSubscription mode).
	// Unset (0) inherits ProactivePercent/EmergencyPercent, so a Project that
	// sets none of these decides exactly as it does today.
	//
	// There is deliberately no per-Project maxSnapshotAge counterpart: the
	// Claude subscription is one account shared by every Project, so snapshot
	// staleness is fleet-wide and only the operator-wide
	// TOKEN_BUDGET_MAX_SNAPSHOT_AGE sets it.
	//
	// NO CEL RULES HERE, deliberately: SpawnCeilingByKind's two XValidation
	// rules plus MaxProperties=12 exist because envtest's apiserver rejects a
	// CRD whose CEL cost estimate exceeds budget. Plain Minimum/Maximum markers
	// are free; CEL here could break the entire envtest suite.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +optional
	FiveHourProactivePercent int `json:"fiveHourProactivePercent,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +optional
	FiveHourEmergencyPercent int `json:"fiveHourEmergencyPercent,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +optional
	WeeklyProactivePercent int `json:"weeklyProactivePercent,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +optional
	WeeklyEmergencyPercent int `json:"weeklyEmergencyPercent,omitempty"`
	// ResetSchedule is a 5-field cron (robfig ParseStandard) marking each window
	// reset boundary (customWindow mode). Empty disables the custom window.
	// +kubebuilder:validation:Pattern=`^$|^(\S+\s+){4}\S+$`
	// +optional
	ResetSchedule string `json:"resetSchedule,omitempty"`
	// WindowDuration is the declared window length as a Go duration (e.g. "5h",
	// "168h"). It bounds the reset-boundary search; pair it with ResetSchedule.
	// +optional
	WindowDuration string `json:"windowDuration,omitempty"`
	// TokenLimit is the absolute total-token budget per window (customWindow mode).
	// +optional
	TokenLimit int64 `json:"tokenLimit,omitempty"`
	// SpawnCeilingByKind gates each Task kind independently in claudeSubscription
	// mode: work of kind K is held once account usage reaches the given percent.
	// Keys are Task kinds; kinds absent here fall through to proactive/emergency.
	// +kubebuilder:validation:MaxProperties=12
	// +kubebuilder:validation:XValidation:rule="self.all(k, self[k] >= 0 && self[k] <= 100)",message="spawnCeilingByKind values must be 0..100"
	// +kubebuilder:validation:XValidation:rule="self.all(k, k in ['implement','review','clarify','selfImprove','triageIssue','brainstorm','issueLifecycle','incident','healthCheck','refine','documentation','upgrade'])",message="spawnCeilingByKind keys must be valid Task kinds"
	// +optional
	SpawnCeilingByKind map[string]int32 `json:"spawnCeilingByKind,omitempty"`
	// PollIntervalSeconds is how often the operator polls Claude account usage
	// (claudeSubscription mode). Floor 180 (enforced operator-side too).
	// +kubebuilder:validation:Minimum=180
	// +optional
	PollIntervalSeconds *int32 `json:"pollIntervalSeconds,omitempty"`
	// MonitorOverage surfaces the pay-as-you-go overage pool on dashboards. It is
	// read-only and never gates spawning.
	// +optional
	MonitorOverage *bool `json:"monitorOverage,omitempty"`
}

// BudgetConfig resolves the project's token-budget configuration, layering the
// per-Project spec over the operator-wide defaults: a nil spec inherits the
// defaults verbatim, while a present spec overrides each field it sets (zero-
// valued scalars fall back to the default) and its Enabled field is taken
// literally. The result is what budget.Evaluate consumes.
func (p *Project) BudgetConfig(defaults budget.Config) budget.Config {
	cfg := defaults
	s := p.Spec.TokenBudget
	if s == nil {
		return cfg
	}
	cfg.Enabled = s.Enabled
	if s.Mode != "" {
		cfg.Mode = budget.Mode(s.Mode)
	}
	if s.ProactivePercent > 0 {
		cfg.ProactivePercent = s.ProactivePercent
	}
	if s.EmergencyPercent > 0 {
		cfg.EmergencyPercent = s.EmergencyPercent
	}
	if s.FiveHourProactivePercent > 0 {
		cfg.FiveHourProactivePercent = s.FiveHourProactivePercent
	}
	if s.FiveHourEmergencyPercent > 0 {
		cfg.FiveHourEmergencyPercent = s.FiveHourEmergencyPercent
	}
	if s.WeeklyProactivePercent > 0 {
		cfg.WeeklyProactivePercent = s.WeeklyProactivePercent
	}
	if s.WeeklyEmergencyPercent > 0 {
		cfg.WeeklyEmergencyPercent = s.WeeklyEmergencyPercent
	}
	if s.ResetSchedule != "" {
		cfg.ResetSchedule = s.ResetSchedule
	}
	if s.WindowDuration != "" {
		if d, err := time.ParseDuration(s.WindowDuration); err == nil {
			cfg.WindowDuration = d
		}
	}
	if s.TokenLimit > 0 {
		cfg.TokenLimit = s.TokenLimit
	}
	if len(s.SpawnCeilingByKind) > 0 {
		cfg.SpawnCeilingByKind = make(map[string]int, len(s.SpawnCeilingByKind))
		for k, v := range s.SpawnCeilingByKind {
			cfg.SpawnCeilingByKind[k] = int(v)
		}
	}
	return cfg
}

// BudgetWindowState maps the persisted custom-window accumulator (Project
// status) into a budget.WindowState; the zero value when unset.
func (p *Project) BudgetWindowState() budget.WindowState {
	st := p.Status.TokenBudget
	if st == nil {
		return budget.WindowState{}
	}
	ws := budget.WindowState{WindowTokens: st.WindowTokens}
	if st.WindowStart != nil {
		ws.WindowStart = st.WindowStart.Time
	}
	return ws
}

// SetBudgetWindowState writes a rolled custom-window accumulator back onto the
// Project status, allocating the status block on first use.
func (p *Project) SetBudgetWindowState(ws budget.WindowState) {
	if p.Status.TokenBudget == nil {
		p.Status.TokenBudget = &TokenBudgetStatus{}
	}
	t := metav1.NewTime(ws.WindowStart)
	p.Status.TokenBudget.WindowStart = &t
	p.Status.TokenBudget.WindowTokens = ws.WindowTokens
}

// BudgetSubscription maps the persisted Claude-subscription snapshot (Project
// status) into a budget.Subscription; the zero value when unset.
func (p *Project) BudgetSubscription() budget.Subscription {
	st := p.Status.TokenBudget
	if st == nil {
		return budget.Subscription{}
	}
	sub := budget.Subscription{
		FiveHourPercent: float64(st.FiveHourPercent),
		WeeklyPercent:   float64(st.WeeklyPercent),
	}
	if st.FiveHourReset != nil {
		sub.FiveHourReset = st.FiveHourReset.Time
	}
	if st.WeeklyReset != nil {
		sub.WeeklyReset = st.WeeklyReset.Time
	}
	return sub
}

// QueueSpec configures the in-operator agent-work admission queue.
type QueueSpec struct {
	// Capacity N: max concurrently-admitted normal-class events (defaults to
	// MaxConcurrentAgents, else 3).
	// +optional
	Capacity int `json:"capacity,omitempty"`
	// AlertCapacity M: reserved concurrent slots for alert-class events (default 1).
	// +optional
	AlertCapacity int `json:"alertCapacity,omitempty"`
}

// QueueCapacity resolves the normal-pool admission capacity (contract A.6,
// repointed from MaxConcurrentTasks to MaxConcurrentAgents). NOTE: this
// floors at 3 even when MaxConcurrentAgents == 0, so it must NEVER be used to
// implement the full-project pause kill switch - that is a direct
// proj.Spec.MaxConcurrentAgents == 0 check (see TestQueueCapacity_PauseMustNotUseFloor).
func (p *Project) QueueCapacity() int {
	if p.Spec.Queue != nil && p.Spec.Queue.Capacity > 0 {
		return p.Spec.Queue.Capacity
	}
	if p.Spec.MaxConcurrentAgents > 0 {
		return p.Spec.MaxConcurrentAgents
	}
	return 3
}

func (p *Project) AlertCapacity() int {
	if p.Spec.Queue != nil && p.Spec.Queue.AlertCapacity > 0 {
		return p.Spec.Queue.AlertCapacity
	}
	return 1
}

// ProjectStatus defines the observed state of a Project.
type ProjectStatus struct {
	// +optional
	WebhookURL string `json:"webhookURL,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// +optional
	Memory *MemoryStatus `json:"memory,omitempty"`
	// +optional
	Grafana *GrafanaStatus `json:"grafana,omitempty"`
	// +optional
	LastIssueScan *metav1.Time `json:"lastIssueScan,omitempty"`
	// +optional
	LastBrainstorm *metav1.Time `json:"lastBrainstorm,omitempty"`
	// LastDocumentation is the last time the documentation-sync cron ran; it bounds
	// the diff-since-last-doc window each tick computes per enrolled repo.
	// +optional
	LastDocumentation *metav1.Time `json:"lastDocumentation,omitempty"`
	// LastRefine is the last time the project's refine pre-step completed.
	// +optional
	LastRefine *metav1.Time `json:"lastRefine,omitempty"`
	// LastUpgrade is the last time the upgrade cron TICKED (not the last time an
	// upgrade completed): the stamp advances the schedule, so an upgrade Task
	// that never terminates must not refire the cron on every reconcile pass.
	// +optional
	LastUpgrade *metav1.Time `json:"lastUpgrade,omitempty"`
	// TokenBudget carries the token-budget accumulator/snapshot (issue #189).
	// +optional
	TokenBudget *TokenBudgetStatus `json:"tokenBudget,omitempty"`
	// ScanMarks are per-item high-water marks of the last GitHub activity the
	// issue/PR scans have accounted for. They survive Task GC so a long-handled
	// item is not re-triaged after its Task is reaped on operator restart.
	// Pruned each scan to the currently-open item set of the scanned repos.
	// +optional
	// +listType=map
	// +listMapKey=repo
	// +listMapKey=number
	ScanMarks []ScanMark `json:"scanMarks,omitempty"`
	// RepositoryCount is the number of Repository CRs whose spec.projectRef
	// names this Project. Computed on reconcile.
	// +optional
	RepositoryCount int `json:"repositoryCount,omitempty"`
	// OpenIssuesCount is the number of non-terminal issueLifecycle/clarify Tasks
	// for this project. Computed on reconcile.
	// +optional
	OpenIssuesCount int `json:"openIssuesCount,omitempty"`
	// OpenIncidentsCount is the number of non-terminal incident Tasks for this
	// project. Computed on reconcile.
	// +optional
	OpenIncidentsCount int `json:"openIncidentsCount,omitempty"`
	// BrainstormPausedAt is set when a brainstorm agent reports the idea space is
	// exhausted. While set, no brainstorm session is scheduled for this project.
	// +optional
	BrainstormPausedAt *metav1.Time `json:"brainstormPausedAt,omitempty"`
	// BrainstormPauseReason carries the agent's verbatim reason, for the operator
	// to read without opening the Task.
	// +optional
	BrainstormPauseReason string `json:"brainstormPauseReason,omitempty"`
	// LastMovementAt records the last time ANY brainstorm-resume trigger fired
	// for this project (push, merge, maintainer-comment, maintainer-close,
	// manual), stamped UNCONDITIONALLY - regardless of whether brainstorm was
	// paused at the time (I3 fix round).
	//
	// This is deliberately NOT the same signal as the AnnBrainstormResume
	// annotation, which StampBrainstormResume only ever writes on a project
	// that IS ALREADY paused (the reconcile's clearBrainstormPauseIfRequested
	// is its single consumer, and annotating an unpaused project would be an
	// unconditional write per webhook delivery for no scheduling effect). That
	// left a gap: a merge/push/maintainer trigger landing WHILE a brainstorm
	// session was in flight for a project that was NOT YET paused was silently
	// discarded, so the session's own eventual exhausted verdict could pause a
	// project that had already moved - the OPPOSITE of the design's intended
	// fail direction ("over-resumes rather than under-resumes"). The exhausted
	// outcome handler (internal/restapi/outcome.go) compares this field
	// against the session's own Task start time and refuses the pause when
	// movement is newer, so the annotation path stays paused-only while this
	// field is always live for that comparison.
	// +optional
	LastMovementAt *metav1.Time `json:"lastMovementAt,omitempty"`
}

// ScanMark records the last GitHub activity timestamp the issue/PR scan has
// accounted for on one item, keyed by (Repo, Number). It survives Task GC,
// letting a scan skip re-triaging an item that has had no new activity since it
// was last handled. IsPR scopes prune authority: issueScan prunes only issue
// marks; nothing currently prunes PR marks (mrScan, the only writer, was
// deleted in the 2026-07-13 redesign).
type ScanMark struct {
	Repo   string `json:"repo"`
	Number int    `json:"number"`
	// +optional
	IsPR bool `json:"isPR,omitempty"`
	// AccountedAt is the GitHub UpdatedAt the scan last accounted for.
	AccountedAt metav1.Time `json:"accountedAt"`
}

// TokenBudgetStatus carries the observed token-budget state for a Project
// (issue #189): the custom-window accumulator and the latest Claude-subscription
// snapshot reported by the wrapper.
type TokenBudgetStatus struct {
	// WindowStart is when the current custom-window opened (the most recent reset
	// boundary). WindowTokens is the total tokens spent in it so far.
	// +optional
	WindowStart *metav1.Time `json:"windowStart,omitempty"`
	// +optional
	WindowTokens int64 `json:"windowTokens,omitempty"`
	// FiveHourPercent / WeeklyPercent were the wrapper-reported Claude usage
	// percentages (whole percent, 0..100) for the rolling 5h and weekly windows.
	// Deprecated: no longer written (Task A8). Subscription state now lives only
	// in the fleet-wide account-usage store (poller-fed, issue #189 follow-up).
	// Retained on the CRD for backward compatibility with already-persisted
	// status; the gate no longer reads them.
	// +optional
	FiveHourPercent int `json:"fiveHourPercent,omitempty"`
	// Deprecated: see FiveHourPercent.
	// +optional
	FiveHourReset *metav1.Time `json:"fiveHourReset,omitempty"`
	// Deprecated: see FiveHourPercent.
	// +optional
	WeeklyPercent int `json:"weeklyPercent,omitempty"`
	// Deprecated: see FiveHourPercent.
	// +optional
	WeeklyReset *metav1.Time `json:"weeklyReset,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Webhook",type=string,JSONPath=`.status.webhookURL`
// +kubebuilder:printcolumn:name="Repos",type=integer,JSONPath=`.status.repositoryCount`
// +kubebuilder:printcolumn:name="OpenIssues",type=integer,JSONPath=`.status.openIssuesCount`
// +kubebuilder:printcolumn:name="OpenIncidents",type=integer,JSONPath=`.status.openIncidentsCount`

// Project is the top-level grouping for repositories and tasks.
type Project struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ProjectSpec   `json:"spec,omitempty"`
	Status ProjectStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ProjectList contains a list of Project.
type ProjectList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Project `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(GroupVersion, &Project{}, &ProjectList{})
		return nil
	})
}
