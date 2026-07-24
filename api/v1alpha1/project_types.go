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
	// MaxTurnsPerTask is the LIFETIME turn backstop across every pod of a
	// Task, for ALL agent kinds (contract A.6). Task.Spec.MaxTurnsPerTask
	// overrides this per-Task when set.
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
	// MaxTurnsPerPod bounds turns within ONE pod's life. The implement agent
	// kind is EXEMPT (a long, healthy coding run must not be cut off mid-pod;
	// the boot-crash and TTL watchdogs remain its runaway bounds).
	// +kubebuilder:default=40
	// +optional
	MaxTurnsPerPod int `json:"maxTurnsPerPod,omitempty"`
	// MaxReviewRounds bounds request_changes round-trips on a kind=review Task.
	// +kubebuilder:default=3
	// +optional
	MaxReviewRounds int `json:"maxReviewRounds,omitempty"`
	// MaxPodRecreations bounds boot-crash respawns of one Task's agent pod.
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
	// implement, incident, issueLifecycle, selfImprove) plus the "healthCheck"
	// pseudo-key: healthCheck shares Kind=brainstorm but is resolved against this
	// key first (falling back to the brainstorm entry when absent), letting
	// healthCheck's recurring classification work be tiered separately from
	// brainstorm's creative work. A missing or empty entry falls back to Model.
	// Values are authoritative model IDs (claude-opus-5, claude-sonnet-5).
	// +kubebuilder:validation:MaxProperties=11
	// +kubebuilder:validation:XValidation:rule="self.all(k, k in ['implement','review','clarify','triageIssue','brainstorm','issueLifecycle','incident','selfImprove','refine','healthCheck','documentation'])",message="modelByKind keys must be one of: implement, review, clarify, triageIssue, brainstorm, issueLifecycle, incident, selfImprove, refine, healthCheck, documentation"
	// +kubebuilder:validation:XValidation:rule="self.all(k, self[k].startsWith('claude-') && self[k].size() <= 64)",message="modelByKind values must be a claude model ID (start with 'claude-', max 64 chars)"
	// +optional
	ModelByKind map[string]string `json:"modelByKind,omitempty"`
	// EffortByKind overrides the project-wide Effort per Task Kind. Same keying as
	// ModelByKind (including the "healthCheck" pseudo-key); a missing or empty
	// entry falls back to Effort. Values are the effort enum (low|medium|high|xhigh|max).
	// +kubebuilder:validation:MaxProperties=11
	// +kubebuilder:validation:XValidation:rule="self.all(k, k in ['implement','review','clarify','triageIssue','brainstorm','issueLifecycle','incident','selfImprove','refine','healthCheck','documentation'])",message="effortByKind keys must be one of: implement, review, clarify, triageIssue, brainstorm, issueLifecycle, incident, selfImprove, refine, healthCheck, documentation"
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
	// documentation) plus the "*" wildcard, which is appended to every kind BEFORE
	// that kind's own entry. This is TRUSTED project config (maintainer-supplied
	// via helmfile), never user/issue text, so assignment.go may interpolate it.
	// +optional
	// +kubebuilder:validation:MaxProperties=12
	PromptAppendByKind map[string]string `json:"promptAppendByKind,omitempty"`
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
	// MaxOpenProposals caps the total open, unapproved agent proposals across
	// ALL repos in the project; at or above this the brainstorm cycle is
	// skipped. Default 5.
	// +kubebuilder:default=5
	// +optional
	MaxOpenProposals int `json:"maxOpenProposals,omitempty"`
	// StaleProposalDays configures the staleness reaper that auto-closes
	// bot-authored proposals with no human engagement (no human comment, no live
	// work) for at least that many days, clearing dead proposals out of the
	// MaxOpenProposals backlog. Semantics (liveness finding #8): a POSITIVE value
	// sets an explicit window; the UNSET default (0) enables the reaper with a
	// generous-but-finite default window (defaultStaleProposalDays) so un-approved
	// proposals do not accumulate unboundedly; a NEGATIVE value is the explicit
	// opt-out that disables the reaper entirely.
	// +optional
	StaleProposalDays int `json:"staleProposalDays,omitempty"`
	// +kubebuilder:validation:items:Enum=docs;memory;internet
	// +optional
	Sources []string `json:"sources,omitempty"`
}

// RefineActivity configures the cron-cycle refiner pre-step.
type RefineActivity struct {
	// ClosedLookbackDays bounds how far back closed issues are loaded for
	// already-implemented detection. Default 30 when zero.
	// +optional
	ClosedLookbackDays int `json:"closedLookbackDays,omitempty"`
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
	// Refine configures the project-refiner pre-step. No schedule: refine fires
	// off the existing scan cadence as a mandatory barrier before scans/brainstorm.
	// +optional
	Refine RefineActivity `json:"refine,omitempty"`
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
	// +kubebuilder:default=60
	// +optional
	ConversationIdleMinutes int `json:"conversationIdleMinutes,omitempty"`
	// ApprovalPhrases is the closed, per-project wordlist an approving
	// maintainer comment must match. The match is an ANCHORED WHOLE-LINE
	// match, NOT a substring (fix C3 / D-B, USER DECISION): some LINE of the
	// normalised body must match ^\s*(<phrase>)[\s.!]*$ - the comment must
	// CONSIST OF the phrase, not merely contain it. Substring matching meant
	// "I can't approve this until the tests pass" APPROVED - and because the
	// grammar takes the maintainer's MOST RECENT comment, their corrective
	// follow-up approved too. Anchored, "go ahead" approves and "don't go
	// ahead with this" does not. Empty means the DEFAULT list
	// (DefaultApprovalPhrases); it can NEVER mean "any text approves".
	// +optional
	// +kubebuilder:validation:MaxItems=20
	// +kubebuilder:validation:items:MinLength=2
	ApprovalPhrases []string `json:"approvalPhrases,omitempty"`
}

// DefaultApprovalPhrases is the closed wordlist used when a project does not
// configure ScmSpec.ApprovalPhrases. An empty configuration NEVER means "any
// text approves" (fix C3 / D-B) - it means this list.
func DefaultApprovalPhrases() []string {
	return []string{"lgtm", "approve", "approved", "ship it", "go ahead", "go", "implement it"}
}

// EffectiveApprovalPhrases returns the project's configured approval
// wordlist, or DefaultApprovalPhrases() when unset (a nil Scm block or an
// empty/absent ApprovalPhrases). It can NEVER return "match anything".
func EffectiveApprovalPhrases(p *Project) []string {
	if p.Spec.Scm != nil && len(p.Spec.Scm.ApprovalPhrases) > 0 {
		return p.Spec.Scm.ApprovalPhrases
	}
	return DefaultApprovalPhrases()
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
	Scm *ScmSpec `json:"scm,omitempty"`
	// +optional
	Grafana *GrafanaSpec `json:"grafana,omitempty"`
	// +optional
	Documentation *DocumentationSpec `json:"documentation,omitempty"`
	// +optional
	Queue *QueueSpec `json:"queue,omitempty"`
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
	// AutoApproveTataraProposals releases a bot-authored, tatara-proposed issue
	// (marked <!-- tatara-proposed-by:<kind> -->) straight into
	// implement->review->auto-merge->deploy without a second human gate: the
	// brainstorm/incident investigation that produced the proposal IS the review.
	// Never applies to a human-authored issue, marker or not - the bot-authorship
	// check is independent and mandatory - and never to a body edited since filing
	// (the Issue Spec.ProposalBodyHash anchor, set at mint from the SCM-unreachable
	// spec, must still match the current body's fingerprint).
	//
	// This field gates ONLY the approval carve-out, not the marker. Proposal filers
	// stamp the marker UNCONDITIONALLY, so flipping this flag off still changes the
	// stored issue body (the marker is present but inert) - intentional, so a later
	// flip to on can auto-approve proposals filed while it was off. Gate BEHAVIOR
	// with the flag off is exactly today's: the carve-out never fires, every
	// self-proposed chain parks identity-unverified until a human approves.
	// Defaults false; cluster-agnostic charts only flip this per-project via
	// helmfile enrollment values.
	// +kubebuilder:default=false
	// +optional
	AutoApproveTataraProposals bool `json:"autoApproveTataraProposals,omitempty"`
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
	// +kubebuilder:validation:MaxProperties=11
	// +kubebuilder:validation:XValidation:rule="self.all(k, self[k] >= 0 && self[k] <= 100)",message="spawnCeilingByKind values must be 0..100"
	// +kubebuilder:validation:XValidation:rule="self.all(k, k in ['implement','review','clarify','selfImprove','triageIssue','brainstorm','issueLifecycle','incident','healthCheck','refine','documentation'])",message="spawnCeilingByKind keys must be valid Task kinds"
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
