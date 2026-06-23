package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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

// AgentSpec configures the wrapper agent session a Task runs.
type AgentSpec struct {
	// +optional
	Model string `json:"model,omitempty"`
	// +optional
	Image string `json:"image,omitempty"`
	// +kubebuilder:default="bypassPermissions"
	// +optional
	PermissionMode string `json:"permissionMode,omitempty"`
	// +kubebuilder:default=50
	// +optional
	MaxTurnsPerTask int `json:"maxTurnsPerTask,omitempty"`
	// TurnTimeoutSeconds is the per-turn stall (inactivity) window in seconds: a
	// turn is failed only after this long with no agent activity, not at a fixed
	// wall-clock age, so a turn that keeps streaming output is not killed mid-work.
	// The name is kept for CRD compatibility.
	// +kubebuilder:default=1800
	// +optional
	TurnTimeoutSeconds int `json:"turnTimeoutSeconds,omitempty"`
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

// CronActivity schedules one Project scan activity (mrScan or issueScan).
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
	// +kubebuilder:validation:items:Enum=docs;memory;internet
	// +optional
	Sources []string `json:"sources,omitempty"`
}

// HealthCheckActivity schedules the opt-in periodic project-health-check scan:
// a sibling to brainstorm that surveys repo health (CI failures, coverage gaps,
// code to simplify, pipeline steps to add, other tech-debt) and proposes one
// targeted discovery issue per cycle via the tatara-health-check skill.
type HealthCheckActivity struct {
	// +optional
	Enabled bool `json:"enabled,omitempty"`
	// +kubebuilder:validation:Pattern=`^$|^(\S+\s+){4}\S+$`
	// +optional
	Schedule string `json:"schedule,omitempty"`
	// MaxOpenProposals caps the total open, unapproved agent proposals across
	// ALL repos in the project; at or above this the health-check cycle is
	// skipped. Default 5.
	// +kubebuilder:default=5
	// +optional
	MaxOpenProposals int `json:"maxOpenProposals,omitempty"`
	// +kubebuilder:validation:items:Enum=docs;memory;internet
	// +optional
	Sources []string `json:"sources,omitempty"`
}

// ScmCron groups the cron-driven scan activities.
type ScmCron struct {
	// +optional
	MRScan CronActivity `json:"mrScan,omitempty"`
	// +optional
	IssueScan CronActivity `json:"issueScan,omitempty"`
	// +optional
	Brainstorm BrainstormActivity `json:"brainstorm,omitempty"`
	// +optional
	HealthCheck HealthCheckActivity `json:"healthCheck,omitempty"`
}

// ScmSpec binds a Project to one SCM provider and its board/merge policy.
type ScmSpec struct {
	// +kubebuilder:validation:Enum=github;gitlab
	Provider string `json:"provider"`
	Owner    string `json:"owner"`
	BotLogin string `json:"botLogin"`
	// MaintainerLogins are the human maintainer accounts. They are the unified
	// trusted-insider AND approver set (issue #102): together with BotLogin they
	// form the "trusted insider" set used for issue #56 autoapprove, and - when
	// non-empty - a thread comment counts as the human approval go-ahead only if
	// its author is in this list. Empty preserves the historical behavior (any
	// non-bot human reply releases the self-approve hold; only BotLogin is
	// excluded from #56 autoapprove). Overridable per-repository via
	// RepositorySpec.MaintainerLogins.
	// +optional
	MaintainerLogins []string `json:"maintainerLogins,omitempty"`
	// ReporterLogins gates issue/issue-comment intake (issue #102). When non-empty
	// the operator only acts on issues and issue-comments authored by the bot, a
	// maintainer, or an account in this list; everything else is dropped at intake
	// (cron scan and webhook) so unknown third parties cannot drive the lifecycle
	// via prompt injection. Empty preserves the historical open behavior (any
	// author is accepted). Overridable per-repository via
	// RepositorySpec.ReporterLogins.
	// +optional
	ReporterLogins []string `json:"reporterLogins,omitempty"`
	// +optional
	Board *BoardSpec `json:"board,omitempty"`
	// +kubebuilder:validation:Enum=afterApproval;autoMergeOnGreenCI
	// +kubebuilder:default="afterApproval"
	// +optional
	MergePolicy string `json:"mergePolicy,omitempty"`
	// +kubebuilder:validation:Enum=labeledOrMentioned;all
	// +kubebuilder:default="labeledOrMentioned"
	// +optional
	PRReactionScope string `json:"prReactionScope,omitempty"`
	// ApprovalLabel is DEPRECATED and no longer used: approval is now driven by
	// the conversation (the triage agent reads the thread) and projected onto the
	// idea/approved/rejected labels below. Kept only for migration tooling.
	// +kubebuilder:default="tatara/awaiting-approval"
	// +optional
	ApprovalLabel string `json:"approvalLabel,omitempty"`
	// IdeaLabel is DEPRECATED (legacy alias for BrainstormingLabel); kept for lazy migration.
	// +kubebuilder:default="tatara-idea"
	// +optional
	IdeaLabel string `json:"ideaLabel,omitempty"`
	// ApprovedLabel marks an issue approved for implementation.
	// +kubebuilder:default="tatara-approved"
	// +optional
	ApprovedLabel string `json:"approvedLabel,omitempty"`
	// RejectedLabel is DEPRECATED (legacy alias for DeclinedLabel); kept for lazy migration.
	// +kubebuilder:default="tatara-rejected"
	// +optional
	RejectedLabel string `json:"rejectedLabel,omitempty"`
	// BrainstormingLabel marks an issue tatara is triaging / discussing (pre-approval).
	// +kubebuilder:default="tatara-brainstorming"
	// +optional
	BrainstormingLabel string `json:"brainstormingLabel,omitempty"`
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
}

// ProjectSpec defines the desired state of a Project.
type ProjectSpec struct {
	ScmSecretRef string `json:"scmSecretRef"`
	// +kubebuilder:default="tatara"
	// +optional
	TriggerLabel string `json:"triggerLabel,omitempty"`
	// +kubebuilder:default=3
	// +optional
	MaxConcurrentTasks int `json:"maxConcurrentTasks,omitempty"`
	// MaxOpenTasks: Deprecated: no longer enforced. The queue bounds CONCURRENCY
	// (QueueCapacity), not event creation; over-limit events wait in Queued.
	// Retained for CRD backward-compatibility; ignored.
	// +kubebuilder:default=3
	// +optional
	MaxOpenTasks int `json:"maxOpenTasks,omitempty"`
	// +optional
	Agent AgentSpec `json:"agent,omitempty"`
	// +optional
	Memory *MemorySpec `json:"memory,omitempty"`
	// +optional
	Scm *ScmSpec `json:"scm,omitempty"`
	// +optional
	Grafana *GrafanaSpec `json:"grafana,omitempty"`
	// +optional
	Queue *QueueSpec `json:"queue,omitempty"`
}

// QueueSpec configures the in-operator agent-work admission queue.
type QueueSpec struct {
	// Capacity N: max concurrently-admitted normal-class events (defaults to
	// MaxConcurrentTasks, else 3).
	// +optional
	Capacity int `json:"capacity,omitempty"`
	// AlertCapacity M: reserved concurrent slots for alert-class events (default 1).
	// +optional
	AlertCapacity int `json:"alertCapacity,omitempty"`
	// QueuedAutonomousCap: Deprecated: no longer enforced. The queue bounds CONCURRENCY
	// (QueueCapacity), not event creation; over-limit events wait in Queued.
	// Retained for CRD backward-compatibility; ignored.
	// +optional
	QueuedAutonomousCap int `json:"queuedAutonomousCap,omitempty"`
}

func (p *Project) QueueCapacity() int {
	if p.Spec.Queue != nil && p.Spec.Queue.Capacity > 0 {
		return p.Spec.Queue.Capacity
	}
	if p.Spec.MaxConcurrentTasks > 0 {
		return p.Spec.MaxConcurrentTasks
	}
	return 3
}

func (p *Project) AlertCapacity() int {
	if p.Spec.Queue != nil && p.Spec.Queue.AlertCapacity > 0 {
		return p.Spec.Queue.AlertCapacity
	}
	return 1
}

func (p *Project) QueuedAutonomousCap() int {
	if p.Spec.Queue != nil && p.Spec.Queue.QueuedAutonomousCap > 0 {
		return p.Spec.Queue.QueuedAutonomousCap
	}
	if p.Spec.MaxOpenTasks > 0 {
		return p.Spec.MaxOpenTasks
	}
	return 3
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
	LastMRScan *metav1.Time `json:"lastMRScan,omitempty"`
	// +optional
	LastIssueScan *metav1.Time `json:"lastIssueScan,omitempty"`
	// +optional
	LastBrainstorm *metav1.Time `json:"lastBrainstorm,omitempty"`
	// +optional
	LastHealthCheck *metav1.Time `json:"lastHealthCheck,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Webhook",type=string,JSONPath=`.status.webhookURL`

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
