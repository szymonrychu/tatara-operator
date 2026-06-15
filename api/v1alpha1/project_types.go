package v1alpha1

import (
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
	// +kubebuilder:default=1800
	// +optional
	TurnTimeoutSeconds int `json:"turnTimeoutSeconds,omitempty"`
	// +kubebuilder:default=200000
	// +optional
	ContextWindowTokens int `json:"contextWindowTokens,omitempty"`
	// +kubebuilder:default=50
	// +optional
	HandoverThresholdPercent int `json:"handoverThresholdPercent,omitempty"`
	// +kubebuilder:default=10
	// +optional
	MaxLifecycleIterations int `json:"maxLifecycleIterations,omitempty"`
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
	// +optional
	Schedule string `json:"schedule,omitempty"`
	// MaxPerRepo caps the number of in-progress Tasks per repo (one lane per repo).
	// +kubebuilder:default=1
	// +optional
	MaxPerRepo int `json:"maxPerRepo,omitempty"`
}

// BrainstormActivity schedules the opt-in self-driven issue-proposal scan.
type BrainstormActivity struct {
	// +optional
	Enabled bool `json:"enabled,omitempty"`
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
	// MaintainerLogins are the human maintainer accounts. Together with BotLogin
	// they form the "trusted insider" set: issues opened by anyone OUTSIDE this
	// set (third-party contributors) are autoapproved through triage without the
	// self-approve hold. Empty means only BotLogin is excluded from autoapprove.
	// +optional
	MaintainerLogins []string `json:"maintainerLogins,omitempty"`
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
	// MaxOpenTasks is the hard ceiling on non-terminal Tasks the operator will
	// autonomously create for this Project (cron scans + brainstorm). At the cap,
	// scan/brainstorm creation is skipped until open Tasks finish. The reactive
	// webhook path is exempt (human-initiated).
	// +kubebuilder:default=3
	// +optional
	MaxOpenTasks int `json:"maxOpenTasks,omitempty"`
	// +optional
	Agent AgentSpec `json:"agent,omitempty"`
	// +optional
	Memory *MemorySpec `json:"memory,omitempty"`
	// +optional
	Scm *ScmSpec `json:"scm,omitempty"`
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
