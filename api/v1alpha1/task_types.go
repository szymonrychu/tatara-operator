package v1alpha1

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// SystemicGroup describes the systemic-improvement group a lead issue owns.
type SystemicGroup struct {
	SystemicID       string   `json:"systemicId"`
	SameRepoSiblings []int    `json:"sameRepoSiblings,omitempty"` // sibling issue numbers in THIS repo, closed by the lead PR
	CrossRepo        []string `json:"crossRepo,omitempty"`        // "owner/repo#N - title" references, context only
}

// ProposedIssueSpec is a tatara-proposed issue awaiting human approval.
type ProposedIssueSpec struct {
	RepositoryRef string `json:"repositoryRef"`
	Title         string `json:"title"`
	Body          string `json:"body"`
	// +kubebuilder:validation:Enum=bug;improvement
	Kind string `json:"kind"`
	// SystemicID correlates one of several issues opened for a single systemic
	// improvement. When set, createProposal stamps label tatara/systemic-<id>
	// and a sibling footer; the group counts as one against maxOpenProposals.
	// +optional
	SystemicID string `json:"systemicId,omitempty"`
	// Incident is true when this proposal was filed by an incident-investigation
	// agent; createProposal then adds the incident label to the tracker issue.
	// +optional
	Incident bool `json:"incident,omitempty"`
	// AlertGroup is the per-alert-group dedup identity of the incident that filed
	// this proposal: the tatara.dev/alert-group hash label of the in-flight
	// incident Task, falling back to its descriptive AlertRule name. createProposal
	// stamps tatara/alert-group-<hash> on the created incident issue and dedups
	// future incident proposals by it, so a recurring alert tracks onto its
	// existing open issue instead of spawning a near-duplicate. Empty for
	// non-incident proposals.
	// +optional
	AlertGroup string `json:"alertGroup,omitempty"`
}

// Suggestion is one inline code suggestion on a PR/MR.
type Suggestion struct {
	Path string `json:"path"`
	// +kubebuilder:validation:Minimum=1
	Line int    `json:"line"`
	Body string `json:"body"`
}

// ReviewVerdict is the agent's review decision for a human-authored PR/MR.
type ReviewVerdict struct {
	// +kubebuilder:validation:Enum=approve;request_changes;comment
	Decision string `json:"decision"`
	// +optional
	Body string `json:"body,omitempty"`
	// +optional
	Suggestions []Suggestion `json:"suggestions,omitempty"`
}

// PROutcome is the agent's outcome for a tatara-authored PR/MR.
type PROutcome struct {
	// +kubebuilder:validation:Enum=merge;close
	Action string `json:"action"`
	// +optional
	Reason string `json:"reason,omitempty"`
}

// IssueOutcome is the agent's outcome for an issue-triage task.
type IssueOutcome struct {
	// +kubebuilder:validation:Enum=implement;close;discuss
	Action string `json:"action"`
	// +optional
	Comment string `json:"comment,omitempty"` // required when Action==close or discuss
	// +optional
	Plan string `json:"plan,omitempty"` // short description of what will be implemented; posted as an implementation-start message when Action==implement
}

// ImplementOutcome is the agent's declared outcome for an implement task when
// it opens no PR (e.g. a deliberate refusal). Mirrors IssueOutcome.
type ImplementOutcome struct {
	// +kubebuilder:validation:Enum=declined;already_done
	Action string `json:"action"`
	Reason string `json:"reason"` // required; why no implementation
}

// BrainstormOutcome is the agent's declared outcome for a brainstorm task when
// it files no proposal (a deliberate early-exit). Mirrors ImplementOutcome.
type BrainstormOutcome struct {
	// +kubebuilder:validation:Enum=none
	Action string `json:"action"`
	Reason string `json:"reason"` // required; why nothing was proposed
}

// ChangeSummary holds the scope report submitted by the agent at the end of an
// Implement run via the change_summary MCP tool.
type ChangeSummary struct {
	// +optional
	PRTitle string `json:"prTitle,omitempty"`
	// +optional
	PRBody string `json:"prBody,omitempty"`
	// +optional
	DeliveredScope string `json:"deliveredScope,omitempty"`
	// +optional
	RemainingScope string `json:"remainingScope,omitempty"`
	// +optional
	MostProblematic string `json:"mostProblematic,omitempty"` // most problematic part of the change; from the cli most_problematic field
	// Significance is the agent's declared change significance, the lever the
	// push-CD cascade uses to cut the next semver tag (major resets minor+patch,
	// minor resets patch, patch increments). REQUIRED on the change_summary MCP
	// tool and at the REST layer (D2): an agent cannot open a PR without it. The
	// writeback gate refuses to open a change when this is empty.
	// +kubebuilder:validation:Enum=major;minor;patch
	// +optional
	Significance string `json:"significance,omitempty"`
}

// TaskSource records the SCM work-item that originated a webhook-born Task.
type TaskSource struct {
	// +kubebuilder:validation:Enum=github;gitlab
	Provider string `json:"provider"`
	IssueRef string `json:"issueRef"`
	// +optional
	URL string `json:"url,omitempty"`
	// +optional
	AuthorLogin string `json:"authorLogin,omitempty"`
	// +optional
	IsPR bool `json:"isPR,omitempty"`
	// +optional
	Number int `json:"number,omitempty"`
	// HeadSHA is the PR/MR head commit SHA captured at enqueue. It seeds the
	// review Task's role:reviewed ledger entry so same-head re-review dedup works
	// on the very next scan cycle, without waiting for the cron backstop to fill
	// it. Empty for issues.
	// +optional
	HeadSHA string `json:"headSHA,omitempty"`
	// Title is the originating issue/PR/MR title, captured at enqueue. Feeds the
	// branch slug (TaskBranch) and the no-agent PR-title fallback.
	// +optional
	Title string `json:"title,omitempty"`
	// DedupNumber is the linked-issue number for bot-PR tasks. When a bot MR
	// carries "Closes #N" in its body, this field holds N so the dedup logic can
	// match the task against the issue slot (not the PR number). Zero means the
	// task targets the item identified by Number (the PR/issue number itself).
	// +optional
	DedupNumber int `json:"dedupNumber,omitempty"`
}

// repoScopedKinds are task kinds that require a non-empty RepositoryRef.
var repoScopedKinds = map[string]bool{
	"implement":      true,
	"review":         true,
	"selfImprove":    true,
	"triageIssue":    true,
	"issueLifecycle": true,
}

// projectScopedKinds are task kinds that must have an empty RepositoryRef.
var projectScopedKinds = map[string]bool{
	"brainstorm":  true,
	"healthCheck": true,
	"incident":    true,
	"refine":      true,
}

// IsProjectScopedKind reports whether a task kind is project-scoped (operates on
// the whole Project, carries an empty RepositoryRef, and never opens a PR/MR).
func IsProjectScopedKind(kind string) bool {
	return projectScopedKinds[kind]
}

// ValidateTaskSpec validates the RepositoryRef contract for a TaskSpec:
//   - repo-scoped kinds require a non-empty RepositoryRef.
//   - project-scoped kinds require an empty RepositoryRef.
//
// Returns nil when valid. The CRD schema cannot express this kind-conditional
// rule (a field required for some kinds and forbidden for others), so the
// TaskReconciler calls this as a reconcile guard and terminates Tasks that
// violate the contract.
func ValidateTaskSpec(spec TaskSpec) error {
	kind := spec.Kind
	if kind == "" {
		kind = "implement" // matches +kubebuilder:default="implement"
	}
	if repoScopedKinds[kind] && spec.RepositoryRef == "" {
		return fmt.Errorf("task kind %q requires a non-empty repositoryRef", kind)
	}
	if projectScopedKinds[kind] && spec.RepositoryRef != "" {
		return fmt.Errorf("task kind %q must have an empty repositoryRef (project-scoped); got %q", kind, spec.RepositoryRef)
	}
	return nil
}

// TaskSpec defines the desired state of a Task.
type TaskSpec struct {
	ProjectRef string `json:"projectRef"`
	// +optional
	RepositoryRef string `json:"repositoryRef,omitempty"`
	Goal          string `json:"goal"`
	// +optional
	Source *TaskSource `json:"source,omitempty"`
	// +optional
	MaxTurns int `json:"maxTurns,omitempty"`
	// +kubebuilder:validation:Enum=implement;review;selfImprove;triageIssue;brainstorm;issueLifecycle;incident;healthCheck;refine
	// +kubebuilder:default="implement"
	// +optional
	Kind string `json:"kind,omitempty"`
	// ApprovalRequired is reserved for future use; no production code path reads
	// this field for any gating decision. Approval is driven by the SCM
	// conversation flow. Do not set this field expecting behavior - it has none.
	// +optional
	ApprovalRequired bool `json:"approvalRequired,omitempty"`
	// +optional
	ProposedIssue *ProposedIssueSpec `json:"proposedIssue,omitempty"`
	// ReposInScope is the optional declarative list of Project Repository CR
	// names this Task is expected to change. When set, the implement prompt tells
	// the agent the issue spans these repos and writeback posts a WARNING comment
	// for any in-scope repo whose branch produced no commits, instead of skipping
	// it silently. Absent/empty = single-repo behavior (primary repo only), so
	// existing Tasks are unaffected.
	// +optional
	ReposInScope []string `json:"reposInScope,omitempty"`
	// SystemicGroup, when set, marks this Task as the lead for a brainstorm
	// systemic group: it resolves SameRepoSiblings in one combined PR and is
	// aware of CrossRepo siblings (reference only).
	// +optional
	SystemicGroup *SystemicGroup `json:"systemicGroup,omitempty"`
	// AlertRule names the Grafana alert rule that produced an incident Task
	// (commonLabels.alertname, falling back to groupKey). Descriptive only; the
	// dedup key is the tatara.dev/alert-group hash label.
	// +optional
	AlertRule string `json:"alertRule,omitempty"`
}

// Task Phase string literals. Phases are bare strings on Status.Phase (there is
// no single central enum); these consts name the ones the push-CD cascade and
// terminal/active predicates key on so callers stop hand-typing them.
const (
	PhasePlanning  = "Planning"
	PhaseRunning   = "Running"
	PhaseSucceeded = "Succeeded"
	PhaseFailed    = "Failed"
	// PhaseDeploying is the pod-less post-merge phase: the implement PR has
	// auto-merged and the operator (not an agent pod) drives the deploy cascade
	// to tatara-helmfile-applied. It is non-terminal (TaskTerminal is false) and
	// MUST be excluded from per-repo lane occupancy: no pod runs, so counting it
	// against the lane re-creates the lane-starvation trap
	// (operator-laneoccupancy-starves-recovery-2026-06-15). It re-acquires a lane
	// only to spawn a fix agent.
	PhaseDeploying = "Deploying"
)

// TaskTerminal reports whether t has reached a terminal state, accounting for
// the dual Phase / LifecycleState design: issueLifecycle tasks leave Phase
// empty for their whole life and signal completion via LifecycleState. Any
// predicate that must treat finished lifecycle tasks as terminal MUST call
// this helper instead of testing Phase alone.
//
// PhaseDeploying is deliberately NOT terminal: a Task in Deploying is alive but
// pod-less (the operator polls the cascade), so conversation-GC / reaper / lane
// logic must treat it as live-but-podless, not finished.
func TaskTerminal(t *Task) bool {
	if t.Status.Phase == PhaseSucceeded || t.Status.Phase == PhaseFailed {
		return true
	}
	ls := t.Status.LifecycleState
	return ls == "Done" || ls == "Stopped" || ls == "Parked"
}

// TaskDeploying reports whether t is in the pod-less Deploying phase. Lane
// occupancy, reaper, and conversation-GC use this to treat it as a live work
// item that holds no execution lane (no agent pod runs during Deploying).
func TaskDeploying(t *Task) bool {
	return t.Status.Phase == PhaseDeploying
}

// IsRecoverableGiveup reports whether a Parked reason represents an
// implementation that gave up and may be re-rolled (vs a deliberate decline).
func IsRecoverableGiveup(reason string) bool {
	switch reason {
	case "implement-failed", "maxIterations", "refused-no-explanation", "deadline", "deploy-timeout":
		return true
	default:
		return false
	}
}

// TaskStatus defines the observed state of a Task.
type TaskStatus struct {
	// +kubebuilder:validation:Enum=Planning;Running;Succeeded;Failed;Deploying
	// NOTE: Pending and AwaitingApproval are intentionally absent: no code path
	// ever writes them (approval is now driven by the SCM conversation flow and
	// projected onto labels, not a Phase transition). They are removed here to
	// keep the CRD enum honest and prevent confusion with LifecycleState.
	// Deploying is the pod-less post-merge deploy-supervision phase (PhaseDeploying).
	// +optional
	Phase string `json:"phase,omitempty"`
	// +optional
	PodName string `json:"podName,omitempty"`
	// +optional
	TurnsCompleted int `json:"turnsCompleted,omitempty"`
	// +optional
	PrURL string `json:"prURL,omitempty"`
	// +optional
	ResultSummary string `json:"resultSummary,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// +optional
	DiscoveredIssues []string `json:"discoveredIssues,omitempty"`
	// +optional
	ReviewVerdict *ReviewVerdict `json:"reviewVerdict,omitempty"`
	// +optional
	PROutcome *PROutcome `json:"prOutcome,omitempty"`
	// +optional
	IssueOutcome *IssueOutcome `json:"issueOutcome,omitempty"`
	// +optional
	ImplementOutcome *ImplementOutcome `json:"implementOutcome,omitempty"`
	// +optional
	BrainstormOutcome *BrainstormOutcome `json:"brainstormOutcome,omitempty"`
	// +optional
	ChangeSummary *ChangeSummary `json:"changeSummary,omitempty"`
	// FollowupIssueURL is the URL of the follow-up issue opened when
	// ChangeSummary.RemainingScope is non-empty. Used as an idempotency guard to
	// prevent opening a second follow-up issue on re-entry.
	// +optional
	FollowupIssueURL string `json:"followupIssueURL,omitempty"`
	// +optional
	GateEnteredAt *metav1.Time `json:"gateEnteredAt,omitempty"`

	// Lifecycle fields (issueLifecycle kind only; empty on all other kinds).

	// +kubebuilder:validation:Enum=Triage;Conversation;Implement;MRCI;Merge;MainCI;Done;Stopped;Parked
	// +optional
	LifecycleState string `json:"lifecycleState,omitempty"`
	// +optional
	LastActivityAt *metav1.Time `json:"lastActivityAt,omitempty"`
	// +optional
	DeadlineAt *metav1.Time `json:"deadlineAt,omitempty"`
	// +optional
	HeadBranch string `json:"headBranch,omitempty"`
	// +optional
	PRNumber int `json:"prNumber,omitempty"`
	// +optional
	MergeCommitSHA string `json:"mergeCommitSHA,omitempty"`
	// MergedHeadSHA is the source-branch head commit SHA of the most recently
	// merged PR/MR. Recorded on a successful Merge and deliberately preserved
	// across clearMergedChangeState so the next MRCI cycle can detect a re-opened
	// PR that re-proposes the already-merged commits with no new fix (the
	// deterministic task branch is reused; if a post-merge re-implement does not
	// advance it, writeBackOpenChange opens a duplicate of the merged change).
	// +optional
	MergedHeadSHA string `json:"mergedHeadSHA,omitempty"`
	// ResolvedModel is the MODEL env resolved for this Task's agent pod at spawn
	// (modelForKind: per-kind override else project-wide). Stamped once at
	// pod-creation; read by the token/terminal metrics so $ is priced by the
	// model that actually ran. +optional
	ResolvedModel string `json:"resolvedModel,omitempty"`
	// +optional
	CumulativeTokens int64 `json:"cumulativeTokens,omitempty"`
	// +optional
	LastTurnInputTokens int64 `json:"lastTurnInputTokens,omitempty"`
	// CumulativeInput is the running total of uncached input tokens
	// (turnUsage.InputTokens) across all turns of this Task. +optional
	CumulativeInput int64 `json:"cumulativeInput,omitempty"`
	// CumulativeOutput is the running total of output tokens across all turns
	// of this Task. +optional
	CumulativeOutput int64 `json:"cumulativeOutput,omitempty"`
	// CumulativeCacheRead is the running total of cache-read input tokens
	// across all turns of this Task. +optional
	CumulativeCacheRead int64 `json:"cumulativeCacheRead,omitempty"`
	// CumulativeCacheCreation is the running total of cache-creation input
	// tokens across all turns of this Task. +optional
	CumulativeCacheCreation int64 `json:"cumulativeCacheCreation,omitempty"`
	// +optional
	LifecycleIterations int `json:"lifecycleIterations,omitempty"`
	// +optional
	Handover string `json:"handover,omitempty"`
	// ConversationObjectKey is the S3 object key under which the wrapper stores
	// and restores this Task's full Claude conversation transcript (issue #114).
	// Stable across lifecycle phases. Empty until conversation persistence is
	// configured and the first run has reported it (or a forked key is set for a
	// brainstorm-derived issue).
	// +optional
	ConversationObjectKey string `json:"conversationObjectKey,omitempty"`
	// SessionID is the Claude session id of the persisted conversation. The
	// operator passes it back to the next pod (as CONVERSATION_SESSION_ID) so a
	// fresh pod resumes via `claude --resume <id>` instead of starting empty.
	// +optional
	SessionID string `json:"sessionID,omitempty"`
	// ImplementContext is an optional re-entry prompt injected at the start of
	// the next Implement agent turn (e.g. CI failure details, conflict notice).
	// Cleared after the turn is submitted so a later fresh entry is clean.
	// +optional
	ImplementContext string `json:"implementContext,omitempty"`
	// ImplementEmptyRetries counts consecutive Implement runs that finished
	// with zero commits (no PR opened). Bounded retry guard: after the cap the
	// task is commented + parked with reason "implement-empty" instead of
	// silently parked as a benign no-change. Reset to 0 when a run opens a PR.
	// +optional
	ImplementEmptyRetries int `json:"implementEmptyRetries,omitempty"`
	// ImplementGiveUps counts implementation attempts that gave up for this
	// issue's durable lifecycle Task (transition Implement->Parked with a
	// recoverable reason). Bounds the auto-reroll backstop. +optional
	ImplementGiveUps int `json:"implementGiveUps,omitempty"`
	// WritebackSkip4xxAttempts counts consecutive writeback sweeps that opened
	// no PR because every project repo returned a permanent 4xx from OpenChange
	// (issue #166: the un-triageable 4xx-skip loop). Bounded loop-breaker: once
	// it reaches writebackSkip4xxCap the writeback gate stops re-sweeping the SCM
	// and records a terminal WritebackFailed condition instead of churning the
	// SCM API every reconcile. Reset to 0 when a PR opens.
	// +optional
	WritebackSkip4xxAttempts int `json:"writebackSkip4xxAttempts,omitempty"`
	// PendingComments are free-form comments queued by the agent via the
	// comment MCP tool, posted to the task's linked issue on the next
	// reconcile and then cleared. Does not change the lifecycle state.
	// +optional
	PendingComments []string `json:"pendingComments,omitempty"`
	// PendingInterjections are comment bodies queued by the webhook when a new
	// issue/MR comment arrives while an agent turn is in flight. The reconciler
	// delivers each to the live wrapper session (as mid-session user input) and
	// then clears them. Does not change the lifecycle state.
	// +optional
	PendingInterjections []string `json:"pendingInterjections,omitempty"`
	// WorkItems is the work-item ledger: every SCM artifact (issues, PRs,
	// proposals) this Task spans. Carried as the single source of truth for
	// dedup, stall recovery, and prompt-building. Seeded lazily from Spec.Source
	// on first reconcile; maintained by the operator as the agent drives actions
	// via MCP tools.
	// +optional
	WorkItems []WorkItemRef `json:"workItems,omitempty"`
	// ParkReason is the reason string passed to the last Parked transition.
	// Cleared when the Task transitions out of Parked. Carried for context and
	// observability; does NOT gate re-activation.
	// +optional
	ParkReason string `json:"parkReason,omitempty"`

	// Deploy-supervision fields (PhaseDeploying only; empty otherwise). The
	// implement Task does not go terminal at PR-merge: it enters Deploying and
	// the operator drives the push-CD cascade to a tatara-helmfile apply, then
	// resolves Done + closes the originating issue.

	// DeployDeadline is the wall-clock deadline for the deploy cascade
	// (now + Project deployBudgetSeconds, single-hop override applied per
	// artifact). On exceed, the Task parks recoverable with reason deploy-timeout.
	// +optional
	DeployDeadline *metav1.Time `json:"deployDeadline,omitempty"`
	// CascadeStage tracks how far this Task's artifact has propagated toward the
	// terminal tatara-helmfile apply.
	// +kubebuilder:validation:Enum=tagged;parent-pr-open;parent-merged;helmfile-applied
	// +optional
	CascadeStage string `json:"cascadeStage,omitempty"`
	// DeployedVersion is the semver (vX.Y.Z) this Task's artifact published and is
	// driving toward the cluster.
	// +optional
	DeployedVersion string `json:"deployedVersion,omitempty"`
	// DeployArtifact is the deploy-ledger artifact identity (repo@vX.Y.Z) this
	// Task records, the key the apply-outcome sweep matches against applied pins.
	// +optional
	DeployArtifact string `json:"deployArtifact,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Lifecycle",type=string,JSONPath=`.status.lifecycleState`
// +kubebuilder:printcolumn:name="Kind",type=string,JSONPath=`.spec.kind`
// +kubebuilder:printcolumn:name="Turns",type=integer,JSONPath=`.status.turnsCompleted`

// Task is one agent session driving a Repository toward a goal.
type Task struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TaskSpec   `json:"spec,omitempty"`
	Status TaskStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// TaskList contains a list of Task.
type TaskList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Task `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(GroupVersion, &Task{}, &TaskList{})
		return nil
	})
}
