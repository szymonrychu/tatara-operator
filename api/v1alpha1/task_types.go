package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// ConditionApprovalApproved is set True once a human removes the approval label.
const ConditionApprovalApproved = "ApprovalApproved"

// ProposedIssueSpec is a tatara-proposed issue awaiting human approval.
type ProposedIssueSpec struct {
	RepositoryRef string `json:"repositoryRef"`
	Title         string `json:"title"`
	Body          string `json:"body"`
	// +kubebuilder:validation:Enum=bug;improvement
	Kind string `json:"kind"`
}

// Suggestion is one inline code suggestion on a PR/MR.
type Suggestion struct {
	Path string `json:"path"`
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
}

// ImplementOutcome is the agent's declared outcome for an implement task when
// it opens no PR (e.g. a deliberate refusal). Mirrors IssueOutcome.
type ImplementOutcome struct {
	// +kubebuilder:validation:Enum=declined
	Action string `json:"action"`
	Reason string `json:"reason"` // required; why no implementation
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
}

// TaskSpec defines the desired state of a Task.
type TaskSpec struct {
	ProjectRef    string `json:"projectRef"`
	RepositoryRef string `json:"repositoryRef"`
	Goal          string `json:"goal"`
	// +optional
	Source *TaskSource `json:"source,omitempty"`
	// +optional
	MaxTurns int `json:"maxTurns,omitempty"`
	// +kubebuilder:validation:Enum=implement;review;selfImprove;triageIssue;brainstorm;healthCheck;issueLifecycle
	// +kubebuilder:default="implement"
	// +optional
	Kind string `json:"kind,omitempty"`
	// +optional
	ApprovalRequired bool `json:"approvalRequired,omitempty"`
	// +optional
	ProposedIssue *ProposedIssueSpec `json:"proposedIssue,omitempty"`
}

// TaskStatus defines the observed state of a Task.
type TaskStatus struct {
	// +kubebuilder:validation:Enum=Pending;AwaitingApproval;Planning;Running;Succeeded;Failed
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
	// +optional
	CumulativeTokens int64 `json:"cumulativeTokens,omitempty"`
	// +optional
	LastTurnInputTokens int64 `json:"lastTurnInputTokens,omitempty"`
	// +optional
	LifecycleIterations int `json:"lifecycleIterations,omitempty"`
	// +optional
	Handover string `json:"handover,omitempty"`
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
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
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
