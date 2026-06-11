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
	// +kubebuilder:validation:Enum=implement;close
	Action string `json:"action"`
	// +optional
	Comment string `json:"comment,omitempty"` // required when Action==close
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
	// +kubebuilder:validation:Enum=implement;review;selfImprove;triageIssue;brainstorm
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
	GateEnteredAt *metav1.Time `json:"gateEnteredAt,omitempty"`
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
