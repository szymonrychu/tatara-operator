package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TaskSource records the SCM work-item that originated a webhook-born Task.
type TaskSource struct {
	// +kubebuilder:validation:Enum=github;gitlab
	Provider string `json:"provider"`
	IssueRef string `json:"issueRef"`
	// +optional
	URL string `json:"url,omitempty"`
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
}

// TaskStatus defines the observed state of a Task.
type TaskStatus struct {
	// +kubebuilder:validation:Enum=Pending;Planning;Running;Succeeded;Failed
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
	SchemeBuilder.Register(&Task{}, &TaskList{})
}
