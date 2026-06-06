package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SubtaskSpec defines the desired state of a Subtask. Created/updated by the
// agent via the REST API and consumed by the Task reconciler's turn loop.
type SubtaskSpec struct {
	TaskRef string `json:"taskRef"`
	Title   string `json:"title"`
	// +optional
	Detail string `json:"detail,omitempty"`
	// +optional
	Order int `json:"order,omitempty"`
}

// SubtaskStatus defines the observed state of a Subtask.
type SubtaskStatus struct {
	// +kubebuilder:validation:Enum=Pending;Running;Done;Failed
	// +optional
	Phase string `json:"phase,omitempty"`
	// +optional
	TurnID string `json:"turnId,omitempty"`
	// +optional
	Result string `json:"result,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Order",type=integer,JSONPath=`.spec.order`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`

// Subtask is a unit of work fed to a Task's agent session one turn at a time.
type Subtask struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SubtaskSpec   `json:"spec,omitempty"`
	Status SubtaskStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SubtaskList contains a list of Subtask.
type SubtaskList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Subtask `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Subtask{}, &SubtaskList{})
}
