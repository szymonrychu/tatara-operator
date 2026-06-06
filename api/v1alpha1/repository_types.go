package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RepositorySpec defines the desired state of a Repository.
type RepositorySpec struct {
	ProjectRef string `json:"projectRef"`
	URL        string `json:"url"`
	// +kubebuilder:default="main"
	// +optional
	DefaultBranch string `json:"defaultBranch,omitempty"`
	// +kubebuilder:default=true
	// +optional
	IngestEnabled bool `json:"ingestEnabled,omitempty"`
}

// RepositoryStatus defines the observed state of a Repository.
type RepositoryStatus struct {
	// +kubebuilder:validation:Enum=Pending;Ingesting;Ingested;Failed
	// +optional
	Phase string `json:"phase,omitempty"`
	// +optional
	LastIngestedCommit string `json:"lastIngestedCommit,omitempty"`
	// +optional
	LastIngestTime *metav1.Time `json:"lastIngestTime,omitempty"`
	// +optional
	JobName string `json:"jobName,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Commit",type=string,JSONPath=`.status.lastIngestedCommit`

// Repository is a git remote ingested into tatara-memory for a Project.
type Repository struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RepositorySpec   `json:"spec,omitempty"`
	Status RepositoryStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// RepositoryList contains a list of Repository.
type RepositoryList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Repository `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Repository{}, &RepositoryList{})
}
