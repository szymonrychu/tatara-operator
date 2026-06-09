package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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
	// ReingestSchedule is a standard 5-field cron expression (e.g. "0 6 * * *")
	// that triggers a periodic catch-up re-ingest in addition to push webhooks.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=9
	// +kubebuilder:validation:Pattern=`^(\S+\s+){4}\S+$`
	ReingestSchedule string `json:"reingestSchedule"`
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
	// LastScheduledReingest is the last time the cron schedule stamped a
	// reingest-requested annotation; used as the base for the next fire.
	// +optional
	LastScheduledReingest *metav1.Time `json:"lastScheduledReingest,omitempty"`
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
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(GroupVersion, &Repository{}, &RepositoryList{})
		return nil
	})
}
