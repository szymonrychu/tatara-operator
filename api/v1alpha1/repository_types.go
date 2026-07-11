package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// BoolVal returns the value of a *bool field, or def when the pointer is nil.
// Use this to dereference IngestEnabled, SemanticIngest, and similar
// +kubebuilder:default=true pointer fields without risking a nil dereference.
func BoolVal(b *bool, def bool) bool {
	if b == nil {
		return def
	}
	return *b
}

// RepositorySpec defines the desired state of a Repository.
type RepositorySpec struct {
	ProjectRef string `json:"projectRef"`
	URL        string `json:"url"`
	// +kubebuilder:default="main"
	// +optional
	DefaultBranch string `json:"defaultBranch,omitempty"`
	// +kubebuilder:default=true
	// +optional
	IngestEnabled *bool `json:"ingestEnabled,omitempty"`
	// SemanticIngest enables Phase 2 LLM semantic extraction for this
	// repository's ingest Job. Defaults true; set false to run AST-only
	// ingest and avoid per-changed-file LLM cost.
	// +kubebuilder:default=true
	// +optional
	SemanticIngest *bool `json:"semanticIngest,omitempty"`
	// ReingestSchedule is a standard 5-field cron expression (e.g. "0 6 * * *")
	// that triggers a periodic catch-up re-ingest in addition to push webhooks.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=9
	// +kubebuilder:validation:Pattern=`^(\S+\s+){4}\S+$`
	ReingestSchedule string `json:"reingestSchedule"`
	// ReporterLogins, when non-nil, overrides the Project's ScmSpec.ReporterLogins
	// intake allowlist for THIS repository (issue #102). A nil value inherits the
	// project list; an explicit empty list ([]) opens intake for this repo only.
	// +optional
	ReporterLogins *[]string `json:"reporterLogins,omitempty"`
	// MaintainerLogins, when non-nil, overrides the Project's
	// ScmSpec.MaintainerLogins (the unified maintainer/approver set) for THIS
	// repository (issue #102). A nil value inherits the project list; an explicit
	// empty list ([]) clears the maintainer/approver set for this repo only.
	// +optional
	MaintainerLogins *[]string `json:"maintainerLogins,omitempty"`
}

// RepositoryStatus defines the observed state of a Repository.
type RepositoryStatus struct {
	// +kubebuilder:validation:Enum=Ingesting;Ingested;Failed
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
	// IngestFailureCount tracks consecutive ingest Job failures for exponential
	// backoff between re-creations.
	// +optional
	IngestFailureCount int `json:"ingestFailureCount,omitempty"`
	// LastIngestFailureTime is the timestamp of the most recent ingest Job
	// failure; used alongside IngestFailureCount to compute backoff.
	// +optional
	LastIngestFailureTime *metav1.Time `json:"lastIngestFailureTime,omitempty"`
	// +optional
	JobName string `json:"jobName,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// OpenIssuesCount is the number of non-terminal issueLifecycle/clarify Tasks
	// scoped to this repo. Computed on reconcile.
	// +optional
	OpenIssuesCount int `json:"openIssuesCount,omitempty"`
	// OpenIncidentsCount is the number of non-terminal incident Tasks scoped to
	// this repo. Computed on reconcile.
	// +optional
	OpenIncidentsCount int `json:"openIncidentsCount,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Project",type=string,JSONPath=`.spec.projectRef`
// +kubebuilder:printcolumn:name="URL",type=string,JSONPath=`.spec.url`,priority=1
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Commit",type=string,JSONPath=`.status.lastIngestedCommit`,priority=1
// +kubebuilder:printcolumn:name="OpenIssues",type=integer,JSONPath=`.status.openIssuesCount`
// +kubebuilder:printcolumn:name="OpenIncidents",type=integer,JSONPath=`.status.openIncidentsCount`

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
