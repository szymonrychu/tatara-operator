package v1alpha1

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	QueueClassNormal = "normal"
	QueueClassAlert  = "alert"

	QueueStateQueued   = "Queued"
	QueueStateAdmitted = "Admitted"
	// QueueStateDone is retained for API/test compatibility. The dispatcher no
	// longer transitions events to Done; reconcileDone GC-deletes them instead.
	QueueStateDone = "Done"
)

// QueuedEventPayload is the Task blueprint a producer stashes; the dispatcher
// rebuilds the Task from it verbatim on admission. Producers keep ownership of
// label/goal/source construction so the dispatcher stays generic.
type QueuedEventPayload struct {
	Goal          string            `json:"goal,omitempty"`
	Kind          string            `json:"kind"`
	RepositoryRef string            `json:"repositoryRef,omitempty"`
	Source        *TaskSource       `json:"source,omitempty"`
	Labels        map[string]string `json:"labels,omitempty"`
	Annotations   map[string]string `json:"annotations,omitempty"`
	// GenerateName is used when Name is empty; Name is a fixed deterministic
	// Task name (issueLifecycle) that makes admission idempotent.
	GenerateName string `json:"generateName,omitempty"`
	Name         string `json:"name,omitempty"`
	// Provider + PodRepo feed agent.StampPodName on the rebuilt Task.
	Provider string `json:"provider,omitempty"`
	PodRepo  string `json:"podRepo,omitempty"`
	// +optional
	SystemicGroup *SystemicGroup `json:"systemicGroup,omitempty"`
	// AlertRule is carried from the incident webhook onto the built Task.
	// +optional
	AlertRule string `json:"alertRule,omitempty"`
	// DedupKey is carried from the incident webhook onto the built Task's
	// Spec.DedupKey (the incident dedup identity, item 6).
	// +optional
	DedupKey string `json:"dedupKey,omitempty"`
}

type QueuedEventSpec struct {
	Seq           int64              `json:"seq"`
	Class         string             `json:"class"`
	Kind          string             `json:"kind"`
	Autonomous    bool               `json:"autonomous,omitempty"`
	ProjectRef    string             `json:"projectRef"`
	RepositoryRef string             `json:"repositoryRef,omitempty"`
	DedupKey      string             `json:"dedupKey,omitempty"`
	Payload       QueuedEventPayload `json:"payload"`
}

type QueuedEventStatus struct {
	State      string       `json:"state,omitempty"`
	TaskRef    string       `json:"taskRef,omitempty"`
	AdmittedAt *metav1.Time `json:"admittedAt,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Seq",type=integer,JSONPath=`.spec.seq`
// +kubebuilder:printcolumn:name="Class",type=string,JSONPath=`.spec.class`
// +kubebuilder:printcolumn:name="Kind",type=string,JSONPath=`.spec.kind`
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`
type QueuedEvent struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              QueuedEventSpec   `json:"spec,omitempty"`
	Status            QueuedEventStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type QueuedEventList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []QueuedEvent `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(GroupVersion, &QueuedEvent{}, &QueuedEventList{})
		return nil
	})
}

// ValidateQueuedEventSpec mirrors ValidateTaskSpec's kind/repo-scoping rules.
func ValidateQueuedEventSpec(spec QueuedEventSpec) error {
	if spec.Seq <= 0 {
		return fmt.Errorf("queuedevent: seq must be positive")
	}
	if spec.Class != QueueClassNormal && spec.Class != QueueClassAlert {
		return fmt.Errorf("queuedevent: invalid class %q", spec.Class)
	}
	if spec.ProjectRef == "" {
		return fmt.Errorf("queuedevent: projectRef required")
	}
	if !IsKnownKind(spec.Kind) {
		return fmt.Errorf("queuedevent: invalid kind %q", spec.Kind)
	}
	if projectScopedKinds[spec.Kind] && spec.RepositoryRef != "" {
		return fmt.Errorf("queuedevent: kind %q is project-scoped, repositoryRef must be empty", spec.Kind)
	}
	if repoScopedKinds[spec.Kind] && spec.RepositoryRef == "" {
		return fmt.Errorf("queuedevent: kind %q requires repositoryRef", spec.Kind)
	}
	return nil
}
