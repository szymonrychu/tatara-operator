package v1alpha1

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// MergeRequestName returns the CR name for a MergeRequest: mr-<repositoryRef>-<number>.
func MergeRequestName(repoRef string, number int) string {
	return fmt.Sprintf("mr-%s-%d", repoRef, number)
}

// MergeRequestSpec defines the desired state of a MergeRequest.
type MergeRequestSpec struct {
	RepositoryRef string `json:"repositoryRef"`
	// +kubebuilder:validation:Minimum=1
	Number     int    `json:"number"`
	URL        string `json:"url"`
	ProjectRef string `json:"projectRef"`
}

// MergeRequestStatus defines the observed state of a MergeRequest.
type MergeRequestStatus struct {
	// +optional
	Title string `json:"title,omitempty"`
	// +optional
	Author string `json:"author,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxLength=65536
	Body string `json:"body,omitempty"`
	// +optional
	CreatedAt *metav1.Time `json:"createdAt,omitempty"`
	// +optional
	UpdatedAt *metav1.Time `json:"updatedAt,omitempty"`
	// +kubebuilder:validation:Enum=open;merged;closed
	// +optional
	State string `json:"state,omitempty"`
	// Status is the platform's review state. OPERATOR-OWNED: written only from
	// an ACCEPTED review submit_outcome (C.5). No agent writes it.
	// +kubebuilder:validation:Enum=new;approved;needs-changes;rejected
	// +optional
	Status string `json:"status,omitempty"`
	// +optional
	HeadBranch string `json:"headBranch,omitempty"`
	// HeadSHA is the MIRROR's last-synced head. It is NEVER trusted for a merge
	// or an approval decision: both re-fetch the head LIVE (fix 10).
	// +optional
	HeadSHA string `json:"headSHA,omitempty"`
	// +kubebuilder:validation:Enum=none;pending;running;green;red
	// +optional
	CIStatus string `json:"ciStatus,omitempty"`
	// +optional
	Mergeable bool `json:"mergeable,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxItems=200
	Comments []Comment `json:"comments,omitempty"`
	// +optional
	CommentCount int `json:"commentCount,omitempty"`
	// SpilledComments / SpilledCommentsRefs: the MR needs the SAME spill guard the
	// Issue has (fix E1). v2 gave it none - and a PR with 5 review rounds x 30
	// inline findings is this platform's own NORMAL output.
	// SpilledCommentsRefs ACCUMULATES (fix M19): each spill batch appends its own
	// tatara-memory track_id. A single scalar ref silently orphaned every earlier
	// batch on the second spill.
	// +optional
	SpilledComments int `json:"spilledComments,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxItems=50
	SpilledCommentsRefs []string `json:"spilledCommentsRefs,omitempty"`
	// CommentsRetainedFrom is the eviction watermark (fix M18). Same contract as
	// Issue.status.commentsRetainedFrom.
	// +optional
	CommentsRetainedFrom *metav1.Time `json:"commentsRetainedFrom,omitempty"`
	// +optional
	MergedAt *metav1.Time `json:"mergedAt,omitempty"`
	// +optional
	DeployedAt *metav1.Time `json:"deployedAt,omitempty"`
	// +optional
	DeployedVersion string `json:"deployedVersion,omitempty"`
	// Significance is IMPLEMENT-OWNED (fix 12): written once from the implement
	// Task's submit_outcome. A review outcome may only ESCALATE it (max on the
	// ordering patch < minor < major); an attempt to LOWER it is ignored and
	// logged WARN. The in-cluster reviewer is documented-flaky and must never be
	// able to downgrade a major release to a patch.
	// +kubebuilder:validation:Enum=major;minor;patch
	// +optional
	Significance string `json:"significance,omitempty"`
	// ReviewedSHA is the LIVE head SHA read at the moment the review outcome was
	// ACCEPTED. Merge passes it to SCMWriter.Merge as the expected head; a 409
	// means the head moved (fix 10) -> Status=new, stage back to reviewing.
	// +optional
	ReviewedSHA string `json:"reviewedSHA,omitempty"`
	// ReviewRounds counts ACCEPTED request_changes verdicts on this MR. At
	// Project.spec.agent.maxReviewRounds the Task parks (review-loop-exhausted).
	// +optional
	ReviewRounds int `json:"reviewRounds,omitempty"`
	// PendingReview is the durable intent the MergeRequest reconciler drains
	// (fix M8; the mechanism is H3-5 + V6-4). Non-nil means "a review is owed to
	// the forge". THE STAGE MACHINE IS GATED ON IT BEING NIL (F.3), so a pod can
	// never be spawned to fix findings that have not been recorded yet.
	// +optional
	PendingReview *PendingReview `json:"pendingReview,omitempty"`
	// PendingComments are durable comment/reply intents (fix M8), drained by the
	// same reconciler.
	// +optional
	// +kubebuilder:validation:MaxItems=20
	PendingComments []PendingComment `json:"pendingComments,omitempty"`
	// +optional
	LastSyncedAt *metav1.Time `json:"lastSyncedAt,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Task",type=string,JSONPath=`.metadata.ownerReferences[?(@.controller==true)].name`
// +kubebuilder:printcolumn:name="Repo",type=string,JSONPath=`.spec.repositoryRef`
// +kubebuilder:printcolumn:name="Num",type=integer,JSONPath=`.spec.number`
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=`.status.status`
// +kubebuilder:printcolumn:name="CI",type=string,JSONPath=`.status.ciStatus`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// MergeRequest is a mirror of a forge PR/MR, owned by the Task that is
// working it.
type MergeRequest struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MergeRequestSpec   `json:"spec,omitempty"`
	Status MergeRequestStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MergeRequestList contains a list of MergeRequest.
type MergeRequestList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MergeRequest `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(GroupVersion, &MergeRequest{}, &MergeRequestList{})
		return nil
	})
}
