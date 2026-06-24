package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// Work-item role constants.
const (
	RoleProposed = "proposed"
	RoleSource   = "source"
	RoleCloses   = "closes"
	RoleOpenedPR = "openedPR"
	RoleReviewed = "reviewed"
)

// Work-item kind constants.
const (
	WorkItemIssue = "issue"
	WorkItemPR    = "pr"
)

// Work-item state constants.
const (
	WIProposed    = "proposed"
	WIApproved    = "approved"
	WIDeclined    = "declined"
	WIImplemented = "implemented"
	WIOpen        = "open"
	WIClosed      = "closed"
	WIMerged      = "merged"
)

// WorkItemRef is one SCM artifact tracked by a Task's work-item ledger.
type WorkItemRef struct {
	Provider string `json:"provider"`
	Repo     string `json:"repo"`
	// +optional
	Number int    `json:"number,omitempty"`
	Kind   string `json:"kind"`
	Role   string `json:"role"`
	// +optional
	State string `json:"state,omitempty"`
	// +optional
	Title string `json:"title,omitempty"`
	// +optional
	HeadSHA string `json:"headSHA,omitempty"`
	// +optional
	LastRefreshedAt *metav1.Time `json:"lastRefreshedAt,omitempty"`
}
