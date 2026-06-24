package v1alpha1

import (
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

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

// RepoFromIssueRef extracts the "owner/repo" part from an IssueRef like
// "owner/repo#N" or "owner/repo!N". Returns "" when the ref is unparseable.
func RepoFromIssueRef(issueRef string) string {
	idx := strings.LastIndexAny(issueRef, "#!")
	if idx <= 0 {
		return ""
	}
	return issueRef[:idx]
}

// TaskMatchesItem reports whether the Task's seed identity (Spec.Source:
// repo from IssueRef, number = DedupNumber if set else Number) OR any ledger
// entry matches the given (repo, number). For Tasks created before the ledger
// (no Spec.Source) it falls back to the legacy source-repo/source-number labels
// so the ~1148 existing Tasks remain matched during the rollout period.
func TaskMatchesItem(t *Task, repo string, number int) bool {
	if s := t.Spec.Source; s != nil {
		srcRepo := RepoFromIssueRef(s.IssueRef)
		dedupNum := s.DedupNumber
		if dedupNum == 0 {
			dedupNum = s.Number
		}
		if srcRepo == repo && dedupNum == number {
			return true
		}
	}
	for _, wi := range t.Status.WorkItems {
		if wi.Repo == repo && wi.Number == number {
			return true
		}
	}
	// Legacy fallback: Tasks created before Phase 1 carry source-repo/source-number
	// labels but no Spec.Source. Use the raw string values; the consts are deleted
	// in Phase 2 Task 9 to prevent new code from re-using them.
	repoSlug := strings.ReplaceAll(repo, "/", ".")
	numStr := fmt.Sprintf("%d", number)
	if t.Labels["tatara.io/source-repo"] == repoSlug && t.Labels["tatara.io/source-number"] == numStr {
		return true
	}
	return false
}

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
