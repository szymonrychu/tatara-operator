package v1alpha1

import (
	"fmt"
	"sort"
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

// TaskReposInScope returns a sorted, deduplicated list of "owner/repo" slugs this
// Task spans, derived from the ledger entries and the Spec.Source IssueRef.
// This is the authoritative clone-scope helper shared by the agent and controller.
func TaskReposInScope(t *Task) []string {
	seen := map[string]struct{}{}
	if s := t.Spec.Source; s != nil {
		if r := RepoFromIssueRef(s.IssueRef); r != "" {
			seen[r] = struct{}{}
		}
	}
	for _, wi := range t.Status.WorkItems {
		if wi.Repo != "" {
			seen[wi.Repo] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for r := range seen {
		out = append(out, r)
	}
	sort.Strings(out)
	return out
}

// WorkItemsContext formats a human-readable summary of the task's work-item
// ledger for inclusion in the agent prompt or TATARA_WORK_ITEMS env. Returns ""
// when WorkItems is empty.
func WorkItemsContext(t *Task) string {
	if len(t.Status.WorkItems) == 0 {
		return ""
	}
	var sb strings.Builder
	sb.WriteString("## Spanned work items\n")
	for _, wi := range t.Status.WorkItems {
		// Issues are repo#N everywhere. PR/MR refs follow provider convention:
		// GitLab MRs are repo!N, GitHub PRs are repo#N.
		ref := fmt.Sprintf("%s#%d", wi.Repo, wi.Number)
		if wi.Kind == WorkItemPR && wi.Provider == "gitlab" {
			ref = fmt.Sprintf("%s!%d", wi.Repo, wi.Number)
		}
		line := fmt.Sprintf("- [%s] %s (role:%s, state:%s)", wi.Kind, ref, wi.Role, wi.State)
		if wi.Title != "" {
			line += " - " + wi.Title
		}
		// Umbrella member-state suffix: PR/MR branch + CI + mergeability so the
		// prompt carries the live cross-repo status without a re-crawl.
		if wi.HeadBranch != "" {
			line += " branch:" + wi.HeadBranch
		}
		if wi.CIStatus != "" {
			line += " CI:" + wi.CIStatus
		}
		if wi.Mergeable != "" {
			line += " mergeable:" + wi.Mergeable
		}
		sb.WriteString(line + "\n")
	}
	return sb.String()
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
	// Umbrella member-state fields (7-kind redesign): kept fresh by light SCM
	// polls (refreshUmbrellaMembers) and rendered whole into the pod's turn-0
	// context bundle so a fresh pod reconstructs the full cross-repo state from
	// the CR alone.

	// Labels are the current SCM labels on this member.
	// +optional
	Labels []string `json:"labels,omitempty"`
	// HeadBranch is the PR/MR source branch.
	// +optional
	HeadBranch string `json:"headBranch,omitempty"`
	// CIStatus is the member's CI/pipeline status: ""|pending|success|failure.
	// +optional
	CIStatus string `json:"ciStatus,omitempty"`
	// Mergeable is the member's mergeability: unknown|clean|dirty|blocked|behind.
	// +optional
	Mergeable string `json:"mergeable,omitempty"`
	// Body is the issue/PR body captured at the last poll (turn-0 bundle source).
	// +optional
	Body string `json:"body,omitempty"`
	// LastRefreshedAt is the last-synced cursor for this member.
	// +optional
	LastRefreshedAt *metav1.Time `json:"lastRefreshedAt,omitempty"`
}
