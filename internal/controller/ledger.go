package controller

import (
	"fmt"
	"strings"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// repoFromIssueRef delegates to the api package so the two call sites share
// one implementation.
func repoFromIssueRef(issueRef string) string {
	return tatarav1alpha1.RepoFromIssueRef(issueRef)
}

// UpsertWorkItem upserts ref into task.Status.WorkItems. Idempotent by
// (Repo, Number, Kind) when Number > 0; for unfiled proposals (Number==0)
// it matches by (Repo, Title, Role). On match it updates Role, State, HeadSHA,
// Title, and LastRefreshedAt in place; otherwise appends.
func UpsertWorkItem(task *tatarav1alpha1.Task, ref tatarav1alpha1.WorkItemRef) {
	for i := range task.Status.WorkItems {
		wi := &task.Status.WorkItems[i]
		var match bool
		if ref.Number == 0 {
			match = wi.Repo == ref.Repo && wi.Title == ref.Title && wi.Role == ref.Role
		} else {
			match = wi.Repo == ref.Repo && wi.Number == ref.Number && wi.Kind == ref.Kind
		}
		if match {
			if ref.Role != "" {
				wi.Role = ref.Role
			}
			if ref.State != "" {
				wi.State = ref.State
			}
			if ref.HeadSHA != "" {
				wi.HeadSHA = ref.HeadSHA
			}
			if ref.Title != "" {
				wi.Title = ref.Title
			}
			if ref.LastRefreshedAt != nil {
				wi.LastRefreshedAt = ref.LastRefreshedAt
			}
			return
		}
	}
	task.Status.WorkItems = append(task.Status.WorkItems, ref)
}

// taskMatchesItem delegates to tatarav1alpha1.TaskMatchesItem so controller
// and webhook share one implementation. See api/v1alpha1/workitem_types.go.
func taskMatchesItem(t *tatarav1alpha1.Task, repo string, number int) bool {
	return tatarav1alpha1.TaskMatchesItem(t, repo, number)
}

// reposInScope returns a sorted, deduplicated list of "owner/repo" slugs this
// Task spans, derived from the ledger entries and the Spec.Source IssueRef.
// Delegates to tatarav1alpha1.TaskReposInScope so the controller and agent
// packages share one implementation.
func reposInScope(t *tatarav1alpha1.Task) []string {
	return tatarav1alpha1.TaskReposInScope(t)
}

// seedLedgerFromSpec populates Status.WorkItems from Spec.Source, SystemicGroup,
// and Status.PRNumber when WorkItems is empty. No-op when non-empty (idempotent).
func seedLedgerFromSpec(t *tatarav1alpha1.Task) {
	if len(t.Status.WorkItems) > 0 {
		return
	}
	s := t.Spec.Source
	if s == nil {
		return
	}
	repo := repoFromIssueRef(s.IssueRef)
	if repo == "" {
		return
	}

	// Primary source entry.
	role := tatarav1alpha1.RoleSource
	if s.IsPR {
		role = tatarav1alpha1.RoleReviewed
	}
	UpsertWorkItem(t, tatarav1alpha1.WorkItemRef{
		Provider: s.Provider,
		Repo:     repo,
		Number:   s.Number,
		Kind:     kindForIsPR(s.IsPR),
		Role:     role,
		State:    tatarav1alpha1.WIOpen,
		Title:    s.Title,
	})

	// SystemicGroup siblings.
	if sg := t.Spec.SystemicGroup; sg != nil {
		for _, n := range sg.SameRepoSiblings {
			UpsertWorkItem(t, tatarav1alpha1.WorkItemRef{
				Provider: s.Provider,
				Repo:     repo,
				Number:   n,
				Kind:     tatarav1alpha1.WorkItemIssue,
				Role:     tatarav1alpha1.RoleCloses,
				State:    tatarav1alpha1.WIOpen,
			})
		}
		for _, ref := range sg.CrossRepo {
			crossRepo, crossNum := parseCrossRepoRef(ref)
			if crossRepo == "" {
				continue
			}
			UpsertWorkItem(t, tatarav1alpha1.WorkItemRef{
				Provider: s.Provider,
				Repo:     crossRepo,
				Number:   crossNum,
				Kind:     tatarav1alpha1.WorkItemIssue,
				Role:     tatarav1alpha1.RoleCloses,
				State:    tatarav1alpha1.WIOpen,
				Title:    ref,
			})
		}
	}

	// Existing PR if already opened. HeadSHA is left empty at seed time: the only
	// value available here is Status.HeadBranch (a branch name, not a commit SHA),
	// and storing it would let any HeadSHA consumer compare a branch string to a
	// real SHA and silently never match. UpsertWorkItem skips empty fields on
	// refresh, so the Phase-3 cron backstop populates the real SHA later.
	if t.Status.PRNumber > 0 {
		UpsertWorkItem(t, tatarav1alpha1.WorkItemRef{
			Provider: s.Provider,
			Repo:     repo,
			Number:   t.Status.PRNumber,
			Kind:     tatarav1alpha1.WorkItemPR,
			Role:     tatarav1alpha1.RoleOpenedPR,
			State:    tatarav1alpha1.WIOpen,
		})
	}
}

// proposalBacklogFromTasks counts open, undecided brainstorm proposals across a
// set of Tasks by inspecting each Task's role:proposed ledger entries. Entries
// in state "proposed" are counted; "approved", "declined", and "implemented"
// are terminal and skipped. Tasks belonging to a systemic group (non-nil
// Spec.SystemicGroup with a non-empty SystemicID) count as one entry per
// unique SystemicID regardless of how many Tasks share that ID; Tasks without
// a systemic group each contribute one standalone count.
//
// Only Tasks that have at least one role:proposed entry in state "proposed" are
// included; Tasks with an empty WorkItems ledger return 0 (the caller falls
// back to the SCM-issue count for those).
func proposalBacklogFromTasks(tasks []tatarav1alpha1.Task) int {
	groups := map[string]bool{}
	standalone := 0
	for i := range tasks {
		t := &tasks[i]
		hasOpen := false
		for _, wi := range t.Status.WorkItems {
			if wi.Role == tatarav1alpha1.RoleProposed && wi.State == tatarav1alpha1.WIProposed {
				hasOpen = true
				break
			}
		}
		if !hasOpen {
			continue
		}
		if sg := t.Spec.SystemicGroup; sg != nil && sg.SystemicID != "" {
			groups[sg.SystemicID] = true
		} else {
			standalone++
		}
	}
	return standalone + len(groups)
}

// closeSourceIssueLedger sets State:closed ONLY on the primary source issue
// entry the operator actually closed on merge: the ledger entry whose Repo and
// Number match Spec.Source (repo from IssueRef, Spec.Source.Number). Sibling
// role:closes entries (same-repo SameRepoSiblings and cross-repo CrossRepo) are
// NOT auto-closed by the merge - the PR body only carries "Closes #SourceNumber",
// and the Closes keyword never auto-closes cross-repo issues - so they are left
// WIOpen for the Phase-3 backstop to reconcile from live SCM. Closing them here
// would make the ledger (the dedup/backstop source of truth) report a still-open
// sibling as resolved. Pure function; no client calls.
func closeSourceIssueLedger(t *tatarav1alpha1.Task) {
	s := t.Spec.Source
	if s == nil {
		return
	}
	srcRepo := repoFromIssueRef(s.IssueRef)
	if srcRepo == "" {
		return
	}
	for i := range t.Status.WorkItems {
		wi := &t.Status.WorkItems[i]
		if wi.Kind == tatarav1alpha1.WorkItemIssue &&
			wi.Repo == srcRepo && wi.Number == s.Number &&
			(wi.Role == tatarav1alpha1.RoleSource || wi.Role == tatarav1alpha1.RoleCloses) {
			wi.State = tatarav1alpha1.WIClosed
		}
	}
}

func kindForIsPR(isPR bool) string {
	if isPR {
		return tatarav1alpha1.WorkItemPR
	}
	return tatarav1alpha1.WorkItemIssue
}

// parseCrossRepoRef parses a "owner/repo#N - title" cross-repo reference
// (as emitted by electSystemicLeads) into (repo, number). Returns ("", 0) on
// parse failure.
func parseCrossRepoRef(ref string) (string, int) {
	// Format: "owner/repo#N - title". Split on the FIRST '#': the repo slug
	// "owner/repo" never contains '#', and a title may, so LastIndex would pick
	// the wrong separator and drop the entry.
	idx := strings.IndexByte(ref, '#')
	if idx <= 0 {
		return "", 0
	}
	repo := ref[:idx]
	rest := ref[idx+1:]
	// rest is "N - title" or just "N"
	numStr := rest
	if spaceIdx := strings.IndexByte(rest, ' '); spaceIdx >= 0 {
		numStr = rest[:spaceIdx]
	}
	var n int
	if _, err := fmt.Sscanf(numStr, "%d", &n); err != nil {
		return "", 0
	}
	return repo, n
}
