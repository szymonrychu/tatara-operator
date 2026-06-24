package controller

import (
	"fmt"
	"sort"
	"strings"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// repoFromIssueRef extracts the "owner/repo" part from an IssueRef like
// "owner/repo#N" or "owner/repo!N". Returns "" when the ref is unparseable.
func repoFromIssueRef(issueRef string) string {
	idx := strings.LastIndexAny(issueRef, "#!")
	if idx <= 0 {
		return ""
	}
	return issueRef[:idx]
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

// taskMatchesItem reports whether the Task's seed identity (Spec.Source:
// repo from IssueRef, number = DedupNumber if set else Number) OR any ledger
// entry matches the given (repo, number).
func taskMatchesItem(t *tatarav1alpha1.Task, repo string, number int) bool {
	if s := t.Spec.Source; s != nil {
		srcRepo := repoFromIssueRef(s.IssueRef)
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
	return false
}

// reposInScope returns a sorted, deduplicated list of "owner/repo" slugs this
// Task spans, derived from the ledger entries and the Spec.Source IssueRef.
func reposInScope(t *tatarav1alpha1.Task) []string {
	seen := map[string]struct{}{}
	if s := t.Spec.Source; s != nil {
		if r := repoFromIssueRef(s.IssueRef); r != "" {
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
