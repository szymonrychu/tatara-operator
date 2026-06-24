package controller

import (
	"context"
	"strings"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// refreshLedger is Tier-1 of the cron backstop. It fetches the current SCM state
// for each open WorkItemRef (issues and PRs) and updates State, HeadSHA, and
// LastRefreshedAt in place. Already-terminal entries (closed/merged) are skipped.
// Returns true when at least one entry changed.
func refreshLedger(ctx context.Context, reader scm.SCMReader, task *tatarav1alpha1.Task) bool {
	// Build per-repo caches so we make one pair of SCM calls per repo, not one per
	// work-item. Repos are only queried when there is at least one non-terminal
	// entry of that kind in the ledger.
	issueCache := map[string]map[int]bool{} // repo -> set of open issue numbers
	prCache := map[string]map[int]string{}  // repo -> PR number -> current HeadSHA

	// Collect repos needing issue/PR lookups.
	issueRepos := map[string]bool{}
	prRepos := map[string]bool{}
	for _, wi := range task.Status.WorkItems {
		if wi.Repo == "" {
			continue
		}
		switch wi.Kind {
		case tatarav1alpha1.WorkItemIssue:
			if !isWITerminal(wi.State) {
				issueRepos[wi.Repo] = true
			}
		case tatarav1alpha1.WorkItemPR:
			if !isWITerminal(wi.State) {
				prRepos[wi.Repo] = true
			}
		}
	}

	// Fetch issue states.
	for repo := range issueRepos {
		owner, name, _ := strings.Cut(repo, "/")
		issues, err := reader.ListOpenIssues(ctx, owner, name)
		if err != nil {
			// Skip this repo on error; better to miss an update than block the sweep.
			continue
		}
		m := make(map[int]bool, len(issues))
		for _, iss := range issues {
			m[iss.Number] = true
		}
		issueCache[repo] = m
	}

	// Fetch PR states.
	for repo := range prRepos {
		owner, name, _ := strings.Cut(repo, "/")
		prs, err := reader.ListOpenPRs(ctx, owner, name)
		if err != nil {
			continue
		}
		m := make(map[int]string, len(prs))
		for _, pr := range prs {
			m[pr.Number] = pr.HeadSHA
		}
		prCache[repo] = m
	}

	changed := false
	now := metav1.NewTime(time.Now())

	for i := range task.Status.WorkItems {
		wi := &task.Status.WorkItems[i]
		if wi.Repo == "" {
			continue
		}
		switch wi.Kind {
		case tatarav1alpha1.WorkItemIssue:
			if isWITerminal(wi.State) {
				continue
			}
			openSet, cached := issueCache[wi.Repo]
			if !cached {
				continue
			}
			if !openSet[wi.Number] {
				// Issue is no longer open in SCM.
				wi.State = tatarav1alpha1.WIClosed
				wi.LastRefreshedAt = &now
				changed = true
			}
		case tatarav1alpha1.WorkItemPR:
			if isWITerminal(wi.State) {
				continue
			}
			prMap, cached := prCache[wi.Repo]
			if !cached {
				continue
			}
			currentSHA, open := prMap[wi.Number]
			if !open {
				// PR is no longer in the open list: closed (or merged; we cannot
				// distinguish via SCMReader without GetPRState, so use WIClosed as a
				// conservative signal - backstopAction only cares open vs not-open).
				wi.State = tatarav1alpha1.WIClosed
				wi.LastRefreshedAt = &now
				changed = true
			} else if currentSHA != "" && currentSHA != wi.HeadSHA {
				// PR is still open but the head SHA advanced.
				wi.HeadSHA = currentSHA
				wi.LastRefreshedAt = &now
				changed = true
			}
		}
	}

	return changed
}

// isWITerminal reports whether a WorkItemRef state is already terminal and
// needs no further SCM refresh.
func isWITerminal(state string) bool {
	return state == tatarav1alpha1.WIClosed ||
		state == tatarav1alpha1.WIMerged ||
		state == tatarav1alpha1.WIDeclined ||
		state == tatarav1alpha1.WIImplemented
}
