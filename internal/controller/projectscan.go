package controller

import (
	"sort"
	"time"
)

// candidate is one scannable work item (PR, issue, or board item) normalized
// for selection + dedup. number/repo identify it; labels drive priority;
// updatedAt drives stale-first ordering.
type candidate struct {
	repo      string
	number    int
	author    string
	headSHA   string
	labels    []string
	updatedAt time.Time
	isPR      bool
}

func hasLabel(labels []string, want string) bool {
	if want == "" {
		return false
	}
	for _, l := range labels {
		if l == want {
			return true
		}
	}
	return false
}

// selectCandidates partitions into priority-labelled and rest, sorts each
// least-recently-updated first, concatenates priority++rest, and caps at n.
// When priorityLabel is empty, items with no labels sort before items with any
// labels (stale-first within each group), so unlabeled items are preferred.
func selectCandidates(in []candidate, priorityLabel string, n int) []candidate {
	if n < 1 {
		n = 1
	}
	staleFirst := func(s []candidate) {
		sort.SliceStable(s, func(i, j int) bool { return s[i].updatedAt.Before(s[j].updatedAt) })
	}

	if priorityLabel != "" {
		var withPriority, rest []candidate
		for _, c := range in {
			if hasLabel(c.labels, priorityLabel) {
				withPriority = append(withPriority, c)
			} else {
				rest = append(rest, c)
			}
		}
		staleFirst(withPriority)
		staleFirst(rest)
		out := append(withPriority, rest...)
		if len(out) > n {
			out = out[:n]
		}
		return out
	}

	// No priority label: prefer unlabeled items (stale-first), then labeled (stale-first).
	var unlabeled, labeled []candidate
	for _, c := range in {
		if len(c.labels) == 0 {
			unlabeled = append(unlabeled, c)
		} else {
			labeled = append(labeled, c)
		}
	}
	staleFirst(unlabeled)
	staleFirst(labeled)
	out := append(unlabeled, labeled...)
	if len(out) > n {
		out = out[:n]
	}
	return out
}
