package controller

import (
	"sort"
	"strconv"
	"strings"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

const (
	labelSourceRepo   = "tatara.io/source-repo"
	labelSourceNumber = "tatara.io/source-number"
	labelSourceKind   = "tatara.io/source-kind"
	labelHeadSHA      = "tatara.io/head-sha"
	labelActivity     = "tatara.io/activity"
)

// sanitizeRepoLabel makes a repo slug DNS-label-safe by replacing '/' with '.'.
func sanitizeRepoLabel(repo string) string {
	return strings.ReplaceAll(repo, "/", ".")
}

// scanTaskLabels builds the operator-stamped dedup labels for a cron Task.
// head-sha is omitted for non-PR candidates.
func scanTaskLabels(c candidate, activity, kind string) map[string]string {
	l := map[string]string{
		labelSourceRepo:   sanitizeRepoLabel(c.repo),
		labelSourceNumber: strconv.Itoa(c.number),
		labelSourceKind:   kind,
		labelActivity:     activity,
	}
	if c.headSHA != "" {
		l[labelHeadSHA] = c.headSHA
	}
	return l
}

// isDeduped reports whether a candidate already has a Task that should suppress
// a re-pick, per the dedup rules:
//   - any non-terminal Task for (repo, number) -> skip
//   - PR: a terminal Task at the same head-sha -> skip (already handled revision)
//   - issue: a terminal Task whose creation is at/after the candidate updatedAt -> skip
func isDeduped(c candidate, existing []tatarav1alpha1.Task) bool {
	repoLabel := sanitizeRepoLabel(c.repo)
	numLabel := strconv.Itoa(c.number)
	for i := range existing {
		t := &existing[i]
		if t.Labels[labelSourceRepo] != repoLabel || t.Labels[labelSourceNumber] != numLabel {
			continue
		}
		if !isTerminal(t.Status.Phase) {
			return true
		}
		if c.isPR {
			if t.Labels[labelHeadSHA] == c.headSHA && c.headSHA != "" {
				return true
			}
			continue
		}
		// issue: terminal Task suppresses unless the issue saw newer activity.
		if !c.updatedAt.After(t.CreationTimestamp.Time) {
			return true
		}
	}
	return false
}

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
