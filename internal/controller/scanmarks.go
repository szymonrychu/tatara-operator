package controller

import (
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// scanMarkKey is the identity of a scanned item across issues and PRs (GitHub
// issue/PR numbers share one sequence per repo, so (repo, number) is unique).
func scanMarkKey(repo string, number int) string {
	return fmt.Sprintf("%s#%d", repo, number)
}

// lookupScanMark returns the mark for (repo, number) or nil.
func lookupScanMark(marks []tatarav1alpha1.ScanMark, repo string, number int) *tatarav1alpha1.ScanMark {
	for i := range marks {
		if marks[i].Repo == repo && marks[i].Number == number {
			return &marks[i]
		}
	}
	return nil
}

// scanMarkUpsert is a per-item observation to fold into the mark set.
type scanMarkUpsert struct {
	repo      string
	number    int
	updatedAt time.Time
	isPR      bool
}

// buildScanMarks merges upserts into cur and prunes stale marks, returning a new
// slice. Prune authority is scoped to isPR (issueScan prunes only issue marks,
// mrScan only PR marks) and to scanned repos: a mark is dropped only when its
// repo was scanned this cycle (in scanned), its IsPR matches, and its key is not
// in keep (the currently-open item set). Marks in un-scanned repos, or of the
// other item type, are carried through untouched so the two scans never clobber
// each other's marks. Upserts set AccountedAt to the observed updatedAt.
func buildScanMarks(cur []tatarav1alpha1.ScanMark, upserts []scanMarkUpsert, keep, scanned map[string]bool, isPR bool) []tatarav1alpha1.ScanMark {
	out := make([]tatarav1alpha1.ScanMark, 0, len(cur)+len(upserts))
	for _, m := range cur {
		if m.IsPR == isPR && scanned[m.Repo] && !keep[scanMarkKey(m.Repo, m.Number)] {
			continue // stale: scanned this cycle, no longer open
		}
		out = append(out, m)
	}
	for _, u := range upserts {
		if u.updatedAt.IsZero() {
			continue // board/synthetic items carry no timestamp; never mark them
		}
		at := metav1.NewTime(u.updatedAt)
		if existing := lookupScanMark(out, u.repo, u.number); existing != nil {
			existing.AccountedAt = at
			existing.IsPR = u.isPR
			continue
		}
		out = append(out, tatarav1alpha1.ScanMark{Repo: u.repo, Number: u.number, IsPR: u.isPR, AccountedAt: at})
	}
	return out
}
