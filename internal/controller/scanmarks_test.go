package controller

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func mark(repo string, n int, isPR bool, ts int64) tatarav1alpha1.ScanMark {
	return tatarav1alpha1.ScanMark{Repo: repo, Number: n, IsPR: isPR, AccountedAt: metav1.NewTime(time.Unix(ts, 0))}
}

func TestLookupScanMark(t *testing.T) {
	marks := []tatarav1alpha1.ScanMark{mark("o/r", 5, false, 100), mark("o/r", 6, true, 200)}
	if got := lookupScanMark(marks, "o/r", 5); got == nil || got.AccountedAt.Unix() != 100 {
		t.Fatalf("want mark 5@100, got %+v", got)
	}
	if got := lookupScanMark(marks, "o/r", 99); got != nil {
		t.Fatalf("want nil for absent, got %+v", got)
	}
}

func TestBuildScanMarks_UpsertNewAndBump(t *testing.T) {
	cur := []tatarav1alpha1.ScanMark{mark("o/r", 5, false, 100)}
	keep := map[string]bool{"o/r#5": true, "o/r#7": true}
	scanned := map[string]bool{"o/r": true}
	upserts := []scanMarkUpsert{
		{repo: "o/r", number: 5, updatedAt: time.Unix(150, 0), isPR: false}, // bump existing
		{repo: "o/r", number: 7, updatedAt: time.Unix(300, 0), isPR: false}, // new
	}
	got := buildScanMarks(cur, upserts, keep, scanned, false)
	if m := lookupScanMark(got, "o/r", 5); m == nil || m.AccountedAt.Unix() != 150 {
		t.Fatalf("want 5 bumped to 150, got %+v", m)
	}
	if m := lookupScanMark(got, "o/r", 7); m == nil || m.AccountedAt.Unix() != 300 {
		t.Fatalf("want 7@300 appended, got %+v", m)
	}
}

func TestBuildScanMarks_PruneClosedInScannedRepoOnly(t *testing.T) {
	cur := []tatarav1alpha1.ScanMark{
		mark("o/r", 5, false, 100),     // scanned repo, still open -> keep
		mark("o/r", 8, false, 100),     // scanned repo, not open -> prune
		mark("o/other", 9, false, 100), // repo NOT scanned this cycle -> keep
	}
	keep := map[string]bool{"o/r#5": true}
	scanned := map[string]bool{"o/r": true}
	got := buildScanMarks(cur, nil, keep, scanned, false)
	if lookupScanMark(got, "o/r", 5) == nil {
		t.Fatal("open item 5 must be kept")
	}
	if lookupScanMark(got, "o/r", 8) != nil {
		t.Fatal("closed item 8 in scanned repo must be pruned")
	}
	if lookupScanMark(got, "o/other", 9) == nil {
		t.Fatal("item in un-scanned repo must be kept")
	}
}

func TestBuildScanMarks_PruneScopedByIsPR(t *testing.T) {
	cur := []tatarav1alpha1.ScanMark{
		mark("o/r", 5, false, 100), // issue mark
		mark("o/r", 6, true, 100),  // PR mark
	}
	keep := map[string]bool{} // nothing open this cycle
	scanned := map[string]bool{"o/r": true}
	// Pruning as issueScan (isPR=false) must NOT touch the PR mark.
	got := buildScanMarks(cur, nil, keep, scanned, false)
	if lookupScanMark(got, "o/r", 5) != nil {
		t.Fatal("issue mark 5 should be pruned by issue-scoped prune")
	}
	if lookupScanMark(got, "o/r", 6) == nil {
		t.Fatal("PR mark 6 must survive an issue-scoped prune")
	}
}
