package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// A mark at/after the issue's UpdatedAt, no live Task, human had the last word
// (so botHadLastWord does not fire): the new stale-mark gate is the only thing
// that can skip -> expect no QE and the skipped_stale_mark metric.
func TestIssueScan_SkipsWhenActivityNotNewerThanMark(t *testing.T) {
	const bot = "tatara-bot"
	const projName = "stalemark-skip"
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 5}}
	proj, repoObj := seedScanProject(t, projName, cron)
	// Seed a mark equal to the issue's UpdatedAt (100).
	proj.Status.ScanMarks = []tatarav1alpha1.ScanMark{
		{Repo: "o/r", Number: 5, IsPR: false, AccountedAt: metav1.NewTime(time.Unix(100, 0))},
	}
	reader := &lastWordReader{
		fakeReader:    fakeReader{issues: []scm.IssueRef{{Repo: "o/r", Number: 5, Author: "human", UpdatedAt: time.Unix(100, 0)}}},
		issueComments: map[int][]scm.IssueComment{5: {blwCmt(bot, 1), blwCmt("human", 2)}}, // human last word
	}
	r := newScanReconciler(reader)
	reg := prometheus.NewRegistry()
	r.Metrics = obs.NewOperatorMetrics(reg)

	_, _ = r.issueScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{*repoObj}, nil, cron.IssueScan)

	if qes := listScanQEs(t, projName); len(qes) != 0 {
		t.Fatalf("want 0 QEs (no new activity since mark), got %d", len(qes))
	}
	require.GreaterOrEqual(t,
		counterValue(t, reg, "tatara_scan_items_total", map[string]string{"activity": "issueScan", "outcome": "skipped_stale_mark"}),
		float64(1), "skipped_stale_mark must fire")
}

// New activity (UpdatedAt 200 > mark 100), human last word, no terminal Task:
// the mark gate must NOT skip; a fresh Task is created.
func TestIssueScan_CreatesWhenActivityNewerThanMark(t *testing.T) {
	const bot = "tatara-bot"
	const projName = "stalemark-create"
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 5}}
	proj, repoObj := seedScanProject(t, projName, cron)
	proj.Status.ScanMarks = []tatarav1alpha1.ScanMark{
		{Repo: "o/r", Number: 5, IsPR: false, AccountedAt: metav1.NewTime(time.Unix(100, 0))},
	}
	reader := &lastWordReader{
		fakeReader:    fakeReader{issues: []scm.IssueRef{{Repo: "o/r", Number: 5, Author: "human", UpdatedAt: time.Unix(200, 0)}}},
		issueComments: map[int][]scm.IssueComment{5: {blwCmt(bot, 1), blwCmt("human", 2)}},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	_, _ = r.issueScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{*repoObj}, nil, cron.IssueScan)

	if qes := listScanQEs(t, projName); len(qes) != 1 {
		t.Fatalf("want 1 QE (new activity past mark), got %d", len(qes))
	}
}

// After a scan, the observed UpdatedAt is persisted as the item's mark, and a
// mark for a now-closed issue (not in ListOpenIssues) is pruned.
func TestIssueScan_PersistsAndPrunesMarks(t *testing.T) {
	const projName = "stalemark-persist"
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 5}}
	proj, repoObj := seedScanProject(t, projName, cron)
	// Pre-seed a mark for issue 8 which is NOT in the open set below -> must prune.
	proj.Status.ScanMarks = []tatarav1alpha1.ScanMark{
		{Repo: "o/r", Number: 8, IsPR: false, AccountedAt: metav1.NewTime(time.Unix(50, 0))},
	}
	reader := &lastWordReader{
		fakeReader:    fakeReader{issues: []scm.IssueRef{{Repo: "o/r", Number: 5, Author: "human", UpdatedAt: time.Unix(300, 0)}}},
		issueComments: map[int][]scm.IssueComment{5: {blwCmt("human", 2)}},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	_, _ = r.issueScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{*repoObj}, nil, cron.IssueScan)

	fresh := &tatarav1alpha1.Project{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: proj.Namespace, Name: proj.Name}, fresh); err != nil {
		t.Fatalf("get project: %v", err)
	}
	if m := lookupScanMark(fresh.Status.ScanMarks, "o/r", 5); m == nil || m.AccountedAt.Unix() != 300 {
		t.Fatalf("want issue 5 mark persisted at 300, got %+v", m)
	}
	if m := lookupScanMark(fresh.Status.ScanMarks, "o/r", 8); m != nil {
		t.Fatalf("want closed issue 8 mark pruned, got %+v", m)
	}
}
