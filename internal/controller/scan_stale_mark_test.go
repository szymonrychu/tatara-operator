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
	if err := k8sClient.Status().Update(context.Background(), proj); err != nil {
		t.Fatalf("seed mark via status update: %v", err)
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

// A bot-authored PR with a mark at/after its UpdatedAt and no live Task, human
// last word on the PR: the stale-mark gate skips re-creation.
func TestMRScan_BotPR_SkipsWhenActivityNotNewerThanMark(t *testing.T) {
	const bot = "tatara-bot"
	const projName = "stalemark-mr-skip"
	cron := &tatarav1alpha1.ScmCron{MRScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 5}}
	proj, repoObj := seedScanProject(t, projName, cron)
	proj.Status.ScanMarks = []tatarav1alpha1.ScanMark{
		{Repo: "o/r", Number: 7, IsPR: true, AccountedAt: metav1.NewTime(time.Unix(100, 0))},
	}
	reader := &lastWordReader{
		fakeReader: fakeReader{prs: []scm.PRRef{{Repo: "o/r", Number: 7, Author: bot, UpdatedAt: time.Unix(100, 0)}}},
		prComments: map[int][]scm.IssueComment{7: {blwCmt(bot, 1), blwCmt("human", 2)}},
	}
	r := newScanReconciler(reader)
	reg := prometheus.NewRegistry()
	r.Metrics = obs.NewOperatorMetrics(reg)

	_ = r.mrScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{*repoObj}, nil, cron.MRScan)

	if qes := listScanQEs(t, projName); len(qes) != 0 {
		t.Fatalf("want 0 QEs (bot PR, no new activity since mark), got %d", len(qes))
	}
	require.GreaterOrEqual(t,
		counterValue(t, reg, "tatara_scan_items_total", map[string]string{"activity": "mrScan", "outcome": "skipped_stale_mark"}),
		float64(1), "skipped_stale_mark must fire on mrScan")
}

// New activity (UpdatedAt 200 > mark 100), human last word, no terminal Task:
// the mark gate must NOT skip; a fresh Task is created.
func TestMRScan_BotPR_CreatesWhenActivityNewerThanMark(t *testing.T) {
	const bot = "tatara-bot"
	const projName = "stalemark-mr-create"
	cron := &tatarav1alpha1.ScmCron{MRScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 5}}
	proj, repoObj := seedScanProject(t, projName, cron)
	proj.Status.ScanMarks = []tatarav1alpha1.ScanMark{
		{Repo: "o/r", Number: 7, IsPR: true, AccountedAt: metav1.NewTime(time.Unix(100, 0))},
	}
	reader := &lastWordReader{
		fakeReader: fakeReader{prs: []scm.PRRef{{Repo: "o/r", Number: 7, Author: bot, UpdatedAt: time.Unix(200, 0)}}},
		prComments: map[int][]scm.IssueComment{7: {blwCmt(bot, 1), blwCmt("human", 2)}},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	_ = r.mrScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{*repoObj}, nil, cron.MRScan)

	if qes := listScanQEs(t, projName); len(qes) != 1 {
		t.Fatalf("want 1 QE (new activity past mark), got %d", len(qes))
	}
}

// After a scan, the observed UpdatedAt is persisted as the PR's mark, and a
// mark for a now-closed PR (not in ListOpenPRs) is pruned.
func TestMRScan_PersistsAndPrunesMarks(t *testing.T) {
	const bot = "tatara-bot"
	const projName = "stalemark-mr-persist"
	cron := &tatarav1alpha1.ScmCron{MRScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 5}}
	proj, repoObj := seedScanProject(t, projName, cron)
	// Pre-seed a mark for PR 8 which is NOT in the open set below -> must prune.
	proj.Status.ScanMarks = []tatarav1alpha1.ScanMark{
		{Repo: "o/r", Number: 8, IsPR: true, AccountedAt: metav1.NewTime(time.Unix(50, 0))},
	}
	if err := k8sClient.Status().Update(context.Background(), proj); err != nil {
		t.Fatalf("seed mark via status update: %v", err)
	}
	reader := &lastWordReader{
		fakeReader: fakeReader{prs: []scm.PRRef{{Repo: "o/r", Number: 7, Author: bot, UpdatedAt: time.Unix(300, 0)}}},
		prComments: map[int][]scm.IssueComment{7: {blwCmt("human", 2)}},
	}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	_ = r.mrScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{*repoObj}, nil, cron.MRScan)

	fresh := &tatarav1alpha1.Project{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: proj.Namespace, Name: proj.Name}, fresh); err != nil {
		t.Fatalf("get project: %v", err)
	}
	if m := lookupScanMark(fresh.Status.ScanMarks, "o/r", 7); m == nil || m.AccountedAt.Unix() != 300 {
		t.Fatalf("want PR 7 mark persisted at 300, got %+v", m)
	}
	if m := lookupScanMark(fresh.Status.ScanMarks, "o/r", 8); m != nil {
		t.Fatalf("want closed PR 8 mark pruned, got %+v", m)
	}
}

// The clarify->implement handoff producer (issueScan's second, ungated
// Task-creating loop) has the same GC-rescan gap as the gated triage/mrScan
// paths: once a terminal implement Task is GC'd, an open issue still carrying
// tatara-implementation re-satisfies needsImplementProducer and spawns a fresh
// implement Task on every scan. A mark at/after the issue's UpdatedAt, no live
// Task of any kind: the stale-mark gate must skip and record skipped_stale_mark.
func TestIssueScan_ImplementProducer_SkipsWhenActivityNotNewerThanMark(t *testing.T) {
	const impl = "tatara-implementation"

	t.Run("mark at UpdatedAt -> no implement QE, metric fires", func(t *testing.T) {
		const projName = "stalemark-impl-skip"
		cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 5}}
		proj, repoObj := seedScanProject(t, projName, cron)
		proj.Status.ScanMarks = []tatarav1alpha1.ScanMark{
			{Repo: "o/r", Number: 5, IsPR: false, AccountedAt: metav1.NewTime(time.Unix(100, 0))},
		}
		reader := &fakeReader{issues: []scm.IssueRef{
			{Repo: "o/r", Number: 5, Author: "human", Labels: []string{impl}, UpdatedAt: time.Unix(100, 0)},
		}}
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
	})

	// Companion: new activity (UpdatedAt 200 > mark 100) must still produce the
	// implement Task, proving the gate does not drop real handoffs.
	t.Run("mark before UpdatedAt -> implement QE produced", func(t *testing.T) {
		const projName = "stalemark-impl-create"
		cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 5}}
		proj, repoObj := seedScanProject(t, projName, cron)
		proj.Status.ScanMarks = []tatarav1alpha1.ScanMark{
			{Repo: "o/r", Number: 5, IsPR: false, AccountedAt: metav1.NewTime(time.Unix(100, 0))},
		}
		reader := &fakeReader{issues: []scm.IssueRef{
			{Repo: "o/r", Number: 5, Author: "human", Labels: []string{impl}, UpdatedAt: time.Unix(200, 0)},
		}}
		r := newScanReconciler(reader)
		r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

		_, _ = r.issueScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{*repoObj}, nil, cron.IssueScan)

		qes := listScanQEs(t, projName)
		if len(qes) != 1 {
			t.Fatalf("want 1 implement QE (new activity past mark), got %d", len(qes))
		}
		if qes[0].Spec.Payload.Kind != "implement" {
			t.Fatalf("want implement QE, got kind %q", qes[0].Spec.Payload.Kind)
		}
	})
}

// HIGH-severity fix: a transient createScanTask (QueuedEvent Create) failure
// must not advance the item's scan mark. Advancing it anyway would make the
// freshness gate (isDeduped/stale-mark check) treat the item as already
// accounted-for on the very next scan, permanently dropping it despite the
// fast 60s backlog re-fire (deferred>0 -> backlog=true) intended to retry it.
// qeCreateErrorClient (projectscan_deferral_test.go) injects the failure.

// TestIssueScan_FailedCreate_DoesNotAdvanceMark: no pre-existing mark, human
// issue passes every gate and reaches createScanTask, whose QueuedEvent
// Create fails. Expect: no QE created, no mark recorded for the item, and
// issueScan reports backlog=true so the fast retry re-attempts it.
func TestIssueScan_FailedCreate_DoesNotAdvanceMark(t *testing.T) {
	const projName = "stalemark-failcreate-noadvance"
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 5}}
	proj, repoObj := seedScanProject(t, projName, cron)
	reader := &fakeReader{issues: []scm.IssueRef{
		{Repo: "o/r", Number: 1, Author: "human", UpdatedAt: time.Unix(100, 0)},
	}}
	r := newScanReconcilerWithQEError(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	backlog, _ := r.issueScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{*repoObj}, nil, cron.IssueScan)

	if qes := listScanQEs(t, projName); len(qes) != 0 {
		t.Fatalf("want 0 QEs (create failed), got %d", len(qes))
	}
	fresh := &tatarav1alpha1.Project{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: proj.Namespace, Name: proj.Name}, fresh); err != nil {
		t.Fatalf("get project: %v", err)
	}
	if m := lookupScanMark(fresh.Status.ScanMarks, "o/r", 1); m != nil {
		t.Fatalf("want no mark recorded after failed create, got %+v", m)
	}
	if !backlog {
		t.Fatal("failed create must set backlog=true so the fast retry re-attempts the item")
	}
}

// TestIssueScan_FailedCreate_DoesNotPruneExistingMark: a pre-existing mark
// older than the candidate's UpdatedAt (so the stale-mark gate lets it
// through to createScanTask, which then fails). Expect: the existing mark is
// left exactly as-is - neither advanced to the new UpdatedAt nor pruned.
func TestIssueScan_FailedCreate_DoesNotPruneExistingMark(t *testing.T) {
	const projName = "stalemark-failcreate-nopruune"
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 5}}
	proj, repoObj := seedScanProject(t, projName, cron)
	proj.Status.ScanMarks = []tatarav1alpha1.ScanMark{
		{Repo: "o/r", Number: 1, IsPR: false, AccountedAt: metav1.NewTime(time.Unix(50, 0))},
	}
	if err := k8sClient.Status().Update(context.Background(), proj); err != nil {
		t.Fatalf("seed mark via status update: %v", err)
	}
	reader := &fakeReader{issues: []scm.IssueRef{
		{Repo: "o/r", Number: 1, Author: "human", UpdatedAt: time.Unix(100, 0)},
	}}
	r := newScanReconcilerWithQEError(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	_, _ = r.issueScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{*repoObj}, nil, cron.IssueScan)

	fresh := &tatarav1alpha1.Project{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: proj.Namespace, Name: proj.Name}, fresh); err != nil {
		t.Fatalf("get project: %v", err)
	}
	m := lookupScanMark(fresh.Status.ScanMarks, "o/r", 1)
	if m == nil {
		t.Fatal("want existing mark preserved (not pruned), got none")
	}
	if m.AccountedAt.Unix() != 50 {
		t.Fatalf("want mark left at 50 (not advanced to 100 on failed create), got %d", m.AccountedAt.Unix())
	}
}

// TestIssueScan_ImplementProducer_FailedCreate_DoesNotAdvanceMark: the
// clarify->implement handoff producer loop also calls createScanTask; its
// failure must not advance the mark either.
func TestIssueScan_ImplementProducer_FailedCreate_DoesNotAdvanceMark(t *testing.T) {
	const impl = "tatara-implementation"
	const projName = "stalemark-failcreate-implproducer"
	cron := &tatarav1alpha1.ScmCron{IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 5}}
	proj, repoObj := seedScanProject(t, projName, cron)
	reader := &fakeReader{issues: []scm.IssueRef{
		{Repo: "o/r", Number: 5, Author: "human", Labels: []string{impl}, UpdatedAt: time.Unix(100, 0)},
	}}
	r := newScanReconcilerWithQEError(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	_, _ = r.issueScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{*repoObj}, nil, cron.IssueScan)

	if qes := listScanQEs(t, projName); len(qes) != 0 {
		t.Fatalf("want 0 QEs (create failed), got %d", len(qes))
	}
	fresh := &tatarav1alpha1.Project{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: proj.Namespace, Name: proj.Name}, fresh); err != nil {
		t.Fatalf("get project: %v", err)
	}
	if m := lookupScanMark(fresh.Status.ScanMarks, "o/r", 5); m != nil {
		t.Fatalf("want no mark recorded after failed implement-producer create, got %+v", m)
	}
}

// TestMRScan_BotPR_FailedCreate_DoesNotAdvanceMark: the bot-PR issueLifecycle
// re-adoption path in mrScan also calls createScanTask; its failure must not
// advance the PR's mark.
func TestMRScan_BotPR_FailedCreate_DoesNotAdvanceMark(t *testing.T) {
	const bot = "tatara-bot"
	const projName = "stalemark-mr-failcreate-noadvance"
	cron := &tatarav1alpha1.ScmCron{MRScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *", MaxPerRepo: 5}}
	proj, repoObj := seedScanProject(t, projName, cron)
	reader := &lastWordReader{
		fakeReader: fakeReader{prs: []scm.PRRef{{Repo: "o/r", Number: 7, Author: bot, UpdatedAt: time.Unix(100, 0)}}},
		prComments: map[int][]scm.IssueComment{7: {blwCmt(bot, 1), blwCmt("human", 2)}},
	}
	r := newScanReconcilerWithQEError(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	backlog := r.mrScan(context.Background(), proj, reader, []tatarav1alpha1.Repository{*repoObj}, nil, cron.MRScan)

	if qes := listScanQEs(t, projName); len(qes) != 0 {
		t.Fatalf("want 0 QEs (create failed), got %d", len(qes))
	}
	fresh := &tatarav1alpha1.Project{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: proj.Namespace, Name: proj.Name}, fresh); err != nil {
		t.Fatalf("get project: %v", err)
	}
	if m := lookupScanMark(fresh.Status.ScanMarks, "o/r", 7); m != nil {
		t.Fatalf("want no mark recorded after failed create, got %+v", m)
	}
	if !backlog {
		t.Fatal("failed create must set backlog=true so the fast retry re-attempts the item")
	}
}
