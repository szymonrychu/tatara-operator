package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/robfig/cron/v3"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// nextExpectedSeriesExists reports whether obs.SweepNextExpectedTimestamp
// currently has a published series for (project, activity), WITHOUT creating
// one as a side effect - unlike testutil.ToFloat64(vec.WithLabelValues(...)),
// which creates the child and reads its (zero) value, so it cannot distinguish
// "no series was ever published" from "a series was published holding zero".
// A regression that published epoch-zero instead of deleting the series on
// disable would read as "no series" under the old assertion and go green;
// this reads the real gathered state instead. Mirrors sweepErrorsTotalSeries
// in sweep_test.go.
func nextExpectedSeriesExists(t *testing.T, project, activity string) bool {
	t.Helper()
	families, err := ctrlmetrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != "operator_sweep_next_expected_timestamp_seconds" {
			continue
		}
		for _, m := range fam.GetMetric() {
			var gotProject, gotActivity string
			for _, lp := range m.GetLabel() {
				switch lp.GetName() {
				case "project":
					gotProject = lp.GetValue()
				case "activity":
					gotActivity = lp.GetValue()
				}
			}
			if gotProject == project && gotActivity == activity {
				return true
			}
		}
	}
	return false
}

// nextExpectedUnix is the producer-side half of the cadence-aware heartbeat
// (issue #441 / tatara-observability#65): the operator owns each activity's cron
// in the Project CR, so it publishes the next expected run itself and the alert
// rule carries one grace period instead of a per-activity threshold table. This
// is kube-state-metrics' kube_cronjob_next_schedule_time pattern.
//
// It is the correct formula for brainstorm/documentation, which run through
// activityDue with no per-repo phase shift. issueScan/sweep do NOT run through
// this formula in production (see earliestIssueScanFire) - they run through
// reposDueForScan's per-repo offset instead - but this pure-function table
// still exercises nextExpectedUnix's own contract directly (empty/unparseable
// schedule, never-run fallback to CreationTimestamp), independent of that.
//
// All times are time.Local on purpose: cron.ParseStandard's SpecSchedule carries
// Location = time.Local, so Next() returns the local wall-clock fire time.
func TestNextExpectedUnix(t *testing.T) {
	created := time.Date(2026, 7, 1, 12, 0, 0, 0, time.Local)
	last := time.Date(2026, 7, 24, 3, 0, 0, 0, time.Local)

	proj := &tatarav1alpha1.Project{}
	proj.Name = "nx-proj"
	proj.CreationTimestamp = metav1.NewTime(created)
	lastStamp := metav1.NewTime(last)

	cases := []struct {
		name     string
		schedule string
		last     *metav1.Time
		wantOK   bool
		want     time.Time
	}{
		{
			name:     "nightly docs cron from last success",
			schedule: "0 3 * * *",
			last:     &lastStamp,
			wantOK:   true,
			want:     time.Date(2026, 7, 25, 3, 0, 0, 0, time.Local),
		},
		{
			name:     "nightly brainstorm cron from last success",
			schedule: "0 6 * * *",
			last:     &lastStamp,
			wantOK:   true,
			want:     time.Date(2026, 7, 24, 6, 0, 0, 0, time.Local),
		},
		{
			name:     "four-hourly sweep cron from last success",
			schedule: "0 */4 * * *",
			last:     &lastStamp,
			wantOK:   true,
			want:     time.Date(2026, 7, 24, 4, 0, 0, 0, time.Local),
		},
		{
			name:     "never run falls back to the project creation timestamp",
			schedule: "0 3 * * *",
			last:     nil,
			wantOK:   true,
			want:     time.Date(2026, 7, 2, 3, 0, 0, 0, time.Local),
		},
		{
			name:     "empty schedule is disabled",
			schedule: "",
			last:     &lastStamp,
			wantOK:   false,
		},
		{
			name:     "unparseable schedule",
			schedule: "not a cron",
			last:     &lastStamp,
			wantOK:   false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := nextExpectedUnix(proj, tc.schedule, tc.last)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if !tc.wantOK {
				return
			}
			if want := float64(tc.want.Unix()); got != want {
				t.Fatalf("next expected = %v (%s), want %v (%s)",
					got, time.Unix(int64(got), 0), want, tc.want)
			}
		})
	}
}

// TestEarliestIssueScanFire is the pure-function table test for the
// phase-shifted issueScan/sweep formula (review finding: the plain
// nextExpectedUnix boundary is not when issueScan/sweep actually fires -
// reposDueForScan phase-shifts each repo by a deterministic per-repo offset).
func TestEarliestIssueScanFire(t *testing.T) {
	created := time.Date(2026, 7, 1, 12, 0, 0, 0, time.Local)
	last := time.Date(2026, 7, 24, 3, 0, 0, 0, time.Local)

	proj := &tatarav1alpha1.Project{}
	proj.Name = "es-proj"
	proj.CreationTimestamp = metav1.NewTime(created)
	lastStamp := metav1.NewTime(last)
	oneRepo := []tatarav1alpha1.Repository{{}}
	oneRepo[0].Name = "es-proj-repo"

	t.Run("matches repoNextFire computed independently for a single repo", func(t *testing.T) {
		sched, err := cron.ParseStandard("0 */4 * * *")
		if err != nil {
			t.Fatalf("parse fixture cron: %v", err)
		}
		period := cronPeriod(sched, last)
		off := scanOffset(proj.Name, oneRepo[0].Name, "issueScan", period)
		want := float64(repoNextFire(sched, off, last).Unix())

		got, ok, cronErr := earliestIssueScanFire(proj, "0 */4 * * *", oneRepo, &lastStamp)
		if !ok || cronErr != nil {
			t.Fatalf("ok=%v cronErr=%v, want ok=true cronErr=nil", ok, cronErr)
		}
		if got != want {
			t.Fatalf("earliest = %v (%s), want %v (%s)", got, time.Unix(int64(got), 0), want, time.Unix(int64(want), 0))
		}
	})

	t.Run("empty schedule is disabled", func(t *testing.T) {
		_, ok, cronErr := earliestIssueScanFire(proj, "", oneRepo, &lastStamp)
		if ok || cronErr != nil {
			t.Fatalf("ok=%v cronErr=%v, want ok=false cronErr=nil", ok, cronErr)
		}
	})

	t.Run("unparseable schedule reports a cronErr", func(t *testing.T) {
		_, ok, cronErr := earliestIssueScanFire(proj, "x x x x x", oneRepo, &lastStamp)
		if ok || cronErr == nil {
			t.Fatalf("ok=%v cronErr=%v, want ok=false cronErr!=nil", ok, cronErr)
		}
	})

	t.Run("zero repos: no series, no cron error", func(t *testing.T) {
		_, ok, cronErr := earliestIssueScanFire(proj, "0 */4 * * *", nil, &lastStamp)
		if ok || cronErr != nil {
			t.Fatalf("ok=%v cronErr=%v, want ok=false cronErr=nil (empty repo list is config, not an error)", ok, cronErr)
		}
	})

	t.Run("never run falls back to the project creation timestamp", func(t *testing.T) {
		sched, err := cron.ParseStandard("0 3 * * *")
		if err != nil {
			t.Fatalf("parse fixture cron: %v", err)
		}
		period := cronPeriod(sched, created)
		off := scanOffset(proj.Name, oneRepo[0].Name, "issueScan", period)
		want := float64(repoNextFire(sched, off, created).Unix())

		got, ok, cronErr := earliestIssueScanFire(proj, "0 3 * * *", oneRepo, nil)
		if !ok || cronErr != nil {
			t.Fatalf("ok=%v cronErr=%v, want ok=true cronErr=nil", ok, cronErr)
		}
		if got != want {
			t.Fatalf("earliest = %v, want %v", got, want)
		}
	})

	t.Run("multiple repos: takes the minimum fire, not the first", func(t *testing.T) {
		sched, err := cron.ParseStandard("0 */4 * * *")
		if err != nil {
			t.Fatalf("parse fixture cron: %v", err)
		}
		repos := []tatarav1alpha1.Repository{{}, {}, {}}
		repos[0].Name = "repo-a"
		repos[1].Name = "repo-b"
		repos[2].Name = "repo-c"
		period := cronPeriod(sched, last)
		want := repoNextFire(sched, scanOffset(proj.Name, repos[0].Name, "issueScan", period), last)
		for _, rp := range repos[1:] {
			if fire := repoNextFire(sched, scanOffset(proj.Name, rp.Name, "issueScan", period), last); fire.Before(want) {
				want = fire
			}
		}

		got, ok, cronErr := earliestIssueScanFire(proj, "0 */4 * * *", repos, &lastStamp)
		if !ok || cronErr != nil {
			t.Fatalf("ok=%v cronErr=%v, want ok=true cronErr=nil", ok, cronErr)
		}
		if got != float64(want.Unix()) {
			t.Fatalf("earliest = %v, want %v (the minimum across repos)", got, want.Unix())
		}
	})
}

// TestPublishNextExpected_ThroughRunScans: the gauge must be published for every
// ENABLED activity on a real reconcile pass, and for NO disabled one - and a
// series that was published while an activity was enabled must be actively
// retracted once it is disabled, not left frozen at its last value (review
// finding: a GaugeVec child, once created, is exported for the life of the
// process).
func TestPublishNextExpected_ThroughRunScans(t *testing.T) {
	t.Run("enabled activities publish the true phase-shifted fire, disabled ones do not", func(t *testing.T) {
		cronSpec := &tatarav1alpha1.ScmCron{
			// Yearly: never due inside a test run, so no fresh pass runs, no forge
			// call happens, and the deferred publication is what is under test.
			IssueScan:     tatarav1alpha1.CronActivity{Schedule: "0 0 1 1 *"},
			Brainstorm:    tatarav1alpha1.BrainstormActivity{Enabled: false, Schedule: "0 6 * * *"},
			Documentation: tatarav1alpha1.CronActivity{Schedule: "0 3 * * *"},
		}
		proj, repo := seedScanProject(t, "nx-enabled", cronSpec)
		last := metav1.NewTime(time.Date(2026, 7, 24, 3, 0, 0, 0, time.Local))
		proj.Status.LastIssueScan = &last
		if err := k8sClient.Status().Update(context.Background(), proj); err != nil {
			t.Fatalf("seed LastIssueScan: %v", err)
		}

		r := newScanReconciler(&fakeReader{})
		r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
		if _, err := r.runScans(context.Background(), proj); err != nil {
			t.Fatalf("runScans: %v", err)
		}

		// issueScan/sweep fire phase-shifted per repo (reposDueForScan's own
		// scanOffset/repoNextFire arithmetic), NOT at the raw "0 0 1 1 *" cron
		// boundary (review finding). seedScanProject enrolls exactly one repo, so
		// the earliest fire is that repo's own hashed offset applied to the cron.
		// Computed here via the same production helpers rather than a hardcoded
		// timestamp, so the assertion tracks the formula rather than freezing
		// today's hash output.
		sched, err := cron.ParseStandard(cronSpec.IssueScan.Schedule)
		if err != nil {
			t.Fatalf("parse fixture cron: %v", err)
		}
		period := cronPeriod(sched, last.Time)
		off := scanOffset(proj.Name, repo.Name, "issueScan", period)
		want := float64(repoNextFire(sched, off, last.Time).Unix())
		for _, activity := range []string{"issueScan", SweepActivity} {
			if got := testutil.ToFloat64(obs.SweepNextExpectedTimestamp.WithLabelValues("nx-enabled", activity)); got != want {
				t.Errorf("next expected{%s} = %v, want %v", activity, got, want)
			}
		}
		// brainstorm has a schedule but Enabled=false: no series at all.
		if nextExpectedSeriesExists(t, "nx-enabled", "brainstorm") {
			t.Error("next expected{brainstorm} series exists, want none (activity is disabled)")
		}
		// documentation has a schedule but Spec.Documentation is nil: no series.
		if nextExpectedSeriesExists(t, "nx-enabled", "documentation") {
			t.Error("next expected{documentation} series exists, want none (documentation not enabled)")
		}
	})

	t.Run("unparseable schedule publishes no series and meters invalid_cron", func(t *testing.T) {
		cronSpec := &tatarav1alpha1.ScmCron{
			// "x x x x x" satisfies the CRD's 5-field Pattern (so admission accepts
			// it, unlike "not a cron" which is only 3 tokens) but is not a valid
			// cron.ParseStandard schedule.
			IssueScan: tatarav1alpha1.CronActivity{Schedule: "x x x x x"},
		}
		proj, _ := seedScanProject(t, "nx-badcron", cronSpec)

		before := testutil.ToFloat64(obs.SweepErrorsTotal.WithLabelValues("nx-badcron", "issueScan", "invalid_cron"))

		r := newScanReconciler(&fakeReader{})
		r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
		if _, err := r.runScans(context.Background(), proj); err != nil {
			t.Fatalf("runScans: %v", err)
		}

		if nextExpectedSeriesExists(t, "nx-badcron", "issueScan") {
			t.Error("next expected{issueScan} series exists, want none (unparseable cron publishes no series)")
		}
		if got := testutil.ToFloat64(obs.SweepErrorsTotal.WithLabelValues("nx-badcron", "issueScan", "invalid_cron")); got != before+1 {
			t.Errorf("invalid_cron = %v, want %v (exactly once per tick, not once per detection site)", got, before+1)
		}
	})

	t.Run("documentation with Enabled=false publishes no series", func(t *testing.T) {
		cronSpec := &tatarav1alpha1.ScmCron{
			Documentation: tatarav1alpha1.CronActivity{Schedule: "0 3 * * *"},
		}
		proj, _ := seedScanProject(t, "nx-doc-off", cronSpec)
		proj.Spec.Documentation = &tatarav1alpha1.DocumentationSpec{Enabled: false, Repo: "https://github.com/o/docs.git"}

		r := newScanReconciler(&fakeReader{})
		r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
		if _, err := r.runScans(context.Background(), proj); err != nil {
			t.Fatalf("runScans: %v", err)
		}
		if nextExpectedSeriesExists(t, "nx-doc-off", "documentation") {
			t.Error("next expected{documentation} series exists, want none (Documentation.Enabled=false)")
		}
	})

	t.Run("documentation Enabled=true with empty Repo publishes no series", func(t *testing.T) {
		cronSpec := &tatarav1alpha1.ScmCron{
			Documentation: tatarav1alpha1.CronActivity{Schedule: "0 3 * * *"},
		}
		proj, _ := seedScanProject(t, "nx-doc-norepo", cronSpec)
		proj.Spec.Documentation = &tatarav1alpha1.DocumentationSpec{Enabled: true, Repo: ""}

		r := newScanReconciler(&fakeReader{})
		r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
		if _, err := r.runScans(context.Background(), proj); err != nil {
			t.Fatalf("runScans: %v", err)
		}
		if nextExpectedSeriesExists(t, "nx-doc-norepo", "documentation") {
			t.Error("next expected{documentation} series exists, want none (Documentation.Repo empty)")
		}
	})

	t.Run("sweep-disabled annotation publishes no issueScan or sweep series", func(t *testing.T) {
		cronSpec := &tatarav1alpha1.ScmCron{
			IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 */4 * * *"},
		}
		proj, _ := seedScanProject(t, "nx-sweep-off", cronSpec)
		proj.Annotations = map[string]string{SweepAnnotation: SweepDisabledValue}

		r := newScanReconciler(&fakeReader{})
		r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
		if _, err := r.runScans(context.Background(), proj); err != nil {
			t.Fatalf("runScans: %v", err)
		}
		for _, activity := range []string{"issueScan", SweepActivity} {
			if nextExpectedSeriesExists(t, "nx-sweep-off", activity) {
				t.Errorf("next expected{%s} series exists, want none (sweep disabled via annotation)", activity)
			}
		}
	})

	t.Run("enabled-then-disabled transition retracts a previously published series", func(t *testing.T) {
		cronSpec := &tatarav1alpha1.ScmCron{
			// Yearly, same reasoning as the first subtest: never due inside a test
			// run, so only the deferred publication is under test.
			Brainstorm: tatarav1alpha1.BrainstormActivity{Enabled: true, Schedule: "0 0 1 1 *"},
		}
		proj, _ := seedScanProject(t, "nx-transition", cronSpec)

		r := newScanReconciler(&fakeReader{})
		r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
		if _, err := r.runScans(context.Background(), proj); err != nil {
			t.Fatalf("runScans (enabled pass): %v", err)
		}
		if !nextExpectedSeriesExists(t, "nx-transition", "brainstorm") {
			t.Fatalf("next expected{brainstorm} series missing after an enabled pass")
		}

		// Flip Enabled off, exactly as a human editing the Project spec would,
		// and reconcile again with the SAME in-memory object runScans already
		// operates on directly (it never re-fetches Spec for this check).
		proj.Spec.Scm.Cron.Brainstorm.Enabled = false
		if _, err := r.runScans(context.Background(), proj); err != nil {
			t.Fatalf("runScans (disabled pass): %v", err)
		}
		if nextExpectedSeriesExists(t, "nx-transition", "brainstorm") {
			t.Error("next expected{brainstorm} series still exists after Enabled flipped to false, want retracted")
		}
	})

	// Round-2 review finding: repos was nil on both pre-list early returns
	// (scanReader failure, projectReposForScan failure), and earliestIssueScanFire
	// cannot tell "never fetched this tick" from "genuinely zero repos" - both
	// look like an empty slice - so publishNextExpected retracted the series on
	// EVERY tick a Project's reader is broken (e.g. a deleted scmSecretRef),
	// silencing the exact alert this gauge exists to raise. reposLoaded now
	// distinguishes the two; these two subtests keep them distinguished.
	t.Run("scanReader failure leaves an existing issueScan/sweep series intact", func(t *testing.T) {
		cronSpec := &tatarav1alpha1.ScmCron{
			IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 */4 * * *"},
		}
		proj, _ := seedScanProject(t, "nx-reader-fail", cronSpec)

		r := newScanReconciler(&fakeReader{})
		r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
		if _, err := r.runScans(context.Background(), proj); err != nil {
			t.Fatalf("runScans (healthy pass): %v", err)
		}
		before := testutil.ToFloat64(obs.SweepNextExpectedTimestamp.WithLabelValues("nx-reader-fail", "issueScan"))
		if before == 0 {
			t.Fatalf("setup: expected a published issueScan series after a healthy pass")
		}

		// Break the reader exactly like a deleted/renamed scmSecretRef would:
		// scanReader's r.Get on the secret now fails, so runScans returns a
		// requeue with a NIL error (no reconcile-error metric, just a log line)
		// before repos is ever listed - the concrete scenario the review named.
		proj.Spec.ScmSecretRef = "does-not-exist"
		if _, err := r.runScans(context.Background(), proj); err != nil {
			t.Fatalf("runScans (broken reader pass): %v", err)
		}

		if !nextExpectedSeriesExists(t, "nx-reader-fail", "issueScan") {
			t.Fatal("next expected{issueScan} series was retracted on a reader failure, want it left intact")
		}
		if got := testutil.ToFloat64(obs.SweepNextExpectedTimestamp.WithLabelValues("nx-reader-fail", "issueScan")); got != before {
			t.Errorf("next expected{issueScan} = %v after a reader failure, want unchanged %v", got, before)
		}
		if !nextExpectedSeriesExists(t, "nx-reader-fail", SweepActivity) {
			t.Error("next expected{sweep} series was retracted on a reader failure, want it left intact")
		}
	})

	t.Run("genuinely zero enrolled repos still retracts (distinct from a reader failure)", func(t *testing.T) {
		cronSpec := &tatarav1alpha1.ScmCron{
			IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 */4 * * *"},
		}
		proj, repo := seedScanProject(t, "nx-zero-repos", cronSpec)

		r := newScanReconciler(&fakeReader{})
		r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
		if _, err := r.runScans(context.Background(), proj); err != nil {
			t.Fatalf("runScans (healthy pass): %v", err)
		}
		if !nextExpectedSeriesExists(t, "nx-zero-repos", "issueScan") {
			t.Fatalf("setup: expected a published issueScan series after a healthy pass")
		}

		// Delete the one enrolled repo: projectReposForScan now succeeds
		// (reposLoaded=true) but returns an empty list - a genuinely repo-less
		// Project, which must still retract, unlike the reader-failure case above
		// (reposLoaded=false) where the list call never even ran.
		if err := k8sClient.Delete(context.Background(), repo); err != nil {
			t.Fatalf("delete fixture repo: %v", err)
		}
		if _, err := r.runScans(context.Background(), proj); err != nil {
			t.Fatalf("runScans (zero-repo pass): %v", err)
		}

		if nextExpectedSeriesExists(t, "nx-zero-repos", "issueScan") {
			t.Error("next expected{issueScan} series exists after enrolling zero repos, want retracted")
		}
		if nextExpectedSeriesExists(t, "nx-zero-repos", SweepActivity) {
			t.Error("next expected{sweep} series exists after enrolling zero repos, want retracted")
		}
	})
}

// TestProjectReconcile_NotFound_RetractsNextExpected: finding 2 named four
// disable paths (brainstorm off, sweep-disabled annotation, documentation off,
// documentation repo cleared) and the round-1 fix covered all four inside
// publishNextExpected - but a DELETED Project never reaches publishNextExpected
// at all, since runScans is never called for an object that no longer exists.
// ProjectReconciler.Reconcile returns immediately on IsNotFound; without explicit
// teardown there a deleted Project's series stays frozen in the past forever,
// and the consumer rule is a time()-minus-gauge comparison, so this pages
// permanently until the pod restarts (round-2 review finding).
func TestProjectReconcile_NotFound_RetractsNextExpected(t *testing.T) {
	obs.SweepNextExpectedTimestamp.WithLabelValues("nx-gone", "issueScan").Set(1700000000)
	obs.SweepNextExpectedTimestamp.WithLabelValues("nx-gone", SweepActivity).Set(1700000000)

	if _, err := reconcileProject(t, "nx-gone"); err != nil {
		t.Fatalf("reconcile deleted project: %v", err)
	}

	for _, activity := range []string{"issueScan", SweepActivity} {
		if nextExpectedSeriesExists(t, "nx-gone", activity) {
			t.Errorf("next expected{%s} series exists after reconciling a deleted Project, want retracted", activity)
		}
	}
}
