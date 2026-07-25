package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// nextExpectedUnix is the producer-side half of the cadence-aware heartbeat
// (issue #441 / tatara-observability#65): the operator owns each activity's cron
// in the Project CR, so it publishes the next expected run itself and the alert
// rule carries one grace period instead of a per-activity threshold table. This
// is kube-state-metrics' kube_cronjob_next_schedule_time pattern.
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

// TestPublishNextExpected_ThroughRunScans: the gauge must be published for every
// ENABLED activity on a real reconcile pass, and for NO disabled one. A series
// for a configured-but-off activity would page for a run that is never going to
// happen.
func TestPublishNextExpected_ThroughRunScans(t *testing.T) {
	t.Run("enabled activities publish, disabled ones do not", func(t *testing.T) {
		cronSpec := &tatarav1alpha1.ScmCron{
			// Yearly: never due inside a test run, so no fresh pass runs, no forge
			// call happens, and the deferred publication is what is under test.
			IssueScan:     tatarav1alpha1.CronActivity{Schedule: "0 0 1 1 *"},
			Brainstorm:    tatarav1alpha1.BrainstormActivity{Enabled: false, Schedule: "0 6 * * *"},
			Documentation: tatarav1alpha1.CronActivity{Schedule: "0 3 * * *"},
		}
		proj, _ := seedScanProject(t, "nx-enabled", cronSpec)
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

		// "0 0 1 1 *" is 1 January. Next() after a 2026-07-24 base is 2027-01-01
		// at local midnight (cron.ParseStandard schedules carry Location =
		// time.Local).
		want := float64(time.Date(2027, 1, 1, 0, 0, 0, 0, time.Local).Unix())
		for _, activity := range []string{"issueScan", SweepActivity} {
			if got := testutil.ToFloat64(obs.SweepNextExpectedTimestamp.WithLabelValues("nx-enabled", activity)); got != want {
				t.Errorf("next expected{%s} = %v, want %v", activity, got, want)
			}
		}
		// brainstorm has a schedule but Enabled=false: no series.
		if got := testutil.ToFloat64(obs.SweepNextExpectedTimestamp.WithLabelValues("nx-enabled", "brainstorm")); got != 0 {
			t.Errorf("next expected{brainstorm} = %v, want 0 (activity is disabled, no series)", got)
		}
		// documentation has a schedule but Spec.Documentation is nil: no series.
		if got := testutil.ToFloat64(obs.SweepNextExpectedTimestamp.WithLabelValues("nx-enabled", "documentation")); got != 0 {
			t.Errorf("next expected{documentation} = %v, want 0 (documentation not enabled, no series)", got)
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

		if got := testutil.ToFloat64(obs.SweepNextExpectedTimestamp.WithLabelValues("nx-badcron", "issueScan")); got != 0 {
			t.Errorf("next expected{issueScan} = %v, want 0 (unparseable cron publishes no series)", got)
		}
		if got := testutil.ToFloat64(obs.SweepErrorsTotal.WithLabelValues("nx-badcron", "issueScan", "invalid_cron")); got != before+1 {
			t.Errorf("invalid_cron = %v, want %v (exactly once per tick, not once per detection site)", got, before+1)
		}
	})
}
