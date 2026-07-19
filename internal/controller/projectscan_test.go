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

// TestRunScans_SweepGaugeRehydrate is the #386 regression test: the sweep
// heartbeat gauge (obs.SweepLastSuccessTimestamp) is process-local and dies on
// every redeploy, while Status.Last* is persisted in etcd and tracks real
// progress. runScans must rehydrate the gauges from the persisted stamps on
// every reconcile, not only when a pass freshly stamps them.
func TestRunScans_SweepGaugeRehydrate(t *testing.T) {
	reader := &fakeReader{}

	t.Run("never scanned leaves issueScan/sweep gauges unset", func(t *testing.T) {
		// Every schedule on the seeded cron is empty, so no activity runs a
		// fresh pass either - this isolates the top-of-runScans rehydrate.
		proj, _ := seedScanProject(t, "gauge-never", &tatarav1alpha1.ScmCron{})
		obs.SweepLastSuccessTimestamp.WithLabelValues("issueScan").Set(0)
		obs.SweepLastSuccessTimestamp.WithLabelValues(SweepActivity).Set(0)

		r := newScanReconciler(reader)
		r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
		if _, err := r.runScans(context.Background(), proj); err != nil {
			t.Fatalf("runScans: %v", err)
		}

		if got := testutil.ToFloat64(obs.SweepLastSuccessTimestamp.WithLabelValues("issueScan")); got != 0 {
			t.Fatalf("issueScan gauge = %v, want 0 (unset, true NoData for a never-scanned project)", got)
		}
		if got := testutil.ToFloat64(obs.SweepLastSuccessTimestamp.WithLabelValues(SweepActivity)); got != 0 {
			t.Fatalf("sweep gauge = %v, want 0 (unset, true NoData for a never-scanned project)", got)
		}
	})

	t.Run("persisted LastIssueScan rehydrates issueScan/sweep gauges with zero due repos", func(t *testing.T) {
		cron := &tatarav1alpha1.ScmCron{
			// Yearly: never due within a test run, so the sweep due-check finds
			// zero due repos and stampScan is never invoked.
			IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 0 1 1 *"},
		}
		proj, _ := seedScanProject(t, "gauge-issuescan", cron)
		want := metav1.NewTime(time.Now().Add(-3 * time.Hour))
		proj.Status.LastIssueScan = &want
		if err := k8sClient.Status().Update(context.Background(), proj); err != nil {
			t.Fatalf("seed LastIssueScan: %v", err)
		}
		obs.SweepLastSuccessTimestamp.WithLabelValues("issueScan").Set(0)
		obs.SweepLastSuccessTimestamp.WithLabelValues(SweepActivity).Set(0)

		r := newScanReconciler(reader)
		r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
		if _, err := r.runScans(context.Background(), proj); err != nil {
			t.Fatalf("runScans: %v", err)
		}

		wantUnix := float64(want.Unix())
		if got := testutil.ToFloat64(obs.SweepLastSuccessTimestamp.WithLabelValues("issueScan")); got != wantUnix {
			t.Fatalf("issueScan gauge = %v, want rehydrated %v (persisted LastIssueScan)", got, wantUnix)
		}
		if got := testutil.ToFloat64(obs.SweepLastSuccessTimestamp.WithLabelValues(SweepActivity)); got != wantUnix {
			t.Fatalf("sweep gauge = %v, want rehydrated %v (persisted LastIssueScan)", got, wantUnix)
		}
	})

	t.Run("persisted LastBrainstorm and LastDocumentation rehydrate their gauges", func(t *testing.T) {
		proj, _ := seedScanProject(t, "gauge-brainstormdoc", &tatarav1alpha1.ScmCron{})
		wantBrainstorm := metav1.NewTime(time.Now().Add(-5 * time.Hour))
		wantDocumentation := metav1.NewTime(time.Now().Add(-7 * time.Hour))
		proj.Status.LastBrainstorm = &wantBrainstorm
		proj.Status.LastDocumentation = &wantDocumentation
		if err := k8sClient.Status().Update(context.Background(), proj); err != nil {
			t.Fatalf("seed LastBrainstorm/LastDocumentation: %v", err)
		}
		obs.SweepLastSuccessTimestamp.WithLabelValues("brainstorm").Set(0)
		obs.SweepLastSuccessTimestamp.WithLabelValues("documentation").Set(0)

		r := newScanReconciler(reader)
		r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())
		if _, err := r.runScans(context.Background(), proj); err != nil {
			t.Fatalf("runScans: %v", err)
		}

		if got, want := testutil.ToFloat64(obs.SweepLastSuccessTimestamp.WithLabelValues("brainstorm")), float64(wantBrainstorm.Unix()); got != want {
			t.Fatalf("brainstorm gauge = %v, want rehydrated %v (persisted LastBrainstorm)", got, want)
		}
		if got, want := testutil.ToFloat64(obs.SweepLastSuccessTimestamp.WithLabelValues("documentation")), float64(wantDocumentation.Unix()); got != want {
			t.Fatalf("documentation gauge = %v, want rehydrated %v (persisted LastDocumentation)", got, want)
		}
	})
}
