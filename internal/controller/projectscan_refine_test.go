package controller

// The refine pre-scan barrier that used to gate on the brainstorm cron tick is
// gone - see projectscan_refine_cron_test.go for refine's own cron and its
// tests. TestRefine_OnePerProjectPerCycle below still applies unchanged: one
// refine Task per due tick, not one per reconcile.

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// listRefineQEs returns QueuedEvents for the project whose kind == "refine".
func listRefineQEs(t *testing.T, project string) []tatarav1alpha1.QueuedEvent {
	t.Helper()
	qes := listScanQEs(t, project)
	var out []tatarav1alpha1.QueuedEvent
	for _, qe := range qes {
		if qe.Spec.Kind == "refine" || qe.Spec.Payload.Kind == "refine" {
			out = append(out, qe)
		}
	}
	return out
}

// listBrainstormQEs is defined in projectscan_run_test.go.

// TestRefine_OnePerProjectPerCycle: with a refine task already in flight, a
// second reconcile does NOT create a second refine QueuedEvent.
func TestRefine_OnePerProjectPerCycle(t *testing.T) {
	proj := seedRefineCronProject(t, "refine-dedup")
	reader := &fakeReader{}
	r := newScanReconciler(reader)
	r.Metrics = obs.NewOperatorMetrics(prometheus.NewRegistry())

	ctx := context.Background()

	if _, _, _, _, err := r.runScans(ctx, proj); err != nil {
		t.Fatalf("runScans 1: %v", err)
	}
	if len(listRefineQEs(t, "refine-dedup")) != 1 {
		t.Fatalf("want exactly 1 refine QE after first run")
	}

	if _, _, _, _, err := r.runScans(ctx, proj); err != nil {
		t.Fatalf("runScans 2: %v", err)
	}
	if len(listRefineQEs(t, "refine-dedup")) != 1 {
		t.Fatalf("want still exactly 1 refine QE (dedup), got more")
	}
}
