package obs

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// The heartbeat gauge is written by EVERY Project (stampScan, SweepProject, and
// the runScans rehydration), so without `project` in the label set three
// Projects share one series and clobber each other last-write-wins - issue #441,
// the same failure and the same fix as temporalio/temporal#9600. `project` is
// positionally first, matching TasksMintedPerSweep{project,stage} and
// SweepMintCapHitTotal{project,cap} in this same file.
func TestSweepLastSuccessTimestampLabels(t *testing.T) {
	SweepLastSuccessTimestamp.WithLabelValues("label-test-proj", "issueScan").Set(1)
	assertLabelNames(t, gatheredLabelNames(t, SweepLastSuccessTimestamp,
		"operator_sweep_last_success_timestamp_seconds"),
		[]string{"activity", "project"})
}

func TestSweepErrorsTotalLabels(t *testing.T) {
	SweepErrorsTotal.WithLabelValues("label-test-proj", "issueScan", "invalid_cron").Inc()
	assertLabelNames(t, gatheredLabelNames(t, SweepErrorsTotal,
		"operator_sweep_errors_total"),
		[]string{"activity", "project", "reason"})
}

func assertLabelNames(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("label names = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("label names = %v, want %v", got, want)
		}
	}
}

// SweepErrorsTotal must expose the full (activity x reason) closed set at zero
// for a Project, so increase(operator_sweep_errors_total[1h]) is well-defined
// from that Project's FIRST RECONCILE rather than from its first error - a
// CounterVec with no WithLabelValues call has NO series at all. Project names
// are not known at process start, so this seeding moved out of init() and onto
// the Project reconcile path (issue #441). It must be idempotent: Reconcile
// calls it on every pass.
func TestSeedSweepErrorsForProject(t *testing.T) {
	const wantPerProject = 2*13 + 3*6 // sweep/nightlySweep x 13, brainstorm/documentation/issueScan x 6

	before := testutil.CollectAndCount(SweepErrorsTotal)
	SeedSweepErrorsForProject("seed-test-proj")
	after := testutil.CollectAndCount(SweepErrorsTotal)
	if after-before != wantPerProject {
		t.Fatalf("seeding added %d series, want %d", after-before, wantPerProject)
	}

	SeedSweepErrorsForProject("seed-test-proj")
	if again := testutil.CollectAndCount(SweepErrorsTotal); again != after {
		t.Fatalf("re-seeding the same project added %d series, want 0 (must be idempotent)", again-after)
	}

	SeedSweepErrorsForProject("seed-test-proj-2")
	if two := testutil.CollectAndCount(SweepErrorsTotal); two-after != wantPerProject {
		t.Fatalf("seeding a SECOND project added %d series, want %d (per-project, not shared)", two-after, wantPerProject)
	}
}
