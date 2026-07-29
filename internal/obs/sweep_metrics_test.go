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

func TestSweepNextExpectedTimestampLabels(t *testing.T) {
	SweepNextExpectedTimestamp.WithLabelValues("label-test-proj", "documentation").Set(1)
	assertLabelNames(t, gatheredLabelNames(t, SweepNextExpectedTimestamp,
		"operator_sweep_next_expected_timestamp_seconds"),
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
	// sweep x 13 (nightlySweep dropped: dead, no live producer), brainstorm x 1
	// (demand-driven now, only stamp_failed can fire), documentation/issueScan
	// x 2 each (invalid_cron, stamp_failed), refine x 3 (its own cron, Task 3).
	const wantPerProject = 13 + 1 + 2*2 + 3

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

func TestSweepSkippedTotalLabels(t *testing.T) {
	SweepSkippedTotal.WithLabelValues("label-test-proj", "sweep", "mr_claimed_by_other_task").Inc()
	assertLabelNames(t, gatheredLabelNames(t, SweepSkippedTotal,
		"operator_sweep_skipped_total"),
		[]string{"activity", "project", "reason"})
}

// SweepSkippedTotal carries the same zero-baseline requirement as
// SweepErrorsTotal: the skip it counts (issue #477's already-claimed MR) is a
// BENIGN steady state, so the series that proves it is NOT happening has to
// exist before the first skip does.
func TestSeedSweepSkippedForProject(t *testing.T) {
	const wantPerProject = 1 // sweep x mr_claimed_by_other_task

	before := testutil.CollectAndCount(SweepSkippedTotal)
	SeedSweepErrorsForProject("skip-seed-proj")
	after := testutil.CollectAndCount(SweepSkippedTotal)
	if after-before != wantPerProject {
		t.Fatalf("seeding added %d skip series, want %d", after-before, wantPerProject)
	}
	SeedSweepErrorsForProject("skip-seed-proj")
	if again := testutil.CollectAndCount(SweepSkippedTotal); again != after {
		t.Fatalf("re-seeding added %d skip series, want 0 (must be idempotent)", again-after)
	}
}
