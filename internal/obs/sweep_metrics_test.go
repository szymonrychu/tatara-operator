package obs

import (
	"os"
	"path/filepath"
	"regexp"
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
	// sweep x 17 (nightlySweep dropped: dead, no live producer; +4 for the
	// fail() sites the list had drifted from, issue #495), brainstorm x 1
	// (demand-driven now, only stamp_failed can fire), documentation/issueScan
	// x 2 each (invalid_cron, stamp_failed), refine x 3 (its own cron, Task 3).
	const wantPerProject = 17 + 1 + 2*2 + 3

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

// TestSweepSeedReasonsCoverEveryFailSite is the #495 instrumentation half.
// sweepSeedReasons carries a "keep in sync with sweep.go's fail(reason, ...)
// call sites" comment and had silently drifted from four of them, including
// reconcile_ownership. An unseeded reason has NO series until its first error,
// and a counter series born AT its first error has no earlier sample to
// increase from - so increase(operator_sweep_errors_total{reason=...}[1h]) is
// blind to exactly the first failure after every pod roll. That is why the
// alert for this class had to be written against Loki ERROR lines instead of
// the counter that exists for it. A comment cannot enforce that; this test can.
func TestSweepSeedReasonsCoverEveryFailSite(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("..", "controller", "sweep.go"))
	if err != nil {
		t.Fatalf("read sweep.go: %v", err)
	}
	seeded := map[string]bool{}
	for _, r := range sweepSeedReasons {
		seeded[r] = true
	}
	used := map[string]bool{}
	for _, m := range regexp.MustCompile(`fail\("([a-z_]+)"`).FindAllStringSubmatch(string(src), -1) {
		used[m[1]] = true
	}
	if len(used) == 0 {
		t.Fatal("found no fail(\"...\") call sites in sweep.go - the scan is broken, not the seed list")
	}
	for reason := range used {
		if !seeded[reason] {
			t.Errorf("sweep.go fails with reason %q but sweepSeedReasons does not seed it: "+
				"increase() cannot see its first increment", reason)
		}
	}
	for reason := range seeded {
		if reason == "list_tasks" {
			continue // seeded deliberately; incremented outside fail()
		}
		if !used[reason] {
			t.Errorf("sweepSeedReasons seeds %q but no fail(%q, ...) call site remains in sweep.go: "+
				"a permanently dead zero series", reason, reason)
		}
	}
}
