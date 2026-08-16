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
	// sweep x 21 (nightlySweep dropped: dead, no live producer; +4 for the
	// fail() sites the list had drifted from, issue #495; +resolve_live_owner,
	// issue #521; +adopt_upgrade_mr and +count_upgrade_lanes, the two failure
	// sites of the dependency-MR adoption arm; +record_upgrade_deferral, the
	// lane-release deferral record), brainstorm x 1 (demand-driven now, only
	// stamp_failed can fire), documentation/issueScan x 2 each (invalid_cron,
	// stamp_failed), refine x 3 (its own cron, Task 3), upgrade x 3 (plain cron
	// plus its own capacity-count failure).
	const wantPerProject = 21 + 1 + 2*2 + 3 + 3

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
	// (sweep, webhook) x the closed SweepSkip* vocabulary. The webhook is a
	// PRODUCER of this counter since issue #521's review: MintForItem names the
	// clause that refused an issue, and a webhook mint is not a sweep pass.
	// The 9th member is upgrade_headroom_bound (dependency-MR adoption deferred
	// to a later pass by maxOpenUpgrades).
	const wantPerProject = 2 * 9

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

// TestSweepSkipReasonsMatchSweepConstants is the skip-side twin of
// TestSweepSeedReasonsCoverEveryFailSite, and it exists for the same reason:
// sweepSkipReasons carries a "keep in sync with sweep.go's SweepSkip*
// constants" comment, and a comment cannot enforce that. An unseeded skip
// reason has NO series until its first skip, and a counter born AT its first
// skip has no earlier sample to increase from - so
// increase(operator_sweep_skipped_total{reason=...}[1h]) is blind to exactly
// the first skip after every pod roll, which is the observability hole issue
// #521 spent 19 hours inside. Fails BOTH ways: an unseeded constant, and a
// seeded reason with no constant left.
func TestSweepSkipReasonsMatchSweepConstants(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("..", "controller", "sweep.go"))
	if err != nil {
		t.Fatalf("read sweep.go: %v", err)
	}
	seeded := map[string]bool{}
	for _, r := range sweepSkipReasons {
		seeded[r] = true
	}
	declared := map[string]bool{}
	for _, m := range regexp.MustCompile(`SweepSkip[A-Za-z]+\s*=\s*"([a-z_]+)"`).FindAllStringSubmatch(string(src), -1) {
		declared[m[1]] = true
	}
	if len(declared) == 0 {
		t.Fatal("found no SweepSkip* constants in sweep.go - the scan is broken, not the seed list")
	}
	for reason := range declared {
		if !seeded[reason] {
			t.Errorf("sweep.go declares skip reason %q but sweepSkipReasons does not seed it: "+
				"increase() cannot see its first increment", reason)
		}
	}
	for reason := range seeded {
		if !declared[reason] {
			t.Errorf("sweepSkipReasons seeds %q but no SweepSkip* constant declares it: "+
				"a permanently dead zero series", reason)
		}
	}
}

func TestSweepStaleOwnerRepairedTotalLabels(t *testing.T) {
	SweepStaleOwnerRepairedTotal.WithLabelValues("label-test-proj", "sweep").Inc()
	assertLabelNames(t, gatheredLabelNames(t, SweepStaleOwnerRepairedTotal,
		"operator_sweep_stale_owner_repaired_total"),
		[]string{"activity", "project"})
}

func TestMintOutcomeTotalLabels(t *testing.T) {
	MintOutcomeTotal.WithLabelValues("clarify", "created").Inc()
	assertLabelNames(t, gatheredLabelNames(t, MintOutcomeTotal,
		"operator_intake_mint_outcome_total"),
		[]string{"kind", "outcome"})
}

func TestSweepOrphanStrandedSecondsLabels(t *testing.T) {
	SweepOrphanStrandedSeconds.WithLabelValues("label-test-proj", "tatara-operator", "510").Set(1)
	assertLabelNames(t, gatheredLabelNames(t, SweepOrphanStrandedSeconds,
		"operator_sweep_orphan_stranded_seconds"),
		[]string{"number", "project", "repo"})
}

// The gauge carries a per-issue label, so a healed issue's series MUST leave
// the registry or /metrics grows without bound (contract K.1 CARDINALITY, the
// same rule ClearMergeCursorStalled exists for). Clearing is scoped to
// (project, repo) because SweepProject is called with dueRepos, not every repo.
func TestClearSweepOrphanStranded(t *testing.T) {
	SweepOrphanStrandedSeconds.WithLabelValues("clear-proj", "repo-a", "1").Set(1)
	SweepOrphanStrandedSeconds.WithLabelValues("clear-proj", "repo-b", "2").Set(1)
	ClearSweepOrphanStranded("clear-proj", "repo-a")
	if n := testutil.CollectAndCount(SweepOrphanStrandedSeconds); n < 1 {
		t.Fatal("clearing one repo removed every series")
	}
	ClearSweepOrphanStranded("clear-proj", "repo-a")
	SweepOrphanStrandedSeconds.WithLabelValues("clear-proj", "repo-a", "1").Set(1)
	ClearSweepOrphanStranded("clear-proj", "repo-a")
	ClearSweepOrphanStranded("clear-proj", "repo-b")
}

// TestRetainSweepOrphanStranded is the deadman's cardinality half.
// ClearSweepOrphanStranded only ever runs from INSIDE a sweep pass, so every
// transition that stops the sweep looking at a (project, repo) - the sweep
// disabled, the issueScan cron emptied, a Repository unenrolled, the Project
// deleted - freezes that series at its last value and it is scraped forever.
// The alert is max by (project) over a threshold, so a frozen 19h series pages
// permanently until the pod rolls.
func TestRetainSweepOrphanStranded(t *testing.T) {
	SweepOrphanStrandedSeconds.Reset()
	SweepOrphanStrandedSeconds.WithLabelValues("retain-proj", "kept-repo", "1").Set(1)
	SweepOrphanStrandedSeconds.WithLabelValues("retain-proj", "gone-repo", "2").Set(1)
	SweepOrphanStrandedSeconds.WithLabelValues("other-proj", "gone-repo", "3").Set(1)

	RetainSweepOrphanStranded("retain-proj", []string{"kept-repo"})
	if n := testutil.CollectAndCount(SweepOrphanStrandedSeconds); n != 2 {
		t.Fatalf("series after retain = %d, want 2 (kept-repo and the OTHER project's, untouched)", n)
	}

	// nil keep retracts the whole project: that IS the disabled / no-cron /
	// deleted case.
	RetainSweepOrphanStranded("retain-proj", nil)
	if n := testutil.CollectAndCount(SweepOrphanStrandedSeconds); n != 1 {
		t.Fatalf("series after full retract = %d, want 1 (only the other project's)", n)
	}
	RetainSweepOrphanStranded("other-proj", nil)
	if n := testutil.CollectAndCount(SweepOrphanStrandedSeconds); n != 0 {
		t.Fatalf("series after retracting every project = %d, want 0", n)
	}
}

// TestWebhookActivityMatchesTheControllerConstant pins obs.WebhookActivity to
// controller.WebhookActivity. The literal is duplicated here to avoid a reverse
// import (the sweepSeedReasons precedent), and a duplicated literal that
// nothing checks is how a seeded activity silently stops matching its producer.
func TestWebhookActivityMatchesTheControllerConstant(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("..", "controller", "intake.go"))
	if err != nil {
		t.Fatalf("read intake.go: %v", err)
	}
	m := regexp.MustCompile(`WebhookActivity\s*=\s*"([a-z_]+)"`).FindStringSubmatch(string(src))
	if m == nil {
		t.Fatal("found no WebhookActivity constant in intake.go - the scan is broken, not the constant")
	}
	if m[1] != WebhookActivity {
		t.Fatalf("controller.WebhookActivity = %q but obs.WebhookActivity = %q: the seeded series "+
			"and the producer's label have drifted apart", m[1], WebhookActivity)
	}
}
