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
	// sweep x 19 (nightlySweep dropped: dead, no live producer; +4 for the
	// fail() sites the list had drifted from, issue #495; +resolve_live_owner,
	// issue #521; +enqueue_adopt_upgrade, the sweep's adoption-enqueue arm
	// failing, Task 8 - adopt_upgrade_mr left WITH the mint it named: it is a
	// plain error return in queue_controller.go, not a fail(reason, ...) call
	// in sweep.go, so it never belonged in this sweep.go-scanned set), queue x 1
	// (admit_adopted_upgrade: the dispatcher's own admit-time re-check-then-mint
	// failure, counted under its own activity rather than sweep.go's - Task 8
	// review, hard rule 4), brainstorm x 1 (demand-driven now, only
	// stamp_failed can fire), documentation/issueScan x 2 each (invalid_cron,
	// stamp_failed), refine x 3 (its own cron, Task 3), upgrade x 3 (plain cron
	// plus its own capacity-count failure).
	const wantPerProject = 19 + 1 + 1 + 2*2 + 3 + 3

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
	// upgrade_headroom_bound is gone (Task 8): the sweep enqueues an adoptable
	// dependency merge request instead of minting it directly, so there is no
	// per-pass lane cap left to defer against.
	const wantPerProject = 2 * 8

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

// TestQueueAdmitErrorReasonIsSeeded is TestSweepSeedReasonsCoverEveryFailSite's
// twin for the DISPATCHER's own admission work (queue_controller.go), which
// sweepSeedReasons/that test never scans. admitAdoptedUpgrade's genuine
// machinery failures (a Get, resolveLiveMROwner, MintAdoptedUpgradeTask, or
// drop's own Delete) went completely unmetered until Task 8's review found it:
// AdoptionEventDroppedTotal counts REFUSALS ("a non-zero rate is the mechanism
// WORKING"), not errors, and the retired adopt_upgrade_mr reason - sweep.go's
// own direct-mint failure, deleted along with the direct mint itself - was the
// last thing that looked like coverage for this path. Fails both ways: the
// constant renamed/removed with queueSeedReasons left stale, or
// queueSeedReasons carrying a reason the constant no longer names.
func TestQueueAdmitErrorReasonIsSeeded(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("..", "controller", "queue_controller.go"))
	if err != nil {
		t.Fatalf("read queue_controller.go: %v", err)
	}
	m := regexp.MustCompile(`adoptAdmitErrorReason\s*=\s*"([a-z_]+)"`).FindStringSubmatch(string(src))
	if m == nil {
		t.Fatal("found no `adoptAdmitErrorReason = \"...\"` declaration in queue_controller.go - " +
			"the scan is broken, not the seed list")
	}
	declared := m[1]
	seeded := map[string]bool{}
	for _, r := range queueSeedReasons {
		seeded[r] = true
	}
	if !seeded[declared] {
		t.Errorf("queue_controller.go declares adoptAdmitErrorReason = %q but queueSeedReasons "+
			"does not seed it: increase() cannot see its first increment", declared)
	}
	for _, r := range queueSeedReasons {
		if r != declared {
			t.Errorf("queueSeedReasons seeds %q but adoptAdmitErrorReason names %q: "+
				"a permanently dead zero series", r, declared)
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

// TestAdoptionDropReasonsMatchTheirProducers is TestSweepSkipReasonsMatchSweepConstants
// for the adoption half. Every drop reason is an inline literal at its call site
// rather than a named constant, so nothing but this scan ties them to the seeded
// set: an unseeded reason has NO series until its first drop and
// increase(...[1h]) is blind to it, and a seeded reason no site produces is a
// permanently dead zero series. Fails both ways.
//
// THREE PRODUCERS, NOT ONE. The dispatcher drops a QUEUED adoption via
// drop(...); the webhook refuses one BEFORE the enqueue, on the same counter,
// when the repository URL yields no forge slug; and the webhook's freshness
// handlers (handleMRSynchronize/handleMRClosed, mirror_refresh.go) drop one
// whose merge request merged or closed while it waited. Scanning only the
// dispatcher would have called every one of those other reasons a dead series.
//
// mirror_refresh.go's producer is dropQueuedAdoption(ctx, ..., reason string),
// and its two call sites in handleMRClosed pass "merged"/"closed" as LITERALS
// at the call, deliberately - not the shared `state` local handleMRClosed also
// computes for the mirror write, which this scan would have no structural link
// to (a future unrelated `state := "..."` anywhere else in that file would be
// misread as a new adoption-drop reason, and a future
// dropQueuedAdoption(ctx, ..., "declined") passed some OTHER way would go
// unseen). Scanning the call site itself, the same shape as the dispatcher's
// drop("...") above, keeps the tie structural instead of a name coincidence.
func TestAdoptionDropReasonsMatchTheirProducers(t *testing.T) {
	producers := []struct {
		file string
		path []string
		re   *regexp.Regexp
	}{
		{"queue_controller.go", []string{"..", "controller", "queue_controller.go"},
			regexp.MustCompile(`drop\("([a-z_]+)"\)`)},
		{"webhook/server.go", []string{"..", "webhook", "server.go"},
			regexp.MustCompile(`AdoptionEventDroppedTotal\.WithLabelValues\([^)]*"([a-z_]+)"\)`)},
		{"webhook/mirror_refresh.go", []string{"..", "webhook", "mirror_refresh.go"},
			regexp.MustCompile(`dropQueuedAdoption\([^)]*"([a-z_]+)"\)`)},
	}
	seeded := map[string]bool{}
	for _, r := range adoptionDropReasons {
		seeded[r] = true
	}
	produced := map[string]bool{}
	for _, p := range producers {
		src, err := os.ReadFile(filepath.Join(p.path...))
		if err != nil {
			t.Fatalf("read %s: %v", p.file, err)
		}
		found := p.re.FindAllStringSubmatch(string(src), -1)
		if len(found) == 0 {
			t.Fatalf("found no adoption-drop call sites in %s - the scan is broken, not the seed list", p.file)
		}
		for _, m := range found {
			produced[m[1]] = true
		}
	}
	for reason := range produced {
		if !seeded[reason] {
			t.Errorf("an adoption is dropped with reason %q but adoptionDropReasons does not seed it: "+
				"increase() cannot see its first increment", reason)
		}
	}
	for reason := range seeded {
		if !produced[reason] {
			t.Errorf("adoptionDropReasons seeds %q but no producer emits it: "+
				"a permanently dead zero series", reason)
		}
	}
}

// TestQueueActivityMatchesTheControllerConstant is
// TestWebhookActivityMatchesTheControllerConstant for the dispatcher's own
// activity label, and exists for the same reason: the literal is duplicated to
// avoid a reverse import, and a duplicated literal nothing checks is how a
// seeded series silently stops matching its producer.
func TestQueueActivityMatchesTheControllerConstant(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("..", "controller", "sweep.go"))
	if err != nil {
		t.Fatalf("read sweep.go: %v", err)
	}
	m := regexp.MustCompile(`QueueActivity\s*=\s*"([a-z_]+)"`).FindStringSubmatch(string(src))
	if m == nil {
		t.Fatal("found no QueueActivity constant in sweep.go - the scan is broken, not the constant")
	}
	if m[1] != QueueActivity {
		t.Fatalf("controller.QueueActivity = %q but obs.QueueActivity = %q: the seeded series "+
			"and the producer's label have drifted apart", m[1], QueueActivity)
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

// THE ADOPTION COUNTERS MUST BE SEEDABLE WITHOUT THE LEADER-ONLY PATH.
// SeedSweepErrorsForProject runs from ProjectReconciler, which is
// leader-elected, but AdoptionEnqueuedTotal{activity="webhook"} and
// AdoptionEventDroppedTotal{reason="merged"|"closed"} are incremented by the
// webhook on ALL replicas and merged/closed have no non-webhook producer at all.
// A child born at value 1 is invisible to increase(...[1h]), so the non-leader
// replicas need their own seeder (cmd/manager's adoptionSeedRunnable) and that
// seeder needs this function to exist separately.
func TestSeedAdoptionForProject(t *testing.T) {
	wantEnq := len(adoptionActivities)
	wantDrop := len(adoptionDropReasons)

	beforeEnq := testutil.CollectAndCount(AdoptionEnqueuedTotal)
	beforeDrop := testutil.CollectAndCount(AdoptionEventDroppedTotal)
	SeedAdoptionForProject("adopt-seed-proj")
	if got := testutil.CollectAndCount(AdoptionEnqueuedTotal) - beforeEnq; got != wantEnq {
		t.Fatalf("seeding added %d enqueued series, want %d", got, wantEnq)
	}
	if got := testutil.CollectAndCount(AdoptionEventDroppedTotal) - beforeDrop; got != wantDrop {
		t.Fatalf("seeding added %d dropped series, want %d", got, wantDrop)
	}

	afterEnq := testutil.CollectAndCount(AdoptionEnqueuedTotal)
	SeedAdoptionForProject("adopt-seed-proj")
	if again := testutil.CollectAndCount(AdoptionEnqueuedTotal); again != afterEnq {
		t.Fatalf("re-seeding added %d series, want 0 (the webhook start hook and the Project reconcile both call it)", again-afterEnq)
	}

	// Every seeded child must be a genuine ZERO baseline, not a value.
	if v := testutil.ToFloat64(AdoptionEventDroppedTotal.WithLabelValues("adopt-seed-proj", "merged")); v != 0 {
		t.Fatalf("seeded merged series = %v, want 0", v)
	}
}

// SeedSweepErrorsForProject must keep seeding the adoption pair too: the leader
// covers a Project enrolled AFTER the webhook servers started, which is the one
// case the start-time seeder cannot see.
func TestSeedSweepErrorsForProjectStillSeedsAdoption(t *testing.T) {
	SeedSweepErrorsForProject("adopt-via-sweep-proj")
	if v := testutil.ToFloat64(AdoptionEnqueuedTotal.WithLabelValues("adopt-via-sweep-proj", WebhookActivity)); v != 0 {
		t.Fatalf("webhook-activity series = %v, want a seeded 0", v)
	}
}
