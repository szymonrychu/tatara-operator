package controller

import (
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestScanOffsetSpansFullPeriod pins the production regression directly:
// scanOffset's modulo was against a NANOSECOND period against a 32-bit hash,
// so every offset collapsed into [0, 4.3s) regardless of the configured
// period. For infrastructure's real "0 */4 * * *" schedule (period=4h) that
// put helmfile's offset at 3.730s - inside the few seconds an issueScan pass
// takes to run, so helmfile lost the race with reconcile jitter on every pass.
func TestScanOffsetSpansFullPeriod(t *testing.T) {
	const period = 4 * time.Hour
	off := scanOffset("infrastructure", "helmfile", "issueScan", period)
	if off < 5*time.Second {
		t.Fatalf("scanOffset(infrastructure/helmfile, issueScan, 4h) = %v, want >= 5s "+
			"(a no-op modulo collapsed every offset into [0, 4.3s) regardless of period)", off)
	}

	// A realistic repo set must spread across the period, not cluster near
	// zero: at least one offset must land past the 5s band a real sweep pass
	// runs in.
	names := []string{"containers", "charts", "helmfile", "terraform", "ansible"}
	spread := false
	for _, n := range names {
		off := scanOffset("infrastructure", n, "issueScan", period)
		if off < 0 || off >= period {
			t.Fatalf("scanOffset(%s) = %v, out of [0, %v)", n, off, period)
		}
		if off >= 5*time.Second {
			spread = true
		}
	}
	if !spread {
		t.Fatalf("every offset in %v collapsed under 5s for a %v period; expected a spread", names, period)
	}
}

// TestScanOffsetBoundedAcrossPeriodSizes guards the general [0, period)
// contract (not just the 4h production case) for a small and a large period,
// plus the non-positive-period no-op.
func TestScanOffsetBoundedAcrossPeriodSizes(t *testing.T) {
	for _, period := range []time.Duration{time.Minute, 24 * time.Hour} {
		for _, repo := range []string{"a", "b", "c", "helmfile", "terraform"} {
			off := scanOffset("p", repo, "issueScan", period)
			if off < 0 || off >= period {
				t.Fatalf("scanOffset(%s, %v) = %v, out of [0, %v)", repo, period, off, period)
			}
		}
	}
	if got := scanOffset("p", "r", "issueScan", 0); got != 0 {
		t.Fatalf("scanOffset(period=0) = %v, want 0", got)
	}
	if got := scanOffset("p", "r", "issueScan", -time.Hour); got != 0 {
		t.Fatalf("scanOffset(period<0) = %v, want 0", got)
	}
}

// fairnessProject builds a Project with an hourly issueScan cron, anchored so
// dueBase(proj, proj.Status.LastIssueScan) resolves to `base` when
// Status.LastIssueScan is nil.
func fairnessProject(base time.Time) *tatarav1alpha1.Project {
	return &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "infrastructure",
			CreationTimestamp: metav1.Time{Time: base},
		},
		Spec: tatarav1alpha1.ProjectSpec{
			Scm: &tatarav1alpha1.ScmSpec{
				Cron: &tatarav1alpha1.ScmCron{
					IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *"},
				},
			},
		},
	}
}

// findOffsetPair returns two repo names under the given (project, activity,
// period) whose scanOffset values are distinct, with the first strictly
// smaller than the second - deterministic given the candidate list, so the
// test is reproducible without hand-computing hash outputs.
func findOffsetPair(t *testing.T, project, activity string, period time.Duration) (loName, hiName string, lo, hi time.Duration) {
	t.Helper()
	candidates := []string{"containers", "charts", "helmfile", "terraform", "ansible", "repo-a", "repo-b", "repo-c"}
	for i := 0; i < len(candidates); i++ {
		for j := 0; j < len(candidates); j++ {
			if i == j {
				continue
			}
			a := scanOffset(project, candidates[i], activity, period)
			b := scanOffset(project, candidates[j], activity, period)
			if a < b {
				return candidates[i], candidates[j], a, b
			}
		}
	}
	t.Fatal("no distinct offset pair found among candidates")
	return "", "", 0, 0
}

// TestReposDueForScanStarvationRegression is the per-repo-memory regression
// guard (defect 2): reposDueForScan must anchor each repo on ITS OWN stamp,
// not the project-wide one. Without it, a pass that sweeps the earlier of two
// repos and then advances the shared project stamp PAST the later repo's fire
// pushes that repo's recomputed fire a full cron period forward - the later
// repo is deferred, not merely delayed, even though it was legitimately due.
//
// B carries its own PRIOR-period stamp (simulating steady-state operation
// after the upgrade's one-time catch-up pass, case 5's fallback), so its fire
// this period is fixed at base+offB independent of what happens to the
// project-wide stamp. A pre-per-repo-memory reposDueForScan reads ONLY the
// project-wide base for every repo and must fail this assertion.
func TestReposDueForScanStarvationRegression(t *testing.T) {
	base := time.Date(2026, 6, 27, 10, 0, 0, 0, time.UTC)
	sched, err := cron.ParseStandard("0 * * * *")
	if err != nil {
		t.Fatal(err)
	}
	period := cronPeriod(sched, base)
	loName, hiName, offA, offB := findOffsetPair(t, "infrastructure", "issueScan", period)

	proj := fairnessProject(base) // creation irrelevant; LastIssueScan drives dueBase below
	projStamp := metav1.Time{Time: base}
	proj.Status.LastIssueScan = &projStamp

	repoA := tatarav1alpha1.Repository{ObjectMeta: metav1.ObjectMeta{Name: loName}}
	repoB := tatarav1alpha1.Repository{ObjectMeta: metav1.ObjectMeta{Name: hiName}}
	// B was swept last period: its own stamp anchors this period's fire at
	// base+offB (any value in (base-period, base] does, since repoNextFire's
	// sched.Next(stamp-offB) lands on `base` either way), independent of
	// whatever happens to the project-wide value afterward.
	bPrevStamp := metav1.Time{Time: base.Add(-time.Minute)}
	repoB.Status.LastIssueScan = &bPrevStamp

	repos := []tatarav1alpha1.Repository{repoA, repoB}
	r := &ProjectReconciler{}

	// Reconcile lands strictly between A's and B's fire this period: A is due,
	// B is not yet.
	tCheck := base.Add(offA).Add((offB - offA) / 2)
	due, _, ok := r.reposDueForScan(proj, "issueScan", repos, tCheck)
	if !ok {
		t.Fatal("reposDueForScan not ok")
	}
	if len(due) != 1 || due[0].Name != loName {
		t.Fatalf("at tCheck expected only %s due, got %v", loName, repoNamesOf(due))
	}

	// The pass takes real wall-clock time sweeping A: the project-wide stamp
	// advances to just past B's true fire (base+offB), the exact shape
	// defect 2 exploits.
	advanced := metav1.Time{Time: base.Add(offB).Add(time.Second)}
	proj.Status.LastIssueScan = &advanced

	due2, _, ok2 := r.reposDueForScan(proj, "issueScan", repos, advanced.Time)
	if !ok2 {
		t.Fatal("reposDueForScan not ok (second check)")
	}
	found := false
	for i := range due2 {
		if due2[i].Name == hiName {
			found = true
		}
	}
	if !found {
		t.Fatalf("%s was legitimately due (fire=%v) but the project-wide stamp advancing past it "+
			"deferred it a full period; due=%v", hiName, base.Add(offB), repoNamesOf(due2))
	}
}

// TestReposDueForScanSweptRepoWaitsForItsNextFire is case 4: a repo that WAS
// swept and stamped (on its own per-repo field) must not be due again until
// its next phase-shifted fire, even though the shared project-wide base may
// say otherwise.
func TestReposDueForScanSweptRepoWaitsForItsNextFire(t *testing.T) {
	base := time.Date(2026, 6, 27, 10, 0, 0, 0, time.UTC)
	sched, err := cron.ParseStandard("0 * * * *")
	if err != nil {
		t.Fatal(err)
	}
	period := cronPeriod(sched, base)
	off := scanOffset("infrastructure", "repo-a", "issueScan", period)

	proj := fairnessProject(base)
	repo := tatarav1alpha1.Repository{ObjectMeta: metav1.ObjectMeta{Name: "repo-a"}}
	stampTime := metav1.Time{Time: base.Add(off)}
	repo.Status.LastIssueScan = &stampTime
	repos := []tatarav1alpha1.Repository{repo}
	r := &ProjectReconciler{}

	// Immediately after being stamped: not due again.
	due, _, ok := r.reposDueForScan(proj, "issueScan", repos, base.Add(off).Add(time.Second))
	if !ok {
		t.Fatal("reposDueForScan not ok")
	}
	if len(due) != 0 {
		t.Fatalf("freshly-stamped repo is due again early: %v", repoNamesOf(due))
	}

	// At its next phase-shifted fire (one period later): due.
	nextFire := repoNextFire(sched, off, stampTime.Time)
	due2, _, ok2 := r.reposDueForScan(proj, "issueScan", repos, nextFire)
	if !ok2 {
		t.Fatal("reposDueForScan not ok (second check)")
	}
	if len(due2) != 1 {
		t.Fatalf("repo should be due at its next phase-shifted fire %v, due=%v", nextFire, repoNamesOf(due2))
	}
}

// TestReposDueForScanFallsBackToProjectBase is case 5, the upgrade path: a
// repo carrying no per-repo stamp anchors on the project-wide dueBase exactly
// as reposDueForScan did before per-repo memory existed, so a rollout does
// not silently change behavior for a never-stamped repo, and a starved repo
// under the old project-wide-only scheme sweeps immediately once its
// project-wide-computed fire is in the past.
func TestReposDueForScanFallsBackToProjectBase(t *testing.T) {
	base := time.Date(2026, 6, 27, 10, 0, 0, 0, time.UTC)
	sched, err := cron.ParseStandard("0 * * * *")
	if err != nil {
		t.Fatal(err)
	}
	period := cronPeriod(sched, base)
	off := scanOffset("infrastructure", "repo-a", "issueScan", period)

	proj := fairnessProject(base)
	repo := tatarav1alpha1.Repository{ObjectMeta: metav1.ObjectMeta{Name: "repo-a"}} // Status.LastIssueScan nil
	repos := []tatarav1alpha1.Repository{repo}
	r := &ProjectReconciler{}

	wantFire := repoNextFire(sched, off, base) // project-wide dueBase == base
	due, _, ok := r.reposDueForScan(proj, "issueScan", repos, wantFire.Add(-time.Second))
	if !ok {
		t.Fatal("reposDueForScan not ok")
	}
	if len(due) != 0 {
		t.Fatalf("repo due before its project-base fire: %v", repoNamesOf(due))
	}
	due2, _, ok2 := r.reposDueForScan(proj, "issueScan", repos, wantFire)
	if !ok2 {
		t.Fatal("reposDueForScan not ok (second check)")
	}
	if len(due2) != 1 {
		t.Fatalf("never-stamped repo should fall back to the project base and be due at %v, due=%v", wantFire, repoNamesOf(due2))
	}
}

func repoNamesOf(repos []tatarav1alpha1.Repository) []string {
	out := make([]string, len(repos))
	for i := range repos {
		out[i] = repos[i].Name
	}
	return out
}
