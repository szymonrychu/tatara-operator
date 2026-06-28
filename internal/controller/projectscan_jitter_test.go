package controller

import (
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestScanOffsetDeterministicAndBounded guards the core of issue #181: the
// per-(project, repo, activity) offset is stable across calls and always lands
// in [0, period).
func TestScanOffsetDeterministicAndBounded(t *testing.T) {
	const period = time.Hour
	for _, repo := range []string{"containers", "charts", "helmfile", "terraform", "ansible"} {
		a := scanOffset("infrastructure", repo, "mrScan", period)
		b := scanOffset("infrastructure", repo, "mrScan", period)
		if a != b {
			t.Fatalf("scanOffset not deterministic for %s: %v != %v", repo, a, b)
		}
		if a < 0 || a >= period {
			t.Fatalf("scanOffset out of range for %s: %v not in [0, %v)", repo, a, period)
		}
	}
	// Distinct (repo, activity) inputs vary the offset (de-synchronization). With
	// five real infra repos a 32-bit hash mod 3600s is overwhelmingly collision
	// free; require at least two distinct slots to prove spreading.
	slots := map[time.Duration]bool{}
	for _, repo := range []string{"containers", "charts", "helmfile", "terraform", "ansible"} {
		slots[scanOffset("infrastructure", repo, "mrScan", period)] = true
	}
	if len(slots) < 2 {
		t.Fatalf("expected spread offsets across repos, got %d distinct", len(slots))
	}
	// Zero/negative period is a safe no-op (no spread).
	if got := scanOffset("p", "r", "mrScan", 0); got != 0 {
		t.Fatalf("scanOffset(period=0) = %v, want 0", got)
	}
}

func TestCronPeriodHourly(t *testing.T) {
	sched, err := cron.ParseStandard("0 * * * *")
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 6, 27, 10, 17, 0, 0, time.UTC)
	if got := cronPeriod(sched, base); got != time.Hour {
		t.Fatalf("cronPeriod = %v, want 1h", got)
	}
}

func TestRepoNextFireShifted(t *testing.T) {
	sched, err := cron.ParseStandard("0 * * * *")
	if err != nil {
		t.Fatal(err)
	}
	// With a 15m offset the fire after 10:00 lands at 10:15, and the fire after
	// 10:15 lands at the next period's slot 11:15.
	base := time.Date(2026, 6, 27, 10, 0, 0, 0, time.UTC)
	if got := repoNextFire(sched, 15*time.Minute, base); !got.Equal(base.Add(15 * time.Minute)) {
		t.Fatalf("repoNextFire after 10:00 = %v, want 10:15", got)
	}
	at1015 := base.Add(15 * time.Minute)
	if got := repoNextFire(sched, 15*time.Minute, at1015); !got.Equal(base.Add(time.Hour + 15*time.Minute)) {
		t.Fatalf("repoNextFire after 10:15 = %v, want 11:15", got)
	}
}

// jitterProject builds a Project with an hourly issueScan and the given repos.
func jitterProject(creation time.Time, repoNames ...string) (*tatarav1alpha1.Project, []tatarav1alpha1.Repository) {
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "infrastructure",
			CreationTimestamp: metav1.Time{Time: creation},
		},
		Spec: tatarav1alpha1.ProjectSpec{
			Scm: &tatarav1alpha1.ScmSpec{
				Cron: &tatarav1alpha1.ScmCron{
					IssueScan: tatarav1alpha1.CronActivity{Schedule: "0 * * * *"},
				},
			},
		},
	}
	var repos []tatarav1alpha1.Repository
	for _, n := range repoNames {
		repos = append(repos, tatarav1alpha1.Repository{ObjectMeta: metav1.ObjectMeta{Name: n}})
	}
	return proj, repos
}

// simulateFires walks the reconcile loop deterministically: at each wake it
// fires the due repos, advances the shared project stamp, and requeues to the
// soonest per-repo fire. It returns each repo's fire times. This mirrors how
// runScans drives reposDueForScan + stampScan.
func simulateFires(proj *tatarav1alpha1.Project, repos []tatarav1alpha1.Repository, start, end time.Time) map[string][]time.Time {
	r := &ProjectReconciler{}
	fires := map[string][]time.Time{}
	now := start
	for iter := 0; iter < 10000 && now.Before(end); iter++ {
		due, soonest, ok := r.reposDueForScan(proj, "issueScan", repos, now)
		if !ok {
			panic("reposDueForScan not ok")
		}
		if len(due) > 0 {
			for _, repo := range due {
				fires[repo.Name] = append(fires[repo.Name], now)
			}
			proj.Status.LastIssueScan = &metav1.Time{Time: now}
			_, soonest, _ = r.reposDueForScan(proj, "issueScan", repos, now)
		}
		if !soonest.After(now) {
			panic("soonest did not advance")
		}
		now = soonest
	}
	return fires
}

// TestReposDueForScanSpreadsAndFiresOnce is the end-to-end guard for issue #181:
// over a steady-state hour every repo fires exactly once, and the fires are
// spread across the interval rather than synchronized at the top of the hour.
func TestReposDueForScanSpreadsAndFiresOnce(t *testing.T) {
	base := time.Date(2026, 6, 27, 10, 0, 0, 0, time.UTC)
	names := []string{"containers", "charts", "helmfile", "terraform", "ansible"}
	proj, repos := jitterProject(base, names...)

	fires := simulateFires(proj, repos, base, base.Add(3*time.Hour))

	// Inspect a steady-state hour (warmup excluded): [11:00, 12:00).
	winStart, winEnd := base.Add(time.Hour), base.Add(2*time.Hour)
	distinct := map[time.Time]bool{}
	for _, n := range names {
		count := 0
		for _, ts := range fires[n] {
			if !ts.Before(winStart) && ts.Before(winEnd) {
				count++
				distinct[ts] = true
			}
		}
		if count != 1 {
			t.Fatalf("repo %s fired %d times in steady-state hour, want exactly 1 (times=%v)", n, count, fires[n])
		}
	}
	// De-synchronization: the five repos must not all land on the same instant
	// (that synchronized fan-out is the bug). Expect distinct fire times.
	if len(distinct) < 2 {
		t.Fatalf("fires synchronized: only %d distinct fire time(s) in the hour", len(distinct))
	}
}

// TestReposDueForScanDeterministic guards that the spread is reproducible across
// runs (no randomness, no wall-clock dependence) so restarts/pods agree.
func TestReposDueForScanDeterministic(t *testing.T) {
	base := time.Date(2026, 6, 27, 10, 0, 0, 0, time.UTC)
	names := []string{"containers", "charts", "helmfile", "terraform", "ansible"}

	proj1, repos1 := jitterProject(base, names...)
	proj2, repos2 := jitterProject(base, names...)
	a := simulateFires(proj1, repos1, base, base.Add(3*time.Hour))
	b := simulateFires(proj2, repos2, base, base.Add(3*time.Hour))

	for _, n := range names {
		if len(a[n]) != len(b[n]) {
			t.Fatalf("repo %s fire count differs: %d vs %d", n, len(a[n]), len(b[n]))
		}
		for i := range a[n] {
			if !a[n][i].Equal(b[n][i]) {
				t.Fatalf("repo %s fire %d differs: %v vs %v", n, i, a[n][i], b[n][i])
			}
		}
	}
}

// TestReposDueForScanEmptyReposRequeues guards the no-repos fallback: an empty
// project must requeue to the next period instead of returning a zero time
// (which would busy-loop the reconciler).
func TestReposDueForScanEmptyRepos(t *testing.T) {
	base := time.Date(2026, 6, 27, 10, 0, 0, 0, time.UTC)
	proj, _ := jitterProject(base)
	r := &ProjectReconciler{}
	now := base.Add(30 * time.Minute)
	due, soonest, ok := r.reposDueForScan(proj, "issueScan", nil, now)
	if !ok {
		t.Fatal("ok = false, want true for valid schedule")
	}
	if len(due) != 0 {
		t.Fatalf("due = %d, want 0 for no repos", len(due))
	}
	if !soonest.Equal(base.Add(time.Hour)) {
		t.Fatalf("soonest = %v, want next top-of-hour 11:00", soonest)
	}
}

// TestReposDueForScanDisabledOrBadCron mirrors activityDue's contract: empty
// schedule or malformed cron returns ok=false so the activity is skipped.
func TestReposDueForScanDisabledOrBadCron(t *testing.T) {
	base := time.Date(2026, 6, 27, 10, 0, 0, 0, time.UTC)
	r := &ProjectReconciler{}
	repos := []tatarav1alpha1.Repository{{ObjectMeta: metav1.ObjectMeta{Name: "r0"}}}

	disabled, _ := jitterProject(base, "r0")
	disabled.Spec.Scm.Cron.IssueScan.Schedule = ""
	if _, _, ok := r.reposDueForScan(disabled, "issueScan", repos, base); ok {
		t.Fatal("empty schedule: ok = true, want false")
	}

	bad, _ := jitterProject(base, "r0")
	bad.Spec.Scm.Cron.IssueScan.Schedule = "not a cron"
	if _, _, ok := r.reposDueForScan(bad, "issueScan", repos, base); ok {
		t.Fatal("bad cron: ok = true, want false")
	}
}
