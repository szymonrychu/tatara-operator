package controller

// THE LANE-FREE FAST PATH.
//
// eab78709 gave adoption a webhook fast path: an adoptable merge-request
// DELIVERY stamps SweepRequestedAnnotation on the Repository and the leader's
// sweep arm mints under the cap. That closed the arrival half of the latency
// and left the RELEASE half wide open.
//
// The production shape (2026-08-16, project `infrastructure`, repo `charts`):
// the dependency engine opened FIVE merge requests in one run, maxOpenUpgrades
// is 2, so the pass adopted two and deferred three upgrade_headroom_bound. The
// two adopted Tasks reached `done` seconds later - the lanes were free - and the
// three deferred merge requests went on waiting for the next 0 */4 * * *
// issueScan tick, because NOTHING pulls a sweep forward when a lane FREES. One
// of them (1023) had to be adopted by forcing a sweep by hand.
//
// This is the COMMON case, not a corner: with a cap of 2 and an engine run of
// 5-6, most of every burst is deferred, so most adoptions eat up to four hours
// of delay after the capacity to serve them already exists.
//
// The mechanism under test is the SAME marker idiom the webhook uses, and it is
// deliberately not a second minter: the Task reconcile that observes the freed
// lane stamps SweepRequestedAnnotation and stops. The leader's sweep arm remains
// the only place a Task is minted and the only place maxOpenUpgrades is spent,
// which is what keeps the cap inside one serialized writer.

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// laneTaskReconciler is the Task reconciler the lane-release hook runs on: the
// leader-elected one, driven through its own reconcileStage rather than through
// a hand-called helper, so these tests fail if the hook is written but not
// wired.
func laneTaskReconciler(c client.Client) *TaskReconciler {
	return &TaskReconciler{
		Client:  c,
		Scheme:  c.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
	}
}

// finishLane drives ONE adopted upgrade Task to `done` and reconciles it, which
// is exactly what production does when an adopted bump merges: the lane is now
// free and the project can adopt one more.
func finishLane(t *testing.T, c client.Client, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, number int, now time.Time) {

	t.Helper()
	ctx := context.Background()
	var task tatarav1alpha1.Task
	key := client.ObjectKey{Namespace: proj.Namespace, Name: AdoptedUpgradeTaskName(proj.Name, repo.Name, number)}
	if err := c.Get(ctx, key, &task); err != nil {
		t.Fatalf("get adopted task %d: %v", number, err)
	}
	task.Status.State = tatarav1alpha1.StateDone
	if err := c.Status().Update(ctx, &task); err != nil {
		t.Fatalf("finish adopted task %d: %v", number, err)
	}
	if _, err := laneTaskReconciler(c).reconcileStage(ctx, proj, &task, now); err != nil {
		t.Fatalf("reconcile finished task %d: %v", number, err)
	}
}

// prRefs is one engine burst, in the order the forge listed it.
func prRefs(repo *tatarav1alpha1.Repository, numbers ...int) []scm.PRRef {
	out := make([]scm.PRRef, 0, len(numbers))
	for _, n := range numbers {
		out = append(out, enginePR(repo, n))
	}
	return out
}

func laneRepo(t *testing.T, c client.Client, repo *tatarav1alpha1.Repository) *tatarav1alpha1.Repository {
	t.Helper()
	var out tatarav1alpha1.Repository
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(repo), &out); err != nil {
		t.Fatalf("get repository %s: %v", repo.Name, err)
	}
	return &out
}

func adoptedNumbers(t *testing.T, c client.Client, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, numbers ...int) map[int]bool {

	t.Helper()
	out := map[int]bool{}
	for _, n := range numbers {
		key := client.ObjectKey{Namespace: proj.Namespace, Name: AdoptedUpgradeTaskName(proj.Name, repo.Name, n)}
		if err := c.Get(context.Background(), key, &tatarav1alpha1.Task{}); err == nil {
			out[n] = true
		}
	}
	return out
}

// THE DEFECT, AS PRODUCTION SHOWED IT. Five merge requests, a cap of two, three
// deferred - and then a lane frees and nothing happens. The assertion is the
// pulled-forward predicate itself (sweepRequested), because that is the exact
// test reposDueForScan applies: a repo whose phase-shifted slot has not arrived
// is swept NOW only if something asked for it.
func TestUpgradeLaneRelease_AFreedLanePullsTheDeferredRepoForward(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 8, 16, 0, 0, 0, 0, time.UTC)
	proj := headroomProject("lane-free-proj", 2)
	proj.Status.LastIssueScan = &metav1.Time{Time: base}
	repo := sweepRepo("lane-free-proj")
	c := newMirrorClient(t, proj, repo)

	// The engine's run: five merge requests in one burst, newest first as the
	// forge lists them.
	burst := []int{1023, 1022, 1021, 1020, 1019}
	rd := &sweepReader{}
	for _, n := range burst {
		rd.prs = append(rd.prs, enginePR(repo, n))
	}
	runSweep(t, c, proj, repo, rd)

	adopted := adoptedNumbers(t, c, proj, repo, burst...)
	if len(adopted) != 2 {
		t.Fatalf("first pass adopted %v, want exactly 2 (maxOpenUpgrades)", adopted)
	}
	if !adopted[1019] || !adopted[1020] {
		t.Fatalf("first pass adopted %v, want the two OLDEST (1019, 1020)", adopted)
	}

	// A LANE FREES. The oldest adopted bump merged and its Task went done.
	freed := base.Add(7 * time.Minute)
	finishLane(t, c, proj, repo, 1019, freed)

	if !sweepRequested(laneRepo(t, c, repo), "issueScan", base) {
		t.Fatalf("a lane freed at %s with three merge requests deferred "+
			"upgrade_headroom_bound and NOTHING pulled the sweep forward: %s carries no "+
			"sweep request, so reposDueForScan leaves it on its phase-shifted 0 */4 * * * "+
			"slot and merge request 1021 waits up to four hours for capacity that already exists",
			freed, repo.Name)
	}

	// AND THE PAYOFF: the pass that request buys actually adopts the next one,
	// oldest-first, and still exactly one - the cap has not moved.
	runSweep(t, c, proj, repo, rd)
	adopted = adoptedNumbers(t, c, proj, repo, burst...)
	if !adopted[1021] {
		t.Fatalf("the pulled-forward pass did not adopt 1021 into the freed lane; adopted=%v", adopted)
	}
	if len(adopted) != 3 {
		t.Fatalf("adopted=%v; want exactly 3 Tasks (1019 done + two live lanes)", adopted)
	}
	live, err := (&ProjectReconciler{Client: c}).openUpgradeLaneCount(ctx, proj)
	if err != nil {
		t.Fatalf("count lanes: %v", err)
	}
	if live > 2 {
		t.Fatalf("live upgrade lanes = %d, past a maxOpenUpgrades of 2", live)
	}
}

// A PASS RECORDS WHAT IT DEFERRED, and that record is what makes the fan-out
// PRECISE. A freed lane is project-wide, but marking every enrolled repository
// would buy a full forge listing of the whole project for every finished Task;
// marking only the finished Task's own repository would miss the deferral that
// is sitting in a sibling repo. The pass that actually deferred is the one place
// that knows, so it writes it down.
func TestSweepPRs_RecordsAndClearsTheHeadroomDeferral(t *testing.T) {
	proj := headroomProject("lane-record-proj", 1)
	repo := sweepRepo("lane-record-proj")
	c := newMirrorClient(t, proj, repo)

	rd := &sweepReader{prs: prRefs(repo, 62, 61)}
	runSweep(t, c, proj, repo, rd)

	if laneRepo(t, c, repo).Annotations[tatarav1alpha1.UpgradeDeferredAnnotation] == "" {
		t.Fatal("the pass deferred merge request 62 upgrade_headroom_bound and recorded nothing; " +
			"a freed lane has no way to know WHICH repository is waiting")
	}

	// The deferral is CLEARED by the first pass that defers nothing. Without
	// that, a stale record would keep marking a repo with nothing left to adopt.
	finishLane(t, c, proj, repo, 61, time.Now())
	runSweep(t, c, proj, repo, rd)
	if got := laneRepo(t, c, repo).Annotations[tatarav1alpha1.UpgradeDeferredAnnotation]; got != "" {
		t.Fatalf("a pass that deferred nothing left the deferral record in place (%q)", got)
	}
}

// THE TRIGGER CANNOT RE-ARM ITSELF. A finished Task marking repos, causing a
// sweep, which mints a Task, which finishes, which marks again is a 30-second
// forge-listing loop. Two things stop it, and this pins both:
//
//  1. the mark is LATCHED per Task, so re-reconciling a Task that is already
//     terminal - which happens on every resync and on every owned-object event -
//     never stamps a second time;
//  2. the mark is gated on a DEFERRAL RECORD, which only a pass that actually
//     ran out of headroom writes and any pass that does not clears. Once the
//     backlog is drained the next freed lane marks nothing and writes nothing,
//     so the drain terminates one pass after the last adoption rather than on a
//     timer.
func TestUpgradeLaneRelease_DoesNotReArmWhenNothingIsLeftToAdopt(t *testing.T) {
	base := time.Date(2026, 8, 16, 0, 0, 0, 0, time.UTC)
	proj := headroomProject("lane-drain-proj", 2)
	proj.Status.LastIssueScan = &metav1.Time{Time: base}
	repo := sweepRepo("lane-drain-proj")
	c := newMirrorClient(t, proj, repo)

	rd := &sweepReader{prs: prRefs(repo, 12, 11, 10)}
	runSweep(t, c, proj, repo, rd)

	// Lane 1 frees: 12 is still deferred, so the repo is marked.
	t1 := base.Add(time.Minute)
	finishLane(t, c, proj, repo, 10, t1)
	first := laneRepo(t, c, repo).Annotations[tatarav1alpha1.SweepRequestedAnnotation]
	if first == "" {
		t.Fatal("the first freed lane must pull the sweep forward")
	}

	// The pulled-forward pass drains the backlog: 12 is adopted and NOTHING is
	// deferred, so the record is cleared.
	runSweep(t, c, proj, repo, rd)
	if !adoptedNumbers(t, c, proj, repo, 12)[12] {
		t.Fatal("the pulled-forward pass did not adopt 12")
	}

	// Lane 2 frees with an empty forge backlog. Nothing may be stamped: this is
	// the step that would otherwise loop.
	finishLane(t, c, proj, repo, 11, base.Add(2*time.Minute))
	if got := laneRepo(t, c, repo).Annotations[tatarav1alpha1.SweepRequestedAnnotation]; got != first {
		t.Fatalf("a freed lane with nothing left to adopt re-armed the sweep: %q -> %q", first, got)
	}

	// And the LATCH: re-reconciling the already-terminal Task from step 1 - which
	// every resync does - stamps nothing either, even if a later burst re-marks
	// the repository.
	if err := markAnnotation(context.Background(), c, laneRepo(t, c, repo),
		tatarav1alpha1.UpgradeDeferredAnnotation, base.Format(time.RFC3339), true); err != nil {
		t.Fatalf("re-arm the deferral record: %v", err)
	}
	var done tatarav1alpha1.Task
	key := client.ObjectKey{Namespace: proj.Namespace, Name: AdoptedUpgradeTaskName(proj.Name, repo.Name, 10)}
	if err := c.Get(context.Background(), key, &done); err != nil {
		t.Fatalf("get finished task: %v", err)
	}
	if _, err := laneTaskReconciler(c).reconcileStage(context.Background(), proj, &done, base.Add(time.Hour)); err != nil {
		t.Fatalf("re-reconcile finished task: %v", err)
	}
	if got := laneRepo(t, c, repo).Annotations[tatarav1alpha1.SweepRequestedAnnotation]; got != first {
		t.Fatalf("re-reconciling an already-released Task stamped again: %q -> %q", first, got)
	}
}

// INERTNESS. adoptBranchPrefix is empty on every project but `infrastructure`,
// and with it empty NOTHING here may do anything: no merge request classifies
// adoptable, so no pass ever defers, so no repository ever carries a deferral
// record, so a terminal upgrade Task - the cron kind still mints them - marks
// nothing and writes nothing at all, not even its own latch.
func TestUpgradeLaneRelease_IsInertWithNoAdoptBranchPrefix(t *testing.T) {
	ctx := context.Background()
	proj := headroomProject("lane-inert-proj", 2)
	proj.Spec.UpgradePolicy.AdoptBranchPrefix = "" // the shipped default
	repo := sweepRepo("lane-inert-proj")
	c := newMirrorClient(t, proj, repo)

	runSweep(t, c, proj, repo, &sweepReader{prs: prRefs(repo, 82, 81, 80)})

	before := laneRepo(t, c, repo)
	if len(before.Annotations) != 0 {
		t.Fatalf("an inert project annotated its repository: %v", before.Annotations)
	}

	// A cron-minted upgrade Task, which an inert project still has, reaching a
	// terminal state.
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "upgrade-cron-1", Namespace: proj.Namespace},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: proj.Name, RepositoryRef: repo.Name, Kind: "upgrade",
			Goal: "bump the pins", MergeOrder: []string{repo.Name},
		},
	}
	if err := c.Create(ctx, task); err != nil {
		t.Fatalf("create cron upgrade task: %v", err)
	}
	task.Status.State = tatarav1alpha1.StateDone
	if err := c.Status().Update(ctx, task); err != nil {
		t.Fatalf("finish cron upgrade task: %v", err)
	}
	if _, err := laneTaskReconciler(c).reconcileStage(ctx, proj, task, time.Now()); err != nil {
		t.Fatalf("reconcile cron upgrade task: %v", err)
	}

	after := laneRepo(t, c, repo)
	if len(after.Annotations) != 0 {
		t.Fatalf("an inert project's freed lane annotated its repository: %v", after.Annotations)
	}
	if after.ResourceVersion != before.ResourceVersion {
		t.Fatalf("an inert project's freed lane WROTE to its repository: rv %s -> %s",
			before.ResourceVersion, after.ResourceVersion)
	}
	var fresh tatarav1alpha1.Task
	if err := c.Get(ctx, client.ObjectKeyFromObject(task), &fresh); err != nil {
		t.Fatalf("get cron upgrade task: %v", err)
	}
	if v := fresh.Annotations[tatarav1alpha1.AnnUpgradeLaneReleased]; v != "" {
		t.Fatalf("an inert project latched a release it never made: %q", v)
	}
}
