package controller

// The PULLED-FORWARD SWEEP SLOT. A webhook that sees an adoptable dependency
// merge request cannot mint it - the cap is project-wide and the webhook server
// runs on every replica - so it stamps SweepRequestedAnnotation on the
// Repository and lets the leader's own sweep, which is serialized per project
// and already enforces maxOpenUpgrades correctly, do the minting.
//
// THE MARKER IS COMPARED, NEVER CLEARED, and these tests are what pin that. A
// clear-after-serving marker needs a compare-and-delete against a webhook that
// may stamp between the list and the clear; comparing the request instant
// against the SAME dueBase the rest of reposDueForScan already anchors on needs
// no write at all, and stampScan advancing LastIssueScan retires every request
// older than the pass that served it.
//
// EVERY TEST HERE ASSERTS A DELTA, NEVER AN ABSOLUTE DUE SET. Each repo's slot
// is a deterministic hash of (project, repo, activity) spread across the cron
// period (issue #181), so "nothing is due yet" depends on which names the
// fixture happens to use and would pin the hash rather than the behaviour.
// Measuring the due set with and without the annotation isolates exactly the
// thing this mechanism adds.

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// requestSweep stamps the marker at ts, as the webhook does.
func requestSweep(repo *tatarav1alpha1.Repository, ts time.Time) {
	if repo.Annotations == nil {
		repo.Annotations = map[string]string{}
	}
	repo.Annotations[tatarav1alpha1.SweepRequestedAnnotation] = ts.UTC().Format(time.RFC3339)
}

func dueSet(t *testing.T, r *ProjectReconciler, proj *tatarav1alpha1.Project,
	repos []tatarav1alpha1.Repository, now time.Time) map[string]bool {

	t.Helper()
	due, _, ok := r.reposDueForScan(proj, "issueScan", repos, now)
	if !ok {
		t.Fatal("reposDueForScan not ok")
	}
	out := map[string]bool{}
	for i := range due {
		out[due[i].Name] = true
	}
	return out
}

// sweepFixture builds a project whose issueScan matches production's 0 */4 * * *
// and returns it with a repo that is NOT already due at `now`, which is the only
// repo whose behaviour this mechanism can change.
func sweepFixture(t *testing.T, base, now time.Time) (*ProjectReconciler, *tatarav1alpha1.Project,
	[]tatarav1alpha1.Repository, int) {

	t.Helper()
	proj, repos := jitterProject(base, "charts", "helmfile", "operator", "wrapper", "cli", "memory")
	proj.Spec.Scm.Cron.IssueScan.Schedule = "0 */4 * * *"
	proj.Status.LastIssueScan = &metav1.Time{Time: base}
	r := &ProjectReconciler{}
	due := dueSet(t, r, proj, repos, now)
	for i := range repos {
		if !due[repos[i].Name] {
			return r, proj, repos, i
		}
	}
	t.Fatal("fixture is degenerate: every repo is already due, so the marker can prove nothing")
	return nil, nil, nil, 0
}

// A repo whose phase-shifted slot has not arrived is due NOW once a webhook has
// asked for it, and NO other repo is dragged along. Without this the marker is
// inert and the four-hour wait is unchanged.
func TestReposDueForScan_SweepRequestPullsTheSlotForward(t *testing.T) {
	base := time.Date(2026, 8, 16, 10, 0, 0, 0, time.UTC)
	now := base.Add(time.Minute)
	r, proj, repos, idx := sweepFixture(t, base, now)
	before := dueSet(t, r, proj, repos, now)

	requestSweep(&repos[idx], now)
	after := dueSet(t, r, proj, repos, now)

	if !after[repos[idx].Name] {
		t.Fatalf("%s asked for its slot and did not get it", repos[idx].Name)
	}
	if len(after) != len(before)+1 {
		t.Fatalf("the request must add exactly one repo: before=%v after=%v", before, after)
	}
}

// The request is RETIRED by the pass that serves it, without being deleted:
// stampScan advances LastIssueScan past the request instant, and dueBase moves
// with it. A marker that outlived its pass would make the repo due on every
// reconcile forever - a 30s forge-listing loop.
func TestReposDueForScan_ServedRequestDoesNotRefire(t *testing.T) {
	base := time.Date(2026, 8, 16, 10, 0, 0, 0, time.UTC)
	now := base.Add(time.Minute)
	r, proj, repos, idx := sweepFixture(t, base, now)
	name := repos[idx].Name

	requestSweep(&repos[idx], now)
	if !dueSet(t, r, proj, repos, now)[name] {
		t.Fatal("the request must fire once")
	}

	// The pass ran: stampScan advanced the project stamp past the request.
	served := now.Add(time.Second)
	proj.Status.LastIssueScan = &metav1.Time{Time: served}
	if dueSet(t, r, proj, repos, served.Add(time.Second))[name] {
		t.Fatal("a served request must not refire; the marker is retired by the stamp, not by a delete")
	}
}

// A BURST IS ONE REQUEST. Five deliveries write five instants onto ONE key; the
// last one wins and a single pass serves them all. This is why the marker is a
// set and not a counter, and it is what makes the fast path cost one repo
// listing per burst rather than one per merge request.
func TestReposDueForScan_BurstOfRequestsCollapsesToOnePass(t *testing.T) {
	base := time.Date(2026, 8, 16, 10, 0, 0, 0, time.UTC)
	now := base.Add(time.Minute)
	r, proj, repos, idx := sweepFixture(t, base, now)
	name := repos[idx].Name
	before := dueSet(t, r, proj, repos, now)

	for i := 0; i < 5; i++ {
		requestSweep(&repos[idx], now.Add(time.Duration(i)*time.Second))
	}
	after := dueSet(t, r, proj, repos, now.Add(10*time.Second))
	if !after[name] || len(after) != len(before)+1 {
		t.Fatalf("five stamps must yield the same one due repo: before=%v after=%v", before, after)
	}

	proj.Status.LastIssueScan = &metav1.Time{Time: now.Add(11 * time.Second)}
	if dueSet(t, r, proj, repos, now.Add(12*time.Second))[name] {
		t.Fatal("one pass serves the whole burst")
	}
}

// A GARBAGE MARKER CHANGES NOTHING. An unparseable value must not make a repo
// permanently due - that would turn the 30s project reconcile into a 30s
// forge-listing loop, a worse failure than the four-hour wait this mechanism
// removes - and must not panic.
func TestReposDueForScan_UnparseableRequestIsIgnored(t *testing.T) {
	base := time.Date(2026, 8, 16, 10, 0, 0, 0, time.UTC)
	now := base.Add(time.Minute)
	r, proj, repos, idx := sweepFixture(t, base, now)
	before := dueSet(t, r, proj, repos, now)

	for _, bad := range []string{"not-a-timestamp", "", "1755331200"} {
		repos[idx].Annotations = map[string]string{tatarav1alpha1.SweepRequestedAnnotation: bad}
		after := dueSet(t, r, proj, repos, now)
		if len(after) != len(before) || after[repos[idx].Name] {
			t.Fatalf("marker %q must be ignored: before=%v after=%v", bad, before, after)
		}
	}
}

// A STALE REQUEST IS NOT A REQUEST. One stamped BEFORE the last scan was already
// served by that scan; honouring it again would re-sweep on every reconcile for
// as long as the annotation existed.
func TestReposDueForScan_RequestOlderThanTheLastScanIsIgnored(t *testing.T) {
	base := time.Date(2026, 8, 16, 10, 0, 0, 0, time.UTC)
	now := base.Add(time.Minute)
	r, proj, repos, idx := sweepFixture(t, base, now)
	before := dueSet(t, r, proj, repos, now)

	requestSweep(&repos[idx], base.Add(-time.Hour))
	after := dueSet(t, r, proj, repos, now)
	if len(after) != len(before) || after[repos[idx].Name] {
		t.Fatalf("a pre-scan request must be ignored: before=%v after=%v", before, after)
	}
}

// The marker is issueScan's ALONE. brainstorm and documentation have no per-repo
// slot and nothing stamps a request for them; honouring it for every activity
// would let one merge-request webhook drag unrelated crons forward.
func TestSweepRequested_OnlyAppliesToIssueScan(t *testing.T) {
	base := time.Date(2026, 8, 16, 10, 0, 0, 0, time.UTC)
	repo := &tatarav1alpha1.Repository{}
	requestSweep(repo, base.Add(time.Hour))

	if !sweepRequested(repo, "issueScan", base) {
		t.Fatal("issueScan must honour the request")
	}
	for _, activity := range []string{"brainstorm", "documentation", "refine", "upgrade"} {
		if sweepRequested(repo, activity, base) {
			t.Fatalf("%s must ignore the request", activity)
		}
	}
}
