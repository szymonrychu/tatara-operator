package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// issueOwnedBy builds a mirror Issue CR carrying a controller ownerRef to
// taskName - the exact shape the live victims of #521 carry
// (iss-tatara-operator-510/512/520/523/524, each naming a Task that
// `kubectl get` reports NotFound).
func issueOwnedBy(repo string, number int, taskName string) *tatarav1alpha1.Issue {
	ctrl := true
	return &tatarav1alpha1.Issue{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tatarav1alpha1.IssueName(repo, number),
			Namespace: testNS,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: tatarav1alpha1.GroupVersion.String(),
				Kind:       "Task",
				Name:       taskName,
				UID:        types.UID("u-" + taskName),
				Controller: &ctrl,
			}},
		},
	}
}

func liveTask(name string) *tatarav1alpha1.Task {
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS, UID: types.UID("u-" + name)},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "orphan-proj"},
	}
}

// minterOn wraps one fake client in the SAME Minter shape the sweep builds
// (APIReader == Client: these tests need the uncached read to resolve).
func minterOn(c client.Client) *Minter {
	return &Minter{Client: c, APIReader: c, Scheme: c.Scheme()}
}

// TestResolveLiveOwnerNilCR: no mirror yet is not ownership. It goes through
// the TYPED front door on purpose - a typed nil *Issue boxed in a client.Object
// interface is NOT == nil, so answering "no mirror" has to happen before the
// value is boxed or the first method call panics.
func TestResolveLiveOwnerNilCR(t *testing.T) {
	proj := sweepProject("orphan-proj")
	c := newMirrorClient(t)
	var missing *tatarav1alpha1.Issue
	got, err := minterOn(c).resolveLiveIssueOwner(context.Background(), proj, missing, SweepActivity)
	if err != nil {
		t.Fatalf("resolveLiveOwner: %v", err)
	}
	if got != "" {
		t.Fatalf("resolveLiveOwner = %q, want \"\"", got)
	}
}

// TestResolveLiveOwnerNoControllerRef: fix H13 has a failed Task RELEASE its
// controller ownership, and per B.1 a zero-owner object is never collected. A
// plain-owner-only CR has no controller owner and is an orphan.
func TestResolveLiveOwnerNoControllerRef(t *testing.T) {
	proj := sweepProject("orphan-proj")
	iss := issueOwnedBy("tatara-operator", 1, "plain-task")
	iss.OwnerReferences[0].Controller = nil
	c := newMirrorClient(t, iss)

	got, err := minterOn(c).resolveLiveOwner(context.Background(), proj, iss, SweepActivity)
	if err != nil {
		t.Fatalf("resolveLiveOwner: %v", err)
	}
	if got != "" {
		t.Fatalf("resolveLiveOwner = %q, want \"\" (no controller ref)", got)
	}
	if n := len(getIssueCR(t, c, iss.Name).OwnerReferences); n != 1 {
		t.Fatalf("a plain owner ref must NOT be dropped, %d refs remain, want 1", n)
	}
}

// TestResolveLiveOwnerLiveTask: the ordinary owned case. The ref is returned
// and NOTHING is written - stealing an issue from a running Task is the one
// outcome strictly worse than #521.
func TestResolveLiveOwnerLiveTask(t *testing.T) {
	proj := sweepProject("orphan-proj")
	iss := issueOwnedBy("tatara-operator", 1, "owner-task")
	c := newMirrorClient(t, iss, liveTask("owner-task"))

	got, err := minterOn(c).resolveLiveOwner(context.Background(), proj, iss, SweepActivity)
	if err != nil {
		t.Fatalf("resolveLiveOwner: %v", err)
	}
	if got != "owner-task" {
		t.Fatalf("resolveLiveOwner = %q, want \"owner-task\"", got)
	}
	if _, owned := own.ControllerOwner(getIssueCR(t, c, iss.Name)); !owned {
		t.Fatal("a LIVE owner's controller ref must survive untouched")
	}
}

// TestResolveLiveOwnerDeadTaskDropsRef IS issue #521. An Issue CR whose owning
// Task was reaped keeps a dangling controller ownerRef forever; the old
// predicate read owned=true off it and skipped the issue silently on every
// pass, for 19 hours across five issues. The ref must be DROPPED (a write, not
// an in-memory ignore: the same ref misroutes ownerTaskRequests, the reaper
// cascade and ourMR), counted, and the caller told there is no live owner.
func TestResolveLiveOwnerDeadTaskDropsRef(t *testing.T) {
	proj := sweepProject("orphan-proj")
	iss := issueOwnedBy("tatara-operator", 510, "reaped-task")
	c := newMirrorClient(t, iss)

	before := testutil.ToFloat64(obs.SweepStaleOwnerRepairedTotal.WithLabelValues(proj.Name, SweepActivity))
	got, err := minterOn(c).resolveLiveOwner(context.Background(), proj, iss, SweepActivity)
	if err != nil {
		t.Fatalf("resolveLiveOwner: %v", err)
	}
	if got != "" {
		t.Fatalf("resolveLiveOwner = %q, want \"\" (the named Task does not exist)", got)
	}
	stored := getIssueCR(t, c, iss.Name)
	if len(stored.OwnerReferences) != 0 {
		t.Fatalf("the tombstone ref must be DROPPED IN ETCD, %d refs remain", len(stored.OwnerReferences))
	}
	after := testutil.ToFloat64(obs.SweepStaleOwnerRepairedTotal.WithLabelValues(proj.Name, SweepActivity))
	if after-before != 1 {
		t.Fatalf("SweepStaleOwnerRepairedTotal delta = %v, want 1", after-before)
	}
}

// TestResolveLiveOwnerDeadTaskKeepsOtherOwners: the drop is surgical. A plain
// ref to a DIFFERENT, still-live Task holds the GC open (B.1) and must survive.
func TestResolveLiveOwnerDeadTaskKeepsOtherOwners(t *testing.T) {
	proj := sweepProject("orphan-proj")
	iss := issueOwnedBy("tatara-operator", 512, "reaped-task")
	iss.OwnerReferences = append(iss.OwnerReferences, metav1.OwnerReference{
		APIVersion: tatarav1alpha1.GroupVersion.String(), Kind: "Task",
		Name: "other-task", UID: types.UID("u-other-task"),
	})
	c := newMirrorClient(t, iss, liveTask("other-task"))

	if _, err := minterOn(c).resolveLiveOwner(context.Background(), proj, iss, SweepActivity); err != nil {
		t.Fatalf("resolveLiveOwner: %v", err)
	}
	stored := getIssueCR(t, c, iss.Name)
	if len(stored.OwnerReferences) != 1 || stored.OwnerReferences[0].Name != "other-task" {
		t.Fatalf("owner refs after drop = %+v, want exactly the live plain owner", stored.OwnerReferences)
	}
}

// TestResolveLiveOwnerIsIdempotent: the sweep runs hourly forever. A second
// pass over an already-repaired CR must be a no-op, and must NOT re-count -
// a repair counter that ticks on every pass cannot distinguish "five issues
// were repaired once" from "one issue is being repaired forever".
func TestResolveLiveOwnerIsIdempotent(t *testing.T) {
	proj := sweepProject("orphan-proj")
	iss := issueOwnedBy("tatara-operator", 520, "reaped-task")
	c := newMirrorClient(t, iss)
	m := minterOn(c)

	if _, err := m.resolveLiveOwner(context.Background(), proj, iss, SweepActivity); err != nil {
		t.Fatalf("first resolveLiveOwner: %v", err)
	}
	mid := testutil.ToFloat64(obs.SweepStaleOwnerRepairedTotal.WithLabelValues(proj.Name, SweepActivity))
	if _, err := m.resolveLiveOwner(context.Background(), proj, getIssueCR(t, c, iss.Name), SweepActivity); err != nil {
		t.Fatalf("second resolveLiveOwner: %v", err)
	}
	after := testutil.ToFloat64(obs.SweepStaleOwnerRepairedTotal.WithLabelValues(proj.Name, SweepActivity))
	if after != mid {
		t.Fatalf("a second pass re-counted the repair: %v -> %v", mid, after)
	}
}

// TestSweepMintsIssueWhoseOwningTaskWasReaped is the END-TO-END #521
// regression, and the one test that would have caught the outage. An OPEN
// forge issue whose mirror carries a controller ref to a reaped Task must be
// minted BY THE SAME PASS that discovers the ref is a tombstone - not deferred,
// not skipped.
func TestSweepMintsIssueWhoseOwningTaskWasReaped(t *testing.T) {
	proj := sweepProject("orphan-proj")
	repo := sweepRepo("orphan-proj")
	iss := issueOwnedBy(repo.Name, 510, "reaped-task")
	c := newMirrorClient(t, proj, repo, iss)

	rd := &sweepReader{issues: []scm.IssueRef{{
		Number: 510, State: "open", Author: "szymonrychu", Title: "stranded",
		CreatedAt: time.Now().Add(-19 * time.Hour),
	}}}
	runSweep(t, c, proj, repo, rd)

	tasks := sweepTasks(t, c, proj.Name)
	if len(tasks) != 1 {
		t.Fatalf("minted %d tasks, want 1 (the reaped owner's tombstone ref must not block the mint)", len(tasks))
	}
	if len(getIssueCR(t, c, iss.Name).OwnerReferences) == 0 {
		t.Fatal("the mint must leave the fresh Task as controller owner, not zero owners")
	}
	if name, _ := own.ControllerOwner(getIssueCR(t, c, iss.Name)); name == "reaped-task" {
		t.Fatal("the tombstone ref survived the pass")
	}
}

// TestSweepCountsEverySkipReason pins that a skipped issue is COUNTED under the
// clause that refused it. The counter is half the answer (the log line names
// WHICH issue), and it is the half an alert can read: a skip rate that never
// returns to zero is a stuck intake, which is precisely what nobody could see
// for 19 hours on #521.
func TestSweepCountsEverySkipReason(t *testing.T) {
	tests := map[string]struct {
		setup      func(proj *tatarav1alpha1.Project) []client.Object
		issue      scm.IssueRef
		wantReason string
	}{
		"a live owner": {
			setup: func(proj *tatarav1alpha1.Project) []client.Object {
				return []client.Object{issueOwnedBy("tatara-operator", 7, "owner-task"), liveTask("owner-task")}
			},
			issue:      scm.IssueRef{Number: 7, State: "open", Author: "carol"},
			wantReason: SweepSkipIssueOwned,
		},
		"a reporter outside the allowlist": {
			setup: func(proj *tatarav1alpha1.Project) []client.Object {
				proj.Spec.Scm.ReporterLogins = []string{"alice"}
				return nil
			},
			issue:      scm.IssueRef{Number: 8, State: "open", Author: "mallory"},
			wantReason: SweepSkipReporterNotAllowed,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			proj := sweepProject("skip-proj")
			repo := sweepRepo("skip-proj")
			objs := []client.Object{proj, repo}
			objs = append(objs, tc.setup(proj)...)
			c := newMirrorClient(t, objs...)

			before := testutil.ToFloat64(
				obs.SweepSkippedTotal.WithLabelValues(proj.Name, SweepActivity, tc.wantReason))
			runSweep(t, c, proj, repo, &sweepReader{issues: []scm.IssueRef{tc.issue}})
			after := testutil.ToFloat64(
				obs.SweepSkippedTotal.WithLabelValues(proj.Name, SweepActivity, tc.wantReason))

			if after-before != 1 {
				t.Fatalf("SweepSkippedTotal{reason=%q} delta = %v, want 1", tc.wantReason, after-before)
			}
			if n := len(sweepTasks(t, c, proj.Name)); n != 0 {
				t.Fatalf("a skipped issue minted %d tasks, want 0", n)
			}
		})
	}
}

// TestSweepCountsBudgetBoundSkip: maxNewTasksPerSweep=1 with two orphans means
// the second orphan is DEFERRED, and the issue that paid for the cap must be
// named. obs.SweepMintCapHitTotal already says WHICH cap bound, once per pass;
// this says WHICH ISSUES it cost.
func TestSweepCountsBudgetBoundSkip(t *testing.T) {
	proj := sweepProject("budget-proj")
	proj.Spec.MaxNewTasksPerSweep = 1
	repo := sweepRepo("budget-proj")
	c := newMirrorClient(t, proj, repo)

	before := testutil.ToFloat64(
		obs.SweepSkippedTotal.WithLabelValues(proj.Name, SweepActivity, SweepSkipMintBudget))
	runSweep(t, c, proj, repo, &sweepReader{issues: []scm.IssueRef{
		{Number: 11, State: "open", Author: "szymonrychu"},
		{Number: 12, State: "open", Author: "szymonrychu"},
	}})
	after := testutil.ToFloat64(
		obs.SweepSkippedTotal.WithLabelValues(proj.Name, SweepActivity, SweepSkipMintBudget))

	if after-before != 1 {
		t.Fatalf("SweepSkippedTotal{reason=mint_budget_bound} delta = %v, want 1", after-before)
	}
	if n := len(sweepTasks(t, c, proj.Name)); n != 1 {
		t.Fatalf("minted %d tasks under maxNewTasksPerSweep=1, want 1", n)
	}
}

// TestRepoNames pins the `repos` field on the sweep_pass log line.
// SweepProject is called with dueRepos, not with every repo
// (projectscan.go), so "the sweep ran" has never meant "the sweep looked at
// repo X" - reconstructing a pass's repo set from the per-repo cadence and
// wall-clock timestamps is what made #521 a 19-hour diagnosis.
func TestRepoNames(t *testing.T) {
	tests := map[string]struct {
		in   []tatarav1alpha1.Repository
		want []string
	}{
		"empty": {in: nil, want: []string{}},
		"preserves sweep order": {
			in: []tatarav1alpha1.Repository{
				{ObjectMeta: metav1.ObjectMeta{Name: "tatara-operator"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "tatara-cli"}},
			},
			want: []string{"tatara-operator", "tatara-cli"},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := repoNames(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("repoNames = %v, want %v", got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("repoNames = %v, want %v", got, tc.want)
				}
			}
		})
	}
}

// TestMintOutcomeDistinguishesNothingOwedFromDestroyed is the defect this enum
// closes. A bare `created bool` collapsed four states into one bit, which is
// how resume.go logged "re-minted the issue fresh" for a mint that never
// happened. Each state must be individually nameable.
func TestMintOutcomeDistinguishesNothingOwedFromDestroyed(t *testing.T) {
	proj := sweepProject("outcome-proj")
	repo := sweepRepo("outcome-proj")
	ext := scm.Issue{Number: 30, State: "open", Author: "szymonrychu", Title: "t", URL: "https://example.invalid/30"}

	t.Run("a fresh name is MintCreated", func(t *testing.T) {
		c := newMirrorClient(t, proj, repo)
		_, outcome, err := minterOn(c).MintIssueTask(context.Background(), proj, repo, ext,
			tatarav1alpha1.StageTriaging, "", nil)
		if err != nil {
			t.Fatalf("MintIssueTask: %v", err)
		}
		if outcome != MintCreated {
			t.Fatalf("outcome = %q, want %q", outcome, MintCreated)
		}
	})

	t.Run("a LIVE twin is MintExistingLive, not MintCreated", func(t *testing.T) {
		c := newMirrorClient(t, proj, repo)
		m := minterOn(c)
		if _, _, err := m.MintIssueTask(context.Background(), proj, repo, ext,
			tatarav1alpha1.StageTriaging, "", nil); err != nil {
			t.Fatalf("first MintIssueTask: %v", err)
		}
		_, outcome, err := m.MintIssueTask(context.Background(), proj, repo, ext,
			tatarav1alpha1.StageTriaging, "", nil)
		if err != nil {
			t.Fatalf("second MintIssueTask: %v", err)
		}
		if outcome != MintExistingLive {
			t.Fatalf("outcome = %q, want %q", outcome, MintExistingLive)
		}
	})

	t.Run("a TERMINAL twin is MintTombstoneDeleted and the name is freed", func(t *testing.T) {
		name := tatarav1alpha1.IntakeTaskName(proj.Name, SweepIssueKind, repo.Name, ext.Number)
		dead := &tatarav1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
			Spec:       tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Kind: SweepIssueKind},
			Status:     tatarav1alpha1.TaskStatus{Stage: tatarav1alpha1.StageDelivered},
		}
		c := newMirrorClient(t, proj, repo, dead)

		_, outcome, err := minterOn(c).MintIssueTask(context.Background(), proj, repo, ext,
			tatarav1alpha1.StageTriaging, "", nil)
		if err != nil {
			t.Fatalf("MintIssueTask: %v", err)
		}
		if outcome != MintTombstoneDeleted {
			t.Fatalf("outcome = %q, want %q", outcome, MintTombstoneDeleted)
		}
		var gone tatarav1alpha1.Task
		gerr := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, &gone)
		if gerr == nil {
			t.Fatal("the terminal twin was not deleted, so the name is still blocked")
		}
	})

	t.Run("a non-orphan issue is MintNotOwed", func(t *testing.T) {
		c := newMirrorClient(t, proj, repo, issueOwnedBy(repo.Name, 31, "owner-task"), liveTask("owner-task"))
		owned := scm.Issue{Number: 31, State: "open", Author: "szymonrychu"}
		_, outcome, err := minterOn(c).MintForItem(context.Background(), proj, repo,
			ForgeItem{Issue: owned}, false, nil)
		if err != nil {
			t.Fatalf("MintForItem: %v", err)
		}
		if outcome != MintNotOwed {
			t.Fatalf("outcome = %q, want %q", outcome, MintNotOwed)
		}
	})
}

// TestSweepRequeuesOnTombstoneDelete: MintTombstoneDeleted means work is OWED
// and has NOT happened. The pass must ask for a fast requeue rather than
// waiting a full sweep period - the "silent next-pass" is the shape of the
// defect, not an acceptable fallback.
func TestSweepRequeuesOnTombstoneDelete(t *testing.T) {
	proj := sweepProject("requeue-proj")
	repo := sweepRepo("requeue-proj")
	name := tatarav1alpha1.IntakeTaskName(proj.Name, SweepIssueKind, repo.Name, 40)
	dead := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Kind: SweepIssueKind},
		Status:     tatarav1alpha1.TaskStatus{Stage: tatarav1alpha1.StageDelivered},
	}
	c := newMirrorClient(t, proj, repo, dead)

	r := &ProjectReconciler{Client: c, Scheme: c.Scheme(), Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry())}
	rd := &sweepReader{issues: []scm.IssueRef{{Number: 40, State: "open", Author: "szymonrychu"}}}
	requeue, err := r.SweepProject(context.Background(), proj, rd, []tatarav1alpha1.Repository{*repo}, nil, SweepActivity)
	if err != nil {
		t.Fatalf("SweepProject: %v", err)
	}
	if requeue != sweepRemintDelay {
		t.Fatalf("requeueAfter = %v, want %v", requeue, sweepRemintDelay)
	}
}

// TestSweepStrandedGaugeMarksABudgetBoundOrphan is the deadman. The only sweep
// alert today is a liveness heartbeat that reported GREEN while five issues
// rotted for 19 hours - it cannot catch this class, because the sweep WAS
// running correctly and doing nothing. This gauge measures the WORK.
func TestSweepStrandedGaugeMarksABudgetBoundOrphan(t *testing.T) {
	proj := sweepProject("stranded-proj")
	proj.Spec.MaxNewTasksPerSweep = 1
	repo := sweepRepo("stranded-proj")
	obs.ClearSweepOrphanStranded(proj.Name, repo.Name)
	c := newMirrorClient(t, proj, repo)

	old := time.Now().Add(-19 * time.Hour)
	runSweep(t, c, proj, repo, &sweepReader{issues: []scm.IssueRef{
		{Number: 11, State: "open", Author: "szymonrychu", CreatedAt: old},
		{Number: 12, State: "open", Author: "szymonrychu", CreatedAt: old},
	}})

	minted := testutil.ToFloat64(obs.SweepOrphanStrandedSeconds.WithLabelValues(proj.Name, repo.Name, "11"))
	deferred := testutil.ToFloat64(obs.SweepOrphanStrandedSeconds.WithLabelValues(proj.Name, repo.Name, "12"))
	if minted != 0 {
		t.Fatalf("the MINTED issue must carry no stranded series, got %v", minted)
	}
	if deferred < 19*60*60 {
		t.Fatalf("the DEFERRED issue's stranded age = %v, want at least 19h in seconds", deferred)
	}
}

// TestSweepStrandedGaugeClearsWhenTheIssueIsMinted: the gauge must go away the
// pass the issue is served, or it is a permanent false alarm.
func TestSweepStrandedGaugeClearsWhenTheIssueIsMinted(t *testing.T) {
	proj := sweepProject("stranded-clear-proj")
	proj.Spec.MaxNewTasksPerSweep = 1
	repo := sweepRepo("stranded-clear-proj")
	obs.ClearSweepOrphanStranded(proj.Name, repo.Name)
	c := newMirrorClient(t, proj, repo)

	old := time.Now().Add(-19 * time.Hour)
	issues := []scm.IssueRef{
		{Number: 21, State: "open", Author: "szymonrychu", CreatedAt: old},
		{Number: 22, State: "open", Author: "szymonrychu", CreatedAt: old},
	}
	runSweep(t, c, proj, repo, &sweepReader{issues: issues})
	if v := testutil.ToFloat64(obs.SweepOrphanStrandedSeconds.WithLabelValues(proj.Name, repo.Name, "22")); v == 0 {
		t.Fatal("issue 22 should be stranded after the first budget-bound pass")
	}
	runSweep(t, c, proj, repo, &sweepReader{issues: issues}) // second pass, fresh budget
	if v := testutil.ToFloat64(obs.SweepOrphanStrandedSeconds.WithLabelValues(proj.Name, repo.Name, "22")); v != 0 {
		t.Fatalf("issue 22 stranded series survived the pass that minted it: %v", v)
	}
	if n := len(sweepTasks(t, c, proj.Name)); n != 2 {
		t.Fatalf("minted %d tasks over two passes, want 2", n)
	}
}

// mrOwnedBy is issueOwnedBy's MergeRequest twin: a mirror carrying a controller
// ownerRef to taskName. The reaper treats Issue and MergeRequest SYMMETRICALLY
// (releaseOwnership runs the same OldestSurvivingOwner release over both), so
// the dangling-controller-ref defect of #521 exists on this arm byte for byte.
func mrOwnedBy(repo string, number int, taskName string) *tatarav1alpha1.MergeRequest {
	ctrl := true
	return &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tatarav1alpha1.MergeRequestName(repo, number),
			Namespace: testNS,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: tatarav1alpha1.GroupVersion.String(),
				Kind:       "Task",
				Name:       taskName,
				UID:        types.UID("u-" + taskName),
				Controller: &ctrl,
			}},
		},
		Spec: tatarav1alpha1.MergeRequestSpec{
			RepositoryRef: repo, Number: number,
			URL: "https://github.com/szymonrychu/tatara-operator/pull/60",
		},
	}
}

func getMRCR(t *testing.T, c client.Client, name string) *tatarav1alpha1.MergeRequest {
	t.Helper()
	var mr tatarav1alpha1.MergeRequest
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, &mr); err != nil {
		t.Fatalf("get mergerequest %s: %v", name, err)
	}
	return &mr
}

// TestResolveLiveOwnerDropsADeadMROwner is #521 on the MergeRequest arm. The
// resolver is ONE function over client.Object for exactly this reason: a
// parallel copy is how the two arms drift.
func TestResolveLiveOwnerDropsADeadMROwner(t *testing.T) {
	proj := sweepProject("mr-liveness-proj")
	mr := mrOwnedBy("tatara-operator", 60, "reaped-review-task")
	c := newMirrorClient(t, mr)

	before := testutil.ToFloat64(obs.SweepStaleOwnerRepairedTotal.WithLabelValues(proj.Name, SweepActivity))
	got, err := minterOn(c).resolveLiveOwner(context.Background(), proj, mr, SweepActivity)
	if err != nil {
		t.Fatalf("resolveLiveOwner: %v", err)
	}
	if got != "" {
		t.Fatalf("resolveLiveOwner = %q, want \"\" (the named Task does not exist)", got)
	}
	if n := len(getMRCR(t, c, mr.Name).OwnerReferences); n != 0 {
		t.Fatalf("the tombstone ref must be DROPPED IN ETCD, %d refs remain", n)
	}
	if d := testutil.ToFloat64(obs.SweepStaleOwnerRepairedTotal.WithLabelValues(proj.Name, SweepActivity)) - before; d != 1 {
		t.Fatalf("SweepStaleOwnerRepairedTotal delta = %v, want 1", d)
	}
}

// TestSweepMintsReviewForPRWhoseOwningTaskWasReaped is the MR half of the #521
// END-TO-END regression. An OPEN human PR whose MergeRequest mirror carries a
// controller ref to a reaped Task was classified PRIgnore forever: no mint, no
// counter, no log - the same silent state as the Issue arm, on the artifact a
// human is actively waiting on.
func TestSweepMintsReviewForPRWhoseOwningTaskWasReaped(t *testing.T) {
	proj := sweepProject("mr-orphan-proj")
	repo := sweepRepo("mr-orphan-proj")
	mr := mrOwnedBy(repo.Name, 60, "reaped-review-task")
	c := newMirrorClient(t, proj, repo, mr)

	runSweep(t, c, proj, repo, &sweepReader{prs: []scm.PRRef{humanPR(60)}})

	tasks := sweepTasks(t, c, proj.Name)
	if len(tasks) != 1 || tasks[0].Spec.Kind != SweepReviewKind {
		t.Fatalf("tasks = %+v, want exactly one review Task (the reaped owner's tombstone ref must not block the mint)", tasks)
	}
	name, owned := own.ControllerOwner(getMRCR(t, c, mr.Name))
	if !owned || name == "reaped-review-task" {
		t.Fatalf("MR controller owner = %q (owned=%v), want the freshly minted review Task", name, owned)
	}
}

// TestClassifyPRTakesTheLIVEOwner pins the predicate itself, on both clauses
// that read ownership. Clause 1b (PRClaimed) and clause 3 (the orphan check)
// used the identical own.ControllerOwner(cr) call, so a DEAD claimant kept an
// adoptable PR unadoptable and a DEAD reviewer kept a human PR unreviewed.
func TestClassifyPRTakesTheLIVEOwner(t *testing.T) {
	proj := sweepProject("classify-live-proj")
	repo := sweepRepo("classify-live-proj")
	owner := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "classify-live-proj-clarify-owner", Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: proj.Name, Kind: "clarify", Goal: "g"},
	}
	botPR := scm.PRRef{
		Repo: "szymonrychu/tatara-operator", HeadRepo: "szymonrychu/tatara-operator",
		Number: 7, Author: "tatara-bot", HeadBranch: agent.TaskBranch(owner),
	}

	if got := ClassifyPR(proj, repo, botPR, owner, "some-live-other-task"); got != PRClaimed {
		t.Fatalf("a LIVE claimant: ClassifyPR = %q, want %q", got, PRClaimed)
	}
	if got := ClassifyPR(proj, repo, botPR, owner, ""); got != PRAdopt {
		t.Fatalf("a REAPED claimant: ClassifyPR = %q, want %q (a dangling ref is not a claim)", got, PRAdopt)
	}
	if got := ClassifyPR(proj, repo, humanPR(60), nil, "some-live-review-task"); got != PRIgnore {
		t.Fatalf("a LIVE reviewer: ClassifyPR = %q, want %q", got, PRIgnore)
	}
	if got := ClassifyPR(proj, repo, humanPR(60), nil, ""); got != PRReview {
		t.Fatalf("a REAPED reviewer: ClassifyPR = %q, want %q", got, PRReview)
	}
}

// TestEnsureTaskForMRCommentMintsWhenTheOwnerWasReaped: the webhook fast path
// returned the DEAD owner's name to the caller, which then delivered the human's
// comment as a TaskEvent to a Task that does not exist.
func TestEnsureTaskForMRCommentMintsWhenTheOwnerWasReaped(t *testing.T) {
	proj := sweepProject("mr-comment-proj")
	repo := sweepRepo("mr-comment-proj")
	mr := mrOwnedBy(repo.Name, 61, "reaped-review-task")
	mr.Status.State = "open"
	mr.Status.Author = "contributor"
	mr.Status.HeadBranch = "fix/their-branch"
	c := newMirrorClient(t, proj, repo, mr)

	name, minted, err := minterOn(c).EnsureTaskForMRComment(context.Background(), proj, repo, mr, "contributor")
	if err != nil {
		t.Fatalf("EnsureTaskForMRComment: %v", err)
	}
	if name == "reaped-review-task" {
		t.Fatal("the webhook was handed the name of a Task the API server does not have")
	}
	if !minted || name == "" {
		t.Fatalf("EnsureTaskForMRComment = (%q, %v), want a freshly minted review Task", name, minted)
	}
	if !scanTaskExistsIn(t, c, name) {
		t.Fatalf("EnsureTaskForMRComment named %q but no such Task exists", name)
	}
}

func scanTaskExistsIn(t *testing.T, c client.Client, name string) bool {
	t.Helper()
	var tk tatarav1alpha1.Task
	err := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, &tk)
	return err == nil
}

// TestResolveLiveOwnerReturnsAConcurrentlyBoundLiveOwner is the duplicate-mint
// race. issueCR reads through the CACHED client and can name a Task that was
// reaped; a concurrent webhook binds a DIFFERENT, live Task as controller in
// the same window. dropStaleOwner's FRESH read sees that Task, finds nothing to
// drop - and reporting "no owner" from there mints a duplicate.
// createTaskRaceSafe only absorbs that when the live Task holds the SAME
// deterministic name, which a takeover or incident Task does not.
func TestResolveLiveOwnerReturnsAConcurrentlyBoundLiveOwner(t *testing.T) {
	proj := sweepProject("race-proj")
	stale := issueOwnedBy("tatara-operator", 70, "reaped-task")
	fresh := issueOwnedBy("tatara-operator", 70, "takeover-task")
	c := newMirrorClient(t, fresh, liveTask("takeover-task"))

	got, err := minterOn(c).resolveLiveOwner(context.Background(), proj, stale, SweepActivity)
	if err != nil {
		t.Fatalf("resolveLiveOwner: %v", err)
	}
	if got != "takeover-task" {
		t.Fatalf("resolveLiveOwner = %q, want \"takeover-task\" (a live owner bound in the read window)", got)
	}
}

// TestDropStaleOwnerReadsThroughTheUncachedReader: the RetryOnConflict re-read
// must go through the APIReader, like the liveness Get one line above it. A
// cached re-read returns the SAME stale resourceVersion, so every Conflict
// retry loses again, all five attempts burn, and fail("resolve_live_owner")
// skips the issue and fails the whole pass. Proved by content, not by call
// counting: the uncached store carries a plain owner the cached snapshot has
// not seen, and it must survive the drop.
func TestDropStaleOwnerReadsThroughTheUncachedReader(t *testing.T) {
	proj := sweepProject("uncached-proj")
	cached := issueOwnedBy("tatara-operator", 71, "reaped-task")
	uncached := issueOwnedBy("tatara-operator", 71, "reaped-task")
	uncached.OwnerReferences = append(uncached.OwnerReferences, metav1.OwnerReference{
		APIVersion: tatarav1alpha1.GroupVersion.String(), Kind: "Task",
		Name: "landed-after-the-cache-snapshot", UID: types.UID("u-landed"),
	})
	writer := newMirrorClient(t, cached)
	reader := newMirrorClient(t, uncached, liveTask("landed-after-the-cache-snapshot"))
	m := &Minter{Client: writer, APIReader: reader, Scheme: writer.Scheme()}

	if _, err := m.resolveLiveOwner(context.Background(), proj, cached.DeepCopy(), SweepActivity); err != nil {
		t.Fatalf("resolveLiveOwner: %v", err)
	}
	stored := getIssueCR(t, writer, cached.Name)
	if len(stored.OwnerReferences) != 1 || stored.OwnerReferences[0].Name != "landed-after-the-cache-snapshot" {
		t.Fatalf("owner refs after drop = %+v, want the concurrently-added plain owner intact "+
			"(the drop re-read through the cached client and clobbered it)", stored.OwnerReferences)
	}
}

// TestResolveLiveOwnerTransientGetErrorKeepsTheRef guards the single most
// dangerous line in the change. A transient API error is NOT "the Task does not
// exist": dropping a LIVE owner's ref steals an issue out from under a running
// Task, which is strictly worse than the bug being fixed.
func TestResolveLiveOwnerTransientGetErrorKeepsTheRef(t *testing.T) {
	proj := sweepProject("transient-proj")
	iss := issueOwnedBy("tatara-operator", 72, "owner-task")
	base := newMirrorClient(t, iss, liveTask("owner-task"))
	watchable, ok := base.(client.WithWatch)
	if !ok {
		t.Fatalf("mirror client is %T, want a client.WithWatch to intercept", base)
	}
	c := interceptor.NewClient(watchable, interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, isTask := obj.(*tatarav1alpha1.Task); isTask {
				return apierrors.NewServiceUnavailable("etcd leader election in progress")
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	})
	m := &Minter{Client: c, APIReader: c, Scheme: base.Scheme()}

	before := testutil.ToFloat64(obs.SweepStaleOwnerRepairedTotal.WithLabelValues(proj.Name, SweepActivity))
	got, err := m.resolveLiveOwner(context.Background(), proj, iss.DeepCopy(), SweepActivity)
	if err == nil {
		t.Fatalf("resolveLiveOwner = (%q, nil), want an error: a transient API failure is not proof of absence", got)
	}
	if _, owned := own.ControllerOwner(getIssueCR(t, base, iss.Name)); !owned {
		t.Fatal("a transient API error dropped a LIVE owner's controller ref")
	}
	if d := testutil.ToFloat64(obs.SweepStaleOwnerRepairedTotal.WithLabelValues(proj.Name, SweepActivity)) - before; d != 0 {
		t.Fatalf("SweepStaleOwnerRepairedTotal delta = %v, want 0", d)
	}
}

// TestResolveLiveOwnerIgnoresANonTaskControllerRef: own.ControllerOwner is
// Task-kind only (B.2 rule 4 - a Task never owns a Task), so a Project's
// controller ref is not ownership for this purpose and must not be dropped or
// resolved as a Task name.
func TestResolveLiveOwnerIgnoresANonTaskControllerRef(t *testing.T) {
	proj := sweepProject("nontask-proj")
	ctrl := true
	iss := issueOwnedBy("tatara-operator", 73, "irrelevant")
	iss.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: tatarav1alpha1.GroupVersion.String(), Kind: "Project",
		Name: "nontask-proj", UID: types.UID("u-proj"), Controller: &ctrl,
	}}
	c := newMirrorClient(t, iss)

	got, err := minterOn(c).resolveLiveOwner(context.Background(), proj, iss, SweepActivity)
	if err != nil {
		t.Fatalf("resolveLiveOwner: %v", err)
	}
	if got != "" {
		t.Fatalf("resolveLiveOwner = %q, want \"\" (a non-Task controller ref is not Task ownership)", got)
	}
	if n := len(getIssueCR(t, c, iss.Name).OwnerReferences); n != 1 {
		t.Fatalf("a non-Task owner ref was dropped, %d refs remain, want 1", n)
	}
}

// TestMintForItemNamesTheClauseThatRefusedIt: IsOrphanIssue's own doc says a
// caller cannot structurally skip without naming the clause - and the first
// non-sweep caller discarded it with `orphan, _`. A human opens an issue, the
// reporter allowlist refuses, and there is no counter and no log: the same
// green-and-silent state as #521, on the path a human notices first.
func TestMintForItemNamesTheClauseThatRefusedIt(t *testing.T) {
	proj := sweepProject("mintforitem-proj")
	proj.Spec.Scm.ReporterLogins = []string{"alice"}
	repo := sweepRepo("mintforitem-proj")
	c := newMirrorClient(t, proj, repo)

	// The WEBHOOK's funnel, not the sweep's: activity="sweep" on a live forge
	// delivery is exactly the mislabelling L9 names.
	m := minterOn(c)
	m.Activity = WebhookActivity
	before := testutil.ToFloat64(obs.SweepSkippedTotal.WithLabelValues(
		proj.Name, WebhookActivity, SweepSkipReporterNotAllowed))
	_, outcome, err := m.MintForItem(context.Background(), proj, repo,
		ForgeItem{Issue: scm.Issue{Number: 80, State: "open", Author: "mallory"}}, true, nil)
	if err != nil {
		t.Fatalf("MintForItem: %v", err)
	}
	if outcome != MintNotOwed {
		t.Fatalf("outcome = %q, want %q", outcome, MintNotOwed)
	}
	if d := testutil.ToFloat64(obs.SweepSkippedTotal.WithLabelValues(
		proj.Name, WebhookActivity, SweepSkipReporterNotAllowed)) - before; d != 1 {
		t.Fatalf("SweepSkippedTotal{activity=webhook,reason=issue_reporter_not_allowed} delta = %v, want 1", d)
	}
}

// TestMintForItemCountsTheOutcome (L10): MintOutcomeTotal was incremented only
// inside MintIssueTask/MintReviewTask, so every classify decline - the most
// common outcome on the webhook path - was invisible.
func TestMintForItemCountsTheOutcome(t *testing.T) {
	proj := sweepProject("outcome-count-proj")
	repo := sweepRepo("outcome-count-proj")
	c := newMirrorClient(t, proj, repo, issueOwnedBy(repo.Name, 81, "owner-task"), liveTask("owner-task"))

	before := testutil.ToFloat64(obs.MintOutcomeTotal.WithLabelValues(SweepIssueKind, string(MintNotOwed)))
	if _, _, err := minterOn(c).MintForItem(context.Background(), proj, repo,
		ForgeItem{Issue: scm.Issue{Number: 81, State: "open", Author: "szymonrychu"}}, false, nil); err != nil {
		t.Fatalf("MintForItem: %v", err)
	}
	if d := testutil.ToFloat64(obs.MintOutcomeTotal.WithLabelValues(SweepIssueKind, string(MintNotOwed))) - before; d != 1 {
		t.Fatalf("MintOutcomeTotal{kind=issue,outcome=not_owed} delta = %v, want 1", d)
	}
}

// TestSweepStrandsAnOrphanWhoseMintPathErrored (M8): the deadman under-reported
// exactly when the sweep was failing. An orphan whose list_comments/get_issue/
// mint_issue_task/resolve_live_owner fails hits fail() and continue with NO
// series set, so the gauge read zero for an issue that just ended a pass with
// no live owning Task - contradicting the gauge's own Help text.
func TestSweepStrandsAnOrphanWhoseMintPathErrored(t *testing.T) {
	proj := sweepProject("strand-error-proj")
	repo := sweepRepo("strand-error-proj")
	obs.ClearSweepOrphanStranded(proj.Name, repo.Name)
	c := newMirrorClient(t, proj, repo)

	r := &ProjectReconciler{Client: c, Scheme: c.Scheme(), Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry())}
	rd := &sweepReader{
		issues:          []scm.IssueRef{{Number: 90, State: "open", Author: "szymonrychu", CreatedAt: time.Now().Add(-19 * time.Hour)}},
		listCommentsErr: errors.New("forge 502"),
	}
	if _, err := r.SweepProject(context.Background(), proj, rd, []tatarav1alpha1.Repository{*repo}, nil, SweepActivity); err == nil {
		t.Fatal("SweepProject: want the per-item error surfaced")
	}
	if v := testutil.ToFloat64(obs.SweepOrphanStrandedSeconds.WithLabelValues(proj.Name, repo.Name, "90")); v < 19*60*60 {
		t.Fatalf("stranded age = %v, want at least 19h in seconds: an orphan whose pass ERRORED "+
			"still ended that pass with no live owning Task", v)
	}
}

// TestSweepPRSkipsAreCountedAndLogged (L11): sweepPRs counted skips with no
// companion log line, and its budget-bound path counted NOTHING at all - the
// one arm where an orphan PR silently waits for the next pass.
func TestSweepPRSkipsAreCountedAndLogged(t *testing.T) {
	proj := sweepProject("pr-skip-proj")
	proj.Spec.MaxNewTasksPerSweep = 1
	repo := sweepRepo("pr-skip-proj")
	c := newMirrorClient(t, proj, repo)

	before := testutil.ToFloat64(obs.SweepSkippedTotal.WithLabelValues(proj.Name, SweepActivity, SweepSkipMintBudget))
	runSweep(t, c, proj, repo, &sweepReader{prs: []scm.PRRef{humanPR(60), humanPR(61)}})
	if d := testutil.ToFloat64(obs.SweepSkippedTotal.WithLabelValues(proj.Name, SweepActivity, SweepSkipMintBudget)) - before; d != 1 {
		t.Fatalf("SweepSkippedTotal{reason=mint_budget_bound} delta = %v, want 1 on the PR arm", d)
	}
	if n := len(sweepTasks(t, c, proj.Name)); n != 1 {
		t.Fatalf("minted %d tasks under maxNewTasksPerSweep=1, want 1", n)
	}
}

// TestSoonerRequeue (L12): the per-repo requeue was last-wins, not a minimum.
// Harmless while sweepRemintDelay is the only delay constant, and a trap the
// moment a second one appears.
func TestSoonerRequeue(t *testing.T) {
	tests := map[string]struct {
		cur, d, want time.Duration
	}{
		"first non-zero wins":       {0, 30 * time.Second, 30 * time.Second},
		"a later LONGER delay":      {30 * time.Second, 5 * time.Minute, 30 * time.Second},
		"a later SHORTER delay":     {5 * time.Minute, 30 * time.Second, 30 * time.Second},
		"zero never overwrites":     {30 * time.Second, 0, 30 * time.Second},
		"negative never overwrites": {30 * time.Second, -time.Second, 30 * time.Second},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := soonerRequeue(tc.cur, tc.d); got != tc.want {
				t.Fatalf("soonerRequeue(%v, %v) = %v, want %v", tc.cur, tc.d, got, tc.want)
			}
		})
	}
}
