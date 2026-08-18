package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// TestReconcile_PermanentForge404MarksClosedInsteadOfLooping is
// tatara-operator#426: a permanently-deleted upstream MR/issue 404s on every
// forge write and the reconciler used to return that error unchanged, so
// controller-runtime requeued with exponential backoff forever. A 404 is a
// PERMANENT response - it never clears on retry - so the reconciler must
// instead mark the mirror closed and stop, exactly like a real forge close.
func TestReconcile_PermanentForge404MarksClosedInsteadOfLooping(t *testing.T) {
	ctx := context.Background()
	task := mdTask("t1", "implement", tatarav1alpha1.StateMerged)
	mr := mdMR(task, "helmfile", 1311)
	now := metav1.Now()
	mr.Status.Ownership = tatarav1alpha1.OwnershipTatara
	mr.Status.OwnershipReason = "takeover-requested-by:alice"
	mr.Status.OwnershipChangedAt = &now

	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("helmfile"), task, mr)
	f := newFakeForge(t)
	f.commentErr = &scm.HTTPError{Status: 404, Path: "/projects/szymonrychu%2Fhelmfile/issues/1311/notes", Body: `{"message":"404 Not found"}`}

	d := mdNewDriver(t, f, c)
	r := mdMRReconciler(c, d)

	res, err := r.Reconcile(ctx, reqFor(mr))
	if err != nil {
		t.Fatalf("reconcile must NOT propagate a permanent 404 into an error requeue, got: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Fatalf("want a normal cadence requeue, got %+v", res)
	}

	got := mdGetMR(t, c, mr.Name)
	if got.Status.State != "closed" {
		t.Fatalf("want mirror marked closed on a permanent 404, got state=%q", got.Status.State)
	}
}

// TestReconcile_MirrorRead404IsVisibleInPrometheus closes the observability gap
// the incident's Q12 found: 53 GitHub 404s produced NO
// operator_scm_request_errors_by_status_total series at all, because the mirror
// read path never recorded one. operator_mirror_sync_total carries no status, so
// nothing in Prometheus could tell a permanent 404 from a retryable 504.
func TestReconcile_MirrorRead404IsVisibleInPrometheus(t *testing.T) {
	rd := &mirrorReader{prCommentsErr: &scm.HTTPError{Status: 404, Path: "/x", Body: "gone"}}
	_, r, mr := mrGoneFixture(t, rd)
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	r.Driver = &StageDriver{Client: r.Client, Metrics: m}

	if _, err := r.Reconcile(context.Background(), reqFor(mr)); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := testutil.ToFloat64(m.SCMRequestErrorByStatusCounter("github", "list_pr_comments", "404")); got != 1 {
		t.Fatalf("operator_scm_request_errors_by_status_total{verb=\"list_pr_comments\",status=\"404\"} = %v, want 1", got)
	}
}

// TestReconcile_MirrorRead5xxIsVisibleInPrometheus: telling a PERMANENT 404 from
// a RETRYABLE 504 is the whole point of the counter, so the retryable arm has to
// record too - recording only inside the gone branch would leave the metric
// unable to make the one distinction it exists for.
func TestReconcile_MirrorRead5xxIsVisibleInPrometheus(t *testing.T) {
	rd := &mirrorReader{prCommentsErr: &scm.HTTPError{Status: 504, Path: "/x", Body: "gateway timeout"}}
	_, r, mr := mrGoneFixture(t, rd)
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	r.Driver = &StageDriver{Client: r.Client, Metrics: m}

	if _, err := r.Reconcile(context.Background(), reqFor(mr)); err == nil {
		t.Fatal("a 504 must still propagate")
	}
	if got := testutil.ToFloat64(m.SCMRequestErrorByStatusCounter("github", "list_pr_comments", "504")); got != 1 {
		t.Fatalf("504 series = %v, want 1", got)
	}
}

// TestRecordSCMReadError_IgnoresNonForgeFailures: the reconcilers hand this the
// error from a whole mirror sync, which also wraps scm.OwnerRepo parse failures
// and objbudget APISERVER writes. scm.ErrorStatus calls every non-HTTPError
// "network", so recording unconditionally would report an etcd conflict as an
// unreachable forge on the SCM-request panel.
func TestRecordSCMReadError_IgnoresNonForgeFailures(t *testing.T) {
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	recordSCMReadError(m, "github", "list_pr_comments", errors.New("etcd: conflict on status update"))
	if got := testutil.ToFloat64(m.SCMRequestErrorByStatusCounter("github", "list_pr_comments", "network")); got != 0 {
		t.Fatalf("a non-forge failure was recorded as an SCM request error: %v", got)
	}
	recordSCMReadError(nil, "github", "list_pr_comments", &scm.HTTPError{Status: 404})
}

// mrGoneFixture builds the MergeRequest reconciler with the CADENCE MIRROR SYNC
// ON (mdMRReconciler deliberately leaves ReaderFor nil) and no Driver, so the
// only forge call a reconcile can make is the thread read under test.
func mrGoneFixture(t *testing.T, rd *mirrorReader) (client.Client, *MergeRequestReconciler, *tatarav1alpha1.MergeRequest) {
	t.Helper()
	proj, repo := mirrorProject("tatara-bot"), mirrorRepo()
	task := taskAtStage(tatarav1alpha1.StateAwaitingReview, "")
	mr := &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name: tatarav1alpha1.MergeRequestName(repo.Name, 62), Namespace: testNS,
			OwnerReferences: mdCtrlOwnerRefs(task),
		},
		Spec: tatarav1alpha1.MergeRequestSpec{
			RepositoryRef: repo.Name, Number: 62, ProjectRef: proj.Name,
			URL: "https://github.com/szymonrychu/tatara-operator/pull/62",
		},
		Status: tatarav1alpha1.MergeRequestStatus{State: "open"},
	}
	c := newMirrorClient(t, proj, repo, task, mr, scmSecret())
	r := &MergeRequestReconciler{
		Client:     c,
		SpillerFor: func(*tatarav1alpha1.Project) objbudget.Spiller { return &mirrorSpiller{} },
		ReaderFor:  func(string, string) (scm.SCMReader, error) { return rd, nil },
	}
	return c, r, mr
}

// TestReconcile_MirrorReadPermanent404MarksClosed is tatara-operator#621. The
// #436 guard sits BELOW the mirror thread read, so a 404 out of ListPRComments
// returned raw from mergerequest_controller.go:111 and requeued forever - three
// deleted mtg-decks PRs produced 53 ERROR lines in an hour and were still
// doubling toward the 1000s cap when the alert was investigated. A permanent
// 404/410 on the READ must reach the same disposition the WRITES already reach.
func TestReconcile_MirrorReadPermanent404MarksClosed(t *testing.T) {
	rd := &mirrorReader{prCommentsErr: &scm.HTTPError{
		Status: 404,
		Path:   "/repos/szymonrychu/mtg-decks/issues/62/comments",
		Body:   `{"message":"Not Found"}`,
	}}
	c, r, mr := mrGoneFixture(t, rd)

	res, err := r.Reconcile(context.Background(), reqFor(mr))
	if err != nil {
		t.Fatalf("a permanent 404 on the mirror READ must not requeue as an error, got: %v", err)
	}
	if res.RequeueAfter != CIRefreshCadenceActive {
		t.Fatalf("RequeueAfter = %v, want min(cadence, ciCadence) = %v", res.RequeueAfter, CIRefreshCadenceActive)
	}
	if rd.headSHACalls != 1 {
		t.Fatalf("repo-readable probe calls = %d, want exactly 1", rd.headSHACalls)
	}
	// The probe must address the REPO the mirror belongs to. A probe handed the
	// wrong (or an empty) slug answers for something else entirely.
	if rd.headSHAArgs != "szymonrychu|tatara-operator" {
		t.Fatalf("probe addressed %q, want szymonrychu|tatara-operator", rd.headSHAArgs)
	}
	got := mdGetMR(t, c, mr.Name)
	if got.Status.State != "closed" {
		t.Fatalf("want mirror marked closed, got state=%q", got.Status.State)
	}
	// Without the stamp mirrorSyncDue never goes false, so every later reconcile
	// re-reads the thread it has just proven gone and re-probes the repo - at the
	// 5-minute CI tick that is a HIGHER forge rate than the backoff being removed,
	// with the drains starved on every pass.
	if got.Status.LastSyncedAt == nil {
		t.Fatal("LastSyncedAt was not stamped: the gone thread will be re-read on every reconcile")
	}
}

// TestReconcile_MirrorReadGoneStopsRereadingUntilTheNextCadence is the other half
// of that stamp: the SECOND reconcile must make no forge call at all and must
// fall through to the CI backstop and the drains instead of taking the gone
// branch again.
func TestReconcile_MirrorReadGoneStopsRereadingUntilTheNextCadence(t *testing.T) {
	rd := &mirrorReader{prCommentsErr: &scm.HTTPError{Status: 404, Path: "/x", Body: "gone"}}
	_, r, mr := mrGoneFixture(t, rd)

	if _, err := r.Reconcile(context.Background(), reqFor(mr)); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if _, err := r.Reconcile(context.Background(), reqFor(mr)); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if rd.prCalls != 1 || rd.headSHACalls != 1 {
		t.Fatalf("second reconcile re-read the gone thread: prCalls=%d headSHACalls=%d, want 1 and 1",
			rd.prCalls, rd.headSHACalls)
	}
}

// TestReconcile_MirrorRead410DoesNotCloseTheMirror pins the MergeRequest side to
// the NARROW predicate. status.state="closed" makes stage.MRTerminal true, which
// finalizes the OWNER TASK as rejected(mr-closed-externally) and posts a terminal
// comment on the driving issue - so this disposition may not rest on a status
// GitHub also returns repo-wide for a repository with Issues disabled, which the
// /repos probe cannot see. The Issue side keeps 404/410 because its disposition
// is a skipped read and nothing else.
func TestReconcile_MirrorRead410DoesNotCloseTheMirror(t *testing.T) {
	rd := &mirrorReader{prCommentsErr: &scm.HTTPError{Status: 410, Path: "/x", Body: `{"message":"Gone"}`}}
	c, r, mr := mrGoneFixture(t, rd)

	if _, err := r.Reconcile(context.Background(), reqFor(mr)); err == nil {
		t.Fatal("a 410 on the MR read must propagate, not terminate the owner Task")
	}
	if got := mdGetMR(t, c, mr.Name); got.Status.State != "open" {
		t.Fatalf("want state left open on a 410, got %q", got.Status.State)
	}
}

// TestReconcile_MirrorRead404WithUnreadableRepoStaysLoud is the discriminator
// the incident demands: a 404 on ONE thread proves that PR is gone, but a
// deleted repo or a revoked token 404s EVERY thread in the repo. Confirming the
// repo is still readable is what stops a credential fault silently closing every
// mirror in it - that failure must stay loud.
func TestReconcile_MirrorRead404WithUnreadableRepoStaysLoud(t *testing.T) {
	gone := &scm.HTTPError{Status: 404, Path: "/repos/szymonrychu/mtg-decks/issues/62/comments", Body: `{"message":"Not Found"}`}
	rd := &mirrorReader{
		prCommentsErr: gone,
		headSHAErr:    &scm.HTTPError{Status: 404, Path: "/repos/szymonrychu/mtg-decks", Body: `{"message":"Not Found"}`},
	}
	c, r, mr := mrGoneFixture(t, rd)

	if _, err := r.Reconcile(context.Background(), reqFor(mr)); err == nil {
		t.Fatal("a repo-wide 404 must propagate, not close the mirror")
	}
	if got := mdGetMR(t, c, mr.Name); got.Status.State != "open" {
		t.Fatalf("want state left open when the repo itself is unreadable, got %q", got.Status.State)
	}
}

// TestReconcile_TerminalMirrorIsNotThreadRead is tatara-operator#436's own
// exclusion (mergerequest_controller.go:237-241) pushed up to the thread READ:
// a merged or closed mirror's thread can no longer gate anything, and re-reading
// it forever is pure forge load. It is also the ONLY way a 404 can reach the
// mark-closed function and flip an already-merged mirror to closed.
func TestReconcile_TerminalMirrorIsNotThreadRead(t *testing.T) {
	for _, state := range []string{"merged", "closed"} {
		t.Run(state, func(t *testing.T) {
			rd := &mirrorReader{}
			c, r, mr := mrGoneFixture(t, rd)

			live := mdGetMR(t, c, mr.Name)
			live.Status.State = state
			if err := c.Status().Update(context.Background(), live); err != nil {
				t.Fatalf("set state=%s: %v", state, err)
			}

			if _, err := r.Reconcile(context.Background(), reqFor(mr)); err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			if rd.prCalls != 0 || rd.calls != 0 || rd.headSHACalls != 0 {
				t.Fatalf("terminal mirror was read: prCalls=%d calls=%d headSHACalls=%d, want 0/0/0",
					rd.prCalls, rd.calls, rd.headSHACalls)
			}
			if got := mdGetMR(t, c, mr.Name); got.Status.LastSyncedAt != nil {
				t.Fatalf("LastSyncedAt was stamped on a mirror that was never read: %v", got.Status.LastSyncedAt)
			}
		})
	}
}

// TestReconcile_MergedMirrorIsNeverMarkedClosed is tatara-operator#621.
// MergeRequest.Status.State is not mirror-local: "merged" -> "closed" makes
// stage.AllMRsMerged false while AllMRsTerminal stays true, so terminalMREdge
// / OwnMRsShippedEdge (reviewpost.go:421-429, :463-465) finalize the owner Task
// rejected(mr-closed-externally) with a public terminal comment on the driving
// issue, instead of done. A forge 404 may stop work; it may not rewrite a merge
// that already happened.
func TestReconcile_MergedMirrorIsNeverMarkedClosed(t *testing.T) {
	ctx := context.Background()
	task := mdTask("t1", "implement", tatarav1alpha1.StateMerged)
	mr := mdMR(task, "helmfile", 1311)
	mr.Status.State = "merged"
	now := metav1.Now()
	mr.Status.Ownership = tatarav1alpha1.OwnershipTatara
	mr.Status.OwnershipReason = "takeover-requested-by:alice"
	mr.Status.OwnershipChangedAt = &now

	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("helmfile"), task, mr)
	f := newFakeForge(t)
	f.commentErr = &scm.HTTPError{Status: 404, Path: "/projects/szymonrychu%2Fhelmfile/issues/1311/notes", Body: `{"message":"404 Not found"}`}

	d := mdNewDriver(t, f, c)
	r := mdMRReconciler(c, d)

	res, err := r.Reconcile(ctx, reqFor(mr))
	if err != nil {
		t.Fatalf("reconcile must NOT propagate a permanent 404 into an error requeue, got: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Fatalf("want a normal cadence requeue (the loop must still be stopped), got %+v", res)
	}

	got := mdGetMR(t, c, mr.Name)
	if got.Status.State != "merged" {
		t.Fatalf("a permanent 404 rewrote an already-merged mirror to %q", got.Status.State)
	}
}

// TestMarkMergeRequestThreadGone_RefusesARecordedMerge is the same invariant one
// layer down, driven directly because the caller's terminal exclusion makes it
// unreachable through Reconcile. The refusal lives on the WRITE, not on that one
// call site, so it has to be pinned there: whoever adds a second caller must not
// have to rediscover why "closed" may not overwrite a merge.
//
// MergedAt is the second arm because it is the merge path's own receipt and
// survives any later edit to State.
func TestMarkMergeRequestThreadGone_RefusesARecordedMerge(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state string
		merge bool
	}{
		{"state merged", "merged", false},
		{"mergedAt stamped", "open", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, r, mr := mrGoneFixture(t, &mirrorReader{})
			live := mdGetMR(t, c, mr.Name)
			live.Status.State = tc.state
			if tc.merge {
				now := metav1.Now()
				live.Status.MergedAt = &now
			}
			if err := c.Status().Update(context.Background(), live); err != nil {
				t.Fatalf("seed: %v", err)
			}

			if err := markMergeRequestThreadGone(context.Background(), c, r.spiller(nil), live, time.Now()); err != nil {
				t.Fatalf("markMergeRequestThreadGone: %v", err)
			}
			got := mdGetMR(t, c, mr.Name)
			if got.Status.State != tc.state {
				t.Fatalf("a recorded merge was rewritten to %q", got.Status.State)
			}
			if got.Status.LastSyncedAt != nil {
				t.Fatalf("the refused write still stamped LastSyncedAt: %v", got.Status.LastSyncedAt)
			}
		})
	}
}

// TestReconcile_MirrorRead5xxStaysRetryable pins the guard to the STATUS, not to
// the call site: the same object and the same ListPRComments call answered 504
// once mid-incident. A gateway timeout is retryable and must never be read as
// "the upstream PR is gone".
func TestReconcile_MirrorRead5xxStaysRetryable(t *testing.T) {
	rd := &mirrorReader{prCommentsErr: &scm.HTTPError{
		Status: 504,
		Path:   "/repos/szymonrychu/mtg-decks/issues/62/comments",
		Body:   "gateway timeout",
	}}
	c, r, mr := mrGoneFixture(t, rd)

	if _, err := r.Reconcile(context.Background(), reqFor(mr)); err == nil {
		t.Fatal("a 504 must propagate as a retryable error")
	}
	if rd.headSHACalls != 0 {
		t.Fatalf("the repo probe must run ONLY on the 404/410 path, got %d calls", rd.headSHACalls)
	}
	if got := mdGetMR(t, c, mr.Name); got.Status.State != "open" {
		t.Fatalf("want state left open on a 5xx, got %q", got.Status.State)
	}
}
