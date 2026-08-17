package controller

import (
	"context"
	"testing"

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
	if res.RequeueAfter <= 0 {
		t.Fatalf("want a normal cadence requeue, got %+v", res)
	}
	if rd.headSHACalls != 1 {
		t.Fatalf("repo-readable probe calls = %d, want exactly 1", rd.headSHACalls)
	}
	if got := mdGetMR(t, c, mr.Name); got.Status.State != "closed" {
		t.Fatalf("want mirror marked closed, got state=%q", got.Status.State)
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
