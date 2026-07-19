package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// mbWriter records Comment calls and, when commentErr is set, fails them -
// proving a forge outage never blocks the park itself (Task 9).
type mbWriter struct {
	scm.SCMWriter
	comments   []mbComment
	commentErr error
}

type mbComment struct {
	Token, IssueRef, Body string
}

func (w *mbWriter) Comment(_ context.Context, token, issueRef, body string) error {
	w.comments = append(w.comments, mbComment{token, issueRef, body})
	return w.commentErr
}

// mbTask builds a review-kind Task with a Source but NO owned MR/Issue refs
// (the exact shape of an interrupted MintReviewTask mint - live proof:
// mt-r-tatara-cli-87), stageEnteredAt backdated by age.
func mbTask(name string, age time.Duration) *tatarav1alpha1.Task {
	return mbTaskAtStage(name, age, tatarav1alpha1.StageReviewing)
}

// mbTaskAtStage is mbTask with the FROM stage overridable, so the backstop's
// stage-legality guard (issue #381 review BLOCKER) can be exercised against
// stages other than reviewing.
func mbTaskAtStage(name string, age time.Duration, fromStage string) *tatarav1alpha1.Task {
	entered := metav1.NewTime(time.Now().Add(-age))
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: mdNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef: "proj", Kind: "review",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "https://github.com/szymonrychu/tatara-cli/pull/87",
				Number: 87, IsPR: true, Title: "fix the thing",
			},
		},
		Status: tatarav1alpha1.TaskStatus{Stage: fromStage, StageEnteredAt: &entered},
	}
}

func mbReconciler(c client.Client, w scm.SCMWriter) (*TaskReconciler, *obs.OperatorMetrics) {
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	return &TaskReconciler{
		Client:    c,
		Metrics:   m,
		Session:   panicSession{newFakeSession()},
		PodConfig: agent.PodConfig{Namespace: mdNS},
		SCMFor:    func(string) (scm.SCMWriter, error) { return w, nil },
	}, m
}

// TestMRBindingBackstop_ParksPastGrace reproduces mt-r-tatara-cli-87: a
// review Task, Source-bearing, zero MRRefs and zero IssueRefs, past the
// grace window - parks awaiting-human, fires the metric.
func TestMRBindingBackstop_ParksPastGrace(t *testing.T) {
	ctx := context.Background()
	proj := tsProject(3)
	task := mbTask("mt-r-tatara-cli-87", mrBindingBackstopGrace+time.Minute)
	c := newMirrorClient(t, proj, mdSecret(), task)
	w := &mbWriter{}
	r, m := mbReconciler(c, w)

	handled, err := r.reconcileMRBindingBackstop(ctx, proj, task, time.Now())
	if err != nil || !handled {
		t.Fatalf("reconcileMRBindingBackstop = %v, %v, want handled=true, err=nil", handled, err)
	}
	got := mdGetTask(t, c, task.Name)
	if got.Status.Stage != tatarav1alpha1.StageParked || got.Status.StageReason != stage.ReasonAwaitingHuman {
		t.Fatalf("stage = %s(%s), want parked(awaiting-human)", got.Status.Stage, got.Status.StageReason)
	}
	if n := testutil.ToFloat64(m.MRBindingBackstopParkedCounter(proj.Name, "review")); n != 1 {
		t.Fatalf("operator_mr_binding_backstop_total = %v, want 1", n)
	}
	if len(w.comments) != 1 {
		t.Fatalf("Comment calls = %d, want exactly 1", len(w.comments))
	}
	if w.comments[0].IssueRef != "szymonrychu/tatara-cli#87" {
		t.Fatalf("comment issueRef = %q, want szymonrychu/tatara-cli#87", w.comments[0].IssueRef)
	}
}

// TestMRBindingBackstop_WithinGraceIsUntouched: the same shape, but still
// inside the grace window - must not park (a live, merely-slow bind must
// not be preempted).
func TestMRBindingBackstop_WithinGraceIsUntouched(t *testing.T) {
	ctx := context.Background()
	proj := tsProject(3)
	task := mbTask("t-within-grace", mrBindingBackstopGrace/2)
	c := newMirrorClient(t, proj, mdSecret(), task)
	r, _ := mbReconciler(c, &mbWriter{})

	handled, err := r.reconcileMRBindingBackstop(ctx, proj, task, time.Now())
	if err != nil || handled {
		t.Fatalf("reconcileMRBindingBackstop = %v, %v, want handled=false, err=nil", handled, err)
	}
	got := mdGetTask(t, c, task.Name)
	if got.Status.Stage != tatarav1alpha1.StageReviewing {
		t.Fatalf("stage = %s, want unchanged reviewing", got.Status.Stage)
	}
}

// TestMRBindingBackstop_SourcelessIsUntouched: a brainstorm/refine/incident
// Task has NO Source by construction and must never be caught by this
// predicate, no matter its age or empty refs.
func TestMRBindingBackstop_SourcelessIsUntouched(t *testing.T) {
	ctx := context.Background()
	proj := tsProject(3)
	task := mbTask("t-sourceless", mrBindingBackstopGrace+time.Hour)
	task.Spec.Source = nil
	task.Spec.Kind = "brainstorm"
	c := newMirrorClient(t, proj, mdSecret(), task)
	r, _ := mbReconciler(c, &mbWriter{})

	handled, err := r.reconcileMRBindingBackstop(ctx, proj, task, time.Now())
	if err != nil || handled {
		t.Fatalf("reconcileMRBindingBackstop = %v, %v, want handled=false, err=nil", handled, err)
	}
}

// TestMRBindingBackstop_WithRefsIsUntouched: a Task carrying an MRRef (the
// bind landed) must never be caught, regardless of age.
func TestMRBindingBackstop_WithRefsIsUntouched(t *testing.T) {
	ctx := context.Background()
	proj := tsProject(3)
	task := mbTask("t-has-mr", mrBindingBackstopGrace+time.Hour)
	task.Status.MRRefs = []string{tatarav1alpha1.MergeRequestName("tatara-cli", 87)}
	c := newMirrorClient(t, proj, mdSecret(), task)
	r, _ := mbReconciler(c, &mbWriter{})

	handled, err := r.reconcileMRBindingBackstop(ctx, proj, task, time.Now())
	if err != nil || handled {
		t.Fatalf("reconcileMRBindingBackstop = %v, %v, want handled=false, err=nil", handled, err)
	}
}

// TestMRBindingBackstop_AlreadyParkedFiresOnce: once parked, the SAME Task
// must never be re-entered on a later reconcile (parked->parked is not a
// legal edge, and re-firing the metric/comment every reconcile would be a
// notification spam bug).
func TestMRBindingBackstop_AlreadyParkedFiresOnce(t *testing.T) {
	ctx := context.Background()
	proj := tsProject(3)
	task := mbTask("t-already-parked", mrBindingBackstopGrace+time.Hour)
	task.Status.Stage = tatarav1alpha1.StageParked
	task.Status.StageReason = stage.ReasonAwaitingHuman
	c := newMirrorClient(t, proj, mdSecret(), task)
	r, m := mbReconciler(c, &mbWriter{})

	handled, err := r.reconcileMRBindingBackstop(ctx, proj, task, time.Now())
	if err != nil || handled {
		t.Fatalf("reconcileMRBindingBackstop = %v, %v, want handled=false, err=nil", handled, err)
	}
	if n := testutil.ToFloat64(m.MRBindingBackstopParkedCounter(proj.Name, "review")); n != 0 {
		t.Fatalf("operator_mr_binding_backstop_total = %v, want 0 (already parked)", n)
	}
}

// TestMRBindingBackstop_CommentFailureDoesNotBlockPark: the park is the
// load-bearing correctness action; a forge outage on the notification
// comment must never leave the Task un-parked or the reconcile erroring
// forever.
func TestMRBindingBackstop_CommentFailureDoesNotBlockPark(t *testing.T) {
	ctx := context.Background()
	proj := tsProject(3)
	task := mbTask("t-comment-fails", mrBindingBackstopGrace+time.Minute)
	c := newMirrorClient(t, proj, mdSecret(), task)
	w := &mbWriter{commentErr: fmt.Errorf("dial tcp: connection refused")}
	r, m := mbReconciler(c, w)

	handled, err := r.reconcileMRBindingBackstop(ctx, proj, task, time.Now())
	if err != nil || !handled {
		t.Fatalf("reconcileMRBindingBackstop = %v, %v, want handled=true, err=nil", handled, err)
	}
	got := mdGetTask(t, c, task.Name)
	if got.Status.Stage != tatarav1alpha1.StageParked {
		t.Fatalf("stage = %s, want parked despite the comment failure", got.Status.Stage)
	}
	if n := testutil.ToFloat64(m.MRBindingBackstopParkedCounter(proj.Name, "review")); n != 1 {
		t.Fatalf("operator_mr_binding_backstop_total = %v, want 1 even though the comment failed", n)
	}
}

// TestMRBindingBackstop_TriagingIsNotParked is the issue #381 review BLOCKER:
// triaging->parked is NOT a legal F.3 edge (stage.Transitions has no such row),
// so a source-bearing triaging Task with zero refs past grace must fall
// through to normal reconcile (the triage-stalled clock owns that case)
// instead of calling r.enter into an illegal edge, which would return
// *stage.IllegalTransitionError and infinite-requeue crash-loop the Task.
func TestMRBindingBackstop_TriagingIsNotParked(t *testing.T) {
	ctx := context.Background()
	proj := tsProject(3)
	task := mbTaskAtStage("t-triaging", mrBindingBackstopGrace+time.Minute, tatarav1alpha1.StageTriaging)
	c := newMirrorClient(t, proj, mdSecret(), task)
	r, m := mbReconciler(c, &mbWriter{})

	handled, err := r.reconcileMRBindingBackstop(ctx, proj, task, time.Now())
	if err != nil || handled {
		t.Fatalf("reconcileMRBindingBackstop = %v, %v, want handled=false, err=nil (falls through to triage-stalled clock)", handled, err)
	}
	got := mdGetTask(t, c, task.Name)
	if got.Status.Stage != tatarav1alpha1.StageTriaging {
		t.Fatalf("stage = %s, want unchanged triaging", got.Status.Stage)
	}
	if n := testutil.ToFloat64(m.MRBindingBackstopParkedCounter(proj.Name, "review")); n != 0 {
		t.Fatalf("operator_mr_binding_backstop_total = %v, want 0", n)
	}
}

// TestMRBindingBackstop_RepairsUnboundMRInsteadOfParking is PR 388: the twin's
// MergeRequest CR exists but the mint was interrupted before the bind, leaving
// it UNBOUND (no controller owner) and the Task with empty mrRefs. The backstop
// must run the intake funnel's own repairMRBinding instead of parking a Task a
// transient error left behind: refs stamped, CR owned, no park, no metric, no
// human summoned.
func TestMRBindingBackstop_RepairsUnboundMRInsteadOfParking(t *testing.T) {
	ctx := context.Background()
	proj := tsProject(3)
	task := mbTask("mt-r-tatara-cli-87", mrBindingBackstopGrace+time.Minute)
	mr := mdMR(task, "tatara-cli", 87)
	mr.OwnerReferences = nil // the interrupted mint's unbound stub
	c := newMirrorClient(t, proj, mdSecret(), mdRepo("tatara-cli"), task, mr)
	w := &mbWriter{}
	r, m := mbReconciler(c, w)
	r.Scheme = c.Scheme()

	handled, err := r.reconcileMRBindingBackstop(ctx, proj, task, time.Now())
	if err != nil || !handled {
		t.Fatalf("reconcileMRBindingBackstop = %v, %v, want handled=true (repaired), err=nil", handled, err)
	}
	got := mdGetTask(t, c, task.Name)
	if got.Status.Stage != tatarav1alpha1.StageReviewing {
		t.Fatalf("stage = %s(%s), want unchanged reviewing: a repairable bind must not park",
			got.Status.Stage, got.Status.StageReason)
	}
	wantRef := tatarav1alpha1.MergeRequestName("tatara-cli", 87)
	if len(got.Status.MRRefs) != 1 || got.Status.MRRefs[0] != wantRef {
		t.Fatalf("mrRefs = %v, want [%s]", got.Status.MRRefs, wantRef)
	}
	gotMR := mdGetMR(t, c, wantRef)
	if owner, ok := own.ControllerOwner(gotMR); !ok || owner != task.Name {
		t.Fatalf("mr controller owner = %q, %v, want %s", owner, ok, task.Name)
	}
	if n := testutil.ToFloat64(m.MRBindingBackstopParkedCounter(proj.Name, "review")); n != 0 {
		t.Fatalf("operator_mr_binding_backstop_total = %v, want 0 (repaired, not parked)", n)
	}
	if len(w.comments) != 0 {
		t.Fatalf("Comment calls = %d, want 0: no human is summoned for a repaired bind", len(w.comments))
	}
}

// TestMRBindingBackstop_RepairsUnboundIssueInsteadOfParking is the Issue-mint
// counterpart: an interrupted MintIssueTask left its Issue CR an unbound stub.
func TestMRBindingBackstop_RepairsUnboundIssueInsteadOfParking(t *testing.T) {
	ctx := context.Background()
	proj := tsProject(3)
	task := mbTaskAtStage("mt-i-tatara-cli-12", mrBindingBackstopGrace+time.Minute, tatarav1alpha1.StageClarifying)
	task.Spec.Kind = "clarify"
	task.Spec.Source = &tatarav1alpha1.TaskSource{
		Provider: "github",
		IssueRef: "https://github.com/szymonrychu/tatara-cli/issues/12#12",
		Number:   12, Title: "fix the other thing",
	}
	iss := mdIssue(task, "tatara-cli", 12)
	iss.OwnerReferences = nil
	c := newMirrorClient(t, proj, mdSecret(), mdRepo("tatara-cli"), task, iss)
	r, m := mbReconciler(c, &mbWriter{})
	r.Scheme = c.Scheme()

	handled, err := r.reconcileMRBindingBackstop(ctx, proj, task, time.Now())
	if err != nil || !handled {
		t.Fatalf("reconcileMRBindingBackstop = %v, %v, want handled=true (repaired), err=nil", handled, err)
	}
	got := mdGetTask(t, c, task.Name)
	if got.Status.Stage != tatarav1alpha1.StageClarifying {
		t.Fatalf("stage = %s(%s), want unchanged clarifying", got.Status.Stage, got.Status.StageReason)
	}
	wantRef := tatarav1alpha1.IssueName("tatara-cli", 12)
	if len(got.Status.IssueRefs) != 1 || got.Status.IssueRefs[0] != wantRef {
		t.Fatalf("issueRefs = %v, want [%s]", got.Status.IssueRefs, wantRef)
	}
	if n := testutil.ToFloat64(m.MRBindingBackstopParkedCounter(proj.Name, "clarify")); n != 0 {
		t.Fatalf("operator_mr_binding_backstop_total = %v, want 0 (repaired, not parked)", n)
	}
}

// TestMRBindingBackstop_ForeignOwnedMRStillParks: the twin's MergeRequest CR is
// controller-owned by ANOTHER Task. The repair must never steal (the intake
// funnel's own rule), so this bind is unrepairable and the backstop parks
// awaiting-human exactly as before.
func TestMRBindingBackstop_ForeignOwnedMRStillParks(t *testing.T) {
	ctx := context.Background()
	proj := tsProject(3)
	task := mbTask("mt-r-tatara-cli-87", mrBindingBackstopGrace+time.Minute)
	other := mbTask("mt-r-tatara-cli-87-other", mrBindingBackstopGrace+time.Minute)
	mr := mdMR(other, "tatara-cli", 87) // controller-owned by the OTHER task
	c := newMirrorClient(t, proj, mdSecret(), mdRepo("tatara-cli"), task, mr)
	w := &mbWriter{}
	r, m := mbReconciler(c, w)
	r.Scheme = c.Scheme()

	handled, err := r.reconcileMRBindingBackstop(ctx, proj, task, time.Now())
	if err != nil || !handled {
		t.Fatalf("reconcileMRBindingBackstop = %v, %v, want handled=true, err=nil", handled, err)
	}
	got := mdGetTask(t, c, task.Name)
	if got.Status.Stage != tatarav1alpha1.StageParked || got.Status.StageReason != stage.ReasonAwaitingHuman {
		t.Fatalf("stage = %s(%s), want parked(awaiting-human): an unrepairable bind falls through to the park",
			got.Status.Stage, got.Status.StageReason)
	}
	if n := testutil.ToFloat64(m.MRBindingBackstopParkedCounter(proj.Name, "review")); n != 1 {
		t.Fatalf("operator_mr_binding_backstop_total = %v, want 1", n)
	}
	if len(w.comments) != 1 {
		t.Fatalf("Comment calls = %d, want 1", len(w.comments))
	}
}

// TestMRBindingBackstop_DeliveredIsNotParked: delivered->parked is likewise
// not a legal F.3 edge (delivered's only exit is ->failed on operator-error),
// so the same illegal-edge guard must exclude it too.
func TestMRBindingBackstop_DeliveredIsNotParked(t *testing.T) {
	ctx := context.Background()
	proj := tsProject(3)
	task := mbTaskAtStage("t-delivered", mrBindingBackstopGrace+time.Minute, tatarav1alpha1.StageDelivered)
	c := newMirrorClient(t, proj, mdSecret(), task)
	r, m := mbReconciler(c, &mbWriter{})

	handled, err := r.reconcileMRBindingBackstop(ctx, proj, task, time.Now())
	if err != nil || handled {
		t.Fatalf("reconcileMRBindingBackstop = %v, %v, want handled=false, err=nil", handled, err)
	}
	got := mdGetTask(t, c, task.Name)
	if got.Status.Stage != tatarav1alpha1.StageDelivered {
		t.Fatalf("stage = %s, want unchanged delivered", got.Status.Stage)
	}
	if n := testutil.ToFloat64(m.MRBindingBackstopParkedCounter(proj.Name, "review")); n != 0 {
		t.Fatalf("operator_mr_binding_backstop_total = %v, want 0", n)
	}
}
