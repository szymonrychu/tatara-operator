package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// newUnparkTestReconciler builds a ProjectReconciler backed by a fake client
// seeded with objs, with APIReader pointed at the SAME fake client (the
// uncached-reader idiom ApplyUnpark relies on - see unpark.go's ApplyUnpark
// doc comment) and a fresh metrics registry so UnparkDeclined calls do not
// panic on a nil Metrics.
func newUnparkTestReconciler(t *testing.T, objs ...client.Object) *ProjectReconciler {
	t.Helper()
	c := newMirrorClient(t, objs...)
	return &ProjectReconciler{
		Client:    c,
		Scheme:    c.Scheme(),
		APIReader: c,
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
	}
}

// objectKeyOf is client.ObjectKeyFromObject, named for readability at the call
// site in this file's tests (fresh := &Task{}; r.Get(ctx, objectKeyOf(task), fresh)).
func objectKeyOf(o client.Object) client.ObjectKey {
	return client.ObjectKeyFromObject(o)
}

// THE STALL, RETRIED. The webhook fast path lost the cache race and declined
// with not-all-approved, but it persisted the verdict. The next driveUnparks
// pass re-derives the owned-Issue half from LIVE state - which by now shows the
// approval - and re-enters implementing. Without this the Task is stranded
// forever with the approval label visible on the forge.
func TestDriveUnparks_RetriesIdentityUnverifiedFromThePersistedVerdict(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxOpenTasks = 6

	task := &tatarav1alpha1.Task{}
	task.Namespace = "tatara"
	task.Name = "infrastructure-clarify-2026-07-27-gtwgp"
	task.Spec.ProjectRef = "infrastructure"
	task.Spec.Kind = "clarify"
	task.Status.Stage = tatarav1alpha1.StageParked
	task.Status.StageReason = stage.ReasonIdentityUnverified
	task.Status.StageEnteredAt = &metav1.Time{Time: time.Now()}
	task.Status.IssueRefs = []string{"iss-helmfile-26"}
	task.Status.PendingEvents = []tatarav1alpha1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Repo: "helmfile", Number: 26,
		Author: "szymonrychu", Body: "go ahead",
	}}
	task.Status.ApprovalVerdict = &tatarav1alpha1.ApprovalVerdict{
		At:                metav1.Now(),
		IssueRef:          "iss-helmfile-26",
		CommentExternalID: "3606943691",
		Author:            "szymonrychu",
		Phrase:            "go ahead",
	}

	iss := &tatarav1alpha1.Issue{}
	iss.Namespace = "tatara"
	iss.Name = "iss-helmfile-26"
	iss.Status.State = "open"
	iss.Status.Status = "approved" // the write the fast path's cached read missed

	r := newUnparkTestReconciler(t, proj, task, iss)

	if err := r.driveUnparks(context.Background(), proj, time.Now()); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}

	fresh := &tatarav1alpha1.Task{}
	if err := r.Get(context.Background(), objectKeyOf(task), fresh); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if fresh.Status.Stage != tatarav1alpha1.StageImplementing {
		t.Fatalf("stage = %s(%s), want implementing: the backstop did not retry the identity-unverified park",
			fresh.Status.Stage, fresh.Status.StageReason)
	}
}

// No verdict means the grammar has NEVER passed for this Task, and the backstop
// cannot manufacture one. It must decline with grammar-not-passed and leave the
// Task exactly where it was: driving a grammar-less re-entry into implementing
// is the fully autonomous hallucinated-approval-to-prod path.
func TestDriveUnparks_IdentityUnverifiedWithNoVerdictStaysParked(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxOpenTasks = 6

	task := &tatarav1alpha1.Task{}
	task.Namespace = "tatara"
	task.Name = "t"
	task.Spec.ProjectRef = "infrastructure"
	task.Spec.Kind = "clarify"
	task.Status.Stage = tatarav1alpha1.StageParked
	task.Status.StageReason = stage.ReasonIdentityUnverified
	task.Status.StageEnteredAt = &metav1.Time{Time: time.Now()}
	task.Status.IssueRefs = []string{"iss-helmfile-26"}
	task.Status.PendingEvents = []tatarav1alpha1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Author: "szymonrychu", Body: "go ahead",
	}}

	iss := &tatarav1alpha1.Issue{}
	iss.Namespace = "tatara"
	iss.Name = "iss-helmfile-26"
	iss.Status.State = "open"
	iss.Status.Status = "approved"

	r := newUnparkTestReconciler(t, proj, task, iss)

	if err := r.driveUnparks(context.Background(), proj, time.Now()); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}

	fresh := &tatarav1alpha1.Task{}
	if err := r.Get(context.Background(), objectKeyOf(task), fresh); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if fresh.Status.Stage != tatarav1alpha1.StageParked {
		t.Fatalf("stage = %s, want parked: the backstop re-entered with NO grammar verdict", fresh.Status.Stage)
	}
}

// THE SECOND SECURITY BOUNDARY. A verdict on record proves the grammar passed
// ONCE, against the thread state at THAT time. It says nothing about whether
// the Task's owned Issues are STILL all approved right now - an Issue can be
// reopened, a new owned Issue can be added and not yet approved, or the
// approval can simply not (yet) be visible on this one. The backstop must
// re-derive that half from LIVE state on every pass, not treat the stored
// verdict as a blanket licence: a verdict plus a currently-unapproved owned
// Issue must still decline (DeclineNotAllApproved) and leave the Task parked.
func TestDriveUnparks_IdentityUnverifiedWithVerdictButNotAllApprovedStaysParked(t *testing.T) {
	proj := &tatarav1alpha1.Project{}
	proj.Namespace = "tatara"
	proj.Name = "infrastructure"
	proj.Spec.MaxOpenTasks = 6

	task := &tatarav1alpha1.Task{}
	task.Namespace = "tatara"
	task.Name = "infrastructure-clarify-2026-07-27-notapproved"
	task.Spec.ProjectRef = "infrastructure"
	task.Spec.Kind = "clarify"
	task.Status.Stage = tatarav1alpha1.StageParked
	task.Status.StageReason = stage.ReasonIdentityUnverified
	task.Status.StageEnteredAt = &metav1.Time{Time: time.Now()}
	task.Status.IssueRefs = []string{"iss-helmfile-27"}
	task.Status.PendingEvents = []tatarav1alpha1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Repo: "helmfile", Number: 27,
		Author: "szymonrychu", Body: "go ahead",
	}}
	// A verdict IS on record - the grammar passed at some point - but the live
	// owned Issue is still open and NOT approved: the other half of the F.6
	// rule the backstop must re-derive from live state, not assume from the
	// verdict.
	task.Status.ApprovalVerdict = &tatarav1alpha1.ApprovalVerdict{
		At:                metav1.Now(),
		IssueRef:          "iss-helmfile-27",
		CommentExternalID: "3606943700",
		Author:            "szymonrychu",
		Phrase:            "go ahead",
	}

	iss := &tatarav1alpha1.Issue{}
	iss.Namespace = "tatara"
	iss.Name = "iss-helmfile-27"
	iss.Status.State = "open"
	iss.Status.Status = "" // NOT approved live

	r := newUnparkTestReconciler(t, proj, task, iss)

	if err := r.driveUnparks(context.Background(), proj, time.Now()); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}

	fresh := &tatarav1alpha1.Task{}
	if err := r.Get(context.Background(), objectKeyOf(task), fresh); err != nil {
		t.Fatalf("get task: %v", err)
	}
	if fresh.Status.Stage != tatarav1alpha1.StageParked {
		t.Fatalf("stage = %s, want parked: a stored verdict re-entered the Task despite live state showing "+
			"NOT all owned Issues approved", fresh.Status.Stage)
	}
	if got := testutil.ToFloat64(r.Metrics.UnparkDeclinedCounter(stage.ReasonIdentityUnverified, string(DeclineNotAllApproved))); got != 1 {
		t.Fatalf("operator_unpark_declined_total{identity-unverified,not-all-approved} = %v, want 1", got)
	}
}
