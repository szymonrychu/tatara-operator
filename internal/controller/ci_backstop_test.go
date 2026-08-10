package controller

// PR A item 3. The CI webhook is the PRIMARY writer of
// MergeRequest.status.ciStatus; this is the missed-delivery backstop. A forge
// that drops a delivery - or an MR whose mirror still carried the previous head
// when the delivery landed - would otherwise leave an agent staring at a status
// that will never change again.
//
// The backstop read is AGGREGATE (GetCommitCIStatus folds every check and commit
// status on the commit), which is what entitles it, unlike a single check_run
// webhook, to write green.

import (
	"context"
	"errors"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// ciBackstopReader records every GetCommitCIStatus call so the tests can assert
// the read happened AND what it was asked about (GitLab's full-project-path
// convention is easy to get wrong, and merge.go got it wrong once already).
type ciBackstopReader struct {
	scm.SCMReader
	status string
	err    error
	calls  []struct{ owner, repo, sha string }
}

func (r *ciBackstopReader) ListIssueComments(context.Context, string, string, int) ([]scm.IssueComment, error) {
	return nil, nil
}

func (r *ciBackstopReader) ListPRComments(context.Context, string, string, int) ([]scm.IssueComment, error) {
	return nil, nil
}

func (r *ciBackstopReader) GetCommitCIStatus(_ context.Context, owner, repo, sha string) (string, error) {
	r.calls = append(r.calls, struct{ owner, repo, sha string }{owner, repo, sha})
	return r.status, r.err
}

func ciBackstopMR(task *tatarav1alpha1.Task, status tatarav1alpha1.MergeRequestStatus) *tatarav1alpha1.MergeRequest {
	mr := &tatarav1alpha1.MergeRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name: tatarav1alpha1.MergeRequestName("tatara-operator", 42), Namespace: testNS,
		},
		Spec: tatarav1alpha1.MergeRequestSpec{
			RepositoryRef: "tatara-operator", Number: 42, ProjectRef: "proj",
			URL: "https://github.com/szymonrychu/tatara-operator/pull/42",
		},
		Status: status,
	}
	if task != nil {
		yes := true
		mr.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: tatarav1alpha1.GroupVersion.String(),
			Kind:       "Task", Name: task.Name, UID: task.UID, Controller: &yes,
		}}
	}
	return mr
}

// A MergeRequest owned by a Task that is actively working it gets a LIVE CI
// re-read, stamped into status.ciStatus/status.ciUpdatedAt in the mirror
// vocabulary, and the reconcile requeues on the tightened CI cadence rather
// than the hourly mirror one.
func TestCIBackstop_ActiveTask_RefreshesLiveAndTightensCadence(t *testing.T) {
	task := taskAtStage(tatarav1alpha1.StateUnderImplementation, "")
	task.UID = "task-uid"
	mr := ciBackstopMR(task, tatarav1alpha1.MergeRequestStatus{State: "open", HeadSHA: "head-abc"})
	c := newMirrorClient(t, mirrorProject("tatara-bot"), mirrorRepo(), scmSecret(), task, mr)
	rd := &ciBackstopReader{status: "success"}
	r := &MergeRequestReconciler{
		Client:     c,
		SpillerFor: func(*tatarav1alpha1.Project) objbudget.Spiller { return &mirrorSpiller{} },
		ReaderFor:  func(string, string) (scm.SCMReader, error) { return rd, nil },
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: mr.Name},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter != CIRefreshCadenceActive {
		t.Fatalf("RequeueAfter = %v, want the tightened CI cadence %v", res.RequeueAfter, CIRefreshCadenceActive)
	}
	if len(rd.calls) != 1 {
		t.Fatalf("live CI reads = %d, want exactly 1", len(rd.calls))
	}
	if rd.calls[0].sha != "head-abc" {
		t.Fatalf("CI read at sha %q, want the mirrored head head-abc", rd.calls[0].sha)
	}
	if rd.calls[0].owner != "szymonrychu" || rd.calls[0].repo != "tatara-operator" {
		t.Fatalf("CI read owner/repo = %q/%q, want szymonrychu/tatara-operator", rd.calls[0].owner, rd.calls[0].repo)
	}

	got := getMRCR(t, c, mr.Name)
	if got.Status.CIStatus != scm.CIMirrorGreen {
		t.Fatalf("ciStatus = %q, want %q - the gate vocabulary must be crossed to the mirror one",
			got.Status.CIStatus, scm.CIMirrorGreen)
	}
	if got.Status.CIUpdatedAt == nil {
		t.Fatal("ciUpdatedAt must be stamped alongside ciStatus")
	}
}

// A failing aggregate read lands as red.
func TestCIBackstop_RedIsMirroredAsRed(t *testing.T) {
	task := taskAtStage(tatarav1alpha1.StateAwaitingReview, "")
	task.UID = "task-uid"
	mr := ciBackstopMR(task, tatarav1alpha1.MergeRequestStatus{State: "open", HeadSHA: "head-abc"})
	c := newMirrorClient(t, mirrorProject("tatara-bot"), mirrorRepo(), scmSecret(), task, mr)
	rd := &ciBackstopReader{status: "failure"}
	r := &MergeRequestReconciler{
		Client:     c,
		SpillerFor: func(*tatarav1alpha1.Project) objbudget.Spiller { return &mirrorSpiller{} },
		ReaderFor:  func(string, string) (scm.SCMReader, error) { return rd, nil },
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: mr.Name},
	}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := getMRCR(t, c, mr.Name); got.Status.CIStatus != scm.CIMirrorRed {
		t.Fatalf("ciStatus = %q, want %q", got.Status.CIStatus, scm.CIMirrorRed)
	}
}

// A fresh observation is not re-read. The backstop is a backstop: inside the
// cadence the webhook is the authority and the forge is left alone.
func TestCIBackstop_FreshObservation_NoRead(t *testing.T) {
	task := taskAtStage(tatarav1alpha1.StateUnderImplementation, "")
	task.UID = "task-uid"
	fresh := metav1.NewTime(time.Now())
	mr := ciBackstopMR(task, tatarav1alpha1.MergeRequestStatus{
		State: "open", HeadSHA: "head-abc", CIStatus: scm.CIMirrorGreen, CIUpdatedAt: &fresh,
	})
	c := newMirrorClient(t, mirrorProject("tatara-bot"), mirrorRepo(), scmSecret(), task, mr)
	rd := &ciBackstopReader{status: "failure"}
	r := &MergeRequestReconciler{
		Client:     c,
		SpillerFor: func(*tatarav1alpha1.Project) objbudget.Spiller { return &mirrorSpiller{} },
		ReaderFor:  func(string, string) (scm.SCMReader, error) { return rd, nil },
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: mr.Name},
	}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(rd.calls) != 0 {
		t.Fatalf("live CI reads = %d, want 0 while the observation is inside the cadence", len(rd.calls))
	}
	if got := getMRCR(t, c, mr.Name); got.Status.CIStatus != scm.CIMirrorGreen {
		t.Fatalf("ciStatus = %q, want the untouched %q", got.Status.CIStatus, scm.CIMirrorGreen)
	}
}

// A merged MR's CI can no longer gate anything, so it is not re-read. Neither
// is one whose mirror carries no head at all - there is nothing to ask about.
func TestCIBackstop_TerminalOrHeadlessMR_NoRead(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status tatarav1alpha1.MergeRequestStatus
	}{
		{"merged", tatarav1alpha1.MergeRequestStatus{State: "merged", HeadSHA: "head-abc"}},
		{"closed", tatarav1alpha1.MergeRequestStatus{State: "closed", HeadSHA: "head-abc"}},
		{"no head mirrored yet", tatarav1alpha1.MergeRequestStatus{State: "open"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task := taskAtStage(tatarav1alpha1.StateUnderImplementation, "")
			task.UID = "task-uid"
			mr := ciBackstopMR(task, tc.status)
			c := newMirrorClient(t, mirrorProject("tatara-bot"), mirrorRepo(), scmSecret(), task, mr)
			rd := &ciBackstopReader{status: "success"}
			r := &MergeRequestReconciler{
				Client:     c,
				SpillerFor: func(*tatarav1alpha1.Project) objbudget.Spiller { return &mirrorSpiller{} },
				ReaderFor:  func(string, string) (scm.SCMReader, error) { return rd, nil },
			}
			if _, err := r.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{Namespace: testNS, Name: mr.Name},
			}); err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			if len(rd.calls) != 0 {
				t.Fatalf("live CI reads = %d, want 0", len(rd.calls))
			}
		})
	}
}

// A forge error on the backstop read is SWALLOWED. The drains that run after it
// - pendingReview above all - are the only thing that advances a Task off
// reviewing, and a CI read that 502s must not be able to wedge them.
func TestCIBackstop_ForgeErrorIsNonFatal(t *testing.T) {
	task := taskAtStage(tatarav1alpha1.StateUnderImplementation, "")
	task.UID = "task-uid"
	mr := ciBackstopMR(task, tatarav1alpha1.MergeRequestStatus{State: "open", HeadSHA: "head-abc"})
	c := newMirrorClient(t, mirrorProject("tatara-bot"), mirrorRepo(), scmSecret(), task, mr)
	rd := &ciBackstopReader{err: errors.New("502 bad gateway")}
	r := &MergeRequestReconciler{
		Client:     c,
		SpillerFor: func(*tatarav1alpha1.Project) objbudget.Spiller { return &mirrorSpiller{} },
		ReaderFor:  func(string, string) (scm.SCMReader, error) { return rd, nil },
	}
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: mr.Name},
	})
	if err != nil {
		t.Fatalf("a failed CI backstop read must not fail the reconcile, got: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Fatalf("want a normal cadence requeue, got %+v", res)
	}
	if got := getMRCR(t, c, mr.Name); got.Status.CIUpdatedAt != nil {
		t.Fatal("a failed read must NOT date an observation that never happened")
	}
}

// CIRefreshCadence is the cadence table. Only a Task actively working the MR
// earns the tightened interval; everything else rides the ordinary mirror
// cadence, because nobody is waiting on it.
func TestCIRefreshCadence(t *testing.T) {
	for _, tc := range []struct {
		name string
		task *tatarav1alpha1.Task
		want time.Duration
	}{
		{"unowned mirror", nil, MirrorCadenceActive},
		{"implementing", taskAtStage(tatarav1alpha1.StateUnderImplementation, ""), CIRefreshCadenceActive},
		{"awaiting review", taskAtStage(tatarav1alpha1.StateAwaitingReview, ""), CIRefreshCadenceActive},
		{"merging", taskAtStage(tatarav1alpha1.StateMerged, ""), CIRefreshCadenceActive},
		{"parked", taskAtStage(tatarav1alpha1.StateRefined, "awaiting-human"), MirrorCadenceParked},
		{"done", taskAtStage(tatarav1alpha1.StateDone, "delivered"), MirrorCadenceActive},
		{"rejected", taskAtStage(tatarav1alpha1.StateRejected, "declined"), MirrorCadenceActive},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := CIRefreshCadence(tc.task); got != tc.want {
				t.Fatalf("CIRefreshCadence = %v, want %v", got, tc.want)
			}
		})
	}
	if CIRefreshCadenceActive >= MirrorCadenceActive {
		t.Fatalf("the CI backstop cadence (%v) must be TIGHTER than the mirror cadence (%v): "+
			"an agent cannot sit an hour on a pipeline that went red in ninety seconds",
			CIRefreshCadenceActive, MirrorCadenceActive)
	}
}
