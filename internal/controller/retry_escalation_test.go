package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// reProject is wfProject plus the SCM secret ref the escalation needs to write
// to the forge.
func reProject() *tatarav1alpha1.Project {
	p := wfProject()
	p.Spec.ScmSecretRef = "scm-secret"
	return p
}

// reExhaustedTask is a Task whose retry lane is spent: parked in the lane, at
// the cap, with no schedule armed - which is exactly the state driveRetryLane
// reaches on the pass after the last backoff was served.
func reExhaustedTask(name, reason string) *tatarav1alpha1.Task {
	t := retryParkedTask(name, tatarav1alpha1.StateMerged, reason)
	t.Status.RetryAttempts = tatarav1alpha1.MaxUnparkRetries
	t.Status.IssueRefs = []string{tatarav1alpha1.IssueName("repo-a", 42)}
	return t
}

// TestRetryExhaustionReparksAndComments is the unit's point: a spent lane must
// be LOUD. Before it, the same event was a park nobody found for days.
func TestRetryExhaustionReparksAndComments(t *testing.T) {
	proj := reProject()
	task := reExhaustedTask("t-exhausted", stage.ReasonCIFailed)
	issue := mdIssue(task, "repo-a", 42)
	c := newMirrorClient(t, proj, mdSecret(), mdRepo("repo-a"), issue, task)
	w := &mbWriter{}
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	r := &ProjectReconciler{Client: c, APIReader: c, Scheme: c.Scheme(), Metrics: m,
		SCMFor: func(string) (scm.SCMWriter, error) { return w, nil }}

	if err := r.driveUnparks(context.Background(), proj, time.Now()); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}

	got := mdGetTask(t, c, task.Name)
	if got.Status.ParkReason != stage.ReasonRetryExhausted {
		t.Fatalf("parkReason = %q, want retry-exhausted", got.Status.ParkReason)
	}
	if got.Status.State != tatarav1alpha1.StateMerged {
		t.Fatalf("state = %q, want unchanged merged: a repark never moves the Task", got.Status.State)
	}
	if len(w.comments) != 1 {
		t.Fatalf("Comment calls = %d, want exactly 1", len(w.comments))
	}
	if w.comments[0].IssueRef != "szymonrychu/repo-a#42" {
		t.Fatalf("comment issueRef = %q, want szymonrychu/repo-a#42", w.comments[0].IssueRef)
	}
	if n := testutil.ToFloat64(m.TaskRetryExhaustedCounter(stage.ReasonCIFailed, tatarav1alpha1.StateMerged)); n != 1 {
		t.Fatalf("operator_task_retry_exhausted_total{reason=ci-failed,state=merged} = %v, want 1", n)
	}
}

// TestRetryExhaustionCommentNamesTheBlockerAndEveryAttempt: a comment that says
// only "I gave up" sends a human hunting. It has to name what it was waiting
// on, how many laps it spent and over what window.
func TestRetryExhaustionCommentNamesTheBlockerAndEveryAttempt(t *testing.T) {
	proj := reProject()
	task := reExhaustedTask("t-exhausted-body", stage.ReasonMergeConflictRetry)
	c := newMirrorClient(t, proj, mdSecret(), mdRepo("repo-a"), mdIssue(task, "repo-a", 42), task)
	w := &mbWriter{}
	r := &ProjectReconciler{Client: c, APIReader: c, Scheme: c.Scheme(), Metrics: wfMetrics(),
		SCMFor: func(string) (scm.SCMWriter, error) { return w, nil }}

	if err := r.driveUnparks(context.Background(), proj, time.Now()); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}
	if len(w.comments) != 1 {
		t.Fatalf("Comment calls = %d, want 1", len(w.comments))
	}
	body := w.comments[0].Body
	for _, want := range []string{
		stage.ReasonMergeConflictRetry, // the blocker, by its own name
		"5",                            // every attempt
		"1m", "2m", "4m", "8m", "16m",  // and the schedule they were spent on
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("escalation comment does not mention %q:\n%s", want, body)
		}
	}
}

// TestRetryExhaustionCommentsExactlyOnce: the annotation latch. Without it the
// 30s driver posts the same escalation on every pass for as long as the park
// lasts - the comment storm that makes a loud signal unreadable.
func TestRetryExhaustionCommentsExactlyOnce(t *testing.T) {
	proj := reProject()
	task := reExhaustedTask("t-exhausted-once", stage.ReasonCIFailed)
	c := newMirrorClient(t, proj, mdSecret(), mdRepo("repo-a"), mdIssue(task, "repo-a", 42), task)
	w := &mbWriter{}
	r := &ProjectReconciler{Client: c, APIReader: c, Scheme: c.Scheme(), Metrics: wfMetrics(),
		SCMFor: func(string) (scm.SCMWriter, error) { return w, nil }}

	for i := 0; i < 3; i++ {
		if err := r.driveUnparks(context.Background(), proj, time.Now()); err != nil {
			t.Fatalf("driveUnparks pass %d: %v", i, err)
		}
	}
	if len(w.comments) != 1 {
		t.Fatalf("Comment calls = %d over three passes, want exactly 1", len(w.comments))
	}
	got := mdGetTask(t, c, task.Name)
	if got.Annotations[tatarav1alpha1.AnnRetryExhaustedCommented] == "" {
		t.Fatalf("the escalation latch was never stamped; nothing bounds the comment")
	}
}

// TestRetryExhaustionCommentsOnceEvenWhenTheReparkFails is what actually
// exercises the latch. In the happy path the repark changes the reason and the
// lane is never re-entered, so the latch is dead weight there; the window it
// exists for is a repark that FAILS, which leaves the Task in the lane at the
// cap and brings the next 30s pass straight back here.
func TestRetryExhaustionCommentsOnceEvenWhenTheReparkFails(t *testing.T) {
	proj := reProject()
	task := reExhaustedTask("t-exhausted-repark-fails", stage.ReasonCIFailed)
	c := newMirrorClientIntercepted(t, interceptor.Funcs{
		SubResourceUpdate: func(_ context.Context, _ client.Client, _ string,
			obj client.Object, _ ...client.SubResourceUpdateOption) error {
			if tk, ok := obj.(*tatarav1alpha1.Task); ok && tk.Name == "t-exhausted-repark-fails" {
				return errors.New("apiserver is having a moment")
			}
			return nil
		},
	}, proj, mdSecret(), mdRepo("repo-a"), mdIssue(task, "repo-a", 42), task)
	w := &mbWriter{}
	r := &ProjectReconciler{Client: c, APIReader: c, Scheme: c.Scheme(), Metrics: wfMetrics(),
		SCMFor: func(string) (scm.SCMWriter, error) { return w, nil }}

	for i := 0; i < 2; i++ {
		if err := r.driveUnparks(context.Background(), proj, time.Now()); err == nil {
			t.Fatalf("pass %d: driveUnparks swallowed the failed repark", i)
		}
	}
	if len(w.comments) != 1 {
		t.Fatalf("Comment calls = %d over two passes whose repark failed, want exactly 1", len(w.comments))
	}
	got := mdGetTask(t, c, task.Name)
	if got.Status.ParkReason != stage.ReasonCIFailed {
		t.Fatalf("parkReason = %q, want the blocker unchanged: the repark did not land", got.Status.ParkReason)
	}
}

// A forge outage must not cost the repark. The park is the correctness-critical
// half: without it the lane re-escalates every 30s forever, calling the forge
// each time.
func TestRetryExhaustionReparksEvenWhenTheCommentFails(t *testing.T) {
	proj := reProject()
	task := reExhaustedTask("t-exhausted-forgedown", stage.ReasonCIFailed)
	c := newMirrorClient(t, proj, mdSecret(), mdRepo("repo-a"), mdIssue(task, "repo-a", 42), task)
	w := &mbWriter{commentErr: context.DeadlineExceeded}
	r := &ProjectReconciler{Client: c, APIReader: c, Scheme: c.Scheme(), Metrics: wfMetrics(),
		SCMFor: func(string) (scm.SCMWriter, error) { return w, nil }}

	if err := r.driveUnparks(context.Background(), proj, time.Now()); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}
	got := mdGetTask(t, c, task.Name)
	if got.Status.ParkReason != stage.ReasonRetryExhausted {
		t.Fatalf("parkReason = %q, want retry-exhausted despite the failed comment", got.Status.ParkReason)
	}
	if got.Annotations[tatarav1alpha1.AnnRetryExhaustedCommented] != "" {
		t.Fatalf("the latch was stamped for a comment that never landed; the next pass can never retry it")
	}
}

// A Task owning no issue still escalates: the metric and the park are the parts
// that must not depend on there being somewhere to write.
func TestRetryExhaustionWithNoOwnedIssueStillReparks(t *testing.T) {
	proj := reProject()
	task := reExhaustedTask("t-exhausted-noissue", stage.ReasonMRSurfaceSpent)
	task.Status.IssueRefs = nil
	c := newMirrorClient(t, proj, mdSecret(), task)
	w := &mbWriter{}
	r := &ProjectReconciler{Client: c, APIReader: c, Scheme: c.Scheme(), Metrics: wfMetrics(),
		SCMFor: func(string) (scm.SCMWriter, error) { return w, nil }}

	if err := r.driveUnparks(context.Background(), proj, time.Now()); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}
	if len(w.comments) != 0 {
		t.Fatalf("Comment calls = %d, want 0 with no owned issue", len(w.comments))
	}
	if got := mdGetTask(t, c, task.Name); got.Status.ParkReason != stage.ReasonRetryExhausted {
		t.Fatalf("parkReason = %q, want retry-exhausted", got.Status.ParkReason)
	}
}

// TestRetryExhaustedIsReleasedByAHumanComment closes the loop: the escalation
// is only useful if answering it actually resumes the Task, and answering it
// hands the machine a fresh budget for whatever it hits next.
func TestRetryExhaustedIsReleasedByAHumanComment(t *testing.T) {
	proj := reProject()
	task := retryParkedTask("t-exhausted-answered", tatarav1alpha1.StateAwaitingReview, stage.ReasonRetryExhausted)
	task.Status.RetryAttempts = tatarav1alpha1.MaxUnparkRetries
	task.Status.IssueRefs = []string{tatarav1alpha1.IssueName("repo-a", 42)}
	task.Status.PendingEvents = []tatarav1alpha1.TaskEvent{{
		At: metav1.Now(), Kind: "issue_comment", Author: "human", Body: "rerun it",
	}}
	c := newMirrorClient(t, proj, mdSecret(), mdRepo("repo-a"), mdIssue(task, "repo-a", 42), task)
	r := &ProjectReconciler{Client: c, APIReader: c, Scheme: c.Scheme(), Metrics: wfMetrics()}

	if err := r.driveUnparks(context.Background(), proj, time.Now()); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}
	got := mdGetTask(t, c, task.Name)
	if tatarav1alpha1.Parked(got) {
		t.Fatalf("still parked(%s): retry-exhausted must behave like every other human wait", got.Status.ParkReason)
	}
	if got.Status.RetryAttempts != 0 {
		t.Fatalf("retryAttempts = %d, want 0: a human answer buys a fresh budget", got.Status.RetryAttempts)
	}
}
