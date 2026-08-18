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

// TestAFailedLatchDoesNotRePostTheEscalation. The latch write used to return
// early, BEFORE applyRetryExhaustion - so the Task kept its unarmed retry park
// at the cap, the next 30s pass re-entered, and it commented AGAIN. One
// transient apiserver error was a duplicate "tatara stopped retrying" on a
// human's issue; a persistent one was a comment every thirty seconds. The
// re-park alone suffices: retry-exhausted is UnparkHuman, so the lane never
// looks at this Task again.
func TestAFailedLatchDoesNotRePostTheEscalation(t *testing.T) {
	proj := reProject()
	task := reExhaustedTask("t-exhausted-latch-fails", stage.ReasonCIFailed)
	c := newMirrorClientIntercepted(t, interceptor.Funcs{
		Patch: func(_ context.Context, _ client.WithWatch, obj client.Object,
			_ client.Patch, _ ...client.PatchOption) error {
			if tk, ok := obj.(*tatarav1alpha1.Task); ok && tk.Name == "t-exhausted-latch-fails" {
				return errors.New("apiserver is having a moment")
			}
			return nil
		},
	}, proj, mdSecret(), mdRepo("repo-a"), mdIssue(task, "repo-a", 42), task)
	w := &mbWriter{}
	r := &ProjectReconciler{Client: c, APIReader: c, Scheme: c.Scheme(), Metrics: wfMetrics(),
		SCMFor: func(string) (scm.SCMWriter, error) { return w, nil }}

	for i := 0; i < 3; i++ {
		if err := r.driveUnparks(context.Background(), proj, time.Now()); err != nil {
			t.Fatalf("pass %d: driveUnparks: %v", i, err)
		}
	}
	if len(w.comments) != 1 {
		t.Fatalf("Comment calls = %d over three passes whose latch failed, want exactly 1", len(w.comments))
	}
	got := mdGetTask(t, c, task.Name)
	if got.Status.ParkReason != stage.ReasonRetryExhausted {
		t.Fatalf("parkReason = %q, want retry-exhausted: the latch is not allowed to block the re-park",
			got.Status.ParkReason)
	}
}

// TestAFailedIssueLookupPostponesTheEscalation. ("", false) used to mean both
// "this Task owns no issue" and "the lookup failed", and the caller re-parked
// either way - which moved the reason to retry-exhausted and made the
// escalation permanently silent on ONE transient apiserver error. That is
// exactly the silent park this file exists to prevent.
func TestAFailedIssueLookupPostponesTheEscalation(t *testing.T) {
	proj := reProject()
	task := reExhaustedTask("t-exhausted-lookup-fails", stage.ReasonCIFailed)
	fail := true
	c := newMirrorClientIntercepted(t, interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey,
			obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*tatarav1alpha1.Issue); ok && fail {
				return errors.New("etcd is having a moment")
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	}, proj, mdSecret(), mdRepo("repo-a"), mdIssue(task, "repo-a", 42), task)
	w := &mbWriter{}
	r := &ProjectReconciler{Client: c, APIReader: c, Scheme: c.Scheme(), Metrics: wfMetrics(),
		SCMFor: func(string) (scm.SCMWriter, error) { return w, nil }}

	if err := r.driveUnparks(context.Background(), proj, time.Now()); err == nil {
		t.Fatalf("driveUnparks swallowed the failed issue lookup")
	}
	got := mdGetTask(t, c, task.Name)
	if got.Status.ParkReason != stage.ReasonCIFailed {
		t.Fatalf("parkReason = %q, want the blocker unchanged: re-parking here loses the escalation forever",
			got.Status.ParkReason)
	}
	if len(w.comments) != 0 {
		t.Fatalf("Comment calls = %d, want 0: nothing was resolved to comment on", len(w.comments))
	}

	// The next pass, with the apiserver back, escalates for real.
	fail = false
	if err := r.driveUnparks(context.Background(), proj, time.Now()); err != nil {
		t.Fatalf("driveUnparks (recovered): %v", err)
	}
	if len(w.comments) != 1 {
		t.Fatalf("Comment calls = %d after recovery, want 1", len(w.comments))
	}
	if got := mdGetTask(t, c, task.Name); got.Status.ParkReason != stage.ReasonRetryExhausted {
		t.Fatalf("parkReason = %q, want retry-exhausted once the escalation landed", got.Status.ParkReason)
	}
}

// TestExhaustionEscalatesOnTheMergeRequestWhenThereIsNoIssue is the population
// the lane is FOR. Cron-minted upgrade Tasks and documentation batches own zero
// Issue CRs by construction - their deliverable IS the merge request - and they
// go through the merge corridor, which is where ci-failed and
// merge-conflict-retry are written. Issue-only, their exhaustion was silent on
// the forge and only the metric moved.
func TestExhaustionEscalatesOnTheMergeRequestWhenThereIsNoIssue(t *testing.T) {
	tests := []struct {
		provider string
		wantRef  string
	}{
		{"github", "szymonrychu/repo-a#7"},
		{"gitlab", "szymonrychu/repo-a!7"},
	}
	for _, tc := range tests {
		t.Run(tc.provider, func(t *testing.T) {
			proj := reProject()
			proj.Spec.Scm.Provider = tc.provider
			task := reExhaustedTask("t-exhausted-mr-only", stage.ReasonMergeConflictRetry)
			task.Status.IssueRefs = nil
			task.Status.MRRefs = []string{tatarav1alpha1.MergeRequestName("repo-a", 7)}
			c := newMirrorClient(t, proj, mdSecret(), mdRepo("repo-a"), mdMR(task, "repo-a", 7), task)
			w := &mbWriter{}
			r := &ProjectReconciler{Client: c, APIReader: c, Scheme: c.Scheme(), Metrics: wfMetrics(),
				SCMFor: func(string) (scm.SCMWriter, error) { return w, nil }}

			if err := r.driveUnparks(context.Background(), proj, time.Now()); err != nil {
				t.Fatalf("driveUnparks: %v", err)
			}
			if len(w.comments) != 1 {
				t.Fatalf("Comment calls = %d, want exactly 1 on the merge request", len(w.comments))
			}
			if w.comments[0].IssueRef != tc.wantRef {
				t.Fatalf("comment ref = %q, want %q", w.comments[0].IssueRef, tc.wantRef)
			}
			if got := mdGetTask(t, c, task.Name); got.Status.ParkReason != stage.ReasonRetryExhausted {
				t.Fatalf("parkReason = %q, want retry-exhausted", got.Status.ParkReason)
			}
		})
	}
}
