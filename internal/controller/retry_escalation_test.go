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
//
// retryBlocker IS the reason, and that is not decoration. The counter is scoped
// to the blocker it was spent on (stage.ArmRetry, which is the only thing that
// increments it), so attempts=5 with an empty or foreign retryBlocker is a state
// no writer produces - and the exhaustion check now refuses to escalate on one,
// because inheriting another blocker's spend is what dragged a human in after
// zero laps on the blocker the comment named.
func reExhaustedTask(name, reason string) *tatarav1alpha1.Task {
	t := retryParkedTask(name, tatarav1alpha1.StateMerged, reason)
	t.Status.RetryAttempts = tatarav1alpha1.MaxUnparkRetries
	t.Status.RetryBlocker = reason
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

// TestADeliveredEscalationIsLatchedEvenWhenTheReparkFails is LOW 6, and it is
// the window swallowing the latch error does NOT close. Both writes go to the
// same object on the same apiserver, so their failures are CORRELATED: during a
// blip the latch write fails, the re-park fails too, the Task stays in the lane
// at the cap, and the next 30s pass posts the escalation comment again - which
// is the duplicate-comment storm the latch exists to prevent. Falling through is
// only safe when the re-park actually lands; when it does not, the latch for a
// comment that DID land has to be re-tried before returning.
func TestADeliveredEscalationIsLatchedEvenWhenTheReparkFails(t *testing.T) {
	proj := reProject()
	task := reExhaustedTask("t-exhausted-latch-and-repark-fail", stage.ReasonCIFailed)
	patches := 0
	c := newMirrorClientIntercepted(t, interceptor.Funcs{
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object,
			p client.Patch, opts ...client.PatchOption) error {
			if tk, ok := obj.(*tatarav1alpha1.Task); ok && tk.Name == "t-exhausted-latch-and-repark-fail" {
				patches++
				if patches == 1 {
					return errors.New("apiserver is having a moment")
				}
			}
			return cl.Patch(ctx, obj, p, opts...)
		},
		SubResourceUpdate: func(_ context.Context, _ client.Client, _ string,
			obj client.Object, _ ...client.SubResourceUpdateOption) error {
			if tk, ok := obj.(*tatarav1alpha1.Task); ok && tk.Name == "t-exhausted-latch-and-repark-fail" {
				return errors.New("apiserver is having a moment")
			}
			return nil
		},
	}, proj, mdSecret(), mdRepo("repo-a"), mdIssue(task, "repo-a", 42), task)
	w := &mbWriter{}
	r := &ProjectReconciler{Client: c, APIReader: c, Scheme: c.Scheme(), Metrics: wfMetrics(),
		SCMFor: func(string) (scm.SCMWriter, error) { return w, nil }}

	for i := 0; i < 3; i++ {
		if err := r.driveUnparks(context.Background(), proj, time.Now()); err == nil {
			t.Fatalf("pass %d: driveUnparks swallowed the failed repark", i)
		}
	}
	if len(w.comments) != 1 {
		t.Fatalf("Comment calls = %d over three passes, want exactly 1: a correlated apiserver blip "+
			"re-posts the escalation on every pass", len(w.comments))
	}
	got := mdGetTask(t, c, task.Name)
	if got.Annotations[tatarav1alpha1.AnnRetryExhaustedCommented] != parkStamp(got) {
		t.Fatalf("latch = %q, want the delivered comment latched at %q",
			got.Annotations[tatarav1alpha1.AnnRetryExhaustedCommented], parkStamp(got))
	}
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

// reStrandedTask is FINDING 1's shape: a Task the retry lane RELEASED (it
// carries the blocker and the laps it spent on it), whose replacement pods then
// spent the agent-stop re-arm cap, so reconcilePodStage re-parked it no-outcome
// from the live state the lane had put it back into.
//
// The merged merge request is not decoration either: ci-failed and
// merge-conflict-retry are written ONLY on the anyMerged arms of stage.CIRed and
// stage.MergeConflict, so EVERY Task in the lane has one by construction - which
// is exactly what makes Unpark's no-outcome arm decline merged-mr forever.
func reStrandedTask(name, blocker string, attempts int) *tatarav1alpha1.Task {
	t := retryParkedTask(name, tatarav1alpha1.StateAwaitingReview, stage.ReasonNoOutcome)
	t.Status.RetryBlocker = blocker
	t.Status.RetryAttempts = attempts
	t.Status.Stats.AgentStops = tatarav1alpha1.AgentStopReArmCap
	t.Status.IssueRefs = []string{tatarav1alpha1.IssueName("repo-a", 42)}
	t.Status.MRRefs = []string{tatarav1alpha1.MergeRequestName("repo-a", 7)}
	return t
}

// reMergedMR is mdMR already landed, which is the only state anyMerged reads.
func reMergedMR(task *tatarav1alpha1.Task, repo string, number int) *tatarav1alpha1.MergeRequest {
	mr := mdMR(task, repo, number)
	mr.Status.State = "merged"
	return mr
}

// TestALaneReleasedTaskCappedByTheAgentStopsIsNotSilent is FINDING 1. The lane's
// whole promise is "either it runs, or a human is told out loud", and the
// agent-stop re-arm cap is a hole in it: it parks no-outcome, whose own un-park
// arm declines merged-mr forever, whose class driveStrandedParks skips, and
// whose reason is no longer a retry reason - so nothing owns the Task, no
// comment reaches the issue, and it ages out at ParkRetention exactly as
// helmfile#27/#32 and terraform!221 did before the lane existed.
func TestALaneReleasedTaskCappedByTheAgentStopsIsNotSilent(t *testing.T) {
	proj := reProject()
	task := reStrandedTask("t-lane-stranded", stage.ReasonCIFailed, 2)
	c := newMirrorClient(t, proj, mdSecret(), mdRepo("repo-a"),
		reMergedMR(task, "repo-a", 7), mdIssue(task, "repo-a", 42), task)
	w := &mbWriter{}
	m := obs.NewOperatorMetrics(prometheus.NewRegistry())
	r := &ProjectReconciler{Client: c, APIReader: c, Scheme: c.Scheme(), Metrics: m,
		SCMFor: func(string) (scm.SCMWriter, error) { return w, nil }}

	if err := r.driveUnparks(context.Background(), proj, time.Now()); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}
	got := mdGetTask(t, c, task.Name)
	if got.Status.ParkReason != stage.ReasonRetryExhausted {
		t.Fatalf("parkReason = %q, want retry-exhausted: a lane that ends anywhere else ends with nobody owning it",
			got.Status.ParkReason)
	}
	if got.Status.State != tatarav1alpha1.StateAwaitingReview {
		t.Fatalf("state = %q, want unchanged awaiting-review: a repark never moves the Task", got.Status.State)
	}
	if len(w.comments) != 1 {
		t.Fatalf("Comment calls = %d, want exactly 1: the escalation is the lane's promise", len(w.comments))
	}
	if ref := w.comments[0].IssueRef; ref != "szymonrychu/repo-a#42" {
		t.Fatalf("comment issueRef = %q, want szymonrychu/repo-a#42", ref)
	}
	if body := w.comments[0].Body; !strings.Contains(body, stage.ReasonCIFailed) {
		t.Fatalf("the escalation does not name the blocker the lane was spending laps on:\n%s", body)
	}
	if n := testutil.ToFloat64(m.TaskRetryExhaustedCounter(stage.ReasonNoOutcome,
		tatarav1alpha1.StateAwaitingReview)); n != 1 {
		t.Fatalf("operator_task_retry_exhausted_total{reason=no-outcome,state=awaiting-review} = %v, want 1", n)
	}

	// The SECOND pass must add nothing: the repark moved the reason out of
	// no-outcome, so the arm never looks at this Task again.
	if err := r.driveUnparks(context.Background(), proj, time.Now()); err != nil {
		t.Fatalf("driveUnparks (second pass): %v", err)
	}
	if len(w.comments) != 1 {
		t.Fatalf("Comment calls after two passes = %d, want 1", len(w.comments))
	}
}

// TestANoOutcomeParkOutsideTheLaneIsLeftExactlyAsItWas is the other half, and it
// is the one the fix could easily get wrong: no-outcome is written by several
// unrelated paths (reconcileCaps, a pre-implement state that never terminated)
// and MOST of them never went near the retry lane. Escalating those would turn a
// timer park that the world can still release into a human wait.
func TestANoOutcomeParkOutsideTheLaneIsLeftExactlyAsItWas(t *testing.T) {
	tests := []struct {
		name     string
		blocker  string
		attempts int
	}{
		{"never entered the lane", "", 0},
		{"a blocker with no laps spent on it is not a lane", stage.ReasonCIFailed, 0},
		{"a laps count against a reason outside the lane", stage.ReasonAwaitingHuman, 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			proj := reProject()
			task := reStrandedTask("t-plain-no-outcome", tc.blocker, tc.attempts)
			c := newMirrorClient(t, proj, mdSecret(), mdRepo("repo-a"),
				reMergedMR(task, "repo-a", 7), mdIssue(task, "repo-a", 42), task)
			w := &mbWriter{}
			r := &ProjectReconciler{Client: c, APIReader: c, Scheme: c.Scheme(), Metrics: wfMetrics(),
				SCMFor: func(string) (scm.SCMWriter, error) { return w, nil }}

			if err := r.driveUnparks(context.Background(), proj, time.Now()); err != nil {
				t.Fatalf("driveUnparks: %v", err)
			}
			if got := mdGetTask(t, c, task.Name); got.Status.ParkReason != stage.ReasonNoOutcome {
				t.Fatalf("parkReason = %q, want no-outcome: this Task never entered the retry lane",
					got.Status.ParkReason)
			}
			if len(w.comments) != 0 {
				t.Fatalf("Comment calls = %d, want 0: escalating a park outside the lane is a false alarm",
					len(w.comments))
			}
		})
	}
}

// TestAReleasableNoOutcomeParkStillReleases: the escalation must be keyed on the
// REFUSAL, not on the lane marker alone. A lane-released Task whose merge
// requests have NOT landed is one the timer arm can still re-drive, and handing
// it to a human instead would end a recoverable Task early.
func TestAReleasableNoOutcomeParkStillReleases(t *testing.T) {
	proj := reProject()
	task := reStrandedTask("t-lane-releasable", stage.ReasonCIFailed, 2)
	c := newMirrorClient(t, proj, mdSecret(), mdRepo("repo-a"),
		mdMR(task, "repo-a", 7), mdIssue(task, "repo-a", 42), task)
	w := &mbWriter{}
	r := &ProjectReconciler{Client: c, APIReader: c, Scheme: c.Scheme(), Metrics: wfMetrics(),
		SCMFor: func(string) (scm.SCMWriter, error) { return w, nil }}

	if err := r.driveUnparks(context.Background(), proj, time.Now()); err != nil {
		t.Fatalf("driveUnparks: %v", err)
	}
	got := mdGetTask(t, c, task.Name)
	if tatarav1alpha1.Parked(got) {
		t.Fatalf("parkReason = %q, want released: nothing had merged, so the timer arm owns this park",
			got.Status.ParkReason)
	}
	if len(w.comments) != 0 {
		t.Fatalf("Comment calls = %d, want 0: a Task that just resumed has nothing to escalate", len(w.comments))
	}
}

// TestTheStrandedLaneCommentIsHonestAboutWhatStopped: the agent-stop cap is not
// the only writer of a no-outcome park on a lane-released Task - reconcileCaps
// writes the same reason for a pod that became Ready and then ended without an
// outcome, where stats.agentStops is 0. A body that says "the agent asked to
// stop 0 times" is the sentence that makes a human distrust the rest of it.
func TestTheStrandedLaneCommentIsHonestAboutWhatStopped(t *testing.T) {
	parked := time.Now().Add(-2 * time.Hour)
	capped := strandedLaneComment(stage.ReasonCIFailed, 2, tatarav1alpha1.AgentStopReArmCap, parked, time.Now())
	if !strings.Contains(capped, "asked to stop 3 times") {
		t.Fatalf("the capped body does not say what the agent did:\n%s", capped)
	}
	died := strandedLaneComment(stage.ReasonCIFailed, 2, 0, parked, time.Now())
	if strings.Contains(died, "stop 0 times") {
		t.Fatalf("the un-graceful body claims an agent-stop count that nobody recorded:\n%s", died)
	}
	if !strings.Contains(died, "the pod ended without submitting an outcome") {
		t.Fatalf("the un-graceful body does not say what stopped:\n%s", died)
	}
	for _, body := range []string{capped, died} {
		if !strings.Contains(body, stage.ReasonCIFailed) {
			t.Fatalf("the escalation does not name the blocker it was retrying:\n%s", body)
		}
		if strings.Contains(body, "still standing") {
			t.Fatalf("the blocker CLEARED - that is why the lane released the task:\n%s", body)
		}
	}
}
