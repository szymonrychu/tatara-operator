package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// PR B, GATE SITES 2 AND 3. Site 2 resolves the CI hold /outcome puts an
// accepted-but-unverified submission in; site 3 is the red-CI gate at
// awaiting-review, which used to fire only on edge.To == merged.

const ciWaitNow = "2026-08-10T12:00:00Z"

func ciWaitAt(t *testing.T) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, ciWaitNow)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return ts
}

// ciWaitReconciler is tsReconciler with a forge wired, which is what the gate
// needs: the MIRROR is only the trigger and the forge is the verdict.
func ciWaitReconciler(c client.Client, f *fakeForge) *TaskReconciler {
	return &TaskReconciler{
		Client:    c,
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Session:   panicSession{newFakeSession()},
		PodConfig: tsPodConfig(),
		SCMFor:    func(string) (scm.SCMWriter, error) { return f, nil },
	}
}

// ciWaitFixture is a Task HELD at under-implementation: its implement outcome
// was accepted, status.ciWaitSince is stamped, and the mirror carries mirrorCI
// while the forge reports liveCI at the same head.
func ciWaitFixture(t *testing.T, mirrorCI, liveCI string, heldFor time.Duration) (
	*tatarav1alpha1.Task, *fakeForge, client.Client) {
	t.Helper()
	now := ciWaitAt(t)
	task := mdTask("t1", "implement", tatarav1alpha1.StateUnderImplementation)
	entered := metav1.NewTime(now.Add(-heldFor - time.Minute))
	held := metav1.NewTime(now.Add(-heldFor))
	task.Status.StateEnteredAt = &entered
	task.Status.CIWaitSince = &held
	task.Status.AgentKind = "implement"
	mr := mdMR(task, "tatara-operator", 7)
	mr.Status.HeadSHA = "sha-pushed"
	mr.Status.CIStatus = mirrorCI
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := newFakeForge(t)
	f.head[7] = "sha-pushed"
	f.state[7] = scm.PRState{CIStatus: liveCI, HeadSHA: "sha-pushed"}
	return task, f, c
}

// GREEN RELEASES THE HOLD into the transition /outcome would have made.
func TestCIWaitGreenAdvancesToAwaitingReview(t *testing.T) {
	task, f, c := ciWaitFixture(t, scm.CIMirrorGreen, "success", 2*time.Minute)
	r := ciWaitReconciler(c, f)

	_, handled, err := r.reconcileClocks(context.Background(), mdProject(), task, ciWaitAt(t))
	if err != nil {
		t.Fatalf("reconcileClocks: %v", err)
	}
	if !handled {
		t.Fatalf("handled = false: a held task must never fall through to the other clocks")
	}
	got := mdGetTask(t, c, "t1")
	if got.Status.State != tatarav1alpha1.StateAwaitingReview {
		t.Fatalf("state = %q, want awaiting-review", got.Status.State)
	}
	if got.Status.CIWaitSince != nil {
		t.Fatalf("ciWaitSince still set: the hold must be cleared in the same write as the advance")
	}
}

// A MIRROR THAT STILL SAYS PENDING KEEPS HOLDING, and does NOT park. The webhook
// stamp and the 5m backstop are what move it, and both need the Task un-parked.
func TestCIWaitPendingKeepsHoldingWithoutParking(t *testing.T) {
	task, f, c := ciWaitFixture(t, scm.CIMirrorRunning, "pending", 2*time.Minute)
	r := ciWaitReconciler(c, f)

	res, handled, err := r.reconcileClocks(context.Background(), mdProject(), task, ciWaitAt(t))
	if err != nil {
		t.Fatalf("reconcileClocks: %v", err)
	}
	if !handled || res.RequeueAfter == 0 {
		t.Fatalf("handled=%v requeue=%v, want a handled poll", handled, res.RequeueAfter)
	}
	got := mdGetTask(t, c, "t1")
	if got.Status.State != tatarav1alpha1.StateUnderImplementation {
		t.Fatalf("state = %q, want it held at under-implementation", got.Status.State)
	}
	if got.Status.ParkReason != "" {
		t.Fatalf("parkReason = %q: the hold must NOT park - a parked task takes the reconciler's "+
			"early return and drops to the 24h mirror cadence, so nothing would ever clear it",
			got.Status.ParkReason)
	}
	if got.Status.CIWaitSince == nil {
		t.Fatalf("the hold was cleared with no verdict")
	}
}

// RED BOUNCES, and the bounce is the one that is easy to get wrong: the Task is
// ALREADY at under-implementation, so EnterStage takes its to==from silent-no-op
// path and drops the mutate with it. The counter, the cleared hold and the
// revoked acceptance must land anyway.
func TestCIWaitRedRevokesTheAcceptanceAndRunsTheAgentAgain(t *testing.T) {
	task, f, c := ciWaitFixture(t, scm.CIMirrorRed, "failure", 2*time.Minute)
	// The acceptance /outcome stamped. Without revoking it, an agent that fixes
	// CI and resubmits the SAME payload hashes to the same fingerprint and is
	// answered with a replay 200 that advances nothing.
	task.Status.Conditions = []metav1.Condition{{
		Type: tatarav1alpha1.ConditionOutcomeAccepted, Status: metav1.ConditionTrue,
		Reason: "Implement", Message: "fingerprint-abc",
		LastTransitionTime: metav1.NewTime(ciWaitAt(t).Add(-2 * time.Minute)),
	}}
	if err := c.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("seed condition: %v", err)
	}
	r := ciWaitReconciler(c, f)

	_, handled, err := r.reconcileClocks(context.Background(), mdProject(), task, ciWaitAt(t))
	if err != nil {
		t.Fatalf("reconcileClocks: %v", err)
	}
	if !handled {
		t.Fatalf("handled = false")
	}
	got := mdGetTask(t, c, "t1")
	if got.Status.State != tatarav1alpha1.StateUnderImplementation {
		t.Fatalf("state = %q, want under-implementation: the agent that can fix it runs again", got.Status.State)
	}
	if got.Status.CIWaitSince != nil {
		t.Fatalf("ciWaitSince still set after a red verdict: the hold would never end")
	}
	if got.Status.CIRedReentries != 1 {
		t.Fatalf("ciRedReentries = %d, want 1: the self-edge must not swallow the bound on the loop",
			got.Status.CIRedReentries)
	}
	if tatarav1alpha1.OutcomeCondition(got) != nil {
		t.Fatalf("the OutcomeAccepted condition survived: an identical resubmit would replay as a no-op")
	}
	note := ""
	for _, n := range got.Status.Notes {
		if n.Agent == "operator" && strings.Contains(n.Body, "CI is RED") {
			note = n.Body
		}
	}
	if note == "" {
		t.Fatalf("no ci-red operator note; notes = %+v", got.Status.Notes)
	}
	// The head was never reviewed, so the note must not tell the agent about a
	// review discussion that does not exist.
	if strings.Contains(note, "do not re-open the review discussion") {
		t.Fatalf("the pre-review note carries the reviewed-head prose: %q", note)
	}
	if !strings.Contains(note, "sha-pushed") {
		t.Fatalf("the note does not name the pushed head: %q", note)
	}
}

// THE HOLD IS NEVER A DEAD END. A forge that stops delivering CI events costs a
// bounded wait and then the pre-PR-B behaviour, not a stranded Task.
func TestCIWaitDeadlineFailsOpenIntoAwaitingReview(t *testing.T) {
	task, f, c := ciWaitFixture(t, scm.CIMirrorPending, "pending",
		tatarav1alpha1.CIWaitDeadline+time.Minute)
	r := ciWaitReconciler(c, f)

	if _, _, err := r.reconcileClocks(context.Background(), mdProject(), task, ciWaitAt(t)); err != nil {
		t.Fatalf("reconcileClocks: %v", err)
	}
	got := mdGetTask(t, c, "t1")
	if got.Status.State != tatarav1alpha1.StateAwaitingReview {
		t.Fatalf("state = %q, want awaiting-review at the deadline", got.Status.State)
	}
	if got.Status.CIWaitSince != nil {
		t.Fatalf("ciWaitSince survived the deadline")
	}
	if got.Status.ParkReason != "" {
		t.Fatalf("parkReason = %q: expiry is fail-open, never a park", got.Status.ParkReason)
	}
}

// A MIRROR THAT SAYS RED AND A FORGE THAT CANNOT BE READ KEEPS HOLDING. The
// destructive action is never taken on an unconfirmed reading, and the deadline
// still bounds it.
func TestCIWaitRedWithAnUnreadableForgeKeepsHolding(t *testing.T) {
	task, f, c := ciWaitFixture(t, scm.CIMirrorRed, "failure", 2*time.Minute)
	f.prStateErr = errors.New("502 bad gateway")
	r := ciWaitReconciler(c, f)

	if _, _, err := r.reconcileClocks(context.Background(), mdProject(), task, ciWaitAt(t)); err != nil {
		t.Fatalf("reconcileClocks: %v", err)
	}
	got := mdGetTask(t, c, "t1")
	if got.Status.State != tatarav1alpha1.StateUnderImplementation || got.Status.CIWaitSince == nil {
		t.Fatalf("state = %q ciWaitSince = %v, want it still held", got.Status.State, got.Status.CIWaitSince)
	}
}

// ciMirrorVerdict's fold, which is the CHEAP TRIGGER for both sites: it runs on
// every reconcile, so it must never cost a forge call and must never turn a repo
// with no CI at all into a hold that runs the full deadline.
func TestCIMirrorVerdictFold(t *testing.T) {
	mr := func(state, ci string) tatarav1alpha1.MergeRequest {
		return tatarav1alpha1.MergeRequest{
			Status: tatarav1alpha1.MergeRequestStatus{State: state, CIStatus: ci},
		}
	}
	tests := []struct {
		name string
		mrs  []tatarav1alpha1.MergeRequest
		want string
	}{
		{"no mrs", nil, ciVerdictClear},
		{"green", []tatarav1alpha1.MergeRequest{mr("open", scm.CIMirrorGreen)}, ciVerdictClear},
		{"none is not pending", []tatarav1alpha1.MergeRequest{mr("open", scm.CIMirrorNone)}, ciVerdictClear},
		{"unobserved is not pending", []tatarav1alpha1.MergeRequest{mr("open", "")}, ciVerdictClear},
		{"pending", []tatarav1alpha1.MergeRequest{mr("open", scm.CIMirrorPending)}, ciVerdictPending},
		{"running", []tatarav1alpha1.MergeRequest{mr("open", scm.CIMirrorRunning)}, ciVerdictPending},
		{"red", []tatarav1alpha1.MergeRequest{mr("open", scm.CIMirrorRed)}, ciVerdictRed},
		{
			"red beats pending across repos",
			[]tatarav1alpha1.MergeRequest{mr("open", scm.CIMirrorRunning), mr("open", scm.CIMirrorRed)},
			ciVerdictRed,
		},
		{
			"pending beats green across repos",
			[]tatarav1alpha1.MergeRequest{mr("open", scm.CIMirrorGreen), mr("open", scm.CIMirrorPending)},
			ciVerdictPending,
		},
		{
			"a merged sibling's ci can no longer block anything",
			[]tatarav1alpha1.MergeRequest{mr("merged", scm.CIMirrorRed), mr("open", scm.CIMirrorGreen)},
			ciVerdictClear,
		},
		{
			"a closed sibling's ci can no longer block anything",
			[]tatarav1alpha1.MergeRequest{mr("closed", scm.CIMirrorRed), mr("open", scm.CIMirrorGreen)},
			ciVerdictClear,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ciMirrorVerdict(tc.mrs); got != tc.want {
				t.Fatalf("ciMirrorVerdict = %q, want %q", got, tc.want)
			}
		})
	}
}

// --- GATE SITE 3: awaiting-review -----------------------------------------

// reviewRedFixture is a Task sitting at awaiting-review - a review pod has
// spawned and no verdict has come back - whose pipeline has since gone red.
func reviewRedFixture(t *testing.T, mirrorCI, liveCI string,
	mrOpts ...func(*tatarav1alpha1.MergeRequest)) (*tatarav1alpha1.Task, *fakeForge, client.Client) {
	t.Helper()
	now := ciWaitAt(t)
	task := mdTask("t1", "implement", tatarav1alpha1.StateAwaitingReview)
	entered := metav1.NewTime(now.Add(-10 * time.Minute))
	task.Status.StateEnteredAt = &entered
	task.Status.AgentKind = "review"
	mr := mdMR(task, "tatara-operator", 7)
	mr.Status.HeadSHA = "sha-pushed"
	mr.Status.CIStatus = mirrorCI
	for _, o := range mrOpts {
		o(mr)
	}
	c := newMirrorClient(t, mdProject(), mdSecret(), mdRepo("tatara-operator"), task, mr)

	f := newFakeForge(t)
	f.head[7] = "sha-pushed"
	f.state[7] = scm.PRState{CIStatus: liveCI, HeadSHA: "sha-pushed"}
	return task, f, c
}

// THE DEFECT PR B3 FIXES. The gate used to fire only on edge.To == merged, so a
// pipeline that went red while the review pod was reading the diff cost the whole
// round before anything noticed.
func TestAwaitingReviewBouncesWhenCIGoesRedUnderTheReviewPod(t *testing.T) {
	task, f, c := reviewRedFixture(t, scm.CIMirrorRed, "failure")
	r := ciWaitReconciler(c, f)

	_, handled, err := r.reconcileClocks(context.Background(), mdProject(), task, ciWaitAt(t))
	if err != nil {
		t.Fatalf("reconcileClocks: %v", err)
	}
	if !handled {
		t.Fatalf("handled = false: the red pipeline did not bounce the task")
	}
	got := mdGetTask(t, c, "t1")
	if got.Status.State != tatarav1alpha1.StateUnderImplementation ||
		got.Status.StateReason != stage.ReasonCIRed {
		t.Fatalf("state = %q(%q), want under-implementation(ci-red)", got.Status.State, got.Status.StateReason)
	}
	if got.Status.CIRedReentries != 1 {
		t.Fatalf("ciRedReentries = %d, want 1", got.Status.CIRedReentries)
	}
}

// B4: A TAKEN-OVER MR IS STILL GATED. A takeover flips status.ownership to
// "external" and leaves the mirror in place, so the fold and the live read both
// still see it. Ownership decides who may push and merge, never whether CI is
// read.
func TestAwaitingReviewBouncesATakenOverMRToo(t *testing.T) {
	task, f, c := reviewRedFixture(t, scm.CIMirrorRed, "failure",
		func(m *tatarav1alpha1.MergeRequest) {
			m.Status.Ownership = tatarav1alpha1.OwnershipExternal
			m.Status.OwnershipReason = "takeover-requested-by:alice"
		})
	r := ciWaitReconciler(c, f)

	if _, _, err := r.reconcileClocks(context.Background(), mdProject(), task, ciWaitAt(t)); err != nil {
		t.Fatalf("reconcileClocks: %v", err)
	}
	if got := mdGetTask(t, c, "t1"); got.Status.State != tatarav1alpha1.StateUnderImplementation {
		t.Fatalf("state = %q, want under-implementation: ownership does not exempt an MR from the CI gate",
			got.Status.State)
	}
}

// THE MIRROR IS THE TRIGGER, NOT THE VERDICT. A mirror that lags behind a forge
// that is green again must not bounce a Task, and - the point of the design - a
// mirror that is NOT red must not spend a forge call at all. This block runs on
// a 30s requeue for every reviewing Task in the fleet.
func TestAwaitingReviewConsultsTheForgeOnlyWhenTheMirrorSaysRed(t *testing.T) {
	tests := []struct {
		name     string
		mirrorCI string
		liveCI   string
		wantRead bool
		wantStg  string
	}{
		{"green mirror never reads the forge", scm.CIMirrorGreen, "failure", false, tatarav1alpha1.StateAwaitingReview},
		{"pending mirror never reads the forge", scm.CIMirrorRunning, "failure", false, tatarav1alpha1.StateAwaitingReview},
		{"red mirror reads and bounces", scm.CIMirrorRed, "failure", true, tatarav1alpha1.StateUnderImplementation},
		{"red mirror that the forge contradicts stays put", scm.CIMirrorRed, "success", true, tatarav1alpha1.StateAwaitingReview},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task, f, c := reviewRedFixture(t, tc.mirrorCI, tc.liveCI)
			r := ciWaitReconciler(c, f)
			if _, _, err := r.reconcileClocks(context.Background(), mdProject(), task, ciWaitAt(t)); err != nil {
				t.Fatalf("reconcileClocks: %v", err)
			}
			if got := (f.prStateCalls > 0); got != tc.wantRead {
				t.Fatalf("forge read = %v (calls %d), want %v", got, f.prStateCalls, tc.wantRead)
			}
			if got := mdGetTask(t, c, "t1"); got.Status.State != tc.wantStg {
				t.Fatalf("state = %q, want %q", got.Status.State, tc.wantStg)
			}
		})
	}
}

// IT FAILS OPEN, like every non-merging site: the merge corridor re-reads within
// 60s and is the gate that must hold.
func TestAwaitingReviewFailsOpenWhenTheForgeCannotBeRead(t *testing.T) {
	task, f, c := reviewRedFixture(t, scm.CIMirrorRed, "failure")
	f.prStateErr = errors.New("502 bad gateway")
	r := ciWaitReconciler(c, f)

	if _, _, err := r.reconcileClocks(context.Background(), mdProject(), task, ciWaitAt(t)); err != nil {
		t.Fatalf("reconcileClocks: %v", err)
	}
	if got := mdGetTask(t, c, "t1"); got.Status.State != tatarav1alpha1.StateAwaitingReview {
		t.Fatalf("state = %q, want awaiting-review: an unreadable forge must not bounce a task", got.Status.State)
	}
}
