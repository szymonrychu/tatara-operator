package restapi_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// blockingCreateIssueForge blocks the FIRST CreateIssue call until release is
// closed, and lets every other call through immediately. It is how the C7
// concurrency test proves the fingerprint claim happens BEFORE the
// forge/child-mint side effect: goroutine A is parked inside CreateIssue,
// which only runs after A's claim already landed, so goroutine B - issued
// while A is still parked - must observe the fingerprint already claimed.
type blockingCreateIssueForge struct {
	*recordingForge
	entered chan struct{}
	release chan struct{}
	blocked int32 // atomic: CAS, NOT sync.Once - Once serializes concurrent
	// callers, so a second (buggy-code) caller reaching CreateIssue while the
	// first is still parked would deadlock on Once's own internal lock
	// instead of proving the double side effect.
}

func (f *blockingCreateIssueForge) CreateIssue(ctx context.Context, repoURL, token string,
	req scm.IssueReq) (scm.CreatedIssue, error) {
	if atomic.CompareAndSwapInt32(&f.blocked, 0, 1) {
		close(f.entered)
		<-f.release
	}
	return f.recordingForge.CreateIssue(ctx, repoURL, token, req)
}

// doRaw is e.do without a *testing.T, so it is safe to call from a goroutine
// that is not the one running the test (require/t.Helper() are not).
func doRaw(e *v2Env, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	w := httptest.NewRecorder()
	e.r.ServeHTTP(w, req)
	return w
}

// brainstormProposeBody is the one body both tests below POST. It is a
// CreateIssue-driving outcome, which is what lets blockingCreateIssueForge park a
// handler in the window between its claim and its commit.
const brainstormProposeBody = `{"kind":"brainstorm","payload":{"action":"propose","proposals":[` +
	`{"repo":"tatara-operator","title":"one","body":"b","kind":"bug"}]}}`

// brainstormProposeFingerprint asks the server for the fingerprint of that body
// rather than duplicating the hash, by POSTing once against a throwaway env and
// reading the condition back. commit overwrites the claim's Reason but keeps its
// Message, so the fingerprint survives the commit.
func brainstormProposeFingerprint(t *testing.T) string {
	t.Helper()
	e := buildV2(t, v2Opts{writer: newRecordingForge()}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("fp", "tatara", "brainstorm", tatarav1alpha1.StageBrainstorming, "brainstorm"))
	w := e.do(t, http.MethodPost, "/tasks/fp/outcome", brainstormProposeBody)
	require.Equal(t, http.StatusOK, w.Code)
	cond := tatarav1alpha1.OutcomeCondition(e.task(t, "fp"))
	require.NotNil(t, cond)
	return cond.Message
}

// C7: the OutcomeAccepted fingerprint must be CLAIMED atomically, before any
// forge/child-mint side effect, so two concurrent identical POSTs cannot both
// perform it. Goroutine A is parked inside CreateIssue - which the fix places
// AFTER the claim - so its claim has already landed. Goroutine B, fired while
// A is still parked, must be turned away WITHOUT calling CreateIssue itself:
// exactly one issue gets filed, exactly one clarify Task gets minted.
//
// THIS TEST MUST FAIL IF THE CLAIM IS EVER MOVED AFTER VALIDATION. That ordering
// is not an accident to be tidied away: with a validate-then-claim handler,
// B reaches CreateIssue while A is parked in it and the platform files the issue
// twice, mints the clarify Task twice and double-increments ReviewRounds.
//
// B is answered 409 "outcome in flight, retry", NOT 200: A's claim is BARE
// (Reason "Outcome" - A has not committed, it is parked in the forge call), and
// a bare claim inside OutcomeClaimTTL is indistinguishable from an in-flight
// replica by design. 200 here would be a lie - nothing of B's request was done.
// The honest answer is "retry"; A's commit then makes the retry a real 200
// replay.
func TestOutcome_ConcurrentIdenticalPOSTsClaimFingerprintOnce(t *testing.T) {
	base := newRecordingForge()
	forge := &blockingCreateIssueForge{recordingForge: base, entered: make(chan struct{}), release: make(chan struct{})}
	e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StageBrainstorming, "brainstorm"))

	body := brainstormProposeBody

	var wg sync.WaitGroup
	var wA *httptest.ResponseRecorder
	wg.Add(1)
	go func() {
		defer wg.Done()
		wA = doRaw(e, http.MethodPost, "/tasks/t1/outcome", body)
	}()

	select {
	case <-forge.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("goroutine A never reached CreateIssue")
	}

	// A is parked inside CreateIssue; its claim already landed. B must be
	// turned away without reaching the forge at all.
	wB := e.do(t, http.MethodPost, "/tasks/t1/outcome", body)

	close(forge.release)
	wg.Wait()

	require.Equal(t, http.StatusOK, wA.Code, "the winner's request succeeds")
	require.Equal(t, http.StatusConflict, wB.Code,
		"the loser of the race sees A's BARE claim inside its TTL: the honest answer is 409 retry, not a 200 that did nothing")
	require.Len(t, forge.createdRefs, 1,
		"the fingerprint claim must admit exactly one side effect for two identical concurrent POSTs")

	var tasks tatarav1alpha1.TaskList
	require.NoError(t, e.c.List(context.Background(), &tasks, client.InNamespace(ns)))
	clarifies := 0
	for i := range tasks.Items {
		if tasks.Items[i].Spec.Kind == "clarify" {
			clarifies++
		}
	}
	require.Equal(t, 1, clarifies, "exactly one child clarify Task must be minted")
}

// THE RE-CLAIM MUST REFRESH THE LEASE'S CLOCK, and that refresh is only
// observable in the window between the re-claim and the commit - which is why
// this test parks a handler inside CreateIssue to look at it. Every other test of
// the orphan re-claim asserts on state that commit() overwrote, so the claim's own
// timestamp goes unobserved and the refresh could vanish without a single failure.
//
// A re-claim that keeps the ORPHANED stub's timestamp is a lease born already
// expired: the next identical retry, 1s later, re-reads a claim older than
// OutcomeClaimTTL, calls it orphaned in turn, re-claims it and runs every side
// effect a SECOND time - the issue filed twice, the child Task minted twice. That
// needs no second replica and no race; it is reachable on a SINGLE-version
// cluster with one operator pod, purely by retrying.
//
// The refresh works only because setCondition (outcome.go) is a WHOLE-STRUCT
// overwrite. meta.SetStatusCondition would leave LastTransitionTime untouched
// here, because it only re-stamps it when Status CHANGES and this one goes
// True -> True. See the warning on setCondition.
func TestOutcome_ReclaimOfAnOrphanedStubRefreshesTheLeaseClock(t *testing.T) {
	fp := brainstormProposeFingerprint(t)

	base := newRecordingForge()
	forge := &blockingCreateIssueForge{recordingForge: base, entered: make(chan struct{}), release: make(chan struct{})}
	e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StageBrainstorming, "brainstorm"))

	// An ORPHANED STUB: a bare claim on our own fingerprint, older than the TTL.
	stale := frozenNow.Add(-tatarav1alpha1.OutcomeClaimTTL - time.Second)
	outcomeClaimStub(t, e, "t1", fp, tatarav1alpha1.OutcomeReasonClaimed, stale)

	var wg sync.WaitGroup
	var w *httptest.ResponseRecorder
	wg.Add(1)
	go func() {
		defer wg.Done()
		w = doRaw(e, http.MethodPost, "/tasks/t1/outcome", brainstormProposeBody)
	}()

	select {
	case <-forge.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the request never reached CreateIssue: the orphaned stub must be RE-CLAIMED and processed, not replayed")
	}

	// Parked inside CreateIssue: the re-claim has landed and commit has not run,
	// so this is the claim's OWN condition, before anything overwrites it.
	cond := tatarav1alpha1.OutcomeCondition(e.task(t, "t1"))
	require.NotNil(t, cond, "the re-claim must leave a claim behind")
	require.Equal(t, tatarav1alpha1.OutcomeReasonClaimed, cond.Reason,
		"still BARE: we are parked before commit, so this is the re-claim's own condition")
	require.True(t, cond.LastTransitionTime.Time.Equal(frozenNow),
		"the re-claim must refresh the lease clock to NOW (got %s, want %s): keeping the orphan's %s stamp mints a lease "+
			"that is already expired, and the next retry re-claims it and duplicates every side effect",
		cond.LastTransitionTime.Time, frozenNow, stale)

	close(forge.release)
	wg.Wait()

	require.Equal(t, http.StatusOK, w.Code, "an orphaned stub must self-heal")
	require.Len(t, forge.createdRefs, 1, "exactly one issue for the one outcome that was actually processed")
}
