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

// C7: the OutcomeAccepted fingerprint must be CLAIMED atomically, before any
// forge/child-mint side effect, so two concurrent identical POSTs cannot both
// perform it. Goroutine A is parked inside CreateIssue - which the fix places
// AFTER the claim - so its claim has already landed. Goroutine B, fired while
// A is still parked, must be turned away as a duplicate WITHOUT calling
// CreateIssue itself: exactly one issue gets filed, exactly one clarify Task
// gets minted.
func TestOutcome_ConcurrentIdenticalPOSTsClaimFingerprintOnce(t *testing.T) {
	base := newRecordingForge()
	forge := &blockingCreateIssueForge{recordingForge: base, entered: make(chan struct{}), release: make(chan struct{})}
	e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StageBrainstorming, "brainstorm"))

	body := `{"kind":"brainstorm","payload":{"action":"propose","proposals":[` +
		`{"repo":"tatara-operator","title":"one","body":"b","kind":"bug"}]}}`

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
	require.Equal(t, http.StatusOK, wB.Code, "the loser observes the SAME idempotent no-op a sequential retry gets")
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
