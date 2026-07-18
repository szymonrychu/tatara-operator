package restapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// reviewPanicForge answers the ONE read /outcome is allowed to make
// (GetPRHead) and PANICS on PostReview. THE /outcome HANDLER MAKES NO FORGE
// WRITE: for kind=review it does exactly two things - one read, then it
// persists the intent (mr.status.pendingReview). The MergeRequest RECONCILER
// posts it (Task 13). If this panics, /outcome posted a review and the whole
// crash-safety story (C.5.3) is void.
type reviewPanicForge struct {
	scm.SCMWriter
	heads map[int]string
}

func (f *reviewPanicForge) GetPRHead(_ context.Context, _, _ string, number int) (string, error) {
	sha, ok := f.heads[number]
	if !ok {
		return "", fmt.Errorf("no head for %d", number)
	}
	return sha, nil
}

func (f *reviewPanicForge) PostReview(_ context.Context, _, _ string, _ int, _ string,
	_ []scm.ReviewFinding) (string, error) {
	panic("BUG: /outcome posted a review to the forge. The MergeRequest reconciler posts it (C.5.3).")
}

func (f *reviewPanicForge) Merge(_ context.Context, _, _ string, _ int, _, _ string) (string, error) {
	panic("BUG: /outcome merged. Merge is the operator's, from the merging stage (C.5.2).")
}

// deadlineForge records the ctx deadline every CreateIssue call was handed, so
// the budget test can prove the bounded context is what actually reaches the
// forge - not the unbounded r.Context() the kind handlers pull for themselves.
type deadlineForge struct {
	*recordingForge
	deadlines []time.Time
	hadNone   bool
}

func (f *deadlineForge) CreateIssue(ctx context.Context, repoURL, token string,
	req scm.IssueReq) (scm.CreatedIssue, error) {
	if dl, ok := ctx.Deadline(); ok {
		f.deadlines = append(f.deadlines, dl)
	} else {
		f.hadNone = true
	}
	return f.recordingForge.CreateIssue(ctx, repoURL, token, req)
}

// THE LEASE IS ONLY SOUND IF A HANDLER CANNOT OUTLIVE ITS OWN CLAIM. The
// brainstorm path loops CreateIssue once per proposal and no http.Server in the
// request path sets a WriteTimeout, so three slow proposals could run past
// OutcomeClaimTTL - at which point an identical retry sees an "orphaned" stub
// that is still live, re-claims, and files every issue a SECOND time. postOutcome
// bounds the request context with OutcomeHandlerBudget at the TOP, before the
// claim, and OutcomeHandlerBudget < OutcomeClaimTTL, so that cannot happen. The
// bound must reach the FORGE calls, which is what this asserts.
func TestOutcome_HandlerContextIsBoundedByTheBudget(t *testing.T) {
	forge := &deadlineForge{recordingForge: newRecordingForge()}
	e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StageBrainstorming, "brainstorm"))

	before := time.Now()
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"brainstorm","payload":{"action":"propose","proposals":[`+
			`{"repo":"tatara-operator","title":"one","body":"b","kind":"bug"},`+
			`{"repo":"tatara-operator","title":"two","body":"b","kind":"bug"}]}}`)
	after := time.Now()
	require.Equal(t, http.StatusOK, w.Code)

	require.False(t, forge.hadNone, "a forge call ran on an UNBOUNDED context: the budget does not reach it")
	require.Len(t, forge.deadlines, 2, "every proposal's forge call must carry the deadline")
	// The deadline is anchored at the TOP of the handler, which is somewhere
	// between before and after, so the budget measured from either end brackets it.
	for _, dl := range forge.deadlines {
		require.False(t, dl.Before(before.Add(tatarav1alpha1.OutcomeHandlerBudget)),
			"the deadline must be at least the budget away: it is anchored at the top of the handler")
		require.False(t, dl.After(after.Add(tatarav1alpha1.OutcomeHandlerBudget)),
			"the deadline must be no more than the budget away: some LONGER bound reached the forge instead")
	}
	require.Less(t, tatarav1alpha1.OutcomeHandlerBudget, tatarav1alpha1.OutcomeClaimTTL,
		"a handler that can outlive its own lease lets an identical retry duplicate every side effect")
}

// --- kind gate + idempotency ----------------------------------------------

func TestOutcome_KindMustEqualAgentKind(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageClarifying, "clarify"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"declined","reason":"nope"}}`)
	require.Equal(t, http.StatusConflict, w.Code, "the pod's claim is not trusted")
	require.Equal(t, tatarav1alpha1.StageClarifying, e.task(t, "t1").Status.Stage)
}

func TestOutcome_TerminalStageIs409(t *testing.T) {
	for _, stg := range []string{
		tatarav1alpha1.StageRejected, tatarav1alpha1.StageFailed,
		tatarav1alpha1.StageParked, tatarav1alpha1.StageDelivered,
	} {
		t.Run(stg, func(t *testing.T) {
			e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
				repoV2("tatara-operator", "tatara"),
				taskV2("t1", "tatara", "clarify", stg, "clarify"))
			w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
				`{"kind":"clarify","payload":{"decision":"discuss","reason":"r"}}`)
			require.Equal(t, http.StatusConflict, w.Code)
		})
	}
}

// IDEMPOTENT: a repeat of an IDENTICAL outcome for the same
// (task, agentKind, stage) returns 200 with the unchanged Task. A TTL-stopped
// pod's retry must not 409 the Task into failure.
func TestOutcome_IdenticalRepeatIs200NotA409(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StageBrainstorming, "brainstorm"))

	body := `{"kind":"brainstorm","payload":{"action":"skip","reason":"nothing novel this cycle"}}`
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", body)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, tatarav1alpha1.StageDelivered, e.task(t, "t1").Status.Stage)

	// The Task is now DELIVERED. Without the idempotency record this replay
	// would 409 on the terminal-stage gate.
	w = e.do(t, http.MethodPost, "/tasks/t1/outcome", body)
	require.Equal(t, http.StatusOK, w.Code, "a TTL-stopped pod's retry must not 409")
	require.Equal(t, tatarav1alpha1.StageDelivered, e.task(t, "t1").Status.Stage)

	// A DIFFERENT outcome on a terminal Task is still refused.
	w = e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"brainstorm","payload":{"action":"skip","reason":"a different reason"}}`)
	require.Equal(t, http.StatusConflict, w.Code)
}

// --- implement: MERGE ORDER RESOLUTION ------------------------------------

// THE SINGLE-REPO CASE. In v3 this Task could NEVER merge: mergeOrder was nil,
// C.5.2's `for i := mergeCursor; i < len(spec.mergeOrder)` ran ZERO times, and
// delivered was unreachable.
func TestOutcome_Implement_SingleRepoMergeOrderIsResolved(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StageImplementing, "implement"),
		mrV2("tatara-operator", 295, "t1"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"submitted","title":"T","body":"B","changeSignificance":"patch"}}`)
	require.Equal(t, http.StatusOK, w.Code)

	got := e.task(t, "t1")
	require.Equal(t, []string{"tatara-operator"}, got.Spec.MergeOrder,
		"with one repo there is exactly one order and nothing to get wrong")
	require.Equal(t, tatarav1alpha1.StageReviewing, got.Status.Stage)
	require.Equal(t, "patch", e.mr(t, tatarav1alpha1.MergeRequestName("tatara-operator", 295)).Status.Significance)
}

// MORE THAN ONE REPO: mergeOrder is REQUIRED. There is NO LEXICAL DEFAULT -
// lexical order merges cli BEFORE operator, precisely the DisallowUnknownFields
// fleet outage this redesign exists to prevent.
func TestOutcome_Implement_MultiRepoRequiresMergeOrder(t *testing.T) {
	objs := []client.Object{
		projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"), repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StageImplementing, "implement"),
		mrV2("tatara-operator", 295, "t1"), mrV2("tatara-cli", 80, "t1"),
	}
	e := buildV2(t, v2Opts{writer: panicForge{}}, objs...)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"submitted","title":"T","body":"B","changeSignificance":"minor"}}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "mergeOrder required for a multi-repo change")
	require.Empty(t, e.task(t, "t1").Spec.MergeOrder)

	// A mergeOrder that omits an owned MR's repo is a 400, naming the repo.
	e2 := buildV2(t, v2Opts{writer: panicForge{}}, objs...)
	w = e2.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"submitted","title":"T","body":"B","changeSignificance":"minor","mergeOrder":["tatara-cli"]}}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "mergeOrder does not cover repo tatara-operator")

	// The correct, dependency-ordered answer: operator FIRST, then cli.
	e3 := buildV2(t, v2Opts{writer: panicForge{}}, objs...)
	w = e3.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"submitted","title":"T","body":"B","changeSignificance":"minor","mergeOrder":["tatara-operator","tatara-cli"]}}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, []string{"tatara-operator", "tatara-cli"}, e3.task(t, "t1").Spec.MergeOrder)
}

func TestOutcome_Implement_SubmittedWithNoOpenMRIs400(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StageImplementing, "implement"))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"submitted","title":"T","body":"B","changeSignificance":"patch"}}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "action=submitted but this task owns no open MR")
}

func TestOutcome_Implement_Declined(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StageImplementing, "implement"))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"declined","reason":"the issue is already fixed"}}`)
	require.Equal(t, http.StatusOK, w.Code)
	got := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.StageParked, got.Status.Stage)
	require.Equal(t, "implement-declined", got.Status.StageReason)
}

func TestOutcome_Implement_SubmittedForbidsReason(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StageImplementing, "implement"),
		mrV2("tatara-operator", 295, "t1"))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"submitted","title":"T","body":"B","changeSignificance":"patch","reason":"r"}}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

// THE UNLABELLED-PR WEDGE, CLOSED AT THE FRONT DOOR. changeSignificance is what
// the operator projects onto the PR's semver:<level> label, and CI cuts the
// release tag FROM THAT LABEL (contract H.4). An outcome without one would open
// a PR that merges, publishes nothing, propagates no pin, and leaves the Task in
// deploying until the budget parks it. So it is a 400: the outcome is REFUSED,
// the Task does NOT leave implementing, and the agent re-submits with a level.
func TestOutcome_Implement_SubmittedRequiresChangeSignificance(t *testing.T) {
	for _, tc := range []struct{ name, payload string }{
		{"absent", `{"action":"submitted","title":"T","body":"B"}`},
		{"empty", `{"action":"submitted","title":"T","body":"B","changeSignificance":"  "}`},
		{"out of enum", `{"action":"submitted","title":"T","body":"B","changeSignificance":"breaking"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
				repoV2("tatara-operator", "tatara"),
				taskV2("t1", "tatara", "implement", tatarav1alpha1.StageImplementing, "implement"),
				mrV2("tatara-operator", 295, "t1"))
			w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
				`{"kind":"implement","payload":`+tc.payload+`}`)
			require.Equal(t, http.StatusBadRequest, w.Code)
			require.Equal(t, tatarav1alpha1.StageImplementing, e.task(t, "t1").Status.Stage)
			require.Empty(t, e.mr(t, tatarav1alpha1.MergeRequestName("tatara-operator", 295)).Status.Significance)
		})
	}
}

// --- review ----------------------------------------------------------------

// THE ZERO-FORGE-WRITE TEST. The forge's PostReview PANICS; the handler must
// never reach it. It makes ONE read (GetPRHead) and PERSISTS THE INTENT.
func TestOutcome_Review_PersistsIntentAndNeverPostsToTheForge(t *testing.T) {
	forge := &reviewPanicForge{heads: map[int]string{295: "sha1"}}
	e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StageReviewing, "review"),
		mrV2("tatara-operator", 295, "t1"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"review","payload":{
	  "verdict":"request_changes",
	  "reviewedSHAs":[{"repo":"tatara-operator","number":295,"sha":"sha1"}],
	  "findings":[{"repo":"tatara-operator","number":295,"path":"internal/x.go","line":42,
	               "body":"this races","severity":"critical"}]}}`)
	require.Equal(t, http.StatusOK, w.Code)

	mr := e.mr(t, tatarav1alpha1.MergeRequestName("tatara-operator", 295))
	require.NotNil(t, mr.Status.PendingReview, "the INTENT is persisted; the reconciler posts it")
	require.Equal(t, "## Review: changes requested", mr.Status.PendingReview.Body)
	require.Equal(t, "sha1", mr.Status.PendingReview.SHA)
	require.Equal(t, 1, mr.Status.PendingReview.Round, "round is the idempotency key: reviewRounds + 1")
	require.Len(t, mr.Status.PendingReview.Findings, 1)
	require.Equal(t, "critical", mr.Status.PendingReview.Findings[0].Severity)
	require.Equal(t, "needs-changes", mr.Status.Status)
	require.Equal(t, 1, mr.Status.ReviewRounds)
	require.Equal(t, "sha1", mr.Status.ReviewedSHA, "the SHA the AGENT read, verified still live")

	// The stage does NOT advance here: reviewing -> implementing is gated on
	// every owned MR having pendingReview == nil (C.5.3). The reconciler posts
	// the review, clears the intent, and only then may a pod be spawned to fix
	// findings that have actually been recorded.
	require.Equal(t, tatarav1alpha1.StageReviewing, e.task(t, "t1").Status.Stage)
}

func TestOutcome_Review_ApprovePersistsApprovedStatus(t *testing.T) {
	forge := &reviewPanicForge{heads: map[int]string{295: "sha1"}}
	e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StageReviewing, "review"),
		mrV2("tatara-operator", 295, "t1"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"review","payload":{
	  "verdict":"approve","reviewedSHAs":[{"repo":"tatara-operator","number":295,"sha":"sha1"}]}}`)
	require.Equal(t, http.StatusOK, w.Code)

	mr := e.mr(t, tatarav1alpha1.MergeRequestName("tatara-operator", 295))
	require.Equal(t, "approved", mr.Status.Status)
	require.Equal(t, 0, mr.Status.ReviewRounds, "reviewRounds counts ACCEPTED request_changes only")
	require.Equal(t, "## Review: approved", mr.Status.PendingReview.Body)
}

// emptyCommentReader is an SCMReader whose thread reads return no comments, so a
// head-moved on-demand resync can run in-process without a live forge.
type emptyCommentReader struct{ scm.SCMReader }

func (emptyCommentReader) ListIssueComments(_ context.Context, _, _ string, _ int) ([]scm.IssueComment, error) {
	return nil, nil
}

// THE HEAD-MOVED SELF-HEAL. The operator re-reads the LIVE head, refuses a
// verdict whose reported SHA moved, REFRESHES the MR mirror to the live head on
// demand, and returns a STRUCTURED reason="head-moved" body (the cross-repo
// contract the cli keys on) - never the old plain-text "head moved" string.
// reviewedSHA/pendingReview stay unstamped; the mirror's headSHA advances.
func TestOutcome_Review_HeadMovedRefreshesMirrorAndReturnsStructured409(t *testing.T) {
	forge := &reviewPanicForge{heads: map[int]string{295: "sha-NEW"}}
	reg := prometheus.NewRegistry()
	metrics := obs.NewOperatorMetrics(reg)
	e := buildV2(t, v2Opts{writer: forge, reader: emptyCommentReader{}, metrics: metrics},
		projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StageReviewing, "review"),
		mrV2("tatara-operator", 295, "t1"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"review","payload":{
	  "verdict":"approve","reviewedSHAs":[{"repo":"tatara-operator","number":295,"sha":"sha-OLD"}]}}`)
	require.Equal(t, http.StatusConflict, w.Code)
	require.NotContains(t, w.Body.String(), "head moved since you reviewed it",
		"the old plain-text body must be gone")

	var resp struct {
		Reason          string `json:"reason"`
		Repo            string `json:"repo"`
		Number          int    `json:"number"`
		ReviewedSHA     string `json:"reviewedSHA"`
		LiveSHA         string `json:"liveSHA"`
		MirrorRefreshed bool   `json:"mirrorRefreshed"`
		Message         string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Equal(t, "head-moved", resp.Reason)
	require.Equal(t, "tatara-operator", resp.Repo)
	require.Equal(t, 295, resp.Number)
	require.Equal(t, "sha-OLD", resp.ReviewedSHA)
	require.Equal(t, "sha-NEW", resp.LiveSHA)
	require.True(t, resp.MirrorRefreshed)
	require.Contains(t, resp.Message, "git fetch && git checkout sha-NEW")

	mr := e.mr(t, tatarav1alpha1.MergeRequestName("tatara-operator", 295))
	require.Nil(t, mr.Status.PendingReview, "the stale review is NOT persisted")
	require.Empty(t, mr.Status.ReviewedSHA, "reviewedSHA is NOT stamped on a stale head")
	require.Equal(t, "sha-NEW", mr.Status.HeadSHA, "the mirror is pulled to the live head")

	require.Equal(t, float64(1), testutil.ToFloat64(metrics.ReviewHeadMovedCounter("tatara-operator")),
		"operator_review_head_moved_total{repo} must record the stuck-head signal")
}

// COVERAGE IS TOTAL. A reviewedSHAs that omits an owned MR is a 400, NOT
// "unreviewed but fine": a multi-repo Task is exactly where a review agent is
// most likely to read three MRs and report two.
func TestOutcome_Review_CoverageIsTotal(t *testing.T) {
	forge := &reviewPanicForge{heads: map[int]string{295: "sha1", 80: "sha2"}}
	e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"), repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StageReviewing, "review"),
		mrV2("tatara-operator", 295, "t1"), mrV2("tatara-cli", 80, "t1"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"review","payload":{
	  "verdict":"approve","reviewedSHAs":[{"repo":"tatara-operator","number":295,"sha":"sha1"}]}}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "reviewed_shas does not cover tatara-cli!80")
	require.Nil(t, e.mr(t, tatarav1alpha1.MergeRequestName("tatara-operator", 295)).Status.PendingReview)
}

func TestOutcome_Review_ReviewedSHAsIsRequired(t *testing.T) {
	forge := &reviewPanicForge{heads: map[int]string{295: "sha1"}}
	e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StageReviewing, "review"),
		mrV2("tatara-operator", 295, "t1"))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"review","payload":{"verdict":"approve","reviewedSHAs":[]}}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "reviewedSHAs is required")
}

func TestOutcome_Review_RequestChangesNeedsFindings(t *testing.T) {
	forge := &reviewPanicForge{heads: map[int]string{295: "sha1"}}
	e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StageReviewing, "review"),
		mrV2("tatara-operator", 295, "t1"))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"review","payload":{
	  "verdict":"request_changes","reviewedSHAs":[{"repo":"tatara-operator","number":295,"sha":"sha1"}]}}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

// changeSignificance is IMPLEMENT-OWNED. A review may only ESCALATE it; a LOWER
// value is IGNORED. The in-cluster reviewer is documented-flaky and must never
// downgrade a major release to a patch.
func TestOutcome_Review_ChangeSignificanceEscalatesOnly(t *testing.T) {
	for _, tc := range []struct{ implement, review, want string }{
		{"patch", "major", "major"},
		{"major", "patch", "major"},
		{"minor", "minor", "minor"},
		{"minor", "major", "major"},
		{"major", "minor", "major"},
	} {
		t.Run(tc.implement+"_then_"+tc.review, func(t *testing.T) {
			forge := &reviewPanicForge{heads: map[int]string{295: "sha1"}}
			mr := mrV2("tatara-operator", 295, "t1", func(m *tatarav1alpha1.MergeRequest) {
				m.Status.Significance = tc.implement
			})
			e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(),
				repoV2("tatara-operator", "tatara"),
				taskV2("t1", "tatara", "implement", tatarav1alpha1.StageReviewing, "review"), mr)

			w := e.do(t, http.MethodPost, "/tasks/t1/outcome", fmt.Sprintf(`{"kind":"review","payload":{
			  "verdict":"approve","changeSignificance":%q,
			  "reviewedSHAs":[{"repo":"tatara-operator","number":295,"sha":"sha1"}]}}`, tc.review))
			require.Equal(t, http.StatusOK, w.Code)
			require.Equal(t, tc.want,
				e.mr(t, tatarav1alpha1.MergeRequestName("tatara-operator", 295)).Status.Significance)
		})
	}
}

// I2: RecordReviewOutcome must be WIRED into the review-verdict path, and
// its "request_changes" -> "changes_requested" label must match what
// tatara-quality.yaml's rubber-stamp alert selects
// (operator_review_outcome_total{verdict="changes_requested"}) - the REST
// payload's own verdict vocabulary ("approve"/"request_changes") is NOT the
// metric's label vocabulary ("approved"/"changes_requested").
func TestOutcome_Review_RecordsReviewOutcomeMetric(t *testing.T) {
	for _, tc := range []struct {
		payloadVerdict string
		wantLabel      string
		extraPayload   string
	}{
		{"approve", "approved", ""},
		{"request_changes", "changes_requested",
			`,"findings":[{"repo":"tatara-operator","number":295,"body":"x","severity":"critical"}]`},
	} {
		t.Run(tc.payloadVerdict, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			metrics := obs.NewOperatorMetrics(reg)
			forge := &reviewPanicForge{heads: map[int]string{295: "sha1"}}
			e := buildV2(t, v2Opts{writer: forge, metrics: metrics}, projectV2("tatara"), scmSecretV2(),
				repoV2("tatara-operator", "tatara"),
				taskV2("t1", "tatara", "implement", tatarav1alpha1.StageReviewing, "review"),
				mrV2("tatara-operator", 295, "t1"))

			w := e.do(t, http.MethodPost, "/tasks/t1/outcome", fmt.Sprintf(`{"kind":"review","payload":{
			  "verdict":%q,
			  "reviewedSHAs":[{"repo":"tatara-operator","number":295,"sha":"sha1"}]%s}}`,
				tc.payloadVerdict, tc.extraPayload))
			require.Equal(t, http.StatusOK, w.Code)

			got := testutil.ToFloat64(metrics.ReviewOutcomeCounter("tatara", "tatara-operator", "m", tc.wantLabel))
			require.Equal(t, float64(1), got,
				"operator_review_outcome_total{verdict=%q} must record the review", tc.wantLabel)
		})
	}
}

// --- clarify ---------------------------------------------------------------

// Approval is in NO schema. The agent reports decision=implement with a reason;
// the operator INDEPENDENTLY verifies the C.6 grammar over EVERY owned Issue.
func TestOutcome_Clarify_ImplementRequiresApprovalOnEveryOwnedIssue(t *testing.T) {
	i1 := issueV2("tatara-operator", 291, "t1")
	i2 := issueV2("tatara-operator", 292, "t1")

	// Only ONE of the two issues is approved: the SCOPE gate refuses (fix H9).
	e := buildV2(t, v2Opts{
		writer:   panicForge{},
		approval: &fakeApproval{grant: map[string]bool{i1.Name: true}},
	}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageClarifying, "clarify"), i1, i2)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"clarify","payload":{"decision":"implement","reason":"maintainer said go ahead"}}`)
	require.Equal(t, http.StatusOK, w.Code)
	got := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.StageParked, got.Status.Stage)
	require.Equal(t, "identity-unverified", got.Status.StageReason)
	require.Empty(t, e.issue(t, i1.Name).Status.Approval, "nothing is stamped when the scope gate fails")

	// BOTH approved: the mandate is granted.
	e2 := buildV2(t, v2Opts{
		writer:   panicForge{},
		approval: &fakeApproval{grant: map[string]bool{i1.Name: true, i2.Name: true}},
	}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageClarifying, "clarify"),
		issueV2("tatara-operator", 291, "t1"), issueV2("tatara-operator", 292, "t1"))

	w = e2.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"clarify","payload":{"decision":"implement","reason":"maintainer said go ahead"}}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, tatarav1alpha1.StageApproved, e2.task(t, "t1").Status.Stage)
	require.Equal(t, "approved", e2.issue(t, i1.Name).Status.Status)
	require.NotNil(t, e2.issue(t, i1.Name).Status.Approval)
}

// A nil verifier FAILS CLOSED.
func TestOutcome_Clarify_NoVerifierFailsClosed(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageClarifying, "clarify"),
		issueV2("tatara-operator", 291, "t1"))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"clarify","payload":{"decision":"implement","reason":"trust me"}}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "identity-unverified", e.task(t, "t1").Status.StageReason)
}

func TestOutcome_Clarify_DiscussParksAwaitingHuman(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageClarifying, "clarify"))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"clarify","payload":{"decision":"discuss","reason":"needs a human"}}`)
	require.Equal(t, http.StatusOK, w.Code)
	got := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.StageParked, got.Status.Stage)
	require.Equal(t, "awaiting-human", got.Status.StageReason)
}

func TestOutcome_Clarify_CloseRejectsAndQueuesTheIssueClose(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageClarifying, "clarify"),
		issueV2("tatara-operator", 291, "t1"))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"clarify","payload":{"decision":"close","reason":"wont fix"}}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, tatarav1alpha1.StageRejected, e.task(t, "t1").Status.Stage)
	require.Len(t, e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 291)).Status.PendingComments, 1)
}

func TestOutcome_Clarify_ReasonAlwaysRequired(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageClarifying, "clarify"))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"clarify","payload":{"decision":"discuss"}}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

// --- brainstorm -----------------------------------------------------------

// Each proposal becomes its OWN new clarify Task, owning its OWN Issue.
func TestOutcome_Brainstorm_ProposeSpawnsAClarifyTaskPerProposal(t *testing.T) {
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StageBrainstorming, "brainstorm"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"brainstorm","payload":{
	  "action":"propose","proposals":[
	    {"repo":"tatara-operator","title":"one","body":"b","kind":"bug"},
	    {"repo":"tatara-operator","title":"two","body":"b","kind":"improvement"}]}}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, tatarav1alpha1.StageDelivered, e.task(t, "t1").Status.Stage)
	require.Empty(t, e.task(t, "t1").Status.DocumentedBy,
		"a brainstorm never spawns a docs task (fix 25)")

	var tasks tatarav1alpha1.TaskList
	require.NoError(t, e.c.List(context.Background(), &tasks, client.InNamespace(ns)))
	clarifies := 0
	for i := range tasks.Items {
		if tasks.Items[i].Spec.Kind == "clarify" {
			clarifies++
		}
	}
	require.Equal(t, 2, clarifies)
	require.Len(t, e.forge.createdRefs, 2)

	// Each new clarify Task controller-owns its own Issue.
	var issues tatarav1alpha1.IssueList
	require.NoError(t, e.c.List(context.Background(), &issues, client.InNamespace(ns)))
	require.Len(t, issues.Items, 2)
	for i := range issues.Items {
		require.True(t, *issues.Items[i].OwnerReferences[0].Controller)
		require.NotEqual(t, "t1", issues.Items[i].OwnerReferences[0].Name)
	}
}

// The brainstorm propose path stamps the tatara-proposed-by:brainstorm marker on
// BOTH the forge issue body and the minted Issue CR body - the marker factor of
// the autoApproveTataraProposals carve-out, durable across a mirror refresh.
func TestOutcome_Brainstorm_StampsProposalMarker(t *testing.T) {
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StageBrainstorming, "brainstorm"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"brainstorm","payload":{
	  "action":"propose","proposals":[
	    {"repo":"tatara-operator","title":"one","body":"do the thing","kind":"bug"}]}}`)
	require.Equal(t, http.StatusOK, w.Code)

	require.Len(t, e.forge.createdReqs, 1)
	require.Equal(t, tatarav1alpha1.ProposalKindBrainstorm,
		tatarav1alpha1.ProposalKindFromBody(e.forge.createdReqs[0].Body),
		"forge issue body must carry the brainstorm proposal marker")

	var issues tatarav1alpha1.IssueList
	require.NoError(t, e.c.List(context.Background(), &issues, client.InNamespace(ns)))
	require.Len(t, issues.Items, 1)
	require.Equal(t, tatarav1alpha1.ProposalKindBrainstorm,
		tatarav1alpha1.ProposalKindFromBody(issues.Items[0].Status.Body),
		"Issue CR body must carry the brainstorm proposal marker")
}

func TestOutcome_Brainstorm_ProposalsAreCappedAt5(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StageBrainstorming, "brainstorm"))
	p := `{"repo":"tatara-operator","title":"x","body":"b","kind":"bug"}`
	body := `{"kind":"brainstorm","payload":{"action":"propose","proposals":[` +
		p + "," + p + "," + p + "," + p + "," + p + "," + p + `]}}`
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", body)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

// --- incident -------------------------------------------------------------

func TestOutcome_Incident_FileIssueCreatesTheTrackerUnderThisTask(t *testing.T) {
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StageInvestigating, "incident"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"incident","payload":{
	  "action":"file_issue","alertRules":["tatara-operator-down"],"reason":"real outage",
	  "issue":{"repo":"tatara-operator","title":"operator down","body":"trace"}}}`)
	require.Equal(t, http.StatusOK, w.Code)

	got := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.StageClarifying, got.Status.Stage)
	require.Equal(t, []string{"tatara-operator-down"}, got.Spec.AlertRules,
		"alertRules are merged into spec by the OPERATOR; spec is agent-unwritable")

	iss := e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 101))
	require.Equal(t, "t1", iss.OwnerReferences[0].Name)
	require.True(t, *iss.OwnerReferences[0].Controller)
}

// The incident file_issue path stamps the tatara-proposed-by:incident marker on
// BOTH the forge issue body and the minted Issue CR body (the carve-out's marker
// factor for alert-driven incident issues).
func TestOutcome_Incident_StampsProposalMarker(t *testing.T) {
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StageInvestigating, "incident"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"incident","payload":{
	  "action":"file_issue","alertRules":["tatara-operator-down"],"reason":"real outage",
	  "issue":{"repo":"tatara-operator","title":"operator down","body":"trace"}}}`)
	require.Equal(t, http.StatusOK, w.Code)

	require.Len(t, e.forge.createdReqs, 1)
	require.Equal(t, tatarav1alpha1.ProposalKindIncident,
		tatarav1alpha1.ProposalKindFromBody(e.forge.createdReqs[0].Body),
		"forge issue body must carry the incident proposal marker")

	iss := e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 101))
	require.Equal(t, tatarav1alpha1.ProposalKindIncident,
		tatarav1alpha1.ProposalKindFromBody(iss.Status.Body),
		"Issue CR body must carry the incident proposal marker")
}

// After file_issue on an incident Task whose spec.dedupKey is set, the minted
// Issue CR carries the rule-key label (queue.LabelAlertRuleKey), and the
// forge CreateIssue call carried the tatara-alert-rule=<key> label - the O5
// suppression lookup and the human-visible forge recovery index (O4).
func TestOutcome_Incident_StampsRuleKeyLabel(t *testing.T) {
	task := taskV2("t1", "tatara", "incident", tatarav1alpha1.StageInvestigating, "incident")
	task.Spec.DedupKey = "abc123def4567890" //gitleaks:allow // test fixture, not a secret
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"), task)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"incident","payload":{
	  "action":"file_issue","alertRules":["tatara-operator-down"],"reason":"real outage",
	  "issue":{"repo":"tatara-operator","title":"operator down","body":"trace"}}}`)
	require.Equal(t, http.StatusOK, w.Code)

	iss := e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 101))
	require.Equal(t, "abc123def4567890", iss.Labels[queue.LabelAlertRuleKey])

	require.Len(t, e.forge.createdReqs, 1)
	require.Contains(t, e.forge.createdReqs[0].Labels, "tatara-alert-rule=abc123def4567890")
}

// A genuinely-new-but-related incident issue links itself as a GitHub
// sub-issue under the open tracker (issue.parent), plus a cross-reference
// comment on both, and the operator records result=linked (O8/B2/B3).
func TestOutcome_Incident_ParentLinkSuccess(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := obs.NewOperatorMetrics(reg)
	e := buildV2(t, v2Opts{metrics: metrics}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"), repoV2("tatara-memory", "tatara"),
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StageInvestigating, "incident"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"incident","payload":{
	  "action":"file_issue","alertRules":["tatara-operator-down"],"reason":"related to open tracker",
	  "issue":{"repo":"tatara-operator","title":"operator down","body":"trace",
	    "parent":{"repo":"tatara-memory","number":7}}}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	require.Len(t, e.forge.subIssueCalls, 1)
	require.Equal(t, "acme/tatara-memory#7", e.forge.subIssueCalls[0].ParentRef)
	require.Equal(t, 101, e.forge.subIssueCalls[0].ChildNumber,
		"childNumber must be the newly-filed issue's number")

	require.Len(t, e.forge.comments, 2, "cross-reference comment on both child and parent")
	var sawChild, sawParent bool
	for _, c := range e.forge.comments {
		if c.Ref == "acme/tatara-operator#101" {
			sawChild = true
			require.Contains(t, c.Body, "acme/tatara-memory#7")
		}
		if c.Ref == "acme/tatara-memory#7" {
			sawParent = true
			require.Contains(t, c.Body, "acme/tatara-operator#101")
		}
	}
	require.True(t, sawChild, "no cross-reference comment on the child issue")
	require.True(t, sawParent, "no cross-reference comment on the parent issue")

	require.Equal(t, float64(1), testutil.ToFloat64(metrics.IncidentSublinkCounter("linked")))
}

// AddSubIssue failing with scm.ErrSubIssuesUnsupported (GitLab, or any provider
// error) degrades to a cross-reference-comment-only fallback: the incident
// still succeeds, the relationship is never silently lost, and the metric
// records result=fallback_comment (O8/B3 fallback chain).
func TestOutcome_Incident_ParentLinkFallbackOnUnsupported(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := obs.NewOperatorMetrics(reg)
	e := buildV2(t, v2Opts{metrics: metrics}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"), repoV2("tatara-memory", "tatara"),
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StageInvestigating, "incident"))
	e.forge.addSubIssueErr = scm.ErrSubIssuesUnsupported

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"incident","payload":{
	  "action":"file_issue","alertRules":["tatara-operator-down"],"reason":"related to open tracker",
	  "issue":{"repo":"tatara-operator","title":"operator down","body":"trace",
	    "parent":{"repo":"tatara-memory","number":7}}}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String(),
		"a link failure must never fail the incident outcome - the issue is already filed")

	require.Len(t, e.forge.subIssueCalls, 1, "AddSubIssue is still attempted")
	require.Len(t, e.forge.comments, 2, "fallback still cross-references both issues")

	require.Equal(t, float64(1), testutil.ToFloat64(metrics.IncidentSublinkCounter("fallback_comment")))
	require.Equal(t, float64(0), testutil.ToFloat64(metrics.IncidentSublinkCounter("linked")))
}

// A generic AddSubIssue error (100-child cap, cross-repo 403, unique-parent
// conflict) takes the same fallback path as ErrSubIssuesUnsupported.
func TestOutcome_Incident_ParentLinkFallbackOnGenericError(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := obs.NewOperatorMetrics(reg)
	e := buildV2(t, v2Opts{metrics: metrics}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"), repoV2("tatara-memory", "tatara"),
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StageInvestigating, "incident"))
	e.forge.addSubIssueErr = fmt.Errorf("github: parent already holds 100 sub-issues")

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"incident","payload":{
	  "action":"file_issue","alertRules":["tatara-operator-down"],"reason":"related to open tracker",
	  "issue":{"repo":"tatara-operator","title":"operator down","body":"trace",
	    "parent":{"repo":"tatara-memory","number":7}}}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Len(t, e.forge.comments, 2)
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.IncidentSublinkCounter("fallback_comment")))
}

// When AddSubIssue fails AND the fallback cross-reference comment(s) ALSO
// fail (the same token that lacks cross-org sub-issue perms may also lack
// comment perms on the cross-repo parent - #328's exact failure mode), the
// relationship must not be silently reported as recorded: the metric moves to
// result=failed (not fallback_comment) and an ERROR is logged, while the
// incident outcome itself still succeeds (the issue is already filed).
func TestOutcome_Incident_ParentLinkFailedWhenSubIssueAndCommentBothFail(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := obs.NewOperatorMetrics(reg)
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))
	e := buildV2(t, v2Opts{metrics: metrics, logger: logger}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"), repoV2("tatara-memory", "tatara"),
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StageInvestigating, "incident"))
	e.forge.addSubIssueErr = fmt.Errorf("github: sub-issues cross-org create is forbidden (403)")
	e.forge.commentErr = fmt.Errorf("github: comment forbidden (403)")

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"incident","payload":{
	  "action":"file_issue","alertRules":["tatara-operator-down"],"reason":"related to open tracker",
	  "issue":{"repo":"tatara-operator","title":"operator down","body":"trace",
	    "parent":{"repo":"tatara-memory","number":7}}}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String(),
		"a total link/comment failure must never fail the incident outcome - the issue is already filed")

	require.Len(t, e.forge.subIssueCalls, 1, "AddSubIssue is still attempted")
	require.Len(t, e.forge.comments, 2, "both fallback comments are still attempted, even though they fail")

	require.Equal(t, float64(1), testutil.ToFloat64(metrics.IncidentSublinkCounter("failed")),
		"the declared failed bucket must actually be emitted when nothing landed anywhere")
	require.Equal(t, float64(0), testutil.ToFloat64(metrics.IncidentSublinkCounter("fallback_comment")),
		"fallback_comment must not be reported when the fallback comment itself failed")
	require.Equal(t, float64(0), testutil.ToFloat64(metrics.IncidentSublinkCounter("linked")))

	out := logBuf.String()
	require.Contains(t, out, `"level":"ERROR"`, "total failure must be logged at ERROR, not just WARN: %s", out)
	require.Contains(t, out, `"action":"incident_sublink"`)
	require.Contains(t, out, `"result":"failed"`)
}

// The parent-repo-unresolvable branch (child-only comment) gets the same
// error-capture + result classification: if even that lone fallback comment
// fails, the relationship is recorded nowhere and must surface as
// result=failed with an ERROR log, not a falsely-successful fallback_comment.
func TestOutcome_Incident_ParentRepoUnresolvableAndCommentFailsRecordsFailed(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := obs.NewOperatorMetrics(reg)
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))
	e := buildV2(t, v2Opts{metrics: metrics, logger: logger}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"), // no "tatara-memory" repo CR registered
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StageInvestigating, "incident"))
	e.forge.commentErr = fmt.Errorf("github: comment forbidden (403)")

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"incident","payload":{
	  "action":"file_issue","alertRules":["tatara-operator-down"],"reason":"related to open tracker",
	  "issue":{"repo":"tatara-operator","title":"operator down","body":"trace",
	    "parent":{"repo":"tatara-memory","number":7}}}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String(),
		"an unresolvable parent with a failed fallback comment must never fail the incident outcome")

	require.Empty(t, e.forge.subIssueCalls, "no forge ref to target AddSubIssue at")
	require.Len(t, e.forge.comments, 1, "the lone child-only fallback comment is still attempted")

	require.Equal(t, float64(1), testutil.ToFloat64(metrics.IncidentSublinkCounter("failed")))
	require.Equal(t, float64(0), testutil.ToFloat64(metrics.IncidentSublinkCounter("fallback_comment")))

	out := logBuf.String()
	require.Contains(t, out, `"level":"ERROR"`, "log output: %s", out)
	require.Contains(t, out, `"result":"failed"`)
}

// A parent repo not resolvable in this project (no such Repository CR) must
// never call AddSubIssue (no owner/repo to target) and falls back to a plain
// comment on the CHILD only, still preserving the relationship as text.
func TestOutcome_Incident_ParentRepoUnresolvableFallsBackToChildOnly(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := obs.NewOperatorMetrics(reg)
	e := buildV2(t, v2Opts{metrics: metrics}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"), // no "tatara-memory" repo CR registered
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StageInvestigating, "incident"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"incident","payload":{
	  "action":"file_issue","alertRules":["tatara-operator-down"],"reason":"related to open tracker",
	  "issue":{"repo":"tatara-operator","title":"operator down","body":"trace",
	    "parent":{"repo":"tatara-memory","number":7}}}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String(),
		"an unresolvable parent must never fail the incident outcome")

	require.Empty(t, e.forge.subIssueCalls, "no forge ref to target AddSubIssue at")
	require.Len(t, e.forge.comments, 1, "fallback comment on the child ONLY")
	require.Equal(t, "acme/tatara-operator#101", e.forge.comments[0].Ref)
	require.Contains(t, e.forge.comments[0].Body, "tatara-memory#7")

	require.Equal(t, float64(1), testutil.ToFloat64(metrics.IncidentSublinkCounter("fallback_comment")))
}

// file_issue with NO parent must never call AddSubIssue or post any
// cross-reference comment - the link path is entirely opt-in.
func TestOutcome_Incident_NoParentNoLinkCalls(t *testing.T) {
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StageInvestigating, "incident"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"incident","payload":{
	  "action":"file_issue","alertRules":["tatara-operator-down"],"reason":"standalone",
	  "issue":{"repo":"tatara-operator","title":"operator down","body":"trace"}}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	require.Empty(t, e.forge.subIssueCalls)
	require.Empty(t, e.forge.comments)
}

// issue.parent missing repo or number is a 400: the payload is malformed, not
// just link-worthy.
func TestOutcome_Incident_ParentMissingFieldsRejected(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StageInvestigating, "incident"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"incident","payload":{
	  "action":"file_issue","alertRules":["tatara-operator-down"],"reason":"r",
	  "issue":{"repo":"tatara-operator","title":"t","body":"b","parent":{"repo":"tatara-memory"}}}}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestOutcome_Incident_FalsePositiveRejects(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StageInvestigating, "incident"))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"incident","payload":{
	  "action":"false_positive","alertRules":["flappy"],"reason":"the alert is wrong"}}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, tatarav1alpha1.StageRejected, e.task(t, "t1").Status.Stage)
}

func TestOutcome_Incident_AlertRulesRequiredOnBothActions(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StageInvestigating, "incident"))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"incident","payload":{"action":"false_positive","alertRules":[],"reason":"r"}}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

// --- refine, and the B.3 fold ---------------------------------------------

// THE FOLD: adopt, VERIFY, then delete. The member's artifacts land on the
// umbrella with controller=true in ONE PUT (the API server rejects two
// controller=true refs), and only THEN is the member deleted.
func TestOutcome_Refine_FoldAdoptsVerifiesThenDeletes(t *testing.T) {
	member := taskV2("t2", "tatara", "clarify", tatarav1alpha1.StageClarifying, "clarify")
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "refine", tatarav1alpha1.StageRefining, "refine"),
		member,
		issueV2("tatara-operator", 291, "t2"),
		mrV2("tatara-operator", 295, "t2"),
	)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"refine","payload":{"folds":[{"task":"t2"}]}}`)
	require.Equal(t, http.StatusOK, w.Code)

	// The artifacts are now controller-owned by the UMBRELLA, and the member
	// survives as a PLAIN owner ref until the API server's GC resolves it.
	iss := e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 291))
	ctrl := 0
	for _, o := range iss.OwnerReferences {
		if o.Controller != nil && *o.Controller {
			ctrl++
			require.Equal(t, "t1", o.Name)
		}
	}
	require.Equal(t, 1, ctrl, "exactly one controller owner, always")

	mr := e.mr(t, tatarav1alpha1.MergeRequestName("tatara-operator", 295))
	name, ok := controllerOwnerOf(mr.OwnerReferences)
	require.True(t, ok)
	require.Equal(t, "t1", name)

	// The member is deleted ONLY after the verification passed.
	var gone tatarav1alpha1.Task
	err := e.c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "t2"}, &gone)
	require.True(t, apierrors.IsNotFound(err), "members are deleted only after adoption is VERIFIED")

	got := e.task(t, "t1")
	require.Empty(t, got.Status.FoldInFlight, "foldInFlight is cleared on success")
	require.Equal(t, tatarav1alpha1.StageDelivered, got.Status.Stage)

	// C4: adoption transfers the CONTROLLER ref, but every downstream consumer
	// (the C.6 approval grammar, the reaper's owned-set, the agent bundle)
	// reads the umbrella's Status.IssueRefs/MRRefs SLICES, not ownerRefs. A
	// fold that doesn't append there leaves adopted work unguarded and absent
	// from the bundle.
	require.Contains(t, got.Status.IssueRefs, tatarav1alpha1.IssueName("tatara-operator", 291),
		"the adopted Issue must land in the umbrella's Status.IssueRefs")
	require.Contains(t, got.Status.MRRefs, tatarav1alpha1.MergeRequestName("tatara-operator", 295),
		"the adopted MR must land in the umbrella's Status.MRRefs")
}

func controllerOwnerOf(refs []metav1.OwnerReference) (string, bool) {
	for _, o := range refs {
		if o.Controller != nil && *o.Controller {
			return o.Name, true
		}
	}
	return "", false
}

// A fold member with work in flight is REFUSED: 409 "fold target has work in
// flight" (fix 8).
func TestOutcome_Refine_FoldTargetWithWorkInFlightIs409(t *testing.T) {
	for _, tc := range []struct {
		name  string
		apply func(*tatarav1alpha1.Task)
	}{
		{"running pod", func(m *tatarav1alpha1.Task) {
			started := metav1.NewTime(frozenNow)
			m.Status.PodName, m.Status.PodStartedAt = "t2-implement", &started
		}},
		{"live post-approved stage", func(m *tatarav1alpha1.Task) {
			m.Status.Stage, m.Status.AgentKind = tatarav1alpha1.StageMerging, ""
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			member := taskV2("t2", "tatara", "implement", tatarav1alpha1.StageImplementing, "implement")
			tc.apply(member)
			e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
				repoV2("tatara-operator", "tatara"),
				taskV2("t1", "tatara", "refine", tatarav1alpha1.StageRefining, "refine"), member)

			w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
				`{"kind":"refine","payload":{"folds":[{"task":"t2"}]}}`)
			require.Equal(t, http.StatusConflict, w.Code)
			require.Contains(t, w.Body.String(), "fold target has work in flight")
			require.NoError(t, e.c.Get(context.Background(),
				client.ObjectKey{Namespace: ns, Name: "t2"}, &tatarav1alpha1.Task{}),
				"a refused fold deletes nothing")
		})
	}
}

// A closes[] target whose controller owner is not this Task has an ACTIVE task
// working it: 409 "issue has an active task" (fix 8).
func TestOutcome_Refine_CloseTargetWithAnActiveTaskIs409(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "refine", tatarav1alpha1.StageRefining, "refine"),
		taskV2("t2", "tatara", "clarify", tatarav1alpha1.StageClarifying, "clarify"),
		issueV2("tatara-operator", 291, "t2"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"refine","payload":{"closes":[{"repo":"tatara-operator","number":291,"reason":"stale"}]}}`)
	require.Equal(t, http.StatusConflict, w.Code)
	require.Contains(t, w.Body.String(), "issue has an active task")
}

// closes[] is LIVE-REVALIDATED against SCM immediately before each close:
// refine may act on a view up to an hour stale.
func TestOutcome_Refine_CloseIsLiveRevalidated(t *testing.T) {
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "refine", tatarav1alpha1.StageRefining, "refine"),
		issueV2("tatara-operator", 291, "t1"),
		issueV2("tatara-operator", 292, "t1"))
	// 292 is ALREADY closed on the forge; the mirror still says open.
	e.forge.issueStates[292] = scm.IssueState{Closed: true}

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"refine","payload":{"closes":[
	  {"repo":"tatara-operator","number":291,"reason":"superseded"},
	  {"repo":"tatara-operator","number":292,"reason":"superseded"}]}}`)
	require.Equal(t, http.StatusOK, w.Code)

	require.Len(t, e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 291)).Status.PendingComments, 1)
	require.Empty(t, e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 292)).Status.PendingComments,
		"an issue already closed on the forge is not closed again")
}

func TestOutcome_Refine_LinkAddsAPlainOwner(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "refine", tatarav1alpha1.StageRefining, "refine"),
		taskV2("t2", "tatara", "clarify", tatarav1alpha1.StageClarifying, "clarify"),
		issueV2("tatara-operator", 291, "t2"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"refine","payload":{"links":[{"repo":"tatara-operator","number":291}]}}`)
	require.Equal(t, http.StatusOK, w.Code)

	iss := e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 291))
	ctrl, _ := controllerOwnerOf(iss.OwnerReferences)
	require.Equal(t, "t2", ctrl, "a link never steals the controller flag")
	require.Len(t, iss.OwnerReferences, 2)
	require.Contains(t, e.task(t, "t1").Status.IssueRefs, iss.Name)
}

// A malformed links[] entry must be caught in the TOP validation block, BEFORE
// foldMembers deletes anything. Validating it after the fold made the rejection
// unrecoverable: the members were already gone, so the identical retry - which
// release lets re-validate immediately - hit NotFound on its own fold target and
// 500'd forever.
func TestOutcome_Refine_MalformedLinkRejectsBeforeAnyFoldDeletes(t *testing.T) {
	member := taskV2("t2", "tatara", "clarify", tatarav1alpha1.StageClarifying, "clarify")
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "refine", tatarav1alpha1.StageRefining, "refine"),
		member,
		issueV2("tatara-operator", 291, "t2"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"refine","payload":{"folds":[{"task":"t2"}],"links":[{"repo":"","number":0}]}}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "every links entry requires repo and number")

	require.NoError(t, e.c.Get(context.Background(),
		client.ObjectKey{Namespace: ns, Name: "t2"}, &tatarav1alpha1.Task{}),
		"a rejected refine deletes no fold member")

	// A corrected retry is a different fingerprint, so it re-validates and the
	// valid path still works end to end.
	ok := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"refine","payload":{"folds":[{"task":"t2"}],"links":[{"repo":"tatara-operator","number":291}]}}`)
	require.Equal(t, http.StatusOK, ok.Code)
}

func TestOutcome_Refine_EmptyPayloadIs400(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "refine", tatarav1alpha1.StageRefining, "refine"))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"refine","payload":{}}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "at least one of folds, closes, links")
}

// --- documentation --------------------------------------------------------

func TestOutcome_Documentation_DeclinedDeliversAndStampsDocumentedBy(t *testing.T) {
	covered := taskV2("t9", "tatara", "implement", tatarav1alpha1.StageDelivered, "")
	batch := taskV2("t1", "tatara", "documentation", tatarav1alpha1.StageDocumenting, "documentation")
	batch.Spec.DocumentsTasks = []string{"t9"}

	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-documentation", "tatara"), batch, covered)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"documentation","payload":{"action":"declined","reason":"nothing user-visible"}}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, tatarav1alpha1.StageDelivered, e.task(t, "t1").Status.Stage)
	require.Equal(t, "t1", e.task(t, "t9").Status.DocumentedBy)
}

// outcomeClaimStub seeds the exact durable state a process that DIED between
// claimOutcomeFingerprint and commit leaves behind: the OutcomeAccepted
// condition carrying the request's fingerprint with Reason "Outcome" (the bare
// claim), and no effect anywhere.
func outcomeClaimStub(t *testing.T, e *v2Env, task, fp, reason string, at time.Time) {
	t.Helper()
	cur := e.task(t, task)
	cur.Status.Conditions = append(cur.Status.Conditions, metav1.Condition{
		Type:               tatarav1alpha1.ConditionOutcomeAccepted,
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            fp,
		LastTransitionTime: metav1.NewTime(at),
	})
	require.NoError(t, e.c.Status().Update(context.Background(), cur))
}

// clarifyDiscussFingerprint is the fingerprint of the clarify body used below.
// It is computed the way the handler computes it: sha256("clarify|" + canonical
// payload JSON). The test asks the server for it rather than duplicating the
// hash, by POSTing once against a throwaway env and reading the condition back.
func clarifyDiscussFingerprint(t *testing.T) string {
	t.Helper()
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("fp", "tatara", "clarify", tatarav1alpha1.StageClarifying, "clarify"))
	w := e.do(t, http.MethodPost, "/tasks/fp/outcome", clarifyDiscussBody)
	require.Equal(t, http.StatusOK, w.Code)
	cond := tatarav1alpha1.OutcomeCondition(e.task(t, "fp"))
	require.NotNil(t, cond)
	return cond.Message
}

const clarifyDiscussBody = `{"kind":"clarify","payload":{"decision":"discuss","reason":"r"}}`

// SPEC TEST 2. A claim whose process died before commit is an ORPHANED STUB.
// Inside OutcomeClaimTTL (5m) an identical retry cannot tell "in flight on
// another replica" from "orphaned", so it is told to retry (409) rather than
// being admitted through to a second side effect. Past the TTL it RE-CLAIMS and
// proceeds - the self-heal that stops a 4xx stub wedging forever.
//
// The two cases probe the boundary itself: just under the TTL (4m59s of age) and
// just over it (5m1s). They are written against the constant, not the literal,
// because the TTL's exact value is pinned in api/v1alpha1 (against
// OutcomeHandlerBudget) and this test is about the three-state behaviour at
// whatever the boundary is, not about the number.
func TestOutcome_BareClaimInsideTTLIs409_PastTTLReclaimsAndProceeds(t *testing.T) {
	fp := clarifyDiscussFingerprint(t)

	t.Run("inside the TTL: 409, the task is untouched", func(t *testing.T) {
		e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
			repoV2("tatara-operator", "tatara"),
			taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageClarifying, "clarify"))
		outcomeClaimStub(t, e, "t1", fp, tatarav1alpha1.OutcomeReasonClaimed,
			frozenNow.Add(-tatarav1alpha1.OutcomeClaimTTL+time.Second))

		w := e.do(t, http.MethodPost, "/tasks/t1/outcome", clarifyDiscussBody)
		require.Equal(t, http.StatusConflict, w.Code, "a fresh bare claim means another replica is mid-flight")
		require.Equal(t, tatarav1alpha1.StageClarifying, e.task(t, "t1").Status.Stage,
			"a 409 in-flight answer must change nothing")
	})

	t.Run("past the TTL: re-claim and proceed", func(t *testing.T) {
		e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
			repoV2("tatara-operator", "tatara"),
			taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageClarifying, "clarify"))
		outcomeClaimStub(t, e, "t1", fp, tatarav1alpha1.OutcomeReasonClaimed,
			frozenNow.Add(-tatarav1alpha1.OutcomeClaimTTL-time.Second))

		w := e.do(t, http.MethodPost, "/tasks/t1/outcome", clarifyDiscussBody)
		require.Equal(t, http.StatusOK, w.Code, "an orphaned stub must self-heal, not 409 forever")
		got := e.task(t, "t1")
		require.Equal(t, tatarav1alpha1.StageParked, got.Status.Stage,
			"the outcome must actually be PROCESSED, not replayed as a no-op")
		require.Equal(t, "awaiting-human", got.Status.StageReason)
		cond := tatarav1alpha1.OutcomeCondition(got)
		require.NotNil(t, cond)
		require.Equal(t, "Clarify", cond.Reason, "commit must overwrite the claim's Reason")
	})
}

// SPEC TEST 4. A COMMITTED outcome (Reason != "Outcome") still replays 200 with
// the unchanged Task. This is the TTL-stopped pod's honest retry and it must
// never 409 the Task into failure - the property the whole condition exists for.
func TestOutcome_CommittedOutcomeStillReplays200(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageClarifying, "clarify"))

	first := e.do(t, http.MethodPost, "/tasks/t1/outcome", clarifyDiscussBody)
	require.Equal(t, http.StatusOK, first.Code)
	before := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.StageParked, before.Status.Stage)
	require.Equal(t, "Clarify", tatarav1alpha1.OutcomeCondition(before).Reason)

	second := e.do(t, http.MethodPost, "/tasks/t1/outcome", clarifyDiscussBody)
	require.Equal(t, http.StatusOK, second.Code, "an identical retry of a COMMITTED outcome replays")
	after := e.task(t, "t1")
	require.Equal(t, before.Status.Stage, after.Status.Stage)
	require.Equal(t, before.Status.StageReason, after.Status.StageReason)
	require.Len(t, after.Status.Notes, len(before.Status.Notes),
		"a replay must not re-append the outcome note")
}

// --- A2: a class-B rejection RELEASES the claim -----------------------------

// SPEC TEST 1. Every kind-specific validation failure is CLASS B
// (pre-execution): it runs before any committed effect, so NOTHING may be
// cached under the fingerprint. The claim must be RELEASED, and an identical
// retry must RE-VALIDATE - not take the 200 replay branch, and not sit out the
// claim TTL as an in-flight 409.
func TestOutcome_ValidationRejectionReleasesTheClaim(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageClarifying, "clarify"))

	bad := `{"kind":"clarify","payload":{"decision":"discuss","reason":"  "}}`
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", bad)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Nil(t, tatarav1alpha1.OutcomeCondition(e.task(t, "t1")),
		"a class-B rejection must RELEASE the claim, not leave a stub the retry replays")

	// The IDENTICAL retry must re-validate and 400 again, not 200-and-do-nothing
	// and not 409 claim-in-flight.
	again := e.do(t, http.MethodPost, "/tasks/t1/outcome", bad)
	require.Equal(t, http.StatusBadRequest, again.Code,
		"an identical retry of a released fingerprint must RE-VALIDATE")
	require.Nil(t, tatarav1alpha1.OutcomeCondition(e.task(t, "t1")))

	// And a CORRECTED retry must be processed for real.
	fixed := e.do(t, http.MethodPost, "/tasks/t1/outcome", clarifyDiscussBody)
	require.Equal(t, http.StatusOK, fixed.Code)
	require.Equal(t, tatarav1alpha1.StageParked, e.task(t, "t1").Status.Stage)
}

// The two top-of-handler gates run before any kind handler and stamp nothing,
// so they hold a claim they must not keep.
func TestOutcome_TopOfHandlerGatesReleaseTheClaim(t *testing.T) {
	t.Run("kind-mismatch", func(t *testing.T) {
		e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
			repoV2("tatara-operator", "tatara"),
			taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageClarifying, "clarify"))
		w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
			`{"kind":"implement","payload":{"action":"declined","reason":"nope"}}`)
		require.Equal(t, http.StatusConflict, w.Code)
		require.Nil(t, tatarav1alpha1.OutcomeCondition(e.task(t, "t1")))
	})
	t.Run("terminal-stage", func(t *testing.T) {
		e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
			repoV2("tatara-operator", "tatara"),
			taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageParked, "clarify"))
		w := e.do(t, http.MethodPost, "/tasks/t1/outcome", clarifyDiscussBody)
		require.Equal(t, http.StatusConflict, w.Code)
		require.Nil(t, tatarav1alpha1.OutcomeCondition(e.task(t, "t1")))
	})
	t.Run("payload decode", func(t *testing.T) {
		e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
			repoV2("tatara-operator", "tatara"),
			taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageClarifying, "clarify"))
		w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
			`{"kind":"clarify","payload":{"decision":"discuss","bogusField":1}}`)
		require.Equal(t, http.StatusBadRequest, w.Code)
		require.Nil(t, tatarav1alpha1.OutcomeCondition(e.task(t, "t1")))
	})
}

// The kind SWITCH's default arm is a class-B rejection holding a claim, and it is
// REACHABLE - it is not dead code behind the kind gate. status.agentKind is a
// plain string with no closed-set validation, and the gate only checks that the
// pod's claimed kind EQUALS it. So a Task carrying a bogus agentKind (a hand-edited
// status, a stored CR from a version that knew a kind this one does not, a future
// stage whose AgentKindFor gained a value before the switch did) sails through the
// gate on a matching bogus kind and lands here.
//
// Without the release it 400s while leaving a bare claim behind, and the agent's
// every retry 409s in-flight for the whole OutcomeClaimTTL instead of getting the
// same immediate, actionable 400.
func TestOutcome_UnknownKindReleasesTheClaim(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		// agentKind is bogus, so the kind gate PASSES on a matching bogus kind.
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageClarifying, "bogus"))

	body := `{"kind":"bogus","payload":{"whatever":1}}`
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", body)
	require.Equal(t, http.StatusBadRequest, w.Code, "an unknown kind is a 400, not a claim swallowed in silence")
	require.Nil(t, tatarav1alpha1.OutcomeCondition(e.task(t, "t1")),
		"the unknown-kind arm runs before any effect, so it is class B and must RELEASE the claim")

	// The IDENTICAL retry must re-validate to the same 400, not 409 in-flight.
	again := e.do(t, http.MethodPost, "/tasks/t1/outcome", body)
	require.Equal(t, http.StatusBadRequest, again.Code,
		"an identical retry of a released fingerprint must RE-VALIDATE, not sit out the claim TTL")
	require.Nil(t, tatarav1alpha1.OutcomeCondition(e.task(t, "t1")))
}

// The head-moved 409 is the deliberate self-healing path: it stamps NOTHING and
// tells the agent to re-review the fresh diff. Its claim must be released too,
// or the agent's honest resubmit-with-the-new-sha would 409 in-flight for the
// whole claim TTL.
func TestOutcome_HeadMovedReleasesTheClaim(t *testing.T) {
	forge := &reviewPanicForge{heads: map[int]string{295: "sha-NEW"}}
	e := buildV2(t, v2Opts{writer: forge, reader: emptyCommentReader{}},
		projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StageReviewing, "review"),
		mrV2("tatara-operator", 295, "t1"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"review","payload":{
	  "verdict":"approve","reviewedSHAs":[{"repo":"tatara-operator","number":295,"sha":"sha-OLD"}]}}`)
	require.Equal(t, http.StatusConflict, w.Code)
	require.Nil(t, tatarav1alpha1.OutcomeCondition(e.task(t, "t1")),
		"head-moved stamps nothing, so it must hold no claim either")
}

// An ILLEGAL TRANSITION is refused inside commit's mutate closure, but
// objbudget.FitTask persists whatever the closure already did before it
// errored. So the closure must transition FIRST and note SECOND, or the note
// lands on a refused outcome - and, now that the rejection releases the claim,
// lands AGAIN on every retry.
func TestOutcome_IllegalTransitionWritesNothingAndReleasesTheClaim(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		// triaging has no parked edge at all, so clarify's decision=discuss
		// (-> parked[awaiting-human]) is refused by the F.3 table.
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageTriaging, "clarify"))

	for i := range 3 {
		w := e.do(t, http.MethodPost, "/tasks/t1/outcome", clarifyDiscussBody)
		require.Equal(t, http.StatusConflict, w.Code, "attempt %d", i)
		got := e.task(t, "t1")
		require.Equal(t, tatarav1alpha1.StageTriaging, got.Status.Stage)
		require.Empty(t, got.Status.Notes,
			"a refused transition must leave NO note behind, on any attempt")
		require.Nil(t, tatarav1alpha1.OutcomeCondition(got),
			"an illegal-transition 409 is class B: it must release its claim")
	}
}

// SPEC TEST 5, black-box half: a rejection never undoes a COMMITTED outcome.
//
// The invariant lives in release's ownership check and is asserted directly in
// release_internal_test.go - through the handler it can only be observed as an
// EFFECT, not as a condition. A DIFFERENT outcome's claim overwrites the
// committed condition in the single OutcomeAccepted slot BEFORE any gate runs
// (claim-first is the C7 ordering and must not move), so the committed
// condition is already gone by the time release could look at it. What must
// hold - and does - is that the committed stage, reason and note survive
// untouched, and that the rejection is still a 409.
func TestOutcome_RejectionNeverUndoesACommittedOutcome(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StageClarifying, "clarify"))

	ok := e.do(t, http.MethodPost, "/tasks/t1/outcome", clarifyDiscussBody)
	require.Equal(t, http.StatusOK, ok.Code)
	before := e.task(t, "t1")
	require.Equal(t, "Clarify", tatarav1alpha1.OutcomeCondition(before).Reason)
	require.Equal(t, tatarav1alpha1.StageParked, before.Status.Stage)

	// A DIFFERENT outcome now arrives. The Task is parked (terminal), so it 409s
	// at the terminal gate, which releases the claim it just took.
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"clarify","payload":{"decision":"close","reason":"x"}}`)
	require.Equal(t, http.StatusConflict, w.Code)

	after := e.task(t, "t1")
	require.Equal(t, before.Status.Stage, after.Status.Stage,
		"a rejection must never undo the committed outcome's effect")
	require.Equal(t, before.Status.StageReason, after.Status.StageReason)
	require.Equal(t, before.Status.Notes, after.Status.Notes)
	require.Nil(t, tatarav1alpha1.OutcomeCondition(after),
		"the second outcome's claim clobbered the committed condition before the gate ran, "+
			"and its release then removed the claim it owned: the slot ends up EMPTY")
}
