package restapi_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/controller"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"github.com/szymonrychu/tatara-operator/internal/restapi"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// reviewPanicForge answers the reads /outcome is allowed to make (GetPRHead,
// and the B1 readiness pair) plus the ONE write it makes (EditPR), and PANICS
// on PostReview and Merge. THE /outcome HANDLER POSTS NO REVIEW AND MERGES
// NOTHING: for kind=review it does exactly two things - one read, then it
// persists the intent (mr.status.pendingReview). The MergeRequest RECONCILER
// posts it (Task 13). If PostReview panics, /outcome posted a review and the
// whole crash-safety story (C.5.3) is void.
type reviewPanicForge struct {
	scm.SCMWriter
	heads map[int]string
	// The B1 readiness surface, defaulting to ready. See recordingForge.
	ciStatuses  map[int]string
	mergeStates map[int]scm.MergeState
	// editPRs records every EditPR call in order; editPRErr fails them all, which
	// is how the best-effort contract is tested (a forge refusal must not turn an
	// accepted submit into a 500).
	editPRs   []recordedEditPR
	editPRErr error
}

// recordedEditPR is one EditPR call. The submitted path always sends both
// fields (submit_outcome requires both), so this flattens them; the PATCH
// semantic - that a nil field is not sent at all - is pinned in internal/scm,
// where the request is actually serialised.
type recordedEditPR struct {
	RepoURL string
	Number  int
	Title   string
	Body    string
}

func (f *reviewPanicForge) EditPR(_ context.Context, repoURL, _ string, number int, req scm.EditPRReq) error {
	f.editPRs = append(f.editPRs, recordedEditPR{
		RepoURL: repoURL, Number: number, Title: deref(req.Title), Body: deref(req.Body),
	})
	return f.editPRErr
}

func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func (f *reviewPanicForge) GetPRState(_ context.Context, _, _ string, number int) (scm.PRState, error) {
	ci, ok := f.ciStatuses[number]
	if !ok {
		ci = "success"
	}
	return scm.PRState{Author: "tatara-agent", HeadSHA: f.heads[number], CIStatus: ci}, nil
}

func (f *reviewPanicForge) GetMergeState(_ context.Context, _, _ string, number int) (scm.MergeState, error) {
	if ms, ok := f.mergeStates[number]; ok {
		return ms, nil
	}
	return scm.MergeStateClean, nil
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
		taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StateRefined, "brainstorm"))

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
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"declined","reason":"nope"}}`)
	require.Equal(t, http.StatusConflict, w.Code, "the pod's claim is not trusted")
	require.Equal(t, tatarav1alpha1.StateRefined, e.task(t, "t1").Status.State)
}

// #521 folded `parked` and `failed` into the orthogonal park flag: both old
// shapes are now the SAME terminal condition, Parked(t), alongside the two
// genuine terminal STATES (done, rejected).
func TestOutcome_TerminalStageIs409(t *testing.T) {
	cases := []struct {
		name string
		task *tatarav1alpha1.Task
	}{
		{"rejected", taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateRejected, "clarify")},
		{"done", taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateDone, "clarify")},
		{"parked", parkedTaskV2(t, "t1", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify", stage.ReasonOperatorError)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
				repoV2("tatara-operator", "tatara"), tc.task)
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
		taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StateRefined, "brainstorm"))

	body := `{"kind":"brainstorm","payload":{"action":"skip","reason":"nothing novel this cycle"}}`
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", body)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, tatarav1alpha1.StateDone, e.task(t, "t1").Status.State)

	// The Task is now DELIVERED. Without the idempotency record this replay
	// would 409 on the terminal-stage gate.
	w = e.do(t, http.MethodPost, "/tasks/t1/outcome", body)
	require.Equal(t, http.StatusOK, w.Code, "a TTL-stopped pod's retry must not 409")
	require.Equal(t, tatarav1alpha1.StateDone, e.task(t, "t1").Status.State)

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
	e := buildV2(t, v2Opts{writer: &reviewPanicForge{heads: map[int]string{295: "live-head"}}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		mrV2("tatara-operator", 295, "t1"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"submitted","title":"T","body":"B","changeSignificance":"patch"}}`)
	require.Equal(t, http.StatusOK, w.Code)

	got := e.task(t, "t1")
	require.Equal(t, []string{"tatara-operator"}, got.Spec.MergeOrder,
		"with one repo there is exactly one order and nothing to get wrong")
	require.Equal(t, tatarav1alpha1.StateAwaitingReview, got.Status.State)
	require.Equal(t, "patch", e.mr(t, tatarav1alpha1.MergeRequestName("tatara-operator", 295)).Status.Significance)
}

// MORE THAN ONE REPO: mergeOrder is REQUIRED. There is NO LEXICAL DEFAULT -
// lexical order merges cli BEFORE operator, precisely the DisallowUnknownFields
// fleet outage this redesign exists to prevent.
func TestOutcome_Implement_MultiRepoRequiresMergeOrder(t *testing.T) {
	objs := []client.Object{
		projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"), repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
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
	e3 := buildV2(t, v2Opts{writer: &reviewPanicForge{heads: map[int]string{295: "live-head", 80: "live-head-cli"}}}, objs...)
	w = e3.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"submitted","title":"T","body":"B","changeSignificance":"minor","mergeOrder":["tatara-operator","tatara-cli"]}}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, []string{"tatara-operator", "tatara-cli"}, e3.task(t, "t1").Spec.MergeOrder)
}

// THE MACHINE SIGNAL. At implement accept the operator fetches the LIVE MR
// head from the SCM - never the agent-reported one - and records it as
// status.lastBotHeadSHA. ReconcileOwnership (OP8) reads this cursor to detect
// an unattributable external push.
func TestOutcome_Implement_RecordsLiveBotHeadSHA(t *testing.T) {
	e := buildV2(t, v2Opts{writer: &reviewPanicForge{heads: map[int]string{295: "live-head-999"}}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		mrV2("tatara-operator", 295, "t1"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"submitted","title":"T","body":"B","changeSignificance":"minor"}}`)
	require.Equal(t, http.StatusOK, w.Code)

	mr := e.mr(t, tatarav1alpha1.MergeRequestName("tatara-operator", 295))
	require.Equal(t, "live-head-999", mr.Status.LastBotHeadSHA,
		"lastBotHeadSHA must be the live head fetched from SCM, not an agent-reported sha")
}

// THE SUBMITTED TITLE AND BODY ARE THE MERGE REQUEST'S. They used to reach
// nothing but an internal Task note, so a reviewer asking an agent to correct a
// stale version in the MR title was asking for something no agent could do:
// mr_write exposes open/comment/reply only, and submit_outcome(title=, body=)
// was documented as the carrier while writing neither. This is that write.
func TestOutcome_Implement_SubmittedEditsTheMergeRequestOnTheForge(t *testing.T) {
	f := &reviewPanicForge{heads: map[int]string{295: "live-head"}}
	e := buildV2(t, v2Opts{writer: f}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		mrV2("tatara-operator", 295, "t1"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"submitted","title":"chore(deps): bump terraform 42.99.0 to 43.4.4","body":"the corrected description","changeSignificance":"patch"}}`)
	require.Equal(t, http.StatusOK, w.Code)

	require.Len(t, f.editPRs, 1, "the one open owned merge request must be edited exactly once")
	require.Equal(t, "https://github.com/acme/tatara-operator", f.editPRs[0].RepoURL)
	require.Equal(t, 295, f.editPRs[0].Number)
	require.Equal(t, "chore(deps): bump terraform 42.99.0 to 43.4.4", f.editPRs[0].Title)
	require.Equal(t, "the corrected description", f.editPRs[0].Body)

	// The mirror is refreshed from the value just written, not left stale until
	// the next sweep: the next agent turn and the review pod read the mirror.
	mr := e.mr(t, tatarav1alpha1.MergeRequestName("tatara-operator", 295))
	require.Equal(t, "chore(deps): bump terraform 42.99.0 to 43.4.4", mr.Status.Title)
	require.Equal(t, "the corrected description", mr.Status.Body)
}

// AN EXTERNAL MERGE REQUEST IS A HUMAN'S AND ITS TITLE IS THEIRS. The platform
// may review it but never pushes to it, so it never rewrites its title either -
// while a tatara-owned sibling in the same submit still gets the edit.
func TestOutcome_Implement_SubmittedSkipsAnExternallyOwnedMergeRequest(t *testing.T) {
	f := &reviewPanicForge{heads: map[int]string{295: "h1", 80: "h2"}}
	e := buildV2(t, v2Opts{writer: f}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"), repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		mrV2("tatara-operator", 295, "t1", func(m *tatarav1alpha1.MergeRequest) {
			m.Status.Ownership = tatarav1alpha1.OwnershipExternal
		}),
		mrV2("tatara-cli", 80, "t1", func(m *tatarav1alpha1.MergeRequest) {
			m.Status.Ownership = tatarav1alpha1.OwnershipTatara
		}))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"submitted","title":"T","body":"B","changeSignificance":"minor","mergeOrder":["tatara-cli","tatara-operator"]}}`)
	require.Equal(t, http.StatusOK, w.Code)

	require.Len(t, f.editPRs, 1, "only the tatara-owned merge request may be edited")
	require.Equal(t, 80, f.editPRs[0].Number)

	ext := e.mr(t, tatarav1alpha1.MergeRequestName("tatara-operator", 295))
	require.Equal(t, "an MR", ext.Status.Title, "an external merge request's mirrored title is not rewritten either")
}

// BEST-EFFORT, EXACTLY LIKE THE record_bot_head STAMP. The stage transition has
// already committed by the time the edit runs, so a forge refusal must leave the
// accepted outcome accepted - one clean 200 - rather than turning it into a 500
// the agent would retry against a Task that has already moved on.
func TestOutcome_Implement_ForgeEditFailureDoesNotFailTheSubmit(t *testing.T) {
	f := &reviewPanicForge{heads: map[int]string{295: "live-head"}, editPRErr: fmt.Errorf("forge said 403")}
	e := buildV2(t, v2Opts{writer: f}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		mrV2("tatara-operator", 295, "t1"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"submitted","title":"T","body":"B","changeSignificance":"minor"}}`)
	require.Equal(t, http.StatusOK, w.Code, "a failed title edit must not fail the submit")

	var dto map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &dto),
		"the response body must be exactly ONE valid JSON object; the edit must never touch o.w")

	require.Equal(t, tatarav1alpha1.StateAwaitingReview, e.task(t, "t1").Status.State,
		"the stage transition committed before the edit and stays committed")
	mr := e.mr(t, tatarav1alpha1.MergeRequestName("tatara-operator", 295))
	require.Equal(t, "an MR", mr.Status.Title,
		"the mirror follows the forge: a refused edit leaves it as it was")
}

// A merge request title has the same forge length cap an issue title does
// (GitLab's 255-char Issuable validation applies to both), and an over-long one
// is a 400 that would discard the edit. Clamp before the write, as every other
// forge title site does.
func TestOutcome_Implement_OverLongTitleIsClampedBeforeTheForge(t *testing.T) {
	f := &reviewPanicForge{heads: map[int]string{295: "live-head"}}
	e := buildV2(t, v2Opts{writer: f}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		mrV2("tatara-operator", 295, "t1"))

	long := strings.Repeat("x", tatarav1alpha1.IssueTitleMaxChars+50)
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		fmt.Sprintf(`{"kind":"implement","payload":{"action":"submitted","title":%q,"body":"B","changeSignificance":"minor"}}`, long))
	require.Equal(t, http.StatusOK, w.Code)

	require.Len(t, f.editPRs, 1)
	require.LessOrEqual(t, utf8.RuneCountInString(f.editPRs[0].Title), tatarav1alpha1.IssueTitleMaxChars,
		"the title handed to the forge must be clamped, not the raw agent string")
	require.Equal(t, tatarav1alpha1.ClampIssueTitle(long), f.editPRs[0].Title)
	require.Equal(t, f.editPRs[0].Title, e.mr(t, tatarav1alpha1.MergeRequestName("tatara-operator", 295)).Status.Title,
		"the mirror records what the forge was actually sent")
}

// NO TITLE, NO BODY, NO FORGE CALL. `submitted` requires both fields, so a
// payload missing one is refused in the read-only validation phase - before the
// claim - and the forge is never reached. This is what keeps every 4xx this
// endpoint produces a pure no-write rejection.
func TestOutcome_Implement_SubmittedWithoutTitleIs400AndMakesNoForgeEdit(t *testing.T) {
	f := &reviewPanicForge{heads: map[int]string{295: "live-head"}}
	e := buildV2(t, v2Opts{writer: f}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		mrV2("tatara-operator", 295, "t1"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"submitted","title":"","body":"","changeSignificance":"minor"}}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Empty(t, f.editPRs, "a rejected outcome must not edit anything on the forge")
}

// THE HEAD-RECORD BLOCK MUST NEVER TOUCH o.w. projectSCMWriterAndToken does a
// LIVE k8s Get for the scm secret and writes an HTTP error to whatever
// ResponseWriter it is given when that Get fails - which can happen
// transiently (API hiccup, secret rotation) well after the stage transition
// already committed. Before the discardWriter fix this write landed on o.w,
// and o.ok() then appended a SECOND JSON body after it: the client got an
// error status with two concatenated JSON objects for an outcome that had, in
// fact, already been accepted. Omitting scmSecretV2() reproduces exactly that
// resolution failure (a NotFound Get, standing in for any transient one) and
// asserts the response stays a single, cleanly-decodable 200 with the stamp
// simply skipped.
func TestOutcome_Implement_ScmResolutionFailureDoesNotCorruptTheResponse(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), // no scmSecretV2(): the secret Get fails
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		mrV2("tatara-operator", 295, "t1"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"submitted","title":"T","body":"B","changeSignificance":"minor"}}`)
	require.Equal(t, http.StatusOK, w.Code, "the accepted outcome's own response must win, not the resolver's side-effect error")

	// json.Unmarshal REFUSES trailing data after the first value, so this fails
	// if a second body got appended.
	var dto map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &dto),
		"the response body must be exactly ONE valid JSON object, not two concatenated ones")

	got := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.StateAwaitingReview, got.Status.State, "the stage transition still committed")

	mr := e.mr(t, tatarav1alpha1.MergeRequestName("tatara-operator", 295))
	require.Empty(t, mr.Status.LastBotHeadSHA, "a resolution failure must skip the stamp, not fabricate one")
}

func TestOutcome_Implement_SubmittedWithNoOpenMRIs400(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"submitted","title":"T","body":"B","changeSignificance":"patch"}}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "action=submitted but this task owns no open MR")
}

func TestOutcome_Implement_Declined(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"declined","reason":"the issue is already fixed"}}`)
	require.Equal(t, http.StatusOK, w.Code)
	got := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.StateUnderImplementation, got.Status.State, "a park never changes state")
	require.True(t, tatarav1alpha1.Parked(got))
	require.Equal(t, "implement-declined", got.Status.ParkReason)
}

// TestOutcome_Implement_DeclinedOnTheFrozenWireContract pins the EXACT bytes
// tatara-cli sends for an implement decline. The cli's outcomeArgMap maps the
// agent's snake_case decline_reason onto the WIRE key `reason` and strips
// `task`; there is no `declineReason` key on the wire and there never was.
// Every implement decline is this payload, so a refusal here is the ONLY way an
// implement Task terminates on a decline going away entirely.
func TestOutcome_Implement_DeclinedOnTheFrozenWireContract(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"declined","reason":"the issue is already fixed"}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	got := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.StateUnderImplementation, got.Status.State, "a park never changes state")
	require.True(t, tatarav1alpha1.Parked(got))
	require.Equal(t, stage.ReasonImplementDeclined, got.Status.ParkReason)
	require.Len(t, got.Status.Notes, 1, "the decline is recorded as an agent note")
	require.Equal(t, "declined: the issue is already fixed", got.Status.Notes[0].Body,
		"the agent-facing note text now sources from the single wire `reason` field")
}

// TestOutcome_Implement_DeclineWithoutAReasonIs400: `reason` is REQUIRED on
// action=declined. It is the same single field the gate actions use; only its
// LEGALITY changes with the action.
func TestOutcome_Implement_DeclineWithoutAReasonIs400(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"declined"}}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "action=declined requires a non-empty reason")
}

// TestOutcome_Implement_DeclineReasonIsNotAWireKey: `declineReason` was an
// operator invention that never appeared in tatara-cli's post-outcomeArgMap
// output. It is not in the frozen key set, so DisallowUnknownFields refuses it
// rather than the operator quietly growing a second contract.
func TestOutcome_Implement_DeclineReasonIsNotAWireKey(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"declined","declineReason":"x"}}`)
	require.Equal(t, http.StatusBadRequest, w.Code,
		"declineReason is not in the frozen key set, so DisallowUnknownFields refuses it")
	got := e.task(t, "t1")
	require.False(t, tatarav1alpha1.Parked(got), "a refused payload parks nothing")
	require.Empty(t, got.Status.Notes)
}

func TestOutcome_Implement_SubmittedForbidsReason(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
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
				taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
				mrV2("tatara-operator", 295, "t1"))
			w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
				`{"kind":"implement","payload":`+tc.payload+`}`)
			require.Equal(t, http.StatusBadRequest, w.Code)
			require.Equal(t, tatarav1alpha1.StateUnderImplementation, e.task(t, "t1").Status.State)
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
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateAwaitingReview, "review"),
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
	require.Equal(t, tatarav1alpha1.StateAwaitingReview, e.task(t, "t1").Status.State)
}

func TestOutcome_Review_ApprovePersistsApprovedStatus(t *testing.T) {
	forge := &reviewPanicForge{heads: map[int]string{295: "sha1"}}
	e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateAwaitingReview, "review"),
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
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateAwaitingReview, "review"),
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
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateAwaitingReview, "review"),
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
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateAwaitingReview, "review"),
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
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateAwaitingReview, "review"),
		mrV2("tatara-operator", 295, "t1"))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"review","payload":{
	  "verdict":"request_changes","reviewedSHAs":[{"repo":"tatara-operator","number":295,"sha":"sha1"}]}}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

// #398: a file-level finding (no line - the reviewer is commenting on the
// file as a whole, not one line of it) must not be forced to line=0, which
// the CRD's old `+kubebuilder:validation:Minimum=1` on a bare int rejected as
// Invalid. Omitting "line" from the envelope must round-trip to a nil
// ReviewFinding.Line, not a zero value.
func TestOutcome_Review_FindingLineOmitted_PersistsNilLine(t *testing.T) {
	forge := &reviewPanicForge{heads: map[int]string{295: "sha1"}}
	e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateAwaitingReview, "review"),
		mrV2("tatara-operator", 295, "t1"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"review","payload":{
	  "verdict":"request_changes",
	  "reviewedSHAs":[{"repo":"tatara-operator","number":295,"sha":"sha1"}],
	  "findings":[{"repo":"tatara-operator","number":295,"path":"internal/x.go",
	               "body":"whole file needs a rethink","severity":"high"}]}}`)
	require.Equal(t, http.StatusOK, w.Code)

	mr := e.mr(t, tatarav1alpha1.MergeRequestName("tatara-operator", 295))
	require.Len(t, mr.Status.PendingReview.Findings, 1)
	require.Nil(t, mr.Status.PendingReview.Findings[0].Line, "a file-level finding must persist Line == nil, not 0")
}

// The line=5 counterpart of the above: a finding that DOES carry a line must
// round-trip to *5, not be silently dropped by the *int change.
func TestOutcome_Review_FindingLineRoundTripsToPointer(t *testing.T) {
	forge := &reviewPanicForge{heads: map[int]string{295: "sha1"}}
	e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateAwaitingReview, "review"),
		mrV2("tatara-operator", 295, "t1"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"review","payload":{
	  "verdict":"request_changes",
	  "reviewedSHAs":[{"repo":"tatara-operator","number":295,"sha":"sha1"}],
	  "findings":[{"repo":"tatara-operator","number":295,"path":"internal/x.go","line":5,
	               "body":"this races","severity":"critical"}]}}`)
	require.Equal(t, http.StatusOK, w.Code)

	mr := e.mr(t, tatarav1alpha1.MergeRequestName("tatara-operator", 295))
	require.Len(t, mr.Status.PendingReview.Findings, 1)
	require.NotNil(t, mr.Status.PendingReview.Findings[0].Line)
	require.Equal(t, 5, *mr.Status.PendingReview.Findings[0].Line)
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
				taskV2("t1", "tatara", "implement", tatarav1alpha1.StateAwaitingReview, "review"), mr)

			w := e.do(t, http.MethodPost, "/tasks/t1/outcome", fmt.Sprintf(`{"kind":"review","payload":{
			  "verdict":"approve","changeSignificance":%q,
			  "reviewedSHAs":[{"repo":"tatara-operator","number":295,"sha":"sha1"}]}}`, tc.review))
			require.Equal(t, http.StatusOK, w.Code)
			require.Equal(t, tc.want,
				e.mr(t, tatarav1alpha1.MergeRequestName("tatara-operator", 295)).Status.Significance)
		})
	}
}

// THE ADOPTED SEMVER FLOOR IS A FLOOR, NOT A CEILING.
//
// MintAdoptedUpgradeTask seeds every adopted dependency merge request's mirror
// with controller.AdoptedSignificanceFloor so the common path - approved at
// first review, no upgrade turn, no declared change_significance - still cuts a
// tag. The floor is only safe if the reviewer keeps its say, which means the
// escalation clause below must outrank it for EVERY value a review can declare.
// That is a property of restapi's own rank table and its `sig != "" &&
// rank[sig] > rank[current]` clause, so it is asserted HERE, against the real
// handler, and not against a copy of the table in the package that seeds it.
func TestOutcome_Review_EscalatesTheSeededAdoptedFloor(t *testing.T) {
	for _, declared := range []string{"minor", "major"} {
		t.Run(declared, func(t *testing.T) {
			forge := &reviewPanicForge{heads: map[int]string{295: "sha1"}}
			mr := mrV2("tatara-operator", 295, "t1", func(m *tatarav1alpha1.MergeRequest) {
				m.Status.Significance = controller.AdoptedSignificanceFloor
			})
			e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(),
				repoV2("tatara-operator", "tatara"),
				taskV2("t1", "tatara", "upgrade", tatarav1alpha1.StateAwaitingReview, "review"), mr)

			w := e.do(t, http.MethodPost, "/tasks/t1/outcome", fmt.Sprintf(`{"kind":"review","payload":{
			  "verdict":"approve","changeSignificance":%q,
			  "reviewedSHAs":[{"repo":"tatara-operator","number":295,"sha":"sha1"}]}}`, declared))
			require.Equal(t, http.StatusOK, w.Code)
			require.Equal(t, declared,
				e.mr(t, tatarav1alpha1.MergeRequestName("tatara-operator", 295)).Status.Significance,
				"the seeded adoption floor must be the LOWEST rank, or a reviewer who reads a "+
					"breaking change in the changelog cannot raise the release off patch")
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
				taskV2("t1", "tatara", "implement", tatarav1alpha1.StateAwaitingReview, "review"),
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

// --- gate (#521 folded the clarify kind's decision=implement into implement's
// action=approved|discuss|rejected gate) -----------------------------------

// gateResponseDTO decodes the folded gate's 200 refusal body
// ({"granted":false,"reason":...,"declared":...}). A GRANT does not use this
// shape at all: it returns the plain Task DTO, same as every other accepted
// outcome.
type gateResponseDTO struct {
	Granted  bool   `json:"granted"`
	Reason   string `json:"reason"`
	Declared string `json:"declared"`
}

// gatePlanNoteBody / gatePlanNoteAt / gatePlanNoteID are the fixed plan note
// the gate tests pin: action=approved is UNCONDITIONALLY required to name a
// planNoteId (the plan pin is orthogonal to who approved), so every gate
// fixture that reaches the approval branch needs one real note in
// status.notes and its id in the payload.
const gatePlanNoteBody = "plan: do the thing"

var gatePlanNoteAt = metav1.NewTime(frozenNow.Add(-30 * time.Minute))
var gatePlanNoteID = tatarav1alpha1.NewNoteID(gatePlanNoteAt, "plan", gatePlanNoteBody)

func gatePlanNote() tatarav1alpha1.Note {
	return tatarav1alpha1.Note{
		ID: gatePlanNoteID, At: gatePlanNoteAt, Agent: "implement", Kind: "plan", Body: gatePlanNoteBody,
	}
}

// gateHandoffNote is a note the SAME live agent wrote on the SAME Task that is
// NOT a plan. It is the fixture for the plan pin's kind check: `planNoteId` is
// a client-supplied id and nothing about the wire says the agent must send the
// id of its plan.
const gateHandoffNoteBody = "handoff: picked the work up again after the TTL stop"

var gateHandoffNoteAt = metav1.NewTime(frozenNow.Add(-20 * time.Minute))
var gateHandoffNoteID = tatarav1alpha1.NewNoteID(gateHandoffNoteAt, "handoff", gateHandoffNoteBody)

func gateHandoffNote() tatarav1alpha1.Note {
	return tatarav1alpha1.Note{
		ID: gateHandoffNoteID, At: gateHandoffNoteAt, Agent: "implement",
		Kind: "handoff", Body: gateHandoffNoteBody,
	}
}

// gateSupersededPlanNote is an OLDER plan note, superseded by gatePlanNote.
// status.pinnedPlanNoteId is re-derived to the NEWEST plan note on every note
// write, so this one is a plan note the re-check will never read.
const gateSupersededPlanBody = "plan: an earlier, narrower plan"

var gateSupersededPlanAt = metav1.NewTime(frozenNow.Add(-90 * time.Minute))
var gateSupersededPlanID = tatarav1alpha1.NewNoteID(gateSupersededPlanAt, "plan", gateSupersededPlanBody)

func gateSupersededPlanNote() tatarav1alpha1.Note {
	return tatarav1alpha1.Note{
		ID: gateSupersededPlanID, At: gateSupersededPlanAt, Agent: "implement",
		Kind: "plan", Body: gateSupersededPlanBody,
	}
}

// gateTaskV2 is taskV2 plus the pinned plan note, for the gate's action=approved
// tests: Spec.Kind and status.agentKind are both "implement" post-#521 - the
// clarify kind is gone, folded entirely into implement.
func gateTaskV2(name, projectRef, state string) *tatarav1alpha1.Task {
	tk := taskV2(name, projectRef, "implement", state, "implement")
	tk.Status.Notes = []tatarav1alpha1.Note{gatePlanNote()}
	return tk
}

// gateTaskWithJournalV2 is gateTaskV2 with the journal written out in full,
// OLDEST FIRST, exactly as postNote grows it - pinnedPlanNoteId walks it
// backwards looking for the newest `plan`.
func gateTaskWithJournalV2(name, projectRef, state string, notes ...tatarav1alpha1.Note) *tatarav1alpha1.Task {
	tk := taskV2(name, projectRef, "implement", state, "implement")
	tk.Status.Notes = notes
	return tk
}

// Approval is in NO schema. The agent reports action=approved with a reason;
// the operator INDEPENDENTLY runs the C.6 citation check over EVERY owned Issue.
func TestOutcome_Gate_ImplementRequiresApprovalOnEveryOwnedIssue(t *testing.T) {
	i1 := issueV2("tatara-operator", 291, "t1")
	i2 := issueV2("tatara-operator", 292, "t1")

	// Only ONE of the two issues is approved: the SCOPE gate refuses (fix H9).
	e := buildV2(t, v2Opts{
		writer:   panicForge{},
		approval: &fakeApproval{grant: map[string]bool{i1.Name: true}},
	}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		gateTaskV2("t1", "tatara", tatarav1alpha1.StateRefined), i1, i2)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"approved","reason":"maintainer said go ahead","planNoteId":"`+gatePlanNoteID+`"}}`)
	require.Equal(t, http.StatusOK, w.Code)
	var resp gateResponseDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.False(t, resp.Granted, "a refusal is a 200 with granted:false, never a park")
	got := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.StateRefined, got.Status.State, "a refusal must not move the task")
	require.False(t, tatarav1alpha1.Parked(got), "#521: a gate refusal does NOT park; the agent is still alive")
	require.Empty(t, e.issue(t, i1.Name).Status.Approval, "nothing is stamped when the scope gate fails")

	// BOTH approved: the mandate is granted.
	e2 := buildV2(t, v2Opts{
		writer:   panicForge{},
		approval: &fakeApproval{grant: map[string]bool{i1.Name: true, i2.Name: true}},
	}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		gateTaskV2("t1", "tatara", tatarav1alpha1.StateRefined),
		issueV2("tatara-operator", 291, "t1"), issueV2("tatara-operator", 292, "t1"))

	w = e2.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"approved","reason":"maintainer said go ahead","planNoteId":"`+gatePlanNoteID+`"}}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, tatarav1alpha1.StateUnderImplementation, e2.task(t, "t1").Status.State,
		"a GRANT enters under-implementation (#521: was approved)")
	require.Equal(t, "approved", e2.issue(t, i1.Name).Status.Status)
	require.NotNil(t, e2.issue(t, i1.Name).Status.Approval)
}

// 2026-07-28 security review IMPORTANT 7: the entire safety claim behind
// Task 9's identity-unverified -> conversing widening is that conversing ->
// approved is gated by THIS handler's verifyApprovalScope, which runs the
// LIVE C.6 citation check per request. This test is what certifies that widening:
// a Task standing at conversing gets NO credit for anything that happened
// before it arrived there, so when the live citation check refuses, the Task
// goes straight back to parked(identity-unverified) with no approval stamped.
//
// It replaces TestOutcome_Conversing_ApprovalVerdictIsNeverConsulted, which
// made the same assertion three times over an absent / zero-value /
// stale-dated Task.status.approvalVerdict. That field is gone as of
// agent-judged-approval-gate step D, so "the stored verdict is never
// consulted" is now a property of the type system rather than of this
// handler, and one case carries the whole claim.
//
// Note for accuracy: a clarify agent standing at conversing CAN move a Task
// toward code execution, via decision=implement -> approved -> admission to
// implementing - "cannot reach implementing directly" only ever described
// the ONE edge, never the two-hop path through a GENUINE citation-check pass.
func TestOutcome_Conversing_ImplementRefusedWhenLiveCitationCheckFails(t *testing.T) {
	i1 := issueV2("tatara-operator", 291, "t1")
	// "conversing" is gone (#521: folded into StateRefined, one of the states
	// stage.Live now covers), but the property under test - a Task standing at
	// the gate gets NO credit for anything that happened before it arrived
	// there - is unchanged.
	task := gateTaskV2("t1", "tatara", tatarav1alpha1.StateRefined)

	// fakeApproval grants NOTHING: the live C.6 citation check fails on this request.
	e := buildV2(t, v2Opts{
		writer:   panicForge{},
		approval: &fakeApproval{grant: map[string]bool{}},
	}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"), task, i1)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"approved","reason":"the maintainer surely meant yes","planNoteId":"`+gatePlanNoteID+`"}}`)
	require.Equal(t, http.StatusOK, w.Code)
	var resp gateResponseDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.False(t, resp.Granted, "refined -> under-implementation must be refused when the live scope gate fails")

	got := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.StateRefined, got.Status.State)
	require.False(t, tatarav1alpha1.Parked(got),
		"#521: the OLD contract parked at identity-unverified; a refusal now keeps the agent live instead")
	require.Empty(t, e.issue(t, i1.Name).Status.Approval, "nothing is stamped when the live scope gate fails")
}

// An auto-approval through the clarify submit path (the primary auto-approve
// site: a bot proposal reaching implement) increments operator_auto_approve_total
// by proposal kind, so the last-gate-removed release is queryable without
// log-scraping (hard rule 13).
func TestOutcome_Gate_AutoApproveIncrementsCounter(t *testing.T) {
	i1 := issueV2("tatara-operator", 291, "t1", func(iss *tatarav1alpha1.Issue) {
		iss.Status.Author = "tatara-bot"
		iss.Status.Body = tatarav1alpha1.StampProposalMarker("do the thing", tatarav1alpha1.ProposalKindBrainstorm)
	})
	reg := prometheus.NewRegistry()
	metrics := obs.NewOperatorMetrics(reg)
	e := buildV2(t, v2Opts{
		writer:   panicForge{},
		metrics:  metrics,
		approval: &fakeApproval{grant: map[string]bool{i1.Name: true}, auto: true},
	}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		gateTaskV2("t1", "tatara", tatarav1alpha1.StateRefined), i1)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"approved","reason":"the brainstorm was the review","planNoteId":"`+gatePlanNoteID+`"}}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, tatarav1alpha1.StateUnderImplementation, e.task(t, "t1").Status.State)
	require.True(t, e.issue(t, i1.Name).Status.Approval.Auto, "evidence must record Auto:true")
	require.Equal(t, 1.0, testutil.ToFloat64(metrics.AutoApproveCounter(tatarav1alpha1.ProposalKindBrainstorm)))
}

// TestOutcome_Gate_CitationsReachTheVerifierVerbatim: the handler is a pipe.
// It parses the agent's citations off the gate payload and hands them to the
// verifier UNCHANGED, for every owned Issue - it never inspects, normalises or
// judges them itself. The whole verification lives in controller.verifyOneIssue.
func TestOutcome_Gate_CitationsReachTheVerifierVerbatim(t *testing.T) {
	i1 := issueV2("tatara-operator", 291, "t1")
	i2 := issueV2("tatara-operator", 292, "t1")
	fa := &fakeApproval{grant: map[string]bool{i1.Name: true, i2.Name: true}, needCitation: true}
	e := buildV2(t, v2Opts{writer: panicForge{}, approval: fa},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		gateTaskV2("t1", "tatara", tatarav1alpha1.StateRefined), i1, i2)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"approved","reason":"maintainer approved",`+
			`"planNoteId":"`+gatePlanNoteID+`","approvingMaintainer":"maintainer",`+
			`"approvalCitations":[{"id":"c-2","quote":"go ahead, I approve!"}]}}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, tatarav1alpha1.StateUnderImplementation, e.task(t, "t1").Status.State)

	require.Len(t, fa.gotCitations, 2, "every owned Issue is offered the citation set")
	for _, got := range fa.gotCitations {
		require.Equal(t, []tatarav1alpha1.ApprovalCitation{{ID: "c-2", Quote: "go ahead, I approve!"}}, got)
	}
	for _, d := range fa.gotDeclared {
		require.Equal(t, "maintainer", d, "declared must reach the verifier unchanged")
	}
}

// TestOutcome_Gate_CitationRefusalIs200AndDoesNotPark is the load-bearing
// contract shape. A missing or failing citation is a REFUSAL, not a client
// error: 200 + granted:false, and the Task stays exactly where it is - #521
// deleted the OLD park-at-identity-unverified behaviour outright, because
// under the merged model the agent is still alive and should be told no and
// keep talking, not stalled. Only malformed JSON and a missing action / blank
// reason stay 4xx.
func TestOutcome_Gate_CitationRefusalIs200AndDoesNotPark(t *testing.T) {
	for _, tc := range []struct {
		name     string
		body     string
		approval *fakeApproval
	}{
		{
			name: "no citations at all",
			body: `"action":"approved","reason":"maintainer-1 approved","planNoteId":"` + gatePlanNoteID + `"`,
			approval: &fakeApproval{grant: map[string]bool{"iss-tatara-operator-291": true},
				needCitation: true},
		},
		{
			name: "citations present but the verifier refuses them",
			body: `"action":"approved","reason":"x","planNoteId":"` + gatePlanNoteID + `",` +
				`"approvingMaintainer":"someone","approvalCitations":[{"id":"c-1","quote":"go ahead"}]`,
			// The verifier refuses (a non-maintainer's comment, a fabricated
			// quote, a replayed id - the reason is the controller's business).
			approval: &fakeApproval{grant: map[string]bool{}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			i1 := issueV2("tatara-operator", 291, "t1")
			e := buildV2(t, v2Opts{writer: panicForge{}, approval: tc.approval},
				projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
				gateTaskV2("t1", "tatara", tatarav1alpha1.StateRefined), i1)

			w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
				`{"kind":"implement","payload":{`+tc.body+`}}`)
			require.Equal(t, http.StatusOK, w.Code, "a refusal is never a 4xx")
			var resp gateResponseDTO
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
			require.False(t, resp.Granted)

			got := e.task(t, "t1")
			require.Equal(t, tatarav1alpha1.StateRefined, got.Status.State)
			require.False(t, tatarav1alpha1.Parked(got), "#521: a gate refusal does not park")
			require.Empty(t, e.issue(t, i1.Name).Status.Approval, "nothing is stamped on a refusal")
		})
	}
}

// TestOutcome_Gate_BadShapeStays4xx pins the OTHER side of the line: a
// payload the operator cannot even read is still a client error.
func TestOutcome_Gate_BadShapeStays4xx(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"malformed json", `{"action":`},
		{"unknown field", `{"action":"approved","reason":"x","planNoteId":"` + gatePlanNoteID + `","approvalCitation":[]}`},
		{"unknown field inside a citation", `{"action":"approved","reason":"x","planNoteId":"` + gatePlanNoteID + `",` +
			`"approvingMaintainer":"m","approvalCitations":[{"id":"c-1","text":"go ahead"}]}`},
		{"missing action", `{"reason":"x"}`},
		{"blank reason", `{"action":"approved","reason":"   ","planNoteId":"` + gatePlanNoteID + `"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := buildV2(t, v2Opts{writer: panicForge{},
				approval: &fakeApproval{grant: map[string]bool{"iss-tatara-operator-291": true}}},
				projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
				gateTaskV2("t1", "tatara", tatarav1alpha1.StateRefined),
				issueV2("tatara-operator", 291, "t1"))
			w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
				`{"kind":"implement","payload":`+tc.body+`}`)
			require.GreaterOrEqual(t, w.Code, 400)
			require.Less(t, w.Code, 500)
		})
	}
}

// TestOutcome_Gate_GrantedWithNilEvidenceIsNotWritten: a LIVE Issue that the
// verifier grants with NO evidence must not approve anything, at either of the
// two places it could. status=approved with a nil approval is an approved Issue
// with NO approver - it defeats the idempotence short-circuit, defeats the
// single-use replay guard, and projects tatara-approved to the forge with nobody
// behind it.
//
// WHICH GUARD ACTUALLY RUNS HERE, since this used to credit the wrong one. With
// grantWithNilEvidence the request is refused by verifyApprovalScope's
// `!ok || ev == nil` arm BEFORE the write loop is entered at all, so the
// writer's own nil guard never executes on this path - deleting it leaves this
// test green. Both assertions below still earn their place: they pin the OUTCOME
// (no Issue write, no Task advance) rather than a particular guard, and the Task
// half is the one that matters, because the earlier half-fix skipped the write
// and then let control fall through to stage.Enter(approved) anyway. The writer
// guard's own coverage is negative and lives in
// TestOutcome_Gate_ClosedIssueDoesNotBlockALiveOne.
func TestOutcome_Gate_GrantedWithNilEvidenceIsNotWritten(t *testing.T) {
	i1 := issueV2("tatara-operator", 291, "t1")
	e := buildV2(t, v2Opts{writer: panicForge{},
		approval: &fakeApproval{grant: map[string]bool{i1.Name: true}, grantWithNilEvidence: true}},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		gateTaskV2("t1", "tatara", tatarav1alpha1.StateRefined), i1)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"approved","reason":"x","planNoteId":"`+gatePlanNoteID+`",`+
			`"approvingMaintainer":"maintainer","approvalCitations":[{"id":"c-2","quote":"go ahead"}]}}`)
	require.Equal(t, http.StatusOK, w.Code)

	got := e.issue(t, i1.Name)
	require.Nil(t, got.Status.Approval)
	require.NotEqual(t, "approved", got.Status.Status,
		"an approver-less approval must never be written")
	// Skipping the Issue write is only HALF the guard, and asserting only that
	// half is what certified the hole below: control still fell through to
	// stage.Enter(under-implementation), so the Task advanced on an evidence
	// map that approved nothing.
	require.Equal(t, tatarav1alpha1.StateRefined, e.task(t, "t1").Status.State,
		"the Task must not reach under-implementation on an approver-less approval")
}

// TestOutcome_Clarify_ClosedIssueIsNotALicence is the gate hole this branch's
// review found, and it is reachable by a HUMAN, not just a misbehaving agent.
//
// ownedIssues returns every owned Issue whatever its state. The verifier returns
// (iss.Status.Approval, true) for an out-of-scope Issue - correct in isolation,
// since a closed thread is not pending approval and must not block the others -
// and that stored approval is routinely nil. verifyApprovalScope then refused
// only on len(issues)==0, so a Task whose ONLY Issue a human had CLOSED produced
// granted=true over an all-nil map: no citation, no maintainer comment, no
// evidence, and decision=implement walked to approved and on to implementing.
//
// A human CLOSING the issue is the strongest veto they have. It must not be the
// thing that releases the work. THE EMPTY SET IS NOT A LICENCE: all([]) == true
// must never gate code execution, and after the Task-level twin was deleted this
// loop is the only place that principle is enforced at all.
func TestOutcome_Gate_ClosedIssueIsNotALicence(t *testing.T) {
	closed := issueV2("tatara-operator", 291, "t1", func(i *tatarav1alpha1.Issue) {
		i.Status.State = "closed"
	})
	e := buildV2(t, v2Opts{writer: panicForge{}, approval: &fakeApproval{}},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		gateTaskV2("t1", "tatara", tatarav1alpha1.StateRefined), closed)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"approved","reason":"they closed it, so ship it",`+
			`"planNoteId":"`+gatePlanNoteID+`","approvingMaintainer":"maintainer",`+
			`"approvalCitations":[{"id":"c-2","quote":"go ahead"}]}}`)
	require.Equal(t, http.StatusOK, w.Code)

	got := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.StateRefined, got.Status.State,
		"a Task whose only Issue is CLOSED must not reach under-implementation")
	require.False(t, tatarav1alpha1.Parked(got), "#521: a gate refusal does not park")
	require.Nil(t, e.issue(t, closed.Name).Status.Approval)
}

// TestOutcome_Clarify_EmptyScopeRefusalIsAttributed covers the two refusal paths
// that used to be INVISIBLE. Both return before verifyApprovalScope ever calls
// VerifyApproval, and the reason constant, the
// operator_approval_refused_total{reason} increment and the
// action=approval_refused INFO line all live inside that call - so a Task with
// no live Issue parked with NO attribution of any kind.
//
// The closed-issue row is the one that matters operationally: it is a human
// CLOSING the only Issue of a clarify Task, the strongest veto they have, and
// the exact gate hole fix L3-14 closed. The park itself was always alerted
// (alerts/tatara-operator.yaml matches stageReason NEGATIVELY, so
// identity-unverified is covered), but a park alert with no reason attribution,
// next to a WARN naming the wrong cause, sent triage at the clarify agent
// instead of at the human's veto.
func TestOutcome_Gate_EmptyScopeRefusalIsAttributed(t *testing.T) {
	for _, tc := range []struct {
		name    string
		objs    []client.Object
		wantOwn float64
	}{
		{
			name: "every owned Issue is out of scope",
			objs: []client.Object{issueV2("tatara-operator", 291, "t1", func(i *tatarav1alpha1.Issue) {
				i.Status.State = "closed"
			})},
			wantOwn: 1,
		},
		{
			name:    "the Task owns no Issue at all",
			objs:    nil,
			wantOwn: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			metrics := obs.NewOperatorMetrics(reg)
			var logBuf bytes.Buffer
			base := []client.Object{projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
				gateTaskV2("t1", "tatara", tatarav1alpha1.StateRefined)}
			e := buildV2(t, v2Opts{writer: panicForge{}, metrics: metrics,
				logger:   slog.New(slog.NewJSONHandler(&logBuf, nil)),
				approval: &fakeApproval{}},
				append(base, tc.objs...)...)

			w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
				`{"kind":"implement","payload":{"action":"approved","reason":"ship it",`+
					`"planNoteId":"`+gatePlanNoteID+`","approvingMaintainer":"maintainer",`+
					`"approvalCitations":[{"id":"c-2","quote":"go ahead"}]}}`)
			require.Equal(t, http.StatusOK, w.Code)

			var resp gateResponseDTO
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
			require.False(t, resp.Granted)
			require.Equal(t, controller.ApprovalRefusedNoLiveIssue, resp.Reason)

			got := e.task(t, "t1")
			require.Equal(t, tatarav1alpha1.StateRefined, got.Status.State)
			require.False(t, tatarav1alpha1.Parked(got), "#521: a gate refusal does not park")

			require.Equal(t, float64(1),
				testutil.ToFloat64(metrics.ApprovalRefusedCounter(controller.ApprovalRefusedNoLiveIssue)),
				"the refusal must move operator_approval_refused_total{reason=no-live-issue}")

			line := findLogLine(t, logBuf.Bytes(), "approval_refused")
			require.Equal(t, controller.ApprovalRefusedNoLiveIssue, line["reason"])
			require.Equal(t, "t1", line["task"])

			// The WARN must name the condition and count LIVE Issues, not owned
			// ones. Reporting len(issues) here counted the very Issues the
			// refusal was about excluding.
			warn := findLogLineAtLevel(t, logBuf.Bytes(), "WARN")
			require.Equal(t, controller.ApprovalRefusedNoLiveIssue, warn["reason"])
			require.Equal(t, float64(0), warn["live_issues"],
				"no Issue was in scope; that is the whole refusal")
			require.Equal(t, tc.wantOwn, warn["owned_issues"])
		})
	}
}

// TestOutcome_Clarify_ClosedIssueDoesNotBlockALiveOne is the other half of the
// scope filter, and the reason it is a FILTER and not a blanket "every owned
// Issue must produce evidence". A human closing ONE issue of a multi-issue Task
// must not strand the rest: the closed thread drops out of the scope loop
// entirely rather than contributing a nil that refuses the whole Task.
//
// It also pins the SECOND place that filter has to be applied. The write loop
// iterates every OWNED Issue and looks each one up in the evidence map, which
// deliberately contains no out-of-scope entry - so without the same skip there,
// this ordinary SUCCESS logged "approval granted with nil evidence; refusing to
// write an approver-less approval" at ERROR, once per closed Issue, about an
// approver-less approval that never happened. Nothing was mis-written, but an
// ERROR naming a security failure on the happy path is a triage trap and
// violates hard rule 12's level discipline.
func TestOutcome_Gate_ClosedIssueDoesNotBlockALiveOne(t *testing.T) {
	live := issueV2("tatara-operator", 291, "t1")
	closed := issueV2("tatara-cli", 12, "t1", func(i *tatarav1alpha1.Issue) {
		i.Status.State = "closed"
	})
	var logBuf bytes.Buffer
	e := buildV2(t, v2Opts{writer: panicForge{},
		logger:   slog.New(slog.NewJSONHandler(&logBuf, nil)),
		approval: &fakeApproval{grant: map[string]bool{live.Name: true}, needCitation: true}},
		projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"), repoV2("tatara-cli", "tatara"),
		gateTaskV2("t1", "tatara", tatarav1alpha1.StateRefined), live, closed)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"approved","reason":"maintainer-1 said go ahead",`+
			`"planNoteId":"`+gatePlanNoteID+`","approvingMaintainer":"maintainer-1",`+
			`"approvalCitations":[{"id":"c-2","quote":"go ahead"}]}}`)
	require.Equal(t, http.StatusOK, w.Code)

	require.Equal(t, tatarav1alpha1.StateUnderImplementation, e.task(t, "t1").Status.State)
	require.Equal(t, "approved", e.issue(t, live.Name).Status.Status)
	require.NotEqual(t, "approved", e.issue(t, closed.Name).Status.Status,
		"the out-of-scope Issue is skipped, never written")
	// NOT a vacuous absence assertion: deleting the write loop's scope skip
	// makes this line RED (verified by mutation - the closed Issue's nil lookup
	// fires the ERROR). It is the only coverage the writer's nil guard has, and
	// it is what would catch the two scope filters drifting apart again.
	require.NotContains(t, logBuf.String(), "approver-less approval",
		"a successful approval alongside a closed Issue must not log an approver-less-approval ERROR")
	require.NotContains(t, logBuf.String(), `"level":"ERROR"`,
		"the happy path must log no ERROR at all")
}

// A nil verifier FAILS CLOSED.
func TestOutcome_Gate_NoVerifierFailsClosed(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		gateTaskV2("t1", "tatara", tatarav1alpha1.StateRefined),
		issueV2("tatara-operator", 291, "t1"))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"approved","reason":"trust me","planNoteId":"`+gatePlanNoteID+`"}}`)
	require.Equal(t, http.StatusOK, w.Code)
	var resp gateResponseDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.False(t, resp.Granted)
	got := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.StateRefined, got.Status.State)
	require.False(t, tatarav1alpha1.Parked(got), "#521: a refusal never parks, even fail-closed")
}

// TestOutcome_Clarify_GrantIsAuditedWithTheApprover is the counterpart to the
// refusal telemetry, and the asymmetry it closes is not cosmetic. A REFUSAL is
// recorded twice (operator_approval_refused_total{reason} and an
// action=approval_refused INFO line); the GRANT was recorded only as
// action=submit_outcome, outcome=implement, issues=N, which names neither the
// approver nor the comment. The only other record of who approved is
// Issue.Status.Approval, and that is ONE slot overwritten by the next approval
// on the same Issue - so without this line, "who released this change into
// push-CD" becomes unanswerable the moment a second approval lands. This is the
// most security-relevant business action in the system; hard rule 12 wants it at
// INFO with structured fields.
//
// The field names are pinned deliberately: they are exactly the ones the deleted
// controller-side writer used, so a log consumer written against either sees one
// shape rather than two.
func TestOutcome_Gate_GrantIsAuditedWithTheApprover(t *testing.T) {
	i1 := issueV2("tatara-operator", 291, "t1")
	var logBuf bytes.Buffer
	e := buildV2(t, v2Opts{writer: panicForge{},
		logger:   slog.New(slog.NewJSONHandler(&logBuf, nil)),
		approval: &fakeApproval{grant: map[string]bool{i1.Name: true}, needCitation: true}},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		gateTaskV2("t1", "tatara", tatarav1alpha1.StateRefined), i1)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"approved","reason":"maintainer said go ahead",`+
			`"planNoteId":"`+gatePlanNoteID+`","approvingMaintainer":"maintainer",`+
			`"approvalCitations":[{"id":"1","quote":"go ahead"}]}}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, tatarav1alpha1.StateUnderImplementation, e.task(t, "t1").Status.State)

	line := findLogLine(t, logBuf.Bytes(), "approval_verified")
	require.Equal(t, "INFO", line["level"], "the approval grant must be an INFO business action")
	require.Equal(t, i1.Name, line["issue"])
	require.Equal(t, "t1", line["task"])
	require.Equal(t, "maintainer", line["maintainer_login"],
		"the audit line must name the APPROVER; that is the whole point of it")
	require.Equal(t, "1", line["cited_comment_id"],
		"the audit line must name the comment the approval was built from")
	require.Equal(t, false, line["auto"])
}

// findLogLine returns the ONE JSON log record whose "action" field matches, and
// fails if there is not exactly one. Asserting on a substring of the buffer
// would pass on a line that carried the action but none of the fields, which is
// precisely the failure this test exists to catch.
func findLogLine(t *testing.T, buf []byte, action string) map[string]any {
	t.Helper()
	var found []map[string]any
	for _, raw := range bytes.Split(bytes.TrimSpace(buf), []byte("\n")) {
		if len(raw) == 0 {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal(raw, &rec); err != nil {
			continue
		}
		if rec["action"] == action {
			found = append(found, rec)
		}
	}
	require.Len(t, found, 1, "want exactly one action=%s log line, got %d", action, len(found))
	return found[0]
}

// findLogLineAtLevel is findLogLine for a record that carries no action field.
// The scope-refusal WARN is deliberately not an action= business-event line -
// the action=approval_refused INFO beside it is - so it can only be found by
// level.
func findLogLineAtLevel(t *testing.T, buf []byte, level string) map[string]any {
	t.Helper()
	var found []map[string]any
	for _, raw := range bytes.Split(bytes.TrimSpace(buf), []byte("\n")) {
		if len(raw) == 0 {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal(raw, &rec); err != nil {
			continue
		}
		if rec["level"] == level {
			found = append(found, rec)
		}
	}
	require.Len(t, found, 1, "want exactly one %s log line, got %d", level, len(found))
	return found[0]
}

// TestOutcome_Clarify_AlreadyApprovedIssueNeedsNoFreshCitation exercises
// verifyOneIssue's clause-2 idempotence THROUGH the handler. An Issue that
// already carries evidence is approved - clause (2) asks whether every live
// Issue CARRIES evidence, not whether it can be re-derived on this request -
// which is what keeps the autoApproveTataraProposals evidence (no comment to
// re-match) alive and stops a maintainer's later "thanks!" from revoking a grant
// already given.
//
// SCOPE CAVEAT, so this is not read as more than it is: the assertion runs
// against fakeApproval, so what it pins is the HANDLER's behaviour given a
// verifier that reports an already-approved Issue as approved - not
// verifyOneIssue's clause-2 idempotence itself, which is pinned directly in
// internal/controller (TestVerifyOneIssue_CitationFailClosedMatrix). The fake's
// already-approved arm is load-bearing for this test specifically: it grants
// NOTHING here and demands a citation, so removing that arm refuses the request
// and parks the Task.
func TestOutcome_Gate_AlreadyApprovedIssueNeedsNoFreshCitation(t *testing.T) {
	approvedAt := metav1.NewTime(frozenNow.Add(-time.Hour))
	i1 := issueV2("tatara-operator", 291, "t1", func(iss *tatarav1alpha1.Issue) {
		iss.Status.Status = "approved"
		iss.Status.Approval = &tatarav1alpha1.ApprovalEvidence{
			Login: "maintainer-1", CommentID: "c-1", Phrase: "go ahead", CreatedAt: approvedAt,
		}
	})
	e := buildV2(t, v2Opts{writer: panicForge{},
		approval: &fakeApproval{grant: map[string]bool{}, needCitation: true}},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		gateTaskV2("t1", "tatara", tatarav1alpha1.StateRefined), i1)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"approved","reason":"already approved last turn","planNoteId":"`+gatePlanNoteID+`"}}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, tatarav1alpha1.StateUnderImplementation, e.task(t, "t1").Status.State,
		"an Issue that already carries evidence must not need a fresh citation")

	got := e.issue(t, i1.Name).Status.Approval
	require.NotNil(t, got)
	require.Equal(t, "maintainer-1", got.Login, "the ORIGINAL approver must survive the re-verification")
	require.Equal(t, "c-1", got.CommentID)
}

func TestOutcome_Gate_DiscussParksAwaitingHuman(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateRefined, "implement"))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"discuss","reason":"needs a human"}}`)
	require.Equal(t, http.StatusOK, w.Code)
	got := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.StateRefined, got.Status.State, "a park never changes state")
	require.True(t, tatarav1alpha1.Parked(got))
	require.Equal(t, "awaiting-human", got.Status.ParkReason)
}

// An /outcome-driven park (this handler's own stage.Park call, not routed
// through controller.ParkTask) must still count against
// operator_task_parked_total - the same choke-point gap already closed for
// operator_task_terminal_total (D1) via commit()'s TaskTerminalEntry call.
// Regression coverage for the metric-wiring audit (issue #370).
func TestOutcome_Gate_DiscussParksAwaitingHuman_RecordsParkedMetric(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := obs.NewOperatorMetrics(reg)
	e := buildV2(t, v2Opts{writer: panicForge{}, metrics: metrics}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateRefined, "implement"))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"discuss","reason":"needs a human"}}`)
	require.Equal(t, http.StatusOK, w.Code)

	got := testutil.ToFloat64(metrics.TaskParkedCounter(tatarav1alpha1.StateRefined, "awaiting-human"))
	require.Equal(t, float64(1), got,
		"operator_task_parked_total{state=refined,parkReason=awaiting-human} must record the outcome-driven park")
}

func TestOutcome_Gate_RejectedRejectsAndQueuesTheIssueClose(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateRefined, "implement"),
		issueV2("tatara-operator", 291, "t1"))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"rejected","reason":"wont fix"}}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, tatarav1alpha1.StateRejected, e.task(t, "t1").Status.State)
	require.Len(t, e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 291)).Status.PendingComments, 1)
}

func TestOutcome_Gate_ReasonAlwaysRequired(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateRefined, "implement"))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"discuss"}}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

// --- new #521 coverage: the fold, the required-tests list ------------------

// TestOutcome_KindClarifyIsRejected: `clarify` is not an alias for `implement`,
// deliberately. Its decisions folded into implement's action enum, but the
// KIND itself is gone: a pod that still sends kind=clarify hits the unknown-
// kind arm, same as any other bogus kind.
func TestOutcome_KindClarifyIsRejected(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		// agentKind is "clarify" too, so the kind gate passes on the matching
		// bogus kind and the request reaches the switch's default arm.
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateRefined, "clarify"))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"clarify","payload":{"decision":"discuss","reason":"r"}}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "unknown outcome kind")
}

// TestOutcome_ImplementSubmittedStillWorksUnderTheMergedKind: the fold only
// ADDED the gate actions to implement's action enum; the pre-existing code
// path (action=submitted from under-implementation) must not have regressed.
func TestOutcome_ImplementSubmittedStillWorksUnderTheMergedKind(t *testing.T) {
	e := buildV2(t, v2Opts{writer: &reviewPanicForge{heads: map[int]string{295: "live-head"}}},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		mrV2("tatara-operator", 295, "t1"))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"submitted","title":"T","body":"B","changeSignificance":"patch"}}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, tatarav1alpha1.StateAwaitingReview, e.task(t, "t1").Status.State,
		"the code path did not regress under the merged kind")
}

// TestOutcome_Gate_RefusalIs200AndDoesNotPark pins the exact refusal body
// shape and the state/park invariant in one place, independent of which
// verifier clause refused.
func TestOutcome_Gate_RefusalIs200AndDoesNotPark(t *testing.T) {
	i1 := issueV2("tatara-operator", 291, "t1")
	e := buildV2(t, v2Opts{writer: panicForge{},
		approval: &fakeApproval{grant: map[string]bool{}, refusalReason: controller.ApprovalRefusedNoMaintainer}},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		gateTaskV2("t1", "tatara", tatarav1alpha1.StateRefined), i1)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"approved","reason":"x","planNoteId":"`+gatePlanNoteID+`",`+
			`"approvingMaintainer":"someone","approvalCitations":[{"id":"c-1","quote":"go ahead"}]}}`)
	require.Equal(t, http.StatusOK, w.Code, "a gate refusal is a 200, never a 4xx")

	var resp gateResponseDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.False(t, resp.Granted)
	require.Equal(t, controller.ApprovalRefusedNoMaintainer, resp.Reason)
	require.Equal(t, "someone", resp.Declared)

	got := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.StateRefined, got.Status.State,
		"the OLD contract parked at identity-unverified; #521 keeps the agent live instead")
	require.False(t, tatarav1alpha1.Parked(got), "a gate refusal must NEVER park")
}

// TestOutcome_Gate_PlanNoteIdMustNameThePlanNote is THE PLAN PIN'S OWN
// AUTHENTICITY CHECK.
//
// The pin is the anti-scope-drift control: it is supposed to guarantee that the
// code that ships is the change the plan the maintainer approved describes. It
// works by hashing a note at grant and re-hashing it at submit - so WHICH note
// is hashed is the whole control. `planNoteId` is CLIENT-SUPPLIED and the wire
// says nothing about its kind, so an agent may send the id of any note it has
// written: a handoff note, a turn note, a superseded plan. Resolving it by id
// ALONE hashed whatever it named and called that the approved plan.
//
// The re-check resolves the plan note a DIFFERENT way - the newest note of kind
// `plan` (status.pinnedPlanNoteId) - so the two must agree or the pin proves
// nothing whichever way it lands: an id naming a note the re-check never reads
// either defeats the control (nothing to compare, no drift ever detected) or
// fires it spuriously (a mismatch on a plan nobody touched).
func TestOutcome_Gate_PlanNoteIdMustNameThePlanNote(t *testing.T) {
	for _, tc := range []struct {
		name   string
		notes  []tatarav1alpha1.Note
		citeID string
	}{
		{
			// The DEFEAT: a note of another kind entirely.
			name:   "a handoff note is not a plan note",
			notes:  []tatarav1alpha1.Note{gatePlanNote(), gateHandoffNote()},
			citeID: gateHandoffNoteID,
		},
		{
			// The SPURIOUS FIRE: a real plan note, but not the one the re-check
			// reads, so the very next untouched submit reports drift.
			name:   "a superseded plan note is not the plan note",
			notes:  []tatarav1alpha1.Note{gateSupersededPlanNote(), gatePlanNote()},
			citeID: gateSupersededPlanID,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			i1 := issueV2("tatara-operator", 291, "t1")
			e := buildV2(t, v2Opts{writer: panicForge{},
				approval: &fakeApproval{grant: map[string]bool{i1.Name: true}}},
				projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
				gateTaskWithJournalV2("t1", "tatara", tatarav1alpha1.StateRefined, tc.notes...), i1)

			w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
				`{"kind":"implement","payload":{"action":"approved","reason":"maintainer said go ahead",`+
					`"planNoteId":"`+tc.citeID+`"}}`)
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())

			var resp gateResponseDTO
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
			require.False(t, resp.Granted, "the gate must not grant on a pin it cannot re-check")
			require.Equal(t, controller.ApprovalRefusedPlanNoteNotPlan, resp.Reason)
			require.Equal(t, "plan-note-not-plan", resp.Reason,
				"the refusal reason is the wire vocabulary the agent branches on")

			got := e.task(t, "t1")
			require.Equal(t, tatarav1alpha1.StateRefined, got.Status.State,
				"a refused gate leaves the agent live at the gate; it never reaches code")
			require.False(t, tatarav1alpha1.Parked(got), "a gate refusal must NEVER park")
			require.Nil(t, e.issue(t, i1.Name).Status.Approval,
				"nothing is approved and nothing is pinned when the gate refuses")
		})
	}
}

// TestOutcome_Gate_TheGrantPinsTheSameNoteTheRecheckReads is the other half:
// the grant and the re-check must resolve the SAME note, end to end, or the pin
// is decorative. It walks a real Task through both halves of the control - the
// grant that takes the hash, then the submit that re-takes it - over a journal
// that carries decoy notes of other kinds, and asserts the hash the grant
// stored is the hash of the note the re-check re-reads.
func TestOutcome_Gate_TheGrantPinsTheSameNoteTheRecheckReads(t *testing.T) {
	i1 := issueV2("tatara-operator", 291, "t1")
	task := gateTaskWithJournalV2("t1", "tatara", tatarav1alpha1.StateRefined,
		gateSupersededPlanNote(), gatePlanNote(), gateHandoffNote())
	e := buildV2(t, v2Opts{writer: &reviewPanicForge{heads: map[int]string{295: "live-head"}},
		approval: &fakeApproval{grant: map[string]bool{i1.Name: true}}},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		task, i1, mrV2("tatara-operator", 295, "t1"))

	// THE GRANT, citing the Task's actual plan note.
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"approved","reason":"maintainer said go ahead",`+
			`"planNoteId":"`+gatePlanNoteID+`"}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Equal(t, tatarav1alpha1.StateUnderImplementation, e.task(t, "t1").Status.State)

	ev := e.issue(t, i1.Name).Status.Approval
	require.NotNil(t, ev)
	require.Equal(t, fmt.Sprintf("%x", sha256.Sum256([]byte(gatePlanNoteBody))), ev.PlanHash,
		"the grant pins the PLAN note, not the newest note and not whatever id it was handed")

	// THE RE-CHECK, with nothing touched. It resolves the same note, so it agrees.
	w = e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"submitted","title":"T","body":"B","changeSignificance":"patch"}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), controller.ApprovalRefusedPlanHashMismatch,
		"an untouched plan must never read as drift: that is grant and re-check disagreeing")
	require.Equal(t, tatarav1alpha1.StateAwaitingReview, e.task(t, "t1").Status.State)
}

// TestOutcome_Gate_AutoApprovePathGrantsWithNeitherGateField: neither
// approvingMaintainer nor approvalCitations is present - the auto-approve
// path - and the verifier grants.
func TestOutcome_Gate_AutoApprovePathGrantsWithNeitherGateField(t *testing.T) {
	i1 := issueV2("tatara-operator", 291, "t1", func(iss *tatarav1alpha1.Issue) {
		iss.Status.Author = "tatara-bot"
		iss.Status.Body = tatarav1alpha1.StampProposalMarker("do the thing", tatarav1alpha1.ProposalKindBrainstorm)
	})
	e := buildV2(t, v2Opts{writer: panicForge{},
		approval: &fakeApproval{grant: map[string]bool{i1.Name: true}, auto: true}},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		gateTaskV2("t1", "tatara", tatarav1alpha1.StateRefined), i1)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"approved","reason":"bot proposal, auto-approved",`+
			`"planNoteId":"`+gatePlanNoteID+`"}}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, tatarav1alpha1.StateUnderImplementation, e.task(t, "t1").Status.State)
	require.True(t, e.issue(t, i1.Name).Status.Approval.Auto)
}

// TestOutcome_Gate_AutoApprovePathIsRefusedWhenTheVerifierSaysNoCitation: the
// auto-approve path (neither gate field) is still a REAL verification, not a
// rubber stamp - a verifier that wants a citation and gets none still refuses.
func TestOutcome_Gate_AutoApprovePathIsRefusedWhenTheVerifierSaysNoCitation(t *testing.T) {
	i1 := issueV2("tatara-operator", 291, "t1")
	e := buildV2(t, v2Opts{writer: panicForge{},
		approval: &fakeApproval{grant: map[string]bool{i1.Name: true}, needCitation: true}},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		gateTaskV2("t1", "tatara", tatarav1alpha1.StateRefined), i1)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"approved","reason":"trust me","planNoteId":"`+gatePlanNoteID+`"}}`)
	require.Equal(t, http.StatusOK, w.Code)

	var resp gateResponseDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.False(t, resp.Granted)
	require.Equal(t, "no-citation", resp.Reason)
	require.Equal(t, controller.ApprovalRefusedNoCitation, resp.Reason)

	got := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.StateRefined, got.Status.State)
	require.False(t, tatarav1alpha1.Parked(got))
}

// TestOutcome_Gate_DeclaredKeyIsAlwaysPresentOnRefusal: `declared` is a
// DEFINED field on every refusal body, including the auto-approve path where
// the agent legitimately sent no approvingMaintainer - "" there means "the
// agent declared no approver", not "the field never showed up". gateResponseDTO
// above cannot see the difference between an empty string and an absent key
// (it has no omitempty of its own), so this test decodes the RAW body into a
// map and checks for the key's presence with the two-value lookup instead.
func TestOutcome_Gate_DeclaredKeyIsAlwaysPresentOnRefusal(t *testing.T) {
	i1 := issueV2("tatara-operator", 291, "t1")
	e := buildV2(t, v2Opts{writer: panicForge{},
		approval: &fakeApproval{grant: map[string]bool{i1.Name: true}, needCitation: true}},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		gateTaskV2("t1", "tatara", tatarav1alpha1.StateRefined), i1)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"approved","reason":"trust me","planNoteId":"`+gatePlanNoteID+`"}}`)
	require.Equal(t, http.StatusOK, w.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &raw))
	require.False(t, raw["granted"].(bool))
	declared, present := raw["declared"]
	require.True(t, present, "declared must be present on every refusal body, even when the agent sent none")
	require.Equal(t, "", declared, "the auto-approve path legitimately sent no approvingMaintainer")
}

// TestOutcome_Gate_OneOfThePairWithoutTheOtherIsRefused: approvingMaintainer
// and approvalCitations travel as a PAIR. Both present is a human-cited
// approval, both absent is the autoApproveTataraProposals path; one without the
// other is a client bug, refused BEFORE any verifier call - 400 both ways round.
//
// It is refused UP FRONT, not by the verifier, because a citation with no
// declared approver skips the two cross-checks the declaration exists for
// (approver-not-maintainer, approver-mismatch) and a declared approver with no
// citation has nothing to be checked against - so a verifier that saw either
// half alone would answer a question nobody asked. The refusal is counted under
// its own reason so a client sending half a pair is visible as itself rather
// than as generic gate noise.
func TestOutcome_Gate_OneOfThePairWithoutTheOtherIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{
			name: "approvingMaintainer without approvalCitations",
			body: `"action":"approved","reason":"x","planNoteId":"` + gatePlanNoteID + `","approvingMaintainer":"maintainer"`,
		},
		{
			name: "approvalCitations without approvingMaintainer",
			body: `"action":"approved","reason":"x","planNoteId":"` + gatePlanNoteID +
				`","approvalCitations":[{"id":"c-1","quote":"go ahead"}]`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := testutil.ToFloat64(
				obs.RestOutcomeRejectedTotal.WithLabelValues("implement", "gate-pair-mismatch"))
			approval := &fakeApproval{}
			e := buildV2(t, v2Opts{writer: panicForge{}, approval: approval},
				projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
				gateTaskV2("t1", "tatara", tatarav1alpha1.StateRefined),
				issueV2("tatara-operator", 291, "t1"))
			w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"implement","payload":{`+tc.body+`}}`)
			require.Equal(t, http.StatusBadRequest, w.Code)
			require.Contains(t, w.Body.String(),
				"approvingMaintainer and approvalCitations must both be present or both be absent")

			require.Empty(t, approval.gotCitations, "the pair rule refuses BEFORE any verification runs")

			after := testutil.ToFloat64(
				obs.RestOutcomeRejectedTotal.WithLabelValues("implement", "gate-pair-mismatch"))
			require.Equal(t, before+1, after,
				"the refusal is counted under its own reason, not folded into a generic one")

			got := e.task(t, "t1")
			require.Equal(t, tatarav1alpha1.StateRefined, got.Status.State, "a refused gate moves nothing")
			require.False(t, tatarav1alpha1.Parked(got))
			require.Empty(t, e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 291)).Status.Approval,
				"nothing is stamped when the pair rule refuses")
		})
	}
}

// TestOutcome_Gate_MissingPlanNoteIdIsRefused pins design addendum item 10's
// fifth required gate test: planNoteId STAYS unconditionally required on
// action=approved on BOTH the human-cited and the auto-approve paths, because
// the plan pin is orthogonal to who approved - an agent that names a citation
// or skips the pair entirely still has to name the plan note it wants pinned.
// It is refused UP FRONT as a missing required field (400, same class as a
// missing action or blank reason), before findNote or the verifier ever runs.
func TestOutcome_Gate_MissingPlanNoteIdIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{
			name: "human-cited path without planNoteId",
			body: `"action":"approved","reason":"x","approvingMaintainer":"maintainer",` +
				`"approvalCitations":[{"id":"c-1","quote":"go ahead"}]`,
		},
		{
			name: "auto-approve path without planNoteId",
			body: `"action":"approved","reason":"x"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := testutil.ToFloat64(
				obs.RestOutcomeRejectedTotal.WithLabelValues("implement", "missing-field"))
			approval := &fakeApproval{grant: map[string]bool{"iss-tatara-operator-291": true}}
			e := buildV2(t, v2Opts{writer: panicForge{}, approval: approval},
				projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
				gateTaskV2("t1", "tatara", tatarav1alpha1.StateRefined),
				issueV2("tatara-operator", 291, "t1"))
			w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"implement","payload":{`+tc.body+`}}`)
			require.Equal(t, http.StatusBadRequest, w.Code)
			require.Contains(t, w.Body.String(), "action=approved requires planNoteId")

			require.Empty(t, approval.gotCitations, "planNoteId is checked BEFORE any verification runs")

			after := testutil.ToFloat64(
				obs.RestOutcomeRejectedTotal.WithLabelValues("implement", "missing-field"))
			require.Equal(t, before+1, after, "the refusal is counted as a missing-field rejection")

			got := e.task(t, "t1")
			require.Equal(t, tatarav1alpha1.StateRefined, got.Status.State, "a refused gate moves nothing")
			require.False(t, tatarav1alpha1.Parked(got))
			require.Empty(t, e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 291)).Status.Approval,
				"nothing is stamped when planNoteId is missing")
		})
	}
}

// planPinnedFixtureV2 builds an implement Task MID-IMPLEMENTATION under a
// grant, plus the owned Issue carrying the approval evidence the gate wrote.
// grantBody is the plan note's body AS IT STOOD AT GRANT (what
// ApprovalEvidence.PlanHash was taken over); planBody is its body NOW.
// Passing a planBody != grantBody IS the agent rewriting the approved plan
// after the grant - the exact artifact the merged model created, because the
// same live agent is now approved and implements, instead of a fresh implement
// pod starting after a clarify Task ended.
func planPinnedFixtureV2(grantBody, planBody string) (*tatarav1alpha1.Task, *tatarav1alpha1.Issue) {
	task := taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement")
	task.Status.Notes = []tatarav1alpha1.Note{{
		ID: gatePlanNoteID, At: gatePlanNoteAt, Agent: "implement", Kind: "plan", Body: planBody,
	}}
	task.Status.PinnedPlanNoteID = gatePlanNoteID
	iss := issueV2("tatara-operator", 291, "t1", func(is *tatarav1alpha1.Issue) {
		is.Status.Status = "approved"
		is.Status.Approval = &tatarav1alpha1.ApprovalEvidence{
			Login: "maintainer", CommentID: "c-1", Phrase: "go ahead",
			PlanHash: fmt.Sprintf("%x", sha256.Sum256([]byte(grantBody))),
		}
	})
	return task, iss
}

// TestOutcome_Implement_PlanHashMismatchSendsTheTaskBackToTheGate is the
// ANTI-SCOPE-DRIFT control, and it owns a live table edge
// (`under-implementation -> refined`) that nothing else exercises.
//
// The plan note is the artifact the gate approved and the agent can edit
// afterwards. A submitted outcome whose plan no longer hashes to the value
// pinned at grant is shipping code nobody approved, so it MUST NOT reach
// awaiting-review: it goes back to `refined` to ask again - the CHEAP path out,
// never a park, because an agent that finds the plan gate expensive would
// simply stop updating its plan note.
func TestOutcome_Implement_PlanHashMismatchSendsTheTaskBackToTheGate(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := obs.NewOperatorMetrics(reg)
	task, iss := planPinnedFixtureV2(gatePlanNoteBody, "plan: actually rewrite the whole scheduler")
	e := buildV2(t, v2Opts{writer: panicForge{}, metrics: metrics},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		task, iss, mrV2("tatara-operator", 295, "t1"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"submitted","title":"T","body":"B","changeSignificance":"patch"}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	var resp gateResponseDTO
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.False(t, resp.Granted, "a refusal is a 200 with granted:false")
	require.Equal(t, controller.ApprovalRefusedPlanHashMismatch, resp.Reason)
	require.Equal(t, "plan-hash-mismatch", resp.Reason,
		"the refusal reason is the wire vocabulary the agent branches on")

	got := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.StateRefined, got.Status.State,
		"the CHEAP path out is back to the gate, never a park")
	require.False(t, tatarav1alpha1.Parked(got))
	require.Empty(t, got.Spec.MergeOrder, "no code shipped, so mergeOrder is never resolved")
	require.Empty(t, e.mr(t, tatarav1alpha1.MergeRequestName("tatara-operator", 295)).Status.Significance,
		"a refused submit must not label the MR for release")

	require.Equal(t, float64(1),
		testutil.ToFloat64(metrics.ApprovalRefusedCounter(controller.ApprovalRefusedPlanHashMismatch)))
}

// TestOutcome_Implement_SubmittedShipsWhenThePlanStillMatchesItsPin is the
// other half, so the refusal above cannot pass vacuously: the SAME fixture with
// an untouched plan note ships normally.
func TestOutcome_Implement_SubmittedShipsWhenThePlanStillMatchesItsPin(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := obs.NewOperatorMetrics(reg)
	task, iss := planPinnedFixtureV2(gatePlanNoteBody, gatePlanNoteBody)
	e := buildV2(t, v2Opts{writer: &reviewPanicForge{heads: map[int]string{295: "live-head"}}, metrics: metrics},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		task, iss, mrV2("tatara-operator", 295, "t1"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"submitted","title":"T","body":"B","changeSignificance":"patch"}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	got := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.StateAwaitingReview, got.Status.State)
	require.Equal(t, []string{"tatara-operator"}, got.Spec.MergeOrder)
	require.Equal(t, "patch",
		e.mr(t, tatarav1alpha1.MergeRequestName("tatara-operator", 295)).Status.Significance)

	require.Equal(t, float64(0),
		testutil.ToFloat64(metrics.ApprovalRefusedCounter(controller.ApprovalRefusedPlanHashMismatch)))
}

// TestOutcome_ImplementReasonLegalityIsDecidedByAction is the operator's own
// half of the frozen wire contract: there is ONE `reason` key, and WHICH
// actions may carry it is decided by `action`. tatara-cli made exactly one of
// its two agent-facing arguments legal per action (submitted/declined refuse
// the gate `reason`; approved/discuss/rejected refuse `decline_reason`), which
// collapses on the wire to one key with per-action legality. The operator is
// the trust boundary and enforces it itself, because an old or hand-rolled
// client can send anything.
func TestOutcome_ImplementReasonLegalityIsDecidedByAction(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		code int
	}{
		{"submitted refuses reason", `"action":"submitted","title":"T","body":"B","changeSignificance":"patch","reason":"r"`, http.StatusBadRequest},
		{"declined requires reason", `"action":"declined"`, http.StatusBadRequest},
		{"declined accepts reason", `"action":"declined","reason":"already fixed"`, http.StatusOK},
		{"approved requires reason", `"action":"approved","planNoteId":"x"`, http.StatusBadRequest},
		{"discuss requires reason", `"action":"discuss"`, http.StatusBadRequest},
		{"rejected requires reason", `"action":"rejected"`, http.StatusBadRequest},
		// The invented key is gone from the schema, so it is now an unknown
		// field on every action - the operator can never grow a second contract
		// behind tatara-cli's back again.
		{"declineReason is unknown on declined", `"action":"declined","declineReason":"y"`, http.StatusBadRequest},
		{"declineReason is unknown on approved", `"action":"approved","reason":"x","declineReason":"nope"`, http.StatusBadRequest},
		{"declineReason is unknown on discuss", `"action":"discuss","reason":"x","declineReason":"nope"`, http.StatusBadRequest},
		{"declineReason is unknown on rejected", `"action":"rejected","reason":"x","declineReason":"nope"`, http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
				repoV2("tatara-operator", "tatara"),
				taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"))
			w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"implement","payload":{`+tc.body+`}}`)
			require.Equal(t, tc.code, w.Code, "%s: %s", tc.name, w.Body.String())
		})
	}
}

// TestApprovalRefusalVocabularyIsEleven pins the CLOSED vocabulary's size and
// shape: exactly eleven reasons, every one lowercase-hyphenated (the label
// value on operator_approval_refused_total{reason} and the folded gate's 200
// body). The eleventh is plan-note-not-plan, the plan pin's authenticity check.
func TestApprovalRefusalVocabularyIsEleven(t *testing.T) {
	require.Len(t, controller.ApprovalRefusals, 11)
	for _, r := range controller.ApprovalRefusals {
		require.Regexp(t, `^[a-z][a-z-]*[a-z]$`, r, "refusal reason %q must be lowercase-hyphenated", r)
	}
}

// --- brainstorm -----------------------------------------------------------

// Each proposal becomes its OWN new implement Task (the gate mint target;
// #521 folded clarify's kind away and the CRD enum no longer accepts it),
// owning its OWN Issue.
func TestOutcome_Brainstorm_ProposeSpawnsAGateTaskPerProposal(t *testing.T) {
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StateRefined, "brainstorm"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"brainstorm","payload":{
	  "action":"propose","proposals":[
	    {"repo":"tatara-operator","title":"one","body":"b","kind":"bug"},
	    {"repo":"tatara-operator","title":"two","body":"b","kind":"improvement"}]}}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, tatarav1alpha1.StateDone, e.task(t, "t1").Status.State)
	require.Empty(t, e.task(t, "t1").Status.DocumentedBy,
		"a brainstorm never spawns a docs task (fix 25)")

	var tasks tatarav1alpha1.TaskList
	require.NoError(t, e.c.List(context.Background(), &tasks, client.InNamespace(ns)))
	gateTasks := 0
	for i := range tasks.Items {
		if tasks.Items[i].Spec.Kind == "implement" {
			gateTasks++
			// The gate Task is minted PARKED on a flag-off project: nobody has
			// engaged with the proposal yet, so it must not cost a pod.
			require.Equal(t, tatarav1alpha1.StateNew, tasks.Items[i].Spec.InitialState)
			require.Equal(t, stage.ReasonBacklogSweep, tasks.Items[i].Spec.InitialParkReason)
		}
	}
	require.Equal(t, 2, gateTasks)
	require.Len(t, e.forge.createdRefs, 2)

	// Each new gate Task controller-owns its own Issue.
	var issues tatarav1alpha1.IssueList
	require.NoError(t, e.c.List(context.Background(), &issues, client.InNamespace(ns)))
	require.Len(t, issues.Items, 2)
	for i := range issues.Items {
		require.True(t, *issues.Items[i].OwnerReferences[0].Controller)
		require.NotEqual(t, "t1", issues.Items[i].OwnerReferences[0].Name)
	}
}

// The mint park is the MIRROR of the approval carve-out, so a project that has
// autoApproveTataraProposals ON keeps today's behaviour byte for byte: the gate
// Task mints UN-parked, triage routes it to `refined`, and the agent that runs
// there is granted by autoApproveApplies without a maintainer comment.
func TestOutcome_Brainstorm_ProposedGateTaskIsLiveWhenAutoApproveIsOn(t *testing.T) {
	proj := projectV2("tatara")
	proj.Spec.AutoApproveTataraProposals = true
	e := buildV2(t, v2Opts{}, proj, scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StateRefined, "brainstorm"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"brainstorm","payload":{
	  "action":"propose","proposals":[
	    {"repo":"tatara-operator","title":"one","body":"b","kind":"bug"}]}}`)
	require.Equal(t, http.StatusOK, w.Code)

	var tasks tatarav1alpha1.TaskList
	require.NoError(t, e.c.List(context.Background(), &tasks, client.InNamespace(ns)))
	gateTasks := 0
	for i := range tasks.Items {
		if tasks.Items[i].Spec.Kind == "implement" {
			gateTasks++
			require.Equal(t, tatarav1alpha1.StateNew, tasks.Items[i].Spec.InitialState)
			require.Empty(t, tasks.Items[i].Spec.InitialParkReason,
				"the flag ON must mint the gate Task un-parked, exactly as before")
		}
	}
	require.Equal(t, 1, gateTasks)
}

// The brainstorm propose path stamps the tatara-proposed-by:brainstorm marker on
// BOTH the forge issue body and the minted Issue CR body - the marker factor of
// the autoApproveTataraProposals carve-out, durable across a mirror refresh.
func TestOutcome_Brainstorm_StampsProposalMarker(t *testing.T) {
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StateRefined, "brainstorm"))

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
	require.Equal(t, tatarav1alpha1.ComputeProposalContentHash(issues.Items[0].Status.Body),
		issues.Items[0].Spec.ProposalBodyHash,
		"Issue CR Spec must anchor the filing-time body hash")
	require.True(t, tatarav1alpha1.ProposalBodyMatchesAnchor(
		issues.Items[0].Status.Body, issues.Items[0].Spec.ProposalBodyHash))
	require.Equal(t, tatarav1alpha1.ProposalKindBrainstorm, issues.Items[0].Spec.ProposalKind,
		"Issue CR Spec must carry the DURABLE provenance the backlog counts on")
}

func TestOutcome_Brainstorm_ProposalsAreCappedAt5(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StateRefined, "brainstorm"))
	p := `{"repo":"tatara-operator","title":"x","body":"b","kind":"bug"}`
	body := `{"kind":"brainstorm","payload":{"action":"propose","proposals":[` +
		p + "," + p + "," + p + "," + p + "," + p + "," + p + `]}}`
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", body)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

// --- incident -------------------------------------------------------------

func TestOutcome_Incident_FileIssueCreatesTheTrackerUnderThisTask(t *testing.T) {
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StateRefined, "incident"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"incident","payload":{
	  "action":"file_issue","alertRules":["tatara-operator-down"],"reason":"real outage",
	  "issue":{"repo":"tatara-operator","title":"operator down","body":"trace"}}}`)
	require.Equal(t, http.StatusOK, w.Code)

	got := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.StateDone, got.Status.State,
		"filing the tracker FINISHES the incident Task: it opens no MR, so `refined -> done` is its path out")
	require.Equal(t, []string{"tatara-operator-down"}, got.Spec.AlertRules,
		"alertRules are merged into spec by the OPERATOR; spec is agent-unwritable")

	iss := e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 101))
	require.Equal(t, "t1", iss.OwnerReferences[0].Name)
	require.True(t, *iss.OwnerReferences[0].Controller)
}

// TestOutcome_Incident_FileIssueAtTheReachableStateFilesExactlyOneForgeIssue is
// the REACHABLE (state, agentKind) pair: AgentKindFor(refined, spec.kind
// incident) is "incident", so `refined` is the only state an incident pod ever
// runs at and the only state a file_issue outcome can arrive from. The old
// fixture built the Task at `new`, where AgentKindFor returns "" for every
// spec.kind - an unreachable pair that hid an illegal transition behind a
// legal-only-from-`new` edge.
//
// file_issue is non-idempotent (it mints a forge issue), so a 409 here is not a
// cosmetic wrong status code: o.conflict RELEASES the outcome claim, the agent
// retries, and CreateIssue fires again, unboundedly.
func TestOutcome_Incident_FileIssueAtTheReachableStateFilesExactlyOneForgeIssue(t *testing.T) {
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StateRefined, "incident"))

	body := `{"kind":"incident","payload":{
	  "action":"file_issue","alertRules":["tatara-operator-down"],"reason":"real outage",
	  "issue":{"repo":"tatara-operator","title":"operator down","body":"trace"}}}`

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", body)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Len(t, e.forge.createdReqs, 1, "exactly one forge issue for one file_issue")

	got := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.StateDone, got.Status.State,
		"a filed tracker is the incident Task's terminal: it opens no MR, so `refined -> done` is its only path out")
	require.Empty(t, got.Status.StateReason, "an ordinary delivery carries no reason")
	require.False(t, tatarav1alpha1.Parked(got))
	require.Len(t, got.Status.Notes, 1)
	require.Equal(t, "file_issue: real outage", got.Status.Notes[0].Body)

	// THE RETRY. A released claim is what turns a 409 into unbounded
	// duplication, so the second identical call must mint nothing.
	w2 := e.do(t, http.MethodPost, "/tasks/t1/outcome", body)
	require.NotEqual(t, http.StatusConflict, w2.Code, "the retry must not 409 the claim back open")
	require.Len(t, e.forge.createdReqs, 1, "a retry must never mint a second forge issue")
}

// The incident file_issue path stamps the tatara-proposed-by:incident marker on
// BOTH the forge issue body and the minted Issue CR body (the carve-out's marker
// factor for alert-driven incident issues).
func TestOutcome_Incident_StampsProposalMarker(t *testing.T) {
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StateRefined, "incident"))

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
	require.Equal(t, tatarav1alpha1.ComputeProposalContentHash(iss.Status.Body),
		iss.Spec.ProposalBodyHash,
		"Issue CR Spec must anchor the filing-time body hash")
	require.Equal(t, tatarav1alpha1.ProposalKindIncident, iss.Spec.ProposalKind,
		"an incident tracker issue is stamped incident, so it never counts as a brainstorm proposal")
}

// After file_issue on an incident Task whose spec.dedupKey is set, the minted
// Issue CR carries the rule-key label (queue.LabelAlertRuleKey), and the
// forge CreateIssue call carried the tatara-alert-rule=<key> label - the O5
// suppression lookup and the human-visible forge recovery index (O4).
func TestOutcome_Incident_StampsRuleKeyLabel(t *testing.T) {
	task := taskV2("t1", "tatara", "incident", tatarav1alpha1.StateRefined, "incident")
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
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StateRefined, "incident"))

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
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StateRefined, "incident"))
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
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StateRefined, "incident"))
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
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StateRefined, "incident"))
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
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StateRefined, "incident"))
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
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StateRefined, "incident"))

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
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StateRefined, "incident"))

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
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StateRefined, "incident"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"incident","payload":{
	  "action":"file_issue","alertRules":["tatara-operator-down"],"reason":"r",
	  "issue":{"repo":"tatara-operator","title":"t","body":"b","parent":{"repo":"tatara-memory"}}}}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestOutcome_Incident_FalsePositiveRejects(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StateRefined, "incident"))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"incident","payload":{
	  "action":"false_positive","alertRules":["flappy"],"reason":"the alert is wrong"}}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, tatarav1alpha1.StateRejected, e.task(t, "t1").Status.State)
}

func TestOutcome_Incident_AlertRulesRequiredOnBothActions(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StateRefined, "incident"))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"incident","payload":{"action":"false_positive","alertRules":[],"reason":"r"}}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

// --- title clamping ---

// This is the path that fired: an over-long agent title 400s on GitLab and the
// handler drops the whole submitted outcome.
//
// On the provider axis: the fake forge does not branch on provider, so these
// cases cannot prove anything about a per-forge API. What they pin is that the
// clamp is NOT conditional on the project's provider, which is the deliberate
// choice recorded on IssueTitleMaxChars - GitLab's cap is applied to GitHub too.
func TestOutcome_Incident_TitleIsClamped(t *testing.T) {
	tests := []struct {
		name     string
		provider string
		title    string
		want     string
	}{
		{"github_short", "github", "issue" + strings.Repeat("-X", 25),
			"issue" + strings.Repeat("-X", 25)},
		{"github_long", "github", "issue" + strings.Repeat("-X", 150),
			"issue" + strings.Repeat("-X", 118) + "...(truncated)"},
		{"gitlab_short", "gitlab", "issue" + strings.Repeat("-X", 25),
			"issue" + strings.Repeat("-X", 25)},
		{"gitlab_long", "gitlab", "issue" + strings.Repeat("-X", 150),
			"issue" + strings.Repeat("-X", 118) + "...(truncated)"},
		// 300 CJK characters is 900 bytes. A byte-based clamp would cut this to
		// a third of the characters the forge would have taken - the literal
		// below pins 241 CHARACTERS kept, which is the whole point of the rune
		// clamp and the one assertion TruncateUTF8 could not satisfy.
		{"gitlab_long_multibyte", "gitlab", strings.Repeat("界", 300),
			strings.Repeat("界", 241) + "...(truncated)"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(),
				repoV2("tatara-operator", "tatara"),
				taskV2("t1", "tatara", "incident", tatarav1alpha1.StateRefined, "incident"))
			p := e.project(t, "tatara")
			p.Spec.Scm.Provider = tc.provider
			require.NoError(t, e.c.Update(context.Background(), p))

			w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
				fmt.Sprintf(`{"kind":"incident","payload":{"action":"file_issue","alertRules":["tatara-operator-down"],"reason":"real outage","issue":{"repo":"tatara-operator","title":%s,"body":"trace"}}}`,
					strconv.Quote(tc.title)))
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())

			require.Len(t, e.forge.createdReqs, 1)
			require.Equal(t, tc.want, e.forge.createdReqs[0].Title,
				"forge must receive the clamped title, not the raw agent input")
			require.LessOrEqual(t, utf8.RuneCountInString(e.forge.createdReqs[0].Title),
				tatarav1alpha1.IssueTitleMaxChars,
				"clamped title must fit the character limit")

			// Assert the minted Issue CR's Status.Title matches the clamped title
			// (the CR is a mirror of what the forge stored).
			iss := e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 101))
			require.Equal(t, tc.want, iss.Status.Title,
				"Issue CR Status.Title must match the clamped title sent to the forge")
		})
	}
}

// TestOutcome_Brainstorm_TitleIsClamped asserts that brainstorm propose clamps
// over-long agent-supplied titles to IssueTitleMaxChars before filing on the
// forge, and that the spawned clarify Task's Goal preserves the raw unclamped
// title (the Goal is agent context, not a forge mirror).
func TestOutcome_Brainstorm_TitleIsClamped(t *testing.T) {
	tests := []struct {
		name     string
		provider string
		title    string
		want     string
	}{
		{"github_short", "github", "proposal" + strings.Repeat("-Y", 25),
			"proposal" + strings.Repeat("-Y", 25)},
		{"github_long", "github", "proposal" + strings.Repeat("-Y", 150),
			"proposal" + strings.Repeat("-Y", 116) + "-" + "...(truncated)"},
		{"gitlab_short", "gitlab", "proposal" + strings.Repeat("-Y", 25),
			"proposal" + strings.Repeat("-Y", 25)},
		{"gitlab_long", "gitlab", "proposal" + strings.Repeat("-Y", 150),
			"proposal" + strings.Repeat("-Y", 116) + "-" + "...(truncated)"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			title := tc.title
			e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(),
				repoV2("tatara-operator", "tatara"),
				taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StateRefined, "brainstorm"))
			p := e.project(t, "tatara")
			p.Spec.Scm.Provider = tc.provider
			require.NoError(t, e.c.Update(context.Background(), p))

			w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
				fmt.Sprintf(`{"kind":"brainstorm","payload":{"action":"propose","proposals":[{"repo":"tatara-operator","title":%s,"body":"b","kind":"bug"}]}}`,
					strconv.Quote(title)))
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())

			require.Len(t, e.forge.createdReqs, 1)
			require.Equal(t, tc.want, e.forge.createdReqs[0].Title,
				"forge must receive the clamped title, not the raw agent input")
			require.LessOrEqual(t, utf8.RuneCountInString(e.forge.createdReqs[0].Title),
				tatarav1alpha1.IssueTitleMaxChars,
				"clamped title must fit the character limit")

			// Assert the minted Issue CR's Status.Title matches the clamped title.
			iss := e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 101))
			require.Equal(t, tc.want, iss.Status.Title,
				"Issue CR Status.Title must match the clamped title sent to the forge")

			// Assert the spawned Task's Goal begins with the RAW unclamped title.
			// The Goal is agent context, not a forge mirror, so it is deliberately
			// not clamped (it has its own GoalMaxBytes BYTE limit). The clamp must
			// not leak out of the forge-write path into the prompt the agent reads.
			var tasks tatarav1alpha1.TaskList
			require.NoError(t, e.c.List(context.Background(), &tasks, client.InNamespace(ns)))
			var spawnedGoal string
			for i := range tasks.Items {
				if tasks.Items[i].Spec.Kind == controller.SweepIssueKind {
					spawnedGoal = tasks.Items[i].Spec.Goal
					break
				}
			}
			require.NotEmpty(t, spawnedGoal, "brainstorm propose must spawn a follow-up task")
			require.True(t, strings.HasPrefix(spawnedGoal, title),
				"spawned Task Goal must begin with the raw unclamped title")
		})
	}
}

// Once titles are clamped, a length-caused 400 is IMPOSSIBLE - so a log line
// that reports only the raw agent-supplied length points whoever reads it at a
// cause that cannot be the one firing ("title_chars=900" beside a 400 that the
// 255-rune value actually took), while the title that did reach the forge
// appears nowhere at all. Both counts have to be on the line: the raw one says
// what the agent wrote, the sent one says what was rejected.
func TestOutcome_Incident_ForgeFailureLogsRawAndSentTitleLengths(t *testing.T) {
	var logBuf bytes.Buffer
	e := buildV2(t, v2Opts{logger: slog.New(slog.NewJSONHandler(&logBuf, nil))},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StateRefined, "incident"))
	e.forge.createIssueErr = fmt.Errorf(
		`scm: /projects/szymonrychu%%2Fcharts/issues -> 400: {"message":{"title":["is too long (maximum is 255 characters)"]}}`)

	title := "incident" + strings.Repeat("-X", 400) // 808 runes, well over the cap
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		fmt.Sprintf(`{"kind":"incident","payload":{"action":"file_issue","alertRules":["tatara-operator-down"],"reason":"real outage","issue":{"repo":"tatara-operator","title":%s,"body":"trace"}}}`,
			strconv.Quote(title)))
	require.Equal(t, http.StatusBadGateway, w.Code, w.Body.String())

	require.Len(t, e.forge.createdReqs, 1)
	sent := e.forge.createdReqs[0].Title
	require.Equal(t, tatarav1alpha1.IssueTitleMaxChars, utf8.RuneCountInString(sent),
		"precondition: the forge was handed the clamped title")

	// The prefix is written out literally rather than as TruncateRunes(sent, 80):
	// deriving it with the same call the field is built from would hold for any
	// implementation of TruncateRunes, including a broken one.
	entry := lastErrorLog(t, &logBuf, "restapi: filing the incident tracker issue failed")
	require.Equal(t, float64(808), entry["title_chars"],
		"title_chars must report what the AGENT supplied")
	require.Equal(t, float64(255), entry["sent_title_chars"],
		"sent_title_chars must report what actually reached the forge - without it the "+
			"rejected value is unrecoverable from the log line")
	require.Equal(t, "incident"+strings.Repeat("-X", 36), entry["title_prefix"],
		"title_prefix must be the first 80 runes of the SENT title, so the line describes one request")
}

// The sibling issue-filing paths take the same forge 400 and were diagnosable
// only from the incident one. All three log identically or none of them is
// reliably diagnosable.
func TestOutcome_Brainstorm_ForgeFailureLogsRawAndSentTitleLengths(t *testing.T) {
	var logBuf bytes.Buffer
	e := buildV2(t, v2Opts{logger: slog.New(slog.NewJSONHandler(&logBuf, nil))},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StateRefined, "brainstorm"))
	e.forge.createIssueErr = fmt.Errorf("scm: 400 title is too long")

	title := "proposal" + strings.Repeat("-Y", 400)
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		fmt.Sprintf(`{"kind":"brainstorm","payload":{"action":"propose","proposals":[{"repo":"tatara-operator","title":%s,"body":"b","kind":"bug"}]}}`,
			strconv.Quote(title)))
	require.Equal(t, http.StatusBadGateway, w.Code, w.Body.String())

	require.Len(t, e.forge.createdReqs, 1)
	entry := lastErrorLog(t, &logBuf, "restapi: filing a brainstorm proposal failed")
	require.Equal(t, float64(808), entry["title_chars"])
	require.Equal(t, float64(255), entry["sent_title_chars"])
	require.Equal(t, "proposal"+strings.Repeat("-Y", 36), entry["title_prefix"])
}

// The third CreateIssue site. Two of three pinned is not "all three log
// identically": the unpinned one is free to regress silently.
func TestIssueWrite_Create_ForgeFailureLogsRawAndSentTitleLengths(t *testing.T) {
	var logBuf bytes.Buffer
	e := buildV2(t, v2Opts{logger: slog.New(slog.NewJSONHandler(&logBuf, nil))},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify"))
	e.forge.createIssueErr = fmt.Errorf("scm: 400 title is too long")

	title := "issue" + strings.Repeat("-X", 400)
	w := e.do(t, http.MethodPost, "/projects/tatara/scm/issue-write",
		fmt.Sprintf(`{"task":"t1","action":"create","repo":"tatara-operator","title":%s,"body":"B"}`,
			strconv.Quote(title)))
	require.Equal(t, http.StatusBadGateway, w.Code, w.Body.String())

	entry := lastErrorLog(t, &logBuf, "restapi: creating issue failed")
	require.Equal(t, float64(805), entry["title_chars"])
	require.Equal(t, float64(255), entry["sent_title_chars"])
	require.Equal(t, "issue"+strings.Repeat("-X", 37)+"-", entry["title_prefix"])
	require.Equal(t, "t1", entry["task"],
		"the failure line needs the Task name to correlate against the agent run; "+
			"both sibling sites and this handler's own success line carry it")
}

// lastErrorLog returns the last JSON log record in buf whose msg matches, so an
// assertion names the field it wants rather than substring-matching the line.
func lastErrorLog(t *testing.T, buf *bytes.Buffer, msg string) map[string]any {
	t.Helper()
	var found map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var entry map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &entry), "log line is not JSON: %s", line)
		if entry["msg"] == msg && entry["level"] == "ERROR" {
			found = entry
		}
	}
	require.NotNil(t, found, "no ERROR log with msg=%q in:\n%s", msg, buf.String())
	return found
}

// --- Fix 7 (#400): investigation-comment cooldown -------------------------

// buildV2WithCooldown mirrors buildV2 (handlers_v2_test.go) but also wires
// IncidentInvestigationCommentCooldown and an overridable now(), neither of
// which v2Opts exposes. Defined here rather than adding a field to v2Opts, to
// keep this workstream's edits confined to outcome.go/outcome_test.go.
func buildV2WithCooldown(t *testing.T, metrics *obs.OperatorMetrics, cooldown time.Duration,
	nowFn func() time.Time, objs ...client.Object) *v2Env {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, tatarav1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	fc := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&tatarav1alpha1.Project{}, &tatarav1alpha1.Repository{},
			&tatarav1alpha1.Task{}, &tatarav1alpha1.Issue{}, &tatarav1alpha1.MergeRequest{}).
		Build()

	env := &v2Env{c: fc, now: frozenNow}
	env.forge = newRecordingForge()
	env.spiller = &fakeSpiller{}

	s := restapi.NewServer(restapi.Config{
		Client: fc, Namespace: ns,
		SCMFor:                               func(string) (scm.SCMWriter, error) { return env.forge, nil },
		SpillerFor:                           func(*tatarav1alpha1.Project) objbudget.Spiller { return env.spiller },
		Now:                                  nowFn,
		Metrics:                              metrics,
		IncidentInvestigationCommentCooldown: cooldown,
	})
	r := chi.NewRouter()
	s.Mount(r, nil)
	env.r = r
	return env
}

// buildV2WithCooldownAndInterceptor mirrors buildV2WithCooldown but also
// wires a fake-client interceptor, so a test can fail a SPECIFIC downstream
// write (e.g. the reset-FitIssue Update after a comment has already posted)
// without touching the others.
func buildV2WithCooldownAndInterceptor(t *testing.T, metrics *obs.OperatorMetrics, cooldown time.Duration,
	nowFn func() time.Time, funcs interceptor.Funcs, objs ...client.Object) *v2Env {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, tatarav1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	fc := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&tatarav1alpha1.Project{}, &tatarav1alpha1.Repository{},
			&tatarav1alpha1.Task{}, &tatarav1alpha1.Issue{}, &tatarav1alpha1.MergeRequest{}).
		WithInterceptorFuncs(funcs).
		Build()

	env := &v2Env{c: fc, now: frozenNow}
	env.forge = newRecordingForge()
	env.spiller = &fakeSpiller{}

	s := restapi.NewServer(restapi.Config{
		Client: fc, Namespace: ns,
		SCMFor:                               func(string) (scm.SCMWriter, error) { return env.forge, nil },
		SpillerFor:                           func(*tatarav1alpha1.Project) objbudget.Spiller { return env.spiller },
		Now:                                  nowFn,
		Metrics:                              metrics,
		IncidentInvestigationCommentCooldown: cooldown,
	})
	r := chi.NewRouter()
	s.Mount(r, nil)
	env.r = r
	return env
}

// incidentTrackerV2 builds an open tracker Issue CR carrying the alert
// rule-key label incidentComment gates comment_issue on (queue.LabelAlertRuleKey).
func incidentTrackerV2(repo string, number int) *tatarav1alpha1.Issue {
	return issueV2(repo, number, "tracker-task", func(i *tatarav1alpha1.Issue) {
		i.Labels = map[string]string{queue.LabelAlertRuleKey: "rule-1"}
	})
}

// TestOutcome_IncidentComment_ZeroCooldownConfigNeverSuppresses proves the
// cooldown gate actually reads Config.IncidentInvestigationCommentCooldown
// (D2 wiring) rather than a hardcoded constant: with cooldown=0, two
// comment_issue outcomes at the SAME instant both reach the forge.
func TestOutcome_IncidentComment_ZeroCooldownConfigNeverSuppresses(t *testing.T) {
	metrics := obs.NewOperatorMetrics(prometheus.NewRegistry())
	e := buildV2WithCooldown(t, metrics, 0, func() time.Time { return frozenNow },
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		incidentTrackerV2("tatara-operator", 101),
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StateRefined, "incident"),
		taskV2("t2", "tatara", "incident", tatarav1alpha1.StateRefined, "incident"))

	w1 := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"incident","payload":{"action":"comment_issue","alertRules":["rule-1"],"reason":"r",
		  "comment":{"repo":"tatara-operator","number":101,"body":"one"}}}`)
	require.Equal(t, http.StatusOK, w1.Code, w1.Body.String())
	w2 := e.do(t, http.MethodPost, "/tasks/t2/outcome",
		`{"kind":"incident","payload":{"action":"comment_issue","alertRules":["rule-1"],"reason":"r",
		  "comment":{"repo":"tatara-operator","number":101,"body":"two"}}}`)
	require.Equal(t, http.StatusOK, w2.Code, w2.Body.String())
	require.Len(t, e.forge.comments, 2,
		"Config.IncidentInvestigationCommentCooldown=0 must reach the handler and never suppress")
}

// TestIncidentComment_CooldownSuppressesAndCounts is the 3-call sequence:
// post, then (within cooldown) suppress-and-count, then (after cooldown)
// post-with-prefix and counter reset. The suppressed call still terminates
// its Task at rejected(tracked-elsewhere) - only the forge write is skipped.
func TestIncidentComment_CooldownSuppressesAndCounts(t *testing.T) {
	metrics := obs.NewOperatorMetrics(prometheus.NewRegistry())
	cur := frozenNow
	e := buildV2WithCooldown(t, metrics, 30*time.Minute, func() time.Time { return cur },
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		incidentTrackerV2("tatara-operator", 101),
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StateRefined, "incident"),
		taskV2("t2", "tatara", "incident", tatarav1alpha1.StateRefined, "incident"),
		taskV2("t3", "tatara", "incident", tatarav1alpha1.StateRefined, "incident"))

	w1 := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"incident","payload":{"action":"comment_issue","alertRules":["rule-1"],"reason":"first",
		  "comment":{"repo":"tatara-operator","number":101,"body":"evidence one"}}}`)
	require.Equal(t, http.StatusOK, w1.Code, w1.Body.String())
	require.Equal(t, tatarav1alpha1.StateRejected, e.task(t, "t1").Status.State)
	require.Len(t, e.forge.comments, 1)
	require.Equal(t, "evidence one", e.forge.comments[0].Body)
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.IncidentTrackerCommentCounter("posted")))

	cur = cur.Add(5 * time.Minute) // well within the 30m cooldown
	w2 := e.do(t, http.MethodPost, "/tasks/t2/outcome",
		`{"kind":"incident","payload":{"action":"comment_issue","alertRules":["rule-1"],"reason":"second",
		  "comment":{"repo":"tatara-operator","number":101,"body":"evidence two"}}}`)
	require.Equal(t, http.StatusOK, w2.Code, w2.Body.String())
	require.Equal(t, tatarav1alpha1.StateRejected, e.task(t, "t2").Status.State,
		"the suppressed path must still terminate the Task at rejected(tracked-elsewhere)")
	require.Len(t, e.forge.comments, 1, "the SCM write itself must be suppressed")
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.IncidentTrackerCommentCounter("suppressed")))

	tracked := e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 101))
	require.Equal(t, 1, tracked.Status.SuppressedInvestigationCount)

	cur = cur.Add(30 * time.Minute) // now 35m past t1's post: cooldown cleared
	w3 := e.do(t, http.MethodPost, "/tasks/t3/outcome",
		`{"kind":"incident","payload":{"action":"comment_issue","alertRules":["rule-1"],"reason":"third",
		  "comment":{"repo":"tatara-operator","number":101,"body":"evidence three"}}}`)
	require.Equal(t, http.StatusOK, w3.Code, w3.Body.String())
	require.Equal(t, tatarav1alpha1.StateRejected, e.task(t, "t3").Status.State)
	require.Len(t, e.forge.comments, 2)
	require.Contains(t, e.forge.comments[1].Body, "1 prior evidence comment")
	require.Contains(t, e.forge.comments[1].Body, "evidence three")
	require.Equal(t, float64(2), testutil.ToFloat64(metrics.IncidentTrackerCommentCounter("posted")))

	tracked2 := e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 101))
	require.Equal(t, 0, tracked2.Status.SuppressedInvestigationCount,
		"the counter resets once the comment actually posts")
	require.NotNil(t, tracked2.Status.LastInvestigationCommentAt)
	require.Nil(t, tracked2.Status.LastRefireCommentAt, "must never share the refire marker field")
	require.Nil(t, tracked2.Status.LastDeployTimeoutCommentAt, "must never share the deploy-timeout marker field")
}

// TestIncidentComment_CooldownExactThresholdIsNotSuppressed: elapsed exactly
// equal to the cooldown must NOT be suppressed (strict "<", matching the
// IncidentRefireCommentCooldown precedent).
func TestIncidentComment_CooldownExactThresholdIsNotSuppressed(t *testing.T) {
	metrics := obs.NewOperatorMetrics(prometheus.NewRegistry())
	cur := frozenNow
	e := buildV2WithCooldown(t, metrics, 30*time.Minute, func() time.Time { return cur },
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		incidentTrackerV2("tatara-operator", 101),
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StateRefined, "incident"),
		taskV2("t2", "tatara", "incident", tatarav1alpha1.StateRefined, "incident"))

	w1 := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"incident","payload":{"action":"comment_issue","alertRules":["rule-1"],"reason":"r",
		  "comment":{"repo":"tatara-operator","number":101,"body":"one"}}}`)
	require.Equal(t, http.StatusOK, w1.Code, w1.Body.String())

	cur = cur.Add(30 * time.Minute) // elapsed == cooldown, exactly
	w2 := e.do(t, http.MethodPost, "/tasks/t2/outcome",
		`{"kind":"incident","payload":{"action":"comment_issue","alertRules":["rule-1"],"reason":"r",
		  "comment":{"repo":"tatara-operator","number":101,"body":"two"}}}`)
	require.Equal(t, http.StatusOK, w2.Code, w2.Body.String())
	require.Len(t, e.forge.comments, 2, "exactly-at-threshold must NOT be suppressed")
}

// TestIncidentComment_CommentErrorReleasesClaim is Fix 5b (#406):
// incidentComment's SCM-comment-error branch must release the outcome claim
// so an immediate identical retry re-validates (claimWon) instead of 409ing
// for the rest of the claim TTL.
func TestIncidentComment_CommentErrorReleasesClaim(t *testing.T) {
	metrics := obs.NewOperatorMetrics(prometheus.NewRegistry())
	e := buildV2WithCooldown(t, metrics, 30*time.Minute, func() time.Time { return frozenNow },
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		incidentTrackerV2("tatara-operator", 101),
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StateRefined, "incident"))
	e.forge.commentErr = fmt.Errorf("github: comment failed transiently")

	body := `{"kind":"incident","payload":{"action":"comment_issue","alertRules":["rule-1"],"reason":"r",
	  "comment":{"repo":"tatara-operator","number":101,"body":"evidence"}}}`

	w1 := e.do(t, http.MethodPost, "/tasks/t1/outcome", body)
	require.Equal(t, http.StatusBadGateway, w1.Code)
	require.Equal(t, tatarav1alpha1.StateRefined, e.task(t, "t1").Status.State,
		"a failed SCM comment must not move the stage")

	e.forge.commentErr = nil
	w2 := e.do(t, http.MethodPost, "/tasks/t1/outcome", body)
	require.Equal(t, http.StatusOK, w2.Code, w2.Body.String(),
		"a held claim (missing o.release()) would 409 this identical retry instead of re-validating")
	require.Len(t, e.forge.comments, 2, "both attempts must have reached the forge")
	require.Equal(t, tatarav1alpha1.StateRejected, e.task(t, "t1").Status.State)
}

// TestIncidentComment_GateReasons is #445: the comment_issue gate must
// distinguish an Issue CR that is simply absent from the operator's mirror
// (e.g. validly GC'd by the reaper once its owning Task was cleaned up) from
// an Issue CR that exists but was never labelled as an incident tracker.
// Before this fix both cases returned the identical reason/message
// ("not-a-tracker"), which made a legitimately GC'd tracker indistinguishable
// from a deliberate non-tracker rejection and drove agents to re-file the
// same fault as a fresh internal issue.
func TestIncidentComment_GateReasons(t *testing.T) {
	tests := []struct {
		name        string
		extraObjs   []client.Object
		wantReason  string
		wantMessage string
	}{
		{
			name:        "issue CR absent from mirror",
			extraObjs:   nil,
			wantReason:  "not-mirrored",
			wantMessage: "not present in the operator's mirror",
		},
		{
			name:        "issue CR present but unlabelled",
			extraObjs:   []client.Object{issueV2("tatara-operator", 101, "tracker-task")},
			wantReason:  "not-a-tracker",
			wantMessage: "not a tracked incident issue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics := obs.NewOperatorMetrics(prometheus.NewRegistry())
			objs := append([]client.Object{
				projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
				taskV2("t1", "tatara", "incident", tatarav1alpha1.StateRefined, "incident"),
			}, tt.extraObjs...)
			e := buildV2WithCooldown(t, metrics, 30*time.Minute, func() time.Time { return frozenNow }, objs...)

			before := testutil.ToFloat64(obs.RestOutcomeRejectedTotal.WithLabelValues("incident", tt.wantReason))

			w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
				`{"kind":"incident","payload":{"action":"comment_issue","alertRules":["rule-1"],"reason":"r",
				  "comment":{"repo":"tatara-operator","number":101,"body":"evidence"}}}`)

			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			require.Contains(t, w.Body.String(), tt.wantMessage)
			require.Len(t, e.forge.comments, 0, "a rejected gate must never reach the forge")
			after := testutil.ToFloat64(obs.RestOutcomeRejectedTotal.WithLabelValues("incident", tt.wantReason))
			require.Equal(t, before+1, after,
				"the rejection must be counted under its own distinct reason label")
		})
	}

	// The two branches must never share a reason: run both in one build so a
	// regression that reintroduces one shared reason string is caught even if
	// each subtest's before/after delta alone would not see it.
	metrics := obs.NewOperatorMetrics(prometheus.NewRegistry())
	e := buildV2WithCooldown(t, metrics, 30*time.Minute, func() time.Time { return frozenNow },
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		issueV2("tatara-operator", 102, "tracker-task"),
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StateRefined, "incident"),
		taskV2("t2", "tatara", "incident", tatarav1alpha1.StateRefined, "incident"))

	wAbsent := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"incident","payload":{"action":"comment_issue","alertRules":["rule-1"],"reason":"r",
		  "comment":{"repo":"tatara-operator","number":999,"body":"evidence"}}}`)
	wUnlabelled := e.do(t, http.MethodPost, "/tasks/t2/outcome",
		`{"kind":"incident","payload":{"action":"comment_issue","alertRules":["rule-1"],"reason":"r",
		  "comment":{"repo":"tatara-operator","number":102,"body":"evidence"}}}`)

	require.Equal(t, http.StatusBadRequest, wAbsent.Code)
	require.Equal(t, http.StatusBadRequest, wUnlabelled.Code)
	require.NotEqual(t, wAbsent.Body.String(), wUnlabelled.Body.String(),
		"CR-absent and CR-present-but-unlabelled must produce distinguishable messages")
}

// TestIncidentComment_ResetFitIssueFailureIsBestEffort proves the post-comment
// FitIssue (cooldown-marker reset) is best-effort: the forge comment has
// already landed by the time it runs, so a failure there must not fail the
// whole request - that would leave the Task unterminated and duplicate the
// comment on retry. The Task must still terminate rejected(tracked-elsewhere)
// with exactly one forge comment.
func TestIncidentComment_ResetFitIssueFailureIsBestEffort(t *testing.T) {
	metrics := obs.NewOperatorMetrics(prometheus.NewRegistry())
	funcs := interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, subResourceName string,
			obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if iss, ok := obj.(*tatarav1alpha1.Issue); ok && subResourceName == "status" &&
				iss.Status.LastInvestigationCommentAt != nil {
				// Only the reset call stamps LastInvestigationCommentAt; the
				// suppressed-increment call never touches this field.
				return fmt.Errorf("injected: status update failed")
			}
			return c.SubResource(subResourceName).Update(ctx, obj, opts...)
		},
	}
	e := buildV2WithCooldownAndInterceptor(t, metrics, 30*time.Minute, func() time.Time { return frozenNow }, funcs,
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		incidentTrackerV2("tatara-operator", 101),
		taskV2("t1", "tatara", "incident", tatarav1alpha1.StateRefined, "incident"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"incident","payload":{"action":"comment_issue","alertRules":["rule-1"],"reason":"r",
		  "comment":{"repo":"tatara-operator","number":101,"body":"evidence"}}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String(),
		"a failed cooldown-marker reset must not fail the request; the comment already posted")
	require.Len(t, e.forge.comments, 1, "the comment reaches the forge exactly once")
	require.Equal(t, tatarav1alpha1.StateRejected, e.task(t, "t1").Status.State,
		"the Task must still terminate despite the reset failure")
}

// --- refine, and the B.3 fold ---------------------------------------------

// THE FOLD: adopt, VERIFY, then delete. The member's artifacts land on the
// umbrella with controller=true in ONE PUT (the API server rejects two
// controller=true refs), and only THEN is the member deleted.
func TestOutcome_Refine_FoldAdoptsVerifiesThenDeletes(t *testing.T) {
	// foldMemberBusy treats StateRefined as LIVE (#521: it is one of the states
	// an agent can actually be conversing in), so a foldable, quiescent member
	// must sit at StateNew - not yet triaged into any agent's live work.
	member := taskV2("t2", "tatara", "clarify", tatarav1alpha1.StateNew, "clarify")
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "refine", tatarav1alpha1.StateRefined, "refine"),
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
	require.Equal(t, tatarav1alpha1.StateDone, got.Status.State)

	// C4: adoption transfers the CONTROLLER ref, but every downstream consumer
	// (the C.6 approval citation check, the reaper's owned-set, the agent bundle)
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
			m.Status.State, m.Status.AgentKind = tatarav1alpha1.StateMerged, ""
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			member := taskV2("t2", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement")
			tc.apply(member)
			e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
				repoV2("tatara-operator", "tatara"),
				taskV2("t1", "tatara", "refine", tatarav1alpha1.StateRefined, "refine"), member)

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
		taskV2("t1", "tatara", "refine", tatarav1alpha1.StateRefined, "refine"),
		taskV2("t2", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify"),
		issueV2("tatara-operator", 291, "t2"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"refine","payload":{"closes":[{"repo":"tatara-operator","number":291,"reason":"stale"}]}}`)
	require.Equal(t, http.StatusConflict, w.Code)
	require.Contains(t, w.Body.String(), "issue has an active task")
}

// The unverified-adoption path documents "foldInFlight cleared, members NOT
// deleted", and until issue #467 it cleared nothing: the umbrella failed with a
// live marker naming members that still existed, which is a 7-day GC block
// (FailedRetention) on every one of them.
func TestOutcome_Refine_UnverifiedFoldClearsTheMarker(t *testing.T) {
	var stamped bool
	stripController := interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey,
			obj client.Object, opts ...client.GetOption) error {
			if err := c.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if _, ok := obj.(*tatarav1alpha1.Issue); ok {
				obj.SetOwnerReferences(nil) // step 3 re-list finds no controller owner
			}
			return nil
		},
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string,
			obj client.Object, opts ...client.SubResourceUpdateOption) error {
			// Step 1 must ANCHOR the marker it writes: without the stamp the TTL
			// has nothing to measure and the reaper is back to guessing.
			if tk, ok := obj.(*tatarav1alpha1.Task); ok &&
				len(tk.Status.FoldInFlight) > 0 && tk.Status.FoldInFlightSince != nil {
				stamped = true
			}
			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
	}
	e := buildV2WithCooldownAndInterceptor(t, obs.NewOperatorMetrics(prometheus.NewRegistry()), 0,
		func() time.Time { return frozenNow }, stripController,
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "refine", tatarav1alpha1.StateRefined, "refine"),
		// foldMemberBusy treats StateRefined as LIVE (#521), so the member
		// must be quiescent (StateNew) for this request to reach the
		// adoption-verification step at all, rather than 409ing earlier at
		// the liveness gate.
		taskV2("t2", "tatara", "clarify", tatarav1alpha1.StateNew, "clarify"),
		issueV2("tatara-operator", 291, "t2"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"refine","payload":{"folds":[{"task":"t2"}]}}`)
	require.Equal(t, http.StatusConflict, w.Code)

	require.NoError(t, e.c.Get(context.Background(),
		client.ObjectKey{Namespace: ns, Name: "t2"}, &tatarav1alpha1.Task{}),
		"an unverified adoption deletes nothing")
	got := e.task(t, "t1")
	// #521: `failed` is gone. An unverified fold adoption now PARKS the
	// umbrella in place rather than moving it to a dead terminal stage.
	require.Equal(t, tatarav1alpha1.StateRefined, got.Status.State, "a park never changes state")
	require.True(t, tatarav1alpha1.Parked(got))
	require.Equal(t, stage.ReasonFoldAdoptionUnverified, got.Status.ParkReason)
	require.Empty(t, got.Status.FoldInFlight,
		"a failed umbrella must not pin its members for FailedRetention")
	require.Nil(t, got.Status.FoldInFlightSince)
	require.True(t, stamped, "step 1 must stamp foldInFlightSince alongside foldInFlight")
}

// The self-termination gate (issue #467): closing an issue THIS Task owns is
// what stops this Task at rejected(issue-closed), so an outcome that does it in
// the same breath as a fold kills the umbrella mid-adoption and strands every
// member. Refused BEFORE foldMembers, where a rejection still costs nothing.
func TestOutcome_Refine_ClosingAnOwnedIssueMidFoldIs409(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "refine", tatarav1alpha1.StateRefined, "refine"),
		taskV2("t2", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify"),
		issueV2("tatara-operator", 291, "t1"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"refine","payload":{
	  "folds":[{"task":"t2"}],
	  "closes":[{"repo":"tatara-operator","number":291,"reason":"stale"}]}}`)
	require.Equal(t, http.StatusConflict, w.Code)
	require.Contains(t, w.Body.String(), "closing an owned issue would stop this task mid-fold")

	require.NoError(t, e.c.Get(context.Background(),
		client.ObjectKey{Namespace: ns, Name: "t2"}, &tatarav1alpha1.Task{}),
		"a refused refine deletes nothing")
	require.Empty(t, e.task(t, "t1").Status.FoldInFlight,
		"the fold never started, so no marker is left behind")
}

// closes[] is LIVE-REVALIDATED against SCM immediately before each close:
// refine may act on a view up to an hour stale.
func TestOutcome_Refine_CloseIsLiveRevalidated(t *testing.T) {
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "refine", tatarav1alpha1.StateRefined, "refine"),
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
		taskV2("t1", "tatara", "refine", tatarav1alpha1.StateRefined, "refine"),
		taskV2("t2", "tatara", "clarify", tatarav1alpha1.StateRefined, "clarify"),
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

// TestOutcome_Refine_LinkOnAZeroOwnerArtifactClaimsTheControllerFlag is issue
// #536: the SECOND producer of a zero-controller-owner artifact, and the one no
// reap is involved in.
//
// A zero-owner Issue is not a broken state - it is the reaper's designed hand-off
// to the sweep, and the window before the re-mint is routinely hours wide
// (1 h 41 m on iss-tatara-operator-526). linkArtifact was the ONLY own.AddPlainOwner
// call site with no controller precondition of any kind, so a links[] entry
// landing in that window wrote the artifact's FIRST and ONLY ownerRef as plain:
// zero controller owners, B.2 rule 5 broken, the critical repair-guard alert 2.5 s
// later. Its sibling folds[] has always paired the append with a handover in one
// Update; links[] must never be able to leave a sole plain owner behind either.
func TestOutcome_Refine_LinkOnAZeroOwnerArtifactClaimsTheControllerFlag(t *testing.T) {
	orphan := issueV2("tatara-operator", 526, "", func(i *tatarav1alpha1.Issue) {
		i.OwnerReferences = nil // the reaper dropped the last ref; the sweep has not re-minted
	})
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "refine", tatarav1alpha1.StateRefined, "refine"),
		orphan)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"refine","payload":{"links":[{"repo":"tatara-operator","number":526}]}}`)
	require.Equal(t, http.StatusOK, w.Code)

	iss := e.issue(t, tatarav1alpha1.IssueName("tatara-operator", 526))
	ctrl, owned := controllerOwnerOf(iss.OwnerReferences)
	require.True(t, owned,
		"links[] left the artifact with a sole PLAIN owner and NO controller owner (contract B.2 rule 5)")
	require.Equal(t, "t1", ctrl, "the linking umbrella must claim what it just took out of the sweep's hands")
	require.Len(t, iss.OwnerReferences, 1, "the claim is ONE ownerRef, appended and promoted in one Update")
	require.Contains(t, e.task(t, "t1").Status.IssueRefs, iss.Name)
}

// A malformed links[] entry must be caught in the TOP validation block, BEFORE
// foldMembers deletes anything. Validating it after the fold made the rejection
// unrecoverable: the members were already gone, so the identical retry - which
// release lets re-validate immediately - hit NotFound on its own fold target and
// 500'd forever.
func TestOutcome_Refine_MalformedLinkRejectsBeforeAnyFoldDeletes(t *testing.T) {
	// foldMemberBusy treats StateRefined as LIVE (#521), so the corrected retry
	// below (which must actually complete the fold) needs a quiescent member.
	member := taskV2("t2", "tatara", "clarify", tatarav1alpha1.StateNew, "clarify")
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "refine", tatarav1alpha1.StateRefined, "refine"),
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
		taskV2("t1", "tatara", "refine", tatarav1alpha1.StateRefined, "refine"))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"refine","payload":{}}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "at least one of folds, closes, links")
}

// --- documentation --------------------------------------------------------

func TestOutcome_Documentation_DeclinedDeliversAndStampsDocumentedBy(t *testing.T) {
	covered := taskV2("t9", "tatara", "implement", tatarav1alpha1.StateDone, "")
	batch := taskV2("t1", "tatara", "documentation", tatarav1alpha1.StateUnderImplementation, "documentation")
	batch.Spec.DocumentsTasks = []string{"t9"}

	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-documentation", "tatara"), batch, covered)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"documentation","payload":{"action":"declined","reason":"nothing user-visible"}}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, tatarav1alpha1.StateDone, e.task(t, "t1").Status.State)
	require.Equal(t, "t1", e.task(t, "t9").Status.DocumentedBy)
}

// TestOutcome_Documentation_DeclinedOnTheFrozenWireContract is the
// documentation half of the same frozen key set. Its schema's action enum is
// submitted|declined only, and a declined batch carries its reason on the same
// single `reason` key and finishes at done(doc-timeout) - the nightly batch's
// ONLY non-MR terminal.
func TestOutcome_Documentation_DeclinedOnTheFrozenWireContract(t *testing.T) {
	covered := taskV2("t9", "tatara", "implement", tatarav1alpha1.StateDone, "")
	batch := taskV2("t1", "tatara", "documentation", tatarav1alpha1.StateUnderImplementation, "documentation")
	batch.Spec.DocumentsTasks = []string{"t9"}

	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-documentation", "tatara"), batch, covered)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"documentation","payload":{"action":"declined","reason":"nothing user-visible"}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	got := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.StateDone, got.Status.State)
	require.Equal(t, stage.ReasonDocTimeout, got.Status.StateReason)
	require.False(t, tatarav1alpha1.Parked(got), "a declined documentation batch is DONE, not parked")
	require.Len(t, got.Status.Notes, 1)
	require.Equal(t, "declined: nothing user-visible", got.Status.Notes[0].Body)
	require.Equal(t, "t1", e.task(t, "t9").Status.DocumentedBy)
}

// TestOutcome_Documentation_DeclineWithoutAReasonIs400: the documentation
// schema shares implement's single `reason` field and its per-action legality.
func TestOutcome_Documentation_DeclineWithoutAReasonIs400(t *testing.T) {
	batch := taskV2("t1", "tatara", "documentation", tatarav1alpha1.StateUnderImplementation, "documentation")
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-documentation", "tatara"), batch)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"documentation","payload":{"action":"declined"}}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "action=declined requires a non-empty reason")
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

// gateDiscussFingerprint is the fingerprint of the gate discuss body used
// below. It is computed the way the handler computes it: sha256("implement|" +
// canonical payload JSON). The test asks the server for it rather than
// duplicating the hash, by POSTing once against a throwaway env and reading
// the condition back.
func gateDiscussFingerprint(t *testing.T) string {
	t.Helper()
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("fp", "tatara", "implement", tatarav1alpha1.StateRefined, "implement"))
	w := e.do(t, http.MethodPost, "/tasks/fp/outcome", gateDiscussBody)
	require.Equal(t, http.StatusOK, w.Code)
	cond := tatarav1alpha1.OutcomeCondition(e.task(t, "fp"))
	require.NotNil(t, cond)
	return cond.Message
}

const gateDiscussBody = `{"kind":"implement","payload":{"action":"discuss","reason":"r"}}`

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
	fp := gateDiscussFingerprint(t)

	t.Run("inside the TTL: 409, the task is untouched", func(t *testing.T) {
		e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
			repoV2("tatara-operator", "tatara"),
			taskV2("t1", "tatara", "implement", tatarav1alpha1.StateRefined, "implement"))
		outcomeClaimStub(t, e, "t1", fp, tatarav1alpha1.OutcomeReasonClaimed,
			frozenNow.Add(-tatarav1alpha1.OutcomeClaimTTL+time.Second))

		w := e.do(t, http.MethodPost, "/tasks/t1/outcome", gateDiscussBody)
		require.Equal(t, http.StatusConflict, w.Code, "a fresh bare claim means another replica is mid-flight")
		require.Equal(t, tatarav1alpha1.StateRefined, e.task(t, "t1").Status.State,
			"a 409 in-flight answer must change nothing")
	})

	t.Run("past the TTL: re-claim and proceed", func(t *testing.T) {
		e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
			repoV2("tatara-operator", "tatara"),
			taskV2("t1", "tatara", "implement", tatarav1alpha1.StateRefined, "implement"))
		outcomeClaimStub(t, e, "t1", fp, tatarav1alpha1.OutcomeReasonClaimed,
			frozenNow.Add(-tatarav1alpha1.OutcomeClaimTTL-time.Second))

		w := e.do(t, http.MethodPost, "/tasks/t1/outcome", gateDiscussBody)
		require.Equal(t, http.StatusOK, w.Code, "an orphaned stub must self-heal, not 409 forever")
		got := e.task(t, "t1")
		require.Equal(t, tatarav1alpha1.StateRefined, got.Status.State,
			"a park never changes state; the outcome must actually be PROCESSED, not replayed as a no-op")
		require.True(t, tatarav1alpha1.Parked(got))
		require.Equal(t, "awaiting-human", got.Status.ParkReason)
		cond := tatarav1alpha1.OutcomeCondition(got)
		require.NotNil(t, cond)
		require.Equal(t, "Implement", cond.Reason, "commit must overwrite the claim's Reason")
	})
}

// SPEC TEST 4. A COMMITTED outcome (Reason != "Outcome") still replays 200 with
// the unchanged Task. This is the TTL-stopped pod's honest retry and it must
// never 409 the Task into failure - the property the whole condition exists for.
func TestOutcome_CommittedOutcomeStillReplays200(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateRefined, "implement"))

	first := e.do(t, http.MethodPost, "/tasks/t1/outcome", gateDiscussBody)
	require.Equal(t, http.StatusOK, first.Code)
	before := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.StateRefined, before.Status.State)
	require.True(t, tatarav1alpha1.Parked(before))
	require.Equal(t, "Implement", tatarav1alpha1.OutcomeCondition(before).Reason)

	second := e.do(t, http.MethodPost, "/tasks/t1/outcome", gateDiscussBody)
	require.Equal(t, http.StatusOK, second.Code, "an identical retry of a COMMITTED outcome replays")
	after := e.task(t, "t1")
	require.Equal(t, before.Status.State, after.Status.State)
	require.Equal(t, before.Status.ParkReason, after.Status.ParkReason)
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
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateRefined, "implement"))

	bad := `{"kind":"implement","payload":{"action":"discuss","reason":"  "}}`
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
	fixed := e.do(t, http.MethodPost, "/tasks/t1/outcome", gateDiscussBody)
	require.Equal(t, http.StatusOK, fixed.Code)
	got := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.StateRefined, got.Status.State)
	require.True(t, tatarav1alpha1.Parked(got))
}

// The two top-of-handler gates run before any kind handler and stamp nothing,
// so they hold a claim they must not keep.
func TestOutcome_TopOfHandlerGatesReleaseTheClaim(t *testing.T) {
	t.Run("kind-mismatch", func(t *testing.T) {
		e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
			repoV2("tatara-operator", "tatara"),
			taskV2("t1", "tatara", "implement", tatarav1alpha1.StateRefined, "implement"))
		w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
			`{"kind":"review","payload":{"verdict":"approve","reviewedSHAs":[]}}`)
		require.Equal(t, http.StatusConflict, w.Code)
		require.Nil(t, tatarav1alpha1.OutcomeCondition(e.task(t, "t1")))
	})
	t.Run("terminal-stage", func(t *testing.T) {
		e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
			repoV2("tatara-operator", "tatara"),
			parkedTaskV2(t, "t1", "tatara", "implement", tatarav1alpha1.StateRefined, "implement", stage.ReasonAwaitingHuman))
		w := e.do(t, http.MethodPost, "/tasks/t1/outcome", gateDiscussBody)
		require.Equal(t, http.StatusConflict, w.Code)
		require.Nil(t, tatarav1alpha1.OutcomeCondition(e.task(t, "t1")))
	})
	t.Run("payload decode", func(t *testing.T) {
		e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
			repoV2("tatara-operator", "tatara"),
			taskV2("t1", "tatara", "implement", tatarav1alpha1.StateRefined, "implement"))
		w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
			`{"kind":"implement","payload":{"action":"discuss","bogusField":1}}`)
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
		taskV2("t1", "tatara", "clarify", tatarav1alpha1.StateRefined, "bogus"))

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
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateAwaitingReview, "review"),
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
		// deployed carries NO edge to rejected (deployed work is never rewound,
		// per the F.3 table), so the gate's action=rejected is illegal from
		// here. #521: nothing targets a park - stage.Park is orthogonal to the
		// edge table - so the discuss action this test used to exercise can
		// no longer produce an illegal transition at all; rejected still can.
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateDeployed, "implement"))

	body := `{"kind":"implement","payload":{"action":"rejected","reason":"nope"}}`
	for i := range 3 {
		w := e.do(t, http.MethodPost, "/tasks/t1/outcome", body)
		require.Equal(t, http.StatusConflict, w.Code, "attempt %d", i)
		got := e.task(t, "t1")
		require.Equal(t, tatarav1alpha1.StateDeployed, got.Status.State)
		require.Empty(t, got.Status.Notes,
			"a refused transition must leave NO note behind, on any attempt")
		require.Nil(t, tatarav1alpha1.OutcomeCondition(got),
			"an illegal-transition 409 is class B: it must release its claim")
	}
}

// SPEC TEST 5, black-box half: a rejection never undoes a COMMITTED outcome.
//
// The invariant lives in release's ownership check and is asserted directly in
// release_internal_test.go. It used to be observable through the handler only as
// an EFFECT: a DIFFERENT outcome's claim overwrote the committed condition in the
// single OutcomeAccepted slot BEFORE any gate ran, so the committed condition was
// already gone by the time release could look at it, and the slot ended up EMPTY.
//
// #578 MADE THE CLAIM LAZY, so the condition itself now survives too: the
// rejection 409s at the terminal gate having written nothing at all, and the
// committed record it never touched is still there afterwards - reason and
// fingerprint unchanged.
func TestOutcome_RejectionNeverUndoesACommittedOutcome(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateRefined, "implement"))

	ok := e.do(t, http.MethodPost, "/tasks/t1/outcome", gateDiscussBody)
	require.Equal(t, http.StatusOK, ok.Code)
	before := e.task(t, "t1")
	require.Equal(t, "Implement", tatarav1alpha1.OutcomeCondition(before).Reason)
	require.Equal(t, tatarav1alpha1.StateRefined, before.Status.State, "a park never changes state")
	require.True(t, tatarav1alpha1.Parked(before))

	// A DIFFERENT outcome now arrives. The Task is parked (terminal for this
	// purpose), so it 409s at the terminal gate, which releases the claim it
	// just took.
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"rejected","reason":"x"}}`)
	require.Equal(t, http.StatusConflict, w.Code)

	after := e.task(t, "t1")
	require.Equal(t, before.Status.State, after.Status.State,
		"a rejection must never undo the committed outcome's effect")
	require.Equal(t, before.Status.ParkReason, after.Status.ParkReason)
	require.Equal(t, before.Status.Notes, after.Status.Notes)
	got := tatarav1alpha1.OutcomeCondition(after)
	require.NotNil(t, got, "#578: a 409 that never claimed cannot clobber the committed record")
	require.Equal(t, tatarav1alpha1.OutcomeCondition(before).Reason, got.Reason)
	require.Equal(t, tatarav1alpha1.OutcomeCondition(before).Message, got.Message,
		"the committed outcome's fingerprint must survive a different outcome's rejection")
}

// A kind=review submit_outcome(review) against an already-MERGED PR is a 2xx
// no-op (the review target landed), NOT a 400 that respawn-loops the pod.
func TestOutcome_Review_MergedMR_NoOpNot400(t *testing.T) {
	mr := mrV2("tatara-agent-skills", 33, "t1")
	mr.Status.State = "merged"
	before := testutil.ToFloat64(obs.RestOutcomeAcceptedTotal.WithLabelValues("review", "mr-terminal-noop"))
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-agent-skills", "tatara"),
		taskV2("t1", "tatara", "review", tatarav1alpha1.StateAwaitingReview, "review"), mr)
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"review","payload":{"verdict":"approve","reviewedSHAs":[{"repo":"tatara-agent-skills","number":33,"sha":"s"}]}}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"noop":true`)
	require.Contains(t, w.Body.String(), `"reason":"mr-terminal"`)
	after := testutil.ToFloat64(obs.RestOutcomeAcceptedTotal.WithLabelValues("review", "mr-terminal-noop"))
	require.Equal(t, before+1, after,
		"operator_rest_outcome_accepted_total{kind=review,outcome=mr-terminal-noop} must record the terminal no-op")
}

// A kind=review submit_outcome against an MR a maintainer TOOK OVER (the parent
// review Task controller-owns zero MRs; the MR's controller flag moved to a
// takeover Task, this Task demoted to a plain owner) is a 2xx no-op, NOT the 400
// that respawn-loops the pod. The reconciler-side finalize is what actually
// retires the Task; this only ends the in-flight turn cleanly.
func TestOutcome_Review_TakenOverMR_NoOpNot400(t *testing.T) {
	// The MR's controller is the takeover Task; the review Task t1 is a demoted
	// plain owner, so it controller-owns zero MRs.
	mr := mrV2("tatara-agent-skills", 33, "tk1", func(m *tatarav1alpha1.MergeRequest) {
		m.OwnerReferences = []metav1.OwnerReference{ownerRef("t1", false), ownerRef("tk1", true)}
		m.Status.Ownership = tatarav1alpha1.OwnershipTatara
		m.Status.OwnershipReason = "takeover-requested-by:maintainer"
	})
	review := taskV2("t1", "tatara", "review", tatarav1alpha1.StateAwaitingReview, "review")
	review.Status.MRRefs = []string{tatarav1alpha1.MergeRequestName("tatara-agent-skills", 33)}
	takeover := taskV2("tk1", "tatara", "takeover", tatarav1alpha1.StateUnderImplementation, "implement")

	before := testutil.ToFloat64(obs.RestOutcomeAcceptedTotal.WithLabelValues("review", "mr-taken-over-noop"))
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-agent-skills", "tatara"), review, takeover, mr)
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"review","payload":{"verdict":"approve","reviewedSHAs":[{"repo":"tatara-agent-skills","number":33,"sha":"s"}]}}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"noop":true`)
	require.Contains(t, w.Body.String(), `"reason":"mr-taken-over"`)
	after := testutil.ToFloat64(obs.RestOutcomeAcceptedTotal.WithLabelValues("review", "mr-taken-over-noop"))
	require.Equal(t, before+1, after,
		"operator_rest_outcome_accepted_total{kind=review,outcome=mr-taken-over-noop} must record the takeover no-op")
}

// A kind=review Task that never owned any MR (mrRefs empty, zero owned MRs) must
// KEEP the pre-existing 400: neither terminalNoop (empty slice is not terminal)
// nor the takeover no-op (TaskTakenOver is false with no mrRefs) may swallow it.
func TestOutcome_Review_NoMRsAtAll_Still400(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-agent-skills", "tatara"),
		taskV2("t1", "tatara", "review", tatarav1alpha1.StateAwaitingReview, "review"))
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"review","payload":{"verdict":"approve","reviewedSHAs":[{"repo":"tatara-agent-skills","number":33,"sha":"s"}]}}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "this task owns no open MR")
}

func TestOutcome_Review_ClosedMR_NoOpNot400(t *testing.T) {
	mr := mrV2("tatara-agent-skills", 33, "t1")
	mr.Status.State = "closed"
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-agent-skills", "tatara"),
		taskV2("t1", "tatara", "review", tatarav1alpha1.StateAwaitingReview, "review"), mr)
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"review","payload":{"verdict":"approve","reviewedSHAs":[{"repo":"tatara-agent-skills","number":33,"sha":"s"}]}}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"reason":"mr-terminal"`)
}

// An OPEN MR is unchanged: the review handler proceeds through the normal
// live-head-read path, never the terminal no-op. reviewPanicForge (not the
// bare panicForge{} the brief sketched) answers GetPRHead with the reviewed
// SHA so the normal path completes instead of nil-pointer-panicking the whole
// test binary on the live head read every open-MR review legitimately makes.
func TestOutcome_Review_OpenMR_NotNoOp(t *testing.T) {
	mr := mrV2("tatara-agent-skills", 33, "t1") // open
	e := buildV2(t, v2Opts{writer: &reviewPanicForge{heads: map[int]string{33: "s"}}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-agent-skills", "tatara"),
		taskV2("t1", "tatara", "review", tatarav1alpha1.StateAwaitingReview, "review"), mr)
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"review","payload":{"verdict":"approve","reviewedSHAs":[{"repo":"tatara-agent-skills","number":33,"sha":"s"}]}}`)
	require.NotContains(t, w.Body.String(), `"reason":"mr-terminal"`, "an open MR is never the terminal no-op")
}

// The gate (implement) Task a brainstorm proposal becomes is minted directly
// here, and carried the same issue #517 pod-name gap as the intake, docbatch
// and takeover mints: no annotation, so it fell back to wrapper-<task-name>.
func TestOutcome_Brainstorm_ProposedGateTaskCarriesPodName(t *testing.T) {
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StateRefined, "brainstorm"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"brainstorm","payload":{
	  "action":"propose","proposals":[
	    {"repo":"tatara-operator","title":"one","body":"b","kind":"bug"}]}}`)
	require.Equal(t, http.StatusOK, w.Code)

	var tasks tatarav1alpha1.TaskList
	require.NoError(t, e.c.List(context.Background(), &tasks, client.InNamespace(ns)))
	var gateTask *tatarav1alpha1.Task
	for i := range tasks.Items {
		if tasks.Items[i].Spec.Kind == "implement" {
			gateTask = &tasks.Items[i]
		}
	}
	require.NotNil(t, gateTask)
	// recordingForge.nextNumber starts at 100; the first CreateIssue yields 101.
	// "imp" is the implement kind's pod-name type token (#521: was "clr").
	require.Equal(t, "imp-tatara-tatara-operator-i101", agent.PodName(gateTask))
}

// --- upgrade --------------------------------------------------------------

// An upgrade agent has NO approval gate: nobody filed an issue for a scheduled
// dependency bump and there is no maintainer comment to cite. Its outcome shares
// documentation's schema (action enum submitted|declined) and routes straight to
// oc.implement, so it never reaches oc.gate.
func TestOutcome_Upgrade_SubmittedRoutesToImplementAndAwaitsReview(t *testing.T) {
	e := buildV2(t, v2Opts{writer: &reviewPanicForge{heads: map[int]string{295: "live-head", 80: "live-head-cli"}}},
		projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"), repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "upgrade", tatarav1alpha1.StateUnderImplementation, "upgrade"),
		mrV2("tatara-operator", 295, "t1"), mrV2("tatara-cli", 80, "t1"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"upgrade","payload":{"action":"submitted","title":"cilium 1.16 -> 1.17","body":"hop 1 of 4",`+
			`"changeSignificance":"minor","mergeOrder":["tatara-operator","tatara-cli"]}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	got := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.StateAwaitingReview, got.Status.State,
		"an upgrade Task's own merge request is reviewed ON THE SAME TASK")
	require.Equal(t, []string{"tatara-operator", "tatara-cli"}, got.Spec.MergeOrder,
		"mergeOrder is what makes a multi-repo upgrade unit merge in publish-dependency order")
	require.Equal(t, "minor", e.mr(t, tatarav1alpha1.MergeRequestName("tatara-operator", 295)).Status.Significance)
}

// The gate fields are refused on an upgrade outcome exactly as they are on an
// implement `submitted`: a scheduled upgrade that claims an approval is a client
// bug, not a shortcut.
func TestOutcome_Upgrade_GateFieldsAreRefused(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "upgrade", tatarav1alpha1.StateUnderImplementation, "upgrade"),
		mrV2("tatara-operator", 295, "t1"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"upgrade","payload":{"action":"submitted","title":"T","body":"B","changeSignificance":"patch",`+
			`"approvingMaintainer":"someone"}}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "only valid when action=approved")
}

// declined is a correct and common answer for a scheduled kind: most cycles have
// nothing worth taking. It parks with a reason rather than shipping nothing.
func TestOutcome_Upgrade_DeclinedParksWithItsReason(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "upgrade", tatarav1alpha1.StateUnderImplementation, "upgrade"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"upgrade","payload":{"action":"declined","reason":"every candidate is under its minimumReleaseAge"}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	got := e.task(t, "t1")
	require.True(t, tatarav1alpha1.Parked(got), "a declined upgrade has shipped nothing; it parks and the reaper collects it")
	require.Len(t, got.Status.Notes, 1)
	require.Equal(t, "declined: every candidate is under its minimumReleaseAge", got.Status.Notes[0].Body)
}

// The pod's claim is NEVER trusted: kind must equal status.agentKind.
func TestOutcome_Upgrade_KindMustMatchStatusAgentKind(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		mrV2("tatara-operator", 295, "t1"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"upgrade","payload":{"action":"submitted","title":"T","body":"B","changeSignificance":"patch"}}`)
	require.Equal(t, http.StatusConflict, w.Code)
	require.Contains(t, w.Body.String(), "kind does not match the task's agent kind")
}

// TestOutcome_Commit_NotesCapEvictsOldest_WithNoteCap: commit is the single door
// every agentNote write goes through. When notes hit MaxNotes, WithNoteCap evicts
// the oldest down to MaxNotes. The evicted batch goes to the Spiller; Discarding
// drops it and records no entry in Status.Stats.NotesSpilledRefs while still
// advancing NotesSpilled (#616).
func TestOutcome_Commit_NotesCapEvictsOldest_WithNoteCap(t *testing.T) {
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	task := taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement")

	// Seed with exactly MaxNotes=50 notes, spaced by 1 second.
	for i := 0; i < tatarav1alpha1.MaxNotes; i++ {
		at := metav1.NewTime(now.Add(time.Duration(i) * time.Second))
		task.Status.Notes = append(task.Status.Notes, tatarav1alpha1.Note{
			At: at, Agent: "operator", Kind: "note", Body: fmt.Sprintf("seeded note %d", i),
		})
	}

	e := buildV2(t, v2Opts{writer: panicForge{}, now: func() time.Time { return now }, spillerFor: func(*tatarav1alpha1.Project) objbudget.Spiller { return objbudget.Discarding }},
		projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"), task)

	// Post a declined outcome which writes an agent note through commit.
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"declined","reason":"the issue is already fixed"}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	// Read the final state.
	got := e.task(t, "t1")

	// Journal did not grow to 51.
	require.Len(t, got.Status.Notes, tatarav1alpha1.MaxNotes,
		"commit with WithNoteCap must evict the oldest note to stay at MaxNotes, not grow to 51")

	// Newest note (the one from the declined outcome) is present.
	require.Contains(t, got.Status.Notes[len(got.Status.Notes)-1].Body, "declined: the issue is already fixed",
		"the newly appended decline note must be at the end")

	// Oldest seeded note is gone (evicted).
	for _, n := range got.Status.Notes {
		require.NotEqual(t, "seeded note 0", n.Body,
			"the oldest seeded note must be evicted when the cap is hit")
	}

	// NotesSpilledRefs is empty (Discarding doesn't record refs).
	require.Empty(t, got.Status.Stats.NotesSpilledRefs,
		"Discarding spiller must not record any spilledRefs in Status.Stats.NotesSpilledRefs")
}
