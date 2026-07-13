package restapi_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
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

// THE HEAD-MOVED 409. The operator re-reads the LIVE head and refuses a verdict
// whose reported SHA moved. NOTHING is stamped.
func TestOutcome_Review_HeadMovedIs409AndStampsNothing(t *testing.T) {
	forge := &reviewPanicForge{heads: map[int]string{295: "sha-NEW"}}
	e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StageReviewing, "review"),
		mrV2("tatara-operator", 295, "t1"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"review","payload":{
	  "verdict":"approve","reviewedSHAs":[{"repo":"tatara-operator","number":295,"sha":"sha-OLD"}]}}`)
	require.Equal(t, http.StatusConflict, w.Code)
	require.Contains(t, w.Body.String(), "head moved since you reviewed it")

	mr := e.mr(t, tatarav1alpha1.MergeRequestName("tatara-operator", 295))
	require.Nil(t, mr.Status.PendingReview)
	require.Empty(t, mr.Status.ReviewedSHA)
	require.Equal(t, "new", mr.Status.Status)
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
