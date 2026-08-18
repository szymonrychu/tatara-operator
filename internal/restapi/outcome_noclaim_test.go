package restapi_test

import (
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// ISSUE #578, DEFECT A: submit_outcome must not mutate and THEN fail.
//
// The idempotency CLAIM used to be stamped at the top of postOutcome, above the
// terminal/parked/kind gates, above the payload decode and above every kind
// handler's read-only validation block - so EVERY 4xx this endpoint can produce
// wrote status.conditions first and failed second. `release()` undid it, which
// made the NET effect nil, but the write was observable (an agent that task_get'd
// in between saw OutcomeAccepted=True for an outcome it had just been told was
// refused, concluded the write had landed, and re-submitted) and best-effort (a
// failed release leaves the bogus claim until OutcomeClaimTTL).
//
// The claim is now LAZY - taken at the boundary between validation and execution
// - so a rejected request writes NOTHING AT ALL. This is the regression pin: for
// every 4xx shape the endpoint has, the Task must come back byte-identical.
func TestOutcome_A_NoStatusMutationOnAnyRejection(t *testing.T) {
	const (
		implTask   = "t1"
		wantNoCond = "a 4xx must leave NO OutcomeAccepted condition: not a committed one, not a claim"
	)

	cases := []struct {
		name string
		// task is the fixture this case posts against, so the gate cases can
		// present a done / parked / wrong-kind Task.
		task func() *tatarav1alpha1.Task
		mrs  bool
		body string
		want int
	}{
		{
			name: "implement submitted with no title",
			task: func() *tatarav1alpha1.Task {
				return taskV2(implTask, "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement")
			},
			mrs:  true,
			body: `{"kind":"implement","payload":{"action":"submitted","body":"b","changeSignificance":"patch"}}`,
			want: http.StatusBadRequest,
		},
		{
			name: "implement with an action the enum does not have",
			task: func() *tatarav1alpha1.Task {
				return taskV2(implTask, "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement")
			},
			mrs:  true,
			body: `{"kind":"implement","payload":{"action":"teleported"}}`,
			want: http.StatusBadRequest,
		},
		{
			name: "implement declined with a blank reason",
			task: func() *tatarav1alpha1.Task {
				return taskV2(implTask, "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement")
			},
			mrs:  true,
			body: `{"kind":"implement","payload":{"action":"declined","reason":"   "}}`,
			want: http.StatusBadRequest,
		},
		{
			name: "implement submitted owning NO mr at all",
			task: func() *tatarav1alpha1.Task {
				return taskV2(implTask, "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement")
			},
			mrs:  false,
			body: `{"kind":"implement","payload":{"action":"submitted","title":"t","body":"b","changeSignificance":"patch"}}`,
			want: http.StatusBadRequest,
		},
		{
			name: "review with an unknown verdict",
			task: func() *tatarav1alpha1.Task {
				return taskV2(implTask, "tatara", "implement", tatarav1alpha1.StateAwaitingReview, "review")
			},
			mrs:  true,
			body: `{"kind":"review","payload":{"verdict":"shrug","reviewedSHAs":[{"repo":"tatara-operator","number":295,"sha":"sha1"}]}}`,
			want: http.StatusBadRequest,
		},
		{
			name: "review with no reviewedSHAs",
			task: func() *tatarav1alpha1.Task {
				return taskV2(implTask, "tatara", "implement", tatarav1alpha1.StateAwaitingReview, "review")
			},
			mrs:  true,
			body: `{"kind":"review","payload":{"verdict":"approve"}}`,
			want: http.StatusBadRequest,
		},
		{
			name: "a key the operator does not know",
			task: func() *tatarav1alpha1.Task {
				return taskV2(implTask, "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement")
			},
			mrs:  true,
			body: `{"kind":"implement","payload":{"action":"declined","reason":"r","bogusField":1}}`,
			want: http.StatusBadRequest,
		},
		{
			name: "the kind does not match status.agentKind",
			task: func() *tatarav1alpha1.Task {
				return taskV2(implTask, "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement")
			},
			mrs:  true,
			body: `{"kind":"brainstorm","payload":{"action":"skip","reason":"r"}}`,
			want: http.StatusConflict,
		},
		{
			name: "the task is already terminal",
			task: func() *tatarav1alpha1.Task {
				return taskV2(implTask, "tatara", "implement", tatarav1alpha1.StateDone, "implement")
			},
			mrs:  true,
			body: `{"kind":"implement","payload":{"action":"declined","reason":"r"}}`,
			want: http.StatusConflict,
		},
		{
			name: "the task is parked",
			task: func() *tatarav1alpha1.Task {
				tk := taskV2(implTask, "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement")
				tk.Status.ParkReason = "awaiting-human"
				return tk
			},
			mrs:  true,
			body: `{"kind":"implement","payload":{"action":"declined","reason":"r"}}`,
			want: http.StatusConflict,
		},
		{
			name: "brainstorm with an out-of-range proposal set",
			task: func() *tatarav1alpha1.Task {
				return taskV2(implTask, "tatara", "brainstorm", tatarav1alpha1.StateRefined, "brainstorm")
			},
			body: `{"kind":"brainstorm","payload":{"action":"propose","proposals":[]}}`,
			want: http.StatusBadRequest,
		},
		{
			name: "refine with nothing in it",
			task: func() *tatarav1alpha1.Task {
				return taskV2(implTask, "tatara", "refine", tatarav1alpha1.StateRefined, "refine")
			},
			body: `{"kind":"refine","payload":{"folds":[],"closes":[],"links":[]}}`,
			want: http.StatusBadRequest,
		},
		{
			// The alertRules spec merge used to run BEFORE this rejection, so the
			// 400 followed a mutation on the SPEC as well as on the claim.
			name: "incident comment_issue against an untracked target",
			task: func() *tatarav1alpha1.Task {
				return taskV2(implTask, "tatara", "incident", tatarav1alpha1.StateRefined, "incident")
			},
			body: `{"kind":"incident","payload":{"action":"comment_issue","reason":"r","alertRules":["rule-a"],
			         "comment":{"repo":"tatara-operator","number":404,"body":"b"}}}`,
			want: http.StatusBadRequest,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixtures := []client.Object{projectV2("tatara"), scmSecretV2(),
				repoV2("tatara-operator", "tatara"), tc.task()}
			if tc.mrs {
				fixtures = append(fixtures, mrV2("tatara-operator", 295, implTask))
			}
			e := buildV2(t, v2Opts{writer: panicForge{}}, fixtures...)

			before := e.task(t, implTask)
			w := e.do(t, http.MethodPost, "/tasks/"+implTask+"/outcome", tc.body)
			require.Equal(t, tc.want, w.Code, "body: %s", w.Body.String())

			after := e.task(t, implTask)
			require.Nil(t, tatarav1alpha1.OutcomeCondition(after), wantNoCond)
			require.Equal(t, before.Status, after.Status,
				"a rejected outcome must not mutate status AT ALL - #578 defect A")
			require.Equal(t, before.Spec, after.Spec,
				"a rejected outcome must not mutate spec either")
		})
	}
}

// ISSUE #578, DEFECT B: a Task whose MR already merged, woken by NEW human
// instructions on the issue, must not be answered with a doomed 400.
//
// THIS IS THE LIVE SHAPE THAT BURNED mt-i-mtg-decks-22. spec.kind is ISSUE, not
// review - stage.AgentKindFor keys awaiting-review on the STATE, so a kind=issue
// Task there runs an agentKind=REVIEW pod. The all-MRs-terminal carve-out was
// gated on spec.kind == "review", so this Task fell straight through to
// `400 this task owns no open MR`, which the wrapper re-prompted as a MANDATORY
// retry: 7 pod runs, 3 pod recreations, parked(no-outcome).
func TestOutcome_B_MergedMRThenNewInstructionsIsAccepted(t *testing.T) {
	cases := []struct {
		name      string
		specKind  string
		state     string
		agentKind string
		mrState   string
		body      string
		wantLabel string
		wantJSON  string
	}{
		{
			name:      "kind=issue at awaiting-review, its own MR merged out of band",
			specKind:  "issue",
			state:     tatarav1alpha1.StateAwaitingReview,
			agentKind: "review",
			mrState:   "merged",
			body: `{"kind":"review","payload":{"verdict":"approve","reviewedSHAs":[
			         {"repo":"tatara-operator","number":295,"sha":"sha1"}]}}`,
			wantLabel: "mr-terminal-noop",
			wantJSON:  `{"noop":true,"reason":"mr-terminal"}`,
		},
		{
			name:      "kind=implement at awaiting-review, its own MR closed out of band",
			specKind:  "implement",
			state:     tatarav1alpha1.StateAwaitingReview,
			agentKind: "review",
			mrState:   "closed",
			body: `{"kind":"review","payload":{"verdict":"approve","reviewedSHAs":[
			         {"repo":"tatara-operator","number":295,"sha":"sha1"}]}}`,
			wantLabel: "mr-terminal-noop",
			wantJSON:  `{"noop":true,"reason":"mr-terminal"}`,
		},
		{
			// THE SUBMIT SIDE OF THE SAME FACT, and it is NOT a no-op any more.
			// A merge request that CLOSED unmerged is terminal but is not a
			// delivery, so the Task still has nothing to attach its outcome to and
			// the no-op stands. The merged half moved to
			// TestOutcome_Implement_EveryOwnedMRMerged_DeliversInsteadOfNoOpping:
			// the no-op there neither advanced the Task nor parked it, which left
			// it with no reachable terminal and cost 127 pods.
			name:      "kind=issue submitting code when its only MR was closed unmerged",
			specKind:  "issue",
			state:     tatarav1alpha1.StateUnderImplementation,
			agentKind: "implement",
			mrState:   "closed",
			body: `{"kind":"implement","payload":{"action":"submitted","title":"t","body":"b",
			         "changeSignificance":"patch"}}`,
			wantLabel: "mr-terminal-noop",
			wantJSON:  `{"noop":true,"reason":"mr-terminal"}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mr := mrV2("tatara-operator", 295, "t1")
			mr.Status.State = tc.mrState
			before := testutil.ToFloat64(
				obs.RestOutcomeAcceptedTotal.WithLabelValues(tc.agentKind, tc.wantLabel))

			e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
				repoV2("tatara-operator", "tatara"),
				taskV2("t1", "tatara", tc.specKind, tc.state, tc.agentKind), mr)

			stateBefore := e.task(t, "t1").Status.State
			w := e.do(t, http.MethodPost, "/tasks/t1/outcome", tc.body)

			require.Equal(t, http.StatusOK, w.Code,
				"a merged/closed review target is NOT a client error: the agent cannot fix it by retrying")
			require.JSONEq(t, tc.wantJSON, w.Body.String())
			require.Equal(t, before+1, testutil.ToFloat64(
				obs.RestOutcomeAcceptedTotal.WithLabelValues(tc.agentKind, tc.wantLabel)),
				"the no-op is COUNTED as accepted, never as a silent success")

			after := e.task(t, "t1")
			require.Equal(t, stateBefore, after.Status.State,
				"the endpoint does not finalize; the convergent reconciler edge does")
			require.Nil(t, tatarav1alpha1.OutcomeCondition(after),
				"a no-op commits nothing, so it must hold no claim either")
		})
	}
}

// The 400 that REMAINS is the one a retry can actually clear: a Task owning no
// MergeRequest mirror at all (an empty set is not terminal) is a mint/binding
// fault, and once intake repairs the binding the identical resubmit succeeds.
// Widening the carve-out must not swallow it.
func TestOutcome_B_NoMRAtAllIsStillA400(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "issue", tatarav1alpha1.StateAwaitingReview, "review"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"review","payload":{"verdict":"approve","reviewedSHAs":[
		   {"repo":"tatara-operator","number":295,"sha":"sha1"}]}}`)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "this task owns no open MR")
	require.Nil(t, tatarav1alpha1.OutcomeCondition(e.task(t, "t1")),
		"#578 defect A: even the 400 that stays writes nothing")
}
