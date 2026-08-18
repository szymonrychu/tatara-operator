package restapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// errForgeDown is the transient forge failure the fail-open path is proved on.
var errForgeDown = errors.New("forge unreachable")

// notReadyBody is the structured 409 an agent gets back. It is decoded rather
// than string-matched because the field names are a CROSS-REPO CONTRACT
// (tatara-cli keys on `reason`), and a test that only greps the prose would go
// on passing after a rename that breaks the cli.
type notReadyBody struct {
	Reason  string `json:"reason"`
	Error   string `json:"error"`
	Message string `json:"message"`
	Blocked []struct {
		Repo     string   `json:"repo"`
		Number   int      `json:"number"`
		HeadSHA  string   `json:"headSHA"`
		Blockers []string `json:"blockers"`
		Detail   string   `json:"detail"`
	} `json:"blocked"`
}

func decodeNotReady(t *testing.T, raw []byte) notReadyBody {
	t.Helper()
	var out notReadyBody
	require.NoError(t, json.Unmarshal(raw, &out))
	return out
}

// submitImplement is the one-repo implement submission every case below makes.
const submitImplement = `{"kind":"implement","payload":{"action":"submitted","title":"T","body":"B","changeSignificance":"patch"}}`

// THE THREE REFUSAL AXES, one case each (PR B1). Before this the implement
// submitted path made exactly one forge call - a best-effort GetPRHead AFTER the
// transition had already committed - so all three of these advanced the Task to
// awaiting-review and spawned a review pod on a change that could not merge.
func TestOutcome_ImplementSubmit_RefusesADirtyPR(t *testing.T) {
	tests := []struct {
		name string
		// mutate configures the forge and the MR mirror for this axis.
		forge    func(*recordingForge)
		mr       func(*tatarav1alpha1.MergeRequest)
		blockers []string
		detail   string
	}{
		{
			name:     "ci red at the pushed head",
			forge:    func(f *recordingForge) { f.ciStatuses[295] = "failure" },
			blockers: []string{"ci-red"},
			detail:   "CI is RED at the pushed head",
		},
		{
			name:     "the branch conflicts with its base",
			forge:    func(f *recordingForge) { f.mergeStates[295] = scm.MergeStateDirty },
			blockers: []string{"conflict"},
			detail:   "CONFLICTS with its base",
		},
		{
			name:  "a request_changes round with nothing pushed to answer it",
			forge: func(f *recordingForge) {},
			mr: func(m *tatarav1alpha1.MergeRequest) {
				m.Status.Status = "needs-changes"
				m.Status.ReviewedSHA = "live-295"
			},
			blockers: []string{"unresolved-review"},
			detail:   "the head has not moved since",
		},
		{
			name: "all three at once are all named",
			forge: func(f *recordingForge) {
				f.ciStatuses[295] = "failure"
				f.mergeStates[295] = scm.MergeStateDirty
			},
			mr: func(m *tatarav1alpha1.MergeRequest) {
				m.Status.Status = "needs-changes"
				m.Status.ReviewedSHA = "live-295"
			},
			blockers: []string{"ci-red", "conflict", "unresolved-review"},
			detail:   "CI is RED at the pushed head",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			forge := newRecordingForge()
			forge.heads["295"] = "live-295"
			tc.forge(forge)
			mrOpts := []func(*tatarav1alpha1.MergeRequest){}
			if tc.mr != nil {
				mrOpts = append(mrOpts, tc.mr)
			}
			e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(),
				repoV2("tatara-operator", "tatara"),
				taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
				mrV2("tatara-operator", 295, "t1", mrOpts...))

			w := e.do(t, http.MethodPost, "/tasks/t1/outcome", submitImplement)
			require.Equal(t, http.StatusConflict, w.Code,
				"a dirty PR must be REFUSED while the agent is still in-turn, not accepted")

			body := decodeNotReady(t, w.Body.Bytes())
			require.Equal(t, "pr-not-ready", body.Reason)
			require.Len(t, body.Blocked, 1)
			require.Equal(t, "tatara-operator", body.Blocked[0].Repo)
			require.Equal(t, 295, body.Blocked[0].Number)
			require.ElementsMatch(t, tc.blockers, body.Blocked[0].Blockers,
				"the refusal must name EXACTLY which axis failed - that is the whole point of it")
			require.Contains(t, body.Blocked[0].Detail, tc.detail)
			require.Contains(t, body.Message, "Your turn is NOT over")

			// THE REFUSAL WROTE NOTHING. It runs in the read-only validation phase,
			// before the claim, so there is no transition, no note, no claim to wait
			// out and no park - the agent fixes the PR and submits again.
			got := e.task(t, "t1")
			require.Equal(t, tatarav1alpha1.StateUnderImplementation, got.Status.State)
			require.Empty(t, got.Status.ParkReason, "a refusal must never park the task")
			require.Empty(t, got.Status.Notes, "a refusal must record no agent note")
			require.Nil(t, got.Status.CIWaitSince)
			require.Nil(t, tatarav1alpha1.OutcomeCondition(got),
				"an unclaimed refusal must leave no idempotency claim behind")
			require.Empty(t, got.Spec.MergeOrder, "nothing downstream of the claim may have run")
		})
	}
}

// protectedForge is a recordingForge that ALSO answers the required-context
// question - the GitHub shape on a repo with branch protection. A bare
// recordingForge deliberately does not implement scm.RequiredCheckLister at all:
// that is the GitLab shape, and it must never suppress.
type protectedForge struct {
	*recordingForge
	required bool
	err      error
}

func (f *protectedForge) HasRequiredChecks(context.Context, string, string, int) (bool, error) {
	return f.required, f.err
}

// THE FORGE, NOT THE CHECK LIST, DECIDES WHETHER RED CI BLOCKS. GitHub's
// `unstable` means precisely "this merge is not blocked, and the checks that are
// red are not the ones that block it". tatara-helmfile#424 is the shape: the
// `GitGuardian Security Checks` app was FAILURE, `helmfile-diff` - the only
// required context - was SUCCESS, mergeable_state was `unstable`, and the
// submit was refused with ci-red anyway, so the Task could never leave
// under-implementation.
//
// Every other merge state keeps today's fail-closed behaviour, and `clean` is
// the one that matters most: GitLab reports can_be_merged with a RED pipeline,
// so suppressing on clean would silently stop honouring red CI on every GitLab
// merge request.
//
// Every case here has a required context; the axis that says whether one EXISTS
// is TestOutcome_ImplementSubmit_RedCIIsNotSuppressedWithoutARequiredContext.
func TestOutcome_ImplementSubmit_RedCIIsSuppressedOnlyWhenTheForgeSaysUnstable(t *testing.T) {
	tests := []struct {
		name    string
		forge   func(*recordingForge)
		wantRed bool
	}{
		{
			name:    "unstable is the forge saying the red checks are not required",
			forge:   func(f *recordingForge) { f.mergeStates[295] = scm.MergeStateUnstable },
			wantRed: false,
		},
		{
			name:    "blocked is a red REQUIRED check and still blocks",
			forge:   func(f *recordingForge) { f.mergeStates[295] = scm.MergeStateBlocked },
			wantRed: true,
		},
		{
			name:    "clean is the gitlab shape (can_be_merged with a red pipeline) and still blocks",
			forge:   func(f *recordingForge) { f.mergeStates[295] = scm.MergeStateClean },
			wantRed: true,
		},
		{
			name:    "behind still blocks",
			forge:   func(f *recordingForge) { f.mergeStates[295] = scm.MergeStateBehind },
			wantRed: true,
		},
		{
			name:    "unknown still blocks",
			forge:   func(f *recordingForge) { f.mergeStates[295] = scm.MergeStateUnknown },
			wantRed: true,
		},
		{
			name:    "an unreadable merge state fails CLOSED on the suppression",
			forge:   func(f *recordingForge) { f.mergeStateErr = errForgeDown },
			wantRed: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			forge := newRecordingForge()
			forge.heads["295"] = "live-295"
			forge.ciStatuses[295] = "failure"
			tc.forge(forge)
			e := buildV2(t, v2Opts{writer: &protectedForge{recordingForge: forge, required: true}},
				projectV2("tatara"), scmSecretV2(),
				repoV2("tatara-operator", "tatara"),
				taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
				mrV2("tatara-operator", 295, "t1"))

			w := e.do(t, http.MethodPost, "/tasks/t1/outcome", submitImplement)
			if tc.wantRed {
				require.Equal(t, http.StatusConflict, w.Code)
				require.Equal(t, []string{"ci-red"}, decodeNotReady(t, w.Body.Bytes()).Blocked[0].Blockers)
				require.Equal(t, tatarav1alpha1.StateUnderImplementation, e.task(t, "t1").Status.State)
				return
			}
			require.Equal(t, http.StatusOK, w.Code,
				"the forge says this merge is not blocked by the red checks; refusing it strands the task")
			got := e.task(t, "t1")
			require.Equal(t, tatarav1alpha1.StateAwaitingReview, got.Status.State)
			require.Nil(t, got.Status.CIWaitSince,
				"suppressed ci-red is not a pending hold either: the submit is fully accepted")
		})
	}
}

// `unstable` IS ALSO WHAT A REPO WITH NO REQUIRED CHECKS REPORTS. GitHub says
// `blocked` when a REQUIRED check fails and `unstable` when only non-required
// ones do - so on a repo with no branch protection, or protection with zero
// required status contexts, EVERY red PR is `unstable`. A suppression keyed on
// the merge state alone therefore deletes the whole ci-red axis on every
// unprotected repo, and an agent could submit fully red CI there.
//
// The suppression is gated on the PR actually carrying a required context, and
// it FAILS CLOSED: an unreadable required-context set, and a provider that
// cannot answer the question at all (GitLab), both keep refusing.
func TestOutcome_ImplementSubmit_RedCIIsNotSuppressedWithoutARequiredContext(t *testing.T) {
	tests := []struct {
		name    string
		writer  func(*recordingForge) scm.SCMWriter
		wantRed bool
	}{
		{
			name: "a required context exists and is green: the #424 shape",
			writer: func(f *recordingForge) scm.SCMWriter {
				return &protectedForge{recordingForge: f, required: true}
			},
			wantRed: false,
		},
		{
			name: "an UNPROTECTED repo reports unstable for a fully red PR and must still refuse",
			writer: func(f *recordingForge) scm.SCMWriter {
				return &protectedForge{recordingForge: f, required: false}
			},
			wantRed: true,
		},
		{
			name: "an unreadable required-context set fails CLOSED",
			writer: func(f *recordingForge) scm.SCMWriter {
				return &protectedForge{recordingForge: f, required: true, err: errForgeDown}
			},
			wantRed: true,
		},
		{
			name:    "a forge that cannot answer the question at all (gitlab) fails CLOSED",
			writer:  func(f *recordingForge) scm.SCMWriter { return f },
			wantRed: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			forge := newRecordingForge()
			forge.heads["295"] = "live-295"
			forge.ciStatuses[295] = "failure"
			forge.mergeStates[295] = scm.MergeStateUnstable
			e := buildV2(t, v2Opts{writer: tc.writer(forge)}, projectV2("tatara"), scmSecretV2(),
				repoV2("tatara-operator", "tatara"),
				taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
				mrV2("tatara-operator", 295, "t1"))

			w := e.do(t, http.MethodPost, "/tasks/t1/outcome", submitImplement)
			if tc.wantRed {
				require.Equal(t, http.StatusConflict, w.Code,
					"nothing gates this merge, so nothing suppresses the red either")
				require.Equal(t, []string{"ci-red"}, decodeNotReady(t, w.Body.Bytes()).Blocked[0].Blockers)
				require.Equal(t, tatarav1alpha1.StateUnderImplementation, e.task(t, "t1").Status.State)
				return
			}
			require.Equal(t, http.StatusOK, w.Code)
			require.Equal(t, tatarav1alpha1.StateAwaitingReview, e.task(t, "t1").Status.State)
		})
	}
}

// IDEMPOTENT, AND THE SECOND CALL RE-READS THE FORGE. This is the property that
// makes the refusal usable at all: the agent fixes CI and resubmits the SAME
// payload, which hashes to the SAME fingerprint. If the refusal had claimed it,
// the fixed resubmit would be answered with a replay 200 that advances nothing.
func TestOutcome_ImplementSubmit_RefusalIsIdempotentAndTheFixedResubmitLands(t *testing.T) {
	forge := newRecordingForge()
	forge.heads["295"] = "live-295"
	forge.ciStatuses[295] = "failure"
	e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		mrV2("tatara-operator", 295, "t1"))

	for i := 0; i < 3; i++ {
		w := e.do(t, http.MethodPost, "/tasks/t1/outcome", submitImplement)
		require.Equal(t, http.StatusConflict, w.Code, "refusal %d must be identical", i)
	}
	require.Equal(t, tatarav1alpha1.StateUnderImplementation, e.task(t, "t1").Status.State)

	// The agent fixes the pipeline and submits the byte-identical outcome.
	forge.ciStatuses[295] = "success"
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", submitImplement)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, tatarav1alpha1.StateAwaitingReview, e.task(t, "t1").Status.State,
		"the identical resubmit after a fix must be evaluated afresh, never replayed")
}

// PENDING IS NOT A VERDICT. It is accepted - the agent's work IS done and must
// not be re-run - and held, because handing a change whose pipeline has not
// answered to a review pod is how a reviewer spends a turn on a diff the next
// check_suite delivery invalidates.
func TestOutcome_ImplementSubmit_PendingCIHoldsInsteadOfAdvancing(t *testing.T) {
	forge := newRecordingForge()
	forge.heads["295"] = "live-295"
	forge.ciStatuses[295] = "pending"
	e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		mrV2("tatara-operator", 295, "t1"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", submitImplement)
	require.Equal(t, http.StatusOK, w.Code, "pending is not a refusal: the outcome is ACCEPTED")

	got := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.StateUnderImplementation, got.Status.State,
		"the advance is HELD, not taken")
	require.NotNil(t, got.Status.CIWaitSince, "the hold must be recorded or nothing can resolve it")
	require.Empty(t, got.Status.ParkReason,
		"the hold must NOT park: a parked task takes the reconciler's early return and drops to the 24h mirror cadence")
	require.Equal(t, []string{"tatara-operator"}, got.Spec.MergeOrder,
		"the outcome is fully accepted; only the transition waits")
	require.NotEmpty(t, got.Status.Notes, "the submitted note is part of the acceptance")
}

// A GREEN (or CI-less) HEAD IS THE UNCHANGED PATH. This is the regression fence
// on the whole feature: the common case must still advance in the same call.
func TestOutcome_ImplementSubmit_GreenAdvancesWithNoHold(t *testing.T) {
	for _, ci := range []string{"success", ""} {
		t.Run("ci="+ci, func(t *testing.T) {
			forge := newRecordingForge()
			forge.heads["295"] = "live-295"
			forge.ciStatuses[295] = ci
			e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(),
				repoV2("tatara-operator", "tatara"),
				taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
				mrV2("tatara-operator", 295, "t1"))

			w := e.do(t, http.MethodPost, "/tasks/t1/outcome", submitImplement)
			require.Equal(t, http.StatusOK, w.Code)
			got := e.task(t, "t1")
			require.Equal(t, tatarav1alpha1.StateAwaitingReview, got.Status.State)
			require.Nil(t, got.Status.CIWaitSince)
		})
	}
}

// IT FAILS OPEN. A forge blip must not cost an agent the whole turn's work: the
// outcome is accepted exactly as it was before PR B, and the gate at
// awaiting-review catches a red pipeline one rung later.
func TestOutcome_ImplementSubmit_AForgeReadFailureAcceptsTheOutcome(t *testing.T) {
	forge := newRecordingForge()
	forge.heads["295"] = "live-295"
	forge.prStateErr = errForgeDown
	e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		mrV2("tatara-operator", 295, "t1"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", submitImplement)
	require.Equal(t, http.StatusOK, w.Code)
	got := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.StateAwaitingReview, got.Status.State)
	require.Nil(t, got.Status.CIWaitSince)
}

// MergeStateBlocked and MergeStateBehind are NOT conflicts and must not refuse.
// blocked is what GitHub reports for a failing REQUIRED check (already the CI
// axis) and for a PR awaiting an approval the agent cannot give itself; behind
// is the normal state of every PR opened against a moving default branch.
func TestOutcome_ImplementSubmit_OnlyDirtyIsAConflict(t *testing.T) {
	for _, ms := range []scm.MergeState{scm.MergeStateBlocked, scm.MergeStateBehind, scm.MergeStateUnknown} {
		t.Run(string(ms), func(t *testing.T) {
			forge := newRecordingForge()
			forge.heads["295"] = "live-295"
			forge.mergeStates[295] = ms
			e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(),
				repoV2("tatara-operator", "tatara"),
				taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
				mrV2("tatara-operator", 295, "t1"))

			w := e.do(t, http.MethodPost, "/tasks/t1/outcome", submitImplement)
			require.Equal(t, http.StatusOK, w.Code)
			require.Equal(t, tatarav1alpha1.StateAwaitingReview, e.task(t, "t1").Status.State)
		})
	}
}

// B4: A TAKEN-OVER MR IS STILL GATED. A maintainer takeover flips
// status.ownership to "external"; it does NOT delete the mirror, so the MR is
// still an owned open MergeRequest and the readiness read still sees it.
// Ownership decides who may PUSH and who may MERGE, never whether CI is read.
func TestOutcome_ImplementSubmit_RefusesATakenOverMRToo(t *testing.T) {
	forge := newRecordingForge()
	forge.heads["295"] = "live-295"
	forge.ciStatuses[295] = "failure"
	e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		mrV2("tatara-operator", 295, "t1", func(m *tatarav1alpha1.MergeRequest) {
			m.Status.Ownership = tatarav1alpha1.OwnershipExternal
			m.Status.OwnershipReason = "takeover-requested-by:alice"
		}))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", submitImplement)
	require.Equal(t, http.StatusConflict, w.Code,
		"ownership gates who may push and merge; it never exempts an MR from the CI gate")
	require.Equal(t, []string{"ci-red"}, decodeNotReady(t, w.Body.Bytes()).Blocked[0].Blockers)
}

// THE REVIEW-AGENT EQUIVALENT. An approve hands the change to a POD-LESS merge
// corridor that cannot fix a failing test; issue #476 is what that costs.
func TestOutcome_ReviewApprove_RefusesARedOrConflictedHead(t *testing.T) {
	tests := []struct {
		name  string
		forge func(*recordingForge)
		want  string
	}{
		{"red", func(f *recordingForge) { f.ciStatuses[295] = "failure" }, "ci-red"},
		{"conflicted", func(f *recordingForge) { f.mergeStates[295] = scm.MergeStateDirty }, "conflict"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			forge := newRecordingForge()
			forge.heads["295"] = "live-295"
			tc.forge(forge)
			e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(),
				repoV2("tatara-operator", "tatara"),
				taskV2("t1", "tatara", "review", tatarav1alpha1.StateAwaitingReview, "review"),
				mrV2("tatara-operator", 295, "t1"))

			w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
				`{"kind":"review","payload":{"verdict":"approve","reviewedSHAs":[`+
					`{"repo":"tatara-operator","number":295,"sha":"live-295"}]}}`)
			require.Equal(t, http.StatusConflict, w.Code)
			require.Equal(t, []string{tc.want}, decodeNotReady(t, w.Body.Bytes()).Blocked[0].Blockers)

			mr := e.mr(t, tatarav1alpha1.MergeRequestName("tatara-operator", 295))
			require.Nil(t, mr.Status.PendingReview, "a refused approval must persist no review intent")
			require.Equal(t, "new", mr.Status.Status)
		})
	}
}

// THE APPROVAL DROPS THE CHANGE-REQUEST AXIS. A reviewer cannot push, so
// refusing its approval for "nothing was pushed since the last round" would
// leave it with no legal outcome on that PR at all - and the in-cluster reviewer
// is documented-flaky, so a round that decides the previous round's findings
// were wrong is a real outcome, not a contract violation.
func TestOutcome_ReviewApprove_DoesNotJudgeTheChangeRequestAxis(t *testing.T) {
	forge := newRecordingForge()
	forge.heads["295"] = "live-295"
	e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "review", tatarav1alpha1.StateAwaitingReview, "review"),
		mrV2("tatara-operator", 295, "t1", func(m *tatarav1alpha1.MergeRequest) {
			m.Status.Status = "needs-changes"
			m.Status.ReviewedSHA = "live-295"
		}))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"review","payload":{"verdict":"approve","reviewedSHAs":[`+
			`{"repo":"tatara-operator","number":295,"sha":"live-295"}]}}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "approved", e.mr(t, tatarav1alpha1.MergeRequestName("tatara-operator", 295)).Status.Status)
}

// request_changes is NEVER refused. It is already the "this is not ready"
// answer, and refusing it would leave the reviewer with no legal outcome on the
// exact PR that most needs its findings recorded.
func TestOutcome_ReviewRequestChanges_IsNeverRefusedForReadiness(t *testing.T) {
	forge := newRecordingForge()
	forge.heads["295"] = "live-295"
	forge.ciStatuses[295] = "failure"
	forge.mergeStates[295] = scm.MergeStateDirty
	e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "review", tatarav1alpha1.StateAwaitingReview, "review"),
		mrV2("tatara-operator", 295, "t1"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"review","payload":{"verdict":"request_changes","reviewedSHAs":[`+
			`{"repo":"tatara-operator","number":295,"sha":"live-295"}],`+
			`"findings":[{"repo":"tatara-operator","number":295,"path":"a.go","line":1,"severity":"high","body":"the pipeline is red"}]}}`)
	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, e.mr(t, tatarav1alpha1.MergeRequestName("tatara-operator", 295)).Status.PendingReview)
}

// The refusal counter carries the AXIS SET, not a flat label: the operational
// question a refusal raises is which of the three keeps firing.
func TestOutcome_ImplementSubmit_RefusalCounterNamesTheAxes(t *testing.T) {
	obs.RestOutcomeRejectedTotal.Reset()
	forge := newRecordingForge()
	forge.heads["295"] = "live-295"
	forge.ciStatuses[295] = "failure"
	forge.mergeStates[295] = scm.MergeStateDirty
	e := buildV2(t, v2Opts{writer: forge}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"),
		mrV2("tatara-operator", 295, "t1"))

	require.Equal(t, http.StatusConflict, e.do(t, http.MethodPost, "/tasks/t1/outcome", submitImplement).Code)
	require.Equal(t, 1.0,
		testutil.ToFloat64(obs.RestOutcomeRejectedTotal.WithLabelValues("implement", "not-ready:ci-red+conflict")))
}
