package stage_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// ---------------------------------------------------------------------------
// The transition table IS the contract.
// ---------------------------------------------------------------------------

// TWENTY-SIX edges. A new edge is a design decision, not a diff. The #521 plan
// specified 21; the 22nd and 23rd are both the nightly DOCUMENTATION BATCH,
// which the plan did not trace: it is MINTED straight into under-implementation
// (no driving issue to triage, no approval to gate) and it FINISHES at
// done(doc-timeout) having opened no MR, so `refined -> done` does not reach it.
// The 24th and 25th are `(create) -> done` and `(create) -> rejected`, the #521
// TERMINAL-RESET GUARD: pruning removed status.stage on the read path, every
// pre-#521 Task is served stateless and re-takes the create edge, and one that
// had already finished must land on its terminal instead of being re-triaged.
//
// It was TWENTY-SIX until #604 deleted `(create) -> refined`. That edge had
// exactly one occupant - the maintainer-gated takeover mint - and takeover owns
// ZERO Issue CRs by construction, so the approval gate the state exists to run
// could never grant for the only kind the edge served: every takeover burned a
// pod and then parked awaiting-human, permanently. Takeover now rides
// `(create) -> under-implementation`, which is where its own re-take un-park
// already landed.
//
// The 26th is `under-implementation -> merged`, and it is the ONLY edge in the
// table that exists because the world moved rather than because the platform
// did: every merge request the Task owns has ALREADY merged on the forge, so
// there is nothing left to open, review or merge. LegalFor GUARD 7 makes
// AllMRsMerged the edge's own precondition, which is what keeps it from being a
// door past review out of the state where code is written. It is the same fact
// OwnMRsShippedEdge already finalizes from awaiting-review, recognised one state
// earlier - and without it a Task whose work shipped out of band had NO reachable
// terminal from under-implementation at all: submit_outcome no-opped, the agent
// wrote a handoff note, the pod came down and the dispatcher minted another, 127
// times (upgrade-qe-e4016501fd9107d9).
func TestTransitionTableHasExactlyTwentySixEdges(t *testing.T) {
	n := 0
	for _, edges := range stage.Transitions {
		n += len(edges)
	}
	require.Equal(t, 26, n,
		"the table is the contract; a new edge is a design decision, not a diff")
}

func TestTransitionTableIsExactlyTheDocumentedShape(t *testing.T) {
	want := map[string][]string{
		stage.Create: {v1alpha1.StateNew, v1alpha1.StateUnderImplementation,
			v1alpha1.StateDone, v1alpha1.StateRejected},
		v1alpha1.StateNew:     {v1alpha1.StateRefined, v1alpha1.StateAwaitingReview, v1alpha1.StateRejected},
		v1alpha1.StateRefined: {v1alpha1.StateUnderImplementation, v1alpha1.StateDone, v1alpha1.StateRejected},
		v1alpha1.StateUnderImplementation: {v1alpha1.StateAwaitingReview, v1alpha1.StateMerged,
			v1alpha1.StateRefined, v1alpha1.StateDone, v1alpha1.StateRejected},
		v1alpha1.StateAwaitingReview: {v1alpha1.StateUnderImplementation, v1alpha1.StateMerged, v1alpha1.StateDone, v1alpha1.StateRejected},
		v1alpha1.StateMerged:         {v1alpha1.StateDeployed, v1alpha1.StateAwaitingReview, v1alpha1.StateUnderImplementation, v1alpha1.StateRejected},
		v1alpha1.StateDeployed:       {v1alpha1.StateDone},
		v1alpha1.StateDone:           {stage.Reap},
		v1alpha1.StateRejected:       {stage.Reap},
	}
	require.Len(t, stage.Transitions, len(want))
	for from, tos := range want {
		got := []string{}
		for _, e := range stage.Transitions[from] {
			got = append(got, e.To)
		}
		require.ElementsMatch(t, tos, got, "edges out of %q", from)
	}
}

// NO edge in the table may target a park: parking is orthogonal to state now,
// so it is stage.Park, not an edge. Leaving one behind is how the two axes
// re-fuse.
func TestNoTransitionTargetsAPark(t *testing.T) {
	for from, edges := range stage.Transitions {
		for _, e := range edges {
			require.NotEqual(t, stage.ParkTarget, e.To, "edge out of %q targets a park", from)
			require.NotEqual(t, "parked", e.To)
			require.NotEqual(t, "failed", e.To)
		}
	}
}

// refined -> done is REQUIRED: brainstorm, refine and incident Tasks finish
// without ever opening an MR, so awaiting-review->done and deployed->done are
// both unreachable for them.
func TestRefinedToDoneExistsForTheNonCodeKinds(t *testing.T) {
	for _, kind := range []string{"brainstorm", "refine", "incident"} {
		tk := taskOfKind(v1alpha1.StateRefined, kind)
		require.True(t, stage.LegalFor(tk, nil, v1alpha1.StateRefined, v1alpha1.StateDone),
			"a %s Task has no other path to done", kind)
	}
}

// GUARD 6. refined -> done belongs to the three non-code kinds (brainstorm,
// refine, incident) alone. An implement, takeover, review or documentation
// Task reaching done from refined has opened no MR and faced no review - the
// same hole GUARDS 3-5 close for their own edges.
func TestLegalFor_RefinedToDoneIsTheNonCodeKindsOnly(t *testing.T) {
	for _, kind := range []string{"implement", "takeover", "review", "documentation", "upgrade"} {
		require.False(t,
			stage.LegalFor(taskOfKind(v1alpha1.StateRefined, kind), nil,
				v1alpha1.StateRefined, v1alpha1.StateDone),
			"%s: refined -> done belongs to brainstorm, refine and incident only", kind)
	}
	require.False(t, stage.LegalFor(nil, nil, v1alpha1.StateRefined, v1alpha1.StateDone),
		"no Task in scope is not a licence")
}

// under-implementation -> done is the documentation batch's terminal.
func TestUnderImplementationToDoneExistsForTheDocumentationBatch(t *testing.T) {
	tk := taskOfKind(v1alpha1.StateUnderImplementation, "documentation")
	require.True(t, stage.LegalFor(tk, nil, v1alpha1.StateUnderImplementation, v1alpha1.StateDone))
	require.NoError(t, stage.Enter(tk, nil, v1alpha1.StateDone, stage.ReasonDocTimeout, now))
	require.Equal(t, v1alpha1.StateDone, tk.Status.State)
	require.Equal(t, stage.ReasonDocTimeout, tk.Status.StateReason)
}

// GUARD 4 AND GUARD 5: the two edges the table describes as kind-specific but
// LegalFor left CALLER-GATED. A caller-gated rule is not a rule - the whole
// point of LegalFor is that the guard travels with the edge, and the three
// existing guards prove the house standard. Without these, an implement Task
// could finish at done with no MR and no review (guard 5), and any kind could
// be triaged straight into the review lane (guard 4).
func TestLegalFor_UnderImplementationToDoneIsTheDocumentationBatchOnly(t *testing.T) {
	for _, kind := range []string{"implement", "takeover", "brainstorm", "incident", "refine", "review", "upgrade"} {
		require.False(t,
			stage.LegalFor(taskOfKind(v1alpha1.StateUnderImplementation, kind), nil,
				v1alpha1.StateUnderImplementation, v1alpha1.StateDone),
			"a %s Task reaching done from under-implementation has shipped nothing and been reviewed by nobody", kind)
	}
	require.False(t, stage.LegalFor(nil, nil, v1alpha1.StateUnderImplementation, v1alpha1.StateDone),
		"no Task in scope is not a licence")
}

func TestLegalFor_NewToAwaitingReviewIsTheReviewKindOnly(t *testing.T) {
	require.True(t, stage.LegalFor(taskOfKind(v1alpha1.StateNew, "review"), nil,
		v1alpha1.StateNew, v1alpha1.StateAwaitingReview))
	for _, kind := range []string{"implement", "takeover", "brainstorm", "incident", "refine", "documentation", "upgrade"} {
		require.False(t,
			stage.LegalFor(taskOfKind(v1alpha1.StateNew, kind), nil,
				v1alpha1.StateNew, v1alpha1.StateAwaitingReview),
			"%s: triage routes every non-review kind to refined; the review lane is not a back door past the gate", kind)
	}
	require.False(t, stage.LegalFor(nil, nil, v1alpha1.StateNew, v1alpha1.StateAwaitingReview))
}

func TestEveryStateHasABudgetAndAnOnElapseEdge(t *testing.T) {
	for _, s := range stage.AllStates() {
		_, ok := stage.Budget(s)
		require.True(t, ok, "state %q has no budget row", s)
		_, ok = stage.OnElapse(s)
		require.True(t, ok, "state %q has no onElapse row", s)
	}
}

func TestBudgetTableIsVerbatim(t *testing.T) {
	for _, tc := range []struct {
		state string
		want  time.Duration
	}{
		{v1alpha1.StateNew, 5 * time.Minute},
		{v1alpha1.StateRefined, v1alpha1.ConversationIdleDefault},
		{v1alpha1.StateUnderImplementation, v1alpha1.ConversationIdleDefault},
		{v1alpha1.StateAwaitingReview, v1alpha1.ConversationIdleDefault},
		{v1alpha1.StateMerged, 4 * time.Hour},
		{v1alpha1.StateDeployed, 2 * time.Hour},
		{v1alpha1.StateDone, v1alpha1.DeliveredRetention},
		{v1alpha1.StateRejected, v1alpha1.RejectedRetention},
	} {
		got, ok := stage.Budget(tc.state)
		require.True(t, ok)
		require.Equal(t, tc.want, got, "budget for %q", tc.state)
	}
}

func TestOnElapseTargetsAreParkOrReapAndNothingElse(t *testing.T) {
	for _, s := range stage.AllStates() {
		e, _ := stage.OnElapse(s)
		require.Contains(t, []string{stage.ParkTarget, stage.Reap}, e.To,
			"onElapse for %q must park or reap, never transition", s)
		if e.To == stage.ParkTarget {
			require.True(t, stage.IsParkReason(e.Reason), "onElapse reason %q for %q is not a park reason", e.Reason, s)
		}
	}
}

// ---------------------------------------------------------------------------
// The kind guard.
// ---------------------------------------------------------------------------

// GUARD 1. A kind=review Task may NEVER reach under-implementation or merged.
// Not from anywhere, by any path. Merging or fixing a human's PR is a HUMAN
// action.
func TestLegalFor_KindReviewCanNeverReachUnderImplementationOrMerged(t *testing.T) {
	mrs := []v1alpha1.MergeRequest{{}} // pendingReview nil: GUARD 2 open
	for _, from := range append(stage.AllStates(), stage.Create) {
		for _, to := range []string{v1alpha1.StateUnderImplementation, v1alpha1.StateMerged} {
			tk := taskOfKind(from, "review")
			require.False(t, stage.LegalFor(tk, mrs, from, to),
				"kind=review must never reach %s from %s", to, from)
		}
	}
}

func TestLegalFor_ANonReviewKindKeepsBothEdges(t *testing.T) {
	mrs := []v1alpha1.MergeRequest{{}}
	tk := taskOfKind(v1alpha1.StateAwaitingReview, "implement")
	require.True(t, stage.LegalFor(tk, mrs, v1alpha1.StateAwaitingReview, v1alpha1.StateUnderImplementation))
	require.True(t, stage.LegalFor(tk, mrs, v1alpha1.StateAwaitingReview, v1alpha1.StateMerged))
}

func TestLegalFor_TakeoverKindIsNotBarred(t *testing.T) {
	mrs := []v1alpha1.MergeRequest{{}}
	tk := taskOfKind(v1alpha1.StateAwaitingReview, "takeover")
	require.True(t, stage.LegalFor(tk, mrs, v1alpha1.StateAwaitingReview, v1alpha1.StateMerged))
}

// GUARD 2 (C.5.3). A non-nil pendingReview means a review is owed to the forge
// and the mirror has not recorded it yet; a pod spawned then renders a bundle
// with no findings in it and burns maxReviewRounds. An EMPTY owned-MR set does
// not open the gate either.
func TestLegalFor_AwaitingReviewExitsAreGatedOnPendingReviewNil(t *testing.T) {
	tk := taskOfKind(v1alpha1.StateAwaitingReview, "implement")
	pending := []v1alpha1.MergeRequest{{Status: v1alpha1.MergeRequestStatus{
		PendingReview: &v1alpha1.PendingReview{Body: "b", SHA: "abc"}}}}
	for _, to := range []string{v1alpha1.StateUnderImplementation, v1alpha1.StateMerged} {
		require.False(t, stage.LegalFor(tk, pending, v1alpha1.StateAwaitingReview, to))
		require.False(t, stage.LegalFor(tk, nil, v1alpha1.StateAwaitingReview, to),
			"an EMPTY owned-MR set does not open the gate")
		require.True(t, stage.LegalFor(tk, []v1alpha1.MergeRequest{{}}, v1alpha1.StateAwaitingReview, to))
	}
}

// GUARD 3. awaiting-review -> done is the kind=review external-merge finalize
// and nothing else may take it.
func TestLegalFor_AwaitingReviewToDoneIsReviewKindAllMergedOnly(t *testing.T) {
	merged := []v1alpha1.MergeRequest{{Status: v1alpha1.MergeRequestStatus{State: "merged"}}}
	open := []v1alpha1.MergeRequest{{Status: v1alpha1.MergeRequestStatus{State: "open"}}}

	require.True(t, stage.LegalFor(taskOfKind(v1alpha1.StateAwaitingReview, "review"), merged,
		v1alpha1.StateAwaitingReview, v1alpha1.StateDone))
	require.False(t, stage.LegalFor(taskOfKind(v1alpha1.StateAwaitingReview, "review"), open,
		v1alpha1.StateAwaitingReview, v1alpha1.StateDone))
	require.False(t, stage.LegalFor(taskOfKind(v1alpha1.StateAwaitingReview, "implement"), merged,
		v1alpha1.StateAwaitingReview, v1alpha1.StateDone))
	require.False(t, stage.LegalFor(nil, merged, v1alpha1.StateAwaitingReview, v1alpha1.StateDone))
}

// awaiting-review -> rejected is deliberately NOT gated: it is shared with the
// issue-closed stop, whose caller passes mrs == nil.
func TestLegalFor_AwaitingReviewToRejectedIsUngated(t *testing.T) {
	require.True(t, stage.LegalFor(taskOfKind(v1alpha1.StateAwaitingReview, "implement"), nil,
		v1alpha1.StateAwaitingReview, v1alpha1.StateRejected))
}

// A PSEUDO-TARGET IS NOT A STATE. Reap is a real To in the table (done and
// rejected both carry it), so Legal(done, Reap) is TRUE and Enter would have
// happily stamped status.state = "(reap)" - a value outside the enum, with no
// budget row, no agent kind and no exit. ParkTarget and Respawn are caught today
// only because they are absent from the table, which is luck rather than a rule.
func TestEnterRefusesEveryPseudoTarget(t *testing.T) {
	for _, from := range []string{v1alpha1.StateDone, v1alpha1.StateRejected} {
		for _, to := range []string{stage.Reap, stage.Respawn, stage.ParkTarget, stage.Create} {
			tk := task(from)
			err := stage.Enter(tk, nil, to, "", now)
			require.Error(t, err, "Enter(%s -> %s) must be refused: %s is not a state", from, to, to)
			require.Equal(t, from, tk.Status.State, "a refused Enter must not stamp anything")
		}
	}
}

// ---------------------------------------------------------------------------
// Enter: the stamps, the reason rules, the park guard.
// ---------------------------------------------------------------------------

func TestEveryTransitionClearsBothTimestampsAndPodRecreations(t *testing.T) {
	tk := task(v1alpha1.StateRefined)
	tk.Status.PodStartedAt = ptrTime(now.Add(-time.Hour))
	tk.Status.StateWorkStartedAt = ptrTime(now.Add(-time.Hour))
	tk.Status.Stats.PodRecreations = 2
	tk.Status.StageElapsedCarrySeconds = 999

	require.NoError(t, stage.Enter(tk, nil, v1alpha1.StateUnderImplementation, "", now))
	require.Nil(t, tk.Status.PodStartedAt, "a stale podStartedAt disarms clock 1 and back-dates the pod TTL")
	require.Nil(t, tk.Status.StateWorkStartedAt)
	require.Equal(t, 0, tk.Status.Stats.PodRecreations)
	require.Equal(t, 0, tk.Status.StageElapsedCarrySeconds, "a fresh entry never inherits a stale carry")
	require.Equal(t, now, tk.Status.StateEnteredAt.Time)
}

func TestEnterStampsTheAgentKindFromStateAndOriginKind(t *testing.T) {
	tk := taskOfKind(v1alpha1.StateRefined, "brainstorm")
	require.NoError(t, stage.Enter(tk, nil, v1alpha1.StateDone, "", now))
	require.Empty(t, tk.Status.AgentKind, "done runs no agent")

	tk2 := taskOfKind(v1alpha1.StateNew, "brainstorm")
	require.NoError(t, stage.Enter(tk2, nil, v1alpha1.StateRefined, "", now))
	require.Equal(t, stage.AgentBrainstorm, tk2.Status.AgentKind)
}

func TestEnterRequiresAReasonOnRejectedAndNotOnDone(t *testing.T) {
	tk := task(v1alpha1.StateRefined)
	require.ErrorAs(t, stage.Enter(tk, nil, v1alpha1.StateRejected, "", now), new(*stage.MissingReasonError))
	require.NoError(t, stage.Enter(tk, nil, v1alpha1.StateRejected, stage.ReasonDeclined, now))

	tk2 := taskOfKind(v1alpha1.StateRefined, "brainstorm")
	require.NoError(t, stage.Enter(tk2, nil, v1alpha1.StateDone, "", now),
		"done absorbed `delivered`, whose ordinary success path carries no reason")
}

func TestEnterRefusesAReasonOutsideTheClosedSet(t *testing.T) {
	tk := task(v1alpha1.StateRefined)
	require.ErrorAs(t, stage.Enter(tk, nil, v1alpha1.StateRejected, "pod-not-ready", now),
		new(*stage.UnknownReasonError))
}

func TestEnterRefusesAnIllegalEdge(t *testing.T) {
	tk := task(v1alpha1.StateNew)
	require.ErrorAs(t, stage.Enter(tk, nil, v1alpha1.StateMerged, "", now), new(*stage.IllegalTransitionError))
	require.Equal(t, v1alpha1.StateNew, tk.Status.State)
}

func TestEnterFromAnEmptyStateUsesTheCreateEdge(t *testing.T) {
	tk := &v1alpha1.Task{Spec: v1alpha1.TaskSpec{Kind: "implement"}}
	require.NoError(t, stage.Enter(tk, nil, v1alpha1.StateNew, "", now))
	require.Equal(t, v1alpha1.StateNew, tk.Status.State)

	// #604: NO mint may land on `refined` any more, whatever its kind. The gate
	// state is reachable through TRIAGE only, so the kind arriving there has
	// always been decided by triageTarget - which is what makes "did anyone
	// check this kind owns an Issue?" answerable in one place.
	tk2 := &v1alpha1.Task{Spec: v1alpha1.TaskSpec{Kind: "takeover"}}
	require.Error(t, stage.Enter(tk2, nil, v1alpha1.StateRefined, "", now),
		"a takeover minted into `refined` owns zero Issues, so the gate can never grant and it "+
			"strands there: the edge was deleted in #604")

	tk3 := &v1alpha1.Task{Spec: v1alpha1.TaskSpec{Kind: "documentation"}}
	require.NoError(t, stage.Enter(tk3, nil, v1alpha1.StateUnderImplementation, "", now),
		"the nightly documentation batch is minted straight into implementation work")

	tk3b := &v1alpha1.Task{Spec: v1alpha1.TaskSpec{Kind: "takeover"}}
	require.NoError(t, stage.Enter(tk3b, nil, v1alpha1.StateUnderImplementation, "", now),
		"a takeover is minted straight into the work, where its own re-take un-park already lands")

	tk4 := &v1alpha1.Task{Spec: v1alpha1.TaskSpec{Kind: "implement"}}
	require.Error(t, stage.Enter(tk4, nil, v1alpha1.StateMerged, "", now),
		"a mint may only land on new, under-implementation or a terminal")
}

// THE #521 TERMINAL-RESET EDGES. Pruning removed status.stage on the READ path,
// so every pre-#521 Task boots STATELESS and re-derives through the create edge.
// A Task that had already finished must land on its terminal instead of being
// re-triaged, and the only writer of status.state is stage.Enter - so the create
// edge has to reach done and rejected or the guard has no legal way to stamp it.
func TestCreateEdgeReachesBothTerminalsForTheResetGuard(t *testing.T) {
	done := &v1alpha1.Task{Spec: v1alpha1.TaskSpec{Kind: "implement"}}
	require.NoError(t, stage.Enter(done, nil, v1alpha1.StateDone, "", now))
	require.Equal(t, v1alpha1.StateDone, done.Status.State)

	rej := &v1alpha1.Task{Spec: v1alpha1.TaskSpec{Kind: "implement"}}
	require.NoError(t, stage.Enter(rej, nil, v1alpha1.StateRejected, stage.ReasonIssueClosed, now))
	require.Equal(t, v1alpha1.StateRejected, rej.Status.State)
	require.Equal(t, stage.ReasonIssueClosed, rej.Status.StateReason)

	// rejected still MUST name why, on this edge like every other.
	bare := &v1alpha1.Task{Spec: v1alpha1.TaskSpec{Kind: "implement"}}
	require.ErrorAs(t, stage.Enter(bare, nil, v1alpha1.StateRejected, "", now),
		new(*stage.MissingReasonError))
}

// Enter validates `reason` against the vocabulary of the TARGET STATE, not
// against the union of all three, wherever the target HAS a vocabulary.
//
// The union is 36 reasons wide and 28 of them are PARK reasons, which live in a
// different field on purpose - `status.stateReason` and `status.parkReason` are
// disjoint vocabularies and the CRD says so. Validating against the union let
// Enter stamp `rejected` with awaiting-human, or `done` with declined: a
// stateReason outside the closed set the field documents, written by the one
// function that is supposed to be the guard. It was unreachable only because
// every call site happened to pass a compile-time constant, and "no caller does
// it today" is not an invariant.
func TestEnterRefusesAReasonOutsideTheTargetStatesVocabulary(t *testing.T) {
	for _, tc := range []struct {
		name   string
		to     string
		reason string
	}{
		{"a park reason on rejected", v1alpha1.StateRejected, stage.ReasonAwaitingHuman},
		{"a done reason on rejected", v1alpha1.StateRejected, stage.ReasonDocTimeout},
		{"a park reason on done", v1alpha1.StateDone, stage.ReasonBacklogSweep},
		{"a reject reason on done", v1alpha1.StateDone, stage.ReasonDeclined},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tk := &v1alpha1.Task{Spec: v1alpha1.TaskSpec{Kind: "implement"}}
			require.True(t, stage.ValidReason(tc.reason),
				"the fixture must be a member of the GLOBAL set, or this proves nothing")
			require.ErrorAs(t, stage.Enter(tk, nil, tc.to, tc.reason, now),
				new(*stage.UnknownReasonError))
			require.Empty(t, tk.Status.State, "a refused Enter stamps nothing")
			require.Empty(t, tk.Status.StateReason)
		})
	}
}

// The other half, so the refusals above cannot pass vacuously: each terminal's
// OWN vocabulary still goes through, and the six states with no closed set of
// their own keep the global check - stage.CIRed really does enter
// under-implementation with ci-red, so tightening those to "no reason at all"
// would be tightening past the machine.
func TestEnterAcceptsTheTargetStatesOwnVocabulary(t *testing.T) {
	rej := &v1alpha1.Task{Spec: v1alpha1.TaskSpec{Kind: "implement"}}
	require.NoError(t, stage.Enter(rej, nil, v1alpha1.StateRejected, stage.ReasonDeclined, now))
	require.Equal(t, stage.ReasonDeclined, rej.Status.StateReason)

	done := &v1alpha1.Task{Spec: v1alpha1.TaskSpec{Kind: "implement"}}
	require.NoError(t, stage.Enter(done, nil, v1alpha1.StateDone, stage.ReasonDocTimeout, now))
	require.Equal(t, stage.ReasonDocTimeout, done.Status.StateReason)

	red := &v1alpha1.Task{Spec: v1alpha1.TaskSpec{Kind: "implement"},
		Status: v1alpha1.TaskStatus{State: v1alpha1.StateMerged}}
	require.NoError(t, stage.Enter(red, nil, v1alpha1.StateUnderImplementation, stage.ReasonCIRed, now))
	require.Equal(t, stage.ReasonCIRed, red.Status.StateReason)

	live := &v1alpha1.Task{Spec: v1alpha1.TaskSpec{Kind: "implement"}}
	require.NoError(t, stage.Enter(live, nil, v1alpha1.StateNew, "", now))
	require.Empty(t, live.Status.StateReason)
}

// pod-not-ready IS NOT A MEMBER (fix V7-7): it was never a terminal state, it
// was a respawn trigger wearing a terminal's name.
func TestPodNotReadyIsNotAReason(t *testing.T) {
	require.False(t, stage.ValidReason("pod-not-ready"))
	require.False(t, stage.IsParkReason("pod-not-ready"))
}

func TestReasonsIsTheClosedSetAndTheThreeVocabulariesAreDisjoint(t *testing.T) {
	require.Len(t, stage.Reasons, 37)
	for _, r := range stage.RejectReasons {
		require.False(t, stage.IsParkReason(r), "%q cannot be both a reject and a park reason", r)
		require.False(t, stage.IsDoneReason(r))
	}
	for _, r := range stage.DoneReasons {
		require.False(t, stage.IsParkReason(r))
		require.False(t, stage.IsRejectReason(r))
	}
	// Every reason is Prometheus-label-safe: they are all metric label values.
	for _, r := range stage.Reasons {
		require.Regexp(t, `^[a-z][a-z-]*[a-z]$`, r)
	}
}

func TestMRExternalTerminalReasonsAreInTheClosedSet(t *testing.T) {
	require.True(t, stage.IsDoneReason(stage.ReasonMRMergedExternally))
	require.True(t, stage.IsRejectReason(stage.ReasonMRClosedExternally))
	require.True(t, stage.IsRejectReason(stage.ReasonMRTakenOver))
	require.True(t, stage.IsRejectReason(stage.ReasonIssueClosed))
	require.True(t, stage.IsParkReason(stage.ReasonMergeAuthRefused))
	require.True(t, stage.IsParkReason(stage.ReasonHandoffStalled))
}

// ---------------------------------------------------------------------------
// The issue-closed stop.
// ---------------------------------------------------------------------------

func TestAllowsIssueClosedStopCoversEveryLiveAndMergedStateAndNotDeployed(t *testing.T) {
	for _, s := range []string{
		v1alpha1.StateNew, v1alpha1.StateRefined, v1alpha1.StateUnderImplementation,
		v1alpha1.StateAwaitingReview, v1alpha1.StateMerged,
	} {
		require.True(t, stage.AllowsIssueClosedStop(s), "%s must stop on a closed issue", s)
		require.True(t, stage.Legal(s, v1alpha1.StateRejected), "%s needs the rejected edge to stop with", s)
	}
	require.False(t, stage.AllowsIssueClosedStop(v1alpha1.StateDeployed),
		"merged, deploying work is finished and a late issue close must not rewind it")
	require.False(t, stage.AllowsIssueClosedStop(v1alpha1.StateDone))
	require.False(t, stage.AllowsIssueClosedStop(v1alpha1.StateRejected))
}

// ---------------------------------------------------------------------------
// The three clocks.
// ---------------------------------------------------------------------------

func TestArmedClock_TheThreeClocksAreSelectedByStampsNotByState(t *testing.T) {
	// CLOCK 1 ADMISSION: no pod yet.
	tk := task(v1alpha1.StateUnderImplementation)
	clock, since, budget, edge := stage.ArmedClock(tk, false)
	require.Equal(t, stage.ClockAdmission, clock)
	require.Equal(t, tk.Status.StateEnteredAt.Time, since)
	require.Equal(t, v1alpha1.AdmissionStarvedBudget, budget)
	require.Equal(t, stage.ReasonAdmissionStarved, edge.Reason)

	// CLOCK 2 READINESS: pod exists, never became Ready.
	tk.Status.PodStartedAt = ptrTime(now.Add(-time.Minute))
	clock, since, budget, edge = stage.ArmedClock(tk, false)
	require.Equal(t, stage.ClockReadiness, clock)
	require.Equal(t, tk.Status.PodStartedAt.Time, since,
		"the READINESS clock never measures from stateEnteredAt: that includes the admission queue")
	require.Equal(t, v1alpha1.PodReadyTimeout, budget)
	require.Equal(t, stage.Respawn, edge.To)

	// CLOCK 3, in a live state, is the IDLE clock, measured from the LATEST of
	// the human's last word, this pod's readiness and the agent's last turn end.
	tk.Status.StateWorkStartedAt = ptrTime(now.Add(-10 * time.Minute))
	tk.Status.ConversationLastEventAt = ptrTime(now.Add(-5 * time.Minute))
	tk.Annotations = map[string]string{
		v1alpha1.AnnCurrentTurn:  "turn-1",
		v1alpha1.AnnTurnComplete: now.Add(-6 * time.Minute).UTC().Format(time.RFC3339),
	}
	clock, since, _, _ = stage.ArmedClock(tk, false)
	require.Equal(t, stage.ClockWork, clock)
	require.Equal(t, tk.Status.ConversationLastEventAt.Time, since,
		"the human spoke last, so the silence started with them")
}

func TestArmedClock_PauseDisarmsOnlyTheAdmissionClock(t *testing.T) {
	tk := task(v1alpha1.StateUnderImplementation)
	clock, _, _, _ := stage.ArmedClock(tk, true)
	require.Equal(t, stage.ClockNone, clock, "a pause must not be a backlog shredder")

	tk.Status.PodStartedAt = ptrTime(now)
	clock, _, _, _ = stage.ArmedClock(tk, true)
	require.Equal(t, stage.ClockReadiness, clock, "clocks 2 and 3 measure a pod that already exists")
}

// merged is OPERATOR-DRIVEN: it runs clock 3 ONLY, from stateEnteredAt, against
// its own 4h budget. It gets NO 24h admission clock (fix V7-8) - if it did, it
// could never reach merge-timeout and the bounded merge cycle would never engage.
func TestArmedClock_MergedStallsIntoMergeTimeoutNotAdmissionStarved(t *testing.T) {
	tk := task(v1alpha1.StateMerged)
	tk.Status.StateEnteredAt = ptrTime(now.Add(-5 * time.Hour))
	edge, fired := stage.Elapsed(tk, false, now)
	require.True(t, fired)
	require.Equal(t, stage.ReasonMergeTimeout, edge.Reason)
	require.Equal(t, stage.ParkTarget, edge.To)
}

func TestArmedClock_NoStateEnteredAtArmsNothing(t *testing.T) {
	tk := task(v1alpha1.StateMerged)
	tk.Status.StateEnteredAt = nil
	clock, _, _, _ := stage.ArmedClock(tk, false)
	require.Equal(t, stage.ClockNone, clock)
}

func TestArmedClock_AnUnknownStateArmsNothing(t *testing.T) {
	tk := task("not-a-state")
	clock, _, _, _ := stage.ArmedClock(tk, false)
	require.Equal(t, stage.ClockNone, clock)
}

// A Task that has been QUEUED behind three other agents for 20 hours is not
// stalled work: it is covered by clock 1 and does not terminate until 24h.
func TestSteadyState_QueuedBehindThreeAgentsDoesNotTerminate(t *testing.T) {
	tk := task(v1alpha1.StateUnderImplementation)
	tk.Status.StateEnteredAt = ptrTime(now.Add(-20 * time.Hour))
	_, fired := stage.Elapsed(tk, false, now)
	require.False(t, fired)

	tk.Status.StateEnteredAt = ptrTime(now.Add(-25 * time.Hour))
	edge, fired := stage.Elapsed(tk, false, now)
	require.True(t, fired)
	require.Equal(t, stage.ReasonAdmissionStarved, edge.Reason)
}

// O3: a never-Ready pod respawns FOREVER. There is no recreation ceiling left,
// so RecordRespawn has no terminal - but it MUST keep counting, because
// stats.podRecreations is the churn alert's only input.
func TestSteadyState_NeverReadyPodRespawnsAndNeverExhausts(t *testing.T) {
	tk := task(v1alpha1.StateUnderImplementation)
	tk.Status.PodStartedAt = ptrTime(now.Add(-10 * time.Minute))
	edge, fired := stage.Elapsed(tk, false, now)
	require.True(t, fired)
	require.Equal(t, stage.Respawn, edge.To, "a never-Ready pod RESPAWNS; it does not terminate the Task")

	for i := 1; i <= 50; i++ {
		e := stage.RecordRespawn(tk)
		require.Equal(t, stage.Respawn, e.To, "lap %d must still respawn: O3 deleted maxPodRecreations", i)
		require.Equal(t, i, tk.Status.Stats.PodRecreations,
			"the count feeds operator_pod_recreations_total and must not stop with the cap")
	}
}

// The WORK clock measures WORK, not queue wait: a Task that burned its whole
// budget QUEUEING must not die having never run a pod.
func TestWorkClockMeasuresWorkNotQueueWait(t *testing.T) {
	tk := task(v1alpha1.StateUnderImplementation)
	tk.Status.StateEnteredAt = ptrTime(now.Add(-3 * time.Hour))
	tk.Status.PodStartedAt = ptrTime(now.Add(-2 * time.Minute))
	tk.Status.StateWorkStartedAt = ptrTime(now.Add(-1 * time.Minute))
	tk.Status.ConversationLastEventAt = ptrTime(now.Add(-1 * time.Minute))
	_, fired := stage.Elapsed(tk, false, now)
	require.False(t, fired, "3h of queue wait must not spend the work budget")
}

// ---------------------------------------------------------------------------
// The park clock and the carry.
// ---------------------------------------------------------------------------

func TestMergeTimeoutClockSurvivesUnpark(t *testing.T) {
	tk := task(v1alpha1.StateMerged)
	tk.Status.StateEnteredAt = ptrTime(now.Add(-4 * time.Hour))
	require.NoError(t, stage.Park(tk, stage.ReasonMergeTimeout, now))
	require.Equal(t, int((4 * time.Hour).Seconds()), tk.Status.StageElapsedCarrySeconds)

	// Parked: the retention clock, not the merge budget.
	clock, _, budget, edge := stage.ArmedClock(tk, false)
	require.Equal(t, stage.ClockWork, clock)
	require.Equal(t, v1alpha1.ParkRetention, budget, "a carry must not shorten ParkRetention")
	require.Equal(t, stage.Reap, edge.To)

	// Un-parked: a FRESH bounded window, no carry subtraction.
	require.Equal(t, stage.DeclineNone, stage.Unpark(stage.UnparkInput{Task: tk, Now: now}))
	require.Equal(t, 1, tk.Status.MergeReentries)
	require.Equal(t, int((4 * time.Hour).Seconds()), tk.Status.StageElapsedCarrySeconds,
		"the carry survives so reported residency stays continuous")
	_, since, budget, _ := stage.ArmedClock(tk, false)
	require.Equal(t, now, since)
	require.Equal(t, v1alpha1.TimeoutReentryBudget, budget)
	_, fired := stage.Elapsed(tk, false, now.Add(time.Minute))
	require.False(t, fired, "issue #513: a re-entry that arrives over budget buys zero merge time")
}

func TestDeployTimeoutClockSurvivesUnpark(t *testing.T) {
	tk := task(v1alpha1.StateDeployed)
	tk.Status.StateEnteredAt = ptrTime(now.Add(-2 * time.Hour))
	require.NoError(t, stage.Park(tk, stage.ReasonDeployTimeout, now))
	require.Equal(t, stage.DeclineNone, stage.Unpark(stage.UnparkInput{Task: tk, Now: now}))
	require.Equal(t, 1, tk.Status.DeployReentries)
	_, _, budget, _ := stage.ArmedClock(tk, false)
	require.Equal(t, v1alpha1.TimeoutReentryBudget, budget)
}

func TestMergeTimeoutCarryAccumulatesAcrossMultipleLaps(t *testing.T) {
	tk := task(v1alpha1.StateMerged)
	tk.Status.StateEnteredAt = ptrTime(now.Add(-4 * time.Hour))
	at := now
	require.NoError(t, stage.Park(tk, stage.ReasonMergeTimeout, at))
	require.Equal(t, stage.DeclineNone, stage.Unpark(stage.UnparkInput{Task: tk, Now: at}))

	at = at.Add(30 * time.Minute)
	require.NoError(t, stage.Park(tk, stage.ReasonMergeTimeout, at))
	require.Equal(t, int((4*time.Hour + 30*time.Minute).Seconds()), tk.Status.StageElapsedCarrySeconds)
	require.InDelta(t, (4*time.Hour + 30*time.Minute).Seconds(),
		stage.StateElapsedSeconds(tk, at), 1,
		"reported residency is CUMULATIVE across the whole round trip")
}

func TestCarryDoesNotLeakIntoAFreshEntry(t *testing.T) {
	tk := task(v1alpha1.StateMerged)
	tk.Status.StateEnteredAt = ptrTime(now.Add(-4 * time.Hour))
	require.NoError(t, stage.Park(tk, stage.ReasonMergeTimeout, now))
	require.Equal(t, stage.DeclineNone, stage.Unpark(stage.UnparkInput{Task: tk, Now: now}))

	// A HEAD-MOVED exit leaves merged via awaiting-review; the carry is laundered.
	require.NoError(t, stage.Enter(tk, []v1alpha1.MergeRequest{{}}, v1alpha1.StateAwaitingReview, "", now))
	require.Equal(t, 0, tk.Status.StageElapsedCarrySeconds)
	require.NoError(t, stage.Enter(tk, []v1alpha1.MergeRequest{{}}, v1alpha1.StateMerged, "", now))
	_, _, budget, _ := stage.ArmedClock(tk, false)
	require.Equal(t, 4*time.Hour, budget, "a genuinely fresh merged entry gets its FULL base budget")
}

// ---------------------------------------------------------------------------
// The five bounded cycles.
// ---------------------------------------------------------------------------

func TestCycleCap_MergeReentriesBoundedThenReparkedBlocked(t *testing.T) {
	tk := task(v1alpha1.StateMerged)
	for lap := 1; lap <= v1alpha1.MaxMergeReentries; lap++ {
		require.NoError(t, stage.Park(tk, stage.ReasonMergeTimeout, now))
		require.Equal(t, stage.DeclineNone, stage.Unpark(stage.UnparkInput{Task: tk, Now: now}), "lap %d", lap)
		require.Equal(t, lap, tk.Status.MergeReentries)
	}
	require.NoError(t, stage.Park(tk, stage.ReasonMergeTimeout, now))
	require.Equal(t, stage.DeclineRetryBudgetSpent, stage.Unpark(stage.UnparkInput{Task: tk, Now: now}))
	require.Equal(t, stage.ReasonMergeBlocked, tk.Status.ParkReason,
		"the bound is spent: re-park with an UnparkNever reason so it ages out instead of retrying forever")
	require.Equal(t, v1alpha1.StateMerged, tk.Status.State, "a re-park does not move the Task")
}

func TestCycleCap_DeployReentriesBoundedThenReparkedBlocked(t *testing.T) {
	tk := task(v1alpha1.StateDeployed)
	for lap := 1; lap <= v1alpha1.MaxDeployReentries; lap++ {
		require.NoError(t, stage.Park(tk, stage.ReasonDeployTimeout, now))
		require.Equal(t, stage.DeclineNone, stage.Unpark(stage.UnparkInput{Task: tk, Now: now}))
		require.Equal(t, lap, tk.Status.DeployReentries)
	}
	require.NoError(t, stage.Park(tk, stage.ReasonDeployTimeout, now))
	require.Equal(t, stage.DeclineRetryBudgetSpent, stage.Unpark(stage.UnparkInput{Task: tk, Now: now}))
	require.Equal(t, stage.ReasonDeployBlocked, tk.Status.ParkReason)
}

// CYCLE 4, the one that SPAWNS A POD every lap (fix M3-9).
func TestCycleCap_HeadMoveReentriesBounded(t *testing.T) {
	tk := task(v1alpha1.StateMerged)
	for lap := 1; lap <= v1alpha1.MaxHeadMoveReentries; lap++ {
		e, ok := stage.HeadMoved(tk, v1alpha1.MaxHeadMoveReentries)
		require.True(t, ok)
		require.Equal(t, v1alpha1.StateAwaitingReview, e.To)
		require.Equal(t, lap, tk.Status.HeadMoveReentries)
	}
	e, ok := stage.HeadMoved(tk, v1alpha1.MaxHeadMoveReentries)
	require.True(t, ok)
	require.Equal(t, stage.ParkTarget, e.To)
	require.Equal(t, stage.ReasonHeadMoving, e.Reason)
}

// CYCLE 5, the red-CI bounce (issue #476).
func TestCIRedRouting(t *testing.T) {
	t.Run("review kind parks awaiting-human", func(t *testing.T) {
		e, ok := stage.CIRed(taskOfKind(v1alpha1.StateAwaitingReview, "review"), nil, 3)
		require.True(t, ok)
		require.Equal(t, stage.ParkTarget, e.To)
		require.Equal(t, stage.ReasonAwaitingHuman, e.Reason)
	})
	t.Run("an already-merged sibling parks ci-red", func(t *testing.T) {
		mrs := []v1alpha1.MergeRequest{{Status: v1alpha1.MergeRequestStatus{State: "merged"}}}
		e, ok := stage.CIRed(task(v1alpha1.StateMerged), mrs, 3)
		require.True(t, ok)
		require.Equal(t, stage.ParkTarget, e.To)
		require.Equal(t, stage.ReasonCIRed, e.Reason,
			"re-implementing would re-propose merged code and recreate deleted branches")
	})
	t.Run("bounces to under-implementation, bounded", func(t *testing.T) {
		tk := task(v1alpha1.StateAwaitingReview)
		for lap := 1; lap <= v1alpha1.MaxCIRedReentries; lap++ {
			e, ok := stage.CIRed(tk, nil, v1alpha1.MaxCIRedReentries)
			require.True(t, ok)
			require.Equal(t, v1alpha1.StateUnderImplementation, e.To)
			require.Equal(t, stage.ReasonCIRed, e.Reason)
			require.Equal(t, lap, tk.Status.CIRedReentries)
		}
		e, ok := stage.CIRed(tk, nil, v1alpha1.MaxCIRedReentries)
		require.True(t, ok)
		require.Equal(t, stage.ParkTarget, e.To)
		require.Equal(t, stage.ReasonCIBlocked, e.Reason)
	})
}

func TestCIRedEdgeIsLegalFromBothSides(t *testing.T) {
	mrs := []v1alpha1.MergeRequest{{}}
	tk := task(v1alpha1.StateAwaitingReview)
	require.True(t, stage.LegalFor(tk, mrs, v1alpha1.StateAwaitingReview, v1alpha1.StateUnderImplementation))
	require.True(t, stage.LegalFor(tk, mrs, v1alpha1.StateMerged, v1alpha1.StateUnderImplementation))
}

// CYCLE 1, review rounds. O3 deleted the round cap: a review round-trip counts
// conversation, not progress, and review-loop-exhausted killed converging pairs.
func TestRequestChangesRouting(t *testing.T) {
	t.Run("review kind parks awaiting-human unconditionally", func(t *testing.T) {
		e, ok := stage.RequestChanges(taskOfKind(v1alpha1.StateAwaitingReview, "review"))
		require.True(t, ok)
		require.Equal(t, stage.ParkTarget, e.To)
		require.Equal(t, stage.ReasonAwaitingHuman, e.Reason)
	})
	t.Run("non-review kind re-enters under-implementation", func(t *testing.T) {
		e, ok := stage.RequestChanges(task(v1alpha1.StateAwaitingReview))
		require.True(t, ok)
		require.Equal(t, v1alpha1.StateUnderImplementation, e.To)
	})
	t.Run("a hundred rounds still re-enters: the cap is gone", func(t *testing.T) {
		tk := task(v1alpha1.StateAwaitingReview)
		tk.Status.Stats.Turns = 400
		e, ok := stage.RequestChanges(tk)
		require.True(t, ok)
		require.Equal(t, v1alpha1.StateUnderImplementation, e.To,
			"review-loop-exhausted is no longer reachable from RequestChanges")
	})
}

func TestBudgetExit(t *testing.T) {
	t.Run("no exit for an operator-driven state", func(t *testing.T) {
		tk := task(v1alpha1.StateMerged)
		tk.Status.Stats.Turns = 9999
		_, ok := stage.BudgetExit(tk, false)
		require.False(t, ok, "merged runs no pod, so the pod budgets do not apply")
	})
	t.Run("400 turns does not park: the turn ceiling is deleted", func(t *testing.T) {
		tk := task(v1alpha1.StateUnderImplementation)
		tk.Status.Stats.Turns = 400
		_, ok := stage.BudgetExit(tk, false)
		require.False(t, ok, "a 400-turn Task is a long job, not a stalled one")
	})
	t.Run("10 pod recreations does not park: the recreation ceiling is deleted", func(t *testing.T) {
		tk := task(v1alpha1.StateUnderImplementation)
		tk.Status.Stats.PodRecreations = 10
		_, ok := stage.BudgetExit(tk, false)
		require.False(t, ok, "the churn alert replaced maxPodRecreations, not another cap")
	})
	t.Run("no outcome parks with NO recreation gate", func(t *testing.T) {
		tk := task(v1alpha1.StateUnderImplementation)
		tk.Status.Stats.PodRecreations = 0
		e, ok := stage.BudgetExit(tk, true)
		require.True(t, ok, "podStoppedNoOutcome is a fact about now, not a budget, and keeps firing")
		require.Equal(t, stage.ReasonNoOutcome, e.Reason)
	})
}

// ---------------------------------------------------------------------------
// Un-park.
// ---------------------------------------------------------------------------

func withHumanEvent(tk *v1alpha1.Task) *v1alpha1.Task {
	tk.Status.PendingEvents = append(tk.Status.PendingEvents,
		v1alpha1.TaskEvent{At: metav1.NewTime(now), Author: "szymonrychu", Body: "please continue"})
	return tk
}

func openIssue() []v1alpha1.Issue {
	return []v1alpha1.Issue{{Status: v1alpha1.IssueStatus{State: "open"}}}
}

func TestUnpark_BacklogSweepPromotesOnNonBotEventUnderCap(t *testing.T) {
	tk := withHumanEvent(task(v1alpha1.StateNew))
	require.NoError(t, stage.Park(tk, stage.ReasonBacklogSweep, now))
	require.Equal(t, stage.DeclineNone, stage.Unpark(stage.UnparkInput{
		Task: tk, BotLogin: "tatara-bot", ActiveTasks: 1, MaxOpenTasks: 6, Now: now}))
	require.Empty(t, tk.Status.ParkReason)
	require.Equal(t, v1alpha1.StateNew, tk.Status.State)
}

func TestUnpark_BacklogSweepIgnoresBotEvent(t *testing.T) {
	tk := task(v1alpha1.StateNew)
	tk.Status.PendingEvents = []v1alpha1.TaskEvent{{Author: "tatara-bot"}}
	require.NoError(t, stage.Park(tk, stage.ReasonBacklogSweep, now))
	require.Equal(t, stage.DeclineNoHumanEvent, stage.Unpark(stage.UnparkInput{
		Task: tk, BotLogin: "tatara-bot", ActiveTasks: 1, MaxOpenTasks: 6, Now: now}),
		"the operator's own park comment must never un-park the Task it parked")
	require.Equal(t, stage.ReasonBacklogSweep, tk.Status.ParkReason)
}

func TestUnpark_BacklogSweepDefersOverCapAndRetainsTheEvent(t *testing.T) {
	tk := withHumanEvent(task(v1alpha1.StateNew))
	require.NoError(t, stage.Park(tk, stage.ReasonBacklogSweep, now))
	require.Equal(t, stage.DeclineOverCap, stage.Unpark(stage.UnparkInput{
		Task: tk, BotLogin: "bot", ActiveTasks: 6, MaxOpenTasks: 6, Now: now}))
	require.Equal(t, stage.ReasonBacklogSweep, tk.Status.ParkReason)
	require.Len(t, tk.Status.PendingEvents, 1, "the event is RETAINED, never dropped")
}

func TestUnpark_AwaitingHumanReturnsTheTaskToWhereItWas(t *testing.T) {
	for _, state := range []string{v1alpha1.StateNew, v1alpha1.StateRefined, v1alpha1.StateAwaitingReview} {
		tk := withHumanEvent(task(state))
		require.NoError(t, stage.Park(tk, stage.ReasonAwaitingHuman, now))
		require.Equal(t, stage.DeclineNone, stage.Unpark(stage.UnparkInput{
			Task: tk, BotLogin: "bot", Issues: openIssue(), LiveHasRoom: true, Now: now}))
		require.Equal(t, state, tk.Status.State,
			"un-park NEVER moves state; that is the whole point of park being a flag")
		require.Empty(t, tk.Status.ParkReason)
	}
}

// THE EMPTY SET IS NOT A LICENCE (fix V6-3).
func TestUnpark_AwaitingHumanWithNoOpenIssuesStaysParked(t *testing.T) {
	tk := withHumanEvent(task(v1alpha1.StateRefined))
	require.NoError(t, stage.Park(tk, stage.ReasonAwaitingHuman, now))
	require.Equal(t, stage.DeclineNoOpenIssues, stage.Unpark(stage.UnparkInput{
		Task: tk, BotLogin: "bot", LiveHasRoom: true, Now: now}))
}

// ... BUT AN OWNED MERGE REQUEST IS ONE. A cron-minted upgrade Task and a
// nightly documentation batch own ZERO Issue CRs by construction (nobody files
// an issue for a dependency sweep), so the issue-only refusal above made
// submit_outcome(action=discuss) - the resumable exit those two kinds were
// given - a PERMANENT wedge: awaiting-human is UnparkHuman, but every human
// comment re-declined no-open-issues until the Task aged out at ParkRetention.
// The deliverable a human is commenting on is the merge request.
func TestUnpark_AwaitingHumanResumesAnMRBackedTaskThatOwnsNoIssues(t *testing.T) {
	for _, kind := range []string{"upgrade", "documentation"} {
		tk := withHumanEvent(taskOfKind(v1alpha1.StateUnderImplementation, kind))
		require.NoError(t, stage.Park(tk, stage.ReasonAwaitingHuman, now))
		mrs := []v1alpha1.MergeRequest{{Status: v1alpha1.MergeRequestStatus{State: "open"}}}
		require.Equal(t, stage.DeclineNone, stage.Unpark(stage.UnparkInput{
			Task: tk, BotLogin: "bot", MRs: mrs, LiveHasRoom: true, Now: now}), "kind %s", kind)
		require.Empty(t, tk.Status.ParkReason, "kind %s", kind)
		require.Equal(t, v1alpha1.StateUnderImplementation, tk.Status.State,
			"un-park NEVER moves state")
	}
}

// AND IT IS NOT A RESURRECTION PATH. Neither an open Issue nor an open merge
// request left means the work is genuinely finished, and it still declines.
func TestUnpark_AwaitingHumanDeclinesWhenNeitherIssueNorMRIsOpen(t *testing.T) {
	for _, state := range []string{"merged", "closed"} {
		tk := withHumanEvent(taskOfKind(v1alpha1.StateUnderImplementation, "upgrade"))
		require.NoError(t, stage.Park(tk, stage.ReasonAwaitingHuman, now))
		mrs := []v1alpha1.MergeRequest{{Status: v1alpha1.MergeRequestStatus{State: state}}}
		require.Equal(t, stage.DeclineNoOpenIssues, stage.Unpark(stage.UnparkInput{
			Task: tk, BotLogin: "bot", MRs: mrs, LiveHasRoom: true, Now: now}), "mr state %s", state)
		require.Equal(t, stage.ReasonAwaitingHuman, tk.Status.ParkReason, "mr state %s", state)
	}
}

// ONE FRESH COMMENT PER LAP, which is what the cap was always about: each lap
// can spawn a review pod and a chatty MR thread must not spawn one per comment.
// Before H1-J the loop below re-used a single event five times, because a
// consumed event stayed eligible forever - the hot loop itself, written down as
// a fixture.
func TestUnpark_AwaitingHumanReviewKindBoundedByHumanReviewRounds(t *testing.T) {
	tk := taskOfKind(v1alpha1.StateAwaitingReview, "review")
	require.NoError(t, stage.Park(tk, stage.ReasonAwaitingHuman, now))
	for lap := 1; lap <= v1alpha1.MaxHumanReviewRounds; lap++ {
		withHumanEvent(tk)
		require.Equal(t, stage.DeclineNone, stage.Unpark(stage.UnparkInput{
			Task: tk, BotLogin: "bot", LiveHasRoom: true, Now: now}), "lap %d", lap)
		require.Equal(t, lap, tk.Status.HumanReviewRounds)
		require.NoError(t, stage.Park(tk, stage.ReasonAwaitingHuman, now))
	}
	withHumanEvent(tk)
	require.Equal(t, stage.DeclineRoundsExhausted, stage.Unpark(stage.UnparkInput{
		Task: tk, BotLogin: "bot", LiveHasRoom: true, Now: now}),
		"STAY PARKED. Do not spawn another review pod")
}

func TestUnpark_AwaitingHumanReviewKindBypassesRoundsCapOnExternallyOwnedMR(t *testing.T) {
	tk := withHumanEvent(taskOfKind(v1alpha1.StateAwaitingReview, "review"))
	tk.Status.HumanReviewRounds = v1alpha1.MaxHumanReviewRounds
	require.NoError(t, stage.Park(tk, stage.ReasonAwaitingHuman, now))
	mrs := []v1alpha1.MergeRequest{{Status: v1alpha1.MergeRequestStatus{
		State: "open", Ownership: v1alpha1.OwnershipExternal}}}
	require.Equal(t, stage.DeclineNone, stage.Unpark(stage.UnparkInput{
		Task: tk, BotLogin: "bot", MRs: mrs, LiveHasRoom: true, Now: now}),
		"#511: a take-over request is not the review ping-pong the cap bounds")
	require.Equal(t, v1alpha1.MaxHumanReviewRounds, tk.Status.HumanReviewRounds,
		"and it does NOT spend a round")
}

func TestUnpark_AwaitingHumanReviewKindRefusesReentryOnMergedMR(t *testing.T) {
	tk := withHumanEvent(taskOfKind(v1alpha1.StateAwaitingReview, "review"))
	require.NoError(t, stage.Park(tk, stage.ReasonAwaitingHuman, now))
	mrs := []v1alpha1.MergeRequest{{Status: v1alpha1.MergeRequestStatus{State: "merged"}}}
	require.Equal(t, stage.DeclineMergedMR, stage.Unpark(stage.UnparkInput{
		Task: tk, BotLogin: "bot", MRs: mrs, LiveHasRoom: true, Now: now}),
		"issue #393: a pod spawned against an already-merged MR has no legal outcome")
}

// THE LIVE-POD CEILING, generalised from the conversing ceiling. A full ceiling
// degrades responsiveness; it never drops the event.
func TestUnpark_DeclinesWhenTheLiveCeilingIsFull(t *testing.T) {
	for _, reason := range []string{stage.ReasonAwaitingHuman, stage.ReasonIdentityUnverified} {
		tk := withHumanEvent(task(v1alpha1.StateRefined))
		require.NoError(t, stage.Park(tk, reason, now))
		require.Equal(t, stage.DeclineNoLiveRoom, stage.Unpark(stage.UnparkInput{
			Task: tk, BotLogin: "bot", Issues: openIssue(), LiveHasRoom: false, Now: now}),
			"reason %s", reason)
		require.Equal(t, reason, tk.Status.ParkReason)
		require.Len(t, tk.Status.PendingEvents, 1)
	}
}

// The ceiling has nothing to say about a state that runs no pod.
func TestUnpark_TheLiveCeilingDoesNotGateAnOperatorDrivenState(t *testing.T) {
	tk := task(v1alpha1.StateMerged)
	require.NoError(t, stage.Park(tk, stage.ReasonMergeTimeout, now))
	require.Equal(t, stage.DeclineNone, stage.Unpark(stage.UnparkInput{
		Task: tk, LiveHasRoom: false, Now: now}))
}

func TestUnpark_IdentityUnverifiedNeverGrantsAnything(t *testing.T) {
	tk := withHumanEvent(task(v1alpha1.StateRefined))
	require.NoError(t, stage.Park(tk, stage.ReasonIdentityUnverified, now))
	require.Equal(t, stage.DeclineNone, stage.Unpark(stage.UnparkInput{
		Task: tk, BotLogin: "bot", LiveHasRoom: true, Now: now}))
	require.Equal(t, v1alpha1.StateRefined, tk.Status.State,
		"the authorization lives in restapi.verifyApprovalScope and nowhere else")
}

func TestUnpark_NoOutcome(t *testing.T) {
	t.Run("refuses a park from a pre-implement state", func(t *testing.T) {
		tk := task(v1alpha1.StateNew)
		require.NoError(t, stage.Park(tk, stage.ReasonNoOutcome, now))
		require.Equal(t, stage.DeclineWrongParkedFrom, stage.Unpark(stage.UnparkInput{
			Task: tk, LiveHasRoom: true, Now: now}),
			"#406: a pre-implement no-outcome park must not auto-escalate")
	})
	t.Run("refuses when an owned MR already merged", func(t *testing.T) {
		tk := task(v1alpha1.StateUnderImplementation)
		require.NoError(t, stage.Park(tk, stage.ReasonNoOutcome, now))
		mrs := []v1alpha1.MergeRequest{{Status: v1alpha1.MergeRequestStatus{State: "merged"}}}
		require.Equal(t, stage.DeclineMergedMR, stage.Unpark(stage.UnparkInput{
			Task: tk, MRs: mrs, LiveHasRoom: true, Now: now}))
	})
	// O3: THE TURN GATE IS GONE. It was the second clock a no-outcome park landed
	// on, so clearing the park only bought a refusal on a count nothing else
	// enforces any more.
	t.Run("re-arms at 400 turns: the lifetime turn cap no longer vetoes", func(t *testing.T) {
		tk := task(v1alpha1.StateUnderImplementation)
		tk.Status.Stats.Turns = 400
		require.NoError(t, stage.Park(tk, stage.ReasonNoOutcome, now))
		require.Equal(t, stage.DeclineNone, stage.Unpark(stage.UnparkInput{
			Task: tk, LiveHasRoom: true, Now: now}))
		require.Empty(t, tk.Status.ParkReason)
	})
	t.Run("re-arms in place from under-implementation", func(t *testing.T) {
		tk := task(v1alpha1.StateUnderImplementation)
		require.NoError(t, stage.Park(tk, stage.ReasonNoOutcome, now))
		require.Equal(t, stage.DeclineNone, stage.Unpark(stage.UnparkInput{
			Task: tk, LiveHasRoom: true, Now: now}))
		require.Equal(t, v1alpha1.StateUnderImplementation, tk.Status.State)
	})
}

func TestUnpark_HandoffStalled(t *testing.T) {
	tk := withHumanEvent(task(v1alpha1.StateAwaitingReview))
	require.NoError(t, stage.Park(tk, stage.ReasonHandoffStalled, now))
	require.Equal(t, stage.DeclineNone, stage.Unpark(stage.UnparkInput{
		Task: tk, BotLogin: "bot", LiveHasRoom: true, Now: now}))
	require.Equal(t, 1, tk.Status.HumanReviewRounds)
	require.Equal(t, v1alpha1.StateAwaitingReview, tk.Status.State)
}

func TestUnpark_EveryUnparkNeverReasonDeclinesNoReentry(t *testing.T) {
	for _, r := range stage.ParkReasons {
		class, ok := stage.UnparkClassFor(r)
		require.True(t, ok)
		if class != stage.UnparkNever {
			continue
		}
		tk := withHumanEvent(task(v1alpha1.StateUnderImplementation))
		require.NoError(t, stage.Park(tk, r, now))
		require.Equal(t, stage.DeclineNoReentry, stage.Unpark(stage.UnparkInput{
			Task: tk, BotLogin: "bot", Issues: openIssue(), LiveHasRoom: true,
			ActiveTasks: 0, MaxOpenTasks: 6, Now: now}),
			"reason %q must have no re-entry: re-entering an exhaustion terminal escapes its own cap one lap at a time", r)
		require.Equal(t, r, tk.Status.ParkReason)
	}
}

func TestUnpark_AnUnparkedTaskIsNamedNotSwallowed(t *testing.T) {
	require.Equal(t, stage.DeclineNotParked, stage.Unpark(stage.UnparkInput{
		Task: task(v1alpha1.StateRefined), Now: now}))
}

func TestUnparkDeclineVocabularyIsClosedAndLabelSafe(t *testing.T) {
	require.NotEmpty(t, stage.UnparkDeclines)
	for _, d := range stage.UnparkDeclines {
		require.Regexp(t, `^[a-z][a-z-]*[a-z]$`, d,
			"a decline is a metric label value and must be Prometheus-safe")
	}
	require.NotContains(t, stage.UnparkDeclines, "unresolved",
		"a decline that cannot name its condition is the defect this vocabulary exists to prevent")
}

// Every un-park RE-ARMS the clock. A Task un-parked with a stale stateEnteredAt
// re-parks on the same reconcile pass - live in #513.
func TestUnparkReArmsTheClock(t *testing.T) {
	tk := withHumanEvent(task(v1alpha1.StateRefined))
	tk.Status.StateEnteredAt = ptrTime(now.Add(-100 * time.Hour))
	tk.Status.PodStartedAt = ptrTime(now.Add(-100 * time.Hour))
	tk.Status.StateWorkStartedAt = ptrTime(now.Add(-100 * time.Hour))
	tk.Status.Stats.PodRecreations = 3
	require.NoError(t, stage.Park(tk, stage.ReasonAwaitingHuman, now))

	require.Equal(t, stage.DeclineNone, stage.Unpark(stage.UnparkInput{
		Task: tk, BotLogin: "bot", Issues: openIssue(), LiveHasRoom: true, Now: now}))
	require.Equal(t, now, tk.Status.StateEnteredAt.Time)
	require.Nil(t, tk.Status.PodStartedAt)
	require.Nil(t, tk.Status.StateWorkStartedAt)
	require.Equal(t, 0, tk.Status.Stats.PodRecreations)
	require.Equal(t, now, tk.Status.ConversationLastEventAt.Time, "a live state's idle clock is re-armed too")
}

// ---------------------------------------------------------------------------
// ReenterOnReviewChangesRequested.
// ---------------------------------------------------------------------------

func TestReenterOnReviewChangesRequested(t *testing.T) {
	mrs := []v1alpha1.MergeRequest{{}}
	for _, tc := range []struct {
		state string
		want  bool
	}{
		{v1alpha1.StateAwaitingReview, true},
		{v1alpha1.StateMerged, true},
		{v1alpha1.StateUnderImplementation, false},
		{v1alpha1.StateNew, false},
		{v1alpha1.StateRefined, false},
		{v1alpha1.StateDone, false},
		{v1alpha1.StateRejected, false},
		{v1alpha1.StateDeployed, false},
	} {
		tk := task(tc.state)
		require.Equal(t, tc.want, stage.ReenterOnReviewChangesRequested(tk, mrs, now), "from %s", tc.state)
		if tc.want {
			require.Equal(t, v1alpha1.StateUnderImplementation, tk.Status.State)
		}
	}
}

func TestReenterOnReviewChangesRequested_ResetsMergeAndHeadBudgets(t *testing.T) {
	tk := task(v1alpha1.StateMerged)
	tk.Status.MergeReentries, tk.Status.HeadMoveReentries, tk.Status.CIRedReentries = 2, 2, 2
	tk.Status.MergeConflictReentries = 2
	require.True(t, stage.ReenterOnReviewChangesRequested(tk, []v1alpha1.MergeRequest{{}}, now))
	require.Zero(t, tk.Status.MergeReentries)
	require.Zero(t, tk.Status.HeadMoveReentries)
	require.Zero(t, tk.Status.CIRedReentries)
	require.Zero(t, tk.Status.MergeConflictReentries)
}

func TestReenterOnReviewChangesRequested_MergeTimeoutAccounting(t *testing.T) {
	tk := task(v1alpha1.StateMerged)
	require.NoError(t, stage.Park(tk, stage.ReasonMergeTimeout, now))
	require.True(t, stage.ReenterOnReviewChangesRequested(tk, nil, now))
	require.Equal(t, 1, tk.Status.MergeReentries)
	require.Equal(t, v1alpha1.StateMerged, tk.Status.State, "NEVER under-implementation: that recreates deleted branches")
	require.Empty(t, tk.Status.ParkReason)

	tk.Status.MergeReentries = v1alpha1.MaxMergeReentries
	require.NoError(t, stage.Park(tk, stage.ReasonMergeTimeout, now))
	require.False(t, stage.ReenterOnReviewChangesRequested(tk, nil, now),
		"budget spent: fold rather than race driveUnparks' own terminal")
}

func TestReenterOnReviewChangesRequested_NoOutcomeGuards(t *testing.T) {
	merged := []v1alpha1.MergeRequest{{Status: v1alpha1.MergeRequestStatus{State: "merged"}}}
	tk := task(v1alpha1.StateUnderImplementation)
	require.NoError(t, stage.Park(tk, stage.ReasonNoOutcome, now))
	require.False(t, stage.ReenterOnReviewChangesRequested(tk, merged, now))

	// The turn gate here is gone with Unpark's (O3): both read the same retired
	// ceiling, and leaving one of them would just move the wedge.
	tk2 := task(v1alpha1.StateUnderImplementation)
	tk2.Status.Stats.Turns = 400
	require.NoError(t, stage.Park(tk2, stage.ReasonNoOutcome, now))
	require.True(t, stage.ReenterOnReviewChangesRequested(tk2, nil, now))

	tk3 := task(v1alpha1.StateUnderImplementation)
	require.NoError(t, stage.Park(tk3, stage.ReasonNoOutcome, now))
	require.True(t, stage.ReenterOnReviewChangesRequested(tk3, nil, now))
	require.Empty(t, tk3.Status.ParkReason)
}

func TestReenterOnReviewChangesRequested_EveryOtherParkFolds(t *testing.T) {
	for _, r := range stage.ParkReasons {
		if r == stage.ReasonMergeTimeout || r == stage.ReasonNoOutcome {
			continue
		}
		tk := task(v1alpha1.StateUnderImplementation)
		require.NoError(t, stage.Park(tk, r, now))
		require.False(t, stage.ReenterOnReviewChangesRequested(tk, nil, now), "reason %q", r)
	}
}

// ---------------------------------------------------------------------------
// MR helpers.
// ---------------------------------------------------------------------------

func TestMRStateHelpers(t *testing.T) {
	open := v1alpha1.MergeRequest{Status: v1alpha1.MergeRequestStatus{State: "open"}}
	blank := v1alpha1.MergeRequest{}
	merged := v1alpha1.MergeRequest{Status: v1alpha1.MergeRequestStatus{State: "merged"}}
	closed := v1alpha1.MergeRequest{Status: v1alpha1.MergeRequestStatus{State: "closed"}}

	require.False(t, stage.MRTerminal(open))
	require.False(t, stage.MRTerminal(blank))
	require.True(t, stage.MRTerminal(merged))
	require.True(t, stage.MRTerminal(closed))

	require.False(t, stage.AllMRsTerminal(nil), "an empty slice is NOT terminal")
	require.True(t, stage.AllMRsTerminal([]v1alpha1.MergeRequest{merged, closed}))
	require.False(t, stage.AllMRsTerminal([]v1alpha1.MergeRequest{merged, open}))

	require.False(t, stage.AllMRsMerged(nil))
	require.True(t, stage.AllMRsMerged([]v1alpha1.MergeRequest{merged, merged}))
	require.False(t, stage.AllMRsMerged([]v1alpha1.MergeRequest{merged, closed}))
}

// A kind=review Task reviews a HUMAN's PR: it owns ZERO Issues by construction,
// so the gate at `refined` can never grant for it. Without new -> awaiting-review
// a review Task minted ACTIVE can never complete triage at all.
func TestNewToAwaitingReviewExistsForTheReviewKind(t *testing.T) {
	tk := taskOfKind(v1alpha1.StateNew, "review")
	require.True(t, stage.LegalFor(tk, nil, v1alpha1.StateNew, v1alpha1.StateAwaitingReview))
	require.NoError(t, stage.Enter(tk, nil, v1alpha1.StateAwaitingReview, "", now))
	require.Equal(t, stage.AgentReview, tk.Status.AgentKind)
}

// WHY AN UPGRADE TASK IS MINTED STRAIGHT INTO under-implementation, and never
// triaged to refined like every other unconstrained kind.
//
// refined is where the APPROVAL GATE runs, and the only edge out of it into
// under-implementation is submit_outcome(action=approved). tatara-cli's upgrade
// outcome schema has no such action - its enum is submitted|declined only,
// because a scheduled upgrade has no issue thread and no maintainer comment to
// cite. An upgrade Task parked at refined would submit `submitted`, the operator
// would try refined -> awaiting-review, and THAT EDGE DOES NOT EXIST: the Task
// would respawn and re-submit against the same refusal forever.
//
// So upgrade takes the documentation batch's shape instead - Create ->
// under-implementation, no triage, no gate - and these are the two halves of
// that fact, pinned so a later "tidy the kind lists" change cannot quietly
// reintroduce the loop.
func TestUpgradeIsMintedIntoUnderImplementationBecauseRefinedHasNoExit(t *testing.T) {
	require.False(t, stage.Legal(v1alpha1.StateRefined, v1alpha1.StateAwaitingReview),
		"refined -> awaiting-review must stay absent: it is what forces upgrade to be minted past the gate, not through it")
	require.True(t, stage.LegalFor(taskOfKind(v1alpha1.StateNew, "upgrade"), nil,
		stage.Create, v1alpha1.StateUnderImplementation),
		"the Create edge into under-implementation is how an upgrade Task starts")
	require.True(t, stage.LegalFor(taskOfKind(v1alpha1.StateUnderImplementation, "upgrade"), nil,
		v1alpha1.StateUnderImplementation, v1alpha1.StateAwaitingReview),
		"submit_outcome(kind=upgrade, action=submitted) must be able to hand the MR to review")
}

// THE UNPARK HOT LOOP (adjacent 4, mt-r-tatara-helmfile-424). The Task logged
// action=unpark reason_from=awaiting-human every 30 seconds, indefinitely,
// without ever running a turn: Unpark RETAINS pendingEvents by contract, the
// only production drain runs after a turn is submitted to a pod, and a Task
// that re-parks before a pod exists therefore never consumes its trigger. The
// same event released the same park forever.
//
// UnparkConsumedAt is what terminates it: an event may release exactly ONE
// park.
func TestUnparkConsumesItsTriggerSoTheSameEventCannotReleaseTwice(t *testing.T) {
	tk := task(v1alpha1.StateRefined)
	require.NoError(t, stage.Park(tk, stage.ReasonAwaitingHuman, now))
	tk.Status.PendingEvents = []v1alpha1.TaskEvent{{Author: "szymonrychu", Body: "have another look"}}

	in := func() stage.UnparkInput {
		return stage.UnparkInput{
			Task: tk, BotLogin: "bot", LiveHasRoom: true, Now: now,
			Issues: []v1alpha1.Issue{{Status: v1alpha1.IssueStatus{State: "open"}}},
		}
	}
	require.Equal(t, stage.DeclineNone, stage.Unpark(in()), "the human comment releases the park once")

	// The Task re-parks before any pod ever reaches the pod-time drain - the
	// observed shape - so the event is still sitting in pendingEvents.
	require.NoError(t, stage.Park(tk, stage.ReasonAwaitingHuman, now))
	require.Equal(t, stage.DeclineNoHumanEvent, stage.Unpark(in()),
		"the SAME event must not release a SECOND park: that is the 30s hot loop")
}

// Consumption is a STAMP and not a delete, because the turn-0 bundle still
// renders the event. Only its eligibility to release another park is spent.
func TestUnparkStillRenderableAfterConsumption(t *testing.T) {
	tk := task(v1alpha1.StateRefined)
	require.NoError(t, stage.Park(tk, stage.ReasonAwaitingHuman, now))
	tk.Status.PendingEvents = []v1alpha1.TaskEvent{{Author: "szymonrychu", Body: "have another look"}}

	require.Equal(t, stage.DeclineNone, stage.Unpark(stage.UnparkInput{
		Task: tk, BotLogin: "bot", LiveHasRoom: true, Now: now,
		Issues: []v1alpha1.Issue{{Status: v1alpha1.IssueStatus{State: "open"}}},
	}))

	require.Len(t, tk.Status.PendingEvents, 1, "the event is RETAINED for the bundle, never dropped")
	require.Equal(t, "have another look", tk.Status.PendingEvents[0].Body)
	require.NotNil(t, tk.Status.PendingEvents[0].UnparkConsumedAt, "it is stamped as spent")
	require.Equal(t, now, tk.Status.PendingEvents[0].UnparkConsumedAt.UTC())
}

// A GENUINE second comment still works. This is a de-duplication, not a
// lockout.
func TestANewNonBotEventReleasesTheParkAgain(t *testing.T) {
	tk := task(v1alpha1.StateRefined)
	require.NoError(t, stage.Park(tk, stage.ReasonAwaitingHuman, now))
	tk.Status.PendingEvents = []v1alpha1.TaskEvent{{Author: "szymonrychu", Body: "first"}}

	in := func() stage.UnparkInput {
		return stage.UnparkInput{
			Task: tk, BotLogin: "bot", LiveHasRoom: true, Now: now,
			Issues: []v1alpha1.Issue{{Status: v1alpha1.IssueStatus{State: "open"}}},
		}
	}
	require.Equal(t, stage.DeclineNone, stage.Unpark(in()))
	require.NoError(t, stage.Park(tk, stage.ReasonAwaitingHuman, now))
	require.Equal(t, stage.DeclineNoHumanEvent, stage.Unpark(in()))

	tk.Status.PendingEvents = append(tk.Status.PendingEvents,
		v1alpha1.TaskEvent{Author: "szymonrychu", Body: "second"})
	require.Equal(t, stage.DeclineNone, stage.Unpark(in()),
		"a fresh human comment must still release the park")
}

// The #511 stand-down bypass is the loop's MOTOR: it skips the round cap and
// spends no HumanReviewRounds, so before UnparkConsumedAt there was no
// monotonic quantity anywhere on that path. It still spends no round - a
// take-over request is not a review round - but it can no longer fire twice on
// one request.
func TestStandDownBypassFiresOncePerTakeoverRequest(t *testing.T) {
	tk := taskOfKind(v1alpha1.StateAwaitingReview, "review")
	tk.Status.HumanReviewRounds = v1alpha1.MaxHumanReviewRounds
	require.NoError(t, stage.Park(tk, stage.ReasonAwaitingHuman, now))
	tk.Status.PendingEvents = []v1alpha1.TaskEvent{{Author: "szymonrychu", Body: "I am taking this over"}}

	in := func() stage.UnparkInput {
		return stage.UnparkInput{
			Task: tk, BotLogin: "bot", LiveHasRoom: true, Now: now,
			MRs: []v1alpha1.MergeRequest{{Status: v1alpha1.MergeRequestStatus{
				Ownership: v1alpha1.OwnershipExternal,
			}}},
		}
	}
	require.Equal(t, stage.DeclineNone, stage.Unpark(in()),
		"the take-over comment bypasses the spent round cap, exactly as #511 established")
	require.Equal(t, v1alpha1.MaxHumanReviewRounds, tk.Status.HumanReviewRounds,
		"and it must still spend NO round")

	require.NoError(t, stage.Park(tk, stage.ReasonAwaitingHuman, now))
	require.Equal(t, stage.DeclineNoHumanEvent, stage.Unpark(in()),
		"the bypass spends nothing, so the consumed stamp is the ONLY thing bounding it")
}
