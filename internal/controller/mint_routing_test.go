package controller

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// THE ZERO-ISSUE ROUTING INVARIANT (tatara-operator#604).
//
// `refined` is where the approval gate runs, and its ONLY forward edge into the
// work is submit_outcome(action=approved) -> verifyApprovalScope, which refuses
// with no-live-issue for a Task owning zero Issue CRs. mintIssueCRs is the ONE
// writer of status.issueRefs and it bails on `src.IsPR`, on a nil source, and on
// kind=review - so a kind whose Tasks never carry an ISSUE source owns zero
// Issues forever and can never satisfy that gate.
//
// The argument was written out in prose three times before #604 - for the review
// lane (Transitions' `new -> awaiting-review` comment), for adopted upgrade
// (GUARD 5) and for cron upgrade (restapi/outcome.go) - and takeover was the one
// place it was not applied: it was minted straight into `refined` owning zero
// Issues, burned a pod, and parked awaiting-human forever.
//
// IT LIVES IN internal/controller, NOT internal/stage, and that is the round-2
// review's correction. #604 deleted the `Create -> refined` edge, so after it
// NOTHING is minted into the gate and a table keyed on mint states alone can
// never evaluate its own predicate - it was dead code wearing an invariant's doc
// block. The route that survives is TRIAGE, and triageTarget is decided here. So
// the entry set below is mint states UNION the real triageTarget's answer, and
// the predicate runs for every kind that can reach `refined` by either road.
//
// mintRouting is TOTAL over stage.OriginKinds(). A kind added to
// originAgentKinds with no row here FAILS, which is the forcing function: the
// next author has to say where their kind is minted and whether it owns an
// Issue, rather than discovering the answer from a stranded Task in production.
//
// WHAT THIS TABLE CANNOT DO, stated so nobody trusts it for more than it is: the
// mintStates are DECLARED here, not read back from each minter, because a minter
// picks its InitialState in Go rather than from a table. So flipping
// takeover_mint.go back to StateRefined does not fail this test - the row would
// simply have become a lie. Two other things catch that: the per-minter tests
// pin each real minter's InitialState directly
// (TestMintOrUnparkTakeoverTask_MintsBoundIntoUnderImplementation), and stage's
// TestCreateEdgeCannotMintIntoTheGate refuses the edge outright, so there is
// nowhere for a refined mint to land. The TRIAGE half, by contrast, is derived
// and cannot go stale.
var mintRouting = map[string]struct {
	// mintStates is every Create-edge target a minter picks for this kind.
	// Empty spec.initialState means `new`, so `new` is spelled out here.
	mintStates []string
	// ownsIssueAtGate says whether a Task of this kind owns >= 1 Issue CR by
	// the time it faces the gate. It is the property mintIssueCRs decides: TRUE
	// only for a kind whose Source is a real ISSUE (not a PR, not absent).
	ownsIssueAtGate bool
	// why records the reason, so a row that changes has to argue for itself.
	why string
}{
	"implement": {
		mintStates:      []string{v1alpha1.StateNew},
		ownsIssueAtGate: true,
		why:             "the only kind with an ISSUE source; triaged to refined and the gate has something to approve",
	},
	"takeover": {
		mintStates:      []string{v1alpha1.StateUnderImplementation},
		ownsIssueAtGate: false,
		why:             "#604: source is always IsPR, so mintIssueCRs bails and it owns zero Issues; minted straight into the work, which is where its own re-take un-park already lands",
	},
	"review": {
		mintStates:      []string{v1alpha1.StateNew},
		ownsIssueAtGate: false,
		why:             "reviews a HUMAN's PR; owns zero Issues by construction and takes the new -> awaiting-review review lane (GUARD 5)",
	},
	"upgrade": {
		mintStates:      []string{v1alpha1.StateNew, v1alpha1.StateUnderImplementation},
		ownsIssueAtGate: false,
		why:             "ADOPTED takes new -> awaiting-review (GUARD 5); CRON-minted goes straight into under-implementation. Neither has a driving issue",
	},
	"documentation": {
		mintStates:      []string{v1alpha1.StateUnderImplementation},
		ownsIssueAtGate: false,
		why:             "the nightly batch has no driving issue and no approval to gate; it finishes at done(doc-timeout) via GUARD 4",
	},
	"brainstorm": {
		mintStates:      []string{v1alpha1.StateNew},
		ownsIssueAtGate: false,
		why:             "no Source at all, but it leaves refined through GUARD 6's refined -> done terminal, not through the gate",
	},
	"incident": {
		mintStates:      []string{v1alpha1.StateNew},
		ownsIssueAtGate: false,
		why:             "alert-born, no Source; leaves refined through GUARD 6's refined -> done terminal",
	},
	"refine": {
		mintStates:      []string{v1alpha1.StateNew},
		ownsIssueAtGate: false,
		why:             "backlog groomer, no Source; leaves refined through GUARD 6's refined -> done terminal",
	},
}

// entryStates is every state a Task of this kind can arrive at before it has
// submitted anything: the states its minters pick, plus - for a kind minted at
// `new` - wherever the REAL triageTarget sends it from there. Triage is the only
// remaining road into `refined`, so deriving this half rather than declaring it
// is what gives the invariant below any teeth at all.
//
// Both upgrade shapes are asked, because triageTarget splits on AdoptedUpgrade
// and the two answers differ. Asking only the plain shape would silently drop
// the adopted lane from the check.
func entryStates(kind string, mintStates []string) []string {
	seen := map[string]bool{}
	var out []string
	add := func(s string) {
		if s != "" && !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	for _, s := range mintStates {
		add(s)
		if s != v1alpha1.StateNew {
			continue
		}
		for _, shape := range triageShapes(kind) {
			if to, ok := triageTarget(shape); ok {
				add(to)
			}
		}
	}
	return out
}

// triageShapes returns the Task shapes triageTarget can be handed for kind: the
// plain one, and the ADOPTED one (spec.source pointing at an existing merge
// request, stage.AdoptedMR) that the upgrade arm splits on.
func triageShapes(kind string) []*v1alpha1.Task {
	plain := &v1alpha1.Task{Spec: v1alpha1.TaskSpec{Kind: kind}}
	adopted := &v1alpha1.Task{Spec: v1alpha1.TaskSpec{
		Kind:   kind,
		Source: &v1alpha1.TaskSource{IsPR: true, Number: 1},
	}}
	return []*v1alpha1.Task{plain, adopted}
}

// canLeaveRefined derives, from the MACHINE rather than from a second declared
// bool, whether a Task of this kind has any forward edge out of `refined`:
//
//   - refined -> under-implementation is the approval gate, and it can only
//     grant for a kind that owns an Issue (verifyApprovalScope's first branch).
//   - refined -> done is GUARD 6's non-code terminal, and LegalFor is the
//     authority on who may take it.
//
// A kind for which BOTH are shut is stranded at `refined`: a live state that
// runs a pod, elapses, and parks awaiting-human with nothing that can ever
// un-park it.
func canLeaveRefined(kind string, ownsIssueAtGate bool) bool {
	if ownsIssueAtGate {
		return true
	}
	task := &v1alpha1.Task{Spec: v1alpha1.TaskSpec{Kind: kind}}
	task.Status.State = v1alpha1.StateRefined
	return stage.LegalFor(task, nil, v1alpha1.StateRefined, v1alpha1.StateDone)
}

func TestNoOriginKindCanReachAGateItCannotLeave(t *testing.T) {
	reached := false
	for _, kind := range stage.OriginKinds() {
		row, ok := mintRouting[kind]
		require.Truef(t, ok,
			"origin kind %q has no mintRouting row: say where it is minted and whether it owns an "+
				"Issue by the time it faces the gate, or it can be routed into `refined` and strand "+
				"there exactly as takeover did (#604)", kind)

		for _, entry := range entryStates(kind, row.mintStates) {
			if entry != v1alpha1.StateRefined {
				continue
			}
			reached = true
			require.Truef(t, canLeaveRefined(kind, row.ownsIssueAtGate),
				"kind %q reaches `refined` but has NO forward edge out of it: it owns no Issue so the "+
					"approval gate refuses no-live-issue forever, and GUARD 6 closes refined -> done "+
					"to it. It would burn a pod and park awaiting-human permanently. (%s)",
				kind, row.why)
		}
	}
	// The predicate above was UNREACHABLE in #604's first cut: no row listed
	// `refined`, so the loop body never ran and canLeaveRefined was called by
	// nothing at all. Assert the check actually executed, so this cannot rot back
	// into a totality test that only looks like an invariant.
	require.True(t, reached,
		"no origin kind reaches `refined` at all, so canLeaveRefined never ran. Either triageTarget "+
			"stopped routing anything to the gate - in which case the gate is dead code and should be "+
			"deleted rather than left untested - or entryStates has stopped deriving the triage route "+
			"it exists to derive")
}

// The table must not accumulate rows for kinds that no longer exist: a stale row
// is a claim about the machine that nothing checks.
func TestMintRoutingHasNoRowsForRetiredKinds(t *testing.T) {
	live := map[string]bool{}
	for _, k := range stage.OriginKinds() {
		live[k] = true
	}
	for kind := range mintRouting {
		require.Truef(t, live[kind],
			"mintRouting has a row for %q, which is not in originAgentKinds any more", kind)
	}
}
