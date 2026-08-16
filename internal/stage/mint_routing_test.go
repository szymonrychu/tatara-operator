package stage_test

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
// with no-live-issue for a Task owning zero Issue CRs. controller.mintIssueCRs
// is the ONE writer of status.issueRefs and it bails on `src.IsPR`, on a nil
// source, and on kind=review - so a kind whose Tasks never carry an ISSUE source
// owns zero Issues forever and can never satisfy that gate.
//
// The argument was written out in prose three times before #604 - for the review
// lane (Transitions' `new -> awaiting-review` comment), for adopted upgrade
// (GUARD 5) and for cron upgrade (restapi/outcome.go) - and takeover was the one
// place it was not applied: it was minted straight into `refined` owning zero
// Issues, burned a pod, and parked awaiting-human forever. This table is that
// prose turned into a predicate, for the same reason LegalFor exists at all: a
// property that travels with the table cannot be missed by the next kind.
//
// mintRouting is TOTAL over stage.OriginKinds(). A kind added to
// originAgentKinds with no row here FAILS, which is the forcing function: the
// next author has to say where their kind is minted and whether it owns an
// Issue, rather than discovering the answer from a stranded Task in production.
//
// WHAT THIS TABLE CANNOT DO, stated so nobody trusts it for more than it is: the
// mintStates are DECLARED here, not read back from each minter, because
// internal/stage is pure and cannot import the controllers that do the minting.
// So flipping takeover_mint.go back to StateRefined does not fail this test -
// the row would simply have become a lie. Two other things catch that: the
// per-minter tests pin each real minter's InitialState directly
// (TestMintOrUnparkTakeoverTask_MintsBoundIntoUnderImplementation), and
// TestCreateEdgeCannotMintIntoTheGate below refuses the edge outright, so there
// is nowhere for a refined mint to land. This table's job is the NEW kind that
// has no test yet, and the invariant it must answer before it gets one.
var mintRouting = map[string]struct {
	// mintStates is every Create-edge target a minter picks for this kind.
	// Empty spec.initialState means `new`, so `new` is spelled out here.
	mintStates []string
	// ownsIssueAtGate says whether a Task of this kind owns >= 1 Issue CR by
	// the time it faces the gate. It is the property controller.mintIssueCRs
	// decides: TRUE only for a kind whose Source is a real ISSUE (not a PR,
	// not absent).
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
	return stage.LegalFor(taskOfKind(v1alpha1.StateRefined, kind), nil,
		v1alpha1.StateRefined, v1alpha1.StateDone)
}

func TestNoOriginKindIsMintedIntoAGateItCannotLeave(t *testing.T) {
	for _, kind := range stage.OriginKinds() {
		row, ok := mintRouting[kind]
		require.Truef(t, ok,
			"origin kind %q has no mintRouting row: say where it is minted and whether it owns an "+
				"Issue by the time it faces the gate, or it can be minted into `refined` and strand "+
				"there exactly as takeover did (#604)", kind)

		for _, mintState := range row.mintStates {
			if mintState != v1alpha1.StateRefined {
				continue
			}
			require.Truef(t, canLeaveRefined(kind, row.ownsIssueAtGate),
				"kind %q is minted into `refined` but has NO forward edge out of it: it owns no Issue "+
					"so the approval gate refuses no-live-issue forever, and GUARD 6 closes refined -> "+
					"done to it. It would burn a pod and park awaiting-human permanently. (%s)",
				kind, row.why)
		}
	}
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

// THE DIRECT #604 PIN. `Create -> refined` existed for exactly one occupant -
// the maintainer-gated takeover mint - and that occupant could never leave the
// state the edge delivered it to. The edge is gone; nothing may put it back
// without also answering the invariant above.
func TestCreateEdgeCannotMintIntoTheGate(t *testing.T) {
	for _, e := range stage.Transitions[stage.Create] {
		require.NotEqualf(t, v1alpha1.StateRefined, e.To,
			"a Create edge targets `refined` again. Every kind minted there faces the approval gate "+
				"with no triage behind it; if the kind owns zero Issues the gate can never grant and "+
				"the Task strands (#604). Route it to `new` (triage decides) or to "+
				"`under-implementation` (the work) instead")
	}
}
