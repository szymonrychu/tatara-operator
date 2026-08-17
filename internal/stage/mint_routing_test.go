package stage_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// THE DIRECT #604 PIN. `Create -> refined` existed for exactly one occupant -
// the maintainer-gated takeover mint - and that occupant could never leave the
// state the edge delivered it to: `refined`'s only forward edge into the work is
// submit_outcome(action=approved) -> verifyApprovalScope, which refuses
// no-live-issue for a Task owning zero Issue CRs. The edge is gone; nothing may
// put it back without also answering the routing invariant, which lives in
// internal/controller/mint_routing_test.go because the OTHER route into
// `refined` - triage - is decided by triageTarget, and triageTarget is
// UNEXPORTED there. NOT an import restriction: this file is package stage_test
// and could legally import controller. Keep this in step with the same
// statement on LegalFor's GUARD 6, which points the reader here.
func TestCreateEdgeCannotMintIntoTheGate(t *testing.T) {
	for _, e := range stage.Transitions[stage.Create] {
		require.NotEqualf(t, v1alpha1.StateRefined, e.To,
			"a Create edge targets `refined` again. Every kind minted there faces the approval gate "+
				"with no triage behind it; if the kind owns zero Issues the gate can never grant and "+
				"the Task strands (#604). Route it to `new` (triage decides) or to "+
				"`under-implementation` (the work) instead")
	}
}
