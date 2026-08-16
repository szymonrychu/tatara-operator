package v1alpha1_test

import (
	"os"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

// THE TWO initialState ENUMS ARE ONE FACT, AND enqueue.go IS WHY (#604).
//
// QueuedEventPayload.InitialState is copied VERBATIM onto TaskSpec.InitialState
// by internal/queue/enqueue.go. So a value the payload admits but the Task
// rejects does not fail at the queue - it fails at the Task CREATE, on every
// reconcile pass, forever. That is the stranded-object shape #604 fixed for the
// takeover Task, reproduced one enforcement site later and against an object
// nobody is watching.
//
// This reads the MARKERS rather than the generated CRDs on purpose: the markers
// are what a person edits, and `make manifests` regenerates the charts from
// them, so a drift introduced by hand is caught here before it is baked in.
func TestInitialStateEnumsAgreeAcrossTaskAndQueuedEvent(t *testing.T) {
	enumRe := regexp.MustCompile(`\+kubebuilder:validation:Enum=([^\s]+)\n[^\n]*\+optional\n[^\n]*InitialState string`)

	read := func(path string) string {
		t.Helper()
		b, err := os.ReadFile(path)
		require.NoErrorf(t, err, "read %s", path)
		m := enumRe.FindSubmatch(b)
		require.NotNilf(t, m, "no InitialState Enum marker found in %s", path)
		return string(m[1])
	}

	task := read("task_types.go")
	queued := read("queuedevent_types.go")

	require.Equalf(t, task, queued,
		"TaskSpec.InitialState admits %q but QueuedEventPayload.InitialState admits %q. "+
			"enqueue.go copies the payload value onto the Task VERBATIM, so a value only the "+
			"payload admits builds a Task the API server rejects on every reconcile pass forever",
		task, queued)

	// And `refined` is gone from both: it is the value #604 removed, because the
	// (create) -> refined edge no longer exists and an admitted spec whose edge
	// is missing is a permanent IllegalTransitionError loop.
	require.NotContainsf(t, task, "refined",
		"`refined` is back in the initialState enum. There is no (create) -> refined edge, so a "+
			"Task minted there fails stage.Enter on every pass and increments "+
			"operator_illegal_state_transition_total - whose alert blames the TABLE, not the spec (#604)")
}
