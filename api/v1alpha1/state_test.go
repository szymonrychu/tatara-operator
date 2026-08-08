package v1alpha1_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// PURE UNIT. No envtest.
func TestTaskDoneIsExactlyDoneAndRejected(t *testing.T) {
	for _, state := range []string{v1alpha1.StateDone, v1alpha1.StateRejected} {
		tk := &v1alpha1.Task{Status: v1alpha1.TaskStatus{State: state}}
		require.True(t, v1alpha1.TaskDone(tk), "%s must be done", state)
	}
	for _, state := range []string{
		v1alpha1.StateNew, v1alpha1.StateRefined, v1alpha1.StateUnderImplementation,
		v1alpha1.StateAwaitingReview, v1alpha1.StateMerged, v1alpha1.StateDeployed,
	} {
		tk := &v1alpha1.Task{Status: v1alpha1.TaskStatus{State: state}}
		require.False(t, v1alpha1.TaskDone(tk), "%s must NOT be done", state)
	}
}

// THE #521 REGRESSION TEST. TaskDone(parked) was TRUE, which skipped intake's
// live-twin branch at intake.go:311 and let the Create 409 fall through to the
// delete at intake.go:332.
func TestTaskDoneIsFalseForEveryParkedNonTerminalState(t *testing.T) {
	for _, state := range []string{
		v1alpha1.StateNew, v1alpha1.StateRefined, v1alpha1.StateUnderImplementation,
		v1alpha1.StateAwaitingReview, v1alpha1.StateMerged, v1alpha1.StateDeployed,
	} {
		tk := &v1alpha1.Task{Status: v1alpha1.TaskStatus{
			State: state, ParkReason: "awaiting-human", ParkedAt: &metav1.Time{},
		}}
		require.False(t, v1alpha1.TaskDone(tk),
			"a parked %s Task is stalled, not finished; TaskDone(parked)==true is issue #521", state)
		require.True(t, v1alpha1.Parked(tk))
	}
}

func TestParkedIsTheEmptyStringTest(t *testing.T) {
	require.False(t, v1alpha1.Parked(&v1alpha1.Task{}))
	require.True(t, v1alpha1.Parked(&v1alpha1.Task{
		Status: v1alpha1.TaskStatus{ParkReason: "backlog-sweep"}}))
}

func TestTaskIsTerminalOutcomeIsTheSameSetAsTaskDone(t *testing.T) {
	require.True(t, v1alpha1.TaskIsTerminalOutcome(v1alpha1.StateDone))
	require.True(t, v1alpha1.TaskIsTerminalOutcome(v1alpha1.StateRejected))
	for _, state := range []string{
		v1alpha1.StateNew, v1alpha1.StateRefined, v1alpha1.StateUnderImplementation,
		v1alpha1.StateAwaitingReview, v1alpha1.StateMerged, v1alpha1.StateDeployed,
	} {
		require.False(t, v1alpha1.TaskIsTerminalOutcome(state))
	}
}

func TestNoteIDIsDeterministicAndStable(t *testing.T) {
	at := metav1.Unix(1700000000, 0)
	a := v1alpha1.NewNoteID(at, "plan", "the plan body")
	b := v1alpha1.NewNoteID(at, "plan", "the plan body")
	c := v1alpha1.NewNoteID(at, "plan", "the plan body edited")
	require.Equal(t, a, b, "the same note must hash to the same id or planNoteId cannot be quoted back")
	require.NotEqual(t, a, c)
	require.Regexp(t, `^n-[0-9a-f]{16}$`, a)
}

func TestSpecKindEnumHasImplementAndNotClarify(t *testing.T) {
	require.True(t, v1alpha1.IsKnownKind("implement"),
		"every Task the merged model mints carries spec.kind=implement; the QueuedEvent validator must accept it")
	require.False(t, v1alpha1.IsKnownKind("clarify"))
}
