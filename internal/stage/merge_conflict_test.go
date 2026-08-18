package stage_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// CYCLE 6, the conflict self-heal: a tatara-owned merge request that has gone
// DIRTY goes back to the agent that can reconcile it.
func TestMergeConflictRouting(t *testing.T) {
	t.Run("review kind parks awaiting-human", func(t *testing.T) {
		e, ok := stage.MergeConflict(taskOfKind(v1alpha1.StateMerged, "review"), nil, 3)
		require.True(t, ok)
		require.Equal(t, stage.ParkTarget, e.To)
		require.Equal(t, stage.ReasonAwaitingHuman, e.Reason)
	})
	t.Run("an already-merged sibling parks the retry lane", func(t *testing.T) {
		mrs := []v1alpha1.MergeRequest{{Status: v1alpha1.MergeRequestStatus{State: "merged"}}}
		e, ok := stage.MergeConflict(task(v1alpha1.StateMerged), mrs, 3)
		require.True(t, ok)
		require.Equal(t, stage.ParkTarget, e.To)
		require.Equal(t, stage.ReasonMergeConflictRetry, e.Reason,
			"re-implementing would re-propose merged code and recreate deleted branches, "+
				"so this arm still PARKS - but a base branch that moved is a machine's to clear")
		class, _ := stage.UnparkClassFor(e.Reason)
		require.Equal(t, stage.UnparkRetry, class)
		require.True(t, stage.IsMergeStagePark(e.Reason))
	})
	t.Run("bounces to under-implementation, bounded, then degrades to merge-blocked", func(t *testing.T) {
		tk := task(v1alpha1.StateMerged)
		for lap := 1; lap <= v1alpha1.MaxMergeConflictReentries; lap++ {
			e, ok := stage.MergeConflict(tk, nil, v1alpha1.MaxMergeConflictReentries)
			require.True(t, ok)
			require.Equal(t, v1alpha1.StateUnderImplementation, e.To)
			require.Equal(t, stage.ReasonMergeConflict, e.Reason)
			require.Equal(t, lap, tk.Status.MergeConflictReentries)
		}
		e, ok := stage.MergeConflict(tk, nil, v1alpha1.MaxMergeConflictReentries)
		require.True(t, ok)
		require.Equal(t, stage.ParkTarget, e.To)
		require.Equal(t, stage.ReasonMergeBlocked, e.Reason,
			"exhaustion must land on the SAME terminal the stall-and-time-out path reaches today")
	})
}

// TestMergeConflictCapStaysOutsideTheRetryLane is the explicit decision H2-C
// asks for: a spent MaxMergeConflictReentries budget parks merge-blocked, which
// is UnparkNever, and must NOT move into the lane - that would be a second
// budget stacked on an exhausted one, escaping a cap a lap at a time.
func TestMergeConflictCapStaysOutsideTheRetryLane(t *testing.T) {
	tk := task(v1alpha1.StateMerged)
	tk.Status.MergeConflictReentries = v1alpha1.MaxMergeConflictReentries
	e, ok := stage.MergeConflict(tk, nil, v1alpha1.MaxMergeConflictReentries)
	require.True(t, ok)
	require.Equal(t, stage.ReasonMergeBlocked, e.Reason)
	class, _ := stage.UnparkClassFor(e.Reason)
	require.Equal(t, stage.UnparkNever, class)
}
