package stage_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// TestEnter_ClearsLastTurn: status.lastTurn is the CURRENT pod's continuation
// payload and is cleared on every transition, exactly like PodStartedAt. Carried
// across an edge, a TTL stop in the new stage would build its synthetic handoff
// note out of the PREVIOUS agent kind's final text and present it to the next
// pod as that pod's own continuation state (issue #527).
func TestEnter_ClearsLastTurn(t *testing.T) {
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	task := &v1alpha1.Task{
		Spec: v1alpha1.TaskSpec{Kind: "incident"},
		Status: v1alpha1.TaskStatus{
			Stage: v1alpha1.StageImplementing,
			LastTurn: &v1alpha1.LastTurn{
				FinalText:   "half-written reconciler",
				PushedRepos: []string{"tatara-operator"},
			},
		},
	}

	require.NoError(t, stage.Enter(task, nil, v1alpha1.StageReviewing, "", now))
	require.Nil(t, task.Status.LastTurn,
		"lastTurn survived a stage transition: the next stage's TTL stop would hand off the previous stage's work")
	require.Nil(t, task.Status.PodStartedAt, "precondition: the pod clocks are cleared on the same edge")
}
