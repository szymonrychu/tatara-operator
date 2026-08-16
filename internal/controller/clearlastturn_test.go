package controller

import (
	"testing"

	"github.com/stretchr/testify/require"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// A STALE failedRepos IS WORSE THAN A STALE finalText, BECAUSE IT DISARMS THE
// #527 DETECTOR RATHER THAN MERELY MISREPORTING.
//
// clearLastTurn retires the continuation state a G.7 stop has just spent. Left
// behind, a failed set makes the NEXT pod's stop compute contentFree=false even
// when that pod produced nothing at all - so RecordEmptySynthetic never fires,
// syntheticNoteLostBody() is never written, and a genuinely content-free stop
// becomes indistinguishable from a real handoff. That is precisely the silent
// loss #527 exists to catch, and it would be re-armed by a slice nobody cleared.
func TestClearLastTurn_RetiresTheFailedReposToo(t *testing.T) {
	task := &tatarav1alpha1.Task{
		Status: tatarav1alpha1.TaskStatus{
			LastTurnFinalText:   "opened PR #91",
			LastTurnPushedRepos: []string{"tatara-operator"},
			LastTurnFailedRepos: []string{"tatara-cli"},
			LastTurnReposTurnID: "turn-42",
		},
	}

	clearLastTurn(task)

	require.Empty(t, task.Status.LastTurnFinalText)
	require.Empty(t, task.Status.LastTurnPushedRepos)
	require.Empty(t, task.Status.LastTurnReposTurnID,
		"an id left behind names a turn whose lists are gone, and it gates the backstop's clear")
	require.Empty(t, task.Status.LastTurnFailedRepos,
		"the stop that just ran already rendered this into the notes journal; "+
			"leaving it makes the next stop attribute this pod's loss to that one")
}
