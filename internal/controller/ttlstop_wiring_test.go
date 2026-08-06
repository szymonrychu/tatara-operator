// The wiring half of issue #527. internal/agent proves the stop sequence USES
// LastFinalText/PushedRepos; this proves ttlStop actually SUPPLIES them. The
// original defect lived entirely in the gap between those two facts: the
// mechanism was correct and its only caller passed it nothing.
package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// ttlStopTask is a Task whose pod is an hour past t0 with a recorded last turn.
func ttlStopTask(name string, lastTurn *tatarav1alpha1.LastTurn) *tatarav1alpha1.Task {
	started := metav1.NewTime(time.Now().Add(-2 * time.Hour))
	return &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: mdNS, UID: types.UID("uid-" + name)},
		Spec:       tatarav1alpha1.TaskSpec{Kind: "implement", ProjectRef: "proj", Goal: "g"},
		Status: tatarav1alpha1.TaskStatus{
			Stage:        tatarav1alpha1.StageImplementing,
			PodStartedAt: &started,
			LastTurn:     lastTurn,
		},
	}
}

func ttlStopProject() *tatarav1alpha1.Project {
	p := tsProject(3)
	p.Spec.AgentPodTTLSeconds = 3600
	p.Spec.Agent.TurnTimeoutSeconds = 1
	return p
}

// TestTTLStop_SyntheticNoteCarriesLastTurn is the root-cause regression at the
// call site. The pod is gone (the wrapper is unreachable), so the synthetic path
// runs - exactly the production shape - and the note it writes must contain the
// last turn's work, not a placeholder.
func TestTTLStop_SyntheticNoteCarriesLastTurn(t *testing.T) {
	task := ttlStopTask("t-ttlwire", &tatarav1alpha1.LastTurn{
		At:          metav1.Now(),
		FinalText:   "rebased onto main; the merge gate is still red on lint",
		PushedRepos: []string{"tatara-operator", "tatara-observability"},
	})
	c := newMirrorClient(t, task)
	r := tsReconciler(c)

	_, err := r.ttlStop(context.Background(), ttlStopProject(), task, "implement", time.Now())
	require.NoError(t, err)

	fresh := mdGetTask(t, c, "t-ttlwire")
	require.Len(t, fresh.Status.Notes, 1, "the TTL stop must leave exactly its handoff note")
	body := fresh.Status.Notes[0].Body
	require.Contains(t, body, "rebased onto main; the merge gate is still red on lint",
		"ttlStop did not pass status.lastTurn.finalText into TTLStopInput (#527)")
	require.Contains(t, body, "tatara-operator, tatara-observability",
		"ttlStop did not pass status.lastTurn.pushedRepos into TTLStopInput (#527)")
	require.NotContains(t, body, agent.SyntheticNoteLostMarker)

	require.Nil(t, fresh.Status.LastTurn, "lastTurn describes a pod that no longer exists")
	require.Nil(t, fresh.Status.PodStartedAt, "the TTL stop re-arms the Task for a fresh pod")
}

// TestTTLStop_NoLastTurnWritesTheLossPlaceholder: with nothing recorded there is
// genuinely nothing to hand off. The note must SAY that rather than render an
// empty-fields handoff, which is what the next pod (and the alert) read.
func TestTTLStop_NoLastTurnWritesTheLossPlaceholder(t *testing.T) {
	task := ttlStopTask("t-ttlwire-empty", nil)
	c := newMirrorClient(t, task)
	r := tsReconciler(c)

	_, err := r.ttlStop(context.Background(), ttlStopProject(), task, "implement", time.Now())
	require.NoError(t, err)

	fresh := mdGetTask(t, c, "t-ttlwire-empty")
	require.Len(t, fresh.Status.Notes, 1)
	require.Contains(t, fresh.Status.Notes[0].Body, agent.SyntheticNoteLostMarker)
}

// TestTTLStop_LongFinalTextStillFitsTheNote: LastTurn.FinalText is capped at
// 2048 bytes and Note.Body at 4096 precisely so the note the stop writes is
// always writable. A note the API server rejects is an EMPTY journal.
func TestTTLStop_LongFinalTextStillFitsTheNote(t *testing.T) {
	task := ttlStopTask("t-ttlwire-long", &tatarav1alpha1.LastTurn{
		At:        metav1.Now(),
		FinalText: strings.Repeat("x", tatarav1alpha1.LastTurnFinalTextMaxBytes),
	})
	c := newMirrorClient(t, task)
	r := tsReconciler(c)

	_, err := r.ttlStop(context.Background(), ttlStopProject(), task, "implement", time.Now())
	require.NoError(t, err)

	fresh := mdGetTask(t, c, "t-ttlwire-long")
	require.Len(t, fresh.Status.Notes, 1)
	require.LessOrEqual(t, len(fresh.Status.Notes[0].Body), tatarav1alpha1.NoteBodyMaxBytes)
}
