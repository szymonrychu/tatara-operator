package restapi_test

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
)

// cappedTaskV2 is a Task whose journal already sits exactly at the cap, so the
// ONE note commit appends makes eviction - and therefore the spiller - load
// bearing on the outcome path.
func cappedTaskV2(name string) *tatarav1alpha1.Task {
	task := taskV2(name, "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement")
	for i := 0; i < tatarav1alpha1.MaxNotes; i++ {
		task.Status.Notes = append(task.Status.Notes, tatarav1alpha1.Note{
			At:    metav1.NewTime(frozenNow.Add(time.Duration(i) * time.Minute)),
			Agent: "implement", Kind: "note", Body: fmt.Sprintf("note-%02d", i),
		})
	}
	return task
}

// TestOutcome_AtCapWithBrokenSpillerIs503 is postNote's contract, one door over.
// commit carries WithNoteCap now (#616), so a Task sitting at the cap makes
// ErrSpillFailed / ErrSpillerUnconfigured reachable on the OUTCOME path for the
// first time. Both are transient by construction - a tatara-memory outage, or
// an endpoint the Project has not published yet - so they must answer 503 +
// Retry-After, not the generic 500 writeClientErr gives every other error. A
// 500 tells the agent to give up on a condition that clears by itself, and it
// disagrees with what postNote says about the very same Task.
func TestOutcome_AtCapWithBrokenSpillerIs503(t *testing.T) {
	tests := []struct {
		name string
		opts v2Opts
	}{
		{name: "spill fails", opts: v2Opts{writer: panicForge{}, spillerErr: errors.New("tatara-memory unreachable")}},
		{name: "no spiller configured", opts: v2Opts{
			writer:     panicForge{},
			spillerFor: func(*tatarav1alpha1.Project) objbudget.Spiller { return nil },
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := buildV2(t, tc.opts, projectV2("tatara"), scmSecretV2(),
				repoV2("tatara-operator", "tatara"), cappedTaskV2("t1"))

			w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
				`{"kind":"implement","payload":{"action":"declined","reason":"main already carries this change"}}`)
			require.Equal(t, http.StatusServiceUnavailable, w.Code, w.Body.String())
			require.Equal(t, "10", w.Header().Get("Retry-After"))

			// The block loses NOTHING and commits NOTHING: no transition, no
			// note, no eviction.
			got := e.task(t, "t1")
			require.Len(t, got.Status.Notes, tatarav1alpha1.MaxNotes)
			require.Equal(t, "note-00", got.Status.Notes[0].Body)
			require.Equal(t, 0, got.Status.Stats.NotesSpilled)
			require.Equal(t, tatarav1alpha1.StateUnderImplementation, got.Status.State)
		})
	}
}

// TestOutcome_AtCapOnAMemoryDisabledProjectCommits is the other half: on a
// Project that is memory-free by configuration the resolver hands out
// objbudget.Discarding, so the same write evicts, drops, and lands.
func TestOutcome_AtCapOnAMemoryDisabledProjectCommits(t *testing.T) {
	e := buildV2(t, v2Opts{
		writer:     panicForge{},
		spillerFor: func(*tatarav1alpha1.Project) objbudget.Spiller { return objbudget.Discarding },
	}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"), cappedTaskV2("t1"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"declined","reason":"main already carries this change"}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	got := e.task(t, "t1")
	require.Len(t, got.Status.Notes, tatarav1alpha1.MaxNotes)
	require.Equal(t, "note-01", got.Status.Notes[0].Body, "the oldest note was dropped")
	require.Equal(t, 1, got.Status.Stats.NotesSpilled)
	require.Empty(t, got.Status.Stats.NotesSpilledRefs, "a discarded batch is stored nowhere; it gets no ref")
}
