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

// fillToCap seats a Task's journal exactly at the cap, so the ONE note commit
// appends makes COUNT eviction - and therefore the spiller - load bearing on
// the outcome path.
func fillToCap(task *tatarav1alpha1.Task) *tatarav1alpha1.Task {
	for i := 0; i < tatarav1alpha1.MaxNotes; i++ {
		task.Status.Notes = append(task.Status.Notes, tatarav1alpha1.Note{
			At:    metav1.NewTime(frozenNow.Add(time.Duration(i) * time.Minute)),
			Agent: "implement", Kind: "note", Body: fmt.Sprintf("note-%02d", i),
		})
	}
	return task
}

func cappedTaskV2(name string) *tatarav1alpha1.Task {
	return fillToCap(taskV2(name, "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"))
}

// TestOutcome_AtCapWithABrokenSpillerCommitsUncapped pins the policy under the
// status code. commit IS the stage transition, and the PR's own rule is that a
// tatara-memory blip must never block one - but commit is also the one door
// every agentNote goes through, so it carries WithNoteCap. When the cap alone is
// what needs the spiller, the cap yields: FitTask re-runs WITHOUT it, the
// outcome lands, and the journal sits one over MaxNotes until the next
// successful note write trims it. That is exactly the pre-#616 status quo, and
// it drops nothing.
func TestOutcome_AtCapWithABrokenSpillerCommitsUncapped(t *testing.T) {
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
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())

			got := e.task(t, "t1")
			require.True(t, tatarav1alpha1.Parked(got), "the transition landed")
			require.Len(t, got.Status.Notes, tatarav1alpha1.MaxNotes+1, "the cap yielded rather than blocking")
			require.Equal(t, "note-00", got.Status.Notes[0].Body, "nothing was evicted, so nothing was dropped")
			require.Equal(t, 0, got.Status.Stats.NotesSpilled)
		})
	}
}

// TestOutcome_AtCapWithABrokenSpillerDoesNotStrandAForgeWrite is why the test
// above is not a cosmetic status-code choice. commit runs AFTER the
// non-idempotent forge write (incident file_issue's CreateIssue), so refusing it
// leaves the tracker issue minted and the Task in its old stage. The claim buys
// OutcomeClaimTTL and no more: classifyOutcomeClaim then re-claims the orphaned
// stub, the handler re-runs from the top, and CreateIssue fires again - one
// duplicate tracker issue per TTL for the length of the outage.
func TestOutcome_AtCapWithABrokenSpillerDoesNotStrandAForgeWrite(t *testing.T) {
	e := buildV2(t, v2Opts{spillerErr: errors.New("tatara-memory unreachable")},
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"),
		fillToCap(taskV2("t1", "tatara", "incident", tatarav1alpha1.StateRefined, "incident")))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", `{"kind":"incident","payload":{
	  "action":"file_issue","alertRules":["tatara-operator-down"],"reason":"real outage",
	  "issue":{"repo":"tatara-operator","title":"operator down","body":"trace"}}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Len(t, e.forge.createdReqs, 1)

	got := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.StateDone, got.Status.State,
		"the stage moved, so nothing re-claims the stub and re-mints the issue")
	require.Len(t, got.Status.Notes, tatarav1alpha1.MaxNotes+1)
}

// TestOutcome_ByteEvictionWithABrokenSpillerIs503 is the 503 that SURVIVES.
// Dropping the cap only helps when the cap is what needed the spiller; a journal
// over ObjectByteBudget needs eviction either way, A.7's SPILL FIRST genuinely
// applies, and refusing is the only answer that does not lose a note. 45 notes
// is under MaxNotes, so the count cap is not in play at all.
func TestOutcome_ByteEvictionWithABrokenSpillerIs503(t *testing.T) {
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
			task := taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement")
			for i := 0; i < 45; i++ {
				task.Status.Notes = append(task.Status.Notes,
					bigNote(frozenNow.Add(time.Duration(i)*time.Minute), fmt.Sprintf("note-%02d-", i), 20_000))
			}

			e := buildV2(t, tc.opts, projectV2("tatara"), scmSecretV2(),
				repoV2("tatara-operator", "tatara"), task)

			w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
				`{"kind":"implement","payload":{"action":"declined","reason":"main already carries this change"}}`)
			require.Equal(t, http.StatusServiceUnavailable, w.Code, w.Body.String())
			require.Equal(t, "10", w.Header().Get("Retry-After"))

			// The block loses NOTHING and commits NOTHING.
			got := e.task(t, "t1")
			require.Len(t, got.Status.Notes, 45)
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
