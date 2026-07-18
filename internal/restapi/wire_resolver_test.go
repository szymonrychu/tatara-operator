package restapi_test

// Issue #345: cmd/manager/wire.go built the REST API server with Spiller,
// CIFor, and Memory left nil, asymmetric with the webhook server and
// TaskReconciler. The fix wires restapi.Config.SpillerFor / MemoryFor as
// PER-PROJECT resolvers (mirroring webhook's newSpillerFor), because each
// Project runs its own tatara-memory endpoint - a single flat instance would
// silently spill/rehydrate the wrong project's data. These tests prove the
// resolver is invoked with the correct Project on every call site the fix
// touches, not just that SOME spiller/memory client is wired.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/restapi"
)

// bigNote returns a Note whose marshalled size is roughly n bytes, well past
// postNote's noteBodyMaxBytes truncation (which only applies to a body
// submitted through the HTTP endpoint, not one seeded directly on the CR).
func bigNote(at time.Time, prefix string, n int) tatarav1alpha1.Note {
	body := strings.Repeat(prefix+"x", n/len(prefix+"x")+1)[:n]
	return tatarav1alpha1.Note{At: metav1.NewTime(at), Agent: "implement", Kind: "note", Body: body}
}

// TestPostNote_SpillerFor_PerProjectEvictionNoPanic proves fault (1) from
// issue #345 is fixed: objbudget.FitTask's internal byte-budget eviction
// (objbudget.go evictOldest -> sp.Spill) must not panic on a nil Spiller,
// and - the reason SpillerFor must be a per-project resolver, not a flat
// field - each Project's eviction batch must land on THAT Project's
// spiller, never a sibling Project's.
func TestPostNote_SpillerFor_PerProjectEvictionNoPanic(t *testing.T) {
	cases := []struct {
		name    string
		project string
	}{
		{"project alpha", "alpha"},
		{"project beta", "beta"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spillers := map[string]*fakeSpiller{
				"alpha": {},
				"beta":  {},
			}
			spillerFor := func(proj *tatarav1alpha1.Project) objbudget.Spiller {
				return spillers[proj.Name]
			}

			task := taskV2("t1", tc.project, "implement", tatarav1alpha1.StageImplementing, "implement")
			// 45 notes * ~20KB = ~900KB, already over the 800,000-byte
			// ObjectByteBudget before the handler's own note is appended, and
			// well under the 50-note cap so postNote's pre-spill gate never
			// fires - the ONLY path that can trigger FitTask's internal
			// eviction (and thus sp.Spill on a possibly-nil Spiller) is
			// objbudget.go's own evictOldest loop.
			for i := 0; i < 45; i++ {
				task.Status.Notes = append(task.Status.Notes,
					bigNote(frozenNow.Add(time.Duration(i)*time.Minute), fmt.Sprintf("note-%02d-", i), 20_000))
			}

			e := buildV2(t, v2Opts{spillerFor: spillerFor},
				projectV2(tc.project), scmSecretV2(), repoV2("tatara-operator", tc.project), task)

			require.NotPanics(t, func() {
				w := e.do(t, http.MethodPost, "/tasks/t1/notes", `{"kind":"note","body":"tip over budget"}`)
				require.Equal(t, http.StatusCreated, w.Code, "eviction must succeed, not 500")
			})

			require.NotEmpty(t, spillers[tc.project].batches,
				"the task's own project spiller must have received the evicted batch")
			for name, sp := range spillers {
				if name == tc.project {
					continue
				}
				require.Empty(t, sp.batches, "a sibling project's spiller must never receive another project's eviction")
			}
		})
	}
}

// TestTaskContext_MemoryFor_PerProjectRehydrate proves fault (3) from issue
// #345 is fixed: notes=all rehydration reads through the CORRECT project's
// tatara-memory client, never a sibling project's.
func TestTaskContext_MemoryFor_PerProjectRehydrate(t *testing.T) {
	cases := []struct {
		name    string
		project string
	}{
		{"project alpha", "alpha"},
		{"project beta", "beta"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			memories := map[string]*fakeMemory{
				"alpha": {byTrack: map[string]json.RawMessage{}},
				"beta":  {byTrack: map[string]json.RawMessage{}},
			}
			memoryFor := func(proj *tatarav1alpha1.Project) restapi.NoteFetcher {
				return memories[proj.Name]
			}

			spilled, err := json.Marshal([]tatarav1alpha1.Note{
				{At: metav1.NewTime(frozenNow.Add(-3 * time.Hour)), Agent: "implement", Kind: "note",
					Body: "SPILLED-IN-" + tc.project},
			})
			require.NoError(t, err)
			memories[tc.project].byTrack["track-1"] = spilled

			task := taskV2("t1", tc.project, "clarify", tatarav1alpha1.StageImplementing, "implement")
			task.Status.Notes = []tatarav1alpha1.Note{
				{At: metav1.NewTime(frozenNow), Agent: "implement", Kind: "handoff", Body: "LIVE-NOTE"},
			}
			task.Status.Stats.NotesSpilled = 1
			task.Status.Stats.NotesSpilledRefs = []string{"track-1"}

			e := buildV2(t, v2Opts{memoryFor: memoryFor},
				projectV2(tc.project), scmSecretV2(), repoV2("tatara-operator", tc.project), task)

			w := e.do(t, http.MethodGet, "/tasks/t1/context?notes=all", "")
			require.Equal(t, http.StatusOK, w.Code)
			require.Contains(t, w.Body.String(), "SPILLED-IN-"+tc.project,
				"notes=all must rehydrate through THIS project's memory client")

			for name := range memories {
				if name == tc.project {
					continue
				}
				require.NotContains(t, w.Body.String(), "SPILLED-IN-"+name,
					"a sibling project's spilled notes must never leak into this task's bundle")
			}
		})
	}
}
