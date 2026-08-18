package restapi_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
)

// THE upgrade-qe-e4016501fd9107d9 RESPAWN LOOP, AT ITS SOURCE.
//
// A kind=upgrade Task sits at under-implementation. Its one merge request
// merged on the forge. The agent submits the work it did; every owned merge
// request is terminal, so the handler answered a 2xx no-op - which neither
// advances the Task nor parks it. The Task therefore has NO reachable terminal:
// the agent writes its handoff note, the pod comes down, the dispatcher mints
// another, and the whole cycle repeats every ~80 seconds. Measured: 127 pods and
// 119 turns for ONE bot round, 11.1M cache-read tokens, still running at 4h.
//
// The submit IS the completeness declaration, so it is the signal that the Task
// has delivered - and a delivered Task must move. `merged`, not `done`: the work
// SHIPPED but the Task still owes the deploy ledger, the issue closes and
// deliveredAt, all of which live past `merged` (the same argument
// OwnMRsShippedEdge makes for the awaiting-review shape).
func TestOutcome_Implement_EveryOwnedMRMerged_DeliversInsteadOfNoOpping(t *testing.T) {
	mr := mrV2("tatara-operator", 295, "t1")
	mr.Status.State = "merged"
	task := taskV2("t1", "tatara", "upgrade", tatarav1alpha1.StateUnderImplementation, "upgrade")
	// The live agent pod that submitted this outcome. `merged` runs no agent, so
	// nothing downstream would ever collect it: see deliverShippedMRs.
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: agent.PodName(task), Namespace: ns}}

	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"), task, mr, pod)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"upgrade","payload":{"action":"submitted","title":"t","body":"b","changeSignificance":"patch"}}`)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), `"noop":true`,
		"a no-op leaves the Task in a live state with no legal exit; that IS the loop")

	after := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.StateMerged, after.Status.State)
	require.Empty(t, after.Status.ParkReason)
	require.Equal(t, []string{"tatara-operator"}, after.Spec.MergeOrder,
		"ReconcileMerging parks merge-order-missing (UnparkNever) on an empty order, and a "+
			"cron-minted upgrade Task never had one: the order has to be backfilled from the merged MRs")
	require.NotNil(t, tatarav1alpha1.OutcomeCondition(after),
		"this commits a transition, so it holds the claim like any other committed outcome")

	// `merged` runs no agent, so nothing downstream ever reaches ensureStagePod's
	// stale-pod check: the pod that submitted this outcome has to come down HERE
	// or it holds an admission slot with no clock armed for the life of the Task.
	err := e.c.Get(context.Background(),
		types.NamespacedName{Namespace: ns, Name: agent.PodName(task)}, &corev1.Pod{})
	require.True(t, apierrors.IsNotFound(err), "the agent pod must be torn down with the delivery, got %v", err)
}

// THE OTHER HALF OF THE SAME PREDICATE, AND IT MUST STAY A NO-OP. An owned merge
// request that was CLOSED unmerged is not a delivery, so nothing here may enter
// `merged`: AllMRsMerged is false and the handler falls back to the pre-existing
// terminal no-op rather than inventing a disposition for abandoned work.
func TestOutcome_Implement_OwnedMRClosedUnmerged_StaysANoOp(t *testing.T) {
	mr := mrV2("tatara-operator", 295, "t1")
	mr.Status.State = "closed"

	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "upgrade", tatarav1alpha1.StateUnderImplementation, "upgrade"), mr)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"upgrade","payload":{"action":"submitted","title":"t","body":"b","changeSignificance":"patch"}}`)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), `"reason":"mr-terminal"`)
	after := e.task(t, "t1")
	require.Equal(t, tatarav1alpha1.StateUnderImplementation, after.Status.State)
}

// A MIXED SET IS NOT A DELIVERY EITHER, and this is the case that decides where
// the finalize may live. One merged merge request and one still open means the
// Task has work in flight; AllMRsTerminal is false, so it does not even reach
// the terminal branch and the ordinary submission path runs.
//
// It is also the reason no level-triggered reconciler sweep was added at
// under-implementation: a multi-repo implement Task whose FIRST merge request a
// human merged early has AllMRsTerminal true over the MRs it has opened SO FAR,
// while still owing the next one. Only the agent's own submit says "that was
// everything".
func TestOutcome_Implement_OneMergedOneOpen_TakesTheOrdinaryPath(t *testing.T) {
	merged := mrV2("tatara-operator", 295, "t1")
	merged.Status.State = "merged"
	open := mrV2("tatara-cli", 12, "t1") // open

	e := buildV2(t, v2Opts{writer: &reviewPanicForge{heads: map[int]string{12: "sha1"}}},
		projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"), repoV2("tatara-cli", "tatara"),
		taskV2("t1", "tatara", "upgrade", tatarav1alpha1.StateUnderImplementation, "upgrade"),
		merged, open)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"upgrade","payload":{"action":"submitted","title":"t","body":"b","changeSignificance":"patch"}}`)

	require.NotContains(t, w.Body.String(), `"reason":"mr-terminal"`,
		"an open merge request is never the terminal branch")
	require.NotEqual(t, tatarav1alpha1.StateMerged, e.task(t, "t1").Status.State,
		"a Task with work still open must never skip review")
}
