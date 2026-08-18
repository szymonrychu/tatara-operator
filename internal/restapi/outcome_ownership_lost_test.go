package restapi_test

import (
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// THE 400 THAT RESPAWN-LOOPED mt-i-tatara-operator-622, from the submit side.
//
// The Task's work was pushed and green in two repos. Its merge requests had been
// handed to review Tasks by the ownership flip, so ownedMRs - which reads the
// controller ownerRef, the source of truth - returned nothing, while
// status.mrRefs still listed both. `submit_outcome(action=submitted)` answered
// `400 action=submitted but this task owns no open MR` on three consecutive
// pods, and the wrapper re-prompted every one of them as a mandatory retry.
//
// A 400 says "you can fix this and retry". This one cannot be fixed by the
// agent: it does not own the merge requests any more and no retry will change
// that. The taken-over carve-out already exists for exactly this and its own
// comment says the carve-outs are KIND-AGNOSTIC (#578) - but the predicate
// behind it still refused every kind except review, so an implement Task fell
// straight through to the doomed 400.
func TestOutcome_OwnershipLostImplementSubmitIsNoop(t *testing.T) {
	const reviewTask = "mt-r-tatara-operator-625"

	impl := taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement")
	impl.Status.MRRefs = []string{tatarav1alpha1.MergeRequestName("tatara-operator", 625)}
	impl.Status.Stats.MRCount = 1

	// The mirror moved to the review Task; the implement Task is left a plain
	// (non-controller) owner, exactly as own.HandOverController leaves it.
	mr := mrV2("tatara-operator", 625, reviewTask)
	mr.OwnerReferences = append(mr.OwnerReferences, ownerRef("t1", false))
	mr.Status.Ownership = tatarav1alpha1.OwnershipExternal

	before := testutil.ToFloat64(
		obs.RestOutcomeAcceptedTotal.WithLabelValues("implement", "mr-taken-over-noop"))

	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"), impl,
		&tatarav1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{Name: reviewTask, Namespace: ns},
			Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "tatara", RepositoryRef: "tatara-operator", Kind: "review", Goal: "review"},
			Status:     tatarav1alpha1.TaskStatus{State: tatarav1alpha1.StateAwaitingReview},
		}, mr)

	stateBefore := e.task(t, "t1").Status.State
	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"submitted","title":"t","body":"b","changeSignificance":"patch"}}`)

	require.Equal(t, http.StatusOK, w.Code,
		"an agent that no longer owns its merge requests cannot fix that by retrying: body %s", w.Body.String())
	require.JSONEq(t, `{"noop":true,"reason":"mr-taken-over"}`, w.Body.String())
	require.Equal(t, before+1, testutil.ToFloat64(
		obs.RestOutcomeAcceptedTotal.WithLabelValues("implement", "mr-taken-over-noop")),
		"the no-op is COUNTED as accepted, never as a silent success")

	after := e.task(t, "t1")
	require.Equal(t, stateBefore, after.Status.State,
		"the endpoint does not finalize; the convergent reconciler edge does")
	require.Nil(t, tatarav1alpha1.OutcomeCondition(after),
		"a no-op commits nothing, so it must hold no claim either")
}

// The 400 that must REMAIN: a Task whose refs are still its own owns no open MR
// because it opened none. That is an agent error, a retry after opening one
// genuinely succeeds, and the widened carve-out must not swallow it.
func TestOutcome_NoMRRefsAtAllIsStillA400(t *testing.T) {
	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"),
		taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement"))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"submitted","title":"t","body":"b","changeSignificance":"patch"}}`)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "this task owns no open MR")
}

// A ref that names a MergeRequest CR which no longer EXISTS is not proof of a
// takeover - external deletion must not retire a Task - so that shape keeps the
// 400 the mint/binding repair clears.
func TestOutcome_DanglingMRRefIsStillA400(t *testing.T) {
	impl := taskV2("t1", "tatara", "implement", tatarav1alpha1.StateUnderImplementation, "implement")
	impl.Status.MRRefs = []string{tatarav1alpha1.MergeRequestName("tatara-operator", 625)}
	impl.Status.Stats.MRCount = 1

	e := buildV2(t, v2Opts{writer: panicForge{}}, projectV2("tatara"), scmSecretV2(),
		repoV2("tatara-operator", "tatara"), impl)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"implement","payload":{"action":"submitted","title":"t","body":"b","changeSignificance":"patch"}}`)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "this task owns no open MR")
}
