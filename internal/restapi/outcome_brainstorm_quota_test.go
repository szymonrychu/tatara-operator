package restapi_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// brainstormProposeBodyN builds a submit_outcome propose payload with n
// proposals whose titles are "p0".."p(n-1)", so truncation order is checkable.
func brainstormProposeBodyN(n int) string {
	items := make([]string, 0, n)
	for i := 0; i < n; i++ {
		items = append(items, fmt.Sprintf(
			`{"repo":"tatara-operator","title":"p%d","body":"grounded in r1/main.go:1","kind":"improvement"}`, i))
	}
	return `{"kind":"brainstorm","payload":{"action":"propose","proposals":[` + strings.Join(items, ",") + `]}}`
}

// clarifyTaskCount counts clarify Tasks in the namespace: one per KEPT
// proposal is the invariant a truncating handler must preserve.
func clarifyTaskCount(t *testing.T, e *v2Env) int {
	t.Helper()
	var tasks tatarav1alpha1.TaskList
	require.NoError(t, e.c.List(context.Background(), &tasks, client.InNamespace(ns)))
	n := 0
	for i := range tasks.Items {
		if tasks.Items[i].Spec.Kind == "clarify" {
			n++
		}
	}
	return n
}

// The operator is the AUTHORITY on the quota: an agent that files more than K
// gets exactly K issues opened, not a 400 and not K+n.
func TestBrainstormProposeTruncatesToQuota(t *testing.T) {
	task := taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StageBrainstorming, "brainstorm")
	task.Annotations = map[string]string{tatarav1alpha1.AnnBrainstormQuota: "2"}
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"), task)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", brainstormProposeBodyN(4))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	require.Len(t, e.forge.createdReqs, 2, "the operator truncates to the session quota")
	require.Equal(t, "p0", e.forge.createdReqs[0].Title, "truncation keeps payload order")
	require.Equal(t, "p1", e.forge.createdReqs[1].Title)
	require.Equal(t, 2, clarifyTaskCount(t, e), "one clarify Task per KEPT proposal, never per submitted one")
}

func TestBrainstormProposeBelowQuotaIsUntouched(t *testing.T) {
	task := taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StageBrainstorming, "brainstorm")
	task.Annotations = map[string]string{tatarav1alpha1.AnnBrainstormQuota: "3"}
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"), task)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", brainstormProposeBodyN(2))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Len(t, e.forge.createdReqs, 2)
}

// Every filed proposal carries the configured brainstorming label. The label is
// INFORMATIONAL: nothing reads it for control flow any more (counting is
// Spec.ProposalKind on the CR; approval has been comment-only since C.6).
func TestBrainstormProposeAppliesTheBrainstormingLabel(t *testing.T) {
	tests := []struct {
		name      string
		scmPatch  func(*tatarav1alpha1.ScmSpec)
		wantLabel string
	}{
		{"default label", nil, "tatara-brainstorming"},
		{"project override", func(s *tatarav1alpha1.ScmSpec) { s.BrainstormingLabel = "ideas" }, "ideas"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			proj := projectV2("tatara")
			if tc.scmPatch != nil {
				tc.scmPatch(proj.Spec.Scm)
			}
			task := taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StageBrainstorming, "brainstorm")
			task.Annotations = map[string]string{tatarav1alpha1.AnnBrainstormQuota: "1"}
			e := buildV2(t, v2Opts{}, proj, scmSecretV2(), repoV2("tatara-operator", "tatara"), task)

			w := e.do(t, http.MethodPost, "/tasks/t1/outcome", brainstormProposeBodyN(1))
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.Len(t, e.forge.createdReqs, 1)
			require.Equal(t, []string{tc.wantLabel}, e.forge.createdReqs[0].Labels)
		})
	}
}

// action=skip increments the breaker; action=propose resets it. The breaker
// must NOT move on anything other than these two outcomes - in particular not
// on a bare reconcile pass, which would trip it within a minute of a healthy
// at-target project.
func TestBrainstormSkipCountsTowardTheBreaker(t *testing.T) {
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"))

	for i := 1; i <= 3; i++ {
		name := fmt.Sprintf("skip%d", i)
		task := taskV2(name, "tatara", "brainstorm", tatarav1alpha1.StageBrainstorming, "brainstorm")
		require.NoError(t, e.c.Create(context.Background(), task))

		w := e.do(t, http.MethodPost, "/tasks/"+name+"/outcome",
			`{"kind":"brainstorm","payload":{"action":"skip","reason":"nothing novel"}}`)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		require.Equal(t, i, e.project(t, "tatara").Status.BrainstormConsecutiveSkips)
	}

	task := taskV2("propose1", "tatara", "brainstorm", tatarav1alpha1.StageBrainstorming, "brainstorm")
	task.Annotations = map[string]string{tatarav1alpha1.AnnBrainstormQuota: "1"}
	require.NoError(t, e.c.Create(context.Background(), task))

	w := e.do(t, http.MethodPost, "/tasks/propose1/outcome", brainstormProposeBodyN(1))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Equal(t, 0, e.project(t, "tatara").Status.BrainstormConsecutiveSkips,
		"a productive session resets the breaker")
}

// TestBrainstormBreakerTripCountsCrossingsOnlyNotTrippedState is the O10
// semantics guard: operator_brainstorm_breaker_trip_total must count TRIPS
// (BrainstormConsecutiveSkips crossing its configured threshold), never "the
// breaker is currently tripped". bumpBrainstormSkips is the only place the
// counter is ever incremented, so it is the only place a genuine crossing can
// be told apart from a value that has not moved since the last pass - the
// reconcile loop re-evaluates the same already-tripped state on every
// event-triggered pass and cannot make that distinction on its own.
func TestBrainstormBreakerTripCountsCrossingsOnlyNotTrippedState(t *testing.T) {
	two := 2
	proj := projectV2("tatara")
	proj.Spec.Scm.Cron = &tatarav1alpha1.ScmCron{
		Brainstorm: tatarav1alpha1.BrainstormActivity{MaxConsecutiveSkips: &two},
	}
	metrics := obs.NewOperatorMetrics(prometheus.NewRegistry())
	e := buildV2(t, v2Opts{metrics: metrics}, proj, scmSecretV2(), repoV2("tatara-operator", "tatara"))

	skip := func(name string) {
		task := taskV2(name, "tatara", "brainstorm", tatarav1alpha1.StageBrainstorming, "brainstorm")
		require.NoError(t, e.c.Create(context.Background(), task))
		w := e.do(t, http.MethodPost, "/tasks/"+name+"/outcome",
			`{"kind":"brainstorm","payload":{"action":"skip","reason":"nothing novel"}}`)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	}
	tripped := func() float64 { return testutil.ToFloat64(metrics.BrainstormBreakerTripCounter("tatara")) }

	skip("skip1")
	require.Equal(t, 1, e.project(t, "tatara").Status.BrainstormConsecutiveSkips)
	require.Equal(t, 0.0, tripped(), "one skip below the threshold of 2 must not trip the breaker")

	skip("skip2")
	require.Equal(t, 2, e.project(t, "tatara").Status.BrainstormConsecutiveSkips)
	require.Equal(t, 1.0, tripped(), "the second skip crosses the threshold: exactly one trip")

	skip("skip3")
	require.Equal(t, 3, e.project(t, "tatara").Status.BrainstormConsecutiveSkips,
		"the cron path ignores the breaker, so a session can still run and skip past the threshold")
	require.Equal(t, 1.0, tripped(), "still tripped, not a NEW trip: the counter must not move again")

	task := taskV2("propose1", "tatara", "brainstorm", tatarav1alpha1.StageBrainstorming, "brainstorm")
	task.Annotations = map[string]string{tatarav1alpha1.AnnBrainstormQuota: "1"}
	require.NoError(t, e.c.Create(context.Background(), task))
	w := e.do(t, http.MethodPost, "/tasks/propose1/outcome", brainstormProposeBodyN(1))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Equal(t, 0, e.project(t, "tatara").Status.BrainstormConsecutiveSkips)
	require.Equal(t, 1.0, tripped(), "a reset is not itself a trip")

	skip("skip4")
	require.Equal(t, 1.0, tripped(), "one skip after reset, below threshold again: still just the one trip")
	skip("skip5")
	require.Equal(t, 2.0, tripped(), "a fresh dry streak crossing the threshold again is a genuinely NEW trip")
}

// TestBrainstormBreakerNeverTripsWhenDisabled: MaxConsecutiveSkips unset on the
// Project (ResolveMaxConsecutiveSkips returns 0, "disabled" per its doc
// comment) must never fire the trip counter no matter how many skips land.
func TestBrainstormBreakerNeverTripsWhenDisabled(t *testing.T) {
	metrics := obs.NewOperatorMetrics(prometheus.NewRegistry())
	e := buildV2(t, v2Opts{metrics: metrics}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"))

	for i := 1; i <= 5; i++ {
		name := fmt.Sprintf("skip%d", i)
		task := taskV2(name, "tatara", "brainstorm", tatarav1alpha1.StageBrainstorming, "brainstorm")
		require.NoError(t, e.c.Create(context.Background(), task))
		w := e.do(t, http.MethodPost, "/tasks/"+name+"/outcome",
			`{"kind":"brainstorm","payload":{"action":"skip","reason":"nothing novel"}}`)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	}
	require.Equal(t, 0.0, testutil.ToFloat64(metrics.BrainstormBreakerTripCounter("tatara")))
}
