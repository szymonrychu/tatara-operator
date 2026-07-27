package restapi_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
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
