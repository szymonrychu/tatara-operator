package restapi_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// exhausted is the THIRD brainstorm outcome action: "nothing worth proposing
// until the project moves". One is enough - no threshold, no counting - because
// four automatic resume triggers plus a manual annotation bound the cost of a
// wrong call to "until the project next moves".
func TestBrainstormExhaustedStampsThePause(t *testing.T) {
	task := taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StageBrainstorming, "brainstorm")
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"), task)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"brainstorm","payload":{"action":"exhausted","reason":"every open lane is blocked on a human decision"}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	proj := e.project(t, "tatara")
	require.NotNil(t, proj.Status.BrainstormPausedAt, "action=exhausted must stamp the pause")
	require.Equal(t, "every open lane is blocked on a human decision", proj.Status.BrainstormPauseReason,
		"the agent's reason is stored VERBATIM and never parsed")
}

// A skip is the agent correctly reporting "nothing THIS cycle". It is
// transient, counts toward nothing, and stamps no state: a skip no longer has
// any scheduling consequence at all.
func TestBrainstormSkipStampsNoPause(t *testing.T) {
	task := taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StageBrainstorming, "brainstorm")
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"), task)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"brainstorm","payload":{"action":"skip","reason":"nothing novel"}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	proj := e.project(t, "tatara")
	require.Nil(t, proj.Status.BrainstormPausedAt, "a skip must never pause the project")
	require.Empty(t, proj.Status.BrainstormPauseReason)
}

// exhausted is validated exactly like skip: a non-empty reason is required.
func TestBrainstormExhaustedRequiresAReason(t *testing.T) {
	task := taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StageBrainstorming, "brainstorm")
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"), task)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"brainstorm","payload":{"action":"exhausted","reason":"   "}}`)
	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	require.Nil(t, e.project(t, "tatara").Status.BrainstormPausedAt)
}

func TestBrainstormRejectsAnUnknownAction(t *testing.T) {
	task := taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StageBrainstorming, "brainstorm")
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"), task)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"brainstorm","payload":{"action":"pause","reason":"r"}}`)
	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "propose, skip, exhausted")
}

// A paused project that produces proposals again is UNPAUSED by the propose
// path itself: the agent just proved the idea space is not exhausted, and
// waiting for one of the five external triggers would be strictly worse.
func TestBrainstormProposeClearsAnExistingPause(t *testing.T) {
	proj := projectV2("tatara")
	task := taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StageBrainstorming, "brainstorm")
	task.Annotations = map[string]string{tatarav1alpha1.AnnBrainstormQuota: "1"}
	e := buildV2(t, v2Opts{}, proj, scmSecretV2(), repoV2("tatara-operator", "tatara"), task)

	p := e.project(t, "tatara")
	now := metav1.Now()
	p.Status.BrainstormPausedAt = &now
	p.Status.BrainstormPauseReason = "dry"
	require.NoError(t, e.c.Status().Update(context.Background(), p))
	require.NotNil(t, e.project(t, "tatara").Status.BrainstormPausedAt)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome", brainstormProposeBodyN(1))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Nil(t, e.project(t, "tatara").Status.BrainstormPausedAt,
		"a productive session clears the pause")
	require.Empty(t, e.project(t, "tatara").Status.BrainstormPauseReason)
}
