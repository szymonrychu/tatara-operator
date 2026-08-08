package restapi_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// exhausted is the THIRD brainstorm outcome action: "nothing worth proposing
// until the project moves". One is enough - no threshold, no counting - because
// four automatic resume triggers plus a manual annotation bound the cost of a
// wrong call to "until the project next moves".
func TestBrainstormExhaustedStampsThePause(t *testing.T) {
	task := taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StateRefined, "brainstorm")
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
	task := taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StateRefined, "brainstorm")
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
	task := taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StateRefined, "brainstorm")
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"), task)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"brainstorm","payload":{"action":"exhausted","reason":"   "}}`)
	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	// Assert the BODY, not just the status code: pre-fix code also 400s on
	// action=bad-action (TestBrainstormRejectsAnUnknownAction), so a bare
	// status-code check here passes against code that never validates
	// exhausted's reason at all (review finding I6).
	require.Contains(t, w.Body.String(), "action=exhausted requires a non-empty reason")
	require.Nil(t, e.project(t, "tatara").Status.BrainstormPausedAt)
}

// TestBrainstormExhaustedRefusesThePauseWhenTheProjectMovedDuringTheSession is
// I3's fix round proof. The design spec's intended fail direction is
// "over-resumes rather than under-resumes" (ResumeBrainstormOnPush's own
// comment). Before the fix, StampBrainstormResume early-returned on an
// UNPAUSED project, so a merge/push/maintainer trigger landing WHILE this
// session was in flight was silently discarded, and the session's own
// eventual exhausted verdict paused a project that had already moved - the
// OPPOSITE of the intended direction, wedged until the next qualifying
// event. taskV2 stamps StageEnteredAt to frozenNow-1h; a movement stamped
// AFTER that (here, 30 minutes ago) must refuse the pause.
func TestBrainstormExhaustedRefusesThePauseWhenTheProjectMovedDuringTheSession(t *testing.T) {
	task := taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StateRefined, "brainstorm")
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"), task)

	p := e.project(t, "tatara")
	moved := metav1.NewTime(frozenNow.Add(-30 * time.Minute))
	p.Status.LastMovementAt = &moved
	require.NoError(t, e.c.Status().Update(context.Background(), p))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"brainstorm","payload":{"action":"exhausted","reason":"every open lane is blocked on a human decision"}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	proj := e.project(t, "tatara")
	require.Nil(t, proj.Status.BrainstormPausedAt,
		"a movement newer than this session's own start must refuse the pause")
	require.Empty(t, proj.Status.BrainstormPauseReason)
	// The Task itself must still complete normally: refusing the pause is not
	// refusing the outcome.
	require.Equal(t, tatarav1alpha1.StateDone, e.task(t, "t1").Status.State)
}

// TestBrainstormExhaustedStillPausesWhenMovementPredatesTheSession proves the
// refusal is scoped to genuinely CONCURRENT movement, not any movement ever
// recorded: a stamp from before this session even started is stale evidence
// the session already accounted for, and must not block a real exhausted
// verdict forever.
func TestBrainstormExhaustedStillPausesWhenMovementPredatesTheSession(t *testing.T) {
	task := taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StateRefined, "brainstorm")
	e := buildV2(t, v2Opts{}, projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"), task)

	p := e.project(t, "tatara")
	moved := metav1.NewTime(frozenNow.Add(-2 * time.Hour)) // before StageEnteredAt (frozenNow-1h)
	p.Status.LastMovementAt = &moved
	require.NoError(t, e.c.Status().Update(context.Background(), p))

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"brainstorm","payload":{"action":"exhausted","reason":"every open lane is blocked on a human decision"}}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	proj := e.project(t, "tatara")
	require.NotNil(t, proj.Status.BrainstormPausedAt,
		"movement that predates this session's own start must not block its exhausted verdict")
}

func TestBrainstormRejectsAnUnknownAction(t *testing.T) {
	task := taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StateRefined, "brainstorm")
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
	task := taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StateRefined, "brainstorm")
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

// TestBrainstormExhaustedFailsClosedWhenThePauseStampFails is I1's fix round
// proof. Before the fix, the Task committed to Delivered FIRST and the pause
// stamp ran best-effort AFTER: a failed stamp still returned 200, the Task
// was already terminal so nothing ever retried, and the project fell
// straight back into C2's busy-loop with the one brake that exists for it
// silently lost. The fix reorders the write: stamp the pause BEFORE
// committing the Task, and fail the WHOLE request (500) if the stamp fails,
// so the agent's outcome submission retries and neither half is left
// half-done.
func TestBrainstormExhaustedFailsClosedWhenThePauseStampFails(t *testing.T) {
	metrics := obs.NewOperatorMetrics(prometheus.NewRegistry())
	funcs := interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, subResourceName string,
			obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if _, ok := obj.(*tatarav1alpha1.Project); ok && subResourceName == "status" {
				return fmt.Errorf("injected: project status update failed")
			}
			return c.SubResource(subResourceName).Update(ctx, obj, opts...)
		},
	}
	task := taskV2("t1", "tatara", "brainstorm", tatarav1alpha1.StateRefined, "brainstorm")
	e := buildV2WithCooldownAndInterceptor(t, metrics, 0, func() time.Time { return frozenNow }, funcs,
		projectV2("tatara"), scmSecretV2(), repoV2("tatara-operator", "tatara"), task)

	w := e.do(t, http.MethodPost, "/tasks/t1/outcome",
		`{"kind":"brainstorm","payload":{"action":"exhausted","reason":"every open lane is blocked on a human decision"}}`)
	require.Equal(t, http.StatusInternalServerError, w.Code, w.Body.String(),
		"a failed pause stamp must fail the whole request, not just log-and-continue")
	require.Equal(t, tatarav1alpha1.StateRefined, e.task(t, "t1").Status.State,
		"the Task must stay non-terminal so the agent's retry can land the pause; "+
			"committing to Delivered first is exactly the fail-open bug this test pins")
	require.Nil(t, e.project(t, "tatara").Status.BrainstormPausedAt,
		"the failed write must not leave a half-applied pause")
}
