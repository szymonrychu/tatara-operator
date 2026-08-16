package webhook_test

// A QUEUED ADOPTION IS A POINTER TO A LIVE FORGE OBJECT, which is exactly why
// queueing was once rejected outright (MEMORY.md 2026-08-16). These two handlers
// are the answer: Renovate force-pushes each successive bump onto the same
// branch, keeping the same number and the same merge request, and a merge
// request can merge or close entirely while its event waits behind a full pool.

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// prSynchronized renders a pull_request.synchronize delivery for number,
// carrying a NEW head sha/title/body - the shape Renovate force-pushes onto
// its own branch each time it re-bumps the same dependency.
func prSynchronized(login, headBranch string, number int, sha, title, body string) []byte {
	n := strconv.Itoa(number)
	return []byte(`{"action":"synchronize","pull_request":{"number":` + n +
		`,"title":"` + title + `","body":"` + body + `"` +
		`,"user":{"login":"` + login + `"},` +
		`"head":{"sha":"` + sha + `","ref":"` + headBranch + `","repo":{"full_name":"o/r"}},` +
		`"html_url":"https://github.com/o/r/pull/` + n + `"},` +
		`"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},` +
		`"sender":{"login":"` + login + `"}}`)
}

// prClosed renders a pull_request.closed delivery for number, merged when
// merged is true (a happy-path adoption racing the queue) and a plain close
// otherwise (a human or the engine withdrawing the proposal).
func prClosed(login string, number int, merged bool) []byte {
	n := strconv.Itoa(number)
	return []byte(`{"action":"closed","pull_request":{"number":` + n +
		`,"merged":` + strconv.FormatBool(merged) +
		`,"head":{"sha":"sha-` + n + `","ref":"renovate/cilium"},` +
		`"html_url":"https://github.com/o/r/pull/` + n + `"},` +
		`"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},` +
		`"sender":{"login":"` + login + `"}}`)
}

// postPRSynchronize signs and delivers a pull_request synchronize webhook,
// asserting a 202 - postPROpened's sibling for the freshness handlers.
func postPRSynchronize(t *testing.T, h http.Handler, projName, secretVal string, body []byte) {
	t.Helper()
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "pull_request")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))
	w := post(t, h, projName, hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)
}

// postPRClosed is postPRSynchronize's closed-delivery twin.
func postPRClosed(t *testing.T, h http.Handler, projName, secretVal string, body []byte) {
	t.Helper()
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "pull_request")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))
	w := post(t, h, projName, hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)
}

// admitAdoptionEvent flips a still-Queued adoption event to Admitted, as the
// dispatcher would the instant a pool slot frees - so a test can assert the
// freshness handlers leave spent work alone.
func admitAdoptionEvent(t *testing.T, c client.Client, qe *tatarav1.QueuedEvent) {
	t.Helper()
	qe.Status.State = tatarav1.QueueStateAdmitted
	qe.Status.TaskRef = "some-task"
	require.NoError(t, c.Status().Update(context.Background(), qe))
}

// SYNCHRONIZE REFRESHES A STILL-QUEUED SNAPSHOT. Without it the dispatcher mints
// against a head SHA the engine has already replaced, and AdoptUpgradeMR clause
// (g) - which binds a refusal marker to a head SHA - rules on the wrong tree.
func TestMRSynchronize_RefreshesAStillQueuedAdoption(t *testing.T) {
	const secretVal = "whsec-fresh1"
	c := seedClient(t,
		adoptionProject("fr1", "fr1-scm", "tatara-bot", "renovate/", 2, nil),
		secret("fr1-scm", secretVal),
		repository("charts", "fr1", baseRepoURL, "main"),
	)
	h, _ := newServer(t, c)
	postPROpened(t, h, "fr1", secretVal, prOpenedOnBranch("tatara-bot", "renovate/cilium", 77))
	adoptionEvent(t, c, "fr1", "charts", 77) // sanity: sha-77 queued

	postPRSynchronize(t, h, "fr1", secretVal,
		prSynchronized("tatara-bot", "renovate/cilium", 77, "sha-78", "chore(deps): bump to v1.18", "new release notes"))

	qe := adoptionEvent(t, c, "fr1", "charts", 77)
	require.Equal(t, "sha-78", qe.Spec.Payload.AdoptedUpgrade.HeadSHA)
	require.Equal(t, "chore(deps): bump to v1.18", qe.Spec.Payload.AdoptedUpgrade.Title)
	require.Equal(t, "new release notes", qe.Spec.Payload.AdoptedUpgrade.Body)
	require.Equal(t, tatarav1.QueueStateQueued, qe.Status.State)
}

// AN ALREADY-ADMITTED EVENT IS LEFT ALONE. Its Task exists, its mirror exists,
// and MergeRequest.status.headSHA is the authority from that point on
// (stampMRHead, which this same handler already drives). Rewriting the spent
// event's payload would be a write with no reader.
func TestMRSynchronize_LeavesAnAdmittedAdoptionAlone(t *testing.T) {
	const secretVal = "whsec-fresh2"
	c := seedClient(t,
		adoptionProject("fr2", "fr2-scm", "tatara-bot", "renovate/", 2, nil),
		secret("fr2-scm", secretVal),
		repository("charts", "fr2", baseRepoURL, "main"),
	)
	h, _ := newServer(t, c)
	postPROpened(t, h, "fr2", secretVal, prOpenedOnBranch("tatara-bot", "renovate/cilium", 78))
	qe := adoptionEvent(t, c, "fr2", "charts", 78)
	admitAdoptionEvent(t, c, qe)

	postPRSynchronize(t, h, "fr2", secretVal,
		prSynchronized("tatara-bot", "renovate/cilium", 78, "sha-79", "new title", "new body"))

	var got tatarav1.QueuedEvent
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: qe.Name}, &got))
	require.Equal(t, "sha-78", got.Spec.Payload.AdoptedUpgrade.HeadSHA, "an admitted event's snapshot must not be rewritten")
}

// A CONFLICTING CONCURRENT WRITE MUST RETRY, NOT LOSE THE NEWER HEAD SHA.
// Production runs 3 webhook replicas with no leader election on this path, so
// two near-simultaneous synchronize deliveries for the SAME merge request -
// exactly "Renovate force-pushes each successive bump" - can both reach
// refreshQueuedAdoption's Update with a stale resourceVersion. A single
// Get-then-Update drops the loser's write silently; retry.RetryOnConflict must
// re-Get and re-apply it instead.
func TestMRSynchronize_ConflictingUpdateRetriesAndWins(t *testing.T) {
	const secretVal = "whsec-fresh7"
	var attempts int32
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithObjects(
			adoptionProject("fr7", "fr7-scm", "tatara-bot", "renovate/", 2, nil),
			secret("fr7-scm", secretVal),
			repository("charts", "fr7", baseRepoURL, "main"),
		).
		WithStatusSubresource(&tatarav1.Project{}, &tatarav1.Repository{}, &tatarav1.Task{}, &tatarav1.QueuedEvent{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if _, isQE := obj.(*tatarav1.QueuedEvent); isQE {
					if atomic.AddInt32(&attempts, 1) == 1 {
						// The FIRST Update attempt loses the race: simulate the second
						// replica's write having already landed and bumped the
						// resourceVersion this attempt was read against.
						return apierrors.NewConflict(
							schema.GroupResource{Group: "tatara.dev", Resource: "queuedevents"},
							obj.GetName(), errors.New("injected conflict"))
					}
				}
				return cl.Update(ctx, obj, opts...)
			},
		}).Build()
	h, _ := newServer(t, c)
	postPROpened(t, h, "fr7", secretVal, prOpenedOnBranch("tatara-bot", "renovate/cilium", 82))

	postPRSynchronize(t, h, "fr7", secretVal,
		prSynchronized("tatara-bot", "renovate/cilium", 82, "sha-conflict", "new title", "new body"))

	qe := adoptionEvent(t, c, "fr7", "charts", 82)
	require.Equal(t, "sha-conflict", qe.Spec.Payload.AdoptedUpgrade.HeadSHA,
		"the retry must re-Get and win after the first attempt's conflict, not drop the newer head")
	require.GreaterOrEqual(t, atomic.LoadInt32(&attempts), int32(2), "one conflict, then a successful retry")
}

// A CONCURRENT DROP DURING A REFRESH IS BENIGN, NOT AN ERROR. A synchronize
// delivery immediately followed by a close/merge - on the same replica or a
// different one - can have dropQueuedAdoption delete the event between
// refreshQueuedAdoption's Get and its Update. The Update then 404s; that must
// not surface as a failure or recreate the event.
func TestMRSynchronize_ConcurrentDropDuringRefreshIsBenign(t *testing.T) {
	const secretVal = "whsec-fresh8"
	var updates int32
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithObjects(
			adoptionProject("fr8", "fr8-scm", "tatara-bot", "renovate/", 2, nil),
			secret("fr8-scm", secretVal),
			repository("charts", "fr8", baseRepoURL, "main"),
		).
		WithStatusSubresource(&tatarav1.Project{}, &tatarav1.Repository{}, &tatarav1.Task{}, &tatarav1.QueuedEvent{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				qe, isQE := obj.(*tatarav1.QueuedEvent)
				if isQE && atomic.AddInt32(&updates, 1) == 1 {
					// Simulate a concurrent handleMRClosed deleting the event between
					// refreshQueuedAdoption's Get and this Update.
					if derr := cl.Delete(ctx, qe.DeepCopy()); derr != nil {
						return derr
					}
				}
				return cl.Update(ctx, obj, opts...)
			},
		}).Build()
	h, _ := newServer(t, c)
	postPROpened(t, h, "fr8", secretVal, prOpenedOnBranch("tatara-bot", "renovate/cilium", 83))

	// Must not error, panic, or recreate the event - postPRSynchronize itself
	// already asserts the response is a 202.
	postPRSynchronize(t, h, "fr8", secretVal,
		prSynchronized("tatara-bot", "renovate/cilium", 83, "sha-race", "t", "b"))

	var got tatarav1.QueuedEvent
	err := c.Get(context.Background(), types.NamespacedName{
		Namespace: ns, Name: queue.QueuedEventName("fr8", queue.AdoptUpgradeDedupKey("charts", 83)),
	}, &got)
	require.Error(t, err, "the concurrent close deleted the event; refreshQueuedAdoption must not recreate it")
}

// A MERGE DELETES THE QUEUED EVENT. Admitting it would burn an agent pod on a
// merge request that no longer exists to review, and nothing downstream would
// notice until the review pod read a merged MR.
func TestMRClosed_DeletesAStillQueuedAdoption(t *testing.T) {
	const secretVal = "whsec-fresh3"
	c := seedClient(t,
		adoptionProject("fr3", "fr3-scm", "tatara-bot", "renovate/", 2, nil),
		secret("fr3-scm", secretVal),
		repository("charts", "fr3", baseRepoURL, "main"),
	)
	h, _ := newServer(t, c)
	postPROpened(t, h, "fr3", secretVal, prOpenedOnBranch("tatara-bot", "renovate/cilium", 79))
	qe := adoptionEvent(t, c, "fr3", "charts", 79)
	before := testutil.ToFloat64(obs.AdoptionEventDroppedTotal.WithLabelValues("fr3", "merged"))

	postPRClosed(t, h, "fr3", secretVal, prClosed("tatara-bot", 79, true))

	var got tatarav1.QueuedEvent
	err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: qe.Name}, &got)
	require.Error(t, err, "a merged still-queued adoption must be deleted")
	require.Equal(t, before+1, testutil.ToFloat64(obs.AdoptionEventDroppedTotal.WithLabelValues("fr3", "merged")))
}

// A PLAIN CLOSE DELETES IT TOO, under its own reason so the two are separable
// on a dashboard: a merged bump is the happy path racing the queue, a closed one
// is a human or the engine withdrawing the proposal.
func TestMRClosed_DeletesAStillQueuedAdoptionOnAPlainClose(t *testing.T) {
	const secretVal = "whsec-fresh4"
	c := seedClient(t,
		adoptionProject("fr4", "fr4-scm", "tatara-bot", "renovate/", 2, nil),
		secret("fr4-scm", secretVal),
		repository("charts", "fr4", baseRepoURL, "main"),
	)
	h, _ := newServer(t, c)
	postPROpened(t, h, "fr4", secretVal, prOpenedOnBranch("tatara-bot", "renovate/cilium", 80))
	qe := adoptionEvent(t, c, "fr4", "charts", 80)
	before := testutil.ToFloat64(obs.AdoptionEventDroppedTotal.WithLabelValues("fr4", "closed"))

	postPRClosed(t, h, "fr4", secretVal, prClosed("tatara-bot", 80, false))

	var got tatarav1.QueuedEvent
	err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: qe.Name}, &got)
	require.Error(t, err, "a closed still-queued adoption must be deleted")
	require.Equal(t, before+1, testutil.ToFloat64(obs.AdoptionEventDroppedTotal.WithLabelValues("fr4", "closed")))
}

// AN ADMITTED EVENT IS NOT DELETED. Its Task owns the merge request now and the
// stage machine converges a merged/closed MR on its own (merging already treats
// State=="merged" as done). Deleting the event here would strip the Task's
// LabelQueuedEvent accounting mid-flight.
func TestMRClosed_LeavesAnAdmittedAdoptionAlone(t *testing.T) {
	const secretVal = "whsec-fresh5"
	c := seedClient(t,
		adoptionProject("fr5", "fr5-scm", "tatara-bot", "renovate/", 2, nil),
		secret("fr5-scm", secretVal),
		repository("charts", "fr5", baseRepoURL, "main"),
	)
	h, _ := newServer(t, c)
	postPROpened(t, h, "fr5", secretVal, prOpenedOnBranch("tatara-bot", "renovate/cilium", 81))
	qe := adoptionEvent(t, c, "fr5", "charts", 81)
	admitAdoptionEvent(t, c, qe)

	postPRClosed(t, h, "fr5", secretVal, prClosed("tatara-bot", 81, true))

	var got tatarav1.QueuedEvent
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: qe.Name}, &got),
		"an admitted event must survive a merge/close delivery")
}

// NO QUEUED ADOPTION IS THE NORMAL CASE. Every merge request in the platform
// gets synchronize and closed deliveries; almost none of them has a queued
// adoption. A NotFound must be silent and must not change the response.
func TestMRSynchronizeAndClosed_NoQueuedAdoptionIsSilent(t *testing.T) {
	const secretVal = "whsec-fresh6"
	c := seedClient(t,
		adoptionProject("fr6", "fr6-scm", "tatara-bot", "renovate/", 2, nil),
		secret("fr6-scm", secretVal),
		repository("charts", "fr6", baseRepoURL, "main"),
	)
	h, _ := newServer(t, c)

	postPRSynchronize(t, h, "fr6", secretVal,
		prSynchronized("octocat", "some-branch", 99, "sha-x", "unrelated PR", "body"))
	postPRClosed(t, h, "fr6", secretVal, prClosed("octocat", 99, false))
}
