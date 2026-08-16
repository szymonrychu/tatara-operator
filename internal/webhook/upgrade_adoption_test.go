package webhook_test

// THE WEBHOOK FAST PATH FOR DEPENDENCY-UPGRADE ADOPTION.
//
// Adoption shipped in v2.16.0 driven by the SWEEP ALONE, so an adoptable merge
// request waited up to a full issueScan period (0 */4 * * * in production) for
// its Task. The merge-request-open webhook was delivered and answered 202 the
// whole time: handleMROpened's first line dropped every bot-authored delivery,
// and the engine now authors AS the bot.
//
// THE WEBHOOK DOES NOT MINT, AND THAT HALF OF THE DESIGN IS UNCHANGED. The
// webhook server runs on EVERY replica (HandlerRunnable.NeedLeaderElection() ==
// false) and a burst of engine merge requests load-balances across all of them,
// so any check-then-mint in this handler is a distributed race no in-process
// lock can close. What changed is WHAT the handler leaves behind: it used to
// stamp a one-shot SweepRequestedAnnotation that pulled the repo's sweep slot
// forward, and the pass it pulled forward computed one headroom for the whole
// pass, adopted up to that many merge requests, and CLEARED the marker of every
// one it skipped - so the third merge request of a three-MR Renovate run sat
// unadopted for four hours with both siblings already merged. Now it ENQUEUES a
// durable QueuedEvent, and the leader-elected dispatcher admits it the moment a
// slot frees. These tests pin that split, and pin that arming it changed nothing
// for any other author.

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/controller"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// adoptionProject is projectWithReporters ARMED for adoption exactly the way
// project-infrastructure is in production: the dependency engine authors as the
// project's own botLogin, under a configured adoptBranchPrefix, with the upgrade
// cron's maxOpenUpgrades as the lane cap.
func adoptionProject(name, secretRef, bot, prefix string, maxOpen int, engines []string) *tatarav1.Project {
	p := projectWithReporters(name, secretRef, "tatara", bot, nil)
	p.Spec.UpgradePolicy = &tatarav1.UpgradePolicySpec{
		Engine: "renovate", MajorStrategy: "nextHopOnly",
		AdoptBranchPrefix: prefix, UpgradeEngineLogins: engines,
	}
	p.Spec.Scm.Cron = &tatarav1.ScmCron{
		Upgrade: tatarav1.UpgradeActivity{Schedule: "0 */4 * * *", MaxOpenUpgrades: maxOpen},
	}
	return p
}

// prOpenedOnBranch renders a pull_request.opened delivery authored by login on
// headBranch, opened from a branch of the BASE repository - the only shape a
// dependency engine ever opens.
func prOpenedOnBranch(login, headBranch string, number int) []byte {
	return prOpenedFromRepo(login, headBranch, "o/r", number)
}

// prOpenedFromRepo is prOpenedOnBranch with head.repo.full_name spelled out, so
// a test can render the FORK shape (a head repo that is not the base repo) that
// AdoptUpgradeMR clause (d) exists to refuse.
func prOpenedFromRepo(login, headBranch, headRepo string, number int) []byte {
	n := strconv.Itoa(number)
	return []byte(`{"action":"opened","pull_request":{"number":` + n +
		`,"title":"chore(deps): update cilium to v1.17.` + n + `"` +
		`,"body":"### Release Notes","user":{"login":"` + login + `"},` +
		`"head":{"sha":"sha-` + n + `","ref":"` + headBranch + `","repo":{"full_name":"` + headRepo + `"}},` +
		`"html_url":"https://github.com/o/r/pull/` + n + `"},` +
		`"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},` +
		`"sender":{"login":"` + login + `"}}`)
}

const baseRepoURL = "https://github.com/o/r.git"

// adoptionEvent reads the QueuedEvent an adoptable delivery is expected to have
// created, by its DETERMINISTIC name - the same name a sweep enqueue would
// compute, which is what makes the two collide instead of double-enqueueing.
func adoptionEvent(t *testing.T, c client.Client, project, repoName string, number int) *tatarav1.QueuedEvent {
	t.Helper()
	var qe tatarav1.QueuedEvent
	err := c.Get(context.Background(), types.NamespacedName{
		Namespace: ns,
		Name:      queue.QueuedEventName(project, queue.AdoptUpgradeDedupKey(repoName, number)),
	}, &qe)
	require.NoError(t, err, "an adoptable delivery must create a queued adoption")
	return &qe
}

// noAdoptionEvent asserts the inverse: this delivery queued nothing at all.
func noAdoptionEvent(t *testing.T, c client.Client, project string) {
	t.Helper()
	for _, qe := range allQEs(t, c, project) {
		require.Nil(t, qe.Spec.Payload.AdoptedUpgrade,
			"this delivery must not queue an adoption")
	}
}

// THE FIX, RESTATED. An adoptable delivery becomes a DURABLE QUEUE ENTRY, not a
// one-shot marker the very pass it pulls forward is free to clear.
func TestPROpened_AdoptableUpgradeMR_EnqueuesAQueuedAdoption(t *testing.T) {
	const secretVal = "whsec-a1"
	c := seedClient(t,
		adoptionProject("ad1", "ad1-scm", "tatara-bot", "renovate/", 2, nil),
		secret("ad1-scm", secretVal),
		repository("charts", "ad1", baseRepoURL, "main"),
	)
	before := testutil.ToFloat64(obs.AdoptionEnqueuedTotal.WithLabelValues("ad1", obs.WebhookActivity))
	h, _ := newServer(t, c)
	postPROpened(t, h, "ad1", secretVal, prOpenedOnBranch("tatara-bot", "renovate/cilium", 77))

	qe := adoptionEvent(t, c, "ad1", "charts", 77)
	require.NotNil(t, qe.Spec.Payload.AdoptedUpgrade)
	require.Equal(t, 77, qe.Spec.Payload.AdoptedUpgrade.Number)
	require.Equal(t, "renovate/cilium", qe.Spec.Payload.AdoptedUpgrade.HeadBranch)
	require.Equal(t, "sha-77", qe.Spec.Payload.AdoptedUpgrade.HeadSHA)
	require.Equal(t, "tatara-bot", qe.Spec.Payload.AdoptedUpgrade.Author)
	require.Equal(t, "charts", qe.Spec.RepositoryRef)
	require.Equal(t, "upgrade", qe.Spec.Kind)
	require.Equal(t, tatarav1.QueueClassNormal, qe.Spec.Class)
	require.Equal(t, tatarav1.QueueStateQueued, qe.Status.State)
	require.NoError(t, tatarav1.ValidateQueuedEventSpec(qe.Spec))

	// PRIORITY 2, DELIBERATELY (design D3). Priority 1 is "a human is waiting",
	// and admitPool sorts on (priority, seq) - so a twelve-MR Renovate run at
	// priority 1 would drain ahead of the next stage of every task already
	// underway, with the starvation guard reserving exactly one slot after an
	// hour of it. No human waits on a bump.
	require.Equal(t, 2, tatarav1.EffectiveQueuePriority(qe.Spec))

	require.Empty(t, allTasks(t, c, "ad1"),
		"the webhook enqueues; the leader-elected dispatcher is the only minter")
	require.Equal(t, before+1,
		testutil.ToFloat64(obs.AdoptionEnqueuedTotal.WithLabelValues("ad1", obs.WebhookActivity)))
}

// THE SNAPSHOT MUST SURVIVE THE FORK GUARD, and this is the assertion that pins
// it. scm.WebhookEvent.Repo is a CLONE URL and scm.PRRef.Repo is a SLUG;
// AdoptUpgradeMR clause (d) is `pr.HeadRepo == "" || pr.HeadRepo != pr.Repo ->
// refuse` and it fails CLOSED. Copying ev.Repo straight onto the payload would
// therefore have every webhook-originated adoption refused at admit time, with
// the enqueue itself looking perfectly healthy. The test asks the SAME predicate
// the dispatcher asks, off the SAME payload.
func TestPROpened_AdoptableUpgradeMR_QueuedSnapshotPassesTheForkGuard(t *testing.T) {
	const secretVal = "whsec-a1b"
	proj := adoptionProject("ad1b", "ad1b-scm", "tatara-bot", "renovate/", 2, nil)
	c := seedClient(t, proj, secret("ad1b-scm", secretVal),
		repository("charts", "ad1b", baseRepoURL, "main"))
	h, _ := newServer(t, c)
	postPROpened(t, h, "ad1b", secretVal, prOpenedOnBranch("tatara-bot", "renovate/cilium", 41))

	a := adoptionEvent(t, c, "ad1b", "charts", 41).Spec.Payload.AdoptedUpgrade
	require.Equal(t, "o/r", a.Repo, "payload.repo is the forge SLUG, never the clone URL")
	require.Equal(t, "o/r", a.HeadRepo)
	require.True(t, controller.AdoptUpgradeMR(proj, prRefFrom(a), nil, "", nil),
		"the queued snapshot must be adoptable by the very predicate the dispatcher re-asks")
}

// The same predicate, off a genuine fork: still refused. The candidate arm is a
// SHAPE test (prefix + owned author), so a fork delivery does reach the queue -
// and the dispatcher drops it as not_adoptable rather than handing a stranger's
// tree an agent pod with merge rights.
func TestPROpened_ForkedUpgradeMR_QueuedSnapshotIsRefusedByTheForkGuard(t *testing.T) {
	const secretVal = "whsec-a1c"
	proj := adoptionProject("ad1c", "ad1c-scm", "tatara-bot", "renovate/", 2, nil)
	c := seedClient(t, proj, secret("ad1c-scm", secretVal),
		repository("charts", "ad1c", baseRepoURL, "main"))
	h, _ := newServer(t, c)
	postPROpened(t, h, "ad1c", secretVal,
		prOpenedFromRepo("tatara-bot", "renovate/cilium", "stranger/r", 42))

	a := adoptionEvent(t, c, "ad1c", "charts", 42).Spec.Payload.AdoptedUpgrade
	require.Equal(t, "stranger/r", a.HeadRepo)
	require.False(t, controller.AdoptUpgradeMR(proj, prRefFrom(a), nil, "", nil),
		"a head repo that is not the base repo is a fork and must never adopt")
}

// prRefFrom is the dispatcher's own prRefFromAdopted, rebuilt here because that
// helper is unexported: the point of these two tests is to ask AdoptUpgradeMR
// exactly what admission asks it, off exactly the fields the payload carries.
func prRefFrom(a *tatarav1.AdoptedUpgradeRef) scm.PRRef {
	return scm.PRRef{
		Number: a.Number, Title: a.Title, Author: a.Author,
		HeadSHA: a.HeadSHA, HeadBranch: a.HeadBranch, Body: a.Body,
		Labels: a.Labels, Repo: a.Repo, HeadRepo: a.HeadRepo,
	}
}

// An allowlisted upgradeEngineLogins author takes the same path. It is not the
// bot, so it never met the self-loop gate - but it DID meet the reporter
// allowlist, which the candidate arm must bypass the way the sweep does
// (ClassifyPR clause 1c consults no allowlist at all).
func TestPROpened_AdoptableUpgradeMR_EngineLoginAuthor(t *testing.T) {
	const secretVal = "whsec-a2"
	proj := adoptionProject("ad2", "ad2-scm", "tatara-bot", "renovate/", 2, []string{"renovate[bot]"})
	proj.Spec.Scm.ReporterLogins = []string{"alice"} // renovate[bot] is NOT a reporter
	c := seedClient(t, proj, secret("ad2-scm", secretVal),
		repository("charts", "ad2", baseRepoURL, "main"))
	h, _ := newServer(t, c)
	postPROpened(t, h, "ad2", secretVal, prOpenedOnBranch("renovate[bot]", "renovate/cilium", 12))

	qe := adoptionEvent(t, c, "ad2", "charts", 12)
	require.Equal(t, "renovate[bot]", qe.Spec.Payload.AdoptedUpgrade.Author)
	require.Empty(t, allTasks(t, c, "ad2"))
}

// THE MULTI-REPLICA BURST, WHICH IS HOW IT ACTUALLY ARRIVES. The operator runs
// three replicas and the webhook Service load-balances across all of them, so
// five engine merge requests land on independent PROCESSES that share only the
// API server. No in-process lock spans them - and none is needed, because each
// merge request is its own natural key and no replica reads a cap.
func TestPROpened_MultiReplicaBurst_QueuesEveryMergeRequestAndMintsNothing(t *testing.T) {
	const secretVal = "whsec-a3"
	c := seedClient(t,
		adoptionProject("ad3", "ad3-scm", "tatara-bot", "renovate/", 2, nil),
		secret("ad3-scm", secretVal),
		repository("charts", "ad3", baseRepoURL, "main"),
	)
	// Five INDEPENDENT servers over one client: the multi-replica shape. Nothing
	// is shared between them but the API server itself.
	replicas := make([]http.Handler, 5)
	for i := range replicas {
		replicas[i], _ = newServer(t, c)
	}
	numbers := []int{81, 82, 83, 84, 85}
	var wg sync.WaitGroup
	for i, n := range numbers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			postPROpened(t, replicas[i], "ad3", secretVal,
				prOpenedOnBranch("tatara-bot", "renovate/dep-"+strconv.Itoa(n), n))
		}()
	}
	wg.Wait()

	require.Empty(t, allTasks(t, c, "ad3"),
		"no replica may mint: admission belongs to the leader-elected dispatcher, the only serialized writer")
	// ALL FIVE, not two. maxOpenUpgrades is 2 and that cap is deliberately NOT
	// spent here: the surplus waits in the queue and admits the moment a lane
	// frees, which is the whole defect this change removes.
	for _, n := range numbers {
		require.NotNil(t, adoptionEvent(t, c, "ad3", "charts", n).Spec.Payload.AdoptedUpgrade)
	}
}

// THE DUPLICATE DELIVERY COLLAPSES ON THE NATURAL KEY. Five processes sharing
// only an API server, all handed the SAME merge request: the deterministic
// QueuedEvent name makes four of the five Creates fail AlreadyExists, which
// EnqueueEvent treats as "not an error, not created" - so no sequence number is
// burned either.
func TestPROpened_AdoptableUpgradeMR_ConcurrentDeliveriesEnqueueOnce(t *testing.T) {
	const secretVal = "whsec-a4"
	c := seedClient(t,
		adoptionProject("ad4", "ad4-scm", "tatara-bot", "renovate/", 2, nil),
		secret("ad4-scm", secretVal),
		repository("charts", "ad4", baseRepoURL, "main"),
	)
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h, _ := newServer(t, c)
			postPROpened(t, h, "ad4", secretVal, prOpenedOnBranch("tatara-bot", "renovate/cilium", 90))
		}()
	}
	wg.Wait()

	n := 0
	for _, qe := range allQEs(t, c, "ad4") {
		if qe.Spec.Payload.AdoptedUpgrade != nil {
			n++
		}
	}
	require.Equal(t, 1, n, "five concurrent deliveries must produce exactly one queued adoption")
}

// AN ENQUEUE FAILURE IS A 500, matching this file's MintTombstoneDeleted policy:
// the adoption is still OWED, and a silent 202 discards the only fast signal
// this merge request gets. The forge redelivers within seconds.
func TestPROpened_AdoptableUpgradeMR_EnqueueFailureIs500(t *testing.T) {
	const secretVal = "whsec-a9"
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithObjects(
			adoptionProject("ad9", "ad9-scm", "tatara-bot", "renovate/", 2, nil),
			secret("ad9-scm", secretVal),
			repository("charts", "ad9", baseRepoURL, "main"),
		).
		WithStatusSubresource(&tatarav1.Project{}, &tatarav1.Repository{}, &tatarav1.Task{}, &tatarav1.QueuedEvent{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object,
				opts ...client.CreateOption) error {
				if _, isQE := obj.(*tatarav1.QueuedEvent); isQE {
					return errors.New("apiserver said no")
				}
				return c.Create(ctx, obj, opts...)
			},
		}).Build()
	h, _ := newServer(t, c)

	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "pull_request")
	body := prOpenedOnBranch("tatara-bot", "renovate/cilium", 99)
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))
	w := post(t, h, "ad9", hdr, body)
	require.Equal(t, http.StatusInternalServerError, w.Code,
		"a lost enqueue is a lost signal; the forge must redeliver")
}

// INERTNESS 1: adoption DISARMED (no adoptBranchPrefix). A bot-authored merge
// request on a renovate/ branch is still swallowed by the self-loop guard, and
// nothing is queued.
func TestPROpened_AdoptionDisarmed_BotAuthoredStillIgnored(t *testing.T) {
	const secretVal = "whsec-a5"
	c := seedClient(t,
		projectWithReporters("ad5", "ad5-scm", "tatara", "tatara-bot", nil),
		secret("ad5-scm", secretVal),
		repository("charts", "ad5", baseRepoURL, "main"),
	)
	h, _ := newServer(t, c)
	postPROpened(t, h, "ad5", secretVal, prOpenedOnBranch("tatara-bot", "renovate/cilium", 5))
	require.Empty(t, allTasks(t, c, "ad5"))
	noAdoptionEvent(t, c, "ad5")
}

// INERTNESS 2: adoption ARMED, but the author is neither botLogin nor an
// upgradeEngineLogins entry. Prefix alone never adopts - a human pushing
// renovate/whatever gets the review Task they always got, minted inline here
// exactly as before.
func TestPROpened_AdoptionArmed_HumanAuthorOnPrefixedBranchStillReviews(t *testing.T) {
	const secretVal = "whsec-a6"
	c := seedClient(t,
		adoptionProject("ad6", "ad6-scm", "tatara-bot", "renovate/", 2, nil),
		secret("ad6-scm", secretVal),
		repository("charts", "ad6", baseRepoURL, "main"),
	)
	h, _ := newServer(t, c)
	postPROpened(t, h, "ad6", secretVal, prOpenedOnBranch("mallory", "renovate/cilium", 6))

	tasks := allTasks(t, c, "ad6")
	require.Len(t, tasks, 1)
	require.Equal(t, "review", tasks[0].Spec.Kind, "an unowned author on the prefix is a review, never an adoption")
	noAdoptionEvent(t, c, "ad6")
}

// INERTNESS 3: a bot-authored merge request on a branch OUTSIDE the prefix is
// still the agent's own PR, and the self-loop guard must still swallow it with
// nothing queued.
func TestPROpened_AdoptionArmed_BotAuthoredOffPrefixStillIgnored(t *testing.T) {
	const secretVal = "whsec-a7"
	c := seedClient(t,
		adoptionProject("ad7", "ad7-scm", "tatara-bot", "renovate/", 2, nil),
		secret("ad7-scm", secretVal),
		repository("charts", "ad7", baseRepoURL, "main"),
	)
	h, _ := newServer(t, c)
	postPROpened(t, h, "ad7", secretVal, prOpenedOnBranch("tatara-bot", "tatara/task-implement-9", 7))
	require.Empty(t, allTasks(t, c, "ad7"))
	noAdoptionEvent(t, c, "ad7")
}

// A delivery this project has no Repository for queues nothing and still 202s:
// there is nothing to adopt INTO, and the sweep would never look at it either.
func TestPROpened_AdoptableButUnknownRepo_QueuesNothing(t *testing.T) {
	const secretVal = "whsec-a8"
	c := seedClient(t,
		adoptionProject("ad8", "ad8-scm", "tatara-bot", "renovate/", 2, nil),
		secret("ad8-scm", secretVal),
		repository("charts", "ad8", "https://github.com/o/other.git", "main"),
	)
	h, _ := newServer(t, c)
	postPROpened(t, h, "ad8", secretVal, prOpenedOnBranch("tatara-bot", "renovate/cilium", 8))
	require.Empty(t, allTasks(t, c, "ad8"))
	noAdoptionEvent(t, c, "ad8")
}
