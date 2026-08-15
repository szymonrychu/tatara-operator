package webhook_test

// THE WEBHOOK FAST PATH FOR DEPENDENCY-UPGRADE ADOPTION.
//
// Adoption shipped in v2.16.0 driven by the SWEEP ALONE, so an adoptable merge
// request waited up to a full issueScan period (0 */4 * * * in production) for
// its Task. The merge-request-open webhook was delivered and answered 202 the
// whole time: handleMROpened's first line dropped every bot-authored delivery,
// and the engine now authors AS the bot.
//
// THE WEBHOOK DOES NOT MINT, AND THAT IS THE WHOLE DESIGN. maxOpenUpgrades is a
// project-wide budget, the webhook server runs on EVERY replica
// (HandlerRunnable.NeedLeaderElection() == false), and a burst of engine merge
// requests load-balances across all of them - so any check-then-mint in this
// handler is a distributed race no in-process lock can close. Instead an
// adoptable delivery PULLS THE REPO'S SWEEP SLOT FORWARD, and the leader-only,
// per-project-serialized sweep does the minting under the cap it already
// enforces correctly. These tests pin that split, and pin that arming it
// changed nothing for any other author.

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
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
// headBranch.
func prOpenedOnBranch(login, headBranch string, number int) []byte {
	n := strconv.Itoa(number)
	return []byte(`{"action":"opened","pull_request":{"number":` + n +
		`,"title":"chore(deps): update cilium to v1.17.` + n + `"` +
		`,"body":"### Release Notes","user":{"login":"` + login + `"},` +
		`"head":{"sha":"sha-` + n + `","ref":"` + headBranch + `"},` +
		`"html_url":"https://github.com/o/r/pull/` + n + `"},` +
		`"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},` +
		`"sender":{"login":"` + login + `"}}`)
}

const baseRepoURL = "https://github.com/o/r.git"

// sweepRequest reads the repo's pulled-forward sweep marker, or "" when unset.
func sweepRequest(t *testing.T, c client.Client, repoName string) string {
	t.Helper()
	var repo tatarav1.Repository
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: ns, Name: repoName}, &repo))
	return repo.Annotations[tatarav1.SweepRequestedAnnotation]
}

// THE DEFECT, RESTATED FOR THE NEW SHAPE. A bot-authored merge request on the
// configured adoptBranchPrefix is exactly what the dependency engine opens, and
// the webhook did nothing at all with it - no mint, and no signal to the leader
// either, so the Task appeared only when the next 4-hourly sweep ran.
func TestPROpened_AdoptableUpgradeMR_RequestsAnImmediateSweep(t *testing.T) {
	const secretVal = "whsec-a1"
	c := seedClient(t,
		adoptionProject("ad1", "ad1-scm", "tatara-bot", "renovate/", 2, nil),
		secret("ad1-scm", secretVal),
		repository("charts", "ad1", baseRepoURL, "main"),
	)
	h, _ := newServer(t, c)
	postPROpened(t, h, "ad1", secretVal, prOpenedOnBranch("tatara-bot", "renovate/cilium", 77))

	stamp := sweepRequest(t, c, "charts")
	require.NotEmpty(t, stamp, "an adoptable delivery must pull this repo's sweep slot forward")
	_, err := time.Parse(time.RFC3339, stamp)
	require.NoError(t, err, "the marker is an RFC3339 instant the sweep compares against its own base")

	require.Empty(t, allTasks(t, c, "ad1"),
		"the webhook must NOT mint: maxOpenUpgrades is a project budget and this handler runs on every replica")
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

	require.NotEmpty(t, sweepRequest(t, c, "charts"))
	require.Empty(t, allTasks(t, c, "ad2"))
}

// THE MULTI-REPLICA BURST, WHICH IS HOW IT ACTUALLY ARRIVES. The operator runs
// three replicas and the webhook Service load-balances across all of them, so
// five engine merge requests land on independent PROCESSES that share only the
// API server. No in-process lock spans them. Five servers, no shared mutex:
// still zero Tasks minted here, and one idempotent marker for the leader.
func TestPROpened_MultiReplicaBurst_MintsNothingOnAnyReplica(t *testing.T) {
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
	var wg sync.WaitGroup
	for i, n := range []int{81, 82, 83, 84, 85} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			postPROpened(t, replicas[i], "ad3", secretVal,
				prOpenedOnBranch("tatara-bot", "renovate/dep-"+strconv.Itoa(n), n))
		}()
	}
	wg.Wait()

	require.Empty(t, allTasks(t, c, "ad3"),
		"no replica may mint: the cap belongs to the leader-only sweep, which is the only serialized writer")
	require.NotEmpty(t, sweepRequest(t, c, "charts"),
		"the burst collapses to ONE marker - a set, not a counter, so it debounces itself")
}

// INERTNESS 1: adoption DISARMED (no adoptBranchPrefix). A bot-authored merge
// request on a renovate/ branch is still swallowed by the self-loop guard, and
// nothing is stamped.
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
	require.Empty(t, sweepRequest(t, c, "charts"))
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
	require.Empty(t, sweepRequest(t, c, "charts"), "a review mint needs no sweep; it already happened")
}

// INERTNESS 3: a bot-authored merge request on a branch OUTSIDE the prefix is
// still the agent's own PR, and the self-loop guard must still swallow it with
// no marker.
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
	require.Empty(t, sweepRequest(t, c, "charts"))
}

// A delivery this project has no Repository for stamps nothing and still 202s.
func TestPROpened_AdoptableButUnknownRepo_StampsNothing(t *testing.T) {
	const secretVal = "whsec-a8"
	c := seedClient(t,
		adoptionProject("ad8", "ad8-scm", "tatara-bot", "renovate/", 2, nil),
		secret("ad8-scm", secretVal),
		repository("charts", "ad8", "https://github.com/o/other.git", "main"),
	)
	h, _ := newServer(t, c)
	postPROpened(t, h, "ad8", secretVal, prOpenedOnBranch("tatara-bot", "renovate/cilium", 8))
	require.Empty(t, allTasks(t, c, "ad8"))
	require.Empty(t, sweepRequest(t, c, "charts"))
}
