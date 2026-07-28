package webhook_test

// THE PLATFORM'S FRONT DOOR (contract F.3's Create edge, B.4's intake).
//
// A human opens an issue. THE WEBHOOK IS NOW THE PRIMARY MINTER (Task 3): it
// mints an ACTIVE (triaging) clarify Task immediately, in-request, via the
// shared controller.Minter funnel, and owns the mirror Issue CR right away
// instead of leaving it ownerless for the sweep. It also stamps the DURABLE
// LIVENESS MARKER: the tatara.dev/webhook-originated annotation on the mirror
// Issue CR, which MintStage reads to pick triaging over parked(backlog-sweep).
//
// The B.4 sweep is now a BACKSTOP, not the sole intake: its own pass over an
// issue the webhook already minted is a no-op, because both paths key the
// Task off the same deterministic IntakeTaskName (project, kind, repo,
// number) and MintForItem's adopt-or-create only mints when that natural key
// is still unowned (see TestSweepAfterWebhook_NoDoubleMint in
// primary_mint_test.go). The marker itself still matters for the sweep's OWN
// cold-start pass: it is the ONLY thing that tells a freshly-opened human
// issue apart from a three-year-old untouched backlog issue if the webhook
// mint is ever unavailable and the sweep has to intake it cold - reading a
// zero-comment open issue as "a human has the last word" without the marker
// would mint the ENTIRE cutover backlog ACTIVE, the 150-issue re-triage storm
// parked(backlog-sweep) exists to prevent.

import (
	"context"
	"net/http"
	"strconv"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	clientgotesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/controller"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"github.com/szymonrychu/tatara-operator/internal/webhook"
)

// issueOpenedBy renders an issues.<action> delivery authored by login.
func issueOpenedBy(action, login string, number int) []byte {
	n := strconv.Itoa(number)
	return []byte(`{"action":"` + action + `","issue":{"number":` + n +
		`,"title":"the login page 500s","body":"steps to reproduce","user":{"login":"` + login + `"},` +
		`"html_url":"https://github.com/o/r/issues/` + n + `"},` +
		`"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},` +
		`"sender":{"login":"` + login + `"}}`)
}

// postIssueOpened signs and delivers an issues webhook, asserting a 202.
func postIssueOpened(t *testing.T, h http.Handler, projName, secretVal string, body []byte) {
	t.Helper()
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issues")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))
	w := post(t, h, projName, hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)
}

// TestIssueOpened_MintsAndConsumesTheWebhookMarker: a human opens a NEW issue.
// The mirror Issue CR does not exist yet; the webhook stamps the marker, mints
// the Task in-request (Task 3: the webhook is the PRIMARY minter), owns the CR -
// and then CONSUMES the marker (fix F7-1). The marker's cold-start value is a
// property of an UNOWNED issue; once this mint owns the CR the sweep skips it, so
// a lingering marker would only re-activate the issue after a later park + reap.
// Consumed-exactly-once: the mint that read it clears it.
func TestIssueOpened_MintsAndConsumesTheWebhookMarker(t *testing.T) {
	const secretVal = "whsec-open1"
	c := seedClient(t,
		projectWithReporters("openproj", "openproj-scm", "tatara", "tatara-bot", nil),
		secret("openproj-scm", secretVal),
		repository("repo-open", "openproj", "https://github.com/o/r.git", "main"),
	)
	h, _ := newServer(t, c)

	postIssueOpened(t, h, "openproj", secretVal, issueOpenedBy("opened", "alice", 7))

	var iss tatarav1.Issue
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: ns, Name: tatarav1.IssueName("repo-open", 7)}, &iss))
	require.NotEmpty(t, iss.OwnerReferences, "the webhook now mints a Task and owns the mirror CR immediately")
	require.Empty(t, iss.Annotations[controller.AnnWebhookOriginated],
		"a successful webhook mint CONSUMES the marker (F7-1: consumed-exactly-once)")

	// THE WEBHOOK IS NOW THE PRIMARY MINTER (Task 3 supersedes Task 21's call).
	require.Len(t, allTasks(t, c, "openproj"), 1, "the webhook must mint a Task immediately")
}

// TestIssueReopened_MintsAndConsumesTheWebhookMarker: a reopen is the same live
// signal as an open. GitLab collapses open/reopen into "opened"; GitHub keeps
// them apart. The successful mint consumes the marker either way (F7-1).
func TestIssueReopened_MintsAndConsumesTheWebhookMarker(t *testing.T) {
	const secretVal = "whsec-open2"
	c := seedClient(t,
		projectWithReporters("reopenproj", "reopenproj-scm", "tatara", "tatara-bot", nil),
		secret("reopenproj-scm", secretVal),
		repository("repo-reopen", "reopenproj", "https://github.com/o/r.git", "main"),
	)
	h, _ := newServer(t, c)

	postIssueOpened(t, h, "reopenproj", secretVal, issueOpenedBy("reopened", "alice", 11))

	var iss tatarav1.Issue
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: ns, Name: tatarav1.IssueName("repo-reopen", 11)}, &iss))
	require.NotEmpty(t, iss.OwnerReferences, "the reopen mint owns the mirror CR")
	require.Empty(t, iss.Annotations[controller.AnnWebhookOriginated],
		"a successful webhook mint CONSUMES the marker (F7-1)")
}

// TestIssueOpened_BotAuthoredNeverMarks: a BOT-opened issue is not a human
// waiting on us. Marking it would hand the operator's own issue-writes an ACTIVE
// Task - a self-trigger loop with no human in it. Reuses the SAME bot predicate
// every other inbound path uses (Project.spec.scm.botLogin).
func TestIssueOpened_BotAuthoredNeverMarks(t *testing.T) {
	const secretVal = "whsec-open3"
	c := seedClient(t,
		projectWithReporters("botproj", "botproj-scm", "tatara", "tatara-bot", nil),
		secret("botproj-scm", secretVal),
		repository("repo-bot", "botproj", "https://github.com/o/r.git", "main"),
	)
	h, reg := newServer(t, c)

	postIssueOpened(t, h, "botproj", secretVal, issueOpenedBy("opened", "tatara-bot", 3))

	var iss tatarav1.Issue
	err := c.Get(context.Background(),
		types.NamespacedName{Namespace: ns, Name: tatarav1.IssueName("repo-bot", 3)}, &iss)
	require.Error(t, err, "a bot-opened issue must not even mint a mirror CR")
	require.Equal(t, 1.0, counterValue(t, reg, "operator_webhook_events_total",
		map[string]string{"provider": "github", "kind": "issue", "action": "opened", "result": "ignored"}))
}

// TestIssueOpened_NonReporterNeverMarks: the issue #102 reporter gate applies to
// the marker exactly as it applies to comments. An INJECTED issue never becomes
// an ACTIVE Task.
func TestIssueOpened_NonReporterNeverMarks(t *testing.T) {
	const secretVal = "whsec-open4"
	c := seedClient(t,
		projectWithReporters("gateproj", "gateproj-scm", "tatara", "tatara-bot", []string{"alice"}),
		secret("gateproj-scm", secretVal),
		repository("repo-gate", "gateproj", "https://github.com/o/r.git", "main"),
	)
	h, _ := newServer(t, c)

	postIssueOpened(t, h, "gateproj", secretVal, issueOpenedBy("opened", "mallory", 4))

	var iss tatarav1.Issue
	require.Error(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: ns, Name: tatarav1.IssueName("repo-gate", 4)}, &iss),
		"an issue from outside the reporter allowlist must not be marked")
}

// TestIssueOpened_OwnedIssueIsNeverRemarked: the marker means "no Task has ever
// looked at this". An issue a Task already OWNS is not an intake candidate (the
// sweep's orphan predicate skips it), so a reopen on it must not leave a marker
// that would re-activate the issue after a LATER park + reap.
func TestIssueOpened_OwnedIssueIsNeverRemarked(t *testing.T) {
	const secretVal = "whsec-open5"
	owned := &tatarav1.Issue{
		ObjectMeta: metav1.ObjectMeta{
			Name: tatarav1.IssueName("repo-owned", 12), Namespace: ns,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: tatarav1.GroupVersion.String(), Kind: "Task",
				Name: "owner-task", UID: types.UID("u-owner"), Controller: ptrBool(true),
			}},
		},
		Spec: tatarav1.IssueSpec{RepositoryRef: "repo-owned", Number: 12, ProjectRef: "ownedproj"},
	}
	c := seedClient(t,
		projectWithReporters("ownedproj", "ownedproj-scm", "tatara", "tatara-bot", nil),
		secret("ownedproj-scm", secretVal),
		repository("repo-owned", "ownedproj", "https://github.com/o/r.git", "main"),
		owned,
	)
	h, _ := newServer(t, c)

	postIssueOpened(t, h, "ownedproj", secretVal, issueOpenedBy("reopened", "alice", 12))

	var iss tatarav1.Issue
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: ns, Name: tatarav1.IssueName("repo-owned", 12)}, &iss))
	require.Empty(t, iss.Annotations[controller.AnnWebhookOriginated],
		"an issue an active Task already owns must never carry the marker")
}

// TestIssueOpened_UnknownRepoIsIgnored: an issue on a repo this project has not
// enrolled has no mirror to stamp. Accept and ignore.
func TestIssueOpened_UnknownRepoIsIgnored(t *testing.T) {
	const secretVal = "whsec-open6"
	c := seedClient(t,
		projectWithReporters("unkproj", "unkproj-scm", "tatara", "tatara-bot", nil),
		secret("unkproj-scm", secretVal),
	)
	h, reg := newServer(t, c)

	postIssueOpened(t, h, "unkproj", secretVal, issueOpenedBy("opened", "alice", 8))

	var il tatarav1.IssueList
	require.NoError(t, c.List(context.Background(), &il))
	require.Empty(t, il.Items)
	require.Equal(t, 1.0, counterValue(t, reg, "operator_webhook_events_total",
		map[string]string{"provider": "github", "kind": "issue", "action": "opened", "result": "ignored"}))
}

// TestIssueClosed_NeverMarks: only opened/reopened are the live intake signal.
// A close, a label change or an edit must not mark.
func TestIssueClosed_NeverMarks(t *testing.T) {
	const secretVal = "whsec-open7"
	c := seedClient(t,
		projectWithReporters("closeproj", "closeproj-scm", "tatara", "tatara-bot", nil),
		secret("closeproj-scm", secretVal),
		repository("repo-close", "closeproj", "https://github.com/o/r.git", "main"),
	)
	h, _ := newServer(t, c)

	postIssueOpened(t, h, "closeproj", secretVal, issueOpenedBy("closed", "alice", 5))

	var il tatarav1.IssueList
	require.NoError(t, c.List(context.Background(), &il))
	require.Empty(t, il.Items, "a close is not an intake signal")
}

func ptrBool(b bool) *bool { return &b }

// TestIssueOpened_LaggingCacheStillMintsTriaging IS THE REGRESSION TEST FOR
// mtg-decks#9. The handler runs against a client whose Issue reads lag (an
// informer cache that has not observed a Create yet). Before the fix,
// MarkWebhookOriginated returned that NotFound, handleIssueOpened 500'd at
// server.go:648, and MintForItem at :660 never ran - a maintainer opened an
// issue and the platform did nothing, with no retry from GitHub. It must now
// answer 202 and mint an ACTIVE clarify Task.
func TestIssueOpened_LaggingCacheStillMintsTriaging(t *testing.T) {
	const secretVal = "whsec-lag1"
	remaining := 2
	c := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithObjects(
			projectWithReporters("lagproj", "lagproj-scm", "tatara", "tatara-bot", nil),
			secret("lagproj-scm", secretVal),
			repository("repo-lag", "lagproj", "https://github.com/o/r.git", "main"),
		).
		WithStatusSubresource(&tatarav1.Project{}, &tatarav1.Repository{}, &tatarav1.Task{},
			&tatarav1.QueuedEvent{}, &tatarav1.Issue{}, &tatarav1.MergeRequest{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, cli client.WithWatch, key client.ObjectKey,
				obj client.Object, opts ...client.GetOption) error {
				if _, isIssue := obj.(*tatarav1.Issue); isIssue && remaining > 0 {
					remaining--
					return apierrors.NewNotFound(
						tatarav1.GroupVersion.WithResource("issues").GroupResource(), key.Name)
				}
				return cli.Get(ctx, key, obj, opts...)
			},
		}).
		Build()
	h, _ := newServer(t, c)

	// postIssueOpened asserts 202 itself. Before the fix this is a 500.
	postIssueOpened(t, h, "lagproj", secretVal, issueOpenedBy("opened", "alice", 9))

	var tl tatarav1.TaskList
	require.NoError(t, c.List(context.Background(), &tl))
	require.Len(t, tl.Items, 1, "a brand-new maintainer issue must mint exactly one Task")
	require.Equal(t, tatarav1.StageTriaging, tl.Items[0].Spec.InitialStage,
		"the mint must be ACTIVE: a human just opened this issue")
}

// newServerWithReader mirrors newServer but wires reader as Config.APIReader -
// production's manager.GetAPIReader(), an UNCACHED client distinct from the
// cached Client. Used only by TestIssueOpened_UncachedReaderClosesTheRace,
// which needs the two genuinely separate (real vs. cache-lagging) reads that a
// single client cannot model.
func newServerWithReader(c client.Client, reader client.Reader) http.Handler {
	return webhook.NewServer(webhook.Config{
		Client:    c,
		APIReader: reader,
		Namespace: ns,
		Metrics:   obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Seq:       &queue.SeqSource{Client: c, Namespace: ns},
	}).Handler()
}

// TestIssueOpened_UncachedReaderClosesTheRace IS FINDING 3'S REGRESSION TEST
// (2026-07-28 review round 1, following mtg-decks#9). The test above
// (TestIssueOpened_LaggingCacheStillMintsTriaging) proves MarkWebhookOriginated
// itself survives a lagging cache. It does NOT prove the whole request does: a
// review pass found that under sustained lag, a request could still 500
// DOWNSTREAM of a successful mark and mint - in mirror.SyncIssue's own
// objbudget.FitIssue read, called from MintIssueTask right after the Task is
// created - and because that happens AFTER the Task already exists, the
// marker is left stamped forever: neither the webhook (the error return skips
// its own ClearWebhookOriginated call) nor a later sweep pass (whose own
// createTaskRaceSafe now finds a live twin and takes the repair branch, which
// never touches the marker) ever clears it, so the issue re-activates on
// every later reap cycle.
//
// cachedClient and liveReader below share ONE k8s.io/client-go/testing.ObjectTracker
// - only cachedClient carries the lag interceptor - modeling manager.GetClient()
// (informer-backed, can lag a write it just made) against manager.GetAPIReader()
// (direct to the API server, never lags) exactly as production wires
// webhook.Config.APIReader. MarkWebhookOriginated now reads through
// Server.reader(), which prefers APIReader, so ITS OWN read never depends on
// the lagging client catching up - and because that keeps Mark's own footprint
// on the cached client down to at most one Get on the create path (finding 1),
// objbudget.FitIssue's own pre-existing createLagBackoff (~3.1s across 5
// attempts, unrelated to this fix and out of its scope: it is shared by
// ~15 call sites across the whole codebase) comfortably absorbs the rest for
// a realistic, bounded lag. (issueCR - MintForItem's own orphan-classification
// read - was ALSO tried on the uncached reader during this review round; it
// broke TestResumeNoReentryPark_DirectMintCacheLagStillActive, whose own doc
// comment documents that only the specific re-entrant read that needs
// freshness goes through APIReader, not the general classification read, so
// intake.go's issueCR deliberately stays on the cached Client - see its own
// doc comment for the full reasoning.)
func TestIssueOpened_UncachedReaderClosesTheRace(t *testing.T) {
	const secretVal = "whsec-lag2"
	scheme := newScheme(t)
	tracker := clientgotesting.NewObjectTracker(scheme, serializer.NewCodecFactory(scheme).UniversalDecoder())
	for _, o := range []client.Object{
		projectWithReporters("lagproj2", "lagproj2-scm", "tatara", "tatara-bot", nil),
		secret("lagproj2-scm", secretVal),
		repository("repo-lag2", "lagproj2", "https://github.com/o/r.git", "main"),
	} {
		require.NoError(t, tracker.Add(o))
	}

	// 5, not "generously large": FitIssue's own getWaitingOutCreateLag retries a
	// cached NotFound up to 5 times (objbudget.createLagBackoff's Steps), and
	// this budget must run out WITHIN that window - 1 for SyncIssue's own
	// ensureIssueCR probe (harmless: it just falls through to Create's
	// AlreadyExists) plus at most 4 of FitIssue's 5 attempts, leaving its LAST
	// attempt to see real data. A budget large enough to also swallow FitIssue's
	// final attempt would fail this test for a reason FINDING 3 does not claim
	// to fix (objbudget's own pre-existing, separately-owned backoff), not for a
	// regression in the code this test actually exercises.
	remaining := 5
	lag := interceptor.Funcs{
		Get: func(ctx context.Context, cli client.WithWatch, key client.ObjectKey,
			obj client.Object, opts ...client.GetOption) error {
			if _, isIssue := obj.(*tatarav1.Issue); isIssue && remaining > 0 {
				remaining--
				return apierrors.NewNotFound(
					tatarav1.GroupVersion.WithResource("issues").GroupResource(), key.Name)
			}
			return cli.Get(ctx, key, obj, opts...)
		},
	}
	withStatus := func(b *fake.ClientBuilder) *fake.ClientBuilder {
		return b.WithStatusSubresource(&tatarav1.Project{}, &tatarav1.Repository{}, &tatarav1.Task{},
			&tatarav1.QueuedEvent{}, &tatarav1.Issue{}, &tatarav1.MergeRequest{})
	}
	cachedClient := withStatus(fake.NewClientBuilder().WithScheme(scheme).WithObjectTracker(tracker)).
		WithInterceptorFuncs(lag).Build()
	liveReader := withStatus(fake.NewClientBuilder().WithScheme(scheme).WithObjectTracker(tracker)).Build()

	h := newServerWithReader(cachedClient, liveReader)

	// postIssueOpened asserts 202 itself. Before MarkWebhookOriginated read
	// through the uncached reader (finding 1/3), this budget was enough to also
	// fail Mark's own Get, producing the worse defect: a 500 downstream of an
	// already-successful mark and mint, with the marker left stamped forever.
	postIssueOpened(t, h, "lagproj2", secretVal, issueOpenedBy("opened", "alice", 9))

	var tl tatarav1.TaskList
	require.NoError(t, liveReader.List(context.Background(), &tl))
	require.Len(t, tl.Items, 1, "a brand-new maintainer issue must mint exactly one Task even while the cache lags")

	var iss tatarav1.Issue
	require.NoError(t, liveReader.Get(context.Background(),
		types.NamespacedName{Namespace: ns, Name: tatarav1.IssueName("repo-lag2", 9)}, &iss))
	require.Empty(t, iss.Annotations[controller.AnnWebhookOriginated],
		"a successful mint must consume the marker (F7-1), not leak it under a lagging cache")
}
