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

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/controller"
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
