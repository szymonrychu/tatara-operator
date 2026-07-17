package webhook_test

// Task 3: the webhook is the PRIMARY minter. handleIssueOpened, handleMROpened
// and the orphan-comment path call controller.Minter.MintForItem immediately,
// within the HTTP handler - a new human issue/PR mints its Task on webhook
// delivery, not at the next B.4 sweep tick. The sweep remains the BACKSTOP:
// its own pass over the same natural key is a no-op (TestSweepAfterWebhook_NoDoubleMint).

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
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// prOpenedBy renders a pull_request.<action> delivery authored by login.
func prOpenedBy(action, login string, number int) []byte {
	n := strconv.Itoa(number)
	return []byte(`{"action":"` + action + `","pull_request":{"number":` + n +
		`,"user":{"login":"` + login + `"},"head":{"sha":"abc","ref":"fix"},` +
		`"html_url":"https://github.com/o/r/pull/` + n + `"},` +
		`"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},` +
		`"sender":{"login":"` + login + `"}}`)
}

// postPROpened signs and delivers a pull_request webhook, asserting a 202.
func postPROpened(t *testing.T, h http.Handler, projName, secretVal string, body []byte) {
	t.Helper()
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "pull_request")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))
	w := post(t, h, projName, hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)
}

// A human opens a NEW issue: the webhook mints an ACTIVE clarify Task NOW, and
// the mirror Issue CR is owned by it. (Supersedes the old "mints nothing" test.)
func TestIssueOpened_MintsClarifyTaskImmediately(t *testing.T) {
	const secretVal = "whsec-mint1"
	c := seedClient(t,
		projectWithReporters("mp", "mp-scm", "tatara", "tatara-bot", nil),
		secret("mp-scm", secretVal),
		repository("repo-open", "mp", "https://github.com/o/r.git", "main"),
	)
	h, _ := newServer(t, c)
	postIssueOpened(t, h, "mp", secretVal, issueOpenedBy("opened", "alice", 353))

	tasks := allTasks(t, c, "mp")
	require.Len(t, tasks, 1)
	require.Equal(t, "clarify", tasks[0].Spec.Kind)
	require.Equal(t, tatarav1.StageTriaging, tasks[0].Spec.InitialStage)

	var iss tatarav1.Issue
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Namespace: ns, Name: tatarav1.IssueName("repo-open", 353)}, &iss))
	owner, ok := own.ControllerOwner(&iss)
	require.True(t, ok)
	require.Equal(t, tasks[0].Name, owner)
}

// A bot-authored issue.opened mints nothing (self-loop guard).
func TestIssueOpened_BotAuthored_NoMint(t *testing.T) {
	const secretVal = "whsec-mint2"
	c := seedClient(t,
		projectWithReporters("mp2", "mp2-scm", "tatara", "tatara-bot", nil),
		secret("mp2-scm", secretVal),
		repository("repo-b", "mp2", "https://github.com/o/r.git", "main"),
	)
	h, _ := newServer(t, c)
	postIssueOpened(t, h, "mp2", secretVal, issueOpenedBy("opened", "tatara-bot", 5))
	require.Empty(t, allTasks(t, c, "mp2"))
}

// An author outside a non-empty reporter allowlist mints nothing (issue #102).
func TestIssueOpened_NotAllowedReporter_NoMint(t *testing.T) {
	const secretVal = "whsec-mint3"
	c := seedClient(t,
		projectWithReporters("mp3", "mp3-scm", "tatara", "tatara-bot", []string{"alice"}),
		secret("mp3-scm", secretVal),
		repository("repo-c", "mp3", "https://github.com/o/r.git", "main"),
	)
	h, _ := newServer(t, c)
	postIssueOpened(t, h, "mp3", secretVal, issueOpenedBy("opened", "mallory", 8))
	require.Empty(t, allTasks(t, c, "mp3"))
}

// After the webhook mints, a sweep pass over the same issue no-ops (backstop
// idempotency): still exactly one Task.
func TestSweepAfterWebhook_NoDoubleMint(t *testing.T) {
	const secretVal = "whsec-mint4"
	proj := projectWithReporters("mp4", "mp4-scm", "tatara", "tatara-bot", nil)
	repo := repository("tatara-operator", "mp4", "https://github.com/o/r.git", "main")
	c := seedClient(t, proj, secret("mp4-scm", secretVal), repo)
	h, _ := newServer(t, c)
	postIssueOpened(t, h, "mp4", secretVal, issueOpenedBy("opened", "alice", 353))
	require.Len(t, allTasks(t, c, "mp4"), 1)

	// Drive the shared funnel again as the sweep would, same natural key.
	m := &controller.Minter{Client: c, APIReader: c, Scheme: c.Scheme()}
	_, created, err := m.MintForItem(context.Background(), proj, repo,
		controller.ForgeItem{Issue: scm.Issue{Number: 353, State: "open", Author: "alice"}}, false, nil)
	require.NoError(t, err)
	require.False(t, created, "the Issue CR is owned; the sweep backstop no-ops")
	require.Len(t, allTasks(t, c, "mp4"), 1)
}

// A human opens a PR: the webhook mints a review Task immediately.
func TestPROpened_MintsReviewTaskImmediately(t *testing.T) {
	const secretVal = "whsec-mint5"
	c := seedClient(t,
		projectWithReporters("mp5", "mp5-scm", "tatara", "tatara-bot", nil),
		secret("mp5-scm", secretVal),
		repository("repo-pr", "mp5", "https://github.com/o/r.git", "main"),
	)
	h, _ := newServer(t, c)
	postPROpened(t, h, "mp5", secretVal, prOpenedBy("opened", "alice", 42))
	tasks := allTasks(t, c, "mp5")
	require.Len(t, tasks, 1)
	require.Equal(t, "review", tasks[0].Spec.Kind)
}

// commentBodyOn renders an issue_comment.created delivery on the given issue
// number, authored by login (same shape as reporter_gate_test.go's
// issueCommentBy, parameterized on number so orphan/owned cases can target
// distinct issues).
func commentBodyOn(number int, login string) []byte {
	n := strconv.Itoa(number)
	return []byte(`{"action":"created","issue":{"number":` + n +
		`,"title":"old bug","body":"still broken","html_url":"https://github.com/o/r/issues/` + n + `"},` +
		`"comment":{"id":5,"body":"please implement now","user":{"login":"` + login + `"}},` +
		`"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},` +
		`"sender":{"login":"` + login + `"}}`)
}

// postIssueComment signs and delivers an issue_comment webhook, asserting a 202.
func postIssueComment(t *testing.T, h http.Handler, projName, secretVal string, body []byte) {
	t.Helper()
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issue_comment")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))
	w := post(t, h, projName, hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)
}

// A comment lands on an issue with NO mirror CR yet: commentIsOrphan reads a
// NotFound and the orphan-comment path mints a clarify Task immediately,
// same as issue-opened would have.
func TestOrphanComment_NoMirror_MintsTask(t *testing.T) {
	const secretVal = "whsec-oc1"
	c := seedClient(t,
		projectWithReporters("ocp1", "ocp1-scm", "tatara", "tatara-bot", nil),
		secret("ocp1-scm", secretVal),
		repository("repo-oc1", "ocp1", "https://github.com/o/r.git", "main"),
	)
	h, _ := newServer(t, c)

	postIssueComment(t, h, "ocp1", secretVal, commentBodyOn(21, "alice"))

	tasks := allTasks(t, c, "ocp1")
	require.Len(t, tasks, 1, "a comment on an orphan (no-mirror) issue must mint a Task")
	require.Equal(t, "clarify", tasks[0].Spec.Kind)
	require.Equal(t, tatarav1.StageTriaging, tasks[0].Spec.InitialStage,
		"a live HMAC-verified human comment is a liveness signal like issues.opened; must mint ACTIVE, not parked")
}

// A comment lands on an issue whose mirror CR already exists but carries no
// controller owner (e.g. a stale/partial mirror from a failed prior mint):
// commentIsOrphan reports true and the orphan-comment path mints.
func TestOrphanComment_UnownedMirror_MintsTask(t *testing.T) {
	const secretVal = "whsec-oc2"
	const repoName = "repo-oc2"
	const projName = "ocp2"
	unowned := &tatarav1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: tatarav1.IssueName(repoName, 22), Namespace: ns},
		Spec:       tatarav1.IssueSpec{RepositoryRef: repoName, Number: 22, ProjectRef: projName},
	}
	c := seedClient(t,
		projectWithReporters(projName, "ocp2-scm", "tatara", "tatara-bot", nil),
		secret("ocp2-scm", secretVal),
		repository(repoName, projName, "https://github.com/o/r.git", "main"),
		unowned,
	)
	h, _ := newServer(t, c)

	postIssueComment(t, h, projName, secretVal, commentBodyOn(22, "alice"))

	tasks := allTasks(t, c, projName)
	require.Len(t, tasks, 1, "a comment on an un-owned mirror must mint a Task")
	require.Equal(t, "clarify", tasks[0].Spec.Kind)
	require.Equal(t, tatarav1.StageTriaging, tasks[0].Spec.InitialStage,
		"a live HMAC-verified human comment is a liveness signal like issues.opened; must mint ACTIVE, not parked")
}

// A comment lands on an issue whose mirror IS owned by an existing Task:
// commentIsOrphan reports false, no new Task is minted, and the ordinary
// pending-event path (contract E.3) still runs - the owning Task's
// pendingEvents gets the comment queued.
func TestOwnedMirrorComment_NoMint_PendingPathRuns(t *testing.T) {
	const secretVal = "whsec-oc3"
	const repoName = "repo-oc3"
	const projName = "ocp3"
	task := &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "existing-task-oc3", Namespace: ns},
		Spec:       tatarav1.TaskSpec{Kind: "clarify", ProjectRef: projName, Goal: "g"},
	}
	owned := &tatarav1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: tatarav1.IssueName(repoName, 23), Namespace: ns},
		Spec:       tatarav1.IssueSpec{RepositoryRef: repoName, Number: 23, ProjectRef: projName},
	}
	own.AddPlainOwner(owned, task)
	require.NoError(t, own.HandOverController(owned, nil, task))
	c := seedClient(t,
		projectWithReporters(projName, "ocp3-scm", "tatara", "tatara-bot", nil),
		secret("ocp3-scm", secretVal),
		repository(repoName, projName, "https://github.com/o/r.git", "main"),
		task, owned,
	)
	h, _ := newServer(t, c)

	postIssueComment(t, h, projName, secretVal, commentBodyOn(23, "alice"))

	require.Len(t, allTasks(t, c, projName), 1, "an owned mirror must not mint a second Task")

	var got tatarav1.Task
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: task.Name}, &got))
	require.Len(t, got.Status.PendingEvents, 1, "the pending path must still queue the comment on the owning Task")
}
