package webhook_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// closedIssueBody builds a GitHub issues.closed webhook body for issue #7,
// carrying the trigger label so it is not dropped by the reporter/label gate
// before reaching the closed-event handling.
func closedIssueBody(senderLogin string) []byte {
	return []byte(`{"action":"closed","sender":{"login":"` + senderLogin + `"},` +
		`"issue":{"number":7,"title":"Fix","body":"please fix","user":{"login":"reporter"},` +
		`"labels":[{"name":"tatara"}],` +
		`"html_url":"https://github.com/o/r/issues/7"},` +
		`"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
}

func postClosedIssue(t *testing.T, c client.Client, projName, secretVal, sender string) {
	t.Helper()
	h, _ := newServer(t, c)
	body := closedIssueBody(sender)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issues")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))
	w := post(t, h, projName, hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)
}

// TestIssueClosed_HumanActor_ParksOwningTask is FIX-1(b): a human closing the
// tracked issue is the only veto over the operator's implement->review->
// merge->deploy lifecycle (including the auto-approve release path, item 4a).
// The owning non-terminal front-half Task must be parked, not left running.
func TestIssueClosed_HumanActor_ParksOwningTask(t *testing.T) {
	const secretVal = "whsec"
	proj := projectWithBot("projic1", "projic1-scm", "tatara", "tatara-bot")
	repo := repository("repoic1", "projic1", "https://github.com/o/r.git", "main")
	task := lifecycleTask("taskic1", "projic1", "repoic1", 7, "Implement")

	c := seedClient(t, proj, secret("projic1-scm", secretVal), repo)
	require.NoError(t, c.Create(context.Background(), task))
	require.NoError(t, c.Status().Update(context.Background(), task))

	postClosedIssue(t, c, "projic1", secretVal, "a-human")

	var got tatarav1.Task
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "taskic1"}, &got))
	require.Equal(t, "Parked", got.Status.DeployState, "a human closing the tracked issue must park the owning task")
}

// TestIssueClosed_BotActor_NotParked: the bot's own triageCloseIssue close (a
// legitimate close outcome) fires this same webhook event with the bot as
// sender - it must not re-enter and park/disturb the task.
func TestIssueClosed_BotActor_NotParked(t *testing.T) {
	const secretVal = "whsec"
	proj := projectWithBot("projic2", "projic2-scm", "tatara", "tatara-bot")
	repo := repository("repoic2", "projic2", "https://github.com/o/r.git", "main")
	task := lifecycleTask("taskic2", "projic2", "repoic2", 7, "Triage")

	c := seedClient(t, proj, secret("projic2-scm", secretVal), repo)
	require.NoError(t, c.Create(context.Background(), task))
	require.NoError(t, c.Status().Update(context.Background(), task))

	postClosedIssue(t, c, "projic2", secretVal, "tatara-bot")

	var got tatarav1.Task
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "taskic2"}, &got))
	require.Equal(t, "Triage", got.Status.DeployState, "the bot's own close must not park the task")
}

// TestIssueClosed_NoOwningTask_NoOp: a closed event with no owning live Task
// must be accepted as a no-op (nothing to park, nothing else created).
func TestIssueClosed_NoOwningTask_NoOp(t *testing.T) {
	const secretVal = "whsec"
	proj := projectWithBot("projic3", "projic3-scm", "tatara", "tatara-bot")
	repo := repository("repoic3", "projic3", "https://github.com/o/r.git", "main")
	c := seedClient(t, proj, secret("projic3-scm", secretVal), repo)

	postClosedIssue(t, c, "projic3", secretVal, "a-human")

	var tasks tatarav1.TaskList
	require.NoError(t, c.List(context.Background(), &tasks, client.InNamespace(ns)))
	require.Empty(t, tasks.Items, "a closed event with no owning task must not spawn one")
}

// TestIssueClosed_AlreadyTerminalTask_NotReParked: a task already Done is not
// re-touched by the closed event (idempotent no-op).
func TestIssueClosed_AlreadyTerminalTask_NotReParked(t *testing.T) {
	const secretVal = "whsec"
	proj := projectWithBot("projic4", "projic4-scm", "tatara", "tatara-bot")
	repo := repository("repoic4", "projic4", "https://github.com/o/r.git", "main")
	task := lifecycleTask("taskic4", "projic4", "repoic4", 7, "Done")

	c := seedClient(t, proj, secret("projic4-scm", secretVal), repo)
	require.NoError(t, c.Create(context.Background(), task))
	require.NoError(t, c.Status().Update(context.Background(), task))

	postClosedIssue(t, c, "projic4", secretVal, "a-human")

	var got tatarav1.Task
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "taskic4"}, &got))
	require.Equal(t, "Done", got.Status.DeployState)
}
