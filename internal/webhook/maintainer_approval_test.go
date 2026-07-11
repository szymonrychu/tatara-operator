package webhook_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// approvedLabelBody builds a GitHub issues.labeled webhook body applying the
// tatara-approved label to issue #7, with the given event sender (actor).
func approvedLabelBody(senderLogin string) []byte {
	return []byte(`{"action":"labeled","sender":{"login":"` + senderLogin + `"},` +
		`"label":{"name":"tatara-approved"},` +
		`"issue":{"number":7,"title":"Fix","body":"please fix","user":{"login":"reporter"},` +
		`"labels":[{"name":"tatara"},{"name":"tatara-approved"}],` +
		`"html_url":"https://github.com/o/r/issues/7"},` +
		`"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
}

func postApprovedLabel(t *testing.T, c client.Client, projName, secretVal, sender string) {
	t.Helper()
	h, _ := newServer(t, c)
	body := approvedLabelBody(sender)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issues")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))
	w := post(t, h, projName, hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)
}

// TestApprovedLabel_MaintainerActor_RecordsApproval: a MaintainerLogins member
// applying the approved label records a verified approval on the owning
// front-half Task and re-drives it back to Triage so the implement gate
// re-evaluates.
func TestApprovedLabel_MaintainerActor_RecordsApproval(t *testing.T) {
	const secretVal = "whsec"
	proj := projectWithBot("projma1", "projma1-scm", "tatara", "tatara-bot")
	proj.Spec.Scm.MaintainerLogins = []string{"szymon"}
	repo := repository("repoma1", "projma1", "https://github.com/o/r.git", "main")
	task := lifecycleTask("taskma1", "projma1", "repoma1", 7, "Conversation")

	c := seedClient(t, proj, secret("projma1-scm", secretVal), repo)
	require.NoError(t, c.Create(context.Background(), task))
	require.NoError(t, c.Status().Update(context.Background(), task))

	postApprovedLabel(t, c, "projma1", secretVal, "szymon")

	var got tatarav1.Task
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "taskma1"}, &got))
	require.Equal(t, "szymon", got.Status.ApprovedByMaintainer, "maintainer approved-label must record the verified approval")
	require.Equal(t, "Triage", got.Status.DeployState, "recording approval must re-drive the front half back to Triage")
}

// TestApprovedLabel_NonMaintainerActor_NotRecorded: a non-maintainer applying the
// approved label is NOT a verified approval - nothing is recorded and the task
// stays put (an agent/reporter cannot self-approve by setting the label).
func TestApprovedLabel_NonMaintainerActor_NotRecorded(t *testing.T) {
	const secretVal = "whsec"
	proj := projectWithBot("projma2", "projma2-scm", "tatara", "tatara-bot")
	proj.Spec.Scm.MaintainerLogins = []string{"szymon"} // "rando" is not a maintainer
	repo := repository("repoma2", "projma2", "https://github.com/o/r.git", "main")
	task := lifecycleTask("taskma2", "projma2", "repoma2", 7, "Conversation")

	c := seedClient(t, proj, secret("projma2-scm", secretVal), repo)
	require.NoError(t, c.Create(context.Background(), task))
	require.NoError(t, c.Status().Update(context.Background(), task))

	postApprovedLabel(t, c, "projma2", secretVal, "rando")

	var got tatarav1.Task
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "taskma2"}, &got))
	require.Empty(t, got.Status.ApprovedByMaintainer, "non-maintainer approved-label must NOT record an approval")
	require.Equal(t, "Conversation", got.Status.DeployState, "non-maintainer approved-label must not re-drive the task")
}

// TestApprovedLabel_BotActor_NotRecorded: the operator/agent setting the approved
// label itself is the bot actor - dropped by the bot-actor guard, never recorded.
// This is the structural exclusion of agents/bots from the approval gate.
func TestApprovedLabel_BotActor_NotRecorded(t *testing.T) {
	const secretVal = "whsec"
	proj := projectWithBot("projma3", "projma3-scm", "tatara", "tatara-bot")
	proj.Spec.Scm.MaintainerLogins = []string{"tatara-bot"} // even if misconfigured as a maintainer
	repo := repository("repoma3", "projma3", "https://github.com/o/r.git", "main")
	task := lifecycleTask("taskma3", "projma3", "repoma3", 7, "Conversation")

	c := seedClient(t, proj, secret("projma3-scm", secretVal), repo)
	require.NoError(t, c.Create(context.Background(), task))
	require.NoError(t, c.Status().Update(context.Background(), task))

	postApprovedLabel(t, c, "projma3", secretVal, "tatara-bot")

	var got tatarav1.Task
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "taskma3"}, &got))
	require.Empty(t, got.Status.ApprovedByMaintainer, "bot-actor approved-label must never record an approval")
	require.Equal(t, "Conversation", got.Status.DeployState, "bot-actor approved-label must not re-drive the task")
}
