package webhook_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// issueLabeledBody builds a GitHub issues.labeled webhook body with the given
// event sender (actor) and issue author, carrying the trigger label so it passes
// the trigger-label gate.
func issueLabeledBody(senderLogin, authorLogin string) []byte {
	return []byte(`{"action":"labeled","sender":{"login":"` + senderLogin + `"},` +
		`"label":{"name":"tatara-implementation"},` +
		`"issue":{"number":7,"title":"Fix","body":"please fix","user":{"login":"` + authorLogin + `"},` +
		`"labels":[{"name":"tatara"},{"name":"tatara-implementation"}],` +
		`"html_url":"https://github.com/o/r/issues/7"},` +
		`"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
}

// TestIssueLabeled_BotActor_NoTaskCreated is the finding-3 (first half) regression:
// when the operator itself flips a managed phase label (e.g. tatara-implementation
// on a clarify->implement handoff), GitHub echoes issues.labeled with sender==bot.
// That self-write must be dropped, NOT spawn a fresh clarify Task (which would
// re-stamp brainstorming while an implement Task already owns the issue).
func TestIssueLabeled_BotActor_NoTaskCreated(t *testing.T) {
	proj := newProjectWithScm(t, "tatara-bot", "labeledOrMentioned")
	repo := repository("scmrepo", proj.Name, "https://github.com/o/r.git", "main")
	srv, c := newWebhookServer(t, proj, repo)
	h := srv.Server.Handler()

	body := issueLabeledBody("tatara-bot", "alice") // sender == bot (operator self-write)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issues")
	hdr.Set("X-Hub-Signature-256", ghSign("whsec", body))

	w := post(t, h, proj.Name, hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	require.Empty(t, allQEs(t, c, proj.Name),
		"a bot-actor issues.labeled (operator self-write) must not enqueue any task")
	require.Empty(t, allTasks(t, c, proj.Name))
}

// TestIssueLabeled_HumanActor_TaskCreated is the positive control: the same labeled
// event from a HUMAN actor is a genuine trigger and still creates a clarify Task -
// the bot-actor guard must not over-block human label actions.
func TestIssueLabeled_HumanActor_TaskCreated(t *testing.T) {
	proj := newProjectWithScm(t, "tatara-bot", "labeledOrMentioned")
	repo := repository("scmrepo", proj.Name, "https://github.com/o/r.git", "main")
	srv, c := newWebhookServer(t, proj, repo)
	h := srv.Server.Handler()

	body := issueLabeledBody("alice", "alice") // human actor
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issues")
	hdr.Set("X-Hub-Signature-256", ghSign("whsec", body))

	w := post(t, h, proj.Name, hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	qe := singleQueuedEvent(t, c, proj.Name)
	require.Equal(t, "clarify", qe.Spec.Payload.Kind)
}
