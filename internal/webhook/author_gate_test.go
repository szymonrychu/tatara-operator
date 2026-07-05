package webhook_test

// Tests for the trusted-author bypass: maintainers and listed reporters skip the
// trigger-label (issue) and prReactionScope (PR) gates so their issues/PRs process
// on create/update without a manual "tatara" label. Third-party behavior unchanged.

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// glUnlabeledMRByActor builds a minimal GitLab Merge Request Hook payload where
// the event actor (user.username) is the given login and there are no labels.
// On GitLab, AuthorLogin == ActorLogin in the webhook payload, so this simulates
// a trusted maintainer performing an update action on any MR (including a
// third-party one) - the server must NOT treat the actor as the resource author.
func glUnlabeledMRByActor(actor string) []byte {
	return []byte(`{"object_kind":"merge_request","user":{"username":"` + actor + `"}` +
		`,"project":{"git_http_url":"https://gitlab.com/o/r.git","path_with_namespace":"o/r"}` +
		`,"object_attributes":{"iid":77,"title":"third-party MR","description":"text"` +
		`,"url":"https://gitlab.com/o/r/-/merge_requests/77","action":"update"` +
		`,"source_branch":"feature","last_commit":{"id":"aaa"}}` +
		`,"labels":[],"changes":{}}`)
}

func projectWithMaintainer(name, secretRef, trigger, bot string, maintainers []string) *tatarav1.Project {
	return &tatarav1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: tatarav1.ProjectSpec{
			ScmSecretRef: secretRef,
			TriggerLabel: trigger,
			Scm: &tatarav1.ScmSpec{
				Provider:         "github",
				Owner:            "o",
				BotLogin:         bot,
				PRReactionScope:  "labeledOrMentioned",
				MaintainerLogins: maintainers,
			},
		},
	}
}

func unlabeledIssueBy(author string) []byte {
	return []byte(`{"action":"opened","sender":{"login":"` + author + `"},"issue":{"number":20,"title":"Fix bug","body":"text","user":{"login":"` + author + `"},"labels":[],"html_url":"https://github.com/o/r/issues/20"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
}

func unlabeledUnmentionedPRBy(author string) []byte {
	return []byte(`{"action":"opened","sender":{"login":"` + author + `"},"pull_request":{"number":21,"title":"PR","body":"just a PR","user":{"login":"` + author + `"},"labels":[],"html_url":"https://github.com/o/r/pull/21","head":{"sha":"aaa","ref":"fix-branch"}},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
}

func TestTrustedAuthorGate_IssueFromMaintainer_BypassesLabelGate(t *testing.T) {
	const secretVal = "whsec-ag1"
	c := seedClient(t,
		projectWithMaintainer("projag1", "projag1-scm", "tatara", "bot", []string{"szymonrychu"}),
		secret("projag1-scm", secretVal),
		repository("repoag1", "projag1", "https://github.com/o/r.git", "main"),
	)
	h, reg := newServer(t, c)

	body := unlabeledIssueBy("szymonrychu")
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issues")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "projag1", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	require.Len(t, qel.Items, 1, "unlabeled issue from a maintainer must bypass the label gate and create a task")
	require.Equal(t, 1.0, counterValue(t, reg, "operator_webhook_events_total",
		map[string]string{"provider": "github", "kind": "issue", "action": "opened", "result": "task_created"}))
}

func TestTrustedAuthorGate_IssueFromThirdParty_StillIgnored(t *testing.T) {
	const secretVal = "whsec-ag2"
	c := seedClient(t,
		projectWithMaintainer("projag2", "projag2-scm", "tatara", "bot", []string{"szymonrychu"}),
		secret("projag2-scm", secretVal),
		repository("repoag2", "projag2", "https://github.com/o/r.git", "main"),
	)
	h, reg := newServer(t, c)

	body := unlabeledIssueBy("randouser")
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issues")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "projag2", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	require.Empty(t, qel.Items, "unlabeled issue from a third party must still be ignored")
	require.Equal(t, 1.0, counterValue(t, reg, "operator_webhook_events_total",
		map[string]string{"provider": "github", "kind": "issue", "action": "opened", "result": "ignored"}))
}

func TestTrustedAuthorGate_PRFromMaintainer_BypassesScopeGate(t *testing.T) {
	const secretVal = "whsec-ag3"
	c := seedClient(t,
		projectWithMaintainer("projag3", "projag3-scm", "tatara", "bot", []string{"szymonrychu"}),
		secret("projag3-scm", secretVal),
		repository("repoag3", "projag3", "https://github.com/o/r.git", "main"),
	)
	h, reg := newServer(t, c)

	body := unlabeledUnmentionedPRBy("szymonrychu")
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "pull_request")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "projag3", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	require.Len(t, qel.Items, 1, "unlabeled un-mentioned PR from a maintainer must bypass the scope gate and create a task")
	require.Equal(t, 1.0, counterValue(t, reg, "operator_webhook_events_total",
		map[string]string{"provider": "github", "kind": "mr", "action": "opened", "result": "task_created"}))
}

func TestTrustedAuthorGate_PRFromThirdParty_StillGated(t *testing.T) {
	const secretVal = "whsec-ag4"
	c := seedClient(t,
		projectWithMaintainer("projag4", "projag4-scm", "tatara", "bot", []string{"szymonrychu"}),
		secret("projag4-scm", secretVal),
		repository("repoag4", "projag4", "https://github.com/o/r.git", "main"),
	)
	h, reg := newServer(t, c)

	body := unlabeledUnmentionedPRBy("randouser")
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "pull_request")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "projag4", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	require.Empty(t, qel.Items, "unlabeled un-mentioned PR from a third party must still be gated")
	require.Equal(t, 1.0, counterValue(t, reg, "operator_webhook_events_total",
		map[string]string{"provider": "github", "kind": "mr", "action": "opened", "result": "ignored"}))
}

// TestTrustedAuthorGate_GitLabMaintainerActor_DoesNotBypassGate is a SECURITY
// regression test. On GitLab, AuthorLogin == ActorLogin (the webhook event actor),
// so a trusted maintainer performing any action on a third-party, unlabeled MR
// must NOT bypass the scope gate. Without a provider guard, IsTrustedAuthor would
// see AuthorLogin=maintainer, skip the gate, and feed a third-party payload to an
// agent (prompt-injection surface).
//
// This test MUST FAIL before the provider-guard fix (gate wrongly bypassed) and
// PASS after (gate correctly fires -> event ignored).
func TestTrustedAuthorGate_GitLabMaintainerActor_DoesNotBypassGate(t *testing.T) {
	const secretVal = "gl-maintainer-actor-secret"
	proj := &tatarav1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "projgl1", Namespace: ns},
		Spec: tatarav1.ProjectSpec{
			ScmSecretRef: "projgl1-scm",
			TriggerLabel: "tatara",
			Scm: &tatarav1.ScmSpec{
				Provider:         "gitlab",
				Owner:            "o",
				BotLogin:         "tatara-bot",
				PRReactionScope:  "labeledOrMentioned",
				MaintainerLogins: []string{"szymonrychu"},
			},
		},
	}
	c := seedClient(t,
		proj,
		secret("projgl1-scm", secretVal),
		repository("repogl1", "projgl1", "https://gitlab.com/o/r.git", "main"),
	)
	h, reg := newServer(t, c)

	body := glUnlabeledMRByActor("szymonrychu")
	hdr := http.Header{}
	hdr.Set("X-Gitlab-Event", "Merge Request Hook")
	hdr.Set("X-Gitlab-Token", secretVal)

	w := post(t, h, "projgl1", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	require.Empty(t, qel.Items,
		"GitLab MR actor==maintainer must NOT bypass the gate (actor != resource author on GitLab)")
	require.Equal(t, 1.0, counterValue(t, reg, "operator_webhook_events_total",
		map[string]string{"provider": "gitlab", "kind": "mr", "action": "synchronize", "result": "ignored"}))
}

// TestTrustedAuthorGate_GitHubBotPR_IsProcessed documents the bot-PR webhook
// path on GitHub: when the bot account opens an unlabeled, un-mentioned PR and
// is also listed as a trusted maintainer, the provider-guarded trusted bypass
// fires (provider=="github") and the PR is processed rather than ignored.
func TestTrustedAuthorGate_GitHubBotPR_IsProcessed(t *testing.T) {
	const secretVal = "whsec-ag5"
	c := seedClient(t,
		projectWithMaintainer("projag5", "projag5-scm", "tatara", "tatara-bot", []string{"tatara-bot"}),
		secret("projag5-scm", secretVal),
		repository("repoag5", "projag5", "https://github.com/o/r.git", "main"),
	)
	h, reg := newServer(t, c)

	body := unlabeledUnmentionedPRBy("tatara-bot")
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "pull_request")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "projag5", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	require.Len(t, qel.Items, 1,
		"GitHub bot-authored PR must be processed when bot is a trusted maintainer")
	require.Equal(t, 1.0, counterValue(t, reg, "operator_webhook_events_total",
		map[string]string{"provider": "github", "kind": "mr", "action": "opened", "result": "task_created"}))
}
