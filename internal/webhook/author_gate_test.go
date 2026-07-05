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
