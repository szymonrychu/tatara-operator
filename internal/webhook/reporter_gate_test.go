package webhook_test

// Tests for the issue #102 reporter intake gate: when ScmSpec.ReporterLogins is
// configured, the operator only acts on issues / issue-comments authored by the
// bot, a maintainer, or a listed reporter; everything else is dropped at intake.

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func projectWithReporters(name, secretRef, trigger, bot string, reporters []string) *tatarav1.Project {
	return &tatarav1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: tatarav1.ProjectSpec{
			ScmSecretRef: secretRef,
			TriggerLabel: trigger,
			Scm: &tatarav1.ScmSpec{
				Provider: "github", Owner: "o", BotLogin: bot,
				ReporterLogins: reporters,
			},
		},
	}
}

func issueOpenedBy(author string) []byte {
	return []byte(`{"action":"opened","sender":{"login":"` + author + `"},"issue":{"number":7,"title":"Fix the bug","body":"please fix","user":{"login":"` + author + `"},"labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/issues/7"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
}

func issueCommentBy(author string) []byte {
	return []byte(`{"action":"created","issue":{"number":9,"title":"old bug","body":"still broken","html_url":"https://github.com/o/r/issues/9"},"comment":{"id":5,"body":"please implement now","user":{"login":"` + author + `"}},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},"sender":{"login":"` + author + `"}}`)
}

func TestReporterGate_IssueFromNonReporter_Dropped(t *testing.T) {
	const secretVal = "whsec-rg1"
	c := seedClient(t,
		projectWithReporters("projrg1", "projrg1-scm", "tatara", "bot", []string{"alice"}),
		secret("projrg1-scm", secretVal),
		repository("reporg1", "projrg1", "https://github.com/o/r.git", "main"),
	)
	h, reg := newServer(t, c)

	body := issueOpenedBy("mallory")
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issues")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "projrg1", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	require.Empty(t, qel.Items, "issue from a non-reporter must not create a task")
	require.Equal(t, 1.0, counterValue(t, reg, "operator_webhook_events_total",
		map[string]string{"provider": "github", "kind": "issue", "action": "opened", "result": "ignored"}))
}

func TestReporterGate_IssueFromReporter_Accepted(t *testing.T) {
	const secretVal = "whsec-rg2"
	c := seedClient(t,
		projectWithReporters("projrg2", "projrg2-scm", "tatara", "bot", []string{"alice"}),
		secret("projrg2-scm", secretVal),
		repository("reporg2", "projrg2", "https://github.com/o/r.git", "main"),
	)
	h, _ := newServer(t, c)

	body := issueOpenedBy("alice")
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issues")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "projrg2", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	require.Len(t, qel.Items, 1, "issue from a listed reporter must create a task")
}

func TestReporterGate_CommentFromNonReporter_Ignored(t *testing.T) {
	const secretVal = "whsec-rg3"
	c := seedClient(t,
		projectWithReporters("projrg3", "projrg3-scm", "tatara", "bot", []string{"alice"}),
		secret("projrg3-scm", secretVal),
		repository("reporg3", "projrg3", "https://github.com/o/r.git", "main"),
	)
	h, reg := newServer(t, c)

	body := issueCommentBy("mallory")
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issue_comment")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "projrg3", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	require.Empty(t, qel.Items, "comment from a non-reporter must not drive the lifecycle")
	require.Equal(t, 1.0, counterValue(t, reg, "operator_webhook_events_total",
		map[string]string{"provider": "github", "kind": "issue", "action": "created", "result": "ignored"}))
}

func TestReporterGate_CommentFromReporter_CreatesTask(t *testing.T) {
	const secretVal = "whsec-rg4"
	c := seedClient(t,
		projectWithReporters("projrg4", "projrg4-scm", "tatara", "bot", []string{"alice"}),
		secret("projrg4-scm", secretVal),
		repository("reporg4", "projrg4", "https://github.com/o/r.git", "main"),
	)
	h, _ := newServer(t, c)

	body := issueCommentBy("alice")
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issue_comment")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "projrg4", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	require.Len(t, qel.Items, 1, "comment from a listed reporter must create a triage task")
}
