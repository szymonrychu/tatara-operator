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

func issueCommentBy(author string) []byte {
	return []byte(`{"action":"created","issue":{"number":9,"title":"old bug","body":"still broken","html_url":"https://github.com/o/r/issues/9"},"comment":{"id":5,"body":"please implement now","user":{"login":"` + author + `"}},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},"sender":{"login":"` + author + `"}}`)
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

// TestReporterGate_CommentFromReporter_Delivered verifies that a comment from
// a listed reporter clears the reporter gate and reaches deliverPendingEvent
// (result=accepted), instead of being dropped as ignored. The webhook mints no
// Task itself (B.4 sweep owns intake); this only proves the gate passed.
func TestReporterGate_CommentFromReporter_Delivered(t *testing.T) {
	const secretVal = "whsec-rg4"
	c := seedClient(t,
		projectWithReporters("projrg4", "projrg4-scm", "tatara", "bot", []string{"alice"}),
		secret("projrg4-scm", secretVal),
		repository("reporg4", "projrg4", "https://github.com/o/r.git", "main"),
	)
	h, reg := newServer(t, c)

	body := issueCommentBy("alice")
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issue_comment")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "projrg4", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	require.Equal(t, 1.0, counterValue(t, reg, "operator_webhook_events_total",
		map[string]string{"provider": "github", "kind": "issue", "action": "created", "result": "accepted"}),
		"comment from a listed reporter must clear the gate (result=accepted)")
	require.Equal(t, 0.0, counterValue(t, reg, "operator_webhook_events_total",
		map[string]string{"provider": "github", "kind": "issue", "action": "created", "result": "ignored"}))
}
