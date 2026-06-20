package webhook_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func TestWebhookIssue_SetsSourceTitle(t *testing.T) {
	const secretVal = "whsec"
	c := seedClient(t,
		project("projt", "projt-scm", "tatara"),
		secret("projt-scm", secretVal),
		repository("repot", "projt", "https://github.com/o/r.git", "main"),
	)
	h, _ := newServer(t, c)

	body := []byte(`{"action":"opened","issue":{"number":42,"title":"Fix flaky CI on push events","body":"some body","labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/issues/42"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issues")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "projt", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	require.Len(t, qel.Items, 1)
	require.NotNil(t, qel.Items[0].Spec.Payload.Source)
	require.Equal(t, "Fix flaky CI on push events", qel.Items[0].Spec.Payload.Source.Title)
}

func TestWebhookIssueComment_SetsSourceTitle(t *testing.T) {
	const secretVal = "whsec2"
	c := seedClient(t,
		project("projtc", "projtc-scm", "tatara"),
		secret("projtc-scm", secretVal),
		repository("repojtc", "projtc", "https://github.com/o/r.git", "main"),
	)
	h, _ := newServer(t, c)

	// issue_comment on an issue that has the trigger label - triggers issueLifecycle
	body := []byte(`{"action":"created","issue":{"number":55,"title":"Add metrics endpoint","body":"issue body","labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/issues/55"},"comment":{"id":1,"body":"please proceed"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},"sender":{"login":"user"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issue_comment")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "projtc", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	require.GreaterOrEqual(t, len(qel.Items), 1)
	found := false
	for _, qe := range qel.Items {
		if qe.Spec.ProjectRef == "projtc" && qe.Spec.Payload.Source != nil {
			require.Equal(t, "Add metrics endpoint", qe.Spec.Payload.Source.Title)
			found = true
		}
	}
	require.True(t, found, "no QueuedEvent with Source.Title found")
}
