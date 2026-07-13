package webhook_test

// TestLabelWebhook_NeverWritesIssueStatus is the Section I fault-injection
// test: "a test asserting NO cron/sweep path writes Issue.status.status"
// (fix 16 / M23) - approval is COMMENT-ONLY. A labeled webhook, with no
// accompanying comment, must never touch the new-style Issue CR's
// status.status field. There is no label -> status path at all.

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func TestLabelWebhook_NeverWritesIssueStatus(t *testing.T) {
	const secretVal = "whsec-lbl1"
	proj := &tatarav1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "projlbl1", Namespace: ns},
		Spec: tatarav1.ProjectSpec{
			ScmSecretRef: "projlbl1-scm",
			TriggerLabel: "tatara",
			Scm: &tatarav1.ScmSpec{
				Provider: "github", Owner: "o", BotLogin: "tatara-bot",
				MaintainerLogins: []string{"maintainer"},
			},
		},
	}
	repo := repository("repolbl1", "projlbl1", "https://github.com/o/r.git", "main")

	iss := &tatarav1.Issue{
		ObjectMeta: metav1.ObjectMeta{Name: tatarav1.IssueName("repolbl1", 7), Namespace: ns},
		Spec: tatarav1.IssueSpec{
			RepositoryRef: "repolbl1", Number: 7, ProjectRef: "projlbl1",
			URL: "https://github.com/o/r/issues/7",
		},
		Status: tatarav1.IssueStatus{State: "open", Status: "new"},
	}

	c := seedClient(t, proj, secret("projlbl1-scm", secretVal), repo, iss)
	h, _ := newServer(t, c)

	// A maintainer applying an ARBITRARY label (not a comment) to the issue.
	body := []byte(`{"action":"labeled","sender":{"login":"maintainer"},` +
		`"label":{"name":"bug"},` +
		`"issue":{"number":7,"title":"Fix","body":"please fix","user":{"login":"reporter"},` +
		`"labels":[{"name":"bug"}],"html_url":"https://github.com/o/r/issues/7"},` +
		`"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issues")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "projlbl1", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var got tatarav1.Issue
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: iss.Name}, &got))
	require.Equal(t, "new", got.Status.Status, "no webhook path may write Issue.status.status from a LABEL - approval is comment-only")
}
