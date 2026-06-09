package webhook_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// proposalTask is a bot-authored, approval-required proposal Task matching a
// given issue ref, the kind of Task the approval flip should release.
func proposalTask(name, projectRef, issueRef, botLogin string) *tatarav1.Task {
	return &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: tatarav1.TaskSpec{
			ProjectRef:       projectRef,
			RepositoryRef:    "scmrepo",
			Goal:             "fix",
			Kind:             "implement",
			ApprovalRequired: true,
			ProposedIssue:    &tatarav1.ProposedIssueSpec{RepositoryRef: "scmrepo", Title: "T", Body: "B", Kind: "bug"},
			Source:           &tatarav1.TaskSource{Provider: "github", IssueRef: issueRef, AuthorLogin: botLogin},
		},
	}
}

func approvalCondTrue(t *testing.T, c client.Client, taskName string) bool {
	t.Helper()
	var tk tatarav1.Task
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: taskName}, &tk))
	cond := apimeta.FindStatusCondition(tk.Status.Conditions, tatarav1.ConditionApprovalApproved)
	return cond != nil && cond.Status == metav1.ConditionTrue
}

func TestApprovalFlipGating(t *testing.T) {
	const botLogin = "tatara-bot"

	// unlabeled payload: actor (sender) is the human removing the label, the
	// issue author is the bot (reliable on GitHub).
	flipBody := func(actor, author string) []byte {
		return []byte(`{"action":"unlabeled","sender":{"login":"` + actor + `"},"label":{"name":"tatara/awaiting-approval"},"issue":{"number":7,"title":"T","body":"B","user":{"login":"` + author + `"},"labels":[],"html_url":"https://github.com/o/r/issues/7"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
	}

	t.Run("human removes label on a bot proposal -> flip", func(t *testing.T) {
		proj := newProjectWithScm(t, botLogin, "labeledOrMentioned")
		repo := newRepo(t, proj.Name, "https://github.com/o/r.git")
		task := proposalTask("flip-yes", proj.Name, "o/r#7", botLogin)
		srv, c := newWebhookServer(t, proj, repo, task)
		h := srv.Server.Handler()

		body := flipBody("human", botLogin)
		hdr := http.Header{}
		hdr.Set("X-GitHub-Event", "issues")
		hdr.Set("X-Hub-Signature-256", ghSign("whsec", body))

		w := post(t, h, proj.Name, hdr, body)
		require.Equal(t, http.StatusAccepted, w.Code)
		require.True(t, approvalCondTrue(t, c, task.Name), "human removing label on bot proposal must flip approval")
	})

	t.Run("bot removes its own label -> no flip", func(t *testing.T) {
		proj := newProjectWithScm(t, botLogin, "labeledOrMentioned")
		repo := newRepo(t, proj.Name, "https://github.com/o/r.git")
		task := proposalTask("flip-bot", proj.Name, "o/r#7", botLogin)
		srv, c := newWebhookServer(t, proj, repo, task)
		h := srv.Server.Handler()

		body := flipBody(botLogin, botLogin)
		hdr := http.Header{}
		hdr.Set("X-GitHub-Event", "issues")
		hdr.Set("X-Hub-Signature-256", ghSign("whsec", body))

		w := post(t, h, proj.Name, hdr, body)
		require.Equal(t, http.StatusAccepted, w.Code)
		require.False(t, approvalCondTrue(t, c, task.Name), "bot removing its own label must NOT flip approval")
	})

	t.Run("human removes label on a non-proposal/human issue -> no flip", func(t *testing.T) {
		proj := newProjectWithScm(t, botLogin, "labeledOrMentioned")
		repo := newRepo(t, proj.Name, "https://github.com/o/r.git")
		// Task is not a proposal: no ProposedIssue, source author is a human.
		humanTask := &tatarav1.Task{
			ObjectMeta: metav1.ObjectMeta{Name: "flip-human", Namespace: ns},
			Spec: tatarav1.TaskSpec{
				ProjectRef: proj.Name, RepositoryRef: "scmrepo", Goal: "g", Kind: "implement",
				Source: &tatarav1.TaskSource{Provider: "github", IssueRef: "o/r#7", AuthorLogin: "human"},
			},
		}
		srv, c := newWebhookServer(t, proj, repo, humanTask)
		h := srv.Server.Handler()

		// Human author issue (issue.user.login != bot).
		body := flipBody("human", "human")
		hdr := http.Header{}
		hdr.Set("X-GitHub-Event", "issues")
		hdr.Set("X-Hub-Signature-256", ghSign("whsec", body))

		w := post(t, h, proj.Name, hdr, body)
		require.Equal(t, http.StatusAccepted, w.Code)
		require.False(t, approvalCondTrue(t, c, humanTask.Name), "non-proposal human issue must NOT flip approval")
	})
}
