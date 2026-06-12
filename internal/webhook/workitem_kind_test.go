package webhook_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/webhook"
)

func newProjectWithScm(t *testing.T, botLogin, prReactionScope string) *tatarav1.Project {
	t.Helper()
	return &tatarav1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "scmproj", Namespace: ns},
		Spec: tatarav1.ProjectSpec{
			ScmSecretRef: "scmproj-scm",
			TriggerLabel: "tatara",
			Scm: &tatarav1.ScmSpec{
				Provider:        "github",
				Owner:           "o",
				BotLogin:        botLogin,
				PRReactionScope: prReactionScope,
				ApprovalLabel:   "tatara/awaiting-approval",
			},
		},
	}
}

func newRepo(t *testing.T, projectRef, url string) *tatarav1.Repository {
	t.Helper()
	return &tatarav1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "scmrepo", Namespace: ns},
		Spec:       tatarav1.RepositorySpec{ProjectRef: projectRef, URL: url, DefaultBranch: "main"},
	}
}

func newWebhookServer(t *testing.T, objs ...client.Object) (webhook.ExposedServer, client.Client) {
	t.Helper()
	sec := secret("scmproj-scm", "whsec")
	allObjs := append([]client.Object{sec}, objs...)
	c := seedClient(t, allObjs...)
	reg := prometheus.NewRegistry()
	srv := webhook.NewServer(webhook.Config{
		Client:    c,
		Namespace: ns,
		Metrics:   obs.NewOperatorMetrics(reg),
	})
	return webhook.ExposedServer{Server: srv}, c
}

func singleTask(t *testing.T, c client.Client, projectName string) tatarav1.Task {
	t.Helper()
	var tasks tatarav1.TaskList
	require.NoError(t, c.List(context.Background(), &tasks, client.InNamespace(ns)))
	var matching []tatarav1.Task
	for _, tk := range tasks.Items {
		if tk.Spec.ProjectRef == projectName {
			matching = append(matching, tk)
		}
	}
	require.Len(t, matching, 1, "expected exactly one task for project %q", projectName)
	return matching[0]
}

func TestHandleWorkItemKind(t *testing.T) {
	t.Run("issue with trigger label -> issueLifecycle task entering at Implement, ApprovalRequired=false", func(t *testing.T) {
		proj := newProjectWithScm(t, "tatara-bot", "labeledOrMentioned")
		repo := newRepo(t, proj.Name, "https://github.com/o/r.git")
		srv, c := newWebhookServer(t, proj, repo)
		h := srv.Server.Handler()

		body := []byte(`{"action":"labeled","sender":{"login":"alice"},"label":{"name":"tatara"},"issue":{"number":7,"title":"Fix","body":"please fix","user":{"login":"alice"},"labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/issues/7"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
		hdr := http.Header{}
		hdr.Set("X-GitHub-Event", "issues")
		hdr.Set("X-Hub-Signature-256", ghSign("whsec", body))

		w := post(t, h, proj.Name, hdr, body)
		require.Equal(t, http.StatusAccepted, w.Code)

		tk := singleTask(t, c, proj.Name)
		// kind switch: was "implement", now "issueLifecycle" (migration note: in-flight
		// "implement" tasks created before this deploy still complete via old writeback arm)
		require.Equal(t, "issueLifecycle", tk.Spec.Kind)
		require.Equal(t, "Implement", tk.Status.LifecycleState)
		require.False(t, tk.Spec.ApprovalRequired)
		require.NotNil(t, tk.Spec.Source)
		require.Equal(t, "alice", tk.Spec.Source.AuthorLogin)
		require.False(t, tk.Spec.Source.IsPR)
		require.Equal(t, 7, tk.Spec.Source.Number)
	})

	t.Run("PR opened by botLogin -> issueLifecycle MRCI task", func(t *testing.T) {
		proj := newProjectWithScm(t, "tatara-bot", "labeledOrMentioned")
		repo := newRepo(t, proj.Name, "https://github.com/o/r.git")
		srv, c := newWebhookServer(t, proj, repo)
		h := srv.Server.Handler()

		body := []byte(`{"action":"opened","sender":{"login":"tatara-bot"},"pull_request":{"number":9,"title":"PR","body":"body","user":{"login":"tatara-bot"},"labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/pull/9","head":{"sha":"deadbeef","ref":"feature"}},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
		hdr := http.Header{}
		hdr.Set("X-GitHub-Event", "pull_request")
		hdr.Set("X-Hub-Signature-256", ghSign("whsec", body))

		w := post(t, h, proj.Name, hdr, body)
		require.Equal(t, http.StatusAccepted, w.Code)

		tk := singleTask(t, c, proj.Name)
		// kind switch: was "selfImprove", now "issueLifecycle" (migration note: in-flight
		// "selfImprove" tasks created before this deploy still complete via old writeback arm)
		require.Equal(t, "issueLifecycle", tk.Spec.Kind)
		require.Equal(t, "MRCI", tk.Status.LifecycleState)
		require.Equal(t, 9, tk.Status.PRNumber)
		require.False(t, tk.Spec.ApprovalRequired)
		require.NotNil(t, tk.Spec.Source)
		require.Equal(t, "tatara-bot", tk.Spec.Source.AuthorLogin)
		require.True(t, tk.Spec.Source.IsPR)
		require.Equal(t, 9, tk.Spec.Source.Number)
	})

	t.Run("human PR with trigger label -> review task", func(t *testing.T) {
		proj := newProjectWithScm(t, "tatara-bot", "labeledOrMentioned")
		repo := newRepo(t, proj.Name, "https://github.com/o/r.git")
		srv, c := newWebhookServer(t, proj, repo)
		h := srv.Server.Handler()

		body := []byte(`{"action":"opened","sender":{"login":"alice"},"pull_request":{"number":9,"title":"PR","body":"body","user":{"login":"alice"},"labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/pull/9","head":{"sha":"deadbeef","ref":"feature"}},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
		hdr := http.Header{}
		hdr.Set("X-GitHub-Event", "pull_request")
		hdr.Set("X-Hub-Signature-256", ghSign("whsec", body))

		w := post(t, h, proj.Name, hdr, body)
		require.Equal(t, http.StatusAccepted, w.Code)

		tk := singleTask(t, c, proj.Name)
		require.Equal(t, "review", tk.Spec.Kind)
		require.False(t, tk.Spec.ApprovalRequired)
		require.NotNil(t, tk.Spec.Source)
		require.Equal(t, "alice", tk.Spec.Source.AuthorLogin)
		require.True(t, tk.Spec.Source.IsPR)
		require.Equal(t, 9, tk.Spec.Source.Number)
	})

	t.Run("human PR without trigger label or mention -> no task (gated)", func(t *testing.T) {
		proj := newProjectWithScm(t, "tatara-bot", "labeledOrMentioned")
		repo := newRepo(t, proj.Name, "https://github.com/o/r.git")
		srv, c := newWebhookServer(t, proj, repo)
		h := srv.Server.Handler()

		body := []byte(`{"action":"opened","sender":{"login":"alice"},"pull_request":{"number":11,"title":"PR","body":"just a PR","labels":[],"html_url":"https://github.com/o/r/pull/11","head":{"sha":"abc","ref":"fix-branch"}},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
		hdr := http.Header{}
		hdr.Set("X-GitHub-Event", "pull_request")
		hdr.Set("X-Hub-Signature-256", ghSign("whsec", body))

		w := post(t, h, proj.Name, hdr, body)
		require.Equal(t, http.StatusAccepted, w.Code)

		var tasks tatarav1.TaskList
		require.NoError(t, c.List(context.Background(), &tasks, client.InNamespace(ns)))
		require.Empty(t, tasks.Items, "gated PR must not create a task")
	})
}
