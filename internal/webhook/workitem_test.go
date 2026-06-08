package webhook_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

func TestIssueWithTriggerLabelCreatesTask(t *testing.T) {
	const secretVal = "whsec"
	c := seedClient(t,
		project("proj1wi", "proj1wi-scm", "tatara"),
		secret("proj1wi-scm", secretVal),
		repository("repo1wi", "proj1wi", "https://github.com/o/r.git", "main"),
	)
	h, reg := newServer(t, c)

	body := []byte(`{"action":"opened","issue":{"number":7,"title":"Fix the bug","body":"please fix","labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/issues/7"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issues")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "proj1wi", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var tasks tatarav1.TaskList
	require.NoError(t, c.List(context.Background(), &tasks, client.InNamespace(ns)))
	require.Len(t, tasks.Items, 1)
	tk := tasks.Items[0]
	require.Equal(t, "proj1wi", tk.Spec.ProjectRef)
	require.Equal(t, "repo1wi", tk.Spec.RepositoryRef)
	require.Equal(t, "please fix", tk.Spec.Goal)
	require.NotNil(t, tk.Spec.Source)
	require.Equal(t, "github", tk.Spec.Source.Provider)
	require.Equal(t, "o/r#7", tk.Spec.Source.IssueRef)
	require.Equal(t, "https://github.com/o/r/issues/7", tk.Spec.Source.URL)
	// owner-ref'd to the Project
	require.Len(t, tk.OwnerReferences, 1)
	require.Equal(t, "Project", tk.OwnerReferences[0].Kind)
	require.Equal(t, "proj1wi", tk.OwnerReferences[0].Name)

	require.Equal(t, 1.0, counterValue(t, reg, "operator_webhook_events_total", map[string]string{"provider": "github", "kind": "issue", "result": "task_created"}))
}

func TestDuplicateIssueEventDoesNotCreateSecondTask(t *testing.T) {
	const secretVal = "whsec"
	c := seedClient(t,
		project("proj1wi", "proj1wi-scm", "tatara"),
		secret("proj1wi-scm", secretVal),
		repository("repo1wi", "proj1wi", "https://github.com/o/r.git", "main"),
	)
	h, reg := newServer(t, c)

	body := []byte(`{"action":"opened","issue":{"number":7,"title":"Fix the bug","body":"please fix","labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/issues/7"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issues")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	// Creating an issue with the label fires both issues.opened and
	// issues.labeled for the SAME issue. Only one Task should result.
	require.Equal(t, http.StatusAccepted, post(t, h, "proj1wi", hdr, body).Code)
	require.Equal(t, http.StatusAccepted, post(t, h, "proj1wi", hdr, body).Code)

	var tasks tatarav1.TaskList
	require.NoError(t, c.List(context.Background(), &tasks, client.InNamespace(ns)))
	require.Len(t, tasks.Items, 1, "duplicate issue event must not create a second task")
	require.Equal(t, 1.0, counterValue(t, reg, "operator_webhook_events_total", map[string]string{"provider": "github", "kind": "issue", "result": "duplicate"}))
}

func TestWorkItemNoLabelNoTask(t *testing.T) {
	const secretVal = "whsec"
	c := seedClient(t,
		project("proj2wi", "proj2wi-scm", "tatara"),
		secret("proj2wi-scm", secretVal),
		repository("repo2wi", "proj2wi", "https://github.com/o/r.git", "main"),
	)
	h, _ := newServer(t, c)
	body := []byte(`{"action":"opened","issue":{"number":8,"title":"x","body":"y","labels":[{"name":"bug"}],"html_url":"https://github.com/o/r/issues/8"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issues")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "proj2wi", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)
	var tasks tatarav1.TaskList
	require.NoError(t, c.List(context.Background(), &tasks, client.InNamespace(ns)))
	require.Empty(t, tasks.Items)
}

func TestWorkItemLabeledButNoRepoMatch(t *testing.T) {
	const secretVal = "whsec"
	c := seedClient(t,
		project("proj3wi", "proj3wi-scm", "tatara"),
		secret("proj3wi-scm", secretVal),
		repository("repo3wi", "proj3wi", "https://github.com/o/OTHER.git", "main"),
	)
	h, reg := newServer(t, c)
	body := []byte(`{"action":"opened","issue":{"number":9,"title":"x","body":"y","labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/issues/9"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issues")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "proj3wi", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)
	var tasks tatarav1.TaskList
	require.NoError(t, c.List(context.Background(), &tasks, client.InNamespace(ns)))
	require.Empty(t, tasks.Items)
	require.Equal(t, 1.0, counterValue(t, reg, "operator_webhook_events_total", map[string]string{"provider": "github", "kind": "issue", "result": "no_repo"}))
}
