package webhook_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// allTasks lists every Task in the namespace for the given project.
func allTasks(t *testing.T, c client.Client, projName string) []tatarav1.Task {
	t.Helper()
	var tl tatarav1.TaskList
	require.NoError(t, c.List(context.Background(), &tl, client.InNamespace(ns)))
	var out []tatarav1.Task
	for i := range tl.Items {
		if tl.Items[i].Spec.ProjectRef == projName {
			out = append(out, tl.Items[i])
		}
	}
	return out
}

func allQEs(t *testing.T, c client.Client, projName string) []tatarav1.QueuedEvent {
	t.Helper()
	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	var out []tatarav1.QueuedEvent
	for i := range qel.Items {
		if qel.Items[i].Spec.ProjectRef == projName {
			out = append(out, qel.Items[i])
		}
	}
	return out
}

// TestRouting_PushCreatesNoTask locks the redesign removal of the documentation
// push trigger: a push webhook to an enrolled component's default branch marks
// the repo for re-ingest but enqueues NO Task/QueuedEvent (documentation is a
// scheduled kind now, not a per-merge webhook).
func TestRouting_PushCreatesNoTask(t *testing.T) {
	const secretVal = "whsec"
	c := seedClient(t,
		project("push-proj", "push-proj-scm", "tatara"),
		secret("push-proj-scm", secretVal),
		repository("component-repo", "push-proj", "https://github.com/o/component.git", "main"),
	)
	h, _ := newServer(t, c)

	body := []byte(`{"ref":"refs/heads/main","before":"base1","after":"head2","repository":{"clone_url":"https://github.com/o/component.git"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "push")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "push-proj", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	require.Empty(t, allQEs(t, c, "push-proj"), "push must not enqueue any QueuedEvent")
	require.Empty(t, allTasks(t, c, "push-proj"), "push must not create any Task")
}
