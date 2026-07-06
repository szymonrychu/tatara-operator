package webhook_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// docProject builds a Project with the documentation agent enabled, pointing
// at a docs repo URL. c allows the caller to further tweak the spec.
func docProject(name, secretRef string, docsRepoURL string) *tatarav1.Project {
	return &tatarav1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: tatarav1.ProjectSpec{
			ScmSecretRef:  secretRef,
			Documentation: &tatarav1.DocumentationSpec{Enabled: true, Repo: docsRepoURL},
		},
	}
}

func pushBody(cloneURL, before, after string) []byte {
	return []byte(`{"ref":"refs/heads/main","before":"` + before + `","after":"` + after + `","repository":{"clone_url":"` + cloneURL + `"}}`)
}

func docQueuedEvents(t *testing.T, c client.Client, projName string) []tatarav1.QueuedEvent {
	t.Helper()
	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	var matching []tatarav1.QueuedEvent
	for _, qe := range qel.Items {
		if qe.Spec.ProjectRef == projName && qe.Spec.Payload.Kind == "documentation" {
			matching = append(matching, qe)
		}
	}
	return matching
}

// TestPush_EnqueuesDocumentationTask asserts a merge to a component repo's
// default branch spawns a documentation QueuedEvent scoped to the docs repo,
// with the source repo/base/head SHA carried as annotations.
func TestPush_EnqueuesDocumentationTask(t *testing.T) {
	const secretVal = "whsec"
	c := seedClient(t,
		docProject("doc-proj", "doc-proj-scm", "https://github.com/o/docs.git"),
		secret("doc-proj-scm", secretVal),
		repository("component-repo", "doc-proj", "https://github.com/o/component.git", "main"),
		repository("docs-repo", "doc-proj", "https://github.com/o/docs.git", "main"),
	)
	h, _ := newServer(t, c)

	body := pushBody("https://github.com/o/component.git", "base1234", "head5678")
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "push")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "doc-proj", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	matching := docQueuedEvents(t, c, "doc-proj")
	require.Len(t, matching, 1)
	qe := matching[0]
	require.Equal(t, "docs-repo", qe.Spec.Payload.RepositoryRef, "documentation Task must be repo-scoped to the DOCS repo")
	require.Equal(t, "https://github.com/o/component.git", qe.Spec.Payload.Annotations[tatarav1.AnnSourceRepo])
	require.Equal(t, "base1234", qe.Spec.Payload.Annotations[tatarav1.AnnSourceBaseSHA])
	require.Equal(t, "head5678", qe.Spec.Payload.Annotations[tatarav1.AnnSourceHeadSHA])
}

// TestPush_NoDocumentationTaskWhenDisabled asserts no documentation QueuedEvent
// is created when the Project has no Documentation block (nil, inert default).
func TestPush_NoDocumentationTaskWhenDisabled(t *testing.T) {
	const secretVal = "whsec"
	c := seedClient(t,
		project("plain-proj", "plain-proj-scm", "tatara"),
		secret("plain-proj-scm", secretVal),
		repository("component-repo", "plain-proj", "https://github.com/o/component.git", "main"),
	)
	h, _ := newServer(t, c)

	body := pushBody("https://github.com/o/component.git", "base1234", "head5678")
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "push")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "plain-proj", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	require.Empty(t, docQueuedEvents(t, c, "plain-proj"))
}

// TestPush_NoDocumentationTaskWhenExplicitlyDisabled asserts
// Documentation.Enabled=false (explicit, not the nil default) also gates off.
func TestPush_NoDocumentationTaskWhenExplicitlyDisabled(t *testing.T) {
	const secretVal = "whsec"
	proj := docProject("disabled-proj", "disabled-proj-scm", "https://github.com/o/docs.git")
	proj.Spec.Documentation.Enabled = false
	c := seedClient(t,
		proj,
		secret("disabled-proj-scm", secretVal),
		repository("component-repo", "disabled-proj", "https://github.com/o/component.git", "main"),
		repository("docs-repo", "disabled-proj", "https://github.com/o/docs.git", "main"),
	)
	h, _ := newServer(t, c)

	body := pushBody("https://github.com/o/component.git", "base1234", "head5678")
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "push")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "disabled-proj", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	require.Empty(t, docQueuedEvents(t, c, "disabled-proj"))
}

// TestPush_NoDocumentationTaskForDocsRepoItself asserts the self-trigger
// guard: a merge to the docs repo's own default branch must NOT spawn another
// documentation Task, or it loops.
func TestPush_NoDocumentationTaskForDocsRepoItself(t *testing.T) {
	const secretVal = "whsec"
	c := seedClient(t,
		docProject("selfguard-proj", "selfguard-proj-scm", "https://github.com/o/docs.git"),
		secret("selfguard-proj-scm", secretVal),
		repository("docs-repo", "selfguard-proj", "https://github.com/o/docs.git", "main"),
	)
	h, _ := newServer(t, c)

	body := pushBody("https://github.com/o/docs.git", "base1234", "head5678")
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "push")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "selfguard-proj", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	require.Empty(t, docQueuedEvents(t, c, "selfguard-proj"))
}

// TestPush_NoDocumentationTaskWhenDocsRepoNotEnrolled asserts that without an
// enrolled Repository CR for the docs repo, no documentation Task is created
// (the bot would have no push access and no CR to resolve the RepositoryRef).
func TestPush_NoDocumentationTaskWhenDocsRepoNotEnrolled(t *testing.T) {
	const secretVal = "whsec"
	c := seedClient(t,
		docProject("noenroll-proj", "noenroll-proj-scm", "https://github.com/o/docs.git"),
		secret("noenroll-proj-scm", secretVal),
		repository("component-repo", "noenroll-proj", "https://github.com/o/component.git", "main"),
		// docs-repo Repository CR deliberately absent.
	)
	h, _ := newServer(t, c)

	body := pushBody("https://github.com/o/component.git", "base1234", "head5678")
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "push")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "noenroll-proj", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	require.Empty(t, docQueuedEvents(t, c, "noenroll-proj"))
}

// TestPush_DocumentationDedupsOnHeadSHA asserts a redelivered webhook for the
// SAME head SHA does not double-spawn, but a DIFFERENT merge (new head SHA)
// does spawn a second documentation Task.
func TestPush_DocumentationDedupsOnHeadSHA(t *testing.T) {
	const secretVal = "whsec"
	c := seedClient(t,
		docProject("dedup-proj", "dedup-proj-scm", "https://github.com/o/docs.git"),
		secret("dedup-proj-scm", secretVal),
		repository("component-repo", "dedup-proj", "https://github.com/o/component.git", "main"),
		repository("docs-repo", "dedup-proj", "https://github.com/o/docs.git", "main"),
	)
	h, _ := newServer(t, c)

	hdr := func(body []byte) http.Header {
		hh := http.Header{}
		hh.Set("X-GitHub-Event", "push")
		hh.Set("X-Hub-Signature-256", ghSign(secretVal, body))
		return hh
	}

	body1 := pushBody("https://github.com/o/component.git", "base1", "head1")
	require.Equal(t, http.StatusAccepted, post(t, h, "dedup-proj", hdr(body1), body1).Code)
	// Re-delivery of the exact same push event.
	require.Equal(t, http.StatusAccepted, post(t, h, "dedup-proj", hdr(body1), body1).Code)
	require.Len(t, docQueuedEvents(t, c, "dedup-proj"), 1, "redelivered webhook for the same head SHA must not double-spawn")

	// A second, distinct merge (new head SHA) must spawn a second Task.
	body2 := pushBody("https://github.com/o/component.git", "head1", "head2")
	require.Equal(t, http.StatusAccepted, post(t, h, "dedup-proj", hdr(body2), body2).Code)
	require.Len(t, docQueuedEvents(t, c, "dedup-proj"), 2, "a distinct merge must spawn a distinct documentation Task")
}
