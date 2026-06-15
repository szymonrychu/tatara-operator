package webhook_test

// Tests for audit findings in internal/webhook/server.go.
// Each test is named after its finding number and must fail before the fix and
// pass after.

import (
	"context"
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/webhook"
)

// --- Finding 1: GitLab MR IssueRef uses '!' not '#'; LastIndex returns -1 -> panic ---

// gitLabMRHook constructs a minimal GitLab Merge Request Hook payload for an MR
// with the given trigger label.
const glMRPayloadWithTriggerLabel = `{
  "object_kind": "merge_request",
  "user": {"username": "tatara-bot"},
  "project": {"git_http_url": "https://gitlab.com/group/proj.git", "path_with_namespace": "group/proj"},
  "object_attributes": {
    "iid": 42, "title": "fix", "description": "body",
    "url": "https://gitlab.com/group/proj/-/merge_requests/42",
    "action": "open", "source_branch": "feature",
    "last_commit": {"id": "abc"}
  },
  "labels": [{"title": "tatara"}],
  "changes": {"labels": {"previous": [], "current": [{"title": "tatara"}]}}
}`

// TestFinding1_GitLabMRWebhook_NoPanicOnBangSeparator verifies that a GitLab
// MR webhook (IssueRef = "group/proj!42") does NOT panic when the server
// derives the repo-slug from the IssueRef. Before the fix, LastIndex("#") == -1
// causes a slice panic; after the fix it must return 202.
func TestFinding1_GitLabMRWebhook_NoPanicOnBangSeparator(t *testing.T) {
	const secretVal = "gl-secret"
	proj := &tatarav1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "glproj1", Namespace: ns},
		Spec: tatarav1.ProjectSpec{
			ScmSecretRef: "glproj1-scm",
			TriggerLabel: "tatara",
			Scm: &tatarav1.ScmSpec{
				Provider: "gitlab", Owner: "group", BotLogin: "tatara-bot",
			},
		},
	}
	// GitLab uses X-Gitlab-Token, not HMAC; the secret value is the token itself.
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "glproj1-scm", Namespace: ns},
		Data: map[string][]byte{
			"webhookSecret": []byte(secretVal),
			"token":         []byte("pat"),
		},
	}
	repo := &tatarav1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "glrepo1", Namespace: ns},
		Spec:       tatarav1.RepositorySpec{ProjectRef: "glproj1", URL: "https://gitlab.com/group/proj.git", DefaultBranch: "main"},
	}

	c := seedClient(t, proj, sec, repo)
	reg := prometheus.NewRegistry()
	srv := webhook.NewServer(webhook.Config{
		Client:    c,
		Namespace: ns,
		Metrics:   obs.NewOperatorMetrics(reg),
	})
	h := srv.Handler()

	body := []byte(glMRPayloadWithTriggerLabel)
	hdr := http.Header{}
	hdr.Set("X-Gitlab-Event", "Merge Request Hook")
	hdr.Set("X-Gitlab-Token", secretVal)

	// Must NOT panic; must return 202.
	w := post(t, h, "glproj1", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code, "GitLab MR webhook must not panic and must return 202")
}

// TestFinding1_GitLabNoteOnMR_NoPanic verifies that a GitLab Note Hook for an
// MR (IssueRef = "group/proj!42") processed via handleIssueComment does NOT
// panic at createLifecycleTaskAtTriage's repoSlug derivation.
const glNoteOnMRPayload = `{
  "object_kind": "note",
  "user": {"username": "alice"},
  "project": {"git_http_url": "https://gitlab.com/group/proj.git", "path_with_namespace": "group/proj"},
  "object_attributes": {
    "note": "looks good", "url": "https://gitlab.com/group/proj/-/notes/1",
    "action": "create"
  },
  "merge_request": {"iid": 43, "source_branch": "feature"},
  "issue": {"iid": 0},
  "labels": []
}`

func TestFinding1_GitLabNoteOnMR_NoPanic(t *testing.T) {
	const secretVal = "gl-note-secret"
	proj := &tatarav1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "glnoteproj", Namespace: ns},
		Spec: tatarav1.ProjectSpec{
			ScmSecretRef: "glnoteproj-scm",
			TriggerLabel: "tatara",
			Scm: &tatarav1.ScmSpec{
				Provider: "gitlab", Owner: "group", BotLogin: "tatara-bot",
			},
		},
	}
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "glnoteproj-scm", Namespace: ns},
		Data: map[string][]byte{
			"webhookSecret": []byte(secretVal),
			"token":         []byte("pat"),
		},
	}
	repo := &tatarav1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "glnoterepo", Namespace: ns},
		Spec:       tatarav1.RepositorySpec{ProjectRef: "glnoteproj", URL: "https://gitlab.com/group/proj.git", DefaultBranch: "main"},
	}

	c := seedClient(t, proj, sec, repo)
	reg := prometheus.NewRegistry()
	srv := webhook.NewServer(webhook.Config{
		Client:    c,
		Namespace: ns,
		Metrics:   obs.NewOperatorMetrics(reg),
	})
	h := srv.Handler()

	body := []byte(glNoteOnMRPayload)
	hdr := http.Header{}
	hdr.Set("X-Gitlab-Event", "Note Hook")
	hdr.Set("X-Gitlab-Token", secretVal)

	// Must not panic.
	w := post(t, h, "glnoteproj", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code, "GitLab Note on MR must not panic")
}

// --- Finding 2: TOCTOU race creates duplicate issueLifecycle Tasks ---

// TestFinding2_ConcurrentDuplicateCreate_IdempotentViaAlreadyExists verifies
// that two webhook deliveries for the same work item that both pass the in-Go
// dedup scan (simulating a TOCTOU race window) do not end up with two live Tasks.
// The fix is a deterministic Task name, so the second Create returns AlreadyExists,
// counted as "duplicate" (202), not an error.
func TestFinding2_ConcurrentDuplicateCreate_IdempotentViaAlreadyExists(t *testing.T) {
	const secretVal = "whsec-f2"
	proj := project("f2proj", "f2proj-scm", "tatara")
	sec := secret("f2proj-scm", secretVal)
	repo := repository("f2repo", "f2proj", "https://github.com/o/r.git", "main")

	c := seedClient(t, proj, sec, repo)
	reg := prometheus.NewRegistry()
	srv := webhook.NewServer(webhook.Config{
		Client:    c,
		Namespace: ns,
		Metrics:   obs.NewOperatorMetrics(reg),
	})
	h := srv.Handler()

	body := []byte(`{"action":"labeled","sender":{"login":"alice"},"label":{"name":"tatara"},"issue":{"number":42,"title":"Fix","body":"fix body","labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/issues/42"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issues")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	// First delivery: creates the task.
	w1 := post(t, h, "f2proj", hdr, body)
	require.Equal(t, http.StatusAccepted, w1.Code)

	// Second delivery (simulating the concurrent racing goroutine that also
	// passed the list-based dedup scan before the first Create committed):
	// with deterministic names this Create returns AlreadyExists -> 202 duplicate.
	w2 := post(t, h, "f2proj", hdr, body)
	require.Equal(t, http.StatusAccepted, w2.Code)

	var tasks tatarav1.TaskList
	require.NoError(t, c.List(context.Background(), &tasks, client.InNamespace(ns)))
	require.Len(t, tasks.Items, 1, "duplicate concurrent creates must result in exactly one Task")

	require.Equal(t, 2.0, counterValue(t, reg, "operator_webhook_events_total",
		map[string]string{"provider": "github", "kind": "issue", "action": "labeled", "result": "duplicate"})+
		counterValue(t, reg, "operator_webhook_events_total",
			map[string]string{"provider": "github", "kind": "issue", "action": "labeled", "result": "task_created"}),
		"one task_created + one duplicate must sum to 2 total accepted events")
}

// --- Finding 3: Empty webhookSecret passes GitHub HMAC verification ---

// TestFinding3_EmptyWebhookSecret_Rejected verifies that when the webhookSecret
// key is present but empty, the webhook returns 500 (secret error) rather than
// accepting requests signed with HMAC("", body).
func TestFinding3_EmptyWebhookSecret_Rejected(t *testing.T) {
	proj := project("emptysec-proj", "emptysec-scm", "tatara")
	// Secret key present but value is empty.
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "emptysec-scm", Namespace: ns},
		Data:       map[string][]byte{"webhookSecret": []byte("")},
	}
	c := seedClient(t, proj, sec)
	reg := prometheus.NewRegistry()
	srv := webhook.NewServer(webhook.Config{
		Client:    c,
		Namespace: ns,
		Metrics:   obs.NewOperatorMetrics(reg),
	})
	h := srv.Handler()

	// Sign with an empty key (what an attacker would send).
	body := []byte(`{"ref":"refs/heads/main","after":"sha","repository":{"clone_url":"https://github.com/o/r.git"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "push")
	hdr.Set("X-Hub-Signature-256", ghSign("", body))

	w := post(t, h, "emptysec-proj", hdr, body)
	// Must reject: empty secret is a misconfiguration, not a valid secret.
	require.Equal(t, http.StatusInternalServerError, w.Code,
		"empty webhookSecret must cause 500 (secret error), not accept the request")
}

// --- Finding 4: GitLab MR kind selection uses AuthorLogin == ActorLogin ---

// TestFinding4_GitLabMR_BotActorDoesNotClassifyAsIssueLifecycle verifies that
// when a GitLab MR event carries AuthorLogin==BotLogin (because it is the actor,
// not the MR author), the webhook does NOT incorrectly create kind=issueLifecycle.
// For GitLab we cannot determine the real MR author from the payload alone, so
// the classification must default to "review" (or be deferred), not "issueLifecycle".
const glMRPayloadBotActor = `{
  "object_kind": "merge_request",
  "user": {"username": "tatara-bot"},
  "project": {"git_http_url": "https://gitlab.com/group/proj.git", "path_with_namespace": "group/proj"},
  "object_attributes": {
    "iid": 55, "title": "human MR", "description": "please review",
    "url": "https://gitlab.com/group/proj/-/merge_requests/55",
    "action": "open", "source_branch": "feature",
    "last_commit": {"id": "abc"}
  },
  "labels": [{"title": "tatara"}],
  "changes": {"labels": {"previous": [], "current": [{"title": "tatara"}]}}
}`

func TestFinding4_GitLabMR_BotActorDoesNotClassifyAsIssueLifecycle(t *testing.T) {
	const secretVal = "gl-f4-secret"
	proj := &tatarav1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "glf4proj", Namespace: ns},
		Spec: tatarav1.ProjectSpec{
			ScmSecretRef: "glf4proj-scm", //gitleaks:allow - k8s Secret name, not a credential
			TriggerLabel: "tatara",
			Scm: &tatarav1.ScmSpec{
				Provider:        "gitlab",
				Owner:           "group",
				BotLogin:        "tatara-bot",
				PRReactionScope: "all", // react to all MRs
			},
		},
	}
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "glf4proj-scm", Namespace: ns},
		Data: map[string][]byte{
			"webhookSecret": []byte(secretVal),
			"token":         []byte("pat"),
		},
	}
	repo := &tatarav1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: "glf4repo", Namespace: ns},
		Spec:       tatarav1.RepositorySpec{ProjectRef: "glf4proj", URL: "https://gitlab.com/group/proj.git", DefaultBranch: "main"},
	}

	c := seedClient(t, proj, sec, repo)
	reg := prometheus.NewRegistry()
	srv := webhook.NewServer(webhook.Config{
		Client:    c,
		Namespace: ns,
		Metrics:   obs.NewOperatorMetrics(reg),
	})
	h := srv.Handler()

	body := []byte(glMRPayloadBotActor)
	hdr := http.Header{}
	hdr.Set("X-Gitlab-Event", "Merge Request Hook")
	hdr.Set("X-Gitlab-Token", secretVal)

	w := post(t, h, "glf4proj", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var tasks tatarav1.TaskList
	require.NoError(t, c.List(context.Background(), &tasks, client.InNamespace(ns)))
	require.Len(t, tasks.Items, 1, "GitLab MR with bot actor must create a task")
	tk := tasks.Items[0]
	require.Equal(t, "review", tk.Spec.Kind,
		"GitLab MR where actor==bot must NOT be classified as issueLifecycle (actor != author)")
}

// --- Finding 7: Nil Metrics in NewServer causes nil-deref in count() ---

// TestFinding7_NilMetricsPanicsAtNewServer verifies that constructing a Server
// with nil Metrics panics immediately with a clear message, rather than silently
// accepting a nil pointer that explodes on the first webhook event.
func TestFinding7_NilMetricsPanicsAtNewServer(t *testing.T) {
	require.Panics(t, func() {
		webhook.NewServer(webhook.Config{
			Client:    seedClient(t),
			Namespace: ns,
			Metrics:   nil,
		})
	}, "NewServer with nil Metrics must panic immediately")
}
