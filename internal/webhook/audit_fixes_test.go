package webhook_test

// Tests for audit findings in internal/webhook/server.go.
// Each test is named after its finding number and must fail before the fix and
// pass after.

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

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

	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	require.Len(t, qel.Items, 1, "duplicate concurrent creates must result in exactly one QueuedEvent")

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

	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	require.Len(t, qel.Items, 1, "GitLab MR with bot actor must create a QueuedEvent")
	require.Equal(t, "review", qel.Items[0].Spec.Payload.Kind,
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

// --- R2 Finding 1: Bot-PR dedup uses PR number not linked-issue number ---

// TestR2Finding1_BotPRClosingIssue_DedupsWithIssueTask verifies that a bot PR
// "Closes #7" does NOT create a duplicate issueLifecycle Task when a task for
// issue #7 already exists. Before the fix, the pre-create scan uses ev.Number
// (PR number 21) while the existing task carries label source-number=7; the
// scan misses it and creates a duplicate.
func TestR2Finding1_BotPRClosingIssue_DedupsWithIssueTask(t *testing.T) {
	const secretVal = "whsec-r2f1"
	proj := &tatarav1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "r2f1proj", Namespace: ns},
		Spec: tatarav1.ProjectSpec{
			ScmSecretRef: "r2f1proj-scm",
			TriggerLabel: "tatara",
			Scm: &tatarav1.ScmSpec{
				Provider: "github", Owner: "o", BotLogin: "tatara-bot",
				PRReactionScope: "all",
			},
		},
	}
	sec := secret("r2f1proj-scm", secretVal)
	repo := repository("r2f1repo", "r2f1proj", "https://github.com/o/r.git", "main")

	// Pre-existing issueLifecycle task for issue #7 (as if created by issueScan).
	existingIssueTask := &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "r2f1-issue7-task",
			Namespace: ns,
			Labels: map[string]string{
				tatarav1.LabelSourceRepo:   "o.r",
				tatarav1.LabelSourceNumber: "7",
				tatarav1.LabelSourceKind:   "issueLifecycle",
			},
		},
		Spec: tatarav1.TaskSpec{
			ProjectRef:    "r2f1proj",
			RepositoryRef: "r2f1repo",
			Kind:          "issueLifecycle",
			Goal:          "issue body",
			Source: &tatarav1.TaskSource{
				Provider: "github",
				IssueRef: "o/r#7",
				Number:   7,
			},
		},
		Status: tatarav1.TaskStatus{
			LifecycleState: "Implement",
		},
	}

	c := seedClient(t, proj, sec, repo)
	require.NoError(t, c.Create(context.Background(), existingIssueTask))
	require.NoError(t, c.Status().Update(context.Background(), existingIssueTask))

	reg := prometheus.NewRegistry()
	srv := webhook.NewServer(webhook.Config{
		Client:    c,
		Namespace: ns,
		Metrics:   obs.NewOperatorMetrics(reg),
	})
	h := srv.Handler()

	// Bot PR #21 that closes issue #7.
	body := []byte(`{"action":"opened","sender":{"login":"tatara-bot"},"pull_request":{"number":21,"title":"fix: closes #7","body":"Closes #7","user":{"login":"tatara-bot"},"labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/pull/21","head":{"sha":"abc","ref":"fix-branch"}},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "pull_request")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "r2f1proj", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var tasks tatarav1.TaskList
	require.NoError(t, c.List(context.Background(), &tasks, client.InNamespace(ns)))
	require.Len(t, tasks.Items, 1, "bot PR 'Closes #7' must NOT create a duplicate task when issue #7 task already exists")

	// Metric must be "duplicate", not "task_created".
	dupCount := counterValue(t, reg, "operator_webhook_events_total",
		map[string]string{"provider": "github", "kind": "mr", "action": "opened", "result": "duplicate"})
	require.Equal(t, 1.0, dupCount, "dedup must produce result=duplicate metric")
}

// TestR2Finding1_BotPRTaskNameMatchesIssueTaskName verifies that an issueLifecycle
// task created from a bot PR "Closes #7" gets the same deterministic name as an
// issueLifecycle task created from a direct issue #7 webhook. This ensures the
// AlreadyExists guard fires when both paths race.
func TestR2Finding1_BotPRTaskName_MatchesIssueDerivedName(t *testing.T) {
	const secretVal = "whsec-r2f1b"
	proj := &tatarav1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "r2f1bproj", Namespace: ns},
		Spec: tatarav1.ProjectSpec{
			ScmSecretRef: "r2f1bproj-scm", //gitleaks:allow - k8s Secret name, not a credential
			TriggerLabel: "tatara",
			Scm: &tatarav1.ScmSpec{
				Provider: "github", Owner: "o", BotLogin: "tatara-bot",
				PRReactionScope: "all",
			},
		},
	}
	sec := secret("r2f1bproj-scm", secretVal)
	repo := repository("r2f1brepo", "r2f1bproj", "https://github.com/o/r.git", "main")

	c := seedClient(t, proj, sec, repo)

	reg := prometheus.NewRegistry()
	srv := webhook.NewServer(webhook.Config{
		Client:    c,
		Namespace: ns,
		Metrics:   obs.NewOperatorMetrics(reg),
	})
	h := srv.Handler()

	// First: issue #7 labeled -> creates issueLifecycle task.
	issueBody := []byte(`{"action":"labeled","sender":{"login":"alice"},"label":{"name":"tatara"},"issue":{"number":7,"title":"fix","body":"fix body","labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/issues/7"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issues")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, issueBody))
	w1 := post(t, h, "r2f1bproj", hdr, issueBody)
	require.Equal(t, http.StatusAccepted, w1.Code)

	var qelAfterIssue tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qelAfterIssue, client.InNamespace(ns)))
	require.Len(t, qelAfterIssue.Items, 1)
	issueTaskName := qelAfterIssue.Items[0].Spec.Payload.Name

	// Second: bot PR #21 "Closes #7" -> must produce same payload.Name -> dedup key collision -> duplicate.
	prBody := []byte(`{"action":"opened","sender":{"login":"tatara-bot"},"pull_request":{"number":21,"title":"fix closes #7","body":"Closes #7","user":{"login":"tatara-bot"},"labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/pull/21","head":{"sha":"abc","ref":"fix-branch"}},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"}}`)
	hdr2 := http.Header{}
	hdr2.Set("X-GitHub-Event", "pull_request")
	hdr2.Set("X-Hub-Signature-256", ghSign(secretVal, prBody))
	w2 := post(t, h, "r2f1bproj", hdr2, prBody)
	require.Equal(t, http.StatusAccepted, w2.Code)

	// Still only one QueuedEvent.
	var qelAfterPR tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qelAfterPR, client.InNamespace(ns)))
	require.Len(t, qelAfterPR.Items, 1, "bot PR 'Closes #7' must resolve to same payload name as issue #7 QueuedEvent")
	require.Equal(t, issueTaskName, qelAfterPR.Items[0].Spec.Payload.Name, "payload names must match so dedup fires")

	dupCount := counterValue(t, reg, "operator_webhook_events_total",
		map[string]string{"provider": "github", "kind": "mr", "action": "opened", "result": "duplicate"})
	require.Equal(t, 1.0, dupCount)
}

// --- R2 Finding 2: Stopped+terminal task excluded from both find functions ---

// TestR2Finding2_StoppedTerminalTask_ReactivatedOnComment verifies that a human
// comment on an issue whose lifecycle task is Stopped+Succeeded causes the task to
// be reactivated (not a duplicate created). Before the fix, findReactivatableTask
// only matches "Parked" -> not found -> createLifecycleTaskAtTriage runs -> duplicate.
func TestR2Finding2_StoppedTerminalTask_ReactivatedOnComment(t *testing.T) {
	const secretVal = "whsec-r2f2"
	proj := projectWithBot("r2f2proj", "r2f2proj-scm", "tatara", "tatara-bot")
	repo := repository("r2f2repo", "r2f2proj", "https://github.com/o/r.git", "main")

	// Stopped task with terminal Phase=Succeeded (the orphan state the spec describes).
	dl := metav1.NewTime(time.Now().Add(time.Hour))
	now := metav1.Now()
	stoppedTask := &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "r2f2-stopped-task",
			Namespace: ns,
			Labels: map[string]string{
				tatarav1.LabelSourceRepo:   "o.r",
				tatarav1.LabelSourceNumber: "7",
				tatarav1.LabelSourceKind:   "issueLifecycle",
			},
		},
		Spec: tatarav1.TaskSpec{
			ProjectRef:    "r2f2proj",
			RepositoryRef: "r2f2repo",
			Kind:          "issueLifecycle",
			Goal:          "issue body",
			Source: &tatarav1.TaskSource{
				Provider: "github",
				IssueRef: "o/r#7",
				Number:   7,
			},
		},
		Status: tatarav1.TaskStatus{
			LifecycleState: "Stopped",
			Phase:          "Succeeded", // terminal phase -> excluded by old findLifecycleTask
			DeadlineAt:     &dl,
			LastActivityAt: &now,
		},
	}

	c := seedClient(t, proj, secret("r2f2proj-scm", secretVal), repo)
	require.NoError(t, c.Create(context.Background(), stoppedTask))
	require.NoError(t, c.Status().Update(context.Background(), stoppedTask))

	h, _ := newServer(t, c)

	body := []byte(issueCommentBody)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issue_comment")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "r2f2proj", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var tasks tatarav1.TaskList
	require.NoError(t, c.List(context.Background(), &tasks, client.InNamespace(ns)))
	require.Len(t, tasks.Items, 1, "Stopped+Succeeded task must be reactivated, not duplicated")

	var got tatarav1.Task
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "r2f2-stopped-task"}, &got))
	require.Equal(t, "Triage", got.Status.LifecycleState, "Stopped+Succeeded task must be reactivated to Triage")
}

// --- R2 Finding 3: GitHub action is unnormalized in metric label ---

// TestR2Finding3_GitHubUnusualAction_NormalizedToOther verifies that a GitHub
// event with an unusual action (e.g. "assigned") produces metric label action="other"
// rather than "assigned", keeping the label set closed.
func TestR2Finding3_GitHubUnusualAction_NormalizedToOther(t *testing.T) {
	const secretVal = "whsec-r2f3"
	proj := project("r2f3proj", "r2f3proj-scm", "tatara")
	sec := secret("r2f3proj-scm", secretVal)
	repo := repository("r2f3repo", "r2f3proj", "https://github.com/o/r.git", "main")

	c := seedClient(t, proj, sec, repo)
	reg := prometheus.NewRegistry()
	srv := webhook.NewServer(webhook.Config{
		Client:    c,
		Namespace: ns,
		Metrics:   obs.NewOperatorMetrics(reg),
	})
	h := srv.Handler()

	// GitHub "issues" event with action="assigned" - not in the normalized set.
	body := []byte(`{"action":"assigned","issue":{"number":5,"title":"fix","body":"body","labels":[],"html_url":"https://github.com/o/r/issues/5"},"assignee":{"login":"alice"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},"sender":{"login":"alice"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issues")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "r2f3proj", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	// The metric action label must be "other", not "assigned".
	otherCount := counterValue(t, reg, "operator_webhook_events_total",
		map[string]string{"provider": "github", "kind": "issue", "action": "other", "result": "ignored"})
	assignedCount := counterValue(t, reg, "operator_webhook_events_total",
		map[string]string{"provider": "github", "kind": "issue", "action": "assigned", "result": "ignored"})
	require.Equal(t, 1.0, otherCount, "unusual GitHub action must be normalized to 'other' in metric")
	require.Equal(t, 0.0, assignedCount, "raw 'assigned' action must not appear as metric label")
}

// --- R2 Finding 4: reactivateTask two sequential Updates inside one RetryOnConflict ---
// The fix is structural (split into two RetryOnConflict blocks). The observable
// behavior change is that even after the status update, the metadata update failure
// results in 202 (not 500), since the reactivation was already committed.
// Testing the split directly requires a fake client that errors on the second
// Update only - not easily composable. We verify the happy-path still works after
// refactor (no regression) and document the structural fix as no-test for the
// error-path branch.

// TestR2Finding4_ReactivateTask_HappyPath_StillWorks verifies the two-step
// reactivate (status + annotations) still succeeds after splitting into two
// independent RetryOnConflict blocks.
func TestR2Finding4_ReactivateTask_HappyPath_StillWorks(t *testing.T) {
	const secretVal = "whsec-r2f4"
	proj := projectWithBot("r2f4proj", "r2f4proj-scm", "tatara", "tatara-bot")
	repo := repository("r2f4repo", "r2f4proj", "https://github.com/o/r.git", "main")
	task := parkedLifecycleTask("r2f4-task", "r2f4proj", "r2f4repo")

	c := seedClient(t, proj, secret("r2f4proj-scm", secretVal), repo)
	require.NoError(t, c.Create(context.Background(), task))
	require.NoError(t, c.Status().Update(context.Background(), task))

	h, _ := newServer(t, c)

	body := []byte(issueCommentBodyIssue9)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issue_comment")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "r2f4proj", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var got tatarav1.Task
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "r2f4-task"}, &got))
	require.Equal(t, "Triage", got.Status.LifecycleState, "reactivated task must be Triage")
	require.Equal(t, "", got.Status.Phase, "Phase must be cleared")
	require.Empty(t, got.Annotations[tatarav1.AnnCurrentTurn], "turn annotations must be cleared")
}

// --- R2 Finding 5: readBody silently truncates payloads > 5 MiB ---

// TestR2Finding5_OversizedBody_Returns413 verifies that a payload larger than 5 MiB
// returns 413 (not a 401 signature mismatch or 200 with a corrupted payload).
func TestR2Finding5_OversizedBody_Returns413(t *testing.T) {
	const secretVal = "whsec-r2f5"
	proj := project("r2f5proj", "r2f5proj-scm", "tatara")
	sec := secret("r2f5proj-scm", secretVal)

	c := seedClient(t, proj, sec)
	reg := prometheus.NewRegistry()
	srv := webhook.NewServer(webhook.Config{
		Client:    c,
		Namespace: ns,
		Metrics:   obs.NewOperatorMetrics(reg),
	})
	h := srv.Handler()

	// Build a body that is just over 5 MiB (5<<20 + 1 bytes).
	// The body itself needn't be valid JSON; we just want readBody to detect overflow.
	oversized := bytes.Repeat([]byte("x"), (5<<20)+1)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "push")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, oversized))

	req, err := makePostRequest("/operator/webhooks/r2f5proj", hdr, oversized)
	require.NoError(t, err)

	rr := newRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusRequestEntityTooLarge, rr.Code,
		"payload > 5 MiB must return 413, not 401 or other status")

	tooLargeCount := counterValue(t, reg, "operator_webhook_events_total",
		map[string]string{"provider": "unknown", "kind": "other", "action": "other", "result": "too_large"})
	require.Equal(t, 1.0, tooLargeCount, "oversized body must increment result=too_large metric")
}

// --- R2 Finding 6: Provider mismatch not detected ---

// TestR2Finding6_ProviderMismatch_Returns400 verifies that a GitHub-style webhook
// delivery to a GitLab-configured project returns 400 (provider_mismatch), not a
// confusing 401 bad_signature from the wrong HMAC scheme.
func TestR2Finding6_ProviderMismatch_Returns400(t *testing.T) {
	const secretVal = "whsec-r2f6"
	// Project configured for GitLab.
	proj := &tatarav1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: "r2f6proj", Namespace: ns},
		Spec: tatarav1.ProjectSpec{
			ScmSecretRef: "r2f6proj-scm",
			TriggerLabel: "tatara",
			Scm: &tatarav1.ScmSpec{
				Provider: "gitlab", Owner: "group", BotLogin: "tatara-bot",
			},
		},
	}
	sec := secret("r2f6proj-scm", secretVal)

	c := seedClient(t, proj, sec)
	reg := prometheus.NewRegistry()
	srv := webhook.NewServer(webhook.Config{
		Client:    c,
		Namespace: ns,
		Metrics:   obs.NewOperatorMetrics(reg),
	})
	h := srv.Handler()

	// Deliver a GitHub-style webhook (X-GitHub-Event header).
	body := []byte(`{"action":"labeled","issue":{"number":1,"title":"x","body":"y","labels":[],"html_url":"https://github.com/g/r/issues/1"},"repository":{"clone_url":"https://github.com/g/r.git","full_name":"g/r"},"sender":{"login":"alice"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issues")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "r2f6proj", hdr, body)
	require.Equal(t, http.StatusBadRequest, w.Code,
		"GitHub delivery to GitLab-configured project must return 400 (provider_mismatch), not 401")

	mismatchCount := counterValue(t, reg, "operator_webhook_events_total",
		map[string]string{"provider": "github", "kind": "other", "action": "other", "result": "provider_mismatch"})
	require.Equal(t, 1.0, mismatchCount, "provider mismatch must increment result=provider_mismatch metric")
}

// makePostRequest and newRecorder are helpers for tests that can't use the post()
// helper directly (e.g. when the body is a []byte not a string).
func makePostRequest(path string, hdr http.Header, body []byte) (*http.Request, error) {
	req, err := http.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	for k, vs := range hdr {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	return req, nil
}

type testRecorder struct {
	Code   int
	Body   strings.Builder
	header http.Header
}

func newRecorder() *testRecorder {
	return &testRecorder{Code: http.StatusOK, header: http.Header{}}
}

func (r *testRecorder) Header() http.Header         { return r.header }
func (r *testRecorder) WriteHeader(code int)        { r.Code = code }
func (r *testRecorder) Write(b []byte) (int, error) { return r.Body.WriteString(string(b)) }
