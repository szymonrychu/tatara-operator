package webhook_test

// Tests for issue_comment on an untracked work item: a human comment on an
// issue OR an MR with no live lifecycle task creates a Task at Triage (issue
// #25); bot comments must not.

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// issueCommentUntracked: human comment on issue 9 (no existing task).
const issueCommentUntracked = `{"action":"created","issue":{"number":9,"title":"old bug","body":"still broken","html_url":"https://github.com/o/r/issues/9"},"comment":{"id":5,"body":"Any update on this?","user":{"login":"user"}},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},"sender":{"login":"user"}}`

// issueCommentUntrackedBot: bot comment on issue 9 (no existing task).
const issueCommentUntrackedBot = `{"action":"created","issue":{"number":9,"title":"old bug","body":"still broken","html_url":"https://github.com/o/r/issues/9"},"comment":{"id":6,"body":"Looking into it","user":{"login":"tatara-bot"}},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},"sender":{"login":"tatara-bot"}}`

// prCommentUntracked: human comment on PR 11 (no existing task, IsPR=true because pull_request key present).
const prCommentUntracked = `{"action":"created","issue":{"number":11,"title":"fix pr","body":"pr body","pull_request":{"url":"https://api.github.com/repos/o/r/pulls/11"},"html_url":"https://github.com/o/r/pull/11"},"comment":{"id":7,"body":"please review","user":{"login":"user"}},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},"sender":{"login":"user"}}`

// TestIssueComment_HumanOnUntrackedIssue_CreatesTriageTask verifies that a
// human issue_comment on an issue with no live lifecycle task creates one
// issueLifecycle Task at Triage (via the lifecycle-entry annotation).
func TestIssueComment_HumanOnUntrackedIssue_CreatesTriageTask(t *testing.T) {
	const secretVal = "whsec-ut1"
	proj := projectWithBot("projicut1", "projicut1-scm", "tatara", "tatara-bot")
	repo := repository("repoicut1", "projicut1", "https://github.com/o/r.git", "main")

	c := seedClient(t, proj, secret("projicut1-scm", secretVal), repo)
	h, _ := newServer(t, c)

	body := []byte(issueCommentUntracked)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issue_comment")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "projicut1", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	// A lifecycle Task must have been created.
	var tasks tatarav1.TaskList
	require.NoError(t, c.List(context.Background(), &tasks, client.InNamespace(ns)))
	require.Len(t, tasks.Items, 1, "human issue_comment on untracked issue must create one Task")

	tk := tasks.Items[0]
	require.Equal(t, "issueLifecycle", tk.Spec.Kind)
	require.NotNil(t, tk.Spec.Source)
	require.Equal(t, "o/r#9", tk.Spec.Source.IssueRef)
	require.False(t, tk.Spec.Source.IsPR, "task must not be IsPR for an issue comment")

	// Lifecycle entry annotation must be "Triage".
	require.Equal(t, "Triage", tk.Annotations[tatarav1.LifecycleEntryAnnotation],
		"issue_comment-created task must enter at Triage")

	// Dedup labels must match issueScan convention.
	require.Equal(t, "o.r", tk.Labels[tatarav1.LabelSourceRepo])
	require.Equal(t, "9", tk.Labels[tatarav1.LabelSourceNumber])
	require.Equal(t, "issueLifecycle", tk.Labels[tatarav1.LabelSourceKind])
}

// TestIssueComment_BotOnUntrackedIssue_DoesNotCreateTask verifies that a bot
// comment on an issue with no live task does NOT create a task.
func TestIssueComment_BotOnUntrackedIssue_DoesNotCreateTask(t *testing.T) {
	const secretVal = "whsec-ut2"
	proj := projectWithBot("projicut2", "projicut2-scm", "tatara", "tatara-bot")
	repo := repository("repoicut2", "projicut2", "https://github.com/o/r.git", "main")

	c := seedClient(t, proj, secret("projicut2-scm", secretVal), repo)
	h, _ := newServer(t, c)

	body := []byte(issueCommentUntrackedBot)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issue_comment")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "projicut2", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var tasks tatarav1.TaskList
	require.NoError(t, c.List(context.Background(), &tasks, client.InNamespace(ns)))
	require.Empty(t, tasks.Items, "bot comment on untracked issue must NOT create a task")
}

// TestIssueComment_HumanOnUntrackedPR_CreatesTriageTask verifies that a human
// comment on an MR (IsPR=true) with no live task creates an issueLifecycle Task
// at Triage with Source.IsPR set (issue #25: comments on an MR with no nursing
// agent spawn one).
func TestIssueComment_HumanOnUntrackedPR_CreatesTriageTask(t *testing.T) {
	const secretVal = "whsec-ut3"
	proj := projectWithBot("projicut3", "projicut3-scm", "tatara", "tatara-bot")
	repo := repository("repoicut3", "projicut3", "https://github.com/o/r.git", "main")

	c := seedClient(t, proj, secret("projicut3-scm", secretVal), repo)
	h, _ := newServer(t, c)

	body := []byte(prCommentUntracked)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issue_comment")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "projicut3", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var tasks tatarav1.TaskList
	require.NoError(t, c.List(context.Background(), &tasks, client.InNamespace(ns)))
	require.Len(t, tasks.Items, 1, "human MR comment on untracked MR must create one Task")

	tk := tasks.Items[0]
	require.Equal(t, "issueLifecycle", tk.Spec.Kind)
	require.NotNil(t, tk.Spec.Source)
	require.Equal(t, "o/r#11", tk.Spec.Source.IssueRef)
	require.True(t, tk.Spec.Source.IsPR, "task must be IsPR for an MR comment")
	require.Equal(t, "Triage", tk.Annotations[tatarav1.LifecycleEntryAnnotation])
	require.Equal(t, "11", tk.Labels[tatarav1.LabelSourceNumber])
}

// TestIssueComment_HumanOnUntrackedIssue_ExistingLiveTask_NoNewTask verifies
// that when a live task already exists for the issue, the existing reactivation
// path runs (no new task is created).
func TestIssueComment_HumanOnUntrackedIssue_ExistingLiveTask_NoNewTask(t *testing.T) {
	const secretVal = "whsec-ut4"
	proj := projectWithBot("projicut4", "projicut4-scm", "tatara", "tatara-bot")
	repo := repository("repoicut4", "projicut4", "https://github.com/o/r.git", "main")
	// Existing live task for issue 9 - must match IssueRef used in the comment payload.
	existingTask := lifecycleTask("taskicut4", "projicut4", "repoicut4", 9, "Conversation")
	// Override the IssueRef to match the comment payload (issue 9, not 7).
	existingTask.Spec.Source.IssueRef = "o/r#9"

	c := seedClient(t, proj, secret("projicut4-scm", secretVal), repo)
	require.NoError(t, c.Create(context.Background(), existingTask))
	require.NoError(t, c.Status().Update(context.Background(), existingTask))

	h, _ := newServer(t, c)

	body := []byte(issueCommentUntracked)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issue_comment")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "projicut4", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var tasks tatarav1.TaskList
	require.NoError(t, c.List(context.Background(), &tasks, client.InNamespace(ns)))
	require.Len(t, tasks.Items, 1, "when live task exists, only reactivate - do not create a second task")
}
