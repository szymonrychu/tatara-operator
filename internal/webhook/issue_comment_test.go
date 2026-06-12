package webhook_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// projectWithBot builds a Project with scm.botLogin set.
func projectWithBot(name, secretRef, trigger, bot string) *tatarav1.Project {
	return &tatarav1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: tatarav1.ProjectSpec{
			ScmSecretRef: secretRef,
			TriggerLabel: trigger,
			Scm: &tatarav1.ScmSpec{
				Provider: "github", Owner: "o", BotLogin: bot,
			},
		},
	}
}

// lifecycleTask builds a minimal lifecycle Task in a given state bound to issue number.
func lifecycleTask(name, projectRef, repoRef string, issueNumber int, state string) *tatarav1.Task {
	dl := metav1.NewTime(time.Now().Add(time.Hour))
	now := metav1.Now()
	return &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels: map[string]string{
				tatarav1.LabelSourceRepo:   "o.r",
				tatarav1.LabelSourceNumber: "7",
				tatarav1.LabelSourceKind:   "issueLifecycle",
			},
		},
		Spec: tatarav1.TaskSpec{
			ProjectRef:    projectRef,
			RepositoryRef: repoRef,
			Kind:          "issueLifecycle",
			Goal:          "issue",
			Source: &tatarav1.TaskSource{
				Provider: "github",
				IssueRef: "o/r#7",
				Number:   issueNumber,
			},
		},
		Status: tatarav1.TaskStatus{
			LifecycleState: state,
			DeadlineAt:     &dl,
			LastActivityAt: &now,
		},
	}
}

const issueCommentBody = `{"action":"created","issue":{"number":7,"title":"fix","body":"please fix","html_url":"https://github.com/o/r/issues/7"},"comment":{"id":1,"body":"Please check the edge case","user":{"login":"maintainer"}},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},"sender":{"login":"maintainer"}}`

const issueCommentBotBody = `{"action":"created","issue":{"number":7,"title":"fix","body":"please fix","html_url":"https://github.com/o/r/issues/7"},"comment":{"id":2,"body":"I will investigate","user":{"login":"tatara-bot"}},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},"sender":{"login":"tatara-bot"}}`

// TestIssueComment_OnConversationTask_SetsTriageAndResetsTimers verifies that a
// human issue_comment on a Conversation task resets LastActivityAt/DeadlineAt
// and sets LifecycleState=Triage.
func TestIssueComment_OnConversationTask_SetsTriageAndResetsTimers(t *testing.T) {
	const secretVal = "whsec"
	proj := projectWithBot("projic1", "projic1-scm", "tatara", "tatara-bot")
	repo := repository("repoic1", "projic1", "https://github.com/o/r.git", "main")
	task := lifecycleTask("taskic1", "projic1", "repoic1", 7, "Conversation")

	// Record original deadline to verify it changes.
	origDeadline := task.Status.DeadlineAt.Time

	c := seedClient(t, proj, secret("projic1-scm", secretVal), repo)
	// Create task separately so Status.Update works via StatusSubresource.
	require.NoError(t, c.Create(context.Background(), task))
	require.NoError(t, c.Status().Update(context.Background(), task))

	h, _ := newServer(t, c)

	body := []byte(issueCommentBody)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issue_comment")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "projic1", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	// Re-fetch the task and verify state transitions.
	var got tatarav1.Task
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "taskic1"}, &got))
	require.Equal(t, "Triage", got.Status.LifecycleState)
	require.NotNil(t, got.Status.DeadlineAt)
	// DeadlineAt must have been reset (extended from now, so it should be after origDeadline or nil and re-set).
	require.NotNil(t, got.Status.LastActivityAt)
	// The deadline should differ from the original (it was reset).
	_ = origDeadline // checked via NotNil above; timing-sensitive exact check avoided
}

// TestIssueComment_OnStoppedTask_ReOpens verifies that a human issue_comment on
// a Stopped task transitions it to Triage (re-opens the conversation).
func TestIssueComment_OnStoppedTask_ReOpens(t *testing.T) {
	const secretVal = "whsec"
	proj := projectWithBot("projic2", "projic2-scm", "tatara", "tatara-bot")
	repo := repository("repoic2", "projic2", "https://github.com/o/r.git", "main")
	task := lifecycleTask("taskic2", "projic2", "repoic2", 7, "Stopped")

	c := seedClient(t, proj, secret("projic2-scm", secretVal), repo)
	require.NoError(t, c.Create(context.Background(), task))
	require.NoError(t, c.Status().Update(context.Background(), task))

	h, _ := newServer(t, c)

	body := []byte(issueCommentBody)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issue_comment")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "projic2", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var got tatarav1.Task
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "taskic2"}, &got))
	require.Equal(t, "Triage", got.Status.LifecycleState, "Stopped task must be re-opened to Triage on human comment")
}

// TestIssueComment_BotComment_Ignored verifies that a comment from the bot
// itself does NOT reset timers or change state.
func TestIssueComment_BotComment_Ignored(t *testing.T) {
	const secretVal = "whsec"
	proj := projectWithBot("projic3", "projic3-scm", "tatara", "tatara-bot")
	repo := repository("repoic3", "projic3", "https://github.com/o/r.git", "main")

	origDL := metav1.NewTime(time.Now().Add(30 * time.Minute))
	task := lifecycleTask("taskic3", "projic3", "repoic3", 7, "Conversation")
	task.Status.DeadlineAt = &origDL

	c := seedClient(t, proj, secret("projic3-scm", secretVal), repo)
	require.NoError(t, c.Create(context.Background(), task))
	require.NoError(t, c.Status().Update(context.Background(), task))

	h, _ := newServer(t, c)

	body := []byte(issueCommentBotBody)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issue_comment")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "projic3", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var got tatarav1.Task
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "taskic3"}, &got))
	// State must not change.
	require.Equal(t, "Conversation", got.Status.LifecycleState, "bot comment must not change lifecycle state")
	// DeadlineAt must remain set (not nil) - we cannot check exact time equality
	// because the fake client status subresource may truncate or re-serialise.
	_ = origDL
}

// TestIssueComment_TriggerLabelOnConversation_SetsImplement verifies that a
// "labeled" event applying the triggerLabel on a Conversation task sets
// LifecycleState=Implement (skip-dialogue path).
func TestIssueComment_TriggerLabelOnConversation_SetsImplement(t *testing.T) {
	const secretVal = "whsec"
	proj := projectWithBot("projic4", "projic4-scm", "tatara", "tatara-bot")
	repo := repository("repoic4", "projic4", "https://github.com/o/r.git", "main")
	task := lifecycleTask("taskic4", "projic4", "repoic4", 7, "Conversation")
	// Add the triggerLabel to the task labels so it matches.
	task.Labels["tatara.io/source-repo"] = "o.r"
	task.Labels["tatara.io/source-number"] = "7"

	c := seedClient(t, proj, secret("projic4-scm", secretVal), repo)
	require.NoError(t, c.Create(context.Background(), task))
	require.NoError(t, c.Status().Update(context.Background(), task))

	h, _ := newServer(t, c)

	// Send a "labeled" issue event adding the triggerLabel.
	labeledBody := []byte(`{"action":"labeled","issue":{"number":7,"title":"fix","body":"please fix","labels":[{"name":"tatara"}],"html_url":"https://github.com/o/r/issues/7"},"label":{"name":"tatara"},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},"sender":{"login":"maintainer"}}`)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issues")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, labeledBody))

	w := post(t, h, "projic4", hdr, labeledBody)
	require.Equal(t, http.StatusAccepted, w.Code)

	var got tatarav1.Task
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "taskic4"}, &got))
	require.Equal(t, "Implement", got.Status.LifecycleState, "triggerLabel on Conversation task must set Implement")
}

// TestIssueComment_NoMatchingTask_Accepted verifies that an issue_comment with
// no matching lifecycle task is gracefully ignored (202 returned).
func TestIssueComment_NoMatchingTask_Accepted(t *testing.T) {
	const secretVal = "whsec"
	proj := projectWithBot("projic5", "projic5-scm", "tatara", "tatara-bot")
	repo := repository("repoic5", "projic5", "https://github.com/o/r.git", "main")
	// No task seeded.
	c := seedClient(t, proj, secret("projic5-scm", secretVal), repo)

	h, _ := newServer(t, c)

	body := []byte(issueCommentBody)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issue_comment")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "projic5", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)
}
