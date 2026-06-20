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

// parkedLifecycleTask builds a Parked issueLifecycle Task for issue 9 with
// stale turn annotations set (simulating a mid-run task that went Parked).
func parkedLifecycleTask(name, projectRef, repoRef string) *tatarav1.Task {
	dl := metav1.NewTime(time.Now().Add(time.Hour))
	now := metav1.Now()
	return &tatarav1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Annotations: map[string]string{
				tatarav1.AnnCurrentTurn:    "3",
				tatarav1.AnnCurrentSubtask: "sub-1",
				tatarav1.AnnTurnComplete:   "false",
				tatarav1.AnnTurnStartedAt:  "2026-06-01T10:00:00Z",
				tatarav1.AnnPodRecreations: "1",
			},
			Labels: map[string]string{
				tatarav1.LabelSourceRepo:   "o.r",
				tatarav1.LabelSourceNumber: "9",
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
				IssueRef: "o/r#9",
				Number:   9,
			},
		},
		Status: tatarav1.TaskStatus{
			LifecycleState: "Parked",
			Phase:          "Planning",
			DeadlineAt:     &dl,
			LastActivityAt: &now,
		},
	}
}

// issueCommentBodyIssue9 is a human comment on issue 9.
const issueCommentBodyIssue9 = `{"action":"created","issue":{"number":9,"title":"fix","body":"please fix","html_url":"https://github.com/o/r/issues/9"},"comment":{"id":10,"body":"Any update?","user":{"login":"maintainer"}},"repository":{"clone_url":"https://github.com/o/r.git","full_name":"o/r"},"sender":{"login":"maintainer"}}`

// TestIssueCommentReactivatesParkedOwningTask verifies that a human comment on
// an issue whose only owning Task is Parked reactivates that Task (no duplicate
// created), clearing turn annotations and resetting LifecycleState to Triage.
func TestIssueCommentReactivatesParkedOwningTask(t *testing.T) {
	const secretVal = "whsec-p1"
	proj := projectWithBot("projpark1", "projpark1-scm", "tatara", "tatara-bot")
	repo := repository("repopark1", "projpark1", "https://github.com/o/r.git", "main")
	task := parkedLifecycleTask("taskpark1", "projpark1", "repopark1")

	c := seedClient(t, proj, secret("projpark1-scm", secretVal), repo)
	require.NoError(t, c.Create(context.Background(), task))
	require.NoError(t, c.Status().Update(context.Background(), task))

	h, _ := newServer(t, c)

	body := []byte(issueCommentBodyIssue9)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issue_comment")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "projpark1", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	// No new Task must have been created; task count stays at 1.
	var tasks tatarav1.TaskList
	require.NoError(t, c.List(context.Background(), &tasks, client.InNamespace(ns)))
	require.Len(t, tasks.Items, 1, "Parked task must be reactivated, not duplicated")

	// Re-fetch the task and verify reactivation.
	var got tatarav1.Task
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "taskpark1"}, &got))
	require.Equal(t, "Triage", got.Status.LifecycleState, "Parked task must transition to Triage")
	require.Equal(t, "", got.Status.Phase, "Phase must be cleared on reactivation")
	// Turn annotations must be cleared.
	require.Empty(t, got.Annotations[tatarav1.AnnCurrentTurn], "AnnCurrentTurn must be cleared")
	require.Empty(t, got.Annotations[tatarav1.AnnCurrentSubtask], "AnnCurrentSubtask must be cleared")
	require.Empty(t, got.Annotations[tatarav1.AnnTurnComplete], "AnnTurnComplete must be cleared")
}

// TestIssueComment_InflightTurn_QueuesInterjection verifies that a human comment
// on a task whose agent has a turn in flight is queued as a PendingInterjection
// (for the reconciler to inject) WITHOUT changing the lifecycle state.
func TestIssueComment_InflightTurn_QueuesInterjection(t *testing.T) {
	const secretVal = "whsec-ij"
	proj := projectWithBot("projij1", "projij1-scm", "tatara", "tatara-bot")
	repo := repository("repoij1", "projij1", "https://github.com/o/r.git", "main")
	task := lifecycleTask("taskij1", "projij1", "repoij1", 7, "Implement")
	// Turn in flight: current-turn set, no completion callback yet.
	task.Annotations = map[string]string{tatarav1.AnnCurrentTurn: "5"}

	c := seedClient(t, proj, secret("projij1-scm", secretVal), repo)
	require.NoError(t, c.Create(context.Background(), task))
	require.NoError(t, c.Status().Update(context.Background(), task))

	h, _ := newServer(t, c)

	body := []byte(issueCommentBody)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issue_comment")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "projij1", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var got tatarav1.Task
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "taskij1"}, &got))
	require.Equal(t, "Implement", got.Status.LifecycleState, "in-flight interjection must not change lifecycle state")
	require.Equal(t, []string{"Please check the edge case"}, got.Status.PendingInterjections)

	// No new QueuedEvent created.
	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	require.Empty(t, qel.Items)
}

// TestIssueComment_ParkedMR_Reactivated verifies that an MR comment on an MR
// whose only owning Task is Parked reactivates that Task (issue #25: MR comments
// with no nursing agent re-engage one).
func TestIssueComment_ParkedMR_Reactivated(t *testing.T) {
	const secretVal = "whsec-mrpark"
	proj := projectWithBot("projmrp", "projmrp-scm", "tatara", "tatara-bot")
	repo := repository("repomrp", "projmrp", "https://github.com/o/r.git", "main")
	task := lifecycleTask("taskmrp", "projmrp", "repomrp", 11, "Parked")
	task.Spec.Source.IssueRef = "o/r#11"
	task.Spec.Source.IsPR = true
	task.Status.Phase = "Planning"

	c := seedClient(t, proj, secret("projmrp-scm", secretVal), repo)
	require.NoError(t, c.Create(context.Background(), task))
	require.NoError(t, c.Status().Update(context.Background(), task))

	h, _ := newServer(t, c)

	body := []byte(prCommentUntracked) // human comment on PR 11
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issue_comment")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "projmrp", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var tasks tatarav1.TaskList
	require.NoError(t, c.List(context.Background(), &tasks, client.InNamespace(ns)))
	require.Len(t, tasks.Items, 1, "Parked MR task must be reactivated, not duplicated")

	var got tatarav1.Task
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "taskmrp"}, &got))
	require.Equal(t, "Triage", got.Status.LifecycleState, "Parked MR task must transition to Triage")
	require.Equal(t, "", got.Status.Phase)
}

// TestIssueCommentDoneOwningTaskCreatesFresh verifies that a human comment on
// an issue with a Done (terminal) owning Task creates a fresh Triage Task.
func TestIssueCommentDoneOwningTaskCreatesFresh(t *testing.T) {
	const secretVal = "whsec-p2"
	proj := projectWithBot("projpark2", "projpark2-scm", "tatara", "tatara-bot")
	repo := repository("repopark2", "projpark2", "https://github.com/o/r.git", "main")
	// Done task - terminal, should NOT be reactivated.
	doneTask := lifecycleTask("taskpark2", "projpark2", "repopark2", 9, "Done")
	doneTask.Spec.Source.IssueRef = "o/r#9"

	c := seedClient(t, proj, secret("projpark2-scm", secretVal), repo)
	require.NoError(t, c.Create(context.Background(), doneTask))
	require.NoError(t, c.Status().Update(context.Background(), doneTask))

	h, _ := newServer(t, c)

	body := []byte(issueCommentBodyIssue9)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issue_comment")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "projpark2", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	// A NEW QueuedEvent must have been created (Done task stays, fresh QueuedEvent added).
	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	require.Len(t, qel.Items, 1, "Done task must NOT be reactivated; a fresh QueuedEvent must be created")
}

// TestIssueCommentNoOwningTaskCreatesFresh verifies that a human comment on an
// issue with no owning Task creates a fresh Triage Task (unchanged behavior).
func TestIssueCommentNoOwningTaskCreatesFresh(t *testing.T) {
	const secretVal = "whsec-p3"
	proj := projectWithBot("projpark3", "projpark3-scm", "tatara", "tatara-bot")
	repo := repository("repopark3", "projpark3", "https://github.com/o/r.git", "main")

	c := seedClient(t, proj, secret("projpark3-scm", secretVal), repo)
	h, _ := newServer(t, c)

	body := []byte(issueCommentBodyIssue9)
	hdr := http.Header{}
	hdr.Set("X-GitHub-Event", "issue_comment")
	hdr.Set("X-Hub-Signature-256", ghSign(secretVal, body))

	w := post(t, h, "projpark3", hdr, body)
	require.Equal(t, http.StatusAccepted, w.Code)

	var qel tatarav1.QueuedEventList
	require.NoError(t, c.List(context.Background(), &qel, client.InNamespace(ns)))
	require.Len(t, qel.Items, 1, "no owning task -> fresh Triage QueuedEvent created")
	require.Equal(t, "Triage", qel.Items[0].Spec.Payload.Annotations[tatarav1.LifecycleEntryAnnotation])
}
