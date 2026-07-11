package controller

// Task 10: pr_outcome=close egress for issueLifecycle (bot-gated close egress).

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// closeSignalWriter records ClosePR calls and returns a configurable author from GetPRState.
type closeSignalWriter struct {
	lifecycleFakeSCMWriter
	author        string
	closeCalls    int
	lastCloseBody string
}

func (f *closeSignalWriter) GetPRState(_ context.Context, _, _ string, _ int) (scm.PRState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return scm.PRState{Author: f.author, CIStatus: "success", Closed: false}, nil
}

func (f *closeSignalWriter) ClosePR(_ context.Context, _, _ string, _ int, body string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeCalls++
	f.lastCloseBody = body
	return nil
}

// seedSucceededImplementTaskWithOpenPR seeds a project (with BotLogin=botLogin),
// a repo, and a task in Phase=Succeeded with PrURL set to PR #prNumber. Returns
// the task.
func seedSucceededImplementTaskWithOpenPR(t *testing.T, r *TaskReconciler, suffix string, botLogin string, prNumber int) *tatarav1alpha1.Task {
	t.Helper()
	ctx := context.Background()

	name := "lc-co-" + suffix
	projName := "lc-co-proj-" + suffix
	repoName := "lc-co-repo-" + suffix
	secName := "lc-co-sec-" + suffix

	mkSecret(t, secName, map[string][]byte{"token": []byte("tok"), "webhookSecret": []byte("wh")})

	scmSpec := &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: botLogin}
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: projName, Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: secName,
			Scm:          scmSpec,
			Agent: tatarav1alpha1.AgentSpec{
				Model: "claude-x", Image: "wrapper:1", PermissionMode: "bypassPermissions",
				MaxTurnsPerTask: 50, TurnTimeoutSeconds: 1800,
			},
		},
	}
	if err := k8sClient.Create(ctx, proj); err != nil {
		t.Fatalf("create project %s: %v", projName, err)
	}
	proj.Status.Memory = stableMemStatus("http://mem.svc:8080")
	if err := k8sClient.Status().Update(ctx, proj); err != nil {
		t.Fatalf("set memory ready: %v", err)
	}

	repo := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: repoName, Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef:       projName,
			URL:              "https://github.com/o/r.git",
			DefaultBranch:    "main",
			ReingestSchedule: "0 6 * * *",
		},
	}
	if err := k8sClient.Create(ctx, repo); err != nil {
		t.Fatalf("create repo %s: %v", repoName, err)
	}

	src := &tatarav1alpha1.TaskSource{
		Provider: "github", IssueRef: "o/r#5", URL: "https://github.com/o/r/issues/5", Number: 5,
	}
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    projName,
			RepositoryRef: repoName,
			Goal:          "issue goal",
			Kind:          "issueLifecycle",
			Source:        src,
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task %s: %v", name, err)
	}

	// Set Succeeded phase with open PR.
	task.Status.Phase = "Succeeded"
	task.Status.DeployState = "Implement"
	task.Status.PrURL = "https://github.com/o/r/pull/267"
	task.Status.PRNumber = prNumber
	task.Status.HeadBranch = "tatara/task-" + name
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed implement status: %v", err)
	}

	// Refresh to get latest resourceVersion.
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(task), task); err != nil {
		t.Fatalf("refresh task: %v", err)
	}
	return task
}

// TestFinishImplement_PROutcomeCloseClosesPR verifies that when the agent signals
// pr_outcome=close on a bot-authored PR, the operator closes the PR and
// sets DeployState=Stopped.
func TestFinishImplement_PROutcomeCloseClosesPR(t *testing.T) {
	fw := &closeSignalWriter{author: "szymonrychu-bot"}
	r := newLifecycleReconciler(t, &fw.lifecycleFakeSCMWriter)
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	task := seedSucceededImplementTaskWithOpenPR(t, r, "bot", "szymonrychu-bot", 267)
	task.Status.PROutcome = &tatarav1alpha1.PROutcome{Action: "close", Reason: "superseded by #265"}
	require.NoError(t, r.Status().Update(context.Background(), task))

	// Refresh so task has the PROutcome set in its Status.
	require.NoError(t, r.Get(context.Background(), client.ObjectKeyFromObject(task), task))

	_, err := r.finishImplement(context.Background(), task)
	require.NoError(t, err)

	assert.Equal(t, 1, fw.closeCalls, "must close the PR")
	assert.Equal(t, "superseded by #265", fw.lastCloseBody)

	var got tatarav1alpha1.Task
	require.NoError(t, r.Get(context.Background(), client.ObjectKeyFromObject(task), &got))
	assert.Equal(t, "Stopped", got.Status.DeployState)
}

// TestFinishImplement_PROutcomeCloseNonBotParks verifies that when the agent
// signals close for a non-bot-authored PR, the operator withholds the close
// and parks the task instead.
func TestFinishImplement_PROutcomeCloseNonBotParks(t *testing.T) {
	fw := &closeSignalWriter{author: "some-human"}
	r := newLifecycleReconciler(t, &fw.lifecycleFakeSCMWriter)
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }

	task := seedSucceededImplementTaskWithOpenPR(t, r, "human", "szymonrychu-bot", 267)
	task.Status.PROutcome = &tatarav1alpha1.PROutcome{Action: "close", Reason: "superseded"}
	require.NoError(t, r.Status().Update(context.Background(), task))

	// Refresh so task has the PROutcome set in its Status.
	require.NoError(t, r.Get(context.Background(), client.ObjectKeyFromObject(task), task))

	_, err := r.finishImplement(context.Background(), task)
	require.NoError(t, err)

	assert.Equal(t, 0, fw.closeCalls, "must NOT close a non-bot PR")

	var got tatarav1alpha1.Task
	require.NoError(t, r.Get(context.Background(), client.ObjectKeyFromObject(task), &got))
	assert.Equal(t, "Parked", got.Status.DeployState)
}
