package controller

// Regression test locking issueLifecycle bot-authorship enforcement at the
// MRCI egress gate (lifecycle_mrci.go:41-62).
// Confirms no coverage was lost when the former selfImproveBotAuthored gate
// was deleted (Task 13 deletion).

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// authorshipFakeWriter is an SCMWriter that returns a configurable PR author
// and CI status from GetPRState, and counts Merge calls.
type authorshipFakeWriter struct {
	scm.SCMWriter
	mu         sync.Mutex
	prAuthor   string
	ciStatus   string
	mergeCalls int
}

func (f *authorshipFakeWriter) GetPRState(_ context.Context, _, _ string, _ int) (scm.PRState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return scm.PRState{Author: f.prAuthor, CIStatus: f.ciStatus}, nil
}

func (f *authorshipFakeWriter) Merge(_ context.Context, _, _ string, _ int, _ string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mergeCalls++
	return "", nil
}

func (f *authorshipFakeWriter) Comment(_ context.Context, _, _, _ string) error { return nil }

// newLifecycleTestReconciler builds a TaskReconciler wired with the given SCMWriter.
func newLifecycleTestReconciler(t *testing.T, fw scm.SCMWriter) *TaskReconciler {
	t.Helper()
	r := newLifecycleReconciler(t, nil)
	r.SCMFor = func(string) (scm.SCMWriter, error) { return fw, nil }
	return r
}

const (
	lcAuthorshipProj   = "lc-authorship-proj"
	lcAuthorshipRepo   = "lc-authorship-repo"
	lcAuthorshipSecret = "lc-authorship-sec"
	lcAuthorshipTask   = "lc-authorship-task"
)

// newLifecycleTaskInMRCI seeds a minimal project+repo+secret+issueLifecycle task
// in MRCI state. The project BotLogin is set to "szymonrychu-bot".
// repoName parameterises the GitHub repository slug; prNumber is the seeded PR.
func newLifecycleTaskInMRCI(t *testing.T, r *TaskReconciler, repoName string, prNumber int) *tatarav1alpha1.Task {
	t.Helper()
	ctx := context.Background()

	mkSecret(t, lcAuthorshipSecret, map[string][]byte{"token": []byte("tok"), "webhookSecret": []byte("wh")})

	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: lcAuthorshipProj, Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: lcAuthorshipSecret,
			Scm: &tatarav1alpha1.ScmSpec{
				Provider: "github", Owner: "szymonrychu", BotLogin: "szymonrychu-bot",
			},
			Agent: tatarav1alpha1.AgentSpec{
				Model: "claude-x", Image: "wrapper:1", PermissionMode: "bypassPermissions",
				MaxTurnsPerTask: 50, TurnTimeoutSeconds: 1800,
			},
		},
	}
	if err := r.Create(ctx, proj); err != nil {
		t.Fatalf("create project: %v", err)
	}
	proj.Status.Memory = &tatarav1alpha1.MemoryStatus{Phase: "Ready", Endpoint: "http://mem.svc:8080"}
	if err := r.Status().Update(ctx, proj); err != nil {
		t.Fatalf("set memory ready: %v", err)
	}

	repo := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: lcAuthorshipRepo, Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef:       lcAuthorshipProj,
			URL:              fmt.Sprintf("https://github.com/szymonrychu/%s.git", repoName),
			DefaultBranch:    "main",
			ReingestSchedule: "0 6 * * *",
		},
	}
	if err := r.Create(ctx, repo); err != nil {
		t.Fatalf("create repo: %v", err)
	}

	prRef := fmt.Sprintf("szymonrychu/%s#%d", repoName, prNumber)
	prWebURL := fmt.Sprintf("https://github.com/szymonrychu/%s/pull/%d", repoName, prNumber)
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: lcAuthorshipTask, Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    lcAuthorshipProj,
			RepositoryRef: lcAuthorshipRepo,
			Goal:          fmt.Sprintf("fix issue #%d", prNumber),
			Kind:          "issueLifecycle",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github",
				IssueRef: prRef,
				URL:      prWebURL,
				IsPR:     true,
				Number:   prNumber,
			},
		},
	}
	if err := r.Create(ctx, task); err != nil {
		t.Fatalf("create task: %v", err)
	}
	task.Status.LifecycleState = "MRCI"
	task.Status.PRNumber = prNumber
	task.Status.PrURL = prWebURL
	task.Status.HeadBranch = "tatara/task-" + lcAuthorshipTask
	if err := r.Status().Update(ctx, task); err != nil {
		t.Fatalf("seed mrci state: %v", err)
	}
	return task
}

// testProject fetches the project seeded by newLifecycleTaskInMRCI.
func testProject(t *testing.T) *tatarav1alpha1.Project {
	t.Helper()
	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Namespace: testNS, Name: lcAuthorshipProj}, &proj))
	return &proj
}

// TestMRCI_NonBotPRParks verifies that handleMRCI parks the task when the PR
// is not authored by the bot, and never calls Merge.
func TestMRCI_NonBotPRParks(t *testing.T) {
	fw := &authorshipFakeWriter{prAuthor: "some-human", ciStatus: "success"}
	r := newLifecycleTestReconciler(t, fw)
	task := newLifecycleTaskInMRCI(t, r, "tatara-operator", 267) // LifecycleState=MRCI, bot login=szymonrychu-bot
	_, err := r.handleMRCI(context.Background(), testProject(t), task)
	require.NoError(t, err)
	var got tatarav1alpha1.Task
	require.NoError(t, r.Get(context.Background(), client.ObjectKeyFromObject(task), &got))
	assert.Equal(t, "Parked", got.Status.LifecycleState, "non-bot PR must park, never merge")
	fw.mu.Lock()
	mergeCalls := fw.mergeCalls
	fw.mu.Unlock()
	assert.Equal(t, 0, mergeCalls, "must never merge a non-bot PR")
}
