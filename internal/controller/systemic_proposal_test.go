package controller

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// capturingWriter extends fakeProposalWriter to capture the IssueReq.
type capturingWriter struct {
	scm.SCMWriter
	mu  sync.Mutex
	req scm.IssueReq
}

func (c *capturingWriter) CreateIssue(_ context.Context, _, _ string, req scm.IssueReq) (scm.CreatedIssue, error) {
	c.mu.Lock()
	c.req = req
	c.mu.Unlock()
	return scm.CreatedIssue{Ref: "o/r#100", URL: "https://github.com/o/r/issues/100"}, nil
}

func (c *capturingWriter) AddBoardItem(_ context.Context, _ string, _ scm.BoardRef, _ string) error {
	return nil
}

func (c *capturingWriter) captured() scm.IssueReq {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.req
}

// seedSystemicTask seeds a Task with ProposedIssue.SystemicID set.
func seedSystemicTask(t *testing.T, name, proj, repo, secret, title, systemicID string) *tatarav1alpha1.Task {
	t.Helper()
	ctx := context.Background()

	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secret, Namespace: testNS},
		Data:       map[string][]byte{"token": []byte("tok"), "webhookSecret": []byte("w")},
	}
	require.NoError(t, k8sClient.Create(ctx, sec))

	project := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: proj, Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef: secret,
			Scm: &tatarav1alpha1.ScmSpec{
				Provider: "github",
				Owner:    "o",
				BotLogin: "tatara-bot",
			},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, project))
	project.Status.Memory = stableMemStatus("http://mem.svc")
	require.NoError(t, k8sClient.Status().Update(ctx, project))

	r := &tatarav1alpha1.Repository{
		ObjectMeta: metav1.ObjectMeta{Name: repo, Namespace: testNS},
		Spec: tatarav1alpha1.RepositorySpec{
			ProjectRef:       proj,
			URL:              "https://github.com/o/r.git",
			DefaultBranch:    "main",
			ReingestSchedule: "0 6 * * *",
		},
	}
	require.NoError(t, k8sClient.Create(ctx, r))

	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    proj,
			RepositoryRef: repo,
			Kind:          "implement",
			Goal:          title,
			ProposedIssue: &tatarav1alpha1.ProposedIssueSpec{
				RepositoryRef: repo,
				Title:         title,
				Body:          "description of the systemic proposal",
				Kind:          "improvement",
				SystemicID:    systemicID,
			},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, task))

	var fresh tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, &fresh))
	return &fresh
}

// TestCreateProposal_SystemicLabelAndFooter verifies that when SystemicID is
// set, the created issue carries the tatara/systemic-<id> label and the footer.
func TestCreateProposal_SystemicLabelAndFooter(t *testing.T) {
	cw := &capturingWriter{}
	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		SCMFor:  func(string) (scm.SCMWriter, error) { return cw, nil },
	}

	task := seedSystemicTask(t, "systemic-prop-1", "systemic-proj-1", "systemic-repo-1", "systemic-scm-1",
		"Add CI health survey to brainstorm context", "grp1")

	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))

	_, err := r.createProposal(context.Background(), &proj, task)
	require.NoError(t, err)

	req := cw.captured()

	// Label tatara/systemic-grp1 must be present.
	found := false
	for _, l := range req.Labels {
		if l == "tatara/systemic-grp1" {
			found = true
		}
	}
	require.True(t, found, "expected label tatara/systemic-grp1 in %v", req.Labels)

	// Footer must mention the systemicID.
	require.True(t, strings.Contains(req.Body, "Part of systemic improvement grp1"),
		"expected systemic footer in body: %q", req.Body)
}

// TestCreateProposal_NoSystemicID_NoExtraLabel verifies that without SystemicID
// no systemic label is added and the body has no systemic footer.
func TestCreateProposal_NoSystemicID_NoExtraLabel(t *testing.T) {
	cw := &capturingWriter{}
	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		SCMFor:  func(string) (scm.SCMWriter, error) { return cw, nil },
	}

	task := seedProposalTask(t, "systemic-plain-1", "systemic-plain-proj", "systemic-plain-repo", "systemic-plain-scm",
		"Regular standalone proposal title here")

	var proj tatarav1alpha1.Project
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Spec.ProjectRef}, &proj))

	_, err := r.createProposal(context.Background(), &proj, task)
	require.NoError(t, err)

	req := cw.captured()

	for _, l := range req.Labels {
		if strings.HasPrefix(l, "tatara/systemic-") {
			t.Fatalf("unexpected systemic label %q on standalone proposal", l)
		}
	}
	require.False(t, strings.Contains(req.Body, "Part of systemic improvement"),
		"unexpected systemic footer in standalone body: %q", req.Body)
}
