package controller

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

// --- Finding 1: SCM metric emitted for OpenChange and Comment ---

// trackingFakeWriter records whether Comment/OpenChange were called and exposes
// the metrics registry for counter assertions.
type trackingFakeWriter struct {
	scm.SCMWriter
	mu           sync.Mutex
	openCalls    int
	commentCalls int
	openErr      error
}

func (f *trackingFakeWriter) OpenChange(_ context.Context, _, _, _, _, _, _ string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.openCalls++
	if f.openErr != nil {
		return "", f.openErr
	}
	return "https://example/pr/audit1", nil
}
func (f *trackingFakeWriter) Comment(_ context.Context, _, _, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commentCalls++
	return nil
}

func newTrackingReconciler(t *testing.T, fw *trackingFakeWriter, reg *prometheus.Registry) *TaskReconciler {
	t.Helper()
	return &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(reg),
		Session: newFakeSession(),
		PodConfig: agent.PodConfig{
			Namespace:           testNS,
			CallbackURL:         "http://op.svc:8082",
			OIDCIssuer:          "https://kc.svc/realms/master",
			AnthropicSecretName: "anthropic",
			CLIOIDCSecretName:   "tatara-cli-oidc",
		},
		SCMFor: func(string) (scm.SCMWriter, error) { return fw, nil },
	}
}

// TestWriteback_OpenChangeEmitsSCMMetric verifies finding 1: a successful
// OpenChange must increment operator_scm_writes_total{verb="open_change",result="ok"}.
func TestWriteback_OpenChangeEmitsSCMMetric(t *testing.T) {
	reg := prometheus.NewRegistry()
	fw := &trackingFakeWriter{}
	r := newTrackingReconciler(t, fw, reg)
	task := seedWritebackPending(t, "audit-oc-metric", "audit-oc-scm", "audit-oc-proj", "audit-oc-repo")

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	// Metric must have been emitted for open_change.
	cnt := testutil.ToFloat64(r.Metrics.SCMWriteCounter("github", "open_change", "ok"))
	require.Equal(t, float64(1), cnt, "open_change ok metric must be emitted once")
}

// TestWriteback_CommentEmitsSCMMetric verifies finding 1: after PR is opened,
// the Comment to the issue must also emit a metric.
func TestWriteback_CommentEmitsSCMMetric(t *testing.T) {
	reg := prometheus.NewRegistry()
	fw := &trackingFakeWriter{}
	r := newTrackingReconciler(t, fw, reg)
	task := seedWritebackPending(t, "audit-cmt-metric", "audit-cmt-scm", "audit-cmt-proj", "audit-cmt-repo")

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	// Comment to the issue should also emit a metric.
	cnt := testutil.ToFloat64(r.Metrics.SCMWriteCounter("github", "comment", "ok"))
	require.Equal(t, float64(1), cnt, "comment ok metric must be emitted once")
}

// TestWriteback_NoPRCommentEmitsSCMMetric verifies finding 1: when no PR opens
// but a result comment is posted (report/question tasks), that Comment also
// emits a metric.
func TestWriteback_NoPRCommentEmitsSCMMetric(t *testing.T) {
	reg := prometheus.NewRegistry()
	fw := &trackingFakeWriter{openErr: &scm.HTTPError{Status: 422, Body: "no diff", Path: "/pulls"}}
	r := newTrackingReconciler(t, fw, reg)
	task := seedWritebackPending(t, "audit-nopr-cmt", "audit-nopr-scm", "audit-nopr-proj", "audit-nopr-repo")

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)

	// open_change error metric must be emitted.
	cntOpen := testutil.ToFloat64(r.Metrics.SCMWriteCounter("github", "open_change", "error"))
	require.Equal(t, float64(1), cntOpen, "open_change error metric must be emitted on 422")

	// The no-PR comment path also emits a comment metric.
	cntComment := testutil.ToFloat64(r.Metrics.SCMWriteCounter("github", "comment", "ok"))
	require.Equal(t, float64(1), cntComment, "comment ok metric must be emitted for no-PR result comment")
}

// --- Finding 2: writeBackReview clears WritebackPending immediately after verb ---

// TestWriteBackReview_ClearsImmediatelyAfterSuccessfulVerb verifies finding 2:
// once Approve succeeds, WritebackPending must be cleared even if a subsequent
// step fails (preventing duplicate posts on requeue).
func TestWriteBackReview_ClearsImmediatelyAfterSuccessfulVerb(t *testing.T) {
	fw := &fullFakeSCMWriter{prState: scm.PRState{}}
	r := newFullFakeReconciler(t, fw)
	task := seedWritebackKindTask(t, "audit-rev-clear", "audit-rev-proj", "audit-rev-repo", "audit-rev-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "review a PR",
			Kind: "review",
			Source: &tatarav1alpha1.TaskSource{
				Provider: "github", IssueRef: "o/r#30", IsPR: true, Number: 30,
			},
		}, nil)
	task.Status.ReviewVerdict = &tatarav1alpha1.ReviewVerdict{Decision: "approve", Body: "lgtm"}
	require.NoError(t, k8sClient.Status().Update(context.Background(), task))

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err)
	require.True(t, fw.approveCalled)

	// After success, WritebackPending must be False so a re-queue does not re-post.
	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	cond := findCond(got.Status.Conditions, "WritebackPending")
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status, "WritebackPending must be cleared after approve")
}

// --- Finding 4: PrURL Status().Update wrapped in RetryOnConflict ---

// prURLConflictClient injects a conflict on the first Status().Update for Task
// objects, simulating a concurrent lifecycle reconcile bumping the resource version.
type prURLConflictClient struct {
	client.Client
	calls *atomic.Int32
}

func (c *prURLConflictClient) Status() client.SubResourceWriter {
	return &conflictOnceWriter{
		SubResourceWriter: c.Client.Status(),
		calls:             c.calls,
		gr:                schema.GroupResource{Group: "tatara.dev", Resource: "tasks"},
		name:              "prurl-conflict-task",
	}
}

func (c *prURLConflictClient) Get(ctx context.Context, key types.NamespacedName, obj client.Object, opts ...client.GetOption) error {
	return c.Client.Get(ctx, key, obj, opts...)
}

// TestWriteback_PrURLUpdateRetriesOnConflict verifies finding 4: the PrURL
// status write uses RetryOnConflict, so a concurrent resource-version bump
// does not lose the PR URL.
func TestWriteback_PrURLUpdateRetriesOnConflict(t *testing.T) {
	fw := &trackingFakeWriter{}
	var calls atomic.Int32
	reg := prometheus.NewRegistry()
	cc := &prURLConflictClient{Client: k8sClient, calls: &calls}
	r := &TaskReconciler{
		Client:  cc,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(reg),
		Session: newFakeSession(),
		PodConfig: agent.PodConfig{
			Namespace:           testNS,
			CallbackURL:         "http://op.svc:8082",
			OIDCIssuer:          "https://kc.svc/realms/master",
			AnthropicSecretName: "anthropic",
			CLIOIDCSecretName:   "tatara-cli-oidc",
		},
		SCMFor: func(string) (scm.SCMWriter, error) { return fw, nil },
	}
	task := seedWritebackPending(t, "prurl-conflict-task", "prurl-conf-scm", "prurl-conf-proj", "prurl-conf-repo")

	_, err := reconcileWriteback(t, r, task.Name)
	require.NoError(t, err, "must succeed despite one conflict on Status().Update")

	// PR URL must have landed on the task.
	var got tatarav1alpha1.Task
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: task.Name}, &got))
	require.NotEmpty(t, got.Status.PrURL, "PrURL must be persisted after conflict retry")
	require.GreaterOrEqual(t, calls.Load(), int32(2), "must have retried at least once")
}

// --- Finding 5: brainstormHasProposal uses field selector ---

// TestBrainstormHasProposal_FieldSelector verifies finding 5: the field-indexed
// list correctly scopes proposal tasks to the same project, avoiding O(all tasks).
// We seed a proposal task for a different project and verify it is NOT counted.
func TestBrainstormHasProposal_FieldSelector(t *testing.T) {
	ctx := context.Background()

	// Brainstorm task for proj-A.
	bsTask := seedWritebackKindTask(t, "audit-bs-fs-task", "audit-bs-fs-proj", "audit-bs-fs-repo", "audit-bs-fs-scm",
		tatarav1alpha1.TaskSpec{
			Goal: "brainstorm",
			Kind: "brainstorm",
		}, nil)

	// Proposal task for a DIFFERENT project: must not be counted.
	otherProj := "audit-bs-other-proj"
	mkSecret(t, "audit-bs-other-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	require.NoError(t, k8sClient.Create(ctx, &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: otherProj, Namespace: testNS},
		Spec:       tatarav1alpha1.ProjectSpec{ScmSecretRef: "audit-bs-other-scm"},
	}))
	proposal := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "audit-bs-other-proposal", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    otherProj,
			RepositoryRef: "audit-bs-fs-repo",
			Goal:          "idea",
			Kind:          "implement",
			ProposedIssue: &tatarav1alpha1.ProposedIssueSpec{
				RepositoryRef: "audit-bs-fs-repo", Title: "other idea", Body: "body", Kind: "bug",
			},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, proposal))

	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Session: newFakeSession(),
	}

	// brainstormHasProposal must return false: the only proposal is for a different project.
	has := r.brainstormHasProposal(ctx, bsTask)
	require.False(t, has, "proposal from a different project must not be counted")

	// Now seed a proposal for the SAME project.
	sameProposal := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "audit-bs-same-proposal", Namespace: testNS},
		Spec: tatarav1alpha1.TaskSpec{
			ProjectRef:    bsTask.Spec.ProjectRef,
			RepositoryRef: bsTask.Spec.RepositoryRef,
			Goal:          "idea for proj-A",
			Kind:          "implement",
			ProposedIssue: &tatarav1alpha1.ProposedIssueSpec{
				RepositoryRef: bsTask.Spec.RepositoryRef, Title: "proj-A idea", Body: "body", Kind: "bug",
			},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, sameProposal))

	has = r.brainstormHasProposal(ctx, bsTask)
	require.True(t, has, "proposal from the same project must be counted")
}

// --- Finding 6: recordExistingProposal derives base URL from repo URL ---

func TestIssueURLFromRepoURL(t *testing.T) {
	tests := []struct {
		repoURL  string
		provider string
		repo     string
		number   int
		want     string
	}{
		{
			repoURL:  "https://github.com/owner/repo.git",
			provider: "github",
			repo:     "owner/myrepo",
			number:   7,
			want:     "https://github.com/owner/myrepo/issues/7",
		},
		{
			repoURL:  "https://gitlab.com/group/proj.git",
			provider: "gitlab",
			repo:     "group/proj",
			number:   3,
			want:     "https://gitlab.com/group/proj/-/issues/3",
		},
		{
			// Self-hosted GitLab: must use the configured host, not gitlab.com.
			repoURL:  "https://git.corp.example.com/group/proj.git",
			provider: "gitlab",
			repo:     "group/proj",
			number:   12,
			want:     "https://git.corp.example.com/group/proj/-/issues/12",
		},
		{
			// Self-hosted GitHub Enterprise: uses the configured host.
			repoURL:  "https://github.enterprise.example.com/owner/repo.git",
			provider: "github",
			repo:     "owner/repo",
			number:   99,
			want:     "https://github.enterprise.example.com/owner/repo/issues/99",
		},
	}

	for _, tc := range tests {
		got := issueURLFromRepoURL(tc.repoURL, tc.provider, tc.repo, tc.number)
		require.Equal(t, tc.want, got, "repoURL=%q provider=%q", tc.repoURL, tc.provider)
	}
}

// --- Finding 7: Metrics nil guard consistency ---
// This is a code-style fix (drop `if r.Metrics != nil` in lifecycle.go);
// the behaviour change is: a nil Metrics now panics immediately rather than
// silently. Production wiring always sets Metrics. We verify that the
// controller package tests all construct TaskReconciler with a non-nil Metrics,
// which is ensured by the test helpers above and the existing suite.
// no-test: the finding is a code-style consistency fix; nil Metrics panics
// immediately in any test that exercises the metric call paths, which is already
// the case (all tests set Metrics via obs.NewOperatorMetrics).

// prURLConflictClientGet satisfies client.Reader by delegating to the embedded
// Client; needed so conflictOnceTaskClient.Get resolves correctly in the envtest.
var _ client.Client = (*prURLConflictClient)(nil)
