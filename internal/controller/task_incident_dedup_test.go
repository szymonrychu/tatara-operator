package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// dedupComment records one writer.Comment call for assertions.
type dedupComment struct {
	issueRef string
	body     string
}

// incidentDedupFakeWriter is a minimal scm.SCMWriter double for the layer-1
// dedup gate: GetIssueState returns Closed=true for any "repoSlug#number" key
// present in closed, open (Closed=false) otherwise; Comment just records the
// call. Embedding scm.SCMWriter satisfies every other interface method with a
// nil-panic-on-call stub, matching the existing fakeProposalWriter convention
// in proposal_dedup_test.go - none of those other methods are exercised here.
type incidentDedupFakeWriter struct {
	scm.SCMWriter
	closed   map[string]bool
	comments []dedupComment
}

func (f *incidentDedupFakeWriter) GetIssueState(_ context.Context, repoURL, _ string, number int) (scm.IssueState, error) {
	key := fmt.Sprintf("%s#%d", repoURL, number)
	return scm.IssueState{Closed: f.closed[key]}, nil
}

func (f *incidentDedupFakeWriter) Comment(_ context.Context, _, issueRef, body string) error {
	f.comments = append(f.comments, dedupComment{issueRef: issueRef, body: body})
	return nil
}

// seedIncidentDedupProject creates the minimum objects the gate reads before
// it ever calls SCMFor: an scm-token secret and a Project with Spec.Scm set
// (provider/owner/bot login) and Status.Memory already stably Ready (so the
// admission memory-gate never interferes with these tests, mirroring
// setProjectMemoryReady/stableMemStatus's effect used elsewhere in this
// package).
func seedIncidentDedupProject(t *testing.T, name string) {
	t.Helper()
	ctx := context.Background()
	secret := name + "-scm"
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secret, Namespace: testNS},
		Data:       map[string][]byte{"token": []byte("tok")},
	}
	require.NoError(t, k8sClient.Create(ctx, sec))
	p := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef:       secret,
			Scm:                &tatarav1alpha1.ScmSpec{Provider: "github", Owner: "o", BotLogin: "tatara-bot"},
			MaxConcurrentTasks: 3,
			Agent: tatarav1alpha1.AgentSpec{
				Model: "claude-x", Image: "wrapper:1", PermissionMode: "bypassPermissions",
				MaxTurnsPerTask: 50, TurnTimeoutSeconds: 1800,
			},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, p))
	p.Status.Memory = stableMemStatus("http://mem-" + name + ".tatara.svc:8080")
	require.NoError(t, k8sClient.Status().Update(ctx, p))
}

// seedPriorIncidentTask creates a terminal (Succeeded) Kind=incident Task
// carrying alertRule and a single tracked issue URL in
// Status.DiscoveredIssues, simulating an earlier incident investigation that
// already opened (and is tracking) an issue - terminal on purpose, since the
// gate must match terminal-INCLUDED prior Tasks (dedup is not limited to
// in-flight Tasks).
func seedPriorIncidentTask(t *testing.T, name, projectRef, alertRule, issueURL string) {
	t.Helper()
	ctx := context.Background()
	tk := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: projectRef, Kind: "incident", Goal: "investigate", AlertRule: alertRule},
	}
	require.NoError(t, k8sClient.Create(ctx, tk))
	tk.Status.Phase = "Succeeded"
	tk.Status.DiscoveredIssues = []string{issueURL}
	require.NoError(t, k8sClient.Status().Update(ctx, tk))
}

// newIncidentDedupReconciler builds a TaskReconciler wired with fw as the
// only SCM writer (no ReaderFor, matching the gate's own call - gatedComment
// tolerates a nil ReaderFor, see comment_gate.go:294) and fs as the agent
// session, so a fall-through case can still reach a real spawn.
func newIncidentDedupReconciler(fw scm.SCMWriter, fs agent.Session) *TaskReconciler {
	return &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
		Session: fs,
		SCMFor:  func(string) (scm.SCMWriter, error) { return fw, nil },
		PodConfig: agent.PodConfig{
			Namespace:           testNS,
			CallbackURL:         "http://op-internal.tatara.svc:8082",
			OIDCIssuer:          "https://keycloak.tatara.svc/realms/master",
			AnthropicSecretName: "anthropic",
			CLIOIDCSecretName:   "tatara-cli-oidc",
		},
	}
}

// TestTaskReconcile_IncidentDedup_OpenIssueSkipsSpawn: a prior incident Task
// with the same AlertRule and a still-open tracker issue must terminate the
// new Task Succeeded/IncidentDuplicate, spawn no pod, and post exactly one
// re-fire comment on the existing issue.
func TestTaskReconcile_IncidentDedup_OpenIssueSkipsSpawn(t *testing.T) {
	seedIncidentDedupProject(t, "p-idd-open")
	seedPriorIncidentTask(t, "t-idd-open-prior", "p-idd-open", "HighErrorRate", "https://github.com/o/r/issues/42")
	mkIncidentTask(t, "t-idd-open-new", "p-idd-open", "HighErrorRate")

	fw := &incidentDedupFakeWriter{closed: map[string]bool{}}
	r := newIncidentDedupReconciler(fw, newFakeSession())

	if _, err := reconcileTask(t, r, "t-idd-open-new"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	tk := getTask(t, "t-idd-open-new")
	require.Equal(t, "Succeeded", tk.Status.Phase, "duplicate incident must terminate Succeeded")
	cond := findCond(tk.Status.Conditions, "Ready")
	require.NotNil(t, cond)
	require.Equal(t, "IncidentDuplicate", cond.Reason)

	pod := &corev1.Pod{}
	err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: agent.PodName(tk)}, pod)
	require.True(t, apierrors.IsNotFound(err), "duplicate incident must not spawn a pod, got err=%v", err)

	require.Len(t, fw.comments, 1, "exactly one re-fire comment must be posted")
	require.Equal(t, "o/r#42", fw.comments[0].issueRef)
	require.Contains(t, fw.comments[0].body, "HighErrorRate")
}

// TestTaskReconcile_IncidentDedup_ClosedIssueFallsThrough: a prior incident
// Task with the same AlertRule but a CLOSED tracker issue (the fix
// regressed) must NOT short-circuit - the new Task proceeds to a normal
// spawn (Phase=Planning, pod created).
func TestTaskReconcile_IncidentDedup_ClosedIssueFallsThrough(t *testing.T) {
	seedIncidentDedupProject(t, "p-idd-closed")
	seedPriorIncidentTask(t, "t-idd-closed-prior", "p-idd-closed", "HighErrorRate", "https://github.com/o/r/issues/42")
	mkIncidentTask(t, "t-idd-closed-new", "p-idd-closed", "HighErrorRate")

	fw := &incidentDedupFakeWriter{closed: map[string]bool{"o/r#42": true}}
	r := newIncidentDedupReconciler(fw, newFakeSession())

	if _, err := reconcileTask(t, r, "t-idd-closed-new"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	tk := getTask(t, "t-idd-closed-new")
	require.Equal(t, "Planning", tk.Status.Phase, "closed prior tracker issue must fall through to a normal spawn")

	pod := &corev1.Pod{}
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: agent.PodName(tk)}, pod),
		"expected a spawned pod on fall-through")
	require.Empty(t, fw.comments, "no re-fire comment when the gate does not fire")
}

// TestTaskReconcile_IncidentDedup_NoPriorTaskFallsThrough: no prior incident
// Task exists for this AlertRule at all - normal spawn, untouched by the gate.
func TestTaskReconcile_IncidentDedup_NoPriorTaskFallsThrough(t *testing.T) {
	seedIncidentDedupProject(t, "p-idd-none")
	mkIncidentTask(t, "t-idd-none-new", "p-idd-none", "HighErrorRate")

	fw := &incidentDedupFakeWriter{closed: map[string]bool{}}
	r := newIncidentDedupReconciler(fw, newFakeSession())

	if _, err := reconcileTask(t, r, "t-idd-none-new"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	tk := getTask(t, "t-idd-none-new")
	require.Equal(t, "Planning", tk.Status.Phase, "no prior Task for this AlertRule must spawn normally")
	pod := &corev1.Pod{}
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: agent.PodName(tk)}, pod))
	require.Empty(t, fw.comments)
}

// TestTaskReconcile_IncidentDedup_DifferentAlertRuleFallsThrough: a prior
// incident Task with an OPEN tracker issue but a DIFFERENT AlertRule must not
// match (alertname-precision, not DedupKey) - normal spawn.
func TestTaskReconcile_IncidentDedup_DifferentAlertRuleFallsThrough(t *testing.T) {
	seedIncidentDedupProject(t, "p-idd-diff")
	seedPriorIncidentTask(t, "t-idd-diff-prior", "p-idd-diff", "OtherAlert", "https://github.com/o/r/issues/42")
	mkIncidentTask(t, "t-idd-diff-new", "p-idd-diff", "HighErrorRate")

	fw := &incidentDedupFakeWriter{closed: map[string]bool{}}
	r := newIncidentDedupReconciler(fw, newFakeSession())

	if _, err := reconcileTask(t, r, "t-idd-diff-new"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	tk := getTask(t, "t-idd-diff-new")
	require.Equal(t, "Planning", tk.Status.Phase, "a different AlertRule must not match the dedup gate")
	pod := &corev1.Pod{}
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: agent.PodName(tk)}, pod))
	require.Empty(t, fw.comments)
}
