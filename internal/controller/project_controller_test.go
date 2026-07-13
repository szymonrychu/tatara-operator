package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	tataradevv1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/memory"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

func newProjectReconciler() *ProjectReconciler {
	r, _ := newProjectReconcilerWithReg()
	return r
}

func newProjectReconcilerWithReg() (*ProjectReconciler, *prometheus.Registry) {
	reg := prometheus.NewRegistry()
	return &ProjectReconciler{
		Client:              k8sClient,
		Scheme:              k8sClient.Scheme(),
		Metrics:             obs.NewOperatorMetrics(reg),
		ExternalWebhookBase: "https://tatara.example/operator/webhooks",
		MemoryConfig: memory.Config{
			Namespace:        testNS,
			MemoryImage:      "harbor.example/tatara-memory:test",
			LightragImage:    "harbor.example/lightrag:test",
			Neo4jImage:       "neo4j:5-community",
			OpenAISecretName: "openai-shared",
			OIDCIssuer:       "https://keycloak.example/realms/tatara",
			OIDCAudience:     "tatara-memory",
		},
	}, reg
}

func reconcileProject(t *testing.T, name string) (ctrl.Result, error) {
	t.Helper()
	r := newProjectReconciler()
	return r.Reconcile(logf.IntoContext(context.Background(), logf.Log), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: name},
	})
}

func mkSecret(t *testing.T, name string, data map[string][]byte) {
	t.Helper()
	s := &corev1.Secret{}
	s.Name = name
	s.Namespace = testNS
	s.Data = data
	if err := k8sClient.Create(context.Background(), s); err != nil {
		t.Fatalf("create secret %s: %v", name, err)
	}
}

func getProject(t *testing.T, name string) *tataradevv1alpha1.Project {
	t.Helper()
	p := &tataradevv1alpha1.Project{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, p); err != nil {
		t.Fatalf("get project %s: %v", name, err)
	}
	return p
}

func TestReconcileProject_ComputesCounts(t *testing.T) {
	mkTaskProject(t, "p-counts", 3)
	mkTaskRepository(t, "r-counts-a", "p-counts")
	mkTaskRepository(t, "r-counts-b", "p-counts")
	mkTaskWithKind(t, "t-issue-open", "p-counts", "r-counts-a", "clarify")
	mkTaskWithKind(t, "t-incident-open", "p-counts", "r-counts-a", "incident")
	mkTaskWithKindTerminal(t, "t-issue-closed", "p-counts", "r-counts-a", "clarify")

	if _, err := reconcileProject(t, "p-counts"); err != nil {
		t.Fatalf("reconcileProject: %v", err)
	}

	proj := getProject(t, "p-counts")
	if proj.Status.RepositoryCount != 2 {
		t.Errorf("RepositoryCount = %d, want 2", proj.Status.RepositoryCount)
	}
	if proj.Status.OpenIssuesCount != 1 {
		t.Errorf("OpenIssuesCount = %d, want 1 (terminal excluded)", proj.Status.OpenIssuesCount)
	}
	if proj.Status.OpenIncidentsCount != 1 {
		t.Errorf("OpenIncidentsCount = %d, want 1", proj.Status.OpenIncidentsCount)
	}
}

func waitProjectReady(t *testing.T, name string, want metav1.ConditionStatus) *tataradevv1alpha1.Project {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		p := getProject(t, name)
		c := apierrors.FindStatusCondition(p.Status.Conditions, "Ready")
		if c != nil && c.Status == want {
			return p
		}
		time.Sleep(interval)
	}
	t.Fatalf("project %s Ready never reached %s", name, want)
	return nil
}

func TestProjectReconcile_ValidSecret(t *testing.T) {
	ctx := context.Background()
	mkSecret(t, "valid-scm", map[string][]byte{
		"token":         []byte("ghp_x"),
		"webhookSecret": []byte("hmac"),
	})
	p := &tataradevv1alpha1.Project{}
	p.Name = "proj-valid"
	p.Namespace = testNS
	p.Spec.ScmSecretRef = "valid-scm"
	if err := k8sClient.Create(ctx, p); err != nil {
		t.Fatalf("create project: %v", err)
	}

	if _, err := reconcileProject(t, "proj-valid"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	got := waitProjectReady(t, "proj-valid", metav1.ConditionTrue)
	want := "https://tatara.example/operator/webhooks/proj-valid"
	if got.Status.WebhookURL != want {
		t.Errorf("webhookURL = %q, want %q", got.Status.WebhookURL, want)
	}
}

func TestProjectReconcile_MissingSecret(t *testing.T) {
	ctx := context.Background()
	p := &tataradevv1alpha1.Project{}
	p.Name = "proj-nosecret"
	p.Namespace = testNS
	p.Spec.ScmSecretRef = "does-not-exist"
	if err := k8sClient.Create(ctx, p); err != nil {
		t.Fatalf("create project: %v", err)
	}

	if _, err := reconcileProject(t, "proj-nosecret"); err != nil {
		t.Fatalf("reconcile returned error, want nil (status carries failure): %v", err)
	}
	got := waitProjectReady(t, "proj-nosecret", metav1.ConditionFalse)
	c := apierrors.FindStatusCondition(got.Status.Conditions, "Ready")
	if c.Reason != "SecretNotFound" {
		t.Errorf("reason = %q, want SecretNotFound", c.Reason)
	}
}

func TestProjectReconcile_MissingKeys(t *testing.T) {
	ctx := context.Background()
	mkSecret(t, "partial-scm", map[string][]byte{"token": []byte("ghp_x")})
	p := &tataradevv1alpha1.Project{}
	p.Name = "proj-partialkeys"
	p.Namespace = testNS
	p.Spec.ScmSecretRef = "partial-scm"
	if err := k8sClient.Create(ctx, p); err != nil {
		t.Fatalf("create project: %v", err)
	}

	if _, err := reconcileProject(t, "proj-partialkeys"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := waitProjectReady(t, "proj-partialkeys", metav1.ConditionFalse)
	c := apierrors.FindStatusCondition(got.Status.Conditions, "Ready")
	if c.Reason != "SecretMissingKeys" {
		t.Errorf("reason = %q, want SecretMissingKeys", c.Reason)
	}
}

// TestGaugeRecomputeThrottled verifies that maybeRecomputeGauges skips the
// expensive ProjectList+TaskList scans when called within the throttle interval
// and runs them once the interval has elapsed.
func TestGaugeRecomputeThrottled(t *testing.T) {
	ctx := logf.IntoContext(context.Background(), logf.Log)

	// Two reconcilers with different intervals to test behaviour precisely.
	r := newProjectReconciler()
	// Short interval: first call should fire; second immediate call should skip.
	r.GaugeRecomputeInterval = 5 * time.Minute

	// First call: lastGaugeRecompute is zero, so recompute must run.
	before := r.lastGaugeRecompute
	r.maybeRecomputeGauges(ctx)
	if !r.lastGaugeRecompute.After(before) {
		t.Fatal("first maybeRecomputeGauges call did not update lastGaugeRecompute")
	}
	after := r.lastGaugeRecompute

	// Immediate second call: interval not elapsed, so lastGaugeRecompute must NOT change.
	r.maybeRecomputeGauges(ctx)
	if !r.lastGaugeRecompute.Equal(after) {
		t.Fatal("second immediate maybeRecomputeGauges call updated lastGaugeRecompute; expected skip")
	}

	// Backdate lastGaugeRecompute past the interval and confirm a third call fires.
	r.lastGaugeRecompute = time.Now().Add(-r.GaugeRecomputeInterval - time.Second)
	r.maybeRecomputeGauges(ctx)
	if !r.lastGaugeRecompute.After(after) {
		t.Fatal("third maybeRecomputeGauges call (interval elapsed) did not update lastGaugeRecompute")
	}
}

// gatherIssueState reads tatara_issue_state for the given issue from reg.
// Returns the gauge value if the series exists, or -1 when absent.
func gatherIssueState(t *testing.T, reg *prometheus.Registry, issue, state, incident string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "tatara_issue_state" {
			continue
		}
		for _, m := range mf.GetMetric() {
			var gotIssue, gotState, gotIncident string
			for _, lp := range m.GetLabel() {
				switch lp.GetName() {
				case "issue":
					gotIssue = lp.GetValue()
				case "state":
					gotState = lp.GetValue()
				case "incident":
					gotIncident = lp.GetValue()
				}
			}
			if gotIssue == issue && gotState == state && gotIncident == incident {
				return m.GetGauge().GetValue()
			}
		}
	}
	return -1
}

// TestUpdateIssueStateCounts_EmitsPerIssue verifies that a LIVE Task with an
// issue-scoped Source emits one gauge series labelled with its STAGE.
func TestUpdateIssueStateCounts_EmitsPerIssue(t *testing.T) {
	ctx := context.Background()
	r, reg := newProjectReconcilerWithReg()
	mkSecret(t, "isc-proj-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	mkProject(t, "isc-proj", "isc-proj-scm")
	_ = mkRepo(t, "isc-repo", "isc-proj")

	task := &tataradevv1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "isc-task-1", Namespace: testNS},
		Spec: tataradevv1alpha1.TaskSpec{
			ProjectRef: "isc-proj",
			Kind:       "clarify",
			Goal:       "test",
			Source: &tataradevv1alpha1.TaskSource{
				Provider: "github", IssueRef: "acme/repo#42", Number: 42,
			},
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task: %v", err)
	}
	task.Status.Stage = tataradevv1alpha1.StageImplementing
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("set stage: %v", err)
	}

	r.updateIssueStateCounts(ctx)

	if got := gatherIssueState(t, reg, "acme/repo#42", "implementing", "false"); got != 1 {
		t.Fatalf("tatara_issue_state{issue=acme/repo#42,state=implementing,incident=false} = %v, want 1", got)
	}
}

// TestUpdateIssueStateCounts_SkipsFinished: a Task whose work is over drops out
// of the gauge entirely (the Reset() before each pass is what makes it durable).
func TestUpdateIssueStateCounts_SkipsFinished(t *testing.T) {
	ctx := context.Background()
	r, reg := newProjectReconcilerWithReg()
	mkSecret(t, "iscd-proj-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	mkProject(t, "iscd-proj", "iscd-proj-scm")

	task := &tataradevv1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "iscd-task-1", Namespace: testNS},
		Spec: tataradevv1alpha1.TaskSpec{
			ProjectRef: "iscd-proj",
			Kind:       "clarify",
			Goal:       "test",
			Source: &tataradevv1alpha1.TaskSource{
				Provider: "github", IssueRef: "acme/repo#77", Number: 77,
			},
		},
	}
	if err := k8sClient.Create(ctx, task); err != nil {
		t.Fatalf("create task: %v", err)
	}
	task.Status.Stage = tataradevv1alpha1.StageDelivered
	if err := k8sClient.Status().Update(ctx, task); err != nil {
		t.Fatalf("set stage: %v", err)
	}

	r.updateIssueStateCounts(ctx)

	if got := gatherIssueState(t, reg, "acme/repo#77", "delivered", "false"); got != -1 {
		t.Fatalf("a delivered Task must emit no tatara_issue_state series; got %v", got)
	}
}

func TestIssueStateFor(t *testing.T) {
	cases := []struct {
		name  string
		stage string
		want  string
	}{
		{"triaging", tataradevv1alpha1.StageTriaging, "triaging"},
		{"clarifying", tataradevv1alpha1.StageClarifying, "clarifying"},
		{"implementing", tataradevv1alpha1.StageImplementing, "implementing"},
		{"reviewing", tataradevv1alpha1.StageReviewing, "reviewing"},
		{"merging", tataradevv1alpha1.StageMerging, "merging"},
		{"deploying", tataradevv1alpha1.StageDeploying, "deploying"},
		{"delivered is finished", tataradevv1alpha1.StageDelivered, ""},
		{"parked is finished", tataradevv1alpha1.StageParked, ""},
		{"failed is finished", tataradevv1alpha1.StageFailed, ""},
		{"rejected is finished", tataradevv1alpha1.StageRejected, ""},
		{"unstamped", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := &tataradevv1alpha1.Task{}
			task.Status.Stage = tc.stage
			if got := issueStateFor(task); got != tc.want {
				t.Errorf("issueStateFor(stage=%q) = %q, want %q", tc.stage, got, tc.want)
			}
		})
	}
}

// TestUpdateTaskStageGauges_CountAgeAndReset guards contract K.1's
// operator_task_stage (a COUNT per stage,kind bucket, not per-task) and
// operator_task_stage_age_seconds (per-task). A distinctive kind isolates the
// count assertion from every other test's Tasks sharing this envtest
// namespace. The Reset-then-recompute pass proves a removed Task's series is
// gone (contract M22), not just left stale.
func TestUpdateTaskStageGauges_CountAgeAndReset(t *testing.T) {
	ctx := context.Background()
	r, _ := newProjectReconcilerWithReg()
	mkSecret(t, "tsg-proj-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	mkProject(t, "tsg-proj", "tsg-proj-scm")

	// refine is a valid CRD-enum kind that never traverses implementing/reviewing
	// (its agent stage is refining -> delivered), so no other test in this shared
	// envtest namespace pollutes the {implementing|reviewing, refine} count buckets.
	const kind = "refine"
	entered := metav1.NewTime(time.Now().Add(-90 * time.Second))

	t1 := &tataradevv1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "tsg-task-1", Namespace: testNS},
		Spec:       tataradevv1alpha1.TaskSpec{ProjectRef: "tsg-proj", Kind: kind, Goal: "g1"},
	}
	if err := k8sClient.Create(ctx, t1); err != nil {
		t.Fatalf("create t1: %v", err)
	}
	t1.Status.Stage = tataradevv1alpha1.StageImplementing
	t1.Status.StageEnteredAt = &entered
	if err := k8sClient.Status().Update(ctx, t1); err != nil {
		t.Fatalf("set t1 status: %v", err)
	}

	t2 := &tataradevv1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "tsg-task-2", Namespace: testNS},
		Spec:       tataradevv1alpha1.TaskSpec{ProjectRef: "tsg-proj", Kind: kind, Goal: "g2"},
	}
	if err := k8sClient.Create(ctx, t2); err != nil {
		t.Fatalf("create t2: %v", err)
	}
	t2.Status.Stage = tataradevv1alpha1.StageReviewing
	t2.Status.StageEnteredAt = &entered
	if err := k8sClient.Status().Update(ctx, t2); err != nil {
		t.Fatalf("set t2 status: %v", err)
	}

	r.updateTaskStageGauges(ctx)

	if got := testutil.ToFloat64(r.Metrics.TaskStageGauge(tataradevv1alpha1.StageImplementing, kind)); got != 1 {
		t.Fatalf("operator_task_stage{implementing,%s} = %v, want 1", kind, got)
	}
	if got := testutil.ToFloat64(r.Metrics.TaskStageGauge(tataradevv1alpha1.StageReviewing, kind)); got != 1 {
		t.Fatalf("operator_task_stage{reviewing,%s} = %v, want 1", kind, got)
	}
	if got := testutil.ToFloat64(r.Metrics.TaskStageAgeGauge("tsg-task-1", tataradevv1alpha1.StageImplementing, kind)); got < 80 || got > 300 {
		t.Fatalf("operator_task_stage_age_seconds{tsg-task-1} = %v, want ~90", got)
	}
	if got := testutil.ToFloat64(r.Metrics.TaskStageAgeGauge("tsg-task-2", tataradevv1alpha1.StageReviewing, kind)); got < 80 || got > 300 {
		t.Fatalf("operator_task_stage_age_seconds{tsg-task-2} = %v, want ~90", got)
	}

	// Remove t2 and recompute: its stage bucket and its per-task age series
	// must both be gone, not left retaining their last value.
	if err := k8sClient.Delete(ctx, t2); err != nil {
		t.Fatalf("delete t2: %v", err)
	}
	r.updateTaskStageGauges(ctx)

	if got := testutil.ToFloat64(r.Metrics.TaskStageGauge(tataradevv1alpha1.StageReviewing, kind)); got != 0 {
		t.Fatalf("operator_task_stage{reviewing,%s} after delete = %v, want 0 (series gone)", kind, got)
	}
	if got := testutil.ToFloat64(r.Metrics.TaskStageAgeGauge("tsg-task-2", tataradevv1alpha1.StageReviewing, kind)); got != 0 {
		t.Fatalf("operator_task_stage_age_seconds{tsg-task-2} after delete = %v, want 0 (series gone)", got)
	}
	if got := testutil.ToFloat64(r.Metrics.TaskStageGauge(tataradevv1alpha1.StageImplementing, kind)); got != 1 {
		t.Fatalf("operator_task_stage{implementing,%s} after unrelated delete = %v, want 1 (unaffected)", kind, got)
	}
}

// TestUpdateQueueAgeGauge_OldestPerBucket guards contract K.1's
// operator_queue_age_seconds: the age of the OLDEST QueuedEvent in a bucket,
// not the newest. This envtest namespace is shared cluster-wide across the
// whole package's test run, so the assertion does not require the bucket to
// be empty beforehand - it only requires the reported age to be at least as
// old as the deliberately-older event and clearly older than the
// deliberately-newer one, which holds regardless of any other test's leftover
// QueuedEvents in the same bucket.
func TestUpdateQueueAgeGauge_OldestPerBucket(t *testing.T) {
	ctx := context.Background()
	r, _ := newProjectReconcilerWithReg()
	mkSecret(t, "qag-proj-scm", map[string][]byte{"token": []byte("t"), "webhookSecret": []byte("w")})
	mkProject(t, "qag-proj", "qag-proj-scm")

	priority := 1
	older := &tataradevv1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "qag-older-", Namespace: testNS},
		Spec: tataradevv1alpha1.QueuedEventSpec{
			Seq: 1, Class: tataradevv1alpha1.QueueClassNormal, Kind: "incident",
			ProjectRef: "qag-proj", Priority: &priority,
			Payload: tataradevv1alpha1.QueuedEventPayload{Kind: "incident"},
		},
	}
	if err := k8sClient.Create(ctx, older); err != nil {
		t.Fatalf("create older QueuedEvent: %v", err)
	}

	time.Sleep(1500 * time.Millisecond)

	newer := &tataradevv1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "qag-newer-", Namespace: testNS},
		Spec: tataradevv1alpha1.QueuedEventSpec{
			Seq: 2, Class: tataradevv1alpha1.QueueClassNormal, Kind: "incident",
			ProjectRef: "qag-proj", Priority: &priority,
			Payload: tataradevv1alpha1.QueuedEventPayload{Kind: "incident"},
		},
	}
	if err := k8sClient.Create(ctx, newer); err != nil {
		t.Fatalf("create newer QueuedEvent: %v", err)
	}

	r.updateQueueAgeGauge(ctx)

	olderAge := time.Since(older.CreationTimestamp.Time).Seconds()
	newerAge := time.Since(newer.CreationTimestamp.Time).Seconds()

	got := testutil.ToFloat64(r.Metrics.QueueAgeGauge(tataradevv1alpha1.QueueClassNormal, "1", tataradevv1alpha1.QueueStateQueued))
	if got < olderAge-0.5 {
		t.Fatalf("operator_queue_age_seconds = %v, want >= %v (at least as old as the OLDER event)", got, olderAge)
	}
	if got <= newerAge+0.4 {
		t.Fatalf("operator_queue_age_seconds = %v looks like it picked the NEWER event (age %v); want the OLDER one to win", got, newerAge)
	}
}

var _ = client.IgnoreNotFound
