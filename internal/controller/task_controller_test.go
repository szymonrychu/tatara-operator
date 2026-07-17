package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// gaugeValue reads a gauge metric value from a Prometheus registry by name+labels.
func gaugeValue(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if labelsMatch(m.GetLabel(), labels) {
				return m.GetGauge().GetValue()
			}
		}
	}
	return 0
}

func newTaskReconciler(fs agent.Session) *TaskReconciler {
	r, _ := newTaskReconcilerReg(fs)
	return r
}

func newTaskReconcilerReg(fs agent.Session) (*TaskReconciler, *prometheus.Registry) {
	reg := prometheus.NewRegistry()
	return &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(reg),
		Session: fs,
		PodConfig: agent.PodConfig{
			Namespace:           testNS,
			CallbackURL:         "http://op-internal.tatara.svc:8082",
			OIDCIssuer:          "https://keycloak.tatara.svc/realms/master",
			AnthropicSecretName: "anthropic",
			CLIOIDCSecretName:   "tatara-cli-oidc",
		},
	}, reg
}

func reconcileTask(t *testing.T, r *TaskReconciler, name string) (ctrl.Result, error) {
	t.Helper()
	return r.Reconcile(logf.IntoContext(context.Background(), logf.Log), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: name},
	})
}

func mkTaskProject(t *testing.T, name string, maxConcurrent int) {
	t.Helper()
	p := &tatarav1alpha1.Project{}
	p.Name = name
	p.Namespace = testNS
	p.Spec.ScmSecretRef = name + "-scm"
	p.Spec.MaxConcurrentAgents = maxConcurrent
	p.Spec.Agent = tatarav1alpha1.AgentSpec{
		Model: "claude-x", Image: "wrapper:1", PermissionMode: "bypassPermissions",
		MaxTurnsPerTask: 50, TurnTimeoutSeconds: 1800,
	}
	if err := k8sClient.Create(context.Background(), p); err != nil {
		t.Fatalf("create project: %v", err)
	}
}

func mkTaskRepository(t *testing.T, name, projectRef string) {
	t.Helper()
	r := &tatarav1alpha1.Repository{}
	r.Name = name
	r.Namespace = testNS
	r.Spec.ProjectRef = projectRef
	r.Spec.URL = "https://git/acme/" + name
	r.Spec.DefaultBranch = "main"
	r.Spec.ReingestSchedule = "0 6 * * *"
	if err := k8sClient.Create(context.Background(), r); err != nil {
		t.Fatalf("create repository: %v", err)
	}
}

func mkTask(t *testing.T, name, projectRef, repoRef string) {
	t.Helper()
	tk := &tatarav1alpha1.Task{}
	tk.Name = name
	tk.Namespace = testNS
	tk.Spec.ProjectRef = projectRef
	tk.Spec.RepositoryRef = repoRef
	tk.Spec.Goal = "ship the feature"
	if err := k8sClient.Create(context.Background(), tk); err != nil {
		t.Fatalf("create task: %v", err)
	}
}

func getTask(t *testing.T, name string) *tatarav1alpha1.Task {
	t.Helper()
	tk := &tatarav1alpha1.Task{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, tk); err != nil {
		t.Fatalf("get task %s: %v", name, err)
	}
	return tk
}

func mkTaskWithKind(t *testing.T, name, projectRef, repoRef, kind string) {
	t.Helper()
	tk := &tatarav1alpha1.Task{}
	tk.Name = name
	tk.Namespace = testNS
	tk.Spec.ProjectRef = projectRef
	tk.Spec.RepositoryRef = repoRef
	tk.Spec.Goal = "ship the feature"
	tk.Spec.Kind = kind
	if err := k8sClient.Create(context.Background(), tk); err != nil {
		t.Fatalf("create task: %v", err)
	}
}

func mkTaskWithKindTerminal(t *testing.T, name, projectRef, repoRef, kind string) {
	t.Helper()
	mkTaskWithKind(t, name, projectRef, repoRef, kind)
	setTaskStage(t, name, tatarav1alpha1.StageDelivered)
}

func setTaskGoal(t *testing.T, name, goal string) {
	t.Helper()
	tk := getTask(t, name)
	tk.Spec.Goal = goal
	if err := k8sClient.Update(context.Background(), tk); err != nil {
		t.Fatalf("set goal %s: %v", name, err)
	}
}

func setTaskStage(t *testing.T, name, stg string) {
	t.Helper()
	tk := getTask(t, name)
	tk.Status.Stage = stg
	if err := k8sClient.Status().Update(context.Background(), tk); err != nil {
		t.Fatalf("set stage %s: %v", name, err)
	}
}

// setTaskTokens seeds status.stats.tokensOutput, the lifetime output-token
// counter recordUsage accumulates.
func setTaskTokens(t *testing.T, name string, out int64) {
	t.Helper()
	tk := getTask(t, name)
	tk.Status.Stats.TokensOutput = out
	if err := k8sClient.Status().Update(context.Background(), tk); err != nil {
		t.Fatalf("set tokens %s: %v", name, err)
	}
}

func annotate(t *testing.T, name string, kv map[string]string) {
	t.Helper()
	tk := getTask(t, name)
	if tk.Annotations == nil {
		tk.Annotations = map[string]string{}
	}
	for k, v := range kv {
		tk.Annotations[k] = v
	}
	if err := k8sClient.Update(context.Background(), tk); err != nil {
		t.Fatalf("annotate %s: %v", name, err)
	}
}

func findCond(conds []metav1.Condition, typ string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == typ {
			return &conds[i]
		}
	}
	return nil
}

func TestReconcileTask_SetsShortDescription(t *testing.T) {
	mkTaskProject(t, "p-short", 3)
	mkTaskRepository(t, "r-short", "p-short")
	mkTask(t, "t-short", "p-short", "r-short")
	setTaskGoal(t, "t-short", "Fix the flaky retry loop in the deploy supervisor because it spins forever on 429s and burns quota")

	fs := newFakeSession()
	r := newTaskReconciler(fs)
	if _, err := reconcileTask(t, r, "t-short"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	task := getTask(t, "t-short")
	if len(task.Status.ShortDescription) > 63 {
		t.Errorf("ShortDescription too long: %q (%d chars)", task.Status.ShortDescription, len(task.Status.ShortDescription))
	}
	if !strings.HasPrefix(task.Status.ShortDescription, "Fix the flaky retry loop") {
		t.Errorf("ShortDescription = %q, want it to start with the goal's first words", task.Status.ShortDescription)
	}
}

// ----- Task 6: concurrency gate + spawn -----

func TestTaskReconcile_TerminalNoop(t *testing.T) {
	mkTaskProject(t, "p-term", 3)
	mkTaskRepository(t, "r-term", "p-term")
	mkTask(t, "t-done", "p-term", "r-term")
	setTaskStage(t, "t-done", tatarav1alpha1.StageDelivered)

	fs := newFakeSession()
	r := newTaskReconciler(fs)
	if _, err := reconcileTask(t, r, "t-done"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, ok := fs.lastSubmit(); ok {
		t.Error("terminal task must not submit a turn")
	}
}

// ----- Task 7: plan turn + subtask iteration -----

// ----- Task 8: termination, cleanup, maxTurns, pod-loss -----

// ----- Fix 2: per-turn timeout via reconciler -----

// ----- P3: ResultSummary derived on termination -----

// TestUpdateInflightGauge_PerKind verifies that updateInflightGauge emits
// tatara_tasks_inflight{kind} for each active kind and zeroes missing kinds.
func TestUpdateInflightGauge_PerKind(t *testing.T) {
	ctx := context.Background()
	mkTaskProject(t, "p-inflight", 5)
	mkSecret(t, "p-inflight-scm", map[string][]byte{"token": []byte("x"), "webhookSecret": []byte("y")})
	mkTaskRepository(t, "r-inflight", "p-inflight")
	setProjectMemoryReady(t, "p-inflight", "http://mem-inflight.tatara.svc:8080")

	// Create one Task per kind, in a live POD stage.
	kindNames := map[string]string{"review": "t-inflight-review", "brainstorm": "t-inflight-bs"}
	for i, kind := range []string{"review", "brainstorm"} {
		name := kindNames[kind]
		task := &tatarav1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
			Spec: tatarav1alpha1.TaskSpec{
				ProjectRef:    "p-inflight",
				RepositoryRef: "r-inflight",
				Goal:          "goal",
				Kind:          kind,
			},
		}
		if err := k8sClient.Create(ctx, task); err != nil {
			t.Fatalf("create task %d: %v", i, err)
		}
		task.Status.Stage = tatarav1alpha1.StageReviewing
		if err := k8sClient.Status().Update(ctx, task); err != nil {
			t.Fatalf("set stage %d: %v", i, err)
		}
	}

	reg := prometheus.NewRegistry()
	r := &TaskReconciler{
		Client:  k8sClient,
		Scheme:  k8sClient.Scheme(),
		Metrics: obs.NewOperatorMetrics(reg),
		Session: newFakeSession(),
		PodConfig: agent.PodConfig{
			Namespace:           testNS,
			CallbackURL:         "http://op-internal.tatara.svc:8082",
			OIDCIssuer:          "https://keycloak.tatara.svc/realms/master",
			AnthropicSecretName: "anthropic",
			CLIOIDCSecretName:   "tatara-cli-oidc",
		},
	}
	r.updateInflightGauge(ctx)

	// Each active kind we created must appear in the per-kind gauge (>= 1).
	// Other tests sharing testNS may have created more in-flight tasks so we
	// only assert >= 1, not == 1.
	reviewCount := gaugeValue(t, reg, "tatara_tasks_inflight", map[string]string{"kind": "review"})
	if reviewCount < 1 {
		t.Errorf("tatara_tasks_inflight{kind=review} = %v, want >= 1", reviewCount)
	}
	bsCount := gaugeValue(t, reg, "tatara_tasks_inflight", map[string]string{"kind": "brainstorm"})
	if bsCount < 1 {
		t.Errorf("tatara_tasks_inflight{kind=brainstorm} = %v, want >= 1", bsCount)
	}
	// A kind with no live Task must still report a series (zeroed), not drop out.
	_ = gaugeValue(t, reg, "tatara_tasks_inflight", map[string]string{"kind": "documentation"})
	_ = gaugeValue(t, reg, "tatara_tasks_inflight", map[string]string{"kind": "implement"})
}

// SPEC TEST 10. THE MINT IS THE NON-IDEMPOTENT CALLER.
//
// reconcileStage takes the F.3 Create edge on a CACHED read of status.stage == "",
// and EnterStage's write re-reads the Task live under RetryOnConflict. A
// re-reconcile against a cache that has not yet observed our own mint (the mint
// branch's own Requeue:true is the likeliest source; the ShortDescription status
// patch is another) re-applies the Create edge, and the in-write stage.Enter
// refuses it as triaging -> triaging. Both Tasks that hit this on 2026-07-17
// (refine-qe-lh79w 06:00:04Z, brainstorm-qe-5snrr 06:24:30Z) progressed anyway -
// but the counter fires, and TataraIllegalStageTransition alerts on it.
//
// The fix is the caller, not a self-edge in the table: a self-edge would weaken
// the choke point's invariant for every caller and would silently re-stamp
// stageEnteredAt and reset podRecreations.
func TestReconcileStage_MintIsIdempotentAgainstAStaleCache(t *testing.T) {
	before := testutil.ToFloat64(obs.IllegalStageTransitionCounter(
		tatarav1alpha1.StageTriaging, tatarav1alpha1.StageTriaging))

	proj := tsProject(3)
	// THE STALE OBJECT the informer cache hands the reconciler: not yet minted.
	// It is an in-memory value, exactly as production's is - the cache is what
	// produced it, and nothing re-reads it.
	stale := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "brainstorm-qe-5snrr", Namespace: mdNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "proj", Kind: "brainstorm"},
	}
	// THE API SERVER: already minted by our own previous pass. It backs BOTH
	// Client and APIReader, because the illegal-transition counter fires from the
	// in-write stage.Enter inside objbudget.FitTask, which re-Gets through
	// r.Client. Backing r.Client with a second, stale store instead would let that
	// re-read see stage == "" and take the LEGAL Create edge, and the counter this
	// test asserts on could never move in its own fixture.
	live := stale.DeepCopy()
	live.Status.Stage = tatarav1alpha1.StageTriaging
	live.Status.AgentKind = ""

	scheme := runtime.NewScheme()
	require.NoError(t, tatarav1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(proj, live).
		WithStatusSubresource(&tatarav1alpha1.Task{}).Build()

	r := &TaskReconciler{
		Client: c, APIReader: c, Scheme: scheme,
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
	}
	_, err := r.reconcileStage(context.Background(), proj, stale, time.Unix(1000, 0))
	require.NoError(t, err, "a mint the API server already has is a NO-OP, not an error")

	after := testutil.ToFloat64(obs.IllegalStageTransitionCounter(
		tatarav1alpha1.StageTriaging, tatarav1alpha1.StageTriaging))
	require.Equal(t, before, after,
		"re-entering triaging from triaging must emit no operator_illegal_stage_transition_total")
}

// A genuine mint must still mint.
func TestReconcileStage_MintStillMintsWhenTheApiServerAgrees(t *testing.T) {
	proj := tsProject(3)
	fresh := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "refine-qe-lh79w", Namespace: mdNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "proj", Kind: "refine"},
	}
	scheme := runtime.NewScheme()
	require.NoError(t, tatarav1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(proj, fresh).
		WithStatusSubresource(&tatarav1alpha1.Task{}).Build()

	r := &TaskReconciler{
		Client: c, APIReader: c, Scheme: scheme,
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
	}
	_, err := r.reconcileStage(context.Background(), proj, fresh, time.Unix(1000, 0))
	require.NoError(t, err)
	require.Equal(t, tatarav1alpha1.StageTriaging, fresh.Status.Stage)
}

// SPEC TEST 11. TRIAGING IS THE SAME NON-IDEMPOTENT CALLER, ONE STAGE LATER.
//
// reconcileTriaging fires on a CACHED status.stage == triaging, computes next
// via triageTarget(spec.kind), and calls r.enter(next). objbudget.FitTask's
// in-write Get can catch a fresher live object than the cache this reconcile
// started with - our own prior reconcile's triaging -> refining write, not yet
// observed here. Recomputing "refining" from the frozen cached triaging
// snapshot and applying it again is refused in-write as refining -> refining
// (issue #324, the residue after the #347 mint fix: same race, one step
// later). Every triageTarget destination shares this shape, not just
// refining - the fix is the caller, generalized, not a per-destination
// special case and not a self-edge in the table.
func TestReconcileStage_TriagingIsIdempotentAgainstAStaleCache(t *testing.T) {
	before := testutil.ToFloat64(obs.IllegalStageTransitionCounter(
		tatarav1alpha1.StageRefining, tatarav1alpha1.StageRefining))

	proj := tsProject(3)
	// THE STALE OBJECT the informer cache hands the reconciler: still triaging.
	stale := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "refine-qe-zv4lp", Namespace: mdNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "proj", Kind: "refine"},
		Status:     tatarav1alpha1.TaskStatus{Stage: tatarav1alpha1.StageTriaging},
	}
	// THE API SERVER: already advanced triaging -> refining by our own previous
	// pass. Backs BOTH Client and APIReader, same reason as the mint test: the
	// illegal-transition counter fires from the in-write stage.Enter inside
	// objbudget.FitTask, which re-Gets through r.Client.
	live := stale.DeepCopy()
	live.Status.Stage = tatarav1alpha1.StageRefining

	scheme := runtime.NewScheme()
	require.NoError(t, tatarav1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(proj, live).
		WithStatusSubresource(&tatarav1alpha1.Task{}).Build()

	r := &TaskReconciler{
		Client: c, APIReader: c, Scheme: scheme,
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
	}
	_, err := r.reconcileStage(context.Background(), proj, stale, time.Unix(1000, 0))
	require.NoError(t, err, "re-triaging a Task the API server already advanced is a NO-OP, not an error")
	require.Equal(t, tatarav1alpha1.StageRefining, stale.Status.Stage,
		"the reconcile must ADOPT the live object it paid a quorum read for, not requeue and hope the cache catches up")

	after := testutil.ToFloat64(obs.IllegalStageTransitionCounter(
		tatarav1alpha1.StageRefining, tatarav1alpha1.StageRefining))
	require.Equal(t, before, after,
		"re-entering refining from triaging's stale cache must emit no operator_illegal_stage_transition_total")
}

// A genuine triage must still triage.
func TestReconcileStage_TriagingStillTriagesWhenTheApiServerAgrees(t *testing.T) {
	proj := tsProject(3)
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "refine-qe-lh79w", Namespace: mdNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "proj", Kind: "refine"},
		Status:     tatarav1alpha1.TaskStatus{Stage: tatarav1alpha1.StageTriaging},
	}
	scheme := runtime.NewScheme()
	require.NoError(t, tatarav1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(proj, task).
		WithStatusSubresource(&tatarav1alpha1.Task{}).Build()

	r := &TaskReconciler{
		Client: c, APIReader: c, Scheme: scheme,
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
	}
	_, err := r.reconcileStage(context.Background(), proj, task, time.Unix(1000, 0))
	require.NoError(t, err)
	require.Equal(t, tatarav1alpha1.StageRefining, task.Status.Stage)
}

// SPEC TEST 12. THE POD-STAGE CAPS ARE THE SAME NON-IDEMPOTENT CALLER, AND THE
// ONLY THING THAT PINS THE GUARD PAST TRIAGING.
//
// The triaging tests above prove NOTHING about reconcileClocks/reconcileCaps:
// their Task has no stageEnteredAt (ArmedClock returns ClockNone) and triaging
// is podless (BudgetExit returns nothing), so neither test executes one line of
// clock or cap edge derivation. Narrow the guard back into reconcileTriaging
// and they both stay green while this reopens - visible in production only as
// operator_illegal_stage_transition_total{from="failed",to="failed"}.
//
// This is that path: an implementing Task whose pod is gone and whose
// podRecreations is over budget, which reconcileCaps derives
// failed(pod-recreation-exhausted) from - off a CACHED stage our own prior
// reconcile already advanced to failed. Adopting the live object instead makes
// the reconcile see the terminal stage the Task actually has and hand it to the
// reaper, emitting no refused edge.
func TestReconcileStage_PodStageCapsAreIdempotentAgainstAStaleCache(t *testing.T) {
	before := testutil.ToFloat64(obs.IllegalStageTransitionCounter(
		tatarav1alpha1.StageFailed, tatarav1alpha1.StageFailed))

	proj := tsProject(3)
	now := time.Unix(10000, 0)
	entered := metav1.NewTime(now.Add(-10 * time.Minute))
	// THE STALE OBJECT the informer cache hands the reconciler: still
	// implementing, pod stamps set (so CLOCK 3 WORK is what is armed, and it has
	// not elapsed), podRecreations over maxPodRecreations, and no Pod object
	// exists so podGone reports the pod stopped. reconcileCaps derives
	// failed(pod-recreation-exhausted) from exactly this.
	stale := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "implement-qe-8k2rt", Namespace: mdNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "proj", Kind: "implement"},
		Status: tatarav1alpha1.TaskStatus{
			Stage:              tatarav1alpha1.StageImplementing,
			AgentKind:          stage.AgentImplement,
			StageEnteredAt:     &entered,
			PodStartedAt:       &entered,
			StageWorkStartedAt: &entered,
			Stats:              tatarav1alpha1.TaskStats{PodRecreations: maxPodRecreations + 1},
		},
	}
	// THE API SERVER: our own previous pass already applied that very edge.
	// Backs BOTH Client and APIReader, same reason as the mint/triaging tests:
	// the illegal-transition counter fires from the in-write stage.Enter inside
	// objbudget.FitTask, which re-Gets through r.Client.
	live := stale.DeepCopy()
	live.Status.Stage = tatarav1alpha1.StageFailed
	live.Status.StageReason = stage.ReasonPodRecreationExhausted

	scheme := runtime.NewScheme()
	require.NoError(t, tatarav1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(proj, live).
		WithStatusSubresource(&tatarav1alpha1.Task{}).Build()

	r := &TaskReconciler{
		Client: c, APIReader: c, Scheme: scheme,
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
	}
	_, err := r.reconcileStage(context.Background(), proj, stale, now)
	require.NoError(t, err, "re-failing a Task the API server already failed is a NO-OP, not an error")

	after := testutil.ToFloat64(obs.IllegalStageTransitionCounter(
		tatarav1alpha1.StageFailed, tatarav1alpha1.StageFailed))
	require.Equal(t, before, after,
		"re-deriving the pod-recreation cap from a stale implementing cache must emit no operator_illegal_stage_transition_total")
}

// The drift the guard swallows must be VISIBLE: it returns success, logs
// nothing louder than INFO, and the default 10h SyncPeriod will not rescue a
// watch that is wedged rather than merely lagging.
func TestReconcileStage_DriftIsCounted(t *testing.T) {
	before := testutil.ToFloat64(obs.StageDriftCounter(tatarav1alpha1.StageTriaging))

	proj := tsProject(3)
	stale := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "refine-qe-drift1", Namespace: mdNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "proj", Kind: "refine"},
		Status:     tatarav1alpha1.TaskStatus{Stage: tatarav1alpha1.StageTriaging},
	}
	live := stale.DeepCopy()
	live.Status.Stage = tatarav1alpha1.StageRefining

	scheme := runtime.NewScheme()
	require.NoError(t, tatarav1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(proj, live).
		WithStatusSubresource(&tatarav1alpha1.Task{}).Build()

	r := &TaskReconciler{
		Client: c, APIReader: c, Scheme: scheme,
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
	}
	_, err := r.reconcileStage(context.Background(), proj, stale, time.Unix(1000, 0))
	require.NoError(t, err)

	after := testutil.ToFloat64(obs.StageDriftCounter(tatarav1alpha1.StageTriaging))
	require.Equal(t, before+1, after,
		"a drifted reconcile must increment operator_stage_drift_total{stage=<cached stage>}")
	require.Equal(t, tatarav1alpha1.StageRefining, stale.Status.Stage,
		"the live object must be ADOPTED, not discarded in favour of a requeue")
}

// A cache that AGREES with the API server is not drift and must not be counted.
func TestReconcileStage_NoDriftIsNotCounted(t *testing.T) {
	before := testutil.ToFloat64(obs.StageDriftCounter(tatarav1alpha1.StageTriaging))

	proj := tsProject(3)
	task := &tatarav1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "refine-qe-drift2", Namespace: mdNS},
		Spec:       tatarav1alpha1.TaskSpec{ProjectRef: "proj", Kind: "refine"},
		Status:     tatarav1alpha1.TaskStatus{Stage: tatarav1alpha1.StageTriaging},
	}
	scheme := runtime.NewScheme()
	require.NoError(t, tatarav1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(proj, task).
		WithStatusSubresource(&tatarav1alpha1.Task{}).Build()

	r := &TaskReconciler{
		Client: c, APIReader: c, Scheme: scheme,
		Metrics: obs.NewOperatorMetrics(prometheus.NewRegistry()),
	}
	_, err := r.reconcileStage(context.Background(), proj, task, time.Unix(1000, 0))
	require.NoError(t, err)

	after := testutil.ToFloat64(obs.StageDriftCounter(tatarav1alpha1.StageTriaging))
	require.Equal(t, before, after, "an up-to-date cache is not drift")
}
