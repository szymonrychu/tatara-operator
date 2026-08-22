package controller

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/accountusage"
	"github.com/szymonrychu/tatara-operator/internal/budget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// mkBudgetPools creates a project (capacity 5/5 so capacity never gates) with the
// given optional token-budget spec, plus one Queued normal-class event and one
// Queued alert-class event, returning all three.
func mkBudgetPools(t *testing.T, ctx context.Context, name string, tb *tatarav1alpha1.TokenBudgetSpec) (*tatarav1alpha1.Project, *tatarav1alpha1.QueuedEvent, *tatarav1alpha1.QueuedEvent) {
	t.Helper()
	proj := &tatarav1alpha1.Project{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Spec: tatarav1alpha1.ProjectSpec{
			ScmSecretRef:        name + "-scm",
			MaxConcurrentAgents: 5,
			Agent: tatarav1alpha1.AgentSpec{
				Model: "claude-x", Image: "wrapper:1", PermissionMode: "bypassPermissions",
				MaxTurnsPerTask: 50, TurnTimeoutSeconds: 1800,
			},
			Queue:       &tatarav1alpha1.QueueSpec{Capacity: 5, AlertCapacity: 5},
			TokenBudget: tb,
		},
	}
	mustCreate(t, ctx, proj)
	return proj,
		mkQueued(t, ctx, name, 1, tatarav1alpha1.QueueClassNormal, "review"),
		mkQueued(t, ctx, name, 2, tatarav1alpha1.QueueClassAlert, "incident")
}

func mkQueued(t *testing.T, ctx context.Context, projRef string, seq int64, class, kind string) *tatarav1alpha1.QueuedEvent {
	t.Helper()
	q := &tatarav1alpha1.QueuedEvent{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "qe-", Namespace: testNS},
		Spec: tatarav1alpha1.QueuedEventSpec{
			Seq: seq, Class: class, Kind: kind, ProjectRef: projRef,
			Payload: tatarav1alpha1.QueuedEventPayload{Kind: kind, GenerateName: kind + "-"},
		},
	}
	mustCreate(t, ctx, q)
	q.Status.State = tatarav1alpha1.QueueStateQueued
	mustStatusUpdate(t, ctx, q)
	return q
}

func assertQEAdmitted(t *testing.T, ctx context.Context, q *tatarav1alpha1.QueuedEvent, want bool) {
	t.Helper()
	got := refreshQE(t, ctx, q)
	admitted := got.Status.State == tatarav1alpha1.QueueStateAdmitted
	if admitted != want {
		t.Fatalf("%s admitted=%v want %v (state=%q)", q.Name, admitted, want, got.Status.State)
	}
}

// TestAdmit_BudgetGate_DirectDecisions verifies the per-pool gate: a disabled
// (zero) decision admits both pools; a proactive-blocked decision holds the
// normal pool while incidents still admit; an emergency-blocked decision holds
// both.
func TestAdmit_BudgetGate_DirectDecisions(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name                 string
		d                    budget.Decision
		wantNormal, wantAlrt bool
	}{
		{"disabled", budget.Decision{}, true, true},
		{"proactive blocked", budget.Decision{ProactiveBlocked: true, UsedPercent: 60}, false, true},
		{"emergency blocked", budget.Decision{ProactiveBlocked: true, EmergencyBlocked: true, UsedPercent: 90}, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			name := "p-gate-" + strings.ReplaceAll(tc.name, " ", "-")
			proj, nQE, aQE := mkBudgetPools(t, ctx, name, nil)
			r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			qes, tasks := listQEsTasks(t, ctx, proj.Name)
			if _, _, _, err := r.admit(ctx, proj, qes, tasks, tc.d, budget.Config{}, budget.Subscription{}, time.Now()); err != nil {
				t.Fatal(err)
			}
			assertQEAdmitted(t, ctx, nQE, tc.wantNormal)
			assertQEAdmitted(t, ctx, aQE, tc.wantAlrt)
		})
	}
}

// TestAdmit_BudgetBlocked_EmitsMetric verifies the held pool increments
// operator_admission_blocked_total once for its class.
func TestAdmit_BudgetBlocked_EmitsMetric(t *testing.T) {
	ctx := context.Background()
	metrics := obs.NewOperatorMetrics(prometheus.NewRegistry())
	proj, nQE, aQE := mkBudgetPools(t, ctx, "p-gate-metric", nil)
	r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Metrics: metrics}
	qes, tasks := listQEsTasks(t, ctx, proj.Name)
	if _, _, _, err := r.admit(ctx, proj, qes, tasks, budget.Decision{ProactiveBlocked: true, UsedPercent: 60}, budget.Config{}, budget.Subscription{}, time.Now()); err != nil {
		t.Fatal(err)
	}
	assertQEAdmitted(t, ctx, nQE, false)
	assertQEAdmitted(t, ctx, aQE, true)
	if got := testutil.ToFloat64(metrics.AdmissionBlockedCounter(proj.Name, tatarav1alpha1.QueueClassNormal, "", "token_budget")); got != 1 {
		t.Fatalf("admission_blocked{normal} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.AdmissionBlockedCounter(proj.Name, tatarav1alpha1.QueueClassAlert, "", "token_budget")); got != 0 {
		t.Fatalf("admission_blocked{alert} = %v, want 0", got)
	}
}

// TestAdmit_BudgetWindowEvaluation drives the gate through the real config +
// budget.Evaluate path (the same computation Reconcile does), seeding the
// project's persisted custom-window accumulator: under both thresholds admits
// both; over proactive% holds the normal pool while the incident admits; over
// emergency% holds both; a stale (past-window) accumulator rolls to 0 and
// re-admits.
func TestAdmit_BudgetWindowEvaluation(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	curHour := now.Truncate(time.Hour)
	pastHour := now.Add(-2 * time.Hour).Truncate(time.Hour)
	spec := func() *tatarav1alpha1.TokenBudgetSpec {
		return &tatarav1alpha1.TokenBudgetSpec{
			Enabled: true, Mode: "customWindow", ProactivePercent: 50, EmergencyPercent: 80,
			ResetSchedule: "0 * * * *", WindowDuration: "1h", TokenLimit: 1000,
		}
	}
	cases := []struct {
		name                 string
		windowStart          time.Time
		tokens               int64
		wantNormal, wantAlrt bool
	}{
		{"under-all", curHour, 400, true, true},
		{"over-proactive", curHour, 600, false, true},
		{"over-emergency", curHour, 850, false, false},
		{"rolled-window", pastHour, 900, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			proj, nQE, aQE := mkBudgetPools(t, ctx, "p-bw-"+tc.name, spec())
			proj.SetBudgetWindowState(budget.WindowState{WindowStart: tc.windowStart, WindowTokens: tc.tokens})
			mustStatusUpdate(t, ctx, proj)

			r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			cfg := proj.BudgetConfig(r.BudgetDefaults)
			d := budget.Evaluate(cfg, proj.BudgetWindowState(), proj.BudgetSubscription(), time.Now())
			qes, tasks := listQEsTasks(t, ctx, proj.Name)
			if _, _, _, err := r.admit(ctx, proj, qes, tasks, d, cfg, proj.BudgetSubscription(), time.Now()); err != nil {
				t.Fatal(err)
			}
			assertQEAdmitted(t, ctx, nQE, tc.wantNormal)
			assertQEAdmitted(t, ctx, aQE, tc.wantAlrt)
		})
	}
}

func TestBudgetRequeueAfter(t *testing.T) {
	now := time.Date(2026, 6, 27, 10, 20, 30, 0, time.UTC)
	// Hourly cron: next boundary ~39m away, capped at 5m.
	if got := budgetRequeueAfter(budget.Config{ResetSchedule: "0 * * * *"}, now); got != 5*time.Minute {
		t.Fatalf("hourly wait = %v, want 5m (capped)", got)
	}
	// No schedule (e.g. claudeSubscription mode): 60s fallback.
	if got := budgetRequeueAfter(budget.Config{}, now); got != 60*time.Second {
		t.Fatalf("no-schedule wait = %v, want 60s", got)
	}
	// Bad cron: 60s fallback.
	if got := budgetRequeueAfter(budget.Config{ResetSchedule: "not a cron"}, now); got != 60*time.Second {
		t.Fatalf("bad-cron wait = %v, want 60s", got)
	}
	// Soon boundary (per-minute): ~31s, uncapped and positive.
	if got := budgetRequeueAfter(budget.Config{ResetSchedule: "* * * * *"}, now); got <= 0 || got > time.Minute {
		t.Fatalf("per-minute wait = %v, want (0, 1m]", got)
	}
}

// budgetRatio reads operator_token_budget_used_ratio{project,scope} out of reg.
func budgetRatio(t *testing.T, reg *prometheus.Registry, project, scope string) (float64, bool) {
	t.Helper()
	fams, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range fams {
		if f.GetName() != "operator_token_budget_used_ratio" {
			continue
		}
		for _, m := range f.GetMetric() {
			lbl := map[string]string{}
			for _, l := range m.GetLabel() {
				lbl[l.GetName()] = l.GetValue()
			}
			if lbl["project"] == project && lbl["scope"] == scope {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

// TestDoReconcile_BudgetGaugesUseGoverningWindowThresholds locks that the
// used/proactive/emergency triple always describes ONE window. With per-window
// thresholds there is no single mode-wide pair to plot against, so plotting
// ResolvePercents beside a per-window used% would draw a threshold line the
// decision never used. The rollout check reads exactly this: proactive=0.8 while
// the 5h window governs, 0.75 while the weekly window does.
func TestDoReconcile_BudgetGaugesUseGoverningWindowThresholds(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	spec := func() *tatarav1alpha1.TokenBudgetSpec {
		return &tatarav1alpha1.TokenBudgetSpec{
			Enabled: true, Mode: "claudeSubscription",
			ProactivePercent: 50, EmergencyPercent: 80,
			FiveHourProactivePercent: 80, FiveHourEmergencyPercent: 92,
			WeeklyProactivePercent: 75, WeeklyEmergencyPercent: 88,
		}
	}
	cases := []struct {
		name                           string
		empty                          bool
		fiveHourPct, weeklyPct         float64
		wantUsed, wantProac, wantEmerg float64
	}{
		// 60/80 = 0.75 relative pressure beats 20/75 = 0.27, so the 5h window
		// governs and its own 80/92 pair is what gets plotted.
		{"five hour governs", false, 60, 20, 0.60, 0.80, 0.92},
		// 70/75 = 0.93 beats 10/80 = 0.125: the weekly window governs even
		// though its raw percent is checked against a LOWER threshold.
		{"weekly governs", false, 10, 70, 0.70, 0.75, 0.88},
		// No snapshot at all: no window governs, so the whole triple is 0 and
		// stays internally consistent rather than plotting a threshold line for
		// a window the decision never looked at. tatara_account_usage_gate_ready
		// is the signal that this state is the outage, not an idle account.
		{"nothing governs without a snapshot", true, 0, 0, 0, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			metrics := obs.NewOperatorMetrics(reg)
			name := "p-gw-" + strings.ReplaceAll(tc.name, " ", "-")
			proj, nQE, _ := mkBudgetPools(t, ctx, name, spec())
			store := &accountusage.Store{}
			if !tc.empty {
				store.Set(accountusage.Snapshot{
					FiveHour:  accountusage.Window{Percent: tc.fiveHourPct, Reset: now.Add(2 * time.Hour)},
					Weekly:    accountusage.Window{Percent: tc.weeklyPct, Reset: now.Add(48 * time.Hour)},
					UpdatedAt: now,
					Source:    accountusage.SourceWrapper,
				})
			}
			r := &DispatcherReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Metrics: metrics, Usage: store}
			if _, err := r.doReconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(nQE)}); err != nil {
				t.Fatal(err)
			}
			for _, want := range []struct {
				scope string
				value float64
			}{
				{"used", tc.wantUsed},
				{"proactive", tc.wantProac},
				{"emergency", tc.wantEmerg},
			} {
				got, ok := budgetRatio(t, reg, proj.Name, want.scope)
				if !ok {
					t.Fatalf("no operator_token_budget_used_ratio{project=%q,scope=%q}", proj.Name, want.scope)
				}
				if math.Abs(got-want.value) > 1e-9 {
					t.Fatalf("scope=%s ratio = %v, want %v", want.scope, got, want.value)
				}
			}
		})
	}
}

// TestDoReconcile_StaleSnapshotFailsOpen locks the second staleness gate end to
// end: a snapshot at 99% of the 5h window that stopped being refreshed longer
// ago than MaxSnapshotAge stops governing, so the held pool re-opens. FAILING
// OPEN is deliberate (a dead feed must not wedge the platform) and is only
// defensible because tatara_account_usage_gate_ready alerts on it. Without the
// UpdatedAt -> ObservedAt projection this test cannot fail, because
// budget.SnapshotFresh would see a zero ObservedAt and call it fresh forever.
func TestDoReconcile_StaleSnapshotFailsOpen(t *testing.T) {
	ctx := context.Background()
	spec := &tatarav1alpha1.TokenBudgetSpec{
		Enabled: true, Mode: "claudeSubscription", ProactivePercent: 50, EmergencyPercent: 80,
	}
	_, nQE, _ := mkBudgetPools(t, ctx, "p-stale-open", spec)
	store := &accountusage.Store{}
	store.Set(accountusage.Snapshot{
		FiveHour:  accountusage.Window{Percent: 99, Reset: time.Now().Add(2 * time.Hour)},
		UpdatedAt: time.Now().Add(-2 * budget.DefaultMaxSnapshotAge),
		Source:    accountusage.SourceWrapper,
	})
	r := &DispatcherReconciler{
		Client: k8sClient, Scheme: k8sClient.Scheme(), Usage: store,
		BudgetDefaults: budget.Config{MaxSnapshotAge: budget.DefaultMaxSnapshotAge},
	}
	if _, err := r.doReconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(nQE)}); err != nil {
		t.Fatal(err)
	}
	assertQEAdmitted(t, ctx, nQE, true)
}
