package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
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
			ScmSecretRef:       name + "-scm",
			MaxConcurrentTasks: 5,
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
			if _, err := r.admit(ctx, proj, qes, tasks, tc.d); err != nil {
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
	if _, err := r.admit(ctx, proj, qes, tasks, budget.Decision{ProactiveBlocked: true, UsedPercent: 60}); err != nil {
		t.Fatal(err)
	}
	assertQEAdmitted(t, ctx, nQE, false)
	assertQEAdmitted(t, ctx, aQE, true)
	if got := testutil.ToFloat64(metrics.AdmissionBlockedCounter(proj.Name, tatarav1alpha1.QueueClassNormal, "token_budget")); got != 1 {
		t.Fatalf("admission_blocked{normal} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.AdmissionBlockedCounter(proj.Name, tatarav1alpha1.QueueClassAlert, "token_budget")); got != 0 {
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
			if _, err := r.admit(ctx, proj, qes, tasks, d); err != nil {
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
