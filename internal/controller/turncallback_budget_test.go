package controller

import (
	"context"
	"testing"
	"time"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/budget"
)

// mkBudgetProject creates a Project carrying the given token-budget spec plus a
// matching repo + task, returning the task. Mirrors mkTaskProject's required
// Agent fields.
func mkBudgetProject(t *testing.T, name string, spec tatarav1alpha1.TokenBudgetSpec) *tatarav1alpha1.Task {
	t.Helper()
	p := &tatarav1alpha1.Project{}
	p.Name = name
	p.Namespace = testNS
	p.Spec.ScmSecretRef = name + "-scm"
	p.Spec.MaxConcurrentTasks = 3
	p.Spec.Agent = tatarav1alpha1.AgentSpec{
		Model: "claude-x", Image: "wrapper:1", PermissionMode: "bypassPermissions",
		MaxTurnsPerTask: 50, TurnTimeoutSeconds: 1800,
	}
	p.Spec.TokenBudget = &spec
	if err := k8sClient.Create(context.Background(), p); err != nil {
		t.Fatalf("create budget project: %v", err)
	}
	mkTaskRepository(t, name+"-repo", name)
	mkTask(t, name+"-task", name, name+"-repo")
	return getTask(t, name+"-task")
}

func TestUpdateProjectBudget_WindowAccumulatesAndRolls(t *testing.T) {
	task := mkBudgetProject(t, "p-tb-acc", tatarav1alpha1.TokenBudgetSpec{
		Enabled:        true,
		Mode:           "customWindow",
		ResetSchedule:  "0 * * * *",
		WindowDuration: "1h",
		TokenLimit:     1_000_000,
	})
	cb := newCallbackServer()
	ctx := context.Background()

	// Two recorded turns in the same window accumulate.
	if err := cb.updateProjectBudget(ctx, task, 100, true, nil); err != nil {
		t.Fatalf("updateProjectBudget 1: %v", err)
	}
	if err := cb.updateProjectBudget(ctx, task, 250, true, nil); err != nil {
		t.Fatalf("updateProjectBudget 2: %v", err)
	}
	if p := getProject(t, "p-tb-acc"); p.Status.TokenBudget == nil || p.Status.TokenBudget.WindowTokens != 350 {
		t.Fatalf("WindowTokens = %+v, want 350", p.Status.TokenBudget)
	}

	// A non-recorded (stale/duplicate) callback must NOT accumulate.
	if err := cb.updateProjectBudget(ctx, task, 999, false, nil); err != nil {
		t.Fatalf("updateProjectBudget stale: %v", err)
	}
	if p := getProject(t, "p-tb-acc"); p.Status.TokenBudget.WindowTokens != 350 {
		t.Fatalf("stale callback changed WindowTokens to %d, want 350", p.Status.TokenBudget.WindowTokens)
	}

	// Force the accumulator into a past window; a new turn must roll it to 0
	// before adding (not stack onto the stale 900).
	p := getProject(t, "p-tb-acc")
	pastBoundary := time.Now().Add(-90 * time.Minute).Truncate(time.Hour)
	p.SetBudgetWindowState(budget.WindowState{WindowStart: pastBoundary, WindowTokens: 900})
	if err := k8sClient.Status().Update(ctx, p); err != nil {
		t.Fatalf("seed past window: %v", err)
	}
	if err := cb.updateProjectBudget(ctx, task, 100, true, nil); err != nil {
		t.Fatalf("updateProjectBudget roll: %v", err)
	}
	if p := getProject(t, "p-tb-acc"); p.Status.TokenBudget.WindowTokens != 100 {
		t.Fatalf("after roll WindowTokens = %d, want 100", p.Status.TokenBudget.WindowTokens)
	}
}

func TestUpdateProjectBudget_PersistsRateLimitSnapshot(t *testing.T) {
	task := mkBudgetProject(t, "p-tb-sub", tatarav1alpha1.TokenBudgetSpec{
		Enabled: true,
		Mode:    "claudeSubscription",
	})
	cb := newCallbackServer()
	ctx := context.Background()

	fiveReset := time.Now().Add(2 * time.Hour).Unix()
	weekReset := time.Now().Add(72 * time.Hour).Unix()
	rl := &turnRateLimit{
		FiveHourPercent: 30, FiveHourResetUnix: fiveReset,
		WeeklyPercent: 70, WeeklyResetUnix: weekReset,
	}
	// No usage this turn (recorded=false), but the snapshot still persists.
	if err := cb.updateProjectBudget(ctx, task, 0, false, rl); err != nil {
		t.Fatalf("updateProjectBudget rl: %v", err)
	}
	p := getProject(t, "p-tb-sub")
	st := p.Status.TokenBudget
	if st == nil || st.FiveHourPercent != 30 || st.WeeklyPercent != 70 {
		t.Fatalf("snapshot not persisted: %+v", st)
	}
	if st.FiveHourReset == nil || st.FiveHourReset.Unix() != fiveReset {
		t.Fatalf("FiveHourReset = %v, want unix %d", st.FiveHourReset, fiveReset)
	}
	if st.WeeklyReset == nil || st.WeeklyReset.Unix() != weekReset {
		t.Fatalf("WeeklyReset = %v, want unix %d", st.WeeklyReset, weekReset)
	}
}

func TestUpdateProjectBudget_DisabledIsNoop(t *testing.T) {
	task := mkBudgetProject(t, "p-tb-off", tatarav1alpha1.TokenBudgetSpec{
		Enabled:        false,
		Mode:           "customWindow",
		ResetSchedule:  "0 * * * *",
		WindowDuration: "1h",
		TokenLimit:     1000,
	})
	cb := newCallbackServer()
	if err := cb.updateProjectBudget(context.Background(), task, 500, true, nil); err != nil {
		t.Fatalf("updateProjectBudget disabled: %v", err)
	}
	if p := getProject(t, "p-tb-off"); p.Status.TokenBudget != nil {
		t.Fatalf("disabled budget must not write status, got %+v", p.Status.TokenBudget)
	}
}
