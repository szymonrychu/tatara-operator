package v1alpha1

import (
	"reflect"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/szymonrychu/tatara-operator/internal/budget"
)

func TestBudgetConfigNilSpecInheritsDefaults(t *testing.T) {
	defaults := budget.Config{
		Enabled:          true,
		Mode:             budget.ModeCustomWindow,
		ProactivePercent: 50,
		EmergencyPercent: 80,
		ResetSchedule:    "0 * * * *",
		WindowDuration:   time.Hour,
		TokenLimit:       1000,
	}
	p := &Project{}
	got := p.BudgetConfig(defaults)
	if !reflect.DeepEqual(got, defaults) {
		t.Fatalf("nil spec must inherit defaults verbatim:\n got  %+v\n want %+v", got, defaults)
	}
}

func TestBudgetConfigSpecOverridesAndDurationParse(t *testing.T) {
	defaults := budget.Config{
		Enabled:          false,
		Mode:             budget.ModeCustomWindow,
		ProactivePercent: 50,
		EmergencyPercent: 80,
		ResetSchedule:    "0 * * * *",
		WindowDuration:   time.Hour,
		TokenLimit:       1000,
	}
	p := &Project{Spec: ProjectSpec{TokenBudget: &TokenBudgetSpec{
		Enabled:          true,
		Mode:             "claudeSubscription",
		ProactivePercent: 40,
		EmergencyPercent: 90,
		ResetSchedule:    "0 0 * * *",
		WindowDuration:   "168h",
		TokenLimit:       5000,
	}}}
	got := p.BudgetConfig(defaults)
	want := budget.Config{
		Enabled:          true,
		Mode:             budget.ModeClaudeSubscription,
		ProactivePercent: 40,
		EmergencyPercent: 90,
		ResetSchedule:    "0 0 * * *",
		WindowDuration:   168 * time.Hour,
		TokenLimit:       5000,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("override mismatch:\n got  %+v\n want %+v", got, want)
	}
}

func TestBudgetConfigZeroFieldsFallBackToDefaults(t *testing.T) {
	defaults := budget.Config{
		Mode:             budget.ModeCustomWindow,
		ProactivePercent: 55,
		EmergencyPercent: 85,
		ResetSchedule:    "0 * * * *",
		WindowDuration:   2 * time.Hour,
		TokenLimit:       2000,
	}
	// A present block that only flips Enabled; all other (zero) fields inherit
	// the defaults. An unparseable duration also falls back.
	p := &Project{Spec: ProjectSpec{TokenBudget: &TokenBudgetSpec{
		Enabled:        true,
		WindowDuration: "not-a-duration",
	}}}
	got := p.BudgetConfig(defaults)
	want := defaults
	want.Enabled = true
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("fallback mismatch:\n got  %+v\n want %+v", got, want)
	}
}

func TestBudgetWindowStateRoundTrip(t *testing.T) {
	p := &Project{}
	if ws := p.BudgetWindowState(); ws != (budget.WindowState{}) {
		t.Fatalf("unset status must be zero WindowState, got %+v", ws)
	}
	start := time.Date(2026, 6, 27, 10, 0, 0, 0, time.UTC)
	p.SetBudgetWindowState(budget.WindowState{WindowStart: start, WindowTokens: 1234})
	got := p.BudgetWindowState()
	if !got.WindowStart.Equal(start) || got.WindowTokens != 1234 {
		t.Fatalf("round-trip mismatch: got %+v", got)
	}
}

func TestBudgetSubscriptionMapping(t *testing.T) {
	p := &Project{}
	if sub := p.BudgetSubscription(); sub != (budget.Subscription{}) {
		t.Fatalf("unset status must be zero Subscription, got %+v", sub)
	}
	fiveReset := time.Date(2026, 6, 27, 15, 0, 0, 0, time.UTC)
	weekReset := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	ft := metav1.NewTime(fiveReset)
	wt := metav1.NewTime(weekReset)
	p.Status.TokenBudget = &TokenBudgetStatus{
		FiveHourPercent: 30, FiveHourReset: &ft,
		WeeklyPercent: 70, WeeklyReset: &wt,
	}
	sub := p.BudgetSubscription()
	if sub.FiveHourPercent != 30 || !sub.FiveHourReset.Equal(fiveReset) {
		t.Fatalf("5h mapping wrong: %+v", sub)
	}
	if sub.WeeklyPercent != 70 || !sub.WeeklyReset.Equal(weekReset) {
		t.Fatalf("weekly mapping wrong: %+v", sub)
	}
}
