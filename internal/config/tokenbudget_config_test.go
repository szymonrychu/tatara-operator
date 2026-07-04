package config_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/szymonrychu/tatara-operator/internal/budget"
	"github.com/szymonrychu/tatara-operator/internal/config"
)

// setRequired sets the env vars Load() requires so a test can exercise the
// optional token-budget knobs without tripping the required-field checks.
func setRequired(t *testing.T) {
	t.Helper()
	t.Setenv("OIDC_ISSUER", "https://kc/realms/tatara")
	t.Setenv("OIDC_AUDIENCE", "tatara-operator")
	t.Setenv("OPERATOR_OIDC_SECRET_NAME", "tatara-operator")
}

func TestBudgetDefaultsOff(t *testing.T) {
	setRequired(t)
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := cfg.BudgetDefaults()
	want := budget.Config{
		Enabled:          false,
		Mode:             budget.ModeCustomWindow,
		ProactivePercent: budget.DefaultProactivePercent,
		EmergencyPercent: budget.DefaultEmergencyPercent,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("defaults:\n got  %+v\n want %+v", got, want)
	}
}

func TestBudgetDefaultsFromEnv(t *testing.T) {
	setRequired(t)
	t.Setenv("TOKEN_BUDGET_ENABLED", "true")
	t.Setenv("TOKEN_BUDGET_MODE", "claudeSubscription")
	t.Setenv("TOKEN_BUDGET_PROACTIVE_PERCENT", "40")
	t.Setenv("TOKEN_BUDGET_EMERGENCY_PERCENT", "90")
	t.Setenv("TOKEN_BUDGET_RESET_SCHEDULE", "0 * * * *")
	t.Setenv("TOKEN_BUDGET_WINDOW", "5h")
	t.Setenv("TOKEN_BUDGET_TOKEN_LIMIT", "1000000")
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := cfg.BudgetDefaults()
	want := budget.Config{
		Enabled:          true,
		Mode:             budget.ModeClaudeSubscription,
		ProactivePercent: 40,
		EmergencyPercent: 90,
		ResetSchedule:    "0 * * * *",
		WindowDuration:   5 * time.Hour,
		TokenLimit:       1000000,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("from env:\n got  %+v\n want %+v", got, want)
	}
}

func TestBudgetEnabledFailsFastOnBadDefault(t *testing.T) {
	// Enabled customWindow with no schedule/limit must fail Load (Validate), so a
	// misconfigured fleet default surfaces at startup instead of silently
	// disabling the gate.
	setRequired(t)
	t.Setenv("TOKEN_BUDGET_ENABLED", "true")
	t.Setenv("TOKEN_BUDGET_MODE", "customWindow")
	if _, err := config.Load(); err == nil {
		t.Fatal("expected Load to fail for enabled customWindow with no schedule/limit")
	}
}

func TestBudgetEnabledCustomWindowValidLoads(t *testing.T) {
	setRequired(t)
	t.Setenv("TOKEN_BUDGET_ENABLED", "true")
	t.Setenv("TOKEN_BUDGET_RESET_SCHEDULE", "0 * * * *")
	t.Setenv("TOKEN_BUDGET_WINDOW", "1h")
	t.Setenv("TOKEN_BUDGET_TOKEN_LIMIT", "500000")
	if _, err := config.Load(); err != nil {
		t.Fatalf("valid enabled customWindow must load: %v", err)
	}
}

func TestBudgetInvalidIntFailsLoad(t *testing.T) {
	setRequired(t)
	t.Setenv("TOKEN_BUDGET_PROACTIVE_PERCENT", "abc")
	if _, err := config.Load(); err == nil {
		t.Fatal("expected Load to fail on non-integer percent")
	}
}
