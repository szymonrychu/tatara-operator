package budget

import (
	"testing"
	"time"
)

// hourly is a cron that fires at the top of every hour; a 1h window pairs with
// it so the reset-search bound stays tight.
const hourly = "0 * * * *"

func customCfg(limit int64, pro, emg int) Config {
	return Config{
		Enabled:          true,
		Mode:             ModeCustomWindow,
		ProactivePercent: pro,
		EmergencyPercent: emg,
		ResetSchedule:    hourly,
		WindowDuration:   time.Hour,
		TokenLimit:       limit,
	}
}

func TestEvaluateDisabledNeverBlocks(t *testing.T) {
	cfg := customCfg(1000, 50, 80)
	cfg.Enabled = false
	now := time.Date(2026, 6, 27, 10, 30, 0, 0, time.UTC)
	st := WindowState{WindowStart: time.Date(2026, 6, 27, 10, 0, 0, 0, time.UTC), WindowTokens: 100000}
	d := Evaluate(cfg, st, Subscription{}, now)
	if d.ProactiveBlocked || d.EmergencyBlocked {
		t.Fatalf("disabled config must never block, got %+v", d)
	}
}

func TestEvaluateCustomWindowThresholds(t *testing.T) {
	cfg := customCfg(1000, 50, 80)
	winStart := time.Date(2026, 6, 27, 10, 0, 0, 0, time.UTC)
	now := winStart.Add(30 * time.Minute)

	cases := []struct {
		name          string
		tokens        int64
		wantProactive bool
		wantEmergency bool
		wantUsed      float64
	}{
		{"below proactive", 490, false, false, 49},
		{"at proactive", 500, true, false, 50},
		{"between", 700, true, false, 70},
		{"at emergency", 800, true, true, 80},
		{"over limit", 1200, true, true, 120},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := WindowState{WindowStart: winStart, WindowTokens: tc.tokens}
			d := Evaluate(cfg, st, Subscription{}, now)
			if d.ProactiveBlocked != tc.wantProactive || d.EmergencyBlocked != tc.wantEmergency {
				t.Fatalf("blocks: got proactive=%v emergency=%v want %v/%v",
					d.ProactiveBlocked, d.EmergencyBlocked, tc.wantProactive, tc.wantEmergency)
			}
			if d.UsedPercent != tc.wantUsed {
				t.Fatalf("used: got %v want %v", d.UsedPercent, tc.wantUsed)
			}
		})
	}
}

func TestEvaluateRollsStaleWindowToZero(t *testing.T) {
	cfg := customCfg(1000, 50, 80)
	// State recorded in a window that closed two hours ago; the gate must roll it
	// to the current window and read 0 usage rather than the stale 900 tokens.
	now := time.Date(2026, 6, 27, 12, 5, 0, 0, time.UTC)
	st := WindowState{WindowStart: time.Date(2026, 6, 27, 10, 0, 0, 0, time.UTC), WindowTokens: 900}
	d := Evaluate(cfg, st, Subscription{}, now)
	if d.ProactiveBlocked || d.EmergencyBlocked {
		t.Fatalf("stale window must roll to 0 and not block, got %+v", d)
	}
	if d.UsedPercent != 0 {
		t.Fatalf("used: got %v want 0 after roll", d.UsedPercent)
	}
}

func TestRollResetsAndAccumulates(t *testing.T) {
	cfg := customCfg(1000, 50, 80)
	// First turn in a fresh (uninitialised) accumulator.
	t1 := time.Date(2026, 6, 27, 10, 10, 0, 0, time.UTC)
	st := Roll(cfg, WindowState{}, t1, 100)
	wantStart := time.Date(2026, 6, 27, 10, 0, 0, 0, time.UTC)
	if !st.WindowStart.Equal(wantStart) {
		t.Fatalf("window start: got %v want %v", st.WindowStart, wantStart)
	}
	if st.WindowTokens != 100 {
		t.Fatalf("tokens: got %d want 100", st.WindowTokens)
	}
	// Second turn in the same window accumulates.
	t2 := time.Date(2026, 6, 27, 10, 40, 0, 0, time.UTC)
	st = Roll(cfg, st, t2, 250)
	if st.WindowTokens != 350 {
		t.Fatalf("tokens: got %d want 350", st.WindowTokens)
	}
	// Third turn after the next boundary resets, then adds.
	t3 := time.Date(2026, 6, 27, 11, 5, 0, 0, time.UTC)
	st = Roll(cfg, st, t3, 70)
	if !st.WindowStart.Equal(time.Date(2026, 6, 27, 11, 0, 0, 0, time.UTC)) {
		t.Fatalf("window start did not advance: %v", st.WindowStart)
	}
	if st.WindowTokens != 70 {
		t.Fatalf("tokens after roll: got %d want 70", st.WindowTokens)
	}
}

func TestCurrentWindowStart(t *testing.T) {
	sched, err := ParseSchedule(hourly)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 6, 27, 14, 23, 0, 0, time.UTC)
	start, ok := CurrentWindowStart(sched, now, time.Hour)
	if !ok {
		t.Fatal("expected a window start")
	}
	if !start.Equal(time.Date(2026, 6, 27, 14, 0, 0, 0, time.UTC)) {
		t.Fatalf("got %v want 14:00", start)
	}
}

func TestCustomWindowZeroLimitInert(t *testing.T) {
	cfg := customCfg(0, 50, 80)
	now := time.Date(2026, 6, 27, 10, 30, 0, 0, time.UTC)
	st := WindowState{WindowStart: time.Date(2026, 6, 27, 10, 0, 0, 0, time.UTC), WindowTokens: 1_000_000}
	d := Evaluate(cfg, st, Subscription{}, now)
	if d.ProactiveBlocked || d.EmergencyBlocked || d.UsedPercent != 0 {
		t.Fatalf("zero limit must be inert, got %+v", d)
	}
}

func TestSubscriptionUsesMaxOfActiveWindows(t *testing.T) {
	now := time.Date(2026, 6, 27, 10, 0, 0, 0, time.UTC)
	cfg := Config{Enabled: true, Mode: ModeClaudeSubscription, ProactivePercent: 50, EmergencyPercent: 80}
	sub := Subscription{
		FiveHourPercent: 30, FiveHourReset: now.Add(2 * time.Hour),
		WeeklyPercent: 85, WeeklyReset: now.Add(48 * time.Hour),
	}
	d := Evaluate(cfg, WindowState{}, sub, now)
	if d.UsedPercent != 85 {
		t.Fatalf("used: got %v want 85 (max of active windows)", d.UsedPercent)
	}
	if !d.ProactiveBlocked || !d.EmergencyBlocked {
		t.Fatalf("85%% must block both, got %+v", d)
	}
}

func TestSubscriptionIgnoresExpiredAndUnknownResets(t *testing.T) {
	now := time.Date(2026, 6, 27, 10, 0, 0, 0, time.UTC)
	cfg := Config{Enabled: true, Mode: ModeClaudeSubscription, ProactivePercent: 50, EmergencyPercent: 80}
	// 5h window already reset (past); weekly has no reported reset (zero).
	sub := Subscription{
		FiveHourPercent: 95, FiveHourReset: now.Add(-time.Minute),
		WeeklyPercent: 99, WeeklyReset: time.Time{},
	}
	d := Evaluate(cfg, WindowState{}, sub, now)
	if d.UsedPercent != 0 {
		t.Fatalf("expired/unknown snapshots must be ignored, got used=%v", d.UsedPercent)
	}
	if d.ProactiveBlocked || d.EmergencyBlocked {
		t.Fatalf("inert subscription must not block, got %+v", d)
	}
}

func TestResolvePercentsDefaultsAndOrdering(t *testing.T) {
	pro, emg := ResolvePercents(Config{})
	if pro != DefaultProactivePercent || emg != DefaultEmergencyPercent {
		t.Fatalf("defaults: got %d/%d want %d/%d", pro, emg, DefaultProactivePercent, DefaultEmergencyPercent)
	}
	// Emergency below proactive is clamped up to proactive.
	pro, emg = ResolvePercents(Config{ProactivePercent: 70, EmergencyPercent: 40})
	if pro != 70 || emg != 70 {
		t.Fatalf("ordering: got %d/%d want 70/70", pro, emg)
	}
}

func TestValidate(t *testing.T) {
	if err := (Config{Enabled: false}).Validate(); err != nil {
		t.Fatalf("disabled must pass: %v", err)
	}
	good := customCfg(1000, 50, 80)
	if err := good.Validate(); err != nil {
		t.Fatalf("valid custom config: %v", err)
	}
	bad := []Config{
		{Enabled: true, Mode: ModeCustomWindow, ResetSchedule: "not a cron", TokenLimit: 1},
		{Enabled: true, Mode: ModeCustomWindow, ResetSchedule: hourly, TokenLimit: 0},
		{Enabled: true, Mode: "bogus"},
		{Enabled: true, Mode: ModeCustomWindow, ResetSchedule: hourly, TokenLimit: 1, ProactivePercent: 150},
	}
	for i, c := range bad {
		if err := c.Validate(); err == nil {
			t.Fatalf("case %d: expected validation error for %+v", i, c)
		}
	}
}
