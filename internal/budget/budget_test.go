package budget

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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

func TestKindBlocked(t *testing.T) {
	future := time.Now().Add(time.Hour)
	sub := Subscription{FiveHourPercent: 42, FiveHourReset: future, WeeklyPercent: 10, WeeklyReset: future}
	base := Config{Enabled: true, Mode: ModeClaudeSubscription,
		SpawnCeilingByKind: map[string]int{"brainstorm": 40, "incident": 98}}
	cases := []struct {
		name    string
		cfg     Config
		kind    string
		blocked bool
	}{
		{"brainstorm over ceiling", base, "brainstorm", true}, // 42 >= 40
		{"incident under ceiling", base, "incident", false},   // 42 < 98
		{"kind without ceiling not blocked", base, "implement", false},
		{"disabled never blocks", Config{Mode: ModeClaudeSubscription, SpawnCeilingByKind: base.SpawnCeilingByKind}, "brainstorm", false},
		{"customWindow mode never per-kind blocks", Config{Enabled: true, Mode: ModeCustomWindow, SpawnCeilingByKind: base.SpawnCeilingByKind}, "brainstorm", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := KindBlocked(tc.cfg, sub, tc.kind, time.Now()); got != tc.blocked {
				t.Fatalf("KindBlocked=%v want %v", got, tc.blocked)
			}
		})
	}
}

func TestKindBlockedIgnoresExpiredWindow(t *testing.T) {
	past := time.Now().Add(-time.Hour)
	sub := Subscription{FiveHourPercent: 99, FiveHourReset: past} // expired -> ignored
	cfg := Config{Enabled: true, Mode: ModeClaudeSubscription, SpawnCeilingByKind: map[string]int{"brainstorm": 40}}
	if KindBlocked(cfg, sub, "brainstorm", time.Now()) {
		t.Fatal("expired window must not block")
	}
}

func TestResolveWindowPercentsFallback(t *testing.T) {
	tests := []struct {
		name       string
		cfg        Config
		window     Window
		wantProact int
		wantEmerg  int
	}{
		{name: "all unset falls back to package defaults", cfg: Config{}, window: WindowFiveHour, wantProact: 50, wantEmerg: 80},
		{
			name:       "falls back to the mode-wide pair",
			cfg:        Config{ProactivePercent: 60, EmergencyPercent: 85},
			window:     WindowWeekly,
			wantProact: 60, wantEmerg: 85,
		},
		{
			name:       "per-window overrides win",
			cfg:        Config{ProactivePercent: 60, EmergencyPercent: 85, WeeklyProactivePercent: 75, WeeklyEmergencyPercent: 90},
			window:     WindowWeekly,
			wantProact: 75, wantEmerg: 90,
		},
		{
			name:       "five-hour override does not leak into the weekly pair",
			cfg:        Config{FiveHourProactivePercent: 80, FiveHourEmergencyPercent: 90},
			window:     WindowWeekly,
			wantProact: 50, wantEmerg: 80,
		},
		{
			name:       "emergency is ordered at or above proactive",
			cfg:        Config{WeeklyProactivePercent: 75, WeeklyEmergencyPercent: 10},
			window:     WindowWeekly,
			wantProact: 75, wantEmerg: 75,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, e := ResolveWindowPercents(tc.cfg, tc.window)
			require.Equal(t, tc.wantProact, p)
			require.Equal(t, tc.wantEmerg, e)
		})
	}
}

func TestSnapshotFresh(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	cfg := Config{MaxSnapshotAge: 90 * time.Minute}

	tests := []struct {
		name       string
		cfg        Config
		observedAt time.Time
		want       bool
	}{
		{name: "recent snapshot is fresh", cfg: cfg, observedAt: now.Add(-10 * time.Minute), want: true},
		{name: "exactly at max age is still fresh", cfg: cfg, observedAt: now.Add(-90 * time.Minute), want: true},
		{name: "one second past max age is stale", cfg: cfg, observedAt: now.Add(-90*time.Minute - time.Second), want: false},
		{name: "zero observedAt is unknown and treated as fresh", cfg: cfg, want: true},
		{name: "non-positive max age disables the check", cfg: Config{}, observedAt: now.Add(-30 * 24 * time.Hour), want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, SnapshotFresh(tc.cfg, tc.observedAt, now))
		})
	}
}

func TestEvaluateSubscriptionPerWindow(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	live := now.Add(time.Hour)
	base := Config{Enabled: true, Mode: ModeClaudeSubscription}

	perWindow := func() Config {
		c := base
		c.FiveHourProactivePercent, c.FiveHourEmergencyPercent = 80, 90
		c.WeeklyProactivePercent, c.WeeklyEmergencyPercent = 75, 85
		return c
	}

	tests := []struct {
		name       string
		cfg        Config
		sub        Subscription
		wantUsed   float64
		wantWindow Window
		wantProact bool
		wantEmerg  bool
	}{
		{
			name:       "defaults unchanged: 5h over 50 blocks proactive only",
			cfg:        base,
			sub:        Subscription{FiveHourPercent: 55, FiveHourReset: live, WeeklyPercent: 10, WeeklyReset: live},
			wantUsed:   55,
			wantWindow: WindowFiveHour,
			wantProact: true,
		},
		{
			name:       "defaults unchanged: 5h over 80 blocks both",
			cfg:        base,
			sub:        Subscription{FiveHourPercent: 85, FiveHourReset: live, WeeklyPercent: 10, WeeklyReset: live},
			wantUsed:   85,
			wantWindow: WindowFiveHour,
			wantProact: true,
			wantEmerg:  true,
		},
		{
			name:       "defaults unchanged: nothing blocked below 50",
			cfg:        base,
			sub:        Subscription{FiveHourPercent: 20, FiveHourReset: live, WeeklyPercent: 30, WeeklyReset: live},
			wantUsed:   30,
			wantWindow: WindowWeekly,
		},
		{
			name:       "per-window: weekly threshold 75 blocks while 5h threshold 80 does not",
			cfg:        perWindow(),
			sub:        Subscription{FiveHourPercent: 70, FiveHourReset: live, WeeklyPercent: 78, WeeklyReset: live},
			wantUsed:   78,
			wantWindow: WindowWeekly,
			wantProact: true,
		},
		{
			name:       "per-window: 5h wins when it is relatively worse off",
			cfg:        perWindow(),
			sub:        Subscription{FiveHourPercent: 79, FiveHourReset: live, WeeklyPercent: 40, WeeklyReset: live},
			wantUsed:   79,
			wantWindow: WindowFiveHour,
		},
		{
			name:       "per-window: an inactive window contributes nothing",
			cfg:        perWindow(),
			sub:        Subscription{FiveHourPercent: 99, FiveHourReset: now.Add(-time.Minute), WeeklyPercent: 40, WeeklyReset: live},
			wantUsed:   40,
			wantWindow: WindowWeekly,
		},
		{
			name:       "expired reset is ignored (fail open), unchanged from today",
			cfg:        base,
			sub:        Subscription{FiveHourPercent: 99, FiveHourReset: now.Add(-time.Hour)},
			wantUsed:   0,
			wantWindow: "",
		},
		{
			name:       "zero reset is ignored (fail open), unchanged from today",
			cfg:        base,
			sub:        Subscription{FiveHourPercent: 99},
			wantUsed:   0,
			wantWindow: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Evaluate(tc.cfg, WindowState{}, tc.sub, now)
			require.InDelta(t, tc.wantUsed, got.UsedPercent, 0.001)
			require.Equal(t, tc.wantWindow, got.GoverningWindow)
			require.Equal(t, tc.wantProact, got.ProactiveBlocked)
			require.Equal(t, tc.wantEmerg, got.EmergencyBlocked)
			if tc.wantWindow != "" {
				wantPro, wantEmg := ResolveWindowPercents(tc.cfg, tc.wantWindow)
				require.Equal(t, wantPro, got.GoverningProactivePercent)
				require.Equal(t, wantEmg, got.GoverningEmergencyPercent)
			}
		})
	}
}

func TestEvaluateSubscriptionStaleness(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	live := now.Add(time.Hour)
	cfg := Config{Enabled: true, Mode: ModeClaudeSubscription, MaxSnapshotAge: 90 * time.Minute}
	hot := Subscription{FiveHourPercent: 99, FiveHourReset: live}

	tests := []struct {
		name       string
		cfg        Config
		observedAt time.Time
		wantBlock  bool
	}{
		{name: "fresh snapshot governs", cfg: cfg, observedAt: now.Add(-10 * time.Minute), wantBlock: true},
		{name: "exactly at max age still governs", cfg: cfg, observedAt: now.Add(-90 * time.Minute), wantBlock: true},
		{name: "stale snapshot fails open", cfg: cfg, observedAt: now.Add(-91 * time.Minute), wantBlock: false},
		{name: "zero observedAt is treated as fresh (poller path)", cfg: cfg, wantBlock: true},
		{
			name:       "max age unset disables the staleness gate",
			cfg:        Config{Enabled: true, Mode: ModeClaudeSubscription},
			observedAt: now.Add(-30 * 24 * time.Hour),
			wantBlock:  true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sub := hot
			sub.ObservedAt = tc.observedAt
			got := Evaluate(tc.cfg, WindowState{}, sub, now)
			require.Equal(t, tc.wantBlock, got.ProactiveBlocked)
			if !tc.wantBlock {
				// A stale snapshot fails FULLY open: no window governs, so the
				// gauge reports 0 rather than the last-known percent.
				require.Zero(t, got.UsedPercent)
				require.Equal(t, Window(""), got.GoverningWindow)
			}
		})
	}
}

// legacySubscriptionDecision reproduces the pre-per-window algorithm verbatim:
// max() across the active windows, compared against the single mode-wide
// threshold pair. It is the oracle for TestEvaluateDefaultsMatchLegacyDecision.
func legacySubscriptionDecision(cfg Config, sub Subscription, now time.Time) (used float64, proactive, emergency bool) {
	pct := 0.0
	if !sub.FiveHourReset.IsZero() && sub.FiveHourReset.After(now) && sub.FiveHourPercent > pct {
		pct = sub.FiveHourPercent
	}
	if !sub.WeeklyReset.IsZero() && sub.WeeklyReset.After(now) && sub.WeeklyPercent > pct {
		pct = sub.WeeklyPercent
	}
	pro, emg := ResolvePercents(cfg)
	return pct, pct >= float64(pro), pct >= float64(emg)
}

// TestEvaluateDefaultsMatchLegacyDecision is THE compatibility lock: an operator
// with no per-window thresholds and no max snapshot age must decide exactly what
// the single-pair max()-across-windows implementation decided, for every
// combination of window percents, reset states and observedAt values. A zero
// observedAt is fresh (the old wrapper sends no timestamp) and an unset
// MaxSnapshotAge disables staleness entirely (the pre-upgrade config).
func TestEvaluateDefaultsMatchLegacyDecision(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	resets := map[string]time.Time{
		"live":   now.Add(time.Hour),
		"past":   now.Add(-time.Hour),
		"zero":   {},
		"future": now.Add(48 * time.Hour),
	}
	percents := []float64{0, 20, 49, 50, 79, 80, 99, 100}
	observedAts := map[string]time.Time{
		"zero (old wrapper reports no timestamp)": {},
		"recent":  now.Add(-time.Minute),
		"ancient": now.Add(-30 * 24 * time.Hour),
	}
	cfgs := map[string]Config{
		"thresholds unset": {Enabled: true, Mode: ModeClaudeSubscription},
		"explicit 50/80":   {Enabled: true, Mode: ModeClaudeSubscription, ProactivePercent: 50, EmergencyPercent: 80},
		"mode-wide 60/85":  {Enabled: true, Mode: ModeClaudeSubscription, ProactivePercent: 60, EmergencyPercent: 85},
		"max age at default": {Enabled: true, Mode: ModeClaudeSubscription,
			MaxSnapshotAge: DefaultMaxSnapshotAge},
	}

	for cfgName, cfg := range cfgs {
		for obsName, obs := range observedAts {
			// A real max-age plus a real old timestamp is the ONE combination
			// that legitimately diverges: that is the new staleness gate doing
			// its job, covered by TestEvaluateSubscriptionStaleness.
			if cfg.MaxSnapshotAge > 0 && !obs.IsZero() && now.Sub(obs) > cfg.MaxSnapshotAge {
				continue
			}
			for fiveName, fiveReset := range resets {
				for weekName, weekReset := range resets {
					for _, fivePct := range percents {
						for _, weekPct := range percents {
							sub := Subscription{
								FiveHourPercent: fivePct, FiveHourReset: fiveReset,
								WeeklyPercent: weekPct, WeeklyReset: weekReset,
								ObservedAt: obs,
							}
							wantUsed, wantPro, wantEmg := legacySubscriptionDecision(cfg, sub, now)
							got := Evaluate(cfg, WindowState{}, sub, now)
							require.Equalf(t, wantUsed, got.UsedPercent,
								"used: cfg=%s observedAt=%s 5h=%v/%s weekly=%v/%s",
								cfgName, obsName, fivePct, fiveName, weekPct, weekName)
							require.Equalf(t, wantPro, got.ProactiveBlocked,
								"proactive: cfg=%s observedAt=%s 5h=%v/%s weekly=%v/%s",
								cfgName, obsName, fivePct, fiveName, weekPct, weekName)
							require.Equalf(t, wantEmg, got.EmergencyBlocked,
								"emergency: cfg=%s observedAt=%s 5h=%v/%s weekly=%v/%s",
								cfgName, obsName, fivePct, fiveName, weekPct, weekName)
						}
					}
				}
			}
		}
	}
}

// KindBlocked keeps the max()-based semantics deliberately (scope boundary).
func TestKindBlockedUnchangedByPerWindowThresholds(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	live := now.Add(time.Hour)
	cfg := Config{
		Enabled: true, Mode: ModeClaudeSubscription,
		SpawnCeilingByKind:       map[string]int{"implement": 60},
		FiveHourProactivePercent: 80, WeeklyProactivePercent: 75,
	}
	sub := Subscription{FiveHourPercent: 65, FiveHourReset: live, WeeklyPercent: 10, WeeklyReset: live}
	require.True(t, KindBlocked(cfg, sub, "implement", now))

	// A stale snapshot does NOT release the per-kind ceiling either: staleness
	// governs Evaluate only, which is the other half of the same scope boundary.
	cfg.MaxSnapshotAge = 90 * time.Minute
	sub.ObservedAt = now.Add(-24 * time.Hour)
	require.True(t, KindBlocked(cfg, sub, "implement", now))
}
