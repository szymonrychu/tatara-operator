// Package budget evaluates the per-project token-budget admission gate (issue
// #189). It is a leaf package of pure decision logic: given a project's budget
// config, its persisted custom-window accumulator OR the latest Claude-reported
// rate-limit snapshot, and the current time, it reports whether proactive work
// and/or incident work must pause because usage has reached the configured
// percentage of the window limit.
//
// It holds no Kubernetes types so it can be unit-tested in isolation and reused
// by both the dispatcher (admission gate) and the turn-complete callback (window
// accumulation roll).
package budget

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

// Mode selects how a project's token budget is measured.
type Mode string

const (
	// ModeCustomWindow meters the operator's own per-turn token accounting against
	// an absolute TokenLimit within a cron-anchored reset window. Fully
	// operator-side; suits API per-token billing.
	ModeCustomWindow Mode = "customWindow"
	// ModeClaudeSubscription gates on the Claude-code 5h and weekly usage
	// percentages reported by the wrapper (Anthropic anthropic-ratelimit-unified-*
	// headers). Inert until the wrapper reports a snapshot with a future reset.
	ModeClaudeSubscription Mode = "claudeSubscription"
)

// Default percentage thresholds (issue #189): proactive work pauses at 50% of
// the window, incident work is allowed up to 80%.
const (
	DefaultProactivePercent = 50
	DefaultEmergencyPercent = 80
)

// Config is a project's resolved token-budget configuration. The zero value has
// Enabled=false, leaving admission unchanged (backwards-compatible default).
type Config struct {
	Enabled          bool
	Mode             Mode
	ProactivePercent int
	EmergencyPercent int

	// Custom-window mode inputs.
	ResetSchedule  string        // 5-field cron marking each window reset boundary
	WindowDuration time.Duration // declared window length; bounds the reset search
	TokenLimit     int64         // absolute total-token budget per window

	// SpawnCeilingByKind gates each Task kind independently in claudeSubscription
	// mode: work of kind K is held once account usage reaches SpawnCeilingByKind[K]
	// percent. Kinds absent from the map are not per-kind gated (they fall through
	// to the pool-class proactive/emergency thresholds). Ignored in customWindow mode.
	SpawnCeilingByKind map[string]int
}

// WindowState is the persisted custom-window accumulator (carried on Project
// status). It records when the current window opened and how many tokens have
// been spent in it.
type WindowState struct {
	WindowStart  time.Time
	WindowTokens int64
}

// Subscription is the latest Claude-reported usage snapshot (subscription mode).
// Percentages are 0..100. A snapshot counts only while its Reset time is known
// and still in the future; a zero or past Reset is ignored, so the gate can
// never get permanently stuck on a snapshot it cannot expire (and subscription
// mode stays inert until the wrapper reports a proper snapshot).
type Subscription struct {
	FiveHourPercent float64
	FiveHourReset   time.Time
	WeeklyPercent   float64
	WeeklyReset     time.Time

	// Carried for metrics only; not used by the gate in v1.
	OpusPercent    float64
	OpusReset      time.Time
	SonnetPercent  float64
	SonnetReset    time.Time
	OverageEnabled bool
	OveragePercent float64
}

// Decision is the result of evaluating a project's budget at a point in time.
type Decision struct {
	// UsedPercent is the governing usage percentage (0..100+; custom-window mode
	// may exceed 100 if spend overran the limit).
	UsedPercent float64
	// ProactiveBlocked pauses the normal pool (brainstorm, implement, review, ...).
	// EmergencyBlocked pauses the alert pool (incidents). EmergencyBlocked implies
	// ProactiveBlocked because EmergencyPercent is ordered >= ProactivePercent.
	ProactiveBlocked bool
	EmergencyBlocked bool
}

// ParseSchedule parses a 5-field cron schedule (robfig ParseStandard), the same
// parser the project scan crons use.
func ParseSchedule(schedule string) (cron.Schedule, error) {
	return cron.ParseStandard(schedule)
}

// CurrentWindowStart returns the most recent fire of schedule at or before now,
// searched within [now-2*window, now]. window bounds the search so a frequent
// cron does not force an unbounded scan; pass the declared WindowDuration.
// Returns ok=false when no fire falls in the search range (misconfigured
// cron/duration), which the caller treats as "do not roll".
func CurrentWindowStart(sched cron.Schedule, now time.Time, window time.Duration) (time.Time, bool) {
	if window <= 0 {
		window = 7 * 24 * time.Hour // safe upper bound (covers a weekly window)
	}
	floor := now.Add(-2 * window)
	var last time.Time
	found := false
	for t := sched.Next(floor); !t.IsZero() && !t.After(now); t = sched.Next(t) {
		last = t
		found = true
	}
	return last, found
}

// Roll advances a custom-window accumulator to the current window. When now has
// crossed into a new window (the latest reset boundary is after the recorded
// WindowStart, or the state is uninitialised), the token count resets to zero
// and WindowStart is set to that boundary. addTokens (when > 0) is then added to
// the possibly-reset window. addTokens=0 makes Roll a pure read-side roll for
// the admission gate.
func Roll(cfg Config, state WindowState, now time.Time, addTokens int64) WindowState {
	if start, ok := windowStartFor(cfg, now); ok && state.WindowStart.Before(start) {
		state = WindowState{WindowStart: start, WindowTokens: 0}
	}
	if addTokens > 0 {
		state.WindowTokens += addTokens
	}
	return state
}

func windowStartFor(cfg Config, now time.Time) (time.Time, bool) {
	sched, err := ParseSchedule(cfg.ResetSchedule)
	if err != nil {
		return time.Time{}, false
	}
	return CurrentWindowStart(sched, now, cfg.WindowDuration)
}

// Evaluate computes the gate decision for a project. A disabled config always
// returns the zero Decision (nothing blocked).
func Evaluate(cfg Config, state WindowState, sub Subscription, now time.Time) Decision {
	if !cfg.Enabled {
		return Decision{}
	}
	proactive, emergency := ResolvePercents(cfg)
	used := usedPercent(cfg, state, sub, now)
	return Decision{
		UsedPercent:      used,
		ProactiveBlocked: used >= float64(proactive),
		EmergencyBlocked: used >= float64(emergency),
	}
}

func usedPercent(cfg Config, state WindowState, sub Subscription, now time.Time) float64 {
	switch cfg.Mode {
	case ModeClaudeSubscription:
		return subscriptionUsedPercent(sub, now)
	default: // ModeCustomWindow (and unset, for forward-compat)
		if cfg.TokenLimit <= 0 {
			return 0
		}
		rolled := Roll(cfg, state, now, 0)
		return float64(rolled.WindowTokens) / float64(cfg.TokenLimit) * 100
	}
}

func subscriptionUsedPercent(sub Subscription, now time.Time) float64 {
	pct := 0.0
	if active(sub.FiveHourReset, now) && sub.FiveHourPercent > pct {
		pct = sub.FiveHourPercent
	}
	if active(sub.WeeklyReset, now) && sub.WeeklyPercent > pct {
		pct = sub.WeeklyPercent
	}
	return pct
}

// KindBlocked reports whether work of the given kind must be held, given the
// account subscription usage. It applies only in claudeSubscription mode with a
// configured per-kind ceiling; every other case returns false so the caller's
// pool-class Decision remains authoritative.
func KindBlocked(cfg Config, sub Subscription, kind string, now time.Time) bool {
	if !cfg.Enabled || cfg.Mode != ModeClaudeSubscription {
		return false
	}
	ceiling, ok := cfg.SpawnCeilingByKind[kind]
	if !ok || ceiling <= 0 {
		return false
	}
	return subscriptionUsedPercent(sub, now) >= float64(ceiling)
}

// active reports whether a reported snapshot window is still current: a known
// reset time strictly in the future. An unknown (zero) or past reset is ignored
// so the gate cannot get stuck on a snapshot it cannot expire.
func active(reset, now time.Time) bool {
	return !reset.IsZero() && reset.After(now)
}

// ResolvePercents returns the configured thresholds, falling back to the
// defaults for non-positive values and ordering them so emergency >= proactive
// (incidents are never cut off before proactive work).
func ResolvePercents(cfg Config) (proactive, emergency int) {
	proactive = cfg.ProactivePercent
	if proactive <= 0 {
		proactive = DefaultProactivePercent
	}
	emergency = cfg.EmergencyPercent
	if emergency <= 0 {
		emergency = DefaultEmergencyPercent
	}
	if emergency < proactive {
		emergency = proactive
	}
	return proactive, emergency
}

// Validate checks an enabled config is self-consistent. Disabled configs always
// pass. customWindow requires a parseable ResetSchedule and a positive
// TokenLimit; both modes require in-range percentages.
func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.ProactivePercent < 0 || c.ProactivePercent > 100 {
		return fmt.Errorf("budget: proactivePercent %d out of range 0..100", c.ProactivePercent)
	}
	if c.EmergencyPercent < 0 || c.EmergencyPercent > 100 {
		return fmt.Errorf("budget: emergencyPercent %d out of range 0..100", c.EmergencyPercent)
	}
	switch c.Mode {
	case ModeCustomWindow:
		if _, err := ParseSchedule(c.ResetSchedule); err != nil {
			return fmt.Errorf("budget: resetSchedule %q invalid: %w", c.ResetSchedule, err)
		}
		if c.TokenLimit <= 0 {
			return fmt.Errorf("budget: tokenLimit must be positive in customWindow mode")
		}
	case ModeClaudeSubscription:
		// No extra inputs required; inert until the wrapper reports snapshots.
	default:
		return fmt.Errorf("budget: unknown mode %q", c.Mode)
	}
	return nil
}
