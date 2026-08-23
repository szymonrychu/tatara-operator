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

// Window names one Claude subscription usage window. The values match the
// tatara_account_usage_utilization{window} label values, so a threshold and its
// gauge always name the same thing.
type Window string

const (
	WindowFiveHour Window = "five_hour"
	WindowWeekly   Window = "seven_day"
)

// DefaultMaxSnapshotAge is how long a subscription usage snapshot keeps
// governing admission after it was observed.
//
// 90 minutes, and it is a QUALITY bound on the snapshot, not a bound the fleet
// is guaranteed to meet: it is short relative to the 5h window it governs, so a
// snapshot still inside it can never have missed more than about 30% of a 5h
// window's worth of unobserved burn. That clause is the whole justification.
//
// IT DOES NOT MEAN THE GATE IS NORMALLY GOVERNING. The earlier text here
// reasoned from turn DURATION ("every pod-TTL and turn-timeout bound is well
// under an hour, so any fleet that has run a turn in the last 90 minutes has a
// fresh snapshot ... roughly 3x the longest healthy inter-turn gap") and that
// premise is wrong about the binding quantity. A snapshot reaches the store
// only on a turn-complete callback, so store freshness is bounded by the gap
// BETWEEN turn completions across the whole fleet, which is unbounded when the
// queue is empty - not by how long a turn runs, which is irrelevant to it.
// Measured 2026-08-23 on the prod fleet: tatara_account_usage_snapshot_age_seconds
// {source="wrapper"} was 30719s against this 5400s bound, and
// tatara_account_usage_gate_ready read 0 in 40 of 56 samples over 14h - so the
// gate was fail-open, silently, for roughly three quarters of the day.
//
// The number stays 90m deliberately. It was picked from the wrong quantity, and
// the right one (the inter-turn-completion distribution, whose rising edges are
// tatara_account_usage_gate_ready and nothing else in the platform) is only
// measurable now that the feed exists. Picking a replacement constant before
// reading that distribution would repeat the original mistake. Raising it is
// also not free: the 30%-of-a-5h-window clause above is what stops being true.
// What SHOULD change first is where a fresh snapshot comes from between turns
// (an out-of-band push from the wrapper, or the /api/oauth/usage poller), which
// is a different change with a different blast radius.
//
// Until then, TataraAccountUsageFeedDead is traffic-gated on the operator's own
// queue gauges: a fail-open gate on an idle fleet is designed behaviour and
// costs nothing, and only a fail-open gate WITH WORK IN FLIGHT is a fault.
const DefaultMaxSnapshotAge = 90 * time.Minute

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

	// Per-window thresholds (claudeSubscription mode). Each window is checked
	// against its OWN pair; ProactiveBlocked/EmergencyBlocked are OR'd across
	// windows. A non-positive value inherits ProactivePercent/EmergencyPercent,
	// so a Config that sets none of these produces exactly the decision the
	// single-pair implementation produced.
	FiveHourProactivePercent int
	FiveHourEmergencyPercent int
	WeeklyProactivePercent   int
	WeeklyEmergencyPercent   int

	// MaxSnapshotAge stops a subscription snapshot governing once it is older
	// than this. Non-positive disables the check.
	//
	// It closes the F7 de-scope recorded in MEMORY.md 2026-07-04: before it,
	// budget.active() was the ONLY staleness gate, so a snapshot that simply
	// stopped being refreshed kept governing until its last-reported resets_at
	// and only then failed open.
	//
	// EXPIRY FAILS OPEN, matching active()'s existing direction and avoiding
	// wedging the platform on a broken feed. That is only defensible because a
	// dead feed is now alertable (tatara_account_usage_gate_ready). It is also
	// structurally incapable of deadlock: holding all work means no turns run,
	// which means no fresh snapshots, which means the snapshot ages out and the
	// gate re-opens, which means turns run and a fresh snapshot arrives and the
	// gate re-engages. Self-correcting oscillation, not a wedge.
	MaxSnapshotAge time.Duration
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

	// ObservedAt is when this snapshot was observed. ZERO MEANS UNKNOWN, and
	// unknown is treated as FRESH - that is the poller's path and every
	// pre-upgrade snapshot, and treating it as stale would silently disable the
	// gate for them.
	ObservedAt time.Time

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
	//
	// In claudeSubscription mode with per-window thresholds there is no single
	// scalar the two windows share, so "governing" is DEFINED as: the window
	// with the highest ratio of its own percent to its OWN proactive threshold,
	// reported as that window's raw percent. Selecting by relative pressure is
	// what makes "worse off" meaningful across windows with different
	// thresholds; reporting the raw percent is what keeps this gauge comparable
	// to the proactive/emergency threshold gauges, which carry that same
	// window's pair.
	UsedPercent float64
	// ProactiveBlocked pauses the normal pool (brainstorm, implement, review, ...).
	// EmergencyBlocked pauses the alert pool (incidents). EmergencyBlocked implies
	// ProactiveBlocked because EmergencyPercent is ordered >= ProactivePercent.
	ProactiveBlocked bool
	EmergencyBlocked bool

	// GoverningWindow is the window UsedPercent came from, empty in customWindow
	// mode and when no window is active or fresh. GoverningProactivePercent and
	// GoverningEmergencyPercent are ITS thresholds, so the
	// operator_token_budget_used_ratio scope="used"/"proactive"/"emergency"
	// triple always describes one coherent window.
	GoverningWindow           Window
	GoverningProactivePercent int
	GoverningEmergencyPercent int
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
	if cfg.Mode == ModeClaudeSubscription {
		return subscriptionDecision(cfg, sub, now)
	}
	proactive, emergency := ResolvePercents(cfg)
	used := usedPercent(cfg, state, now)
	return Decision{
		UsedPercent:               used,
		ProactiveBlocked:          used >= float64(proactive),
		EmergencyBlocked:          used >= float64(emergency),
		GoverningProactivePercent: proactive,
		GoverningEmergencyPercent: emergency,
	}
}

// subscriptionDecision checks EACH window against its OWN threshold pair and
// ORs the blocks across windows. A window contributes only while its reset is
// known and still in the future (active) and the snapshot itself is fresh
// (SnapshotFresh); everything else fails open, exactly as before.
func subscriptionDecision(cfg Config, sub Subscription, now time.Time) Decision {
	d := Decision{}
	if !SnapshotFresh(cfg, sub.ObservedAt, now) {
		return d
	}
	worst := -1.0
	for _, w := range []struct {
		name    Window
		percent float64
		reset   time.Time
	}{
		{WindowFiveHour, sub.FiveHourPercent, sub.FiveHourReset},
		{WindowWeekly, sub.WeeklyPercent, sub.WeeklyReset},
	} {
		if !active(w.reset, now) {
			continue
		}
		proactive, emergency := ResolveWindowPercents(cfg, w.name)
		if w.percent >= float64(proactive) {
			d.ProactiveBlocked = true
		}
		if w.percent >= float64(emergency) {
			d.EmergencyBlocked = true
		}
		if ratio := w.percent / float64(proactive); ratio > worst {
			worst = ratio
			d.UsedPercent = w.percent
			d.GoverningWindow = w.name
			d.GoverningProactivePercent = proactive
			d.GoverningEmergencyPercent = emergency
		}
	}
	return d
}

// usedPercent meters ModeCustomWindow (and an unset mode, for forward-compat).
// ModeClaudeSubscription no longer reaches here: Evaluate short-circuits to
// subscriptionDecision, because one scalar cannot carry two windows checked
// against two different threshold pairs.
func usedPercent(cfg Config, state WindowState, now time.Time) float64 {
	if cfg.TokenLimit <= 0 {
		return 0
	}
	rolled := Roll(cfg, state, now, 0)
	return float64(rolled.WindowTokens) / float64(cfg.TokenLimit) * 100
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
//
// SCOPE BOUNDARY, deliberate: it keeps the max()-across-windows collapse even
// though Evaluate moved to per-window thresholds, and it ignores
// Config.MaxSnapshotAge. SpawnCeilingByKind is a graduated "how much account
// headroom is left" ladder, nothing configures it in prod today, and changing
// both surfaces at once widens blast radius for no benefit. Its only caller is
// queue_controller.go's per-event decision.
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

// ResolveWindowPercents returns the (proactive, emergency) pair governing one
// window: the per-window override when set, else the mode-wide pair, else the
// package defaults. It delegates the fallback and the emergency >= proactive
// ordering to ResolvePercents so both paths order identically.
func ResolveWindowPercents(cfg Config, w Window) (proactive, emergency int) {
	scoped := cfg
	switch w {
	case WindowFiveHour:
		if cfg.FiveHourProactivePercent > 0 {
			scoped.ProactivePercent = cfg.FiveHourProactivePercent
		}
		if cfg.FiveHourEmergencyPercent > 0 {
			scoped.EmergencyPercent = cfg.FiveHourEmergencyPercent
		}
	case WindowWeekly:
		if cfg.WeeklyProactivePercent > 0 {
			scoped.ProactivePercent = cfg.WeeklyProactivePercent
		}
		if cfg.WeeklyEmergencyPercent > 0 {
			scoped.EmergencyPercent = cfg.WeeklyEmergencyPercent
		}
	}
	return ResolvePercents(scoped)
}

// SnapshotFresh reports whether a snapshot observed at observedAt still governs
// at now. A zero observedAt is UNKNOWN and treated as fresh; a non-positive
// MaxSnapshotAge disables the check. See Config.MaxSnapshotAge for the
// fail-open rationale.
func SnapshotFresh(cfg Config, observedAt, now time.Time) bool {
	if cfg.MaxSnapshotAge <= 0 || observedAt.IsZero() {
		return true
	}
	return now.Sub(observedAt) <= cfg.MaxSnapshotAge
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
