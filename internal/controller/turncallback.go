package controller

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/budget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// pollGetTurnTimeout is the per-task context deadline applied to each GetTurn
// call in PollOnce. It ensures a single slow or unreachable wrapper pod cannot
// stall the entire backstop cycle beyond this window (finding 4).
const pollGetTurnTimeout = 5 * time.Second

// CallbackServer handles the in-cluster /internal/turn-complete endpoint the
// wrapper POSTs to on each turn, and runs the poll backstop for missed
// callbacks.
// When CallbackSecret is non-empty the handler enforces HMAC-SHA256
// verification: the operator injects the secret into each wrapper Pod env
// (CALLBACK_HMAC_SECRET) and the wrapper sends X-Tatara-Signature:
// sha256=<hex(HMAC-SHA256(body, secret))>. Requests that omit or mismatch the
// header are rejected 401. When CallbackSecret is empty the check is skipped
// (backward-compatible with existing deployments that pre-date the field).
type CallbackServer struct {
	Client    client.Client
	Metrics   *obs.OperatorMetrics
	Session   agent.Session
	Namespace string
	// CallbackSecret, when non-empty, activates HMAC-SHA256 verification on
	// /internal/turn-complete. Read from CALLBACK_HMAC_SECRET (delivered to the
	// operator via SecretKeyRef from the callback-hmac Secret). Wrapper Pods get
	// the same secret via SecretKeyRef (CALLBACK_HMAC_SECRET_NAME) and sign their
	// callbacks. Closes the trust gap documented in the original security note
	// when the secret is configured; when empty the NetworkPolicy remains the
	// sole control (finding 1/r3).
	CallbackSecret string
	// PushMetrics, when set, mounts the wrapper push-metrics endpoint on the
	// same internal listener (also not exposed via ingress).
	PushMetrics http.Handler
	// ReaperGrace is the minimum pod age before the reaper will consider
	// deleting it. Zero means use the default (pollRequeue). Set to a small
	// value in tests to bypass the grace window without waiting.
	ReaperGrace time.Duration
	// IdlePodReapAfter is how long an agent pod may sit with no live turn before
	// the reaper deletes it as a leaked wrapper (issue #237). Set from
	// config.IdlePodReapAfter, already clamped to config.MinIdlePodReap. Zero or
	// negative disables the idle backstop (e.g. tests that do not exercise it).
	IdlePodReapAfter time.Duration
	// BudgetDefaults is the operator-wide token-budget config (issue #189). Each
	// Project layers its spec.tokenBudget over this via Project.BudgetConfig. The
	// zero value is disabled, so the budget accounting is inert until configured.
	BudgetDefaults budget.Config
}

// InternalIssueReport is one report_internal_issue call the agent made during
// a turn, as reported by the wrapper's transcript Tailer. Category/Severity
// are the wrapper's already-clamped values (always exactly one of the known
// enum members, e.g. severity is always "error" or "warn"), not raw agent
// input. JSON tags must match tatara-claude-code-wrapper's
// internal/turn.InternalIssueReport exactly.
type InternalIssueReport struct {
	Category      string `json:"category"`
	Severity      string `json:"severity"`
	Description   string `json:"description"`
	OffendingTool string `json:"offending_tool"`
	ResourceID    string `json:"resource_id"`
}

type turnCompletePayload struct {
	TurnID string `json:"turnId"`
	// TaskName is optionally set by the wrapper (TATARA_TASK env) to enable
	// O(1) task resolution via direct Get instead of full-namespace List+scan
	// (findings 4, 6).
	TaskName        string          `json:"taskName,omitempty"`
	State           string          `json:"state"`
	FinalText       string          `json:"finalText"`
	StopReason      string          `json:"stopReason"`
	Error           string          `json:"error"`
	DurationSeconds float64         `json:"durationSeconds"`
	Usage           json.RawMessage `json:"usage,omitempty"`
	// sessionId / conversationObjectKey were the persisted-conversation pointer
	// an older wrapper reports. Conversation persistence is DELETED (the bundle
	// IS the continuation state). The fields are deliberately not declared, so
	// json.Unmarshal - which does NOT use DisallowUnknownFields here - silently
	// ignores them from an old wrapper instead of failing the callback.
	// PushedRepos are the repos the agent actually pushed this turn (contract
	// G.2). It is RETAINED, not dropped: without it the operator cannot tell "no
	// diff" from "forgot to push" on a multi-repo Task, and the G.7 TTL synthetic
	// handoff note is BUILT from it.
	PushedRepos []string `json:"pushedRepos,omitempty"`
	// InternalIssues mirrors tatara-claude-code-wrapper's
	// internal/turn.InternalIssueReport JSON shape exactly (no shared Go
	// module between the two repos - this is a wire contract, not an import).
	// Empty/absent when the turn reported nothing.
	InternalIssues []InternalIssueReport `json:"internalIssues,omitempty"`
	// rateLimit was the per-turn Claude usage snapshot the wrapper reported for
	// the claudeSubscription budget mode (issue #189). Retired: subscription
	// state now lives only in the fleet-wide account-usage poller/store (issue
	// #189 follow-up). The field is deliberately not declared here so an
	// incoming "rateLimit" key from an older wrapper is silently ignored by
	// json.Unmarshal (wire compatibility) instead of being persisted.
}

// turnUsage mirrors the usage object the wrapper posts in the turn-complete
// payload. Fields match the wrapper's turn.Record.Usage JSON (confirmed from
// tatara-claude-code-wrapper/internal/turn/turn.go).
type turnUsage struct {
	InputTokens              int64 `json:"input_tokens"`
	OutputTokens             int64 `json:"output_tokens"`
	CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
	CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
}

// rollTurnStats rolls one turn's usage onto Task.status.stats (contract G.4):
// the four token counters, and stats.turns, which is a LIFETIME count checked
// against maxTurnsPerTask - it is NOT reset by a stage transition or a pod
// respawn, unlike stats.podRecreations.
//
// It runs inside recordUsage's RetryOnConflict closure, behind the same
// stale/duplicate-callback guards, so a replayed callback cannot double-count.
func rollTurnStats(t *tatarav1alpha1.Task, u turnUsage) {
	t.Status.Stats.TokensInput += u.InputTokens
	t.Status.Stats.TokensOutput += u.OutputTokens
	t.Status.Stats.TokensCacheRead += u.CacheReadInputTokens
	t.Status.Stats.TokensCacheCreation += u.CacheCreationInputTokens
	t.Status.Stats.Turns++
}

// Handler returns the http.Handler for POST /internal/turn-complete.
func (s *CallbackServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/turn-complete", s.handleTurnComplete)
	if s.PushMetrics != nil {
		mux.Handle("/internal/metrics/push", s.PushMetrics)
	}
	return mux
}

func (s *CallbackServer) handleTurnComplete(w http.ResponseWriter, r *http.Request) {
	l := log.FromContext(r.Context())
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// Read body once so we can both verify the HMAC and decode the payload.
	rawBody, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body failed", http.StatusBadRequest)
		return
	}
	// HMAC verification: enforced when CallbackSecret is configured (finding 1/r3).
	if s.CallbackSecret != "" {
		sig := r.Header.Get("X-Tatara-Signature")
		if !validHMACSignature(rawBody, sig, s.CallbackSecret) {
			l.Info("turn-complete rejected: invalid or missing HMAC signature",
				"action", "callback_authn_failed")
			if s.Metrics != nil {
				s.Metrics.RecordAuth("rejected")
			}
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}
	var p turnCompletePayload
	if err := json.Unmarshal(rawBody, &p); err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	if p.TurnID == "" {
		http.Error(w, "turnId is required", http.StatusBadRequest)
		return
	}
	if s.Metrics != nil {
		s.Metrics.ObserveTurnDuration(p.DurationSeconds)
	}

	// Resolve once; pass the resolved task into both writes to avoid a second
	// full-namespace List call. When the wrapper supplies taskName we do a
	// direct Get (O(1)); otherwise fall back to the full-namespace List+scan
	// for legacy wrappers (findings 4, 6).
	task, err := s.resolveTaskByTurnWithHint(r.Context(), p.TurnID, p.TaskName)
	if err != nil {
		if errors.Is(err, errTurnNotFound) {
			http.Error(w, "unknown turn", http.StatusNotFound)
			return
		}
		l.Error(err, "resolve task by turn", "turn_id", p.TurnID)
		http.Error(w, "resolve failed", http.StatusInternalServerError)
		return
	}

	var tokenDelta int64
	var usageRecorded bool
	if len(p.Usage) > 0 {
		d, rec, err := s.recordUsage(r.Context(), task, p.Usage, p.TurnID)
		if err != nil {
			l.Error(err, "record turn usage (non-fatal)", "turn_id", p.TurnID)
			// non-fatal: continue to record the result
		}
		tokenDelta, usageRecorded = d, rec
	}

	if err := s.recordResult(r.Context(), agent.TurnResult{
		State: p.State, FinalText: p.FinalText, StopReason: p.StopReason, Err: p.Error,
	}, task, p.TurnID); err != nil {
		l.Error(err, "record turn result", "turn_id", p.TurnID)
		http.Error(w, "record failed", http.StatusInternalServerError)
		return
	}
	// Roll the project's custom-window token accumulator (issue #189). Best-effort:
	// a budget bookkeeping failure never fails the turn. The claudeSubscription
	// snapshot is no longer sourced here; it comes from the fleet-wide
	// account-usage poller (issue #189 follow-up).
	if err := s.updateProjectBudget(r.Context(), task, tokenDelta, usageRecorded); err != nil {
		l.Error(err, "update project token budget (non-fatal)", "turn_id", p.TurnID)
	}
	l.Info("recorded turn result", "action", "turn_complete", "turn_id", p.TurnID, "state", p.State)
	// Re-log each internal-issue report on the operator's own (Loki-collected)
	// stdout: agent pods are not scraped, so this is the only path the
	// description reaches an alertable log stream. One line per issue.
	for _, ii := range p.InternalIssues {
		fields := []any{
			"action", "agent_internal_issue",
			"category", ii.Category,
			"severity", ii.Severity,
			"description", ii.Description,
			"turn_id", p.TurnID,
			"offending_tool", ii.OffendingTool,
			"resource_id", ii.ResourceID,
		}
		if ii.Severity == "error" {
			l.Error(nil, "agent reported an internal issue", fields...)
		} else {
			l.Info("agent reported an internal issue", fields...)
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

var errTurnNotFound = errors.New("no task with that current turn")

// recordUsage parses a raw usage JSON blob and persists LastTurnInputTokens /
// CumulativeTokens on the matching Task via RetryOnConflict.
// Absent or unparseable usage is silently tolerated (no-op).
// turnID is the turn being completed; the guard inside the closure bails when
// the fresh Task's annCurrentTurn no longer matches (stale/duplicate callback)
// or the task is terminal, preventing double-counting (finding 1).
// task must be the already-resolved Task (resolved by the caller to avoid a
// second full-namespace List call).
// It returns the turn's total token delta (input incl. cache-read, plus output)
// and recorded=true only when the per-Task status write actually landed (so the
// caller can roll the project token-budget window without double-counting a
// stale/duplicate callback).
func (s *CallbackServer) recordUsage(ctx context.Context, task *tatarav1alpha1.Task, raw json.RawMessage, turnID string) (delta int64, recorded bool, err error) {
	if len(raw) == 0 {
		return 0, false, nil
	}
	var u turnUsage
	if err := json.Unmarshal(raw, &u); err != nil {
		return 0, false, nil // tolerate malformed usage
	}
	inputTotal := u.InputTokens + u.CacheReadInputTokens
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := s.Client.Get(ctx, types.NamespacedName{Namespace: s.Namespace, Name: task.Name}, fresh); err != nil {
			return fmt.Errorf("reload task for usage: %w", err)
		}
		// Guard: stale callback or task already terminal - skip to avoid double-count.
		if fresh.Annotations[annCurrentTurn] != turnID {
			return nil
		}
		// Guard: annTurnComplete being non-empty means recordResult already landed
		// for this turn (it stamps annTurnComplete). A duplicate callback arriving
		// before the reconcile advances annCurrentTurn would pass the guard above
		// but must not re-accumulate CumulativeTokens (finding 2/r3).
		if fresh.Annotations[annTurnComplete] != "" {
			return nil
		}
		if tatarav1alpha1.TaskDone(fresh) {
			return nil
		}
		rollTurnStats(fresh, u)
		if err := s.Client.Status().Update(ctx, fresh); err != nil {
			return err
		}
		recorded = true
		return nil
	}); err != nil {
		return 0, false, err
	}
	// Mirror the persisted per-turn delta into operator_task_tokens_total, but
	// only when the status write actually landed (the guards above skip duplicate
	// or stale callbacks), so the metric is not double-counted.
	if recorded && s.Metrics != nil {
		project, repo, kind, issue, model := taskTokenLabels(task)
		s.Metrics.AddTaskTokens(project, repo, kind, issue, model,
			u.InputTokens, u.OutputTokens, u.CacheReadInputTokens, u.CacheCreationInputTokens)
		s.Metrics.AddTaskTurn(project, repo, kind, issue)
	}
	return inputTotal + u.OutputTokens, recorded, nil
}

// updateProjectBudget rolls the project's custom-window token accumulator
// (issue #189), then refreshes the used-ratio gauge. It is a no-op unless the
// project's resolved budget is enabled in customWindow mode. Best-effort and
// idempotent: the window roll runs only when this turn's usage actually landed
// (recorded) so a stale/duplicate callback never double-counts. A missing
// Project is tolerated.
//
// claudeSubscription mode is deliberately NOT evaluated here: that snapshot now
// lives only in the fleet-wide account-usage store (poller-fed, issue #189
// follow-up), which the dispatcher admission gate reads directly. Deriving a
// ratio from the per-project Status.TokenBudget subscription fields here would
// race that store with stale/never-updated data (Task A8).
func (s *CallbackServer) updateProjectBudget(ctx context.Context, task *tatarav1alpha1.Task, tokenDelta int64, recorded bool) error {
	projName := task.Spec.ProjectRef
	if projName == "" {
		return nil
	}
	if !recorded || tokenDelta <= 0 {
		return nil // nothing to accumulate
	}
	now := time.Now()
	var ratio float64
	var enabled bool
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		proj := &tatarav1alpha1.Project{}
		if err := s.Client.Get(ctx, types.NamespacedName{Namespace: s.Namespace, Name: projName}, proj); err != nil {
			return client.IgnoreNotFound(err)
		}
		cfg := proj.BudgetConfig(s.BudgetDefaults)
		if !cfg.Enabled || cfg.Mode != budget.ModeCustomWindow {
			enabled = false
			return nil
		}
		enabled = true
		before := proj.BudgetWindowState()
		after := budget.Roll(cfg, before, now, tokenDelta)
		changed := after.WindowTokens != before.WindowTokens || !after.WindowStart.Equal(before.WindowStart)
		if changed {
			proj.SetBudgetWindowState(after)
		}
		ratio = budget.Evaluate(cfg, proj.BudgetWindowState(), budget.Subscription{}, now).UsedPercent / 100
		if !changed {
			return nil
		}
		return s.Client.Status().Update(ctx, proj)
	}); err != nil {
		return err
	}
	if enabled && s.Metrics != nil {
		s.Metrics.SetTokenBudgetUsedRatio(projName, "used", ratio)
	}
	return nil
}

// taskTokenLabels returns the project, repo, kind, and issue labels for token
// metrics. issue is set only for issue-scoped tasks (Spec.Source present),
// preferring the IssueRef and falling back to the numeric Number, and is left
// empty otherwise to bound series cardinality.
func taskTokenLabels(task *tatarav1alpha1.Task) (project, repo, kind, issue, model string) {
	project = task.Spec.ProjectRef
	repo = task.Spec.RepositoryRef
	kind = task.Spec.Kind
	if task.Spec.Source != nil {
		switch {
		case task.Spec.Source.IssueRef != "":
			issue = task.Spec.Source.IssueRef
		case task.Spec.Source.Number > 0:
			issue = strconv.Itoa(task.Spec.Source.Number)
		}
	}
	model = task.Status.ResolvedModel
	return
}

// recordResult bumps the Task's turn-complete annotation to requeue its
// reconcile, behind the stale-turn and terminal guards so a duplicate or
// late callback cannot stamp a turn the Task has already moved past.
// task must be the already-resolved Task; turnID is the turn being completed.
func (s *CallbackServer) recordResult(ctx context.Context, tr agent.TurnResult, task *tatarav1alpha1.Task, turnID string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := s.Client.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, fresh); err != nil {
			return fmt.Errorf("reload task: %w", err)
		}
		// Guard: bail out if the Task has advanced to a different turn (stale
		// callback) or its work is already over.
		if fresh.Annotations[annCurrentTurn] != turnID {
			// Turn has advanced or been cleared; stale callback - no-op.
			return nil
		}
		if tatarav1alpha1.TaskDone(fresh) {
			return nil
		}

		// Stamp turn-complete to requeue the reconcile.
		if fresh.Annotations == nil {
			fresh.Annotations = map[string]string{}
		}
		fresh.Annotations[annTurnComplete] = time.Now().UTC().Format(time.RFC3339)
		return s.Client.Update(ctx, fresh)
	})
}

// resolveTaskByTurnWithHint finds the Task whose current-turn annotation
// matches turnID. When taskName is non-empty it does a direct Get (O(1))
// and verifies the annotation equality; this eliminates the full-namespace
// List+scan on the hot callback path (findings 4, 6). When taskName is empty
// (legacy wrappers that pre-date the taskName field) it falls back to the
// full-namespace List+scan. Tasks with an empty annCurrentTurn are skipped to
// prevent empty-to-empty matches.
func (s *CallbackServer) resolveTaskByTurnWithHint(ctx context.Context, turnID, taskName string) (*tatarav1alpha1.Task, error) {
	if taskName != "" {
		t := &tatarav1alpha1.Task{}
		if err := s.Client.Get(ctx, types.NamespacedName{Namespace: s.Namespace, Name: taskName}, t); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, errTurnNotFound
			}
			return nil, fmt.Errorf("get task by name: %w", err)
		}
		if t.Annotations[annCurrentTurn] != turnID {
			return nil, errTurnNotFound
		}
		return t, nil
	}
	return s.resolveTaskByTurn(ctx, turnID)
}

// resolveTaskByTurn finds the Task whose current-turn annotation matches turnID
// via a full-namespace List scan. Prefer resolveTaskByTurnWithHint when the
// caller knows the task name.
// Tasks with an empty annCurrentTurn are skipped to prevent empty-to-empty matches.
func (s *CallbackServer) resolveTaskByTurn(ctx context.Context, turnID string) (*tatarav1alpha1.Task, error) {
	var list tatarav1alpha1.TaskList
	if err := s.Client.List(ctx, &list, client.InNamespace(s.Namespace)); err != nil {
		return nil, fmt.Errorf("list tasks: %w", err)
	}
	for i := range list.Items {
		ann := list.Items[i].Annotations[annCurrentTurn]
		if ann == "" {
			continue
		}
		if ann == turnID {
			return &list.Items[i], nil
		}
	}
	return nil, errTurnNotFound
}

// PollOnce polls in-flight turns for delivered results that missed a callback.
// It is the backstop body; the ticker loop calls it. It also expires turns that
// have exceeded their deadline so a wedged turn does not requeue forever.
func (s *CallbackServer) PollOnce(ctx context.Context) {
	l := log.FromContext(ctx)
	var list tatarav1alpha1.TaskList
	if err := s.Client.List(ctx, &list, client.InNamespace(s.Namespace)); err != nil {
		l.Error(err, "poll backstop: list tasks failed; backstop disabled this cycle",
			"action", "poll_backstop_list_error")
		return
	}
	for i := range list.Items {
		task := &list.Items[i]
		if tatarav1alpha1.TaskDone(task) {
			continue
		}
		turn := task.Annotations[annCurrentTurn]
		if turn == "" {
			// No turn in flight: the F.4 clocks (admission / readiness / work)
			// own a Task that is not running one. There is no separate spawn
			// watchdog any more.
			continue
		}
		if task.Annotations[annTurnComplete] != "" {
			continue
		}

		// Check for turn timeout before hitting the wrapper.
		if s.isTurnTimedOut(ctx, task) {
			if s.Metrics != nil {
				s.Metrics.TurnTimeout("poll_backstop")
			}
			l.Info("turn timed out in poll backstop", "action", "turn_timeout",
				"task", task.Name, "turn_id", turn)
			_ = s.expireTimedOutTurn(ctx, task, turn)
			continue
		}

		if s.Session == nil {
			continue
		}
		// Bound each GetTurn call so a single slow/unreachable wrapper cannot
		// stall the entire backstop cycle (finding 4/r3).
		getTurnCtx, cancel := context.WithTimeout(ctx, pollGetTurnTimeout)
		tr, err := s.Session.GetTurn(getTurnCtx, agent.BaseURL(task, s.Namespace), turn)
		cancel()
		if err != nil {
			continue
		}
		// Refresh the last-activity annotation so the stall deadline (checked on
		// the next cycle) tracks the wrapper. The backstop owns this annotation;
		// the reconcile path only reads it.
		if !tr.LastActivityAt.IsZero() {
			s.refreshLastActivity(ctx, task.Name, task.Namespace, turn, tr.LastActivityAt.UTC().Format(time.RFC3339))
		}
		if tr.State == "complete" || tr.State == "failed" {
			_ = s.recordResult(ctx, tr, task, turn)
		}
	}
}

// refreshLastActivity stamps the turn-last-activity-at annotation on the task,
// best-effort. It is a no-op when the turn has advanced or the value is
// unchanged, so it adds no write when an idle wrapper reports the same timestamp.
func (s *CallbackServer) refreshLastActivity(ctx context.Context, taskName, namespace, turnID, lastActivity string) {
	_ = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := s.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: taskName}, fresh); err != nil {
			return err
		}
		if fresh.Annotations[annCurrentTurn] != turnID {
			return nil
		}
		if fresh.Annotations[annTurnLastActivity] == lastActivity {
			return nil
		}
		if fresh.Annotations == nil {
			fresh.Annotations = map[string]string{}
		}
		fresh.Annotations[annTurnLastActivity] = lastActivity
		return s.Client.Update(ctx, fresh)
	})
}

// turnTimedOut reports whether a turn has stalled: no agent activity for
// timeoutSeconds + turnTimeoutGrace. The deadline is anchored on the most recent
// of startedAtRaw and lastActivityRaw, so timeoutSeconds is a stall (inactivity)
// window rather than a fixed wall-clock cap: a turn that keeps streaming output
// is not killed mid-work, while a silent (hung) turn still fails on schedule.
// Returns false (safe default) when startedAtRaw is empty or unparseable; falls
// back to startedAtRaw alone when lastActivityRaw is empty or unparseable (e.g.
// the wrapper is unreachable) so the bound is never lost. This is a free function
// so both CallbackServer.isTurnTimedOut and TaskReconciler.isTurnTimedOut can
// call it without duplicating the deadline arithmetic (finding 3/r3).
func turnTimedOut(startedAtRaw, lastActivityRaw string, timeoutSeconds int) bool {
	if startedAtRaw == "" {
		return false
	}
	anchor, err := time.Parse(time.RFC3339, startedAtRaw)
	if err != nil {
		return false
	}
	if lastActivityRaw != "" {
		if la, laErr := time.Parse(time.RFC3339, lastActivityRaw); laErr == nil && la.After(anchor) {
			anchor = la
		}
	}
	if timeoutSeconds <= 0 {
		timeoutSeconds = 1800
	}
	deadline := anchor.Add(time.Duration(timeoutSeconds)*time.Second + turnTimeoutGrace)
	return time.Now().After(deadline)
}

// validHMACSignature checks that sig == "sha256=<hex(HMAC-SHA256(body, secret))>".
// Returns false for any malformed or mismatched signature.
func validHMACSignature(body []byte, sig, secret string) bool {
	const prefix = "sha256="
	if !strings.HasPrefix(sig, prefix) {
		return false
	}
	got, err := hex.DecodeString(sig[len(prefix):])
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	expected := mac.Sum(nil)
	return hmac.Equal(got, expected)
}

// isTurnTimedOut checks the turn against the project turnTimeoutSeconds + grace,
// anchored on max(turn-started-at, turn-last-activity-at) so the window is a
// stall (inactivity) deadline. Returns false when any lookup fails (safe default).
func (s *CallbackServer) isTurnTimedOut(ctx context.Context, task *tatarav1alpha1.Task) bool {
	var project tatarav1alpha1.Project
	if err := s.Client.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.ProjectRef}, &project); err != nil {
		return false
	}
	return turnTimedOut(task.Annotations[annTurnStartedAt], task.Annotations[annTurnLastActivity], project.Spec.Agent.TurnTimeoutSeconds)
}

// expireTimedOutTurn performs the cleanup for a stalled turn: it deletes the
// session + Pod/Service and clears the current-turn annotations so a late
// callback cannot resolve this Task.
//
// It DOES NOT terminate the Task. A stalled turn is a POD problem, not a Task
// outcome: tearing the pod down hands the Task back to the stage machine, which
// re-ensures a pod for the current stage (the F.4 WORK clock bounds the retry
// loop and parks at stage-deadline if the agent keeps hanging). The old machine
// failed the Task outright here, which killed a long healthy stage on one hung
// turn and had no re-entry.
func (s *CallbackServer) expireTimedOutTurn(ctx context.Context, task *tatarav1alpha1.Task, turn string) error {
	if s.Session != nil {
		_ = s.Session.DeleteSession(ctx, agent.BaseURL(task, s.Namespace))
	}
	// Delete Pod and Service best-effort (owner-references ensure GC too).
	p := &corev1.Pod{}
	p.Name = agent.PodName(task)
	p.Namespace = task.Namespace
	_ = s.Client.Delete(ctx, p)
	svc := &corev1.Service{}
	svc.Name = agent.PodName(task)
	svc.Namespace = task.Namespace
	_ = s.Client.Delete(ctx, svc)

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := s.Client.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, fresh); err != nil {
			return err
		}
		if tatarav1alpha1.TaskDone(fresh) {
			return nil
		}
		// Clear turn annotations so late/duplicate callbacks cannot resolve this
		// task and stamp annTurnComplete on the next stage's fresh turn.
		if fresh.Annotations != nil {
			delete(fresh.Annotations, annCurrentTurn)
			delete(fresh.Annotations, annTurnStartedAt)
			delete(fresh.Annotations, annTurnLastActivity)
			delete(fresh.Annotations, annTurnComplete)
		}
		return s.Client.Update(ctx, fresh)
	}); err != nil {
		return err
	}
	log.FromContext(ctx).Info("stalled turn expired; pod torn down for respawn",
		"action", "turn_timeout_expired", "resource_id", task.Name, "turn_id", turn)
	return nil
}

// Start runs the callback HTTP server (callback + push-metrics + health) until
// ctx is done. It serves on every replica (see maintenanceRunnable for the
// leader-only poll/reap loop). Implements manager.Runnable.
func (s *CallbackServer) Start(ctx context.Context, addr string) error {
	srv := &http.Server{Addr: addr, Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		// Use a bounded context to avoid blocking shutdown forever if an
		// in-flight handler is stuck (finding 7, mirrors webhook/server.go:823).
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("callback server: %w", err)
	}
	return nil
}

// RunMaintenance drives the periodic poll backstop and orphan reaper on a
// pollRequeue ticker until ctx is done. It is registered as a LEADER-ONLY
// manager runnable (maintenanceRunnable): only the elected leader polls for
// missed turn callbacks and reaps orphan pods, so N replicas no longer each
// run full-namespace Lists + deletes every cycle. Implements manager.Runnable.
func (s *CallbackServer) RunMaintenance(ctx context.Context) error {
	t := time.NewTicker(pollRequeue)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if s.Session != nil {
				s.PollOnce(ctx)
			}
			// Backstop the one-shot teardown: reap wrapper pods whose Task
			// is gone or terminal. Runs regardless of Session (orphans
			// outlive their session).
			s.ReapOrphans(ctx)
		}
	}
}
