// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/scm"
)

const babysitDefaultDeadlineMinutes = 60

// conversationDefaultIdleMinutes is the fallback conversation idle window when
// ConversationIdleMinutes is unset in the project config. Both enterConversation
// and the handleConversation nil-deadline safety net use this constant so they
// can never silently drift apart.
const conversationDefaultIdleMinutes = 60

// lifecyclePR returns the PR number and URL for a lifecycle task. When the task
// was opened via an issue (issue path), finishImplement writes PrNumber/PrURL;
// when entered directly from a bot PR (IsPR=true), the Source fields carry them.
func lifecyclePR(task *tatarav1alpha1.Task) (number int, url string) {
	if task.Status.PRNumber != 0 {
		return task.Status.PRNumber, task.Status.PrURL
	}
	if task.Spec.Source != nil && task.Spec.Source.IsPR {
		return task.Spec.Source.Number, task.Spec.Source.URL
	}
	return 0, ""
}

// deadlinePassed reports whether the task's DeadlineAt has been reached.
func deadlinePassed(task *tatarav1alpha1.Task) bool {
	if task.Status.DeadlineAt == nil {
		return false
	}
	return time.Now().After(task.Status.DeadlineAt.Time)
}

// ensureDeadlineMinutes sets DeadlineAt on first entry to a poll state when
// unset, using the provided idle window in minutes. Shared by ensureDeadline
// (babysit window) and handleConversation (idle window) so both use the same
// RetryOnConflict logic and can never silently drift apart (finding 15).
func (r *TaskReconciler) ensureDeadlineMinutes(ctx context.Context, task *tatarav1alpha1.Task, minutes int) error {
	if task.Status.DeadlineAt != nil {
		return nil
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		if fresh.Status.DeadlineAt != nil {
			task.Status.DeadlineAt = fresh.Status.DeadlineAt
			return nil
		}
		dl := metav1.NewTime(time.Now().Add(time.Duration(minutes) * time.Minute))
		fresh.Status.DeadlineAt = &dl
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		task.Status.DeadlineAt = fresh.Status.DeadlineAt
		return nil
	})
}

// ensureDeadline sets DeadlineAt on first entry to a poll state when unset.
func (r *TaskReconciler) ensureDeadline(ctx context.Context, task *tatarav1alpha1.Task, project *tatarav1alpha1.Project) error {
	minutes := babysitDefaultDeadlineMinutes
	if project.Spec.Scm != nil && project.Spec.Scm.BabysitDeadlineMinutes > 0 {
		minutes = project.Spec.Scm.BabysitDeadlineMinutes
	}
	return r.ensureDeadlineMinutes(ctx, task, minutes)
}

// clearDeadline clears DeadlineAt on transition out of a poll state.
func (r *TaskReconciler) clearDeadline(ctx context.Context, task *tatarav1alpha1.Task) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		if fresh.Status.DeadlineAt == nil {
			return nil
		}
		fresh.Status.DeadlineAt = nil
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		task.Status.DeadlineAt = nil
		return nil
	})
}

// setDeadlineMinutes unconditionally sets DeadlineAt to now+minutes in a single
// RetryOnConflict. Use it whenever a deadline needs to be reset (overwritten),
// e.g. the interjection-extend path in handleConversation. This replaces the
// clearDeadline+ensureDeadlineMinutes two-write pattern with one write (finding 3/r3).
func (r *TaskReconciler) setDeadlineMinutes(ctx context.Context, task *tatarav1alpha1.Task, minutes int) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		dl := metav1.NewTime(time.Now().Add(time.Duration(minutes) * time.Minute))
		fresh.Status.DeadlineAt = &dl
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		task.Status.DeadlineAt = fresh.Status.DeadlineAt
		return nil
	})
}

// parkWithComment posts a comment on the PR/issue and transitions to Parked.
// For issue-linked tasks it comments on the issue (IssueRef). For bot-PR-entry
// tasks with no issue ref, it falls back to the PR ref derived from lifecyclePR.
// When task.Spec.Source is nil (board/cron tasks) the comment is skipped and
// the park is applied silently - this is logged at INFO (finding 10).
func (r *TaskReconciler) parkWithComment(ctx context.Context, task *tatarav1alpha1.Task, writer scm.SCMWriter, token, reason, msg string) error {
	l := log.FromContext(ctx)
	if task.Spec.Source == nil {
		l.Info("lifecycle: parking without comment (no source ref)",
			"action", "lifecycle_park_no_source",
			"resource_id", task.Name,
			"reason", reason)
	}
	if task.Spec.Source != nil {
		provider := task.Spec.Source.Provider
		// Fallback: board/cron-sourced tasks may have empty Source.Provider; resolve
		// from the project so the SCM metric label is never empty (finding 9).
		if provider == "" {
			var proj tatarav1alpha1.Project
			if gerr := r.Get(ctx, client.ObjectKey{Namespace: task.Namespace, Name: task.Spec.ProjectRef}, &proj); gerr == nil {
				if proj.Spec.Scm != nil {
					provider = proj.Spec.Scm.Provider
				}
			}
		}
		commentRef := task.Spec.Source.IssueRef
		// For bot-PR-entry tasks the binder sets IssueRef to "owner/repo#N" (the PR
		// ref). In the rare case it is empty, build a proper sigil ref so the SCM
		// driver can route to the correct endpoint.  GitLab's Comment() requires
		// "group/proj!iid" (MR) or "group/proj#iid" (issue); a bare web URL has no
		// sigil and causes glBangRef/glHashRef to return "malformed ref", silently
		// losing the park comment.  Match the pattern used by writeBackReview and
		// createScanTask throughout the controller.
		if commentRef == "" && task.Spec.Source.IsPR {
			number, _ := lifecyclePR(task)
			if number > 0 {
				repoURL := r.repoURLForTask(ctx, task)
				if slug, _, serr := repoSlugFromURL(repoURL, provider); serr == nil && slug != "" {
					sep := "#"
					if provider == "gitlab" {
						sep = "!"
					}
					commentRef = fmt.Sprintf("%s%s%d", slug, sep, number)
				}
			}
			// If slug derivation failed or number is unknown, fall back to the PR web
			// URL. This is still wrong for GitLab (will be non-fatal logged), but it
			// avoids silencing the comment on every provider.
			if commentRef == "" {
				if _, prURL := lifecyclePR(task); prURL != "" {
					commentRef = prURL
				} else {
					commentRef = task.Spec.Source.URL
				}
			}
		}
		if commentRef != "" {
			cerr := writer.Comment(ctx, token, commentRef, msg)
			r.recordSCM(provider, "comment", cerr)
			if cerr != nil {
				l.Error(cerr, "lifecycle: park comment (non-fatal)", "resource_id", task.Name)
			}
		}
	}
	return r.setLifecycleState(ctx, task, "Parked", reason)
}

// parkOnDeadline posts msg as a park comment and records the deadline giveup metric.
// Used by MRCI, Merge, and MainCI, which share the identical
// ensureDeadline -> deadlinePassed -> park + RecordGiveup("deadline") sequence
// (finding 13). Returns an error if parkWithComment fails.
func (r *TaskReconciler) parkOnDeadline(ctx context.Context, task *tatarav1alpha1.Task, writer scm.SCMWriter, token, msg string) error {
	if err := r.parkWithComment(ctx, task, writer, token, "deadline", msg); err != nil {
		return err
	}
	if r.LifecycleMetrics != nil {
		r.LifecycleMetrics.RecordGiveup("deadline")
	}
	return nil
}

// deleteWrapper best-effort deletes the wrapper Pod and Service for a task.
// Idempotent: a missing object is not an error. Used by terminate (terminal
// phase), resetAgentRun (re-spawn), and setLifecycleState (terminal lifecycle).
// Thin receiver-bound wrapper over the shared agent.DeleteWrapper so the
// webhook server (different receiver type) can reuse the same teardown.
func (r *TaskReconciler) deleteWrapper(ctx context.Context, task *tatarav1alpha1.Task) error {
	return agent.DeleteWrapper(ctx, r.Client, task.Namespace, task)
}

// setLifecycleState updates task.Status.LifecycleState to `to`, retrying on
// conflict (same pattern as clearWritebackPending). It logs the transition at
// INFO and increments tatara_lifecycle_transition_total{from,to}. On a
// transition into a terminal lifecycle state (Done/Stopped/Parked) it also
// tears down the wrapper Pod+Service so idle agent sessions do not accumulate.
// terminalOutcome classifies a Task's terminal lifecycle transition into
// delivered/churned/abandoned for operator_task_terminal_tokens_total.
// Done is always delivered. Parked is churned when the task gave up and may
// be re-rolled (a recoverable-giveup reason, or a nonzero ImplementGiveUps
// count from a PRIOR give-up on this task's durable lifecycle history even if
// the current reason is not itself recoverable), else abandoned (a
// deliberate decline/duplicate/etc. with no delivery). Stopped and any other
// terminal state are abandoned.
func terminalOutcome(to, reason string, implementGiveUps int) string {
	switch to {
	case "Done":
		return "delivered"
	case "Parked":
		if implementGiveUps > 0 || tatarav1alpha1.IsRecoverableGiveup(reason) {
			return "churned"
		}
		return "abandoned"
	default: // "Stopped" and any other terminal
		return "abandoned"
	}
}

func (r *TaskReconciler) setLifecycleState(ctx context.Context, task *tatarav1alpha1.Task, to, reason string) error {
	l := log.FromContext(ctx)
	// `from` is always overwritten inside the closure (finding 13: the outer
	// task.Status.LifecycleState initializer was dead code since RetryOnConflict
	// always runs the closure at least once and sets `from = fresh.Status...`).
	var from string
	// Captured from the fresh Task inside the closure for the terminal-tokens
	// emission below (reading these, not task.Status, avoids a stale in-memory
	// snapshot - see the emission comment further down).
	var cumIn, cumOut, cumCR, cumCC int64
	var stampedModel string
	var giveUps int

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		from = fresh.Status.LifecycleState
		fresh.Status.LifecycleState = to
		if from == "Implement" && to == "Parked" && tatarav1alpha1.IsRecoverableGiveup(reason) {
			fresh.Status.ImplementGiveUps++
		}
		// Persist the park reason on every Parked transition; clear it otherwise
		// so stale reasons do not linger after re-activation.
		if to == "Parked" {
			fresh.Status.ParkReason = reason
		} else {
			fresh.Status.ParkReason = ""
		}
		// On every Implement entry, reset the empty-run retry budget so each
		// attempt gets a clean counter. ImplementOutcome is only cleared on FRESH
		// triage-initiated entries; CI-failure/merge-conflict re-entries leave a
		// prior ImplementOutcome intact so a stale refusal from a failed
		// clearImplementOutcome is surfaced rather than silently discarded on the
		// next reconcile (finding 8). Human revival (triage-implement) resets both.
		if to == "Implement" {
			fresh.Status.ImplementEmptyRetries = 0
			// Only a fresh triage-implement entry clears ImplementOutcome.
			// CI-failure and merge-conflict re-entries should not discard a stale
			// refusal that clearImplementOutcome may have failed to remove.
			if reason == "triage-implement" || reason == "initial" {
				fresh.Status.ImplementOutcome = nil
			}
		}
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		cumIn, cumOut, cumCR, cumCC = fresh.Status.CumulativeInput, fresh.Status.CumulativeOutput, fresh.Status.CumulativeCacheRead, fresh.Status.CumulativeCacheCreation
		stampedModel = fresh.Status.ResolvedModel
		giveUps = fresh.Status.ImplementGiveUps
		return nil
	}); err != nil {
		return fmt.Errorf("setLifecycleState: %w", err)
	}

	// Emit the task's whole cumulative spend to operator_task_terminal_tokens_total
	// once, on the transition INTO a terminal lifecycle state. Reading the
	// closure-captured cumulatives (not task.Status) avoids a stale in-memory
	// snapshot; guarding on !isLifecycleTerminal(from) prevents a double-emit
	// when setLifecycleState is re-called with an already-terminal `from`.
	if r.Metrics != nil && !isLifecycleTerminal(from) && isLifecycleTerminal(to) {
		r.Metrics.AddTerminalTokens(task.Spec.ProjectRef, task.Spec.RepositoryRef,
			terminalOutcome(to, reason, giveUps), stampedModel,
			cumIn, cumOut, cumCR, cumCC)
	}

	// Clear the boot-crash annotations (attempts budget + captured diagnostics) on
	// any lifecycle-state transition. The budget must accumulate across respawns
	// WITHIN a single lifecycle state (handled by bumpBootCrashAttempts /
	// handleBootCrash), but must NOT carry over into the next state. A fresh
	// Implement/Triage/etc. entry that boot-crashes before any turn starts must get
	// its own maxPodRecreations budget, not an already-spent one, and must not
	// inherit a stale crash cause. recordTurn also clears both (on a successful turn
	// landing), so the within-state respawn path is unaffected.
	// Fast-path: skip the retry loop entirely when both annotations are absent on
	// the in-memory task (the common case). If a concurrent write added one between
	// the status update above and here, it carries over by one reconcile, which is
	// benign (it resets on the NEXT transition). This avoids an extra Get+retry on
	// every transition (finding 18).
	_, hasAttempts := task.Annotations[annBootCrashAttempts]
	_, hasDiag := task.Annotations[annBootCrashDiagnostics]
	if hasAttempts || hasDiag {
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			fresh2 := &tatarav1alpha1.Task{}
			if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh2); err != nil {
				return err
			}
			if fresh2.Annotations == nil {
				return nil
			}
			_, a := fresh2.Annotations[annBootCrashAttempts]
			_, d := fresh2.Annotations[annBootCrashDiagnostics]
			if !a && !d {
				return nil
			}
			delete(fresh2.Annotations, annBootCrashAttempts)
			delete(fresh2.Annotations, annBootCrashDiagnostics)
			delete(fresh2.Annotations, annBootCrashLastPodUID)
			return r.Update(ctx, fresh2)
		}); err != nil {
			// Non-fatal: log and continue. The state transition itself already succeeded.
			log.FromContext(ctx).Error(err, "setLifecycleState: clear boot-crash annotations (non-fatal)",
				"resource_id", task.Name, "to", to)
		}
	}

	l.Info("lifecycle transition",
		"action", "lifecycle_transition",
		"resource_id", task.Name,
		"from", from,
		"to", to,
		"reason", reason,
	)

	if r.LifecycleMetrics != nil {
		r.LifecycleMetrics.RecordTransition(from, to)
		// The tatara_lifecycle_state gauge is NOT maintained by deltas here: it is
		// recomputed from authoritative cluster state by
		// ProjectReconciler.updateLifecycleStateCounts. Delta maintenance drifted on
		// restart (from-series went negative) and never decremented GC'd terminal
		// Tasks; the periodic list-and-Set is the single source of truth.
	}

	task.Status.LifecycleState = to

	// Terminal lifecycle states have no further agent run: tear down the wrapper
	// Pod+Service so idle sessions do not leak CPU/mem + a work PVC. Best-effort;
	// a failure here must not block the (already-applied) state transition.
	if isLifecycleTerminal(to) {
		if err := r.deleteWrapper(ctx, task); err != nil {
			l.Error(err, "lifecycle: delete wrapper on terminal transition (non-fatal)",
				"resource_id", task.Name, "to", to)
		}
	}

	return nil
}

// resetAgentRun clears the agent-run state on the Task so the next lifecycle
// state can spawn a fresh session. It:
//   - sets Status.Phase = ""
//   - deletes the turn annotations (current-turn, current-subtask, turn-complete, turn-started-at, pod-recreations)
//   - removes the WritebackPending condition (sets it False)
//   - deletes the wrapper Pod + Service (belt-and-suspenders; terminate already does this on success)
func (r *TaskReconciler) resetAgentRun(ctx context.Context, task *tatarav1alpha1.Task) error {
	// Delete wrapper pod + service (best-effort; may already be gone from terminate).
	if err := r.deleteWrapper(ctx, task); err != nil {
		return fmt.Errorf("resetAgentRun: %w", err)
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		fresh.Status.Phase = ""
		// A fresh agent run starts with a fresh writeback-skip budget: clear the
		// issue-166 4xx-skip attempt counter so a re-entered Implement (e.g. after a
		// Conversation nudge once the offending repo URL/token is fixed) is not
		// wrongly capped by stale attempts. For one-shot non-lifecycle tasks this is
		// never reached, so their loop cap still persists across re-entry.
		fresh.Status.WritebackSkip4xxAttempts = 0
		// Clear WritebackPending.
		apimeta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
			Type:               "WritebackPending",
			Status:             metav1.ConditionFalse,
			Reason:             "LifecycleReset",
			Message:            "agent run reset for next lifecycle state",
			ObservedGeneration: fresh.Generation,
		})
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		// Clear turn annotations (requires a metadata update, separate from status).
		fresh2 := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh2); err != nil {
			return err
		}
		if fresh2.Annotations != nil {
			delete(fresh2.Annotations, annCurrentTurn)
			delete(fresh2.Annotations, annCurrentSubtask)
			delete(fresh2.Annotations, annTurnComplete)
			delete(fresh2.Annotations, annTurnStartedAt)
			delete(fresh2.Annotations, annTurnLastActivity)
			delete(fresh2.Annotations, annPodRecreations)
			delete(fresh2.Annotations, annAgentUnreachableSince)
		}
		task.Status.Phase = ""
		return r.Update(ctx, fresh2)
	})
}

// needsSpawn reports whether the lifecycle state requires starting a new agent
// run. Only these states need the memory-ready gate.
func needsSpawn(lifecycleState, phase string) bool {
	switch lifecycleState {
	case "", "Triage", "Implement":
		// Gate only when the run has NOT yet finished (no terminal phase).
		return !isTerminal(phase)
	}
	return false
}

// ensurePhaseLabel sets the desired managed phase label on the task's source
// issue (no-op for PR sources or missing source). phase is one of
// "brainstorming"|"approved"|"implementation"|"declined"; it resolves the
// configured label name and delegates to setLifecycleLabel (idempotent).
func (r *TaskReconciler) ensurePhaseLabel(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task, phase string) error {
	if task.Spec.Source == nil || task.Spec.Source.IsPR || task.Spec.Source.IssueRef == "" {
		return nil
	}
	brainstorming, approved, implementation, declined := lifecycleLabels(project.Spec.Scm)
	var desired string
	switch phase {
	case "brainstorming":
		desired = brainstorming
	case "approved":
		desired = approved
	case "implementation":
		desired = implementation
	case "declined":
		desired = declined
	default:
		return nil
	}
	return r.setLifecycleLabel(ctx, project, task, desired)
}

// reconcileLifecycle is the dispatch function for issueLifecycle Tasks. It
// applies the memory-ready gate ONLY on the spawn path (i.e. when about to
// start a new agent run). Terminal-phase outcome consumption, poll states,
// and terminal lifecycle states bypass the gate so a finished run can always
// be torn down and its outcome consumed.
func (r *TaskReconciler) reconcileLifecycle(ctx context.Context, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var project tatarav1alpha1.Project
	if err := r.Get(ctx, client.ObjectKey{Namespace: task.Namespace, Name: task.Spec.ProjectRef}, &project); err != nil {
		r.Metrics.ReconcileResult("Task", "error")
		return ctrl.Result{}, fmt.Errorf("lifecycle: get project: %w", err)
	}

	// Drain agent-queued free-form comments (from the comment MCP tool) to the
	// linked issue before anything else, then clear and requeue. Comments are
	// posted in order; each posted comment is dequeued under RetryOnConflict
	// (preserving any concurrently-appended comment) BEFORE returning, so a
	// post failure can never re-post an already-delivered comment.
	if pending := task.Status.PendingComments; len(pending) > 0 && task.Spec.Source != nil && task.Spec.Source.IssueRef != "" {
		_, _, writer, token, provider, err := r.scmContext(ctx, task)
		if err != nil {
			r.Metrics.ReconcileResult("Task", "error")
			return ctrl.Result{}, fmt.Errorf("lifecycle drain comments: %w", err)
		}
		posted := 0
		var postErr error
		for _, c := range pending {
			commentStart := time.Now()
			cerr := writer.Comment(ctx, token, task.Spec.Source.IssueRef, c)
			r.recordSCM(provider, "comment", cerr)
			if cerr != nil {
				// Log the per-comment failure with how many were already posted so
				// operators can diagnose a stuck comment queue without metric inference
				// (finding 15). interjection drain logs on failure; this now matches.
				l.Error(cerr, "lifecycle: agent comment post failed",
					"action", "scm_agent_comment_error",
					"resource_id", task.Name,
					"posted", posted)
				postErr = cerr
				break
			}
			posted++
			l.Info("lifecycle: agent comment posted",
				"action", "scm_agent_comment",
				"resource_id", task.Name,
				"duration_ms", time.Since(commentStart).Milliseconds())
		}
		if posted > 0 {
			if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				var fresh tatarav1alpha1.Task
				if gerr := r.Get(ctx, client.ObjectKeyFromObject(task), &fresh); gerr != nil {
					return gerr
				}
				if len(fresh.Status.PendingComments) >= posted {
					fresh.Status.PendingComments = fresh.Status.PendingComments[posted:]
				} else {
					fresh.Status.PendingComments = nil
				}
				return r.Status().Update(ctx, &fresh)
			}); err != nil {
				r.Metrics.ReconcileResult("Task", "error")
				return ctrl.Result{}, fmt.Errorf("lifecycle clear comments: %w", err)
			}
		}
		if postErr != nil {
			r.Metrics.ReconcileResult("Task", "error")
			return ctrl.Result{}, fmt.Errorf("lifecycle drain comment: %w", postErr)
		}
		// Record success metric before returning; finding 2: drain success was invisible
		// to the reconcile success counter. Use RequeueAfter to avoid busy-loop when
		// comments are continuously appended (finding 18).
		r.Metrics.ReconcileResult("Task", "success")
		return ctrl.Result{RequeueAfter: pollRequeue}, nil
	}

	// Drain webhook-queued interjections into the live wrapper session: new
	// issue/MR comments that arrived while a turn was in flight must reach the
	// running agent as mid-session input (issue #25). Delivered in order, then
	// cleared under RetryOnConflict so a concurrently-appended interjection is
	// preserved. Skipped when r.Session is nil (tests without a session).
	if len(task.Status.PendingInterjections) > 0 && r.Session != nil {
		// Stale if no turn is in flight: the next turn/triage re-reads the thread,
		// so drop the queue rather than injecting into a session with nothing running.
		if !taskHasInflightTurn(task) {
			if err := r.clearPendingInterjections(ctx, task, len(task.Status.PendingInterjections)); err != nil {
				r.Metrics.ReconcileResult("Task", "error")
				return ctrl.Result{}, fmt.Errorf("lifecycle drop stale interjections: %w", err)
			}
			l.Info("lifecycle: dropped stale interjections (no in-flight turn)",
				"action", "interject_stale", "resource_id", task.Name)
			// Record success before returning (finding 2). RequeueAfter avoids busy-loop
			// when items are continuously appended (finding 18).
			r.Metrics.ReconcileResult("Task", "success")
			return ctrl.Result{RequeueAfter: pollRequeue}, nil
		}
		baseURL := agent.BaseURL(task, task.Namespace)
		total := len(task.Status.PendingInterjections)
		delivered := 0
		var deliverErr error
		for _, text := range task.Status.PendingInterjections {
			ierr := r.Session.Interject(ctx, baseURL, text)
			if ierr != nil {
				var unreachable *agent.UnreachableError
				if errors.As(ierr, &unreachable) {
					// Pod/turn server still booting: keep the queue intact, retry soon.
					l.Info("lifecycle: interject unreachable; keeping queue",
						"action", "interject_retry", "resource_id", task.Name)
					return ctrl.Result{RequeueAfter: pollRequeue}, nil
				}
				deliverErr = ierr
				l.Error(ierr, "lifecycle: interject failed", "resource_id", task.Name)
				break
			}
			delivered++
			l.Info("lifecycle: interjection delivered",
				"action", "interject", "resource_id", task.Name)
		}
		if delivered > 0 {
			if err := r.clearPendingInterjections(ctx, task, delivered); err != nil {
				r.Metrics.ReconcileResult("Task", "error")
				return ctrl.Result{}, fmt.Errorf("lifecycle clear interjections: %w", err)
			}
		}
		if deliverErr != nil || delivered < total {
			// Interjections remain after a delivery error: back off, retry later.
			return ctrl.Result{RequeueAfter: pollRequeue}, nil
		}
		// Record success before returning (finding 2). RequeueAfter avoids busy-loop
		// when items are continuously appended (finding 18).
		r.Metrics.ReconcileResult("Task", "success")
		return ctrl.Result{RequeueAfter: pollRequeue}, nil
	}

	// Memory gate: apply only when about to spawn a new agent run.
	if needsSpawn(task.Status.LifecycleState, task.Status.Phase) {
		if project.Status.Memory == nil || project.Status.Memory.Phase != "Ready" {
			l.Info("lifecycle task gated: project memory not ready",
				"action", "task_memory_gate", "resource_id", task.Name, "project", project.Name)
			return ctrl.Result{RequeueAfter: memGateRequeue}, nil
		}
	}

	// dispatchLifecycle runs the per-state handler and stamps the reconcile metric
	// exactly once, collapsing 6 identical error/success wrapping blocks (finding 14).
	dispatchLifecycle := func(h func() (ctrl.Result, error)) (ctrl.Result, error) {
		res, err := h()
		if err != nil {
			r.Metrics.ReconcileResult("Task", "error")
			return ctrl.Result{}, err
		}
		r.Metrics.ReconcileResult("Task", "success")
		return res, nil
	}

	switch task.Status.LifecycleState {
	case "":
		// First reconcile: initialize from the lifecycle-entry annotation set at
		// create time by the binder/mrScan; default to Triage when absent.
		entry := task.Annotations[tatarav1alpha1.LifecycleEntryAnnotation]
		if entry == "" {
			entry = "Triage"
		}
		if err := r.setLifecycleState(ctx, task, entry, "initial"); err != nil {
			r.Metrics.ReconcileResult("Task", "error")
			return ctrl.Result{}, err
		}
		if entry == "Triage" {
			if err := r.ensurePhaseLabel(ctx, &project, task, "brainstorming"); err != nil {
				r.Metrics.ReconcileResult("Task", "error")
				return ctrl.Result{}, err
			}
		}
		r.Metrics.ReconcileResult("Task", "success")
		return ctrl.Result{RequeueAfter: pollRequeue}, nil
	case "Triage":
		return dispatchLifecycle(func() (ctrl.Result, error) { return r.handleTriage(ctx, &project, task) })
	case "Implement":
		return dispatchLifecycle(func() (ctrl.Result, error) { return r.handleImplement(ctx, &project, task) })
	case "Conversation":
		return dispatchLifecycle(func() (ctrl.Result, error) { return r.handleConversation(ctx, &project, task) })
	case "MRCI":
		return dispatchLifecycle(func() (ctrl.Result, error) { return r.handleMRCI(ctx, &project, task) })
	case "Merge":
		return dispatchLifecycle(func() (ctrl.Result, error) { return r.handleMerge(ctx, &project, task) })
	case "MainCI":
		return dispatchLifecycle(func() (ctrl.Result, error) { return r.handleMainCI(ctx, &project, task) })
	case tatarav1alpha1.LifecycleStateDeploying:
		return dispatchLifecycle(func() (ctrl.Result, error) { return r.reconcileDeploying(ctx, &project, task) })
	case "Done", "Stopped", "Parked":
		r.Metrics.ReconcileResult("Task", "success")
		return ctrl.Result{}, nil
	default:
		r.Metrics.ReconcileResult("Task", "error")
		return ctrl.Result{}, fmt.Errorf("lifecycle: unknown lifecycleState %q for task %s", task.Status.LifecycleState, task.Name)
	}
}

// repoURLForTask fetches the Repository URL for the task's RepositoryRef.
// Returns "" on error (caller falls back gracefully).
func (r *TaskReconciler) repoURLForTask(ctx context.Context, task *tatarav1alpha1.Task) string {
	var repo tatarav1alpha1.Repository
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.RepositoryRef}, &repo); err != nil {
		return ""
	}
	return repo.Spec.URL
}

// clearPendingInterjections removes the first n delivered interjections from
// Status.PendingInterjections under RetryOnConflict, preserving any
// concurrently-appended interjection (same trim pattern as PendingComments).
//
// INVARIANT: PendingInterjections is append-only. Items are only ever added at
// the tail by the webhook path (AppendInterjection). The positional [n:] trim
// is correct ONLY under this invariant: a prepend or reorder would drop the
// wrong entries. If this invariant is ever relaxed, switch to match-and-remove
// by value instead of positional slicing (finding 12).
func (r *TaskReconciler) clearPendingInterjections(ctx context.Context, task *tatarav1alpha1.Task, n int) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var fresh tatarav1alpha1.Task
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), &fresh); err != nil {
			return err
		}
		if len(fresh.Status.PendingInterjections) >= n {
			fresh.Status.PendingInterjections = fresh.Status.PendingInterjections[n:]
		} else {
			fresh.Status.PendingInterjections = nil
		}
		return r.Status().Update(ctx, &fresh)
	})
}

// clearIssueOutcome nils Status.IssueOutcome (RetryOnConflict). Called only
// after the triage action arm has committed its state transition, so a mid-arm
// error retries the same outcome rather than defaulting to implement.
func (r *TaskReconciler) clearIssueOutcome(ctx context.Context, task *tatarav1alpha1.Task) error {
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		fresh.Status.IssueOutcome = nil
		return r.Status().Update(ctx, fresh)
	}); err != nil {
		return fmt.Errorf("triage: clear IssueOutcome: %w", err)
	}
	task.Status.IssueOutcome = nil
	return nil
}

// clearImplementOutcome nils Status.ImplementOutcome (RetryOnConflict). Called
// after the refusal arm has committed its state transition.
func (r *TaskReconciler) clearImplementOutcome(ctx context.Context, task *tatarav1alpha1.Task) error {
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		fresh.Status.ImplementOutcome = nil
		return r.Status().Update(ctx, fresh)
	}); err != nil {
		return fmt.Errorf("implement: clear ImplementOutcome: %w", err)
	}
	task.Status.ImplementOutcome = nil
	return nil
}

// enterConversation sets the conversation idle deadline + LastActivityAt and
// transitions the task to Conversation with the given reason. Shared by the
// discuss, close-withheld, and bot-await-approval triage outcomes.
// NOTE: DeadlineAt is always OVERWRITTEN (not set-if-unset). This is intentional:
// every path that calls enterConversation intends a fresh idle window. This
// differs from ensureDeadlineMinutes (which no-ops when DeadlineAt is set).
// Do not confuse the two (finding 9).
func (r *TaskReconciler) enterConversation(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task, reason string) error {
	idleMinutes := conversationDefaultIdleMinutes
	if project.Spec.Scm != nil && project.Spec.Scm.ConversationIdleMinutes > 0 {
		idleMinutes = project.Spec.Scm.ConversationIdleMinutes
	}
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		now := metav1.Now()
		deadline := metav1.NewTime(now.Add(time.Duration(idleMinutes) * time.Minute))
		fresh.Status.DeadlineAt = &deadline
		fresh.Status.LastActivityAt = &now
		return r.Status().Update(ctx, fresh)
	}); err != nil {
		return fmt.Errorf("enter conversation: set deadline: %w", err)
	}
	return r.setLifecycleState(ctx, task, "Conversation", reason)
}

// hasUnmergedChange reports whether the task produced a code artifact (a pushed
// branch or an opened PR/MR). An issue with an unmerged change must NOT be
// closed by an agent-driven outcome; only handleMainCI (merge recorded + main
// CI green) may close such an issue.
func hasUnmergedChange(task *tatarav1alpha1.Task) bool {
	return task.Status.PrURL != "" || task.Status.HeadBranch != ""
}

// triageCloseIssue calls CloseIssue for the task's source issue. It refuses to
// close when an unmerged change exists (defence-in-depth for the invariant that
// only a merged-and-green lifecycle may close an issue).
func (r *TaskReconciler) triageCloseIssue(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task, comment string) error {
	if task.Spec.Source == nil {
		return nil
	}
	if hasUnmergedChange(task) {
		log.FromContext(ctx).Info("triage close withheld: issue has an unmerged change",
			"action", "scm_close_withheld", "resource_id", task.Name,
			"number", task.Spec.Source.Number, "pr_url", task.Status.PrURL, "head_branch", task.Status.HeadBranch)
		return nil
	}
	_, repo, writer, token, provider, err := r.scmContext(ctx, task)
	if err != nil {
		return fmt.Errorf("triage close: %w", err)
	}
	repoSlug, _, perr := repoSlugFromURL(repo.Spec.URL, provider)
	if perr != nil {
		return perr
	}
	closeStart := time.Now()
	if cerr := writer.CloseIssue(ctx, token, repoSlug, task.Spec.Source.Number, comment); cerr != nil {
		// issue #268: the source issue is permanently gone (410 deleted / 404 not
		// found). Requeuing the close can never succeed, so classify it terminal -
		// record a distinct result="gone" (not "error", which inflated the SCM
		// write-failure-ratio alert), log it, and return nil so controller-runtime
		// does not retry-loop the doomed close forever. Mirrors the AddLabel guard
		// added under #263 (internal/controller/labels.go).
		if isPermanentTargetGone(cerr) {
			r.recordSCMGone(provider, "close_issue", cerr)
			log.FromContext(ctx).Info("triage close: target issue permanently gone; skipping close without requeue",
				"action", "scm_close_issue_target_gone", "resource_id", task.Name,
				"number", task.Spec.Source.Number, "status", scm.ErrorStatus(cerr))
			return nil
		}
		r.recordSCM(provider, "close_issue", cerr)
		return fmt.Errorf("triage close issue: %w", cerr)
	}
	r.recordSCM(provider, "close_issue", nil)
	log.FromContext(ctx).Info("lifecycle triage: issue closed",
		"action", "scm_issue_outcome",
		"resource_id", task.Name,
		"number", task.Spec.Source.Number,
		"duration_ms", time.Since(closeStart).Milliseconds())
	return nil
}

// triagePostComment posts the discuss comment to the source issue.
func (r *TaskReconciler) triagePostComment(ctx context.Context, _ *tatarav1alpha1.Project, task *tatarav1alpha1.Task, comment string) error {
	if task.Spec.Source == nil {
		return nil
	}
	// A blank/whitespace-only body is rejected by the SCM ("422 Body cannot be
	// blank") and would loop the reconcile forever before enterConversation runs.
	// This happens when the agent never calls issue_outcome (nil outcome defaults
	// to discuss with an empty comment) or returns a whitespace-only comment.
	// Nothing to say -> skip the post and let the discuss arm proceed.
	if strings.TrimSpace(comment) == "" {
		log.FromContext(ctx).Info("lifecycle triage: blank discuss comment; skipping post",
			"action", "scm_issue_discuss_skipped_blank", "resource_id", task.Name)
		return nil
	}
	_, _, writer, token, provider, err := r.scmContext(ctx, task)
	if err != nil {
		return fmt.Errorf("triage discuss: %w", err)
	}
	commentStart := time.Now()
	cerr := writer.Comment(ctx, token, task.Spec.Source.IssueRef, comment)
	r.recordSCM(provider, "comment", cerr)
	if cerr != nil {
		return fmt.Errorf("triage discuss comment: %w", cerr)
	}
	log.FromContext(ctx).Info("lifecycle triage: discuss comment posted",
		"action", "scm_issue_discuss",
		"resource_id", task.Name,
		"duration_ms", time.Since(commentStart).Milliseconds())
	return nil
}

// setImplementContext persists ImplementContext on the task via RetryOnConflict.
func (r *TaskReconciler) setImplementContext(ctx context.Context, task *tatarav1alpha1.Task, msg string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		fresh.Status.ImplementContext = msg
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		task.Status.ImplementContext = msg
		return nil
	})
}

// clearMergedChangeState resets the per-MR write-back fields (MergeCommitSHA,
// PrURL, PRNumber) when the lifecycle re-enters Implement AFTER a PR was already
// merged - i.e. only from the MainCI failure route. The merged PR is closed, so
// the fix must land as a brand new MR; leaving these fields set traps the task in
// a non-converging loop:
//   - PrURL set -> writeBackOpenChange short-circuits ("AlreadyWritten") and never
//     opens the new MR, so finishImplement re-enters MRCI against the stale PR.
//   - MergeCommitSHA set -> handleMerge's "already-merged" idempotency guard skips
//     the merge and bounces straight to MainCI, re-checking the stale failing SHA.
//
// Clearing them lets the next Implement->MRCI->Merge cycle open and merge a real
// new change. NOT called on the MRCI-failure or Merge-conflict routes: there the
// PR is still open (MergeCommitSHA is unset) and the fix is pushed to the same PR.
func (r *TaskReconciler) clearMergedChangeState(ctx context.Context, task *tatarav1alpha1.Task) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		fresh.Status.MergeCommitSHA = ""
		fresh.Status.PrURL = ""
		fresh.Status.PRNumber = 0
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		task.Status.MergeCommitSHA = ""
		task.Status.PrURL = ""
		task.Status.PRNumber = 0
		return nil
	})
}

// maybeMarkHandoverResume checks whether the last implement run consumed enough
// context to warrant a handover on the NEXT fresh run. When the threshold is
// reached it:
//   - ensures Status.Handover is set (uses the agent-submitted doc if present,
//     otherwise builds one from ResultSummary + ImplementContext + head branch)
//   - stamps tatara.dev/pending-handover-resume=true on the Task annotations
//   - calls LifecycleMetrics.RecordHandover()
//
// Called after EACH failure->Implement transition (MRCI failure, Merge 405,
// MainCI failure). A ContextWindowTokens<=0 is treated as "no guard" to avoid
// div-by-zero.
func (r *TaskReconciler) maybeMarkHandoverResume(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) error {
	ctxWin := project.Spec.Agent.ContextWindowTokens
	if ctxWin <= 0 {
		return nil
	}
	threshold := project.Spec.Agent.HandoverThresholdPercent
	// <=0 is treated as "unset" and defaults to 25 (issue #114 decision 2: past
	// 25% of the context window, compact instead of full resume). Mirrors the
	// CRD default; the in-code fallback covers objects created before the default
	// (e.g. envtest direct creation). A deliberately-configured 0 ("always
	// handover") cannot be expressed; use 1 for near-always behaviour. Integer
	// division truncates toward zero, so the threshold is effectively raised by
	// <1% (e.g. 24.9% reads as 24 < 25 so handover is delayed by at most one
	// reconcile; intentional).
	if threshold <= 0 {
		threshold = 25
	}
	pct := task.Status.LastTurnInputTokens * 100 / int64(ctxWin)
	if pct < int64(threshold) {
		return nil
	}

	// Build handover doc when Status.Handover is empty.
	handover := task.Status.Handover
	if handover == "" {
		parts := []string{}
		if task.Status.ResultSummary != "" {
			parts = append(parts, "## Prior work summary\n"+task.Status.ResultSummary)
		}
		if task.Status.ImplementContext != "" {
			parts = append(parts, "## Re-entry context\n"+task.Status.ImplementContext)
		}
		if task.Status.HeadBranch != "" {
			parts = append(parts, "Prior work is on branch `"+task.Status.HeadBranch+"`; read its diff.")
		}
		if len(parts) > 0 {
			handover = strings.Join(parts, "\n\n")
		} else {
			handover = "Resume from prior implement run."
		}
	}

	// Persist Handover on status FIRST. If the annotation write below fails, the
	// task is left with Handover set but annPendingHandoverResume unset: safe
	// (implementPrompt ignores the Handover block) and self-heals on retry.
	// The prior ordering (annotation first) was unsafe: a status-write failure
	// left pending-handover-resume=true with empty Handover, causing a silent
	// no-op resume that consumed the annotation without injecting any context.
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		fresh.Status.Handover = handover
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		task.Status.Handover = handover
		return nil
	}); err != nil {
		return fmt.Errorf("maybeMarkHandoverResume: set handover status: %w", err)
	}

	// Persist the resume annotation after the status is committed.
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		if fresh.Annotations == nil {
			fresh.Annotations = map[string]string{}
		}
		fresh.Annotations[annPendingHandoverResume] = "true"
		if err := r.Update(ctx, fresh); err != nil {
			return err
		}
		task.ResourceVersion = fresh.ResourceVersion
		return nil
	}); err != nil {
		return fmt.Errorf("maybeMarkHandoverResume: set annotation: %w", err)
	}

	if r.LifecycleMetrics != nil {
		r.LifecycleMetrics.RecordHandover()
	}
	return nil
}
