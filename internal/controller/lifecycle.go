// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
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
func (r *TaskReconciler) setLifecycleState(ctx context.Context, task *tatarav1alpha1.Task, to, reason string) error {
	l := log.FromContext(ctx)
	// `from` is always overwritten inside the closure (finding 13: the outer
	// task.Status.LifecycleState initializer was dead code since RetryOnConflict
	// always runs the closure at least once and sets `from = fresh.Status...`).
	var from string

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
		return nil
	}); err != nil {
		return fmt.Errorf("setLifecycleState: %w", err)
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
	case "Done", "Stopped", "Parked":
		r.Metrics.ReconcileResult("Task", "success")
		return ctrl.Result{}, nil
	default:
		r.Metrics.ReconcileResult("Task", "error")
		return ctrl.Result{}, fmt.Errorf("lifecycle: unknown lifecycleState %q for task %s", task.Status.LifecycleState, task.Name)
	}
}

// handleTriage drives the Triage agent-run state. On a finished run it reads
// IssueOutcome and transitions: close->Done, discuss->Conversation, implement->Implement.
func (r *TaskReconciler) handleTriage(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	// Run finished -> act on the outcome.
	if isTerminal(task.Status.Phase) {
		return r.finishTriage(ctx, project, task)
	}
	// Run in progress (or not yet started) -> drive another step.
	// Idempotent: ensure the brainstorming label is set (covers reactivation where
	// the task re-enters Triage without going through the case "" initializer).
	if !isTerminal(task.Status.Phase) {
		if err := r.ensurePhaseLabel(ctx, project, task, "brainstorming"); err != nil {
			return ctrl.Result{}, err
		}
	}
	var repo tatarav1alpha1.Repository
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.RepositoryRef}, &repo); err != nil {
		return ctrl.Result{}, fmt.Errorf("triage: get repo: %w", err)
	}
	// Best-effort: on the first Triage of a brainstorm-derived issue, set the
	// fork-from-conversation annotation so the pod forks the brainstorm
	// conversation (issue #114 decision 3). Non-fatal so a fork-setup hiccup never
	// blocks triage.
	if err := r.maybeSetupConversationFork(ctx, task); err != nil {
		log.FromContext(ctx).Info("triage: conversation fork setup failed (non-fatal)",
			"resource_id", task.Name, "err", err.Error())
	}
	// Pass the already-fetched repo URL into buildTriagePromptFor so resolveTriageReader
	// inside it can reuse the URL without another Get (finding 7).
	prompt := r.buildTriagePromptFor(ctx, project, task, repo.Spec.URL)
	return r.driveAgentRun(ctx, project, &repo, task, prompt)
}

// maybeSetupConversationFork, on the first Triage of a brainstorm-derived issue,
// correlates this issueLifecycle Task to the proposal Task that opened the issue
// (matching repo + issue number) and copies the proposal's parent-conversation
// key onto this Task as the fork-from annotation, so the first pod forks the
// brainstorm conversation (issue #114 decision 3). No-op when S3 is off, when
// the Task already carries a fork pointer or its own conversation, when it has no
// issue number, or when no matching proposal with a parent key is found.
func (r *TaskReconciler) maybeSetupConversationFork(ctx context.Context, task *tatarav1alpha1.Task) error {
	if r.PodConfig.S3Bucket == "" {
		return nil
	}
	if task.Spec.Source == nil || task.Spec.Source.Number == 0 {
		return nil
	}
	if task.Annotations[annForkFromConversationKey] != "" ||
		task.Status.SessionID != "" || task.Status.ConversationObjectKey != "" {
		return nil
	}
	var tasks tatarav1alpha1.TaskList
	if err := r.List(ctx, &tasks, client.InNamespace(task.Namespace)); err != nil {
		return err
	}
	parentKey := ""
	for i := range tasks.Items {
		t := &tasks.Items[i]
		if t.Spec.ProposedIssue == nil || t.Spec.Source == nil {
			continue
		}
		if t.Spec.RepositoryRef != task.Spec.RepositoryRef || t.Spec.Source.Number != task.Spec.Source.Number {
			continue
		}
		if k := t.Annotations[annParentConversationKey]; k != "" {
			parentKey = k
			break
		}
	}
	if parentKey == "" {
		return nil
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		if fresh.Annotations == nil {
			fresh.Annotations = map[string]string{}
		}
		if fresh.Annotations[annForkFromConversationKey] != "" {
			return nil
		}
		fresh.Annotations[annForkFromConversationKey] = parentKey
		if err := r.Update(ctx, fresh); err != nil {
			return err
		}
		task.Annotations = fresh.Annotations
		task.ResourceVersion = fresh.ResourceVersion
		return nil
	})
}

// buildTriagePromptFor fetches issue content and comments via ReaderFor (if wired) and
// builds the full triage turn-0 prompt with real title, body, and comment thread included.
// On any error it falls back gracefully with empty title/body.
// Uses resolveTriageReaderURL to share the provider/token/reader/ownerRepo resolution
// boilerplate with finishTriage, avoiding a third independent copy (finding 6).
// repoURL is the already-fetched repository URL from handleTriage (finding 7: avoids
// a second Get of the same Repository within the same reconcile).
func (r *TaskReconciler) buildTriagePromptFor(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task, repoURL string) string {
	l := log.FromContext(ctx)
	if r.ReaderFor == nil || task.Spec.Source == nil {
		return lifecycleTriageText(task, "", "")
	}
	tr := r.resolveTriageReaderURL(ctx, project, task, repoURL)
	if !tr.resolved {
		l.Info("triage: reader not resolved for prompt fetch (non-fatal)", "resource_id", task.Name)
		return lifecycleTriageText(task, "", "")
	}
	content, err := tr.reader.GetIssue(ctx, tr.owner, tr.repoName, tr.issueNum)
	if err != nil {
		l.Info("triage: GetIssue failed (non-fatal)", "resource_id", task.Name, "err", err.Error())
		// Intentional fall-through: content is the zero value so Title/Body are "".
		// buildTriagePrompt handles empty fields gracefully. Unlike the token/reader/
		// OwnerRepo error branches above (which return the lifecycle fallback text),
		// a GetIssue failure keeps any ListIssueComments result that already landed
		// on the next call, so the partial fetch is used rather than thrown away
		// (finding 22).
	}
	comments, err := tr.reader.ListIssueComments(ctx, tr.owner, tr.repoName, tr.issueNum)
	if err != nil {
		l.Info("triage: ListIssueComments failed (non-fatal)", "resource_id", task.Name, "err", err.Error())
		comments = nil
	}
	return buildTriagePrompt(task, content.Title, content.Body, comments)
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

// triageReader holds the pre-resolved SCM reader context for finishTriage so the
// authorship and human-comment checks can share one token fetch, one ReaderFor
// call, and one repo-URL resolution (finding 6).
type triageReader struct {
	reader   scm.SCMReader
	owner    string
	repoName string
	botLogin string
	// approvers is the effective maintainer/approver allowlist for the task's repo
	// (issue #102). When non-empty, only a comment from one of these accounts
	// counts as the human approval go-ahead; empty preserves the historical
	// behavior (any non-bot human reply releases the self-approve hold).
	approvers []string
	issueNum  int
	resolved  bool // false = ReaderFor unavailable; callers treat as "not authored"
}

// resolveTriageReader resolves the SCM reader and repo coordinates once for
// finishTriage. On any error resolved is false and callers fall back safely.
// Calls repoURLForTask internally; use resolveTriageReaderURL when the caller
// already holds the repository URL (finding 7).
func (r *TaskReconciler) resolveTriageReader(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) triageReader {
	return r.resolveTriageReaderURL(ctx, project, task, r.repoURLForTask(ctx, task))
}

// resolveTriageReaderURL is the same as resolveTriageReader but accepts a
// pre-fetched repository URL so callers that already hold it avoid an extra
// Get (finding 7: handleTriage fetches the repo for driveAgentRun and passes
// its URL here rather than letting resolveTriageReader issue a second Get).
func (r *TaskReconciler) resolveTriageReaderURL(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task, repoURL string) triageReader {
	if r.ReaderFor == nil || task.Spec.Source == nil {
		return triageReader{}
	}
	provider := task.Spec.Source.Provider
	if provider == "" && project.Spec.Scm != nil {
		provider = project.Spec.Scm.Provider
	}
	token, err := r.scmToken(ctx, task.Namespace, project.Spec.ScmSecretRef)
	if err != nil {
		return triageReader{}
	}
	reader, err := r.ReaderFor(provider, token)
	if err != nil {
		return triageReader{}
	}
	owner, repoName, err := scm.OwnerRepo(repoURL)
	if err != nil {
		return triageReader{}
	}
	botLogin := ""
	if project.Spec.Scm != nil {
		botLogin = project.Spec.Scm.BotLogin
	}
	// Effective approver list for the self-approve-hold release (issue #102),
	// honoring any per-repository MaintainerLogins override. Best-effort Get: on
	// failure approvers falls back to the project list (nil repo).
	var repo *tatarav1alpha1.Repository
	if task.Spec.RepositoryRef != "" {
		var repoObj tatarav1alpha1.Repository
		if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.RepositoryRef}, &repoObj); err == nil {
			repo = &repoObj
		}
	}
	return triageReader{
		reader:    reader,
		owner:     owner,
		repoName:  repoName,
		botLogin:  botLogin,
		approvers: tatarav1alpha1.EffectiveMaintainerLogins(project, repo),
		issueNum:  task.Spec.Source.Number,
		resolved:  true,
	}
}

// isTataraAuthored uses the pre-resolved triageReader to check the tatara-authored
// marker without an additional token/reader/repo fetch (finding 6).
func (tr triageReader) isTataraAuthored(ctx context.Context) (bool, error) {
	if !tr.resolved {
		return false, nil
	}
	content, err := tr.reader.GetIssue(ctx, tr.owner, tr.repoName, tr.issueNum)
	if err != nil {
		return false, err
	}
	return strings.Contains(content.Body, tataraAuthoredMarker), nil
}

// hasHumanReply uses the pre-resolved triageReader to check for a human comment
// that releases the self-approve hold, without an additional token/reader/repo
// fetch (finding 6). When an approver allowlist is configured (issue #102) only
// a comment from an approver counts; otherwise any non-bot comment does.
func (tr triageReader) hasHumanReply(ctx context.Context) (bool, error) {
	if !tr.resolved {
		return false, nil
	}
	comments, err := tr.reader.ListIssueComments(ctx, tr.owner, tr.repoName, tr.issueNum)
	if err != nil {
		return false, err
	}
	for _, c := range comments {
		if c.Author == "" || c.Author == tr.botLogin {
			continue
		}
		if len(tr.approvers) > 0 && !slices.Contains(tr.approvers, c.Author) {
			continue
		}
		return true, nil
	}
	return false, nil
}

// botHasLastWord reports whether the newest comment on the issue is authored by
// the bot (the bot already had the last word). Newest is by CreatedAt, so it is
// robust to SCM list ordering. No comments -> false (the bot has not spoken).
// Used to suppress repeated hold comments once the bot has responded and no human
// has replied since.
func (tr triageReader) botHasLastWord(ctx context.Context) (bool, error) {
	if !tr.resolved {
		return false, nil
	}
	comments, err := tr.reader.ListIssueComments(ctx, tr.owner, tr.repoName, tr.issueNum)
	if err != nil {
		return false, err
	}
	return botIsLastCommenter(comments, tr.botLogin), nil
}

// finishTriage consumes Status.IssueOutcome after a completed Triage agent run.
func (r *TaskReconciler) finishTriage(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	if task.Status.Phase == "Failed" {
		l.Info("triage agent run failed; parking task",
			"action", "lifecycle_triage_failed", "resource_id", task.Name)
		if err := r.setLifecycleState(ctx, task, "Parked", "triage-failed"); err != nil {
			return ctrl.Result{}, err
		}
		if r.LifecycleMetrics != nil {
			r.LifecycleMetrics.RecordGiveup("triage-failed")
		}
		return ctrl.Result{}, r.resetAgentRun(ctx, task)
	}

	// Resolve the SCM reader once so the discuss-arm silence gate and the
	// implement-guard arm share one token fetch, one ReaderFor call, and one
	// repo-URL resolution rather than repeating them in each helper (finding 6).
	tr := r.resolveTriageReader(ctx, project, task)

	// Phase == Succeeded: read outcome.
	// A nil IssueOutcome means the agent ended without calling issue_outcome (turn cap,
	// NoPendingSubtasks, etc.). Defaulting to "implement" silently converts an
	// inconclusive triage into work; "discuss" is safer: it enters Conversation and
	// awaits human input. Only an explicit outcome.Action=="implement" proceeds to
	// the implement path (finding 2).
	outcome := task.Status.IssueOutcome
	action := "discuss" // default when agent did not set outcome (inconclusive run)
	comment := ""
	if outcome != nil {
		action = outcome.Action
		comment = outcome.Comment
	}

	// IssueOutcome is cleared only AFTER the action arm commits a state
	// transition (see clearIssueOutcome calls below). Clearing before acting
	// would, on any mid-arm SCM error, strand the task with a nil outcome and
	// silently default a close/discuss to implement on the next reconcile.
	// Accepted tradeoff: if the post-SCM status transition (RetryOnConflict)
	// exhausts its retries after the comment/close already landed, the next
	// reconcile re-runs the arm and may post a duplicate triage comment. That
	// is rare and cosmetic, and preferred over the wrong-implement downgrade.
	brainstorming, approved, _, declined := lifecycleLabels(project.Spec.Scm)

	switch action {
	case "close":
		if hasUnmergedChange(task) {
			// Invariant: never close an issue that has an unmerged code change.
			// A human-comment re-triage of an issue whose MR is open/conflicting
			// can yield a "close" outcome; closing here would orphan the unmerged
			// change. Keep the issue open (brainstorming) and await the change being
			// merged-green or abandoned.
			l.Info("triage close withheld: unmerged change exists; keeping issue open",
				"action", "lifecycle_close_withheld", "resource_id", task.Name,
				"pr_url", task.Status.PrURL, "head_branch", task.Status.HeadBranch)
			if err := r.setLifecycleLabel(ctx, project, task, brainstorming); err != nil {
				return ctrl.Result{}, err
			}
			// Silence gate: same authored+no-human-reply check as the discuss arm
			// (finding 3). Without it, a tatara-authored issue with a persistent
			// "close" outcome re-posts the withhold note on every re-triage cycle,
			// spamming the issue. Uses the pre-resolved triageReader (finding 6).
			skipComment := false
			authored, aerr := tr.isTataraAuthored(ctx)
			if aerr != nil {
				l.Info("triage close-withheld: authorship check failed; posting comment (fail open)",
					"action", "lifecycle_close_withheld_silence_check", "resource_id", task.Name, "err", aerr.Error())
			} else if authored {
				human, herr := tr.hasHumanReply(ctx)
				if herr != nil {
					l.Info("triage close-withheld: hasHumanComment failed; posting comment (fail open)",
						"action", "lifecycle_close_withheld_silence_check", "resource_id", task.Name, "err", herr.Error())
				} else if !human {
					skipComment = true
					l.Info("triage close-withheld: tatara-authored issue with no human reply; suppressing note",
						"action", "lifecycle_close_withheld_silent_hold", "resource_id", task.Name)
				}
			}
			if !skipComment {
				lastWord, lerr := tr.botHasLastWord(ctx)
				if lerr != nil {
					l.Info("triage close-withheld: last-word check failed; posting note (fail open)",
						"action", "lifecycle_close_withheld_silence_check", "resource_id", task.Name, "err", lerr.Error())
				} else if lastWord {
					skipComment = true
					l.Info("triage close-withheld: bot already has the last word; suppressing note",
						"action", "lifecycle_close_withheld_silent_hold", "resource_id", task.Name)
				}
			}
			if !skipComment {
				note := comment
				if note != "" {
					note += "\n\n"
				}
				note += "tatara: not closing - this issue has an unmerged change that must be merged (with green main CI) or abandoned first."
				if err := r.triagePostComment(ctx, project, task, note); err != nil {
					return ctrl.Result{}, err
				}
			}
			if err := r.enterConversation(ctx, project, task, "close-withheld-unmerged"); err != nil {
				return ctrl.Result{}, err
			}
			// Record metric AFTER enterConversation commits (finding 3: path was invisible
			// to outcome metrics; now matches the discuss/implement arm discipline).
			r.Metrics.IssueOutcome("close-withheld")
			break
		}
		if err := r.setLifecycleLabel(ctx, project, task, declined); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.triageCloseIssue(ctx, project, task, comment); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.setLifecycleState(ctx, task, "Done", "triage-close"); err != nil {
			return ctrl.Result{}, err
		}
		// Record IssueOutcome("close") AFTER setLifecycleState commits, so a failed
		// transition (RetryOnConflict exhausted) does not double-count on re-reconcile
		// (finding 1). triageCloseIssue is idempotent on the SCM side; the metric is not.
		r.Metrics.IssueOutcome("close")

	case "discuss":
		if err := r.setLifecycleLabel(ctx, project, task, brainstorming); err != nil {
			return ctrl.Result{}, err
		}
		// Silence gate: for tatara-authored issues with no human reply, do not
		// post a repeated "still awaiting go-ahead" comment on every triage cycle.
		// Only post when a human has actually replied since the issue was opened.
		// Human-filed issues always get the comment (authorship check returns false).
		// Uses the pre-resolved triageReader (finding 6: no repeated token/reader fetch).
		skipComment := false
		authored, aerr := tr.isTataraAuthored(ctx)
		if aerr != nil {
			l.Info("triage discuss: authorship check failed; posting comment (fail open)",
				"action", "lifecycle_discuss_silence_check", "resource_id", task.Name, "err", aerr.Error())
		} else if authored {
			human, herr := tr.hasHumanReply(ctx)
			if herr != nil {
				l.Info("triage discuss: hasHumanComment failed; posting comment (fail open)",
					"action", "lifecycle_discuss_silence_check", "resource_id", task.Name, "err", herr.Error())
			} else if !human {
				skipComment = true
				l.Info("triage discuss: tatara-authored issue with no human reply; suppressing comment",
					"action", "lifecycle_discuss_silent_hold", "resource_id", task.Name)
			}
		}
		if !skipComment {
			lastWord, lerr := tr.botHasLastWord(ctx)
			if lerr != nil {
				l.Info("triage discuss: last-word check failed; posting comment (fail open)",
					"action", "lifecycle_discuss_silence_check", "resource_id", task.Name, "err", lerr.Error())
			} else if lastWord {
				skipComment = true
				l.Info("triage discuss: bot already has the last word; suppressing comment",
					"action", "lifecycle_discuss_silent_hold", "resource_id", task.Name)
			}
		}
		if !skipComment {
			if err := r.triagePostComment(ctx, project, task, comment); err != nil {
				return ctrl.Result{}, err
			}
		}
		// Record metric AFTER enterConversation commits so a failed transition does not
		// double-count on the next reconcile (findings 1 & 5). The implement arm records
		// after setLifecycleState; this arm now matches that discipline.
		if err := r.enterConversation(ctx, project, task, "triage-discuss"); err != nil {
			return ctrl.Result{}, err
		}
		r.Metrics.IssueOutcome("discuss")

	case "implement":
		// Author-tiered autoapprove (issue #56): an issue opened by a known
		// third-party contributor (author is neither the bot nor a maintainer) is
		// trusted, so the triage agent's implement decision is honored straight
		// through without the self-approve hold. Bot- and tatara-authored ideas,
		// and the empty/maintainer-authored case, fall through to the guard below.
		if !thirdPartyAuthor(project, task) {
			// Self-approve guard (R1/R2): tatara never approves its OWN idea before a
			// human has engaged. Authorship is detected via the tatara-authored marker
			// in the issue body - the reliable, egress-verified fallback for the
			// bot/maintainer/empty-author case the third-party tier does not cover
			// (Source.AuthorLogin is empty for board-sourced candidates).
			// Uses the pre-resolved triageReader (finding 6: shared reader context).
			authored, aerr := tr.isTataraAuthored(ctx)
			if aerr != nil {
				l.Info("triage: authorship check failed; treating as tatara-authored (fail closed)",
					"action", "lifecycle_triage_guard", "resource_id", task.Name, "err", aerr.Error())
				authored = true
			}
			if authored {
				human, herr := tr.hasHumanReply(ctx)
				if herr != nil {
					l.Info("triage: hasHumanComment failed; parking as brainstorming (fail closed)",
						"action", "lifecycle_triage_guard", "resource_id", task.Name, "err", herr.Error())
					human = false
				}
				if !human {
					if err := r.setLifecycleLabel(ctx, project, task, brainstorming); err != nil {
						return ctrl.Result{}, err
					}
					// Tear down the wrapper BEFORE transitioning to Conversation so a
					// failed resetAgentRun leaves the task in Triage (still owns the pod)
					// rather than in Conversation with a leaked live pod that nothing
					// else will reap (finding 19).
					if err := r.resetAgentRun(ctx, task); err != nil {
						return ctrl.Result{}, err
					}
					if err := r.clearIssueOutcome(ctx, task); err != nil {
						return ctrl.Result{}, err
					}
					if err := r.enterConversation(ctx, project, task, "triage-await-approval"); err != nil {
						return ctrl.Result{}, err
					}
					return ctrl.Result{}, nil
				}
			}
		}
		if err := r.setLifecycleLabel(ctx, project, task, approved); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.setLifecycleState(ctx, task, "Implement", "triage-implement"); err != nil {
			return ctrl.Result{}, err
		}
		r.Metrics.IssueOutcome("implement")

	default:
		// Unknown action: an agent returned an unrecognized action string. Route to the
		// safe discuss/Conversation hold rather than silently triggering implementation
		// (finding 17: the prior bare `default:` with comment "implement and anything else"
		// would have sent any unknown string to the implement path, which is dangerous).
		l.Info("triage: unknown action string; defaulting to discuss (safe fallback)",
			"action", "lifecycle_triage_unknown_action",
			"resource_id", task.Name,
			"unknown_action", action)
		if err := r.setLifecycleLabel(ctx, project, task, brainstorming); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.enterConversation(ctx, project, task, "triage-unknown-action"); err != nil {
			return ctrl.Result{}, err
		}
		r.Metrics.IssueOutcome("discuss")
	}

	if err := r.clearIssueOutcome(ctx, task); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, r.resetAgentRun(ctx, task)
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

// observeProposalLabelReadback checks the task's source issue for a human-applied
// approved or declined label and reflects the result onto the role:proposed ledger
// entry. This is the P4 "read label changes back" path: when a human relabels a
// brainstorm proposal issue, the operator writes it onto the ledger entry so the
// backlog cap and any tooling that reads the ledger see the updated state.
// Returns the observed state ("approved"|"declined"|"") and any error.
// Non-fatal errors are logged by the caller; nil reader or no source is a no-op.
func (r *TaskReconciler) observeProposalLabelReadback(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (string, error) {
	if r.ReaderFor == nil || task.Spec.Source == nil || task.Spec.Source.IssueRef == "" || task.Spec.Source.IsPR {
		return "", nil
	}
	// Only tasks with an UNDECIDED role:proposed entry (still WIProposed) are
	// subject to readback. Once a proposal is approved/declined the decision is
	// terminal, so skipping the SCM ListOpenIssues for already-decided proposals
	// avoids a redundant repo-wide issue list on every idle-conversation reconcile.
	hasUndecidedProposed := false
	for _, wi := range task.Status.WorkItems {
		if wi.Role == tatarav1alpha1.RoleProposed && wi.State == tatarav1alpha1.WIProposed {
			hasUndecidedProposed = true
			break
		}
	}
	if !hasUndecidedProposed {
		return "", nil
	}

	provider := task.Spec.Source.Provider
	if provider == "" && project.Spec.Scm != nil {
		provider = project.Spec.Scm.Provider
	}
	token, err := r.scmToken(ctx, task.Namespace, project.Spec.ScmSecretRef)
	if err != nil {
		return "", fmt.Errorf("observe label: token: %w", err)
	}
	reader, err := r.ReaderFor(provider, token)
	if err != nil {
		return "", fmt.Errorf("observe label: reader: %w", err)
	}
	_, repo, _, _, _, sctxErr := r.scmContext(ctx, task)
	if sctxErr != nil {
		return "", fmt.Errorf("observe label: scm context: %w", sctxErr)
	}
	owner, name, oerr := scm.OwnerRepo(repo.Spec.URL)
	if oerr != nil {
		return "", fmt.Errorf("observe label: repo url: %w", oerr)
	}
	issues, lerr := reader.ListOpenIssues(ctx, owner, name)
	if lerr != nil {
		return "", fmt.Errorf("observe label: list: %w", lerr)
	}
	issueRef := task.Spec.Source.IssueRef
	_, approvedLabel, _, declinedLabel := lifecycleLabels(project.Spec.Scm)
	for _, iss := range issues {
		if fmt.Sprintf("%s#%d", iss.Repo, iss.Number) != issueRef {
			continue
		}
		for _, lb := range iss.Labels {
			switch lb {
			case approvedLabel:
				if uErr := r.upsertProposedEntryState(ctx, task, issueRef, tatarav1alpha1.WIApproved); uErr != nil {
					return "approved", fmt.Errorf("observe label: upsert approved: %w", uErr)
				}
				return "approved", nil
			case declinedLabel:
				if uErr := r.upsertProposedEntryState(ctx, task, issueRef, tatarav1alpha1.WIDeclined); uErr != nil {
					return "declined", fmt.Errorf("observe label: upsert declined: %w", uErr)
				}
				return "declined", nil
			}
		}
		break // issue found, no approved/declined label
	}
	return "", nil
}

// handleConversation manages the idle wait state. No pod is ever spawned here.
// If the deadline has passed the task transitions to Stopped (idle-stop, resumable).
// If DeadlineAt is nil (safety net for tasks whose deadline was never set), set it
// once using project.Spec.Scm.ConversationIdleMinutes (same logic as enterConversation)
// and requeue so the normal deadline path runs on the next reconcile.
// Otherwise it requeues until the deadline.
func (r *TaskReconciler) handleConversation(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	// P4 label readback: if the source issue has been relabeled by a human
	// (tatara-approved or tatara-declined) since the last reconcile, reflect the
	// state onto the role:proposed ledger entry AND drive the decision:
	//   approved -> transition to Implement (mirrors the triage approve path),
	//   declined -> park with "human-declined".
	// A readback ERROR is best-effort and does not block the idle path. A clean
	// approved/declined observation supersedes the idle deadline path (return).
	state, rbErr := r.observeProposalLabelReadback(ctx, project, task)
	if rbErr != nil {
		l.Info("conversation: proposal label readback failed (non-fatal)",
			"action", "conversation_label_readback", "resource_id", task.Name, "err", rbErr.Error())
	} else {
		switch state {
		case tatarav1alpha1.WIApproved:
			if err := r.setLifecycleState(ctx, task, "Implement", "human-approved"); err != nil {
				return ctrl.Result{}, fmt.Errorf("conversation: approve readback to implement: %w", err)
			}
			l.Info("conversation: human-approved proposal; driving implementation",
				"action", "conversation_label_approved", "resource_id", task.Name)
			return ctrl.Result{}, nil
		case tatarav1alpha1.WIDeclined:
			if err := r.setLifecycleState(ctx, task, "Parked", "human-declined"); err != nil {
				return ctrl.Result{}, fmt.Errorf("conversation: decline readback to park: %w", err)
			}
			l.Info("conversation: human-declined proposal; parking",
				"action", "conversation_label_declined", "resource_id", task.Name)
			return ctrl.Result{}, nil
		}
	}

	if task.Status.DeadlineAt == nil {
		// Safety net: set deadline once rather than returning false from
		// deadlinePassed forever and requeuing without bound. Delegates to the
		// shared ensureDeadlineMinutes so this logic cannot drift from the primary
		// path in ensureDeadline (finding 15).
		idleMinutes := conversationDefaultIdleMinutes
		if project.Spec.Scm != nil && project.Spec.Scm.ConversationIdleMinutes > 0 {
			idleMinutes = project.Spec.Scm.ConversationIdleMinutes
		}
		if err := r.ensureDeadlineMinutes(ctx, task, idleMinutes); err != nil {
			return ctrl.Result{}, fmt.Errorf("conversation: set nil deadline: %w", err)
		}
		return ctrl.Result{RequeueAfter: pollRequeue}, nil
	}
	if deadlinePassed(task) {
		// Before stopping, check for pending interjections (finding 10): a human
		// comment that arrived just before the deadline should not be dropped. If
		// PendingInterjections is non-empty, refresh the deadline and re-drive rather
		// than stopping. The interjection drain at the top of reconcileLifecycle will
		// route them to the session (or drop as stale) on the next reconcile.
		if len(task.Status.PendingInterjections) > 0 {
			idleMinutes := conversationDefaultIdleMinutes
			if project.Spec.Scm != nil && project.Spec.Scm.ConversationIdleMinutes > 0 {
				idleMinutes = project.Spec.Scm.ConversationIdleMinutes
			}
			// Reset deadline so the conversation continues for another idle window.
			// setDeadlineMinutes replaces the two-write clearDeadline+ensureDeadlineMinutes
			// pattern with a single RetryOnConflict (finding 3/r3).
			if err := r.setDeadlineMinutes(ctx, task, idleMinutes); err != nil {
				return ctrl.Result{}, err
			}
			log.FromContext(ctx).Info("conversation: deadline passed but interjections pending; extending",
				"action", "conversation_extend_pending", "resource_id", task.Name,
				"pending", len(task.Status.PendingInterjections))
			return ctrl.Result{RequeueAfter: pollRequeue}, nil
		}
		if err := r.setLifecycleState(ctx, task, "Stopped", "idle"); err != nil {
			return ctrl.Result{}, err
		}
		if r.LifecycleMetrics != nil {
			r.LifecycleMetrics.RecordIdleStop()
		}
		return ctrl.Result{}, nil
	}
	return ctrl.Result{RequeueAfter: pollRequeue}, nil
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

// implementPrompt builds the turn-0 prompt for the Implement state.
//   - When Status.ImplementContext is set, appends a "## Re-entry context" block.
//   - When the pending-handover-resume annotation is set, prepends a
//     "## Resume from handover" block so the agent resumes with full context.
func implementPrompt(task *tatarav1alpha1.Task) string {
	base := planTurnText(task.Spec.Goal, taskBranch(task), task.Spec.ProjectRef, task.Name)
	// Hard instruction: if after investigation no PR should be opened, the agent
	// MUST call decline_implementation instead of finishing silently.
	base += "\n\n**IMPORTANT:** If after investigation you will NOT implement this " +
		"issue, you MUST call `decline_implementation` with a clear reason (what you " +
		"considered and why it should not / need not be done). A silent finish with no " +
		"PR and no `decline_implementation` call is NOT allowed and will be re-prompted."
	if len(task.Spec.ReposInScope) > 0 {
		base += "\n\n**This issue spans repos: " + strings.Join(task.Spec.ReposInScope, ", ") +
			".** Edit and push every repo you change; each repo with a change gets its own PR/MR. " +
			"If a listed repo genuinely needs no change, say so explicitly in your result summary."
	}
	if g := task.Spec.SystemicGroup; g != nil && len(g.SameRepoSiblings) > 0 {
		closes := make([]string, 0, len(g.SameRepoSiblings))
		for _, n := range g.SameRepoSiblings {
			closes = append(closes, fmt.Sprintf("Closes #%d", n))
		}
		base += "\n\n**You lead systemic improvement group " + g.SystemicID +
			".** Resolve these sibling issues in this same repo within ONE combined PR and " +
			"close them from the PR body: " + strings.Join(closes, ", ") + "."
	}
	if g := task.Spec.SystemicGroup; g != nil && len(g.CrossRepo) > 0 {
		base += "\n\nRelated work in OTHER repos (reference for context, do NOT edit them here; " +
			"each is led by its own agent): " + strings.Join(g.CrossRepo, "; ") + "."
	}
	if d := skillsDirective("issueLifecycle"); d != "" {
		base += "\n\n" + d
	}
	base += lifecyclePhaseGuidance("Implement")
	if task.Status.ImplementContext != "" {
		base += "\n\n## Re-entry context\n" + task.Status.ImplementContext
	}
	// Compaction path (issue #114 decision 2): when pending-handover-resume is set
	// (LastTurnInputTokens crossed HandoverThresholdPercent), inject the compacted
	// text handover. This is mutually exclusive with full conversation replay:
	// agent.BuildPod skips CONVERSATION_SESSION_ID under the same annotation, so
	// the agent gets EITHER the full transcript (--resume, under threshold) OR the
	// handover summary (at/over threshold), never both (which would overflow the
	// context window the threshold exists to protect).
	if task.Annotations[annPendingHandoverResume] == "true" && task.Status.Handover != "" {
		base += "\n\n## Resume from handover\n" + task.Status.Handover
	}
	return base
}

// handleImplement drives the Implement agent-run state. On a finished run it
// calls writeBackOpenChange to open the MR, then transitions to MRCI.
func (r *TaskReconciler) handleImplement(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	if isTerminal(task.Status.Phase) {
		return r.finishImplement(ctx, task)
	}

	// Fresh-spawn path: Phase == "". Apply backstop + increment iterations.
	if task.Status.Phase == "" {
		maxIter := 10
		if project.Spec.Agent.MaxLifecycleIterations > 0 {
			maxIter = project.Spec.Agent.MaxLifecycleIterations
		}
		if task.Status.LifecycleIterations >= maxIter {
			// Backstop: too many attempts. Post comment and park.
			_, _, writer, token, provider, scmErr := r.scmContext(ctx, task)
			if scmErr == nil && task.Spec.Source != nil && task.Spec.Source.IssueRef != "" {
				msg := "max lifecycle iterations reached; leaving for a human"
				cerr := writer.Comment(ctx, token, task.Spec.Source.IssueRef, msg)
				r.recordSCM(provider, "comment", cerr)
				if cerr != nil {
					log.FromContext(ctx).Error(cerr, "implement: max-iterations comment (non-fatal)", "resource_id", task.Name)
				}
			}
			if err := r.setLifecycleState(ctx, task, "Parked", "maxIterations"); err != nil {
				return ctrl.Result{}, err
			}
			if r.LifecycleMetrics != nil {
				r.LifecycleMetrics.RecordGiveup("maxIterations")
			}
			return ctrl.Result{}, nil
		}
		// Increment LifecycleIterations on fresh spawn. Capture the post-Update object
		// from inside the closure to propagate the new resourceVersion and status without
		// an extra Get round-trip (finding 5: the prior code discarded `fresh` and issued
		// a second Get, inconsistent with setImplementEmptyRetries/setImplementContext).
		var iterFresh tatarav1alpha1.Task
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			if err := r.Get(ctx, client.ObjectKeyFromObject(task), &iterFresh); err != nil {
				return err
			}
			iterFresh.Status.LifecycleIterations++
			return r.Status().Update(ctx, &iterFresh)
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("implement: increment iterations: %w", err)
		}
		// Copy mutable pointers back so driveAgentRun uses the updated resourceVersion.
		task.ResourceVersion = iterFresh.ResourceVersion
		task.Status = iterFresh.Status
		// Idempotent: set implementation label on fresh Implement spawn.
		if err := r.ensurePhaseLabel(ctx, project, task, "implementation"); err != nil {
			return ctrl.Result{}, err
		}
	}

	var repo tatarav1alpha1.Repository
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.RepositoryRef}, &repo); err != nil {
		return ctrl.Result{}, fmt.Errorf("implement: get repo: %w", err)
	}
	// Build the prompt using the current ImplementContext (may contain re-entry
	// instructions). Do NOT clear it here - it must persist until the pod is ready
	// and driveTurns submits the turn-0 prompt. Clearing happens in finishImplement,
	// after the run has completed and the context has been used.
	planText := implementPrompt(task)
	return r.driveAgentRun(ctx, project, &repo, task, planText)
}

// finishImplement opens the MR after the Implement run completes.
func (r *TaskReconciler) finishImplement(ctx context.Context, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	// Clear ImplementContext: by the time finishImplement runs the turn-0 prompt
	// has already been submitted (or the run failed), so the context is stale.
	// Each re-entry overwrites it fresh, so clearing here is safe.
	// After the write, refresh the in-memory task so subsequent status updates use
	// the current resourceVersion and do not conflict.
	if task.Status.ImplementContext != "" {
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			fresh := &tatarav1alpha1.Task{}
			if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
				return err
			}
			if fresh.Status.ImplementContext == "" {
				task.ResourceVersion = fresh.ResourceVersion
				task.Status = fresh.Status
				return nil
			}
			fresh.Status.ImplementContext = ""
			if err := r.Status().Update(ctx, fresh); err != nil {
				return err
			}
			task.ResourceVersion = fresh.ResourceVersion
			task.Status = fresh.Status
			return nil
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("implement: clear ImplementContext: %w", err)
		}
	}

	if task.Status.Phase == "Failed" {
		l.Info("implement agent run failed; parking task",
			"action", "lifecycle_implement_failed", "resource_id", task.Name)
		if err := r.setLifecycleState(ctx, task, "Parked", "implement-failed"); err != nil {
			return ctrl.Result{}, err
		}
		if r.LifecycleMetrics != nil {
			r.LifecycleMetrics.RecordGiveup("implement-failed")
		}
		return ctrl.Result{}, r.resetAgentRun(ctx, task)
	}

	// Phase == Succeeded: open MR via the shared writeBackOpenChange path.
	// writeBackOpenChange sets task.Status.PrURL when a PR was opened.
	if _, err := r.writeBackOpenChange(ctx, task); err != nil {
		return ctrl.Result{}, fmt.Errorf("implement: open change: %w", err)
	}

	// Re-read task to pick up PrURL written by writeBackOpenChange.
	fresh := &tatarav1alpha1.Task{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
		return ctrl.Result{}, fmt.Errorf("implement: re-get task: %w", err)
	}

	if fresh.Status.PrURL == "" {
		// No PR opened. Branch on whether the agent declared a refusal.
		const emptyRetryCap = 2

		outcome := fresh.Status.ImplementOutcome
		codifiedTerminal := outcome != nil &&
			(outcome.Action == "declined" || outcome.Action == "already_done") &&
			strings.TrimSpace(outcome.Reason) != ""
		if codifiedTerminal {
			// Per-action park reason + giveup metric label.
			parkReason := "refused"
			giveupReason := "refused-declined"
			if outcome.Action == "already_done" {
				parkReason = "refused-already-done"
				giveupReason = "refused-already-done"
			}
			// CODIFIED TERMINAL: agent explicitly declared outcome via decline_implementation
			// or already_done. Post the reason as an issue comment, apply the declined label,
			// park with action-specific reason, clear ImplementOutcome, reset retries.
			l.Info("implement: agent declared codified terminal outcome; parking",
				"action", "lifecycle_implement_codified_terminal", "resource_id", task.Name,
				"impl_action", outcome.Action, "park_reason", parkReason)
			// Capture the Project from scmContext so we can pass it to ensurePhaseLabel
			// without a redundant Get (finding 15).
			if refusalProj, _, writer, token, provider, scmErr := r.scmContext(ctx, fresh); scmErr == nil {
				if fresh.Spec.Source != nil && fresh.Spec.Source.IssueRef != "" {
					cerr := writer.Comment(ctx, token, fresh.Spec.Source.IssueRef, outcome.Reason)
					r.recordSCM(provider, "comment", cerr)
					if cerr != nil {
						l.Error(cerr, "implement: post outcome comment (non-fatal)", "resource_id", task.Name)
					}
				}
				// Use the Project returned by scmContext (no redundant Get, finding 15).
				if err := r.ensurePhaseLabel(ctx, &refusalProj, fresh, "declined"); err != nil {
					l.Error(err, "implement: apply declined label (non-fatal)", "resource_id", task.Name)
				}
			} else {
				l.Error(scmErr, "implement: scm context for outcome comment (non-fatal)", "resource_id", task.Name)
			}
			if err := r.setLifecycleState(ctx, fresh, "Parked", parkReason); err != nil {
				return ctrl.Result{}, err
			}
			if r.LifecycleMetrics != nil {
				r.LifecycleMetrics.RecordGiveup(giveupReason)
			}
			if err := r.clearImplementOutcome(ctx, fresh); err != nil {
				return ctrl.Result{}, err
			}
			if err := r.setImplementEmptyRetries(ctx, fresh, 0); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, r.resetAgentRun(ctx, fresh)
		}

		// No declared decline. Re-prompt until cap, then park as refused-no-explanation.
		if fresh.Status.ImplementEmptyRetries < emptyRetryCap {
			if err := r.setImplementEmptyRetries(ctx, fresh, fresh.Status.ImplementEmptyRetries+1); err != nil {
				return ctrl.Result{}, err
			}
			if err := r.setImplementContext(ctx, fresh, emptyImplementReentryPrompt); err != nil {
				return ctrl.Result{}, err
			}
			l.Info("implement: no commit produced; retrying with re-entry nudge",
				"action", "lifecycle_implement_empty_retry", "resource_id", task.Name,
				"attempt", fresh.Status.ImplementEmptyRetries, "cap", emptyRetryCap)
			if r.LifecycleMetrics != nil {
				r.LifecycleMetrics.ImplementEmptyRetry()
			}
			// resetAgentRun clears phase to "" and leaves LifecycleState=Implement,
			// so the next reconcile re-spawns the Implement run with ImplementContext.
			// NOTE: LifecycleIterations will also be incremented on the re-spawn
			// (Phase="" path). This is intentional: each empty-retry is a real
			// spawn cycle and must count against MaxLifecycleIterations so the
			// backstop can fire. See MEMORY.md: the two counters are coupled and
			// MaxLifecycleIterations must always be set > emptyRetryCap.
			return ctrl.Result{}, r.resetAgentRun(ctx, fresh)
		}
		l.Info("implement: no commit after retry cap and no explanation; commenting + parking",
			"action", "lifecycle_implement_empty_parked", "resource_id", task.Name)
		msg := "The implement agent produced no change after " +
			strconv.Itoa(emptyRetryCap) + " attempts and did not explain why via decline_implementation. " +
			"Leaving this for a human - the fix may be unclear, blocked, or already present."
		// parkWithComment posts the comment (with the IsPR ref fallback) and parks
		// atomically. If the SCM context is unavailable, still park so the task does
		// not loop, just without a comment.
		if _, _, writer, token, _, scmErr := r.scmContext(ctx, fresh); scmErr == nil {
			if err := r.parkWithComment(ctx, fresh, writer, token, "refused-no-explanation", msg); err != nil {
				return ctrl.Result{}, err
			}
		} else {
			l.Error(scmErr, "implement: scm context for empty-park comment (parking without comment)",
				"resource_id", task.Name)
			if err := r.setLifecycleState(ctx, fresh, "Parked", "refused-no-explanation"); err != nil {
				return ctrl.Result{}, err
			}
		}
		if r.LifecycleMetrics != nil {
			r.LifecycleMetrics.RecordGiveup("refused-no-explanation")
		}
		return ctrl.Result{}, r.resetAgentRun(ctx, fresh)
	}
	// PR opened: clear any prior empty-retry count so a later re-entry into
	// Implement starts fresh.
	if fresh.Status.ImplementEmptyRetries > 0 {
		if err := r.setImplementEmptyRetries(ctx, fresh, 0); err != nil {
			l.Error(err, "implement: reset empty-retry counter (non-fatal)", "resource_id", task.Name)
		}
	}

	// M4: open a follow-up issue when RemainingScope is set and not already done.
	if err := r.maybeOpenFollowupIssue(ctx, fresh); err != nil {
		// Non-fatal: log and continue so the lifecycle does not stall.
		l.Error(err, "implement: open follow-up issue (non-fatal)", "resource_id", task.Name)
	}

	// Record head branch, PR number, and clear the stale deadline (e.g. from a
	// prior Conversation idle deadline) in one RetryOnConflict write BEFORE the
	// state transition. The RetryOnConflict does its own Get internally so the
	// extra re-Get after maybeOpenFollowupIssue is not needed (finding 17).
	// fresh.Status.PrURL is current from the Get at the top of finishImplement;
	// maybeOpenFollowupIssue does not modify PrURL.
	prNumber := parsePRNumber(fresh.Status.PrURL)
	if prNumber == 0 && fresh.Status.PrURL != "" {
		l.Error(nil, "implement: parsePRNumber returned 0 for non-empty PrURL; unexpected URL shape",
			"resource_id", task.Name, "pr_url", fresh.Status.PrURL)
	}
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		t2 := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), t2); err != nil {
			return err
		}
		t2.Status.HeadBranch = taskBranch(task)
		t2.Status.PRNumber = prNumber
		t2.Status.DeadlineAt = nil // clear stale Conversation/Implement deadline; MRCI sets its own via ensureDeadline
		return r.Status().Update(ctx, t2)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("implement: record pr fields: %w", err)
	}

	// Delegate the state transition to setLifecycleState so the transition log,
	// metric, and wrapper teardown all live in one place.
	if err := r.setLifecycleState(ctx, task, "MRCI", "implement-done"); err != nil {
		return ctrl.Result{}, err
	}

	// resetAgentRun does its own Get+RetryOnConflict internally so a fresh re-Get
	// here is redundant (finding 17).
	return ctrl.Result{}, r.resetAgentRun(ctx, task)
}

// maybeOpenFollowupIssue creates a follow-up issue when ChangeSummary.RemainingScope
// is non-empty and Status.FollowupIssueURL is not yet set (idempotency guard).
// It appends the new issue URL to Status.DiscoveredIssues and records it in
// Status.FollowupIssueURL so re-entry does not open a duplicate.
// Non-fatal: if CreateIssue fails the caller logs and continues.
func (r *TaskReconciler) maybeOpenFollowupIssue(ctx context.Context, task *tatarav1alpha1.Task) error {
	cs := task.Status.ChangeSummary
	if cs == nil || cs.RemainingScope == "" {
		return nil
	}
	// Idempotency guard: already opened.
	if task.Status.FollowupIssueURL != "" {
		return nil
	}

	_, repo, writer, token, provider, err := r.scmContext(ctx, task)
	if err != nil {
		return fmt.Errorf("followup: scm context: %w", err)
	}

	issueTitle := "Follow-up: " + firstLine(task.Spec.Goal) + " (remaining scope)"
	prURL := task.Status.PrURL
	issueBody := cs.RemainingScope + "\n\nOpened as a follow-up to: " + prURL + "\n\n" + tataraAuthoredMarker

	createStart := time.Now()
	created, cerr := writer.CreateIssue(ctx, repo.Spec.URL, token, scm.IssueReq{
		Title: issueTitle,
		Body:  issueBody,
	})
	r.recordSCM(provider, "create_issue", cerr)
	if cerr != nil {
		return fmt.Errorf("followup: create issue: %w", cerr)
	}

	log.FromContext(ctx).Info("lifecycle implement: follow-up issue opened",
		"action", "scm_followup_issue",
		"resource_id", task.Name,
		"issue_url", created.URL,
		"pr_url", prURL,
		"duration_ms", time.Since(createStart).Milliseconds(),
	)

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		// Append to DiscoveredIssues only if not already present.
		alreadyPresent := false
		for _, u := range fresh.Status.DiscoveredIssues {
			if u == created.URL {
				alreadyPresent = true
				break
			}
		}
		if !alreadyPresent {
			fresh.Status.DiscoveredIssues = append(fresh.Status.DiscoveredIssues, created.URL)
		}
		fresh.Status.FollowupIssueURL = created.URL
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		task.Status.DiscoveredIssues = fresh.Status.DiscoveredIssues
		task.Status.FollowupIssueURL = fresh.Status.FollowupIssueURL
		return nil
	})
}

// parsePRNumber extracts the trailing integer from a PR/MR URL
// (e.g. https://github.com/o/r/pull/42 -> 42). Returns 0 when the URL is
// empty or the trailing segment is not numeric; the caller should log a WARN
// when a non-empty URL produces 0 (indicates an unexpected URL shape).
func parsePRNumber(prURL string) int {
	if prURL == "" {
		return 0
	}
	parts := strings.Split(strings.TrimRight(prURL, "/"), "/")
	if len(parts) == 0 {
		return 0
	}
	n, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		// Non-numeric trailing segment: malformed or unexpected URL format.
		// Returns 0; the caller (finishImplement) logs at WARN.
		return 0
	}
	return n
}

// handleMRCI polls the MR CI status, enforces the authorship gate, and
// transitions to Merge (green), Implement (failure), or Parked (deadline/not-bot).
func (r *TaskReconciler) handleMRCI(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	l := log.FromContext(ctx)
	_, repo, writer, token, provider, err := r.scmContext(ctx, task)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("mrci: %w", err)
	}

	number, _ := lifecyclePR(task)

	// Guard: PR number 0 means no PR was opened; calling GetPRState(0) is invalid.
	if number == 0 {
		l.Info("mrci: PR number is 0; parking task",
			"action", "lifecycle_mrci_no_pr", "resource_id", task.Name)
		msg := "lifecycle: no PR number available for MRCI; parking."
		if err := r.parkWithComment(ctx, task, writer, token, "no-pr-number", msg); err != nil {
			return ctrl.Result{}, err
		}
		if r.LifecycleMetrics != nil {
			r.LifecycleMetrics.RecordGiveup("no-pr-number")
		}
		return ctrl.Result{}, nil
	}

	// Authorship gate: PR must be bot-authored.
	st, serr := writer.GetPRState(ctx, repo.Spec.URL, token, number)
	r.recordSCM(provider, "get_pr_state", serr)
	if serr != nil {
		return ctrl.Result{}, fmt.Errorf("mrci: get pr state: %w", serr)
	}
	botLogin := ""
	if project.Spec.Scm != nil {
		botLogin = project.Spec.Scm.BotLogin
	}
	if botLogin != "" && st.Author != botLogin {
		l.Info("mrci: PR not bot-authored; parking",
			"action", "lifecycle_mrci_not_bot", "resource_id", task.Name, "author", st.Author)
		msg := fmt.Sprintf("lifecycle: PR #%d is not authored by the bot (%s); parking.", number, botLogin)
		if err := r.parkWithComment(ctx, task, writer, token, "not-bot-authored", msg); err != nil {
			return ctrl.Result{}, err
		}
		if r.LifecycleMetrics != nil {
			r.LifecycleMetrics.RecordGiveup("not-bot-authored")
		}
		return ctrl.Result{}, nil
	}

	// Duplicate-of-merged guard. After a post-merge MainCI failure,
	// clearMergedChangeState clears PrURL/PRNumber so finishImplement opens a fresh
	// MR. But the deterministic task branch (tatara/task-<name>) is reused, and if
	// the re-implement did not advance it past the already-merged head, the new PR
	// re-proposes the SAME already-merged commits. Observed in-repo: tatara-operator
	// PR #50 duplicated merged PR #46 with an identical head SHA. Nursing it would
	// re-merge identical code and fail MainCI again, bouncing until maxLifecycleIterations
	// parks the task with no diagnostic. Detect it by the head SHA equaling the last
	// merged head, close the duplicate, and park for a human now.
	if task.Status.MergedHeadSHA != "" && st.HeadSHA != "" && st.HeadSHA == task.Status.MergedHeadSHA {
		l.Info("mrci: PR re-proposes the already-merged head; parking",
			"action", "lifecycle_mrci_duplicate_merged", "resource_id", task.Name,
			"pr", number, "head_sha", st.HeadSHA)
		closeMsg := fmt.Sprintf("Closing as a duplicate: this branch was already merged (head %s) and the post-merge default-branch pipeline failure needs a genuinely new fix, not a re-proposal of the same commits.", st.HeadSHA)
		cerr := writer.ClosePR(ctx, repo.Spec.URL, token, number, closeMsg)
		r.recordSCM(provider, "close_pr", cerr)
		if cerr != nil {
			l.Error(cerr, "mrci: close duplicate PR (non-fatal)", "resource_id", task.Name, "pr", number)
		}
		msg := fmt.Sprintf("lifecycle: PR #%d re-proposes the already-merged change (head %s) with no new fix after the post-merge pipeline failure; parking for a human.", number, st.HeadSHA)
		if err := r.parkWithComment(ctx, task, writer, token, "duplicate-merged-change", msg); err != nil {
			return ctrl.Result{}, err
		}
		if r.LifecycleMetrics != nil {
			r.LifecycleMetrics.RecordGiveup("duplicate-merged-change")
		}
		return ctrl.Result{}, nil
	}

	// Set DeadlineAt on first entry.
	if err := r.ensureDeadline(ctx, task, project); err != nil {
		return ctrl.Result{}, fmt.Errorf("mrci: ensure deadline: %w", err)
	}

	// Deadline check (do after authorship so a non-bot PR parks immediately).
	if deadlinePassed(task) {
		msg := fmt.Sprintf("lifecycle: MRCI deadline reached for PR #%d; parking.", number)
		return ctrl.Result{}, r.parkOnDeadline(ctx, task, writer, token, msg)
	}

	switch st.CIStatus {
	case "pending":
		return ctrl.Result{RequeueAfter: pollRequeue}, nil

	case "success":
		if r.LifecycleMetrics != nil && task.Status.DeadlineAt != nil {
			// Elapsed is re-derived from the current BabysitDeadlineMinutes config rather
			// than a stored entry timestamp. If BabysitDeadlineMinutes changed between
			// ensureDeadline and now the elapsed will be slightly skewed, but config rarely
			// changes mid-task and the deviation is bounded to one deadline-window length.
			// Accepted drift; a stored entry timestamp would require a separate status field
			// (finding 16).
			minutes := babysitDefaultDeadlineMinutes
			if project.Spec.Scm != nil && project.Spec.Scm.BabysitDeadlineMinutes > 0 {
				minutes = project.Spec.Scm.BabysitDeadlineMinutes
			}
			elapsed := time.Duration(minutes)*time.Minute - time.Until(task.Status.DeadlineAt.Time)
			if elapsed > 0 {
				r.LifecycleMetrics.ObserveMRCIWait(elapsed.Seconds())
			}
		}
		if err := r.clearDeadline(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.setLifecycleState(ctx, task, "Merge", "mrci-success"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil

	case "failure":
		ctx2 := fmt.Sprintf("MR pipeline failed for PR #%d. Fix the failures and push.", number)
		if err := r.setImplementContext(ctx, task, ctx2); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.clearDeadline(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.maybeMarkHandoverResume(ctx, project, task); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.setLifecycleState(ctx, task, "Implement", "mrci-failure"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil

	default: // "" - no CI configured
		if err := r.clearDeadline(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.setLifecycleState(ctx, task, "Merge", "mrci-no-ci"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}
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

// emptyImplementReentryPrompt nudges a re-spawned Implement agent that produced
// no diff on the prior turn to either deliver the change or stop and explain.
const emptyImplementReentryPrompt = "Your previous attempt finished without " +
	"committing any change, so no PR could be opened and the issue is still open. " +
	"Re-read the issue and the repository, then do EXACTLY ONE of: " +
	"(1) implement the fix and commit it; " +
	"(2) if the change is ALREADY PRESENT (e.g. another run already committed it on the shared branch), " +
	"call `already_done` with a reason naming where the fix already lives; " +
	"(3) if this issue genuinely should NOT be implemented (out of scope, wrong approach, blocked), " +
	"call `decline_implementation` with a clear reason. " +
	"A silent finish with no PR and no `already_done`/`decline_implementation` call is NOT allowed " +
	"and will be escalated to a human."

// setImplementEmptyRetries persists Status.ImplementEmptyRetries via RetryOnConflict.
func (r *TaskReconciler) setImplementEmptyRetries(ctx context.Context, task *tatarav1alpha1.Task, n int) error {
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
			return err
		}
		fresh.Status.ImplementEmptyRetries = n
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		task.Status.ImplementEmptyRetries = n
		return nil
	}); err != nil {
		return fmt.Errorf("setImplementEmptyRetries: %w", err)
	}
	return nil
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

// handleMerge attempts to merge the PR. Handles 405-conflict as a re-implement
// signal (MUST NOT return the error to avoid controller-runtime backoff loop).
func (r *TaskReconciler) handleMerge(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	_, repo, writer, token, provider, err := r.scmContext(ctx, task)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("merge: %w", err)
	}

	number, _ := lifecyclePR(task)

	// Idempotency: if the PR is already merged (MergeCommitSHA already set on the
	// task), skip straight to MainCI without calling Merge again. This handles the
	// case where setLifecycleState("MainCI") failed after a successful Merge on a
	// prior reconcile, which would otherwise re-merge -> 405 -> bogus conflict path.
	if task.Status.MergeCommitSHA != "" {
		// PR was merged in a prior reconcile; advance to MainCI directly.
		if err := r.clearDeadline(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.setLifecycleState(ctx, task, "MainCI", "already-merged"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Set DeadlineAt on first entry.
	if err := r.ensureDeadline(ctx, task, project); err != nil {
		return ctrl.Result{}, fmt.Errorf("merge: ensure deadline: %w", err)
	}

	// Check mergeAllowed policy. Fetch PR state once here; reuse HeadSHA below to
	// avoid a second round-trip (findings 3 & 4).
	prSt, pserr := writer.GetPRState(ctx, repo.Spec.URL, token, number)
	r.recordSCM(provider, "get_pr_state", pserr)
	if pserr != nil {
		return ctrl.Result{}, fmt.Errorf("merge: get pr state: %w", pserr)
	}
	if !r.mergeAllowed(project, prSt) {
		if deadlinePassed(task) {
			msg := fmt.Sprintf("lifecycle: merge deadline reached for PR #%d; parking.", number)
			return ctrl.Result{}, r.parkOnDeadline(ctx, task, writer, token, msg)
		}
		return ctrl.Result{RequeueAfter: pollRequeue}, nil
	}

	// Reuse the already-fetched HeadSHA to detect a later MRCI re-proposal of the
	// same already-merged commits, without a second GetPRState round-trip (finding 3).
	mergedHead := prSt.HeadSHA

	// Attempt merge.
	sha, mergeErr := writer.Merge(ctx, repo.Spec.URL, token, number, "squash")
	r.recordSCM(provider, "merge", mergeErr)
	if mergeErr == nil {
		// Success: record SHA and advance.
		// Derive repo slug once outside the closure for the ledger upsert.
		mergeRepoSlug, _, _ := repoSlugFromURL(repo.Spec.URL, provider)
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			fresh := &tatarav1alpha1.Task{}
			if err := r.Get(ctx, client.ObjectKeyFromObject(task), fresh); err != nil {
				return err
			}
			fresh.Status.MergeCommitSHA = sha
			fresh.Status.MergedHeadSHA = mergedHead
			// Project the merge event onto the ledger: flip the openedPR entry to
			// state:merged so the backstop and dedup helpers see live state.
			if mergeRepoSlug != "" && number > 0 {
				UpsertWorkItem(fresh, tatarav1alpha1.WorkItemRef{
					Provider: provider,
					Repo:     mergeRepoSlug,
					Number:   number,
					Kind:     tatarav1alpha1.WorkItemPR,
					Role:     tatarav1alpha1.RoleOpenedPR,
					State:    tatarav1alpha1.WIMerged,
					HeadSHA:  mergedHead,
				})
			}
			return r.Status().Update(ctx, fresh)
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("merge: record sha: %w", err)
		}
		task.Status.MergeCommitSHA = sha
		task.Status.MergedHeadSHA = mergedHead
		if err := r.clearDeadline(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.setLifecycleState(ctx, task, "MainCI", "merged"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// ErrMergeConflict -> re-implement with resolve instruction.
	if errors.Is(mergeErr, scm.ErrMergeConflict) {
		branch := task.Status.HeadBranch
		ctxMsg := fmt.Sprintf("Merge conflict on branch `%s`. Rebase the default branch into it, resolve conflicts, and push.", branch)
		if err := r.setImplementContext(ctx, task, ctxMsg); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.clearDeadline(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.maybeMarkHandoverResume(ctx, project, task); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.setLifecycleState(ctx, task, "Implement", "merge-conflict"); err != nil {
			return ctrl.Result{}, err
		}
		// MUST return nil error - not returning the error prevents controller-runtime backoff loop.
		return ctrl.Result{}, nil
	}

	// Transient error: requeue or deadline park.
	if deadlinePassed(task) {
		msg := fmt.Sprintf("lifecycle: merge deadline reached (error: %v) for PR #%d; parking.", mergeErr, number)
		return ctrl.Result{}, r.parkOnDeadline(ctx, task, writer, token, msg)
	}
	return ctrl.Result{RequeueAfter: pollRequeue}, nil
}

// handleMainCI polls the default-branch CI for the merge commit SHA,
// closes the issue on green, and re-enters Implement on failure.
func (r *TaskReconciler) handleMainCI(ctx context.Context, project *tatarav1alpha1.Project, task *tatarav1alpha1.Task) (ctrl.Result, error) {
	_, repo, writer, token, provider, err := r.scmContext(ctx, task)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("mainci: %w", err)
	}

	// Set DeadlineAt on first entry.
	if err := r.ensureDeadline(ctx, task, project); err != nil {
		return ctrl.Result{}, fmt.Errorf("mainci: ensure deadline: %w", err)
	}

	if deadlinePassed(task) {
		return ctrl.Result{}, r.parkOnDeadline(ctx, task, writer, token, "lifecycle: MainCI deadline reached; parking.")
	}

	// Get the CI status for the merge commit.
	sha := task.Status.MergeCommitSHA
	// Guard: an empty SHA means the Merge state wrote the SHA but the status update
	// was lost. Requeue to allow Merge to re-run and populate the SHA rather than
	// polling "" until the deadline parks the task.
	if sha == "" {
		return ctrl.Result{RequeueAfter: pollRequeue}, nil
	}
	if r.ReaderFor == nil {
		return ctrl.Result{RequeueAfter: pollRequeue}, nil
	}
	reader, rerr := r.ReaderFor(provider, token)
	if rerr != nil {
		return ctrl.Result{}, fmt.Errorf("mainci: reader: %w", rerr)
	}
	// Derive the commit-status target provider-aware: GitLab needs the full project
	// path (group/sub/project), GitHub needs owner/repo separately.
	var ciOwner, ciRepo string
	if provider == "gitlab" {
		ciOwner, err = scm.GitLabProjectPath(repo.Spec.URL)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("mainci: parse gitlab project path: %w", err)
		}
		ciRepo = ""
	} else {
		ciOwner, ciRepo, err = scm.OwnerRepo(repo.Spec.URL)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("mainci: parse repo url: %w", err)
		}
	}
	ciStatus, cerr := reader.GetCommitCIStatus(ctx, ciOwner, ciRepo, sha)
	r.recordSCM(provider, "get_commit_ci_status", cerr)
	if cerr != nil {
		// Log at Error: a persistent CI-status read failure can silently burn the
		// MainCI deadline with zero observability. The requeue keeps it non-fatal
		// but the error level surfaces it for alerting (finding 12; the prior comment
		// said WARN but used l.Info - corrected to l.Error).
		log.FromContext(ctx).Error(cerr, "mainci: GetCommitCIStatus failed; requeueing",
			"action", "scm_ci_status_error",
			"resource_id", task.Name,
			"sha", sha,
		)
		return ctrl.Result{RequeueAfter: pollRequeue}, nil
	}

	switch ciStatus {
	case "pending", "":
		return ctrl.Result{RequeueAfter: pollRequeue}, nil

	case "success":
		// Close the originating issue (idempotent: swallow 404 / already-closed).
		if task.Spec.Source != nil && task.Spec.Source.IssueRef != "" && !task.Spec.Source.IsPR {
			repoSlug, _, slugErr := repoSlugFromURL(repo.Spec.URL, provider)
			if slugErr == nil {
				closeStart := time.Now()
				closeErr := writer.CloseIssue(ctx, token, repoSlug, task.Spec.Source.Number, "")
				r.recordSCM(provider, "close_issue", closeErr)
				if closeErr != nil {
					var closeHE *scm.HTTPError
					if !errors.As(closeErr, &closeHE) || (closeHE.Status != 404 && closeHE.Status != 422) {
						return ctrl.Result{}, fmt.Errorf("mainci: close issue: %w", closeErr)
					}
					// 404/422: already closed; continue.
				} else {
					// Log the merge-driven close as a distinct business action (finding 8:
					// the generic lifecycle_transition log from setLifecycleState fires but
					// does not distinguish issue-closed-on-merge from other Done transitions).
					log.FromContext(ctx).Info("mainci: issue closed on merge",
						"action", "scm_issue_closed_on_merge",
						"resource_id", task.Name,
						"number", task.Spec.Source.Number,
						"duration_ms", time.Since(closeStart).Milliseconds())
				}
			}
		}
		// Project the issue-close event onto the ledger: set all source/closes
		// issue entries to state:closed so dedup and backstop see live state.
		// Best-effort: a conflict here does not stall the lifecycle transition.
		_ = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			fresh2 := &tatarav1alpha1.Task{}
			if ferr := r.Get(ctx, client.ObjectKeyFromObject(task), fresh2); ferr != nil {
				return ferr
			}
			closeSourceIssueLedger(fresh2)
			return r.Status().Update(ctx, fresh2)
		})
		if err := r.setLifecycleState(ctx, task, "Done", "mainci-success"); err != nil {
			return ctrl.Result{}, err
		}
		if r.LifecycleMetrics != nil {
			elapsed := time.Since(task.CreationTimestamp.Time)
			r.LifecycleMetrics.ObserveLifecycle(elapsed.Seconds())
		}
		return ctrl.Result{}, nil

	case "failure":
		ctxMsg := fmt.Sprintf("Default-branch pipeline failed after merge (SHA %s). The previous MR is already merged; open a NEW MR with the fix and push.", sha)
		if err := r.setImplementContext(ctx, task, ctxMsg); err != nil {
			return ctrl.Result{}, err
		}
		// The merged PR is closed: clear MergeCommitSHA/PrURL/PRNumber so the next
		// Implement->MRCI->Merge cycle opens and merges a fresh MR instead of
		// short-circuiting on the stale merged PR (writeback AlreadyWritten guard)
		// and stale SHA (handleMerge already-merged guard).
		if err := r.clearMergedChangeState(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.clearDeadline(ctx, task); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.maybeMarkHandoverResume(ctx, project, task); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.setLifecycleState(ctx, task, "Implement", "mainci-failure"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil

	default:
		return ctrl.Result{RequeueAfter: pollRequeue}, nil
	}
}
