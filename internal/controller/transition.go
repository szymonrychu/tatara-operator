package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// EnterStage is THE TRANSITION CHOKE POINT. Every F.3 stage transition the
// operator applies goes through this one function - TaskReconciler's,
// StageDriver's, PodWatchReconciler's, the doc batch's - because SIX things must
// happen on every transition and a call site that forgets one of them is a bug
// that ships:
//
//  1. stage.LegalFor VALIDATES the edge, with the kind guard (a kind=review Task
//     may never reach implementing or merging, by any path) and the C.5.3
//     pendingReview gate. An illegal edge is an ERROR log plus
//     operator_illegal_stage_transition_total{from,to}, and IT DOES NOT HAPPEN.
//  2. stage.Enter stamps stageEnteredAt and CLEARS BOTH podStartedAt AND
//     stageWorkStartedAt (fix V7-4). v6 forgot podStartedAt and it is
//     load-bearing: a stale one leaves the Task under NO CLOCK while it queues on
//     a re-entry edge (clock 1 is armed only when podStartedAt == nil) and puts
//     G.7's t0 = podStartedAt + agentPodTTLSeconds ALREADY IN THE PAST for the
//     next pod, so the operator TTL-stops it before its first turn.
//  3. stats.podRecreations resets (stage.Enter).
//  4. The pod of the stage being LEFT is torn down. The pod name is per-TASK
//     (agent.PodName), not per-stage, so a surviving pod would be silently
//     REUSED by the next stage - running the previous stage's agent kind, model
//     and skills against the new stage's work.
//  5. action=stage_transition is logged (contract K.3).
//  6. operator_task_terminal_total{kind,stage,stageReason} fires on EVERY
//     terminal entry (D1). Twenty-nine tatara-observability rules ride on that
//     counter and it is the ONLY counter of terminal outcomes the platform has.
//     The emit lives HERE, not at the call sites, precisely so the next
//     transition someone adds cannot forget it.
//
// mutate runs INSIDE the objbudget.FitTask closure and BEFORE stage.Enter, so a
// caller can persist its counters (the merge cursor, headMoveReentries,
// deliveredAt) in the SAME status write as the stage. FitTask re-runs the closure
// to size the write and again on every conflict retry: use ABSOLUTE ASSIGNMENTS
// only, never increments. That is also why the metric cannot be emitted from
// inside stage.Enter - it would count once per retry.
//
// mrs are the MergeRequests the Task OWNS; they feed the C.5.3 gate. Pass nil
// when the Task owns none or the edge does not need them.
func EnterStage(ctx context.Context, c client.Client, sp objbudget.Spiller, m *obs.OperatorMetrics,
	task *tatarav1alpha1.Task, mrs []tatarav1alpha1.MergeRequest,
	to, reason string, now time.Time, mutate func(*tatarav1alpha1.Task)) error {

	l := log.FromContext(ctx)
	prev := task.Status.Stage // "" on a MINT: a mint is not an outcome (D1).
	from := prev
	if from == "" {
		from = stage.Create
	}

	// Validate BEFORE the write, so an illegal edge costs one counter and zero
	// API calls, and the Task is left exactly as it was.
	if !stage.LegalFor(task, mrs, from, to) {
		err := &stage.IllegalTransitionError{From: from, To: to}
		obs.IllegalStageTransition(from, to)
		l.Error(err, "illegal stage transition refused",
			"action", "illegal_stage_transition", "resource_id", task.Name,
			"from", from, "to", to, "stage_reason", reason, "kind", task.Spec.Kind)
		return err
	}

	leavingPodStage := stage.AgentKindFor(prev) != ""

	var enterErr error
	key := client.ObjectKeyFromObject(task)
	if err := objbudget.FitTask(ctx, c, sp, key, func(t *tatarav1alpha1.Task) {
		if mutate != nil {
			mutate(t)
		}
		enterErr = stage.Enter(t, mrs, to, reason, now)
	}); err != nil {
		return fmt.Errorf("stage: write task %s: %w", key.Name, err)
	}
	if enterErr != nil {
		var ill *stage.IllegalTransitionError
		if errors.As(enterErr, &ill) {
			// Reachable only when another writer moved the stage between the
			// pre-check and the write. It is still an illegal edge and still a bug.
			obs.IllegalStageTransition(ill.From, ill.To)
		}
		return fmt.Errorf("stage: enter %s(%s) on %s: %w", to, reason, key.Name, enterErr)
	}

	// The caller's in-memory copy follows the write, so it observes the new stage
	// and the cleared clocks without a re-Get.
	if mutate != nil {
		mutate(task)
	}
	if err := stage.Enter(task, mrs, to, reason, now); err != nil {
		return fmt.Errorf("stage: enter %s(%s) in memory: %w", to, reason, err)
	}

	// The pod of the stage we just LEFT. Idempotent; a Task with no pod is a no-op.
	if leavingPodStage {
		if err := agent.DeleteWrapper(ctx, c, key.Namespace, task); err != nil {
			return err
		}
	}

	l.Info("stage transition",
		"action", "stage_transition", "resource_id", task.Name, "task", task.Name,
		"from", from, "to", to, "stage_reason", reason, "kind", task.Spec.Kind)
	m.TaskTerminalEntry(task.Spec.Kind, prev, to, reason)
	if to == tatarav1alpha1.StageParked && prev != "" {
		m.TaskParked(prev, reason)
	}
	emitTerminalTokens(m, task, prev, to)
	return nil
}

// emitTerminalTokens fires operator_task_terminal_tokens_total once, on the
// transition INTO a terminal stage. The emit lived in the old machine's
// setDeployState; that machine is gone, and this choke point is the only place
// that sees EVERY terminal entry (the same reason D1 lives here).
//
// The `outcome` label is now the terminal STAGE (delivered/failed/rejected/...),
// matching D1's relabel onto the stage vocabulary; the old
// delivered/churned/abandoned trichotomy described lifecycle states that no
// longer exist. A mint (prev == "") is not an outcome and never emits.
func emitTerminalTokens(m *obs.OperatorMetrics, task *tatarav1alpha1.Task, prev, to string) {
	if m == nil || prev == "" || !tatarav1alpha1.StageIsTerminalOutcome(to) {
		return
	}
	s := task.Status.Stats
	m.AddTerminalTokens(task.Spec.ProjectRef, task.Spec.RepositoryRef, to, task.Status.ResolvedModel,
		s.TokensInput, s.TokensOutput, s.TokensCacheRead, s.TokensCacheCreation)
}

// enter is TaskReconciler's binding of the choke point.
func (r *TaskReconciler) enter(ctx context.Context, proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task,
	mrs []tatarav1alpha1.MergeRequest, to, reason string, now time.Time) error {
	return EnterStage(ctx, r.Client, r.spiller(proj), r.Metrics, task, mrs, to, reason, now, nil)
}

// spiller resolves the A.7 byte-budget spiller for a project. A nil SpillerFor
// (unit tests) yields a nil Spiller, which objbudget only ever calls when a write
// actually needs to evict.
func (r *TaskReconciler) spiller(proj *tatarav1alpha1.Project) objbudget.Spiller {
	if r.SpillerFor == nil || proj == nil {
		return nil
	}
	return r.SpillerFor(proj)
}

// mrReader is the UNCACHED API reader when wired, else the cached client. The
// review-handoff re-drive reads owned MergeRequests through it so it never
// advances a Task off a cache that lags a fresh /outcome's pendingReview write.
// Nil APIReader (unit tests) falls back to the cached client.
func (r *TaskReconciler) mrReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}
