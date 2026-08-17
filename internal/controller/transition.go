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

// EnterStage is THE TRANSITION CHOKE POINT. Every state transition the operator
// applies goes through this one function - TaskReconciler's, StageDriver's,
// PodWatchReconciler's, the doc batch's - because SEVEN things must happen on
// every transition and a call site that forgets one of them is a bug that ships:
//
//  1. stage.LegalFor VALIDATES the edge, with the kind guard (a kind=review Task
//     may never reach under-implementation or merged, by any path) and the C.5.3
//     pendingReview gate. An illegal edge is an ERROR log plus
//     operator_illegal_stage_transition_total{from,to}, and IT DOES NOT HAPPEN.
//     An edge that was LEGAL from the caller's from and is illegal only from
//     the fresher from the in-write re-read found is a LOST RACE, not a table
//     bug: it is dropped silently onto operator_stage_race_lost_total instead
//     (issue #478).
//  2. stage.Enter stamps stateEnteredAt and CLEARS BOTH podStartedAt AND
//     stateWorkStartedAt (fix V7-4). v6 forgot podStartedAt and it is
//     load-bearing: a stale one leaves the Task under NO CLOCK while it queues on
//     a re-entry edge (clock 1 is armed only when podStartedAt == nil) and puts
//     G.7's t0 = podStartedAt + agentPodTTLSeconds ALREADY IN THE PAST for the
//     next pod, so the operator TTL-stops it before its first turn.
//  3. stats.podRecreations resets (stage.Enter).
//  4. The pod of the state being LEFT is torn down. The pod name is per-TASK
//     (agent.PodName), not per-state, so a surviving pod would be silently
//     REUSED by the next state - running the previous state's agent kind, model
//     and skills against the new state's work.
//  5. action=stage_transition is logged (contract K.3).
//  6. operator_task_terminal_total{kind,state,reason} fires on EVERY terminal
//     entry (D1). Twenty-nine tatara-observability rules ride on that counter and
//     it is the ONLY counter of terminal outcomes the platform has. The emit
//     lives HERE, not at the call sites, precisely so the next transition someone
//     adds cannot forget it.
//  7. A TERMINAL destination PARKS every still-open Issue the Task owns - the
//     terminal notice comment plus the tatara-parked label (C.2). It lives here
//     for the same reason 6 does: the reap path used to cover `rejected` and not
//     `done`, so a delivered Task's still-open issue got nothing at all, and
//     since 4ee5c7f the sweep released its ownerRef anyway - MintStage fell
//     through its label gate and re-minted the issue ACTIVE, with a pod, every
//     pass, unbounded. It needs an SCM writer, which this free function cannot
//     manufacture, so it arrives as the WithTerminalIssueRelease option and is
//     a no-op when a caller does not pass one.
//
// IT REFUSES A PARKED TASK, through stage.Enter's own guard. Parking is
// orthogonal to state now (#521), so a caller that wants a parked Task to move
// must un-park it first - through ApplyUnpark or stage.UnparkTakeover, the only
// two functions that clear the flag. Nothing here silently clears it.
//
// mutate runs INSIDE the objbudget.FitTask closure and BEFORE stage.Enter, so a
// caller can persist its counters (the merge cursor, headMoveReentries,
// deliveredAt) in the SAME status write as the state. FitTask re-runs the closure
// to size the write and again on every conflict retry: use ABSOLUTE ASSIGNMENTS
// only, never increments. That is also why the metric cannot be emitted from
// inside stage.Enter - it would count once per retry.
//
// mrs are the MergeRequests the Task OWNS; they feed the C.5.3 gate. Pass nil
// when the Task owns none or the edge does not need them.
//
// The body is enterStage, shared with MintParked, which is the same six things
// plus the create edge's park in the same status write.
func EnterStage(ctx context.Context, c client.Client, sp objbudget.Spiller, m *obs.OperatorMetrics,
	task *tatarav1alpha1.Task, mrs []tatarav1alpha1.MergeRequest,
	to, reason string, now time.Time, mutate func(*tatarav1alpha1.Task), opts ...EnterOption) error {

	return enterStage(ctx, c, sp, m, task, mrs, to, reason, "", now, mutate, opts...)
}

// MintParked is THE CREATE EDGE'S ONE WRITE: the minted state and the mint's
// park reason land in the SAME status update.
//
// A Task carrying Spec.InitialParkReason was minted TO BE PARKED, and applying
// the two as separate writes puts a window between them in which the Task is
// LIVE with no park reason. A crash, a conflict retry or an eviction inside
// that window leaves it there for good, and a live Task is picked up and
// worked: the sweep alone mints 52 parked(backlog-sweep) owners through this
// edge, so the window is 52 agent pods that were never meant to spawn. This is
// the exact non-atomicity that got annotations rejected as a representation for
// the park - park and reason must be one write on the status subresource - and
// #521's create edge reintroduced the split everywhere else had removed.
//
// IT IS THE MINT'S ONLY, and refuses anything else, which is what keeps D1
// intact. A MINT IS NOT AN OUTCOME: those 52 owners never ran and never gave
// up, and counting them would drown the park-rate alert. parkAtMint used to buy
// that exclusion by passing nil metrics by hand; here the park counter is
// simply not emitted on this path at all, and the state != "" refusal is what
// guarantees no genuine park can reach it and go uncounted.
func MintParked(ctx context.Context, c client.Client, sp objbudget.Spiller, m *obs.OperatorMetrics,
	task *tatarav1alpha1.Task, to, parkReason string, now time.Time) error {

	if task.Status.State != "" {
		return fmt.Errorf("stage: MintParked on %s is not a mint: it is already at %s",
			task.Name, task.Status.State)
	}
	if !stage.IsParkReason(parkReason) {
		err := &stage.UnknownReasonError{Reason: parkReason}
		log.FromContext(ctx).Error(err, "mint park refused: reason is not in the park vocabulary",
			"action", "illegal_park", "resource_id", task.Name,
			"state", to, "park_reason", parkReason, "kind", task.Spec.Kind)
		return err
	}
	return enterStage(ctx, c, sp, m, task, nil, to, "", parkReason, now, nil)
}

// enterStage is EnterStage's body, plus the optional park that makes the mint
// one write. parkReason == "" is the ordinary transition; non-empty parks the
// Task at `to` in the same status update, and only MintParked passes it.
func enterStage(ctx context.Context, c client.Client, sp objbudget.Spiller, m *obs.OperatorMetrics,
	task *tatarav1alpha1.Task, mrs []tatarav1alpha1.MergeRequest,
	to, reason, parkReason string, now time.Time, mutate func(*tatarav1alpha1.Task),
	opts ...EnterOption) error {

	l := log.FromContext(ctx)
	var eo enterOpts
	for _, opt := range opts {
		opt(&eo)
	}
	prev := task.Status.State // "" on a MINT: a mint is not an outcome (D1).
	from := prev
	if from == "" {
		from = stage.Create
	}

	// A to==from re-entry is a silent no-op (issue #403), not an illegal edge:
	// stage.Enter's side effects (re-stamping StateEnteredAt, clearing
	// PodStartedAt/PodRecreations) are non-idempotent, so re-applying them here
	// would corrupt clocks already in flight. No self-edges are added to the
	// table for this - it is handled entirely at this choke point.
	if to == from {
		l.Info("state re-entry is a no-op",
			"action", "stage_reentry_noop", "resource_id", task.Name,
			"state", to, "state_reason", reason, "kind", task.Spec.Kind)
		return nil
	}

	// Validate BEFORE the write, so an illegal edge costs one counter and zero
	// API calls, and the Task is left exactly as it was.
	if !stage.LegalFor(task, mrs, from, to) {
		err := &stage.IllegalTransitionError{From: from, To: to}
		obs.IllegalStageTransition(from, to)
		l.Error(err, "illegal state transition refused",
			"action", "illegal_stage_transition", "resource_id", task.Name,
			"from", from, "to", to, "state_reason", reason, "kind", task.Spec.Kind)
		return err
	}

	leavingPodStage := stage.AgentKindFor(prev, task.Spec.Kind) != ""

	var enterErr error
	key := client.ObjectKeyFromObject(task)
	if err := objbudget.FitTask(ctx, c, sp, key, func(t *tatarav1alpha1.Task) {
		if mutate != nil {
			mutate(t)
		}
		if eo.mrTerminal {
			// THE SAME MUTATION AGAIN, and for the mirror-image reason the park
			// below is: the un-park and the terminal state reach the API server as
			// ONE status write, so no window exists in which this Task is live,
			// un-parked and non-terminal. BEFORE Enter, not after - Enter is what
			// refuses a parked Task. internal/stage holds every guard; this only
			// arms the attempt.
			if enterErr = stage.UnparkForMRTerminal(t, to, reason); enterErr != nil {
				return
			}
		}
		enterErr = stage.Enter(t, mrs, to, reason, now)
		if enterErr == nil && parkReason != "" {
			// THE SAME MUTATION, so the state and the park reach the API server
			// as ONE status update. stage.Park after stage.Enter, never before:
			// Enter refuses a parked Task by design, so a closure that parked
			// first could not take its own edge.
			enterErr = stage.Park(t, parkReason, now)
		}
	}); err != nil {
		return fmt.Errorf("stage: write task %s: %w", key.Name, err)
	}
	if enterErr != nil {
		var parked *stage.StillParkedError
		if errors.As(enterErr, &parked) {
			// The fresh read inside FitTask found the Task PARKED. Another writer
			// parked it between this caller's decision and the write, so the
			// caller's intent is stale in exactly the way a lost race is stale:
			// the winner stalled the Task and the loser must not un-stall it by
			// side effect. Dropped, counted, not an error.
			l.Info("state transition dropped: the task is parked",
				"action", "stage_race_lost", "resource_id", task.Name,
				"state", parked.State, "park_reason", parked.ParkReason, "to", to,
				"kind", task.Spec.Kind)
			obs.StageRaceLost(parked.State, to)
			return nil
		}
		var ill *stage.IllegalTransitionError
		if errors.As(enterErr, &ill) {
			if ill.From == ill.To {
				// The actual production race (issue #403): another writer
				// committed this exact target state between our pre-check and
				// the write, so the fresh read inside FitTask derives a
				// redundant from==to edge. Still a silent no-op, not a bug.
				l.Info("state re-entry is a no-op",
					"action", "stage_reentry_noop", "resource_id", task.Name,
					"state", to, "state_reason", reason, "kind", task.Spec.Kind)
				return nil
			}
			if ill.From != from {
				// THE LOST RACE (issue #478). The edge this caller computed was
				// LEGAL from the state it pre-checked, and the pre-check above
				// proved it. Another writer moved the state between that check
				// and this write, so the edge is illegal only from the state the
				// Task now ACTUALLY has - the caller's intent is stale, not
				// wrong. Two controllers legitimately racing for the same exit
				// (the task controller's review_finalize_terminal_mr against the
				// mergerequest controller's review_advance, in the incident that
				// found this) is not a code bug, and it must not be charged to
				// operator_illegal_stage_transition_total, whose alert declares
				// ANY non-zero value a bug in the transition table. That counter
				// keeps its documented meaning: the from it counts is the from
				// the caller really had.
				//
				// Dropping the loser is the correct outcome, not merely the
				// convenient one: the winner wrote a state this caller never
				// evaluated, so re-applying an edge derived from the old one
				// would overwrite a decision made on fresher facts. Whoever
				// still owes work re-derives it from the winner's state on the
				// next reconcile - every state exit is level-triggered.
				l.Info("state transition lost a race and was dropped",
					"action", "stage_race_lost", "resource_id", task.Name,
					"expected_from", from, "live_from", ill.From, "to", to,
					"state_reason", reason, "kind", task.Spec.Kind)
				obs.StageRaceLost(ill.From, ill.To)
				return nil
			}
			// The caller's own from was live and the edge is still not in the
			// table: a genuine table bug, which is exactly what the counter
			// is for.
			obs.IllegalStageTransition(ill.From, ill.To)
		}
		return fmt.Errorf("stage: enter %s(%s) on %s: %w", to, reason, key.Name, enterErr)
	}

	// The caller's in-memory copy follows the write, so it observes the new state
	// and the cleared clocks without a re-Get.
	if mutate != nil {
		mutate(task)
	}
	if eo.mrTerminal {
		if err := stage.UnparkForMRTerminal(task, to, reason); err != nil {
			return fmt.Errorf("stage: un-park %s(%s) in memory: %w", to, reason, err)
		}
	}
	if err := stage.Enter(task, mrs, to, reason, now); err != nil {
		return fmt.Errorf("stage: enter %s(%s) in memory: %w", to, reason, err)
	}
	if parkReason != "" {
		if err := stage.Park(task, parkReason, now); err != nil {
			return fmt.Errorf("stage: park %s in memory: %w", parkReason, err)
		}
	}

	// The pod of the state we just LEFT. Idempotent; a Task with no pod is a
	// no-op. A PARK tears one down too - a parked Task holding a live pod burns
	// an admission slot with no clock armed - which is why ParkTask does the
	// same, and why the park path here does not lean on leavingPodStage.
	if leavingPodStage || parkReason != "" {
		if err := agent.DeleteWrapper(ctx, c, key.Namespace, task); err != nil {
			return err
		}
	}

	// The Task's PERSISTENT WORKSPACE volume, and this is a SEPARATE `if` on
	// purpose - never fold it into the block above, however tempting the shared
	// condition looks.
	//
	// That block also runs for a PARK, and ParkTask tears the pod down by the
	// same reasoning. Parked Tasks are roughly two thirds of the live
	// population and a parked Task can UNPARK AND RESUME, so deleting its
	// workspace would destroy exactly the committed work this volume exists to
	// keep. Only a TERMINAL OUTCOME ({done, rejected}) retires it: a Task that
	// reached one will never run another pod.
	if tatarav1alpha1.TaskIsTerminalOutcome(to) {
		if err := deleteWorkspacePVC(ctx, c, key.Namespace, task); err != nil {
			return err
		}
	}

	// STEP 7 (C.2): A TERMINAL TASK NEVER SILENTLY DROPS AN OPEN ISSUE. Every
	// still-open Issue it owns gets the terminal notice and the tatara-parked
	// label here, at the choke point, rather than only on whatever reap branch
	// happens to cover this state. It runs AFTER the write and is non-fatal -
	// see releaseTerminalIssues for why, and for which half of B.6 the reap pass
	// deliberately keeps.
	releaseTerminalIssues(ctx, eo, task, to)

	l.Info("state transition",
		"action", "stage_transition", "resource_id", task.Name, "task", task.Name,
		"from", from, "to", to, "state_reason", reason, "kind", task.Spec.Kind)
	if parkReason != "" {
		// action=task_parked is logged exactly as ParkTask logs it, so a park is
		// greppable by one action whichever choke point applied it. The COUNTER
		// is not emitted: see MintParked, the only caller.
		l.Info("task parked",
			"action", "task_parked", "resource_id", task.Name, "task", task.Name,
			"state", to, "park_reason", parkReason, "kind", task.Spec.Kind)
	}
	m.TaskTerminalEntry(task.Spec.Kind, prev, to, reason)
	emitTerminalTokens(m, task, prev, to)
	return nil
}

// ParkTask is THE PARK CHOKE POINT, and it is EnterStage's sibling: park is now
// a flag orthogonal to state (#521), so parking is no longer an edge and cannot
// go through EnterStage at all. The same things must happen every time:
//
//  1. stage.Park stamps the WHOLE tuple atomically - parkReason, parkedAt,
//     parkedFromState, and this state's residency folded into the carry. A park
//     that is not atomic with its reason is the #521 bug shape.
//  2. THE POD OF THE PARKED STATE IS TORN DOWN. Park is what takes the pod down;
//     a parked Task holding a live pod burns an admission slot with NO clock
//     armed, which is the invariant repairParkedWithLivePod exists to repair
//     when this path is interrupted.
//  3. action=task_parked is logged and operator_task_parked_total fires.
//
// It is IDEMPOTENT: stage.Park keeps the first reason, so a second park is a
// no-op write and no second metric.
//
// mutate has the same contract as EnterStage's: absolute assignments only, no
// increments, because FitTask re-runs it on every conflict retry.
func ParkTask(ctx context.Context, c client.Client, sp objbudget.Spiller, m *obs.OperatorMetrics,
	task *tatarav1alpha1.Task, reason string, now time.Time, mutate func(*tatarav1alpha1.Task)) error {

	l := log.FromContext(ctx)
	if !stage.IsParkReason(reason) {
		err := &stage.UnknownReasonError{Reason: reason}
		l.Error(err, "park refused: reason is not in the park vocabulary",
			"action", "illegal_park", "resource_id", task.Name,
			"state", task.Status.State, "park_reason", reason, "kind", task.Spec.Kind)
		return err
	}
	if tatarav1alpha1.Parked(task) {
		return nil // already parked; the first reason wins and there is nothing to do
	}
	from := task.Status.State

	var parkErr error
	key := client.ObjectKeyFromObject(task)
	if err := objbudget.FitTask(ctx, c, sp, key, func(t *tatarav1alpha1.Task) {
		if mutate != nil {
			mutate(t)
		}
		parkErr = stage.Park(t, reason, now)
	}); err != nil {
		return fmt.Errorf("stage: park write task %s: %w", key.Name, err)
	}
	if parkErr != nil {
		return fmt.Errorf("stage: park %s on %s: %w", reason, key.Name, parkErr)
	}

	if mutate != nil {
		mutate(task)
	}
	if err := stage.Park(task, reason, now); err != nil {
		return fmt.Errorf("stage: park %s in memory: %w", reason, err)
	}

	if err := agent.DeleteWrapper(ctx, c, key.Namespace, task); err != nil {
		return err
	}

	// STEP 2b, and it belongs to step 2: the turn annotations are POD-scoped
	// (see turn_annotations.go), so the same call that takes the pod down retires
	// the turn that was running in it. Park is the natural owner - the turn is
	// over by definition - and this is THE park choke point, so every park in the
	// operator gets it: awaiting-human, the live-pod eviction, pod-recreation
	// exhausted, admission-starved, the podwatch failure paths, all of them.
	//
	// It is NOT sufficient on its own, and the reason is the idempotence guard at
	// the top of this function. A park whose status write lands but whose
	// teardown fails is re-driven as an already-parked Task and returns before it
	// reaches here - the same hole repairParkedWithLivePod exists to plug for the
	// pod itself. PollOnce carries the matching backstop for these annotations.
	if err := clearTurnAnnotations(ctx, c, task); err != nil {
		return fmt.Errorf("stage: clear turn annotations on parked task %s: %w", key.Name, err)
	}

	l.Info("task parked",
		"action", "task_parked", "resource_id", task.Name, "task", task.Name,
		"state", from, "park_reason", reason, "kind", task.Spec.Kind)
	m.TaskParked(from, reason)
	return nil
}

// UpgradeDeclinedTakeoverPark is the write half of
// stage.UpgradeDeclineToOwnershipLost: a kind=takeover Task parked
// implement-declined, whose merge request has just gone back to its human, is
// re-stamped parked(ownership-lost) so the re-take endpoint and
// DrainStandDownMerge can still reach it (#604 review).
//
// It is deliberately NOT ParkTask with a different reason - ParkTask returns
// early on an already-parked Task, which is the very early return that produced
// the bug. Nor does it need ParkTask's other two jobs: the Task has been parked
// since the decline, so the pod teardown and the turn-annotation clear are
// somebody else's already. Note that the DECLINE park does not run through
// ParkTask either - restapi/outcome.go tears the wrapper down best-effort and
// swallows the error, and never touches the turn annotations - so what actually
// covers those here is the same pair that covers an interrupted ParkTask:
// repairParkedWithLivePod for the pod, PollOnce for the annotations. The only
// thing THIS function does is the status write.
//
// The park counter is NOT re-fired. This is one park's reason being corrected,
// not a second park, and double-counting it would overstate
// operator_task_parked_total against a real park elsewhere. The upgrade has its
// own counter, which is the honest series to ask about it - and it fires only on
// the write that actually changed something, so a converge-by-retry caller
// cannot inflate it.
//
// The guard runs against the CALLER's task before any write, not only inside the
// FitTask closure. FitTask re-runs the closure against a fresh server copy, so a
// refusal decided in there has already been preceded by a Status().Update - the
// caller would be handed an error after a write it cannot see, and could not
// tell whether to retry. This function is exported; a future call site must not
// have to know that.
func UpgradeDeclinedTakeoverPark(ctx context.Context, c client.Client, sp objbudget.Spiller,
	task *tatarav1alpha1.Task, now time.Time) error {

	if _, err := stage.UpgradeDeclineToOwnershipLost(task.DeepCopy(), now); err != nil {
		return fmt.Errorf("stage: upgrade declined takeover park on %s: %w", task.Name, err)
	}

	var changed bool
	var upErr error
	key := client.ObjectKeyFromObject(task)
	if err := objbudget.FitTask(ctx, c, sp, key, func(t *tatarav1alpha1.Task) {
		changed, upErr = stage.UpgradeDeclineToOwnershipLost(t, now)
	}); err != nil {
		return fmt.Errorf("stage: upgrade declined takeover park on %s: %w", key.Name, err)
	}
	if upErr != nil {
		return fmt.Errorf("stage: upgrade declined takeover park on %s: %w", key.Name, upErr)
	}
	if !changed {
		return nil // already converged; a retry of a write that landed
	}
	// AFTER the converged return, not before: a pass that wrote nothing must
	// mutate nothing. Stamping here on the converged path would put ParkedAt=now
	// on the caller's copy while the server still holds the FIRST call's stamp -
	// a timestamp that exists on no object anywhere.
	if _, err := stage.UpgradeDeclineToOwnershipLost(task, now); err != nil {
		return fmt.Errorf("stage: upgrade declined takeover park in memory: %w", err)
	}

	log.FromContext(ctx).Info("declined takeover park upgraded to ownership-lost",
		"action", "takeover_decline_upgraded", "resource_id", task.Name, "task", task.Name,
		"state", task.Status.State, "park_reason", stage.ReasonOwnershipLost, "kind", task.Spec.Kind)
	obs.OwnershipDeclineUpgradedTotal.Inc()
	return nil
}

// emitTerminalTokens fires operator_task_terminal_tokens_total once, on the
// transition INTO a terminal state. The emit lived in the old machine's
// setDeployState; that machine is gone, and this choke point is the only place
// that sees EVERY terminal entry (the same reason D1 lives here).
//
// The `outcome` label is the terminal STATE (done/rejected). A mint
// (prev == "") is not an outcome and never emits.
func emitTerminalTokens(m *obs.OperatorMetrics, task *tatarav1alpha1.Task, prev, to string) {
	if m == nil || prev == "" || !tatarav1alpha1.TaskIsTerminalOutcome(to) {
		return
	}
	s := task.Status.Stats
	m.AddTerminalTokens(task.Spec.ProjectRef, task.Spec.RepositoryRef, to, task.Status.ResolvedModel,
		s.TokensInput, s.TokensOutput, s.TokensCacheRead, s.TokensCacheCreation)
}

// enter is TaskReconciler's binding of the transition choke point.
func (r *TaskReconciler) enter(ctx context.Context, proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task,
	mrs []tatarav1alpha1.MergeRequest, to, reason string, now time.Time) error {
	return EnterStage(ctx, r.Client, r.spiller(proj), r.Metrics, task, mrs, to, reason, now, nil,
		WithTerminalIssueRelease(&TerminalReleaser{Client: r.Client, SCMFor: r.SCMFor, Metrics: r.Metrics}))
}

// park is TaskReconciler's binding of the park choke point.
func (r *TaskReconciler) park(ctx context.Context, proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task,
	reason string, now time.Time) error {
	return ParkTask(ctx, r.Client, r.spiller(proj), r.Metrics, task, reason, now, nil)
}

// enterParkedAtMint is TaskReconciler's binding of the create edge's ONE write:
// the minted state and Spec.InitialParkReason in a single status update. See
// MintParked for why it cannot be two.
func (r *TaskReconciler) enterParkedAtMint(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, to, parkReason string, now time.Time) error {
	return MintParked(ctx, r.Client, r.spiller(proj), r.Metrics, task, to, parkReason, now)
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
