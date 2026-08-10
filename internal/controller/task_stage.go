package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/prompt"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

const (
	// annPodStage names the stage a wrapper Pod was created FOR. The pod name is
	// per-TASK (agent.PodName), not per-stage, so without this a Task advancing
	// implementing -> reviewing would find its implement pod still Running under
	// the review pod's name and REUSE it: the review would run the implement
	// agent's kind, model and skills. An /outcome transition is applied by the
	// REST layer, which does not own pods, so this is where that pod dies.
	annPodStage = "tatara.dev/pod-stage"
	// annStageTurn0 records that turn-0 has been submitted for a specific
	// (stage, podStartedAt) pair. It is not a boolean: a respawn re-stamps
	// podStartedAt, and the fresh pod holds a fresh claude session that has never
	// seen the bundle.
	annStageTurn0 = "tatara.dev/stage-turn0"

	// stageRequeue paces a stage waiting on something external (admission, a pod
	// booting, the StageReconciler's merge poll).
	stageRequeue = 30 * time.Second
	// admissionRequeue paces a Task sitting in the admission queue. CLOCK 1 is a
	// 24h budget; polling it at 30s across a queued backlog is pure churn, and the
	// dispatcher watches Tasks anyway, so admission is event-driven with a slow
	// safety poll.
	admissionRequeue = 5 * time.Minute
)

// taskMaxTurns and taskMaxPodRecreations are DELETED (O3). They were the last
// readers of Project.spec.agent.maxTurnsPerTask / .maxPodRecreations, which are
// now Deprecated fields with zero readers: the ceilings they fed parked working
// agents, and what replaced them is stage.ResidencyCapAll (24h, a constant) plus
// the operator_pod_recreations_total churn alert.

// projectPaused is Project.spec.maxConcurrentAgents == 0, the kill switch. It
// disarms CLOCK 1 (and the pod-less `approved` stage's admission budget, which is
// the same clock by another name). Without the carve-out the pause is a backlog
// shredder: every Task waiting for a slot parks at admission-starved 24h later.
func projectPaused(proj *tatarav1alpha1.Project) bool {
	return proj != nil && proj.Spec.MaxConcurrentAgents == 0
}

// mrBindingBackstopGrace is issue #381 bug B part 2's grace window: how long
// a Source-bearing Task may sit with zero owned MergeRequest and zero owned
// Issue CRs before the backstop treats it as an interrupted two-phase mint
// rather than a live, merely-slow bind. Same value as HandoffDeadline
// (api/v1alpha1/constants.go:97) for the same reason - long enough that a
// live write completes, short enough to catch a genuinely stuck Task inside
// one human work session - but a DISTINCT constant: the two guard unrelated
// predicates and must be free to diverge later.
const mrBindingBackstopGrace = 5 * time.Minute

// reconcileClocks is GAP 5: the F.4 three-clock driver. Nothing else calls
// stage.ArmedClock in production, so without it "the 2h budget parks it", "the
// 24h admission clock" and "merging reaches merge-timeout at 4h" are all fiction
// and F.4's "no stage without an exit deadline" invariant is unimplemented.
//
// It returns handled=true when it applied an edge (or the reaper owns the Task
// and there is nothing to do), in which case the caller returns res.
func (r *TaskReconciler) reconcileClocks(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, now time.Time) (res ctrl.Result, handled bool, err error) {

	l := log.FromContext(ctx)

	// THE CI HOLD (PR B, gate site 2). It runs FIRST, ahead of every other clock,
	// for the same reason the handoff deadline runs ahead of the three: a held
	// Task has an accepted outcome and a suppressed pod, and every clock below
	// would be measuring a stage whose agent is already finished. It always
	// reports handled, so nothing downstream ever sees a held Task.
	if task.Status.CIWaitSince != nil {
		return r.reconcileCIWait(ctx, proj, task, now)
	}

	// EXTERNAL-TERMINAL FINALIZE (fixes #33; widened past kind=review by #578). A
	// Task sitting at awaiting-review whose owned PRs were merged/closed by a
	// human has NO legal outcome: submit_outcome answered "no open MR", the idle
	// pod was reaped and respawned, and it re-reviewed the terminal PR until the
	// budget parked it. This runs UNCONDITIONALLY - it must fire even when no
	// outcome ever committed (so no handoffCondition is armed), which is exactly
	// the #33 shape - and it reuses the shared externalTerminalEdge so this path,
	// the pre-dispatch guard and reviewAdvanceEdge can never disagree.
	//
	// THE KIND GATE MOVED INTO THE EDGE RESOLUTION (#578). It used to sit here,
	// which is what left a kind=issue Task at awaiting-review - running an
	// agentKind=review pod, because AgentKindFor keys that state on the STATE -
	// with no finalize at all once its own MR merged out of band.
	if task.Status.State == tatarav1alpha1.StateAwaitingReview {
		mrs, mrErr := ownedMergeRequests(ctx, r.mrReader(), task)
		if mrErr != nil {
			return ctrl.Result{}, true, mrErr
		}
		if edge, ok := externalTerminalEdge(task, mrs); ok {
			l.Info("task finalized at awaiting-review: every owned MR reached a terminal forge state externally",
				"action", "review_finalize_terminal_mr", "resource_id", task.Name,
				"kind", task.Spec.Kind, "to", edge.To, "reason", edge.Reason)
			return ctrl.Result{}, true, r.enter(ctx, proj, task, mrs, edge.To, edge.Reason, now)
		}
		// TAKEN-OVER PARENT FINALIZE (the takeover sibling of the #33 shape). A
		// maintainer takeover moved the MR mirror's controller ownership onto a
		// takeover Task, so this parent review controller-owns zero MRs (terminalMREdge
		// above cannot fire on the empty set) yet its status.mrRefs are now owned by a
		// different live Task. It has no outcome to post and no lifecycle left: retire
		// it pod-lessly at rejected(mr-taken-over). Runs UNCONDITIONALLY, like the #33
		// finalize, so it fires even when no outcome ever committed.
		if len(mrs) == 0 {
			over, oerr := TaskTakenOver(ctx, r.mrReader(), task)
			if oerr != nil {
				return ctrl.Result{}, true, oerr
			}
			if over {
				l.Info("review task finalized: its review target was taken over by a maintainer; the takeover task now owns it",
					"action", "review_finalize_taken_over", "resource_id", task.Name,
					"to", tatarav1alpha1.StateRejected, "reason", stage.ReasonMRTakenOver)
				return ctrl.Result{}, true, r.enter(ctx, proj, task, mrs,
					tatarav1alpha1.StateRejected, stage.ReasonMRTakenOver, now)
			}
		}
		// THE RED-CI GATE AT AWAITING-REVIEW (PR B, gate site 3). Until now the
		// gate fired only on edge.To == merged, so a pipeline that went red while
		// a review pod was reading the diff cost the whole review round before
		// anything noticed. It bounces here instead, to the agent that can fix it.
		//
		// THE MIRROR IS THE TRIGGER, THE FORGE IS THE VERDICT. This block runs on
		// every awaiting-review reconcile - a 30s requeue - so a live read here
		// unconditionally would be one forge call per reviewing Task per 30s. The
		// mirror's ciStatus is what PR A made real (webhook stamp plus a 5m
		// backstop), so it costs nothing to consult and only a mirror that says
		// RED pays for ciRedAtReviewedHead's live confirmation.
		//
		// IT FAILS OPEN on a read error, like every non-merging site: the merge
		// corridor re-reads within 60s and is the gate that must hold.
		if ciMirrorVerdict(mrs) == ciVerdictRed {
			red, cerr := ciRedAtReviewedHead(ctx, r.Client, r.SCMFor, r.Metrics, proj, mrs)
			switch {
			case cerr != nil:
				l.Error(cerr, "review: the mirror reports RED but live CI could not be read; leaving the task at awaiting-review (the merging gate re-checks every 60s)",
					"action", "ci_red_check_failed", "resource_id", task.Name, "kind", task.Spec.Kind)
			case red != nil:
				l.Info("ci gate: the checks went RED while the task sat at awaiting-review; bouncing it back to the agent",
					"action", "ci_red_at_review", "resource_id", task.Name, "kind", task.Spec.Kind,
					"repo", red.Spec.RepositoryRef, "pr", red.Spec.Number)
				return ctrl.Result{}, true, enterCIRed(ctx, r.Client, r.spiller(proj), r.Metrics, task, mrs, red, now)
			}
		}
	}

	// B4: THE HANDOFF DEADLINE, evaluated BEFORE the three clocks because it is
	// tighter than all of them.
	//
	// kind=review is the ONE outcome kind whose commit makes no stage transition:
	// C.5.3 phase 1 is pure etcd with no forge write, so a forge outage cannot lose
	// an accepted outcome, and the advance is deferred to MergeRequestReconciler ->
	// DrainPendingReview -> advanceAfterReview. That split is deliberate and is NOT
	// being removed. But it makes the Task's progress depend on a SECOND reconciler
	// running, and nothing bounded that: the drain normally lands in ~1s, the
	// reviewing work budget is 4h, the B2 guards suppress the pod caps underneath
	// it, and a suppressed Task holds its admitted concurrency ticket for the whole
	// window. This is the bound.
	if cond := handoffCondition(task); cond != nil {
		// The owned-MR read is LIVE (uncached APIReader), not the informer cache.
		// /outcome persists the MR pendingReview BEFORE it stamps the Task's
		// OutcomeAccepted condition that arms handoffCondition, so once we are here
		// a live read is guaranteed to observe a still-owed pendingReview; a CACHED
		// read could lag the MR watch and show a pre-/outcome nil, which would
		// advance the Task before its review was ever posted.
		mrs, mrErr := ownedMergeRequests(ctx, r.mrReader(), task)
		if mrErr != nil {
			return ctrl.Result{}, true, mrErr
		}
		// RE-DRIVE the deferred reviewing->advance BEFORE enforcing the deadline.
		// The advance in DrainPendingReview is EDGE-triggered - a one-shot at the
		// tail of the drain - and every later MergeRequest reconcile short-circuits
		// at the already-nil pendingReview and never re-attempts it. If that one
		// shot missed (a stale cached owned-MR read, a transient controller-owner
		// sever, or a multi-MR ordering race), nothing else re-evaluates the advance
		// and the Task would sit out the whole HandoffDeadline and park
		// handoff-stalled even though its review already landed. This is the
		// level-triggered backstop: on every reviewing reconcile, if every owned MR
		// has drained its pendingReview, take the F.3 edge NOW. Idempotent -
		// EnterStage refuses a redundant X->X - so a race with the MR reconciler's
		// own advance costs at most one illegal-edge counter, never a double
		// transition.
		if edge, ready := reviewAdvanceEdge(task, mrs); ready {
			// The SAME red-CI gate the edge-triggered advance applies (issue #476),
			// and it FAILS OPEN here for a sharper reason than it does there: this
			// block runs BEFORE the HandoffDeadline below and BEFORE stage.ArmedClock,
			// so returning an error from it is a Task that reaches neither - wedged in
			// reviewing, holding its admitted concurrency ticket, in the one function
			// whose whole job is that no stage lacks an exit deadline.
			if edge.To == tatarav1alpha1.StateMerged {
				red, cerr := ciRedAtReviewedHead(ctx, r.Client, r.SCMFor, r.Metrics, proj, mrs)
				switch {
				case cerr != nil:
					l.Error(cerr, "review: could not read live CI at the reviewed head; advancing anyway (the merging gate re-checks every 60s)",
						"action", "ci_red_check_failed", "resource_id", task.Name, "kind", task.Spec.Kind)
				case red != nil:
					return ctrl.Result{}, true, enterCIRed(ctx, r.Client, r.spiller(proj), r.Metrics, task, mrs, red, now)
				}
			}
			l.Info("review handoff re-driven: the deferred advance had not fired; advancing off awaiting-review",
				"action", "handoff_redriven", "resource_id", task.Name, "state", task.Status.State,
				"to", edge.To, "reason", edge.Reason, "kind", task.Spec.Kind)
			// The SAME park branch advanceAfterReview carries, and for the same
			// reason: reviewAdvanceEdge returns stage.ParkTarget for every
			// kind=review verdict and for the exhausted/refused non-review ones,
			// and EnterStage refuses it by design.
			if edge.To == stage.ParkTarget {
				return ctrl.Result{}, true, r.park(ctx, proj, task, edge.Reason, now)
			}
			return ctrl.Result{}, true, r.enter(ctx, proj, task, mrs, edge.To, edge.Reason, now)
		}
		if elapsed := now.Sub(cond.LastTransitionTime.Time); elapsed > tatarav1alpha1.HandoffDeadline {
			l.Info("review handoff stalled: the outcome committed but the drain never advanced the task",
				"action", "handoff_stalled", "resource_id", task.Name, "state", task.Status.State,
				"outcome_reason", cond.Reason, "deadline", tatarav1alpha1.HandoffDeadline.String(),
				"elapsed", elapsed.String())
			return ctrl.Result{}, true, r.park(ctx, proj, task, stage.ReasonHandoffStalled, now)
		}
	}

	// THE ABSOLUTE RESIDENCY BOUND. It runs BEFORE ArmedClock and it is a
	// SEPARATE check, not a second return value: ArmedClock returns exactly one
	// clock by construction, and that single-clock property is what makes the
	// model auditable. Whichever fires first wins, and residency is the one that
	// fires when the idle clock never will.
	//
	// It exists because #521 generalised liveness: a LIVE state now arms the IDLE
	// clock on conversationLastEventAt, which every human comment re-stamps, and
	// which ArmedClock disarms outright while a turn is in flight - so
	// under-implementation lost the 6h absolute bound the old `implementing`
	// stage had. Without this an implement agent ping-ponging with a reviewer, or
	// simply never finishing a turn, runs forever.
	//
	// Only the live states have a cap. It is measured with StateElapsedSeconds,
	// so it is CUMULATIVE across a park/un-park round trip - a Task that has
	// spent six hours in under-implementation across three re-entries has spent
	// six hours there, and buying a fresh 6h per re-entry is the unbounded-loop
	// shape #480 killed for merging.
	//
	// IT HONOURS THE PAUSE, for the same reason clock 1 does and with more at
	// stake. `paused` (maxConcurrentAgents == 0) is the kill switch, and the
	// contract's one deadline exception exists because "without it the pause kill
	// switch is a backlog shredder" (constants.go). Residency is the WORST clock
	// to leave unguarded: every live cap (24h/6h/4h) is at or below the 24h
	// admission budget, so an unguarded residency check dominates the exemption
	// and parks the entire queued backlog at stage-deadline - which is
	// UnparkNever, so it ages out at ParkRetention and is reaped. A four-hour
	// pause would have destroyed every awaiting-review Task on the project.
	paused := projectPaused(proj)
	if !paused && !tatarav1alpha1.Parked(task) && stage.ResidencyExceeded(task, now) {
		l.Info("state residency budget exceeded",
			"action", "residency_exceeded", "resource_id", task.Name,
			"state", task.Status.State, "kind", task.Spec.Kind,
			"elapsed_seconds", stage.StateElapsedSeconds(task, now))
		r.Metrics.ResidencyExceeded(task.Status.State, task.Spec.Kind)
		return ctrl.Result{}, true, r.park(ctx, proj, task, stage.ReasonStageDeadline, now)
	}

	clock, since, budget, edge := stage.ArmedClock(task, paused)
	// The ONE per-project clock budget in the F.4 model. internal/stage is pure and
	// holds no Project, so the table carries ConversationIdleDefault and the
	// substitution happens here, at the single call site that has the Project in
	// hand. Keeping the table row means TestEveryStateHasABudget still covers the
	// live states and a future state still cannot be added without a deadline.
	//
	// #521 widened the predicate from the single `conversing` stage to every LIVE
	// state, and nothing else about this substitution changed.
	if stage.Live(task.Status.State) && clock == stage.ClockWork && !tatarav1alpha1.Parked(task) {
		budget = tatarav1alpha1.ConversationIdle(proj)
	}
	if clock == stage.ClockNone {
		// parked(backlog-sweep), or a stage with no budget row. Nothing ages out.
		return ctrl.Result{}, false, nil
	}

	elapsed := now.Sub(since)
	if elapsed <= budget {
		return ctrl.Result{RequeueAfter: clockRequeue(clock, budget-elapsed)}, false, nil
	}

	switch edge.To {
	case stage.Reap:
		// done/rejected aged out, or a park reached ParkRetention. The REAPER
		// deletes them (contract B.6); this reconciler never does.
		return ctrl.Result{}, true, nil

	case stage.Respawn:
		// CLOCK 2. PodWatchReconciler is the SINGLE writer of stats.podRecreations
		// and the single deleter of a never-Ready pod (podwatch.go handleNotReady,
		// which reuses bootDeadlineExceeded). Respawning here as well would burn the
		// recreation budget at twice the rate and fail the Task at half the attempts.
		// The one case podwatch CANNOT see is a pod that was DELETED while never
		// Ready (its predicate drops delete events), and reconcilePodStage owns that.
		return ctrl.Result{RequeueAfter: agentBootRequeue}, false, nil
	}

	// A LIVE state's IDLE-budget elapse is NOT a plain park: the agent is owed one
	// handoff turn before its pod dies, or the notes journal - which IS the
	// continuation state - is left empty for whatever pod comes next.
	//
	// GATED ON THE EDGE, NOT ON Live(state) ALONE. A live state also runs the
	// ADMISSION clock (clock 1) before its pod exists, and that elapse is
	// park(admission-starved) with NO pod to hand off from and a DIFFERENT
	// reason. Routing it through the handoff would re-label it awaiting-human -
	// a park with a re-entry rule where the correct one has none - and block for
	// the stopper's timers against a pod that was never admitted.
	if stage.Live(task.Status.State) && !tatarav1alpha1.Parked(task) &&
		edge.Reason == stage.ReasonAwaitingHuman {
		mrs, mrErr := ownedMergeRequests(ctx, r.Client, task)
		if mrErr != nil {
			return ctrl.Result{}, true, mrErr
		}
		l.Info("conversation idle budget elapsed",
			"action", "stage_deadline", "resource_id", task.Name, "state", task.Status.State,
			"clock", clock, "budget", budget.String(), "elapsed", elapsed.String(),
			"to", edge.To, "park_reason", edge.Reason)
		return ctrl.Result{}, true, r.liveHandoffAndPark(ctx, proj, task, mrs, causeIdle, now)
	}

	mrs, mrErr := ownedMergeRequests(ctx, r.Client, task)
	if mrErr != nil {
		return ctrl.Result{}, true, mrErr
	}
	l.Info("state budget elapsed",
		"action", "stage_deadline", "resource_id", task.Name, "state", task.Status.State,
		"clock", clock, "budget", budget.String(), "elapsed", elapsed.String(),
		"to", edge.To, "park_reason", edge.Reason)
	if edge.To != stage.ParkTarget {
		// Only Reap and Respawn are handled above, and every other onElapse row
		// parks. A non-park target here is a table bug.
		return ctrl.Result{}, true, fmt.Errorf("clocks: onElapse for %s targets %q, which is not a park", task.Status.State, edge.To)
	}
	if err := r.park(ctx, proj, task, edge.Reason, now); err != nil {
		return ctrl.Result{}, true, err
	}
	// WS3-I5: on the FIRST park at deploy-timeout, surface the stuck deploy to the
	// human with ONE rate-limited operator comment per owned issue.
	if edge.Reason == stage.ReasonDeployTimeout {
		if err := enqueueDeployTimeoutComment(ctx, r.Client, r.spiller(proj), task, mrs, now); err != nil {
			return ctrl.Result{}, true, err
		}
	}
	return ctrl.Result{}, true, nil
}

// enqueueDeployTimeoutComment enqueues ONE operator comment on each owned OPEN
// issue when a Task first parks at deploy-timeout (WS3-I5), so a stuck deploy is
// surfaced at the FIRST timeout instead of staying silent until the terminal
// failed(deploy-blocked). It is keyed on its OWN cooldown marker
// (Issue.status.lastDeployTimeoutCommentAt), DISTINCT from the incident-refire
// LastRefireCommentAt, so an issue that is both an incident tracker and
// deploy-blocked never has one producer clobber the other's cooldown. It reuses
// the existing PendingComments drain; it spawns no agent. Leader-only (this whole
// reconcile is).
// It is a free function rather than a TaskReconciler method because
// StageDriver.parkOnStalledFanout parks at the SAME reason from the deploy poll
// (#512) and must post the same notice; a park at deploy-timeout that a human
// never hears about is what made the 2026-07-29 wedge invisible for two hours.
func enqueueDeployTimeoutComment(ctx context.Context, c client.Client, sp objbudget.Spiller,
	task *tatarav1alpha1.Task, mrs []tatarav1alpha1.MergeRequest, now time.Time) error {

	issues, err := loadTaskIssues(ctx, c, task)
	if err != nil {
		return err
	}
	// Repo attribution: the MR(s) that did not reach deployedAt.
	var stuck []string
	for i := range mrs {
		if mrs[i].Status.DeployedAt == nil {
			stuck = append(stuck, mrs[i].Spec.RepositoryRef)
		}
	}
	repos := strings.Join(stuck, ", ")
	if repos == "" {
		repos = task.Spec.RepositoryRef
	}
	budget, _ := stage.Budget(tatarav1alpha1.StateDeployed)
	body := fmt.Sprintf("Deployment of `%s` has not completed after `%s`; retry `%d`/`%d`. tatara keeps retrying until it succeeds or the deploy budget is exhausted.",
		repos, budget, task.Status.DeployReentries, tatarav1alpha1.MaxDeployReentries)

	stamp := metav1.NewTime(now)
	for i := range issues {
		iss := &issues[i]
		if iss.Status.State != "open" || iss.Status.LastDeployTimeoutCommentAt != nil {
			continue // closed, or already commented on the first timeout (own cooldown).
		}
		key := client.ObjectKeyFromObject(iss)
		if err := objbudget.FitIssue(ctx, c, sp, key, func(cur *tatarav1alpha1.Issue) {
			if cur.Status.LastDeployTimeoutCommentAt != nil || len(cur.Status.PendingComments) >= 20 {
				return
			}
			cur.Status.PendingComments = append(cur.Status.PendingComments, tatarav1alpha1.PendingComment{
				RequestID: fmt.Sprintf("deploy-timeout-%s", task.Name),
				Action:    "comment",
				Body:      body,
			})
			cur.Status.LastDeployTimeoutCommentAt = &stamp
		}); err != nil {
			return fmt.Errorf("deploy-timeout comment: enqueue on %s: %w", key.Name, err)
		}
		log.FromContext(ctx).Info("deploy stuck: surfaced the first deploy-timeout on an owned issue",
			"action", "deploy_timeout_comment", "resource_id", task.Name, "issue", key.Name, "repos", repos)
	}
	return nil
}

// reconcileCIWait resolves THE CI HOLD (PR B, gate site 2): the implement
// outcome was accepted, the code is pushed, and the advance to awaiting-review
// was held because CI at the pushed head had not answered yet.
//
// IT HAS EXACTLY THREE EXITS AND ALL THREE CLEAR THE HOLD. That totality is the
// design requirement, not a nicety: a hold nothing clears is a Task stranded
// with a suppressed pod and an accepted outcome, which is a worse failure than
// the dirty PR the hold exists to prevent.
//
//   - RED -> the enterCIRed bounce. The Task is already at
//     under-implementation, so the transition is a self-edge no-op and what
//     actually happens is the operator note, the re-entry counter and the
//     un-suppressed pod. It ALSO revokes the OutcomeAccepted condition, and that
//     is load-bearing: the acceptance was conditional and the condition failed,
//     and an agent that fixes CI and resubmits a byte-identical payload would
//     otherwise hash to the same fingerprint and be answered with a replay 200
//     that advances nothing.
//   - CLEAR -> awaiting-review, the transition /outcome would have made.
//   - CIWaitDeadline -> awaiting-review anyway. FAIL OPEN, because that is
//     precisely the pre-PR-B behaviour: a forge that stops delivering CI events
//     costs a bounded wait, never a stranded Task. Gate site 3 still bounces the
//     Task if the pipeline reports red later.
//
// THE TRIGGER IS THE MIRROR, refreshed by PR A's webhook stamp and its 5m
// CIRefreshCadenceActive backstop - which is armed here precisely because the
// hold does NOT park the Task. A mirror that says RED is confirmed live before
// anything destructive happens; a live read that fails or disagrees keeps
// holding, bounded by the deadline above.
func (r *TaskReconciler) reconcileCIWait(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, now time.Time) (ctrl.Result, bool, error) {

	l := log.FromContext(ctx)
	mrs, err := ownedMergeRequests(ctx, r.mrReader(), task)
	if err != nil {
		return ctrl.Result{}, true, err
	}
	clearHold := func(t *tatarav1alpha1.Task) { t.Status.CIWaitSince = nil }
	held := now.Sub(task.Status.CIWaitSince.Time)

	if ciMirrorVerdict(mrs) == ciVerdictRed {
		red, cerr := ciRedAtReviewedHead(ctx, r.Client, r.SCMFor, r.Metrics, proj, mrs)
		switch {
		case cerr != nil:
			l.Error(cerr, "ci hold: the mirror reports RED but live CI could not be read; still holding",
				"action", "ci_wait_check_failed", "resource_id", task.Name, "held", held.String())
		case red != nil:
			l.Info("ci hold: the checks went RED at the pushed head; the accepted submission is revoked and the agent runs again",
				"action", "ci_wait_red", "resource_id", task.Name, "repo", red.Spec.RepositoryRef,
				"pr", red.Spec.Number, "held", held.String())
			return ctrl.Result{}, true, enterCIRedWith(ctx, r.Client, r.spiller(proj), r.Metrics,
				task, mrs, red, now, func(t *tatarav1alpha1.Task) {
					clearHold(t)
					meta.RemoveStatusCondition(&t.Status.Conditions, tatarav1alpha1.ConditionOutcomeAccepted)
				})
		}
	} else if ciMirrorVerdict(mrs) == ciVerdictClear {
		l.Info("ci hold: the checks are green at the pushed head; releasing the submission to review",
			"action", "ci_wait_green", "resource_id", task.Name, "held", held.String())
		return ctrl.Result{}, true, EnterStage(ctx, r.Client, r.spiller(proj), r.Metrics, task, mrs,
			tatarav1alpha1.StateAwaitingReview, "", now, clearHold)
	}

	if held >= tatarav1alpha1.CIWaitDeadline {
		l.Info("ci hold: no CI verdict arrived within the deadline; releasing the submission to review anyway",
			"action", "ci_wait_deadline", "resource_id", task.Name, "held", held.String(),
			"deadline", tatarav1alpha1.CIWaitDeadline.String())
		return ctrl.Result{}, true, EnterStage(ctx, r.Client, r.spiller(proj), r.Metrics, task, mrs,
			tatarav1alpha1.StateAwaitingReview, "", now, clearHold)
	}
	// The webhook stamp wakes this Task through Owns(&MergeRequest{}) and the
	// backstop re-reads every 5m; the poll is only the belt for a delivery and a
	// backstop that BOTH went missing.
	return ctrl.Result{RequeueAfter: stageRequeue}, true, nil
}

// reconcileMRBindingBackstop is issue #381 bug B part 2: a Source-bearing
// Task (Spec.Source.Number > 0 - the same source-bearing gate
// mintIssueCRs uses, task_stage.go:472-475) that still owns ZERO
// MergeRequest and ZERO Issue CRs past mrBindingBackstopGrace is the residue
// of an interrupted two-phase mint write (the Task's own Create landed, the
// bind that would have given it an owned MR/Issue never did - #382 covers
// the LIVE repair path on a fresh mint attempt; this is the backstop for
// the Task that already exists and has nothing left to re-mint against).
// Live proof: mt-r-tatara-cli-87 (issue #381 investigation).
//
// Guarded to fire EXACTLY ONCE: an ALREADY-PARKED Task is excluded up front,
// because the park flag is this function's OWN rate limit for the metric and the
// comment - once parked, this predicate would otherwise stay permanently true (a
// parked Task's refs do not un-empty themselves) and re-fire forever without it.
// The already-parked case is owned by reconcileParkedBindingRepair (repair only,
// no re-park, no metric, no comment), which reconcileStage runs before its
// terminal early-return.
//
// Runs BEFORE the three clocks (F.4): those would EVENTUALLY reach the same
// outcome via turn-budget/pod-recreation exhaustion, but only after hours of
// wasted pod churn against a Task that can never advance - this backstop is
// the fast, targeted path.
func (r *TaskReconciler) reconcileMRBindingBackstop(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, now time.Time) (bool, error) {

	if tatarav1alpha1.Parked(task) ||
		task.Spec.Source == nil || task.Spec.Source.Number <= 0 ||
		len(task.Status.MRRefs) != 0 || len(task.Status.IssueRefs) != 0 {
		return false, nil
	}
	age := now.Sub(stageEnteredAt(task))
	if age <= mrBindingBackstopGrace {
		return false, nil
	}
	// A DONE Task is never parked: park means "stalled, resumable", and issue
	// #381's review BLOCKER (some stages had no legal ->parked edge, so the old
	// code crash-looped on an illegal transition) survives as this one guard.
	// Parking is no longer an edge, so the table cannot refuse it - this
	// predicate has to.
	if tatarav1alpha1.TaskDone(task) {
		return false, nil
	}

	l := log.FromContext(ctx)

	// PR 388: a transient mint failure is REPAIRABLE, and the repair already
	// exists (intake.go repairMRBinding/repairIssueBinding) - but its only other
	// caller is the sweep's intake path, whose ~1h cadence always loses to this
	// backstop. Attempt the bind repair here first; only an unrepairable bind
	// (foreign-owned CR, unresolvable source/repo, write failure) falls through
	// to the park below.
	if r.repairSourceBinding(ctx, proj, task) {
		l.Info("mr-binding backstop: repaired the interrupted mint binding instead of parking",
			"action", "mr_binding_backstop_repaired", "resource_id", task.Name, "kind", task.Spec.Kind,
			"state", task.Status.State, "age", age.String())
		return true, nil
	}

	l.Error(nil, "task owns no merge-request or issue record past grace; likely an interrupted mint - parking awaiting human",
		"action", "mr_binding_backstop_parked", "resource_id", task.Name, "kind", task.Spec.Kind,
		"state", task.Status.State, "age", age.String(), "grace", mrBindingBackstopGrace.String())
	if err := r.park(ctx, proj, task, stage.ReasonAwaitingHuman, now); err != nil {
		return true, err
	}
	r.Metrics.MRBindingBackstopParked(proj.Name, task.Spec.Kind)
	r.commentMRBindingBackstopParked(ctx, proj, task)
	return true, nil
}

// reconcileParkedBindingRepair is the parked-side completion of
// reconcileMRBindingBackstop (2026-07-19 production deadlock, task
// mt-r-tatara-operator-388-6e7958617d9d0119): a Task the watchdog ALREADY
// parked awaiting-human - by an operator version predating the PR 388 repair,
// or because the repair failed transiently before the park - was excluded
// forever by the backstop's own already-parked guard, and the pending-events
// comment path dead-ended too (the unowned mirror stub has no controller
// owner to route by). Both recovery paths dead, the Task was parked for good.
//
// This runs BEFORE reconcileStage's terminal early-return hands parked Tasks
// to the reaper. The predicate is the watchdog's park and ONLY the watchdog's
// park: parked(awaiting-human) + parkedFromState recorded + Source.Number>0 +
// ZERO owned refs. Any awaiting-human park with refs is a genuine human wait
// (a review verdict on a human PR, a gate discuss) and is never touched;
// every other reason (identity-unverified above all) is never touched.
//
// IT NO LONGER UN-PARKS (#521). stage.UnparkTargetForBindingRepair existed
// because un-parking used to mean CHOOSING A STAGE, and this park had no owned
// state to derive one from. Un-park no longer moves state at all, so there is
// nothing to derive and no exception to make: the repair stamps the refs, and
// the ordinary awaiting-human rule resumes the Task on the next human comment -
// which now routes, because the mirror stub it dead-ended on has an owner.
func (r *TaskReconciler) reconcileParkedBindingRepair(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, now time.Time) (bool, error) {

	if !parkedBindingRepairEligible(task) {
		return false, nil
	}
	// The predicate matched on a CACHED read; confirm against the API server
	// before repairing and unparking, so a Task another writer already moved is
	// not re-entered off a stale snapshot (same idiom as the mint/terminal
	// checks in reconcileStage).
	if err := refreshTaskFromAPI(ctx, r.APIReader, task); err != nil {
		return false, err
	}
	if !parkedBindingRepairEligible(task) {
		return false, nil
	}
	l := log.FromContext(ctx)
	if !r.repairSourceBinding(ctx, proj, task) {
		// Unrepairable (foreign-owned CR, unresolvable source, write error): the
		// Task stays parked and the park notice stands.
		l.Info("parked binding repair: repair failed, task stays parked",
			"action", "parked_binding_repair_failed", "resource_id", task.Name,
			"kind", task.Spec.Kind, "parked_from", task.Status.ParkedFromState)
		return false, nil
	}
	// The refs are stamped and the CR owned, so a human comment now drives the
	// ordinary awaiting-human re-entry, in place, in the state the Task never
	// left.
	l.Info("parked binding repair: repaired the interrupted mint binding; staying parked until a human replies",
		"action", "mr_binding_backstop_repaired_parked", "resource_id", task.Name,
		"kind", task.Spec.Kind, "state", task.Status.State)
	return true, nil
}

// parkedBindingRepairEligible is reconcileParkedBindingRepair's predicate: the
// MR-binding watchdog's park flavor, and nothing else.
func parkedBindingRepairEligible(task *tatarav1alpha1.Task) bool {
	return task.Status.ParkReason == stage.ReasonAwaitingHuman &&
		task.Status.ParkedFromState != "" &&
		task.Spec.Source != nil && task.Spec.Source.Number > 0 &&
		len(task.Status.MRRefs) == 0 && len(task.Status.IssueRefs) == 0
}

// repairSourceBinding re-runs the intake funnel's interrupted-mint bind repair
// (repairMRBinding / repairIssueBinding) against a source-bearing Task the
// backstop is about to park (PR 388). The ext snapshot is rebuilt from the
// Task's own Source - the same fields the interrupted mint had in hand; the
// mirror cadence refreshes the rest. true means the refs are now non-empty and
// the Task must NOT be parked. Anything else - unparseable source, unresolvable
// repo, a foreign-owned CR the repair correctly refuses to steal, a write
// error - reports false and the caller parks exactly as before.
func (r *TaskReconciler) repairSourceBinding(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task) bool {

	l := log.FromContext(ctx)
	src := task.Spec.Source
	slug, number, ok := parseSourceURL(src.IssueRef, src.IsPR)
	if !ok {
		return false
	}
	repo, err := r.repositoryForSlug(ctx, proj, task, slug)
	if err != nil {
		l.Error(err, "mr-binding backstop: resolve repository failed; falling through to the park",
			"action", "mr_binding_backstop_repair_failed", "resource_id", task.Name, "slug", slug)
		return false
	}
	if repo == nil {
		return false
	}
	m := &Minter{Client: r.Client, APIReader: r.APIReader, Scheme: r.Scheme, Metrics: r.Metrics}
	sp := r.spiller(proj)
	if src.IsPR {
		ext := scm.MergeRequest{Number: number, URL: src.IssueRef, Title: src.Title,
			Author: src.AuthorLogin, State: "open", HeadSHA: src.HeadSHA}
		err = m.repairMRBinding(ctx, proj, repo, ext, task, sp)
	} else {
		url, _, _ := strings.Cut(src.IssueRef, "#") // MintIssueTask stores "URL#number"
		ext := scm.Issue{Number: number, URL: url, Title: src.Title,
			Author: src.AuthorLogin, State: "open"}
		err = m.repairIssueBinding(ctx, proj, repo, ext, task, sp)
	}
	if err != nil {
		l.Error(err, "mr-binding backstop: bind repair failed; falling through to the park",
			"action", "mr_binding_backstop_repair_failed", "resource_id", task.Name)
		return false
	}
	return len(task.Status.MRRefs) != 0 || len(task.Status.IssueRefs) != 0
}

// repositoryForSlug resolves the Repository CR a Task's source slug names:
// spec.repositoryRef when set, else the project repo whose remote matches the
// slug. It is sourceRepository's slug-keyed sibling: the intake-minted Tasks
// this backstop repairs carry no RepositoryRef and no Source.URL, only the
// IssueRef the slug was parsed from. nil (no error) means "cannot resolve".
func (r *TaskReconciler) repositoryForSlug(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, slug string) (*tatarav1alpha1.Repository, error) {

	if task.Spec.RepositoryRef != "" {
		var repo tatarav1alpha1.Repository
		err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.RepositoryRef}, &repo)
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("backstop repair: get repository %s: %w", task.Spec.RepositoryRef, err)
		}
		return &repo, nil
	}
	repos, err := r.projectRepos(ctx, proj)
	if err != nil {
		return nil, err
	}
	for i := range repos {
		owner, name, err := scm.OwnerRepo(repos[i].Spec.URL)
		if err == nil && owner+"/"+name == slug {
			return &repos[i], nil
		}
	}
	return nil, nil
}

// taskWriterAndToken resolves the SCMWriter + bot token for a Project,
// mirroring ProjectReconciler.scanWriter (projectscan.go:287) - the same
// five lines, duplicated rather than shared across the two reconciler types
// per this repo's KISS rule (three similar lines beat a premature shared
// abstraction across two otherwise-unrelated reconcilers).
func (r *TaskReconciler) taskWriterAndToken(ctx context.Context, proj *tatarav1alpha1.Project) (scm.SCMWriter, string, error) {
	if r.SCMFor == nil {
		return nil, "", fmt.Errorf("mr-binding backstop: SCMFor not wired")
	}
	var sec corev1.Secret
	key := types.NamespacedName{Namespace: proj.Namespace, Name: proj.Spec.ScmSecretRef}
	if err := r.Get(ctx, key, &sec); err != nil {
		return nil, "", fmt.Errorf("mr-binding backstop: get scm secret: %w", err)
	}
	token := string(sec.Data["token"])
	w, err := r.SCMFor(proj.Spec.Scm.Provider)
	if err != nil {
		return nil, "", err
	}
	return w, token, nil
}

// commentMRBindingBackstopParked posts ONE notice on the Task's source
// PR/issue when reconcileMRBindingBackstop parks it. Rate-limited by its
// caller's own "not already parked" guard (see reconcileMRBindingBackstop's
// doc comment) - there is no separate per-comment cooldown field because
// the Task this backstop targets owns, by construction, zero Issue/MR CRs
// to stamp one on. A forge failure here is logged and swallowed: the park
// already landed and is the correctness-critical half of this backstop: it
// must never be retried into existence by re-running the whole function on
// a Task the guard now excludes.
func (r *TaskReconciler) commentMRBindingBackstopParked(ctx context.Context, proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task) {
	l := log.FromContext(ctx)
	slug, number, ok := parseSourceURL(task.Spec.Source.IssueRef, task.Spec.Source.IsPR)
	if !ok {
		l.Error(nil, "mr-binding backstop: source ref is not a recognizable issue/PR URL; comment skipped",
			"action", "mr_binding_backstop_comment_skipped", "resource_id", task.Name,
			"source_ref", task.Spec.Source.IssueRef)
		return
	}
	writer, token, err := r.taskWriterAndToken(ctx, proj)
	if err != nil {
		l.Error(err, "mr-binding backstop: resolve scm writer failed; comment skipped",
			"action", "mr_binding_backstop_comment_skipped", "resource_id", task.Name)
		return
	}
	body := fmt.Sprintf(
		"tatara parked this task: it has not recorded an owned merge-request or issue after `%s` "+
			"(likely an interrupted mint on the operator side). No agent will run against it until a "+
			"human comments here to prompt a retry.",
		mrBindingBackstopGrace.String())
	issueRef := fmt.Sprintf("%s#%d", slug, number)
	if err := writer.Comment(ctx, token, issueRef, body); err != nil {
		l.Error(err, "mr-binding backstop: forge comment failed (non-fatal, task is parked)",
			"action", "mr_binding_backstop_comment_failed", "resource_id", task.Name, "issue_ref", issueRef)
		return
	}
	l.Info("mr-binding backstop: posted the park notice",
		"action", "mr_binding_backstop_commented", "resource_id", task.Name, "issue_ref", issueRef)
}

// handoffCondition returns the OutcomeAccepted condition of a Task whose OWN
// stage agent has COMMITTED its outcome and whose stage has NOT moved - i.e. the
// cross-reconciler handoff is outstanding and its clock should be running. nil
// otherwise. Its LastTransitionTime is when the commit stamped it, which is when
// the handoff started - not stageEnteredAt.
//
// It resolves to exactly one case, kind-agnostically: awaiting-review after a
// review outcome commits. Every other kind's commit calls stage.Enter in the SAME
// write, so no other state can be committed-but-not-advanced. The scoped
// OutcomeCommittedFor is load-bearing and a bare OutcomeCommitted would be a bug:
// the condition is per-TASK and survives across states, so an implement Task is
// already committed the instant it arrives at awaiting-review. A bare claim (Reason
// "Outcome") never matches either: it has no handoff outstanding.
//
// #547: OutcomeCommittedFor ALONE stopped being sufficient. It used to be, because
// every state had its own agent kind, so a commit naming the state's agent could
// only have been made IN that state. Folding clarify into implement (#521) made
// AgentKindFor return the same agent for refined AND under-implementation, so the
// approval commit that MOVES a Task refined -> under-implementation names the agent
// of the state it lands in. Hence the state gate below, which is the invariant this
// comment always claimed, now asserted instead of inferred.
//
// This is the ONE predicate for "this stage's agent is done and the handoff is
// outstanding". All three production sites route through it - reconcileClocks'
// deadline and the two B2 suppression guards - so the scoping holds identically
// at each.
func handoffCondition(task *tatarav1alpha1.Task) *metav1.Condition {
	// GATE 1, structural: awaiting-review is the ONE state whose outcome commit
	// defers its advance to a second reconciler (MergeRequestReconciler ->
	// DrainPendingReview -> advanceAfterReview). Every other state advances in
	// the commit's own write, so it has no handoff to be outstanding and no
	// deadline to bound. This holds whatever AgentKindFor happens to return.
	if task.Status.State != tatarav1alpha1.StateAwaitingReview {
		return nil
	}
	if !tatarav1alpha1.OutcomeCommittedFor(task, stage.AgentKindFor(task.Status.State, task.Spec.Kind)) {
		return nil
	}
	cond := tatarav1alpha1.OutcomeCondition(task)
	// GATE 2, occupancy: stage.Enter never clears the condition, so a commit from
	// a PREVIOUS occupancy of this same state is not this occupancy's handoff:
	// merged -> awaiting-review (HeadMoved, cycle 4) and the kind=review
	// awaiting-human unpark both re-enter awaiting-review carrying Reason=Review
	// from the last round. This occupancy's agent has not run yet.
	//
	// AT the entry stamp is not after it. A commit written in the SAME etcd write
	// as the entry it caused shares its metav1.Time, which is SECOND-granular, so
	// the two stamps are equal and a strict Before() let it through (#547). An
	// outcome can only be this occupancy's handoff if it was committed strictly
	// LATER than the entry - which a genuine review commit always is, since the
	// review pod has to be scheduled, booted and run first.
	//
	// No state stamp is no occupancy to compare against, so no handoff to bound
	// either - ArmedClock disarms on the same condition (stage.go:572). Every
	// path into a state runs stage.Enter, which always stamps it, so a nil stamp
	// is unreachable; failing closed just keeps it that way.
	if task.Status.StateEnteredAt == nil ||
		!cond.LastTransitionTime.After(task.Status.StateEnteredAt.Time) {
		return nil
	}
	return cond
}

// clockRequeue is when to look again so a clock fires without an external event.
func clockRequeue(clock string, remaining time.Duration) time.Duration {
	var cap time.Duration
	switch clock {
	case stage.ClockAdmission:
		cap = admissionRequeue
	case stage.ClockReadiness:
		cap = agentBootRequeue
	default:
		cap = stageRequeue
	}
	if remaining > 0 && remaining < cap {
		return remaining + time.Second
	}
	return cap
}

// reconcileCaps enforces what is LEFT of the F.4 exits every POD stage carries
// on top of its clocks: the pod-stopped-with-no-outcome park, and nothing else.
// O3 deleted the lifetime turn budget and the pod-recreation budget. Pod-less
// stages carry none (stage.BudgetExit returns nothing for them).
func (r *TaskReconciler) reconcileCaps(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, now time.Time) (handled bool, err error) {

	// podStoppedNoOutcome means the pod RAN (it became Ready: stageWorkStartedAt is
	// set) and is now UNUSABLE without the Task having left the stage - i.e. it
	// submitted no outcome. A Task waiting in the ADMISSION QUEUE has podStartedAt
	// == nil and is never this (fix V6-1): the fix that killed every Task that ever
	// queued in normal steady state got exactly this predicate wrong.
	//
	// Unusable, not absent: a wrapper that died in place (OOMKilled, non-zero exit,
	// phase Failed) leaves the Pod object standing under RestartPolicy: Never, and
	// reading only NotFound here is what let one sit at under-implementation for 51
	// minutes holding a live-pod slot. The park this reaches is ReasonNoOutcome,
	// which is an UnparkTimer stall rather than a terminal, and ParkTask takes the
	// corpse down with it.
	stopped := false
	if task.Status.PodStartedAt != nil && task.Status.StateWorkStartedAt != nil {
		gone, reason, gerr := r.podUnusable(ctx, task)
		if gerr != nil {
			return true, gerr
		}
		stopped = gone
		// Before ParkTask takes the corpse down, read the kill off it: the note is
		// the only thing that tells the next pod its predecessor's workspace is gone.
		if reason == obs.RecreationReasonOOMKilled {
			if nerr := r.noteOOMKilledPod(ctx, proj, task); nerr != nil {
				return true, nerr
			}
		}
	}

	edge, ok := stage.BudgetExit(task, stopped)
	if !ok {
		return false, nil
	}

	// B2: an outcome COMMITTED BY THIS STAGE'S OWN AGENT means the agent's work is
	// done and only the C.5.3 phase-2 handoff (DrainPendingReview ->
	// advanceAfterReview) is outstanding. The pod-liveness caps read only pod
	// liveness and stats.podRecreations - they cannot see that - so while
	// status.stage is still reviewing they keep driving a finished Task as an
	// ordinary live pod stage and terminate work that already landed.
	//
	// A BARE CLAIM is deliberately NOT guarded: handoffCondition is nil for
	// Reason "Outcome", so a failed-validation stub stays fully subject to the
	// caps. Guarding it would freeze it forever. A commit from a PREVIOUS
	// occupancy of this stage is nil there too, so a re-entered reviewing Task
	// carries no suppression from the last round.
	//
	// handled=false lets the flow fall through to reconcilePodStage, whose own B2
	// guard returns the poll requeue. The handoff deadline stays armed and bounds
	// the suppression in the other direction.
	//
	// no-outcome is the ONLY reason BudgetExit can return since O3, so the
	// pod-recreation-exhausted arm this condition used to carry is gone with it.
	if handoffCondition(task) != nil && edge.Reason == stage.ReasonNoOutcome {
		log.FromContext(ctx).Info("state budget exit suppressed: the outcome is committed and the handoff is in flight",
			"action", "cap_suppressed_committed_outcome", "resource_id", task.Name,
			"state", task.Status.State, "pod_recreations", task.Status.Stats.PodRecreations,
			"suppressed_reason", edge.Reason)
		return false, nil
	}

	log.FromContext(ctx).Info("state budget exit",
		"action", "stage_budget_exit", "resource_id", task.Name, "state", task.Status.State,
		"turns", task.Status.Stats.Turns, "pod_recreations", task.Status.Stats.PodRecreations,
		"pod_stopped_no_outcome", stopped, "to", edge.To, "park_reason", edge.Reason)
	// The BudgetExit edge parks: a pod that ran and vanished without an outcome
	// stalls the Task where it is rather than moving it. no-outcome is UnparkTimer,
	// so this is a re-drivable stall, not a terminal.
	return true, r.park(ctx, proj, task, edge.Reason, now)
}

// reconcileTriaging is the pod-less TRIAGE stage: the operator classifies the
// origin, MINTS the Issue CRs from the forge mirror (F.2 - the Issue keys are
// NOT carried on the QueuedEvent payload; there is no TaskSpec field for them),
// and picks the next stage from spec.kind.
func (r *TaskReconciler) reconcileTriaging(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, now time.Time) (ctrl.Result, error) {

	// The 49-char name guard (A.1). A CRD cannot constrain metadata.name length and
	// there is no validating webhook, so this is the reconcile guard - and it must
	// fire BEFORE a pod is spawned, because the pod name is the Task name plus a
	// suffix and the kubelet's failure is opaque.
	if len(task.Name) > tatarav1alpha1.MaxTaskNameLength {
		return ctrl.Result{}, r.park(ctx, proj, task, stage.ReasonNameTooLong, now)
	}
	if verr := tatarav1alpha1.ValidateTaskSpec(task.Spec); verr != nil {
		log.FromContext(ctx).Info("triage: invalid task spec",
			"action", "triage_invalid_spec", "resource_id", task.Name, "err", verr.Error())
		return ctrl.Result{}, r.park(ctx, proj, task, stage.ReasonTriageStalled, now)
	}

	if err := r.mintIssueCRs(ctx, proj, task); err != nil {
		return ctrl.Result{}, err
	}

	next, ok := triageTarget(task.Spec.Kind)
	if !ok {
		log.FromContext(ctx).Info("triage: no state for this task kind",
			"action", "triage_unknown_kind", "resource_id", task.Name, "kind", task.Spec.Kind)
		return ctrl.Result{}, r.park(ctx, proj, task, stage.ReasonTriageStalled, now)
	}
	return ctrl.Result{}, r.enter(ctx, proj, task, nil, next, "", now)
}

// triageTarget is the `new` row, as data: the state each ORIGIN kind starts at.
// A kind with no row is a spec bug and stalls triage rather than falling through
// into some default that spawns the wrong agent.
//
// EVERY code-bearing kind starts at `refined`, and `implement` is now one of
// them (#521). That is not a hole in the approval gate: `refined` is where the
// gate RUNS. The old machine reached code through clarifying -> approved ->
// implementing and had to refuse kind=implement at triage because a Task minted
// there would have skipped the gate; under the merged model the same agent
// brainstorms, is approved and implements, and the ONLY edge out of refined into
// under-implementation is the one restapi's gate grants. A Task minted
// kind=implement therefore lands in front of the gate, not past it.
//
// THERE IS NO `documentation` ROW, and its absence is the point. It used to
// return under-implementation, which is NOT in the table's `new` row - so a
// documentation Task that ever reached triage would have failed
// stage.LegalFor(new, under-implementation) and errored on every reconcile pass
// forever. It never reaches triage: docbatch.go mints the nightly batch with
// Spec.InitialState = StateUnderImplementation and the CREATE edge takes it
// there directly (no driving issue to triage, no approval to gate). Falling
// through to the no-row branch means a documentation Task that somehow does
// arrive at `new` parks at triage-stalled, loudly and once, instead of grinding
// against an edge that does not exist.
func triageTarget(kind string) (string, bool) {
	switch kind {
	case stage.AgentBrainstorm, stage.AgentIncident, stage.AgentRefine, stage.AgentImplement:
		return tatarav1alpha1.StateRefined, true
	case "takeover":
		return tatarav1alpha1.StateRefined, true
	case stage.AgentReview:
		return tatarav1alpha1.StateAwaitingReview, true
	default:
		return "", false
	}
}

// mintIssueCRs is triaging's "the operator MINTS the Issue CRs from the forge
// mirror" (F.2). The Task's Source names the originating issue; the Issue CR is
// ensured, owned by this Task, and recorded in status.issueRefs.
//
// A Task with no Source (a brainstorm, a refine, an alert-born incident) owns no
// Issue at mint, and a kind=review Task owns ZERO Issues BY CONSTRUCTION - the
// empty set is not a licence for anything (fix V6-3), so this must never
// fabricate one.
func (r *TaskReconciler) mintIssueCRs(ctx context.Context, proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task) error {
	src := task.Spec.Source
	if src == nil || src.IsPR || src.Number <= 0 || task.Spec.Kind == stage.AgentReview {
		return nil
	}
	repo, err := r.sourceRepository(ctx, proj, task)
	if err != nil || repo == nil {
		return err
	}
	name := tatarav1alpha1.IssueName(repo.Name, src.Number)
	for _, ref := range task.Status.IssueRefs {
		if ref == name {
			return nil // already minted and owned
		}
	}
	if err := ensureIssueCR(ctx, r.Client, proj, repo, src.Number, src.URL); err != nil {
		return err
	}
	if err := ownIssueForTask(ctx, r.Client, proj.Namespace, name, task); err != nil {
		return err
	}
	refs := append(append([]string{}, task.Status.IssueRefs...), name)
	if err := r.patchTaskStatus(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
		for _, ref := range fresh.Status.IssueRefs {
			if ref == name {
				return false
			}
		}
		fresh.Status.IssueRefs = refs
		return true
	}); err != nil {
		return fmt.Errorf("triage: record issueRef %s: %w", name, err)
	}
	log.FromContext(ctx).Info("triage: minted the issue mirror",
		"action", "triage_mint_issue", "resource_id", task.Name, "issue", name)
	return nil
}

// sourceRepository resolves the Repository CR the Task's Source points at:
// spec.repositoryRef when set, else the project repo whose URL matches the
// source's issue URL. nil (no error) means "cannot resolve": triage then mints no
// Issue rather than guessing which repo a human's issue lives in.
func (r *TaskReconciler) sourceRepository(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task) (*tatarav1alpha1.Repository, error) {

	if task.Spec.RepositoryRef != "" {
		var repo tatarav1alpha1.Repository
		err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.RepositoryRef}, &repo)
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("triage: get repository %s: %w", task.Spec.RepositoryRef, err)
		}
		return &repo, nil
	}
	repos, err := r.projectRepos(ctx, proj)
	if err != nil {
		return nil, err
	}
	url := task.Spec.Source.URL
	for i := range repos {
		if url != "" && repoOwnsURL(repos[i].Spec.URL, url) {
			return &repos[i], nil
		}
	}
	return nil, nil
}

// reconcilePodStage drives the stages that RUN AN AGENT (F.2): ensure the
// admission ticket, wait for the dispatcher to admit it, ensure the pod, and -
// once PodWatchReconciler has run the G.10 handshake and armed CLOCK 3 - render
// the bundle and submit turn-0.
//
// It NEVER terminates a Task for queueing. A Task that waits 40 minutes behind
// three live agents in normal steady state reaches its stage normally (fixes
// V6-1, V7-7): the only thing that ends the wait is CLOCK 1, at 24h, and only
// when the project is not paused.
func (r *TaskReconciler) reconcilePodStage(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, agentKind string, now time.Time) (ctrl.Result, error) {

	l := log.FromContext(ctx)

	// B2: a COMMITTED outcome from THIS stage's own agent means there is nothing
	// left for a pod to do. Do NOT respawn a lost pod, do not TTL-rotate, do not
	// re-submit turn-0. Poll until the MergeRequest reconciler's drain advances
	// the stage, or until the handoff deadline parks it at handoff-stalled.
	//
	// handoffCondition - not "is anything committed" - because the condition is
	// per-TASK and survives across BOTH stages and stage re-entries: an implement
	// Task arrives at reviewing with Reason=Implement already committed, and a
	// re-entered reviewing Task (head move, awaiting-human unpark) arrives with the
	// LAST round's Reason=Review committed. A bare committed check would gag the
	// review pod that has not spawned yet in either case.
	if handoffCondition(task) != nil {
		l.Info("stage pod work suppressed: the outcome is committed and the handoff is in flight",
			"action", "pod_stage_suppressed_committed_outcome", "resource_id", task.Name,
			"state", task.Status.State, "agent_kind", agentKind)
		return ctrl.Result{RequeueAfter: stageRequeue}, nil
	}

	admitted, err := r.ensureTicket(ctx, proj, task, agentKind)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !admitted {
		// CLOCK 1 is armed and bounds this wait. Nothing else may end it.
		return ctrl.Result{RequeueAfter: admissionRequeue}, nil
	}

	// A pod that RAN and ended without an outcome - vanished, or died in place and
	// left its corpse standing. reconcileCaps owns the case where the pod had
	// become Ready (it parks no-outcome); reaching here means it never did, so:
	// respawn.
	if task.Status.PodStartedAt != nil {
		gone, reason, gerr := r.podUnusable(ctx, task)
		if gerr != nil {
			return ctrl.Result{}, gerr
		}
		if gone {
			if r.liveStageDiffers(ctx, task) {
				log.FromContext(ctx).Info("wrapper pod unusable but the live stage has moved; not respawning",
					"action", "pod_respawn_skipped_stage_moved", "resource_id", task.Name,
					"acting_stage", task.Status.State, "reason", reason)
				return ctrl.Result{RequeueAfter: agentBootRequeue}, nil
			}
			if reason == obs.RecreationReasonOOMKilled {
				if nerr := r.noteOOMKilledPod(ctx, proj, task); nerr != nil {
					return ctrl.Result{}, nerr
				}
			}
			return r.respawnLostPod(ctx, proj, task, reason, now)
		}
		// THE AGENT ASKED TO BE STOPPED, so stop it - now, not when some clock next
		// comes round. A handoff note written by THIS pod's agent with no turn in
		// flight is the agent saying it is done talking and has left everything the
		// next pod needs; until this branch existed the pod then sat there holding a
		// concurrency slot until the pod TTL, the conversation idle budget or the
		// idle reaper happened to notice, up to hours later. See agentAskedToBeStopped
		// for why both guards are load-bearing. Ahead of the TTL and stall checks
		// because it is the one stop that needs no evidence-gathering at all.
		if r.agentAskedToBeStopped(task) {
			return r.stopAfterAgentHandoff(ctx, proj, task, agentKind, now)
		}
		// G.7 TTL STOP (fix I5). A pod past t0 = podStartedAt + agentPodTTLSeconds is
		// GRACEFULLY stopped: the agent gets ONE handoff turn, else the operator
		// writes a synthetic handoff note - so status.notes is NEVER empty after a
		// stop - and the pod is deleted. Before this, ttlstop.go had ZERO callers:
		// pods were never TTL-stopped and the non-empty-notes guarantee never ran.
		if agent.TTLExpired(proj, task, now) {
			return r.ttlStop(ctx, proj, task, agentKind, now)
		}
		// INACTIVITY NO LONGER MEANS "KILL THE TURN"; IT MEANS "ASK THE AGENT".
		//
		// This used to be a single `turnTimedOut -> stalledTurnStop` branch, and its
		// only input was silence - which a healthy agent produces just as readily as
		// a wedged one (O1's measured case: 2095 seconds of parent-transcript
		// silence with a perfectly healthy subagent underneath). reconcileStall
		// probes the agent instead and only escalates when it cannot or will not
		// answer; an ANSWER lets the turn continue with no bound at all. A wrapper
		// with no probe endpoints falls straight back to the stalledTurnStop call
		// this branch used to make, verbatim. See internal/controller/stall.go.
		//
		// handled=false means the stall machinery had nothing to do (the turn is
		// inside its window), so ordinary stage work continues below exactly as it
		// did before this phase.
		if res, handled, err := r.reconcileStall(ctx, proj, task, agentKind, now); handled || err != nil {
			return res, err
		}
	}

	skipped, err := r.ensureStagePod(ctx, proj, task)
	if err != nil {
		return ctrl.Result{}, err
	}
	if skipped {
		// The Task has live-moved off this stage since dispatch top (a non-leader
		// webhook transition, F6-1); the pod was deliberately NOT created. Early-
		// return so we never fall through to a turn submission against a pod that
		// does not exist - do not rely on the StageWorkStartedAt/turn0-marker
		// consistency of the stale cached view to stop us. Requeue; the converged
		// stage drives the right pod.
		return ctrl.Result{RequeueAfter: stageRequeue}, nil
	}

	// CLOCK 3 is armed by PodWatchReconciler at pod-Ready, and NOT BEFORE: it runs
	// the G.10 contract handshake first, so a wrapper speaking the wrong contract
	// version fails the Task with agent-contract-mismatch BEFORE turn-0, with ZERO
	// turns submitted. Until then there is nothing to do but wait; CLOCK 2 bounds it.
	if task.Status.StateWorkStartedAt == nil {
		return ctrl.Result{RequeueAfter: agentBootRequeue}, nil
	}

	marker := turn0Marker(task)
	if task.Annotations[annStageTurn0] == marker {
		// THE CONVERSATION FOLLOW-UP TURN, and the ONLY stage that gets one.
		//
		// A pod's turn-0 renders the whole bundle, the agent works, it submits an
		// outcome, the state moves and the pod dies. A LIVE state is different by
		// construction - it is where a Task waits with a live agent for the human's
		// NEXT comment - so the pod must be able to take that comment as a further
		// turn or the warmth buys nothing. #521 widened this from the single
		// `conversing` stage to every live state.
		//
		// Three guards, all load-bearing:
		//   - the state is LIVE and un-parked, so a state that runs no pod is
		//     untouched;
		//   - pendingEvents non-empty, so this fires on a real delta and not on
		//     every 30s requeue (and the drain below is what makes it terminate);
		//   - no turn in flight, because POST /v1/messages 409s during a turn and a
		//     racing second submit is exactly the 2026-06 redelivery defect that
		//     double-injected text into a live session.
		if stage.Live(task.Status.State) && !tatarav1alpha1.Parked(task) &&
			len(task.Status.PendingEvents) > 0 && !taskHasInflightTurn(task) {
			return r.submitStageTurn(ctx, proj, task, agentKind, "conversation", now)
		}
		return ctrl.Result{RequeueAfter: stageRequeue}, nil // turn-0 already submitted for THIS pod
	}

	return r.submitStageTurn(ctx, proj, task, agentKind, "turn0", now)
}

// submitStageTurn renders the bundle and submits ONE turn to this Task's live
// pod, then stamps the turn annotations and spends the pendingEvents delta that
// was rendered into it. phase is "turn0" for a pod's first turn and
// "conversation" for a live pod's follow-up; it names the submit site in
// the failure path and in the log line, and is not otherwise behavioural.
//
// The annStageTurn0 marker is stamped for BOTH phases. For turn0 that is its
// original meaning (this pod has been given the bundle). For a conversation turn
// it is already set and re-stamping it is a no-op write that keeps the two paths
// identical rather than branching.
func (r *TaskReconciler) submitStageTurn(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, agentKind, phase string, now time.Time) (ctrl.Result, error) {

	l := log.FromContext(ctx)
	marker := turn0Marker(task)

	// Snapshot the events THIS turn is actually going to carry, before render and
	// before the network round trip below. AppendTaskEvent's caller (the webhook
	// handler) is NOT leader-elected and runs on every replica - unlike this
	// reconcile, which the per-object-key workqueue plus leader election already
	// serialises to at most one in flight per Task - so a new comment can land in
	// status.pendingEvents while SubmitTurn is still outstanding. The drain below
	// must remove exactly this snapshot, not whatever pendingEvents holds by the
	// time it runs, or that late comment is erased having never been rendered
	// into any turn (see drainRenderedEvents, task_events.go).
	rendered := append([]tatarav1alpha1.TaskEvent(nil), task.Status.PendingEvents...)

	text, err := r.renderBundle(ctx, proj, task, agentKind)
	if err != nil {
		return ctrl.Result{}, err
	}
	t0 := time.Now()
	turnID, serr := r.Session.SubmitTurn(ctx, agent.BaseURL(task, task.Namespace), text, r.callbackURL())
	elapsed := time.Since(t0).Seconds()
	if serr != nil {
		return r.handleTurnSubmitFailure(ctx, proj, task, serr, elapsed, phase)
	}
	r.Metrics.TurnSubmit(task.Spec.Kind, "ok", "ok", elapsed)
	l.Info("turn submitted",
		"action", "agent_turn_submit", "resource_id", task.Name, "turn_id", turnID,
		"state", task.Status.State, "agent_kind", agentKind, "phase", phase,
		"events", len(rendered), "bytes", len(text),
		"duration_ms", int64(elapsed*1000))

	if err := r.patchTaskAnnotations(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
		if fresh.Annotations == nil {
			fresh.Annotations = map[string]string{}
		}
		fresh.Annotations[annStageTurn0] = marker
		fresh.Annotations[annCurrentTurn] = turnID
		fresh.Annotations[annTurnStartedAt] = now.UTC().Format(time.RFC3339)
		delete(fresh.Annotations, annTurnComplete)
		delete(fresh.Annotations, annTurnLastActivity)
		return true
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("stamp turn marker (%s): %w", phase, err)
	}
	// E.3: the pending events RENDERED (the `rendered` snapshot above) are the
	// delta, and the delta is spent - remove exactly those, by value, from
	// whatever pendingEvents holds NOW. Anything appended since the snapshot (a
	// webhook write racing this turn) was never rendered and must survive to be
	// carried by a later turn. Draining is also what terminates the conversation
	// follow-up branch: with the rendered delta spent, the next reconcile finds
	// no NEW events (unless one raced in, in which case it correctly still does).
	if len(rendered) > 0 {
		if err := r.patchTaskStatus(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
			drained := drainRenderedEvents(fresh.Status.PendingEvents, rendered)
			if len(drained) == len(fresh.Status.PendingEvents) {
				return false // nothing of the rendered snapshot was still there to remove
			}
			fresh.Status.PendingEvents = drained
			return true
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("drain pending events: %w", err)
		}
	}
	return ctrl.Result{RequeueAfter: stageRequeue}, nil
}

// StalledTurnHandoffWait bounds the whole graceful stop of a STALLED turn.
//
// It is deliberately NOT derived from turnTimeoutSeconds the way the TTL path's
// cap is. We only get here because a turn already burned its entire turn timeout
// plus turnTimeoutGrace without a single activity tick, so spending another
// 2*turnTimeout waiting on it would be waiting hours for the thing we just
// declared dead. The only thing left worth waiting for is the seconds-wide race
// where the turn completes between the stall check and this call - and if it
// does clear, the agent gets its handoff turn like any other stop.
const StalledTurnHandoffWait = 90 * time.Second

// stalledTurnStop runs the SAME G.7 sequence as ttlStop for a turn that stopped
// reporting activity, on a tight bound, and clears the turn annotations so a
// late callback cannot resolve the Task afterwards.
//
// A genuinely hung wrapper never goes idle, so the usual outcome here is
// synthetic_handoff: waitIdle times out, no handoff turn can be submitted (POST
// /v1/messages 409s while a turn is in flight and there is no cancel primitive),
// and the operator writes the handoff note itself. That is the entire point -
// the old hard teardown left NO note at all, which is what made the next pod
// start from nothing.
func (r *TaskReconciler) stalledTurnStop(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, agentKind string, now time.Time) (ctrl.Result, error) {

	turnID := task.Annotations[annCurrentTurn]
	if r.Metrics != nil {
		r.Metrics.TurnTimeout("stage_reconcile")
	}
	var sp objbudget.Spiller
	if r.SpillerFor != nil {
		sp = r.SpillerFor(proj)
	}
	stopper := &agent.TTLStopper{
		Client:  r.Client,
		Session: r.Session,
		Notes: &agent.FitNoteAppender{
			Client:    r.Client,
			Spiller:   sp,
			Namespace: task.Namespace,
		},
		Namespace:            task.Namespace,
		Record:               obs.AgentPodTTLExpired,
		RecordEmptySynthetic: obs.AgentSyntheticHandoffEmpty,
	}
	res, err := stopper.StopWithHandoff(ctx, task, agent.TTLStopInput{
		BaseURL:     agent.BaseURL(task, task.Namespace),
		CallbackURL: r.callbackURL(),
		AgentKind:   agentKind,
		// now, not a pod t0: the stall is what ended this pod, and the bound runs
		// from the moment we noticed it.
		Deadline:    now,
		TurnTimeout: time.Duration(proj.Spec.Agent.TurnTimeoutSeconds) * time.Second,
		MaxWait:     StalledTurnHandoffWait,
		Cause:       agent.TTLCauseStall,
		// #527: the same persisted continuation state ttlStop uses. A stalled turn
		// is the case that needs it MOST - a genuinely hung wrapper never goes
		// idle, so this caller almost always lands on the synthetic note.
		LastFinalText: task.Status.LastTurnFinalText,
		PushedRepos:   task.Status.LastTurnPushedRepos,
	})
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("stalled turn stop %s: %w", task.Name, err)
	}
	// Clear the turn so a late callback cannot resolve it, and re-arm the pod
	// clocks so the next reconcile spawns a continuation pod - the same re-arm
	// ttlStop does, plus the annotation clearing the old hard-teardown path owned.
	if err := clearTurnAnnotations(ctx, r.Client, task); err != nil {
		return ctrl.Result{}, fmt.Errorf("stalled turn clear %s: %w", task.Name, err)
	}
	if err := r.patchTaskStatus(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
		fresh.Status.PodStartedAt = nil
		fresh.Status.StateWorkStartedAt = nil
		clearLastTurn(fresh)
		return true
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("stalled turn re-arm %s: %w", task.Name, err)
	}
	logTTLStop(ctx,
		"stalled turn stopped gracefully; handed off",
		"stalled turn stopped with NO continuation state captured; this turn's work is unrecorded",
		"stalled_turn_stop", res, "resource_id", task.Name, "turn_id", turnID,
		"agent_kind", agentKind, "wait", StalledTurnHandoffWait.String())
	return ctrl.Result{RequeueAfter: agentBootRequeue}, nil
}

// ttlStop runs the G.7 stop sequence for a pod past its TTL and re-arms the Task
// for a fresh continuation pod. The stage is UNCHANGED: nil'ing the pod clocks
// makes the next reconcile spawn a new pod that resumes from the handoff note.
// Total work stays bounded by maxTurnsPerTask and the stage work clock, so a TTL
// rotation is NOT charged to the crash-recreation budget.
func (r *TaskReconciler) ttlStop(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, agentKind string, now time.Time) (ctrl.Result, error) {
	_ = now

	deadline, ok := agent.TTLDeadline(proj, task)
	if !ok {
		return ctrl.Result{}, nil // no TTL configured; nothing to stop
	}
	var sp objbudget.Spiller
	if r.SpillerFor != nil {
		sp = r.SpillerFor(proj)
	}
	stopper := &agent.TTLStopper{
		Client:  r.Client,
		Session: r.Session,
		Notes: &agent.FitNoteAppender{
			Client:    r.Client,
			Spiller:   sp,
			Namespace: task.Namespace,
		},
		Namespace:            task.Namespace,
		Record:               obs.AgentPodTTLExpired,
		RecordEmptySynthetic: obs.AgentSyntheticHandoffEmpty,
	}
	in := agent.TTLStopInput{
		BaseURL:     agent.BaseURL(task, task.Namespace),
		CallbackURL: r.callbackURL(),
		AgentKind:   agentKind,
		Deadline:    deadline,
		TurnTimeout: time.Duration(proj.Spec.Agent.TurnTimeoutSeconds) * time.Second,
		Cause:       agent.TTLCauseTTL,
		// The last turn-complete callback's finalText + pushedRepos, persisted onto
		// the Task by CallbackServer.stampLastTurn. Without them the synthetic note
		// degraded to "(none)"/"none" on EVERY synthetic path, which is what made
		// the non-empty-notes guarantee vacuous (#527).
		LastFinalText: task.Status.LastTurnFinalText,
		PushedRepos:   task.Status.LastTurnPushedRepos,
	}
	res, err := stopper.StopWithHandoff(ctx, task, in)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("ttl stop %s: %w", task.Name, err)
	}
	if err := r.patchTaskStatus(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
		fresh.Status.PodStartedAt = nil
		fresh.Status.StateWorkStartedAt = nil
		clearLastTurn(fresh)
		return true
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("ttl stop re-arm %s: %w", task.Name, err)
	}
	// The pod that was running the turn is gone (StopWithHandoff deleted it), so
	// the turn goes with it - the same clear stalledTurnStop does, for the same
	// reason. Without it a TTL rotation that caught the pod mid-turn hands the
	// replacement pod a Task that still claims a turn is in flight, which gags
	// its conversation follow-up turn and disarms its idle clock until turn-0
	// happens to overwrite the annotation (#566).
	//
	// TWO DIFFERENT THINGS, both retired here and neither a substitute for the
	// other: this clears the POD-SCOPED TURN annotations, and the status patch
	// above clears the LAST-TURN CONTINUATION STATE the stop just spent on a
	// handoff note (#527). One says "a turn is running"; the other says "here is
	// what the last turn produced".
	if err := clearTurnAnnotations(ctx, r.Client, task); err != nil {
		return ctrl.Result{}, fmt.Errorf("ttl stop turn clear %s: %w", task.Name, err)
	}
	logTTLStop(ctx,
		"agent pod TTL-stopped; handed off",
		"agent pod TTL-stopped with NO continuation state captured; the previous pod's work is unrecorded",
		"agent_pod_ttl_stop", res, "resource_id", task.Name, "agent_kind", agentKind)
	return ctrl.Result{RequeueAfter: agentBootRequeue}, nil
}

// logTTLStop logs one G.7 stop at a level and with a wording that match what
// actually happened.
//
// The single line this replaces read "agent pod TTL-stopped; handed off" at INFO
// on EVERY path, including the one where nothing whatsoever was handed off. The
// failure mode therefore had no log signature at all: grepping the operator's
// stdout for the #527 incident turned up two lines, both of them claiming
// success. A stop that captured nothing is an ERROR, and it says so in words a
// human reading Loki can act on.
func logTTLStop(ctx context.Context, okMsg, lostMsg, action string, res agent.TTLStopResult, fields ...any) {
	fields = append([]any{"action", action, "outcome", res.Outcome, "handoff", res.Handoff}, fields...)
	l := log.FromContext(ctx)
	if res.Handoff == agent.TTLHandoffNone {
		l.Error(nil, lostMsg, fields...)
		return
	}
	l.Info(okMsg, fields...)
}

// clearLastTurn retires the continuation state a G.7 stop has just spent.
//
// Every StopWithHandoff caller owes this. By the time a stop returns, the
// payload is IN the notes journal - either the agent's own handoff note or the
// operator's synthetic reconstruction of it - and the journal is what the next
// pod reads. Leaving it on the status means the NEXT pod's stop re-renders the
// SAME turn's text as though it were that pod's work, which is a worse failure
// than the empty note #527 was filed about: an empty note is recognisably empty,
// a stale one is confidently wrong (#527).
//
// stage.stampEnter does the same on every state transition. The one path that
// deliberately does NOT is respawnLostPod; see the argument there.
func clearLastTurn(t *tatarav1alpha1.Task) {
	t.Status.LastTurnFinalText = ""
	t.Status.LastTurnPushedRepos = nil
}

// turn0Marker identifies the pod turn-0 was submitted to. A respawn re-stamps
// podStartedAt and the replacement pod holds a FRESH claude session that has
// never seen the bundle, so it needs turn-0 again.
func turn0Marker(task *tatarav1alpha1.Task) string {
	at := ""
	if task.Status.PodStartedAt != nil {
		at = task.Status.PodStartedAt.UTC().Format(time.RFC3339)
	}
	return task.Status.State + "|" + at
}

func (r *TaskReconciler) callbackURL() string {
	return strings.TrimSuffix(r.PodConfig.CallbackURL, "/") + "/internal/turn-complete"
}

// renderBundle is contract E: the ENTIRE turn-0 text. The bundle IS the
// continuation state - the Task's owned Issues and MergeRequests with their
// threads, its pending events, its notes journal - plus the operator-authored
// assignment for this agent kind. There is no continuation preamble and no
// per-stage prompt builder: the nine hand-rolled assembly sites this replaces
// each re-derived a partial view of the same state and disagreed about it.
func (r *TaskReconciler) renderBundle(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, agentKind string) (string, error) {

	issues, err := ownedIssueCRs(ctx, r.Client, task)
	if err != nil {
		return "", err
	}
	mrs, err := ownedMergeRequests(ctx, r.Client, task)
	if err != nil {
		return "", err
	}
	history, err := ProposalHistoryFor(ctx, r.Client, proj, agentKind)
	if err != nil {
		return "", err
	}
	out, err := prompt.Render(prompt.Input{
		Task:            task,
		Issues:          issues,
		MergeRequests:   mrs,
		Events:          task.Status.PendingEvents,
		Notes:           task.Status.Notes,
		ProposalHistory: history,
		Assignment:      assignmentFor(agentKind, task, proj, agent.WorkspacePVCEnabled(proj, r.PodConfig)),
		MaxBundleBytes:  proj.Spec.MaxBundleBytes,
		Metrics:         r.BundleMetrics,
	})
	if err != nil {
		return "", fmt.Errorf("render bundle for %s: %w", task.Name, err)
	}
	return out, nil
}

// respawnLostPod handles a pod that RAN and is gone. It burns one podRecreations
// and ALWAYS respawns: O3 deleted maxPodRecreations, so there is no terminal
// here any more and park(pod-recreation-exhausted) is no longer reachable from
// this path. pod-not-ready is NOT a park reason and appears nowhere either.
//
// THE COUNT STAYS. stats.podRecreations feeds operator_pod_recreations_total,
// which is the input to the churn alert that replaced the cap
// (sum by (project) (increase(...[1h])) > 6, critical). A crash loop is now
// bounded only by the 24h stage.ResidencyCapAll; the alert is what makes it
// visible before then.
//
// reason is the obs.RecreationReason* label for WHY the pod is being replaced.
//
// proj and now are retained in the signature for call-site symmetry with the
// other pod-lifecycle handlers; nothing in here reads them any more.
func (r *TaskReconciler) respawnLostPod(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, reason string, now time.Time) (ctrl.Result, error) {

	_, _ = proj, now
	// THE CORPSE GOES FIRST, and this is not tidying. This function was written for
	// a pod that had already VANISHED; it is now also reached for one that died in
	// place (RestartPolicy: Never keeps a Failed Pod object standing forever). Pod
	// names are STABLE PER TASK (agent.PodName / BuildPodName), so leaving the
	// corpse means ensureStagePod finds a pod already annotated for this stage on
	// the next pass, returns early, and the replacement is never created - the Task
	// would respawn-count forever and never get a pod. Idempotent: DeleteWrapper
	// swallows NotFound, which is the vanished case.
	if err := r.deleteWrapper(ctx, task); err != nil {
		return ctrl.Result{}, fmt.Errorf("pod respawn corpse delete %s: %w", task.Name, err)
	}
	stage.RecordRespawn(task)
	recreations := task.Status.Stats.PodRecreations
	obs.PodRecreation(task.Spec.ProjectRef, task.Spec.Kind, reason)
	// status.lastTurn* is DELIBERATELY left standing here, unlike the structurally
	// identical patches in ttlStop and stalledTurnStop, which clear it. What
	// differs is what each path already did with it: those two have just SPENT it
	// on a handoff note, so keeping it would replay one turn onto a second pod.
	// This path writes NO note at all - a pod that vanished mid-turn produced
	// nothing to write - so the status is the only surviving trace of the dead
	// pod's work, and clearing it would turn a recoverable crash into guaranteed
	// loss (#527).
	if err := r.patchTaskStatus(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
		fresh.Status.Stats.PodRecreations = recreations
		fresh.Status.PodStartedAt = nil
		fresh.Status.StateWorkStartedAt = nil
		return true
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("record pod respawn: %w", err)
	}
	// The pod IS GONE - it vanished mid-turn, or it died in place and was just
	// deleted above - so the turn died with it and nothing will ever complete it. Clearing here also
	// stops the poll backstop shouting about a stall the reconciler cannot reach:
	// the pod clocks are nil now, so reconcilePodStage's turnTimedOut branch (and
	// therefore stalledTurnStop) is unreachable until a replacement pod exists.
	//
	// THIS PATH CLEARS THE TURN ANNOTATIONS AND KEEPS THE LAST-TURN PAYLOAD, and
	// that asymmetry is deliberate on both halves - do not "simplify" either into
	// the other. They answer different questions about different scopes:
	//
	//	turn annotations (#566)  - "a turn is running in a pod". POD-SCOPED. The
	//	                           pod is gone, so this is now a lie, and four
	//	                           other mechanisms act on it.
	//	status.lastTurn* (#527)  - "here is what the last turn PRODUCED". It is
	//	                           the only surviving trace of the dead pod's
	//	                           work, because this path writes no handoff note
	//	                           (see the patch above), so clearing it turns a
	//	                           recoverable crash into guaranteed loss.
	//
	// ttlStop and stalledTurnStop clear BOTH, for the same reason read the other
	// way: they have already spent the payload on a handoff note.
	if err := clearTurnAnnotations(ctx, r.Client, task); err != nil {
		return ctrl.Result{}, fmt.Errorf("pod respawn turn clear %s: %w", task.Name, err)
	}
	log.FromContext(ctx).Info("agent pod lost; respawning",
		"action", "pod_respawn", "resource_id", task.Name, "pod_recreations", recreations, "reason", reason)
	return ctrl.Result{RequeueAfter: agentBootRequeue}, nil
}

// liveStageDiffers re-reads the Task's stage from the API SERVER (never the
// cache) and reports whether it has moved off the stage this reconcile is acting
// on. It exists for the pod-(re)spawn branches (the #348/#352 live-read idiom):
// the operator runs 3-replica HA (leaderElection: 1 active + 2 standby) and the
// webhook serves on ALL replicas, so a non-leader can apply a human-review
// transition and delete the wrapper pod after refreshTaskFromAPI already adopted
// the older stage at dispatch top. Re-reading live right before a pod (re)create
// catches a transition that landed since, so the leader does not spawn a pod for
// a stage the Task has left. Cheap: only runs when a pod is absent (rare). A read
// error returns false (proceed): the create/respawn paths own their own errors,
// and a nil APIReader (unit tests) trusts the cached read.
func (r *TaskReconciler) liveStageDiffers(ctx context.Context, task *tatarav1alpha1.Task) bool {
	if r.APIReader == nil {
		return false
	}
	live := &tatarav1alpha1.Task{}
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(task), live); err != nil {
		return false
	}
	return live.Status.State != task.Status.State
}

// podUnusable reports whether the Task's wrapper Pod can still serve a turn, and
// the obs.RecreationReason* label for why it cannot.
//
// IT USED TO ASK ONLY "DOES THE POD OBJECT EXIST" (podGone), and with
// RestartPolicy: Never that is not the same question. A wrapper the kernel
// OOM-kills leaves the Pod OBJECT standing in phase Failed, forever: NotFound is
// false, so nothing here fired, no respawn was ever counted, and the Task sat at
// its stage holding one of the project's live-pod slots until the turn-inactivity
// clock noticed ~59 minutes later. A pod nobody can talk to is gone in every
// sense the caller cares about.
//
// THE EXIT-ZERO CARVE-OUT IS LOAD-BEARING - do not "simplify" it to "any
// terminated container". A wrapper that exits 0 is one whose agent wrote its
// handoff note and asked to be stopped, and agentAskedToBeStopped ->
// stopAfterAgentHandoff owns that pod: it tears it down and SPENDS the last-turn
// payload. This check runs FIRST, so treating a clean exit as unusable would
// divert every graceful finish into respawnLostPod - a spurious increment on the
// churn alert, and a payload kept alive for a later synthetic note to replay as
// if it were fresh. Success is not a fault.
func (r *TaskReconciler) podUnusable(ctx context.Context, task *tatarav1alpha1.Task) (bool, string, error) {
	pod := &corev1.Pod{}
	err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: agent.PodName(task)}, pod)
	if apierrors.IsNotFound(err) {
		return true, obs.RecreationReasonPodGone, nil
	}
	if err != nil {
		return false, "", fmt.Errorf("get wrapper pod: %w", err)
	}
	reason, _ := podTerminationReason(pod)
	return reason != "", reason, nil
}

// podTerminationReason classifies a wrapper Pod that has stopped being able to
// serve turns, and reports WHEN the container died. An empty reason means the pod
// is still usable.
//
// A container's own terminated state is preferred over the Pod phase because it
// carries the ACTUAL cause - OOMKilled is only ever visible there, and it is the
// reason the recreation-loop runbook most needs to see.
func podTerminationReason(pod *corev1.Pod) (string, time.Time) {
	for _, cs := range pod.Status.ContainerStatuses {
		term := cs.State.Terminated
		if term == nil {
			continue
		}
		if term.Reason == agent.ReasonOOMKilled {
			return obs.RecreationReasonOOMKilled, term.FinishedAt.Time
		}
		if term.ExitCode != 0 {
			return obs.RecreationReasonContainerExited, term.FinishedAt.Time
		}
	}
	if pod.Status.Phase == corev1.PodFailed {
		return obs.RecreationReasonPodFailed, time.Time{}
	}
	return "", time.Time{}
}

// noteOOMKilledPod puts the OOM kill in the Task's notes journal BEFORE the pod
// is parked or respawned away.
//
// It exists because the two halves of this fix would otherwise not meet.
// TTLStopper.writeSyntheticNote also emits an OOM note, but podUnusable now
// catches most OOM kills FIRST - the reconcile that spots the corpse parks or
// respawns straight away, and no stop sequence ever runs. Without this the
// warning would be written almost nowhere, and the next agent would still be sent
// hunting for a commit that never left the dead pod's ephemeral workspace.
//
// Best-effort by design: an unreadable pod, a non-OOM death, or a note that has
// already landed is a no-op. It must never be the thing that blocks a Task from
// getting off a dead pod.
func (r *TaskReconciler) noteOOMKilledPod(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task) error {

	pod := &corev1.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: agent.PodName(task)}, pod); err != nil {
		return client.IgnoreNotFound(err)
	}
	reason, at := podTerminationReason(pod)
	if reason != obs.RecreationReasonOOMKilled {
		return nil
	}
	for _, n := range task.Status.Notes {
		if n.Agent == agent.NoteAgentOperator && strings.Contains(n.Body, agent.OOMKilledNoteMarker) && !n.At.Time.Before(at) {
			return nil
		}
	}
	notes := &agent.FitNoteAppender{Client: r.Client, Spiller: r.spiller(proj), Namespace: task.Namespace}
	return notes.AppendNote(ctx, task.Name, tatarav1alpha1.Note{
		At:    metav1.NewTime(at),
		Agent: agent.NoteAgentOperator,
		Kind:  agent.NoteKindHandoff,
		Body:  agent.OOMKilledNoteBody(at),
	})
}

// ensureStagePod creates the wrapper Pod + Service for the CURRENT stage, and
// tears down a pod left over from a stage this Task has already LEFT (see
// annPodStage). It returns skipped=true when it deliberately did NOT create a
// pod because the live stage has moved off the one this reconcile is acting on
// (F6-1); the caller must early-return and requeue rather than proceed to turn
// submission.
func (r *TaskReconciler) ensureStagePod(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task) (skipped bool, err error) {

	var repo *tatarav1alpha1.Repository
	if task.Spec.RepositoryRef != "" {
		var got tatarav1alpha1.Repository
		if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.RepositoryRef}, &got); err != nil {
			return false, fmt.Errorf("get repository %s: %w", task.Spec.RepositoryRef, err)
		}
		repo = &got
	}

	existing := &corev1.Pod{}
	getErr := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: agent.PodName(task)}, existing)
	switch {
	case getErr == nil:
		if existing.Annotations[annPodStage] == task.Status.State {
			return false, nil
		}
		log.FromContext(ctx).Info("wrapper pod belongs to a stage this task has left; deleting",
			"action", "stale_stage_pod_delete", "resource_id", task.Name,
			"pod_stage", existing.Annotations[annPodStage], "state", task.Status.State)
		return false, agent.DeleteWrapper(ctx, r.Client, task.Namespace, task)
	case !apierrors.IsNotFound(getErr):
		return false, fmt.Errorf("get wrapper pod: %w", getErr)
	}

	// The pod is ABSENT. Before creating one, re-read the stage LIVE: production
	// runs the operator 3-replica HA and the webhook serves on every replica, so a
	// NON-leader can apply a human-review transition and tear the pod down since
	// dispatch-top's refreshTaskFromAPI adopted the older stage. Creating a pod for
	// a stage the Task has live-left - e.g. a review pod after a maintainer approval
	// already advanced to merging, whose /outcome would then re-arm a bot review
	// over the human's - is the F6-1 race. Skip; the caller requeues and the
	// converged stage drives the right pod. This does not fully close the narrow
	// delete-pod-before-transition window in ApplyReviewApproval (live stage still
	// reads reviewing there); DrainPendingReview's reviewing-gate is the correctness
	// backstop for any pod that slips through.
	if r.liveStageDiffers(ctx, task) {
		log.FromContext(ctx).Info("wrapper pod absent but the live stage has moved; skipping create",
			"action", "pod_create_skipped_stage_moved", "resource_id", task.Name, "acting_stage", task.Status.State)
		return true, nil
	}

	// PRE-DISPATCH EXTERNAL-TERMINAL GUARD (fixes #33 at the source; widened past
	// kind=review by #578). Before building a review pod, if every owned MR
	// already reached a terminal forge state, finalize the Task pod-lessly and
	// spawn NOTHING: re-reviewing a merged/closed PR can only no-op on
	// submit_outcome and respawn-loop. Reuses externalTerminalEdge so this can
	// never disagree with reconcileClocks or reviewAdvanceEdge.
	//
	// The awaiting-review disjunct is what covers a NON-review Task, whose review
	// pod is dispatched from that state (AgentKindFor keys it on the state).
	if task.Spec.Kind == "review" || task.Status.State == tatarav1alpha1.StateAwaitingReview {
		mrs, mrErr := ownedMergeRequests(ctx, r.mrReader(), task)
		if mrErr != nil {
			return false, mrErr
		}
		if edge, ok := externalTerminalEdge(task, mrs); ok {
			log.FromContext(ctx).Info("review pod not spawned: every owned MR reached a terminal forge state externally",
				"action", "review_finalize_terminal_mr", "resource_id", task.Name,
				"kind", task.Spec.Kind, "to", edge.To, "reason", edge.Reason)
			if err := r.enter(ctx, proj, task, mrs, edge.To, edge.Reason, time.Now()); err != nil {
				return false, err
			}
			return true, nil
		}
		// Takeover sibling of the guard above: a taken-over parent controller-owns zero
		// MRs, so terminalMREdge cannot fire, but re-spawning its review pod only 400s
		// on submit_outcome and respawn-loops. Finalize pod-lessly at
		// rejected(mr-taken-over) and spawn nothing.
		if len(mrs) == 0 {
			over, oerr := TaskTakenOver(ctx, r.mrReader(), task)
			if oerr != nil {
				return false, oerr
			}
			if over {
				log.FromContext(ctx).Info("review pod not spawned: the review target was taken over by a maintainer",
					"action", "review_finalize_taken_over", "resource_id", task.Name,
					"to", tatarav1alpha1.StateRejected, "reason", stage.ReasonMRTakenOver)
				if err := r.enter(ctx, proj, task, mrs,
					tatarav1alpha1.StateRejected, stage.ReasonMRTakenOver, time.Now()); err != nil {
					return false, err
				}
				return true, nil
			}
		}
	}

	if err := agent.ValidatePodSecretRefs(proj, r.PodConfig); err != nil {
		return false, err
	}
	// The workspace sizes are per-PROJECT CRD strings, so they are validated here
	// rather than at config load, and with ParseQuantity rather than MustParse: a
	// typo in one Project's spec must not panic the reconcile hot path for every
	// Project. Only asked when the feature is actually on for this Project -
	// numbers nothing reads must not be able to fail a spawn.
	if agent.WorkspacePVCEnabled(proj, r.PodConfig) {
		if err := agent.ValidateWorkspaceQuantities(proj); err != nil {
			return false, err
		}
	}

	// THE POD MUST NOT BE CREATED UNTIL ITS VOLUMES ARE BOUND. Taking the
	// skipped path instead of building a pod is what makes an unprovisionable
	// volume cost ZERO pods rather than ~288 a day through handleNotReady's
	// respawn (the pod-recreation ceiling is gone). ensureWorkspacePVC bounds
	// the wait itself and parks the Task if the volume never binds.
	wsReady, err := r.ensureWorkspacePVC(ctx, proj, task)
	if err != nil {
		return false, err
	}
	if !wsReady {
		return true, nil
	}

	repos, err := r.projectRepos(ctx, proj)
	if err != nil {
		return false, err
	}
	memEndpoint := ""
	if proj.Status.Memory != nil {
		memEndpoint = proj.Status.Memory.Endpoint
	}
	pod := agent.BuildPod(proj, repo, task, repos, memEndpoint, r.PodConfig)
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[annPodStage] = task.Status.State
	if err := r.Create(ctx, pod); err != nil && !apierrors.IsAlreadyExists(err) {
		return false, fmt.Errorf("create wrapper pod: %w", err)
	}
	svc := agent.BuildService(proj, repo, task, r.PodConfig)
	if err := r.Create(ctx, svc); err != nil && !apierrors.IsAlreadyExists(err) {
		return false, fmt.Errorf("create wrapper service: %w", err)
	}

	// This pod runs DEGRADED: it will spawn, work and submit an outcome, but its
	// recall tools fail for the whole turn. Do not rely on the agent noticing -
	// record it here, where the spawn actually happens. The counter is the fleet
	// signal; the Task condition is what a human reads on the CR. Deliberately
	// NO issue comment: the memory-stack alert is the human-facing signal, and a
	// comment per Task was noise-per-task.
	//
	// A project with memory DISABLED is not degraded: it has no stack by design.
	// It still gets a condition (a human reading the Task must be able to see the
	// agent had no recall) but as False/MemoryDisabled and with no
	// operator_agent_pod_degraded_total increment, so a memory-free project does
	// not read as a permanent fleet-wide degradation.
	if !tatarav1alpha1.MemoryStablyReady(proj, time.Now()) {
		disabled := tatarav1alpha1.MemoryDisabled(proj)
		status, reason := metav1.ConditionTrue, tatarav1alpha1.ReasonSpawnedWithoutRecall
		msg := "project memory stack is not stably ready; the agent runs with no recall"
		if disabled {
			status, reason = metav1.ConditionFalse, tatarav1alpha1.ReasonMemoryDisabled
			msg = "memory is disabled for this project; the agent runs without recall by configuration"
		} else {
			r.Metrics.AgentPodDegraded(proj.Name, task.Spec.Kind, "memory")
		}
		log.FromContext(ctx).Info("agent pod spawned with memory recall unavailable",
			"action", "agent_pod_degraded", "resource_id", task.Name,
			"project", proj.Name, "subsystem", "memory", "configured_off", disabled)
		if err := r.patchTaskStatus(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
			return meta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
				Type:               tatarav1alpha1.ConditionMemoryDegraded,
				Status:             status,
				Reason:             reason,
				Message:            msg,
				ObservedGeneration: fresh.Generation,
			})
		}); err != nil {
			// Non-fatal, exactly like stampResolvedModel below: the pod is already
			// created and failing the reconcile here would only re-enter on a path
			// that early-returns on the existing pod, losing the condition anyway.
			log.FromContext(ctx).Error(err, "could not record the MemoryDegraded condition",
				"action", "agent_pod_degraded_condition_failed", "resource_id", task.Name)
		}
	}

	repoURL := ""
	if repo != nil {
		repoURL = repo.Spec.URL
	}
	_ = r.stampResolvedModel(ctx, task, agent.ModelForKind(proj, task.Spec.Kind, task.Labels[tatarav1alpha1.LabelActivity], repoURL))
	return false, nil
}

// ensureTicket ensures the ADMISSION TICKET (a QueuedEvent naming this Task) for
// the pod this stage needs, and reports whether the dispatcher has ADMITTED it.
//
// The ticket - not the Task - is the unit of admission (B.7). `approved` is a
// POD-LESS stage that nonetheless needs one: F.3's approved -> implementing edge
// IS the admission of the implement pod's ticket, and the DISPATCHER applies that
// edge (queue_controller.go admitTicket). This reconciler must never apply it
// itself: that would double-transition.
func (r *TaskReconciler) ensureTicket(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, agentKind string) (bool, error) {

	// Indexed, not a namespace-wide scan: this runs on EVERY reconcile of every
	// pod-stage Task, and the unindexed form was O(all queued events). Falls
	// back to a full-namespace scan only for a direct (uncached) client with no
	// registered field index (isFieldSelectorUnsupported, same as projectRepos
	// above and sweep.go) - a raw envtest client cannot register a field index
	// at all. A CACHED client (production, via registerFieldIndexes; every
	// fake-client test, via WithIndex) that is missing the index fails loudly
	// instead, and mirror_test.go's TestIndexesRegistered is the guard that the
	// production registration cannot be dropped.
	var qel tatarav1alpha1.QueuedEventList
	err := r.List(ctx, &qel, client.InNamespace(task.Namespace),
		client.MatchingFields{queue.TaskRefIndex: task.Name})
	if err != nil && isFieldSelectorUnsupported(err) {
		qel = tatarav1alpha1.QueuedEventList{}
		err = r.List(ctx, &qel, client.InNamespace(task.Namespace))
	}
	if err != nil {
		return false, fmt.Errorf("list admission tickets for task %s: %w", task.Name, err)
	}
	for i := range qel.Items {
		q := &qel.Items[i]
		if q.Spec.Payload.TaskRef != task.Name || q.Spec.Payload.AgentKind != agentKind {
			continue
		}
		return q.Status.State == tatarav1alpha1.QueueStateAdmitted, nil
	}
	if r.Seq == nil {
		// No sequence source (unit tests): admission is not modelled, so the pod
		// path runs unqueued rather than wedging.
		return true, nil
	}

	// Classed by the STAGE's agent kind, not task.Spec.Kind: an incident Task's
	// downstream stages (clarify, implement, ...) are normal-class tickets. Only
	// investigating - whose agentKind IS incident - draws from AlertCapacity;
	// classing every stage of an incident Task as alert starved its own
	// downstream tickets behind AlertCapacity=1 alongside investigating itself.
	class := tatarav1alpha1.QueueClassNormal
	if agentKind == stage.AgentIncident {
		class = tatarav1alpha1.QueueClassAlert
	}
	var opts []queue.EnqueueOption
	if task.Spec.Kind == "incident" {
		// An incident Task's downstream stages (clarify, implement, ...) are
		// normal-class tickets (see the comment above), but they must still
		// outrank sweep-originated normal-class work: admitPool sorts by
		// (priority, seq), so without this a stuck incident could starve
		// behind 150 sweep events (issue #402).
		opts = append(opts, queue.WithPriority(0))
	}
	_, created, err := queue.EnqueueEvent(ctx, r.Client, r.Seq, proj, class, true,
		fmt.Sprintf("ticket|%s|%s", task.Name, agentKind),
		tatarav1alpha1.QueuedEventPayload{
			Kind:          task.Spec.Kind,
			RepositoryRef: task.Spec.RepositoryRef,
			AgentKind:     agentKind,
			TaskRef:       task.Name,
		}, opts...)
	if err != nil {
		return false, fmt.Errorf("enqueue admission ticket for %s: %w", task.Name, err)
	}
	if created {
		log.FromContext(ctx).Info("admission ticket enqueued",
			"action", "queue_ticket_enqueue", "resource_id", task.Name,
			"state", task.Status.State, "agent_kind", agentKind, "class", class)
	}
	return false, nil
}

// ownIssueForTask appends task as the Issue's controller owner (B.2 rule 1). It
// is the free-function twin of ProjectReconciler.ownIssue - the sweep's mint and
// triage's mint are the same adopt-or-create and must not diverge.
func ownIssueForTask(ctx context.Context, c client.Client, ns, name string, task *tatarav1alpha1.Task) error {
	key := types.NamespacedName{Namespace: ns, Name: name}
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var iss tatarav1alpha1.Issue
		if err := c.Get(ctx, key, &iss); err != nil {
			return err
		}
		if cur, ok := own.ControllerOwner(&iss); ok {
			if cur != task.Name {
				return fmt.Errorf("issue %s already has controller owner %s", name, cur)
			}
			return nil
		}
		own.AddPlainOwner(&iss, task)
		if err := own.HandOverController(&iss, nil, task); err != nil {
			return err
		}
		return c.Update(ctx, &iss)
	})
	if err != nil {
		return fmt.Errorf("triage: own issue %s: %w", name, err)
	}
	return nil
}

// repoOwnsURL reports whether an issue URL belongs to a repository's remote.
func repoOwnsURL(repoURL, itemURL string) bool {
	slug, _, ok := parseIssueURL(itemURL)
	if !ok {
		return false
	}
	owner, name, err := scm.OwnerRepo(repoURL)
	if err != nil {
		return false
	}
	return owner+"/"+name == slug
}
