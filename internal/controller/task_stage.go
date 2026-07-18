package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

// defaultMaxTurnsPerTask is contract A.6's LIFETIME turn backstop across every
// pod of a Task, used when neither the Task nor the Project sets one.
const defaultMaxTurnsPerTask = 300

func taskMaxTurns(proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task) int {
	if task.Spec.MaxTurnsPerTask > 0 {
		return task.Spec.MaxTurnsPerTask
	}
	if proj != nil && proj.Spec.Agent.MaxTurnsPerTask > 0 {
		return proj.Spec.Agent.MaxTurnsPerTask
	}
	return defaultMaxTurnsPerTask
}

func taskMaxPodRecreations(proj *tatarav1alpha1.Project) int {
	if proj != nil && proj.Spec.Agent.MaxPodRecreations > 0 {
		return proj.Spec.Agent.MaxPodRecreations
	}
	return maxPodRecreations
}

// projectPaused is Project.spec.maxConcurrentAgents == 0, the kill switch. It
// disarms CLOCK 1 (and the pod-less `approved` stage's admission budget, which is
// the same clock by another name). Without the carve-out the pause is a backlog
// shredder: every Task waiting for a slot parks at admission-starved 24h later.
func projectPaused(proj *tatarav1alpha1.Project) bool {
	return proj != nil && proj.Spec.MaxConcurrentAgents == 0
}

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
		if elapsed := now.Sub(cond.LastTransitionTime.Time); elapsed > tatarav1alpha1.HandoffDeadline {
			mrs, mrErr := ownedMergeRequests(ctx, r.Client, task)
			if mrErr != nil {
				return ctrl.Result{}, true, mrErr
			}
			l.Info("review handoff stalled: the outcome committed but the drain never advanced the task",
				"action", "handoff_stalled", "resource_id", task.Name, "stage", task.Status.Stage,
				"outcome_reason", cond.Reason, "deadline", tatarav1alpha1.HandoffDeadline.String(),
				"elapsed", elapsed.String())
			return ctrl.Result{}, true, r.enter(ctx, proj, task, mrs,
				tatarav1alpha1.StageParked, stage.ReasonHandoffStalled, now)
		}
	}

	paused := projectPaused(proj)
	clock, since, budget, edge := stage.ArmedClock(task, paused)
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
		// delivered/rejected/failed/parked aged out. The REAPER deletes them
		// (contract B.6); this reconciler never does.
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

	mrs, mrErr := ownedMergeRequests(ctx, r.Client, task)
	if mrErr != nil {
		return ctrl.Result{}, true, mrErr
	}
	l.Info("stage budget elapsed",
		"action", "stage_deadline", "resource_id", task.Name, "stage", task.Status.Stage,
		"clock", clock, "budget", budget.String(), "elapsed", elapsed.String(),
		"to", edge.To, "stage_reason", edge.Reason)
	if err := r.enter(ctx, proj, task, mrs, edge.To, edge.Reason, now); err != nil {
		return ctrl.Result{}, true, err
	}
	// WS3-I5: on the FIRST entry into parked(deploy-timeout), surface the stuck
	// deploy to the human with ONE rate-limited operator comment per owned issue.
	if edge.To == tatarav1alpha1.StageParked && edge.Reason == stage.ReasonDeployTimeout {
		if err := r.enqueueDeployTimeoutComment(ctx, proj, task, mrs, now); err != nil {
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
func (r *TaskReconciler) enqueueDeployTimeoutComment(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, mrs []tatarav1alpha1.MergeRequest, now time.Time) error {

	issues, err := loadTaskIssues(ctx, r.Client, task)
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
	budget, _ := stage.Budget(tatarav1alpha1.StageDeploying)
	body := fmt.Sprintf("Deployment of `%s` has not completed after `%s`; retry `%d`/`%d`. tatara keeps retrying until it succeeds or the deploy budget is exhausted.",
		repos, budget, task.Status.DeployReentries, tatarav1alpha1.MaxDeployReentries)

	sp := r.spiller(proj)
	stamp := metav1.NewTime(now)
	for i := range issues {
		iss := &issues[i]
		if iss.Status.State != "open" || iss.Status.LastDeployTimeoutCommentAt != nil {
			continue // closed, or already commented on the first timeout (own cooldown).
		}
		key := client.ObjectKeyFromObject(iss)
		if err := objbudget.FitIssue(ctx, r.Client, sp, key, func(cur *tatarav1alpha1.Issue) {
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

// holdTurnOnMemoryGate enqueues ONE operator comment on each owned OPEN issue
// the first time a Task's turn submission is held at the pre-SubmitTurn
// memory-readiness gate (issue #355), so a held Task is surfaced to a human
// instead of looking like a silent stall - the same first-occurrence,
// rate-limited pattern as enqueueDeployTimeoutComment (WS3-I5), keyed on its
// OWN cooldown marker (Issue.status.lastMemoryGateCommentAt) so it cannot
// clobber or be clobbered by the deploy-timeout or incident-refire producers.
// It reuses the existing PendingComments drain; it spawns no agent.
func (r *TaskReconciler) holdTurnOnMemoryGate(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, now time.Time) error {

	issues, err := loadTaskIssues(ctx, r.Client, task)
	if err != nil {
		return err
	}
	phase := "Provisioning"
	if proj.Status.Memory != nil && proj.Status.Memory.Phase != "" {
		phase = proj.Status.Memory.Phase
	}
	body := fmt.Sprintf("Project `%s` memory stack is not ready (phase `%s`); tatara is holding this task's next agent turn until it recovers, instead of spawning a pod that would immediately fail to reach memory.",
		proj.Name, phase)

	sp := r.spiller(proj)
	stamp := metav1.NewTime(now)
	for i := range issues {
		iss := &issues[i]
		if iss.Status.State != "open" || iss.Status.LastMemoryGateCommentAt != nil {
			continue // closed, or already commented on the first hold (own cooldown).
		}
		key := client.ObjectKeyFromObject(iss)
		if err := objbudget.FitIssue(ctx, r.Client, sp, key, func(cur *tatarav1alpha1.Issue) {
			if cur.Status.LastMemoryGateCommentAt != nil || len(cur.Status.PendingComments) >= 20 {
				return
			}
			cur.Status.PendingComments = append(cur.Status.PendingComments, tatarav1alpha1.PendingComment{
				RequestID: fmt.Sprintf("memory-gate-hold-%s", task.Name),
				Action:    "comment",
				Body:      body,
			})
			cur.Status.LastMemoryGateCommentAt = &stamp
		}); err != nil {
			return fmt.Errorf("memory-gate-hold comment: enqueue on %s: %w", key.Name, err)
		}
		log.FromContext(ctx).Info("turn hold: surfaced the first memory-gate hold on an owned issue",
			"action", "memory_gate_hold_comment", "resource_id", task.Name, "issue", key.Name, "project", proj.Name)
	}
	return nil
}

// handoffCondition returns the OutcomeAccepted condition of a Task whose OWN
// stage agent has COMMITTED its outcome and whose stage has NOT moved - i.e. the
// cross-reconciler handoff is outstanding and its clock should be running. nil
// otherwise. Its LastTransitionTime is when the commit stamped it, which is when
// the handoff started - not stageEnteredAt.
//
// It resolves to exactly one case, kind-agnostically: the reviewing stage after a
// review outcome commits. Every other kind's commit calls stage.Enter in the SAME
// write, so its condition Reason can never name the NEW stage's agent kind, and
// no other stage can be committed-but-not-advanced. That is why the scoped
// OutcomeCommittedFor is load-bearing and a bare OutcomeCommitted would be a bug:
// the condition is per-TASK and survives across stages, so an implement Task is
// already committed the instant it arrives at reviewing. A bare claim (Reason
// "Outcome") never matches either: it has no handoff outstanding.
//
// This is the ONE predicate for "this stage's agent is done and the handoff is
// outstanding". All three production sites route through it - reconcileClocks'
// deadline and the two B2 suppression guards - so the scoping holds identically
// at each.
func handoffCondition(task *tatarav1alpha1.Task) *metav1.Condition {
	if !tatarav1alpha1.OutcomeCommittedFor(task, stage.AgentKindFor(task.Status.Stage)) {
		return nil
	}
	cond := tatarav1alpha1.OutcomeCondition(task)
	// stage.Enter never clears the condition, so a commit from a PREVIOUS
	// occupancy of this same stage is not this occupancy's handoff:
	// merging -> reviewing (HeadMoved, cycle 4) and the kind=review
	// awaiting-human unpark both re-enter reviewing carrying Reason=Review
	// from the last round. This occupancy's agent has not run yet.
	//
	// No stage stamp is no occupancy to compare against, so no handoff to bound
	// either - ArmedClock disarms on the same condition (stage.go:572). Every
	// path into a stage runs stage.Enter, which always stamps it, so a nil stamp
	// is unreachable; failing closed just keeps it that way.
	if task.Status.StageEnteredAt == nil ||
		cond.LastTransitionTime.Time.Before(task.Status.StageEnteredAt.Time) {
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

// reconcileCaps enforces the F.4 exits every POD stage carries on top of its
// clocks: the lifetime turn budget, the pod-recreation budget, and the
// pod-stopped-with-no-outcome park. Pod-less stages carry none (stage.BudgetExit
// returns nothing for them).
func (r *TaskReconciler) reconcileCaps(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, now time.Time) (handled bool, err error) {

	// podStoppedNoOutcome means the pod RAN (it became Ready: stageWorkStartedAt is
	// set) and is now GONE without the Task having left the stage - i.e. it
	// submitted no outcome. A Task waiting in the ADMISSION QUEUE has podStartedAt
	// == nil and is never this (fix V6-1): the fix that killed every Task that ever
	// queued in normal steady state got exactly this predicate wrong.
	stopped := false
	if task.Status.PodStartedAt != nil && task.Status.StageWorkStartedAt != nil {
		gone, gerr := r.podGone(ctx, task)
		if gerr != nil {
			return true, gerr
		}
		stopped = gone
	}

	edge, ok := stage.BudgetExit(task, taskMaxTurns(proj, task), taskMaxPodRecreations(proj), stopped)
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
	// The turn budget is NOT suppressed: it is not a pod-liveness cap, and
	// BudgetExit checks it first, so a Task over maxTurnsPerTask still fails here.
	//
	// handled=false lets the flow fall through to reconcilePodStage, whose own B2
	// guard returns the poll requeue. The handoff deadline stays armed and bounds
	// the suppression in the other direction.
	if handoffCondition(task) != nil &&
		(edge.Reason == stage.ReasonNoOutcome || edge.Reason == stage.ReasonPodRecreationExhausted) {
		log.FromContext(ctx).Info("stage budget exit suppressed: the outcome is committed and the handoff is in flight",
			"action", "cap_suppressed_committed_outcome", "resource_id", task.Name,
			"stage", task.Status.Stage, "pod_recreations", task.Status.Stats.PodRecreations,
			"suppressed_reason", edge.Reason)
		return false, nil
	}

	mrs, mrErr := ownedMergeRequests(ctx, r.Client, task)
	if mrErr != nil {
		return true, mrErr
	}
	log.FromContext(ctx).Info("stage budget exit",
		"action", "stage_budget_exit", "resource_id", task.Name, "stage", task.Status.Stage,
		"turns", task.Status.Stats.Turns, "pod_recreations", task.Status.Stats.PodRecreations,
		"pod_stopped_no_outcome", stopped, "to", edge.To, "stage_reason", edge.Reason)
	return true, r.enter(ctx, proj, task, mrs, edge.To, edge.Reason, now)
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
		return ctrl.Result{}, r.enter(ctx, proj, task, nil,
			tatarav1alpha1.StageFailed, stage.ReasonNameTooLong, now)
	}
	if verr := tatarav1alpha1.ValidateTaskSpec(task.Spec); verr != nil {
		log.FromContext(ctx).Info("triage: invalid task spec",
			"action", "triage_invalid_spec", "resource_id", task.Name, "err", verr.Error())
		return ctrl.Result{}, r.enter(ctx, proj, task, nil,
			tatarav1alpha1.StageFailed, stage.ReasonTriageStalled, now)
	}

	if err := r.mintIssueCRs(ctx, proj, task); err != nil {
		return ctrl.Result{}, err
	}

	next, ok := triageTarget(task.Spec.Kind)
	if !ok {
		log.FromContext(ctx).Info("triage: no stage for this task kind",
			"action", "triage_unknown_kind", "resource_id", task.Name, "kind", task.Spec.Kind)
		return ctrl.Result{}, r.enter(ctx, proj, task, nil,
			tatarav1alpha1.StageFailed, stage.ReasonTriageStalled, now)
	}
	return ctrl.Result{}, r.enter(ctx, proj, task, nil, next, "", now)
}

// triageTarget is F.3's triaging row, as data: the stage each agent kind starts
// at. A kind with no row is a spec bug and fails triage rather than falling
// through into some default that spawns the wrong agent.
func triageTarget(kind string) (string, bool) {
	switch kind {
	case stage.AgentBrainstorm:
		return tatarav1alpha1.StageBrainstorming, true
	case stage.AgentClarify:
		return tatarav1alpha1.StageClarifying, true
	case stage.AgentIncident:
		return tatarav1alpha1.StageInvestigating, true
	case stage.AgentRefine:
		return tatarav1alpha1.StageRefining, true
	case stage.AgentReview:
		return tatarav1alpha1.StageReviewing, true
	case stage.AgentDocumentation:
		return tatarav1alpha1.StageDocumenting, true
	default:
		// NOTE: `implement` is deliberately absent. F.3's triaging row has no
		// implement edge, and there is no triaging -> implementing edge in the
		// table: code execution is reached ONLY through clarifying -> approved ->
		// implementing, i.e. only through the C.6 approval gate. A Task minted with
		// kind=implement therefore fails triage rather than skipping the gate.
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
			"stage", task.Status.Stage, "agent_kind", agentKind)
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

	// A pod that RAN and vanished without an outcome. reconcileCaps has already
	// applied parked(no-outcome) if the recreation budget was spent, so reaching
	// here means there is budget left: respawn.
	if task.Status.PodStartedAt != nil {
		gone, gerr := r.podGone(ctx, task)
		if gerr != nil {
			return ctrl.Result{}, gerr
		}
		if gone {
			if r.liveStageDiffers(ctx, task) {
				log.FromContext(ctx).Info("wrapper pod gone but the live stage has moved; not respawning",
					"action", "pod_respawn_skipped_stage_moved", "resource_id", task.Name, "acting_stage", task.Status.Stage)
				return ctrl.Result{RequeueAfter: agentBootRequeue}, nil
			}
			return r.respawnLostPod(ctx, proj, task, now)
		}
		// G.7 TTL STOP (fix I5). A pod past t0 = podStartedAt + agentPodTTLSeconds is
		// GRACEFULLY stopped: the agent gets ONE handoff turn, else the operator
		// writes a synthetic handoff note - so status.notes is NEVER empty after a
		// stop - and the pod is deleted. Before this, ttlstop.go had ZERO callers:
		// pods were never TTL-stopped and the non-empty-notes guarantee never ran.
		if agent.TTLExpired(proj, task, now) {
			return r.ttlStop(ctx, proj, task, agentKind, now)
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
	if task.Status.StageWorkStartedAt == nil {
		return ctrl.Result{RequeueAfter: agentBootRequeue}, nil
	}

	marker := turn0Marker(task)
	if task.Annotations[annStageTurn0] == marker {
		return ctrl.Result{RequeueAfter: stageRequeue}, nil // turn-0 already submitted for THIS pod
	}

	// Issue #355: re-check memory readiness immediately before committing to a
	// turn submission, closing the gap between the earlier admission-time gate
	// (task_controller.go Reconcile, which only runs while PodStartedAt==nil)
	// and the actual SubmitTurn call below - a gap that spans pod scheduling,
	// image pull, and the G.10 ready handshake, during which the backend can
	// flip unhealthy. This one call site covers every turn0 submission
	// uniformly: a Task's first pod, a respawned pod (respawnLostPod), and a
	// TTL-rotated pod (ttlStop) all funnel through here before SubmitTurn.
	if !tatarav1alpha1.InfraIncidentExempt(task.Spec) && !memoryStablyReady(proj, now) {
		if err := r.holdTurnOnMemoryGate(ctx, proj, task, now); err != nil {
			return ctrl.Result{}, err
		}
		l.Info("turn submission held: project memory not stably ready",
			"action", "task_memory_gate_turn_hold", "resource_id", task.Name, "project", proj.Name)
		r.Metrics.MemoryGateHold(proj.Name)
		return ctrl.Result{RequeueAfter: memGateRequeue}, nil
	}

	text, err := r.renderBundle(ctx, proj, task, agentKind)
	if err != nil {
		return ctrl.Result{}, err
	}
	t0 := time.Now()
	turnID, serr := r.Session.SubmitTurn(ctx, agent.BaseURL(task, task.Namespace), text, r.callbackURL())
	elapsed := time.Since(t0).Seconds()
	if serr != nil {
		return r.handleTurnSubmitFailure(ctx, proj, task, serr, elapsed, "turn0")
	}
	r.Metrics.TurnSubmit(task.Spec.Kind, "ok", "ok", elapsed)
	l.Info("turn submitted",
		"action", "agent_turn_submit", "resource_id", task.Name, "turn_id", turnID,
		"stage", task.Status.Stage, "agent_kind", agentKind, "bytes", len(text),
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
		return ctrl.Result{}, fmt.Errorf("stamp turn-0 marker: %w", err)
	}
	// E.3: the pending events were rendered into THIS bundle. They are the delta,
	// and the delta is spent.
	if len(task.Status.PendingEvents) > 0 {
		if err := r.patchTaskStatus(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
			if len(fresh.Status.PendingEvents) == 0 {
				return false
			}
			fresh.Status.PendingEvents = nil
			return true
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("drain pending events: %w", err)
		}
	}
	return ctrl.Result{RequeueAfter: stageRequeue}, nil
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
		Namespace: task.Namespace,
		Record:    obs.AgentPodTTLExpired,
	}
	in := agent.TTLStopInput{
		BaseURL:     agent.BaseURL(task, task.Namespace),
		CallbackURL: r.callbackURL(),
		AgentKind:   agentKind,
		Deadline:    deadline,
		TurnTimeout: time.Duration(proj.Spec.Agent.TurnTimeoutSeconds) * time.Second,
		// LastFinalText/PushedRepos are not persisted on the Task (only recordResult
		// stamps turn-complete), so the synthetic note degrades to "(none)". The
		// non-empty-notes guarantee still holds: agent handoff, else synthetic.
	}
	outcome, err := stopper.StopWithHandoff(ctx, task, in)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("ttl stop %s: %w", task.Name, err)
	}
	if err := r.patchTaskStatus(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
		fresh.Status.PodStartedAt = nil
		fresh.Status.StageWorkStartedAt = nil
		return true
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("ttl stop re-arm %s: %w", task.Name, err)
	}
	log.FromContext(ctx).Info("agent pod TTL-stopped; handed off",
		"action", "agent_pod_ttl_stop", "resource_id", task.Name,
		"agent_kind", agentKind, "outcome", outcome)
	return ctrl.Result{RequeueAfter: agentBootRequeue}, nil
}

// turn0Marker identifies the pod turn-0 was submitted to. A respawn re-stamps
// podStartedAt and the replacement pod holds a FRESH claude session that has
// never seen the bundle, so it needs turn-0 again.
func turn0Marker(task *tatarav1alpha1.Task) string {
	at := ""
	if task.Status.PodStartedAt != nil {
		at = task.Status.PodStartedAt.UTC().Format(time.RFC3339)
	}
	return task.Status.Stage + "|" + at
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
	out, err := prompt.Render(prompt.Input{
		Task:           task,
		Issues:         issues,
		MergeRequests:  mrs,
		Events:         task.Status.PendingEvents,
		Notes:          task.Status.Notes,
		Assignment:     assignmentFor(agentKind, task),
		MaxBundleBytes: proj.Spec.MaxBundleBytes,
		Metrics:        r.BundleMetrics,
	})
	if err != nil {
		return "", fmt.Errorf("render bundle for %s: %w", task.Name, err)
	}
	return out, nil
}

// respawnLostPod handles a pod that RAN and is gone. It burns one podRecreations;
// the terminal, once the budget is spent, is failed(pod-recreation-exhausted).
// pod-not-ready is NOT a stage reason and appears nowhere.
func (r *TaskReconciler) respawnLostPod(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, now time.Time) (ctrl.Result, error) {

	edge, terminal := stage.RecordRespawn(task, taskMaxPodRecreations(proj))
	recreations := task.Status.Stats.PodRecreations
	if terminal {
		log.FromContext(ctx).Info("agent pod lost; recreation budget exhausted",
			"action", "pod_recreation_exhausted", "resource_id", task.Name,
			"pod_recreations", recreations)
		return ctrl.Result{}, r.enter(ctx, proj, task, nil, edge.To, edge.Reason, now)
	}
	if err := r.patchTaskStatus(ctx, task, func(fresh *tatarav1alpha1.Task) bool {
		fresh.Status.Stats.PodRecreations = recreations
		fresh.Status.PodStartedAt = nil
		fresh.Status.StageWorkStartedAt = nil
		return true
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("record pod respawn: %w", err)
	}
	log.FromContext(ctx).Info("agent pod lost; respawning",
		"action", "pod_respawn", "resource_id", task.Name, "pod_recreations", recreations)
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
	return live.Status.Stage != task.Status.Stage
}

// podGone reports whether the Task's wrapper Pod no longer exists.
func (r *TaskReconciler) podGone(ctx context.Context, task *tatarav1alpha1.Task) (bool, error) {
	pod := &corev1.Pod{}
	err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: agent.PodName(task)}, pod)
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("get wrapper pod: %w", err)
	}
	return false, nil
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
		if existing.Annotations[annPodStage] == task.Status.Stage {
			return false, nil
		}
		log.FromContext(ctx).Info("wrapper pod belongs to a stage this task has left; deleting",
			"action", "stale_stage_pod_delete", "resource_id", task.Name,
			"pod_stage", existing.Annotations[annPodStage], "stage", task.Status.Stage)
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
			"action", "pod_create_skipped_stage_moved", "resource_id", task.Name, "acting_stage", task.Status.Stage)
		return true, nil
	}

	if err := agent.ValidatePodSecretRefs(proj, r.PodConfig); err != nil {
		return false, err
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
	pod.Annotations[annPodStage] = task.Status.Stage
	if err := r.Create(ctx, pod); err != nil && !apierrors.IsAlreadyExists(err) {
		return false, fmt.Errorf("create wrapper pod: %w", err)
	}
	svc := agent.BuildService(proj, repo, task, r.PodConfig)
	if err := r.Create(ctx, svc); err != nil && !apierrors.IsAlreadyExists(err) {
		return false, fmt.Errorf("create wrapper service: %w", err)
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

	var qel tatarav1alpha1.QueuedEventList
	if err := r.List(ctx, &qel, client.InNamespace(task.Namespace)); err != nil {
		return false, fmt.Errorf("list queuedevents: %w", err)
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
	_, created, err := queue.EnqueueEvent(ctx, r.Client, r.Seq, proj, class, true,
		fmt.Sprintf("ticket|%s|%s", task.Name, agentKind),
		tatarav1alpha1.QueuedEventPayload{
			Kind:          task.Spec.Kind,
			RepositoryRef: task.Spec.RepositoryRef,
			AgentKind:     agentKind,
			TaskRef:       task.Name,
		})
	if err != nil {
		return false, fmt.Errorf("enqueue admission ticket for %s: %w", task.Name, err)
	}
	if created {
		log.FromContext(ctx).Info("admission ticket enqueued",
			"action", "queue_ticket_enqueue", "resource_id", task.Name,
			"stage", task.Status.Stage, "agent_kind", agentKind, "class", class)
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
