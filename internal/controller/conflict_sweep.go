package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// THE CONFLICT SWEEP (G2).
//
// scm.GetMergeState had exactly TWO call sites before this file: the merge
// corridor (merge.go, reachable ONLY from state `merged`) and the submit gate
// (restapi/readiness.go, which runs while an agent is mid-turn). Everywhere
// else, an owned merge request that went DIRTY was simply not looked at - so
// the conflict self-heal that already existed (stage.MergeConflict, cycle 6)
// was unreachable from the states a conflict actually appears in.
//
// tatara-operator#625 is the measured shape: the PR went CONFLICTING while its
// Task sat parked and stayed that way until a human merged main in by hand.
// Nothing in the operator had read its mergeability since it left the merge
// corridor, because it never entered one.
//
// TWO POPULATIONS, TWO CLOCKS. They are not the same problem:
//
//   - ACTIVE (unparked) Tasks: THE TRIGGER IS THE MIRROR, THE DECISION IS LIVE -
//     the same idiom the red-CI gate's sites 2 and 3 established (ci_gate.go).
//     MergeRequest.status.mergeable is free to read and allowed to be wrong, so
//     it decides only whether to PAY for a GetMergeState, and only that live
//     read may act. Without it this is one forge call per owned merge request
//     per tick, forever, for a population that is almost never conflicted.
//     DETECTION LATENCY IS THE MIRROR'S, NOT THIS FILE'S FLOOR: status.mergeable
//     has exactly one writer, SyncMergeRequest, which runs on
//     MirrorCadenceActive (ONE HOUR - CIRefreshCadenceActive is a different
//     field and does not touch it). Worst case is therefore about an hour plus
//     the sweep floor below, and the floor is a COST bound, not a latency
//     promise.
//   - PARKED Tasks: no trigger, no floor, the PARKED MIRROR CADENCE. A parked
//     Task can never be acted on here (see the refusal below), so its live read
//     buys a metric and a log line and nothing else; on the five-minute floor
//     that is ~2000 forge calls per parked Task before the reaper collects it at
//     ParkRetention. It is read once per MirrorCadenceParked instead. The mirror
//     TRIGGER is dropped there rather than kept, because gating a 24-hourly read
//     on a field that is itself written 24-hourly makes #625's own worst-case
//     detection two full days; and on GitLab the mirror's Mergeable is
//     `can_be_merged && CI green` (scm/checks.go), so on that provider every
//     red-CI merge request behind an UnparkNever park is a permanent trigger for
//     a read that can never act.
//
// WHAT IT DELIBERATELY DOES NOT DO, and neither is an oversight:
//
//   - IT NEVER UN-PARKS. stage.Enter refuses a parked Task by design ("there is
//     exactly one way out of a park and it is Unpark"), and internal/stage's
//     mergeStageParks exists precisely because automatically re-implementing
//     work that is parked at or past the merge destroys the reviewed merge
//     request holding the only copy of it. A parked Task's conflict is
//     CONFIRMED and COUNTED (result="dirty") so it is visible in Prometheus and
//     in the log, and left alone. #625's park is a human's to release.
//   - IT NEVER ACTS AT under-implementation. That state IS the rebase edge:
//     there is no transition to make, and the merge request stays DIRTY on the
//     forge until the agent pushes. stage.MergeConflict INCREMENTS the re-entry
//     counter as a side effect, so a "self-edge" here spends one lap of a
//     three-lap budget every pass while the agent it just handed the branch to
//     is still cloning - and the lap that trips the cap parks merge-blocked,
//     which is UnparkNever and DELETES the running pod mid-resolution. The
//     conflict is confirmed, counted and logged there, exactly as for a park.
//   - IT DOES NOT TOUCH merge.go. mergeConflictNote is read from there; the
//     applier is re-implemented here WITHOUT the merge cursor, which is
//     merge-corridor-only state and meaningless from awaiting-review.
//   - IT FAILS OPEN ON EVERYTHING. Not just the forge read: a rotated
//     scmSecretRef, a deleted Repository CR and a failing List are all logged,
//     counted result="error" and skipped. This driver runs BEFORE
//     enforceLivePodCeiling, resumeNoReentryParks, driveStrandedParks and
//     ReapTerminal in Reconcile(), so an error returned from here takes all four
//     off the air for that Project - and it is a refinement on top of the merge
//     corridor, which still re-reads mergeability every 60s. It therefore
//     returns NO error at all: the fail-open is structural, not a promise.

// defaultConflictSweepInterval floors how often driveConflictSweeps re-reads an
// ACTIVE Task's conflicted merge requests. 5 minutes is a COST bound - the pass
// costs two namespace-wide Lists plus one forge read per triggered merge
// request - and not a latency claim: what an active Task's detection latency
// actually is, is MirrorCadenceActive, because status.mergeable is what decides
// whether the live read happens at all. A conflict is resolved by an agent
// taking a full turn on the branch, so a tighter floor would buy nothing
// anyway. Parked Tasks do not use this floor; see parkedConflictSweepDue.
const defaultConflictSweepInterval = 5 * time.Minute

// driveConflictSweepsPaced runs driveConflictSweeps for proj at most once per
// ConflictSweepInterval, decoupled from whatever cadence Reconcile() happens to
// run at - the same tatara-operator#368 reasoning driveUnparksPaced carries,
// and for a strictly more expensive pass. Returns the requeue interval to fold
// into soonestRequeue so a conflict is never starved past the floor once
// Reconcile()'s other drivers stop forcing frequent passes. Keyed per project:
// two live Projects must not throttle each other's floor.
//
// The stamp lands BEFORE the run, not after it. Stamped after, a pass that ends
// badly never records itself, so a single broken credential turns the floor off
// entirely and both namespace-wide Lists re-run on EVERY reconcile for as long
// as the breakage lasts - the exact opposite of what pacing is for.
func (r *ProjectReconciler) driveConflictSweepsPaced(ctx context.Context,
	proj *tatarav1alpha1.Project, now time.Time) time.Duration {

	interval := r.ConflictSweepInterval
	if interval <= 0 {
		interval = defaultConflictSweepInterval
	}
	if last, ok := r.lastConflictSweeps[proj.Name]; ok {
		if elapsed := now.Sub(last); elapsed < interval {
			return interval - elapsed
		}
	}
	if r.lastConflictSweeps == nil {
		r.lastConflictSweeps = map[string]time.Time{}
	}
	r.lastConflictSweeps[proj.Name] = now
	r.driveConflictSweeps(ctx, proj, now)
	return interval
}

// driveConflictSweeps routes an owned merge request the forge reports DIRTY
// back to the rebase edge that already exists, from every state that edge is
// reachable from - not just the merge corridor.
//
// BOTH Lists are per PASS, never per Task: the Task List is namespace-wide and
// filtered in-loop on spec.projectRef exactly as driveUnparks does it, and the
// MergeRequest List is taken ONCE and indexed by controller-owner. A per-Task
// MergeRequestList is not free just because it is cached - the cache deep-copies
// every item into the result - and 30 live Tasks against 200 merge requests is
// 6000 object copies per pass.
func (r *ProjectReconciler) driveConflictSweeps(ctx context.Context,
	proj *tatarav1alpha1.Project, now time.Time) {

	if r.SCMFor == nil {
		return // no forge wired (unit tests): the sweep is a refinement, not a precondition
	}
	l := log.FromContext(ctx)
	var tl tatarav1alpha1.TaskList
	if err := r.List(ctx, &tl, client.InNamespace(proj.Namespace)); err != nil {
		conflictSweepFailedOpen(ctx, err, "conflict sweep: list tasks failed; skipping the pass",
			"conflict_sweep_list_error", proj.Name)
		return
	}

	// The MergeRequest index and the credentials are BOTH resolved LAZILY, once
	// per pass, and only if some Task actually gets that far. A project whose
	// Tasks are all outside the swept states must cost the Task List and nothing
	// else; a project whose merge requests are all mergeable must cost the two
	// Lists and nothing else.
	var (
		mrIndex   map[string][]tatarav1alpha1.MergeRequest
		indexErr  error
		indexOnce bool
	)
	owned := func(taskName string) ([]tatarav1alpha1.MergeRequest, error) {
		if !indexOnce {
			indexOnce = true
			mrIndex, indexErr = conflictSweepMRIndex(ctx, r.Client, proj.Namespace)
		}
		return mrIndex[taskName], indexErr
	}
	var (
		writer   scm.SCMWriter
		token    string
		provider string
		credErr  error
		credOnce bool
	)
	forge := func() (scm.SCMWriter, string, string, error) {
		if !credOnce {
			credOnce = true
			writer, token, provider, credErr = r.conflictSweepForge(ctx, proj)
		}
		return writer, token, provider, credErr
	}

	// The parked half runs on its own, far slower clock, and only when it is due.
	includeParked := r.parkedConflictSweepDue(proj, now)

	for i := range tl.Items {
		if ctx.Err() != nil {
			return
		}
		task := &tl.Items[i]
		if task.Spec.ProjectRef != proj.Name {
			continue
		}
		// The states a conflict can be observed in: the three that carry a live
		// agent pod plus the two the operator drives itself. new, done and
		// rejected are out - a reaped Task's stale mirror must not keep costing
		// forge reads forever.
		if !stage.Live(task.Status.State) && !stage.OperatorDriven(task.Status.State) {
			continue
		}
		if tatarav1alpha1.Parked(task) && !includeParked {
			continue
		}
		mrs, err := owned(task.Name)
		if err != nil {
			conflictSweepFailedOpen(ctx, err, "conflict sweep: list merge requests failed; skipping the pass",
				"conflict_sweep_list_error", proj.Name)
			return
		}
		if len(mrs) == 0 {
			continue
		}
		if !r.sweepTaskConflict(ctx, proj, task, mrs, forge, now) {
			l.Info("conflict sweep: the pass cannot reach the forge; skipping the rest of it",
				"action", "conflict_sweep_abandoned", "resource_id", proj.Name)
			return
		}
	}
}

// conflictSweepMRIndex is the ONE MergeRequestList a pass takes, grouped by the
// name of the Task that controller-owns each merge request. Sorted by name
// within a Task, matching ownedMergeRequests (merge.go), so a pass's decisions
// do not depend on List order.
func conflictSweepMRIndex(ctx context.Context, c client.Reader,
	namespace string) (map[string][]tatarav1alpha1.MergeRequest, error) {

	var list tatarav1alpha1.MergeRequestList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("conflict sweep: list mergerequests: %w", err)
	}
	out := map[string][]tatarav1alpha1.MergeRequest{}
	for i := range list.Items {
		ctrl, ok := own.ControllerOwner(&list.Items[i])
		if !ok {
			continue
		}
		out[ctrl] = append(out[ctrl], list.Items[i])
	}
	for _, mrs := range out {
		sort.Slice(mrs, func(i, j int) bool { return mrs[i].Name < mrs[j].Name })
	}
	return out, nil
}

// parkedConflictSweepDue reports whether THIS pass also sweeps the project's
// PARKED Tasks, and stamps the clock when it does. MirrorCadenceParked is the
// interval because it is the same question: how often is it worth paying a
// forge round-trip for a Task nothing is going to act on. Keyed per project,
// like lastConflictSweeps beside it. In-memory, so a restart brings the parked
// half forward once - which is the safe direction.
func (r *ProjectReconciler) parkedConflictSweepDue(proj *tatarav1alpha1.Project, now time.Time) bool {
	if last, ok := r.lastParkedConflictSweeps[proj.Name]; ok && now.Sub(last) < MirrorCadenceParked {
		return false
	}
	if r.lastParkedConflictSweeps == nil {
		r.lastParkedConflictSweeps = map[string]time.Time{}
	}
	r.lastParkedConflictSweeps[proj.Name] = now
	return true
}

// conflictSweepFailedOpen is the one shape every non-forge failure takes: count
// it, log it, and let the caller carry on. Nothing here is ever returned to
// Reconcile().
func conflictSweepFailedOpen(ctx context.Context, err error, msg, action, resourceID string, kv ...any) {
	obs.ConflictSweepTotal.WithLabelValues("error").Inc()
	log.FromContext(ctx).Error(err, msg, append([]any{"action", action, "resource_id", resourceID}, kv...)...)
}

// sweepTaskConflict is one Task's pass over the merge requests it owns. It
// applies stage.MergeConflict AT MOST ONCE - the function MUTATES
// status.mergeConflictReentries, so a second call in the same pass silently
// spends two laps of a three-lap budget.
//
// It returns false when the PASS is over: credentials that do not resolve fail
// identically for every other Task, so retrying them 30 more times only inflates
// the error counter.
func (r *ProjectReconciler) sweepTaskConflict(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, mrs []tatarav1alpha1.MergeRequest,
	forge func() (scm.SCMWriter, string, string, error), now time.Time) bool {

	l := log.FromContext(ctx)
	parked := tatarav1alpha1.Parked(task)
	for i := range mrs {
		mr := &mrs[i]
		if !conflictSweepCandidate(mr, parked) {
			continue
		}
		writer, token, provider, credErr := forge()
		if credErr != nil {
			conflictSweepFailedOpen(ctx, credErr, "conflict sweep: forge credentials unavailable; skipping the pass",
				"conflict_sweep_cred_error", proj.Name)
			return false
		}
		var repo tatarav1alpha1.Repository
		if err := r.Get(ctx, types.NamespacedName{Namespace: mr.Namespace, Name: mr.Spec.RepositoryRef}, &repo); err != nil {
			conflictSweepFailedOpen(ctx, err, "conflict sweep: repository lookup failed; skipping this merge request",
				"conflict_sweep_repo_error", task.Name, "repo", mr.Spec.RepositoryRef, "pr", mr.Spec.Number)
			continue
		}
		ms, err := writer.GetMergeState(ctx, repo.Spec.URL, token, mr.Spec.Number)
		RecordSCM(r.Metrics, provider, "get_merge_state", err)
		if err != nil {
			// FAIL OPEN: the merge corridor still re-reads this every 60s once the
			// Task gets there, and the next sweep is five minutes away.
			obs.ConflictSweepTotal.WithLabelValues("error").Inc()
			l.Error(err, "conflict sweep: live mergeability read failed; skipping",
				"action", "conflict_sweep_read_error", "resource_id", task.Name,
				"repo", mr.Spec.RepositoryRef, "pr", mr.Spec.Number, "provider", provider)
			continue
		}
		// DIRTY ONLY, exactly as the merge corridor has it. blocked is POLICY - a
		// missing approval, a protected branch, a required check - that no commit
		// an agent writes can clear, and behind is not a conflict at all: the
		// forge merges a behind branch itself.
		if ms != scm.MergeStateDirty {
			obs.ConflictSweepTotal.WithLabelValues("clean").Inc()
			continue
		}
		if why, ok := conflictSweepRefusal(task, mrs); !ok {
			// CONFIRMED DIRTY AND DELIBERATELY NOT ACTED ON. Counted and logged so
			// the conflict is visible rather than silent - which is the whole of
			// what #625 was missing - but not routed.
			obs.ConflictSweepTotal.WithLabelValues("dirty").Inc()
			l.Info("conflict sweep: the merge request conflicts with its base and the rebase edge is refused here",
				"action", "conflict_sweep_observed", "resource_id", task.Name,
				"state", task.Status.State, "park_reason", task.Status.ParkReason,
				"repo", mr.Spec.RepositoryRef, "pr", mr.Spec.Number, "refusal", why)
			return true
		}
		if err := r.enterMergeConflict(ctx, proj, task, mrs, mr, repo.Spec.DefaultBranch, now); err != nil {
			conflictSweepFailedOpen(ctx, err, "conflict sweep: routing to the rebase edge failed",
				"conflict_sweep_error", task.Name, "state", task.Status.State)
		}
		return true
	}
	return true
}

// conflictSweepCandidate is the CHEAP TRIGGER: which owned merge requests are
// worth a live read at all.
//
// OWNERSHIP IS THE RAW TEST, not mergeAllowedForOwnership. A stood-down merge
// request is merge-ELIGIBLE and still not ours to push to, and putting an agent
// on a human's branch is the one thing this whole cycle must never do.
//
// status.mergeable is the mirror's answer and is allowed to be wrong - it is
// written by SyncMergeRequest on MirrorCadence, so it lags by up to an hour on
// an active Task. It is a TRIGGER, never a verdict: the caller confirms live
// before acting, and a stale "not mergeable" costs one forge read and counts
// result="clean".
//
// PARKED SKIPS THE TRIGGER. The parked half of the sweep already runs on
// MirrorCadenceParked, which is the same 24 hours status.mergeable is refreshed
// at for a parked Task, so consulting it there would only ever answer with
// yesterday's mirror - and on GitLab it answers "not mergeable" for every red-CI
// merge request, a standing population behind UnparkNever parks.
func conflictSweepCandidate(mr *tatarav1alpha1.MergeRequest, parked bool) bool {
	if mr.Status.Ownership != tatarav1alpha1.OwnershipTatara {
		return false
	}
	if mr.Status.State != "" && mr.Status.State != "open" {
		return false
	}
	return parked || !mr.Status.Mergeable
}

// conflictSweepRefusal reports whether the rebase edge may be applied to task,
// and names the refusal when it may not. It is checked BEFORE stage.MergeConflict
// so a refused Task never has its re-entry counter mutated.
//
// PARKED. A park stops a Task making progress while it waits on something, and
// there is exactly one way out of one (stage.Unpark). No un-park rule exists for
// "the base branch moved", and inventing one here would resurrect a Task behind
// the human its park is waiting on - the exact shape internal/stage's
// mergeStageParks was written to refuse. #625's park is a human's to release;
// what this sweep adds there is the metric and the log line saying so.
//
// ALREADY AT THE REBASE EDGE. under-implementation is not a transition this
// sweep can make - it is the destination - and the merge request stays DIRTY
// until the agent working it pushes. Routing there anyway is not a no-op:
// stage.MergeConflict increments status.mergeConflictReentries, nothing in a
// level-triggered sweep suppresses the next pass, and the fourth lap parks
// merge-blocked (UnparkNever) and deletes the pod that was resolving the
// conflict. There is no latch that would make it safe either - the head SHA does
// not move until the work is done, which is exactly when the state leaves
// under-implementation on its own. So: observe, count, do not act.
//
// UNREACHABLE. `deployed` has exactly one exit (done) and kind=review may never
// reach under-implementation at all, so EnterStage would refuse the edge and
// charge operator_illegal_stage_transition_total for a decision that was never
// a bug. stage.LegalFor is asked directly instead - it travels with the edge and
// also carries awaiting-review's pendingReview gate, so a Task mid-review-post
// is left to finish its round.
func conflictSweepRefusal(task *tatarav1alpha1.Task, mrs []tatarav1alpha1.MergeRequest) (string, bool) {
	if tatarav1alpha1.Parked(task) {
		return "parked", false
	}
	from := task.Status.State
	if from == tatarav1alpha1.StateUnderImplementation {
		return "already-at-the-rebase-edge", false
	}
	if !stage.LegalFor(task, mrs, from, tatarav1alpha1.StateUnderImplementation) {
		return "rebase-edge-unreachable", false
	}
	return "", true
}

// enterMergeConflict applies stage.MergeConflict and persists
// status.mergeConflictReentries in the SAME write as the state. The counter is
// the bound on cycle 6, and a bound written by a second call is a bound a crash
// between the two can lose.
//
// It is enterCIRedWith's applier (ci_gate.go), not a fresh invention, minus the
// self-edge branch: parking is not an edge (#521), so EnterStage refuses it by
// design and an unbranched applier would error on the cap outcome, while the
// to == from case cannot arise here at all - conflictSweepRefusal turns
// under-implementation away before any of this runs.
//
// The note goes on BEFORE the write, exactly as the merge corridor's own
// conflict arm does it: the bundle an implement pod renders carries no
// mergeability field at all, so without the note the agent boots into a fresh
// implementation turn with no idea that its only job is a conflict.
func (r *ProjectReconciler) enterMergeConflict(ctx context.Context, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, mrs []tatarav1alpha1.MergeRequest,
	mr *tatarav1alpha1.MergeRequest, baseBranch string, now time.Time) error {

	from := task.Status.State
	sp := r.spillerFor(proj)
	edge, _ := stage.MergeConflict(task, mrs, tatarav1alpha1.MaxMergeConflictReentries)
	reentries := task.Status.MergeConflictReentries
	if err := appendOperatorNoteTo(ctx, r.Client, sp, task,
		mergeConflictNote(mr, baseBranch, reentries), now); err != nil {
		return err
	}
	mutate := func(t *tatarav1alpha1.Task) { t.Status.MergeConflictReentries = reentries }

	result := "routed"
	if edge.To == stage.ParkTarget {
		result = "capped"
		if err := ParkTask(ctx, r.Client, sp, r.Metrics, task, edge.Reason, now, mutate); err != nil {
			return err
		}
	} else if err := EnterStage(ctx, r.Client, sp, r.Metrics, task, mrs, edge.To, edge.Reason, now, mutate); err != nil {
		return err
	}
	// The merge-cursor stall gauge carries a per-task label and must not outlive
	// the Task's stay in the corridor. A Task with no series is a no-op.
	obs.ClearMergeCursorStalled(task.Name)
	obs.ConflictSweepTotal.WithLabelValues(result).Inc()
	log.FromContext(ctx).Info("conflict sweep: the merge request conflicts with its base; handing the branch back to an agent",
		"action", "conflict_sweep_exit", "resource_id", task.Name, "from", from,
		"to", edge.To, "reason", edge.Reason, "repo", mr.Spec.RepositoryRef,
		"pr", mr.Spec.Number, "base_branch", baseBranch, "merge_conflict_reentries", reentries)
	return nil
}

// conflictSweepForge resolves (writer, token, provider) for a Project. The
// ProjectReconciler has no shared accessor for the trio, and the sweep needs
// all three: the writer to read, the token to authenticate, the provider to
// label operator_scm_requests_total.
func (r *ProjectReconciler) conflictSweepForge(ctx context.Context,
	proj *tatarav1alpha1.Project) (scm.SCMWriter, string, string, error) {

	provider := "github"
	if proj.Spec.Scm != nil && proj.Spec.Scm.Provider != "" {
		provider = proj.Spec.Scm.Provider
	}
	writer, err := r.SCMFor(provider)
	if err != nil {
		return nil, "", provider, fmt.Errorf("conflict sweep: scm writer: %w", err)
	}
	token, err := mirrorSCMToken(ctx, r.Client, proj)
	if err != nil {
		return nil, "", provider, err
	}
	return writer, token, provider, nil
}
