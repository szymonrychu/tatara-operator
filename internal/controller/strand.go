// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// strandedParkGrace is how long a no-re-entry park must SIT before tatara picks
// it up by itself, and it is the pacing that keeps C.3 from being a busy loop.
//
// It is not a cooldown against load - defaultStrandedParkInterval already floors
// the driver's cadence, and a re-entry costs one Task, not a poll. It is a
// window for the CHEAPER answer to arrive first: a human replying on the issue
// resumes it through resumeNoReentryParks with the reply's context intact, which
// is strictly better than an automatic re-mint that starts from the issue body
// alone. Half an hour is long enough that a maintainer watching a failure gets
// there first, and short enough that a stranded issue is picked back up within
// the hour rather than the week ParkRetention would take.
const strandedParkGrace = 30 * time.Minute

// defaultStrandedParkInterval floors how often driveStrandedParks re-Lists
// Tasks in full, matching defaultResumeNoReentryInterval - it is the same
// full-namespace List for the same population, and a Project pinned to a fast
// Reconcile cadence by something else must not turn either into a per-pass cost.
const defaultStrandedParkInterval = 60 * time.Second

// driveStrandedParksPaced runs driveStrandedParks for proj at most once per
// defaultStrandedParkInterval. Same shape, and same reason, as
// resumeNoReentryParksPaced.
func (r *ProjectReconciler) driveStrandedParksPaced(ctx context.Context, proj *tatarav1alpha1.Project,
	now time.Time) (time.Duration, error) {

	interval := defaultStrandedParkInterval
	if last, ok := r.lastStrandedParks[proj.Name]; ok {
		if elapsed := now.Sub(last); elapsed < interval {
			return interval - elapsed, nil
		}
	}
	if err := r.driveStrandedParks(ctx, proj, now); err != nil {
		return 0, err
	}
	if r.lastStrandedParks == nil {
		r.lastStrandedParks = map[string]time.Time{}
	}
	r.lastStrandedParks[proj.Name] = now
	return interval, nil
}

// driveStrandedParks is C.3: TATARA PICKS STRANDED ISSUES BACK UP BY ITSELF.
//
// Eighteen park reasons are stage.UnparkNever - operator-error, ci-red,
// ci-blocked, stage-deadline, agent-contract-mismatch and the rest - and until
// this existed each of them was a permanent dead end with exactly one escape:
// resumeNoReentryParks, which requires a HUMAN to comment. An issue nobody is
// watching therefore sat parked for seven days, was reaped, and (before C.4) had
// its mirror cascade-deleted, at which point IsOrphanIssue could never see it
// again. There is deliberately no command, no label and no annotation for a
// human to type here: the platform is supposed to notice by itself.
//
// IT IS THE SAME MECHANISM THE HUMAN PATH USES, on a clock instead of a reply.
// resumeOne is called verbatim - early reap of the parked Task plus a fresh mint
// through the shared MintForItem funnel - so the safety property resume.go
// documents is preserved exactly: this is NOT a re-entry. The old Task is never
// un-parked, no exhaustion cap is escaped one lap at a time, and the fresh Task
// re-runs EVERY intake gate (tatara-parked, allowed-reporter, MintStage) with
// new counters. A gate that refuses is a mint that does not happen.
//
// WHAT BOUNDS IT is MaxAutoReentries, counted PER ISSUE on the Issue mirror
// (AnnAutoReentries) because the Task is deleted on every lap. The budget is
// spent BEFORE the re-entry runs, so a crash between the two costs one lap and
// never a loop. When it is gone the issue lands in a REAL dead end: the
// once-only notice comment plus the tatara-parked label, so it stops spinning
// AND is visible in the backlog rather than disappearing.
//
// LEADER-ONLY, like resumeNoReentryParks, and it defers to it: a Task carrying
// a human reply is that driver's this pass, not this one's.
func (r *ProjectReconciler) driveStrandedParks(ctx context.Context, proj *tatarav1alpha1.Project, now time.Time) error {
	var tl tatarav1alpha1.TaskList
	if err := r.List(ctx, &tl, client.InNamespace(proj.Namespace)); err != nil {
		return fmt.Errorf("strand: list tasks: %w", err)
	}
	live := make(map[string]bool, len(tl.Items))
	for i := range tl.Items {
		live[tl.Items[i].Name] = true
	}

	var firstErr error
	for i := range tl.Items {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		t := &tl.Items[i]
		if !r.strandedCandidate(proj, t, now) {
			continue
		}
		if err := r.driveOneStrandedPark(ctx, proj, t, live, now); err != nil {
			log.FromContext(ctx).Error(err, "strand: automatic re-entry failed",
				"action", "stranded_park_error", "resource_id", t.Name, "reason", t.Status.ParkReason)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// strandedCandidate is the cheap, read-only half of the decision: everything
// that can be answered from the Task alone, before any Issue is fetched.
func (r *ProjectReconciler) strandedCandidate(proj *tatarav1alpha1.Project,
	t *tatarav1alpha1.Task, now time.Time) bool {

	if t.Spec.ProjectRef != proj.Name || !tatarav1alpha1.Parked(t) {
		return false
	}
	class, ok := stage.UnparkClassFor(t.Status.ParkReason)
	if !ok || (class != stage.UnparkNever && class != stage.UnparkRetired) {
		// A human- or timer-un-parked reason already has an owner: driveUnparks.
		return false
	}
	if t.Status.ParkReason == stage.ReasonBacklogSweep {
		// Belt to the class check's brace. A backlog owner never RAN and never
		// gave up - it exists to own its Issue at zero cost - so re-minting it
		// would spend a budget on a Task that has no failure to recover from.
		return false
	}
	if hasNonBotPendingEvent(t, botLoginOf(proj)) {
		// A human replied. resumeNoReentryParks resumes it in this same pass with
		// the reply's context, which beats anything this driver can do; spending
		// an automatic budget on it as well would double-charge the same recovery.
		return false
	}
	return now.After(parkedAt(t).Add(strandedParkGrace))
}

// driveOneStrandedPark spends the budget, or lands the dead end, for ONE parked
// Task.
//
// EVERY open owned Issue must have budget left, not merely one of them. resumeOne
// severs and re-mints all of them together, so a Task holding one issue with
// budget and one without cannot be re-entered without overspending the second -
// and overspending is the only thing that could turn this into the unbounded loop
// it exists to avoid. In that case the whole Task takes the dead-end arm.
func (r *ProjectReconciler) driveOneStrandedPark(ctx context.Context, proj *tatarav1alpha1.Project,
	t *tatarav1alpha1.Task, live map[string]bool, now time.Time) error {

	issues, err := r.ownedIssues(ctx, t)
	if err != nil {
		return err
	}
	var open []*tatarav1alpha1.Issue
	for i := range issues {
		if issues[i].Status.State == "open" {
			open = append(open, &issues[i])
		}
	}
	if len(open) == 0 {
		// Nothing to pick back up. A Task owning only closed issues is C.4's
		// business (resumeNoReentryParks severs and collects it); a Task owning
		// no issue at all is the reaper's.
		return nil
	}

	for _, iss := range open {
		if autoReentries(iss) >= tatarav1alpha1.MaxAutoReentries {
			return r.landStrandedDeadEnd(ctx, proj, t, open, now)
		}
	}

	// SPEND FIRST. A crash between the increment and the re-entry costs this
	// issue one lap of its budget; a crash the other way round costs the bound
	// itself, which is the whole safety argument.
	for _, iss := range open {
		spent := autoReentries(iss) + 1
		if err := r.annotateIssue(ctx, iss, tatarav1alpha1.AnnAutoReentries, strconv.Itoa(spent)); err != nil {
			return err
		}
		obs.StrandedParkReentryTotal.WithLabelValues(proj.Name, t.Status.ParkReason, obs.StrandedParkReentered).Inc()
		log.FromContext(ctx).Info("picking a stranded park back up automatically",
			"action", "stranded_park_reentry", "resource_id", iss.Name, "task", t.Name,
			"park_reason", t.Status.ParkReason, "kind", t.Spec.Kind,
			"reentries", spent, "max_reentries", tatarav1alpha1.MaxAutoReentries)
	}
	return r.resumeOne(ctx, proj, t, live, resumeTriggerAutoReentry)
}

// landStrandedDeadEnd is where a genuinely broken issue STOPS. The budget is
// spent, so the notice comment and the tatara-parked label go on - once, latched
// by AnnAutoReentryExhausted - and nothing collects the Task early any more: it
// ages out at ParkRetention like every other UnparkNever park, and the sweep
// then re-mints its issue as parked(backlog-sweep) at zero pods, in the backlog,
// where a human can find it.
func (r *ProjectReconciler) landStrandedDeadEnd(ctx context.Context, proj *tatarav1alpha1.Project,
	t *tatarav1alpha1.Task, open []*tatarav1alpha1.Issue, now time.Time) error {

	writer, token, err := r.scanWriter(ctx, proj)
	if err != nil {
		return fmt.Errorf("strand: scm writer: %w", err)
	}
	for _, iss := range open {
		if iss.Annotations[tatarav1alpha1.AnnAutoReentryExhausted] != "" {
			continue
		}
		repo, err := r.repositoryFor(ctx, proj.Namespace, iss.Spec.RepositoryRef)
		if err != nil {
			return err
		}
		if err := commentAndParkIssue(ctx, r.Client, r.Metrics, proj, writer, token, iss, repo, parkNotice{
			Marker:      tatarav1alpha1.AnnAutoReentryExhausted,
			MarkerValue: now.UTC().Format(time.RFC3339),
			Body:        strandedDeadEndComment(t),
			LogFields: []any{"resource_id", iss.Name, "task", t.Name,
				"park_reason", t.Status.ParkReason},
		}); err != nil {
			return err
		}
		obs.StrandedParkReentryTotal.WithLabelValues(proj.Name, t.Status.ParkReason, obs.StrandedParkBudgetExhausted).Inc()
		log.FromContext(ctx).Info("a stranded issue has spent its automatic re-entry budget and stops here",
			"action", "stranded_park_exhausted", "resource_id", iss.Name, "task", t.Name,
			"park_reason", t.Status.ParkReason, "kind", t.Spec.Kind,
			"max_reentries", tatarav1alpha1.MaxAutoReentries)
	}
	return nil
}

// strandedDeadEndComment is the ONE signal a human gets that the platform has
// given up on an issue by itself rather than on a single attempt at it. It names
// the bound, because "tatara tried and stopped" and "tatara tried three times
// and stopped" ask for very different things from the reader.
func strandedDeadEndComment(t *tatarav1alpha1.Task) string {
	return fmt.Sprintf(
		"tatara has stopped working this issue by itself: %d automatic attempts have now ended in "+
			"`%s` (most recently task `%s`).\n\n"+
			"The issue stays open and is labelled `%s`, so the platform will not spend another agent "+
			"on it on its own. Comment here to pick it back up.",
		tatarav1alpha1.MaxAutoReentries, t.Status.ParkReason, t.Name, TataraParkedLabel)
}

// autoReentries reads the persisted budget off an Issue mirror. An absent or
// unparseable value is ZERO, which FAILS OPEN on purpose: an issue that predates
// the counter, or one whose annotation a human hand-edited into nonsense, is
// given the full budget rather than being silently declared a dead end. The
// bound is still enforced from there, because the next write is an absolute
// value derived from this read.
func autoReentries(iss *tatarav1alpha1.Issue) int {
	n, err := strconv.Atoi(iss.Annotations[tatarav1alpha1.AnnAutoReentries])
	if err != nil || n < 0 {
		return 0
	}
	return n
}
