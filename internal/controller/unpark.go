package controller

import (
	"context"
	"fmt"
	"strconv"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
)

// THE F.6 RE-ENTRY DRIVER (fix W3).
//
// stage.Unpark carries a full re-entry body for SIX park reasons, but before
// this file its ONLY production caller was a bespoke webhook limb, gated to
// identity-unverified (since deleted - every comment-driven re-entry is
// ApplyUnpark now). unparkFires (reaper.go) merely CHECKS whether a park would
// re-enter, on a DeepCopy, so the reaper does not collect a re-entryable Task - it
// never APPLIED the transition. So awaiting-human, merge-timeout, deploy-timeout,
// no-outcome and backlog-sweep all had a re-entry body and NO driver: a
// reviewed+approved delivery whose CI stayed red past its budget parked at
// merge-timeout and was stranded forever.
//
// This driver applies stage.Unpark for EVERY park reason that has an F.6
// re-entry rule, identity-unverified included. That reason USED TO BE EXCLUDED,
// on the reasoning that it re-enters only against a freshly SYNCED forge thread
// evaluated by the C.6 citation check (the webhook's job). The exclusion was a defect:
// it made the webhook's ONE evaluation the only one that would ever happen, so a
// fast path that lost a cache race stalled the Task permanently with the
// approval label already visible on the forge (2026-07-27, gitlab helmfile#26).
//
// THERE IS NO DURABLE GRAMMAR VERDICT ANY MORE (agent-judged-approval-gate,
// steps A-D). This driver used to read Task.status.approvalVerdict through a
// grammarPassedFor helper and hand the answer to stage.Unpark as
// UnparkInput.GrammarPassed, so a record written by one webhook request could
// carry a LATER pass into implementing with no live re-check of the thing the
// record attested to. Step A deleted every writer of Task.status.approvalVerdict,
// step B the reader, step C the UnparkInput.GrammarPassed slot and step D the
// API type itself: there is no verdict left anywhere to pass, so this driver
// cannot pass one even by accident. internal/stage does no approval reasoning
// whatsoever.
//
// What that leaves for parked(identity-unverified): a non-bot pendingEvent of
// ANY KIND un-parks it to CONVERSING when the project is under its conversing
// ceiling, and otherwise it declines no-conversing-room and stays exactly where
// it is with its pendingEvent retained. ANY KIND is not a slip: stage's
// hasNonBotEvent tests Author != botLogin and does NOT look at Kind, so an
// issue_edited event re-engages exactly like an issue_comment does. That is
// correct, because re-entry here is a RESPONSIVENESS decision and not an
// authorization one. IMPLEMENTING is UNREACHABLE from this driver
// for that reason - stage.go's ReasonIdentityUnverified arm has no implementing
// branch left to reach. The ONE surviving grant path into implementing is
// restapi's verifyApprovalScope (internal/restapi/outcome.go), which runs the
// LIVE citation check over every owned Issue on every
// submit_outcome(decision=implement):
// judged by an agent standing in conversing with a live pod, against live state,
// never against a record.
//
// ONE CARVE-OUT, AND IT NEVER FIRES FROM HERE. Under
// Project.spec.AutoApproveTataraProposals, a bot-authored, anchor-verified
// proposal with ZERO maintainer comments auto-approves (autoApproveApplies /
// autoApprovalEvidence, approval_grammar.go) - a deliberate, narrowly gated
// exception, not a gap this driver introduces. autoApproveApplies sits inside
// verifyOneIssue, and after step F verifyOneIssue has exactly ONE production
// caller left: GrammarVerifier.VerifyApproval, reached from restapi's
// submit_outcome scope check (internal/restapi/outcome.go). That needs a live
// pod, which a parked Task does not have, so the carve-out is granted where such
// a proposal actually lives - clarifying (or conversing) with a pod attached -
// and never as a side effect of un-parking. Nothing strands: a bot proposal with
// no maintainer comment is not the shape that parks at identity-unverified in
// the first place (the scope check GRANTS it), and one that is parked here for
// some other reason re-enters through conversing on a non-bot pendingEvent like
// every other park reason, then submits its outcome against a live pod.
//
// Outside that flag, there is no path from parked(identity-unverified) into
// implementing here at all. The OTHER reasons keep their own rules unchanged -
// awaiting-human still re-enters implementing when every open owned Issue is
// approved, no-outcome still re-drives implementing - neither of which ever
// consulted the verdict.

// UnparkDecline classifies WHY ApplyUnpark refused to re-enter (target==""),
// so callers can tell an anomalous drift bail from a normal steady-state
// refusal apart (finding: guard-decline and rule-decline used to collapse
// into the same target=="",err==nil shape, which is exactly what let the
// cache-lag decline hide as an unremarkable steady-state outcome).
type UnparkDecline string

const (
	// DeclineNone means ApplyUnpark did not decline (target != "").
	DeclineNone UnparkDecline = ""
	// DeclineGuard means the live Task's State/ParkReason no longer matched
	// what the caller believed was parked (raced past by another writer, or
	// re-parked under a different reason). Rare and anomalous: the caller's
	// view of the world had already drifted from the apiserver.
	DeclineGuard UnparkDecline = "guard"
	// DeclineRule is the FALLBACK for a stage-level decline code this package
	// does not recognise. It is a bug-catcher: every code stage.Unpark
	// can return has its own constant below, and a "rule" label appearing in
	// operator_unpark_declined_total means the two vocabularies have drifted.
	DeclineRule UnparkDecline = "rule"

	// The rest MIRROR stage's decline vocabulary 1:1, so the metric's `kind`
	// label carries WHICH condition refused rather than a shrug. They are
	// declared here, not aliased, because internal/controller owns the metric
	// and must not leak a stage-package identifier into a label value by
	// accident.
	DeclineNoHumanEvent     UnparkDecline = stage.DeclineNoHumanEvent
	DeclineOverCap          UnparkDecline = stage.DeclineOverCap
	DeclineNoLiveRoom       UnparkDecline = stage.DeclineNoLiveRoom
	DeclineNoOpenIssues     UnparkDecline = stage.DeclineNoOpenIssues
	DeclineMergedMR         UnparkDecline = stage.DeclineMergedMR
	DeclineRoundsExhausted  UnparkDecline = stage.DeclineRoundsExhausted
	DeclineTurnsExhausted   UnparkDecline = stage.DeclineTurnsExhausted
	DeclineWrongParkedFrom  UnparkDecline = stage.DeclineWrongParkedFrom
	DeclineNotParked        UnparkDecline = stage.DeclineNotParked
	DeclineRetryBudgetSpent UnparkDecline = stage.DeclineRetryBudgetSpent
	DeclineNoReentry        UnparkDecline = stage.DeclineNoReentry
	DeclineRetryNotDue      UnparkDecline = stage.DeclineRetryNotDue
)

// DeclineFor maps a stage-package decline code onto this package's typed
// vocabulary. An unknown code falls back to DeclineRule, which is a bug-catcher
// rather than a normal outcome.
func DeclineFor(code string) UnparkDecline {
	switch code {
	case stage.DeclineNone:
		return DeclineNone
	case stage.DeclineNoHumanEvent, stage.DeclineOverCap, stage.DeclineNoLiveRoom,
		stage.DeclineNoOpenIssues, stage.DeclineMergedMR,
		stage.DeclineRoundsExhausted, stage.DeclineTurnsExhausted, stage.DeclineWrongParkedFrom,
		stage.DeclineNotParked, stage.DeclineRetryBudgetSpent, stage.DeclineNoReentry,
		stage.DeclineRetryNotDue:
		return UnparkDecline(code)
	default:
		return DeclineRule
	}
}

// ApplyUnpark runs stage.Unpark for one parked Task and persists the re-entry
// under optimistic concurrency. It is the SINGLE application of stage.Unpark,
// shared by the project-reconcile driver (driveUnparks, the time-based reasons)
// and the webhook comment-driven paths (awaiting-human, backlog-sweep and, since
// step A folded away its bespoke limb, identity-unverified), so every F.6
// re-entry flows through one place. activeTasks / maxOpen are supplied by the
// caller (computed once per pass) so a bulk promotion cannot exceed maxOpenTasks
// on a stale count. target is "" when the park did not re-enter; decline then
// says why (DeclineGuard vs DeclineRule) so the caller can log/count them
// differently. decline is always DeclineNone when target != "" or err != nil.
//
// reader is the manager's UNCACHED APIReader (same idiom as TaskReconciler's
// mintedAlready/refreshTaskFromAPI, #347/#348). The in-loop Get MUST use it,
// not the cached c: driveCommentUnpark's caller just wrote a pendingEvent via
// AppendTaskEvent's Status().Update microseconds earlier, and the cached
// informer has not observed that write yet. A cached Get here silently threw
// the fresh state away - fresh.Status.PendingEvents came back empty,
// hasNonBotEvent returned false, and the un-park was refused with ok=false, a
// normal (non-error) outcome, for a Task a human had just told to proceed
// (issue: comment-driven unpark lost the cache-lag race 2/2 in prod). Nil
// reader (unit tests that do not wire one) falls back to c.
//
// conversingHasRoom is the CALLER's precomputed answer to Task 9's F.6
// capacity question (NeedsConversingRoom names which stageReasons ever consult
// it). It is a PARAMETER, not computed in here, on purpose: driveUnparks calls
// ApplyUnpark once per parked Task in a loop (tatara-operator#368 is the whole
// reason that loop is paced at all), and LiveHasRoom's countLive is
// an unindexed namespace List - computing it per-Task here would multiply that
// List by the parked backlog on every pass, exactly the hot-path regression
// #368 exists to prevent. Callers hoist it ONCE per pass instead (2026-07-28
// security review IMPORTANT 4).
func ApplyUnpark(ctx context.Context, c client.Client, reader client.Reader, proj *tatarav1alpha1.Project,
	task *tatarav1alpha1.Task, activeTasks, maxOpen int, liveHasRoom bool, now time.Time) (bool, UnparkDecline, error) {

	// c (cached) is safe here, unlike the retry-loop Task Get below: by the time
	// driveCommentUnpark reaches this call the owning Issue CR is guaranteed to
	// already exist (resolveMirrorTarget/deliverPendingEvent already Got it and
	// early-returns on NotFound), the same-request write that preceded this call
	// only touches the Issue's Status.Comments, and stage.Unpark's
	// openIssues/allApproved read Status.State/Status.Status - fields that write
	// never touched - so no cache-lag can make this Get see a stale approval
	// state; a stale (or genuinely unapproved) read only ever routes into
	// clarifying, never into the silent decline this fix is about.
	issues, err := loadTaskIssues(ctx, c, task)
	if err != nil {
		return false, DeclineNone, err
	}
	mrs, err := loadTaskMRs(ctx, c, task)
	if err != nil {
		return false, DeclineNone, err
	}
	botLogin := botLoginOf(proj)

	getter := reader
	if getter == nil {
		getter = c
	}

	var unparked bool
	var decline UnparkDecline
	// persisted is whatever copy the loop actually wrote, handed back to the
	// caller below. fromReason is captured HERE because that write-back clears
	// Status.ParkReason, and the log line still has to name the park it left.
	var persisted *tatarav1alpha1.Task
	fromReason := task.Status.ParkReason
	key := client.ObjectKeyFromObject(task)
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := getter.Get(ctx, key, fresh); err != nil {
			return err
		}
		// Raced past this park by another writer (or already un-parked): nothing to
		// do. The reason must also still match, or a different park is in play.
		if !tatarav1alpha1.Parked(fresh) ||
			fresh.Status.ParkReason != task.Status.ParkReason {
			unparked = false
			decline = DeclineGuard
			return nil
		}
		// Every field UnparkInput HAS is set here. There is no verdict field to
		// omit any more (step C deleted it), so parked(identity-unverified) can
		// only clear the flag or decline, never grant anything.
		code := stage.Unpark(stage.UnparkInput{
			Task:         fresh,
			Issues:       issues,
			MRs:          mrs,
			ActiveTasks:  activeTasks,
			MaxOpenTasks: maxOpen,
			BotLogin:     botLogin,
			LiveHasRoom:  liveHasRoom,
			Now:          now,
		})
		// DeclineRetryBudgetSpent RE-PARKED the Task with a blocked reason, so its
		// status HAS changed and must be persisted even though nothing un-parked.
		if code != stage.DeclineNone && code != stage.DeclineRetryBudgetSpent {
			unparked = false
			decline = DeclineFor(code)
			return nil
		}
		if err := c.Status().Update(ctx, fresh); err != nil {
			return err
		}
		persisted = fresh
		unparked = code == stage.DeclineNone
		decline = DeclineFor(code)
		return nil
	})
	if err != nil {
		return false, DeclineNone, fmt.Errorf("unpark: apply on %s: %w", task.Name, err)
	}
	// Hand the persisted copy back. Without it the caller carries a Task that
	// still reads parked for the whole rest of the pass, against an API server
	// that no longer agrees - the silent-stale-read failure class #521 exists to
	// remove. It happens AFTER the loop, never inside it: the in-loop guard
	// compares the live parkReason against task's, and clearing task's mid-retry
	// would turn a Conflict retry into a spurious DeclineGuard.
	if persisted != nil {
		*task = *persisted
	}
	if unparked {
		log.FromContext(ctx).Info("unparked task",
			"action", "unpark", "resource_id", task.Name, "state", task.Status.State,
			"reason_from", fromReason)
	}
	return unparked, decline, nil
}

// UnparkTakeoverTask persists stage.UnparkTakeover - the ONE un-park that clears
// the park flag AND moves state in the same write - through the objbudget
// writer, and tears down the pod of the state being left, exactly as EnterStage
// does. Its two call sites are the takeover mint and the stand-down merge
// re-drive; there is no third and there must not be one.
func UnparkTakeoverTask(ctx context.Context, c client.Client, sp objbudget.Spiller, m *obs.OperatorMetrics,
	task *tatarav1alpha1.Task, to string, now time.Time) error {

	from := task.Status.State
	leavingPodStage := stage.AgentKindFor(from, task.Spec.Kind) != ""

	var uErr error
	key := client.ObjectKeyFromObject(task)
	if err := objbudget.FitTask(ctx, c, sp, key, func(t *tatarav1alpha1.Task) {
		uErr = stage.UnparkTakeover(t, to, now)
	}); err != nil {
		return fmt.Errorf("unpark: takeover write task %s: %w", key.Name, err)
	}
	if uErr != nil {
		return fmt.Errorf("unpark: takeover %s -> %s on %s: %w", from, to, key.Name, uErr)
	}
	if err := stage.UnparkTakeover(task, to, now); err != nil {
		return fmt.Errorf("unpark: takeover in memory: %w", err)
	}
	if leavingPodStage {
		if err := agent.DeleteWrapper(ctx, c, key.Namespace, task); err != nil {
			return err
		}
	}
	log.FromContext(ctx).Info("takeover un-park",
		"action", "unpark_takeover", "resource_id", task.Name,
		"from", from, "to", to, "kind", task.Spec.Kind)
	m.TaskTerminalEntry(task.Spec.Kind, from, to, "")
	return nil
}

// driveUnparksPaced runs driveUnparks for proj at most once per
// UnparkDriveInterval (default defaultUnparkDriveInterval), decoupled from
// whatever cadence Reconcile() happens to run at. Fix for the 2026-07-18
// operator_unpark_declined_total burst (tatara-operator#368): a Project stuck
// in memory phase=Provisioning forced Reconcile() onto a 10s-or-faster
// cadence (tatara-operator#367), and driveUnparks - with no pacing of its
// own - re-declined the full parked backlog on every single one of those
// passes. Returns the requeue interval to fold into soonestRequeue so a
// genuinely-eligible F.6 re-entry is never starved past the floor even once
// Reconcile()'s OTHER drivers stop forcing frequent passes. Keyed per project
// (like memoryUnhealthyCycles): two live Projects must not throttle each
// other's floor.
func (r *ProjectReconciler) driveUnparksPaced(ctx context.Context, proj *tatarav1alpha1.Project, now time.Time) (time.Duration, error) {
	interval := r.UnparkDriveInterval
	if interval <= 0 {
		interval = defaultUnparkDriveInterval
	}
	if last, ok := r.lastDriveUnparks[proj.Name]; ok {
		if elapsed := now.Sub(last); elapsed < interval {
			return interval - elapsed, nil
		}
	}
	if err := r.driveUnparks(ctx, proj, now); err != nil {
		return 0, err
	}
	// The O3 migration rides the SAME pacing, deliberately: it is a full-namespace
	// sweep over parked Tasks and it must not run at Reconcile()'s cadence for the
	// same tatara-operator#368 reason driveUnparks must not. It is also
	// self-limiting - every Task it touches is stamped and never seen again - so
	// after the first pass or two it is a no-op scan.
	if err := r.driveRetiredUnparks(ctx, proj, now); err != nil {
		return 0, err
	}
	// The retry-lane migration rides it too, and is self-limiting the same way.
	if err := r.driveRetryLaneMigration(ctx, proj, now); err != nil {
		return 0, err
	}
	// The CI recovery rides the SAME pacing for the SAME reason: it is another
	// full-namespace sweep over parked Tasks, and #368 is about exactly that
	// running at Reconcile()'s cadence.
	if err := r.driveCIRecoveryUnparks(ctx, proj, now); err != nil {
		return 0, err
	}
	if r.lastDriveUnparks == nil {
		r.lastDriveUnparks = map[string]time.Time{}
	}
	r.lastDriveUnparks[proj.Name] = now
	return interval, nil
}

// RetiredParkMigrationWindow is the age cutoff on the O3 retired-park migration.
// A Task parked LONGER than this is left alone.
//
// The migration exists to release Tasks a deleted ceiling stalled, not to
// resurrect a backlog. Old wreckage is genuinely dead work: its issue has moved
// on, its branch has rotted, and un-parking it means minting a pod that reads a
// stale bundle and burns tokens rediscovering that. 48h is chosen against
// ParkRetention (7d): anything older is already most of the way to being reaped
// and should finish aging out rather than wake up.
const RetiredParkMigrationWindow = 48 * time.Hour

// maxRetryBlockerReadsPerPass BOUNDS how many Tasks may pay for a LIVE blocker
// read in one driveUnparks pass, and it is livepods.go's
// maxLivePodEvictionsPerPass one driver over, for the same structural reason.
// Each read is a synchronous forge call with a 30s client timeout, and
// ProjectReconciler runs MaxConcurrentReconciles=1 ACROSS EVERY PROJECT - so an
// uncapped pass over a due backlog (20 parks x 3 merge requests x 30s) is half an
// hour in which no Project reconciles at all: no conflict sweep, no live-pod
// ceiling, no counters, for anybody.
//
// TWO, so the worst case is ~3 minutes of blocked reconciliation rather than
// thirty, and the typical case (a forge answering in milliseconds) is unchanged.
// It does not starve: a Task that IS served either releases or re-arms into a
// backoff of at least UnparkRetryBackoffBase, so it stops competing for the
// budget and the queue drains at 2 per pacing interval - far above any rate at
// which parks actually come due.
const maxRetryBlockerReadsPerPass = 2

// maxRetryLaneMigrationsPerPass BOUNDS the retry-lane migration the same way.
// The migration re-parks with parkedAt=now and every migrated Task then arms the
// SAME UnparkRetryBackoffBase, so an uncapped first pass over the backlog puts
// the entire population into one 30s due-window - a thundering herd the
// migration mints itself, landing on the very read budget above. Ten per pass
// (two writes each) drains a hundred-park backlog in five passes, staggers the
// arm times by the pacing interval as it goes, and costs nothing else: the
// migration is latched per Task, so a Task it does not reach this pass is
// reached by the next one unchanged.
const maxRetryLaneMigrationsPerPass = 10

// driveRetiredUnparks is the O3 MIGRATION, and it is a one-shot sweep dressed as
// a reconciler loop.
//
// Three park reasons - turn-budget-exhausted, review-loop-exhausted and
// pod-recreation-exhausted - were written by ceilings this release deleted.
// Every Task carrying one was stalled by a rule that no longer exists, so it is
// released ONCE, in place, with its clocks re-armed (stage.UnparkRetiredPark).
//
// EXACTLY ONCE PER TASK, IDEMPOTENT FOREVER. The latch is the
// tatara.dev/retired-park-migrated ANNOTATION, and the ordering is: stamp the
// annotation FIRST (a metadata patch), then un-park (a status write). Stamping
// first means the failure mode of a crash between the two is a Task that stays
// parked and is never retried - visible, inert, and fixable by hand. Stamping
// second would make it a Task that un-parks on every single pass forever, which
// is a token-burning loop the annotation exists to prevent. Where the two orders
// disagree, prefer the one that fails closed.
//
// It re-parks nothing and re-migrates nothing. A Task released here that later
// parks turn-budget-exhausted again (it cannot from this build, but an older
// replica mid-rollout can write one) keeps its annotation and is skipped.
func (r *ProjectReconciler) driveRetiredUnparks(ctx context.Context, proj *tatarav1alpha1.Project, now time.Time) error {
	var tl tatarav1alpha1.TaskList
	if err := r.List(ctx, &tl, client.InNamespace(proj.Namespace)); err != nil {
		return fmt.Errorf("unpark: list tasks for the retired-park migration: %w", err)
	}
	l := log.FromContext(ctx)
	var firstErr error
	for i := range tl.Items {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		t := &tl.Items[i]
		if t.Spec.ProjectRef != proj.Name || !tatarav1alpha1.Parked(t) {
			continue
		}
		class, ok := stage.UnparkClassFor(t.Status.ParkReason)
		if !ok || class != stage.UnparkRetired {
			continue
		}
		if _, done := t.Annotations[tatarav1alpha1.AnnRetiredParkMigrated]; done {
			continue
		}
		// THE 48h CUTOFF. parkedAt falls back to stateEnteredAt for a Task parked
		// by a build predating the field, and to the zero time for one with
		// neither - which reads as "parked forever ago" and is therefore SKIPPED.
		// That is the safe direction: an unmigrated Task ages out at ParkRetention
		// exactly as it did before this release.
		reason := t.Status.ParkReason
		if age := now.Sub(parkedAt(t)); age > RetiredParkMigrationWindow {
			l.Info("retired park left to age out: parked longer than the migration window",
				"action", "retired_park_skipped", "resource_id", t.Name,
				"reason", reason, "parked_hours", int(age.Hours()))
			continue
		}
		if err := r.stampRetiredParkMigrated(ctx, t, now); err != nil {
			l.Error(err, "unpark: stamp the retired-park migration latch",
				"action", "retired_park_error", "resource_id", t.Name, "reason", reason)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := r.applyRetiredUnpark(ctx, t, now); err != nil {
			l.Error(err, "unpark: release a retired park",
				"action", "retired_park_error", "resource_id", t.Name, "reason", reason)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		l.Info("released a park written by a retired ceiling",
			"action", "retired_park_unparked", "resource_id", t.Name,
			"reason", reason, "state", t.Status.State, "project", proj.Name)
		r.Metrics.TaskUnparked(reason, obs.UnparkClassRetired)
	}
	return firstErr
}

// stampRetiredParkMigrated writes the once-only latch. It is a METADATA patch,
// not a status write: the two subresources are independent, so this cannot be
// lost by the status update that follows it, and it survives every later status
// mutation the Task goes through.
func (r *ProjectReconciler) stampRetiredParkMigrated(ctx context.Context, t *tatarav1alpha1.Task, now time.Time) error {
	patch := client.MergeFrom(t.DeepCopy())
	if t.Annotations == nil {
		t.Annotations = map[string]string{}
	}
	t.Annotations[tatarav1alpha1.AnnRetiredParkMigrated] = now.UTC().Format(time.RFC3339)
	if err := r.Patch(ctx, t, patch); err != nil {
		return fmt.Errorf("unpark: stamp %s on %s: %w", tatarav1alpha1.AnnRetiredParkMigrated, t.Name, err)
	}
	return nil
}

// applyRetiredUnpark persists stage.UnparkRetiredPark under optimistic
// concurrency, re-reading through the UNCACHED APIReader for the same cache-lag
// reason ApplyUnpark does, and re-checking the park under the retry so a Task
// another writer moved in the meantime is left alone rather than force-cleared.
func (r *ProjectReconciler) applyRetiredUnpark(ctx context.Context, t *tatarav1alpha1.Task, now time.Time) error {
	getter := client.Reader(r.APIReader)
	if getter == nil {
		getter = r.Client
	}
	key := client.ObjectKeyFromObject(t)
	want := t.Status.ParkReason
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := getter.Get(ctx, key, fresh); err != nil {
			return err
		}
		if !tatarav1alpha1.Parked(fresh) || fresh.Status.ParkReason != want {
			return nil // raced past this park; the latch is stamped, so never retried
		}
		if err := stage.UnparkRetiredPark(fresh, now); err != nil {
			return err
		}
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		*t = *fresh
		return nil
	})
}

// retryLaneMigrations maps the two park reasons the lane SUPERSEDED onto their
// lane names. Both lost their park writer when stage.CIRed's and
// stage.MergeConflict's anyMerged arms moved into the lane; both stay in the
// vocabulary (removing an enum member breaks validation on the next status write
// of every object carrying it) and both remain the STATE reason of their
// re-implement edge, which this migration never touches - it reads
// status.parkReason only.
var retryLaneMigrations = map[string]string{
	stage.ReasonCIRed:         stage.ReasonCIFailed,
	stage.ReasonMergeConflict: stage.ReasonMergeConflictRetry,
}

// driveRetryLaneMigration moves the PARKS THAT MOTIVATED THE LANE into it.
//
// WITHOUT IT THIS CHANGE FIXES NOTHING THAT IS ALREADY BROKEN. helmfile#27 and
// #32, ansible!17 and !18 and terraform!221 - the measured population, all with
// merged merge requests, parked Tasks and open issues - carry ci-red and
// merge-conflict TODAY, and those two constants stay UnparkNever. Only parks
// written after the rollout would ever reach the lane; everything already wedged
// would stay wedged and age out at ParkRetention, silently, which is the exact
// failure the lane exists to remove. This is driveRetiredUnparks one class over,
// and for the same reason: a park is a verdict from a rule that no longer exists.
//
// IT RE-PARKS; IT NEVER UN-PARKS, and that is what makes it safe enough to run
// unattended. driveRetiredUnparks releases outright and therefore needs a 48h
// window to avoid resurrecting dead work with a live pod. This one only
// RECLASSIFIES: the Task stays parked, and the lane's own machinery then applies
// every guard it has - a backoff before the first look, a LIVE confirmation that
// the blocker is gone before any release, capacity, and a loud escalation after
// MaxUnparkRetries. The worst case for an ancient wedge is therefore a comment on
// its issue and a park a human can answer, never a pod spawned against a blocker
// that is still standing. The bound is ParkRetention: past it the reaper is about
// to collect the Task anyway, and waking it to escalate would be noise.
//
// ONCE PER TASK, LATCHED ON METADATA, STAMPED FIRST. Same ordering argument as
// AnnRetiredParkMigrated: a crash between the two leaves a Task un-migrated and
// inert (visible, fixable), where the other order leaves one migrating on every
// pass forever.
func (r *ProjectReconciler) driveRetryLaneMigration(ctx context.Context, proj *tatarav1alpha1.Project,
	now time.Time) error {

	var tl tatarav1alpha1.TaskList
	if err := r.List(ctx, &tl, client.InNamespace(proj.Namespace)); err != nil {
		return fmt.Errorf("unpark: list tasks for the retry-lane migration: %w", err)
	}
	l := log.FromContext(ctx)
	var firstErr error
	touched := 0
	for i := range tl.Items {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		t := &tl.Items[i]
		if t.Spec.ProjectRef != proj.Name || !tatarav1alpha1.Parked(t) {
			continue
		}
		reason := t.Status.ParkReason
		to, ok := retryLaneMigrations[reason]
		if !ok {
			continue
		}
		if touched >= maxRetryLaneMigrationsPerPass {
			// THE HERD, CAPPED. Every Task this migrates is re-parked with
			// parkedAt=now and arms the same base backoff, so an uncapped first
			// pass puts the whole backlog into one 30s due-window and then into
			// one pass's blocker-read budget. Nothing is lost: the ones left are
			// untouched and unlatched, and the next paced pass takes the next ten.
			l.Info("retry-lane migration capped for this pass; the rest are migrated by the next one",
				"action", "retry_lane_migration_capped", "project", proj.Name,
				"touched", touched, "max_per_pass", maxRetryLaneMigrationsPerPass)
			break
		}
		if _, done := t.Annotations[tatarav1alpha1.AnnRetryLaneMigrated]; done {
			continue
		}
		if age := now.Sub(parkedAt(t)); age > tatarav1alpha1.ParkRetention {
			l.Info("retry-lane migration skipped: the park is older than ParkRetention and is being collected",
				"action", "retry_lane_migration_skipped", "resource_id", t.Name,
				"reason", reason, "parked_hours", int(age.Hours()))
			continue
		}
		// COUNTED HERE, BEFORE THE WRITES: the cap bounds how much this pass DOES,
		// and a Task whose migration fails has cost the same two apiserver calls
		// as one that succeeds.
		touched++
		if err := r.stampRetryLaneMigrated(ctx, t, now); err != nil {
			l.Error(err, "unpark: stamp the retry-lane migration latch",
				"action", "retry_lane_migration_error", "resource_id", t.Name, "reason", reason)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := r.applyRetryLaneMigration(ctx, t, to, now); err != nil {
			// STRANDED, AND SAID SO IN ITS OWN WORDS. The latch is already
			// written, and nothing ever removes it, so this Task will never be
			// migrated again: it keeps a park reason whose writer no longer
			// exists, stays UnparkNever, and ages out silently at ParkRetention -
			// which is exactly the population this migration was written for. It
			// gets its own action rather than sharing retry_lane_migration_error
			// with the stamp failure (which strands nothing, because an unstamped
			// Task is retried next pass) and it must not read like the ordinary
			// retry_lane_migration_skipped age cutoff, which is a decision rather
			// than a failure.
			l.Error(err, "unpark: a retry-lane migration is STRANDED: the latch is stamped and the re-park failed, "+
				"so this park will never be migrated and ages out unretried",
				"action", "retry_lane_migration_stranded", "resource_id", t.Name,
				"reason", reason, "to", to, "project", proj.Name)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		l.Info("migrated a park written before the retry lane existed: its blocker now gets a bounded, loud retry",
			"action", "retry_lane_migrated", "resource_id", t.Name,
			"reason", reason, "to", to, "state", t.Status.State, "project", proj.Name)
	}
	return firstErr
}

// stampRetryLaneMigrated writes the once-only latch, as a METADATA patch for the
// reason stampRetiredParkMigrated gives.
func (r *ProjectReconciler) stampRetryLaneMigrated(ctx context.Context, t *tatarav1alpha1.Task, now time.Time) error {
	patch := client.MergeFrom(t.DeepCopy())
	if t.Annotations == nil {
		t.Annotations = map[string]string{}
	}
	t.Annotations[tatarav1alpha1.AnnRetryLaneMigrated] = now.UTC().Format(time.RFC3339)
	if err := r.Patch(ctx, t, patch); err != nil {
		return fmt.Errorf("unpark: stamp %s on %s: %w", tatarav1alpha1.AnnRetryLaneMigrated, t.Name, err)
	}
	return nil
}

// applyRetryLaneMigration persists the re-park under optimistic concurrency,
// re-reading through the UNCACHED APIReader and re-checking the park under the
// retry, exactly as the other appliers here do.
//
// A RECLASSIFICATION IS NOT NEW RESIDENCY, and restoring the two residency
// fields around the Repark is not defensive tidying - without it this migration
// KILLS the Tasks it touches. stage.Repark is clearPark + Park, and clearPark
// does not touch stateEnteredAt, so Park folds now-stateEnteredAt into
// stageElapsedCarrySeconds - charging the whole time the Task sat PARKED as time
// it spent WORKING in its state. A three-day-old park hands itself ~259200
// seconds of carry against a ResidencyCapAll of 24h. stage.reArm (every un-park)
// preserves the carry deliberately, so the release then lands a Task that is
// already over the dead-man switch: the review pod is admitted, podwatch stamps
// stateWorkStartedAt, ResidencyExceeded turns true on the very next reconcile and
// task_stage parks it at stage-deadline (UnparkNever) with the pod deleted
// mid-turn. ci-red is taken from awaiting-review as well as merged, so that is
// half the target population, and the outcome is one dead park converted into a
// different dead park having spent an admission slot and a pod to do it. The
// Task did not enter anything: only its reason changed.
//
// parkedAt IS re-stamped, and that is the deliberate half. The lane's backoff
// does NOT measure from it (stage.ArmRetry schedules from now), so the only clock
// it moves is the reaper's ParkRetention - a 6d23h-old park gets a fresh seven
// days. That is accepted rather than narrowed to RetiredParkMigrationWindow's
// 48h for two reasons. First, 48h would exclude the entire measured population
// (helmfile#27/#32, ansible!17/!18, terraform!221 are the parks this exists for,
// and they are far older than two days), which would make the migration a no-op
// exactly where it is needed. Second, the extra window is not "seven more days of
// dead work": within MaxUnparkRetries backoffs - 31 minutes - the Task either
// releases (which is the point) or is re-parked retry-exhausted with a comment on
// its issue, and retry-exhausted re-stamps parkedAt again, so the seven days that
// actually run are a HUMAN's window to answer a question they have just been
// asked. Starting that window days before anyone was asked is the version that
// would be wrong.
func (r *ProjectReconciler) applyRetryLaneMigration(ctx context.Context, t *tatarav1alpha1.Task,
	to string, now time.Time) error {

	getter := client.Reader(r.APIReader)
	if getter == nil {
		getter = r.Client
	}
	key := client.ObjectKeyFromObject(t)
	want := t.Status.ParkReason
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := getter.Get(ctx, key, fresh); err != nil {
			return err
		}
		if !tatarav1alpha1.Parked(fresh) || fresh.Status.ParkReason != want {
			return nil // raced past this park; the latch is stamped, so never retried
		}
		carry, enteredAt := fresh.Status.StageElapsedCarrySeconds, fresh.Status.StateEnteredAt
		if err := stage.Repark(fresh, to, now); err != nil {
			return fmt.Errorf("unpark: migrate %s into the retry lane: %w", key.Name, err)
		}
		fresh.Status.StageElapsedCarrySeconds, fresh.Status.StateEnteredAt = carry, enteredAt
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		*t = *fresh
		return nil
	})
}

// driveCIRecoveryUnparks re-drives a Task whose decline the INFRASTRUCTURE
// caused, once the infrastructure has recovered.
//
// THE MEASURED CASE (containers!1281, !1283). Both were Renovate merge requests
// tatara ADOPTED into upgrade Tasks and OWNS end to end. The agent verified the
// bump, could not submit - the operator's own readiness gate answered 409 ci-red
// at head 4c11cad, because the gitlab.com account was unverified and GitLab
// refused to create child pipelines at all - and declined. That writes
// implement-declined, which is UnparkNever, and nothing re-examines it. The
// account block was then lifted, the pipelines went green, and the operator's own
// mirror came to read ciStatus=green headSHA=4c11cad2 ownership=tatara: it held
// the proof its own decline was void, in its own mirror, and nothing read it.
// For a merge request tatara owns end to end that is a dead end BY CONSTRUCTION -
// driveStrandedParks cannot help either, because it re-mints from an ISSUE and
// an adopted upgrade Task owns none.
//
// IT IS A DRIVER OUTSIDE stage.Unpark, following driveRetiredUnparks exactly.
// implement-declined stays UnparkNever, so the reaper's unparkFires probe still
// answers "nothing will re-enter this" and every Task this driver does not fire
// for ages out at ParkRetention precisely as before. See UnparkCIRecovered for
// why an arm inside Unpark, or an UnparkTimer reclassification, are both wrong.
//
// THE DISTINCTION IT MUST PRESERVE. A decline is a verdict on the CHANGE ("this
// bump is wrong, superseded, unwanted") or on the INFRASTRUCTURE ("I could not
// submit"). The first must stay permanent - helmfile!1407 was declined because
// main had already moved past it and the merge request carried zero net change,
// and re-driving that would re-open a settled decision. Nothing about the park
// separates them afterwards and the free-text decline reason must not be parsed,
// so the discriminator is captured at decline time (AnnDeclineCI) and only
// CIEvidenceRed - the one value the submission gate actually refuses on - counts.
//
// SIX CONDITIONS, ALL REQUIRED, and each one closes a way of getting this
// wrong: the park is one of the two a red pipeline produces - implement-declined
// from the decline verdict, awaiting-human from the discuss verdict, see
// ciRecoverablePark - on a non-takeover Task; the decline-time
// evidence says CI was RED; the merge requests tatara may act on are green NOW;
// their heads still fingerprint to what the decline was made against (so the
// green is about THE SAME CODE, and a merge request that has since closed,
// merged or flipped to `external` drops out of the set and can never match); the
// bound has room; and THE PROJECT HAS A LIVE-POD SLOT.
//
// The last one is not decoration. UnparkCIRecovered re-arms the LIVE state the
// Task parked in, so the next reconcile mints an agent pod - and this driver was
// the ONE un-park path that never asked the ceiling's question, which every
// other one does (stage.Unpark's liveRoomDecline, the reaper's unparkFires
// probe, ownership_redeliver). A refusal here spends NOTHING: no lap of the
// absolute bound, no head latch, and the next pass retries - which is the same
// treatment DeclineNoLiveRoom gets, and for the same reason.
func (r *ProjectReconciler) driveCIRecoveryUnparks(ctx context.Context, proj *tatarav1alpha1.Project, now time.Time) error {
	var tl tatarav1alpha1.TaskList
	if err := r.List(ctx, &tl, client.InNamespace(proj.Namespace)); err != nil {
		return fmt.Errorf("unpark: list tasks for the ci recovery: %w", err)
	}
	l := log.FromContext(ctx)

	// HOISTED, once per pass - the same reasoning as driveUnparks' identical
	// hoist (tatara-operator#368: countLive is an unindexed namespace List and
	// this loop runs over every Task in the project). It is a BUDGET, not a
	// boolean: a batch of recoveries that all fired off one "yes" would admit the
	// whole batch past a ceiling of one. It is computed only if this batch holds
	// a candidate that would resume a live state, and a List error degrades to
	// "no room", which is always the safe direction.
	liveRoomBudget := 0
	for i := range tl.Items {
		t := &tl.Items[i]
		if t.Spec.ProjectRef == proj.Name && tatarav1alpha1.Parked(t) &&
			ciRecoverablePark(t.Status.ParkReason) && stage.Live(t.Status.State) {
			live, listErr := countLive(ctx, r.Client, proj)
			if listErr != nil {
				l.Error(listErr, "unpark: live capacity check failed; treating this ci-recovery pass as no room",
					"action", "ci_recovery_live_room_error", "project", proj.Name)
			} else if ceiling := tatarav1alpha1.MaxLivePods(proj); ceiling > len(live) {
				liveRoomBudget = ceiling - len(live)
			}
			break
		}
	}

	var firstErr error
	for i := range tl.Items {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		t := &tl.Items[i]
		if t.Spec.ProjectRef != proj.Name || !tatarav1alpha1.Parked(t) {
			continue
		}
		if !ciRecoverablePark(t.Status.ParkReason) || t.Spec.Kind == stage.KindTakeover {
			continue
		}
		declineCI := t.Annotations[tatarav1alpha1.AnnDeclineCI]
		declineHeads := t.Annotations[tatarav1alpha1.AnnDeclineHeads]
		if declineCI != tatarav1alpha1.CIEvidenceRed || declineHeads == "" {
			continue
		}
		mrs, err := LoadTaskMRsFor(ctx, r.Client, t)
		if err != nil {
			l.Error(err, "unpark: load merge requests for the ci recovery",
				"action", "ci_recovery_error", "resource_id", t.Name)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		// The SAME aggregation the decline stamped, re-derived from the live
		// mirrors. Green-now and same-head-as-then are one comparison each, and
		// every ownership/state filter is inside CIDeclineEvidence, so the two
		// sides cannot drift into disagreeing about which merge requests count.
		ci, heads := tatarav1alpha1.CIDeclineEvidence(mrs)
		if ci != tatarav1alpha1.CIEvidenceGreen || heads != declineHeads {
			continue
		}
		if t.Annotations[tatarav1alpha1.AnnCIRecoveryHeads] == heads {
			continue // this head has had its one lap
		}
		// BEFORE the bound is charged, deliberately: a Task refused for capacity
		// has not had its recovery, so charging it a lap would spend the whole
		// bound on a busy project without ever running a pod.
		wasLive := stage.Live(t.Status.State)
		if wasLive && liveRoomBudget <= 0 {
			l.Info("ci recovery deferred: the project is at its live-pod ceiling; the next pass retries",
				"action", "ci_recovery_no_live_room", "resource_id", t.Name,
				"park_reason", t.Status.ParkReason, "project", proj.Name)
			continue
		}
		spent := ciRecoveryUnparks(t)
		if spent >= tatarav1alpha1.MaxCIRecoveryUnparks {
			// LAST, deliberately: reaching this line means every other condition
			// held, so the log names a Task that WOULD have been re-driven. It
			// cannot become spam - a Task only gets here once per distinct head,
			// and the head latch refuses the repeats.
			l.Info("ci recovery refused: the absolute bound is spent; the task ages out at ParkRetention",
				"action", "ci_recovery_exhausted", "resource_id", t.Name,
				"unparks", spent, "max_unparks", tatarav1alpha1.MaxCIRecoveryUnparks)
			continue
		}
		// SPEND FIRST, then un-park - the driveRetiredUnparks ordering, for the
		// same reason. A crash between the two leaves a Task that stays parked
		// and is never retried: visible, inert, fixable by hand. The other order
		// would re-drive on every pass forever, which is the token-burning loop
		// these annotations exist to prevent.
		if err := r.stampCIRecoveryLatch(ctx, t, heads, spent+1); err != nil {
			l.Error(err, "unpark: stamp the ci-recovery bound",
				"action", "ci_recovery_error", "resource_id", t.Name)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		// READ BEFORE THE RELEASE. applyCIRecoveryUnpark writes the un-parked
		// object back over t, and the un-park clears parkReason - so a label read
		// after it is unconditionally empty, which is exactly the bug
		// ownership_redeliver.go carried for a release.
		fromReason := t.Status.ParkReason
		if err := r.applyCIRecoveryUnpark(ctx, t, now); err != nil {
			l.Error(err, "unpark: release a ci-blocked give-up",
				"action", "ci_recovery_error", "resource_id", t.Name)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if wasLive {
			// SPEND the budget: this Task now occupies one of the slots the room
			// check above answered for, and the next candidate in this same loop
			// must see the remaining room, not the room this pass STARTED with.
			liveRoomBudget--
		}
		l.Info("released a give-up the infrastructure caused: ci is green again at the head it was given up at",
			"action", "ci_recovery_unparked", "resource_id", t.Name, "state", t.Status.State,
			"park_reason", fromReason, "kind", t.Spec.Kind, "heads", heads,
			"unparks", spent+1, "project", proj.Name)
		r.Metrics.TaskUnparked(fromReason, obs.UnparkClassCIRecovered)
	}
	return firstErr
}

// ciRecoverablePark names the two parks a red pipeline can produce, and it is
// deliberately a list rather than a class: both reasons have other, far more
// common causes, so the reason alone never authorises anything here. It is
// conditions 3-5 - the decline-time evidence, the live re-derivation to green
// and the head fingerprint - that decide, and only the decline and discuss
// commits ever write that evidence.
//
// awaiting-human is the discuss half. An agent that cannot submit because the
// readiness gate answered 409 ci-red may decline or may ask a human, and asking
// is the better answer; before this it was also the only one that could never be
// re-driven.
func ciRecoverablePark(reason string) bool {
	return reason == stage.ReasonImplementDeclined || reason == stage.ReasonAwaitingHuman
}

// ciRecoveryUnparks reads the spent-lap count. An unparsable value reads as the
// ceiling, not as zero: a counter nobody can read must fail towards refusing,
// which is the direction that cannot loop.
func ciRecoveryUnparks(t *tatarav1alpha1.Task) int {
	raw, ok := t.Annotations[tatarav1alpha1.AnnCIRecoveryUnparks]
	if !ok {
		return 0
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return tatarav1alpha1.MaxCIRecoveryUnparks
	}
	return n
}

// stampCIRecoveryLatch writes the bound's two halves - the spent count and the
// head this lap is being charged for - in ONE metadata patch, so the ceiling can
// never advance without the head latch moving with it. Metadata, not status, for
// the reason stampRetiredParkMigrated gives: the subresources are independent, so
// the un-park's status write cannot lose it.
func (r *ProjectReconciler) stampCIRecoveryLatch(ctx context.Context, t *tatarav1alpha1.Task, heads string, spent int) error {
	patch := client.MergeFrom(t.DeepCopy())
	if t.Annotations == nil {
		t.Annotations = map[string]string{}
	}
	t.Annotations[tatarav1alpha1.AnnCIRecoveryUnparks] = strconv.Itoa(spent)
	t.Annotations[tatarav1alpha1.AnnCIRecoveryHeads] = heads
	if err := r.Patch(ctx, t, patch); err != nil {
		return fmt.Errorf("unpark: stamp the ci-recovery bound on %s: %w", t.Name, err)
	}
	return nil
}

// applyCIRecoveryUnpark persists stage.UnparkCIRecovered under optimistic
// concurrency, re-reading through the UNCACHED APIReader and re-checking the park
// under the retry, exactly as applyRetiredUnpark does and for the same reasons.
func (r *ProjectReconciler) applyCIRecoveryUnpark(ctx context.Context, t *tatarav1alpha1.Task, now time.Time) error {
	getter := client.Reader(r.APIReader)
	if getter == nil {
		getter = r.Client
	}
	key := client.ObjectKeyFromObject(t)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := getter.Get(ctx, key, fresh); err != nil {
			return err
		}
		if !tatarav1alpha1.Parked(fresh) || !ciRecoverablePark(fresh.Status.ParkReason) {
			return nil // raced past this park; the bound is stamped, so never retried
		}
		if err := stage.UnparkCIRecovered(fresh, now); err != nil {
			return err
		}
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		*t = *fresh
		return nil
	})
}

// NeedsLiveRoom reports whether a park with this reason could resume a LIVE
// state and therefore consult UnparkInput.LiveHasRoom. A caller sweeping a mixed
// batch skips the capacity List entirely when nothing in it would use the answer
// (2026-07-28 security review IMPORTANT 4).
//
// It is keyed on the REASON alone, deliberately over-approximating: the true
// predicate also needs the Task's state, and the whole point of this helper is
// to answer BEFORE paying for a List. The three comment-driven reasons plus
// no-outcome are the only ones that can land on a live state at all.
func NeedsLiveRoom(parkReason string) bool {
	switch parkReason {
	case stage.ReasonAwaitingHuman, stage.ReasonIdentityUnverified,
		stage.ReasonHandoffStalled, stage.ReasonNoOutcome:
		return true
	case stage.ReasonRetryExhausted:
		// It shares the awaiting-human arm, so it consults the same ceiling.
		return true
	case stage.ReasonCIPending, stage.ReasonCIFailed,
		stage.ReasonMergeConflictRetry, stage.ReasonMRSurfaceSpent:
		// THE RETRY LANE IS NOT EXEMPT, and omitting it here would not be a
		// missed optimisation - it would be a permanent wedge. liveRoomDecline
		// answers no-live-room for ANY live state whenever the caller passed
		// false, and driveUnparks passes NeedsLiveRoom(reason) && budget > 0, so
		// a lane left out of this switch declines every release from
		// awaiting-review forever while still spending its laps.
		return true
	default:
		return false
	}
}

// driveRetryLane is driveUnparks' UnparkRetry arm, applied before ApplyUnpark.
// It returns handled=true when the pass is over for this Task - which is the
// common case, because a retry park spends most of its life waiting.
//
// THE ORDER IS THE WHOLE DESIGN.
//
//   - a lap is CHARGED only where the release will not happen, so the final
//     backoff is actually served rather than truncated the instant the counter
//     reaches the cap;
//   - ON THE ARMED, DUE PATH THE BLOCKER IS ASKED FIRST - before exhaustion,
//     before capacity. It used to be asked last, and that escalated a Task to a
//     human without ever finding out whether the thing it was waiting for had
//     cleared: at RetryAttempts == MaxUnparkRetries a project sitting at
//     maxLivePods (or one whose countLive returned a transient error, which
//     zeroes liveRoomBudget for the entire pass) took the capacity arm, and the
//     capacity arm escalated. The result was a re-park to retry-exhausted and a
//     comment ending "It is still standing." about a conflict that was gone,
//     resumable only by a human. The forge read is the ONE thing that pass had
//     to do, and it was the one thing it skipped;
//   - capacity is therefore consulted BEFORE the read only where it decides the
//     pass by itself and nothing is at stake: the unarmed path (which arms a
//     timer and reads nothing), and an armed lap still BELOW the cap, where a
//     refusal means no release, no escalation and no lap. At the cap the read
//     always happens. Once the blocker is CONFIRMED standing, the next lap is the
//     blocker's cost and not the ceiling's, so it is charged regardless of
//     capacity - which also stops a busy project re-reading the same due park
//     every thirty seconds;
//   - and the release itself is GATED ON THE BLOCKER, which is what makes the
//     cost model below true.
//
// THE COST MODEL, AND WHY THE GATE IS NOT OPTIONAL. MaxUnparkRetries is five
// rather than the re-entry trio's three because "a retry spends no pod on the
// laps that find the blocker still standing". That holds by construction only
// where the re-park happens inside reconcileClocks, i.e. the awaiting-review
// red-CI gate. It does NOT hold for merge-conflict-retry: the conflict sweep
// parks at awaiting-review and ParkTask kills the live review pod, an un-park
// leaves the state at awaiting-review, NO gate in reconcileClocks covers
// mergeability, so reconcilePodStage mints a NEW review pod and the sweep -
// paced at defaultConflictSweepInterval - kills it again up to five minutes
// later. Five laps would be five pods spawned and destroyed mid-turn, five
// admission slots, and 31 minutes of nominal backoff stretched into wall-clock
// hours by a sweep the lane has to wait on. ci-failed at under-implementation
// is the same shape with a full implement turn per lap.
//
// So the lane confirms the blocker itself before releasing (retryBlockerStanding
// - a live forge read, at most len(mergeOrder) calls per Task per lap, and only
// when the backoff is actually due). A blocker still standing costs the next lap
// and no pod at all; the release happens on the pass that finds it gone, which is
// the only pass a pod was ever wanted on. THE DRIVER CAN DO THIS AND
// internal/stage CANNOT: the rule lives here precisely because it needs the forge.
//
// budget is the PASS's read allowance, not this Task's: see
// maxRetryBlockerReadsPerPass. A Task that finds it spent is left exactly as it
// was - still due, nothing charged, nothing released - and the next paced pass
// picks it up.
func (r *ProjectReconciler) driveRetryLane(ctx context.Context, proj *tatarav1alpha1.Project,
	t *tatarav1alpha1.Task, liveRoom bool, budget *retryReadBudget, now time.Time) (handled bool, err error) {

	l := log.FromContext(ctx)
	reason := t.Status.ParkReason
	armed := t.Status.RetryNextAt != nil
	if armed {
		if !stage.RetryDue(t, now) {
			// STILL WAITING, and this is the steady state of the whole lane: it
			// is answered for every waiting park on every 30s pass. Handled here
			// rather than by falling through to ApplyUnpark so that a Task
			// serving a 30-minute backoff costs zero API reads for the sixty
			// passes it sleeps through. stage.Unpark answers the same
			// DeclineRetryNotDue for the two ApplyUnpark callers this driver is
			// not (the webhook and the redeliverer), which is where the rule has
			// to live to be binding. The counter is moved HERE rather than left
			// to driveUnparks' shared decline arm, which this early return never
			// reaches: without it operator_unpark_declined_total{decline=
			// "retry-not-due"} would stay flat for every park this driver owns,
			// which is every waiting retry park in the platform.
			if r.Metrics != nil {
				r.Metrics.UnparkDeclined(reason, string(DeclineRetryNotDue))
			}
			return true, nil
		}
		// CAPACITY, BELOW THE CAP ONLY, and asked before the read because below
		// the cap it decides the pass on its own: no release can happen and no
		// escalation is due, so the read would answer a question nothing acts on -
		// every thirty seconds for as long as the project stayed at its ceiling,
		// consuming a read budget the due parks behind this one need. AT the cap
		// this arm is deliberately NOT taken: that is the pass that decides
		// whether a human is dragged in, and deciding it without reading the
		// forge is the bug the doc above describes.
		if NeedsLiveRoom(reason) && !liveRoom && t.Status.RetryAttempts < tatarav1alpha1.MaxUnparkRetries {
			return true, nil
		}
		// THE BUDGET IS ONLY SPENT WHERE A FORGE READ IS ACTUALLY MADE. A reason
		// with no probe (ci-pending, mr-surface-spent) and an unwired SCMFor cost
		// nothing, so charging them would let two conflict probes defer a lane
		// that was never going to touch the forge at all.
		if r.retryBlockerProbed(reason) && !budget.take() {
			// THE PASS's read allowance is spent. Nothing is charged, nothing is
			// released, the park is still due: the next paced pass reads it. Not
			// logged per Task - driveUnparks logs the pass's deferred count once,
			// and the metric carries the rest.
			r.Metrics.TaskRetryBlockerRead(reason, obs.RetryBlockerReadDeferred)
			return true, nil
		}
		standing, berr := r.retryBlockerStanding(ctx, proj, t)
		if berr != nil {
			// NEITHER open NOR closed: nothing is charged, nothing is released,
			// and the park is still due, so the next paced pass re-reads. Failing
			// open here would spend the pod this gate exists to save; failing
			// closed would charge a lap for the operator's own read failure and
			// escalate a blocker nobody has actually observed.
			//
			// IT IS LOGGED, NOT RETURNED. driveUnparksPaced stamps its 30s latch
			// only after driveUnparks returns nil, and Reconcile abandons the rest
			// of the Project on the error - so one unreachable forge turned every
			// due park into a re-read at the workqueue's rate-limited cadence and
			// took the conflict sweep, the live-pod ceiling and the counters off
			// the air with it, for every Project, since MaxConcurrentReconciles is
			// 1 across all of them. The park is still due; the next pass is the
			// retry, and that is the whole of what returning the error bought.
			l.Error(berr, "retry lane: the live blocker read failed; the park stays due and the next pass re-reads",
				"action", "retry_blocker_read_error", "resource_id", t.Name, "reason", reason,
				"state", t.Status.State)
			r.Metrics.TaskRetryBlockerRead(reason, obs.RetryBlockerReadError)
			return true, nil
		}
		if !standing {
			// The blocker is GONE: hand the Task to the caller's ApplyUnpark,
			// which applies every remaining rule - capacity included, declining
			// no-live-room and retaining the lane if the project has no room.
			// Nothing is spent either way.
			r.Metrics.TaskRetryBlockerRead(reason, obs.RetryBlockerReadCleared)
			return false, nil
		}
		r.Metrics.TaskRetryBlockerRead(reason, obs.RetryBlockerReadStanding)
		l.Info("retry lane: the blocker is still standing at the scheduled retry; charging the next lap instead of a pod",
			"action", "retry_blocker_standing", "resource_id", t.Name, "reason", reason,
			"state", t.Status.State, "attempts_spent", t.Status.RetryAttempts,
			"max_attempts", tatarav1alpha1.MaxUnparkRetries)
		// CONFIRMED STANDING. Exhaustion first - the escalation now names a
		// blocker a live read has just seen - and then the next lap, which is
		// charged whatever the ceiling says: capacity did not cause this and a
		// re-arm mints no pod.
		if t.Status.RetryAttempts >= tatarav1alpha1.MaxUnparkRetries {
			return true, r.escalateExhaustedRetry(ctx, proj, t, now)
		}
		return true, r.chargeNextRetryLap(ctx, t, reason, now)
	}
	// UNARMED: no live observation was made this pass, and none is needed - the
	// park itself is the observation, written by the corridor or the sweep that
	// read the forge. The next lap is what happens now, so this is where
	// exhaustion and capacity are asked.
	if t.Status.RetryAttempts >= tatarav1alpha1.MaxUnparkRetries {
		return true, r.escalateExhaustedRetry(ctx, proj, t, now)
	}
	if NeedsLiveRoom(reason) && !liveRoom {
		// No lap is charged and nothing is armed: the next pass with room picks
		// this up unchanged. Not logged - it is answered every 30s while the
		// project is busy, and DeclineNoLiveRoom already names the same
		// condition for the un-park that DID get to ask.
		return true, nil
	}
	return true, r.chargeNextRetryLap(ctx, t, reason, now)
}

// retryReadBudget is one pass's allowance of LIVE blocker reads, plus how many
// Tasks it turned away, so the driver can say so once rather than per Task.
type retryReadBudget struct {
	remaining int
	deferred  int
}

func (b *retryReadBudget) take() bool {
	if b.remaining <= 0 {
		b.deferred++
		return false
	}
	b.remaining--
	return true
}

// chargeNextRetryLap arms the next backoff and spends the lap it schedules. Both
// entries into it - unarmed, and armed-due-with-a-confirmed-standing-blocker -
// want exactly this, and the race path is a handled no-op rather than an error.
func (r *ProjectReconciler) chargeNextRetryLap(ctx context.Context, t *tatarav1alpha1.Task,
	reason string, now time.Time) error {

	l := log.FromContext(ctx)
	armedNow, err := r.armRetryPark(ctx, t, now)
	if err != nil {
		return err
	}
	if !armedNow {
		// RACED, and this is the branch that used to panic. armRetryPark leaves
		// t untouched on the race path, so t.Status.RetryNextAt is still whatever
		// this pass read - nil on the unarmed path - and the log line below
		// dereferenced it. Beyond the crash it also counted a scheduled lap and
		// logged an arming that never happened. Another writer owns this Task
		// now; the next pass re-reads it.
		return nil
	}
	l.Info("retry lane armed: the blocker is a machine's to clear, so the release is a timer",
		"action", "retry_scheduled", "resource_id", t.Name, "reason", reason,
		"state", t.Status.State, "attempt", t.Status.RetryAttempts,
		"max_attempts", tatarav1alpha1.MaxUnparkRetries,
		"next_at", t.Status.RetryNextAt.Time.UTC().Format(time.RFC3339))
	r.Metrics.TaskRetryScheduled(reason)
	return nil
}

// retryBlockerStanding asks the FORGE whether the blocker this park names is
// still there. It is the release gate: true means the lane spends a lap instead
// of a pod.
//
// LIVE, NOT THE MIRROR, and that is not a preference. MergeRequest.status
// .mergeable and .ciStatus are refreshed for a PARKED Task on MirrorCadenceParked
// (24h, see conflictSweepCandidate's own note), so a mirror-based gate would
// answer with yesterday's forge for the entire 31-minute budget and then escalate
// on it.
//
// WHAT ONE LAP ACTUALLY COSTS, since the rate-limit argument rests on the
// number. The conflict half is ONE GetMergeState: it probes only
// spec.mergeOrder[status.mergeCursor], the repo the corridor is at. The CI half
// is up to len(spec.mergeOrder) GetPRState calls - ciRedAtReviewedHead walks
// every non-merged, non-closed owned merge request and deliberately does NOT
// break early, because a later one may be live-merged and that outranks the red
// verdict. So a lap is 1..len(mergeOrder) reads, not one; there are none at all
// on the sixty passes a Task spends waiting, and maxRetryBlockerReadsPerPass
// bounds how many Tasks may pay in the same pass.
//
// THE CONFLICT PROBE IS CURSOR-SCOPED, and that is what keeps it honest against
// the corridor it is gating. The corridor reads mergeability one repo at a time
// in mergeOrder and stops at the first blocker, so a gate folding "is ANY open
// owned merge request dirty" answers about repos the corridor will not reach for
// laps yet: with [operator, cli, helmfile], operator merged and cli's conflict
// cleared two minutes later, a stale helmfile branch made every lap report
// standing, burnt the whole budget and escalated naming helmfile - when the
// corridor would have merged cli and advanced the cursor, which refunds the
// budget outright. With no mergeOrder resolved (a park written before /outcome
// set one) there is no corridor position to scope to and the fold is all there
// is. ciRedAtReviewedHead's own all-merge-request fold is left ALONE: red CI
// anywhere in the order blocks the whole change, which is what the rest of the
// system already assumes.
//
// Which reasons it probes at all is retryBlockerProbed's, not this function's.
func (r *ProjectReconciler) retryBlockerStanding(ctx context.Context, proj *tatarav1alpha1.Project,
	t *tatarav1alpha1.Task) (bool, error) {

	reason := t.Status.ParkReason
	if !r.retryBlockerProbed(reason) {
		return false, nil
	}
	mrs, err := ownedMergeRequests(ctx, r.Client, t)
	if err != nil {
		return false, err
	}
	if len(mrs) == 0 {
		return false, nil
	}
	if reason == stage.ReasonCIFailed {
		red, cerr := ciRedAtReviewedHead(ctx, r.Client, r.SCMFor, r.Metrics, proj, mrs)
		if cerr != nil {
			return false, fmt.Errorf("unpark: confirm the retry lane's ci blocker on %s: %w", t.Name, cerr)
		}
		return red != nil, nil
	}
	writer, token, provider, ferr := r.conflictSweepForge(ctx, proj)
	if ferr != nil {
		return false, fmt.Errorf("unpark: confirm the retry lane's conflict blocker on %s: %w", t.Name, ferr)
	}
	probe := corridorMergeRequests(t, mrs)
	for i := range probe {
		mr := &probe[i]
		if mr.Status.State != "" && mr.Status.State != "open" {
			continue
		}
		var repo tatarav1alpha1.Repository
		key := types.NamespacedName{Namespace: mr.Namespace, Name: mr.Spec.RepositoryRef}
		if err := r.Get(ctx, key, &repo); err != nil {
			return false, fmt.Errorf("unpark: get repository %s: %w", mr.Spec.RepositoryRef, err)
		}
		ms, merr := writer.GetMergeState(ctx, repo.Spec.URL, token, mr.Spec.Number)
		RecordSCM(r.Metrics, provider, "get_merge_state", merr)
		if merr != nil {
			return false, fmt.Errorf("unpark: merge state %s!%d: %w", mr.Spec.RepositoryRef, mr.Spec.Number, merr)
		}
		// DIRTY ONLY, exactly as the conflict sweep and the merge corridor have
		// it: `blocked` is policy no commit clears, and `behind` is not a
		// conflict at all.
		if ms == scm.MergeStateDirty {
			return true, nil
		}
	}
	return false, nil
}

// retryBlockerProbed reports whether this park reason has a live probe at all,
// and it is the ONE definition of that: retryBlockerStanding's own guard and the
// driver's read-budget charge both ask it, so "which reasons pay for a forge
// read" cannot come to mean two different things in the two places.
//
// A REASON WITH NO PROBE IS NOT STANDING. ci-pending and mr-surface-spent have
// no park writer (see their doc in internal/stage), so there is nothing to
// confirm and the lane behaves as it did before the gate: due means released. An
// unwired SCMFor answers the same way, matching ciRedAtReviewedHead's rule that
// the gate is a refinement of the release, never a precondition for it.
func (r *ProjectReconciler) retryBlockerProbed(reason string) bool {
	if r.SCMFor == nil {
		return false
	}
	return reason == stage.ReasonCIFailed || reason == stage.ReasonMergeConflictRetry
}

// corridorMergeRequests narrows a Task's owned merge requests to the ONE the
// merge corridor is actually at - spec.mergeOrder[status.mergeCursor] - so a
// mergeability probe asks the same question the corridor will. A Task with no
// mergeOrder (or a cursor past its end, which means the corridor has nothing
// left to merge) has no corridor position to scope to and gets the whole set
// back: the caller's fold is then the only answer available, which is what it
// did before the cursor existed.
//
// mrForRepo is the corridor's own resolver, so "which merge request is repo X's"
// cannot drift between the two.
func corridorMergeRequests(t *tatarav1alpha1.Task,
	mrs []tatarav1alpha1.MergeRequest) []tatarav1alpha1.MergeRequest {

	cursor := t.Status.MergeCursor
	if cursor < 0 || cursor >= len(t.Spec.MergeOrder) {
		return mrs
	}
	mr := mrForRepo(mrs, t.Spec.MergeOrder[cursor])
	if mr == nil {
		return nil
	}
	return []tatarav1alpha1.MergeRequest{*mr}
}

// armRetryPark persists stage.ArmRetry under optimistic concurrency, re-reading
// through the UNCACHED APIReader and re-checking the park under the retry,
// exactly as applyRetiredUnpark and applyCIRecoveryUnpark do and for the same
// reason: a Task another writer moved in the meantime must be left alone rather
// than charged a lap for a park that is over.
//
// armed=false is that race, and it is a SENTINEL the caller must honour rather
// than an error: nothing was written, so there is nothing to log, nothing to
// count and - the part that used to crash the leader's whole driveUnparks loop -
// no schedule on t to read back.
func (r *ProjectReconciler) armRetryPark(ctx context.Context, t *tatarav1alpha1.Task,
	now time.Time) (armed bool, err error) {

	getter := client.Reader(r.APIReader)
	if getter == nil {
		getter = r.Client
	}
	key := client.ObjectKeyFromObject(t)
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		armed = false
		fresh := &tatarav1alpha1.Task{}
		if err := getter.Get(ctx, key, fresh); err != nil {
			return err
		}
		// The schedule is compared, not merely tested for nil: this arm now also
		// re-arms an ALREADY-armed park whose blocker is still standing, so
		// "unchanged since this pass read it" is the only race test that covers
		// both entries.
		if fresh.Status.ParkReason != t.Status.ParkReason || !sameRetrySchedule(fresh, t) {
			return nil // raced: another writer armed, released or re-parked it
		}
		if err := stage.ArmRetry(fresh, now); err != nil {
			return fmt.Errorf("unpark: arm the retry lane on %s: %w", key.Name, err)
		}
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		*t = *fresh
		armed = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return armed, nil
}

// sameRetrySchedule reports whether two copies of a Task carry the same
// status.retryNextAt, nil included.
func sameRetrySchedule(a, b *tatarav1alpha1.Task) bool {
	x, y := a.Status.RetryNextAt, b.Status.RetryNextAt
	if x == nil || y == nil {
		return x == nil && y == nil
	}
	return x.Time.Equal(y.Time)
}

// driveUnparks applies stage.Unpark to every parked Task in proj whose park
// reason has an F.6 re-entry rule, INCLUDING identity-unverified - for which
// CONVERSING is the only reachable target, because no approval verdict is ever
// fed to stage.Unpark any more (see the file header). activeTasks is computed
// ONCE and then advanced as each Task re-enters an active stage, so a bulk
// re-entry never exceeds maxOpenTasks (H8).
func (r *ProjectReconciler) driveUnparks(ctx context.Context, proj *tatarav1alpha1.Project, now time.Time) error {
	var tl tatarav1alpha1.TaskList
	if err := r.List(ctx, &tl, client.InNamespace(proj.Namespace)); err != nil {
		return fmt.Errorf("unpark: list tasks: %w", err)
	}
	active, err := r.activeTaskCount(ctx, proj)
	if err != nil {
		return err
	}
	maxOpen := proj.Spec.MaxOpenTasks
	if maxOpen <= 0 {
		maxOpen = 6
	}

	// The List is HOISTED, once per pass, not once per Task (tatara-operator#368:
	// this loop is paced BECAUSE an unindexed per-Task List across the full
	// parked backlog hammered the apiserver before) - but unlike a plain
	// boolean, the ANSWER is a BUDGET spent as this pass actually admits Tasks
	// into conversing, not a single yes/no reused unconditionally for every
	// candidate. A boolean computed once and handed to every parked Task in the
	// batch let a bulk maintainer comment pass - the exact scenario
	// UnparkInput.ActiveTasks' own doc names - push admissions well past the
	// ceiling (e.g. 40 Tasks into conversing against a ceiling of 5): nothing
	// decremented it as Tasks entered conversing (2026-07-28 final review
	// CRITICAL 2). conversingRoomBudget is spent by exactly one per Task that
	// actually lands on conversing, below, so this pass can never admit more
	// than the ceiling has room for. Skipped entirely when nothing in this
	// batch would ever consult it. A transient List error degrades to "no room"
	// (always the SAFE fallback - see UnparkInput.LiveHasRoom's own doc)
	// rather than failing the whole pass closed for every OTHER park reason,
	// which never asked the question in the first place.
	liveRoomBudget := 0
	for i := range tl.Items {
		t := &tl.Items[i]
		if t.Spec.ProjectRef == proj.Name && tatarav1alpha1.Parked(t) && NeedsLiveRoom(t.Status.ParkReason) {
			live, listErr := countLive(ctx, r.Client, proj)
			if listErr != nil {
				log.FromContext(ctx).Error(listErr, "unpark: live capacity check failed; treating this pass as no room",
					"action", "unpark_live_room_error", "project", proj.Name)
			} else if ceiling := tatarav1alpha1.MaxLivePods(proj); ceiling > len(live) {
				liveRoomBudget = ceiling - len(live)
			}
			break
		}
	}

	// The LIVE BLOCKER READ allowance for this pass, spent by the retry lane -
	// see maxRetryBlockerReadsPerPass for why an uncapped pass is a
	// cluster-wide reconcile stall and not merely a slow one.
	readBudget := &retryReadBudget{remaining: maxRetryBlockerReadsPerPass}

	var firstErr error
	for i := range tl.Items {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		t := &tl.Items[i]
		if t.Spec.ProjectRef != proj.Name || !tatarav1alpha1.Parked(t) {
			continue
		}
		liveRoom := NeedsLiveRoom(t.Status.ParkReason) && liveRoomBudget > 0
		if class, ok := stage.UnparkClassFor(t.Status.ParkReason); ok && class == stage.UnparkRetry {
			handled, rerr := r.driveRetryLane(ctx, proj, t, liveRoom, readBudget, now)
			if rerr != nil {
				log.FromContext(ctx).Error(rerr, "unpark: retry lane failed",
					"action", "retry_lane_error", "resource_id", t.Name, "reason", t.Status.ParkReason)
				if firstErr == nil {
					firstErr = rerr
				}
				continue
			}
			if handled {
				continue
			}
		}
		wasLive := stage.Live(t.Status.State)
		unparked, decline, err := ApplyUnpark(ctx, r.Client, r.APIReader, proj, t, active, maxOpen, liveRoom, now)
		if err != nil {
			log.FromContext(ctx).Error(err, "unpark: apply failed",
				"action", "unpark_error", "resource_id", t.Name, "reason", t.Status.ParkReason)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if unparked && wasLive && liveRoomBudget > 0 {
			// SPEND the budget: this Task actually resumed a LIVE state this pass
			// and now occupies one of the slots the room check above answered for.
			// The next candidate in this same loop must see the remaining room,
			// not the room this pass STARTED with.
			liveRoomBudget--
		}
		// GUARD declines are always anomalous (the live object had already
		// drifted from what this pass believed was parked) and worth surfacing
		// here too, unlike most RULE declines: driveUnparks sweeps every parked
		// Task every pass, and a RULE decline is the expected steady state for
		// most of them (e.g. merge-timeout still waiting) - logging every one
		// would be pure log spam, not a signal.
		//
		// NO-CONVERSING-ROOM is the ONE rule decline that is also logged. It is
		// not steady state: it means a human commented on a parked
		// identity-unverified Task and the operator refused to put an agent in
		// front of them purely because the project is at its conversing ceiling
		// - an operator-actionable capacity refusal, and the only remaining
		// terminus of that arm. Platform hard rule 13 wants both channels, and
		// the counter alone (operator_unpark_declined_total) cannot say WHICH
		// Task went unanswered. Bounded by driveUnparksPaced's floor
		// (defaultUnparkDriveInterval) and by the ceiling itself being small.
		switch decline {
		case DeclineGuard:
			log.FromContext(ctx).Info("unpark: declined (drift guard)",
				"action", "unpark_declined", "resource_id", t.Name, "reason", t.Status.ParkReason, "decline", string(decline))
		case DeclineNoLiveRoom:
			log.FromContext(ctx).Info("unpark: declined (project is at its live-pod ceiling; the pending event is retained and the next pass retries)",
				"action", "unpark_declined", "resource_id", t.Name, "reason", t.Status.ParkReason, "decline", string(decline))
		}
		if decline != DeclineNone && r.Metrics != nil {
			r.Metrics.UnparkDeclined(t.Status.ParkReason, string(decline))
		}
		if unparked {
			active++ // the re-entered Task is now active; keep the cap honest this pass.
		}
	}
	if readBudget.deferred > 0 {
		// ONCE per pass, with a count. Per-Task it would be a line every thirty
		// seconds for every due park in a migrated backlog; suppressed entirely it
		// would make a too-small cap invisible. A persistently non-zero
		// operator_task_retry_blocker_read_total{result="deferred"} is what says
		// the cap needs raising.
		log.FromContext(ctx).Info("retry lane: this pass's live blocker-read budget is spent; the rest are still due",
			"action", "retry_blocker_read_deferred", "project", proj.Name,
			"deferred", readBudget.deferred, "max_reads", maxRetryBlockerReadsPerPass)
	}
	return firstErr
}

// loadTaskIssues / loadTaskMRs resolve status.issueRefs / status.mrRefs to their
// CRs. A ref whose CR is gone is skipped (the mirror is not authoritative). They
// are the standalone twins of ProjectReconciler.ownedIssues/ownedMRs so the
// webhook package can drive an un-park without a ProjectReconciler.
func loadTaskIssues(ctx context.Context, c client.Client, t *tatarav1alpha1.Task) ([]tatarav1alpha1.Issue, error) {
	out := make([]tatarav1alpha1.Issue, 0, len(t.Status.IssueRefs))
	for _, name := range t.Status.IssueRefs {
		var iss tatarav1alpha1.Issue
		err := c.Get(ctx, types.NamespacedName{Namespace: t.Namespace, Name: name}, &iss)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("unpark: get issue %s: %w", name, err)
		}
		out = append(out, iss)
	}
	return out, nil
}

func loadTaskMRs(ctx context.Context, c client.Client, t *tatarav1alpha1.Task) ([]tatarav1alpha1.MergeRequest, error) {
	return LoadTaskMRsFor(ctx, c, t)
}

// LoadTaskMRsFor resolves status.mrRefs to their CRs. It is the exported,
// client.Reader-taking twin of loadTaskMRs, so a caller with only a Reader (the
// webhook's driveConversingEntry, which prefers the uncached APIReader for the
// same cache-lag reason every other webhook read does) can load a Task's owned
// MergeRequests without a ProjectReconciler. A ref whose CR is gone is skipped -
// the mirror is not authoritative.
func LoadTaskMRsFor(ctx context.Context, r client.Reader, t *tatarav1alpha1.Task) ([]tatarav1alpha1.MergeRequest, error) {
	out := make([]tatarav1alpha1.MergeRequest, 0, len(t.Status.MRRefs))
	for _, name := range t.Status.MRRefs {
		var mr tatarav1alpha1.MergeRequest
		err := r.Get(ctx, types.NamespacedName{Namespace: t.Namespace, Name: name}, &mr)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("unpark: get mergerequest %s: %w", name, err)
		}
		out = append(out, mr)
	}
	return out, nil
}

// CountActiveTasks counts the non-terminal Tasks in proj. It is the standalone
// twin of ProjectReconciler.activeTaskCount, for the webhook's backlog-sweep cap
// check (H8): a promotion is not a mint, so the cap must be re-checked at re-entry.
func CountActiveTasks(ctx context.Context, c client.Client, proj *tatarav1alpha1.Project) (int, error) {
	var tl tatarav1alpha1.TaskList
	if err := c.List(ctx, &tl, client.InNamespace(proj.Namespace)); err != nil {
		return 0, fmt.Errorf("unpark: list tasks for active count: %w", err)
	}
	n := 0
	for i := range tl.Items {
		if tl.Items[i].Spec.ProjectRef == proj.Name && StageActive(&tl.Items[i]) {
			n++
		}
	}
	return n, nil
}
