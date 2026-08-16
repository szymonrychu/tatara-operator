package controller

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// THE SWEEP (contract B.4).
//
// ONE orphan predicate, in ONE function (IsOrphanIssue), called from BOTH the
// hourly and the nightly path. Do not add a fourth clause and do not inline a
// variant anywhere: every previous "small local variant" of this predicate is
// how the platform ended up with three disagreeing definitions of "does this
// issue need a Task".
//
// It is THE issue/PR intake since the Task 20 cutover: issueScan, mrScan and the
// backstop are gone, and this is the only path from a forge issue to a Task.

const (
	// SweepAnnotation is retained ONLY as an opt-OUT escape hatch: setting it to
	// any value other than SweepEnabledValue disables the sweep for one Project
	// (a break-glass for an intake storm on a single project). It is ON by
	// default - the cutover deleted the machine that used to run instead of it,
	// so an absent annotation cannot mean "no intake".
	SweepAnnotation = "tatara.dev/sweep"
	// SweepEnabledValue is the SweepAnnotation value that turns the sweep on. It
	// is also the DEFAULT when the annotation is absent.
	SweepEnabledValue = "enabled"
	// SweepDisabledValue is the SweepAnnotation value that turns the sweep OFF.
	SweepDisabledValue = "disabled"

	// TataraParkedLabel is the durable marker the operator stamps on an SCM issue
	// when it reaps a terminal Task (B.6), and the ONE label the control path
	// READS (B.4, fix M25).
	//
	// THIS READ IS SAFE WHERE FIX 16'S FORBIDDEN ONE IS NOT, and the distinction
	// is the whole point of fix 16: this read decides COST (do we spend a pod on
	// this issue now?), NEVER AUTHORITY (may this issue be implemented?). Forging
	// the label buys an attacker a Task that stays PARKED - it fails SAFE.
	// Forging an approval label would buy them prod. Do not "helpfully" generalise
	// one rule into the other: approval is comment-only (C.6), and there is no
	// label -> status path.
	TataraParkedLabel = "tatara-parked"

	// AnnWebhookOriginated is the DURABLE LIVENESS MARKER a live, HMAC-verified
	// issues.opened/issues.reopened delivery leaves on the mirror Issue CR, and,
	// FOR AN AUTHOR OUTSIDE THE MAINTAINER/REPORTER ALLOWLISTS, it is the ONLY
	// thing that tells a freshly-opened human issue apart from a three-year-old
	// untouched backlog issue: both are open, human-authored and ZERO-COMMENT, so
	// humanHasLastWord is false for BOTH. MintStage's trusted-human-author clause
	// (below, sweep.go) mints ACTIVE for a maintainer/reporter's issue regardless
	// of this marker, so this paragraph applies only to the untrusted-author case.
	//
	// WITHOUT IT, FOR THAT UNTRUSTED-AUTHOR CASE, THE PLATFORM'S FRONT DOOR IS
	// SHUT: a human opens an issue, the sweep mints parked(backlog-sweep), no pod
	// runs, and nothing happens until the human comments a SECOND time on their
	// own issue. A maintainer's or reporter's issue is unaffected either way - see
	// MintStage's trusted-human-author clause.
	//
	// It CANNOT be replaced by reading "zero comments" as "a human has the last
	// word": that mints the ENTIRE cutover backlog ACTIVE, which is the 150-issue,
	// 17-to-100-pod-hour re-triage storm parked(backlog-sweep) exists to prevent
	// (USER DECISION B2). The signal is LIVENESS, and liveness only comes from the
	// forge telling us something JUST HAPPENED.
	//
	// The webhook writes it (MarkWebhookOriginated); the sweep reads it and CLEARS
	// it on the mint that consumes it (exactly once - a marker that survived would
	// re-activate the issue on every reap/re-mint cycle, which is fix M25's loop by
	// another door). The value is the delivery's RFC3339 timestamp, for audit; only
	// its PRESENCE is load-bearing.
	AnnWebhookOriginated = "tatara.dev/webhook-originated"

	// SweepActivity is the {activity} label value on the sweep heartbeat and
	// error counters.
	SweepActivity = "sweep"

	// THE CLOSED SweepSkip VOCABULARY: every reason the sweep DELIBERATELY does
	// not do a piece of work. It is the {reason} label on
	// obs.SweepSkippedTotal and the `reason` field on the sweep_skip_issue /
	// sweep_skip_pr log lines, and it is kept in sync with obs.sweepSkipReasons
	// by TestSweepSkipReasonsMatchSweepConstants, which scans these constants
	// out of this file (the prose version of that instruction had already
	// drifted once for the fail() reasons - see issue #495).
	//
	// Values are snake_case with NO "skip" prefix, matching SweepSkipMRClaimed,
	// which predates the rest: one label carrying two naming conventions is the
	// drift this vocabulary exists to prevent, and the Go identifier already
	// says "skip".
	//
	// There is deliberately NO "owner gone" member. Before issue #521 a dead
	// owner WAS a silent skip - that is the whole bug. It is now a REPAIR
	// (obs.SweepStaleOwnerRepairedTotal) followed by a mint in the same pass,
	// and "I repaired something" must not share a series with "I did nothing".

	// SweepSkipNone is the empty reason: nothing was skipped.
	SweepSkipNone = ""
	// SweepSkipMRClaimed is clause 1b: an adoptable-by-shape PR whose
	// MergeRequest CR another Task already controller-owns.
	SweepSkipMRClaimed = "mr_claimed_by_other_task"
	// SweepSkipIssueNotOpen is IsOrphanIssue clause (a): the forge says the
	// issue is not open.
	SweepSkipIssueNotOpen = "issue_not_open"
	// SweepSkipIssueOwned is IsOrphanIssue clause (b): a LIVE Task
	// controller-owns the Issue CR. Since #521 "live" is load-bearing - the
	// clause used to accept a ref naming a Task that no longer existed.
	SweepSkipIssueOwned = "issue_owned"
	// SweepSkipReporterNotAllowed is IsOrphanIssue clause (c): the reporter
	// intake gate (issue #102) refused the author.
	SweepSkipReporterNotAllowed = "issue_reporter_not_allowed"
	// SweepSkipMintBudget: one of the two creation budgets bound, so this
	// orphan is deferred to the next pass. Paired with
	// obs.SweepMintCapHitTotal, which says WHICH cap bound.
	//
	// COUNTED ONCE PER PASS (sweepBudget.countBudgetSkip), logged per item. A
	// cap that is full defers every remaining orphan, so counting each one made
	// a healthy project at maxOpenTasks look like a permanent incident; the log
	// lines are what say WHICH issues and pull requests paid for it. It is also
	// excluded from TataraSweepSkipPersistent for that reason - the dedup fixes
	// the series, it does not make a per-pass increment stop clearing the alert
	// threshold.
	SweepSkipMintBudget = "mint_budget_bound"
	// SweepSkipAlreadyMinted: MintExistingLive. A webhook (or a concurrent
	// pass) already minted this natural key and its Task is alive.
	SweepSkipAlreadyMinted = "already_minted"
	// SweepSkipTombstoneDeleted: MintTombstoneDeleted. The deterministic name
	// was held by a dead twin, which has just been deleted; the mint is owed
	// and has NOT happened yet. The pass requeues (sweepRemintDelay) rather
	// than waiting a full sweep period.
	SweepSkipTombstoneDeleted = "tombstone_deleted"
	// SweepSkipMintNotOwed: MintNotOwed. Classification says no Task is owed at
	// all. It has its OWN member rather than folding into already_minted,
	// which asserts a Task exists - the two are opposite facts and a shared
	// series cannot be alerted on.
	SweepSkipMintNotOwed = "mint_not_owed"

	// SweepSkipUpgradeHeadroom: an adoptable dependency merge request the
	// project has no upgrade lane free for this pass. NOT an error and not
	// metered as one: it is picked up oldest-first on a later pass as lanes
	// free. maxOpenUpgrades is the whole point - adoption is unbounded by
	// construction and would otherwise mint a Task per open merge request at
	// once on the first pass after it is armed.
	SweepSkipUpgradeHeadroom = "upgrade_headroom_bound"

	// SweepIssueKind is the Task kind a sweep-minted ISSUE Task carries.
	//
	// It was "clarify" until #521 folded that kind away. It is NOT a hole in the
	// approval gate: an issue Task still enters `refined`, which is where the
	// gate RUNS, and the ONLY edge out of refined into under-implementation is
	// the one restapi grants on a verified citation. What changed is that ONE
	// agent now judges the go-ahead and then writes the code, instead of a
	// clarify pod handing off to a fresh implement pod.
	SweepIssueKind = "implement"
	// SweepReviewKind is the Task kind minted for a HUMAN-authored PR in reaction
	// scope. Every review-kind Task is non-bot-authored BY CONSTRUCTION (clause
	// 2), which is what lets F.3 DELETE the reviewing -> merging edge for
	// kind=review rather than condition it.
	SweepReviewKind = "review"

	// sweepGoalLimit is TaskSpec.Goal's MaxLength (A.4, fix L31). The goal is
	// NON-EVICTABLE: the A.7 byte guard can spill comments and notes but it can
	// never shrink the goal, so an unbounded goal eats the budget the guard is
	// defending.
	sweepGoalLimit = 16384
)

// sweepRemintDelay is how long the pass asks to be requeued after it DELETED a
// stale terminal Task holding a natural key (MintTombstoneDeleted). The mint is
// still OWED, and waiting a full sweep period for it is the silent-next-pass
// shape issue #521 exists to kill.
//
// It is 30s and not 0 because the deleted tombstone's owned Issue/MergeRequest
// mirrors cascade in the BACKGROUND (DeletePropagationBackground): re-minting
// instantly could bind a fresh Task to a mirror the GC is still collecting.
// 30s is comfortably past that, and 120x better than the hourly pass. It is
// also why the owed mint is NOT simply re-driven inline in this same pass.
//
// THE DELAY IS ONLY HALF THE MECHANISM. Waking in 30s does nothing on its own:
// reposDueForScan anchors every repo's slot on Status.LastIssueScan, so a pass
// that stamped would leave the owed repo not-due for a full cron period and the
// wake would find zero due repos. runScans skips the stamp while this requeue
// is pending - see the comment there, which is where the two halves meet.
const sweepRemintDelay = 30 * time.Second

// SweepEnabled reports whether the B.4 sweep runs for proj. ON by default: it is
// the ONLY intake path since the cutover, so it is disabled only by an explicit
// SweepDisabledValue break-glass annotation.
func SweepEnabled(proj *tatarav1alpha1.Project) bool {
	return proj != nil && proj.Annotations[SweepAnnotation] != SweepDisabledValue
}

// IsOrphanIssue is THE orphan predicate. THREE clauses, all required:
//
//	a. issue.state == "open"                           (SCM truth)
//	b. no LIVE Task controller-owns the Issue CR
//	c. IsAllowedReporter(proj, repo, issue.author)     (fix C6)
//
// It returns the orphan verdict AND, when the verdict is false, the SweepSkip
// reason that produced it - the same refactor shape as stage.Unpark ->
// stage.UnparkDetailed, and for the same reason its doc gives: a guard that
// declines without recording a reason is how a high-stakes rule stalls work for
// a day with zero errors and zero logs. A caller cannot structurally skip an
// issue without naming the clause that refused it.
//
// liveOwner is the name of the Task that controller-owns the Issue CR AND STILL
// EXISTS, or "" when there is none. It is NOT read off the CR here, and issue
// #521 is why. The old form called own.ControllerOwner(cr), which returns
// owned=true for any ref carrying controller=true and NEVER checks the named
// Task exists - so an Issue CR whose owning Task was reaped kept a dangling
// controller ownerRef forever, the sweep read it as owned, and five open issues
// went 19 hours with no Task and no log line. Taking the resolved owner as a
// PARAMETER makes "resolve liveness first" structural rather than a convention
// the next caller can forget. Minter.resolveLiveOwner is the resolver, and it
// DROPS a tombstone ref rather than ignoring it.
//
// A zero-owner CR IS an orphan: fix H13 has a failed Task RELEASE its
// controller ownership and drop the ownerRef, and per B.1 an object with zero
// owners is never garbage collected - it is still there, and the mint ADOPTS
// it.
//
// Clause (c) is the reporter intake gate (issue #102, api/v1alpha1/logins.go).
// It is closed-by-default when configured (an empty allowlist preserves the open
// default; an empty LOGIN fails closed under an active gate) and its entire
// purpose is that an INJECTED issue never becomes a Task.
func IsOrphanIssue(proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, iss scm.Issue, liveOwner string) (bool, string) {
	if iss.State != "open" {
		return false, SweepSkipIssueNotOpen
	}
	if liveOwner != "" {
		return false, SweepSkipIssueOwned
	}
	if !tatarav1alpha1.IsAllowedReporter(proj, repo, iss.Author) {
		return false, SweepSkipReporterNotAllowed
	}
	return true, SweepSkipNone
}

// resolveLiveIssueOwner and resolveLiveMROwner are resolveLiveOwner's TYPED
// front doors, and they exist for one reason: a typed nil pointer boxed in a
// client.Object interface is NOT == nil, so "there is no mirror yet" has to be
// answered before the value is boxed or the first method call panics. Both
// arms hand a possibly-nil CR straight out of issueCR/mergeRequestCR.
func (m *Minter) resolveLiveIssueOwner(ctx context.Context, proj *tatarav1alpha1.Project,
	cr *tatarav1alpha1.Issue, activity string) (string, error) {

	if cr == nil {
		return "", nil
	}
	return m.resolveLiveOwner(ctx, proj, cr, activity)
}

func (m *Minter) resolveLiveMROwner(ctx context.Context, proj *tatarav1alpha1.Project,
	cr *tatarav1alpha1.MergeRequest, activity string) (string, error) {

	if cr == nil {
		return "", nil
	}
	return m.resolveLiveOwner(ctx, proj, cr, activity)
}

// terminalOwnerReleasable reports whether a controller owner of cr that EXISTS
// but has FINISHED (api/v1alpha1.TaskDone) may have its ownerRef released by
// resolveLiveOwner.
//
// IT IS THE REAPER'S OWN RULE, restated at the seam that reads it.
// releaseOwnership (B.5) already drops a terminal Task's ref from an Issue
// mirror on exactly this condition - "an OPEN issue must be re-mintable RIGHT
// NOW", fix H13 - and its log line already says "the next sweep re-mints and
// adopts the artifact". So this is not a new policy; it is the same policy
// applied where the wedge is actually observed, instead of only when the owner
// is finally deleted.
//
// A CLOSED mirror keeps its owner. IsOrphanIssue clause (a) skips it anyway, so
// releasing would mint nothing, and the ref is load-bearing there: a retained
// declined brainstorm proposal is stored as a CLOSED mirror deliberately kept
// owned by its rejected Task so it CASCADES with it (DeclinedProposalRetention).
// Dropping that ref would leak a zero-owner CR nothing ever collects.
//
// THE MERGEREQUEST ARM IS DELIBERATELY EXCLUDED, and not by oversight. The
// reaper's MR rule is a DIFFERENT predicate - drop only when step 4 will not
// close the PR (!ourMR) AND the PR is still open - and the orphaning branch
// carries humanReviewRounds forward onto the successor BEFORE it drops. A copy
// of the issue rule here would re-adopt a bot MR the platform is about to
// close, and would silently reset a contributor's review-round count. Giving
// the MR arm its release means porting ourMR and carryHumanReviewRounds to this
// seam, which is a different change with a different blast radius.
func terminalOwnerReleasable(cr client.Object) bool {
	iss, ok := cr.(*tatarav1alpha1.Issue)
	return ok && iss.Status.State != "closed"
}

// resolveLiveOwner answers IsOrphanIssue clause (b): who controller-owns cr AND
// still exists. It returns "" when nobody does.
//
// It takes a client.Object, not an *Issue, because the defect is IDENTICAL on
// the MergeRequest arm - the reaper releases ownership of Issue and
// MergeRequest through one symmetric releaseOwnership, so an MR mirror whose
// controller-owning Task was reaped keeps a dangling ref exactly the same way,
// and ClassifyPR read it exactly the same way (an open human PR classified
// PRIgnore forever: no mint, no counter, no log). Two arms of one defect get
// one resolver; a parallel copy is how they drift.
//
// THE TOMBSTONE BRANCH IS THE #521 FIX. A ref naming a Task the API server does
// not have is not ownership, it is a dangling string, and it is DROPPED - a
// write, not an in-memory ignore. Merely ignoring it would fix the sweep and
// leave three other consumers reading the same lie: ownerTaskRequests
// (stage_controller.go) would enqueue reconciles for a Task that does not
// exist, the reaper cascade would reason about it, and ourMR would key on it.
// Dropping it also lets THIS SAME PASS mint, because the very next call is
// IsOrphanIssue with liveOwner="".
//
// THE TERMINAL BRANCH IS THE SAME DEFECT ONE STATE OVER. #521 taught this
// function that a ref must resolve to a Task that EXISTS; it did not teach it
// that the Task must still be able to WORK. A Task that exists and is `done`
// passed every check and was reported as the owner - and that is not a race, it
// is a durable steady state, because reapDelivered releases NOTHING (unlike the
// rejected arm, which calls releaseTerminal on every tick) and only ever drops
// the ref by deleting the Task, which the doc-reference gate can defer forever.
// Worse, releaseOwnership hands the flag to the OLDEST SURVIVING owner and
// "surviving" meant "exists", so each reap passed the deed to the next dead
// groomer while the daily refine cron appended a fresh one. mtg-decks#14 and
// #16 sat OPEN for twelve days behind three `done` refine Tasks, the maintainer
// asked twice, and the only symptom was one INFO line per pass:
// sweep_skip_issue reason=issue_owned. Comment-driven un-park cannot help - it
// wakes an EXISTING parked Task, and there was none.
//
// It DROPS rather than ignores, for the #521 reason above and for one more that
// is specific to it: the mint that follows calls ownIssueForTask, which REFUSES
// to adopt an Issue carrying a different controller owner ("already has
// controller owner"). An in-memory ignore would turn the silent skip into a
// hard per-item sweep error on every pass.
//
// Both branches are bounded by the SAME two rules that make this safe: the Get
// is uncached, and a transient error is never read as absence.
//
// It lives HERE and not in internal/own because that package is memory-only by
// its own package doc: every function except RepairZeroController mutates an
// object in memory and returns, and the caller owns the Update. The API Get
// that decides liveness does not belong there. own.DropOwner is the pure
// mutation half.
//
// THE GET IS UNCACHED (m.reader()). A stale informer cache that has not yet
// observed a freshly created Task would report NotFound and this function would
// drop a LIVE owner's ref, stealing an issue out from under a running Task -
// strictly worse than the bug it fixes. createTaskRaceSafe already made exactly
// this call for exactly this reason (fix F3-1).
func (m *Minter) resolveLiveOwner(ctx context.Context, proj *tatarav1alpha1.Project,
	cr client.Object, activity string) (string, error) {

	if cr == nil {
		return "", nil
	}
	// TWO iterations, never more. The second exists ONLY for the race
	// dropStaleOwner's fresh read exposes: the caller's copy came from the
	// CACHED client and can name a Task that was reaped, while a concurrent
	// webhook binds a DIFFERENT, LIVE Task as controller in the same window.
	// The fresh Get sees that Task, DropOwner finds nothing to drop, and
	// reporting "no owner" from there mints a DUPLICATE - createTaskRaceSafe
	// only absorbs that when the live Task holds the SAME deterministic
	// natural-key name, which a takeover or incident Task does not. A third
	// hand-over inside the same window is the next pass's business: a loop
	// here could spin against the API server, and the bound is what makes that
	// impossible rather than unlikely.
	for i := 0; i < 2; i++ {
		name, owned := own.ControllerOwner(cr)
		if !owned {
			return "", nil
		}
		var task tatarav1alpha1.Task
		err := m.reader().Get(ctx, types.NamespacedName{Namespace: cr.GetNamespace(), Name: name}, &task)
		if err != nil && !apierrors.IsNotFound(err) {
			return "", fmt.Errorf("sweep: resolve owner task %q of %s: %w", name, cr.GetName(), err)
		}
		// exists is the DISCRIMINATOR between the two branches, and both are read
		// BEFORE the write: dropStaleOwner mirrors fresh refs onto cr and settles
		// nothing about the Task.
		exists := err == nil
		releasing := exists && tatarav1alpha1.TaskDone(&task) && terminalOwnerReleasable(cr)
		if exists && !releasing {
			return name, nil
		}
		dropped, derr := m.dropStaleOwner(ctx, cr, name)
		if derr != nil {
			return "", derr
		}
		switch {
		case dropped && releasing:
			obs.SweepTerminalOwnerReleasedTotal.WithLabelValues(proj.Name, activity).Inc()
			log.FromContext(ctx).Info("sweep: released a controller ownerRef held by a Task that has finished; the issue is mintable again",
				"action", "sweep_terminal_owner_released", "resource_id", cr.GetName(),
				"activity", activity, "project", proj.Name, "terminal_owner", name,
				"owner_kind", task.Spec.Kind, "owner_state", task.Status.State)
		case dropped:
			obs.SweepStaleOwnerRepairedTotal.WithLabelValues(proj.Name, activity).Inc()
			log.FromContext(ctx).Info("sweep: dropped a controller ownerRef naming a Task that no longer exists; the artifact is mintable again",
				"action", "sweep_stale_owner_repaired", "resource_id", cr.GetName(),
				"activity", activity, "project", proj.Name, "stale_owner", name)
		}
		// dropStaleOwner mirrored the FRESH refs onto cr. A controller that is
		// still the one we just resolved as gone means the write did not take;
		// stop rather than re-drop it forever.
		if cur, still := own.ControllerOwner(cr); !still || cur == name {
			return "", nil
		}
	}
	return "", nil
}

// dropStaleOwner removes owner's ownerRef from the Issue/MergeRequest mirror in
// etcd, under RetryOnConflict against a FRESH copy, and mirrors the result onto
// cr so the caller's copy is not left carrying a ref that no longer exists. It
// reports whether it actually wrote, so the repair counter counts repairs and
// not passes. A concurrently reaped CR (NotFound) is not an error: there is
// nothing left to repair.
//
// THE RE-READ IS UNCACHED (m.reader()), matching the liveness Get in
// resolveLiveOwner. Through the cached Client a Conflict retry re-Gets the SAME
// stale resourceVersion and loses again, so all five attempts burn and the
// caller's fail("resolve_live_owner") skips the issue AND fails the whole pass.
// Status is a subresource, so the full Update below cannot clobber it.
func (m *Minter) dropStaleOwner(ctx context.Context, cr client.Object, owner string) (bool, error) {
	key := client.ObjectKeyFromObject(cr)
	wrote := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		wrote = false
		fresh, ok := cr.DeepCopyObject().(client.Object)
		if !ok {
			return fmt.Errorf("sweep: %T is not a client.Object", cr)
		}
		if err := m.reader().Get(ctx, key, fresh); err != nil {
			return err
		}
		if !own.DropOwner(fresh, owner) {
			cr.SetOwnerReferences(fresh.GetOwnerReferences())
			return nil
		}
		if err := m.Client.Update(ctx, fresh); err != nil {
			return err
		}
		cr.SetOwnerReferences(fresh.GetOwnerReferences())
		wrote = true
		return nil
	})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("sweep: drop stale owner %q from %s: %w", owner, key.Name, err)
	}
	return wrote, nil
}

// MintStage returns the stage (and stage reason) a newly minted Task for iss
// enters. TWO stages, and no third:
//
//	triaging                       ACTIVE: it spawns a pod
//	parked / backlog-sweep         it spawns ZERO pods and enqueues NOTHING
//
// The parked branch is what makes the ownership invariant affordable across a
// 150-issue backlog: without it the post-cutover sweep mints 150 ACTIVE Tasks
// that queue against 3 agent slots and spend 17-100 pod-hours re-triaging an
// already-triaged backlog. The trusted-human-author clause below VOIDS that
// affordability guarantee for the slice of the backlog authored by a listed
// maintainer or reporter - see that clause's own paragraph for the measured
// cost, which is small and bounded, not the same order of magnitude as an
// unqualified 150-issue re-triage.
//
// THE TATARA-PARKED CLAUSE READS TASK HISTORY, NOT A COMMENT (fix M25). Keying
// "active vs parked" on "does the bot have the last word" rested on a BEST-EFFORT
// forge write: a 403 on a secondary limit, the reaper deletes anyway, the last
// comment is a human's, the sweep mints ACTIVE, the pod re-triages, it parks -
// the exact loop this exists to kill. The label is durable and it survives the
// reap, so an issue carrying it mints PARKED regardless of who commented last,
// until a human REMOVES it or the operator does on promotion (F.6).
//
// A webhook-originated Task is minted ACTIVE: a webhook is a live,
// HMAC-verified, attributable event, not a heuristic read of a thread the reaper
// may have failed to write to. It is the LIVENESS signal, and it is the only
// thing that separates a just-opened human issue from a cold backlog issue -
// both are open, human-authored and zero-comment.
//
// THE ORDER OF THE FIRST TWO CLAUSES IS THE CONTRACT. The label is checked
// BEFORE the marker, which is the reverse of what a webhook-originated bool used
// to imply, and the reason is that the signal changed shape: it used to be an
// in-process bool on the delivery the webhook was minting from RIGHT NOW, so it
// could not disagree with a label. It is now a DURABLE annotation on the Issue
// CR (AnnWebhookOriginated) that can outlive its delivery and meet a label
// stamped afterwards - by the reaper on a park (B.6), or by a human parking the
// issue by hand. If the marker won:
//
//   - a human who parks a just-opened issue would be overruled by the operator;
//   - and worse, ANY marker left uncleared re-opens the M25 loop from the other
//     side - mint ACTIVE, the pod re-triages, it parks, the reaper stamps
//     tatara-parked, the sweep reads the marker again, ACTIVE, forever.
//
// So tatara-parked is the OUTERMOST gate (fix M25 holds unconditionally: an
// issue carrying it mints PARKED regardless of who spoke last or of what the
// webhook saw, until a human REMOVES it or the operator does on promotion), and
// the marker only decides ACTIVE-vs-backlog for an issue NOBODY has parked. The
// belt to that brace is that the marker is CONSUMED by the mint that reads it.
//
// THE TRUSTED-HUMAN-AUTHOR CLAUSE. The only ordering fact that is pinned and
// load-bearing is AFTER tatara-parked: an explicit human park still wins over
// the author's standing (TestMintStage/PRECEDENCE:_tatara-parked_BEATS_a_trusted_author).
// It is NOT pinned "before the marker" or before humanHasLastWord, and do not
// claim it is: this clause, webhookOriginated and humanHasLastWord all return
// the identical (StateNew, ""), so their relative order among themselves
// has ZERO observable effect on any test or any real mint - moving this
// clause to the last position, after both, leaves every MintStage case green.
// It is written first only because it is cheapest to evaluate and it is the
// one that removes the webhook-delivery dependency entirely, not because
// anything downstream of tatara-parked depends on seeing it before the others.
//
// It states the intended rule directly - a new issue from a maintainer or
// reporter starts a clarify agent - and it is deliberately NARROWER than
// IsTrustedAuthor: it excludes the project's own bot login. IsTrustedAuthor
// documents the bot as a trusted insider (logins.go), and used verbatim here
// it would mint every bot-authored orphan issue ACTIVE too - including an
// abandoned brainstorm proposal issue whose Task was reaped and whose Issue CR
// lost its controller owner along with it. ClassifyPR clause 2 exists because
// the identical property bit on the PR side: prInReactionScope returns true
// immediately for IsTrustedAuthor, so a bot-authored PR sailed through the
// label gate too. This clause closes the issue-side twin of that hole up
// front instead of discovering it in production. It is also what makes the
// sweep a genuine BACKSTOP: a totally lost webhook delivery still results in
// an ACTIVE Task on the next scan instead of a silently parked one.
//
// THE MEASURED COST, because "affordable" above is a claim that needs a
// number and not a guess. On 2026-07-28, 6 orphan open Issue CRs were
// sweep-eligible cluster-wide (tatara 4, infrastructure 1, mtg 1) - the entire
// immediate population this clause exposes to an unconditional mint. 153
// Tasks were parked, 27 of them stage-reason backlog-sweep; every one of those
// 27 retains a controller owner (fix H13 does not release ownership on a
// backlog park), so IsOrphanIssue is false for it and it does NOT re-mint
// until the reaper eventually releases it - the cost is spread over time, not
// paid at once on this change landing. A trusted human's backlog issue costs
// exactly ONE clarify pod, ONCE: once that pod's Task parks again, the
// reaper's tatara-parked stamp (the OUTERMOST gate, above) makes the park
// PERMANENT, so the same issue is never re-minted a second time. Per-pass
// exposure is further bounded by maxNewTasksPerSweep and maxOpenTasks.
//
// It returns the CREATE-EDGE STATE and the PARK REASON to stamp alongside it, in
// that order - not a stage and a stage reason (#521). A backlog owner is `new`
// AND parked(backlog-sweep): the two are orthogonal now, so an owner that costs
// nothing no longer has to wear a fake terminal stage to say so.
func MintStage(proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, iss scm.Issue, webhookOriginated bool) (string, string) {
	if hasLabel(iss.Labels, TataraParkedLabel) {
		return tatarav1alpha1.StateNew, stage.ReasonBacklogSweep
	}
	if iss.Author != botLoginOf(proj) && tatarav1alpha1.IsTrustedAuthor(proj, repo, iss.Author) {
		return tatarav1alpha1.StateNew, ""
	}
	if webhookOriginated {
		return tatarav1alpha1.StateNew, ""
	}
	if humanHasLastWord(proj, iss.Comments) {
		return tatarav1alpha1.StateNew, ""
	}
	return tatarav1alpha1.StateNew, stage.ReasonBacklogSweep
}

// WebhookOriginated reports whether cr carries the marker an issues.opened /
// issues.reopened delivery left on it. A nil cr (no mirror yet: the sweep found
// the issue before any webhook did, or webhooks are not configured) is not
// webhook-originated - and that is the correct default, because the whole
// cutover backlog looks exactly like that.
func WebhookOriginated(cr *tatarav1alpha1.Issue) bool {
	return cr != nil && cr.Annotations[AnnWebhookOriginated] != ""
}

// MarkWebhookOriginated stamps the durable liveness marker on the mirror Issue CR
// for (repo, number). It is the WEBHOOK's half of the signal, and it is the only
// thing the webhook writes for an issues.opened/reopened delivery: THE WEBHOOK
// STILL MINTS NOTHING (the B.4 sweep is the sole intake, and a webhook that
// minted its own Task would race the sweep for the same (repo, number) natural
// key and produce a second owner).
//
// The CR is CREATED OWNERLESS when it does not exist yet, which is the normal
// case for a brand-new issue. Contract B.2 permits an ownerless Issue CR, B.1
// never garbage-collects a zero-owner object, and the sweep's mint is
// adopt-or-create (fix M3-10), so it ADOPTS this CR rather than colliding with
// it.
//
// It NEVER marks an issue a Task already OWNS. An owned issue is not an intake
// candidate (IsOrphanIssue clause (b) skips it), so the marker could not be
// consumed by a mint - it would sit there until the Task parked and the reaper
// released the CR, and then re-activate the very issue that just parked.
//
// The caller must have already applied the bot-actor and reporter-allowlist
// gates: a BOT-authored issue event must never leave a marker.
//
// THE MARKER IS STAMPED AT CREATE TIME on the common path (a brand-new issue),
// so that path performs NO second read and cannot race the informer cache. The
// Get/Update branch below still serves the already-exists case, and it retries
// NotFound as well as Conflict: the CR exists but this client's cache may not
// have observed it yet, and returning that NotFound is what 500'd the delivery
// for mtg-decks#9 and lost the mint outright (GitHub does not retry a 500).
//
// reader, when set, is the manager's UNCACHED reader (webhook.Config.APIReader /
// ProjectReconciler.APIReader), threaded into both the create-time existence
// probe and the Get/Update branch's own Get. Nil falls back to c. This is the
// belt's OTHER half: webhookMarkerBackoff below bounds how long a genuinely
// cached read is retried, but a caller that hands in the uncached reader never
// depends on that backoff at all, because every Get it makes reads the API
// server directly rather than waiting on an informer watch event.
func MarkWebhookOriginated(ctx context.Context, c client.Client, reader client.Reader, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, number int, url string, now time.Time) (bool, error) {

	if reader == nil {
		reader = c
	}
	created, err := ensureIssueCRWithAnnotations(ctx, c, reader, proj, repo, number, url,
		map[string]string{AnnWebhookOriginated: now.UTC().Format(time.RFC3339)})
	if err != nil {
		return false, err
	}
	if created {
		// A CR this call just created has no controller owner and no prior
		// marker, so both guards in the branch below are vacuous for it.
		return true, nil
	}
	key := types.NamespacedName{Namespace: proj.Namespace, Name: tatarav1alpha1.IssueName(repo.Name, number)}
	marked := false
	err = retry.OnError(webhookMarkerBackoff, func(err error) bool {
		return apierrors.IsConflict(err) || apierrors.IsNotFound(err)
	}, func() error {
		marked = false
		var iss tatarav1alpha1.Issue
		if err := reader.Get(ctx, key, &iss); err != nil {
			return err
		}
		if _, owned := own.ControllerOwner(&iss); owned {
			return nil
		}
		if iss.Annotations[AnnWebhookOriginated] != "" {
			return nil // already live; re-stamping would only move the audit timestamp
		}
		if iss.Annotations == nil {
			iss.Annotations = map[string]string{}
		}
		iss.Annotations[AnnWebhookOriginated] = now.UTC().Format(time.RFC3339)
		marked = true
		return c.Update(ctx, &iss)
	})
	if err != nil {
		return false, fmt.Errorf("webhook-originated: mark issue %s: %w", key.Name, err)
	}
	return marked, nil
}

// webhookMarkerBackoff bounds how long the belt-and-braces branches of
// MarkWebhookOriginated/ClearWebhookOriginated retry a Conflict or a NotFound
// against the CACHED client (their reader parameter is nil, e.g. an older
// caller or a unit test with no APIReader wired). retry.DefaultRetry
// (Steps:5, Duration:10ms, Factor:1.0 - ~50ms total) is sized for a write
// conflict between two live clients, not an informer that has not yet
// observed a Create; the live incident this whole fix answers (mtg-decks#9)
// lost the race by 437ms, nearly 9x that budget. Matches objbudget's own
// createLagBackoff (same problem, same shape), ~3.1s of total sleep across 5
// attempts, then a NotFound is real and surfaces.
var webhookMarkerBackoff = wait.Backoff{Steps: 5, Duration: 100 * time.Millisecond, Factor: 2}

// clearWebhookOriginated spends the marker. It runs on the mint that READ it,
// whatever stage that mint chose: a marker that outlived its mint would
// re-activate the issue on the next reap/re-mint cycle, forever.
//
// A marker whose mint was DEFERRED by the creation budget is NOT cleared - the
// issue is minted on a later pass and must still be minted ACTIVE then, or the
// budget would silently demote a live human issue into the backlog.
func (r *ProjectReconciler) clearWebhookOriginated(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, number int) error {

	return ClearWebhookOriginated(ctx, r.Client, r.APIReader, proj.Namespace, tatarav1alpha1.IssueName(repo.Name, number))
}

// ClearWebhookOriginated deletes the AnnWebhookOriginated marker from the mirror
// Issue CR (namespace, issueName). It is the "consumed exactly once" half of
// the liveness contract - the mint that READ the marker clears it, so the
// marker cannot outlive its mint and re-activate the issue on a later
// reap/re-mint cycle. Both the sweep backstop and the webhook PRIMARY mint
// (fix F7-1) call it: before F7-1 only the sweep cleared it, so a webhook mint
// left the marker behind forever.
//
// reader is MarkWebhookOriginated's uncached reader, nil-safe the same way.
// It RETRIES a NotFound exactly like Mark does now, rather than swallowing it
// as a no-op: a cached Get here can lose the SAME read-after-write race Mark
// used to lose, on an Issue CR this same request may have just created, and
// silently no-op'ing that left the marker never cleared (mtg-decks#9's
// sibling defect - the mint still succeeded, but the marker leaked forever
// because this Get never got a second chance). A CR that is genuinely gone
// surfaces as an error after the backoff exhausts, exactly like Mark; every
// caller already logs that error as non-fatal.
func ClearWebhookOriginated(ctx context.Context, c client.Client, reader client.Reader, namespace, issueName string) error {
	if reader == nil {
		reader = c
	}
	key := types.NamespacedName{Namespace: namespace, Name: issueName}
	err := retry.OnError(webhookMarkerBackoff, func(err error) bool {
		return apierrors.IsConflict(err) || apierrors.IsNotFound(err)
	}, func() error {
		var iss tatarav1alpha1.Issue
		if err := reader.Get(ctx, key, &iss); err != nil {
			return err
		}
		if _, ok := iss.Annotations[AnnWebhookOriginated]; !ok {
			return nil
		}
		delete(iss.Annotations, AnnWebhookOriginated)
		return c.Update(ctx, &iss)
	})
	if err != nil {
		return fmt.Errorf("webhook-originated: clear marker on %s: %w", key.Name, err)
	}
	return nil
}

// humanHasLastWord reports whether the last comment on the thread is
// human-authored, i.e. a human is waiting on us. An EMPTY author is never the
// bot and never a human either: a deleted account must not pass an equality gate
// in either direction, so it leaves the issue in the backlog.
func humanHasLastWord(proj *tatarav1alpha1.Project, comments []scm.IssueComment) bool {
	if len(comments) == 0 {
		return false
	}
	last := comments[len(comments)-1]
	if last.Author == "" {
		return false
	}
	return last.Author != botLoginOf(proj)
}

func botLoginOf(proj *tatarav1alpha1.Project) string {
	if proj == nil || proj.Spec.Scm == nil {
		return ""
	}
	return proj.Spec.Scm.BotLogin
}

// AdoptPR is B.4 clause 1: adopt pr into task's mrRefs iff ALL of
//
//	a. pr.author     == Project.spec.scm.botLogin
//	b. pr.headBranch == agent.TaskBranch(task)      (the canonical work branch)
//	c. pr.head.repo  == pr.base.repo                (NO forks, ever)
//
// Clause (c) is what stops an outside contributor from injecting an MR into a
// trusted Task's merge stream: a fork PR may name its head branch ANYTHING,
// including task/<a-real-task>. An UNKNOWN head repo fails CLOSED - a forge that
// will not tell us where the head lives does not get the benefit of the doubt on
// the repo that deploys the cluster.
func AdoptPR(proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task, pr scm.PRRef) bool {
	if task == nil {
		return false
	}
	bot := botLoginOf(proj)
	if bot == "" || pr.Author != bot {
		return false
	}
	if pr.HeadBranch != agent.TaskBranch(task) {
		return false
	}
	return pr.HeadRepo != "" && pr.HeadRepo == pr.Repo
}

// adoptBranchPrefixOf returns the project's configured adoptable head-branch
// prefix, or "" when adoption is not configured (the default).
func adoptBranchPrefixOf(proj *tatarav1alpha1.Project) string {
	if proj == nil || proj.Spec.UpgradePolicy == nil {
		return ""
	}
	return proj.Spec.UpgradePolicy.AdoptBranchPrefix
}

// adoptableAuthor reports whether author is an identity this project owns: the
// platform bot, or an explicitly allowlisted upgradePolicy.upgradeEngineLogins
// dependency-upgrade engine. Exact match, and an EMPTY author is never adoptable
// (a forge that will not say who opened a merge request does not get the benefit
// of the doubt on one the platform would merge).
//
// IT IS ALSO ownershipForAuthor's WHOLE TEST, and that is deliberate: the
// identity adoption accepts and the identity the merge corridor accepts must be
// ONE fact, or an adopted merge request classifies `external` and
// mergeAllowedForOwnership refuses it after the review agent has already
// approved it on the forge.
//
// This is a FORGE LOGIN, which the forge authenticated. It is deliberately not a
// git commit author: see UpgradeEngineLogins' own doc comment, and the three
// places this repo already refuses to key a decision on git author metadata.
func adoptableAuthor(proj *tatarav1alpha1.Project, author string) bool {
	if author == "" {
		return false
	}
	if author == botLoginOf(proj) {
		return true
	}
	if proj == nil || proj.Spec.UpgradePolicy == nil {
		return false
	}
	return slices.Contains(proj.Spec.UpgradePolicy.UpgradeEngineLogins, author)
}

// AdoptionCandidate reports whether a merge request could POSSIBLY adopt on its
// two static facts alone: the configured branch prefix (AdoptUpgradeMR clauses
// a+b) and an identity this project owns (clause c). It answers nothing about
// forks, owners, refusal markers or trigger labels - AdoptUpgradeMR keeps all of
// those, and the real disposition is still ClassifyPR's alone.
//
// IT EXISTS FOR THE WEBHOOK'S SELF-LOOP GATE, WHICH RUNS BEFORE ANY OF THAT.
// handleMROpened must decide whether to drop a bot-authored delivery on its
// first line, before it has looked up a Repository, a mirror CR or a live owner
// - so it cannot call AdoptUpgradeMR, which needs all three. What it needs is
// the narrower question "could this delivery be an adoption at all?", because
// the answer NO must keep meaning "swallow it, it is the agent's own PR" and the
// answer YES must mean "hand it to the shared funnel and let ClassifyPR rule".
//
// DELIBERATELY NOT A THIRD COPY OF THE AUTHOR TEST. It calls adoptableAuthor,
// the same function AdoptUpgradeMR and ownershipForAuthor call, so the identity
// adoption accepts, the identity the merge corridor accepts, and the identity
// the webhook lets past its self-loop guard stay ONE fact.
func AdoptionCandidate(proj *tatarav1alpha1.Project, headBranch, author string) bool {
	prefix := adoptBranchPrefixOf(proj)
	if prefix == "" || !strings.HasPrefix(headBranch, prefix) {
		return false
	}
	return adoptableAuthor(proj, author)
}

// adoptionRefusedSHASep separates the two halves of AnnAdoptionRefused's value,
// "<reason>@<headSHA>". "@" is safe as a separator because every park and state
// reason in internal/stage is kebab-case with no "@" in it.
const adoptionRefusedSHASep = "@"

// adoptionRefused reports whether cr carries a durable adoption refusal that
// still binds at headSHA. It is AdoptUpgradeMR clause (g), factored out because
// parsing a two-part annotation value is not something a predicate should do
// inline.
//
// IT FAILS CLOSED TWICE. A marker with no separator (nothing recorded the head)
// and a live headSHA the forge did not report both refuse: neither can PROVE the
// tree moved on from the one that was declined, and re-reviewing a bump the
// platform already rejected is the failure this marker exists to prevent.
func adoptionRefused(cr *tatarav1alpha1.MergeRequest, headSHA string) bool {
	if cr == nil {
		return false
	}
	value := cr.Annotations[AnnAdoptionRefused]
	if value == "" {
		return false
	}
	i := strings.LastIndex(value, adoptionRefusedSHASep)
	if i < 0 || headSHA == "" {
		return true
	}
	return value[i+len(adoptionRefusedSHASep):] == headSHA
}

// AdoptUpgradeMR reports whether pr is a dependency-upgrade merge request this
// project should adopt into its own upgrade Task. ALL of:
//
//	a. upgradePolicy.adoptBranchPrefix is configured (empty disables adoption)
//	b. pr.headBranch starts with it
//	c. pr.author is adoptable                    (the bot, or an allowlisted engine)
//	d. pr.head.repo == pr.base.repo              (NO forks, ever)
//	e. no Task owns the branch and no live Task owns the mirror CR
//	f. pr does NOT carry the project trigger label
//	g. its mirror carries no durable adoption REFUSAL marker for pr's head SHA
//	h. its mirror has not STOOD DOWN to an external push
//
// (c) is what makes adoption SAFE TO MERGE rather than merely possible. An
// adopted merge request is one the operator will merge on an approve, and
// ownershipForAuthor grants that permission on exactly this identity - so
// requiring it here means the ownership the merge corridor checks and the
// identity this predicate checks are the SAME fact, rather than two facts a
// mint-time flip has to reconcile. It also means anyone who pushes a branch
// called renovate/whatever gets a review, not a merge.
//
// It is ALSO the cutover mechanism. While the engine still runs with a human's
// token this returns false, the merge request falls through to clause 3, and a
// review Task is minted exactly as before. The instant the engine's token moves
// to the bot, the same merge request adopts - ahead of clause 2, which would
// otherwise ignore every bot-authored merge request outright.
//
// (d) is AdoptPR's clause verbatim and for the same reason: a fork merge
// request may name its head branch anything, including the configured prefix,
// and an UNKNOWN head repo fails CLOSED.
//
// (e) is what stops re-adoption. taskForBranch matches on agent.TaskBranch and
// can never see a renovate/* branch, so `owner` is nil for every pass - but the
// adopted Task becomes the mirror CR's controller owner, which surfaces as
// liveOwner, and from then on ClassifyPR's existing liveOwner clause sends this
// merge request to PRIgnore.
//
// (f) is the human override. Applying the trigger label to a prefixed branch is
// an explicit ask for a review, and an explicit human signal always outranks an
// automatic disposition. It also makes this clause safe under
// prReactionScope:"all", where every labelled merge request is reviewable.
//
// (g) IS WHAT MAKES A DECLINE STICK, and without it re-adoption is unbounded.
// The reaper orphans the mirror and deletes a parked or terminal adopted Task;
// (e) then holds no more, because the deterministic Task name is free again and
// the mirror has no live owner - so the very next sweep re-adopts the SAME merge
// request into a fresh Task with a fresh review turn. A bump the upgrade agent
// DECLINED as unsafe therefore comes back, and the next reviewer, who sees none
// of that history, can approve and merge it. The reaper stamps
// AnnAdoptionRefused on the still-open mirror of an adopted Task it DECLINED,
// and this reads it: same durable-annotation mechanism as AnnTerminalClosed, and
// the same reason - a decision the platform already made must outlive the object
// that made it. cr may be nil (no mirror exists, so nothing was ever refused).
//
// IT IS SCOPED TO ONE TREE, NOT TO THE MERGE REQUEST FOREVER, and that scoping
// is what makes it survivable. A dependency engine does NOT open a new merge
// request per bump within a dependency line: Renovate's default branch name is
// renovate/<dep>-<major>.x, and it FORCE-PUSHES each successive bump onto that
// same branch, keeping the same number and the same mirror. A marker keyed on
// the mirror alone would therefore refuse 1.4 because 1.3 was declined, and go
// on refusing every bump of that dependency until a human deleted an annotation
// they have no reason to know exists. adoptionRefused binds the marker to the
// head SHA that was refused, so the declined tree stays declined and a genuinely
// new one gets a fresh decision. Undoing a refusal outright is deleting the
// annotation, or merging the merge request yourself.
//
// THE TRIGGER LABEL IS AN ESCAPE HATCH ONLY WHEN THE ENGINE IS NOT THE BOT. It
// works by making clause (f) refuse adoption so the merge request falls through
// to an ordinary review Task - which is reachable only via clause 3, and clause
// 2 sends every merge request authored by Project.spec.scm.botLogin to PRIgnore
// before clause 3 is reached. So: an engine running under an allowlisted
// upgradeEngineLogins identity is labelled into a review Task; an engine running
// under the bot's own login is not, and there the only recoveries are the
// annotation and a human merge.
//
// (h) REFUSES WHAT THE MERGE CORRIDOR WOULD REFUSE. A human push onto an adopted
// merge request stands its mirror down to `external` with adoptedPushReasonPrefix,
// which mergeAllowedForOwnership rejects; flipToExternal then hands the mirror to
// a kind=review Task, so the adopted Task's own reap sees wasController=false and
// (g) marks nothing. When that review Task is later reaped the mirror is orphaned
// and nothing else stops a re-adoption - which would tell a fresh review pod
// "approving MERGES it" and then hard-error reconcileMerge on every reconcile
// until the merging deadline parks the Task. The test is exactly the merge
// corridor's own, so a stood-down TAKEOVER (external-push:, which IS mergeable
// because a maintainer asked for it) is unaffected.
func AdoptUpgradeMR(proj *tatarav1alpha1.Project, pr scm.PRRef,
	owner *tatarav1alpha1.Task, liveOwner string, cr *tatarav1alpha1.MergeRequest) bool {

	if adoptionRefused(cr, pr.HeadSHA) {
		return false
	}
	if cr != nil && cr.Status.Ownership == tatarav1alpha1.OwnershipExternal && !mergeAllowedForOwnership(cr) {
		return false
	}
	prefix := adoptBranchPrefixOf(proj)
	if prefix == "" {
		return false
	}
	if !strings.HasPrefix(pr.HeadBranch, prefix) {
		return false
	}
	if !adoptableAuthor(proj, pr.Author) {
		return false
	}
	if pr.HeadRepo == "" || pr.HeadRepo != pr.Repo {
		return false
	}
	if owner != nil || liveOwner != "" {
		return false
	}
	return !hasLabel(pr.Labels, proj.Spec.TriggerLabel)
}

// PRDisposition is the outcome of B.4's four-clause PR/MR disposition, plus
// clause 1c's PRAdoptUpgrade.
type PRDisposition string

const (
	// PRAdopt: clause 1. The PR joins the owning Task's mrRefs.
	PRAdopt PRDisposition = "adopt"
	// PRReview: clause 3. A human-authored PR in reaction scope mints a
	// review-kind Task.
	PRReview PRDisposition = "review"
	// PRIgnore: clauses 2 and 4. No Task. No pod. No tokens.
	PRIgnore PRDisposition = "ignore"
	// PRAdoptUpgrade: a dependency-upgrade merge request - head branch under the
	// project's upgradePolicy.adoptBranchPrefix AND authored by the bot or an
	// allowlisted upgradePolicy.upgradeEngineLogins account - with no owner,
	// adopted into its OWN upgrade Task bound to that existing merge request.
	// This is the Renovate fan-out: Renovate opens one merge request per update
	// with the changelog in the body, and every merge decision - including a
	// trivial patch bump - goes through a Task rather than through automerge.
	//
	// BRANCH AND AUTHOR, BOTH. The branch is what distinguishes an upgrade
	// merge request from the agent's own (clause 1 already took those); the
	// author is what makes it safe to MERGE one, because ownershipForAuthor
	// classifies a botLogin-authored merge request `tatara` and nothing has to
	// flip it. Prefix alone would adopt any branch anyone chose to call
	// renovate/something.
	PRAdoptUpgrade PRDisposition = "adopt-upgrade"
	// PRClaimed: clause 1b. The PR is adoptable BY SHAPE (bot-authored, on this
	// Task's branch, same repo) but its MergeRequest CR is already
	// controller-owned by a DIFFERENT Task, so the adoption is not this pass's
	// to make and could never succeed - ownMergeRequest never steals. It is a
	// SKIP, not an error: the owning Task holds its MR legitimately for as long
	// as it lives, including while parked (parked is re-entrant - see the
	// parked->approved/merging edges in internal/stage and DrainStandDownMerge).
	PRClaimed PRDisposition = "claimed"
)

// ClassifyPR is B.4's PR/MR disposition. FOUR clauses, plus clause 1c:
//
//  1. ADOPT into owner's mrRefs iff AdoptPR
//     1c. a DEPENDENCY-UPGRADE merge request (AdoptUpgradeMR: the configured
//     head-branch prefix AND an engine author) with no owner -> adopt it
//     into its OWN upgrade Task. Ahead of clauses 2 and 3, deliberately;
//     see the clause body.
//  2. BOT-AUTHORED and NOT ADOPTABLE  ->  IGNORE. FULL STOP.
//  3. HUMAN-AUTHORED and its MergeRequest CR is an ORPHAN (no controller owner)
//     ->  review Task iff prInReactionScope
//  4. everything else                 ->  IGNORE
//
// liveOwner is the name of the Task that controller-owns the MergeRequest CR
// for (repo, pr.Number) AND STILL EXISTS, or "" when there is none - the exact
// MR analogue of IsOrphanIssue's liveOwner parameter, and taken as a PARAMETER
// for the same reason (issue #521). This function used to call
// own.ControllerOwner(cr) twice, in BOTH clauses below, and that call returns
// owned=true for any ref carrying controller=true without ever checking the
// named Task exists. So an MR mirror whose controller-owning Task was reaped
// kept a dangling ref forever, an open HUMAN PR classified PRIgnore on every
// pass - no mint, no counter, no log - and an adoptable bot PR was held
// PRClaimed by a claimant that no longer existed.
// Minter.resolveLiveMROwner is the resolver, and it DROPS the tombstone ref
// rather than ignoring it.
//
// The ORPHAN half of clause 3 is the exact MR analogue of IsOrphanIssue's
// clause (b), and it is load-bearing in BOTH directions:
//
//	an OWNED MR CR is NOT an orphan. A human's PR never has a task/<name> head
//	branch, so taskForBranch always returns nil for it - meaning nothing else in
//	this function could tell "a PR we are already reviewing" from "a PR we have
//	never seen". A PR under ACTIVE review therefore re-classified as PRReview on
//	the very next hourly pass: a second review Task was created, its
//	ownMergeRequest failed on the existing controller owner, and the pass left a
//	stage-less junk Task behind AND returned an error - which suppresses the
//	sweep heartbeat the sweep-stalled alert reads. Every hour, for as long as the
//	PR stayed open.
//
//	an OWNERLESS MR CR IS an orphan, and it is the survivor of a reap. B.6 leaves
//	a human's still-OPEN PR its mirror and drops the ownerRef (an artifact that is
//	not ours to close must be re-mintable RIGHT NOW), so the mint ADOPTS the CR
//	rather than colliding with it. MintReviewStage decides what stage it comes
//	back at, and that is what stops the 7-day re-review loop.
//
// CLAUSE 2 CLOSES A HOLE prInReactionScope DOES NOT. That predicate returns true
// IMMEDIATELY for IsTrustedAuthor, and logins.go documents THE BOT as a trusted
// author, so the prReactionScope: labeledOrMentioned set in prod does nothing
// here - its own doc-comment says so outright ("Bot-authored PRs never reach
// here"). Two REAL populations were flowing through:
//
//	(a) ORPHANED AGENT PRs. A Task fails or parks with an open owned MR, the
//	    reaper collects the Task, the MergeRequest CR cascades, and nothing
//	    closed the SCM PR. Bot-authored, no owner, not adoptable -> a review Task
//	    -> the documented-flaky review agent approves -> the author check PASSES
//	    (the author IS botLogin) -> merging -> push-CD ships an abandoned,
//	    never-approved change.
//	(b) CI PIN-BUMP PRs. tatara-helmfile's cd-release opens a bot bump PR on
//	    EVERY release with `gh pr merge --auto`, and the wrapper's
//	    refresh-claude-code does it DAILY. Each would mint a review Task, eat the
//	    maxOpenTasks budget, and RACE the forge's own auto-merge on a PR the
//	    platform has no business touching.
//
// Consequence: every review-kind Task is non-bot-authored BY CONSTRUCTION.
// cr is the merge request's MIRROR, or nil when none exists. Only clause 1c
// reads it, for the durable adoption-refusal marker (AdoptUpgradeMR clause g);
// liveOwner is still taken separately because it is a RESOLVED liveness answer
// the caller computes, not something readable off the CR (issue #521).
func ClassifyPR(proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, pr scm.PRRef,
	owner *tatarav1alpha1.Task, liveOwner string, cr *tatarav1alpha1.MergeRequest) PRDisposition {

	if AdoptPR(proj, owner, pr) {
		// CLAUSE 1b (issue #477). An MR already controller-owned by someone
		// other than owner is not adoptable BY ANYONE: ownMergeRequest refuses
		// to steal, so calling it here is a guaranteed hard error, repeated
		// byte-identically on every 4h pass for as long as both Tasks live.
		// taskForBranch already prefers the claiming Task when it shares this
		// branch, so reaching here means the claimant is on a DIFFERENT branch
		// (a takeover hand-over mid-flight, a review mint's expectFrom source).
		// Skip; the claimant is driving the MR. A claimant that no longer
		// EXISTS is not driving anything, so liveOwner is empty for it and the
		// adoption proceeds.
		if liveOwner != "" && liveOwner != owner.Name {
			return PRClaimed
		}
		return PRAdopt
	}
	// CLAUSE 1c. A dependency-upgrade merge request adopts into its OWN upgrade
	// Task. Placed here, ahead of the bot-author clause and ahead of
	// prInReactionScope, and BOTH placements are required:
	//
	//   - ahead of clause 2, or an upgrade engine running with the bot's token
	//     has every merge request swallowed by `pr.Author == bot -> PRIgnore`.
	//     That failure mints nothing, logs nothing above V(1) and counts
	//     nothing: the engine's merge requests simply stop existing as far as
	//     the platform is concerned;
	//   - ahead of clause 3, so that while the engine still runs with a HUMAN's
	//     token nothing changes at all. AdoptUpgradeMR returns false on the
	//     author, the merge request falls through, and prInReactionScope returns
	//     true on its FIRST line for a trusted author - minting the same review
	//     Task it mints today. That is the dormant window, and it is what lets
	//     this release ship before the engine's token moves.
	//
	// AdoptUpgradeMR itself refuses to steal (owner/liveOwner) and refuses a
	// trigger-labelled merge request, so neither clause 1 nor an explicit human
	// review request is shadowed.
	if AdoptUpgradeMR(proj, pr, owner, liveOwner, cr) {
		return PRAdoptUpgrade
	}
	bot := botLoginOf(proj)
	if bot != "" && pr.Author == bot {
		return PRIgnore
	}
	if pr.Author == "" {
		return PRIgnore
	}
	if liveOwner != "" {
		return PRIgnore // not an orphan: a live Task is already working it
	}
	if prInReactionScope(proj, repo, prCandidate(pr), bot) {
		return PRReview
	}
	return PRIgnore
}

// MintReviewStage returns the stage (and reason) a review-kind Task minted for a
// human's PR enters. TWO stages, and the choice turns on ONE question: HAS A
// REVIEW ALREADY BEEN POSTED ON THIS PR?
//
//	triaging                 no MergeRequest CR, or one carrying NO posted verdict
//	                         (status.status empty, or "new" - the head MOVED and a
//	                         FRESH review is genuinely owed). Run the review.
//	parked / awaiting-human  the CR carries a posted verdict. We already said our
//	                         piece and nothing about the PR has changed.
//
// status.status is OPERATOR-OWNED and is written ONLY once a review has LANDED on
// the forge (C.5.3 clearPendingReview), so it is the durable record of "we have
// already reviewed this".
//
// THE PARKED BRANCH IS WHAT STOPS THE 7-DAY RE-REVIEW LOOP. A review-kind Task's
// only terminal is parked(awaiting-human) - fixes C3-2 and V7-1: a human's PR is
// FIXED and MERGED by the human - and B.6 reaps a non-backlog park at 7d, leaving
// the still-open PR its mirror (OWNERLESS, so the sweep re-adopts it). Minting
// that re-adoption ACTIVE would spawn a review pod that re-reviews a PR nobody
// touched and RE-POSTS the same review, every seven days, forever. Minting it
// PARKED re-anchors the mirror at ZERO agent cost and hands the PR back to F.6:
// the next genuine HUMAN comment un-parks it to reviewing, bounded by
// humanReviewRounds (cap 5) - exactly as if the Task had never been reaped.
func MintReviewStage(cr *tatarav1alpha1.MergeRequest) (string, string) {
	if cr == nil {
		return tatarav1alpha1.StateNew, ""
	}
	switch cr.Status.Status {
	case "", "new":
		return tatarav1alpha1.StateNew, ""
	}
	return tatarav1alpha1.StateNew, stage.ReasonAwaitingHuman
}

// carriedHumanReviewRounds reads back the V7-9 counter the reaper stamped on the
// mirror before it orphaned it (AnnHumanReviewRounds). It is clamped to the cap:
// a forged or corrupt annotation can only ever REDUCE the number of review pods
// this PR is still worth, never raise it.
func carriedHumanReviewRounds(cr *tatarav1alpha1.MergeRequest) int {
	if cr == nil {
		return 0
	}
	n, err := strconv.Atoi(cr.Annotations[AnnHumanReviewRounds])
	if err != nil || n < 0 {
		return 0
	}
	return min(n, tatarav1alpha1.MaxHumanReviewRounds)
}

// prCandidate adapts a listing row onto the scan candidate prInReactionScope
// reads (the !1090 token-burn-loop gate: trigger label OR @-mention OR trusted
// insider).
func prCandidate(pr scm.PRRef) candidate {
	return candidate{
		repo:       pr.Repo,
		number:     pr.Number,
		author:     pr.Author,
		headSHA:    pr.HeadSHA,
		headBranch: pr.HeadBranch,
		body:       pr.Body,
		labels:     pr.Labels,
		updatedAt:  pr.UpdatedAt,
		isPR:       true,
	}
}

// StageActive reports whether t counts against Project.spec.maxOpenTasks: NOT
// done and NOT parked. parked(backlog-sweep) Tasks are NOT active and do not
// count - that is the whole point of them, and under the 8-state model it is the
// PARK FLAG that says so rather than a fake terminal stage.
//
// A Task with an EMPTY state has not taken its create edge yet and is not
// counted.
func StageActive(t *tatarav1alpha1.Task) bool {
	if t.Status.State == "" {
		return false
	}
	return !tatarav1alpha1.TaskDone(t) && !tatarav1alpha1.Parked(t)
}

// sweepBudget holds the two creation budgets, which BOTH bind on every pass
// (fix B1 - prod runs maxOpenTasks: 6 today). Remaining orphans are minted on
// the next pass: the predicate is stateless, so nothing is lost.
type sweepBudget struct {
	project string
	maxNew  int
	maxOpen int
	active  int
	minted  int
	hit     map[string]bool
}

func newSweepBudget(proj *tatarav1alpha1.Project, active int) *sweepBudget {
	maxNew := proj.Spec.MaxNewTasksPerSweep
	if maxNew <= 0 {
		maxNew = 5
	}
	maxOpen := proj.Spec.MaxOpenTasks
	if maxOpen <= 0 {
		maxOpen = 6
	}
	return &sweepBudget{project: proj.Name, maxNew: maxNew, maxOpen: maxOpen, active: active, hit: map[string]bool{}}
}

// allow reports whether one more Task may be minted with this PARK REASON. It
// records the cap that bound so the caller can WARN once per pass.
//
// The discriminator is the park flag, not the state: every mint now lands on
// `new` or `refined`, and what decides whether it costs an open-task slot is
// whether it is minted parked. An empty reason is an active mint.
func (b *sweepBudget) allow(ctx context.Context, parkReason string) bool {
	if b.minted >= b.maxNew {
		b.capHit(ctx, obs.SweepCapMaxNewTasksPerSweep)
		return false
	}
	if parkReason == "" && b.active >= b.maxOpen {
		b.capHit(ctx, obs.SweepCapMaxOpenTasks)
		return false
	}
	return true
}

func (b *sweepBudget) record(parkReason string) {
	b.minted++
	if parkReason == "" {
		b.active++
	}
}

// mintedBucket names the sweep_pass log line's two mint counters.
func mintedBucket(parkReason string) string {
	if parkReason == "" {
		return "active"
	}
	return "parked"
}

// countBudgetSkip reports whether THIS pass should still increment
// operator_sweep_skipped_total{reason="mint_budget_bound"}. True exactly once
// per pass, the same dedup capHit already applies to its own counter, and for
// the same reason.
//
// A DEFERRAL PER ITEM IS ONE FACT, NOT N. allow returns false for EVERY
// remaining orphan once active >= maxOpen, so an unconditional counter turned
// "the cap is full" into a series whose rate is the size of the backlog, on
// every pass, forever - the alert-burying shape upgrade_headroom_bound was
// excluded from TataraSweepSkipPersistent for. The LOG line is still emitted per
// item: a counter cannot say WHICH pull request went unanswered, which is the
// entire reason skipPR and skipIssue log at all.
//
// It shares b.hit with capHit; the reason strings and the cap names are disjoint
// vocabularies (mint_budget_bound vs maxNewTasksPerSweep / maxOpenTasks).
func (b *sweepBudget) countBudgetSkip() bool {
	if b.hit[SweepSkipMintBudget] {
		return false
	}
	b.hit[SweepSkipMintBudget] = true
	return true
}

func (b *sweepBudget) capHit(ctx context.Context, cap string) {
	if b.hit[cap] {
		return
	}
	b.hit[cap] = true
	obs.SweepMintCapHitTotal.WithLabelValues(b.project, cap).Inc()
	log.FromContext(ctx).Info("sweep: creation budget bound, remaining orphans deferred to the next pass",
		"action", "sweep_mint_cap_hit", "resource_id", b.project, "cap", cap,
		"max_new_tasks_per_sweep", b.maxNew, "max_open_tasks", b.maxOpen, "active", b.active)
}

// SweepProject runs ONE sweep pass over proj: mint a Task for every orphan
// issue, dispose of every open PR/MR, and stamp the liveness heartbeat once the
// pass has run to completion. activity is the {activity} metric label
// (SweepActivity).
//
// sp is the A.7 spiller for the mirror writes; nil is legal (a sweep-minted
// Issue/MergeRequest is far under the byte budget on its first write).
//
// It returns a non-zero requeue-after when the pass deleted a stale terminal
// Task holding a natural key: that mint is still OWED and must not wait a full
// sweep period (issue #521).
func (r *ProjectReconciler) SweepProject(ctx context.Context, proj *tatarav1alpha1.Project, reader scm.SCMReader,
	repos []tatarav1alpha1.Repository, sp objbudget.Spiller, activity string) (time.Duration, error) {
	l := log.FromContext(ctx)
	now := time.Now()

	active, err := r.activeTaskCount(ctx, proj)
	if err != nil {
		obs.SweepErrorsTotal.WithLabelValues(proj.Name, activity, "list_tasks").Inc()
		// Logged HERE, not by the caller. This is the ONE path on which
		// SweepProject returns without having logged, and runScans' re-report of
		// the returned error is now V(1) (issue #477's observability half: one
		// fault must not surface as two ERROR lines at two different msg values,
		// which double-counted every sweep fault into the ERROR-rate alert).
		l.Error(err, "sweep: list_tasks", "action", "sweep_error",
			"resource_id", proj.Name, "activity", activity, "reason", "list_tasks")
		return 0, fmt.Errorf("sweep: count active tasks: %w", err)
	}
	requeue := time.Duration(0)
	budget := newSweepBudget(proj, active)
	minted := map[string]int{"active": 0, "parked": 0}
	var firstErr error
	fail := func(reason string, err error, kv ...any) {
		obs.SweepErrorsTotal.WithLabelValues(proj.Name, activity, reason).Inc()
		l.Error(err, "sweep: "+reason, append([]any{"action", "sweep_error",
			"resource_id", proj.Name, "activity", activity, "reason", reason}, kv...)...)
		if firstErr == nil {
			firstErr = err
		}
	}

	// THE OP12 DRIFT/REDELIVERY WRITER. GetPRHead (the live-head read
	// ReconcileOwnership's drift check needs) lives on scm.SCMWriter, not
	// scm.SCMReader, so sweepPRs needs its own token-bound writer alongside the
	// listing reader. Resolved ONCE per pass, best-effort: an SCMFor/secret
	// failure here must never abort the mint pipeline sweepPRs already runs off
	// reader - it only means this pass's drift/redelivery convergence is
	// skipped (writer nil, sweepPRs degrades liveHead to "" and treats every PR
	// as having no new comments), same as any other missed-webhook backstop
	// waiting for the next pass.
	writer, token, werr := r.scanWriter(ctx, proj)
	if werr != nil {
		writer = nil
		l.V(1).Info("sweep: scm writer unavailable; ownership drift/comment redelivery skipped this pass",
			"action", "sweep_writer_unavailable", "resource_id", proj.Name, "activity", activity, "error", werr.Error())
	}

	// Adoption headroom for the WHOLE pass, computed once. Per-repo would let a
	// project with five repos mint five times the cap. openUpgradeLaneCount is
	// the same counter the upgrade cron checks, so cron mints and adoptions
	// compete for the same lanes rather than each getting a private allowance -
	// which is what "maxOpenUpgrades governs adoption" has to mean.
	//
	// The nil guard on Cron is load-bearing: adoptBranchPrefixOf reads
	// Spec.UpgradePolicy, which is independent of Spec.Scm.Cron, so a project
	// could configure the prefix with no upgrade cron at all and reading
	// Cron.Upgrade unguarded would panic. In that shape the headroom stays 0 and
	// nothing adopts, which is the right answer: a project with no
	// maxOpenUpgrades has declared no upgrade capacity.
	adoptHeadroom := 0
	if adoptBranchPrefixOf(proj) != "" && proj.Spec.Scm != nil && proj.Spec.Scm.Cron != nil {
		maxOpen := proj.Spec.Scm.Cron.Upgrade.MaxOpenUpgrades
		if maxOpen <= 0 {
			maxOpen = 1
		}
		if live, cerr := r.openUpgradeLaneCount(ctx, proj); cerr != nil {
			// Fail CLOSED: an uncountable lane budget adopts nothing this pass.
			fail("count_upgrade_lanes", cerr)
		} else if live < maxOpen {
			adoptHeadroom = maxOpen - live
		}
	}

	for i := range repos {
		repo := &repos[i]
		owner, name, oerr := scm.OwnerRepo(repo.Spec.URL)
		if oerr != nil {
			fail("owner_repo", oerr, "repo", repo.Name)
			continue
		}
		issues, ierr := reader.ListOpenIssues(ctx, owner, name)
		if ierr != nil {
			fail("list_issues", ierr, "repo", repo.Name)
		} else {
			requeue = soonerRequeue(requeue,
				r.sweepIssues(ctx, proj, repo, reader, owner, name, issues, budget, minted, sp, activity, now, fail))
		}
		prs, perr := reader.ListOpenPRs(ctx, owner, name)
		if perr != nil {
			fail("list_prs", perr, "repo", repo.Name)
			continue
		}
		requeue = soonerRequeue(requeue,
			r.sweepPRs(ctx, proj, repo, reader, writer, token, owner, name, prs, budget, minted, sp, activity,
				&adoptHeadroom, fail))
	}

	// EVERY pass, including the zero: a sweep that mints nothing is the signal
	// that the backlog is converged, and its absence is the signal that it is not.
	for stg, n := range minted {
		obs.TasksMintedPerSweep.WithLabelValues(proj.Name, stg).Observe(float64(n))
	}
	// The heartbeat is LIVENESS, not zero-error health: the repos loop ran to the
	// end, so stamp it whether or not a per-item read failed. Per-item errors are
	// already metered by SweepErrorsTotal and returned below for the requeue.
	// Coupling the heartbeat to a fully-clean pass let one stale CR or one
	// transient forge error silence it for the WHOLE pass, and since the gauge
	// resets on restart the NoData(Alerting) alert then fired while the sweep was
	// in fact running. The activeTaskCount hard-failure returns BEFORE this point,
	// so a sweep that genuinely cannot run still leaves the heartbeat unset.
	obs.SweepLastSuccessTimestamp.WithLabelValues(proj.Name, activity).Set(float64(now.Unix()))
	if firstErr != nil {
		return requeue, firstErr
	}
	l.Info("sweep: pass complete",
		"action", "sweep_pass", "resource_id", proj.Name, "activity", activity,
		"repos", repoNames(repos),
		"minted_active", minted["active"],
		"minted_parked", minted["parked"],
		"active_tasks", budget.active, "duration_ms", time.Since(now).Milliseconds())
	return requeue, nil
}

// repoNames is the `repos` field on the sweep_pass log line. SweepProject is
// handed dueRepos, NOT every repo in the project, so without this a pass-complete
// line cannot be read as "these repos were scanned" - which is a diagnostic
// gap, not a cosmetic one (issue #521 took 19 hours partly because the repo set
// of each pass had to be reconstructed from per-repo cadence config and
// timestamps). Sweep order is preserved: it is the order failures will appear in.
func repoNames(repos []tatarav1alpha1.Repository) []string {
	out := make([]string, 0, len(repos))
	for i := range repos {
		out = append(out, repos[i].Name)
	}
	return out
}

// strandOrphan marks ONE orphan issue as having finished a sweep pass with no
// live owning Task. See obs.SweepOrphanStrandedSeconds for why a heartbeat
// cannot replace it.
func (r *ProjectReconciler) strandOrphan(proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository,
	ref scm.IssueRef, now time.Time) {

	obs.SweepOrphanStrandedSeconds.
		WithLabelValues(proj.Name, repo.Name, strconv.Itoa(ref.Number)).
		Set(now.Sub(ref.CreatedAt).Seconds())
}

// strandOnFailure is strandOrphan for the ERROR exits of the per-issue loop.
// The deadman used to be set only on the budget-bound and tombstone paths, so
// it UNDER-REPORTED exactly when the sweep was failing: an orphan whose
// list_comments / get_issue / mint_issue_task / resolve_live_owner failed hit
// fail() and continue with no series at all, and the gauge read ZERO for an
// issue that had just ended a pass with no live owning Task - the literal
// negation of its own Help text.
//
// The gate is IsOrphanIssue with liveOwner="" - "as far as this pass got, this
// looks like an orphan". On the failures that happen BEFORE ownership is
// resolved that deliberately over-reports (an owned issue whose mirror read
// failed gets a series until the next pass clears it), and that is the right
// direction: a deadman that under-reports while the thing it watches is broken
// is not a deadman.
func (r *ProjectReconciler) strandOnFailure(proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository,
	ext scm.Issue, ref scm.IssueRef, now time.Time) {

	if orphan, _ := IsOrphanIssue(proj, repo, ext, ""); orphan {
		r.strandOrphan(proj, repo, ref, now)
	}
}

// skipIssue records ONE deliberately-skipped issue: the counter for the alert,
// and the log line for the human. BOTH, and the reason is the one the codebase
// already gives for SweepSkippedTotal - a counter cannot say WHICH issue went
// unanswered, which is exactly the gap that let five open issues sit for 19
// hours with a green sweep heartbeat (issue #521).
//
// resource_id is the Issue MIRROR's name (iss-<repo>-<number>), not the
// project: the five live victims are named in kubectl output and in the CR's
// own metadata by that string, so it is what an operator greps for.
//
// A package-level function, not a *ProjectReconciler method: the first non-sweep
// caller (Minter.MintForItem, on the WEBHOOK path) has no reconciler, and its
// having none is exactly why it discarded the skip reason with `orphan, _` and
// left a human's refused issue with no counter and no log.
func skipIssue(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, number int, activity, reason string) {

	skipIssueMetered(ctx, proj, repo, number, activity, reason, true)
}

// skipIssueMetered is skipIssue with the counter under the caller's control. It
// exists for ONE caller - the creation-budget deferral, which logs per item and
// counts once per pass (sweepBudget.countBudgetSkip). Everything else counts
// unconditionally through skipIssue.
func skipIssueMetered(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, number int, activity, reason string, count bool) {

	if count {
		obs.SweepSkippedTotal.WithLabelValues(proj.Name, activity, reason).Inc()
	}
	log.FromContext(ctx).Info("sweep: issue skipped",
		"action", "sweep_skip_issue", "resource_id", tatarav1alpha1.IssueName(repo.Name, number),
		"activity", activity, "reason", reason, "project", proj.Name,
		"repo", repo.Name, "number", number)
}

// skipPR is skipIssue's PR arm. It exists because sweepPRs counted its skips
// with NO companion log line - and a counter cannot say WHICH PR went
// unanswered any more on this arm than on the issue arm - while its
// budget-bound path counted nothing at all.
func skipPR(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, number int, activity, reason string, kv ...any) {

	skipPRMetered(ctx, proj, repo, number, activity, reason, true, kv...)
}

// skipPRMetered is skipIssueMetered's PR arm; see it for why the counter is
// separable from the log line.
func skipPRMetered(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, number int, activity, reason string, count bool, kv ...any) {

	if count {
		obs.SweepSkippedTotal.WithLabelValues(proj.Name, activity, reason).Inc()
	}
	log.FromContext(ctx).Info("sweep: PR skipped",
		append([]any{"action", "sweep_skip_pr",
			"resource_id", tatarav1alpha1.MergeRequestName(repo.Name, number),
			"activity", activity, "reason", reason, "project", proj.Name,
			"repo", repo.Name, "number", number}, kv...)...)
}

// soonerRequeue folds one candidate requeue into the running minimum. The sweep
// used to take the LAST non-zero delay any repo asked for, which is only
// harmless while sweepRemintDelay is the single delay constant in the file - the
// moment a second one exists, last-wins silently discards the urgent one.
func soonerRequeue(cur, d time.Duration) time.Duration {
	if d <= 0 {
		return cur
	}
	if cur == 0 || d < cur {
		return d
	}
	return cur
}

// sweepIssues mints a Task for every orphan issue in one repo, within budget.
func (r *ProjectReconciler) sweepIssues(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository,
	reader scm.SCMReader, owner, name string, issues []scm.IssueRef, budget *sweepBudget, minted map[string]int,
	sp objbudget.Spiller, activity string, now time.Time, fail func(string, error, ...any)) time.Duration {

	// Clear FIRST, set per stranded issue below: an issue this pass serves must
	// lose its series on this pass, or the deadman is a permanent false alarm.
	obs.ClearSweepOrphanStranded(proj.Name, repo.Name)

	requeue := time.Duration(0)
	for _, ref := range issues {
		if ref.IsPR {
			continue
		}
		ext := issueSnapshot(proj, repo, ref)
		cr, gerr := r.issueCR(ctx, proj, repo, ref.Number)
		if gerr != nil {
			fail("get_issue_cr", gerr, "repo", repo.Name, "number", ref.Number)
			r.strandOnFailure(proj, repo, ext, ref, now)
			continue
		}
		// O9 BACKSTOP, and it runs BEFORE the orphan check on purpose. ref is an
		// OPEN forge issue; a retained declined proposal's mirror still says closed
		// and rejected, and it is controller-owned, so IsOrphanIssue is false and
		// the loop would skip it forever. A lost or reporter-gated "reopened"
		// delivery would then wedge it: never counted, never minted, and finally
		// cascaded by its owner's reap. This is also the crash-recovery entry for a
		// half-finished undo (ownerless but still rejected), which is why it keys on
		// the verdict alone and not on ownership.
		if cr != nil && cr.Status.Status == "rejected" && ext.State == "open" {
			if rerr := reopenRetainedProposal(ctx, r.Client, cr, botLoginOf(proj)); rerr != nil {
				fail("reopen_retained_proposal", rerr, "repo", repo.Name, "number", ref.Number)
				r.strandOnFailure(proj, repo, ext, ref, now)
				continue
			}
			// Re-read: the undo dropped the ownerRef in etcd, and IsOrphanIssue below
			// reads ownership off this copy. Without it the mint waits a full sweep
			// period for a reopen the operator has already acted on.
			if cr, gerr = r.issueCR(ctx, proj, repo, ref.Number); gerr != nil {
				fail("get_issue_cr", gerr, "repo", repo.Name, "number", ref.Number)
				r.strandOnFailure(proj, repo, ext, ref, now)
				continue
			}
		}
		liveOwner, lerr := r.minter().resolveLiveIssueOwner(ctx, proj, cr, activity)
		if lerr != nil {
			fail("resolve_live_owner", lerr, "repo", repo.Name, "number", ref.Number)
			r.strandOnFailure(proj, repo, ext, ref, now)
			continue
		}
		orphan, skipReason := IsOrphanIssue(proj, repo, ext, liveOwner)
		if !orphan {
			skipIssue(ctx, proj, repo, ref.Number, activity, skipReason)
			continue
		}
		// The thread is read ONLY for an orphan: an owned issue is re-read by the
		// Issue reconciler on its own MirrorCadence, and re-reading 150 backlog
		// threads on every hourly pass is precisely the forge-request explosion the
		// parked cadence exists to bound.
		comments, cerr := reader.ListIssueComments(ctx, owner, name, ref.Number)
		if cerr != nil {
			fail("list_comments", cerr, "repo", repo.Name, "number", ref.Number)
			r.strandOnFailure(proj, repo, ext, ref, now)
			continue
		}
		ext.Comments = comments
		content, ferr := reader.GetIssue(ctx, owner, name, ref.Number)
		if ferr != nil {
			fail("get_issue", ferr, "repo", repo.Name, "number", ref.Number)
			r.strandOnFailure(proj, repo, ext, ref, now)
			continue
		}
		if content.Title != "" {
			ext.Title = content.Title
		}
		ext.Body = content.Body

		// THE LIVENESS SIGNAL (F.3's Create edge). A human OPENED this issue and an
		// HMAC-verified webhook said so: it mints ACTIVE even though its thread is
		// empty. Nothing else can tell it from a cold backlog issue.
		live := WebhookOriginated(cr)
		stg, reason := MintStage(proj, repo, ext, live)
		if !budget.allow(ctx, reason) {
			skipIssueMetered(ctx, proj, repo, ref.Number, activity, SweepSkipMintBudget,
				budget.countBudgetSkip())
			r.strandOrphan(proj, repo, ref, now)
			continue
		}
		task, outcome, merr := r.minter().MintIssueTask(ctx, proj, repo, ext, stg, reason, sp)
		if merr != nil {
			fail("mint_issue_task", merr, "repo", repo.Name, "number", ref.Number)
			r.strandOnFailure(proj, repo, ext, ref, now)
			continue
		}
		switch outcome {
		case MintCreated:
			// Spent, on the mint that read it - whichever stage that mint chose.
			if live {
				if cerr := r.clearWebhookOriginated(ctx, proj, repo, ref.Number); cerr != nil {
					fail("clear_webhook_marker", cerr, "repo", repo.Name, "number", ref.Number)
				}
			}
			budget.record(reason)
			minted[mintedBucket(reason)]++
			log.FromContext(ctx).Info("sweep: minted task for orphan issue",
				"action", "sweep_mint", "resource_id", task.Name, "activity", activity,
				"repo", repo.Name, "number", ref.Number, "state", stg, "park_reason", reason,
				"webhook_originated", live)
		case MintExistingLive:
			// A webhook already minted this natural key; the sweep's backstop no-ops.
			skipIssue(ctx, proj, repo, ref.Number, activity, SweepSkipAlreadyMinted)
		case MintTombstoneDeleted:
			// The mint is OWED and has NOT happened. Ask for a fast requeue
			// instead of waiting a full sweep period.
			skipIssue(ctx, proj, repo, ref.Number, activity, SweepSkipTombstoneDeleted)
			r.strandOrphan(proj, repo, ref, now)
			requeue = soonerRequeue(requeue, sweepRemintDelay)
		case MintNotOwed:
			// Unreachable: MintIssueTask is called only past IsOrphanIssue, and
			// it never classifies. Named anyway so a future member of the
			// vocabulary cannot be added without meeting this switch - and under
			// its OWN reason, because "nothing was owed" and "a Task already
			// exists" are opposite facts.
			skipIssue(ctx, proj, repo, ref.Number, activity, SweepSkipMintNotOwed)
			r.strandOrphan(proj, repo, ref, now)
		}
	}
	return requeue
}

// sweepPRs applies the four-clause disposition (plus clause 1c's adoption arm)
// to every open PR in one repo,
// then (OP12) drives ReconcileOwnership over the resulting MR mirror: the same
// backfill/drift/comment-cursor-redelivery convergence the webhook fast path
// runs on every mirror bump, so a mirror the webhook never touched (a missed
// webhook, a pre-upgrade mirror, an MR the sweep itself just minted above)
// still converges within one sweep period. writer/token may be nil/empty (the
// SCM writer was unavailable this pass, see SweepProject) - drift/redelivery
// then best-effort no-ops for every PR rather than erroring the pass.
func (r *ProjectReconciler) sweepPRs(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository,
	reader scm.SCMReader, writer scm.SCMWriter, token, owner, name string,
	prs []scm.PRRef, budget *sweepBudget, minted map[string]int, sp objbudget.Spiller, activity string,
	adoptHeadroom *int, fail func(string, error, ...any)) time.Duration {

	l := log.FromContext(ctx)
	requeue := time.Duration(0)

	// PHASE 1: RESOLVE AND CLASSIFY, ACT ON NOTHING. Every read here used to sit
	// at the top of the single acting loop; it is hoisted so the adoption window
	// below can be computed over the merge requests this pass CAN adopt rather
	// than over the ones that merely LOOK adoptable. No read is added or
	// repeated - the acting loop consumes exactly what this one resolved.
	type prPass struct {
		pr        scm.PRRef
		cr        *tatarav1alpha1.MergeRequest
		claimedBy string
		ownerTask *tatarav1alpha1.Task
		disp      PRDisposition
	}
	passes := make([]prPass, 0, len(prs))
	for _, pr := range prs {
		// The mirror is read BEFORE the branch lookup, not after: its controller
		// owner is what disambiguates a head branch that several Tasks share
		// (issue #477, see taskForBranch).
		cr, gerr := r.mergeRequestCR(ctx, proj, repo, pr.Number)
		if gerr != nil {
			fail("get_mr_cr", gerr, "repo", repo.Name, "number", pr.Number)
			continue
		}
		// LIVENESS, not presence (issue #521). A ref naming a reaped Task is a
		// dangling string: it disambiguated taskForBranch onto a Task that does
		// not exist AND made ClassifyPR ignore an open human PR forever. The
		// resolver DROPS it, so this same pass reclassifies and mints.
		claimedBy, lerr := r.minter().resolveLiveMROwner(ctx, proj, cr, activity)
		if lerr != nil {
			fail("resolve_live_owner", lerr, "repo", repo.Name, "number", pr.Number)
			continue
		}
		ownerTask, terr := r.taskForBranch(ctx, proj, pr.HeadBranch, claimedBy)
		if terr != nil {
			fail("get_owning_task", terr, "repo", repo.Name, "number", pr.Number)
			continue
		}
		passes = append(passes, prPass{
			pr: pr, cr: cr, claimedBy: claimedBy, ownerTask: ownerTask,
			disp: ClassifyPR(proj, repo, pr, ownerTask, claimedBy, cr),
		})
	}

	// THE ADOPTION WINDOW, over the CLASSIFIED set. It is the oldest `headroom`
	// merge requests that actually classify PRAdoptUpgrade, so a merge request
	// this project already adopted - always the lowest-numbered, and always
	// PRIgnore because its mirror has a live controller owner - can no longer
	// spend a slot it could never use. Computed from an ownership-blind
	// shape filter, the window on {42 adopted, 43 adopted, 44 free} with one free
	// lane was {42}, and 44 was deferred upgrade_headroom_bound on every pass
	// forever: effective utilisation capped near maxOpenUpgrades/2.
	//
	// Ascending, because the forge lists newest-first (GitLab's default is
	// created_at desc and ListOpenPRs sets no ordering) and the cap is tight, so
	// taking them in list order would starve the oldest merge request
	// indefinitely. Number is the age proxy rather than UpdatedAt: merge-request
	// IIDs are monotonic per project, while UpdatedAt moves every time a pipeline
	// touches it. Fairness is oldest-first WITHIN a repo and repo-iteration-order
	// ACROSS repos; a global oldest-first would need a cross-repo pre-pass this
	// does not justify.
	adoptableNums := make([]int, 0, len(passes))
	for i := range passes {
		if passes[i].disp == PRAdoptUpgrade {
			adoptableNums = append(adoptableNums, passes[i].pr.Number)
		}
	}
	slices.Sort(adoptableNums)
	if len(adoptableNums) > *adoptHeadroom {
		adoptableNums = adoptableNums[:max(*adoptHeadroom, 0)]
	}
	adoptable := make(map[int]bool, len(adoptableNums))
	for _, n := range adoptableNums {
		adoptable[n] = true
	}

	// PHASE 2: ACT. Same order the forge listed in, same body as before.
	for _, p := range passes {
		pr, cr, claimedBy, ownerTask := p.pr, p.cr, p.claimedBy, p.ownerTask
		switch p.disp {
		case PRAdopt:
			if aerr := r.adoptPRIntoTask(ctx, proj, repo, pr, ownerTask, sp); aerr != nil {
				fail("adopt_pr", aerr, "repo", repo.Name, "number", pr.Number)
				continue
			}
			l.Info("sweep: adopted agent PR into its owning task",
				"action", "sweep_adopt_pr", "resource_id", ownerTask.Name, "activity", activity,
				"repo", repo.Name, "number", pr.Number, "head_branch", pr.HeadBranch)
		case PRAdoptUpgrade:
			if *adoptHeadroom <= 0 || !adoptable[pr.Number] {
				skipPR(ctx, proj, repo, pr.Number, activity, SweepSkipUpgradeHeadroom,
					"head_branch", pr.HeadBranch)
				break
			}
			// The PRRef, not the MergeRequest CR: for a merge request no Task
			// has ever bound there IS no CR, and the mint's own bindMRToTask is
			// what creates it. pr carries everything needed - number, title,
			// author, head branch, head SHA, body - from THIS pass's ListOpenPRs
			// call, so there is no second round trip either.
			tk, outcome, aerr := r.minter().MintAdoptedUpgradeTask(ctx, proj, repo, pr, sp)
			if aerr != nil {
				fail("adopt_upgrade_mr", aerr, "repo", repo.Name, "number", pr.Number)
				continue
			}
			if outcome == MintCreated {
				*adoptHeadroom--
				l.Info("sweep: adopted a dependency upgrade merge request into an upgrade task",
					"action", "sweep_adopt_upgrade_mr", "resource_id", tk.Name, "activity", activity,
					"repo", repo.Name, "number", pr.Number, "head_branch", pr.HeadBranch,
					"author", pr.Author)
			}
		case PRReview:
			stg, reason := MintReviewStage(cr)
			if !budget.allow(ctx, reason) {
				// LOGGED per item, COUNTED once per pass. This arm used to count
				// nothing at all, so an orphan PR deferred by the budget was the
				// one skip in the pass with no series and no line anywhere; it
				// then counted every deferred item on every pass, which is the
				// opposite mistake (see sweepBudget.countBudgetSkip).
				skipPRMetered(ctx, proj, repo, pr.Number, activity, SweepSkipMintBudget,
					budget.countBudgetSkip(), "state", stg)
				continue
			}
			task, outcome, merr := r.minter().MintReviewTask(ctx, proj, repo, pr, cr, stg, reason, sp)
			if merr != nil {
				fail("mint_review_task", merr, "repo", repo.Name, "number", pr.Number)
				continue
			}
			switch outcome {
			case MintCreated:
				budget.record(reason)
				minted[mintedBucket(reason)]++
				l.Info("sweep: minted review task for human PR",
					"action", "sweep_mint", "resource_id", task.Name, "activity", activity,
					"repo", repo.Name, "number", pr.Number, "state", stg, "park_reason", reason,
					"kind", SweepReviewKind, "adopted_mirror", cr != nil,
					"human_review_rounds", task.Status.HumanReviewRounds)
			case MintExistingLive:
				// A webhook already minted this natural key; the sweep's backstop no-ops.
				skipPR(ctx, proj, repo, pr.Number, activity, SweepSkipAlreadyMinted)
			case MintNotOwed:
				// Its OWN reason, not folded into already_minted: "nothing was
				// owed" and "a Task already exists" are opposite facts.
				skipPR(ctx, proj, repo, pr.Number, activity, SweepSkipMintNotOwed)
			case MintTombstoneDeleted:
				// The mint is OWED and has NOT happened; ask for a fast requeue.
				skipPR(ctx, proj, repo, pr.Number, activity, SweepSkipTombstoneDeleted)
				requeue = soonerRequeue(requeue, sweepRemintDelay)
			}
		case PRClaimed:
			// Clause 1b (issue #477). NOT an error and NOT metered as one: the
			// claiming Task owns this MR legitimately and the sweep has nothing
			// to do until that Task is reaped. Counted, though - a claim that
			// never clears is a stuck Task, and operator_sweep_skipped_total is
			// where that shows up without burning the ERROR-rate alert.
			skipPR(ctx, proj, repo, pr.Number, activity, SweepSkipMRClaimed,
				"head_branch", pr.HeadBranch, "branch_task", ownerTask.Name, "claimed_by", claimedBy)
		case PRIgnore:
			// Clause 2/4. No Task, no pod, no tokens, and the PR is NOT touched: a
			// CI pin-bump PR carries the forge's own auto-merge and the platform has
			// no business racing it.
			l.V(1).Info("sweep: ignoring PR",
				"action", "sweep_ignore_pr", "resource_id", proj.Name, "activity", activity,
				"repo", repo.Name, "number", pr.Number, "author", pr.Author, "head_branch", pr.HeadBranch)
		}

		// OP12 convergence. Re-read the mirror rather than reusing cr above: the
		// PRReview case just minted it (cr, fetched before the switch, is stale
		// nil for a freshly-minted orphan) - re-Getting is what lets THIS SAME
		// pass immediately redeliver an orphan's pending comment to the review
		// Task the switch just bound as controller owner, instead of waiting a
		// full extra sweep cycle for the mirror to catch up.
		mrCR, merr := r.mergeRequestCR(ctx, proj, repo, pr.Number)
		if merr != nil {
			fail("get_mr_cr_post_classify", merr, "repo", repo.Name, "number", pr.Number)
			continue
		}
		if mrCR == nil {
			continue
		}
		liveHead := ""
		if writer != nil {
			if h, herr := writer.GetPRHead(ctx, repo.Spec.URL, token, pr.Number); herr == nil {
				liveHead = h
			} else {
				RecordSCM(r.Metrics, providerOf(proj), "get_pr_head", herr)
				l.V(1).Info("sweep: live head read failed; drift check skipped this pass",
					"action", "sweep_get_pr_head_error", "resource_id", proj.Name, "activity", activity,
					"repo", repo.Name, "number", pr.Number, "error", herr.Error())
			}
		}
		// Gated on "not already known-tatara" rather than "already external": a
		// freshly-minted orphan's mrCR is still Ownership="" here (ReconcileOwnership
		// itself does the backfill classification below) - gating on == External
		// would miss fetching this pass's comments for exactly the orphan case OP12
		// exists to converge in one pass. A known-tatara (agent-owned) MR skips the
		// read: it has no external comment to redeliver until a drift flip, which
		// does not need newComments to happen.
		var newComments []scm.IssueComment
		if mrCR.Status.Ownership != tatarav1alpha1.OwnershipTatara {
			nc, cerr := listPRCommentsAfter(ctx, reader, owner, name, pr.Number, mrCR.Status.LastMirroredCommentID)
			if cerr != nil {
				fail("list_pr_comments", cerr, "repo", repo.Name, "number", pr.Number)
			} else {
				newComments = nc
			}
		}
		if _, oerr := r.driver().ReconcileOwnership(ctx, proj, repo, mrCR, liveHead, newComments); oerr != nil {
			fail("reconcile_ownership", oerr, "repo", repo.Name, "number", pr.Number)
		}
	}
	return requeue
}

// listPRCommentsAfter lists mr number's comments and cuts the oldest-first
// list to only what is strictly newer than cursor (an ExternalID), so the
// sweep's redelivery pass never re-delivers a comment it already redelivered
// on a prior pass. cursor == "" (never redelivered) returns the full list. A
// cursor whose comment no longer appears in the listing (rare - e.g. a forge
// trim) also returns the full list: ALL still-listed non-bot comments are
// re-delivered, not just the ones after the (now-vanished) cursor. This is
// harmless, not merely tolerated - the mirror's own set-union-on-ExternalID
// dedup (AppendCommentToMirror) makes replaying an already-mirrored comment a
// mirror no-op, and the consumer bundles redelivered comments per turn, so
// re-delivery never surfaces as a duplicate to anything downstream. Returns
// (nil, nil) when reader carries no PRCommentLister capability (a provider
// with no MR-specific comment read).
func listPRCommentsAfter(ctx context.Context, reader scm.SCMReader, owner, name string, number int, cursor string) ([]scm.IssueComment, error) {
	pl, ok := reader.(scm.PRCommentLister)
	if !ok {
		return nil, nil
	}
	all, err := pl.ListPRComments(ctx, owner, name, number)
	if err != nil {
		return nil, err
	}
	if cursor == "" {
		return all, nil
	}
	for i, c := range all {
		if c.ExternalID == cursor {
			return all[i+1:], nil
		}
	}
	return all, nil
}

// issueSnapshot builds the scm.Issue the mirror upsert consumes from a listing
// row. SyncIssue makes NO forge call: it is a pure upsert from this snapshot.
func issueSnapshot(proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, ref scm.IssueRef) scm.Issue {
	state := ref.State
	if state == "" {
		state = "open"
	}
	return scm.Issue{
		Number:    ref.Number,
		URL:       issueURLFromRepoURL(repo.Spec.URL, providerOf(proj), ref.Repo, ref.Number),
		Title:     ref.Title,
		Author:    ref.Author,
		State:     state,
		Labels:    ref.Labels,
		CreatedAt: ref.CreatedAt,
		UpdatedAt: ref.UpdatedAt,
	}
}

// mrSnapshot builds the scm.MergeRequest the mirror upsert consumes. HeadSHA is
// the MIRROR's last-synced head and is NEVER trusted for a merge or an approval
// decision (fix 10): both re-fetch it LIVE.
func mrSnapshot(proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, pr scm.PRRef) scm.MergeRequest {
	return scm.MergeRequest{
		Number:     pr.Number,
		URL:        mrURLFromRepoURL(repo.Spec.URL, providerOf(proj), pr.Repo, pr.Number),
		Title:      pr.Title,
		Author:     pr.Author,
		Body:       pr.Body,
		State:      "open",
		HeadBranch: pr.HeadBranch,
		HeadSHA:    pr.HeadSHA,
		UpdatedAt:  pr.UpdatedAt,
	}
}

// mrURLFromRepoURL is issueURLFromRepoURL's merge-request half: it derives the
// base (scheme+host) from repoURL rather than hardcoding a forge, so self-hosted
// GitLab works.
func mrURLFromRepoURL(repoURL, provider, repo string, number int) string {
	base := "https://github.com"
	if u, err := parseRepoBase(repoURL); err == nil {
		base = u
	} else if provider == "gitlab" {
		base = "https://gitlab.com"
	}
	if provider == "gitlab" {
		return fmt.Sprintf("%s/%s/-/merge_requests/%d", base, repo, number)
	}
	return fmt.Sprintf("%s/%s/pull/%d", base, repo, number)
}

func providerOf(proj *tatarav1alpha1.Project) string {
	if proj == nil || proj.Spec.Scm == nil {
		return ""
	}
	return proj.Spec.Scm.Provider
}

// issueCR delegates to the shared Minter (the sweep's own read path and
// MintForItem's classify-time read must not diverge).
func (r *ProjectReconciler) issueCR(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, number int) (*tatarav1alpha1.Issue, error) {
	return r.minter().issueCR(ctx, proj, repo, number)
}

// mergeRequestCR is issueCR's MergeRequest half. It is what lets ClassifyPR tell
// a PR we are ALREADY REVIEWING (owned CR) and the ORPHANED SURVIVOR of a reap
// (ownerless CR) apart from a PR we have NEVER SEEN (no CR) - a distinction the
// head branch alone can never make for a human's PR.
func (r *ProjectReconciler) mergeRequestCR(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, number int) (*tatarav1alpha1.MergeRequest, error) {
	return r.minter().mergeRequestCR(ctx, proj, repo, number)
}

// taskForBranch resolves the Task an agent head branch names, by matching
// agent.TaskBranch(t) against branch over the project's own Tasks (the
// TaskProjectRefIndex field index, same pattern as activeTaskCount). The
// branch string no longer derives 1:1 from a Task's metadata.name (a
// numbered Task's branch carries its issue/PR number and title slug, a
// documentation Task its source SHA), so CutPrefix+Get is no longer
// sufficient - fix A3 replaces it with a scan-and-match. A branch matching
// no live Task in this project, and a same-named branch belonging to a
// DIFFERENT project's Task, both resolve to nil - which sends a
// bot-authored PR straight to clause 2 (IGNORE), exactly as intended for an
// ORPHANED AGENT PR.
//
// A BRANCH IS NOT UNIQUE TO ONE TASK (issue #477). agent.TaskBranch keys on
// (branchKind, source.number, title slug), and branchKind COLLAPSES review,
// brainstorm, clarify and refine into "chore" - so two clarify Tasks minted for
// the SAME source issue carry the SAME branch, as do an implement Task and the
// takeover Task that resumes its MR (both "feat"). Returning the first match in
// list order therefore picked a Task that does NOT own the MR often enough to
// alert: ClassifyPR said PRAdopt, ownMergeRequest refused ("already has
// controller owner <other>"), and the sweep re-ran the identical impossible
// adoption every 4h forever. claimedBy - the MR mirror's CURRENT controller
// owner, "" when there is none - is the disambiguator: it names which of the
// colliding Tasks the MR actually belongs to, so the sweep converges on a
// no-op instead of erroring.
//
// With no claim to go on, the pick must still be deterministic AND useful: a
// still-pushing Task beats a finished one (a fresh PR on a shared branch was
// pushed by whoever is still running, never by the Task that already
// delivered), newest beats oldest, and name breaks the final tie so the
// answer does not depend on list order.
func (r *ProjectReconciler) taskForBranch(ctx context.Context, proj *tatarav1alpha1.Project, branch, claimedBy string) (*tatarav1alpha1.Task, error) {
	var tl tatarav1alpha1.TaskList
	err := r.List(ctx, &tl, client.InNamespace(proj.Namespace), client.MatchingFields{TaskProjectRefIndex: proj.Name})
	if err != nil {
		if !isFieldSelectorUnsupported(err) {
			return nil, err
		}
		tl = tatarav1alpha1.TaskList{}
		if err := r.List(ctx, &tl, client.InNamespace(proj.Namespace)); err != nil {
			return nil, err
		}
	}
	var best *tatarav1alpha1.Task
	for i := range tl.Items {
		t := &tl.Items[i]
		if t.Spec.ProjectRef != proj.Name || agent.TaskBranch(t) != branch {
			continue
		}
		if claimedBy != "" && t.Name == claimedBy {
			return t, nil
		}
		if betterBranchOwner(best, t) {
			best = t
		}
	}
	return best, nil
}

// betterBranchOwner reports whether cand is a better answer than cur for a
// branch several Tasks share. See taskForBranch for why the order is
// still-pushing, then newest, then name.
func betterBranchOwner(cur, cand *tatarav1alpha1.Task) bool {
	if cur == nil {
		return true
	}
	if a, b := taskStillPushes(cand), taskStillPushes(cur); a != b {
		return a
	}
	if !cand.CreationTimestamp.Equal(&cur.CreationTimestamp) {
		return cand.CreationTimestamp.After(cur.CreationTimestamp.Time)
	}
	return cand.Name > cur.Name
}

// taskStillPushes reports whether t may still push to its branch. It is exactly
// !TaskDone. The old `|| stage == StageParked` clause is gone with #521: parked
// stopped being a terminal, so a parked(ownership-lost) Task - which
// DrainStandDownMerge re-drives and which RESUMES pushing - is already covered
// by !TaskDone.
func taskStillPushes(t *tatarav1alpha1.Task) bool {
	return !tatarav1alpha1.TaskDone(t)
}

// liveTaskPushingTo returns the name of a Task OTHER than exclude that still
// PUSHES to branch, or "" when none does. It is the reaper's last gate before
// deleting a branch on the forge (issue #443).
//
// It asks agent.PushBranch, not agent.TaskBranch (taskForBranch's question): a
// takeover Task pushes to the abandoned MR's EXISTING head branch, which is
// derived from a DIFFERENT Task's name, so the TaskBranch scan would answer
// "nobody" for the one case where deleting destroys live work.
//
// parked counts as LIVE even though TaskDone calls it terminal: a
// parked(ownership-lost) takeover Task is re-entered into under-implementation
// by a maintainer's "take over" comment and RESUMES PUSHING to that same branch.
// Only delivered/failed/rejected are past pushing.
func (r *ProjectReconciler) liveTaskPushingTo(ctx context.Context, proj *tatarav1alpha1.Project, branch, exclude string) (string, error) {
	if branch == "" {
		return "", nil
	}
	var tl tatarav1alpha1.TaskList
	err := r.List(ctx, &tl, client.InNamespace(proj.Namespace), client.MatchingFields{TaskProjectRefIndex: proj.Name})
	if err != nil {
		if !isFieldSelectorUnsupported(err) {
			return "", err
		}
		tl = tatarav1alpha1.TaskList{}
		if err := r.List(ctx, &tl, client.InNamespace(proj.Namespace)); err != nil {
			return "", err
		}
	}
	for i := range tl.Items {
		t := &tl.Items[i]
		if t.Name == exclude || t.Spec.ProjectRef != proj.Name {
			continue
		}
		if !taskStillPushes(t) {
			continue
		}
		if agent.PushBranch(t) == branch {
			return t.Name, nil
		}
	}
	return "", nil
}

// activeTaskCount counts the project's ACTIVE Tasks via the A.3 projectRef field
// index - never a label selector.
func (r *ProjectReconciler) activeTaskCount(ctx context.Context, proj *tatarav1alpha1.Project) (int, error) {
	var tl tatarav1alpha1.TaskList
	err := r.List(ctx, &tl, client.InNamespace(proj.Namespace), client.MatchingFields{TaskProjectRefIndex: proj.Name})
	if err != nil {
		if !isFieldSelectorUnsupported(err) {
			return 0, err
		}
		tl = tatarav1alpha1.TaskList{}
		if err := r.List(ctx, &tl, client.InNamespace(proj.Namespace)); err != nil {
			return 0, err
		}
	}
	n := 0
	for i := range tl.Items {
		if tl.Items[i].Spec.ProjectRef == proj.Name && StageActive(&tl.Items[i]) {
			n++
		}
	}
	return n, nil
}

// adoptPRIntoTask is clause 1's write half: the MergeRequest CR is mirrored,
// owned by the Task, and appended to its mrRefs.
func (r *ProjectReconciler) adoptPRIntoTask(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository,
	pr scm.PRRef, task *tatarav1alpha1.Task, sp objbudget.Spiller) error {

	ext := mrSnapshot(proj, repo, pr)
	if err := r.bindMRToTask(ctx, proj, repo, ext, task, sp); err != nil {
		return err
	}
	mrName := tatarav1alpha1.MergeRequestName(repo.Name, pr.Number)
	return objbudget.FitTask(ctx, r.Client, sp, client.ObjectKeyFromObject(task), func(cur *tatarav1alpha1.Task) {
		for _, ref := range cur.Status.MRRefs {
			if ref == mrName {
				return
			}
		}
		cur.Status.MRRefs = append(cur.Status.MRRefs, mrName)
	})
}

// bindMRToTask delegates to the shared Minter (adoptPRIntoTask's mirror-and-own
// and MintReviewTask's mirror-and-own must not diverge).
func (r *ProjectReconciler) bindMRToTask(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository,
	ext scm.MergeRequest, task *tatarav1alpha1.Task, sp objbudget.Spiller) error {
	return r.minter().bindMRToTask(ctx, proj, repo, ext, task, sp)
}

// issueGoal renders the Task goal from the issue, capped at the A.4 goal limit
// on a RUNE boundary (the goal is NON-EVICTABLE, so its cap is the only thing
// bounding it).
func issueGoal(ext scm.Issue) string {
	goal := fmt.Sprintf("%s\n\n%s\n\n%s", ext.Title, ext.URL, ext.Body)
	if len(goal) <= sweepGoalLimit {
		return goal
	}
	cut := goal[:sweepGoalLimit]
	for len(cut) > 0 && !utf8.ValidString(cut) {
		cut = cut[:len(cut)-1]
	}
	return cut
}

// candidate is one scannable work item (PR, issue, or board item) normalized
// for selection + dedup. number/repo identify it; labels drive priority;
// updatedAt drives stale-first ordering. body is used for PR "Closes #N" parsing.
type candidate struct {
	repo       string
	number     int
	author     string
	headSHA    string
	headBranch string
	body       string
	labels     []string
	updatedAt  time.Time
	isPR       bool
}

// prInReactionScope reports whether a human-authored PR/MR candidate is in the
// project's PR reaction scope. When prReactionScope=="labeledOrMentioned" the PR
// must carry the project trigger label OR @-mention the bot to be reviewed; this
// stops the bot from re-reviewing unlabeled, un-mentioned MRs every scan cycle
// (the !1090 token-burn loop). Any other (empty/unset) scope reviews all PRs,
// preserving the default behavior. Bot-authored PRs never reach here (they take
// the issueLifecycle/MRCI recovery path).
func prInReactionScope(proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, c candidate, bot string) bool {
	if tatarav1alpha1.IsTrustedAuthor(proj, repo, c.author) {
		return true
	}
	scope := ""
	if proj.Spec.Scm != nil {
		scope = proj.Spec.Scm.PRReactionScope
	}
	if scope != "labeledOrMentioned" {
		return true
	}
	if hasLabel(c.labels, proj.Spec.TriggerLabel) {
		return true
	}
	return mentionsBot(c.body, bot)
}

// mentionsBot reports whether text @-mentions the bot login.
func mentionsBot(text, bot string) bool {
	if bot == "" || text == "" {
		return false
	}
	return strings.Contains(text, "@"+bot)
}
