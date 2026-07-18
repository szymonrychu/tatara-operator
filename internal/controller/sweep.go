package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/objbudget"
	"github.com/szymonrychu/tatara-operator/internal/obs"
	"github.com/szymonrychu/tatara-operator/internal/own"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	"github.com/szymonrychu/tatara-operator/internal/stage"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
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
	// issues.opened/issues.reopened delivery leaves on the mirror Issue CR, and it
	// is the ONLY thing that tells a freshly-opened human issue apart from a
	// three-year-old untouched backlog issue: both are open, human-authored and
	// ZERO-COMMENT, so humanHasLastWord is false for BOTH.
	//
	// WITHOUT IT THE PLATFORM'S FRONT DOOR IS SHUT. A human opens an issue, the
	// sweep mints parked(backlog-sweep), no pod runs, and nothing happens until the
	// human comments a SECOND time on their own issue.
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

	// TaskBranchPrefix is the head-branch namespace an agent PR lives in. B.4
	// clause 1(b) is an EXACT match on TaskBranchPrefix + the owning Task's name.
	TaskBranchPrefix = "task/"

	// SweepActivity is the {activity} label value on the sweep heartbeat and
	// error counters.
	SweepActivity = "sweep"

	// SweepIssueKind is the Task kind a sweep-minted ISSUE Task carries. F.3 has
	// NO triaging -> implementing edge: an issue Task enters clarifying, where the
	// C.6 approval grammar gates it, and only an APPROVED Task implements.
	SweepIssueKind = "clarify"
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

// SweepEnabled reports whether the B.4 sweep runs for proj. ON by default: it is
// the ONLY intake path since the cutover, so it is disabled only by an explicit
// SweepDisabledValue break-glass annotation.
func SweepEnabled(proj *tatarav1alpha1.Project) bool {
	return proj != nil && proj.Annotations[SweepAnnotation] != SweepDisabledValue
}

// IsOrphanIssue is THE orphan predicate. THREE clauses, all required:
//
//	a. issue.state == "open"                           (SCM truth)
//	b. no Issue CR for (repo, number) has a controller=true owner
//	c. IsAllowedReporter(proj, repo, issue.author)     (fix C6)
//
// cr is the Issue CR for (repo, iss.Number), or nil when none exists. A zero-
// owner CR IS an orphan: fix H13 has a failed Task RELEASE its controller
// ownership and drop the ownerRef, and per B.1 an object with zero owners is
// never garbage collected - it is still there, and the mint ADOPTS it.
//
// Clause (c) is the reporter intake gate (issue #102, api/v1alpha1/logins.go).
// It is closed-by-default when configured (an empty allowlist preserves the open
// default; an empty LOGIN fails closed under an active gate) and its entire
// purpose is that an INJECTED issue never becomes a Task.
func IsOrphanIssue(proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, iss scm.Issue, cr *tatarav1alpha1.Issue) bool {
	if iss.State != "open" {
		return false
	}
	if cr != nil {
		if _, owned := own.ControllerOwner(cr); owned {
			return false
		}
	}
	return tatarav1alpha1.IsAllowedReporter(proj, repo, iss.Author)
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
// already-triaged backlog.
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
func MintStage(proj *tatarav1alpha1.Project, iss scm.Issue, webhookOriginated bool) (string, string) {
	if hasLabel(iss.Labels, TataraParkedLabel) {
		return tatarav1alpha1.StageParked, stage.ReasonBacklogSweep
	}
	if webhookOriginated {
		return tatarav1alpha1.StageTriaging, ""
	}
	if humanHasLastWord(proj, iss.Comments) {
		return tatarav1alpha1.StageTriaging, ""
	}
	return tatarav1alpha1.StageParked, stage.ReasonBacklogSweep
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
func MarkWebhookOriginated(ctx context.Context, c client.Client, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, number int, url string, now time.Time) (bool, error) {

	if err := ensureIssueCR(ctx, c, proj, repo, number, url); err != nil {
		return false, err
	}
	key := types.NamespacedName{Namespace: proj.Namespace, Name: tatarav1alpha1.IssueName(repo.Name, number)}
	marked := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		marked = false
		var iss tatarav1alpha1.Issue
		if err := c.Get(ctx, key, &iss); err != nil {
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

// clearWebhookOriginated spends the marker. It runs on the mint that READ it,
// whatever stage that mint chose: a marker that outlived its mint would
// re-activate the issue on the next reap/re-mint cycle, forever.
//
// A marker whose mint was DEFERRED by the creation budget is NOT cleared - the
// issue is minted on a later pass and must still be minted ACTIVE then, or the
// budget would silently demote a live human issue into the backlog.
func (r *ProjectReconciler) clearWebhookOriginated(ctx context.Context, proj *tatarav1alpha1.Project,
	repo *tatarav1alpha1.Repository, number int) error {

	return ClearWebhookOriginated(ctx, r.Client, proj.Namespace, tatarav1alpha1.IssueName(repo.Name, number))
}

// ClearWebhookOriginated deletes the AnnWebhookOriginated marker from the mirror
// Issue CR (namespace, issueName), idempotently: a missing CR or missing marker
// is a no-op. It is the "consumed exactly once" half of the liveness contract -
// the mint that READ the marker clears it, so the marker cannot outlive its mint
// and re-activate the issue on a later reap/re-mint cycle. Both the sweep
// backstop and the webhook PRIMARY mint (fix F7-1) call it: before F7-1 only the
// sweep cleared it, so a webhook mint left the marker behind forever.
func ClearWebhookOriginated(ctx context.Context, c client.Client, namespace, issueName string) error {
	key := types.NamespacedName{Namespace: namespace, Name: issueName}
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var iss tatarav1alpha1.Issue
		if err := c.Get(ctx, key, &iss); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
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
//	b. pr.headBranch == "task/<owning-task-name>"
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
	if pr.HeadBranch != TaskBranchPrefix+task.Name {
		return false
	}
	return pr.HeadRepo != "" && pr.HeadRepo == pr.Repo
}

// PRDisposition is the outcome of B.4's four-clause PR/MR disposition.
type PRDisposition string

const (
	// PRAdopt: clause 1. The PR joins the owning Task's mrRefs.
	PRAdopt PRDisposition = "adopt"
	// PRReview: clause 3. A human-authored PR in reaction scope mints a
	// review-kind Task.
	PRReview PRDisposition = "review"
	// PRIgnore: clauses 2 and 4. No Task. No pod. No tokens.
	PRIgnore PRDisposition = "ignore"
)

// ClassifyPR is B.4's PR/MR disposition. FOUR clauses:
//
//  1. ADOPT into owner's mrRefs iff AdoptPR
//  2. BOT-AUTHORED and NOT ADOPTABLE  ->  IGNORE. FULL STOP.
//  3. HUMAN-AUTHORED and its MergeRequest CR is an ORPHAN (no controller owner)
//     ->  review Task iff prInReactionScope
//  4. everything else                 ->  IGNORE
//
// cr is the MergeRequest CR for (repo, pr.Number), or nil when none exists. The
// ORPHAN half of clause 3 is the exact MR analogue of IsOrphanIssue's clause (b),
// and it is load-bearing in BOTH directions:
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
func ClassifyPR(proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, pr scm.PRRef,
	owner *tatarav1alpha1.Task, cr *tatarav1alpha1.MergeRequest) PRDisposition {

	if AdoptPR(proj, owner, pr) {
		return PRAdopt
	}
	bot := botLoginOf(proj)
	if bot != "" && pr.Author == bot {
		return PRIgnore
	}
	if pr.Author == "" {
		return PRIgnore
	}
	if cr != nil {
		if _, owned := own.ControllerOwner(cr); owned {
			return PRIgnore // not an orphan: a live Task is already working it
		}
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
		return tatarav1alpha1.StageTriaging, ""
	}
	switch cr.Status.Status {
	case "", "new":
		return tatarav1alpha1.StageTriaging, ""
	}
	return tatarav1alpha1.StageParked, stage.ReasonAwaitingHuman
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

// StageActive reports whether t counts against Project.spec.maxOpenTasks:
// stage NOT IN (parked, delivered, rejected, failed). parked(backlog-sweep)
// Tasks are NOT active and do not count - that is the whole point of them.
//
// A Task with an EMPTY stage belongs to the dying phase machine (tasks 1-19 are
// additive; the old machine keeps running until Task 20) and is not counted.
func StageActive(t *tatarav1alpha1.Task) bool {
	switch t.Status.Stage {
	case "", tatarav1alpha1.StageParked, tatarav1alpha1.StageDelivered,
		tatarav1alpha1.StageRejected, tatarav1alpha1.StageFailed:
		return false
	default:
		return true
	}
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

// allow reports whether one more Task may be minted at stg. It records the cap
// that bound so the caller can WARN once per pass.
func (b *sweepBudget) allow(ctx context.Context, stg string) bool {
	if b.minted >= b.maxNew {
		b.capHit(ctx, obs.SweepCapMaxNewTasksPerSweep)
		return false
	}
	if stg != tatarav1alpha1.StageParked && b.active >= b.maxOpen {
		b.capHit(ctx, obs.SweepCapMaxOpenTasks)
		return false
	}
	return true
}

func (b *sweepBudget) record(stg string) {
	b.minted++
	if stg != tatarav1alpha1.StageParked {
		b.active++
	}
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
func (r *ProjectReconciler) SweepProject(ctx context.Context, proj *tatarav1alpha1.Project, reader scm.SCMReader,
	repos []tatarav1alpha1.Repository, sp objbudget.Spiller, activity string) error {
	l := log.FromContext(ctx)
	now := time.Now()

	active, err := r.activeTaskCount(ctx, proj)
	if err != nil {
		obs.SweepErrorsTotal.WithLabelValues(activity, "list_tasks").Inc()
		return fmt.Errorf("sweep: count active tasks: %w", err)
	}
	budget := newSweepBudget(proj, active)
	minted := map[string]int{tatarav1alpha1.StageTriaging: 0, tatarav1alpha1.StageParked: 0}
	var firstErr error
	fail := func(reason string, err error, kv ...any) {
		obs.SweepErrorsTotal.WithLabelValues(activity, reason).Inc()
		l.Error(err, "sweep: "+reason, append([]any{"action", "sweep_error",
			"resource_id", proj.Name, "activity", activity, "reason", reason}, kv...)...)
		if firstErr == nil {
			firstErr = err
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
			r.sweepIssues(ctx, proj, repo, reader, owner, name, issues, budget, minted, sp, activity, fail)
		}
		prs, perr := reader.ListOpenPRs(ctx, owner, name)
		if perr != nil {
			fail("list_prs", perr, "repo", repo.Name)
			continue
		}
		r.sweepPRs(ctx, proj, repo, prs, budget, minted, sp, activity, fail)
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
	obs.SweepLastSuccessTimestamp.WithLabelValues(activity).Set(float64(now.Unix()))
	if firstErr != nil {
		return firstErr
	}
	l.Info("sweep: pass complete",
		"action", "sweep_pass", "resource_id", proj.Name, "activity", activity,
		"minted_triaging", minted[tatarav1alpha1.StageTriaging],
		"minted_parked", minted[tatarav1alpha1.StageParked],
		"active_tasks", budget.active, "duration_ms", time.Since(now).Milliseconds())
	return nil
}

// sweepIssues mints a Task for every orphan issue in one repo, within budget.
func (r *ProjectReconciler) sweepIssues(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository,
	reader scm.SCMReader, owner, name string, issues []scm.IssueRef, budget *sweepBudget, minted map[string]int,
	sp objbudget.Spiller, activity string, fail func(string, error, ...any)) {

	for _, ref := range issues {
		if ref.IsPR {
			continue
		}
		ext := issueSnapshot(proj, repo, ref)
		cr, gerr := r.issueCR(ctx, proj, repo, ref.Number)
		if gerr != nil {
			fail("get_issue_cr", gerr, "repo", repo.Name, "number", ref.Number)
			continue
		}
		if !IsOrphanIssue(proj, repo, ext, cr) {
			continue
		}
		// The thread is read ONLY for an orphan: an owned issue is re-read by the
		// Issue reconciler on its own MirrorCadence, and re-reading 150 backlog
		// threads on every hourly pass is precisely the forge-request explosion the
		// parked cadence exists to bound.
		comments, cerr := reader.ListIssueComments(ctx, owner, name, ref.Number)
		if cerr != nil {
			fail("list_comments", cerr, "repo", repo.Name, "number", ref.Number)
			continue
		}
		ext.Comments = comments
		content, ferr := reader.GetIssue(ctx, owner, name, ref.Number)
		if ferr != nil {
			fail("get_issue", ferr, "repo", repo.Name, "number", ref.Number)
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
		stg, reason := MintStage(proj, ext, live)
		if !budget.allow(ctx, stg) {
			continue
		}
		task, created, merr := r.minter().MintIssueTask(ctx, proj, repo, ext, stg, reason, sp)
		if merr != nil {
			fail("mint_issue_task", merr, "repo", repo.Name, "number", ref.Number)
			continue
		}
		if !created {
			// A webhook already minted this natural key; the sweep's backstop no-ops.
			continue
		}
		// Spent, on the mint that read it - whichever stage that mint chose.
		if live {
			if cerr := r.clearWebhookOriginated(ctx, proj, repo, ref.Number); cerr != nil {
				fail("clear_webhook_marker", cerr, "repo", repo.Name, "number", ref.Number)
			}
		}
		budget.record(stg)
		minted[stg]++
		log.FromContext(ctx).Info("sweep: minted task for orphan issue",
			"action", "sweep_mint", "resource_id", task.Name, "activity", activity,
			"repo", repo.Name, "number", ref.Number, "stage", stg, "stage_reason", reason,
			"webhook_originated", live)
	}
}

// sweepPRs applies the four-clause disposition to every open PR in one repo.
func (r *ProjectReconciler) sweepPRs(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository,
	prs []scm.PRRef, budget *sweepBudget, minted map[string]int, sp objbudget.Spiller, activity string,
	fail func(string, error, ...any)) {

	l := log.FromContext(ctx)
	for _, pr := range prs {
		ownerTask, terr := r.taskForBranch(ctx, proj, pr.HeadBranch)
		if terr != nil {
			fail("get_owning_task", terr, "repo", repo.Name, "number", pr.Number)
			continue
		}
		cr, gerr := r.mergeRequestCR(ctx, proj, repo, pr.Number)
		if gerr != nil {
			fail("get_mr_cr", gerr, "repo", repo.Name, "number", pr.Number)
			continue
		}
		switch ClassifyPR(proj, repo, pr, ownerTask, cr) {
		case PRAdopt:
			if aerr := r.adoptPRIntoTask(ctx, proj, repo, pr, ownerTask, sp); aerr != nil {
				fail("adopt_pr", aerr, "repo", repo.Name, "number", pr.Number)
				continue
			}
			l.Info("sweep: adopted agent PR into its owning task",
				"action", "sweep_adopt_pr", "resource_id", ownerTask.Name, "activity", activity,
				"repo", repo.Name, "number", pr.Number, "head_branch", pr.HeadBranch)
		case PRReview:
			stg, reason := MintReviewStage(cr)
			if !budget.allow(ctx, stg) {
				continue
			}
			task, created, merr := r.minter().MintReviewTask(ctx, proj, repo, pr, cr, stg, reason, sp)
			if merr != nil {
				fail("mint_review_task", merr, "repo", repo.Name, "number", pr.Number)
				continue
			}
			if !created {
				// A webhook already minted this natural key; the sweep's backstop no-ops.
				continue
			}
			budget.record(stg)
			minted[stg]++
			l.Info("sweep: minted review task for human PR",
				"action", "sweep_mint", "resource_id", task.Name, "activity", activity,
				"repo", repo.Name, "number", pr.Number, "stage", stg, "stage_reason", reason,
				"kind", SweepReviewKind, "adopted_mirror", cr != nil,
				"human_review_rounds", task.Status.HumanReviewRounds)
		case PRIgnore:
			// Clause 2/4. No Task, no pod, no tokens, and the PR is NOT touched: a
			// CI pin-bump PR carries the forge's own auto-merge and the platform has
			// no business racing it.
			l.V(1).Info("sweep: ignoring PR",
				"action", "sweep_ignore_pr", "resource_id", proj.Name, "activity", activity,
				"repo", repo.Name, "number", pr.Number, "author", pr.Author, "head_branch", pr.HeadBranch)
		}
	}
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

// taskForBranch resolves the Task an agent head branch names ("task/<name>").
// A branch outside the namespace, a Task that no longer exists, and a Task in
// another project all resolve to nil - which sends a bot-authored PR straight to
// clause 2 (IGNORE), exactly as intended for an ORPHANED AGENT PR.
func (r *ProjectReconciler) taskForBranch(ctx context.Context, proj *tatarav1alpha1.Project, branch string) (*tatarav1alpha1.Task, error) {
	name, ok := strings.CutPrefix(branch, TaskBranchPrefix)
	if !ok || name == "" {
		return nil, nil
	}
	var t tatarav1alpha1.Task
	if err := r.Get(ctx, types.NamespacedName{Namespace: proj.Namespace, Name: name}, &t); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if t.Spec.ProjectRef != proj.Name {
		return nil, nil
	}
	return &t, nil
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
