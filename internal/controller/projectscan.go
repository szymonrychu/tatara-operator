package controller

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
	"github.com/szymonrychu/tatara-operator/internal/agent"
	"github.com/szymonrychu/tatara-operator/internal/queue"
	"github.com/szymonrychu/tatara-operator/internal/refine"
	"github.com/szymonrychu/tatara-operator/internal/scm"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// labelRecoveryExhausted is stamped on a bot PR after closeExhaustedPR runs so
// subsequent mrScan cycles skip both re-adoption AND re-close. Removing this
// label alone does NOT fully reset recovery: priorTerminalAttempts still counts
// the existing terminal Tasks for this PR, and if that count still reaches
// maxRecoveryAttempts the very next mrScan cycle will re-close and re-stamp the
// label. To fully reset, delete the terminal Tasks for the PR in addition to
// removing this label, then reopen the PR.
const labelRecoveryExhausted = "tatara-recovery-exhausted"

// isLifecycleTerminal reports whether a lifecycle state counts as terminal for
// dedup purposes (Done/Stopped/Parked free the (repo,number) key on newer activity).
func isLifecycleTerminal(state string) bool {
	switch state {
	case "Done", "Stopped", "Parked":
		return true
	}
	return false
}

// maxRecoveryAttempts bounds how many times mrScan re-adopts the same bot PR
// before giving up. A PR driven to a terminal lifecycle this many times is not
// fixable by another autonomous pass; stop re-spawning agents and leave it for
// a human (the last park comment already explains why).
const maxRecoveryAttempts = 3

// maxImplGiveUps bounds how many times recoverOrphans rerolls an implementation
// that gave up before stopping and escalating to a human.
const maxImplGiveUps = 3

// defaultStaleProposalDays is the generous-but-finite default staleness window for
// the stale-proposal reaper when StaleProposalDays is unset (0). It is the
// intended-idle case (proposals a human never engaged), so the default is generous
// yet non-infinite so dead proposals do not accumulate unboundedly (liveness
// finding #8). A NEGATIVE StaleProposalDays is the explicit opt-out.
const defaultStaleProposalDays = 30

// maxRecoverableParkAge bounds how long a recoverable-giveup Parked task at the
// give-up cap may sit on a still-open issue before the orphan sweep re-pings the
// issue and resolves the task Done for GC, so a permanently-parked task does not
// accumulate silently with no human signal (liveness finding #6). The park anchor
// is Status.LastActivityAt (fallback CreationTimestamp).
const maxRecoverableParkAge = 7 * 24 * time.Hour

// maxDocReactivations bounds how many times a dropped/Parked documentation cycle
// is reactivated by the orphan sweep before it is left terminal for a human
// (liveness finding #7).
const maxDocReactivations = 3

// taskIsPRSlot reports whether the Task targets PR number prNumber in the PR
// slot (as opposed to issue #prNumber). On GitLab issue #N and MR !N are
// distinct objects sharing a number, so identity-by-number is not enough: the
// recovery-attempt count must only include PR-slot tasks. Resolution order:
// Spec.Source (IsPR + Number) for Phase-1+ tasks, then any ledger entry with
// Kind==pr and Number==prNumber, then the legacy LabelIsPR for pre-Phase-1
// Tasks (number carried by the source-number label / Spec.Source.Number).
func taskIsPRSlot(t *tatarav1alpha1.Task, prNumber int) bool {
	if s := t.Spec.Source; s != nil {
		if s.IsPR && s.Number == prNumber {
			return true
		}
	}
	for _, wi := range t.Status.WorkItems {
		if wi.Kind == tatarav1alpha1.WorkItemPR && wi.Number == prNumber {
			return true
		}
	}
	// Legacy fallback for pre-Phase-1 Tasks with no Spec.Source.
	if t.Spec.Source == nil && t.Labels[tatarav1alpha1.LabelIsPR] == "true" {
		return true
	}
	return false
}

// priorTerminalAttempts counts terminal (Done/Stopped/Parked) tasks that already
// targeted this exact PR, so mrScan can stop re-adopting an unfixable PR.
func priorTerminalAttempts(existing []tatarav1alpha1.Task, repoSlug string, prNumber int) int {
	return priorTerminalAttemptsExcluding(existing, repoSlug, prNumber, "")
}

// priorTerminalAttemptsExcluding is priorTerminalAttempts with one Task name
// excluded from the count. The backstop sweep passes the name of the stranded
// Task it is recovering: that Task is itself typically Parked (terminal) and
// appears in `existing`, so without exclusion it would count toward its own
// recovery bound and close an otherwise-reactivatable PR one attempt early.
func priorTerminalAttemptsExcluding(existing []tatarav1alpha1.Task, repoSlug string, prNumber int, excludeName string) int {
	n := 0
	for i := range existing {
		t := &existing[i]
		if excludeName != "" && t.Name == excludeName {
			continue
		}
		// Phase 2: match on spec/ledger identity (with legacy label fallback for
		// pre-Phase-1 Tasks), THEN require the PR slot. taskMatchesItem alone is
		// number-only and would let a terminal issue task for issue #N inflate the
		// recovery count of MR !N on GitLab (distinct objects, same number). The
		// taskIsPRSlot gate restores the IsPR discrimination the old guard provided.
		if !taskMatchesItem(t, repoSlug, prNumber) {
			continue
		}
		if !taskIsPRSlot(t, prNumber) {
			continue
		}
		if isLifecycleTerminal(t.Status.DeployState) {
			n++
		}
	}
	return n
}

// activityNextFire parses a 5-field cron and returns the next fire after base.
// ok=false when the schedule is empty (disabled) or malformed (caller logs).
func activityNextFire(schedule string, base time.Time) (time.Time, bool) {
	if schedule == "" {
		return time.Time{}, false
	}
	parsed, err := cron.ParseStandard(schedule)
	if err != nil {
		return time.Time{}, false
	}
	return parsed.Next(base), true
}

// activityScheduleAndLast returns the cron schedule string and last-scan stamp
// for one activity. Callers are post-guard (Spec.Scm and Cron are non-nil).
func activityScheduleAndLast(proj *tatarav1alpha1.Project, activity string) (string, *metav1.Time) {
	c := proj.Spec.Scm.Cron
	switch activity {
	case "mrScan":
		return c.MRScan.Schedule, proj.Status.LastMRScan
	case "issueScan":
		return c.IssueScan.Schedule, proj.Status.LastIssueScan
	case "cdScan":
		return c.CDScan.Schedule, proj.Status.LastCDScan
	case "brainstorm":
		return c.Brainstorm.Schedule, proj.Status.LastBrainstorm
	case "documentation":
		return c.Documentation.Schedule, proj.Status.LastDocumentation
	case "healthCheck":
		// Retired activity kept inert for stored-CR back-compat: activityDue
		// against it never runs from runScans (the dispatch was dropped).
		return c.HealthCheck.Schedule, proj.Status.LastHealthCheck
	}
	return "", nil
}

// scanOffset returns a deterministic offset in [0, period) for a
// (project, repo, activity) triple. Per-repo scan fires are phase-shifted by
// this offset so they spread across the cron interval instead of all firing at
// the same boundary (the synchronized hourly fan-out of issue #181). It is a
// pure hash of the identifiers, so it is stable across operator restarts and
// pods (no randomness, no wall clock).
func scanOffset(project, repo, activity string, period time.Duration) time.Duration {
	if period <= 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(project + "\x00" + repo + "\x00" + activity))
	return time.Duration(uint64(h.Sum32()) % uint64(period))
}

// cronPeriod returns the nominal interval between two consecutive fires of a
// parsed cron, used to bound per-repo scan offsets. base anchors the
// computation so it is deterministic.
func cronPeriod(sched cron.Schedule, base time.Time) time.Duration {
	f1 := sched.Next(base)
	return sched.Next(f1).Sub(f1)
}

// repoNextFire returns a repo's next phase-shifted fire strictly after `after`,
// given the base cron schedule and the repo's deterministic offset.
func repoNextFire(sched cron.Schedule, offset time.Duration, after time.Time) time.Time {
	return sched.Next(after.Add(-offset)).Add(offset)
}

// label key aliases for readability within this package.
const (
	labelSourceKind = tatarav1alpha1.LabelSourceKind
	labelActivity   = tatarav1alpha1.LabelActivity
	// labelIncident is stamped on issueLifecycle Tasks whose source issue carries
	// the incident SCM label, so tatara_issue_state can distinguish
	// incident-derived issues from regular improvements without SCM round-trips.
	labelIncident = "tatara.io/incident"
)

// headSHAForTask returns the head SHA for a task. It reads the first
// role:openedPR (bot-opened PR) or role:reviewed (human PR under review) ledger
// entry's HeadSHA; falls back to Status.MergedHeadSHA for tasks whose PR was
// merged before the ledger entry was written. Returns "" when none is set.
// role:reviewed is essential: review Tasks never carry a role:openedPR entry, so
// omitting it makes same-head re-review dedup silently fail and the bot
// re-reviews the same MR every scan cycle.
func headSHAForTask(t *tatarav1alpha1.Task) string {
	for _, wi := range t.Status.WorkItems {
		if (wi.Role == tatarav1alpha1.RoleOpenedPR || wi.Role == tatarav1alpha1.RoleReviewed) && wi.HeadSHA != "" {
			return wi.HeadSHA
		}
	}
	return t.Status.MergedHeadSHA
}

// sanitizeRepoLabel makes a repo slug DNS-label-safe by replacing '/' with '.'.
func sanitizeRepoLabel(repo string) string {
	return strings.ReplaceAll(repo, "/", ".")
}

// scanTaskLabels builds the operator-stamped labels for a cron Task.
// The three source dedup labels (source-repo, source-number, head-sha) are no
// longer written here: dedup is driven by Spec.Source and Status.WorkItems.
// Kind and activity labels are retained for observability and non-dedup filtering.
func scanTaskLabels(c candidate, activity, kind string) map[string]string {
	return map[string]string{
		labelSourceKind: kind,
		labelActivity:   activity,
	}
}

// findConvTaskToReactivate returns the first Conversation or Stopped lifecycle
// Task for the candidate whose LastActivityAt is strictly older than the
// candidate's updatedAt (meaning a new comment arrived that we missed). When
// such a task exists the caller should reactivate it to Triage rather than
// creating a duplicate Task. Returns nil when no reactivation is warranted.
func findConvTaskToReactivate(ctx context.Context, c candidate, existing []tatarav1alpha1.Task, reader scm.SCMReader, botLogin string) *tatarav1alpha1.Task {
	if c.isPR {
		return nil
	}
	for i := range existing {
		t := &existing[i]
		// Phase 2: spec/ledger identity only; legacy label fallback in taskMatchesItem.
		if !taskMatchesItem(t, c.repo, c.number) {
			continue
		}
		state := t.Status.DeployState
		if state != "Conversation" && state != "Stopped" {
			continue
		}
		if t.Status.LastActivityAt == nil {
			continue
		}
		if !c.updatedAt.After(t.Status.LastActivityAt.Time) {
			continue
		}
		// Author-aware gate: the bot's own queued comment lands after LastActivityAt
		// and would otherwise re-trigger reactivation every scan (the Conversation
		// re-comment loop). Only reactivate when a HUMAN comment is newer than our
		// last activity. Fail-open (reactivate) when we cannot read the author.
		owner, name, ok := strings.Cut(c.repo, "/")
		if !ok || reader == nil || botLogin == "" {
			return t
		}
		if humanCommentAfter(ctx, reader, owner, name, c.number, botLogin, t.Status.LastActivityAt.Time) {
			return t
		}
		continue
	}
	return nil
}

// adoptLifecycleTask re-enters an existing issueLifecycle Task to Triage in place
// of creating a duplicate. Delegates to adoptLifecycleTaskAt with entry="Triage".
func (r *ProjectReconciler) adoptLifecycleTask(ctx context.Context, proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task) error {
	return r.adoptLifecycleTaskAt(ctx, proj, task, "Triage")
}

// adoptLifecycleTaskAt re-enters an existing issueLifecycle Task to the given
// entry state (Triage or Implement) in place of creating a duplicate. It clears
// the terminal run state (Phase, ImplementEmptyRetries, ParkReason) and re-arms
// the lifecycle clocks. ImplementGiveUps is PRESERVED on an Implement re-entry
// (an auto-reroll consumes attempts toward the cap) but RESET on a Triage
// re-entry (a human re-engaging a blocked issue is a fresh start, not another
// auto-attempt). RetryOnConflict handles racing reconcile writes.
func (r *ProjectReconciler) adoptLifecycleTaskAt(ctx context.Context, proj *tatarav1alpha1.Project, task *tatarav1alpha1.Task, entry string) error {
	now := metav1.Now()
	idleMinutes := 60
	if proj.Spec.Scm != nil && proj.Spec.Scm.ConversationIdleMinutes > 0 {
		idleMinutes = proj.Spec.Scm.ConversationIdleMinutes
	}
	deadline := metav1.NewTime(now.Add(time.Duration(idleMinutes) * time.Minute))
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, fresh); err != nil {
			return err
		}
		fresh.Status.DeployState = entry
		fresh.Status.Phase = ""
		fresh.Status.ImplementEmptyRetries = 0
		fresh.Status.ParkReason = ""
		fresh.Status.LastActivityAt = &now
		fresh.Status.DeadlineAt = &deadline
		if entry == "Triage" {
			// Human re-engagement: clear the auto-reroll attempt count.
			fresh.Status.ImplementGiveUps = 0
		}
		return r.Status().Update(ctx, fresh)
	})
}

// matchingTerminalParkedLifecycleTask finds the first terminal Parked
// issueLifecycle Task for (slug, number) whose ParkReason is recoverable.
// Used by recoverOrphans to detect give-up candidates eligible for reroll.
func matchingTerminalParkedLifecycleTask(existing []tatarav1alpha1.Task, slug string, number int) *tatarav1alpha1.Task {
	for i := range existing {
		t := &existing[i]
		if t.Labels[labelSourceKind] != "issueLifecycle" {
			continue
		}
		if !taskMatchesItem(t, slug, number) {
			continue
		}
		if t.Status.DeployState == "Parked" && tatarav1alpha1.IsRecoverableGiveup(t.Status.ParkReason) {
			return t
		}
	}
	return nil
}

// isDeduped reports whether a candidate already has a Task that should suppress
// a re-pick. Phase labels are the issue's state-of-truth (Option A):
//   - any non-terminal Task for (repo,number) -> skip (fast path)
//   - PR: a terminal Task at the same head-sha -> skip
//   - issue: a managed phase label present on the OPEN issue -> skip (active =>
//     handled by the live Task above; terminal+label => orphan the backstop
//     resumes; declined => no action). No managed label -> legacy/untracked, fall
//     back to activity-vs-creation so a stale terminal Task is not re-triaged
//     unless the issue saw new HUMAN activity.
//
// humanActivity gates the no-managed-label terminal path: it reports whether the
// issue saw human activity strictly after `since` (the terminal Task's creation).
// nil means use the legacy candidate.updatedAt comparison (pure callers/tests).
// Production callers pass a closure built from the SCM reader + botLogin so the
// operator's OWN park/discuss comments (which advance updatedAt) never free the
// dedup key and respawn a duplicate (scm-author-vs-actor-egress-gate pattern).
func isDeduped(c candidate, existing []tatarav1alpha1.Task, managed []string, humanActivity func(c candidate, since time.Time) bool) bool {
	for i := range existing {
		t := &existing[i]
		// Phase 2: match on spec/ledger identity only; legacy label reads removed.
		// For old Tasks without a ledger, taskMatchesItem falls back to
		// Spec.Source (which Phase 1 always set at Task creation), and to any
		// legacy label that happens to match via the OR in the helper.
		// The label fallback in taskMatchesItem's Spec.Source block covers the
		// ~1148 existing Tasks that never carried a ledger.
		if !taskMatchesItem(t, c.repo, c.number) {
			continue
		}
		if !tatarav1alpha1.TaskTerminal(t) {
			return true
		}
		if c.isPR {
			// Same-head terminal dedup: read the headSHA from the ledger
			// (role:openedPR entry) or Status.MergedHeadSHA. Legacy Tasks carry
			// the head-sha label; headSHAForTask returns "" for them and the
			// label path below covers the backward-compat case.
			sha := headSHAForTask(t)
			if sha == "" {
				// Fall back to legacy label for Tasks created before Phase 1.
				sha = t.Labels["tatara.io/head-sha"]
			}
			if sha == c.headSHA && c.headSHA != "" {
				return true
			}
			continue
		}
		// issue: phase label is state-of-truth.
		if hasAnyLabel(c.labels, managed) {
			return true
		}
		if humanActivity != nil {
			if !humanActivity(c, t.CreationTimestamp.Time) {
				return true
			}
		} else if !c.updatedAt.After(t.CreationTimestamp.Time) {
			return true
		}
	}
	return false
}

// lastTerminalNoLabelTask returns the most recent matching terminal Task for an
// issue candidate when the candidate carries no managed phase label and EVERY
// matching Task is terminal. It returns nil otherwise (PR candidate, a managed
// label is present, a non-terminal Task exists, or there are no matching Tasks).
//
// This isolates the only isDeduped path that lets a dormant issue through on the
// cron cadence: terminal-only Tasks, no managed label, with issue updatedAt
// advanced past a terminal Task's creation (projectscan.go isDeduped). The
// operator's own write-back comment advances updatedAt, so without an
// author-aware gate every scan cycle spawns a fresh Task. Callers use the
// returned Task's creation time as the "since" for humanCommentAfter, mirroring
// the reactivation gate in findConvTaskToReactivate.
func lastTerminalNoLabelTask(c candidate, existing []tatarav1alpha1.Task, managed []string) *tatarav1alpha1.Task {
	if c.isPR || hasAnyLabel(c.labels, managed) {
		return nil
	}
	var latest *tatarav1alpha1.Task
	for i := range existing {
		t := &existing[i]
		// Phase 2: spec/ledger identity only; legacy label fallback in taskMatchesItem.
		if !taskMatchesItem(t, c.repo, c.number) {
			continue
		}
		if !tatarav1alpha1.TaskTerminal(t) {
			return nil
		}
		if latest == nil || t.CreationTimestamp.After(latest.CreationTimestamp.Time) {
			latest = t
		}
	}
	return latest
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
	createdAt  time.Time
	updatedAt  time.Time
	isPR       bool
	title      string
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

func hasLabel(labels []string, want string) bool {
	if want == "" {
		return false
	}
	for _, l := range labels {
		if l == want {
			return true
		}
	}
	return false
}

func hasAnyLabel(labels, want []string) bool {
	for _, w := range want {
		if hasLabel(labels, w) {
			return true
		}
	}
	return false
}

// isStaleUnengagedProposal reports whether an open issue is a bot-authored
// brainstorm proposal that has gone stale with no human engagement and no work
// in flight, making it safe for the staleness reaper to auto-close. It is a pure
// in-memory predicate (no SCM): the single SCM read (a comment scan) runs only
// for candidates this returns true for. Gates are ordered to bail fast and are
// all conjunctive:
//  1. not a PR;
//  2. bot-authored (empty author never matches);
//  3. carries the brainstorming phase label and NONE of approved/implementation/
//     declined (an advanced or already-declined proposal is not reaped);
//  4. window>0 and a known, non-zero UpdatedAt older than the window (a zero
//     UpdatedAt means "unknown age" and is never treated as infinitely old);
//  5. no live (non-terminal) Task references the issue;
//  6. no matching Task carries an unmerged change (HARD invariant: only a
//     merged-and-green lifecycle may close an issue; defence-in-depth over gate 3).
func isStaleUnengagedProposal(iss scm.IssueRef, existing []tatarav1alpha1.Task, brainstorming, approved, implementation, declined, botLogin string, window time.Duration) bool {
	if iss.IsPR {
		return false
	}
	if iss.Author == "" || iss.Author != botLogin {
		return false
	}
	if !hasLabel(iss.Labels, brainstorming) || hasAnyLabel(iss.Labels, []string{approved, implementation, declined}) {
		return false
	}
	if window <= 0 || iss.UpdatedAt.IsZero() || time.Since(iss.UpdatedAt) <= window {
		return false
	}
	for i := range existing {
		t := &existing[i]
		if !taskMatchesItem(t, iss.Repo, iss.Number) {
			continue
		}
		if !tatarav1alpha1.TaskTerminal(t) {
			return false
		}
		if hasUnmergedChange(t) {
			return false
		}
	}
	return true
}

// isBotBrainstormProposal reports whether the candidate is a bot-authored, open
// (non-PR) brainstorming proposal still in the proposal phase: it carries the
// brainstorming label and NONE of approved/implementation/declined. It is the
// label-only half of the source-of-churn gate - such a proposal must not spawn a
// fresh triage Task every scan cycle until a human engages it. Mirrors
// isStaleUnengagedProposal's author+label gates without the time / task-liveness
// checks (the reaper owns staleness; this owns "never engaged").
func isBotBrainstormProposal(c candidate, brainstorming, approved, implementation, declined, botLogin string) bool {
	if c.isPR {
		return false
	}
	if c.author == "" || c.author != botLogin {
		return false
	}
	return hasLabel(c.labels, brainstorming) && !hasAnyLabel(c.labels, []string{approved, implementation, declined})
}

func candidatesFromPRs(prs []scm.PRRef) []candidate {
	out := make([]candidate, 0, len(prs))
	for _, p := range prs {
		out = append(out, candidate{
			repo: p.Repo, number: p.Number, author: p.Author, headSHA: p.HeadSHA,
			headBranch: p.HeadBranch,
			body:       p.Body, labels: p.Labels, updatedAt: p.UpdatedAt, isPR: true,
			title: firstLine(p.Body),
		})
	}
	return out
}

// candidatesFromIssues drops rows GitHub reported as PRs (IsPR) so issueScan
// never triages a PR as an issue.
func candidatesFromIssues(iss []scm.IssueRef) []candidate {
	out := make([]candidate, 0, len(iss))
	for _, i := range iss {
		if i.IsPR {
			continue
		}
		out = append(out, candidate{
			repo: i.Repo, number: i.Number, author: i.Author, labels: i.Labels,
			createdAt: i.CreatedAt, updatedAt: i.UpdatedAt, isPR: false,
			title: i.Title,
		})
	}
	return out
}

// candidatesFromBoard maps board items (issues only; Number 0 = draft, skipped)
// to candidates; deduping against per-repo issues happens in the caller via
// (repo, number).
func candidatesFromBoard(items []scm.BoardItem) []candidate {
	out := make([]candidate, 0, len(items))
	for _, b := range items {
		if b.Number == 0 {
			continue
		}
		out = append(out, candidate{repo: b.Repo, number: b.Number, updatedAt: b.UpdatedAt, isPR: false})
	}
	return out
}

const systemicLabelPrefix = "tatara/systemic-"

func systemicIDOf(labels []string) string {
	for _, l := range labels {
		if strings.HasPrefix(l, systemicLabelPrefix) {
			return strings.TrimPrefix(l, systemicLabelPrefix)
		}
	}
	return ""
}

type systemicDecision struct {
	sid              string
	isLead           bool
	leadNumber       int
	sameRepoSiblings []int
	crossRepo        []string
}

func electSystemicLeads(cands []candidate, declinedLabel string) map[string]systemicDecision {
	group := map[string][]candidate{}
	for _, c := range cands {
		if c.isPR {
			continue
		}
		// A maintainer-declined issue is a permanent "no": never group it (as lead or
		// sibling) so approving another group member can never force-close it. Approval
		// of the remaining members is enforced authoritatively at implement + writeback
		// (recorded approval is not yet available at scan time).
		if declinedLabel != "" && hasLabel(c.labels, declinedLabel) {
			continue
		}
		if sid := systemicIDOf(c.labels); sid != "" {
			group[sid] = append(group[sid], c)
		}
	}
	out := map[string]systemicDecision{}
	for sid, members := range group {
		if len(members) < 2 {
			continue
		}
		leadByRepo := map[string]int{}
		for _, m := range members {
			if cur, ok := leadByRepo[m.repo]; !ok || m.number < cur {
				leadByRepo[m.repo] = m.number
			}
		}
		for _, m := range members {
			key := fmt.Sprintf("%s#%d", m.repo, m.number)
			d := systemicDecision{sid: sid, leadNumber: leadByRepo[m.repo]}
			d.isLead = m.number == leadByRepo[m.repo]
			if d.isLead {
				for _, o := range members {
					if o.repo == m.repo && o.number != m.number {
						d.sameRepoSiblings = append(d.sameRepoSiblings, o.number)
					} else if o.repo != m.repo {
						d.crossRepo = append(d.crossRepo, fmt.Sprintf("%s#%d - %s", o.repo, o.number, o.title))
					}
				}
				sort.Ints(d.sameRepoSiblings)
				sort.Strings(d.crossRepo)
			}
			out[key] = d
		}
	}
	return out
}

// createScanTask enqueues one QueuedEvent for a candidate. Returns created=true
// when a new event was enqueued (dedupKey had no existing live work).
//
// labelCand drives the dedup labels (source-repo, source-number). srcCand
// drives the TaskSource (provider, issueRef, number, isPR, authorLogin). For
// most callers they are the same candidate. For bot-PR MRCI entries they
// differ: labelCand carries the linked-issue number (dedup key) while srcCand
// carries the PR identity (number, IsPR=true).
// scanSourceFor builds a TaskSource for a scan-born task candidate. Extracted
// for testability; callers should use createScanTask which infers provider.
func scanSourceFor(provider string, c candidate) *tatarav1alpha1.TaskSource {
	sep := "#"
	if c.isPR && provider == "gitlab" {
		sep = "!"
	}
	src := &tatarav1alpha1.TaskSource{
		Provider: provider,
		IssueRef: fmt.Sprintf("%s%s%d", c.repo, sep, c.number),
		Number:   c.number,
		IsPR:     c.isPR,
		Title:    c.title,
	}
	if c.author != "" {
		src.AuthorLogin = c.author
	}
	if c.isPR {
		src.HeadSHA = c.headSHA
	}
	return src
}

func (r *ProjectReconciler) createScanTask(ctx context.Context, proj *tatarav1alpha1.Project, repo *tatarav1alpha1.Repository, labelCand, srcCand candidate, activity, kind, goal string, extraAnnotations map[string]string, systemicGroup *tatarav1alpha1.SystemicGroup) (bool, error) {
	provider := ""
	if proj.Spec.Scm != nil {
		provider = proj.Spec.Scm.Provider
	}
	src := scanSourceFor(provider, srcCand)
	// When the dedup identity (labelCand) differs from the PR identity (srcCand), the
	// linked issue number is the dedup key. Persist it as DedupNumber so taskMatchesItem
	// can find this task via spec/ledger without relying on the old source-number label.
	if labelCand.number != srcCand.number {
		src.DedupNumber = labelCand.number
	}
	// Dedup key is based on labelCand (the issue/PR that determines the task's identity).
	// For bot-PR MRCI entries, labelCand.number is the linked issue (not the PR#),
	// ensuring that mrScan and issueScan share the same dedup key for the same issue.
	// Always use "#" separator: labelCand always refers to an issue (even when found
	// via a bot-PR), and issueScan always uses "#". Using isPR/provider for "!" would
	// produce a different hash and break cross-scan dedup on GitLab.
	labelIssueRef := fmt.Sprintf("%s#%d", labelCand.repo, labelCand.number)
	dedupKey := kind + "\x00" + labelIssueRef
	taskLabels := scanTaskLabels(labelCand, activity, kind)
	// Stamp the incident flag when the source issue carries the incident SCM label
	// (tatara-incident by default, overridable via Spec.Scm.IncidentLabel). This
	// lets tatara_issue_state distinguish incident-derived issues from regular
	// improvements without per-recompute SCM reads.
	if proj.Spec.Scm != nil && hasLabel(labelCand.labels, incidentLabel(proj.Spec.Scm)) {
		taskLabels[labelIncident] = "true"
	}
	payload := tatarav1alpha1.QueuedEventPayload{
		Kind:          kind,
		RepositoryRef: repo.Name,
		Goal:          goal,
		Source:        src,
		Labels:        taskLabels,
		Annotations:   extraAnnotations,
		GenerateName:  "scan-",
		Provider:      provider,
		PodRepo:       repo.Name,
		SystemicGroup: systemicGroup,
	}
	_, created, err := queue.EnqueueEvent(ctx, r.Client, r.Seq, proj, tatarav1alpha1.QueueClassNormal, true, dedupKey, payload)
	if err != nil {
		return false, fmt.Errorf("enqueue event %s: %w", dedupKey, err)
	}
	if created {
		r.Metrics.ScanTaskCreated(activity, kind)
		log.FromContext(ctx).Info("scan: enqueued",
			"action", "scan_task_created", "resource_id", proj.Name,
			"repo", labelCand.repo, "number", labelCand.number, "kind", kind, "activity", activity)
	}
	return created, nil
}

// createBrainstormTask enqueues a project-scoped brainstorm QueuedEvent.
// Returns created=true when a new event was enqueued.
func (r *ProjectReconciler) createBrainstormTask(ctx context.Context, proj *tatarav1alpha1.Project, goal string, sources []string) (bool, error) {
	provider := ""
	if proj.Spec.Scm != nil {
		provider = proj.Spec.Scm.Provider
	}
	dedupKey := "brainstorm-" + proj.Name
	payload := tatarav1alpha1.QueuedEventPayload{
		Kind:         "brainstorm",
		Goal:         goal,
		Labels:       map[string]string{labelActivity: "brainstorm"},
		Annotations:  map[string]string{tatarav1alpha1.AnnBrainstormSources: strings.Join(sources, ",")},
		GenerateName: "brainstorm-",
		Provider:     provider,
		PodRepo:      "",
	}
	_, created, err := queue.EnqueueEvent(ctx, r.Client, r.Seq, proj, tatarav1alpha1.QueueClassNormal, true, dedupKey, payload)
	if err != nil {
		log.FromContext(ctx).Error(err, "scan: enqueue brainstorm event failed; skipping item", "action", "scan_enqueue_failed", "project", proj.Name)
		// Intentional: project-scoped tasks stamp unconditionally; no backlog/fast-refire coupling,
		// unlike createScanTask which propagates errors for per-issue deferral.
		return false, nil
	}
	if created {
		r.Metrics.ScanTaskCreated("brainstorm", "brainstorm")
	}
	return created, nil
}

// documentationScan is the scheduled documentation-sync tick. For each enrolled
// component repo (excluding the docs repo itself) that advanced since
// Status.LastDocumentation, it enqueues a documentation Task scoped to the docs
// repo carrying the source diff window as annotations. The push webhook trigger
// is retired; this is the sole documentation producer. The agent decides doc
// relevance (no-ops on trivial change); the operator only spawns when the
// source default branch has commits in the since-last-doc window.
func (r *ProjectReconciler) documentationScan(ctx context.Context, proj *tatarav1alpha1.Project, reader scm.SCMReader, repos []tatarav1alpha1.Repository) {
	l := log.FromContext(ctx)
	doc := proj.Spec.Documentation
	if doc == nil || !doc.Enabled || doc.Repo == "" {
		return
	}
	var docsRepo *tatarav1alpha1.Repository
	for i := range repos {
		if scm.SameRemote(doc.Repo, repos[i].Spec.URL) {
			docsRepo = &repos[i]
			break
		}
	}
	if docsRepo == nil {
		// Docs repo not enrolled as a Repository CR: no push access, nowhere to
		// write. Mirrors the retired push path's guard.
		l.Info("documentation: docs repo not enrolled; skipping cycle",
			"action", "scan_documentation_no_docs_repo", "resource_id", proj.Name, "docs_repo", doc.Repo)
		return
	}
	// Liveness finding #7: overlap/orphan guard. The per-head dedup key means two
	// doc Tasks for DIFFERENT source heads never dedup and could run concurrently.
	// Re-sweep dropped/Parked doc cycles so they retry (bounded), then an in-flight
	// guard (mirroring brainstormInFlightProject) suppresses starting a new doc Task
	// while one is already live. Fail-open on a list error (keep prior behavior).
	if existing, lerr := r.existingScanTasks(ctx, proj); lerr == nil {
		reactivated := r.reactivateOrphanedDocTasks(ctx, existing)
		if reactivated || documentationInFlightProject(existing) {
			l.Info("documentation: a doc cycle is already in-flight; skipping new doc Task this tick",
				"action", "scan_documentation_inflight", "resource_id", proj.Name)
			return
		}
	} else {
		l.Error(lerr, "documentation: list tasks for in-flight guard failed; proceeding",
			"action", "scan_documentation_guard_error", "resource_id", proj.Name)
	}

	var since time.Time
	if proj.Status.LastDocumentation != nil {
		since = proj.Status.LastDocumentation.Time
	}
	for i := range repos {
		src := &repos[i]
		if src.Name == docsRepo.Name || scm.SameRemote(doc.Repo, src.Spec.URL) {
			continue // self-trigger guard
		}
		owner, name, err := scm.OwnerRepo(src.Spec.URL)
		if err != nil {
			continue
		}
		commits, err := reader.ListCommits(ctx, owner, name, since)
		if err != nil {
			l.Error(err, "documentation: ListCommits", "action", "scan_list_error", "resource_id", proj.Name, "activity", "documentation", "repo", src.Name)
			continue
		}
		if len(commits) == 0 {
			continue // no change since last doc run
		}
		head, err := reader.GetDefaultBranchHeadSHA(ctx, owner, name)
		if err != nil || head == "" {
			// Fall back to the newest commit in the window as head.
			head = latestCommitSHA(commits)
		}
		base := oldestCommitSHA(commits)
		if _, cerr := r.createDocumentationTask(ctx, proj, docsRepo, src, base, head); cerr != nil {
			l.Error(cerr, "documentation: enqueue", "action", "scan_enqueue_failed", "resource_id", proj.Name, "repo", src.Name)
		}
	}
}

// oldestCommitSHA / latestCommitSHA pick the window boundary SHAs by commit date
// without assuming the reader's ordering.
func oldestCommitSHA(commits []scm.CommitRef) string {
	oldest := commits[0]
	for _, c := range commits[1:] {
		if c.Date.Before(oldest.Date) {
			oldest = c
		}
	}
	return oldest.SHA
}

func latestCommitSHA(commits []scm.CommitRef) string {
	latest := commits[0]
	for _, c := range commits[1:] {
		if c.Date.After(latest.Date) {
			latest = c
		}
	}
	return latest.SHA
}

// createDocumentationTask enqueues a documentation QueuedEvent repo-scoped to the
// docs repo (documentation is the one repo-scoped agent kind). The source repo +
// its diff window ride as annotations, matching the retired push path's shape so
// the skill contract is unchanged. Model tier (sonnet) comes from the Phase-2
// kindDefaultModel map. dedupKey keys on the source head SHA so a head that has
// not advanced re-collapses to the same event (no duplicate work per window).
func (r *ProjectReconciler) createDocumentationTask(ctx context.Context, proj *tatarav1alpha1.Project, docsRepo, sourceRepo *tatarav1alpha1.Repository, baseSHA, headSHA string) (bool, error) {
	provider := ""
	if proj.Spec.Scm != nil {
		provider = proj.Spec.Scm.Provider
	}
	dedupKey := fmt.Sprintf("doc-%s-%s", sourceRepo.Name, headSHA)
	payload := tatarav1alpha1.QueuedEventPayload{
		Kind: "documentation",
		Goal: fmt.Sprintf("Scheduled documentation sync: %s advanced to %s since the last doc "+
			"update. Review the diff and update the documentation repo if it is doc-relevant; "+
			"no-op otherwise.", sourceRepo.Spec.URL, headSHA),
		RepositoryRef: docsRepo.Name,
		GenerateName:  "documentation-",
		Provider:      provider,
		PodRepo:       docsRepo.Name,
		Labels:        map[string]string{labelActivity: "documentation"},
		Annotations: map[string]string{
			tatarav1alpha1.AnnSourceRepo:    sourceRepo.Spec.URL,
			tatarav1alpha1.AnnSourceBaseSHA: baseSHA,
			tatarav1alpha1.AnnSourceHeadSHA: headSHA,
		},
	}
	_, created, err := queue.EnqueueEvent(ctx, r.Client, r.Seq, proj, tatarav1alpha1.QueueClassNormal, true, dedupKey, payload)
	if err != nil {
		log.FromContext(ctx).Error(err, "scan: enqueue documentation event failed; skipping item", "action", "scan_enqueue_failed", "project", proj.Name)
		return false, nil
	}
	if created {
		r.Metrics.ScanTaskCreated("documentation", "documentation")
		log.FromContext(ctx).Info("scan: enqueued documentation",
			"action", "scan_task_created", "resource_id", proj.Name,
			"source_repo", sourceRepo.Name, "docs_repo", docsRepo.Name, "head_sha", headSHA)
	}
	return created, nil
}

// scanReader resolves the token-bound SCMReader for the Project's provider.
func (r *ProjectReconciler) scanReader(ctx context.Context, proj *tatarav1alpha1.Project) (scm.SCMReader, error) {
	if r.ReaderFor == nil {
		return nil, fmt.Errorf("scan: ReaderFor not wired")
	}
	var sec corev1.Secret
	key := types.NamespacedName{Namespace: proj.Namespace, Name: proj.Spec.ScmSecretRef}
	if err := r.Get(ctx, key, &sec); err != nil {
		return nil, fmt.Errorf("scan: get scm secret: %w", err)
	}
	token := string(sec.Data["token"])
	return r.ReaderFor(proj.Spec.Scm.Provider, token)
}

// scanWriter resolves the SCMWriter + token for the Project's provider, mirroring
// scanReader. Used by mrScan to close PRs that recovery has exhausted.
func (r *ProjectReconciler) scanWriter(ctx context.Context, proj *tatarav1alpha1.Project) (scm.SCMWriter, string, error) {
	if r.SCMFor == nil {
		return nil, "", fmt.Errorf("scan: SCMFor not wired")
	}
	var sec corev1.Secret
	key := types.NamespacedName{Namespace: proj.Namespace, Name: proj.Spec.ScmSecretRef}
	if err := r.Get(ctx, key, &sec); err != nil {
		return nil, "", fmt.Errorf("scan: get scm secret: %w", err)
	}
	token := string(sec.Data["token"])
	w, err := r.SCMFor(proj.Spec.Scm.Provider)
	if err != nil {
		return nil, "", err
	}
	return w, token, nil
}

// closeExhaustedPR closes a bot PR that recovery could not land after
// maxRecoveryAttempts. The branch is preserved (ClosePR does not delete it), so
// a human can reopen to retry after removing the tatara-recovery-exhausted label.
// Errors are counted via recovery_close_error so a stuck close path is observable.
func (r *ProjectReconciler) closeExhaustedPR(ctx context.Context, proj *tatarav1alpha1.Project, repos []tatarav1alpha1.Repository, c candidate) {
	l := log.FromContext(ctx)
	repo, ok := r.matchRepoForSlug(repos, c.repo)
	if !ok {
		return
	}
	w, token, err := r.scanWriter(ctx, proj)
	if err != nil {
		l.Error(err, "mrScan: scanWriter for exhausted close (leaving PR open)",
			"resource_id", proj.Name, "repo", c.repo, "pr", c.number)
		r.Metrics.ScanItem("mrScan", "recovery_close_error")
		return
	}
	body := fmt.Sprintf("Autonomous recovery could not land this PR after %d attempts; "+
		"closing as superseded. The branch is preserved - reopen to retry or hand-fix.\n"+
		"To fully reset recovery: (1) delete the existing terminal Tasks for this PR "+
		"from the cluster, (2) remove the `%s` label from the PR, then reopen. "+
		"Removing the label alone is not sufficient: prior terminal Task history "+
		"is counted independently and will re-trigger an immediate re-close.",
		maxRecoveryAttempts, labelRecoveryExhausted)
	if cerr := w.ClosePR(ctx, repo.Spec.URL, token, c.number, body); cerr != nil {
		l.Error(cerr, "mrScan: close exhausted PR failed (leaving open)",
			"resource_id", proj.Name, "repo", c.repo, "pr", c.number)
		r.Metrics.ScanItem("mrScan", "recovery_close_error")
		return
	}
	// Stamp the exhaustion label so subsequent mrScan cycles skip re-adoption AND
	// re-close. Note: removing this label alone does NOT fully reset recovery;
	// see the const comment on labelRecoveryExhausted for the full reset procedure.
	sep := "#"
	if proj.Spec.Scm != nil && proj.Spec.Scm.Provider == "gitlab" {
		sep = "!"
	}
	issueRef := fmt.Sprintf("%s%s%d", c.repo, sep, c.number)
	if lerr := w.AddLabel(ctx, token, issueRef, labelRecoveryExhausted); lerr != nil {
		l.Error(lerr, "mrScan: stamp recovery-exhausted label (non-fatal)",
			"resource_id", proj.Name, "repo", c.repo, "pr", c.number)
		// Distinct outcome: PR was closed but the exhaustion label was not stamped.
		// This is different from recovery_close_error (which means ClosePR itself failed).
		// The next cycle will re-evaluate priorTerminalAttempts and attempt to re-close.
		r.Metrics.ScanItem("mrScan", "recovery_label_error")
		r.Metrics.ScanItem("mrScan", "recovery_closed")
		l.Info("mrScan: closed recovery-exhausted bot PR (label stamp failed)",
			"action", "scan_recovery_closed", "resource_id", proj.Name, "repo", c.repo, "pr", c.number)
		return
	}
	r.Metrics.ScanItem("mrScan", "recovery_closed")
	l.Info("mrScan: closed recovery-exhausted bot PR",
		"action", "scan_recovery_closed", "resource_id", proj.Name, "repo", c.repo, "pr", c.number)
}

// matchRepoForSlug returns the Project Repository whose URL maps to the given
// owner/name slug, or ok=false.
func (r *ProjectReconciler) matchRepoForSlug(repos []tatarav1alpha1.Repository, slug string) (tatarav1alpha1.Repository, bool) {
	for i := range repos {
		owner, name, err := scm.OwnerRepo(repos[i].Spec.URL)
		if err != nil {
			continue
		}
		if owner+"/"+name == slug {
			return repos[i], true
		}
	}
	return tatarav1alpha1.Repository{}, false
}

// projectReposForScan returns all Repositories owned by the Project.
func (r *ProjectReconciler) projectReposForScan(ctx context.Context, proj *tatarav1alpha1.Project) ([]tatarav1alpha1.Repository, error) {
	var list tatarav1alpha1.RepositoryList
	if err := r.List(ctx, &list, client.InNamespace(proj.Namespace)); err != nil {
		return nil, fmt.Errorf("scan: list repositories: %w", err)
	}
	var out []tatarav1alpha1.Repository
	for i := range list.Items {
		if list.Items[i].Spec.ProjectRef == proj.Name {
			out = append(out, list.Items[i])
		}
	}
	return out, nil
}

// labelsColoredAnnotation marks a Project whose managed labels have been colored,
// so the one-shot ensure does not re-issue SCM calls every reconcile.
const labelsColoredAnnotation = "tatara.dev/labels-colored"

// ensureLabelColors best-effort creates/updates the managed tatara labels with
// their colors across the project's repos, once per project (gated by the
// annotation). Failures are logged and tolerated; it never blocks reconcile.
func (r *ProjectReconciler) ensureLabelColors(ctx context.Context, proj *tatarav1alpha1.Project) {
	if proj.Spec.Scm == nil || proj.Annotations[labelsColoredAnnotation] == "true" {
		return
	}
	l := log.FromContext(ctx)
	writer, token, err := r.scanWriter(ctx, proj)
	if err != nil {
		l.Info("ensure label colors: scm writer unavailable (retry next reconcile)",
			"action", "ensure_label_colors", "resource_id", proj.Name, "err", err.Error())
		return
	}
	repos, err := r.projectReposForScan(ctx, proj)
	if err != nil {
		return
	}
	colors := managedLabelColors(proj.Spec.Scm)
	allOK := true
	for i := range repos {
		for name, color := range colors {
			if e := writer.EnsureLabel(ctx, repos[i].Spec.URL, token, name, color); e != nil {
				allOK = false
				l.Info("ensure label colors: EnsureLabel failed (non-fatal)",
					"action", "ensure_label_colors", "resource_id", proj.Name,
					"repo", repos[i].Name, "label", name, "err", e.Error())
			}
		}
	}
	if !allOK {
		return // retry next reconcile
	}
	patch := client.MergeFrom(proj.DeepCopy())
	if proj.Annotations == nil {
		proj.Annotations = map[string]string{}
	}
	proj.Annotations[labelsColoredAnnotation] = "true"
	if e := r.Patch(ctx, proj, patch); e != nil {
		l.Info("ensure label colors: annotation patch failed (non-fatal)",
			"action", "ensure_label_colors", "resource_id", proj.Name, "err", e.Error())
	}
}

// existingScanTasks lists Project-owned Tasks carrying the dedup activity label.
func (r *ProjectReconciler) existingScanTasks(ctx context.Context, proj *tatarav1alpha1.Project) ([]tatarav1alpha1.Task, error) {
	var list tatarav1alpha1.TaskList
	if err := r.List(ctx, &list, client.InNamespace(proj.Namespace)); err != nil {
		return nil, fmt.Errorf("scan: list tasks: %w", err)
	}
	var out []tatarav1alpha1.Task
	for i := range list.Items {
		if list.Items[i].Spec.ProjectRef == proj.Name && list.Items[i].Labels[labelActivity] != "" {
			out = append(out, list.Items[i])
		}
	}
	return out, nil
}

// activityDue computes (base, due, next, ok) for one activity. base is
// Last*Scan|creationTimestamp; ok=false on empty/bad cron.
func (r *ProjectReconciler) activityDue(proj *tatarav1alpha1.Project, activity string) (time.Time, bool, time.Time, bool) {
	schedule, last := activityScheduleAndLast(proj, activity)
	base := proj.CreationTimestamp.Time
	if last != nil {
		base = last.Time
	}
	next, ok := activityNextFire(schedule, base)
	if !ok {
		return base, false, time.Time{}, false
	}
	return base, !time.Now().Before(next), next, true
}

// reposDueForScan returns the repos whose deterministic phase-shifted fire for
// `activity` has occurred since the last project-level scan stamp, plus the
// soonest upcoming per-repo fire (for requeue). ok=false when the schedule is
// empty or malformed. Spreading per-repo fires across the cron interval is the
// fix for the synchronized top-of-hour fan-out that backs up the queue
// (issue #181): the shared project-level stamp still advances on each fire, so
// the (stamp, now] window covers every repo's slot exactly once per period.
func (r *ProjectReconciler) reposDueForScan(proj *tatarav1alpha1.Project, activity string, repos []tatarav1alpha1.Repository, now time.Time) ([]tatarav1alpha1.Repository, time.Time, bool) {
	schedule, last := activityScheduleAndLast(proj, activity)
	if schedule == "" {
		return nil, time.Time{}, false
	}
	sched, err := cron.ParseStandard(schedule)
	if err != nil {
		return nil, time.Time{}, false
	}
	base := proj.CreationTimestamp.Time
	if last != nil {
		base = last.Time
	}
	period := cronPeriod(sched, base)
	var due []tatarav1alpha1.Repository
	var soonest time.Time
	for i := range repos {
		off := scanOffset(proj.Name, repos[i].Name, activity, period)
		if fire := repoNextFire(sched, off, base); !now.Before(fire) {
			due = append(due, repos[i])
		}
		if nf := repoNextFire(sched, off, now); soonest.IsZero() || nf.Before(soonest) {
			soonest = nf
		}
	}
	// No repos (or all offsets coincided): fall back to the unshifted next fire
	// so an empty project still requeues to the next period instead of busy-looping.
	if soonest.IsZero() {
		soonest = sched.Next(now)
	}
	return due, soonest, true
}

// stampScan records the per-activity Last*Scan and persists status.
// RetryOnConflict handles racing reconcile updates so the stamp always lands.
// Returns non-nil on persistent failure so the caller can log+metric the event.
func (r *ProjectReconciler) stampScan(ctx context.Context, proj *tatarav1alpha1.Project, activity string) error {
	now := metav1.Now()
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Project{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: proj.Namespace, Name: proj.Name}, fresh); err != nil {
			return err
		}
		switch activity {
		case "mrScan":
			fresh.Status.LastMRScan = &now
			proj.Status.LastMRScan = &now
		case "issueScan":
			fresh.Status.LastIssueScan = &now
			proj.Status.LastIssueScan = &now
		case "cdScan":
			fresh.Status.LastCDScan = &now
			proj.Status.LastCDScan = &now
		case "brainstorm":
			fresh.Status.LastBrainstorm = &now
			proj.Status.LastBrainstorm = &now
		case "documentation":
			fresh.Status.LastDocumentation = &now
			proj.Status.LastDocumentation = &now
		}
		return r.Status().Update(ctx, fresh)
	})
}

// mrScan lists open PRs across repos, dedups, and enqueues QueuedEvents routed
// by authoritative author -> review (human) | issueLifecycle/MRCI (bot).
func (r *ProjectReconciler) mrScan(ctx context.Context, proj *tatarav1alpha1.Project, reader scm.SCMReader, repos []tatarav1alpha1.Repository, existing []tatarav1alpha1.Task, act tatarav1alpha1.CronActivity) bool {
	l := log.FromContext(ctx)
	start := time.Now()
	bot := ""
	if proj.Spec.Scm != nil {
		bot = proj.Spec.Scm.BotLogin
	}
	seen := map[string]bool{}
	scannedRepos := make(map[string]bool)
	var cands []candidate
	for i := range repos {
		owner, name, err := scm.OwnerRepo(repos[i].Spec.URL)
		if err != nil {
			continue
		}
		prs, err := reader.ListOpenPRs(ctx, owner, name)
		if err != nil {
			l.Error(err, "scan: ListOpenPRs", "action", "scan_list_error", "resource_id", proj.Name, "activity", "mrScan", "repo", repos[i].Name)
			continue
		}
		scannedRepos[owner+"/"+name] = true
		for _, c := range candidatesFromPRs(prs) {
			key := fmt.Sprintf("%s#%d", c.repo, c.number)
			if seen[key] {
				continue
			}
			seen[key] = true
			cands = append(cands, c)
		}
	}
	for range cands {
		r.Metrics.ScanItem("mrScan", "scanned")
	}
	// Dedup BEFORE cap so a stale-but-in-flight item does not waste the cap slot.
	managed := managedPhaseLabels(proj.Spec.Scm)
	gate := r.humanActivityGate(ctx, reader, bot)
	var eligible []candidate
	for _, c := range cands {
		if isDeduped(c, existing, managed, gate) {
			r.Metrics.ScanItem("mrScan", "skipped_dedup")
		} else {
			eligible = append(eligible, c)
		}
	}
	created := 0
	deferred := 0
	// failedMark tracks candidates whose createScanTask call failed this cycle
	// (transient enqueue error, no RetryOnConflict). Their key is excluded from
	// upserts below so the scan mark is NOT advanced - advancing it would make
	// the freshness gate skip the item forever despite the fast 60s backlog
	// retry, silently dropping it from ever getting its Task.
	failedMark := map[string]bool{}
	for _, c := range eligible {
		repo, ok := r.matchRepoForSlug(repos, c.repo)
		if !ok {
			r.Metrics.ScanItem("mrScan", "skipped_norepo")
			continue
		}
		if c.author == bot && bot != "" {
			if hasLabel(c.labels, labelRecoveryExhausted) {
				r.Metrics.ScanItem("mrScan", "recovery_exhausted")
				l.Info("mrScan: skipping permanently parked bot PR (recovery-exhausted label present)",
					"action", "scan_recovery_parked", "resource_id", proj.Name, "repo", c.repo, "pr", c.number)
				continue
			}
			if priorTerminalAttempts(existing, c.repo, c.number) >= maxRecoveryAttempts {
				r.Metrics.ScanItem("mrScan", "recovery_close_attempt")
				r.closeExhaustedPR(ctx, proj, repos, c)
				continue
			}
			dedupNumber := c.number
			if issueNum, linked := scm.LinkedIssueNumber(c.body); linked {
				dedupNumber = issueNum
			}
			if hasLiveLifecycleTaskForIssue(existing, c.repo, dedupNumber) {
				r.Metrics.ScanItem("mrScan", "skipped_dedup")
				continue
			}
			// Stale-activity cutoff (issue #285): a bot lifecycle PR with no new
			// activity since we last accounted for it must not be re-created after
			// its terminal Task is GC'd. Keyed by the PR number (c.number), which
			// is what botHadLastWord below also reads. Zero UpdatedAt never gates.
			if !c.updatedAt.IsZero() {
				if m := lookupScanMark(proj.Status.ScanMarks, c.repo, c.number); m != nil && !c.updatedAt.After(m.AccountedAt.Time) {
					r.Metrics.ScanItem("mrScan", "skipped_stale_mark")
					l.Info("mrScan: skipped bot PR re-creation, no new activity since accounted mark",
						"action", "scan_mr", "resource_id", proj.Name, "repo", c.repo, "pr", c.number,
						"accounted_at", m.AccountedAt.Time, "updated_at", c.updatedAt)
					continue
				}
			}
			labelCand := candidate{
				repo: c.repo, number: dedupNumber, headSHA: c.headSHA,
				labels: c.labels, updatedAt: c.updatedAt, isPR: c.isPR,
			}
			srcCand := candidate{
				repo: c.repo, number: c.number, author: c.author, isPR: true, title: c.title,
			}
			// Bot-last-word backstop (issue #188): a bot-authored PR whose prior
			// lifecycle Task went terminal (e.g. Parked after an MRCI deadline, which
			// posts a comment) is otherwise re-created every cron cycle until recovery
			// is exhausted and the PR is wrongly closed. When tatara posted the most
			// recent comment on the MR and no human has replied, skip re-creation: the
			// terminal Task remains and a human comment reactivates it via the webhook.
			// The live MRCI polling path is unaffected (hasLiveLifecycleTaskForIssue
			// above already skips while a Task is in flight).
			if botHadLastWord(ctx, reader, srcCand, bot) {
				r.Metrics.ScanItem("mrScan", "skipped_bot_last_word")
				l.Info("mrScan: skipped bot PR re-creation, bot had the last word (awaiting human reply)",
					"action", "scan_mr", "resource_id", proj.Name, "repo", c.repo, "pr", c.number)
				continue
			}
			goal := fmt.Sprintf("Review issueLifecycle PR %s#%d", c.repo, c.number)
			ann := map[string]string{tatarav1alpha1.LifecycleEntryAnnotation: "MRCI"}
			ok2, err := r.createScanTask(ctx, proj, &repo, labelCand, srcCand, "mrScan", "issueLifecycle", goal, ann, nil)
			if err != nil {
				l.Error(err, "scan: enqueue mrScan issueLifecycle event", "resource_id", proj.Name, "repo", repo.Name)
				r.Metrics.ScanItem("mrScan", "create_error")
				deferred++
				failedMark[scanMarkKey(c.repo, c.number)] = true
				continue
			}
			if ok2 {
				r.Metrics.ScanItem("mrScan", "picked")
				created++
			}
		} else {
			if !prInReactionScope(proj, &repo, c, bot) {
				r.Metrics.ScanItem("mrScan", "skipped_scope")
				l.Info("mrScan: skipping PR out of reaction scope (unlabeled + un-mentioned under labeledOrMentioned)",
					"action", "scan_pr_out_of_scope", "resource_id", proj.Name, "repo", c.repo, "pr", c.number)
				continue
			}
			goal := fmt.Sprintf("Review and test PR %s#%d", c.repo, c.number)
			// Carry the PR head branch so the review pod checks it out read-only and
			// can run/test the change (issue #114 decision 4).
			var reviewAnn map[string]string
			if c.headBranch != "" {
				reviewAnn = map[string]string{tatarav1alpha1.AnnReviewHeadBranch: c.headBranch}
			}
			ok2, err := r.createScanTask(ctx, proj, &repo, c, c, "mrScan", "review", goal, reviewAnn, nil)
			if err != nil {
				l.Error(err, "scan: enqueue mrScan task", "resource_id", proj.Name, "repo", repo.Name)
				r.Metrics.ScanItem("mrScan", "create_error")
				deferred++
				failedMark[scanMarkKey(c.repo, c.number)] = true
				continue
			}
			if ok2 {
				r.Metrics.ScanItem("mrScan", "picked")
				created++
			}
		}
	}
	// Persist per-item high-water marks (issue #285): record the observed
	// UpdatedAt for every PR candidate we scanned this cycle, and prune marks for
	// items no longer open in the repos we actually listed. Scoped to PRs
	// (isPR=true); issueScan owns issue marks.
	keepKeys := make(map[string]bool, len(cands))
	upserts := make([]scanMarkUpsert, 0, len(cands))
	for _, c := range cands {
		k := scanMarkKey(c.repo, c.number)
		keepKeys[k] = true
		if failedMark[k] {
			continue // creation failed this cycle; do not advance the mark, so the fast retry re-attempts
		}
		upserts = append(upserts, scanMarkUpsert{repo: c.repo, number: c.number, updatedAt: c.updatedAt, isPR: true})
	}
	if err := r.persistScanMarks(ctx, proj, upserts, keepKeys, scannedRepos, true); err != nil {
		l.Error(err, "mrScan: persist scan marks", "action", "scan_mr", "resource_id", proj.Name)
	}
	r.Metrics.ObserveScanDuration("mrScan", time.Since(start).Seconds())
	l.Info("mrScan complete", "action", "scan_mr", "resource_id", proj.Name,
		"listed", len(cands), "picked", created, "duration_ms", time.Since(start).Milliseconds())
	return deferred > 0
}

// issueScan lists open issues (per-repo + board) and enqueues QueuedEvents.
// Returns (backlog, issueCache): backlog=true only when a candidate's enqueue
// transiently failed (a genuine retriable deferral warranting the 60s re-fire);
// terminal skips (out-of-scope, dedup, bot-last-word, stale-reapable, no-human)
// do NOT set it. issueCache holds the per-repo slices fetched this cycle so
// recoverOrphans can reuse them without a second ListOpenIssues round-trip per
// repo (finding 4).
func (r *ProjectReconciler) issueScan(ctx context.Context, proj *tatarav1alpha1.Project, reader scm.SCMReader, repos []tatarav1alpha1.Repository, existing []tatarav1alpha1.Task, act tatarav1alpha1.CronActivity) (bool, map[string][]scm.IssueRef) {
	l := log.FromContext(ctx)
	start := time.Now()
	issueCache := make(map[string][]scm.IssueRef)
	seen := map[string]bool{}
	var cands []candidate
	addUnique := func(cs []candidate) {
		for _, c := range cs {
			key := fmt.Sprintf("%s#%d", c.repo, c.number)
			if seen[key] {
				continue
			}
			seen[key] = true
			cands = append(cands, c)
		}
	}
	for i := range repos {
		owner, name, err := scm.OwnerRepo(repos[i].Spec.URL)
		if err != nil {
			continue
		}
		iss, err := reader.ListOpenIssues(ctx, owner, name)
		if err != nil {
			l.Error(err, "scan: ListOpenIssues", "action", "scan_list_error", "resource_id", proj.Name, "activity", "issueScan", "repo", repos[i].Name)
			continue
		}
		issueCache[owner+"/"+name] = iss
		addUnique(candidatesFromIssues(iss))
	}
	if proj.Spec.Scm.Board != nil {
		board := boardRefFromSpec(proj.Spec.Scm)
		items, err := reader.ListBoardItems(ctx, board)
		if err != nil {
			l.Error(err, "scan: ListBoardItems", "action", "scan_list_error", "resource_id", proj.Name, "activity", "issueScan")
		} else {
			addUnique(candidatesFromBoard(items))
		}
	}
	for range cands {
		r.Metrics.ScanItem("issueScan", "scanned")
	}
	// Reporter intake gate (issue #102): drop candidates authored by accounts
	// outside the per-repo/per-project reporter allowlist so injected issues never
	// become tasks (and never reactivate a conversation below). Board candidates
	// carry no author and are board-curated, so they pass. An empty allowlist
	// preserves the open default.
	if len(cands) > 0 {
		var gated []candidate
		for _, c := range cands {
			if c.author != "" {
				if repo, ok := r.matchRepoForSlug(repos, c.repo); ok &&
					!tatarav1alpha1.IsAllowedReporter(proj, &repo, c.author) {
					r.Metrics.ScanItem("issueScan", "skipped_unauthorized")
					continue
				}
			}
			gated = append(gated, c)
		}
		cands = gated
	}
	// Reactivation pass: when an issue was updated after the bound lifecycle
	// Task's LastActivityAt (missed webhook), reset the Task to Triage instead
	// of creating a duplicate. This runs before dedup so the reactivated task
	// absorbs the candidate and the dedup check below skips it normally.
	botLogin := ""
	if proj.Spec.Scm != nil {
		botLogin = proj.Spec.Scm.BotLogin
	}
	// cc memoizes ListIssueComments for the rest of this scan cycle: the
	// reactivation, dedup, adoption, fresh-creation, bot-last-word and
	// brainstorm-churn gates below each independently ask about the same issue's
	// comments, and without this cache each one refetches over the SCM API.
	cc := newIssueCommentCache(reader)
	for _, c := range cands {
		task := findConvTaskToReactivate(ctx, c, existing, cc, botLogin)
		if task == nil {
			continue
		}
		now := metav1.Now()
		idleMinutes := 60
		if proj.Spec.Scm != nil && proj.Spec.Scm.ConversationIdleMinutes > 0 {
			idleMinutes = proj.Spec.Scm.ConversationIdleMinutes
		}
		deadline := metav1.NewTime(now.Add(time.Duration(idleMinutes) * time.Minute))
		err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
			if fetchErr := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, task); fetchErr != nil {
				return fetchErr
			}
			task.Status.DeployState = "Triage"
			task.Status.LastActivityAt = &now
			task.Status.DeadlineAt = &deadline
			return r.Status().Update(ctx, task)
		})
		if err != nil {
			l.Error(err, "issueScan: reactivate conversation task", "action", "reactivate_conv", "resource_id", task.Name)
			r.Metrics.ScanItem("issueScan", "reactivate_error")
			continue
		}
		l.Info("issueScan: reactivated conversation task", "action", "reactivate_conv", "resource_id", task.Name,
			"issue", fmt.Sprintf("%s#%d", c.repo, c.number))
		r.Metrics.ScanItem("issueScan", "reactivated")
	}

	// Dedup BEFORE enqueue so a stale-but-in-flight item is not re-created.
	managed := managedPhaseLabels(proj.Spec.Scm)
	brainstorming, approved, implementation, declined := lifecycleLabels(proj.Spec.Scm)
	gate := r.humanActivityGate(ctx, cc, botLogin)

	// Implement producer (clarify->implement handoff): an issue carrying
	// tatara-implementation with no live Task is a handed-off stream needing a
	// fresh implement Task. This runs BEFORE the dedup loop because isDeduped
	// treats the managed label + a terminal clarify Task as done and would
	// otherwise dead-end the handoff. createScanTask's kind-scoped dedup key
	// ("implement\x00<issueRef>") plus needsImplementProducer's Task check keep it
	// to one implement Task per episode.
	produced := map[string]bool{}
	// failedMark tracks candidates whose createScanTask call failed this cycle
	// (transient enqueue error, no RetryOnConflict), across both this producer
	// loop and the triage loop below. Their key is excluded from upserts in the
	// mark-collection block so the scan mark is NOT advanced - advancing it
	// would make the freshness gate skip the item forever despite the fast 60s
	// backlog retry, silently dropping it from ever getting its Task.
	failedMark := map[string]bool{}
	for _, c := range cands {
		if !needsImplementProducer(c, existing, implementation) {
			continue
		}
		repo, ok := r.matchRepoForSlug(repos, c.repo)
		if !ok {
			continue
		}
		// Stale-activity cutoff (issue #285): once an implement Task from a prior
		// handoff is GC'd, the issue still carries the implementation label and
		// re-satisfies needsImplementProducer, spawning a fresh agent pod for no
		// new activity. Skip when no new activity since the last accounted mark;
		// a freshly-added label bumps UpdatedAt past any prior mark and still fires.
		if !c.updatedAt.IsZero() {
			if m := lookupScanMark(proj.Status.ScanMarks, c.repo, c.number); m != nil && !c.updatedAt.After(m.AccountedAt.Time) {
				r.Metrics.ScanItem("issueScan", "skipped_stale_mark")
				l.Info("issueScan: skipped implement producer, no new activity since accounted mark",
					"action", "scan_issue", "resource_id", proj.Name, "repo", c.repo, "issue", c.number,
					"accounted_at", m.AccountedAt.Time, "updated_at", c.updatedAt)
				continue
			}
		}
		goal := fmt.Sprintf("Implement issue %s#%d", c.repo, c.number)
		created, cerr := r.createScanTask(ctx, proj, &repo, c, c, "issueScan", "implement", goal, nil, nil)
		if cerr != nil {
			l.Error(cerr, "issueScan: enqueue implement producer event", "action", "scan_issue",
				"resource_id", proj.Name, "repo", repo.Name, "issue", fmt.Sprintf("%s#%d", c.repo, c.number))
			failedMark[scanMarkKey(c.repo, c.number)] = true
			continue
		}
		produced[fmt.Sprintf("%s#%d", c.repo, c.number)] = true
		if created {
			r.Metrics.ScanItem("issueScan", "implement_produced")
			l.Info("issueScan: produced implement Task from clarify->implement handoff",
				"action", "scan_issue", "resource_id", proj.Name, "issue", fmt.Sprintf("%s#%d", c.repo, c.number))
		}
	}

	var eligible []candidate
	for _, c := range cands {
		if produced[fmt.Sprintf("%s#%d", c.repo, c.number)] {
			// The implement producer already claimed this candidate; do not also run
			// it through the issueLifecycle dedup/create path.
			continue
		}
		if isDeduped(c, existing, managed, gate) {
			r.Metrics.ScanItem("issueScan", "skipped_dedup")
		} else {
			eligible = append(eligible, c)
		}
	}
	// Systemic-group dedup: for issues carrying a tatara/systemic-<id> label and
	// having at least one sibling in the group, elect one lead per (sid, repo).
	// Non-lead siblings get a marker comment and no agent. Election runs on the
	// full candidate set (not the post-dedup eligible set) so a sibling stays
	// collapsed even when its lead is currently in-flight (deduped out): a higher-
	// numbered sibling must never be promoted to lead and spawn a second agent.
	systemicLeads := electSystemicLeads(cands, declined)
	created := 0
	deferred := 0
	for _, c := range eligible {
		picked, err := r.issueScanPickOne(ctx, proj, reader, repos, existing, c, systemicLeads, managed, brainstorming, approved, implementation, declined, botLogin, cc)
		if err != nil {
			deferred++
			failedMark[scanMarkKey(c.repo, c.number)] = true
			continue
		}
		if picked {
			created++
		}
	}
	// Persist per-item high-water marks (issue #285): record the observed
	// UpdatedAt for every candidate we scanned this cycle, and prune marks for
	// items no longer open in the repos we actually listed. Scoped to issues
	// (isPR=false); mrScan owns PR marks.
	scannedRepos := make(map[string]bool, len(issueCache))
	for slug := range issueCache {
		scannedRepos[slug] = true
	}
	keepKeys := make(map[string]bool, len(cands))
	upserts := make([]scanMarkUpsert, 0, len(cands))
	for _, c := range cands {
		k := scanMarkKey(c.repo, c.number)
		keepKeys[k] = true
		if failedMark[k] {
			continue // creation failed this cycle; do not advance the mark, so the fast retry re-attempts
		}
		upserts = append(upserts, scanMarkUpsert{repo: c.repo, number: c.number, updatedAt: c.updatedAt, isPR: false})
	}
	if err := r.persistScanMarks(ctx, proj, upserts, keepKeys, scannedRepos, false); err != nil {
		l.Error(err, "issueScan: persist scan marks", "action", "scan_issue", "resource_id", proj.Name)
	}
	r.Metrics.ObserveScanDuration("issueScan", time.Since(start).Seconds())
	l.Info("issueScan complete", "action", "scan_issue", "resource_id", proj.Name,
		"listed", len(cands), "picked", created, "duration_ms", time.Since(start).Milliseconds())
	return deferred > 0, issueCache
}

// issueScanPickOne runs the per-candidate decision body of issueScan for one
// eligible candidate c: systemic-sibling collapse, repo match, adoption,
// human-activity/bot-last-word/stale-reap gates, and task creation. Moved
// verbatim out of issueScan's loop body - every original `continue` became an
// explicit `return false, nil` at the same point (no gate reordered).
// systemicLeads is precomputed once per issueScan cycle by electSystemicLeads
// over the full (pre-dedup) candidate set and passed in unchanged, preserving
// the 2026-06-23 pre-dedup scoping fix.
func (r *ProjectReconciler) issueScanPickOne(
	ctx context.Context,
	proj *tatarav1alpha1.Project,
	reader scm.SCMReader,
	repos []tatarav1alpha1.Repository,
	existing []tatarav1alpha1.Task,
	c candidate,
	systemicLeads map[string]systemicDecision,
	managed []string,
	brainstorming, approved, implementation, declined string,
	botLogin string,
	cc *issueCommentCache,
) (bool, error) {
	l := log.FromContext(ctx)
	key := fmt.Sprintf("%s#%d", c.repo, c.number)
	if d, ok := systemicLeads[key]; ok && !d.isLead {
		// Collapsed sibling: no implementation agent. Mark idempotently and skip.
		if w, token, werr := r.scanWriter(ctx, proj); werr == nil {
			if cerr := commentSiblingMarker(ctx, reader, w, token, c.repo, c.number, d.leadNumber); cerr != nil {
				l.Error(cerr, "issueScan: systemic sibling marker comment", "action", "systemic_sibling_mark",
					"resource_id", proj.Name, "issue", key, "lead", d.leadNumber)
			}
		}
		r.Metrics.SystemicSiblingCollapsed(proj.Name)
		r.Metrics.ScanItem("issueScan", "skipped_systemic_sibling")
		l.Info("issueScan: collapsed systemic sibling (no separate agent)",
			"action", "systemic_dedup", "resource_id", proj.Name,
			"issue", key, "systemic_id", d.sid, "lead", d.leadNumber)
		return false, nil
	}
	repo, ok := r.matchRepoForSlug(repos, c.repo)
	if !ok {
		r.Metrics.ScanItem("issueScan", "skipped_norepo")
		return false, nil
	}
	// Adoption (B1): if an issueLifecycle Task already exists for this issue
	// (Parked from a false refusal, or otherwise live), re-enter it to Triage
	// instead of creating a duplicate. One Task per issue forever; the shared
	// pod/branch is intentional. Done/Stopped Tasks are excluded by the helper
	// so deliberately-closed issues still create fresh on new activity.
	//
	// Defect A gate: mirror findConvTaskToReactivate - only adopt when a HUMAN
	// comment arrived after the task's LastActivityAt. This prevents the
	// re-adoption loop where the same old comment (after CreationTimestamp but
	// before LastActivityAt) re-triggers adoption every cron cycle on a Parked
	// task. Fail-open (adopt) when LastActivityAt is nil (first adoption) or
	// when the SCM reader/botLogin/owner-split is unavailable.
	if adopt := hasLiveOrAdoptableTask(existing, c.repo, c.number); adopt != nil {
		if adopt.Status.LastActivityAt != nil {
			owner, name, cut := strings.Cut(c.repo, "/")
			if cut && reader != nil && botLogin != "" &&
				!humanCommentAfter(ctx, cc, owner, name, c.number, botLogin, adopt.Status.LastActivityAt.Time) {
				r.Metrics.ScanItem("issueScan", "skipped_no_human_activity")
				l.Info("issueScan: skipped adoption, no human activity since last activity",
					"action", "adopt_lifecycle", "resource_id", adopt.Name,
					"issue", fmt.Sprintf("%s#%d", c.repo, c.number),
					"last_activity_at", adopt.Status.LastActivityAt.Time)
				return false, nil
			}
		}
		if err := r.adoptLifecycleTask(ctx, proj, adopt); err != nil {
			l.Error(err, "issueScan: adopt existing lifecycle task",
				"action", "adopt_lifecycle", "resource_id", adopt.Name,
				"issue", fmt.Sprintf("%s#%d", c.repo, c.number))
			r.Metrics.ScanItem("issueScan", "adopt_error")
			// Adopt failure (Task already exists, just not re-entered) deliberately does NOT defer:
			// waiting for the next normal cycle is harmless and avoids a 60s loop on a
			// persistently un-adoptable Task. Only enqueue failures (no Task yet) defer.
			return false, nil
		}
		l.Info("issueScan: adopted existing lifecycle task (re-triage, no duplicate)",
			"action", "adopt_lifecycle", "resource_id", adopt.Name,
			"issue", fmt.Sprintf("%s#%d", c.repo, c.number))
		r.Metrics.ScanItem("issueScan", "adopted")
		return false, nil
	}
	// Stale-activity cutoff (durable high-water mark, issue #285): once we have
	// accounted for an item's activity, skip re-triage until it has genuinely
	// newer activity. This survives Task GC (unlike the terminal-Task gates
	// below), so a fresh restart does not re-spawn an agent for a long-handled
	// issue whose Task was reaped. First sight (no mark) and truly-new activity
	// fall through to the existing gates; the mark is (re)written for every
	// scanned candidate at the end of issueScan regardless of this decision.
	// A zero UpdatedAt (board/synthetic candidate) never gates.
	if !c.updatedAt.IsZero() {
		if m := lookupScanMark(proj.Status.ScanMarks, c.repo, c.number); m != nil && !c.updatedAt.After(m.AccountedAt.Time) {
			r.Metrics.ScanItem("issueScan", "skipped_stale_mark")
			l.Info("issueScan: skipped fresh task creation, no new activity since accounted mark",
				"action", "scan_issue", "resource_id", proj.Name,
				"issue", fmt.Sprintf("%s#%d", c.repo, c.number),
				"accounted_at", m.AccountedAt.Time, "updated_at", c.updatedAt)
			return false, nil
		}
	}
	// Human-activity gate on fresh creation (issue #105): when the only
	// matching Tasks are terminal and the issue has no managed phase label,
	// the bot's own write-back advances updatedAt and isDeduped lets the
	// candidate through, spawning a fresh Task every cron cycle on a dormant
	// issue. Mirror the reactivation gate: create only when a HUMAN comment
	// is newer than the last terminal Task. Fail open (create) when the
	// author cannot be read, preserving current behavior on read errors.
	if lt := lastTerminalNoLabelTask(c, existing, managed); lt != nil {
		owner, name, cut := strings.Cut(c.repo, "/")
		if cut && reader != nil && botLogin != "" &&
			!humanCommentAfter(ctx, cc, owner, name, c.number, botLogin, lt.CreationTimestamp.Time) {
			r.Metrics.ScanItem("issueScan", "skipped_no_human_activity")
			l.Info("issueScan: skipped fresh task creation, no human activity since last terminal task",
				"action", "scan_issue", "resource_id", proj.Name,
				"issue", fmt.Sprintf("%s#%d", c.repo, c.number),
				"last_terminal_task", lt.Name)
			return false, nil
		}
	}
	// Bot-last-word backstop (issue #188): even when no terminal Task gates this
	// candidate (e.g. the prior lifecycle Task was GC'd, so neither the adoption
	// nor the fresh-creation gate above fires), do not spawn a fresh agent when
	// tatara authored the most recent comment and no human has replied -
	// re-triaging would only re-post and complete, looping every cron cycle. A
	// human reply (a newer non-bot comment) clears the gate on the next scan.
	if botHadLastWord(ctx, cc, c, botLogin) {
		r.Metrics.ScanItem("issueScan", "skipped_bot_last_word")
		l.Info("issueScan: skipped fresh task creation, bot had the last word (awaiting human reply)",
			"action", "scan_issue", "resource_id", proj.Name,
			"issue", fmt.Sprintf("%s#%d", c.repo, c.number))
		return false, nil
	}
	// Source-of-churn gate (token conservation, component 5): a bot-authored
	// brainstorming proposal no human has engaged must not be re-triaged every
	// scan cycle. The reaper (staleProposalDays) only closes it once it is ALSO
	// stale; this stops the churn from the first cycle. Any human comment (zero
	// `since` = ever) clears it. Fail-open when SCM/botLogin/owner-split is
	// unavailable, matching botHadLastWord and the reactivation gate.
	//
	// A prior version also treated issue UpdatedAt-after-CreatedAt (with no
	// comment) as human engagement, as a fallback for edits/reactions the SCM
	// reader cannot see directly. That heuristic is author-blind: bot-side
	// mutations bump UpdatedAt too (e.g. setLifecycleLabel reasserting the
	// brainstorming label when a triage reverts to awaiting-approval, see
	// lifecycle.go's "triage-await-approval" arm), so a proposal nobody
	// touched could read as human-engaged and get re-triaged - the exact churn
	// this gate exists to suppress. Comments are the only reader-visible
	// signal that reliably distinguishes a human actor, so that is the whole
	// gate now.
	if isBotBrainstormProposal(c, brainstorming, approved, implementation, declined, botLogin) {
		owner, name, cut := strings.Cut(c.repo, "/")
		if cut && reader != nil && botLogin != "" &&
			!humanBrainstormEngagement(ctx, cc, owner, name, c, botLogin) {
			r.Metrics.ScanItem("issueScan", "skipped_brainstorm_no_human")
			l.Info("issueScan: skipped fresh task creation, brainstorming proposal awaiting human engagement",
				"action", "scan_issue", "resource_id", proj.Name,
				"issue", fmt.Sprintf("%s#%d", c.repo, c.number))
			return false, nil
		}
	}
	if r.reapEligible(proj, scm.IssueRef{Repo: c.repo, Number: c.number, Author: c.author, Labels: c.labels, UpdatedAt: c.updatedAt, IsPR: c.isPR}, existing) {
		r.Metrics.ScanItem("issueScan", "skipped_stale_reapable")
		l.Info("issueScan: skipped fresh task creation, proposal stale+unengaged (reaper will close)",
			"action", "scan_issue", "resource_id", proj.Name,
			"issue", fmt.Sprintf("%s#%d", c.repo, c.number))
		return false, nil
	}
	goal := fmt.Sprintf("Triage issue %s#%d", c.repo, c.number)
	var sg *tatarav1alpha1.SystemicGroup
	if d, ok := systemicLeads[key]; ok && d.isLead && len(d.sameRepoSiblings) > 0 {
		sg = &tatarav1alpha1.SystemicGroup{SystemicID: d.sid, SameRepoSiblings: d.sameRepoSiblings, CrossRepo: d.crossRepo}
		r.Metrics.SystemicGroupLed(proj.Name)
		l.Info("issueScan: systemic group lead", "action", "systemic_dedup", "resource_id", proj.Name,
			"issue", key, "systemic_id", d.sid, "same_repo_siblings", len(d.sameRepoSiblings), "cross_repo", len(d.crossRepo))
	}
	ok2, err := r.createScanTask(ctx, proj, &repo, c, c, "issueScan", "issueLifecycle", goal, nil, sg)
	if err != nil {
		l.Error(err, "scan: enqueue issueScan event", "resource_id", proj.Name, "repo", repo.Name)
		r.Metrics.ScanItem("issueScan", "create_error")
		return false, err
	}
	if ok2 {
		r.Metrics.ScanItem("issueScan", "picked")
	}
	return ok2, nil
}

// brainstorm runs one brainstorm cycle at PROJECT scope: at most one brainstorm
// QueuedEvent per cycle for the whole project. BrainstormActivity.MaxPerCycle is
// deprecated and ignored; the hard cap of one per cycle is enforced here.
// Concurrency is bounded solely by the dispatcher's QueueCapacity.
func (r *ProjectReconciler) brainstorm(ctx context.Context, proj *tatarav1alpha1.Project, reader scm.SCMReader, repos []tatarav1alpha1.Repository, existing []tatarav1alpha1.Task, act tatarav1alpha1.BrainstormActivity) {
	l := log.FromContext(ctx)
	start := time.Now()
	maxProp := act.MaxOpenProposals
	if maxProp < 1 {
		maxProp = 10
	}

	// Project-scoped in-flight guard: any non-terminal brainstorm Task blocks.
	if brainstormInFlightProject(existing) {
		r.Metrics.ScanItem("brainstorm", "skipped_inflight")
		l.Info("brainstorm: in-flight project brainstorm task; skipping cycle",
			"action", "scan_brainstorm", "resource_id", proj.Name)
		r.Metrics.ObserveScanDuration("brainstorm", time.Since(start).Seconds())
		return
	}

	brainstormingLabel, _, _, _ := lifecycleLabels(proj.Spec.Scm)

	// Deterministic primary repo: sort by name, first valid slug wins.
	sortedRepos := make([]tatarav1alpha1.Repository, len(repos))
	copy(sortedRepos, repos)
	sort.Slice(sortedRepos, func(i, j int) bool {
		return sortedRepos[i].Name < sortedRepos[j].Name
	})

	legacyIdea, _ := legacyLabels(proj.Spec.Scm)

	// no-valid-repos is checked before at-cap here (2026-06-13 flooding-incident
	// ordering); healthCheck checks the opposite order below.
	r.runProjectScopedProposalCycle(ctx, proj, reader, sortedRepos, existing,
		brainstormingLabel, legacyIdea, maxProp, "brainstorm", "scan_brainstorm", start, act.Sources,
		false, brainstormGoalProject, r.createBrainstormTask)
}

// runProjectScopedProposalCycle runs the shared 90%-identical middle of
// brainstorm() and healthCheck(): resolve per-repo slugs, accumulate the
// proposal backlog (SCM issue count, capped by ledgerTotal via the caller),
// gather CI state, build the rich repo-state context, build the activity goal
// text, and create the scan task - emitting the same log fields and metric
// calls both callers previously duplicated.
//
// Cap source (P4, migration-safe): the proposal backlog is the MAX of the
// ledger count (proposalBacklogFromTasks over role:proposed entries) and the
// per-repo SCM-issue count. During the migration window some open proposals are
// ledgered (role:proposed) while others live only as SCM brainstorming issues;
// taking the max means the cap can only over-count (safe, throttles) and never
// under-count (which would silently flood). The SCM count must always run -
// gating it on a project-wide "any task has a ledger" flag was wrong: a Task
// with only a role:source seed has a ledger but contributes nothing to the
// proposal count, so that gate zeroed the SCM backlog and bypassed the cap.
//
// The two post-loop early-return guards (no-valid-repos / at-cap) are checked
// in checkCapFirst order: brainstorm checks no-valid-repos first, healthCheck
// checks at-cap first. This order is preserved verbatim per caller (do not let
// this helper pick a single order - it touches the 2026-06-13 flooding-
// incident path).
func (r *ProjectReconciler) runProjectScopedProposalCycle(
	ctx context.Context,
	proj *tatarav1alpha1.Project,
	reader scm.SCMReader,
	sortedRepos []tatarav1alpha1.Repository,
	existing []tatarav1alpha1.Task,
	brainstormingLabel, legacyIdea string,
	maxProp int,
	activityLabel, scanAction string,
	start time.Time,
	sources []string,
	checkCapFirst bool,
	goalBuilder func(slugs []string, repoStateCtx, guidance string) string,
	taskCreator func(ctx context.Context, proj *tatarav1alpha1.Project, goal string, sources []string) (bool, error),
) {
	l := log.FromContext(ctx)
	issuesBySlug := make(map[string][]scm.IssueRef)
	ledgerTotal := proposalBacklogFromTasks(existing)
	scmTotal := 0
	scmAtCap := false
	var slugs []string
	for i := range sortedRepos {
		rp := &sortedRepos[i]
		slug := repoSlug(rp)
		if slug == "" {
			continue
		}
		slugs = append(slugs, slug)
		if scmAtCap {
			// SCM backlog already at cap; skip the issue list for remaining repos
			// (best-effort: their per-repo gauge keeps last cycle's value). Still
			// collect slugs for the goal text.
			continue
		}
		owner, name, err := scm.OwnerRepo(rp.Spec.URL)
		if err != nil {
			continue
		}
		iss, err := reader.ListOpenIssues(ctx, owner, name)
		if err != nil {
			l.Info(activityLabel+": backlog count failed (non-fatal)", "resource_id", proj.Name, "repo", rp.Name, "err", err.Error())
			continue
		}
		issuesBySlug[slug] = iss
		backlog := proposalBacklogCount(iss, brainstormingLabel, legacyIdea)
		r.Metrics.SetOpenProposals(slug, float64(backlog))
		scmTotal += backlog
		if scmTotal >= maxProp {
			scmAtCap = true
		}
	}
	total := scmTotal
	if ledgerTotal > total {
		total = ledgerTotal
	}
	atCap := total >= maxProp
	noValidRepos := len(slugs) == 0

	noValidReposGuard := func() bool {
		if !noValidRepos {
			return false
		}
		l.Info(activityLabel+": no valid repos", "resource_id", proj.Name)
		r.Metrics.ObserveScanDuration(activityLabel, time.Since(start).Seconds())
		return true
	}
	atCapGuard := func() bool {
		if !atCap {
			return false
		}
		r.Metrics.ScanItem(activityLabel, "skipped_cap")
		l.Info(activityLabel+": project backlog at cap; skipping cycle",
			"action", scanAction, "resource_id", proj.Name, "total", total, "cap", maxProp)
		r.Metrics.ObserveScanDuration(activityLabel, time.Since(start).Seconds())
		return true
	}

	if checkCapFirst {
		if atCapGuard() {
			return
		}
		if noValidReposGuard() {
			return
		}
	} else {
		if noValidReposGuard() {
			return
		}
		if atCapGuard() {
			return
		}
	}

	// Build PR / main-CI data (bounded + non-fatal) for the rich repo-state context.
	prsBySlug, prCIBySlug, mainCIBySlug := r.gatherRepoCIState(ctx, proj, reader, sortedRepos, activityLabel)

	// Build rich context from already-fetched data + bounded MR/main reads.
	issuesCtx := r.buildRepoStateContext(ctx, proj, reader, issuesBySlug, prsBySlug, prCIBySlug, mainCIBySlug, sortedRepos)

	goal := goalBuilder(slugs, issuesCtx, scmGuidance(proj))
	created, err := taskCreator(ctx, proj, goal, sources)
	if err != nil {
		l.Error(err, "scan: enqueue "+activityLabel+" event", "resource_id", proj.Name)
		r.Metrics.ObserveScanDuration(activityLabel, time.Since(start).Seconds())
		return
	}
	if created {
		r.Metrics.ScanItem(activityLabel, "picked")
	}
	r.Metrics.ObserveScanDuration(activityLabel, time.Since(start).Seconds())
	l.Info(activityLabel+" complete", "action", scanAction, "resource_id", proj.Name,
		"picked", 1, "duration_ms", time.Since(start).Milliseconds())
}

// appendGuidance appends a PROJECT CHARTER block when guidance is non-empty.
func appendGuidance(goal, guidance string) string {
	if strings.TrimSpace(guidance) == "" {
		return goal
	}
	return goal + "\n\nPROJECT CHARTER: " + guidance
}

// scmGuidance returns the Guidance field from a Project's Scm spec, nil-safe.
func scmGuidance(proj *tatarav1alpha1.Project) string {
	if proj.Spec.Scm == nil {
		return ""
	}
	return proj.Spec.Scm.Guidance
}

// brainstormGoalProject returns the turn-0 goal for a project-level brainstorm
// task. repoStateCtx is the rich three-block string built by buildRepoStateContext
// (ISSUES / OPEN MRs / MAIN HEALTH). When empty a fallback note is substituted.
func brainstormGoalProject(slugs []string, repoStateCtx string, guidance string) string {
	repoList := strings.Join(slugs, ", ")
	stateBlock := "No live repo state available."
	if repoStateCtx != "" {
		stateBlock = repoStateCtx
	}
	goal := "Invoke the `tatara-council-brainstorm` skill FIRST and follow its seven-lens phases in " +
		"order; it owns the whole turn and emits the single terminal action itself (`propose_issue`, or " +
		"`skip_research` when nothing clears the bar or the idea duplicates an open issue), grounded per " +
		"the `tatara-code-quality-proposal` skill.\n\n" +
		"HANDOFF CONTINUATION (do this FIRST): call `list_handoffs` for this project. For each open handoff that " +
		"still describes live, unfinished work, call `get_handoff` and propose continuing it (a `propose_issue` framed " +
		"as resuming that work) before generating fresh ideas. Skip stale/superseded/delivered handoffs. Continuation " +
		"proposals count against the same MaxOpenProposals cap as fresh ideas.\n\n" +
		"MANDATE: propose the highest-leverage code-quality, simplification, or robustness improvement across ALL " +
		"repositories: " + repoList + ". Ground every claim in REAL code.\n\n" +
		"READ REAL CODE (two signals, use both): (1) every listed repo is shallow-cloned read-only into " +
		"`workspace/<owner>/<repo>` - open the actual source, configs, and tests; (2) the code-graph MCP tools " +
		"(`code_search`, `code_explain`, `code_related`, `code_important`, `code_cross_repo`, `code_bridges`, " +
		"`code_communities`) index every enrolled repo - use them for the whole-project map, then open the on-disk " +
		"files they point at to confirm before proposing. See the `tatara-code-quality-proposal` skill.\n\n" +
		stateBlock + "\n\n" +
		"EARLY EXIT (do this FIRST, cheaply): scan the ISSUES / OPEN MRs / MAIN HEALTH state above. If nothing clears " +
		"the bar for a genuinely novel, high-leverage proposal this cycle, call `skip_research(reason)` and STOP. " +
		"Silence over noise.\n\n" +
		"SYSTEMIC MANDATE: prefer a single systemic improvement (a pattern spanning >=2 repositories, a platform-wide " +
		"gap, or recurring debt) over a one-repo tweak. Decompose: dispatch one parallel subagent per repository, then " +
		"synthesize one systemic conclusion.\n\n" +
		"NEW-IDEAS-ONLY CONTRACT - follow exactly ONE path:\n" +
		"1. If the best idea DUPLICATES an existing open issue above: do NOT propose. Finish with a one-line note " +
		"naming the duplicate. Do NOT comment on it.\n" +
		"2. If genuinely novel AND standalone: call `propose_issue`. Set `repo` to the owning repository. Required " +
		"body shape: (a) a one-paragraph problem statement citing the concrete file/symbol you read; (b) a " +
		"DECOMPOSITION into sub-problems; (c) for EACH sub-problem, 2-3 concrete OPTIONS with one-line tradeoffs and " +
		"your recommended pick; (d) the maintainer's decision framed as choosing one option per sub-problem. No flat " +
		"list of open questions.\n\n" +
		"ACTION RULE: a one-repo improvement emits exactly ONE propose_issue. A genuinely systemic improvement MAY " +
		"emit one propose_issue per affected repository (bounded: at most 6), all sharing a single `systemicId` you " +
		"generate. State which path and scope you chose before executing. You are a read-only proposer: never " +
		"implement, never push, never open a PR."
	return appendGuidance(goal+platformProblemGuidance+toolingNoteGuidance, guidance)
}

// healthCheckGoalProject returns the turn-0 goal for a project-level health-check
// task. It mirrors brainstormGoalProject (same dedup-first contract and repoStateCtx
// shape) but drives the tatara-health-check skill across all repo slugs.
func healthCheckGoalProject(slugs []string, repoStateCtx string, guidance string) string {
	repoList := strings.Join(slugs, ", ")

	stateBlock := "No live repo state available."
	if repoStateCtx != "" {
		stateBlock = repoStateCtx
	}

	goal := "Invoke the `tatara-health-check` skill to survey the HEALTH of the project's repositories " +
		"and identify the highest-leverage health issue across ALL repositories: " + repoList + ". " +
		"The skill defines the five health dimensions (CI failures, code coverage gaps, code to simplify, " +
		"CI/CD pipeline steps worth adding, other tech-debt), how to gather evidence (on-disk CI config, an " +
		"actual test/lint run, and the tatara-memory code graph), score leverage, and dedup. " +
		"Run at MAXIMUM reasoning effort. Decompose the survey: dispatch one parallel subagent per repository " +
		"(use the Agent/Workflow tools to fan out, then synthesize their findings into one systemic conclusion). " +
		"\n\n" + stateBlock + "\n\n" +
		"SYSTEMIC MANDATE: prefer the single highest-leverage systemic health gap - a pattern spanning " +
		">=2 repositories (e.g. missing test coverage everywhere, CI flakiness across repos) - " +
		"over a one-repo tweak. Survey the ISSUES, OPEN MRs, and MAIN HEALTH blocks above.\n\n" +
		"DEDUP RULE - you MUST follow exactly ONE of these three paths, in order:\n" +
		"1. If the best finding DUPLICATES an existing open issue listed above: do NOT call propose_issue. " +
		"Finish with a one-line note naming the duplicate (e.g. 'Duplicate of o/repo#N').\n" +
		"2. If the best finding is a sub-aspect or connecting improvement TO an existing issue " +
		"that is NOT marked [bot-engaged]: call comment_on_issue(repo, number, body) on that issue. " +
		"Do NOT call propose_issue.\n" +
		"   An issue marked [bot-engaged] already has your comment - do NOT comment again on it. " +
		"Prefer a NEW finding instead: a genuinely novel standalone issue (path 3, in ANY repo or " +
		"project-wide), or a comment on a DIFFERENT issue that is not [bot-engaged]. Never comment " +
		"twice on the same issue.\n" +
		"3. ONLY if the finding is genuinely novel AND standalone (no existing issue covers it): " +
		"call propose_issue. " +
		"Set the `repo` argument to the specific repository that should own the issue. " +
		"The proposal must be self-contained: the concrete defect with file:line evidence, the proposed fix, " +
		"and a single explicit decision for the human (approve to implement or comment to refine). " +
		"Do NOT produce a list of open questions or ask for input.\n\n" +
		"ACTION RULE: a one-repo finding emits exactly ONE propose_issue. A genuinely systemic " +
		"health gap MAY emit one propose_issue per affected repository (bounded: at most 6), all sharing " +
		"a single `systemicId` string you generate. State which path and scope you chose before executing."
	return appendGuidance(goal+platformProblemGuidance+toolingNoteGuidance, guidance)
}

// gatherRepoCIState fetches open PRs, per-PR CI (bounded to the first 20 PRs),
// and main-branch CI for each repo in sortedRepos. activity is a log prefix
// ("brainstorm" or "healthCheck"). For GitLab repos the CI owner is the full
// project path (URL-encoded by the gitlab client), matching the pattern already
// used by lifecycle.go for main-CI and GetCommitCIStatus. All errors are
// non-fatal; missing data degrades to empty/unknown in the returned maps.
func (r *ProjectReconciler) gatherRepoCIState(
	ctx context.Context,
	proj *tatarav1alpha1.Project,
	reader scm.SCMReader,
	sortedRepos []tatarav1alpha1.Repository,
	activity string,
) (prsBySlug map[string][]scm.PRRef, prCIBySlug map[string]map[int]string, mainCIBySlug map[string]string) {
	l := log.FromContext(ctx)
	prsBySlug = map[string][]scm.PRRef{}
	prCIBySlug = map[string]map[int]string{}
	mainCIBySlug = map[string]string{}
	isGitLab := proj.Spec.Scm != nil && proj.Spec.Scm.Provider == "gitlab"
	for i := range sortedRepos {
		rp := &sortedRepos[i]
		slug := repoSlug(rp)
		if slug == "" {
			continue
		}
		owner, name, err := scm.OwnerRepo(rp.Spec.URL)
		if err != nil {
			continue
		}
		// Resolve provider-correct owner/repo for CI lookups.
		ciOwner, ciRepo := owner, name
		if isGitLab {
			if pp, perr := scm.GitLabProjectPath(rp.Spec.URL); perr == nil {
				ciOwner = pp
				ciRepo = ""
			}
		}
		if prs, perr := reader.ListOpenPRs(ctx, owner, name); perr == nil {
			prsBySlug[slug] = prs
			ci := map[int]string{}
			const prCILimit = 20
			for j, pr := range prs {
				if j >= prCILimit {
					break
				}
				if pr.HeadSHA != "" {
					if st, serr := reader.GetCommitCIStatus(ctx, ciOwner, ciRepo, pr.HeadSHA); serr == nil {
						ci[pr.Number] = st
					}
				}
			}
			prCIBySlug[slug] = ci
		} else {
			l.Info(activity+": list open PRs failed (non-fatal)", "resource_id", proj.Name, "repo", rp.Name, "err", perr.Error())
		}
		if sha, serr := reader.GetDefaultBranchHeadSHA(ctx, ciOwner, ciRepo); serr == nil && sha != "" {
			if st, cerr := reader.GetCommitCIStatus(ctx, ciOwner, ciRepo, sha); cerr == nil {
				mainCIBySlug[slug] = st
			}
		} else if serr != nil {
			l.Info(activity+": main head sha failed (non-fatal)", "resource_id", proj.Name, "repo", rp.Name, "err", serr.Error())
		}
	}
	return
}

// buildRepoStateContext builds the rich context string embedded in the brainstorm
// / healthCheck goal. It emits three blocks: ISSUES (pre-fetched, cap 60),
// OPEN MRs (from prsBySlug, cap 40, per-PR CI from prCIBySlug), and MAIN HEALTH
// (one line per repo from mainCIBySlug). All maps are caller-built and may be nil
// (degrade gracefully).
const maxIssuesContext = 60
const maxMRsContext = 40

func (r *ProjectReconciler) buildRepoStateContext(ctx context.Context, proj *tatarav1alpha1.Project, reader scm.SCMReader, issuesBySlug map[string][]scm.IssueRef, prsBySlug map[string][]scm.PRRef, prCIBySlug map[string]map[int]string, mainCIBySlug map[string]string, repos []tatarav1alpha1.Repository) string {
	l := log.FromContext(ctx)
	botLogin := ""
	provider := ""
	if proj.Spec.Scm != nil {
		botLogin = proj.Spec.Scm.BotLogin
		provider = proj.Spec.Scm.Provider
	}

	// ISSUES block.
	var issueLines []string
	issueTotal := 0
	for i := range repos {
		owner, name, err := scm.OwnerRepo(repos[i].Spec.URL)
		if err != nil {
			continue
		}
		slug := owner + "/" + name
		issues := issuesBySlug[slug]
		for _, iss := range issues {
			if iss.IsPR {
				continue
			}
			if len(issueLines) >= maxIssuesContext {
				issueTotal++
				continue
			}
			issueTotal++
			labels := strings.Join(iss.Labels, ",")
			title := strings.ReplaceAll(strings.ReplaceAll(iss.Title, "\n", " "), "\r", "")
			line := fmt.Sprintf("%s#%d [%s] %s", slug, iss.Number, labels, title)
			if botCommentedOnIssue(ctx, reader, owner, name, iss.Number, botLogin) {
				line += " [bot-engaged]"
			}
			issueLines = append(issueLines, line)
		}
	}
	omitted := issueTotal - len(issueLines)
	issuesBlock := strings.Join(issueLines, "\n")
	if omitted > 0 {
		issuesBlock += fmt.Sprintf("\n(+%d more omitted)", omitted)
		l.Info("brainstorm: buildRepoStateContext: capped issues context",
			"shown", len(issueLines), "omitted", omitted)
	}

	// OPEN MRs block: provider-correct separator (! for gitlab, # for github).
	mrSep := "#"
	if provider == "gitlab" {
		mrSep = "!"
	}
	var mrLines []string
	for i := range repos {
		owner, name, err := scm.OwnerRepo(repos[i].Spec.URL)
		if err != nil {
			continue
		}
		slug := owner + "/" + name
		prs := prsBySlug[slug]
		ciMap := prCIBySlug[slug]
		for _, pr := range prs {
			if len(mrLines) >= maxMRsContext {
				break
			}
			ciStatus := "unknown"
			if ciMap != nil {
				if st, ok := ciMap[pr.Number]; ok && st != "" {
					ciStatus = st
				}
			}
			title := ""
			if pr.Body != "" {
				title = firstLine(pr.Body)
			}
			mrLines = append(mrLines, fmt.Sprintf("%s%s%d [ci:%s] %s", slug, mrSep, pr.Number, ciStatus, title))
		}
	}

	// MAIN HEALTH block: one line per repo.
	var healthLines []string
	for i := range repos {
		owner, name, err := scm.OwnerRepo(repos[i].Spec.URL)
		if err != nil {
			continue
		}
		slug := owner + "/" + name
		status := "unknown"
		if mainCIBySlug != nil {
			if st, ok := mainCIBySlug[slug]; ok && st != "" {
				status = st
			}
		}
		healthLines = append(healthLines, fmt.Sprintf("%s main CI: %s", slug, status))
	}

	var sb strings.Builder
	sb.WriteString("ISSUES:\n")
	if issuesBlock != "" {
		sb.WriteString(issuesBlock)
	}
	sb.WriteString("\n\nOPEN MRs:\n")
	sb.WriteString(strings.Join(mrLines, "\n"))
	sb.WriteString("\n\nMAIN HEALTH:\n")
	sb.WriteString(strings.Join(healthLines, "\n"))
	return sb.String()
}

// humanCommentAfter reports whether the issue has a comment authored by a
// non-bot (a human) with CreatedAt strictly after `since`. On a read error it
// returns true (fail-open: the caller reactivates, preserving the missed-webhook
// recovery the reactivation gate exists for; the discuss/close silence gate makes
// an over-eager reactivation a silent no-op).
func humanCommentAfter(ctx context.Context, reader scm.SCMReader, owner, name string, number int, botLogin string, since time.Time) bool {
	comments, err := reader.ListIssueComments(ctx, owner, name, number)
	if err != nil {
		return true
	}
	return humanCommentInSlice(comments, botLogin, since)
}

// humanCommentInSlice is the pure predicate half of humanCommentAfter, split out
// so callers holding an already-fetched (e.g. cached) comment slice can reuse it
// without a second SCM read.
func humanCommentInSlice(comments []scm.IssueComment, botLogin string, since time.Time) bool {
	for _, c := range comments {
		if c.Author != "" && c.Author != botLogin && c.CreatedAt.After(since) {
			return true
		}
	}
	return false
}

// issueCommentCache memoizes ListIssueComments per (owner,name,number) for the
// lifetime of one scan cycle. Several gates in issueScan (adoption,
// fresh-creation, bot-last-word, brainstorm-churn) each independently ask "has a
// human commented on this issue"; without memoization every one of them fetches
// comments over the SCM API for the same issue in the same cycle. Embeds the real
// reader so it satisfies scm.SCMReader for every other method unchanged, and
// implements scm.PRCommentLister as an uncached passthrough (PR/MR conversation
// reads are outside the doubling this cache targets).
type issueCommentCache struct {
	scm.SCMReader
	m map[string][]scm.IssueComment
}

func newIssueCommentCache(reader scm.SCMReader) *issueCommentCache {
	return &issueCommentCache{SCMReader: reader, m: make(map[string][]scm.IssueComment)}
}

func (cc *issueCommentCache) ListIssueComments(ctx context.Context, owner, name string, number int) ([]scm.IssueComment, error) {
	key := fmt.Sprintf("%s/%s#%d", owner, name, number)
	if v, ok := cc.m[key]; ok {
		return v, nil
	}
	comments, err := cc.SCMReader.ListIssueComments(ctx, owner, name, number)
	if err != nil {
		// Not cached: a transient read error should not poison the rest of the
		// cycle's gates with a permanent miss.
		return nil, err
	}
	cc.m[key] = comments
	return comments, nil
}

func (cc *issueCommentCache) ListPRComments(ctx context.Context, owner, name string, number int) ([]scm.IssueComment, error) {
	if pl, ok := cc.SCMReader.(scm.PRCommentLister); ok {
		return pl.ListPRComments(ctx, owner, name, number)
	}
	return cc.ListIssueComments(ctx, owner, name, number)
}

// humanBrainstormEngagement reports whether a bot-authored brainstorm proposal
// has seen any human engagement: a human comment, ever. Comments are the only
// reader-visible signal that reliably distinguishes a human actor from a bot
// write-back (see the caller's comment for why an UpdatedAt-based fallback was
// removed). Comments are fetched via cc so this shares the single per-cycle
// read with the other gates. Fail-open (true) on a comment-fetch error,
// matching humanCommentAfter.
func humanBrainstormEngagement(ctx context.Context, cc *issueCommentCache, owner, name string, c candidate, botLogin string) bool {
	comments, err := cc.ListIssueComments(ctx, owner, name, c.number)
	if err != nil {
		return true
	}
	return humanCommentInSlice(comments, botLogin, time.Time{})
}

// humanActivityGate returns the isDeduped human-activity predicate for a scan
// cycle: reports whether the candidate's issue saw a non-bot comment strictly
// after `since`. Fail-open (true) when the repo slug cannot be split or the
// reader/botLogin are unavailable, matching humanCommentAfter and the
// reactivation gate. PR candidates have no issue comment timeline, so the
// predicate returns the legacy updatedAt comparison for them (isDeduped never
// reaches the gate for PRs, but keep it correct if called).
func (r *ProjectReconciler) humanActivityGate(ctx context.Context, reader scm.SCMReader, botLogin string) func(c candidate, since time.Time) bool {
	return func(c candidate, since time.Time) bool {
		if c.isPR {
			return c.updatedAt.After(since)
		}
		owner, name, ok := strings.Cut(c.repo, "/")
		if !ok || reader == nil || botLogin == "" {
			return true
		}
		return humanCommentAfter(ctx, reader, owner, name, c.number, botLogin, since)
	}
}

// botIsLastCommenter reports whether the newest comment (by CreatedAt) is
// authored by botLogin - tatara already had the last word and no one has spoken
// since. Newest-by-time is robust to SCM list ordering. No comments -> false.
func botIsLastCommenter(comments []scm.IssueComment, botLogin string) bool {
	newest := -1
	for i := range comments {
		if newest == -1 || comments[i].CreatedAt.After(comments[newest].CreatedAt) {
			newest = i
		}
	}
	return newest >= 0 && comments[newest].Author == botLogin
}

// botHadLastWord reports whether tatara authored the most recent comment on the
// candidate's issue/PR/MR conversation, i.e. the bot already responded and no
// human has replied since. It is the scan-time loop guard for issue #188: a scan
// must not re-spawn an agent when the only new activity is the bot's own comment.
// PR/MR timelines are read via PRCommentLister (GitLab MRs have a distinct notes
// endpoint; GitHub PR comments are issue comments) with a fallback to
// ListIssueComments for readers lacking the capability. Empty botLogin, an
// unsplittable repo, a nil reader, or a read error -> false (fail-open: do not
// suppress scheduling, matching humanCommentAfter and preserving missed-webhook
// recovery).
func botHadLastWord(ctx context.Context, reader scm.SCMReader, c candidate, botLogin string) bool {
	if botLogin == "" || reader == nil {
		return false
	}
	owner, name, ok := strings.Cut(c.repo, "/")
	if !ok {
		return false
	}
	var (
		comments []scm.IssueComment
		err      error
	)
	if c.isPR {
		if pl, okPL := reader.(scm.PRCommentLister); okPL {
			comments, err = pl.ListPRComments(ctx, owner, name, c.number)
		} else {
			comments, err = reader.ListIssueComments(ctx, owner, name, c.number)
		}
	} else {
		comments, err = reader.ListIssueComments(ctx, owner, name, c.number)
	}
	if err != nil {
		return false
	}
	return botIsLastCommenter(comments, botLogin)
}

// botCommentedOnIssue reports whether botLogin already authored a comment on the
// issue. Empty botLogin or any SCM read error -> false (best-effort flag; the
// commentOnIssue egress gate is the authoritative backstop).
func botCommentedOnIssue(ctx context.Context, reader scm.SCMReader, owner, name string, number int, botLogin string) bool {
	if botLogin == "" {
		return false
	}
	comments, err := reader.ListIssueComments(ctx, owner, name, number)
	if err != nil {
		return false
	}
	for _, c := range comments {
		if c.Author == botLogin {
			return true
		}
	}
	return false
}

// systemicMarker returns the idempotency marker + human-facing body for a
// collapsed sibling issue. The returned string is checked against existing
// comments by commentSiblingMarker (reconcile-safe).
func systemicMarker(lead int) string {
	return fmt.Sprintf("Tracked by #%d (systemic group). No separate agent.", lead)
}

// commentSiblingMarker posts the marker once. It is a no-op when a comment
// whose body contains the marker already exists (reconcile-safe).
func commentSiblingMarker(ctx context.Context, reader scm.SCMReader, writer scm.SCMWriter, token, repo string, number, lead int) error {
	owner, name, _ := strings.Cut(repo, "/")
	marker := systemicMarker(lead)
	if comments, err := reader.ListIssueComments(ctx, owner, name, number); err == nil {
		for _, c := range comments {
			if strings.Contains(c.Body, marker) {
				return nil
			}
		}
	}
	return writer.Comment(ctx, token, fmt.Sprintf("%s#%d", repo, number), marker)
}

// repoSlug returns "owner/name" for a Repository URL, or "" on error.
func repoSlug(repo *tatarav1alpha1.Repository) string {
	owner, name, err := scm.OwnerRepo(repo.Spec.URL)
	if err != nil {
		return ""
	}
	return owner + "/" + name
}

// brainstormInFlightProject reports whether ANY non-terminal brainstorm Task
// exists in the project (project-scoped guard, replaces per-repo check).
func brainstormInFlightProject(existing []tatarav1alpha1.Task) bool {
	for i := range existing {
		t := existing[i]
		if t.Labels[labelActivity] == "brainstorm" && !isTerminal(t.Status.Phase) {
			return true
		}
	}
	return false
}

// annDocRetries counts how many times the orphan sweep has reactivated a dropped
// documentation cycle, bounding retries at maxDocReactivations (liveness #7).
const annDocRetries = "tatara.dev/doc-retries"

// documentationInFlightProject reports whether ANY non-terminal documentation Task
// exists in the project. The overlap guard for the doc-sync cron (liveness #7):
// TaskTerminal (not isTerminal(Phase)) so a Parked doc task counts as terminal (it
// is re-swept separately by reactivateOrphanedDocTasks).
func documentationInFlightProject(existing []tatarav1alpha1.Task) bool {
	for i := range existing {
		t := &existing[i]
		if t.Labels[labelActivity] == "documentation" && !tatarav1alpha1.TaskTerminal(t) {
			return true
		}
	}
	return false
}

// reactivateOrphanedDocTasks re-sweeps dropped documentation cycles: a doc Task
// that Parked or Failed is reactivated (Phase/DeployState/park cleared, wrapper
// torn down) so the next reconcile re-runs it, up to maxDocReactivations attempts
// (after which it is left terminal for a human). Returns true when it reactivated
// at least one, so the caller treats a doc cycle as in-flight this tick (liveness
// finding #7).
func (r *ProjectReconciler) reactivateOrphanedDocTasks(ctx context.Context, existing []tatarav1alpha1.Task) bool {
	l := log.FromContext(ctx)
	any := false
	for i := range existing {
		t := &existing[i]
		if t.Labels[labelActivity] != "documentation" {
			continue
		}
		if t.Status.DeployState != "Parked" && t.Status.Phase != tatarav1alpha1.PhaseFailed {
			continue
		}
		retries := 0
		if v := t.Annotations[annDocRetries]; v != "" {
			if n, aerr := strconv.Atoi(v); aerr == nil {
				retries = n
			}
		}
		if retries >= maxDocReactivations {
			continue // exhausted; leave terminal for a human
		}
		if derr := agent.DeleteWrapper(ctx, r.Client, t.Namespace, t); derr != nil {
			l.Error(derr, "documentation: delete wrapper on doc reactivation (non-fatal)",
				"action", "scan_documentation_reactivate", "resource_id", t.Name)
		}
		// Bump the retry counter (metadata) then reset the run state (status).
		if aerr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			fresh := &tatarav1alpha1.Task{}
			if gerr := r.Get(ctx, client.ObjectKeyFromObject(t), fresh); gerr != nil {
				return gerr
			}
			if fresh.Annotations == nil {
				fresh.Annotations = map[string]string{}
			}
			fresh.Annotations[annDocRetries] = strconv.Itoa(retries + 1)
			return r.Update(ctx, fresh)
		}); aerr != nil {
			l.Error(aerr, "documentation: bump doc retry annotation (skipping reactivation)",
				"action", "scan_documentation_reactivate", "resource_id", t.Name)
			continue
		}
		if serr := r.patchTaskStatus(ctx, t, func(fresh *tatarav1alpha1.Task) bool {
			fresh.Status.Phase = ""
			fresh.Status.DeployState = ""
			fresh.Status.ParkReason = ""
			return true
		}); serr != nil {
			l.Error(serr, "documentation: reset doc task status (skipping reactivation)",
				"action", "scan_documentation_reactivate", "resource_id", t.Name)
			continue
		}
		any = true
		l.Info("documentation: reactivated dropped doc cycle",
			"action", "scan_documentation_reactivate", "resource_id", t.Name, "retry", retries+1)
	}
	return any
}

// proposalBacklogCount counts open, undecided ideas in a pre-fetched issue
// slice: non-PR issues bearing the brainstorming or legacy-idea label.
// Issues sharing a tatara/systemic-<id> label count as a single entry so that
// a multi-repo systemic improvement does not inflate the backlog cap.
func proposalBacklogCount(issues []scm.IssueRef, brainstormingLabel, legacyIdea string) int {
	const systemicPrefix = "tatara/systemic-"
	groups := map[string]bool{}
	standalone := 0
	for _, iss := range issues {
		if iss.IsPR {
			continue
		}
		if !hasLabel(iss.Labels, brainstormingLabel) && !hasLabel(iss.Labels, legacyIdea) {
			continue
		}
		sid := ""
		for _, l := range iss.Labels {
			if strings.HasPrefix(l, systemicPrefix) {
				sid = l
				break
			}
		}
		if sid != "" {
			groups[sid] = true
		} else {
			standalone++
		}
	}
	return standalone + len(groups)
}

// proposalBacklog counts open, undecided ideas for repo: open non-PR issues
// bearing the idea label (live ListOpenIssues). This subsumes tatara-originated
// proposals and any human-filed issue parked as an idea, providing conservative
// brainstorm backpressure.
func (r *ProjectReconciler) proposalBacklog(ctx context.Context, reader scm.SCMReader, repo *tatarav1alpha1.Repository, brainstormingLabel string, scmSpec *tatarav1alpha1.ScmSpec) (int, error) {
	owner, name, err := scm.OwnerRepo(repo.Spec.URL)
	if err != nil {
		return 0, err
	}
	issues, err := reader.ListOpenIssues(ctx, owner, name)
	if err != nil {
		return 0, err
	}
	legacyIdea, _ := legacyLabels(scmSpec)
	return proposalBacklogCount(issues, brainstormingLabel, legacyIdea), nil
}

// hasLiveLifecycleTaskForIssue reports whether any non-terminal Task exists for
// (slug, number) in the snapshot, counting Conversation (human-blocked) Tasks
// too. recoverOrphans uses this for dedup rather than checking only active-phase
// tasks: a Conversation lifecycle Task still owns the issue's pod name, so
// spawning a second lifecycle Task for the same issue collides on the pod and
// wedges the new Task in Planning forever. Dedup must keep at most one live
// lifecycle Task per (repo, issue) regardless of whether that Task is currently
// running an agent.
func hasLiveLifecycleTaskForIssue(existing []tatarav1alpha1.Task, slug string, number int) bool {
	for i := range existing {
		t := &existing[i]
		// Phase 2: spec/ledger identity only; legacy label fallback in taskMatchesItem.
		if !taskMatchesItem(t, slug, number) {
			continue
		}
		if tatarav1alpha1.TaskTerminal(t) {
			continue
		}
		return true
	}
	return false
}

// hasKindTaskForIssue reports whether any Task (terminal or live) of the given
// scan kind exists for the issue (slug, number). Used by the implement producer
// to avoid re-producing an implement Task once one already handled the issue (the
// tatara-implementation label persists after implement completes, so a live-only
// check would re-fire every scan cycle).
func hasKindTaskForIssue(existing []tatarav1alpha1.Task, slug string, number int, kind string) bool {
	for i := range existing {
		t := &existing[i]
		if t.Labels[labelSourceKind] != kind {
			continue
		}
		if taskMatchesItem(t, slug, number) {
			return true
		}
	}
	return false
}

// needsImplementProducer reports whether an issue candidate carrying the
// tatara-implementation phase label needs a fresh implement Task. This is the
// clarify->implement handoff producer (CROSS-REPO-CONTRACT): clarify flips the
// label to tatara-implementation and TERMINATES its own Task, leaving the issue
// with only a terminal Task - which isDeduped skips (managed-label state-of-truth
// treats it as done), dead-ending the stream. The producer fires exactly once per
// implementation-label episode: only for an issue (not a PR) carrying the label
// with no live Task of any kind and no prior implement Task already spun up.
func needsImplementProducer(c candidate, existing []tatarav1alpha1.Task, implementation string) bool {
	if c.isPR || !hasLabel(c.labels, implementation) {
		return false
	}
	if hasLiveLifecycleTaskForIssue(existing, c.repo, c.number) {
		return false
	}
	// The issueLifecycle bridge (and its backstop recoverOrphans drain) owns any
	// issue that already has an issueLifecycle Task, terminal or live: the backstop
	// re-adopts such an orphan into the issueLifecycle Implement state, so minting a
	// discrete implement Task too would double-process the stream. The producer is
	// for the clarify->implement handoff, where the terminated Task is a clarify
	// Task (or there is no Task), never an issueLifecycle one.
	if hasKindTaskForIssue(existing, c.repo, c.number, "issueLifecycle") {
		return false
	}
	return !hasKindTaskForIssue(existing, c.repo, c.number, "implement")
}

// hasLiveOrAdoptableTask returns the single issueLifecycle Task for (slug, number)
// that should be ADOPTED rather than duplicated: any matching issueLifecycle Task
// whose DeployState is neither "Done" nor "Stopped". This covers the in-flight
// states (Triage/Conversation/Implement/MRCI/Merge/MainCI), the unstarted state
// (empty DeployState), AND the Parked state that the false-refusal duplicate
// storm produces. Done (deliberately closed) and Stopped (idle, owned by the
// reactivation pass) are excluded so genuinely-finished issues are not resurrected.
// A Parked sibling is preferred over a Done/Stopped one. Returns nil when no
// adoptable Task exists. Pure (snapshot only); caller adopts via an inline status
// reset to Triage, reusing the deterministic pod/branch.
func hasLiveOrAdoptableTask(existing []tatarav1alpha1.Task, slug string, number int) *tatarav1alpha1.Task {
	for i := range existing {
		t := &existing[i]
		if t.Labels[labelSourceKind] != "issueLifecycle" {
			continue
		}
		// Phase 2: spec/ledger identity only; legacy label fallback in taskMatchesItem.
		if !taskMatchesItem(t, slug, number) {
			continue
		}
		switch t.Status.DeployState {
		case "Done", "Stopped":
			continue
		}
		return t
	}
	return nil
}

// reapStaleProposals is the opt-in dead-letter sink the brainstorm lifecycle
// lacks: it auto-closes bot-authored proposals that have sat with no human
// engagement and no work in flight past act.StaleProposalDays, so dead proposals
// stop inflating the MaxOpenProposals backlog (which blocks new brainstorm
// cycles). It is the ONLY automatic "proposal a human never engaged -> close"
// path; triage, isDeduped, handleConversation and the discuss silence gate are
// untouched.
//
// It reuses the issueCache issueScan already returned this cycle (zero extra
// ListOpenIssues). The single SCM read - a comment scan via humanCommentAfter -
// runs ONLY for candidates that already passed the cheap in-memory predicate,
// and is FAIL-CLOSED: humanCommentAfter returns true on a read error, so a
// failed read skips the close (never auto-close a proposal a human may have
// discussed). A successful close swaps the brainstorming label for declined
// (the close-arm "exactly one managed label" contract) and records
// IssueOutcome("stale-close") AFTER CloseIssue succeeds (idempotent on retry).
// staleProposalWindow returns the reaper staleness window and whether the
// stale-proposal reaper is enabled for this project (opt-in: StaleProposalDays>0).
func staleProposalWindow(proj *tatarav1alpha1.Project) (time.Duration, bool) {
	if proj.Spec.Scm == nil || proj.Spec.Scm.Cron == nil {
		return 0, false
	}
	return staleWindowDays(proj.Spec.Scm.Cron.Brainstorm.StaleProposalDays)
}

// staleWindowDays applies the stale-proposal reaper's default-on semantics
// (liveness finding #8) to a StaleProposalDays value: a POSITIVE value is an
// explicit window; the UNSET default (0) enables the reaper with the generous
// defaultStaleProposalDays window; a NEGATIVE value is the explicit opt-out.
func staleWindowDays(days int) (time.Duration, bool) {
	if days < 0 {
		return 0, false
	}
	if days == 0 {
		days = defaultStaleProposalDays
	}
	return time.Duration(days) * 24 * time.Hour, true
}

// reapEligible reports whether the issue is a stale, unengaged proposal the
// reaper will close, so issueScan/recoverOrphans must NOT create a fresh
// lifecycle task for it: such a task would race the same-cycle reaper, leaving
// it to close an issue that owns a live task. Cheap label/time-only check; no
// SCM call (the reaper does the authoritative human-comment check before close).
// This gate is intentionally BROADER than the reaper's close condition: it does
// not read comments, so an issue past the window with an old human comment is
// gated from triage but not closed by the reaper (humanCommentAfter blocks it).
// That is benign - a >window-stale issue's last human comment is itself older
// than the window, so re-triaging would only re-post into a long-cold thread,
// which the bot-last-word gate already suppresses. It self-corrects the moment a
// human comment bumps UpdatedAt back inside the window.
func (r *ProjectReconciler) reapEligible(proj *tatarav1alpha1.Project, iss scm.IssueRef, existing []tatarav1alpha1.Task) bool {
	window, on := staleProposalWindow(proj)
	if !on {
		return false
	}
	botLogin := ""
	if proj.Spec.Scm != nil {
		botLogin = proj.Spec.Scm.BotLogin
	}
	brs, app, impl, dec := lifecycleLabels(proj.Spec.Scm)
	return isStaleUnengagedProposal(iss, existing, brs, app, impl, dec, botLogin, window)
}

func (r *ProjectReconciler) reapStaleProposals(ctx context.Context, proj *tatarav1alpha1.Project, reader scm.SCMReader, issueCache map[string][]scm.IssueRef, existing []tatarav1alpha1.Task, act tatarav1alpha1.BrainstormActivity) {
	// Liveness finding #8: the window is resolved via staleWindowDays so the reaper
	// is ON by default (unset -> generous default) and a NEGATIVE value is the
	// explicit opt-out. Driven off act.StaleProposalDays (the project's brainstorm
	// activity the caller passes).
	window, on := staleWindowDays(act.StaleProposalDays)
	if !on {
		return
	}
	l := log.FromContext(ctx)
	// Re-list tasks so a lifecycle Task created earlier this cycle (issueScan or
	// recoverOrphans) is visible to the live-task gate in isStaleUnengagedProposal.
	// Those paths now skip reap-eligible issues, but re-listing keeps the close
	// invariant - never close an issue that owns a live Task - robust against any
	// other path that may create one. Fail-safe: keep the passed snapshot on error.
	if fresh, ferr := r.existingScanTasks(ctx, proj); ferr == nil {
		existing = fresh
	} else {
		l.Error(ferr, "reap: re-list tasks failed; using passed snapshot",
			"action", "scan_stale_proposal_close", "resource_id", proj.Name)
	}
	brainstorming, approved, implementation, declined := lifecycleLabels(proj.Spec.Scm)
	botLogin := ""
	if proj.Spec.Scm != nil {
		botLogin = proj.Spec.Scm.BotLogin
	}
	for slug, issues := range issueCache {
		for _, iss := range issues {
			if !isStaleUnengagedProposal(iss, existing, brainstorming, approved, implementation, declined, botLogin, window) {
				continue
			}
			owner, name, ok := strings.Cut(slug, "/")
			if !ok {
				continue
			}
			// Fail-closed: humanCommentAfter returns true on any human comment OR a
			// read error, and a true result skips the close. Passing the zero time
			// means "any human comment ever".
			if humanCommentAfter(ctx, reader, owner, name, iss.Number, botLogin, time.Time{}) {
				continue
			}
			w, token, err := r.scanWriter(ctx, proj)
			if err != nil {
				l.Error(err, "reap: scanWriter for stale proposal close (leaving open)",
					"action", "scan_stale_proposal_close", "resource_id", proj.Name)
				r.Metrics.ScanItem("backstop", "stale_reap_error")
				return
			}
			issueRef := fmt.Sprintf("%s#%d", iss.Repo, iss.Number)
			repoSlug := iss.Repo
			if aerr := w.AddLabel(ctx, token, issueRef, declined); aerr != nil {
				l.Error(aerr, "reap: add declined label failed (leaving proposal open)",
					"action", "scan_stale_proposal_close", "resource_id", proj.Name, "issue", issueRef)
				r.Metrics.ScanItem("backstop", "stale_reap_error")
				continue
			}
			if rerr := w.RemoveLabel(ctx, token, issueRef, brainstorming); rerr != nil {
				l.Info("reap: remove brainstorming label failed (non-fatal)",
					"action", "scan_stale_proposal_close", "resource_id", proj.Name, "issue", issueRef, "err", rerr.Error())
			}
			windowDays := int(window / (24 * time.Hour))
			note := fmt.Sprintf("tatara: auto-closing this proposal - it has had no human engagement for %d days "+
				"and no work is in flight. Reopen or re-file to revive.", windowDays)
			if cerr := w.CloseIssue(ctx, token, repoSlug, iss.Number, note); cerr != nil {
				l.Error(cerr, "reap: close stale proposal failed (leaving open)",
					"action", "scan_stale_proposal_close", "resource_id", proj.Name, "issue", issueRef)
				r.Metrics.ScanItem("backstop", "stale_reap_error")
				continue
			}
			r.Metrics.IssueOutcome("stale-close")
			l.Info("stale proposal auto-closed",
				"action", "scan_stale_proposal_close", "resource_id", proj.Name,
				"issue", issueRef, "updated_at", iss.UpdatedAt, "window_days", windowDays)
		}
	}
}

// recoverOrphans starts the correct lifecycle Task for each OPEN issue that
// carries an active phase label but has no live Task (a missed/never-started or
// stalled handler). It RE-LISTS existing Tasks so it sees Tasks mrScan/issueScan
// created earlier this cycle (an open bot MR becomes a live MRCI Task -> not an
// orphan).
//
// issueCache is the per-repo slice map returned by issueScan this cycle. When a
// repo's issues were already fetched by issueScan, recoverOrphans reuses that
// slice instead of issuing a second ListOpenIssues round-trip (finding 4). A nil
// or missing key falls back to a fresh ListOpenIssues call.
func (r *ProjectReconciler) recoverOrphans(ctx context.Context, proj *tatarav1alpha1.Project, reader scm.SCMReader, repos []tatarav1alpha1.Repository, issueCache map[string][]scm.IssueRef) {
	l := log.FromContext(ctx)
	existing, err := r.existingScanTasks(ctx, proj)
	if err != nil {
		l.Error(err, "backstop: list tasks", "action", "backstop_list_error", "resource_id", proj.Name)
		return
	}
	brainstorming, approved, implementation, _ := lifecycleLabels(proj.Spec.Scm)
	legacyIdea, _ := legacyLabels(proj.Spec.Scm)
	// Open-issue index for the closed-issue give-up sweep below: only repos whose
	// issues were listed this cycle are eligible (a list error must not be read as
	// "all issues closed").
	issuesBySlug := make(map[string][]scm.IssueRef)
	listedSlugs := make(map[string]bool)
	for i := range repos {
		owner, name, oerr := scm.OwnerRepo(repos[i].Spec.URL)
		if oerr != nil {
			continue
		}
		slug := owner + "/" + name
		var issues []scm.IssueRef
		if cached, ok := issueCache[slug]; ok {
			// Reuse the slice issueScan already fetched this cycle (finding 4).
			issues = cached
		} else {
			var lerr error
			issues, lerr = reader.ListOpenIssues(ctx, owner, name)
			if lerr != nil {
				l.Error(lerr, "backstop: ListOpenIssues", "action", "backstop_list_error", "resource_id", proj.Name, "repo", repos[i].Name)
				continue
			}
		}
		issuesBySlug[slug] = issues
		listedSlugs[slug] = true
		for _, iss := range issues {
			if iss.IsPR {
				continue
			}
			var entry, goal string
			switch {
			case hasLabel(iss.Labels, implementation):
				entry = "Implement"
				goal = fmt.Sprintf("Resume implementation for %s#%d (phase label present, no live task)", slug, iss.Number)
			case hasLabel(iss.Labels, approved):
				// Fail closed: an orphaned issue bearing only the approved label
				// (with no live task) must NOT auto-enter implementation - label
				// presence is not a verified maintainer approval, and a freshly
				// recovered task carries no recorded approval (Status.ApprovedByMaintainer).
				// Re-triage instead; the front half re-requests explicit maintainer
				// approval. Only the implementation label (a post-handoff, past-the-gate
				// state) resumes at Implement above.
				entry = "Triage"
				goal = fmt.Sprintf("Re-triage approved-labeled issue %s#%d (no live task; awaiting verified maintainer approval)", slug, iss.Number)
			case hasLabel(iss.Labels, brainstorming) || hasLabel(iss.Labels, legacyIdea):
				entry = "Triage"
				goal = fmt.Sprintf("Triage issue %s#%d", slug, iss.Number)
			default:
				continue
			}
			if hasLiveLifecycleTaskForIssue(existing, slug, iss.Number) {
				continue
			}
			// Give-up reroll: if a terminal Parked task with a recoverable reason
			// exists for this (slug, number) and we are in an Implement entry, adopt
			// it in-place rather than spawning a duplicate. At-cap: skip entirely.
			if entry == "Implement" {
				if parked := matchingTerminalParkedLifecycleTask(existing, slug, iss.Number); parked != nil {
					if parked.Status.ImplementGiveUps >= maxImplGiveUps {
						r.Metrics.ScanItem("backstop", "skipped_giveup_cap")
						l.Info("backstop: skipped give-up reroll (at cap)",
							"action", "backstop_giveup_cap", "resource_id", proj.Name,
							"issue", fmt.Sprintf("%s#%d", slug, iss.Number),
							"giveUps", parked.Status.ImplementGiveUps)
						continue
					}
					if aerr := r.adoptLifecycleTaskAt(ctx, proj, parked, "Implement"); aerr != nil {
						l.Error(aerr, "backstop: adopt give-up task", "action", "backstop_recover_error",
							"resource_id", proj.Name, "task", parked.Name)
						continue
					}
					l.Info("backstop: rerolled gave-up implementation", "action", "backstop_recover",
						"resource_id", proj.Name,
						"issue", fmt.Sprintf("%s#%d", slug, iss.Number),
						"giveUps", parked.Status.ImplementGiveUps)
					r.Metrics.ScanItem("backstop", "recovered")
					continue
				}
			}
			if r.reapEligible(proj, iss, existing) {
				// Stale, unengaged proposal the reaper will close this cycle; do
				// not recover it (a fresh task would race the reaper's close).
				r.Metrics.ScanItem("backstop", "skipped_stale_reapable")
				l.Info("backstop: skipped recovery, proposal stale+unengaged (reaper will close)",
					"action", "backstop_recover", "resource_id", proj.Name,
					"issue", fmt.Sprintf("%s#%d", slug, iss.Number))
				continue
			}
			repo, ok := r.matchRepoForSlug(repos, slug)
			if !ok {
				continue
			}
			cand := candidate{repo: slug, number: iss.Number, labels: iss.Labels, updatedAt: iss.UpdatedAt, title: iss.Title}
			ann := map[string]string{tatarav1alpha1.LifecycleEntryAnnotation: entry}
			ok2, cerr := r.createScanTask(ctx, proj, &repo, cand, cand, "backstop", "issueLifecycle", goal, ann, nil)
			if cerr != nil {
				l.Error(cerr, "backstop: create recovery task", "action", "backstop_create_error", "resource_id", proj.Name, "repo", repo.Name)
				continue
			}
			if !ok2 {
				continue
			}
			l.Info("backstop: recovered orphaned issue", "action", "backstop_recover",
				"resource_id", proj.Name, "issue", fmt.Sprintf("%s#%d", slug, iss.Number), "entry", entry)
			r.Metrics.ScanItem("backstop", "recovered")
		}
	}
	// Closed-issue sweep: the reaper spares recoverable give-up tasks while their
	// issue is open. Once the issue is closed (by refine, a maintainer, or merge),
	// transition the task to Done so the reaper can reclaim it and the blocked
	// metric stops for a closed issue. Only repos listed this cycle are judged.
	for i := range existing {
		tk := &existing[i]
		if tk.Status.DeployState != "Parked" ||
			!tatarav1alpha1.IsRecoverableGiveup(tk.Status.ParkReason) ||
			tk.Status.ImplementGiveUps == 0 || tk.Spec.Source == nil {
			continue
		}
		slug := repoFromIssueRef(tk.Spec.Source.IssueRef)
		if slug == "" || !listedSlugs[slug] {
			continue // repo not listed this cycle; cannot judge open vs closed
		}
		open := false
		for _, iss := range issuesBySlug[slug] {
			if !iss.IsPR && taskMatchesItem(tk, slug, iss.Number) {
				open = true
				break
			}
		}
		if open {
			// Liveness finding #6: a recoverable-giveup Parked task at the give-up cap
			// on a STILL-OPEN issue used to be spared by the reaper forever (never
			// GC'd, never re-nudged). Past maxRecoverableParkAge, re-ping the issue with
			// a comment (a human signal) AND resolve the task Done so the reaper
			// reclaims it - a permanently-parked task must not accumulate silently. The
			// under-cap reroll ran earlier this pass, so a still-Parked task here is
			// at-cap (permanently wedged). Park anchor: LastActivityAt (fallback
			// CreationTimestamp).
			if tk.Status.ImplementGiveUps < maxImplGiveUps {
				continue
			}
			anchor := tk.CreationTimestamp.Time
			if tk.Status.LastActivityAt != nil {
				anchor = tk.Status.LastActivityAt.Time
			}
			if time.Since(anchor) < maxRecoverableParkAge {
				continue
			}
			issueRef := tk.Spec.Source.IssueRef
			if w, token, werr := r.scanWriter(ctx, proj); werr == nil && w != nil && issueRef != "" {
				provider := ""
				if proj.Spec.Scm != nil {
					provider = proj.Spec.Scm.Provider
				}
				msg := "tatara: this issue has been parked awaiting a human for a long time after repeated " +
					"failed attempts, and I'm cleaning up the stalled task. Comment here to have me try again."
				cerr := w.Comment(ctx, token, issueRef, msg)
				r.Metrics.SCMWrite(provider, "comment", scmResult(cerr))
				if cerr != nil {
					l.Error(cerr, "backstop: aged-out park re-ping comment (non-fatal)",
						"action", "backstop_park_aged_out", "resource_id", proj.Name, "task", tk.Name)
				}
			}
			if derr := r.markLifecycleDone(ctx, tk, "park-aged-out"); derr != nil {
				l.Error(derr, "backstop: resolve aged-out park Done",
					"action", "backstop_recover_error", "resource_id", proj.Name, "task", tk.Name)
				continue
			}
			r.Metrics.ScanItem("backstop", "park_aged_out")
			l.Info("backstop: recoverable park aged out; re-pinged issue and resolved Done for GC",
				"action", "backstop_park_aged_out", "resource_id", proj.Name, "task", tk.Name)
			continue
		}
		if derr := r.markLifecycleDone(ctx, tk, "issue-closed"); derr != nil {
			l.Error(derr, "backstop: mark give-up task Done (issue closed)",
				"action", "backstop_recover_error", "resource_id", proj.Name, "task", tk.Name)
			continue
		}
		r.Metrics.ScanItem("backstop", "giveup_issue_closed")
		l.Info("backstop: give-up issue closed; task marked Done for GC",
			"action", "backstop_recover", "resource_id", proj.Name, "task", tk.Name)
	}
}

// markLifecycleDone transitions a lifecycle Task to Done with the given reason,
// used by the closed-issue sweep so the reaper can reclaim a spared give-up task
// once its issue is closed. Idempotent: a no-op when already Done.
func (r *ProjectReconciler) markLifecycleDone(ctx context.Context, task *tatarav1alpha1.Task, reason string) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		fresh := &tatarav1alpha1.Task{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: task.Name}, fresh); err != nil {
			return err
		}
		if fresh.Status.DeployState == "Done" {
			return nil
		}
		fresh.Status.DeployState = "Done"
		fresh.Status.ParkReason = reason
		return r.Status().Update(ctx, fresh)
	})
}

// createRefineTask enqueues a project-scoped refine QueuedEvent.
// Returns created=true when a new event was enqueued.
func (r *ProjectReconciler) createRefineTask(ctx context.Context, proj *tatarav1alpha1.Project, goal string) (bool, error) {
	provider := ""
	if proj.Spec.Scm != nil {
		provider = proj.Spec.Scm.Provider
	}
	dedupKey := "refine-" + proj.Name
	payload := tatarav1alpha1.QueuedEventPayload{
		Kind:         "refine",
		Goal:         goal,
		Labels:       map[string]string{labelActivity: "refine"},
		GenerateName: "refine-",
		Provider:     provider,
		PodRepo:      "",
	}
	_, created, err := queue.EnqueueEvent(ctx, r.Client, r.Seq, proj, tatarav1alpha1.QueueClassNormal, true, dedupKey, payload)
	if err != nil {
		log.FromContext(ctx).Error(err, "scan: enqueue refine event failed; skipping", "action", "scan_enqueue_failed", "project", proj.Name)
		return false, nil
	}
	if created {
		r.Metrics.ScanTaskCreated("refine", "refine")
	}
	return created, nil
}

// inflightRefineTask returns the first non-terminal refine Task for the project,
// or nil when no such task exists.
func (r *ProjectReconciler) inflightRefineTask(ctx context.Context, proj *tatarav1alpha1.Project) (*tatarav1alpha1.Task, error) {
	var list tatarav1alpha1.TaskList
	if err := r.List(ctx, &list, client.InNamespace(proj.Namespace)); err != nil {
		return nil, err
	}
	for i := range list.Items {
		t := &list.Items[i]
		if t.Spec.ProjectRef != proj.Name || t.Spec.Kind != "refine" {
			continue
		}
		if !tatarav1alpha1.TaskTerminal(t) {
			return t, nil
		}
	}
	return nil, nil
}

// latestTerminalRefineTask returns the most recently created terminal refine
// Task for the project that was created at/after since (the current cycle's
// due-base), or nil if none exist. Scoping to since prevents a terminal
// refine Task from a past cycle (still around because TaskRetention has not
// GC'd it yet) from satisfying the barrier for every cycle until it expires;
// each brainstorm cycle must be satisfied by a refine Task from that cycle.
func (r *ProjectReconciler) latestTerminalRefineTask(ctx context.Context, proj *tatarav1alpha1.Project, since time.Time) (*tatarav1alpha1.Task, error) {
	var list tatarav1alpha1.TaskList
	if err := r.List(ctx, &list, client.InNamespace(proj.Namespace)); err != nil {
		return nil, err
	}
	var latest *tatarav1alpha1.Task
	for i := range list.Items {
		t := &list.Items[i]
		if t.Spec.ProjectRef != proj.Name || t.Spec.Kind != "refine" {
			continue
		}
		if !tatarav1alpha1.TaskTerminal(t) {
			continue
		}
		if t.CreationTimestamp.Time.Before(since) {
			continue
		}
		if latest == nil || t.CreationTimestamp.After(latest.CreationTimestamp.Time) {
			latest = t
		}
	}
	return latest, nil
}

// refineNeededThisCycle reports whether the project needs a refine run this
// cycle. Returns true when LastRefine is nil or was set before the earliest
// due-activity base time (meaning the refine stamp predates the current cycle).
func (r *ProjectReconciler) refineNeededThisCycle(proj *tatarav1alpha1.Project, earliestDueBase time.Time) bool {
	if proj.Status.LastRefine == nil {
		return true
	}
	return proj.Status.LastRefine.Before(&metav1.Time{Time: earliestDueBase})
}

// stampRefine records LastRefine on the project status.
func (r *ProjectReconciler) stampRefine(ctx context.Context, proj *tatarav1alpha1.Project) error {
	now := metav1.Now()
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &tatarav1alpha1.Project{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: proj.Namespace, Name: proj.Name}, fresh); err != nil {
			return err
		}
		fresh.Status.LastRefine = &now
		proj.Status.LastRefine = &now
		return r.Status().Update(ctx, fresh)
	})
}

// projectRepoSlugs returns owner/repo slugs for all repositories in the project.
func (r *ProjectReconciler) projectRepoSlugs(ctx context.Context, proj *tatarav1alpha1.Project, repos []tatarav1alpha1.Repository) []string {
	var slugs []string
	for i := range repos {
		owner, name, err := scm.OwnerRepo(repos[i].Spec.URL)
		if err != nil {
			continue
		}
		slugs = append(slugs, owner+"/"+name)
	}
	return slugs
}

// runScans runs each due activity and returns the soonest next-fire as a
// requeue duration. Cron parsing/SCM/create failures are logged and skipped per
// activity so one bad activity never blocks the others or crashes the reconciler.
func (r *ProjectReconciler) runScans(ctx context.Context, proj *tatarav1alpha1.Project) (time.Duration, error) {
	l := log.FromContext(ctx)
	if proj.Spec.Scm == nil || proj.Spec.Scm.Cron == nil || r.ReaderFor == nil {
		return 0, nil
	}
	cronSpec := proj.Spec.Scm.Cron
	now := time.Now()
	soonest := time.Duration(0)
	soonestSet := false
	consider := func(next time.Time) {
		d := next.Sub(now)
		if d < 0 {
			d = 0
		}
		if d > maxScheduleRequeue {
			d = maxScheduleRequeue
		}
		if !soonestSet || d < soonest {
			soonest = d
			soonestSet = true
		}
	}

	reader, rerr := r.scanReader(ctx, proj)
	if rerr != nil {
		l.Error(rerr, "scan: resolve reader", "action", "scan_reader_error", "resource_id", proj.Name)
		return maxScheduleRequeue, nil
	}
	repos, err := r.projectReposForScan(ctx, proj)
	if err != nil {
		return 0, err
	}
	existing, err := r.existingScanTasks(ctx, proj)
	if err != nil {
		return 0, err
	}

	// mrScan: per-repo deterministic jitter (issue #181) spreads each repo's fire
	// across the cron interval instead of firing all repos at the boundary.
	if dueRepos, soonest, ok := r.reposDueForScan(proj, "mrScan", repos, now); ok {
		if len(dueRepos) > 0 {
			backlog := r.mrScan(ctx, proj, reader, dueRepos, existing, cronSpec.MRScan)
			// Deploy supervisor (Phase 6): merge review-approved, green, mergeable bot
			// PRs (the gated fallback to native auto-merge). Runs on the mrScan cadence
			// over the same due repos so it reuses the fetched reader.
			r.superviseApprovedPRs(ctx, proj, reader, dueRepos)
			// Enter Deploying for the discrete-kind flow (Phase 8 item 3): drive a merged
			// approved bot PR's umbrella implement Task into pod-less Deploying so the
			// cascade is supervised and its source issue closes on apply. Reads all project
			// repos (not just dueRepos) so a merge on a not-due repo is still caught.
			r.superviseMergedPRs(ctx, proj, repos)
			// Only advance the stamp when there is no backlog. When backlog=true the
			// 60s short requeue must re-fire; if we stamp now, the (stamp, now] window
			// closes and the backlog drain requeue becomes a no-op (finding 3).
			if !backlog {
				if serr := r.stampScan(ctx, proj, "mrScan"); serr != nil {
					l.Error(serr, "scan: persist mrScan stamp failed",
						"action", "scan_stamp_error", "resource_id", proj.Name, "activity", "mrScan")
					r.Metrics.ScanItem("mrScan", "stamp_error")
				}
				// Recompute the soonest per-repo fire from the fresh stamp so the
				// requeue lands on the next repo's slot, not in the past.
				if _, next2, ok2 := r.reposDueForScan(proj, "mrScan", repos, now); ok2 {
					consider(next2)
				}
			} else {
				consider(now.Add(backlogRequeue))
			}
		} else {
			consider(soonest)
		}
	} else if cronSpec.MRScan.Schedule != "" {
		l.Error(fmt.Errorf("invalid cron %q", cronSpec.MRScan.Schedule), "scan: invalid mrScan cron, disabling",
			"action", "scan_cron_invalid", "resource_id", proj.Name, "activity", "mrScan")
	}

	// issueScan: re-list existing Tasks so Tasks created by mrScan above are visible
	// (prevents duplicate issueLifecycle tasks for bot-PR linked issues).
	if fresh, ferr := r.existingScanTasks(ctx, proj); ferr == nil {
		existing = fresh
	}
	if dueRepos, soonest, ok := r.reposDueForScan(proj, "issueScan", repos, now); ok {
		if len(dueRepos) > 0 {
			// recoverOrphans + backstopSweep keep their once-per-cycle cadence: gate
			// them on the unshifted project boundary (true only on the first per-repo
			// fire of the period) so per-repo jitter does not multiply the sweeps.
			_, periodDue, _, _ := r.activityDue(proj, "issueScan")
			backlog, issueCache := r.issueScan(ctx, proj, reader, dueRepos, existing, cronSpec.IssueScan)
			// Only advance the stamp when there is no backlog (mirrors mrScan fix, finding 3).
			if !backlog {
				if serr := r.stampScan(ctx, proj, "issueScan"); serr != nil {
					l.Error(serr, "scan: persist issueScan stamp failed",
						"action", "scan_stamp_error", "resource_id", proj.Name, "activity", "issueScan")
					r.Metrics.ScanItem("issueScan", "stamp_error")
				}
				if _, next2, ok2 := r.reposDueForScan(proj, "issueScan", repos, now); ok2 {
					consider(next2)
				}
			} else {
				consider(now.Add(backlogRequeue))
			}
			if periodDue {
				r.recoverOrphans(ctx, proj, reader, repos, issueCache)
				r.backstopSweep(ctx, proj, reader, repos)
				r.reapStaleProposals(ctx, proj, reader, issueCache, existing, cronSpec.Brainstorm)
			}
		} else {
			consider(soonest)
		}
	} else if cronSpec.IssueScan.Schedule != "" {
		l.Error(fmt.Errorf("invalid cron %q", cronSpec.IssueScan.Schedule), "scan: invalid issueScan cron, disabling",
			"action", "scan_cron_invalid", "resource_id", proj.Name, "activity", "issueScan")
	}

	// cdScan: push-CD deploy-supervision backstop (project-scoped, like brainstorm).
	// Sweeps Deploying Tasks stalled past 1.5x the deploy budget and rerolls them.
	if cronSpec.CDScan.Schedule != "" {
		if _, due, next, ok := r.activityDue(proj, "cdScan"); ok {
			if due {
				r.cdScan(ctx, proj, existing)
				if serr := r.stampScan(ctx, proj, "cdScan"); serr != nil {
					l.Error(serr, "scan: persist cdScan stamp failed",
						"action", "scan_stamp_error", "resource_id", proj.Name, "activity", "cdScan")
					r.Metrics.ScanItem("cdScan", "stamp_error")
				}
				if next2, ok2 := activityNextFire(cronSpec.CDScan.Schedule, now); ok2 {
					consider(next2)
				}
			} else {
				consider(next)
			}
		} else {
			l.Error(fmt.Errorf("invalid cron %q", cronSpec.CDScan.Schedule), "scan: invalid cdScan cron, disabling",
				"action", "scan_cron_invalid", "resource_id", proj.Name, "activity", "cdScan")
		}
	}

	// brainstorm (opt-in), gated by the refine pre-scan barrier: a due
	// brainstorm tick first ensures the project refiner has run this cycle
	// (grooming the backlog + handoffs) before brainstorm executes. "This
	// cycle" = LastRefine is nil or precedes the brainstorm due-base time. The
	// barrier defers ONLY brainstorm - mrScan/issueScan/healthCheck run on
	// their own schedules regardless. Both Succeeded and Failed refine release
	// the gate so a broken refine never wedges brainstorm.
	if cronSpec.Brainstorm.Enabled {
		if base, due, next, ok := r.activityDue(proj, "brainstorm"); ok {
			if due {
				proceed := true
				if r.refineNeededThisCycle(proj, base) {
					terminal, terr := r.latestTerminalRefineTask(ctx, proj, base)
					if terr != nil {
						l.Error(terr, "scan: check terminal refine task", "action", "scan_refine_error", "resource_id", proj.Name)
					}
					if terminal != nil {
						// Stamp LastRefine and fall through to brainstorm.
						if serr := r.stampRefine(ctx, proj); serr != nil {
							l.Error(serr, "scan: stamp LastRefine failed", "action", "scan_stamp_error", "resource_id", proj.Name, "activity", "refine")
						}
					} else {
						// Check or create an in-flight refine task.
						inflight, ierr := r.inflightRefineTask(ctx, proj)
						if ierr != nil {
							l.Error(ierr, "scan: check inflight refine task", "action", "scan_refine_error", "resource_id", proj.Name)
						}
						if inflight == nil {
							slugs := r.projectRepoSlugs(ctx, proj, repos)
							lookback := cronSpec.Refine.ClosedLookbackDays
							if lookback <= 0 {
								lookback = 30
							}
							goal := refine.GoalProject(slugs, lookback)
							_, _ = r.createRefineTask(ctx, proj, goal)
						}
						// Defer brainstorm until refine is terminal; poll at the barrier cadence.
						proceed = false
						consider(now.Add(requeueRefineBarrier))
					}
				}
				if proceed {
					r.brainstorm(ctx, proj, reader, repos, existing, cronSpec.Brainstorm)
					if serr := r.stampScan(ctx, proj, "brainstorm"); serr != nil {
						l.Error(serr, "scan: persist brainstorm stamp failed",
							"action", "scan_stamp_error", "resource_id", proj.Name, "activity", "brainstorm")
						r.Metrics.ScanItem("brainstorm", "stamp_error")
					}
					if next2, ok2 := activityNextFire(cronSpec.Brainstorm.Schedule, now); ok2 {
						consider(next2)
					}
				}
			} else {
				consider(next)
			}
		} else if cronSpec.Brainstorm.Schedule != "" {
			l.Error(fmt.Errorf("invalid cron %q", cronSpec.Brainstorm.Schedule), "scan: invalid brainstorm cron, disabling",
				"action", "scan_cron_invalid", "resource_id", proj.Name, "activity", "brainstorm")
		}
	}

	// documentation (opt-in cron): the scheduled doc-sync tick. Replaces the
	// retired per-merge push trigger. Gated on Spec.Documentation being enabled
	// with a docs repo; each due tick spawns a doc Task per changed source repo
	// and stamps LastDocumentation (advancing the diff window) even when nothing
	// changed, so it does not busy-refire.
	doc := proj.Spec.Documentation
	if cronSpec.Documentation.Schedule != "" && doc != nil && doc.Enabled && doc.Repo != "" {
		if _, due, next, ok := r.activityDue(proj, "documentation"); ok {
			if due {
				r.documentationScan(ctx, proj, reader, repos)
				if serr := r.stampScan(ctx, proj, "documentation"); serr != nil {
					l.Error(serr, "scan: persist documentation stamp failed",
						"action", "scan_stamp_error", "resource_id", proj.Name, "activity", "documentation")
					r.Metrics.ScanItem("documentation", "stamp_error")
				}
				if next2, ok2 := activityNextFire(cronSpec.Documentation.Schedule, now); ok2 {
					consider(next2)
				}
			} else {
				consider(next)
			}
		} else {
			l.Error(fmt.Errorf("invalid cron %q", cronSpec.Documentation.Schedule), "scan: invalid documentation cron, disabling",
				"action", "scan_cron_invalid", "resource_id", proj.Name, "activity", "documentation")
		}
	}

	// healthCheck is RETIRED: its cron dispatch was removed (proposals absorbed
	// into brainstorm). ScmCron.HealthCheck + Status.LastHealthCheck are kept
	// inert for stored-CR back-compat; nothing fires them.

	return soonest, nil
}
