// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// newestTaskForSource returns the most-recently-created Task among tasks whose
// SOURCE identity (TaskMatchesItemAsSource - never a ledger role:closes entry)
// matches (repo, number), or nil when none match. Two bugs this fixes:
//  1. F2 self-match: a systemic lead's own ledger is seeded with a role:closes
//     entry for every sibling it references (seedLedgerFromSpec), so matching
//     via TaskMatchesItem (any role) made the lead's own approval/lock state
//     appear to belong to every sibling it merely leads. Source-identity-only
//     matching (TaskMatchesItemAsSource) fixes this.
//  2. F3 staleness: more than one Task can track the same issue over its life
//     (a terminal clarify Task plus a later re-triage Task created after it
//     went Done). An any-match scan let a stale locked Task from an earlier
//     cycle outlive its successor. Picking the newest by CreationTimestamp
//     fixes this.
func newestTaskForSource(tasks []tatarav1alpha1.Task, repo string, number int) *tatarav1alpha1.Task {
	var newest *tatarav1alpha1.Task
	for i := range tasks {
		t := &tasks[i]
		if !tatarav1alpha1.TaskMatchesItemAsSource(t, repo, number) {
			continue
		}
		if newest == nil || isNewerTask(t, newest) {
			newest = t
		}
	}
	return newest
}

// isNewerTask reports whether t is newer than other. m6: metav1.Time is
// second-granularity, so two Tasks created in the same wall-clock second have
// an equal CreationTimestamp and CreationTimestamp.Before() never picks a
// winner between them - the caller's scan order then decided non-
// deterministically. Falls back to a stable, input-order-independent
// tie-break (greater Name) so the same pair always resolves to the same
// winner; the tie-break carries no recency meaning of its own.
func isNewerTask(t, other *tatarav1alpha1.Task) bool {
	if other.CreationTimestamp.Before(&t.CreationTimestamp) {
		return true
	}
	if t.CreationTimestamp.Equal(&other.CreationTimestamp) {
		return t.Name > other.Name
	}
	return false
}

// issueHasRecordedApproval reports whether the NEWEST Task that IS (repoSlug,
// number) - resolved by source identity only, never a lead's role:closes
// ledger entry - carries a recorded approval (Status.ApprovedByMaintainer
// set). m7: newest-wins, matching issueIsImplementationLocked's resolution
// for the same source identity - an ANY-match scan let a stale approved Task
// from an earlier cycle outlive a newer, unapproved re-triage. A sibling a
// maintainer declined or never approved never has it set (on its newest
// Task), so it fails this check and is stripped from any force-close.
// ApprovedByMaintainer also carries the auto-approve sentinel
// ("<tatara:auto:<kind>>", set by recordAutoApproval) - that is an accepted
// approval source for this check by design (auto-approve composes with
// fan-out), not a bypass to guard against.
func issueHasRecordedApproval(tasks []tatarav1alpha1.Task, repoSlug string, number int) bool {
	t := newestTaskForSource(tasks, repoSlug, number)
	return t != nil && t.Status.ApprovedByMaintainer != ""
}

// issueIsImplementationLocked reports whether the NEWEST Task that IS
// (repoSlug, number) - resolved by source identity only - has
// Status.ImplementationLocked set: its own clarify conversation reached
// no-open-questions with every decision settled (item Request C/d). Used by
// the systemic-group approval fan-out: a sibling with no direct maintainer
// approval of its own is still included in an approved lead's release when it
// is implementation-locked.
func issueIsImplementationLocked(tasks []tatarav1alpha1.Task, repoSlug string, number int) bool {
	t := newestTaskForSource(tasks, repoSlug, number)
	return t != nil && t.Status.ImplementationLocked
}

// filterSystemicGroupByApproval returns a copy of sg keeping only the members
// eligible to co-resolve with the lead: SameRepoSiblings/CrossRepo verified
// either (a) maintainer-approved in their own right (issueHasRecordedApproval)
// or (b) implementation-locked AND leadApproved is true (item Request C/d fan-out:
// an approved lead's release extends to every OTHER member that has no open
// questions of its own, even without its own direct approval). Also returns
// the same-repo sibling numbers that are NOT eligible (for the writeback
// close-strip) and the refs that were included via the fan-out path only
// (for audit logging/metrics at the caller). A nil sg yields (nil, nil, nil).
func filterSystemicGroupByApproval(sg *tatarav1alpha1.SystemicGroup, leadRepo string, leadApproved bool, tasks []tatarav1alpha1.Task) (filtered *tatarav1alpha1.SystemicGroup, unapproved []int, fannedOut []string) {
	if sg == nil {
		return nil, nil, nil
	}
	out := &tatarav1alpha1.SystemicGroup{SystemicID: sg.SystemicID}
	for _, n := range sg.SameRepoSiblings {
		switch {
		case leadRepo != "" && issueHasRecordedApproval(tasks, leadRepo, n):
			out.SameRepoSiblings = append(out.SameRepoSiblings, n)
		case leadApproved && leadRepo != "" && issueIsImplementationLocked(tasks, leadRepo, n):
			out.SameRepoSiblings = append(out.SameRepoSiblings, n)
			fannedOut = append(fannedOut, fmt.Sprintf("%s#%d", leadRepo, n))
		default:
			unapproved = append(unapproved, n)
		}
	}
	for _, ref := range sg.CrossRepo {
		repo, num := parseCrossRepoRef(ref)
		if repo == "" {
			continue
		}
		switch {
		case issueHasRecordedApproval(tasks, repo, num):
			out.CrossRepo = append(out.CrossRepo, ref)
		case leadApproved && issueIsImplementationLocked(tasks, repo, num):
			out.CrossRepo = append(out.CrossRepo, ref)
			fannedOut = append(fannedOut, ref)
		}
	}
	return out, unapproved, fannedOut
}

// leadRepoOf returns the lead issue's repo slug (siblings live in this repo).
func leadRepoOf(task *tatarav1alpha1.Task) string {
	if task.Spec.Source == nil {
		return ""
	}
	return tatarav1alpha1.RepoFromIssueRef(task.Spec.Source.IssueRef)
}

// withApprovedSystemicGroup returns task unchanged when it leads no systemic group,
// else a shallow copy whose Spec.SystemicGroup keeps only currently maintainer-
// approved members. The copy is prompt-only (never persisted): the ledger keeps the
// full discovered group so late approvals still co-resolve on a later reconcile.
func (r *TaskReconciler) withApprovedSystemicGroup(ctx context.Context, task *tatarav1alpha1.Task) *tatarav1alpha1.Task {
	if task.Spec.SystemicGroup == nil {
		return task
	}
	var tl tatarav1alpha1.TaskList
	if err := r.List(ctx, &tl, client.InNamespace(task.Namespace)); err != nil {
		// M3: fail CLOSED on a List error - handing the agent prompt the FULL
		// unfiltered group (the old behaviour, returning task unchanged) would
		// let it reference/close every member with no approval check at all.
		// A copy with an empty (but non-nil) SystemicGroup is the safe default:
		// no member fans out until a later reconcile can actually verify
		// approval state.
		log.FromContext(ctx).Error(err, "systemic approval: list tasks failed; failing closed (no fan-out)",
			"action", "systemic_approval_list_error", "resource_id", task.Name)
		tc := *task
		tc.Spec.SystemicGroup = &tatarav1alpha1.SystemicGroup{SystemicID: task.Spec.SystemicGroup.SystemicID}
		return &tc
	}
	leadApproved := task.Status.ApprovedByMaintainer != ""
	filtered, _, fannedOut := filterSystemicGroupByApproval(task.Spec.SystemicGroup, leadRepoOf(task), leadApproved, tl.Items)
	if len(fannedOut) > 0 {
		log.FromContext(ctx).Info("systemic approval: fan-out to implementation-locked siblings",
			"action", "systemic_approval_fanout", "resource_id", task.Name,
			"approver", task.Status.ApprovedByMaintainer, "member_count", len(fannedOut), "members", fannedOut)
		if r.Metrics != nil {
			r.Metrics.SystemicApprovalFanout()
		}
	}
	tc := *task
	tc.Spec.SystemicGroup = filtered
	return &tc
}

// unapprovedSystemicSiblings returns the set of same-repo sibling numbers in task's
// systemic group that are NOT currently maintainer-approved, for the writeback
// close-strip. Empty when task leads no group or every sibling is approved.
func (r *TaskReconciler) unapprovedSystemicSiblings(ctx context.Context, task *tatarav1alpha1.Task) map[int]bool {
	if task.Spec.SystemicGroup == nil || len(task.Spec.SystemicGroup.SameRepoSiblings) == 0 {
		return nil
	}
	var tl tatarav1alpha1.TaskList
	if err := r.List(ctx, &tl, client.InNamespace(task.Namespace)); err != nil {
		// M3: fail CLOSED on a List error - the old nil return let the
		// writeback caller skip neutralization entirely (every "Closes #N"
		// survives). Treat every same-repo sibling as unapproved instead, so
		// the caller strips them all.
		log.FromContext(ctx).Error(err, "systemic approval: list tasks failed; failing closed (all siblings unapproved)",
			"action", "systemic_approval_list_error", "resource_id", task.Name)
		m := make(map[int]bool, len(task.Spec.SystemicGroup.SameRepoSiblings))
		for _, n := range task.Spec.SystemicGroup.SameRepoSiblings {
			m[n] = true
		}
		return m
	}
	leadApproved := task.Status.ApprovedByMaintainer != ""
	_, unapproved, _ := filterSystemicGroupByApproval(task.Spec.SystemicGroup, leadRepoOf(task), leadApproved, tl.Items)
	if len(unapproved) == 0 {
		return nil
	}
	m := make(map[int]bool, len(unapproved))
	for _, n := range unapproved {
		m[n] = true
	}
	return m
}

// closeDirectiveRE matches a GitHub/GitLab auto-close directive: the keyword
// set both forges recognize (close/fix/resolve/implement, every inflection -
// base, -s, -ed, -ing), an optional colon, an optional "issue"/"issues" word,
// then a comma/"and"-separated list of "#N" refs ("Closes: #7", "Closing #7",
// "Fixes issue #7", "Closes #7, #8", ...). Captures the whole ref-list as
// group 1 so every number in a list can be neutralized individually, not only
// the first. Cross-repo forms ("closes owner/repo#3") deliberately do NOT
// match: systemic CrossRepo members are reference-only and are never closed
// from the lead PR body - "owner/repo" is neither "issue"/"issues" nor a bare
// "#", so the ref-list alternation fails right after the keyword.
var closeDirectiveRE = regexp.MustCompile(
	`(?i)\b(?:clos(?:e|es|ed|ing)|fix(?:es|ed|ing)?|resolv(?:e|es|ed|ing)|implement(?:s|ed|ing)?)` +
		`\s*:?\s*(?:issues?\s+)?(#\d+(?:\s*(?:,|and)\s*(?:issues?\s+)?#\d+)*)`)

// closeDirectiveRefRE extracts each individual "#N" ref out of a
// closeDirectiveRE ref-list match.
var closeDirectiveRefRE = regexp.MustCompile(`#(\d+)`)

// neutralizeUnapprovedCloses rewrites every auto-close directive targeting an
// unapproved sibling number into a plain "refs #N" reference, so merging the lead's
// combined PR never force-closes a sibling a maintainer has not approved.
//   - A directive whose every ref is approved is left byte-for-byte untouched
//     (preserves the agent's original keyword/casing).
//   - A directive whose every ref is unapproved is rewritten to a "refs #N"
//     list, dropping the original keyword entirely.
//   - A MIXED list ("Closes #7, #8" with only #8 approved) is rebuilt ref-by-ref:
//     an unapproved ref becomes "refs #N", an approved ref is normalized to
//     "Closes #N" (the original single keyword cannot represent both states at
//     once).
func neutralizeUnapprovedCloses(body string, unapproved map[int]bool) string {
	if len(unapproved) == 0 {
		return body
	}
	return closeDirectiveRE.ReplaceAllStringFunc(body, func(m string) string {
		refs := closeDirectiveRefRE.FindAllStringSubmatch(m, -1)
		anyUnapproved := false
		for _, sub := range refs {
			if n, err := strconv.Atoi(sub[1]); err == nil && unapproved[n] {
				anyUnapproved = true
				break
			}
		}
		if !anyUnapproved {
			return m
		}
		parts := make([]string, 0, len(refs))
		for _, sub := range refs {
			n, _ := strconv.Atoi(sub[1])
			if unapproved[n] {
				parts = append(parts, "refs #"+sub[1])
			} else {
				parts = append(parts, "Closes #"+sub[1])
			}
		}
		return strings.Join(parts, ", ")
	})
}
