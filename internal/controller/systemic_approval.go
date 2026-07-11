// Copyright 2026 tatara authors.

package controller

import (
	"context"
	"regexp"
	"strconv"

	"sigs.k8s.io/controller-runtime/pkg/client"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// issueHasRecordedApproval reports whether any Task for (repoSlug, number) carries a
// recorded maintainer approval (Status.ApprovedByMaintainer set). This is the strong
// human-gate signal: a sibling a maintainer declined or never approved never has it
// set, so it fails this check and is stripped from any force-close.
func issueHasRecordedApproval(tasks []tatarav1alpha1.Task, repoSlug string, number int) bool {
	for i := range tasks {
		t := &tasks[i]
		if t.Status.ApprovedByMaintainer == "" {
			continue
		}
		if tatarav1alpha1.TaskMatchesItem(t, repoSlug, number) {
			return true
		}
	}
	return false
}

// filterSystemicGroupByApproval returns a copy of sg keeping only maintainer-approved
// members - SameRepoSiblings verified in leadRepo, CrossRepo by their own parsed repo
// - plus the same-repo sibling numbers that are NOT approved (for the writeback
// close-strip). A nil sg yields (nil, nil). This is the authoritative approval gate on
// systemic co-resolution: approving the lead never force-closes an unapproved or
// declined sibling because that sibling is filtered out here before the prompt is
// built and its close directive is stripped at writeback.
func filterSystemicGroupByApproval(sg *tatarav1alpha1.SystemicGroup, leadRepo string, tasks []tatarav1alpha1.Task) (*tatarav1alpha1.SystemicGroup, []int) {
	if sg == nil {
		return nil, nil
	}
	out := &tatarav1alpha1.SystemicGroup{SystemicID: sg.SystemicID}
	var unapproved []int
	for _, n := range sg.SameRepoSiblings {
		if leadRepo != "" && issueHasRecordedApproval(tasks, leadRepo, n) {
			out.SameRepoSiblings = append(out.SameRepoSiblings, n)
		} else {
			unapproved = append(unapproved, n)
		}
	}
	for _, ref := range sg.CrossRepo {
		repo, num := parseCrossRepoRef(ref)
		if repo != "" && issueHasRecordedApproval(tasks, repo, num) {
			out.CrossRepo = append(out.CrossRepo, ref)
		}
	}
	return out, unapproved
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
		return task
	}
	filtered, _ := filterSystemicGroupByApproval(task.Spec.SystemicGroup, leadRepoOf(task), tl.Items)
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
		return nil
	}
	_, unapproved := filterSystemicGroupByApproval(task.Spec.SystemicGroup, leadRepoOf(task), tl.Items)
	if len(unapproved) == 0 {
		return nil
	}
	m := make(map[int]bool, len(unapproved))
	for _, n := range unapproved {
		m[n] = true
	}
	return m
}

// closeDirectiveRE matches a GitHub/GitLab auto-close directive ("closes #12",
// "Fixes #7", "resolved #3", ...) capturing the issue number. Cross-repo forms
// ("closes owner/repo#3") deliberately do NOT match: systemic CrossRepo members are
// reference-only and are never closed from the lead PR body.
var closeDirectiveRE = regexp.MustCompile(`(?i)\b(?:clos(?:e|es|ed)|fix(?:es|ed)?|resolv(?:e|es|ed))\s+#(\d+)`)

// neutralizeUnapprovedCloses rewrites any auto-close directive targeting an
// unapproved sibling number into a plain "refs #N" reference, so merging the lead's
// combined PR never force-closes a sibling a maintainer has not approved. Directives
// for approved siblings (and the lead's own issue) are left intact.
func neutralizeUnapprovedCloses(body string, unapproved map[int]bool) string {
	if len(unapproved) == 0 {
		return body
	}
	return closeDirectiveRE.ReplaceAllStringFunc(body, func(m string) string {
		sub := closeDirectiveRE.FindStringSubmatch(m)
		n, err := strconv.Atoi(sub[1])
		if err == nil && unapproved[n] {
			return "refs #" + sub[1]
		}
		return m
	})
}
