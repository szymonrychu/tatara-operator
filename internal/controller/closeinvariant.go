// Copyright 2026 tatara authors.

package controller

import (
	"fmt"
	"strings"

	tatarav1alpha1 "github.com/szymonrychu/tatara-operator/api/v1alpha1"
)

// OpenOwnedMRs is THE CLOSE INVARIANT's one predicate: an Issue may be closed
// only when nothing the owning Task still has in flight would be STRANDED by
// that close. It returns the CR names of every MergeRequest in mrs that is
// still live on the forge, so a caller both decides and reports with the same
// call - a refusal that cannot name the PR it is protecting is a refusal a
// human cannot act on.
//
// LIVE means "not settled", not "not merged", and the difference is load
// bearing in BOTH directions:
//
//   - `merged` is settled and safe. That is the whole point of the invariant:
//     the work landed, so the issue may close.
//   - `closed` is settled too. A PR the agent abandoned strands nothing - there
//     is no open forge object left pointing at the issue. Treating it as
//     blocking would build a NEW permanent dead end (an issue that can never be
//     closed by anything, ever, because one abandoned PR sits in its history),
//     which is the exact failure class this whole change exists to remove.
//   - "" is NOT settled. An MR CR minted by mrOpen before its first mirror sync
//     carries an empty state, and an unknown state must FAIL CLOSED: the cost of
//     a wrong "open" is a park a later pass clears, the cost of a wrong "merged"
//     is a closed issue with a live PR behind it.
//
// The EMPTY SET IS A PASS here, unlike CloseIssuesOnDelivery's all([]) guard.
// The two answer different questions: delivery asks "did this Task actually
// deliver anything", where zero MRs means there is nothing to have delivered,
// while this asks "would closing strand live work", where zero MRs means there
// is nothing to strand. A refine or clarify Task that declines an issue owns no
// MRs at all and must still be able to close it.
func OpenOwnedMRs(mrs []tatarav1alpha1.MergeRequest) []string {
	var open []string
	for i := range mrs {
		switch mrs[i].Status.State {
		case "merged", "closed":
			continue
		}
		open = append(open, mrs[i].Name)
	}
	return open
}

// ParkedForOpenMRsComment is what a human reads on an Issue whose close was
// REFUSED by the invariant. It names the PRs that blocked it, because the only
// action that clears this park is finishing or abandoning one of them, and a
// comment that does not say which is a comment that costs a kubectl.
func ParkedForOpenMRsComment(taskName string, open []string) string {
	return fmt.Sprintf(
		"tatara did not close this issue: task `%s` still has unfinished pull requests (%s).\n\n"+
			"An issue is closed only once every one of its PRs is merged, so this issue is "+
			"labelled `%s` and stays open instead. Merge or close the PRs above, or comment "+
			"here to have the platform pick it up again.",
		taskName, strings.Join(open, ", "), TataraParkedLabel)
}

// FilterCloseDirectivesForTask is FilterCloseDirectives under the C.1 CLOSE
// INVARIANT, and it is the form every MR-body write must use.
//
// The allowlist alone is not enough. It answers "may this Task auto-close issue
// N at all", and the answer is legitimately yes for an issue the Task owns - but
// the FORGE closes on the FIRST merge, not on the last. A Task shipping repos A
// and B for one issue writes `Closes #N` into both PR bodies; A merges, the
// forge closes the issue, and B is still open. That is the invariant lost to a
// mechanism the operator does not control, and no operator-side guard can undo
// it after the fact - which is why this is a WRITE-TIME strip rather than a
// close-time check.
//
// openSiblings is OpenOwnedMRs over the MergeRequests the Task ALREADY owns,
// evaluated at the moment the body is written. Non-empty means at least one
// sibling PR is live, so NOTHING in this body may carry a close keyword: every
// reference is rebuilt as a bare link, exactly as an unapproved reference
// already is, so the issue stays cross-linked and does not auto-close.
//
// The FIRST PR of a Task keeps its keyword, deliberately: at that moment the
// Task owns no other live MR, so that PR merging genuinely IS every PR merged.
// The residual hole is a second PR opened for the same issue AFTER the first
// merged with its keyword intact; nothing rewrites an already-posted body, and
// the operator-side guards (queueIssueClose, CloseIssuesOnDelivery) are what
// cover the rest.
func FilterCloseDirectivesForTask(body, ownRepo string, allowed map[RepoNum]bool, openSiblings []string) string {
	if len(openSiblings) > 0 {
		// An EMPTY allowlist, not a nil filter: "unknown means stripped" is
		// already this function's documented default, so the invariant reuses it
		// rather than growing a second stripping path that could disagree.
		allowed = nil
	}
	return FilterCloseDirectives(body, ownRepo, allowed)
}
