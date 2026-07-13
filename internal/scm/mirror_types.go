package scm

import "time"

// Issue is a forge issue plus its thread, as read for the Issue CR mirror
// (contract B.4). It is the input to controller.SyncIssue: the mirror upsert is
// a PURE function of this snapshot, so the sync itself makes no forge call and
// every read stays on the paced, rate-limited read path (C.8).
//
// It is distinct from IssueRef (a listing row, no thread) and IssueContent (a
// title/body pair): the mirror needs BOTH plus the comments, in one value.
type Issue struct {
	Number int
	URL    string
	Title  string
	Author string
	Body   string
	// State is SCM truth: open | closed.
	State     string
	Labels    []string
	CreatedAt time.Time
	UpdatedAt time.Time
	// Comments is the thread, oldest-first.
	Comments []IssueComment
}

// MergeRequest is a forge PR/MR plus its thread, as read for the MergeRequest
// CR mirror. HeadSHA is the MIRROR's last-synced head: it is NEVER trusted for
// a merge or an approval decision, both of which re-fetch the head LIVE via
// GetPRHead (fix 10).
type MergeRequest struct {
	Number int
	URL    string
	Title  string
	Author string
	Body   string
	// State is SCM truth: open | merged | closed.
	State      string
	HeadBranch string
	HeadSHA    string
	// CIStatus is the mirrored CI state: none | pending | running | green | red.
	CIStatus  string
	Mergeable bool
	CreatedAt time.Time
	UpdatedAt time.Time
	MergedAt  *time.Time
	// Comments is the thread, oldest-first.
	Comments []IssueComment
}
