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

// THIS REPO CARRIES TWO CI VOCABULARIES AND THEY ARE NOT INTERCHANGEABLE.
//
//   - The GATE vocabulary - "" | pending | success | failure - is what PRState
//     and GetCommitCIStatus return and what merge.go / ci_gate.go compare
//     against. It never reaches etcd.
//   - The MIRROR vocabulary below - none | pending | running | green | red - is
//     what MergeRequest.status.ciStatus holds. The CRD pins an ENUM on that
//     field, so writing "success" onto it is rejected by the API server, not
//     merely inconsistent.
//
// Every writer of the CR field goes through these constants, and every value
// crossing from the gate vocabulary to the mirror one goes through
// MirrorCIStatus.
const (
	CIMirrorNone    = "none"
	CIMirrorPending = "pending"
	CIMirrorRunning = "running"
	CIMirrorGreen   = "green"
	CIMirrorRed     = "red"
)

// MirrorCIStatus maps a gate-vocabulary CI status onto the mirror vocabulary.
//
// Mapping success -> green is sound HERE and only here: both producers of the
// gate vocabulary (GetPRState, GetCommitCIStatus) fold EVERY check and commit
// status on the commit before they answer, so their "success" is an AGGREGATE
// claim. A single check_run webhook carries no such claim and must never take
// this path - see ghSingleCheckCIStatus.
//
// An unrecognised value maps to none rather than to a guess: "I have no CI
// observation" is the honest reading of a word this operator does not know, and
// it is the one reading that cannot let a Task act on a fiction.
func MirrorCIStatus(s string) string {
	switch s {
	case "success":
		return CIMirrorGreen
	case "failure":
		return CIMirrorRed
	case "pending":
		return CIMirrorPending
	default:
		return CIMirrorNone
	}
}

// gateCIStatus is MirrorCIStatus's inverse: it narrows the mirror vocabulary
// back onto the gate one. It is lossy in exactly one place - the mirror's
// running and pending both fold onto the gate's pending, because the gate has
// no word for "started" - and that loss is why the two vocabularies exist.
//
// It lets a provider derive CI ONCE, in the richer mirror vocabulary, and hand
// the gate its narrowed view, instead of two derivations of one fact drifting
// apart (which is what shipped scm_read(kind="ci")=green on a failed pipeline).
// An unrecognised value maps to "" for the same reason MirrorCIStatus maps it
// to none: no observation beats a guess.
func gateCIStatus(s string) string {
	switch s {
	case CIMirrorGreen:
		return "success"
	case CIMirrorRed:
		return "failure"
	case CIMirrorPending, CIMirrorRunning:
		return "pending"
	default:
		return ""
	}
}
