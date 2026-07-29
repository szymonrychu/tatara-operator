package v1alpha1

import "unicode/utf8"

// The Go-side twins of every +kubebuilder:validation:MaxLength marker on a
// field that receives EXTERNALLY-SIZED text - a forge comment, an issue or MR
// body, an agent's REST payload, a rendered alert.
//
// A kubebuilder marker is a comment. Nothing in Go can read it, so every writer
// that has to fit one of these fields either re-declared the number for itself
// or, more often, did not think about it at all. Both halves of that went wrong
// at once in issue #495: internal/restapi and internal/agent each carried their
// own private 4096, while all four TaskEvent.Body construction sites carried
// none - so a forge comment over 4 KB made the API server reject the entire
// Task status update, and the mr_comment redelivery it was carrying (a human's
// reply to a review Task parked awaiting-human) was dropped on every sweep,
// forever.
//
// The limit is enforced in BYTES, not characters or runes. That matters twice
// over: GitHub caps an issue body at 65536 CHARACTERS, which is up to 4x that
// in UTF-8 bytes, and a naive s[:max] cut lands mid-rune and produces invalid
// UTF-8 that the API server's JSON encoder rejects even once the length is
// legal. TruncateUTF8 is the one cut that gets both right; every clamp in this
// repo goes through it.
//
// A constant here that disagrees with its marker is a compile-clean lie, so
// TestCRDMaxLengthMatchesConstants (limits_test.go) reads the GENERATED CRDs
// and fails if any pair drifts.
const (
	// GoalMaxBytes caps TaskSpec.Goal and QueuedTaskBlueprint.Goal. Goal is
	// NON-EVICTABLE: the A.7 byte guard can spill comments and notes but can
	// never shrink the goal, so an unclamped goal eats the budget the guard
	// exists to defend.
	GoalMaxBytes = 16384

	// NoteBodyMaxBytes caps Note.Body. An over-long note the API server rejects
	// is an EMPTY notes journal, and notes ARE the continuation state.
	NoteBodyMaxBytes = 4096

	// TaskEventBodyMaxBytes caps TaskEvent.Body. Forge comments routinely run
	// past it - this platform's own agents write multi-KB ones.
	TaskEventBodyMaxBytes = 4096

	// CommentBodyMaxBytes caps Comment.Body, the A.1 mirror ingest cap. GitHub
	// allows 65536-char bodies, so 25 max-size comments is 1.6 MB - over the
	// etcd ceiling on its own - and a 64 KB comment is not prompt-useful.
	CommentBodyMaxBytes = 8192

	// ReviewFindingBodyMaxBytes caps ReviewFinding.Body. Load-bearing with
	// MaxItems=30: 30 findings x an unbounded body is an A.7 byte-budget input
	// the guard CANNOT evict, since it is spec-adjacent intent.
	ReviewFindingBodyMaxBytes = 8192

	// PendingReviewBodyMaxBytes caps PendingReview.Body.
	PendingReviewBodyMaxBytes = 16384

	// PendingCommentBodyMaxBytes caps PendingComment.Body.
	PendingCommentBodyMaxBytes = 16384

	// IssueBodyMaxBytes and MergeRequestBodyMaxBytes cap the mirrored forge
	// bodies. See the byte-vs-character note above: the forge's own cap is in
	// characters, so a body the forge accepted can still be too long here.
	IssueBodyMaxBytes        = 65536
	MergeRequestBodyMaxBytes = 65536
)

// TruncateUTF8 cuts s to at most maxBytes BYTES on a rune boundary. A string
// that already fits is returned byte-identical - callers compare stored values
// (drainRenderedEvents matches a pending event by its whole value tuple, body
// included), so an unconditional rewrite would not be free.
func TruncateUTF8(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	cut := s[:maxBytes]
	for len(cut) > 0 && !utf8.ValidString(cut) {
		cut = cut[:len(cut)-1]
	}
	return cut
}
