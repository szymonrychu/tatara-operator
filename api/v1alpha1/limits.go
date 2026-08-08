package v1alpha1

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

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

// IssueTitleMaxChars caps the title of every issue this platform files on a
// forge. It is the odd one out among the constants above, twice over, and both
// differences are load-bearing.
//
// It is NOT the twin of a kubebuilder marker. No CRD field caps a title, so
// TestCRDMaxLengthMatchesConstants has nothing to hold it against; what it
// mirrors is the FORGE's own validation. GitLab rejects an issue title over 255
// with a 400 and GitHub imposes no title limit at all, so this is the stricter
// of the two applied to BOTH: the provider is a per-project setting, no
// provider-capability model exists to hang a per-forge limit on, and one shared
// ceiling costs GitHub nothing.
//
// It counts CHARACTERS, not bytes - the one place in this file where the
// byte-vs-character note above bites in the other direction. 255 CJK characters
// is 765 bytes, so clamping this with TruncateUTF8 would mutilate a title the
// forge would have accepted. ClampIssueTitle is the cut that gets it right.
const IssueTitleMaxChars = 255

// titleTruncatedMarker makes a clamped title READ as clamped. Without it a cut
// title is indistinguishable from one the agent merely wrote badly, and the
// reader has no way to know the rest of the sentence ever existed.
const titleTruncatedMarker = "...(truncated)"

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

// TruncateRunes cuts s to at most maxRunes RUNES, always on a rune boundary. It
// is the character-counting twin of TruncateUTF8 and the two are NOT
// interchangeable: TruncateUTF8 enforces the API server's byte limits, this one
// enforces a forge's character limits. A string that already fits is returned
// byte-identical, for the same reason TruncateUTF8 does it.
func TruncateRunes(s string, maxRunes int) string {
	if maxRunes <= 0 {
		return ""
	}
	n := 0
	for i := range s {
		if n == maxRunes {
			return s[:i]
		}
		n++
	}
	return s
}

// ClampIssueTitle cuts title to fit IssueTitleMaxChars, leaving the elision
// marker visible when it had to cut. Trailing whitespace is trimmed off the cut
// first, so the marker never trails a half-written word by three spaces. Line
// breaks inside the title are flattened to spaces (see flattenTitleLines).
//
// Every agent-supplied ISSUE title handed to the forge goes through this: the
// three CreateIssue sites in internal/restapi, and - twice - the deferred edit
// path, once where the intent is queued and again where reviewpost replays it.
// The replay clamp is not redundant: intents live on the Issue CR, so ones
// queued before this existed are still in etcd carrying raw titles, and the
// replay is the only gate they will ever pass again.
//
// It is the clamp the limits contract was missing: the body beside it on the
// same request has been clamped since #495, the title never was, and a GitLab
// 400 on an over-long title discards the whole outcome the agent had just
// submitted.
//
// MR titles are a KNOWN GAP and are deliberately not routed through here yet.
// GitLab applies the same 255-character Issuable validation to a merge request,
// so mrOpen can take the same 400 - but it maps a forge 4xx (status and body)
// straight back to the caller, so the agent is told what happened and loses
// nothing. That is the whole difference: the issue paths swallow the rejection
// into a 502 after the outcome is already spent, the MR path does not. Widening
// the clamp to MR titles is worth doing; it is not what closes #529.
func ClampIssueTitle(title string) string {
	// Flattened FIRST, so both the trim and the cut below see one line. A title
	// is a single line everywhere it is used and an interior line break is never
	// what the author meant, but on the deferred edit path it is worse than
	// cosmetic - see flattenTitleLines.
	title = flattenTitleLines(title)
	// Trimmed BEFORE the length check, not after it. A title only over the cap
	// because of the whitespace around it is not over it at all (the forge strips
	// before it length-validates), and trimming first also stops a long enough run
	// of LEADING whitespace from spending the entire cut and clamping the title
	// down to nothing but the marker.
	//
	// Doing it first is what makes a whitespace-only title collapse to "". Such a
	// title is UNDER the cap, so a length check in front of the trim returns it
	// verbatim and the forge gets a blank title it rejects. On the deferred edit
	// path that rejection is unrecoverable rather than merely noisy: EditIssue
	// 400s on every replay and the drain returns on that error before
	// removePendingComments, so the intent requeues forever and blocks every
	// intent queued behind it. Callers that must not send an empty title already
	// reject one upstream; this only stops whitespace from smuggling past them.
	title = strings.TrimSpace(title)
	if utf8.RuneCountInString(title) <= IssueTitleMaxChars {
		return title
	}
	cut := TruncateRunes(title, IssueTitleMaxChars-utf8.RuneCountInString(titleTruncatedMarker))
	return strings.TrimRightFunc(cut, unicode.IsSpace) + titleTruncatedMarker
}

// flattenTitleLines replaces every line break inside a title with a single
// space. Trimming alone does not reach an INTERIOR one, and an interior line
// break in a title is not cosmetic on the deferred edit path: that intent is
// carried in a LINE-ORIENTED encoding ("title: <t>\n<body>", encoded by
// restapi's editIntentBody and decoded by controller's parseEditIntent), and the
// decoder cuts the title at the first "\n". So title="Fix crash\nSTATUS: done"
// on an edit carrying NO body of its own decoded as title "Fix crash" plus body
// "STATUS: done", and the drain then handed that fragment to EditIssue as a body
// replacement - silently overwriting the issue body on the forge with a piece of
// its own title, while the agent was answered 200 {"deferred":true}.
//
// It flattens rather than rejecting because a rejected edit costs the agent its
// whole write and a flattened one costs it a newline. Doing it in the clamp is
// what puts it on the replay path as well as the queue path - though only for
// breaks that SURVIVED the encode: an interior "\n" in an intent queued before
// this existed is already a hard line break in the stored body, and no decoder
// can tell it from the real one. Those are not recoverable; new ones are.
//
// \r\n collapses to ONE space, not two. \v, \f and the Unicode NEL / LINE
// SEPARATOR / PARAGRAPH SEPARATOR are in the set because a forge, a terminal or
// a Markdown renderer will break a line on them even though strings.Cut does
// not. A title with none of them is returned BYTE-IDENTICAL, for the reason
// TruncateUTF8 gives.
func flattenTitleLines(title string) string {
	if !strings.ContainsAny(title, titleLineBreaks) {
		return title
	}
	title = strings.ReplaceAll(title, "\r\n", "\n")
	return strings.Map(func(r rune) rune {
		if strings.ContainsRune(titleLineBreaks, r) {
			return ' '
		}
		return r
	}, title)
}

// titleLineBreaks is the set flattenTitleLines rewrites: ASCII LF/CR/VT/FF plus
// U+0085 NEL, U+2028 LINE SEPARATOR and U+2029 PARAGRAPH SEPARATOR.
const titleLineBreaks = "\n\r\v\f\u0085\u2028\u2029"
