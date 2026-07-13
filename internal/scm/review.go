package scm

import (
	"errors"
	"fmt"
	"regexp"
	"time"
)

// ErrReviewRefused is a TERMINAL PostReview failure: the forge refused the write
// with a structural 4xx (422 "Can not approve your own pull request", 401, 403).
// Retrying cannot fix it, so the caller parks at review-post-refused instead of
// hot-requeueing - which is exactly what writeback_review.go does today with any
// Approve error, and why the platform would have spun forever on the first
// self-authored review.
var ErrReviewRefused = errors.New("scm: review post refused")

// ErrHeadMoved is returned by Merge when the caller pinned an expected head SHA
// and the forge answered 409 because the head had moved under it. It is
// deliberately distinct from ErrMergeConflict: a conflict means "this cannot
// merge", a moved head means "re-review the new head and try again".
var ErrHeadMoved = errors.New("scm: pr head moved since review")

// reviewEventComment is the ONLY review event this platform ever sends.
//
// With one bot identity, GitHub blocks the PR AUTHOR from making any review
// DECISION on their own PR: APPROVE and REQUEST_CHANGES both 422. Only COMMENT
// is permitted, and COMMENT is the only review event the platform has ever
// successfully posted on a self-authored PR. So the event is a CONSTANT, not a
// parameter: a one-member enum passed as an argument is an invitation to
// regress. The verdict lives in the review BODY ("## Review: approved" /
// "## Review: changes requested") and, authoritatively, in /outcome.
const reviewEventComment = "COMMENT"

// Review is one review-shaped record already on the forge. On GitHub it is a
// review object; on GitLab, which has no review object at all, it is a
// non-system MR note. It exists for the FORGE-SIDE idempotency check: if a body
// already carries the round marker, the post is skipped. The mirror cannot do
// this - only the forge knows what actually landed.
type Review struct {
	ID        string    `json:"id"`
	Body      string    `json:"body"`
	State     string    `json:"state,omitempty"`
	CommitID  string    `json:"commitId,omitempty"`
	Author    string    `json:"author,omitempty"`
	CreatedAt time.Time `json:"createdAt,omitempty"`
}

// PostedComment is one inline comment that is now ON the forge, with the id the
// forge assigned it. GitHub yields these from a SECOND read
// (ListReviewComments); GitLab yields them from the discussions listing.
type PostedComment struct {
	ExternalID string    `json:"externalId"`
	Path       string    `json:"path,omitempty"`
	Line       int       `json:"line,omitempty"`
	InReplyTo  string    `json:"inReplyTo,omitempty"`
	Body       string    `json:"body"`
	CreatedAt  time.Time `json:"createdAt"`
}

// ReviewFinding is one inline finding the reviewer wants anchored to a diff line.
type ReviewFinding struct {
	Path     string `json:"path"`
	Line     int    `json:"line"`
	Body     string `json:"body"`
	Severity string `json:"severity"` // critical|high|medium|low
}

// reviewMarkerRE matches the round marker the caller embeds in the review body:
//
//	<!-- tatara-review round=2 sha=abc123 -->
//
// It is the forge-side idempotency key. On GitHub, whose create-review is ONE
// atomic call, a body carrying this marker truthfully means "everything for this
// round landed". On GitLab, whose path is N+1 calls, the same marker is only
// truthful because it is written LAST (see GitLab.PostReview).
var reviewMarkerRE = regexp.MustCompile(`<!--\s*tatara-review\s+round=(\d+)\s+sha=(\S+?)\s*-->`)

// ParseReviewMarker extracts (round, sha) from a review/note body carrying the
// round marker. ok is false when the body carries no marker.
func ParseReviewMarker(body string) (round, sha string, ok bool) {
	m := reviewMarkerRE.FindStringSubmatch(body)
	if m == nil {
		return "", "", false
	}
	return m[1], m[2], true
}

// HasReviewMarker reports whether body carries the round marker for exactly
// (round, sha). This is the skip test of the forge-side dedup check.
func HasReviewMarker(body, round, sha string) bool {
	r, s, ok := ParseReviewMarker(body)
	return ok && r == round && s == sha
}

// ReviewMarker renders the round marker for (round, sha). Callers prepend it to
// the review body; the providers parse it back out to derive per-finding markers.
func ReviewMarker(round, sha string) string {
	return fmt.Sprintf("<!-- tatara-review round=%s sha=%s -->", round, sha)
}

// findingMarker renders the per-finding marker GitLab needs, because GitLab's
// post is N+1 calls and each unit must be individually skippable on a resumed
// run. k is the finding's index in the findings slice.
//
//	<!-- tatara-review round=2 sha=abc123 finding=3 -->
func findingMarker(round, sha string, k int) string {
	return fmt.Sprintf("<!-- tatara-review round=%s sha=%s finding=%d -->", round, sha, k)
}
