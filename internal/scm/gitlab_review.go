package scm

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/szymonrychu/tatara-operator/internal/obs"
)

// GitLab has NO review object. Everything below is new code, not a mapping of
// existing code: gitlab.go had no `position` handling and no `discussion`
// handling anywhere, and its Suggest() posts plain notes with the path and line
// baked into the body as markdown text, anchored to nothing. This file gives
// GitLab a real, diff-anchored, crash-safe review post - which matters because
// the GitLab project is the one that owns tatara-helmfile, whose deploy runner
// is cluster-admin scoped.

// glDiffRefs are the three shas a GitLab position block needs. All three come
// from ONE read of the MR, cached for the duration of a post.
type glDiffRefs struct {
	BaseSHA  string `json:"base_sha"`
	StartSHA string `json:"start_sha"`
	HeadSHA  string `json:"head_sha"`
}

func (d glDiffRefs) complete() bool {
	return d.BaseSHA != "" && d.StartSHA != "" && d.HeadSHA != ""
}

// glReviewNote is one MR note, whether standalone (the body note) or inside a
// discussion (an inline finding).
type glReviewNote struct {
	ID        int       `json:"id"`
	Body      string    `json:"body"`
	System    bool      `json:"system"`
	CreatedAt time.Time `json:"created_at"`
	Author    struct {
		Username string `json:"username"`
	} `json:"author"`
	Position *struct {
		NewPath string `json:"new_path"`
		NewLine int    `json:"new_line"`
		OldPath string `json:"old_path"`
		OldLine int    `json:"old_line"`
	} `json:"position"`
}

type glDiscussion struct {
	ID    string         `json:"id"`
	Notes []glReviewNote `json:"notes"`
}

// glDiffRefsOf reads the MR's diff_refs. GetPRHead uses it too: the head sha
// GitLab reports in .diff_refs.head_sha is the one the diff is anchored to,
// whereas the MR's top-level .sha can lag, and a merge pinned to a lagging sha
// is not a pin at all.
func (c *GitLab) glDiffRefsOf(ctx context.Context, proj, token string, number int) (glDiffRefs, error) {
	var mr struct {
		DiffRefs glDiffRefs `json:"diff_refs"`
	}
	path := "/projects/" + url.PathEscape(proj) + "/merge_requests/" + strconv.Itoa(number)
	if err := glDo(ctx, c.base(), http.MethodGet, path, token, nil, &mr); err != nil {
		return glDiffRefs{}, err
	}
	return mr.DiffRefs, nil
}

// glMRDiff is one changed file in the MR diff. Diff is the raw unified diff whose
// @@ headers carry the new-side hunk ranges a text position can anchor to.
type glMRDiff struct {
	OldPath string `json:"old_path"`
	NewPath string `json:"new_path"`
	Diff    string `json:"diff"`
}

// glMRDiffsOf reads the MR's per-file diffs ONCE. The result feeds the hunk index
// that decides which findings are anchorable inline (#394).
func (c *GitLab) glMRDiffsOf(ctx context.Context, proj, token string, number int) ([]glMRDiff, error) {
	path := "/projects/" + url.PathEscape(proj) + "/merge_requests/" + strconv.Itoa(number) + "/diffs?per_page=100"
	return glDoPaged[glMRDiff](ctx, c.base(), path, token)
}

// glHunkIndex maps a new-side file path to the ranges of new-side line numbers
// present in the MR diff. A GitLab text position anchors ONLY to a line inside one
// of these ranges; a finding outside them (or with no line) has no line_code and
// the POST 400s "line_code can't be blank" (#394), so it must not be posted as a
// discussion.
type glHunkIndex map[string][][2]int

func (h glHunkIndex) anchorable(path string, line int) bool {
	if line <= 0 {
		return false
	}
	for _, r := range h[path] {
		if line >= r[0] && line <= r[1] {
			return true
		}
	}
	return false
}

// glHunkHeaderRE captures the new-side start and (optional) count from a unified
// diff hunk header: "@@ -a,b +c,d @@" -> c, d. A missing count means 1 line.
var glHunkHeaderRE = regexp.MustCompile(`@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@`)

func glBuildHunkIndex(diffs []glMRDiff) glHunkIndex {
	idx := make(glHunkIndex, len(diffs))
	for _, d := range diffs {
		path := d.NewPath
		if path == "" {
			path = d.OldPath
		}
		for _, m := range glHunkHeaderRE.FindAllStringSubmatch(d.Diff, -1) {
			start, err := strconv.Atoi(m[1])
			if err != nil {
				continue
			}
			count := 1
			if m[2] != "" {
				count, _ = strconv.Atoi(m[2])
			}
			if count <= 0 {
				continue // a pure-deletion hunk has no new-side line to anchor to
			}
			idx[path] = append(idx[path], [2]int{start, start + count - 1})
		}
	}
	return idx
}

// GetPRHead reads the LIVE head sha of an MR from .diff_refs.head_sha.
func (c *GitLab) GetPRHead(ctx context.Context, repoURL, token string, number int) (string, error) {
	proj, err := glProjectPath(repoURL)
	if err != nil {
		return "", err
	}
	refs, err := c.glDiffRefsOf(ctx, proj, token, number)
	if err != nil {
		return "", err
	}
	if refs.HeadSHA == "" {
		return "", fmt.Errorf("gitlab: mr %s!%d returned no diff_refs.head_sha", proj, number)
	}
	return refs.HeadSHA, nil
}

// glMRReviewNotes lists the MR's notes (standalone notes AND the notes inside
// discussions both appear here), oldest-first, with system notes filtered out.
func (c *GitLab) glMRReviewNotes(ctx context.Context, proj, token string, number int) ([]glReviewNote, error) {
	path := "/projects/" + url.PathEscape(proj) + "/merge_requests/" + strconv.Itoa(number) +
		"/notes?per_page=100&sort=asc&order_by=created_at"
	raw, err := glDoPaged[glReviewNote](ctx, c.base(), path, token)
	if err != nil {
		return nil, err
	}
	out := make([]glReviewNote, 0, len(raw))
	for _, n := range raw {
		if n.System {
			continue // "changed the description", "added label" - not review content
		}
		out = append(out, n)
	}
	return out, nil
}

// glDiscussions lists the MR's discussions.
func (c *GitLab) glDiscussions(ctx context.Context, proj, token string, number int) ([]glDiscussion, error) {
	path := "/projects/" + url.PathEscape(proj) + "/merge_requests/" + strconv.Itoa(number) +
		"/discussions?per_page=100"
	return glDoPaged[glDiscussion](ctx, c.base(), path, token)
}

// ListReviews returns the MR's non-system notes as Review records. GitLab has no
// review object, so a note IS the review record: the round marker lives in the
// body of the body-note, which is exactly what the forge-side dedup check reads.
func (c *GitLab) ListReviews(ctx context.Context, repoURL, token string, number int) ([]Review, error) {
	proj, err := glProjectPath(repoURL)
	if err != nil {
		return nil, err
	}
	notes, err := c.glMRReviewNotes(ctx, proj, token, number)
	if err != nil {
		return nil, err
	}
	out := make([]Review, 0, len(notes))
	for _, n := range notes {
		out = append(out, Review{
			ID:        strconv.Itoa(n.ID),
			Body:      n.Body,
			Author:    n.Author.Username,
			CreatedAt: n.CreatedAt,
		})
	}
	return out, nil
}

// PostReview posts a review to GitLab as N diff-anchored discussions plus ONE
// body note. There is no event: GitLab, like GitHub, never gets an approval from
// the bot - the merge is the approval of record on both forges, identically. So
// UNAPPROVE is not sent either.
//
// THE ORDER IS INVERTED RELATIVE TO GITHUB, AND THAT IS THE FIX.
//
// GitHub's create-review is ONE atomic call, so a single marker in the body
// truthfully means "everything for this round landed". GitLab's path is N+1
// calls. A marker posted FIRST would mean only "we started" - and a crash midway
// through the discussions would leave the marker present, the findings missing,
// and a re-run that reads the marker, SKIPS, and never posts them. On the forge
// that owns tatara-helmfile. So:
//
//  1. every finding first, each carrying its OWN marker, skipping the ones
//     already on the MR (which is what makes a resumed post converge);
//  2. the body note LAST, carrying the ROUND marker.
//
// The round marker therefore means "EVERYTHING for this round is on the forge",
// which is the only thing that makes the caller's forge-side skip safe here.
// A marker written before the work it guards is not an idempotency key; it is a
// lie with a timestamp.
//
// The returned id is the BODY NOTE's id. The inline note ids are read back by
// ListReviewComments, so a resumed post and a skipped post reconcile the mirror
// identically.
func (c *GitLab) PostReview(ctx context.Context, repoURL, token string, number int, body string, findings []ReviewFinding) (string, error) {
	proj, err := glProjectPath(repoURL)
	if err != nil {
		return "", err
	}
	round, sha, _ := ParseReviewMarker(body)

	// Step 0: the diff_refs. All three shas come from here, read ONCE and reused
	// for every discussion in this post.
	refs, err := c.glDiffRefsOf(ctx, proj, token, number)
	if err != nil {
		return "", classifyReviewPostError(err)
	}
	if len(findings) > 0 && !refs.complete() {
		// Hard error, NOT a silent degradation to a plain note. A finding with no
		// position is a finding anchored to nothing, which is the exact defect in
		// the old Suggest() path; posting one and calling the round done would
		// tell the next implement pod that a fix landed where it did not.
		return "", fmt.Errorf("gitlab: mr %s!%d has incomplete diff_refs (base=%q start=%q head=%q); cannot anchor %d review findings",
			proj, number, refs.BaseSHA, refs.StartSHA, refs.HeadSHA, len(findings))
	}

	// The new-side hunk ranges, read ONCE. A finding whose (path, line) falls
	// outside every hunk - or that carries no line at all (a file-level finding,
	// #398) - can NEVER be a GitLab text position: the discussion POST 400s
	// "line_code can't be blank" (#394). Such findings degrade to a plain note
	// below rather than being posted inline and looping the caller forever.
	var hunks glHunkIndex
	if len(findings) > 0 {
		diffs, derr := c.glMRDiffsOf(ctx, proj, token, number)
		if derr != nil {
			return "", classifyReviewPostError(derr)
		}
		hunks = glBuildHunkIndex(diffs)
	}

	// Re-entrancy: which per-finding markers are already on the MR? A post killed
	// after the 2nd of 5 discussions must resume at the 3rd, not re-post the first
	// two and not skip the last three.
	existing, err := c.glDiscussions(ctx, proj, token, number)
	if err != nil {
		return "", classifyReviewPostError(err)
	}
	present := make(map[string]bool, len(existing))
	for _, d := range existing {
		for _, n := range d.Notes {
			for k := range findings {
				m := findingMarker(round, sha, k)
				if strings.Contains(n.Body, m) {
					present[m] = true
				}
			}
		}
	}

	// Step 1: FINDINGS FIRST. Anchorable findings post as diff-anchored
	// discussions; unanchorable ones degrade to a plain note carrying the same
	// marker + body. A degrade is WARN + metric, never fatal: one bad finding must
	// not block the rest of the round.
	posted, skipped, degraded := 0, 0, 0
	for k, f := range findings {
		marker := findingMarker(round, sha, k)
		if present[marker] {
			skipped++
			continue
		}
		body := marker + "\n" + f.Body
		if !hunks.anchorable(f.Path, f.Line) {
			if err := c.degradeFinding(ctx, proj, token, number, body, f, round, sha, k, "unanchorable"); err != nil {
				return "", classifyReviewPostError(err)
			}
			degraded++
			continue
		}
		if err := c.postDiscussion(ctx, proj, token, number, refs, body, f); err != nil {
			// A deterministically REFUSED inline post (a 4xx classified terminal, e.g.
			// a residual line_code 400) degrades to a note so the round still lands. A
			// RETRYABLE failure (5xx, network) still aborts WITHOUT the body note, so a
			// re-run re-lists, skips what landed, and posts the rest.
			cerr := classifyReviewPostError(err)
			if errors.Is(cerr, ErrReviewRefused) {
				if derr := c.degradeFinding(ctx, proj, token, number, body, f, round, sha, k, "post-refused"); derr != nil {
					return "", classifyReviewPostError(derr)
				}
				degraded++
				continue
			}
			slog.ErrorContext(ctx, "gitlab: review discussion post failed mid-round",
				"provider", "gitlab", "action", "scm_post_review", "resource_id", fmt.Sprintf("%s!%d", proj, number),
				"round", round, "sha", sha, "finding", k, "posted", posted, "total", len(findings), "error", err.Error())
			return "", cerr
		}
		posted++
	}

	// Step 2: THE BODY NOTE LAST, carrying the round marker.
	noteID, err := c.postMRNote(ctx, proj, token, number, body)
	if err != nil {
		return "", classifyReviewPostError(err)
	}
	slog.InfoContext(ctx, "gitlab: review posted",
		"provider", "gitlab", "action", "scm_post_review", "resource_id", fmt.Sprintf("%s!%d", proj, number),
		"round", round, "sha", sha, "findings_posted", posted, "findings_resumed", skipped,
		"findings_degraded", degraded, "note_id", noteID)
	return noteID, nil
}

// degradeFinding posts an unanchorable finding as a plain MR note (carrying its
// per-finding marker + body, exactly like the inline path would) instead of a
// diff-anchored discussion, and records the WARN + degrade metric. reason is
// "unanchorable" (line outside every hunk / no line) or "post-refused" (the
// inline POST was deterministically refused). The note carries the same marker,
// so the caller's re-list dedup and mirror reconcile treat it uniformly.
func (c *GitLab) degradeFinding(ctx context.Context, proj, token string, number int,
	body string, f ReviewFinding, round, sha string, k int, reason string) error {
	slog.WarnContext(ctx, "gitlab: review finding not anchorable inline; degrading to a plain note",
		"provider", "gitlab", "action", "scm_post_review", "resource_id", fmt.Sprintf("%s!%d", proj, number),
		"round", round, "sha", sha, "finding", k, "path", f.Path, "line", f.Line, "reason", reason)
	if _, err := c.postMRNote(ctx, proj, token, number, body); err != nil {
		return err
	}
	obs.ReviewFindingDegraded("gitlab", reason)
	return nil
}

// postDiscussion posts one diff-anchored discussion. The position block carries
// all three shas from diff_refs; without them GitLab rejects the position and
// the note would silently become an unanchored comment.
func (c *GitLab) postDiscussion(ctx context.Context, proj, token string, number int, refs glDiffRefs, body string, f ReviewFinding) error {
	in := map[string]any{
		"body": body,
		"position": map[string]any{
			"base_sha":      refs.BaseSHA,
			"start_sha":     refs.StartSHA,
			"head_sha":      refs.HeadSHA,
			"position_type": "text",
			"new_path":      f.Path,
			"old_path":      f.Path,
			"new_line":      f.Line,
		},
	}
	path := "/projects/" + url.PathEscape(proj) + "/merge_requests/" + strconv.Itoa(number) + "/discussions"
	var out glDiscussion
	return glDo(ctx, c.base(), http.MethodPost, path, token, in, &out)
}

// postMRNote posts a standalone MR note and returns its id. gitlab.go's mrNote()
// discards the response; the review post needs the id back.
func (c *GitLab) postMRNote(ctx context.Context, proj, token string, number int, body string) (string, error) {
	path := "/projects/" + url.PathEscape(proj) + "/merge_requests/" + strconv.Itoa(number) + "/notes"
	var out glReviewNote
	if err := glDo(ctx, c.base(), http.MethodPost, path, token, map[string]string{"body": body}, &out); err != nil {
		return "", err
	}
	if out.ID == 0 {
		return "", fmt.Errorf("gitlab: mr %s!%d note post returned no id", proj, number)
	}
	return strconv.Itoa(out.ID), nil
}

// ListReviewComments returns the inline discussion notes belonging to the round
// whose body note is reviewID.
//
// The contract calls this "a no-op on GitLab, because POST /discussions already
// returned notes[].id". That is only true on the POST path. It is FALSE on the
// SKIP path: when the forge-side dedup check finds the round marker already
// present, no POST happens, so nothing returned any ids - and the caller still
// has to reconcile the mirror. So this is a real read on GitLab too, and both
// paths converge on the same set of comments.
//
// It resolves the round from reviewID's own body (the round marker the body note
// carries), then returns every discussion note carrying a finding marker for
// that round.
func (c *GitLab) ListReviewComments(ctx context.Context, repoURL, token string, number int, reviewID string) ([]PostedComment, error) {
	proj, err := glProjectPath(repoURL)
	if err != nil {
		return nil, err
	}
	if reviewID == "" {
		return nil, errors.New("gitlab: list review comments: empty review id")
	}
	var bodyNote glReviewNote
	notePath := "/projects/" + url.PathEscape(proj) + "/merge_requests/" + strconv.Itoa(number) +
		"/notes/" + url.PathEscape(reviewID)
	if err := glDo(ctx, c.base(), http.MethodGet, notePath, token, nil, &bodyNote); err != nil {
		return nil, err
	}
	round, sha, ok := ParseReviewMarker(bodyNote.Body)
	if !ok {
		// The body note carries no round marker, so no finding marker can be
		// matched against it. Returning an error rather than an empty slice keeps
		// a malformed post from silently mirroring zero comments.
		return nil, fmt.Errorf("gitlab: mr %s!%d note %s carries no tatara-review round marker", proj, number, reviewID)
	}
	discussions, err := c.glDiscussions(ctx, proj, token, number)
	if err != nil {
		return nil, err
	}
	var out []PostedComment
	for _, d := range discussions {
		for i, n := range d.Notes {
			if n.System || !isRoundFindingNote(n.Body, round, sha) {
				continue
			}
			pc := PostedComment{
				ExternalID: strconv.Itoa(n.ID),
				Body:       n.Body,
				CreatedAt:  n.CreatedAt,
			}
			if n.Position != nil {
				pc.Path = n.Position.NewPath
				pc.Line = n.Position.NewLine
				if pc.Path == "" {
					pc.Path = n.Position.OldPath
				}
				if pc.Line == 0 {
					pc.Line = n.Position.OldLine
				}
			}
			if i > 0 {
				pc.InReplyTo = strconv.Itoa(d.Notes[0].ID)
			}
			out = append(out, pc)
		}
	}
	return out, nil
}

// isRoundFindingNote reports whether a note body carries a finding marker for
// (round, sha). The finding index is not bounded here: the note is claimed by the
// round, whatever K it carried.
func isRoundFindingNote(body, round, sha string) bool {
	return strings.Contains(body, fmt.Sprintf("<!-- tatara-review round=%s sha=%s finding=", round, sha))
}
