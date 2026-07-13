package scm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func readJSON(t *testing.T, r *http.Request) map[string]any {
	t.Helper()
	buf, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, json.Unmarshal(buf, &m))
	return m
}

// ---------------------------------------------------------------------------
// event=COMMENT, and NOTHING else, for BOTH verdicts
// ---------------------------------------------------------------------------

// The platform has ONE bot identity, so GitHub blocks the PR AUTHOR from making
// ANY review DECISION on its own PR: APPROVE and REQUEST_CHANGES BOTH 422. Only
// COMMENT is permitted. The fake forge below FAILS THE TEST if it ever receives
// either - and it never can, because the event is a constant, not a parameter:
// APPROVE and REQUEST_CHANGES are deleted from the PostReview event enum
// entirely, so no code path can produce them. The verdict rides in the BODY.
func TestGitHubPostReview_AlwaysCommentEvent_BothVerdicts(t *testing.T) {
	verdicts := map[string]string{
		"approved":          "<!-- tatara-review round=1 sha=abc123 -->\n## Review: approved\n\nlgtm",
		"changes requested": "<!-- tatara-review round=1 sha=abc123 -->\n## Review: changes requested\n\nfix it",
	}
	for name, body := range verdicts {
		t.Run(name, func(t *testing.T) {
			c := newGitHub(t, func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "/repos/o/r/pulls/5/reviews", r.URL.Path)
				in := readJSON(t, r)
				if in["event"] == "APPROVE" || in["event"] == "REQUEST_CHANGES" {
					t.Fatalf("PostReview sent event=%v: GitHub 422s both on a self-authored PR", in["event"])
				}
				require.Equal(t, "COMMENT", in["event"], "COMMENT is the only event this platform ever sends")
				require.Equal(t, body, in["body"], "the verdict lives in the review BODY")
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(`{"id":9001,"state":"COMMENTED"}`))
			})
			id, err := c.PostReview(context.Background(), "https://github.com/o/r", "tok", 5, body, nil)
			require.NoError(t, err)
			require.Equal(t, "9001", id)
		})
	}
}

// ---------------------------------------------------------------------------
// 422 / 401 / 403 are TERMINAL, not retryable
// ---------------------------------------------------------------------------

// writeback_review.go treats ANY Approve error as firstErr and REQUEUES FOREVER,
// re-driving the members until they all approve. They never can: 422 is
// structural. PostReview must therefore map a structural 4xx to a TYPED TERMINAL
// error, so the caller parks at review-post-refused instead of hot-requeueing.
func TestGitHubPostReview_StructuralFourXXIsTerminal(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
	}{
		{"self-approve 422", http.StatusUnprocessableEntity, `{"message":"Can not approve your own pull request."}`},
		{"401", http.StatusUnauthorized, `{"message":"Bad credentials"}`},
		{"403", http.StatusForbidden, `{"message":"Resource not accessible by integration"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newGitHub(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			})
			_, err := c.PostReview(context.Background(), "https://github.com/o/r", "tok", 5, "## Review: approved", nil)
			require.Error(t, err)
			require.True(t, errors.Is(err, ErrReviewRefused),
				"a %d must be TERMINAL (park at review-post-refused), never a hot requeue", tc.status)
			require.False(t, errors.Is(err, ErrRateLimited))

			// The 422 body must be PARSED, not swallowed: the operator has to be able
			// to say why it parked.
			var he *HTTPError
			require.ErrorAs(t, err, &he)
			require.Equal(t, tc.status, he.Status)
			require.Contains(t, he.Body, strings.Trim(strings.SplitN(tc.body, `"`, 5)[3], `"`))
		})
	}
}

// A 500 is NOT terminal: it stays retryable.
func TestGitHubPostReview_ServerErrorStaysRetryable(t *testing.T) {
	c := newGitHub(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	_, err := c.PostReview(context.Background(), "https://github.com/o/r", "tok", 5, "b", nil)
	require.Error(t, err)
	require.False(t, errors.Is(err, ErrReviewRefused))
}

// A rate-limit 403 is transient, not structural: it must NOT be classified
// terminal, or the first secondary-limit burst parks the Task forever.
func TestGitHubPostReview_RateLimit403IsNotTerminal(t *testing.T) {
	orig := ghRetrySleep
	ghRetrySleep = func(context.Context, time.Duration) error { return nil }
	t.Cleanup(func() { ghRetrySleep = orig })

	c := newGitHub(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"message":"You have exceeded a secondary rate limit."}`))
	})
	_, err := c.PostReview(context.Background(), "https://github.com/o/r", "tok", 5, "b", nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrRateLimited))
	require.False(t, errors.Is(err, ErrReviewRefused), "a throttle is not a refusal")
}

// ---------------------------------------------------------------------------
// PostReview returns ONLY the review id; the comment ids come from a SECOND read
// ---------------------------------------------------------------------------

// GitHub's POST /pulls/{n}/reviews returns the REVIEW object (id, body, state,
// commit_id) and does NOT return the created inline comments with their ids.
// The fake forge below reproduces that exactly (no `comments` array in the
// create response), and the caller must still end up with every posted comment's
// externalId, path, line, inReplyTo and createdAt - via ListReviewComments.
func TestGitHubPostReview_CommentIDsComeFromSecondRead(t *testing.T) {
	var createHits, listHits int
	c := newGitHub(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/o/r/pulls/5/reviews":
			createHits++
			in := readJSON(t, r)
			comments, _ := in["comments"].([]any)
			require.Len(t, comments, 2)
			// EXACTLY what GitHub returns: no comments array.
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"id":4242,"body":"b","state":"COMMENTED","commit_id":"abc123"}`))
		case "/repos/o/r/pulls/5/reviews/4242/comments":
			listHits++
			_, _ = w.Write([]byte(`[
              {"id":11,"path":"a.go","line":10,"body":"f1","created_at":"2026-07-12T10:00:00Z"},
              {"id":12,"path":"b.go","line":0,"original_line":20,"in_reply_to_id":11,"body":"f2","created_at":"2026-07-12T10:00:01Z"}
            ]`))
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	})

	findings := []ReviewFinding{
		{Path: "a.go", Line: 10, Body: "f1", Severity: "high"},
		{Path: "b.go", Line: 20, Body: "f2", Severity: "low"},
	}
	reviewID, err := c.PostReview(context.Background(), "https://github.com/o/r", "tok", 5, "body", findings)
	require.NoError(t, err)
	require.Equal(t, "4242", reviewID)
	require.Equal(t, 1, createHits)

	posted, err := c.ListReviewComments(context.Background(), "https://github.com/o/r", "tok", 5, reviewID)
	require.NoError(t, err)
	require.Equal(t, 1, listHits, "the comment ids come from a SECOND, SEPARATE read")
	require.Len(t, posted, 2)

	require.Equal(t, "11", posted[0].ExternalID)
	require.Equal(t, "a.go", posted[0].Path)
	require.Equal(t, 10, posted[0].Line)
	require.Empty(t, posted[0].InReplyTo)
	require.False(t, posted[0].CreatedAt.IsZero())

	require.Equal(t, "12", posted[1].ExternalID)
	require.Equal(t, 20, posted[1].Line, "a nulled line falls back to original_line, keeping the anchor")
	require.Equal(t, "11", posted[1].InReplyTo)
}

// ---------------------------------------------------------------------------
// ListReviews is the FORGE-SIDE idempotency check
// ---------------------------------------------------------------------------

// A forge whose reviews list already carries the round marker means the post
// landed, even if the process died before recording that it had. The caller
// SKIPS the post entirely. The mirror can never do this check; only the forge
// knows what is actually on the PR.
func TestGitHubListReviews_FindsRoundMarker(t *testing.T) {
	c := newGitHub(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/repos/o/r/pulls/5/reviews", r.URL.Path)
		_, _ = w.Write([]byte(`[
          {"id":1,"body":"an unrelated human review","state":"COMMENTED"},
          {"id":2,"body":"<!-- tatara-review round=2 sha=abc123 -->\n## Review: approved","state":"COMMENTED","commit_id":"abc123","user":{"login":"tatara-bot"}}
        ]`))
	})
	reviews, err := c.ListReviews(context.Background(), "https://github.com/o/r", "tok", 5)
	require.NoError(t, err)
	require.Len(t, reviews, 2)

	var found *Review
	for i := range reviews {
		if HasReviewMarker(reviews[i].Body, "2", "abc123") {
			found = &reviews[i]
		}
	}
	require.NotNil(t, found, "the caller SKIPS the post when the round marker is already on the forge")
	require.Equal(t, "2", found.ID, "and reuses THAT review's id to fetch the comment ids")
	require.Equal(t, "tatara-bot", found.Author)

	// A different round or a different sha is NOT a match: a new round must post.
	require.False(t, HasReviewMarker(found.Body, "3", "abc123"))
	require.False(t, HasReviewMarker(found.Body, "2", "def456"))
}

func TestReviewMarkerRoundTrip(t *testing.T) {
	m := ReviewMarker("7", "deadbeef")
	require.Equal(t, "<!-- tatara-review round=7 sha=deadbeef -->", m)
	round, sha, ok := ParseReviewMarker(m + "\n## Review: approved")
	require.True(t, ok)
	require.Equal(t, "7", round)
	require.Equal(t, "deadbeef", sha)
	_, _, ok = ParseReviewMarker("just a body")
	require.False(t, ok)
}

// ---------------------------------------------------------------------------
// Merge pins the head sha
// ---------------------------------------------------------------------------

func TestGitHubMerge_SendsSHAAndMapsConflictToErrHeadMoved(t *testing.T) {
	t.Run("sends sha", func(t *testing.T) {
		c := newGitHub(t, func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/repos/o/r/pulls/5/merge", r.URL.Path)
			in := readJSON(t, r)
			require.Equal(t, "abc123", in["sha"], "GitHub: PUT /pulls/{n}/merge with sha=<expectedHeadSHA>")
			require.Equal(t, "squash", in["merge_method"])
			_, _ = w.Write([]byte(`{"sha":"mergedsha"}`))
		})
		sha, err := c.Merge(context.Background(), "https://github.com/o/r", "tok", 5, "squash", "abc123")
		require.NoError(t, err)
		require.Equal(t, "mergedsha", sha)
	})

	t.Run("409 on a pinned merge is ErrHeadMoved", func(t *testing.T) {
		c := newGitHub(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"message":"Head branch was modified. Review and try the merge again."}`))
		})
		_, err := c.Merge(context.Background(), "https://github.com/o/r", "tok", 5, "squash", "abc123")
		require.True(t, errors.Is(err, ErrHeadMoved), "the caller re-reviews the new head")
		require.False(t, errors.Is(err, ErrMergeConflict), "a moved head is NOT an unmergeable PR")
	})

	t.Run("409 with no pin stays ErrMergeConflict", func(t *testing.T) {
		c := newGitHub(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusConflict)
		})
		_, err := c.Merge(context.Background(), "https://github.com/o/r", "tok", 5, "squash", "")
		require.True(t, errors.Is(err, ErrMergeConflict))
		require.False(t, errors.Is(err, ErrHeadMoved))
	})

	t.Run("405 is still ErrMergeConflict even with a pin", func(t *testing.T) {
		c := newGitHub(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusMethodNotAllowed)
		})
		_, err := c.Merge(context.Background(), "https://github.com/o/r", "tok", 5, "squash", "abc123")
		require.True(t, errors.Is(err, ErrMergeConflict))
		require.False(t, errors.Is(err, ErrHeadMoved))
	})
}

func TestGitLabMerge_SendsSHAAndMapsConflictToErrHeadMoved(t *testing.T) {
	t.Run("sends sha", func(t *testing.T) {
		c := newGitLab(t, func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/projects/g%2Fp/merge_requests/5/merge", r.URL.EscapedPath())
			in := readJSON(t, r)
			require.Equal(t, "abc123", in["sha"], "GitLab: PUT /merge_requests/{n}/merge with sha=<expectedHeadSHA>")
			require.Equal(t, true, in["squash"])
			_, _ = w.Write([]byte(`{"merge_commit_sha":"mergedsha"}`))
		})
		sha, err := c.Merge(context.Background(), "https://gitlab.com/g/p", "tok", 5, "squash", "abc123")
		require.NoError(t, err)
		require.Equal(t, "mergedsha", sha)
	})

	t.Run("409 on a pinned merge is ErrHeadMoved", func(t *testing.T) {
		c := newGitLab(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"message":"SHA does not match HEAD of source branch"}`))
		})
		_, err := c.Merge(context.Background(), "https://gitlab.com/g/p", "tok", 5, "squash", "abc123")
		require.True(t, errors.Is(err, ErrHeadMoved))
		require.False(t, errors.Is(err, ErrMergeConflict))
	})

	t.Run("406 is still ErrMergeConflict", func(t *testing.T) {
		c := newGitLab(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotAcceptable)
		})
		_, err := c.Merge(context.Background(), "https://gitlab.com/g/p", "tok", 5, "squash", "abc123")
		require.True(t, errors.Is(err, ErrMergeConflict))
		require.False(t, errors.Is(err, ErrHeadMoved))
	})
}

// ---------------------------------------------------------------------------
// GetPRHead
// ---------------------------------------------------------------------------

func TestGetPRHead(t *testing.T) {
	t.Run("github reads head.sha", func(t *testing.T) {
		c := newGitHub(t, func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/repos/o/r/pulls/5", r.URL.Path)
			_, _ = w.Write([]byte(`{"head":{"sha":"livehead"}}`))
		})
		sha, err := c.GetPRHead(context.Background(), "https://github.com/o/r", "tok", 5)
		require.NoError(t, err)
		require.Equal(t, "livehead", sha)
	})

	// GitLab's top-level .sha can LAG the branch. The head the diff is anchored
	// to is .diff_refs.head_sha, and a merge pinned to a lagging sha is not a pin.
	t.Run("gitlab reads diff_refs.head_sha, not .sha", func(t *testing.T) {
		c := newGitLab(t, func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/projects/g%2Fp/merge_requests/5", r.URL.EscapedPath())
			_, _ = w.Write([]byte(`{"sha":"stalesha","diff_refs":{"base_sha":"base1","start_sha":"start1","head_sha":"livehead"}}`))
		})
		sha, err := c.GetPRHead(context.Background(), "https://gitlab.com/g/p", "tok", 5)
		require.NoError(t, err)
		require.Equal(t, "livehead", sha)
	})
}

// ---------------------------------------------------------------------------
// GitLab: the whole review path is new code
// ---------------------------------------------------------------------------

// glForge is a fake GitLab that behaves like the real one: /discussions returns
// notes[].id on create, the MR carries diff_refs, and notes get monotonic ids.
type glForge struct {
	mu          sync.Mutex
	diffRefs    string // raw JSON for the diff_refs object, or "" for none
	discussions []glDiscussion
	notes       []glReviewNote
	nextID      int
	positions   []map[string]any // every position block the forge received
	failDiscOn  int              // fail the Nth (1-based) discussion POST; 0 = never
	discPosts   int
}

func (f *glForge) handler(t *testing.T) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		p := r.URL.EscapedPath()
		switch {
		case r.Method == http.MethodGet && p == "/projects/g%2Fp/merge_requests/5":
			refs := f.diffRefs
			if refs == "" {
				refs = `null`
			}
			_, _ = fmt.Fprintf(w, `{"sha":"stale","diff_refs":%s}`, refs)

		case r.Method == http.MethodGet && p == "/projects/g%2Fp/merge_requests/5/discussions":
			require.NoError(t, json.NewEncoder(w).Encode(f.discussions))

		case r.Method == http.MethodPost && p == "/projects/g%2Fp/merge_requests/5/discussions":
			f.discPosts++
			in := readJSON(t, r)
			if f.failDiscOn != 0 && f.discPosts == f.failDiscOn {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"message":"boom"}`))
				return
			}
			pos, _ := in["position"].(map[string]any)
			f.positions = append(f.positions, pos)
			f.nextID++
			n := glReviewNote{ID: f.nextID, Body: in["body"].(string)}
			if pos != nil {
				n.Position = &struct {
					NewPath string `json:"new_path"`
					NewLine int    `json:"new_line"`
					OldPath string `json:"old_path"`
					OldLine int    `json:"old_line"`
				}{NewPath: pos["new_path"].(string), NewLine: int(pos["new_line"].(float64))}
			}
			d := glDiscussion{ID: fmt.Sprintf("d%d", f.nextID), Notes: []glReviewNote{n}}
			f.discussions = append(f.discussions, d)
			w.WriteHeader(http.StatusCreated)
			require.NoError(t, json.NewEncoder(w).Encode(d))

		case r.Method == http.MethodPost && p == "/projects/g%2Fp/merge_requests/5/notes":
			in := readJSON(t, r)
			f.nextID++
			n := glReviewNote{ID: f.nextID, Body: in["body"].(string)}
			f.notes = append(f.notes, n)
			w.WriteHeader(http.StatusCreated)
			require.NoError(t, json.NewEncoder(w).Encode(n))

		case r.Method == http.MethodGet && strings.HasPrefix(p, "/projects/g%2Fp/merge_requests/5/notes/"):
			id := strings.TrimPrefix(p, "/projects/g%2Fp/merge_requests/5/notes/")
			for _, n := range f.notes {
				if fmt.Sprint(n.ID) == id {
					require.NoError(t, json.NewEncoder(w).Encode(n))
					return
				}
			}
			w.WriteHeader(http.StatusNotFound)

		case r.Method == http.MethodGet && p == "/projects/g%2Fp/merge_requests/5/notes":
			all := append([]glReviewNote{}, f.notes...)
			for _, d := range f.discussions {
				all = append(all, d.Notes...)
			}
			require.NoError(t, json.NewEncoder(w).Encode(all))

		default:
			t.Fatalf("unexpected %s %s", r.Method, p)
		}
	}
}

const glGoodRefs = `{"base_sha":"base1","start_sha":"start1","head_sha":"head1"}`

// Every discussion must carry a position block with ALL THREE shas. gitlab.go
// had no position handling at all: Suggest() posted plain notes with the path and
// line baked into the body as markdown text, anchored to nothing.
func TestGitLabPostReview_PositionCarriesAllThreeSHAs(t *testing.T) {
	f := &glForge{diffRefs: glGoodRefs}
	c := newGitLab(t, f.handler(t))

	body := ReviewMarker("1", "head1") + "\n## Review: changes requested\n\ndetail"
	findings := []ReviewFinding{
		{Path: "a.go", Line: 10, Body: "f0", Severity: "high"},
		{Path: "b.go", Line: 20, Body: "f1", Severity: "low"},
	}
	id, err := c.PostReview(context.Background(), "https://gitlab.com/g/p", "tok", 5, body, findings)
	require.NoError(t, err)
	require.NotEmpty(t, id)

	require.Len(t, f.positions, 2)
	for i, pos := range f.positions {
		require.Equal(t, "base1", pos["base_sha"], "position %d", i)
		require.Equal(t, "start1", pos["start_sha"], "position %d", i)
		require.Equal(t, "head1", pos["head_sha"], "position %d", i)
		require.Equal(t, "text", pos["position_type"], "position %d", i)
	}
	require.Equal(t, "a.go", f.positions[0]["new_path"])
	require.Equal(t, float64(10), f.positions[0]["new_line"])
	require.Equal(t, "b.go", f.positions[1]["new_path"])
	require.Equal(t, float64(20), f.positions[1]["new_line"])

	// The ORDER is load-bearing: findings first, the marker-carrying body note LAST.
	require.Len(t, f.notes, 1)
	require.True(t, HasReviewMarker(f.notes[0].Body, "1", "head1"))
	require.Greater(t, f.notes[0].ID, f.discussions[len(f.discussions)-1].Notes[0].ID,
		"the body note must be posted AFTER every discussion")
	require.Equal(t, fmt.Sprint(f.notes[0].ID), id, "the returned id is the body note's")
}

// A finding with no diff_refs available is a HARD ERROR, not a silently-plain
// note. An unanchored finding is a finding attached to nothing, which is exactly
// the old Suggest() defect; posting one and stamping the round complete would
// tell the next implement pod a fix landed where it did not.
func TestGitLabPostReview_NoDiffRefsIsHardError(t *testing.T) {
	f := &glForge{diffRefs: ""} // MR returns diff_refs: null
	c := newGitLab(t, f.handler(t))

	_, err := c.PostReview(context.Background(), "https://gitlab.com/g/p", "tok", 5,
		ReviewMarker("1", "head1")+"\nbody",
		[]ReviewFinding{{Path: "a.go", Line: 1, Body: "f0"}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "diff_refs")
	require.Empty(t, f.discussions, "nothing may be posted unanchored")
	require.Empty(t, f.notes, "and the round marker must NOT land")
}

// THE CRASH-RESUME TEST. GitLab's post is N+1 calls, so a marker written FIRST
// would mean "we started", not "everything landed": a crash midway through the
// discussions would leave the marker present, the findings missing, and a re-run
// that SKIPS them forever - on the forge that owns tatara-helmfile.
//
// So the findings go FIRST, each carrying its own marker, and the round marker
// goes LAST. A post killed after the 2nd of 5 discussions and re-run must end
// with EXACTLY 5 discussions and 1 body note.
func TestGitLabPostReview_KilledAfterSecondOfFive_ResumesToExactlyFiveAndOne(t *testing.T) {
	f := &glForge{diffRefs: glGoodRefs, failDiscOn: 3} // die on the 3rd discussion
	c := newGitLab(t, f.handler(t))

	body := ReviewMarker("2", "head1") + "\n## Review: changes requested\n\nfive things"
	findings := make([]ReviewFinding, 5)
	for i := range findings {
		findings[i] = ReviewFinding{Path: fmt.Sprintf("f%d.go", i), Line: i + 1, Body: fmt.Sprintf("finding %d", i)}
	}

	// Run 1: dies partway through.
	_, err := c.PostReview(context.Background(), "https://gitlab.com/g/p", "tok", 5, body, findings)
	require.Error(t, err, "the 3rd discussion POST failed")
	require.Len(t, f.discussions, 2, "exactly 2 discussions landed before the crash")
	require.Empty(t, f.notes, "the ROUND MARKER MUST NOT BE ON THE FORGE: the round did not complete")

	// Run 2: the same call, re-entered. It lists the discussions, sees the two
	// markers it already wrote, and posts only the remaining three.
	f.mu.Lock()
	f.failDiscOn = 0
	f.mu.Unlock()

	id, err := c.PostReview(context.Background(), "https://gitlab.com/g/p", "tok", 5, body, findings)
	require.NoError(t, err)

	require.Len(t, f.discussions, 5, "EXACTLY 5 discussions: no duplicates, none lost")
	require.Len(t, f.notes, 1, "EXACTLY 1 body note")
	require.True(t, HasReviewMarker(f.notes[0].Body, "2", "head1"),
		"and only NOW does the round marker mean everything for this round is on the forge")

	// Each finding is present exactly once, in order, each with its own marker.
	for k := range findings {
		want := findingMarker("2", "head1", k)
		hits := 0
		for _, d := range f.discussions {
			if strings.Contains(d.Notes[0].Body, want) {
				hits++
			}
		}
		require.Equal(t, 1, hits, "finding %d must appear exactly once", k)
	}

	// And the comment ids come back for the mirror, from the forge, on both runs.
	posted, err := c.ListReviewComments(context.Background(), "https://gitlab.com/g/p", "tok", 5, id)
	require.NoError(t, err)
	require.Len(t, posted, 5)
	for i, pc := range posted {
		require.NotEmpty(t, pc.ExternalID)
		require.Equal(t, fmt.Sprintf("f%d.go", i), pc.Path)
		require.Equal(t, i+1, pc.Line)
	}
}

// A fully re-run post (nothing failed, just re-entered - the crash-after-post,
// before-mirror-append window) posts NOTHING new beyond the body note it must
// re-post... which is why the CALLER's forge-side dedup (ListReviews + the round
// marker) exists. This test pins the half that lives in the SCM layer: the
// findings are never duplicated.
func TestGitLabPostReview_ReRunDoesNotDuplicateFindings(t *testing.T) {
	f := &glForge{diffRefs: glGoodRefs}
	c := newGitLab(t, f.handler(t))
	body := ReviewMarker("1", "head1") + "\nbody"
	findings := []ReviewFinding{{Path: "a.go", Line: 3, Body: "f0"}}

	_, err := c.PostReview(context.Background(), "https://gitlab.com/g/p", "tok", 5, body, findings)
	require.NoError(t, err)
	_, err = c.PostReview(context.Background(), "https://gitlab.com/g/p", "tok", 5, body, findings)
	require.NoError(t, err)

	require.Len(t, f.discussions, 1, "the per-finding marker makes a re-run a no-op on the discussions")
}

// UNAPPROVE IS NOT SENT. The bot never approves on GitLab either: the merge is
// the approval of record, on both forges, identically.
func TestGitLabPostReview_NeverUnapproves(t *testing.T) {
	f := &glForge{diffRefs: glGoodRefs}
	c := newGitLab(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/unapprove") || strings.Contains(r.URL.Path, "/approve") ||
			strings.Contains(r.URL.Path, "/award_emoji") {
			t.Fatalf("PostReview must never touch %s: the merge is the approval of record", r.URL.Path)
		}
		f.handler(t)(w, r)
	})
	_, err := c.PostReview(context.Background(), "https://gitlab.com/g/p", "tok", 5,
		ReviewMarker("1", "head1")+"\n## Review: approved", nil)
	require.NoError(t, err)
}

// GitLab's ListReviews filters system notes out ("added label", "changed the
// description") - they are not review content, and one of them carrying the word
// "tatara-review" would be a false skip.
func TestGitLabListReviews_FiltersSystemNotesAndFindsMarker(t *testing.T) {
	c := newGitLab(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/projects/g%2Fp/merge_requests/5/notes", r.URL.EscapedPath())
		_, _ = w.Write([]byte(`[
          {"id":1,"body":"assigned to @bot","system":true},
          {"id":2,"body":"<!-- tatara-review round=2 sha=abc123 -->\n## Review: approved","system":false,"author":{"username":"tatara-bot"}}
        ]`))
	})
	reviews, err := c.ListReviews(context.Background(), "https://gitlab.com/g/p", "tok", 5)
	require.NoError(t, err)
	require.Len(t, reviews, 1, "system notes are not reviews")
	require.Equal(t, "2", reviews[0].ID)
	require.True(t, HasReviewMarker(reviews[0].Body, "2", "abc123"))
}
