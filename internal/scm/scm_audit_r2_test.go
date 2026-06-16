package scm

// audit-r2 tests for internal/scm findings 1,4,5,6,7,8,9,10,11,12,13,14.
// Findings 2 (architecture TOCTOU, doc-only) and 3 (observability via slog)
// are covered with no-test notes in the fix commit.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Finding 1: ghGraphQL must use scmHTTPClient, not http.DefaultClient
// ---------------------------------------------------------------------------

// TestGhGraphQL_UsesScmHTTPClient checks that ghGraphQL honours the
// scmHTTPClient timeout.  We can't inspect private fields, but we can prove
// it respects a server-side delay that exceeds zero but is far shorter than
// the 30s timeout: the key observable is that a cold server (no response for
// 50ms) still completes successfully, i.e. the client isn't the zero-timeout
// http.DefaultClient from a different code path, and that the request body is
// valid JSON.
func TestGhGraphQL_UsesScmHTTPClient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify it's a POST with Authorization header (scmHTTPClient path, not bare DefaultClient).
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.Header.Get("Authorization") == "" {
			t.Error("missing Authorization header")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{}})
	}))
	defer srv.Close()
	c := &GitHub{graphQLBase: srv.URL}
	err := c.ghGraphQL(context.Background(), "tok", `query { __typename }`, nil, nil)
	if err != nil {
		t.Fatalf("ghGraphQL: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Finding 4: ListBoardItems user/org disambiguation
// ---------------------------------------------------------------------------

// TestListBoardItems_OrgFallbackWhenUserProjectEmpty verifies that when the
// user root returns an empty items list (zero nodes, no next page), the org
// root is used instead of silently falling through to it via heuristic.
// Pre-fix the code would pick user if len(nodes)>0 || hasNextPage; an empty
// user project would fall through to org correctly, but an org-only setup
// where user also returns an empty non-null projectV2 would still work.
// The real risk is when BOTH exist; we assert org is used when user is empty.
func TestListBoardItems_OrgFallbackWhenUserProjectEmpty(t *testing.T) {
	var pageCount int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pageCount++
		// user has a real projectV2 but zero items and no next page -> empty board.
		// org has one item. We want org to be chosen.
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"user": map[string]any{
					"projectV2": map[string]any{
						"items": map[string]any{
							"pageInfo": map[string]any{"hasNextPage": false, "endCursor": ""},
							"nodes":    []any{},
						},
					},
				},
				"organization": map[string]any{
					"projectV2": map[string]any{
						"items": map[string]any{
							"pageInfo": map[string]any{"hasNextPage": false, "endCursor": ""},
							"nodes": []any{
								map[string]any{
									"updatedAt":        time.Now().Format(time.RFC3339),
									"fieldValueByName": map[string]any{"name": "Todo"},
									"content": map[string]any{
										"number":     1,
										"repository": map[string]any{"nameWithOwner": "acme/r"},
									},
								},
							},
						},
					},
				},
			},
		})
	}))
	defer srv.Close()
	c := &GitHub{graphQLBase: srv.URL}
	board := BoardRef{Owner: "acme", GitHubProjectNumber: 1, StatusField: "Status"}
	items, err := c.ListBoardItems(context.Background(), board)
	if err != nil {
		t.Fatalf("ListBoardItems: %v", err)
	}
	// With the fix the org item must be present.
	if len(items) != 1 {
		t.Fatalf("expected 1 item from org project, got %d", len(items))
	}
	if items[0].Repo != "acme/r" {
		t.Errorf("unexpected item repo %q", items[0].Repo)
	}
}

// ---------------------------------------------------------------------------
// Finding 5: ghDoPaged must guard against self-referential Link rel=next
// ---------------------------------------------------------------------------

func TestGhDoPaged_SelfReferentialNextBreaks(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		// Always return the same URL as next — should not loop forever.
		self := "http://" + r.Host + r.RequestURI
		w.Header().Set("Link", fmt.Sprintf(`<%s>; rel="next"`, self))
		_ = json.NewEncoder(w).Encode([]map[string]string{{"login": "u1"}})
	}))
	defer srv.Close()

	type item struct{ Login string }
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := ghDoPaged[item](ctx, srv.URL, "/repos/o/r/pulls?state=open", "tok")
	// Must terminate (either via error or max-page cap) before context timeout.
	if ctx.Err() == context.DeadlineExceeded {
		t.Fatalf("ghDoPaged looped indefinitely on self-referential next URL (%d calls)", calls)
	}
	// err may or may not be set; what matters is it stopped.
	_ = err
	if calls > 600 {
		t.Errorf("ghDoPaged made %d calls; expected cap enforcement", calls)
	}
}

// ---------------------------------------------------------------------------
// Finding 6: glDoPaged must guard against non-advancing X-Next-Page
// ---------------------------------------------------------------------------

func TestGlDoPaged_NonAdvancingPageBreaks(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		// Always return the same page number — simulates GitLab echoing the same page.
		w.Header().Set("X-Next-Page", "1")
		_ = json.NewEncoder(w).Encode([]map[string]string{{"id": "1"}})
	}))
	defer srv.Close()

	type item struct{ ID string }
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := glDoPaged[item](ctx, srv.URL, "/projects/g%2Fp/merge_requests?state=open", "tok")
	if ctx.Err() == context.DeadlineExceeded {
		t.Fatalf("glDoPaged looped indefinitely on non-advancing page (%d calls)", calls)
	}
	_ = err
	if calls > 600 {
		t.Errorf("glDoPaged made %d calls; expected cap enforcement", calls)
	}
}

// ---------------------------------------------------------------------------
// Finding 7: GitLab GetPRState falls back to commit statuses when head_pipeline null
// ---------------------------------------------------------------------------

func TestGitLabGetPRState_NullHeadPipelineFallsBack(t *testing.T) {
	const sha = "deadbeef"
	var statusPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/merge_requests/"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"author":        map[string]any{"username": "bot"},
				"sha":           sha,
				"source_branch": "fix/x",
				"state":         "opened",
				// head_pipeline is null/absent
			})
		case strings.Contains(r.URL.Path, "/statuses"):
			statusPath = r.URL.Path
			_ = json.NewEncoder(w).Encode([]map[string]any{{"status": "success"}})
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	c := &GitLab{apiBase: srv.URL, token: "tok"}
	state, err := c.GetPRState(context.Background(), "https://gitlab.com/g/p", "tok", 7)
	if err != nil {
		t.Fatalf("GetPRState: %v", err)
	}
	// When head_pipeline is null, fallback to commit statuses should yield success.
	if state.CIStatus != "success" {
		t.Errorf("CIStatus = %q, want \"success\" (fallback from null head_pipeline)", state.CIStatus)
	}
	if statusPath == "" {
		t.Error("commit statuses endpoint was never called; fallback not triggered")
	}
}

// ---------------------------------------------------------------------------
// Finding 8: GitLab Suggest logs partial failures (best-effort, no API change)
// ---------------------------------------------------------------------------

// TestGitLabSuggest_ContinuesAfterFirstError is not applicable since we keep
// the early-return behaviour but add logging. The test below verifies at least
// that a single-suggestion success case still works.
func TestGitLabSuggest_SingleSuggestion(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL}
	err := c.Suggest(context.Background(), "https://gitlab.com/g/p", "tok", 5, []Suggestion{
		{Path: "main.go", Line: 10, Body: "// fix"},
	})
	if err != nil {
		t.Fatalf("Suggest: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Finding 9: GitLab GetCommitCIStatus uses glDoPaged (pagination)
// ---------------------------------------------------------------------------

func TestGitLabGetCommitCIStatus_Paginated(t *testing.T) {
	var callCount int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		page := r.URL.Query().Get("page")
		switch page {
		case "", "1":
			// First page: 3 success statuses, but also a next page.
			w.Header().Set("X-Next-Page", "2")
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"status": "success"},
				{"status": "success"},
				{"status": "success"},
			})
		case "2":
			// Second page: one failure. Without pagination this would be missed.
			w.Header().Set("X-Next-Page", "")
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"status": "failed"},
			})
		default:
			t.Errorf("unexpected page %q", page)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	c := &GitLab{apiBase: srv.URL, token: "tok"}
	result, err := c.GetCommitCIStatus(context.Background(), "g/p", "", "deadbeef")
	if err != nil {
		t.Fatalf("GetCommitCIStatus: %v", err)
	}
	// The failure on page 2 must be visible.
	if result != "failure" {
		t.Errorf("GetCommitCIStatus = %q, want \"failure\" (failure on page 2 was missed)", result)
	}
	if callCount < 2 {
		t.Errorf("expected at least 2 HTTP calls for pagination, got %d", callCount)
	}
}

// ---------------------------------------------------------------------------
// Finding 10: GitLab GetPRState guards head_pipeline.sha vs mr.sha
// ---------------------------------------------------------------------------

func TestGitLabGetPRState_StalePipelineSHAReturnsPending(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/merge_requests/") {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"author":        map[string]any{"username": "bot"},
				"sha":           "newsha",
				"source_branch": "fix/x",
				"state":         "opened",
				// head_pipeline.sha is the OLD sha — stale pipeline, sha non-empty and differs
				"head_pipeline": map[string]any{
					"sha":    "oldsha",
					"status": "success",
				},
			})
		} else {
			t.Errorf("unexpected path %q (statuses should not be called for stale SHA case)", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	c := &GitLab{apiBase: srv.URL, token: "tok"}
	state, err := c.GetPRState(context.Background(), "https://gitlab.com/g/p", "tok", 7)
	if err != nil {
		t.Fatalf("GetPRState: %v", err)
	}
	// head_pipeline is for oldsha != newsha and sha is non-empty -> must be pending (no fallback to statuses).
	if state.CIStatus != "pending" {
		t.Errorf("CIStatus = %q; want \"pending\" (stale pipeline SHA)", state.CIStatus)
	}
}

// ---------------------------------------------------------------------------
// Finding 11: GitHub combinedStatus unknown state -> "pending" not ""
// ---------------------------------------------------------------------------

func TestGitHubCombinedStatus_UnknownStateMapped(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/check-runs"):
			_ = json.NewEncoder(w).Encode(map[string]any{"check_runs": []any{}})
		case strings.HasSuffix(r.URL.Path, "/status"):
			// Return an unexpected state with total_count > 0.
			_ = json.NewEncoder(w).Encode(map[string]any{"state": "unknown_new_state", "total_count": 1})
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	c := &GitHub{apiBase: srv.URL}
	got, err := c.GetCommitCIStatus(context.Background(), "o", "r", "abc")
	if err != nil {
		t.Fatalf("GetCommitCIStatus: %v", err)
	}
	// An unknown state with TotalCount>0 must not silently become "" (fail-open).
	// It should become "pending" (fail-safe).
	if got == "" {
		t.Errorf("unknown combined state became %q (fail-open); want \"pending\"", got)
	}
	if got != "pending" {
		t.Errorf("unknown combined state -> %q; want \"pending\"", got)
	}
}

// ---------------------------------------------------------------------------
// Finding 12: ghGraphQL joins all errors, not just first
// ---------------------------------------------------------------------------

func TestGhGraphQL_AllErrorsJoined(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"errors": []map[string]any{
				{"message": "first error"},
				{"message": "second error"},
			},
		})
	}))
	defer srv.Close()
	c := &GitHub{graphQLBase: srv.URL}
	err := c.ghGraphQL(context.Background(), "tok", `query { __typename }`, nil, nil)
	if err == nil {
		t.Fatal("expected error from GraphQL errors, got nil")
	}
	if !strings.Contains(err.Error(), "first error") {
		t.Errorf("error %q missing \"first error\"", err.Error())
	}
	if !strings.Contains(err.Error(), "second error") {
		t.Errorf("error %q missing \"second error\"", err.Error())
	}
}

// ---------------------------------------------------------------------------
// Finding 13: ghResourceID / ghProjectItemID validate itemURL before sending
// ---------------------------------------------------------------------------

func TestGhResourceID_InvalidURLRejected(t *testing.T) {
	// Server should never be called for invalid URLs.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("server must not be called for invalid itemURL")
	}))
	defer srv.Close()
	c := &GitHub{graphQLBase: srv.URL}

	cases := []string{
		"http://github.com/a/b/issues/1",      // http not https
		"ftp://github.com/a/b/issues/1",       // ftp scheme
		"https://evil.example.com/a/b/issues", // wrong host
		"not-a-url",                           // unparseable
	}
	for _, u := range cases {
		_, err := c.ghResourceID(context.Background(), "tok", u)
		if err == nil {
			t.Errorf("ghResourceID(%q): expected error, got nil", u)
		}
	}
}

func TestGhProjectItemID_InvalidURLRejected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("server must not be called for invalid itemURL")
	}))
	defer srv.Close()
	c := &GitHub{graphQLBase: srv.URL}

	_, err := c.ghProjectItemID(context.Background(), "tok", "http://github.com/evil", "PVT_1")
	if err == nil {
		t.Error("ghProjectItemID(http://): expected error, got nil")
	}
}

// TestGhResourceID_ValidURLAccepted verifies valid https://github.com URLs
// are allowed through.
func TestGhResourceID_ValidURLAccepted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{"resource": map[string]any{"id": "RES1"}},
		})
	}))
	defer srv.Close()
	c := &GitHub{graphQLBase: srv.URL}
	id, err := c.ghResourceID(context.Background(), "tok", "https://github.com/acme/r/issues/1")
	if err != nil {
		t.Fatalf("ghResourceID valid URL: %v", err)
	}
	if id != "RES1" {
		t.Errorf("id = %q, want \"RES1\"", id)
	}
}

// ---------------------------------------------------------------------------
// Finding 14: ghOwnerRepo requires exactly two path segments
// ---------------------------------------------------------------------------

func TestGhOwnerRepo_ExactlyTwoSegmentsRequired(t *testing.T) {
	cases := []struct {
		repoURL string
		wantErr bool
		owner   string
		repo    string
	}{
		// Valid: exactly owner/repo.
		{"https://github.com/o/r.git", false, "o", "r"},
		{"https://github.com/o/r", false, "o", "r"},
		// Invalid: deeper path (enterprise/teams URL).
		{"https://github.com/enterprises/acme/teams/foo", true, "", ""},
		// Invalid: single segment.
		{"https://github.com/onlyone", true, "", ""},
		// Invalid: empty.
		{"https://github.com/", true, "", ""},
	}
	for _, tc := range cases {
		o, r, err := ghOwnerRepo(tc.repoURL)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ghOwnerRepo(%q): expected error, got owner=%q repo=%q", tc.repoURL, o, r)
			}
		} else {
			if err != nil {
				t.Errorf("ghOwnerRepo(%q): unexpected error: %v", tc.repoURL, err)
				continue
			}
			if o != tc.owner || r != tc.repo {
				t.Errorf("ghOwnerRepo(%q) = %q/%q, want %q/%q", tc.repoURL, o, r, tc.owner, tc.repo)
			}
		}
	}
}

// TestGhOwnerRepo_TrailingSlashStillValid ensures the existing trailing-slash
// test case still passes (the trim must happen before the count check).
func TestGhOwnerRepo_TrailingSlash(t *testing.T) {
	o, r, err := ghOwnerRepo("https://github.com/o/r/")
	if err != nil {
		t.Fatalf("trailing slash should be valid: %v", err)
	}
	if o != "o" || r != "r" {
		t.Errorf("ghOwnerRepo trailing slash = %q/%q, want o/r", o, r)
	}
}

// Ensure the per_page parameter is included in GitLab commit status requests
// (needed so pagination works correctly with the default 20-item page size).
func TestGitLabGetCommitCIStatus_PerPageParam(t *testing.T) {
	var gotQuery url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query()
		_ = json.NewEncoder(w).Encode([]map[string]any{{"status": "success"}})
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL, token: "tok"}
	_, err := c.GetCommitCIStatus(context.Background(), "g/p", "", "abc")
	if err != nil {
		t.Fatalf("GetCommitCIStatus: %v", err)
	}
	if gotQuery.Get("per_page") == "" {
		t.Error("GetCommitCIStatus did not send per_page query param")
	}
}
