package scm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestGitLabListOpenPRs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if glTestPath(r) != "/projects/g%2Fp/merge_requests" || r.URL.Query().Get("state") != "opened" {
			t.Fatalf("path=%q query=%q", glTestPath(r), r.URL.RawQuery)
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"iid": 5, "sha": "abc", "author": map[string]any{"username": "alice"},
				"labels": []string{"tatara/priority"}, "updated_at": "2026-06-10T12:00:00Z"},
		})
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL, token: "tok"}
	prs, err := c.ListOpenPRs(context.Background(), "g", "p")
	if err != nil {
		t.Fatalf("ListOpenPRs: %v", err)
	}
	if len(prs) != 1 || prs[0].Repo != "g/p" || prs[0].Number != 5 || prs[0].Author != "alice" || prs[0].HeadSHA != "abc" || prs[0].Labels[0] != "tatara/priority" {
		t.Fatalf("prs = %+v", prs)
	}
}

func TestGitLabListOpenIssues(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if glTestPath(r) != "/projects/g%2Fp/issues" || r.URL.Query().Get("state") != "opened" {
			t.Fatalf("path=%q query=%q", glTestPath(r), r.URL.RawQuery)
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"iid": 7, "author": map[string]any{"username": "dave"}, "labels": []string{"bug"}, "updated_at": "2026-06-10T12:00:00Z"},
		})
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL, token: "tok"}
	iss, err := c.ListOpenIssues(context.Background(), "g", "p")
	if err != nil {
		t.Fatalf("ListOpenIssues: %v", err)
	}
	if len(iss) != 1 || iss[0].Repo != "g/p" || iss[0].Number != 7 || iss[0].IsPR || iss[0].Labels[0] != "bug" {
		t.Fatalf("iss = %+v", iss)
	}
	if iss[0].Author != "dave" {
		t.Fatalf("issue 7 author = %q, want dave: %+v", iss[0].Author, iss[0])
	}
}

func TestGitLabGetIssue(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if glTestPath(r) != "/projects/g%2Fp/issues/21" {
			t.Fatalf("unexpected path: %q", glTestPath(r))
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"iid":         21,
			"title":       "gl title",
			"description": "gl body",
		})
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL, token: "tok"}
	content, err := c.GetIssue(context.Background(), "g/p", "", 21)
	if err != nil {
		t.Fatalf("GetIssue: %v", err)
	}
	if content.Title != "gl title" {
		t.Errorf("Title = %q, want %q", content.Title, "gl title")
	}
	if content.Body != "gl body" {
		t.Errorf("Body = %q, want %q", content.Body, "gl body")
	}
}

func TestGitLabGetIssueState(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if glTestPath(r) != "/projects/g%2Fp/issues/21" {
			t.Fatalf("unexpected path: %q", glTestPath(r))
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"iid":    21,
			"author": map[string]any{"username": "dave"},
			"state":  "closed",
		})
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL}
	st, err := c.GetIssueState(context.Background(), "https://gitlab.com/g/p", "tok", 21)
	if err != nil {
		t.Fatalf("GetIssueState: %v", err)
	}
	if st.Author != "dave" || !st.Closed {
		t.Fatalf("st = %+v, want Author=dave Closed=true", st)
	}
}

// TestGitLabGetIssueSplitOwnerRepo verifies the project id is built from a
// split (owner, repo) coordinate pair, not owner alone. Controller callers that
// derive coordinates via scm.OwnerRepo pass ("g", "p"); the GitLab project path
// must be the joined "g/p" so the read hits the real project, not "/projects/g".
func TestGitLabGetIssueSplitOwnerRepo(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if glTestPath(r) != "/projects/g%2Fp/issues/21" {
			t.Fatalf("unexpected path: %q", glTestPath(r))
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"title": "t", "description": "b"})
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL, token: "tok"}
	content, err := c.GetIssue(context.Background(), "g", "p", 21)
	if err != nil {
		t.Fatalf("GetIssue: %v", err)
	}
	if content.Title != "t" || content.Body != "b" {
		t.Fatalf("content = %+v", content)
	}
}

// TestGitLabListIssueCommentsSplitOwnerRepo verifies notes are read from the
// joined "g/p" project path when the caller passes split (owner, repo).
func TestGitLabListIssueCommentsSplitOwnerRepo(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if glTestPath(r) != "/projects/g%2Fp/issues/3/notes" {
			t.Fatalf("unexpected path: %q", glTestPath(r))
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"author": map[string]any{"username": "alice"}, "body": "hi", "created_at": "2026-01-01T00:00:00Z", "system": false},
		})
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL, token: "tok"}
	comments, err := c.ListIssueComments(context.Background(), "g", "p", 3)
	if err != nil {
		t.Fatalf("ListIssueComments: %v", err)
	}
	if len(comments) != 1 || comments[0].Author != "alice" {
		t.Fatalf("comments = %+v", comments)
	}
}

// TestGitLabListOpenIssuesFullPathEmptyRepo verifies that the full-path calling
// convention (owner="g/p", repo="") does not append a trailing slash, which 404s.
func TestGitLabListOpenIssuesFullPathEmptyRepo(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if glTestPath(r) != "/projects/g%2Fp/issues" {
			t.Fatalf("unexpected path: %q", glTestPath(r))
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{})
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL, token: "tok"}
	if _, err := c.ListOpenIssues(context.Background(), "g/p", ""); err != nil {
		t.Fatalf("ListOpenIssues: %v", err)
	}
}

// TestGitLabListOpenPRsPaginated verifies that ListOpenPRs follows X-Next-Page headers.
func TestGitLabListOpenPRsPaginated(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page := r.URL.Query().Get("page")
		if page == "" || page == "1" {
			w.Header().Set("X-Next-Page", "2")
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"iid": 1, "sha": "s1", "author": map[string]any{"username": "a"}, "updated_at": "2026-01-01T00:00:00Z"},
			})
		} else {
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"iid": 2, "sha": "s2", "author": map[string]any{"username": "b"}, "updated_at": "2026-01-02T00:00:00Z"},
			})
		}
		_ = srv // suppress unused variable warning
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL, token: "tok"}
	prs, err := c.ListOpenPRs(context.Background(), "g", "p")
	if err != nil {
		t.Fatalf("ListOpenPRs paginated: %v", err)
	}
	if len(prs) != 2 {
		t.Fatalf("want 2 PRs across 2 pages, got %d: %+v", len(prs), prs)
	}
}

// TestGitLabListOpenIssuesPaginated verifies that ListOpenIssues follows X-Next-Page headers.
func TestGitLabListOpenIssuesPaginated(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page := r.URL.Query().Get("page")
		if page == "" || page == "1" {
			w.Header().Set("X-Next-Page", "2")
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"iid": 10, "author": map[string]any{"username": "a"}, "updated_at": "2026-01-01T00:00:00Z"},
			})
		} else {
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"iid": 20, "author": map[string]any{"username": "b"}, "updated_at": "2026-01-02T00:00:00Z"},
			})
		}
		_ = srv
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL, token: "tok"}
	issues, err := c.ListOpenIssues(context.Background(), "g", "p")
	if err != nil {
		t.Fatalf("ListOpenIssues paginated: %v", err)
	}
	if len(issues) != 2 {
		t.Fatalf("want 2 issues across 2 pages, got %d: %+v", len(issues), issues)
	}
}

// TestGitLabListIssueCommentsPaginated verifies that ListIssueComments follows X-Next-Page.
func TestGitLabListIssueCommentsPaginated(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page := r.URL.Query().Get("page")
		if page == "" || page == "1" {
			w.Header().Set("X-Next-Page", "2")
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"author": map[string]any{"username": "a"}, "body": "p1", "created_at": "2026-01-01T00:00:00Z", "system": false},
			})
		} else {
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"author": map[string]any{"username": "b"}, "body": "p2", "created_at": "2026-01-02T00:00:00Z", "system": false},
			})
		}
		_ = srv
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL, token: "tok"}
	comments, err := c.ListIssueComments(context.Background(), "g/p", "", 3)
	if err != nil {
		t.Fatalf("ListIssueComments paginated: %v", err)
	}
	if len(comments) != 2 {
		t.Fatalf("want 2 comments across 2 pages, got %d: %+v", len(comments), comments)
	}
}

func TestGitLabCloseIssue(t *testing.T) {
	paths := map[string]bool{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths[r.Method+" "+glTestPath(r)] = true
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL, token: "tok"}
	if err := c.CloseIssue(context.Background(), "tok", "g/p", 7, "closing"); err != nil {
		t.Fatalf("CloseIssue: %v", err)
	}
	noteKey := "POST " + "/projects/" + "g%2Fp" + "/issues/7/notes"
	closeKey := "PUT " + "/projects/" + "g%2Fp" + "/issues/7"
	if !paths[noteKey] {
		t.Fatalf("missing note POST; got %+v", paths)
	}
	if !paths[closeKey] {
		t.Fatalf("missing PUT close; got %+v", paths)
	}
}

func TestGitLabListClosedIssues_FiltersSince(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if glTestPath(r) != "/projects/g%2Fp/issues" {
			t.Errorf("unexpected path: %q", glTestPath(r))
		}
		if r.URL.Query().Get("state") != "closed" {
			t.Errorf("want state=closed, got %q", r.URL.Query().Get("state"))
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"iid": 5, "title": "done", "state": "closed", "closed_at": "2026-06-20T00:00:00Z", "author": map[string]any{"username": "bot"}},
		})
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL, token: "tok"}
	got, err := c.ListClosedIssues(context.Background(), "g", "p", time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Number != 5 || got[0].State != "closed" {
		t.Fatalf("want 1 closed issue #5, got %+v", got)
	}
}

func TestGitLabListCommits_SinceDefaultBranch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if glTestPath(r) != "/projects/g%2Fp/repository/commits" {
			t.Errorf("unexpected path: %q", glTestPath(r))
		}
		if r.URL.Query().Get("since") == "" {
			t.Errorf("want since param set")
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"id": "abc123", "message": "feat: do the thing", "author_name": "bot", "created_at": "2026-06-20T00:00:00Z"},
		})
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL, token: "tok"}
	got, err := c.ListCommits(context.Background(), "g", "p", time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].SHA != "abc123" {
		t.Fatalf("want commit abc123, got %+v", got)
	}
}
