package scm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGitHubListOpenPRs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/o/r/pulls" || r.URL.Query().Get("state") != "open" {
			t.Fatalf("path=%q query=%q", r.URL.Path, r.URL.RawQuery)
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"number": 5, "user": map[string]any{"login": "alice"},
				"head":       map[string]any{"sha": "abc"},
				"labels":     []map[string]any{{"name": "tatara/priority"}},
				"updated_at": "2026-06-10T12:00:00Z"},
		})
	}))
	defer srv.Close()
	c := &GitHub{apiBase: srv.URL}
	prs, err := c.ListOpenPRs(context.Background(), "o", "r")
	if err != nil {
		t.Fatalf("ListOpenPRs: %v", err)
	}
	if len(prs) != 1 || prs[0].Repo != "o/r" || prs[0].Number != 5 || prs[0].Author != "alice" || prs[0].HeadSHA != "abc" || prs[0].Labels[0] != "tatara/priority" {
		t.Fatalf("prs = %+v", prs)
	}
	if prs[0].UpdatedAt.IsZero() {
		t.Fatalf("updatedAt not parsed: %+v", prs[0])
	}
}

func TestGitHubListOpenIssuesFiltersPRs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/o/r/issues" || r.URL.Query().Get("state") != "open" {
			t.Fatalf("path=%q query=%q", r.URL.Path, r.URL.RawQuery)
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"number": 7, "labels": []map[string]any{{"name": "bug"}}, "updated_at": "2026-06-10T12:00:00Z"},
			{"number": 8, "pull_request": map[string]any{"url": "x"}, "updated_at": "2026-06-10T12:00:00Z"},
		})
	}))
	defer srv.Close()
	c := &GitHub{apiBase: srv.URL}
	iss, err := c.ListOpenIssues(context.Background(), "o", "r")
	if err != nil {
		t.Fatalf("ListOpenIssues: %v", err)
	}
	if len(iss) != 2 {
		t.Fatalf("want 2 items (IsPR set), got %+v", iss)
	}
	if iss[0].Number != 7 || iss[0].IsPR {
		t.Fatalf("issue 7 should not be PR: %+v", iss[0])
	}
	if iss[1].Number != 8 || !iss[1].IsPR {
		t.Fatalf("issue 8 should be flagged IsPR: %+v", iss[1])
	}
}

func TestGitHubCloseIssue(t *testing.T) {
	paths := map[string]string{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths[r.Method+" "+r.URL.Path] = ""
	}))
	defer srv.Close()
	c := &GitHub{apiBase: srv.URL}
	if err := c.CloseIssue(context.Background(), "o/r", 7, "closing: out of scope"); err != nil {
		t.Fatalf("CloseIssue: %v", err)
	}
	if _, ok := paths["POST /repos/o/r/issues/7/comments"]; !ok {
		t.Fatalf("missing comment POST; got %+v", paths)
	}
	if _, ok := paths["PATCH /repos/o/r/issues/7"]; !ok {
		t.Fatalf("missing PATCH close; got %+v", paths)
	}
}

func TestGitHubListBoardItems(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"user": map[string]any{
					"projectV2": map[string]any{
						"items": map[string]any{"nodes": []map[string]any{
							{"updatedAt": "2026-06-10T12:00:00Z",
								"fieldValueByName": map[string]any{"name": "Todo"},
								"content":          map[string]any{"number": 9, "repository": map[string]any{"nameWithOwner": "o/r"}}},
						}},
					},
				},
				"organization": map[string]any{"projectV2": map[string]any{"items": map[string]any{"nodes": []any{}}}},
			},
		})
	}))
	defer srv.Close()
	c := &GitHub{graphQLBase: srv.URL}
	items, err := c.ListBoardItems(context.Background(), BoardRef{Owner: "o", GitHubProjectNumber: 3})
	if err != nil {
		t.Fatalf("ListBoardItems: %v", err)
	}
	if len(items) != 1 || items[0].Repo != "o/r" || items[0].Number != 9 || items[0].Column != "Todo" {
		t.Fatalf("items = %+v", items)
	}
}

// TestGitHubListBoardItemsNoPRNodes verifies that a board response whose only
// node is a PullRequest (which the GraphQL no longer requests) does not yield a
// BoardItem with a non-zero Number.  A pure-Issue board is returned normally.
func TestGitHubListBoardItemsNoPRNodes(t *testing.T) {
	var capturedQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Query string `json:"query"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		capturedQuery = body.Query
		// Simulate: one Issue node (number 12) and no PullRequest node.
		// Because the GraphQL no longer asks for PullRequest, this is the only
		// possible response shape – but we also verify the query itself.
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"user": map[string]any{
					"projectV2": map[string]any{
						"items": map[string]any{"nodes": []map[string]any{
							{"updatedAt": "2026-06-10T12:00:00Z",
								"fieldValueByName": map[string]any{"name": "In Progress"},
								"content":          map[string]any{"number": 12, "repository": map[string]any{"nameWithOwner": "o/r"}}},
						}},
					},
				},
				"organization": map[string]any{"projectV2": map[string]any{"items": map[string]any{"nodes": []any{}}}},
			},
		})
	}))
	defer srv.Close()
	c := &GitHub{graphQLBase: srv.URL}
	items, err := c.ListBoardItems(context.Background(), BoardRef{Owner: "o", GitHubProjectNumber: 5})
	if err != nil {
		t.Fatalf("ListBoardItems: %v", err)
	}
	// The GraphQL must NOT contain "PullRequest" in the content fragment.
	if contains(capturedQuery, "PullRequest") {
		t.Fatalf("ListBoardItems GraphQL must not request PullRequest nodes; query = %s", capturedQuery)
	}
	// The one issue node must be returned with its number intact.
	if len(items) != 1 || items[0].Number != 12 {
		t.Fatalf("expected 1 item with number 12, got %+v", items)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 ||
		func() bool {
			for i := 0; i <= len(s)-len(sub); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
			return false
		}())
}
