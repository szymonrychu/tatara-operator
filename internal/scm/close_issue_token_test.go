package scm

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestGitHubCloseIssuePassesToken asserts that CloseIssue sends the supplied
// token in the Authorization header on both the comment POST and the PATCH close.
func TestGitHubCloseIssuePassesToken(t *testing.T) {
	var authHeaders []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeaders = append(authHeaders, r.Header.Get("Authorization"))
	}))
	defer srv.Close()
	c := &GitHub{apiBase: srv.URL}
	if err := c.CloseIssue(context.Background(), "tok-xyz", "o/r", 7, "closing"); err != nil {
		t.Fatalf("CloseIssue: %v", err)
	}
	if len(authHeaders) != 2 {
		t.Fatalf("expected 2 requests (comment + close), got %d", len(authHeaders))
	}
	for i, h := range authHeaders {
		if h != "Bearer tok-xyz" {
			t.Fatalf("request %d Authorization = %q, want %q", i, h, "Bearer tok-xyz")
		}
	}
}

// TestGitLabCloseIssuePassesToken asserts that CloseIssue sends the supplied
// token in the Authorization header on both the note POST and the PUT close.
func TestGitLabCloseIssuePassesToken(t *testing.T) {
	var authHeaders []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeaders = append(authHeaders, r.Header.Get("Private-Token"))
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL}
	if err := c.CloseIssue(context.Background(), "gl-tok-xyz", "g/p", 5, "closing"); err != nil {
		t.Fatalf("CloseIssue: %v", err)
	}
	if len(authHeaders) != 2 {
		t.Fatalf("expected 2 requests (note + close), got %d", len(authHeaders))
	}
	for i, h := range authHeaders {
		if h != "gl-tok-xyz" {
			t.Fatalf("request %d Private-Token = %q, want %q", i, h, "gl-tok-xyz")
		}
	}
}
