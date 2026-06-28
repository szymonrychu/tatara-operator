package scm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestGitLabListPRComments verifies the GitLab merge-request notes endpoint is
// called (NOT the issues endpoint) and notes are returned oldest-first with
// system notes filtered out. This is the GitLab MR half of the issue #188
// bot-last-word gate: MRs live in a separate IID namespace, so reusing the issues
// notes endpoint would read the wrong notes.
func TestGitLabListPRComments(t *testing.T) {
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = glTestPath(r)
		// Return newest-first + a system note to verify sort + filtering.
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"author": map[string]any{"username": "tatara-bot"}, "body": "second", "created_at": t2.Format(time.RFC3339), "system": false},
			{"author": map[string]any{"username": "ci"}, "body": "pipeline passed", "created_at": t2.Format(time.RFC3339), "system": true},
			{"author": map[string]any{"username": "alice"}, "body": "first", "created_at": t1.Format(time.RFC3339), "system": false},
		})
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL, token: "tok"}
	got, err := c.ListPRComments(context.Background(), "g/p", "", 7)
	if err != nil {
		t.Fatalf("ListPRComments: %v", err)
	}
	if gotPath != "/projects/g%2Fp/merge_requests/7/notes" {
		t.Fatalf("ListPRComments hit %q, want the merge_requests notes endpoint", gotPath)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 non-system notes, got %d: %+v", len(got), got)
	}
	if got[0].Author != "alice" || got[1].Author != "tatara-bot" {
		t.Errorf("notes not oldest-first: %+v", got)
	}
}

// TestGitHubListPRComments verifies that on GitHub a PR's comments are read from
// the issues comments endpoint (a PR is an issue), so the bot-last-word gate sees
// the PR conversation timeline.
func TestGitHubListPRComments(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"user": map[string]any{"login": "tatara-bot"}, "body": "parked", "created_at": "2026-01-01T00:00:00Z"},
		})
	}))
	defer srv.Close()
	c := &GitHub{apiBase: srv.URL}
	got, err := c.ListPRComments(context.Background(), "o", "r", 9)
	if err != nil {
		t.Fatalf("ListPRComments: %v", err)
	}
	if gotPath != "/repos/o/r/issues/9/comments" {
		t.Fatalf("ListPRComments hit %q, want the issues comments endpoint", gotPath)
	}
	if len(got) != 1 || got[0].Author != "tatara-bot" {
		t.Fatalf("unexpected comments: %+v", got)
	}
}
