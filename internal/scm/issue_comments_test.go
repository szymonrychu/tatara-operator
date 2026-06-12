package scm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

// TestGitHubListIssueComments verifies the GitHub issue-comments API is called
// and results are returned oldest-first with Author/Body/CreatedAt populated.
func TestGitHubListIssueComments(t *testing.T) {
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/o/r/issues/7/comments" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		// Return in reverse order to verify the API sorts oldest-first.
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"user": map[string]any{"login": "alice"}, "body": "first comment", "created_at": t1.Format(time.RFC3339)},
			{"user": map[string]any{"login": "bob"}, "body": "second comment", "created_at": t2.Format(time.RFC3339)},
		})
	}))
	defer srv.Close()
	c := &GitHub{apiBase: srv.URL}
	got, err := c.ListIssueComments(context.Background(), "o", "r", 7)
	if err != nil {
		t.Fatalf("ListIssueComments: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 comments, got %d: %+v", len(got), got)
	}
	if got[0].Author != "alice" || got[0].Body != "first comment" {
		t.Errorf("comment[0] = %+v, want alice/first comment", got[0])
	}
	if got[1].Author != "bob" || got[1].Body != "second comment" {
		t.Errorf("comment[1] = %+v, want bob/second comment", got[1])
	}
	if !got[0].CreatedAt.Equal(t1) {
		t.Errorf("comment[0].CreatedAt = %v, want %v", got[0].CreatedAt, t1)
	}
	if !got[1].CreatedAt.Equal(t2) {
		t.Errorf("comment[1].CreatedAt = %v, want %v", got[1].CreatedAt, t2)
	}
}

// TestGitHubListIssueComments_Empty verifies an empty list is returned without error.
func TestGitHubListIssueComments_Empty(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]any{})
	}))
	defer srv.Close()
	c := &GitHub{apiBase: srv.URL}
	got, err := c.ListIssueComments(context.Background(), "o", "r", 1)
	if err != nil {
		t.Fatalf("ListIssueComments empty: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("want 0 comments, got %d", len(got))
	}
}

// TestGitHubListIssueComments_Error verifies that a non-2xx response surfaces as an error.
func TestGitHubListIssueComments_Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	c := &GitHub{apiBase: srv.URL}
	_, err := c.ListIssueComments(context.Background(), "o", "r", 99)
	if err == nil {
		t.Fatal("expected error for 404, got nil")
	}
}

// TestGitLabListIssueComments verifies the GitLab notes API is called and
// results are returned oldest-first (ascending created_at order).
func TestGitLabListIssueComments(t *testing.T) {
	t1 := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 2, 2, 0, 0, 0, 0, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		expectedPath := "/projects/" + url.PathEscape("g/p") + "/issues/3/notes"
		if r.URL.Path != expectedPath && r.URL.RawPath != expectedPath {
			t.Fatalf("unexpected path %q (rawPath=%q)", r.URL.Path, r.URL.RawPath)
		}
		// notes are returned newest-first from GitLab by default; the implementation
		// must sort oldest-first.
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"author": map[string]any{"username": "carol"}, "body": "note two", "created_at": t2.Format(time.RFC3339), "system": false},
			{"author": map[string]any{"username": "dave"}, "body": "note one", "created_at": t1.Format(time.RFC3339), "system": false},
		})
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL}
	got, err := c.ListIssueComments(context.Background(), "g/p", "", 3)
	if err != nil {
		t.Fatalf("ListIssueComments: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 comments, got %d: %+v", len(got), got)
	}
	// After oldest-first sort: dave (t1) then carol (t2).
	if got[0].Author != "dave" || got[0].Body != "note one" {
		t.Errorf("comment[0] = %+v, want dave/note one", got[0])
	}
	if got[1].Author != "carol" || got[1].Body != "note two" {
		t.Errorf("comment[1] = %+v, want carol/note two", got[1])
	}
}

// TestGitLabListIssueComments_SystemNotesFiltered verifies that GitLab system
// notes (system=true) are filtered out.
func TestGitLabListIssueComments_SystemNotesFiltered(t *testing.T) {
	t1 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"author": map[string]any{"username": "sys"}, "body": "system note", "created_at": t1.Format(time.RFC3339), "system": true},
			{"author": map[string]any{"username": "human"}, "body": "human note", "created_at": t1.Format(time.RFC3339), "system": false},
		})
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL}
	got, err := c.ListIssueComments(context.Background(), "g/p", "", 1)
	if err != nil {
		t.Fatalf("ListIssueComments: %v", err)
	}
	if len(got) != 1 || got[0].Author != "human" {
		t.Fatalf("want 1 human comment, got %+v", got)
	}
}

// TestGitLabListIssueComments_Error verifies that a non-2xx response surfaces as an error.
func TestGitLabListIssueComments_Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL}
	_, err := c.ListIssueComments(context.Background(), "g/p", "", 5)
	if err == nil {
		t.Fatal("expected error for 500, got nil")
	}
}
