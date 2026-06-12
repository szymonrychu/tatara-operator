package scm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// TestGitHubGetCommitCIStatus exercises the four vocabulary values for a commit sha.
func TestGitHubGetCommitCIStatus(t *testing.T) {
	cases := []struct {
		name   string
		runs   []map[string]string // status, conclusion
		wantCI string
	}{
		{"no runs (none)", nil, ""},
		{"in progress (pending)", []map[string]string{{"status": "in_progress", "conclusion": ""}}, "pending"},
		{"all success", []map[string]string{{"status": "completed", "conclusion": "success"}}, "success"},
		{"one failure", []map[string]string{
			{"status": "completed", "conclusion": "success"},
			{"status": "completed", "conclusion": "failure"},
		}, "failure"},
		{"neutral counts as success", []map[string]string{
			{"status": "completed", "conclusion": "neutral"},
		}, "success"},
		{"skipped counts as success", []map[string]string{
			{"status": "completed", "conclusion": "skipped"},
		}, "success"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/repos/o/r/commits/deadbeef/check-runs" {
					t.Fatalf("unexpected path %q", r.URL.Path)
				}
				runs := make([]map[string]any, 0, len(tc.runs))
				for _, run := range tc.runs {
					runs = append(runs, map[string]any{
						"status":     run["status"],
						"conclusion": run["conclusion"],
					})
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"check_runs": runs})
			}))
			defer srv.Close()
			c := &GitHub{apiBase: srv.URL}
			got, err := c.GetCommitCIStatus(context.Background(), "o", "r", "deadbeef")
			if err != nil {
				t.Fatalf("GetCommitCIStatus: %v", err)
			}
			if got != tc.wantCI {
				t.Fatalf("GetCommitCIStatus = %q, want %q", got, tc.wantCI)
			}
		})
	}
}

// TestGitHubGetCommitCIStatus_Error verifies that a non-2xx response is surfaced as an error.
func TestGitHubGetCommitCIStatus_Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	c := &GitHub{apiBase: srv.URL}
	_, err := c.GetCommitCIStatus(context.Background(), "o", "r", "badbad")
	if err == nil {
		t.Fatal("expected error for 404, got nil")
	}
}

// TestGitLabGetCommitCIStatus exercises the four vocabulary values using the
// GitLab pipelines-for-commit API.
func TestGitLabGetCommitCIStatus(t *testing.T) {
	cases := []struct {
		name     string
		pipeline *string // nil = no pipeline, else status string
		wantCI   string
	}{
		{"no pipeline (none)", nil, ""},
		{"success", strPtr("success"), "success"},
		{"failed (failure)", strPtr("failed"), "failure"},
		{"canceled (failure)", strPtr("canceled"), "failure"},
		{"running (pending)", strPtr("running"), "pending"},
		{"pending (pending)", strPtr("pending"), "pending"},
		{"created (pending)", strPtr("created"), "pending"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				expectedPath := "/projects/" + url.PathEscape("g/p") + "/repository/commits/deadbeef/statuses"
				if r.URL.Path != expectedPath && r.URL.RawPath != expectedPath {
					t.Fatalf("unexpected path %q (rawPath=%q)", r.URL.Path, r.URL.RawPath)
				}
				if tc.pipeline == nil {
					_ = json.NewEncoder(w).Encode([]any{})
					return
				}
				_ = json.NewEncoder(w).Encode([]map[string]any{
					{"status": *tc.pipeline},
				})
			}))
			defer srv.Close()
			c := &GitLab{apiBase: srv.URL}
			got, err := c.GetCommitCIStatus(context.Background(), "g/p", "", "deadbeef")
			if err != nil {
				t.Fatalf("GetCommitCIStatus: %v", err)
			}
			if got != tc.wantCI {
				t.Fatalf("GetCommitCIStatus = %q, want %q", got, tc.wantCI)
			}
		})
	}
}

// TestGitLabGetCommitCIStatus_Error verifies that a non-2xx response surfaces as an error.
func TestGitLabGetCommitCIStatus_Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL}
	_, err := c.GetCommitCIStatus(context.Background(), "g/p", "", "badbad")
	if err == nil {
		t.Fatal("expected error for 500, got nil")
	}
}

func strPtr(s string) *string { return &s }
