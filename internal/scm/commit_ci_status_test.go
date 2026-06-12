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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/repos/o/r/commits/deadbeef/check-runs":
					runs := make([]map[string]any, 0, len(tc.runs))
					for _, run := range tc.runs {
						runs = append(runs, map[string]any{
							"status":     run["status"],
							"conclusion": run["conclusion"],
						})
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"check_runs": runs})
				case "/repos/o/r/commits/deadbeef/status":
					// Combined-status: return no statuses so check-runs dominate.
					_ = json.NewEncoder(w).Encode(map[string]any{"state": "pending", "total_count": 0})
				default:
					t.Errorf("unexpected path %q", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
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

// TestGitHubGetCommitCIStatus_Error verifies that a non-2xx check-runs response is surfaced as an error.
func TestGitHubGetCommitCIStatus_Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return 404 for all paths to trigger an error on the check-runs call.
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

// TestGitHubGetCommitCIStatus_CombinedStatusFolded verifies that GitHub's
// GetCommitCIStatus folds the legacy combined-status API with check-runs:
//   - combined-status=success + no check-runs -> "success"
//   - combined-status=pending + no check-runs -> "pending"
//   - combined-status=failure + no check-runs -> "failure"
//   - combined-status=success + check-run failure -> "failure" (check-run wins)
//   - no check-runs + no combined state -> "" (none)
func TestGitHubGetCommitCIStatus_CombinedStatusFolded(t *testing.T) {
	cases := []struct {
		name          string
		checkRuns     []map[string]string // nil = no check-runs
		combinedState string              // "" = no combined status
		wantCI        string
	}{
		{
			name:          "combined success, no check-runs -> success",
			combinedState: "success",
			wantCI:        "success",
		},
		{
			name:          "combined pending, no check-runs -> pending",
			combinedState: "pending",
			wantCI:        "pending",
		},
		{
			name:          "combined failure, no check-runs -> failure",
			combinedState: "failure",
			wantCI:        "failure",
		},
		{
			name:          "no check-runs, no combined -> empty",
			combinedState: "",
			wantCI:        "",
		},
		{
			name:          "check-run success, combined success -> success",
			checkRuns:     []map[string]string{{"status": "completed", "conclusion": "success"}},
			combinedState: "success",
			wantCI:        "success",
		},
		{
			name:          "check-run failure, combined success -> failure (check-run wins)",
			checkRuns:     []map[string]string{{"status": "completed", "conclusion": "failure"}},
			combinedState: "success",
			wantCI:        "failure",
		},
		{
			name:          "check-run success, combined failure -> failure (combined wins)",
			checkRuns:     []map[string]string{{"status": "completed", "conclusion": "success"}},
			combinedState: "failure",
			wantCI:        "failure",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				checkRunsPath := "/repos/o/r/commits/deadbeef/check-runs"
				statusPath := "/repos/o/r/commits/deadbeef/status"
				switch r.URL.Path {
				case checkRunsPath:
					runs := make([]map[string]any, 0, len(tc.checkRuns))
					for _, run := range tc.checkRuns {
						runs = append(runs, map[string]any{
							"status":     run["status"],
							"conclusion": run["conclusion"],
						})
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"check_runs": runs})
				case statusPath:
					if tc.combinedState == "" {
						_ = json.NewEncoder(w).Encode(map[string]any{"state": "pending", "total_count": 0})
					} else {
						_ = json.NewEncoder(w).Encode(map[string]any{"state": tc.combinedState, "total_count": 1})
					}
				default:
					t.Errorf("unexpected path %q", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
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
