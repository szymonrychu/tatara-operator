package scm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

func TestDeriveCIStatus(t *testing.T) {
	tests := []struct {
		name   string
		checks []CICheck
		want   string
	}{
		{"none: zero checks", nil, "none"},
		{
			"pending: queued check, nothing in progress",
			[]CICheck{{Name: "build", Status: "queued"}},
			"pending",
		},
		{
			"running: one check in progress",
			[]CICheck{{Name: "build", Status: "in_progress"}},
			"running",
		},
		{
			"green: all completed, all success/neutral/skipped",
			[]CICheck{
				{Name: "build", Status: "completed", Conclusion: "success"},
				{Name: "lint", Status: "completed", Conclusion: "neutral"},
				{Name: "docs", Status: "completed", Conclusion: "skipped"},
			},
			"green",
		},
		{
			"red: one failed while another still running",
			[]CICheck{
				{Name: "build", Status: "completed", Conclusion: "failure"},
				{Name: "lint", Status: "in_progress"},
			},
			"red",
		},
		{
			"red: timed_out counts as failing",
			[]CICheck{{Name: "build", Status: "completed", Conclusion: "timed_out"}},
			"red",
		},
		{
			"red: cancelled counts as failing",
			[]CICheck{{Name: "build", Status: "completed", Conclusion: "cancelled"}},
			"red",
		},
		{
			"red: action_required counts as failing",
			[]CICheck{{Name: "build", Status: "completed", Conclusion: "action_required"}},
			"red",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, deriveCIStatus(tc.checks))
		})
	}
}

func TestGitHubPRChecks(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/o/r/pulls/9":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"head":      map[string]any{"sha": "deadbeef"},
				"mergeable": true,
			})
		case "/repos/o/r/commits/deadbeef/check-runs":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"check_runs": []map[string]any{
					{
						"id":         int64(456),
						"name":       "build",
						"status":     "completed",
						"conclusion": "failure",
						"html_url":   "https://github.com/o/r/actions/runs/123/job/456",
					},
					{
						"id":         int64(789),
						"name":       "lint",
						"status":     "in_progress",
						"conclusion": "",
						"html_url":   "https://github.com/o/r/pull/9/checks",
					},
				},
			})
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer srv.Close()

	c := &GitHub{apiBase: srv.URL}
	got, err := c.PRChecks(context.Background(), "https://github.com/o/r.git", "tok", 9)
	require.NoError(t, err)

	require.Equal(t, "deadbeef", got.HeadSHA)
	require.True(t, got.Mergeable)
	require.Equal(t, "red", got.Status)
	require.Len(t, got.Checks, 2)

	require.Equal(t, CICheck{
		Name:       "build",
		Status:     "completed",
		Conclusion: "failure",
		URL:        "https://github.com/o/r/actions/runs/123/job/456",
		JobID:      "456",
	}, got.Checks[0])

	// No /job/(\d+) in html_url: falls back to the check-run id.
	require.Equal(t, CICheck{
		Name:       "lint",
		Status:     "in_progress",
		Conclusion: "",
		URL:        "https://github.com/o/r/pull/9/checks",
		JobID:      "789",
	}, got.Checks[1])
}

func TestGitHubPRChecksMergeableNull(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/o/r/pulls/9":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"head":      map[string]any{"sha": "deadbeef"},
				"mergeable": nil,
			})
		case "/repos/o/r/commits/deadbeef/check-runs":
			_ = json.NewEncoder(w).Encode(map[string]any{"check_runs": []map[string]any{}})
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer srv.Close()

	c := &GitHub{apiBase: srv.URL}
	got, err := c.PRChecks(context.Background(), "https://github.com/o/r.git", "tok", 9)
	require.NoError(t, err)
	require.False(t, got.Mergeable)
	require.Equal(t, "none", got.Status)
	require.Empty(t, got.Checks)
}

func TestGitHubJobLogTail(t *testing.T) {
	t.Run("returns only the last maxBytes on a valid UTF-8 boundary", func(t *testing.T) {
		// 10 ascii bytes then 5 "é" runes (2 bytes each, bytes 10-19). maxBytes=9
		// puts the naive cut at byte 11, the second (continuation) byte of the
		// first é, forcing tailUTF8 to trim forward into valid UTF-8.
		const maxBytes = 9
		filler := strings.Repeat("a", 10)
		multiByte := strings.Repeat("é", 5)
		log := filler + multiByte

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/repos/o/r/actions/jobs/456/logs", r.URL.Path)
			require.Equal(t, "Bearer tok", r.Header.Get("Authorization"))
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte(log))
		}))
		defer srv.Close()

		c := &GitHub{apiBase: srv.URL}
		got, err := c.JobLogTail(context.Background(), "https://github.com/o/r.git", "tok", "456", maxBytes)
		require.NoError(t, err)
		require.LessOrEqual(t, len(got), maxBytes)
		require.True(t, utf8.ValidString(got))
		require.True(t, strings.HasSuffix(log, got))
	})

	t.Run("404 returns empty string with no error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer srv.Close()

		c := &GitHub{apiBase: srv.URL}
		got, err := c.JobLogTail(context.Background(), "https://github.com/o/r.git", "tok", "456", 4000)
		require.NoError(t, err)
		require.Empty(t, got)
	})

	t.Run("non-404 non-2xx surfaces an HTTPError", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("boom"))
		}))
		defer srv.Close()

		c := &GitHub{apiBase: srv.URL}
		_, err := c.JobLogTail(context.Background(), "https://github.com/o/r.git", "tok", "456", 4000)
		require.Error(t, err)
		var he *HTTPError
		require.ErrorAs(t, err, &he)
		require.Equal(t, http.StatusInternalServerError, he.Status)
	})
}

func TestGitLabPRChecksJobStatusMapping(t *testing.T) {
	tests := []struct {
		glStatus       string
		wantStatus     string
		wantConclusion string
	}{
		{"created", "queued", ""},
		{"pending", "queued", ""},
		{"manual", "queued", ""},
		{"scheduled", "queued", ""},
		{"waiting_for_resource", "queued", ""},
		{"preparing", "queued", ""},
		{"running", "in_progress", ""},
		{"success", "completed", "success"},
		{"failed", "completed", "failure"},
		{"canceled", "completed", "cancelled"},
		{"skipped", "completed", "skipped"},
	}
	for _, tc := range tests {
		t.Run(tc.glStatus, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.EscapedPath() {
				case "/projects/g%2Fp/merge_requests/9":
					_ = json.NewEncoder(w).Encode(map[string]any{
						"sha":           "deadbeef",
						"merge_status":  "can_be_merged",
						"head_pipeline": map[string]any{"id": int64(555)},
					})
				case "/projects/g%2Fp/pipelines/555/jobs":
					_ = json.NewEncoder(w).Encode([]map[string]any{
						{"id": int64(111), "name": "build", "status": tc.glStatus, "web_url": "https://gitlab.example/j/111"},
					})
				default:
					t.Fatalf("unexpected path %q", r.URL.EscapedPath())
				}
			}))
			defer srv.Close()

			c := &GitLab{apiBase: srv.URL}
			got, err := c.PRChecks(context.Background(), "https://gitlab.example/g/p.git", "tok", 9)
			require.NoError(t, err)
			require.Equal(t, "deadbeef", got.HeadSHA)
			require.True(t, got.Mergeable)
			require.Len(t, got.Checks, 1)
			require.Equal(t, tc.wantStatus, got.Checks[0].Status)
			require.Equal(t, tc.wantConclusion, got.Checks[0].Conclusion)
			require.Equal(t, "111", got.Checks[0].JobID)
			require.Equal(t, "https://gitlab.example/j/111", got.Checks[0].URL)
		})
	}
}

func TestGitLabPRChecksNoHeadPipeline(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/projects/g%2Fp/merge_requests/9", r.URL.EscapedPath())
		_ = json.NewEncoder(w).Encode(map[string]any{
			"sha":           "deadbeef",
			"merge_status":  "cannot_be_merged",
			"head_pipeline": nil,
		})
	}))
	defer srv.Close()

	c := &GitLab{apiBase: srv.URL}
	got, err := c.PRChecks(context.Background(), "https://gitlab.example/g/p.git", "tok", 9)
	require.NoError(t, err)
	require.Equal(t, "none", got.Status)
	require.False(t, got.Mergeable)
	require.Empty(t, got.Checks)
}

func TestGitLabJobLogTail(t *testing.T) {
	t.Run("returns the trace body", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/projects/g%2Fp/jobs/111/trace", r.URL.EscapedPath())
			require.Equal(t, "tok", r.Header.Get("PRIVATE-TOKEN"))
			_, _ = w.Write([]byte("line1\nline2\n"))
		}))
		defer srv.Close()

		c := &GitLab{apiBase: srv.URL}
		got, err := c.JobLogTail(context.Background(), "https://gitlab.example/g/p.git", "tok", "111", 4000)
		require.NoError(t, err)
		require.Equal(t, "line1\nline2\n", got)
	})

	t.Run("404 returns empty string with no error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer srv.Close()

		c := &GitLab{apiBase: srv.URL}
		got, err := c.JobLogTail(context.Background(), "https://gitlab.example/g/p.git", "tok", "111", 4000)
		require.NoError(t, err)
		require.Empty(t, got)
	})

	t.Run("long log is tailed to maxBytes on a UTF-8 boundary", func(t *testing.T) {
		// 10 ascii bytes then 4 CJK runes (3 bytes each, bytes 10-21). maxBytes=8
		// puts the naive cut at byte 14, inside the second CJK rune's 3-byte
		// sequence (bytes 13-15), forcing tailUTF8 to trim forward into valid UTF-8.
		const maxBytes = 8
		filler := strings.Repeat("x", 10)
		multiByte := "中文中文"
		log := filler + multiByte

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(log))
		}))
		defer srv.Close()

		c := &GitLab{apiBase: srv.URL}
		got, err := c.JobLogTail(context.Background(), "https://gitlab.example/g/p.git", "tok", "111", maxBytes)
		require.NoError(t, err)
		require.LessOrEqual(t, len(got), maxBytes)
		require.True(t, utf8.ValidString(got))
		require.True(t, strings.HasSuffix(log, got))
	})
}

func TestTailUTF8(t *testing.T) {
	tests := []struct {
		name     string
		in       string
		maxBytes int
	}{
		{"empty string", "", 10},
		{"zero maxBytes", "hello", 0},
		{"shorter than maxBytes", "hi", 10},
		{"exact cut on ascii boundary", "0123456789", 5},
		// 9 ascii bytes then 3 "é" runes (2 bytes each, bytes 9-14). maxBytes=5
		// puts the naive cut at byte 10, the continuation byte of the first é.
		{"cut lands mid multi-byte rune", strings.Repeat("a", 9) + "ééé", 5},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tailUTF8(tc.in, tc.maxBytes)
			require.True(t, utf8.ValidString(got))
			require.LessOrEqual(t, len(got), tc.maxBytes)
			if tc.in != "" && tc.maxBytes > 0 {
				require.True(t, strings.HasSuffix(tc.in, got))
			}
		})
	}
}

// Compile-time assertions (var _ CIReader = ...) live in checks.go. This
// exercises interface satisfaction dynamically too, and covers the no-checks
// "none" status through the live PRChecks call path once more via a fresh
// server so the test file doesn't just rely on the static assertions.
func TestCIReaderSatisfiedByBothProviders(t *testing.T) {
	readers := []CIReader{&GitHub{}, &GitLab{}}
	require.Len(t, readers, 2)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/o/r/pulls/1":
			_ = json.NewEncoder(w).Encode(map[string]any{"head": map[string]any{"sha": "abc"}, "mergeable": false})
		case "/repos/o/r/commits/abc/check-runs":
			_ = json.NewEncoder(w).Encode(map[string]any{"check_runs": []map[string]any{}})
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer srv.Close()

	c := &GitHub{apiBase: srv.URL}
	got, err := c.PRChecks(context.Background(), "https://github.com/o/r.git", "tok", 1)
	require.NoError(t, err)
	require.Equal(t, "none", got.Status)
	require.Empty(t, got.Checks)
}
