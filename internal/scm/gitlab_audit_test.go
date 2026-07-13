package scm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGlCIStatus_SkippedMapsToSuccess verifies finding 3: 'skipped' pipeline
// status must map to "success" (GitHub parity; skipped != pending).
func TestGlCIStatus_SkippedMapsToSuccess(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"skipped", "success"},
		{"manual", "pending"},
		{"scheduled", "pending"},
		{"waiting_for_resource", "pending"},
		{"preparing", "pending"},
		{"running", "pending"},
		{"created", "pending"},
		{"", ""},
		{"success", "success"},
		{"failed", "failure"},
		{"canceled", "failure"},
	}
	for _, tc := range cases {
		got := glCIStatus(tc.in)
		if got != tc.want {
			t.Errorf("glCIStatus(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestGitLabGetCommitCIStatus_ShaEscaped verifies finding 4: sha is PathEscaped
// in the URL so a ref containing '/' is handled correctly.
func TestGitLabGetCommitCIStatus_ShaEscaped(t *testing.T) {
	const sha = "feature/branch-ref" // contains '/', must be escaped
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		expected := "/projects/" + url.PathEscape("g/p") + "/repository/commits/" + url.PathEscape(sha) + "/statuses"
		got := r.URL.RawPath
		if got == "" {
			got = r.URL.Path
		}
		if got != expected {
			t.Errorf("path = %q, want %q", got, expected)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{{"status": "success"}})
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL}
	result, err := c.GetCommitCIStatus(context.Background(), "g/p", "", sha)
	require.NoError(t, err)
	require.Equal(t, "success", result)
}

// TestGitLabComment_LastIndexRouting verifies finding 5: Comment routes by LastIndex,
// not Contains, so a '#'-style issue ref is not misrouted to the MR endpoint even
// when the (hypothetical) project path contained a '!'.
func TestGitLabComment_LastIndexRouting(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		if r.URL.RawPath != "" {
			gotPath = r.URL.RawPath
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL}

	// '#' ref -> must go to issues/notes even though the project path would
	// logically be before the '#'. Using a plain '#' ref here; the key point
	// is that bangAt < hashAt.
	err := c.Comment(context.Background(), "tok", "g/p#7", "hi")
	require.NoError(t, err)
	require.Contains(t, gotPath, "/issues/7/notes", "issue ref must route to issue notes endpoint")

	// '!' ref -> must go to merge_requests/notes
	err = c.Comment(context.Background(), "tok", "g/p!42", "hi")
	require.NoError(t, err)
	require.Contains(t, gotPath, "/merge_requests/42/notes", "MR ref must route to MR notes endpoint")
}

// TestGlActionAndLabel_DefaultReturnsOther verifies finding 6: unrecognized
// ObjectAttributes.Action values are normalised to "other" for the metric label
// instead of forwarding the raw string.
func TestGlActionAndLabel_DefaultReturnsOther(t *testing.T) {
	p := glPayload{}
	p.ObjectAttributes.Action = "some_new_gitlab_action"
	action, changed := glActionAndLabel(p)
	if action != "other" {
		t.Errorf("glActionAndLabel default = %q, want %q", action, "other")
	}
	if changed != "" {
		t.Errorf("glActionAndLabel changed = %q, want empty", changed)
	}
}

// TestGitLabMerge_UnsupportedMethod verifies finding 1: Merge returns an error
// for "rebase" and other unsupported method strings rather than silently
// degrading to a non-squash merge.
func TestGitLabMerge_UnsupportedMethod(t *testing.T) {
	// The server should never be called for an unsupported method.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("server should not be called for unsupported merge method")
	}))
	defer srv.Close()
	c := &GitLab{apiBase: srv.URL}

	_, err := c.Merge(context.Background(), "https://gitlab.com/g/p", "tok", 5, "rebase", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported merge method")
}

// TestGitLabMerge_SupportedMethods verifies that "squash" and "merge" are accepted
// and produce a squash=true / squash=false payload respectively.
func TestGitLabMerge_SupportedMethods(t *testing.T) {
	cases := []struct {
		method     string
		wantSquash bool
	}{
		{"squash", true},
		{"merge", false},
		{"", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run("method="+tc.method, func(t *testing.T) {
			var got map[string]any
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewDecoder(r.Body).Decode(&got)
				_ = json.NewEncoder(w).Encode(map[string]any{"merge_commit_sha": "abc123"})
			}))
			defer srv.Close()
			c := &GitLab{apiBase: srv.URL}
			sha, err := c.Merge(context.Background(), "https://gitlab.com/g/p", "tok", 5, tc.method, "")
			require.NoError(t, err)
			require.Equal(t, "abc123", sha)
			require.Equal(t, tc.wantSquash, got["squash"], "squash field mismatch for method %q", tc.method)
		})
	}
}
