package scm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func newGitLab(t *testing.T, h http.HandlerFunc) *GitLab {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return &GitLab{apiBase: srv.URL}
}

func TestGitLabOpenChange(t *testing.T) {
	c := newGitLab(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/projects/g%2Fp/merge_requests", r.URL.EscapedPath())
		require.Equal(t, "gltok", r.Header.Get("PRIVATE-TOKEN"))
		var in map[string]string
		require.NoError(t, json.NewDecoder(r.Body).Decode(&in))
		require.Equal(t, "feature-x", in["source_branch"])
		require.Equal(t, "main", in["target_branch"])
		require.Equal(t, "Fix the bug", in["title"])
		require.Equal(t, "body text", in["description"])
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"web_url": "https://gitlab.com/g/p/-/merge_requests/9"})
	})

	url, err := c.OpenChange(context.Background(), "https://gitlab.com/g/p.git", "gltok", "feature-x", "main", "Fix the bug", "body text")
	require.NoError(t, err)
	require.Equal(t, "https://gitlab.com/g/p/-/merge_requests/9", url)
}

func TestGitLabComment(t *testing.T) {
	c := newGitLab(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/projects/g%2Fp/issues/12/notes", r.URL.EscapedPath())
		require.Equal(t, "gltok", r.Header.Get("PRIVATE-TOKEN"))
		var in map[string]string
		require.NoError(t, json.NewDecoder(r.Body).Decode(&in))
		require.Equal(t, "done", in["body"])
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	require.NoError(t, c.Comment(context.Background(), "gltok", "g/p!12", "done"))
}

func TestGitLabOpenChangeErrorStatus(t *testing.T) {
	c := newGitLab(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"message":["already exists"]}`))
	})
	_, err := c.OpenChange(context.Background(), "https://gitlab.com/g/p.git", "t", "h", "b", "title", "body")
	require.Error(t, err)
	var he *HTTPError
	require.ErrorAs(t, err, &he)
	require.Equal(t, 409, he.Status)
}

func TestGitLabProjectPath(t *testing.T) {
	tests := []struct {
		name    string
		repoURL string
		want    string
		wantErr bool
	}{
		{"git", "https://gitlab.com/g/p.git", "g/p", false},
		{"no-git", "https://gitlab.com/g/p", "g/p", false},
		{"subgroup", "https://gitlab.com/g/sub/p.git", "g/sub/p", false},
		{"bad", "::::", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := glProjectPath(tt.repoURL)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestGitLabIssueRef(t *testing.T) {
	path, iid, err := glIssueRef("g/sub/p!12")
	require.NoError(t, err)
	require.Equal(t, "g/sub/p", path)
	require.Equal(t, 12, iid)

	_, _, err = glIssueRef("garbage")
	require.Error(t, err)
}
