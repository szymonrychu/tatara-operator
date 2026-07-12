package scm

import (
	"context"
	"encoding/json"
	"io"
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

	require.NoError(t, c.Comment(context.Background(), "gltok", "g/p#12", "done"))
}

func TestGitLabCommentMR(t *testing.T) {
	c := newGitLab(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/projects/g%2Fp/merge_requests/42/notes", r.URL.EscapedPath())
		require.Equal(t, "gltok", r.Header.Get("PRIVATE-TOKEN"))
		var in map[string]string
		require.NoError(t, json.NewDecoder(r.Body).Decode(&in))
		require.Equal(t, "done", in["body"])
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	require.NoError(t, c.Comment(context.Background(), "gltok", "g/p!42", "done"))
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

func TestGitLabHashRef(t *testing.T) {
	path, iid, err := glHashRef("g/sub/p#12")
	require.NoError(t, err)
	require.Equal(t, "g/sub/p", path)
	require.Equal(t, 12, iid)

	_, _, err = glHashRef("garbage")
	require.Error(t, err)
}

func TestGitLabBangRef(t *testing.T) {
	path, iid, err := glBangRef("g/sub/p!42")
	require.NoError(t, err)
	require.Equal(t, "g/sub/p", path)
	require.Equal(t, 42, iid)

	_, _, err = glBangRef("garbage")
	require.Error(t, err)

	_, _, err = glBangRef("g/p!notanumber")
	require.Error(t, err)
}

func TestGitLabLabelRef(t *testing.T) {
	proj, iid, resource, err := glLabelRef("g/sub/p#12")
	require.NoError(t, err)
	require.Equal(t, "g/sub/p", proj)
	require.Equal(t, 12, iid)
	require.Equal(t, "issues", resource)

	proj, iid, resource, err = glLabelRef("g/sub/p!42")
	require.NoError(t, err)
	require.Equal(t, "g/sub/p", proj)
	require.Equal(t, 42, iid)
	require.Equal(t, "merge_requests", resource)

	_, _, _, err = glLabelRef("garbage")
	require.Error(t, err)
}

// Issue #301: an MR label write (group/proj!iid) used to fail "malformed issue
// ref" because AddLabel only understood '#'. It must now route to the
// /merge_requests endpoint; an issue ref (#) still routes to /issues.
func TestGitLabAddLabelRoutesIssueAndMR(t *testing.T) {
	t.Run("issue", func(t *testing.T) {
		var body map[string]string
		c := newGitLab(t, func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, http.MethodPut, r.Method)
			require.Equal(t, "/projects/g%2Fp/issues/7", r.URL.EscapedPath())
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			w.WriteHeader(http.StatusOK)
		})
		require.NoError(t, c.AddLabel(context.Background(), "gltok", "g/p#7", "tatara-approved"))
		require.Equal(t, "tatara-approved", body["add_labels"])
	})
	t.Run("mr", func(t *testing.T) {
		var body map[string]string
		c := newGitLab(t, func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, http.MethodPut, r.Method)
			require.Equal(t, "/projects/g%2Fp/merge_requests/42", r.URL.EscapedPath())
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			w.WriteHeader(http.StatusOK)
		})
		require.NoError(t, c.AddLabel(context.Background(), "gltok", "g/p!42", "tatara-approved"))
		require.Equal(t, "tatara-approved", body["add_labels"])
	})
}

func TestGitLabRemoveLabelRoutesIssueAndMR(t *testing.T) {
	t.Run("issue", func(t *testing.T) {
		var body map[string]string
		c := newGitLab(t, func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, http.MethodPut, r.Method)
			require.Equal(t, "/projects/g%2Fp/issues/7", r.URL.EscapedPath())
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			w.WriteHeader(http.StatusOK)
		})
		require.NoError(t, c.RemoveLabel(context.Background(), "gltok", "g/p#7", "tatara-approved"))
		require.Equal(t, "tatara-approved", body["remove_labels"])
	})
	t.Run("mr", func(t *testing.T) {
		var body map[string]string
		c := newGitLab(t, func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, http.MethodPut, r.Method)
			require.Equal(t, "/projects/g%2Fp/merge_requests/42", r.URL.EscapedPath())
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			w.WriteHeader(http.StatusOK)
		})
		require.NoError(t, c.RemoveLabel(context.Background(), "gltok", "g/p!42", "tatara-approved"))
		require.Equal(t, "tatara-approved", body["remove_labels"])
	})
}

func TestGitLabEditIssue_PUTsOnlyProvided(t *testing.T) {
	var gotBody map[string]any
	c := newGitLab(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPut, r.Method)
		require.Equal(t, "/projects/g%2Fp/issues/7", glTestPath(r))
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"iid":7}`))
	})
	title := "new title"
	require.NoError(t, c.EditIssue(context.Background(), "t", "g/p", 7, EditIssueReq{Title: &title}))
	require.Equal(t, "new title", gotBody["title"])
	_, bodyPresent := gotBody["body"]
	require.False(t, bodyPresent, "body must NOT be sent when Body is nil")
}

func TestGitLabEditIssue_404Benign(t *testing.T) {
	c := newGitLab(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	body := "x"
	require.NoError(t, c.EditIssue(context.Background(), "t", "g/p", 7, EditIssueReq{Body: &body}))
}

func TestGitLabEnableAutoMerge(t *testing.T) {
	var gotPath, gotBody string
	c := newGitLab(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.EscapedPath()
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		_, _ = w.Write([]byte(`{}`))
	})
	err := c.EnableAutoMerge(context.Background(), "https://gitlab.com/g/p.git", "gltok",
		"https://gitlab.com/g/p/-/merge_requests/5", "squash")
	require.NoError(t, err)
	require.Equal(t, "/projects/g%2Fp/merge_requests/5/merge", gotPath)
	require.Contains(t, gotBody, "merge_when_pipeline_succeeds")
}

// TestGitLabDisableAutoMerge covers the D1 disarm verb on GitLab: cancel
// merge-when-pipeline-succeeds so an incomplete MR cannot merge itself.
func TestGitLabDisableAutoMerge(t *testing.T) {
	var gotPath, gotMethod string
	c := newGitLab(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.EscapedPath()
		gotMethod = r.Method
		_, _ = w.Write([]byte(`{}`))
	})
	err := c.DisableAutoMerge(context.Background(), "https://gitlab.com/g/p.git", "gltok",
		"https://gitlab.com/g/p/-/merge_requests/5")
	require.NoError(t, err)
	require.Equal(t, http.MethodPost, gotMethod)
	require.Equal(t, "/projects/g%2Fp/merge_requests/5/cancel_merge_when_pipeline_succeeds", gotPath)
}

func TestGLIIDFromURL(t *testing.T) {
	for _, tt := range []struct {
		url  string
		want int
		err  bool
	}{
		{"https://gitlab.com/g/p/-/merge_requests/5", 5, false},
		{"https://gitlab.com/g/p/-/merge_requests/5/", 5, false},
		{"https://gitlab.com/g/p/-/merge_requests/5?tab=x", 5, false},
		{"https://gitlab.com/g/p/-/merge_requests/notanint", 0, true},
	} {
		got, err := glIIDFromURL(tt.url)
		if tt.err {
			require.Error(t, err, tt.url)
			continue
		}
		require.NoError(t, err, tt.url)
		require.Equal(t, tt.want, got, tt.url)
	}
}
