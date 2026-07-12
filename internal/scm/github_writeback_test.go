package scm

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func newGitHub(t *testing.T, h http.HandlerFunc) *GitHub {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return &GitHub{apiBase: srv.URL}
}

func TestGitHubOpenChange(t *testing.T) {
	c := newGitHub(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/repos/o/r/pulls", r.URL.Path)
		require.Equal(t, "Bearer ghtok", r.Header.Get("Authorization"))
		require.Equal(t, "application/vnd.github+json", r.Header.Get("Accept"))
		var in map[string]string
		require.NoError(t, json.NewDecoder(r.Body).Decode(&in))
		require.Equal(t, "feature-x", in["head"])
		require.Equal(t, "main", in["base"])
		require.Equal(t, "Fix the bug", in["title"])
		require.Equal(t, "body text", in["body"])
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"html_url": "https://github.com/o/r/pull/42"})
	})

	url, err := c.OpenChange(context.Background(), "https://github.com/o/r.git", "ghtok", "feature-x", "main", "Fix the bug", "body text")
	require.NoError(t, err)
	require.Equal(t, "https://github.com/o/r/pull/42", url)
}

func TestGitHubComment(t *testing.T) {
	c := newGitHub(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/repos/o/r/issues/7/comments", r.URL.Path)
		require.Equal(t, "Bearer ghtok", r.Header.Get("Authorization"))
		var in map[string]string
		require.NoError(t, json.NewDecoder(r.Body).Decode(&in))
		require.Equal(t, "done", in["body"])
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1})
	})

	require.NoError(t, c.Comment(context.Background(), "ghtok", "o/r#7", "done"))
}

func TestGitHubOpenChangeErrorStatus(t *testing.T) {
	c := newGitHub(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"message":"A pull request already exists"}`))
	})
	_, err := c.OpenChange(context.Background(), "https://github.com/o/r.git", "t", "h", "b", "title", "body")
	require.Error(t, err)
	var he *HTTPError
	require.ErrorAs(t, err, &he)
	require.Equal(t, 422, he.Status)
}

func TestGitHubRemoveLabel(t *testing.T) {
	var hits int
	c := newGitHub(t, func(w http.ResponseWriter, r *http.Request) {
		hits++
		require.Equal(t, http.MethodDelete, r.Method)
		require.Equal(t, "/repos/o/r/issues/7/labels/tatara-rejected", r.URL.Path)
		require.Equal(t, "Bearer ghtok", r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusOK)
	})
	require.NoError(t, c.RemoveLabel(context.Background(), "ghtok", "o/r#7", "tatara-rejected"))
	require.Equal(t, 1, hits)
}

// A label that is not on the issue makes GitHub answer 404 "Label does not
// exist". RemoveLabel is best-effort sibling cleanup, so that 404 is a benign
// no-op and must not surface as an error (otherwise it inflates the SCM write
// error ratio). URL-encoding of slashed phase labels is exercised too.
func TestGitHubRemoveLabelAbsentIsNoop(t *testing.T) {
	c := newGitHub(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodDelete, r.Method)
		require.Equal(t, "/repos/o/r/issues/7/labels/tatara%2Fapproved", r.URL.EscapedPath())
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"Label does not exist"}`))
	})
	require.NoError(t, c.RemoveLabel(context.Background(), "ghtok", "o/r#7", "tatara/approved"))
}

// Genuine RemoveLabel failures (auth, permission, server, etc.) still surface
// so the write-failure metric reflects real outages.
func TestGitHubRemoveLabelRealErrorSurfaces(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusInternalServerError} {
		c := newGitHub(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"message":"nope"}`))
		})
		err := c.RemoveLabel(context.Background(), "ghtok", "o/r#7", "tatara-rejected")
		require.Error(t, err)
		var he *HTTPError
		require.ErrorAs(t, err, &he)
		require.Equal(t, status, he.Status)
	}
}

// Issue #301: approving a bot-authored PR returns a deterministic 422 "Can not
// approve your own pull request". That is terminal, not transient, so Approve must
// swallow it as a benign no-op (the tatara-approved label gates the merge) instead
// of surfacing an error that gets retried forever and floods the write-failure alert.
func TestGitHubApproveToleratesSelfApproval422(t *testing.T) {
	var hits int
	c := newGitHub(t, func(w http.ResponseWriter, r *http.Request) {
		hits++
		require.Equal(t, "/repos/o/r/pulls/5/reviews", r.URL.Path)
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"message":"Unprocessable Entity","errors":[{"resource":"PullRequestReview","field":"user_id","code":"custom","message":"Can not approve your own pull request"}]}`))
	})
	require.NoError(t, c.Approve(context.Background(), "https://github.com/o/r", "tok", 5, "lgtm"))
	require.Equal(t, 1, hits)
}

// A 422 that is NOT the self-approval case (e.g. some other validation failure)
// still surfaces so it is not silently swallowed.
func TestGitHubApprovePropagatesOther422(t *testing.T) {
	c := newGitHub(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"message":"Validation Failed","errors":[{"resource":"PullRequestReview","code":"missing_field","field":"body"}]}`))
	})
	err := c.Approve(context.Background(), "https://github.com/o/r", "tok", 5, "")
	require.Error(t, err)
	var he *HTTPError
	require.ErrorAs(t, err, &he)
	require.Equal(t, http.StatusUnprocessableEntity, he.Status)
}

func TestGitHubParse(t *testing.T) {
	tests := []struct {
		name      string
		repoURL   string
		wantOwner string
		wantRepo  string
		wantErr   bool
	}{
		{"https-git", "https://github.com/o/r.git", "o", "r", false},
		{"https-no-git", "https://github.com/o/r", "o", "r", false},
		{"trailing-slash", "https://github.com/o/r/", "o", "r", false},
		{"bad", "not a url with no path", "", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o, rp, err := ghOwnerRepo(tt.repoURL)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantOwner, o)
			require.Equal(t, tt.wantRepo, rp)
		})
	}
}

func TestGitHubIssueNumber(t *testing.T) {
	o, rp, n, err := ghIssueRef("o/r#123")
	require.NoError(t, err)
	require.Equal(t, "o", o)
	require.Equal(t, "r", rp)
	require.Equal(t, 123, n)

	_, _, _, err = ghIssueRef("garbage")
	require.Error(t, err)
}

func TestGitHubEditIssue_PatchesOnlyProvided(t *testing.T) {
	var gotBody map[string]any
	c := newGitHub(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPatch, r.Method)
		require.Equal(t, "/repos/o/r/issues/7", r.URL.Path)
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"number":7}`))
	})
	title := "new title"
	require.NoError(t, c.EditIssue(context.Background(), "t", "o/r", 7, EditIssueReq{Title: &title}))
	require.Equal(t, "new title", gotBody["title"])
	_, bodyPresent := gotBody["body"]
	require.False(t, bodyPresent, "body must NOT be sent when Body is nil")
}

func TestGitHubEditIssue_404Benign(t *testing.T) {
	c := newGitHub(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	body := "x"
	require.NoError(t, c.EditIssue(context.Background(), "t", "o/r", 7, EditIssueReq{Body: &body}))
}

func TestGitHubEnableAutoMerge(t *testing.T) {
	var gotGraphQL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		switch {
		case strings.Contains(string(body), "resource(url:"):
			_, _ = w.Write([]byte(`{"data":{"resource":{"id":"PR_node_123"}}}`))
		case strings.Contains(string(body), "enablePullRequestAutoMerge"):
			gotGraphQL = string(body)
			_, _ = w.Write([]byte(`{"data":{"enablePullRequestAutoMerge":{"clientMutationId":null}}}`))
		default:
			t.Fatalf("unexpected graphql body: %s", body)
		}
	}))
	defer srv.Close()

	c := &GitHub{graphQLBase: srv.URL}
	err := c.EnableAutoMerge(context.Background(), "https://github.com/o/r.git", "ghtok",
		"https://github.com/o/r/pull/7", "squash")
	require.NoError(t, err)
	require.Contains(t, gotGraphQL, "PR_node_123")
	require.Contains(t, gotGraphQL, "SQUASH")
}

// TestGitHubDisableAutoMerge covers the D1 disarm verb: an incomplete change
// whose PR was already opened with auto-merge armed must have it turned back
// off so the forge cannot merge it once its checks go green.
func TestGitHubDisableAutoMerge(t *testing.T) {
	var gotGraphQL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		switch {
		case strings.Contains(string(body), "resource(url:"):
			_, _ = w.Write([]byte(`{"data":{"resource":{"id":"PR_node_123"}}}`))
		case strings.Contains(string(body), "disablePullRequestAutoMerge"):
			gotGraphQL = string(body)
			_, _ = w.Write([]byte(`{"data":{"disablePullRequestAutoMerge":{"clientMutationId":null}}}`))
		default:
			t.Fatalf("unexpected graphql body: %s", body)
		}
	}))
	defer srv.Close()

	c := &GitHub{graphQLBase: srv.URL}
	err := c.DisableAutoMerge(context.Background(), "https://github.com/o/r.git", "ghtok",
		"https://github.com/o/r/pull/7")
	require.NoError(t, err)
	require.Contains(t, gotGraphQL, "PR_node_123")
}

func TestGitHubEnableAutoMergeError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), "resource(url:") {
			_, _ = w.Write([]byte(`{"data":{"resource":{"id":"PR_node_123"}}}`))
			return
		}
		_, _ = w.Write([]byte(`{"errors":[{"message":"Auto merge is not allowed"}]}`))
	}))
	defer srv.Close()
	c := &GitHub{graphQLBase: srv.URL}
	err := c.EnableAutoMerge(context.Background(), "https://github.com/o/r.git", "t",
		"https://github.com/o/r/pull/7", "squash")
	require.Error(t, err)
}
